import assert from "node:assert";
import { URL } from "node:url";
import aws4 from "aws4";
import { HttpClient } from "~/http";
import { loggerFactory } from "~/logger";
import {
  DEFAULT_AWS_CONNECTION_PARAMS,
  MAX_MSG_SIZE,
  VISIBILITY_TIMEOUT,
} from "~/constants";
import { OversizeMsgError } from "~/errors/oversize-msg.error";
import type { Request as Aws4Options } from "aws4";
import type {
  Store,
  SendMsgParams,
  NextMsgParams,
  Msg,
  SqsConnectionParams,
  CreateQueueResponse,
  ReceiveMessageResponse,
  GetQueueAttributesResponse,
  GetQueueUrlResponse,
} from "~/types";
import { randomId } from "~/random";
import { UnsupportedPriorityError } from "~/errors/unsupported-priority.error";

export class SqsStore implements Store {
  private region: string;
  private credentials: { accessKeyId: string; secretAccessKey: string };
  private queueUrls = new Map<string, string>();
  private receiptHandles = new Map<string, string>();
  private endpoint: string;

  constructor(
    private readonly logger = loggerFactory("sqs.store"),
    private readonly httpClient = new HttpClient()
  ) {}

  async connect(params: Partial<SqsConnectionParams>) {
    const { region, accessKeyId, secretAccessKey, endpoint } = {
      ...DEFAULT_AWS_CONNECTION_PARAMS,
      ...params,
    };

    if (!accessKeyId) throw new Error("AWS access key ID is missing");
    if (!secretAccessKey) throw new Error("AWS secret access key is missing");

    this.region = region;
    this.credentials = { accessKeyId, secretAccessKey };
    this.endpoint = endpoint;

    try {
      await this.request("ListQueues");
    } catch (error) {
      assert(error instanceof Error);
      this.logger.error(error);
    }
  }

  async disconnect() {
    // no connection to close
  }

  async createQueue(queueName: string) {
    try {
      const { QueueUrl: existingQueueUrl } =
        await this.request<GetQueueUrlResponse>("GetQueueUrl", {
          QueueName: `${queueName}.fifo`,
        });

      if (existingQueueUrl) {
        this.queueUrls.set(queueName, existingQueueUrl);
        return false;
      }

      const { QueueUrl: newQueueUrl } = await this.request<CreateQueueResponse>(
        "CreateQueue",
        {
          QueueName: `${queueName}.fifo`,
          Attributes: {
            FifoQueue: "true",
            ContentBasedDeduplication: "false",
            VisibilityTimeout: (VISIBILITY_TIMEOUT / 1000).toString(),
            MaximumMessageSize: MAX_MSG_SIZE.toString(),
          },
        }
      );

      if (newQueueUrl) {
        this.queueUrls.set(queueName, newQueueUrl);
        return true;
      }

      return false;
    } catch (error) {
      assert(error instanceof Error);
      if (error.message.includes("QueueAlreadyExists")) return false;
      throw error;
    }
  }

  async destroyQueue(queueName: string) {
    try {
      const response = await this.request("DeleteQueue", {
        QueueUrl: this.queueUrls.get(queueName),
      });

      if (
        typeof response === "object" &&
        response !== null &&
        "message" in response &&
        typeof response.message === "string" &&
        response.message.includes("exception")
      ) {
        return false;
      }

      return true;
    } catch {
      return false;
    }
  }

  async sendMsg(msg: string, { queueName, customId, priority }: SendMsgParams) {
    if (priority) throw new UnsupportedPriorityError();

    if (Buffer.byteLength(msg, "utf-8") > MAX_MSG_SIZE) {
      throw new OversizeMsgError();
    }

    const msgId = customId ?? "msg_" + randomId();

    await this.request("SendMessage", {
      QueueUrl: this.queueUrls.get(queueName),
      MessageBody: msg,

      /**
       * In an SQS FIFO queue, _within a message group_, a newer message will
       * not be delivered until an older message has been processed. To prevent
       * this blocking behavior, we place each message in its own message group.
       */
      MessageGroupId: msgId,

      MessageDeduplicationId: msgId,
      MessageAttributes: {
        MessageId: {
          DataType: "String",
          StringValue: msgId,
        },
        EnqueuedAt: {
          DataType: "Number",
          StringValue: Date.now().toString(),
        },
      },
    });

    return msgId;
  }

  async nextMsg({ queueName }: NextMsgParams): Promise<Msg | null> {
    const response = await this.request<ReceiveMessageResponse>(
      "ReceiveMessage",
      {
        QueueUrl: this.queueUrls.get(queueName),
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 0,
        MessageAttributeNames: ["All"], // @TODO: Slim down
        AttributeNames: ["All"], // @TODO: Slim down
      }
    );

    if (!response.Messages?.length) return null;

    const sqsMessage = response.Messages[0];

    const { ApproximateReceiveCount } = sqsMessage.Attributes ?? {};
    const { MessageId, EnqueuedAt } = sqsMessage.MessageAttributes ?? {};

    if (!MessageId?.StringValue) {
      throw new Error("MessageDeduplicationId attribute missing");
    }

    if (!EnqueuedAt?.StringValue) {
      throw new Error("EnqueuedAt attribute missing");
    }

    this.receiptHandles.set(MessageId.StringValue, sqsMessage.ReceiptHandle);

    return {
      id: MessageId.StringValue,
      content: sqsMessage.Body,
      enqueuedAt: parseInt(EnqueuedAt.StringValue),
      visibilityTs: Date.now() + VISIBILITY_TIMEOUT, // only an approximation, SQS does not report this
      receiveCount: parseInt(ApproximateReceiveCount, 10),
    };
  }

  async removeMsg(msgId: string, queueName: string) {
    return this.receiptHandles.has(msgId)
      ? await this.removeMsgWithReceiptHandle(msgId, queueName)
      : await this.removeMsgWithoutReceiptHandle(msgId, queueName);
  }

  async getQueueLength(queueName: string) {
    const response = await this.request<GetQueueAttributesResponse>(
      "GetQueueAttributes",
      {
        QueueUrl: this.queueUrls.get(queueName),
        AttributeNames: ["ApproximateNumberOfMessages"],
      }
    );

    const { ApproximateNumberOfMessages } = response.Attributes ?? {};

    if (!ApproximateNumberOfMessages) {
      throw new Error("ApproximateNumberOfMessages attribute missing");
    }

    return parseInt(ApproximateNumberOfMessages, 10);
  }

  // ----------------------------------
  //         private methods
  // ----------------------------------

  private async request<T>(action: string, body: Record<string, unknown> = {}) {
    const endpoint =
      this.endpoint || `https://sqs.${this.region}.amazonaws.com`;
    const url = new URL(endpoint);

    const method = "POST";

    const optionsToSign: Aws4Options = {
      method,
      service: "sqs",
      region: this.region,
      host: url.hostname,
      path: url.pathname,
      headers: {
        "Content-Type": "application/x-amz-json-1.0",
        "X-Amz-Target": `AmazonSQS.${action}`,
      },
      body: JSON.stringify(body),
    };

    const signedOptions = aws4.sign(optionsToSign, this.credentials);

    const response = await this.httpClient.request<T>({
      method,
      url: endpoint,
      headers: signedOptions.headers as Record<string, string>, // @TODO: Type this better
      data: body,
    });

    return response.data;
  }

  private async removeMsgWithReceiptHandle(msgId: string, queueName: string) {
    const receiptHandle = this.receiptHandles.get(msgId);

    if (!receiptHandle) return false;

    try {
      await this.request("DeleteMessage", {
        QueueUrl: this.queueUrls.get(queueName),
        ReceiptHandle: receiptHandle,
      });
      this.receiptHandles.delete(msgId);
      return true;
    } catch (error) {
      assert(error instanceof Error);
      this.logger.error(error);
      return false;
    }
  }

  private async removeMsgWithoutReceiptHandle(
    msgId: string,
    queueName: string
  ) {
    try {
      const queueUrl = this.queueUrls.get(queueName);

      const response = await this.request<ReceiveMessageResponse>(
        "ReceiveMessage",
        {
          QueueUrl: queueUrl,
          MaxNumberOfMessages: 1,
          VisibilityTimeout: 0,
          MessageAttributeNames: ["All"],
          AttributeNames: ["All"],
        }
      );

      if (!response.Messages) return false;

      const message = response.Messages.find(
        (msg) => msg.MessageAttributes?.MessageId?.StringValue === msgId
      );

      if (!message) return false;

      await this.request("DeleteMessage", {
        QueueUrl: queueUrl,
        ReceiptHandle: message.ReceiptHandle,
      });

      return true;
    } catch (error) {
      assert(error instanceof Error);
      this.logger.error(error);
      return false;
    }
  }
}
