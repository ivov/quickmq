import assert from "node:assert";
import { URL } from "node:url";
import aws4 from "aws4";
import { HttpClient, HttpServer, parseRequestBody } from "~/http";
import { loggerFactory } from "~/logger";
import { DEFAULT_SNS_CONNECTION_PARAMS, inTest } from "~/constants";
import type { IncomingMessage, ServerResponse } from "node:http";
import type {
  PubSub,
  Channel,
  SnsConnectionParams,
  Topic,
  ListTopicsResponse,
  CreateTopicResponse,
  SnsMessage,
  SnsAction,
  RawNoticeHandler,
} from "~/types";
import type { Request as Aws4Options } from "aws4";

export class SnsPubSub implements PubSub {
  private region: string;
  private credentials: { accessKeyId: string; secretAccessKey: string };
  private libPrefix: string;
  private endpoint: string;

  private topicArns: Map<Topic, string> = new Map();
  private topicsByArn: Map<string, Topic> = new Map();

  private server: HttpServer;
  private serverUrl: string;
  private handlers: Map<Topic, RawNoticeHandler[]> = new Map();

  constructor(
    private readonly logger = loggerFactory("sns.pubsub"),
    private readonly httpClient = new HttpClient()
  ) {
    this.server = new HttpServer(this.pushHandler);
  }

  async connect(params: Partial<SnsConnectionParams>) {
    const {
      region,
      accessKeyId,
      secretAccessKey,
      libPrefix,
      endpoint,
      serverUrl,
      serverPort,
    } = {
      ...DEFAULT_SNS_CONNECTION_PARAMS,
      ...params,
    };

    if (!accessKeyId) throw new Error("AWS access key ID is missing");
    if (!secretAccessKey) throw new Error("AWS secret access key is missing");

    this.region = region;
    this.credentials = { accessKeyId, secretAccessKey };
    this.libPrefix = libPrefix;
    this.serverUrl = serverUrl;
    this.endpoint = endpoint;

    await this.server.listen(serverPort);

    this.logger.info(`Server listening on port ${serverPort}`);
  }

  async disconnect() {
    await this.server.close();
  }

  async publish(channel: Channel, message: string) {
    const topicArn = await this.getOrCreateTopic(channel);

    await this.request("Publish", {
      TopicArn: topicArn,
      Message: message,
    });
  }

  async subscribe(topic: Topic, handler: RawNoticeHandler) {
    const handlers = this.handlers.get(topic) ?? [];
    handlers.push(handler);
    this.handlers.set(topic, handlers);

    const topicArn = await this.getOrCreateTopic(topic);
    this.topicsByArn.set(topicArn, topic);

    await this.request("Subscribe", {
      TopicArn: topicArn,
      Protocol: inTest ? "http" : "https",
      Endpoint: this.serverUrl,
    });
  }

  // ----------------------------------
  //         private methods
  // ----------------------------------

  private async getOrCreateTopic(topic: Topic) {
    const cachedArn = this.topicArns.get(topic);

    if (cachedArn) return cachedArn;

    const fullTopicName = `${this.libPrefix}_${topic}`;

    const listResponse = await this.request<ListTopicsResponse>("ListTopics");

    const fetchedArn = listResponse.Topics.map((t) => t.TopicArn).find((t) =>
      t.endsWith(`:${fullTopicName}`)
    );

    if (fetchedArn) {
      this.topicArns.set(topic, fetchedArn);
      return fetchedArn;
    }

    const { TopicArn: createdArn } = await this.request<CreateTopicResponse>(
      "CreateTopic",
      { Name: fullTopicName }
    );

    this.topicArns.set(topic, createdArn);

    return createdArn;
  }

  private async request<T>(
    action: SnsAction,
    params: Record<string, unknown> = {}
  ): Promise<T> {
    const endpoint =
      this.endpoint || `https://sns.${this.region}.amazonaws.com`;
    const url = new URL(endpoint);
    const method = "POST";

    const formData = new URLSearchParams({
      Action: action,
      Version: "2010-03-31",
      ...this.httpClient.flatten(params),
    }).toString();

    const optionsToSign: Aws4Options = {
      method,
      service: "sns",
      region: this.region,
      host: url.hostname,
      path: url.pathname,
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: formData,
    };

    const signedOptions = aws4.sign(optionsToSign, this.credentials);

    const response = await this.httpClient.request<string>({
      method,
      url: url.toString(),
      headers: signedOptions.headers as Record<string, string>,
      data: formData,
    });

    return this.parseXml<T>(response.data, action);
  }

  private parseXml<T>(xmlResponse: string, action: SnsAction): T {
    const result: Record<string, any> = {};

    if (action === "CreateTopic") {
      const topicArnMatch = xmlResponse.match(/<TopicArn>([^<]+)<\/TopicArn>/);
      if (topicArnMatch) result.TopicArn = topicArnMatch[1];
      return result as T;
    }

    if (action === "ListTopics") {
      if (xmlResponse.includes("<Topics />")) {
        result.Topics = [];
        return result as T;
      }

      const matches = Array.from(
        xmlResponse.matchAll(/<TopicArn>([^<]+)<\/TopicArn>/g)
      );

      result.Topics =
        matches.length > 0 ? matches.map((m) => ({ TopicArn: m[1] })) : [];

      return result as T;
    }

    if (action === "Publish") {
      const match = xmlResponse.match(/<MessageId>([^<]+)<\/MessageId>/);
      if (match) result.MessageId = match[1];
      return result as T;
    }

    if (action === "Subscribe") return result as T;

    throw new Error(`Unexpected SNS action: ${action}`);
  }

  pushHandler = async (req: IncomingMessage, res: ServerResponse) => {
    try {
      const notice = await parseRequestBody<SnsMessage>(req);

      if (notice.Type === "SubscriptionConfirmation") {
        await this.httpClient.request({
          method: "GET",
          url: notice.SubscribeURL,
        });
      } else if (notice.Type === "Notification") {
        const topicArn = notice.TopicArn;
        const topic = this.topicsByArn.get(topicArn);

        if (topic) {
          const handlers = this.handlers.get(topic) ?? [];
          handlers.forEach((handler) => handler(notice.Message));
        }
      }

      res.setHeader("Content-Type", "text/plain");
      res.writeHead(200);
      res.end();
    } catch (error) {
      assert(error instanceof Error);
      this.logger.error(error);
      res.writeHead(400);
      res.end();
    }
  };
}
