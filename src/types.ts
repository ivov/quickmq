export type QueueType = "redis" | "postgres";

// ----------------------------------
//            producer
// ----------------------------------

import type { RedisOptions } from "ioredis";
import type { Pool } from "pg";

export type RedisProducerConfig = {
  type: "redis";
  queueName: string;
  connectionParams: RedisConnectionParams;
};

export type PostgresProducerConfig = {
  type: "postgres";
  queueName: string;
  connectionParams: PostgresConnectionParams;
};

export type SqsSnsProducerConfig = {
  type: "sqs-sns";
  queueName: string;
  connectionParams: { sqs: SqsConnectionParams; sns: SnsConnectionParams };
};

export type ProducerConfig =
  | RedisProducerConfig
  | PostgresProducerConfig
  | SqsSnsProducerConfig;

export type EnqueueMsgParams = SendMsgParams;

// ----------------------------------
//            consumer
// ----------------------------------

export type ProcessorFn = (msg: Msg) => Promise<void>;

export type ConsumerConfig =
  | RedisConsumerConfig
  | PostgresConsumerConfig
  | SqsSnsConsumerConfig;

export type RedisConsumerConfig = {
  type: "redis";
  queueName: string;
  connectionParams: RedisConnectionParams;
  processorFn: ProcessorFn;
  concurrency?: number;
};

export type PostgresConsumerConfig = {
  type: "postgres";
  queueName: string;
  connectionParams: PostgresConnectionParams;
  processorFn: ProcessorFn;
  concurrency?: number;
};

export type SqsSnsConsumerConfig = {
  type: "sqs-sns";
  queueName: string;
  connectionParams: { sqs: SqsConnectionParams; sns: SnsConnectionParams };
  processorFn: ProcessorFn;
  concurrency?: number;
};

// ----------------------------------
//              store
// ----------------------------------

export type StoreConnectParams =
  | Partial<RedisConnectionParams>
  | Pool
  | Partial<SqsConnectionParams>;

export interface Store {
  connect(params: StoreConnectParams): Promise<void>;
  disconnect(): Promise<void>;

  /** Create a queue, returning `true` if it was created or `false` if it already existed. */
  createQueue(queueName: string): Promise<boolean>;

  /** Destroy a queue, returning `true` if it was found and destroyed or false if not. */
  destroyQueue(queueName: string): Promise<boolean>;

  /** Place a message in the queue, returning the message ID. */
  sendMsg(msg: string, opts: SendMsgParams): Promise<Msg["id"]>;

  /** Return the next available message, or `null` if none found. */
  nextMsg(opts: NextMsgParams): Promise<Msg | null>;

  /**
   * Remove a message by ID from the queue, returning `true` if found and
   * removed or `false` if not.
   *
   * @param messageId ID of the message to remove.
   * @param queueName Name of the queue to remove the message from.
   * @param leaseExpiresAt When a consumer's ownership of the message. After
   * delivery, removal can happen only if this timestamp has not changed since
   * delivery. Therefore, a consumer after processing a message must specify
   * this parameter when deleting the message, but a producer omits this
   * parameter when deleting a message that has not been delivered yet.
   */
  removeMsg(
    messageId: string,
    queueName: string,
    leaseExpiresAt?: UnixTimestampMs
  ): Promise<boolean>;

  /** Retrieve the number of messages currently in the queue. */
  getQueueLength(queueName: string): Promise<number>;
}

// ----------------------------------
//              msg
// ----------------------------------

type UnixTimestampMs = number;

export type Msg = {
  id: string;
  content: string;
  enqueuedAt: UnixTimestampMs;
  visibilityTs: UnixTimestampMs;
  receiveCount: number;
};

export type SendMsgParams = {
  /** Name of the queue to send the message to. */
  queueName: string;

  /** Optional custom ID of the message to send. */
  customId?: string;

  /** Priority of the message to send. */
  priority?: number;
};

export type NextMsgParams = {
  /** Name of the queue to get the next message from. */
  queueName: string;
};

// ----------------------------------
//             pubsub
// ----------------------------------

type PubSubConnectParams =
  | Partial<RedisConnectionParams>
  | { libPrefix: string; pool: Pool };

export interface PubSub {
  connect(params: PubSubConnectParams): Promise<void>;
  disconnect(): Promise<void>;
  subscribe(
    channel: Channel,
    handler: (rawNotice: string) => void
  ): Promise<void>;
  publish(channel: Channel, rawNotice: string): Promise<void>;
}

export type Channel = "to_consumers" | "to_producers";

export type Topic = Channel;

/**
 * Notice sent between producers and consumers via pubsub.
 * Typically for tasks ancillary to queueing and processing.
 * Do not confuse notices with queue messages.
 */
export type Notice = FromConsumerNotice | FromProducerNotice;

export type FromConsumerNotice = MsgProcessedNotice;

export type FromProducerNotice = AbortMsgNotice;

export type MsgProcessedNotice = {
  type: "msg-processed";
  payload: { msgId: string };
};

export type AbortMsgNotice = {
  type: "abort-msg";
  payload: { msgId: string };
};

export type RawNoticeHandler = (rawNotice: string) => void | Promise<void>;

export type NoticeHandler = (notice: Notice) => void | Promise<void>;

// ----------------------------------
//               sns
// ----------------------------------

export type SnsAction = "CreateTopic" | "ListTopics" | "Publish" | "Subscribe";

export type SnsConnectionParams = {
  region: string;
  endpoint: string;
  accessKeyId: string;
  secretAccessKey: string;
  serverUrl: string;
  serverPort: number;
  libPrefix: string;
};

export type ListTopicsResponse = { Topics: { TopicArn: string }[] };

export type CreateTopicResponse = { TopicArn: string };

export type SnsMessage = SnsSubscriptionConfirmation | SnsNotification;

export type SnsSubscriptionConfirmation = {
  Type: "SubscriptionConfirmation";
  SubscribeURL: string;
  Token: string;
  TopicArn: string;
  Message: string;
  MessageId: string;
  Timestamp: string;
  SignatureVersion: string;
  Signature: string;
  SigningCertURL: string;
};

export type SnsNotification = {
  Type: "Notification";
  TopicArn: string;
  Message: string;
  MessageId: string;
  Timestamp: string;
  SignatureVersion: string;
  Signature: string;
  SigningCertURL: string;
  UnsubscribeURL: string;
};

// ----------------------------------
//               sqs
// ----------------------------------

export type SqsConnectionParams = {
  region: string;
  endpoint: string;
  accessKeyId: string;
  secretAccessKey: string;
};

export type GetQueueUrlResponse = { QueueUrl?: string };

export type ListQueuesResponse = { QueueUrls?: string[] };

export type CreateQueueResponse = { QueueUrl?: string };

export type SendMessageResponse = { MessageId: string };

export type ReceiveMessageResponse = {
  Messages?: Array<{
    MessageId: string;
    ReceiptHandle: string;
    Body: string;
    Attributes: {
      MessageDeduplicationId: string;
      ApproximateReceiveCount: string;
    };
    MessageAttributes?: {
      MessageId?: { StringValue: string };
      EnqueuedAt?: { StringValue: string };
      Priority?: { StringValue: string };
    };
  }>;
};

export type GetQueueAttributesResponse = {
  Attributes?: {
    ApproximateNumberOfMessages: string;
  };
};

export type DeleteMessageResponse = {};

export type DeleteQueueResponse = {};

// ----------------------------------
//             redis
// ----------------------------------

export type RedisConnectionParams = {
  host: RedisOptions["host"];
  port: RedisOptions["port"];
  password?: RedisOptions["password"];

  /**
   * Prefix for all Redis keys managed by this library.
   *
   * @default "quick"
   */
  libPrefix: string;
};

export type NextMsgResult =
  | [
      messageId: string,
      messageContent: string,
      enqueuedAt: number,
      visibilityTs: number,
      receiveCount: number,
    ]
  | [];

export type RawPipelineResults =
  | [error: Error | null, result: unknown][]
  | null;

// ----------------------------------
//            postgres
// ----------------------------------

export type PostgresConnectionParams = {
  host: string;
  port: number;
  database: string;
  user: string;
  password?: string;

  /**
   * Prefix for all Postgres pubsub channels managed by this library.
   *
   * @default "quick"
   */
  libPrefix: string;
};

// ----------------------------------
//            promises
// ----------------------------------

type ResolveFn<T> = (result: T | PromiseLike<T>) => void;
type RejectFn = (error: Error) => void;

export interface DeferredPromise<T> {
  promise: Promise<T>;
  resolve: ResolveFn<T>;
  reject: RejectFn;
}
