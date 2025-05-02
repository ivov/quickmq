import type { Pool } from "pg";
import { createDeferredPromise } from "~/promises";
import type {
  Store,
  EnqueueMsgParams,
  ProducerConfig,
  Notice,
  NoticeHandler,
  PubSub,
  DeferredPromise,
  FromConsumerNotice,
} from "~/types";
import { DEFAULT_POSTGRES_CONNECTION_PARAMS } from "~/constants";
import { loggerFactory } from "~/logger";

export class Producer {
  /** Name of the queue connected to. */
  private queueName: string;

  /** Store backing the queue. */
  private store: Store;

  private pubsub: PubSub;

  /** Handlers to call on receiving notices via pubsub. */
  private pubsubHandlers: Map<FromConsumerNotice["type"], NoticeHandler[]> =
    new Map();

  /** Promises for messages that have been enqueued. */
  private enqueued: Map<string, DeferredPromise<void>> = new Map();

  /** Connection pool for store and pubsub, if on Postgres. */
  private pool?: Pool;

  constructor(
    private readonly producerConfig: ProducerConfig,
    private readonly logger = loggerFactory("producer")
  ) {
    this.queueName = this.producerConfig.queueName;
  }

  async init() {
    const { type, connectionParams } = this.producerConfig;

    if (type === "postgres") {
      const { Pool } = await import("pg");
      const { PostgresStore } = await import(
        "../stores/postgres/postgres.store"
      );
      const { PostgresPubSub } = await import("../pubsub/postgres.pubsub");

      this.pool = new Pool({
        ...DEFAULT_POSTGRES_CONNECTION_PARAMS,
        ...connectionParams,
      });
      this.pool.on("error", (error) => this.logger.error(error));

      this.store = new PostgresStore();
      this.pubsub = new PostgresPubSub();

      await this.store.connect(this.pool);
      await this.pubsub.connect({
        pool: this.pool,
        libPrefix: connectionParams.libPrefix,
      });
    } else if (type === "redis") {
      const { RedisStore } = await import("../stores/redis/redis.store");
      const { RedisPubSub } = await import("../pubsub/redis.pubsub");
      this.store = new RedisStore();
      this.pubsub = new RedisPubSub();
      await this.store.connect(connectionParams);
      await this.pubsub.connect(connectionParams);
    } else if (type === "sqs-sns") {
      const { SqsStore } = await import("../stores/sqs/sqs.store");
      const { SnsPubSub } = await import("../pubsub/sns.pubsub");
      this.store = new SqsStore();
      this.pubsub = new SnsPubSub();
      await this.store.connect(connectionParams.sqs);
      await this.pubsub.connect(connectionParams.sns);
    }

    this.on("msg-processed", (notice) => {
      const { msgId } = notice.payload;
      this.enqueued.get(msgId)?.resolve();
      this.enqueued.delete(msgId);
    });

    await this.store.createQueue(this.queueName);

    await this.pubsub.subscribe("to_producers", (rawNotice) => {
      const notice = JSON.parse(rawNotice);

      if (!this.isValidNotice(notice)) return;

      this.logger.debug("Received notice via pubsub", {
        noticeType: notice.type,
      });

      const handlers = this.pubsubHandlers.get(notice.type) ?? [];
      handlers.forEach((handler) => handler(notice));
    });

    this.logger.info("Init completed");
  }

  async shutdown() {
    this.logger.info("Shutting down");

    this.pool?.end();

    await Promise.all([this.store.disconnect(), this.pubsub.disconnect()]);
  }

  /** Place a message in the queue, returning a promise that resolves when it is processed. */
  async enqueue(msg: string, params: EnqueueMsgParams) {
    const msgId = await this.store.sendMsg(msg, params);
    const deferred = createDeferredPromise();
    this.enqueued.set(msgId, deferred);

    this.logger.debug("Enqueued message", { msgId });

    return {
      msgId,
      processed: () => deferred.promise,
    };
  }

  /** Remove a message from the queue, typically for cancellation. */
  async remove(msgId: string) {
    const wasRemoved = await this.store.removeMsg(msgId, this.queueName);

    if (wasRemoved) this.logger.debug("Cancelled message", { msgId });

    return wasRemoved;
  }

  /** Send a notice to all consumers via pubsub. */
  async notify(notice: Notice) {
    await this.pubsub.publish("to_consumers", JSON.stringify(notice));

    this.logger.debug("Sent notice via pubsub", { noticeType: notice.type });
  }

  /** Register a handler for a notice type. */
  on(noticeType: FromConsumerNotice["type"], handler: NoticeHandler) {
    const handlersByType = this.pubsubHandlers.get(noticeType) ?? [];
    handlersByType.push(handler);
    this.pubsubHandlers.set(noticeType, handlersByType);
  }

  /** Retrieve the number of messages currently in the queue. */
  async getQueueLength() {
    return await this.store.getQueueLength(this.queueName);
  }

  // ----------------------------------
  //         private methods
  // ----------------------------------

  private isValidNotice(raw: unknown): raw is FromConsumerNotice {
    if (!raw || typeof raw !== "object") return false;
    if (!("type" in raw) || !("payload" in raw)) return false;
    if (raw.type !== "msg-processed") return false;
    if (!raw.payload || typeof raw.payload !== "object") return false;

    return "msgId" in raw.payload && typeof raw.payload.msgId === "string";
  }
}
