import assert from "node:assert";
import type { Pool } from "pg";
import {
  POLLING_INTERVAL,
  DEFAULT_CONSUMER_CONCURRENCY,
  DEFAULT_POSTGRES_CONNECTION_PARAMS,
} from "~/constants";
import { loggerFactory } from "~/logger";
import { forceShutdownTimeout } from "~/promises";
import type {
  Store,
  ConsumerConfig,
  Notice,
  NoticeHandler,
  PubSub,
  FromProducerNotice,
} from "~/types";

export class Consumer {
  /** Store backing the queue. */
  private store: Store;

  /** PubSub for sending and receiving notices. */
  private pubsub: PubSub;

  /** Timer for polling the queue for messages to process. */
  private pollingTimer: NodeJS.Timeout | undefined;

  /** Handlers to call on receiving notices via pubsub. */
  private pubsubHandlers: Map<FromProducerNotice["type"], NoticeHandler[]> =
    new Map();

  /** Number of messages to process concurrently. */
  private concurrency: number;

  /** Promises for messages that are currently being processed. */
  private processing: Set<Promise<void>> = new Set();

  /** Whether the consumer is currently shutting down. */
  private isShuttingDown = false;

  /** Connection pool for store and pubsub, if on Postgres. */
  private pool: Pool | undefined;

  constructor(
    private readonly consumerConfig: ConsumerConfig,
    private readonly logger = loggerFactory("consumer")
  ) {
    this.concurrency =
      consumerConfig.concurrency ?? DEFAULT_CONSUMER_CONCURRENCY;
  }

  async init() {
    const { type, connectionParams } = this.consumerConfig;

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

    await this.pubsub.subscribe("to_consumers", (rawNotice) => {
      const notice = JSON.parse(rawNotice);

      if (!this.isValidNotice(notice)) return;

      this.logger.debug("Received notice via pubsub", {
        noticeType: notice.type,
      });

      const handlersByType = this.pubsubHandlers.get(notice.type) ?? [];
      handlersByType.forEach((handler) => handler(notice));
    });

    this.pollingTimer = setInterval(
      () => this.processNextMsg(),
      POLLING_INTERVAL
    );

    this.logger.info("Init completed");
  }

  async shutdown() {
    this.logger.info("Shutting down");

    this.isShuttingDown = true;

    clearInterval(this.pollingTimer);

    this.pollingTimer = undefined;

    try {
      await Promise.race([
        Promise.all(Array.from(this.processing)),
        forceShutdownTimeout(),
      ]);
    } finally {
      this.pool?.end();
    }

    await Promise.all([this.store.disconnect(), this.pubsub.disconnect()]);
  }

  /** Send a notice to all consumers via pubsub. */
  async notify(notice: Notice) {
    await this.pubsub.publish("to_producers", JSON.stringify(notice));
    this.logger.debug("Sent notice via pubsub", { noticeType: notice.type });
  }

  /** Register a handler to call for each notice of a type received via pubsub. */
  on(noticeType: FromProducerNotice["type"], handler: NoticeHandler) {
    const handlersByType = this.pubsubHandlers.get(noticeType) ?? [];
    handlersByType.push(handler);
    this.pubsubHandlers.set(noticeType, handlersByType);
  }

  // ----------------------------------
  //         private methods
  // ----------------------------------

  private async processNextMsg() {
    if (this.isShuttingDown) return;

    while (this.processing.size < this.concurrency) {
      const msg = await this.store.nextMsg({
        queueName: this.consumerConfig.queueName,
      });

      if (msg === null) return;

      this.logger.debug("Received message", { msgId: msg.id });

      let processingPromise: Promise<void>;
      const processMsg = async () => {
        try {
          await this.consumerConfig.processorFn(msg);

          this.logger.debug("Processed message", { msgId: msg.id });

          await this.store.removeMsg(
            msg.id,
            this.consumerConfig.queueName,
            msg.visibilityTs
          );

          await this.notify({
            type: "msg-processed",
            payload: { msgId: msg.id },
          });
        } catch (error) {
          assert(error instanceof Error);
          this.logger.error(error);
        } finally {
          this.processing.delete(processingPromise);
        }
      };

      processingPromise = processMsg();
      this.processing.add(processingPromise);
    }
  }

  private isValidNotice(raw: unknown): raw is FromProducerNotice {
    if (!raw || typeof raw !== "object") return false;
    if (!("type" in raw) || !("payload" in raw)) return false;
    if (raw.type !== "abort-msg") return false;
    if (!raw.payload || typeof raw.payload !== "object") return false;

    return "msgId" in raw.payload && typeof raw.payload.msgId === "string";
  }
}
