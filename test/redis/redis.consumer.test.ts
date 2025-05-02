import Redis from "ioredis";
import { Consumer } from "~/actors/consumer";
import {
  DEFAULT_QUEUE_NAME,
  DEFAULT_REDIS_CONNECTION_PARAMS,
} from "~/constants";
import { createDeferredPromise, sleep } from "~/promises";
import type {
  AbortMsgNotice,
  ConsumerConfig,
  Msg,
  MsgProcessedNotice,
  Notice,
} from "~/types";

const LIB_PREFIX = "test_quick_consumer";
const HASH_KEY = `${LIB_PREFIX}:${DEFAULT_QUEUE_NAME}`;
const SORTED_SET_KEY = `${HASH_KEY}:sequence`;
const ALL_KEYS = `${LIB_PREFIX}:*`;
const TO_CONSUMERS_CHANNEL = `${LIB_PREFIX}:to_consumers`;
const TO_PRODUCERS_CHANNEL = `${LIB_PREFIX}:to_producers`;

const CONSUMER_CONFIG: ConsumerConfig = {
  type: "redis",
  queueName: DEFAULT_QUEUE_NAME,
  connectionParams: {
    ...DEFAULT_REDIS_CONNECTION_PARAMS,
    libPrefix: LIB_PREFIX,
  },
  processorFn: jest.fn(),
};

let redis: Redis;

beforeAll(() => {
  redis = new Redis();
});

afterEach(async () => {
  const allKeys = await redis.keys(ALL_KEYS);
  if (allKeys.length > 0) await redis.del(allKeys);
});

afterAll(async () => {
  await redis.quit();
});

describe("pubsub", () => {
  it("on() should receive notices of target type", async () => {
    const consumer = new Consumer(CONSUMER_CONFIG);
    await consumer.init();

    const receivedNotices: Notice[] = [];
    const deferred = createDeferredPromise<void>();

    consumer.on("abort-msg", (notice) => {
      receivedNotices.push(notice);
      deferred.resolve();
    });

    const notice: AbortMsgNotice = {
      type: "abort-msg",
      payload: { msgId: "test_msg" },
    };

    await redis.publish(TO_CONSUMERS_CHANNEL, JSON.stringify(notice));
    await deferred.promise;

    expect(receivedNotices).toHaveLength(1);
    expect(receivedNotices[0]).toEqual(notice);

    await consumer.shutdown();
  });

  it("on() should not receive notices of non-target type", async () => {
    const consumer = new Consumer(CONSUMER_CONFIG);
    await consumer.init();
    const timeoutDeferred = createDeferredPromise<void>();
    let timedOut = false;

    const receivedNotices: Notice[] = [];

    consumer.on("abort-msg", (notice) => {
      receivedNotices.push(notice);
    });

    const notice = {
      type: "other-type",
      payload: { msgId: "test_msg" },
    };

    await redis.publish(TO_CONSUMERS_CHANNEL, JSON.stringify(notice));

    setTimeout(() => {
      timedOut = true;
      timeoutDeferred.resolve();
    }, 100);

    await timeoutDeferred.promise;

    expect(timedOut).toBe(true);
    expect(receivedNotices).toHaveLength(0);

    await consumer.shutdown();
  });

  it("notify() should publish notice to producers", async () => {
    const consumer = new Consumer(CONSUMER_CONFIG);
    await consumer.init();

    const receivedNotices: Notice[] = [];
    const deferred = createDeferredPromise<void>();

    const subscriber = new Redis();
    await subscriber.subscribe(TO_PRODUCERS_CHANNEL);

    subscriber.on("message", (_, message) => {
      receivedNotices.push(JSON.parse(message));
      deferred.resolve();
    });

    const notice: MsgProcessedNotice = {
      type: "msg-processed",
      payload: { msgId: "test_msg" },
    };

    await consumer.notify(notice);
    await deferred.promise;

    expect(receivedNotices).toHaveLength(1);
    expect(receivedNotices[0]).toEqual(notice);

    await subscriber.quit();
    await consumer.shutdown();
  });
});

describe("processing", () => {
  /**
   * At default concurrency, we enqueue two messages sequentially with delays
   * between them to test FIFO ordering. We enqueue the first message and verify
   * its processing after one polling interval, then enqueue the second message
   * and verify its processing after another polling interval. This proves that
   * messages are processed in enqueue order.
   */
  it("should poll queue and process messages in FIFO order", async () => {
    const processedMsgs: string[] = [];
    const firstProcessed = createDeferredPromise<void>();
    const secondProcessed = createDeferredPromise<void>();

    const processorFn = async (msg: Msg) => {
      processedMsgs.push(msg.content);

      await redis
        .multi()
        .hdel(
          HASH_KEY,
          msg.id,
          `${msg.id}:enqueued_at`,
          `${msg.id}:receive_count`
        )
        .zrem(SORTED_SET_KEY, `5:${msg.visibilityTs}:${msg.id}`)
        .exec();

      if (msg.content === "first") firstProcessed.resolve();
      if (msg.content === "second") secondProcessed.resolve();
    };

    const consumer = new Consumer({
      ...CONSUMER_CONFIG,
      processorFn,
    });

    const firstMsgId = "test_msg_1";
    const firstNowMs = Date.now();
    const firstMember = `5:${firstNowMs}:${firstMsgId}`;

    await redis
      .multi()
      .hset(HASH_KEY, firstMsgId, "first")
      .hset(HASH_KEY, `${firstMsgId}:enqueued_at`, firstNowMs)
      .zadd(SORTED_SET_KEY, 0, firstMember)
      .exec();

    await consumer.init();
    await firstProcessed.promise;

    expect(processedMsgs).toHaveLength(1);
    expect(processedMsgs[0]).toBe("first");

    const secondMsgId = "test_msg_2";
    const secondNowMs = Date.now();
    const secondMember = `5:${secondNowMs}:${secondMsgId}`;

    await redis
      .multi()
      .hset(HASH_KEY, secondMsgId, "second")
      .hset(HASH_KEY, `${secondMsgId}:enqueued_at`, secondNowMs)
      .zadd(SORTED_SET_KEY, 0, secondMember)
      .exec();

    await secondProcessed.promise;

    expect(processedMsgs).toHaveLength(2);
    expect(processedMsgs).toEqual(["first", "second"]);

    await consumer.shutdown();
  });

  /**
   * At concurrency 3, we enqueue three messages where the "second" message
   * takes longer to process than "first" and "third", and we track when each
   * message starts processing and their completion order. Since they all start
   * within 100ms of each other and complete out of enqueue order (with the
   * longer message finishing last), this proves they were processed
   * concurrently rather than sequentially.
   */
  it("should process multiple messages concurrently", async () => {
    const processingStarts: number[] = [];
    const completionOrder: string[] = [];
    const allProcessed = createDeferredPromise<void>();

    const processorFn = async (msg: Msg) => {
      processingStarts.push(Date.now());
      await (msg.content === "second" ? sleep(100) : sleep(50));
      completionOrder.push(msg.content);
      if (completionOrder.length === 3) allProcessed.resolve();
    };

    const consumer = new Consumer({
      ...CONSUMER_CONFIG,
      processorFn,
      concurrency: 3,
    });

    await consumer.init();

    const now = Date.now();
    const messages = ["first", "second", "third"];

    for (let i = 0; i < messages.length; i++) {
      const msgId = `msg_${i}`;
      await redis
        .multi()
        .hset(HASH_KEY, msgId, messages[i])
        .hset(HASH_KEY, `${msgId}:enqueued_at`, now + i)
        .zadd(SORTED_SET_KEY, 0, `5:${now + i}:${msgId}`)
        .exec();
    }

    await allProcessed.promise;

    const startTimeSpread =
      Math.max(...processingStarts) - Math.min(...processingStarts);
    expect(startTimeSpread).toBeLessThan(100);

    expect(completionOrder).toEqual(["first", "third", "second"]);

    await consumer.shutdown();
  });
});
