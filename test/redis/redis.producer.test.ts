import Redis from "ioredis";
import { Producer } from "~/actors/producer";
import {
  DEFAULT_QUEUE_NAME,
  DEFAULT_REDIS_CONNECTION_PARAMS,
} from "~/constants";
import type {
  AbortMsgNotice,
  MsgProcessedNotice,
  Notice,
  ProducerConfig,
} from "~/types";
import { createDeferredPromise } from "~/promises";

const LIB_PREFIX = "test_quick_producer";
const HASH_KEY = `${LIB_PREFIX}:${DEFAULT_QUEUE_NAME}`;
const ALL_KEYS = `${LIB_PREFIX}:*`;
const TO_PRODUCERS_CHANNEL = `${LIB_PREFIX}:to_producers`;
const TO_CONSUMERS_CHANNEL = `${LIB_PREFIX}:to_consumers`;

const PRODUCER_CONFIG: ProducerConfig = {
  type: "redis",
  queueName: DEFAULT_QUEUE_NAME,
  connectionParams: {
    ...DEFAULT_REDIS_CONNECTION_PARAMS,
    libPrefix: LIB_PREFIX,
  },
};

const ENQUEUE_MSG_PARAMS = {
  queueName: DEFAULT_QUEUE_NAME,
  priority: 5,
};

let redis: Redis;
let producer: Producer;

beforeAll(() => {
  redis = new Redis();
});

beforeEach(async () => {
  producer = new Producer(PRODUCER_CONFIG);
  await producer.init();
});

afterEach(async () => {
  await producer.shutdown();
  const allKeys = await redis.keys(ALL_KEYS);
  if (allKeys.length > 0) await redis.del(allKeys);
});

afterAll(async () => {
  await redis.quit();
});

describe("enqueue", () => {
  it("should enqueue message and return message ID", async () => {
    const { msgId } = await producer.enqueue("message", ENQUEUE_MSG_PARAMS);
    expect(msgId).toMatch(/^msg_[0-9a-fA-F]{32}$/);

    const msgContent = await redis.hget(HASH_KEY, msgId);
    expect(msgContent).toBe("message");
  });

  it("should return promise that resolves when message is processed", async () => {
    const { msgId, processed } = await producer.enqueue(
      "message",
      ENQUEUE_MSG_PARAMS
    );

    const notice: MsgProcessedNotice = {
      type: "msg-processed",
      payload: { msgId },
    };

    let hasResolved = false;
    const promise = processed().then(() => {
      hasResolved = true;
    });

    await Promise.resolve(); // give microtasks a chance to execute

    expect(hasResolved).toBe(false);

    await redis.publish(TO_PRODUCERS_CHANNEL, JSON.stringify(notice));

    await promise;

    expect(hasResolved).toBe(true);
  });
});

describe("cancel", () => {
  it("should cancel enqueued message", async () => {
    const { msgId } = await producer.enqueue("message", ENQUEUE_MSG_PARAMS);

    const wasCancelled = await producer.remove(msgId);
    expect(wasCancelled).toBe(true);

    const exists = await redis.hexists(HASH_KEY, msgId);
    expect(exists).toBe(0);
  });

  it("should return `false` when cancelling non-existent message", async () => {
    const wasCancelled = await producer.remove("non_existent_msg");
    expect(wasCancelled).toBe(false);
  });
});

describe("on", () => {
  it("should receive notices of registered type", async () => {
    const receivedNotices: Notice[] = [];
    const deferred = createDeferredPromise<void>();

    producer.on("msg-processed", (notice) => {
      receivedNotices.push(notice);
      deferred.resolve();
    });

    const notice: MsgProcessedNotice = {
      type: "msg-processed",
      payload: { msgId: "test_msg" },
    };

    await redis.publish(TO_PRODUCERS_CHANNEL, JSON.stringify(notice));
    await deferred.promise;

    expect(receivedNotices).toHaveLength(1);
    expect(receivedNotices[0]).toEqual(notice);
  });

  it("should not receive notices of unregistered type", async () => {
    const receivedNotices: Notice[] = [];
    const timeoutDeferred = createDeferredPromise<void>();
    let timedOut = false;

    producer.on("msg-processed", (notice) => {
      receivedNotices.push(notice);
    });

    const notice = {
      type: "other-type",
      payload: { msgId: "test_msg" },
    };

    await redis.publish(TO_PRODUCERS_CHANNEL, JSON.stringify(notice));

    setTimeout(() => {
      timedOut = true;
      timeoutDeferred.resolve();
    }, 100);

    await timeoutDeferred.promise;

    expect(timedOut).toBe(true);
    expect(receivedNotices).toHaveLength(0);
  });
});

describe("notify", () => {
  it("should publish notice to consumers", async () => {
    const receivedNotices: Notice[] = [];
    const deferred = createDeferredPromise<void>();

    const subscriber = new Redis();
    await subscriber.subscribe(TO_CONSUMERS_CHANNEL);

    subscriber.on("message", (_, message) => {
      receivedNotices.push(JSON.parse(message));
      deferred.resolve();
    });

    const notice: AbortMsgNotice = {
      type: "abort-msg",
      payload: { msgId: "test_msg" },
    };

    await producer.notify(notice);
    await deferred.promise;

    expect(receivedNotices).toHaveLength(1);
    expect(receivedNotices[0]).toEqual(notice);

    await subscriber.quit();
  });
});

describe("getQueueLength", () => {
  it("should return number of enqueued messages", async () => {
    await expect(producer.getQueueLength()).resolves.toBe(0);

    await producer.enqueue("first", ENQUEUE_MSG_PARAMS);
    await expect(producer.getQueueLength()).resolves.toBe(1);

    await producer.enqueue("second", ENQUEUE_MSG_PARAMS);
    await expect(producer.getQueueLength()).resolves.toBe(2);
  });

  it("should decrease count when messages are removed", async () => {
    const { msgId } = await producer.enqueue("message", ENQUEUE_MSG_PARAMS);
    await expect(producer.getQueueLength()).resolves.toBe(1);

    await producer.remove(msgId);

    await expect(producer.getQueueLength()).resolves.toBe(0);
  });
});
