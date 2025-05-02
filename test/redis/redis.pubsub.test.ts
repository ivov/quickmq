import Redis from "ioredis";
import { RedisPubSub } from "~/pubsub/redis.pubsub";
import { createDeferredPromise } from "~/promises";
import type { Channel, Notice } from "~/types";

const LIB_PREFIX = "test_quick_redis_pubsub";
const ALL_KEYS = `${LIB_PREFIX}:*`;

let redis: Redis;
let pubsub: RedisPubSub;

beforeAll(() => {
  redis = new Redis();
});

beforeEach(async () => {
  pubsub = new RedisPubSub();
  await pubsub.connect({ libPrefix: LIB_PREFIX });
});

afterEach(async () => {
  await pubsub.disconnect();
  const allKeys = await redis.keys(ALL_KEYS);
  if (allKeys.length > 0) await redis.del(allKeys);
});

afterAll(async () => {
  await redis.quit();
});

type SingleSubscriberTestCase = [Channel, Notice];

const singleCases: SingleSubscriberTestCase[] = [
  ["to_consumers", { type: "abort-msg", payload: { msgId: "test-msg-123" } }],
  ["to_producers", { type: "msg-processed", payload: { msgId: "msg_id" } }],
];

it.each(singleCases)(
  "single subscriber to `%s` should receive notice",
  async (channel, notice) => {
    const receivedNotices: string[] = [];
    const deferred = createDeferredPromise<void>();

    await pubsub.subscribe(channel, (notice) => {
      receivedNotices.push(notice);
      deferred.resolve();
    });

    const noticeStr = JSON.stringify(notice);
    await pubsub.publish(channel, noticeStr);
    await deferred.promise;

    expect(receivedNotices).toHaveLength(1);
    expect(receivedNotices[0]).toBe(noticeStr);
  }
);

type MultiSubscriberCase = [Channel, string];

const multiCases: MultiSubscriberCase[] = [
  ["to_consumers", "test notice 1"],
  ["to_producers", "test notice 2"],
];

it.each(multiCases)(
  "multiple subscribers to `%s` should receive notices",
  async (channel, notice) => {
    const firstSubscriberMsgs: string[] = [];
    const secondSubscriberMsgs: string[] = [];
    const deferred = createDeferredPromise<void>();
    let receivedCount = 0;

    await pubsub.subscribe(channel, (msg) => {
      firstSubscriberMsgs.push(msg);
      receivedCount++;
      if (receivedCount === 2) deferred.resolve();
    });

    await pubsub.subscribe(channel, (msg) => {
      secondSubscriberMsgs.push(msg);
      receivedCount++;
      if (receivedCount === 2) deferred.resolve();
    });

    await pubsub.publish(channel, notice);
    await deferred.promise;

    expect(firstSubscriberMsgs).toHaveLength(1);
    expect(secondSubscriberMsgs).toHaveLength(1);
    expect(firstSubscriberMsgs[0]).toBe(notice);
    expect(secondSubscriberMsgs[0]).toBe(notice);
  }
);

type ChannelMismatchCase = [Channel, Channel, string];

const channelMismatchCases: ChannelMismatchCase[] = [
  ["to_producers", "to_consumers", "test notice 1"],
  ["to_consumers", "to_producers", "test notice 2"],
];

it.each(channelMismatchCases)(
  "subscriber to `%s` should not receive notices published to other channel",
  async (subscribeChannel, publishChannel, notice) => {
    const receivedNotices: string[] = [];
    const timeoutDeferred = createDeferredPromise<void>();
    let timedOut = false;

    await pubsub.subscribe(subscribeChannel, (msg) => {
      receivedNotices.push(msg);
    });

    await pubsub.publish(publishChannel, notice);

    setTimeout(() => {
      timedOut = true;
      timeoutDeferred.resolve();
    }, 100);

    await timeoutDeferred.promise;

    expect(timedOut).toBe(true);
    expect(receivedNotices).toHaveLength(0);
  }
);
