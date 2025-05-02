import { SnsPubSub } from "~/pubsub/sns.pubsub";
import { createDeferredPromise, sleep } from "~/promises";
import type { Notice, Topic } from "~/types";

const CONNECTION_PARAMS = {
  region: "eu-west-3",
  endpoint: "http://localhost:4566",
  accessKeyId: "test",
  secretAccessKey: "test",
  serverUrl: "http://host.docker.internal:3001", // for localstack to reach back to host
  serverPort: 3001,
  libPrefix: "test_sns_pubsub",
};

let pubsub: SnsPubSub;

beforeEach(async () => {
  pubsub = new SnsPubSub();
});

afterEach(async () => {
  await pubsub.disconnect();
  await sleep(100);
});

type SingleSubscriberTestCase = [Topic, Notice];

const singleCases: SingleSubscriberTestCase[] = [
  ["to_consumers", { type: "abort-msg", payload: { msgId: "test-msg-123" } }],
  ["to_producers", { type: "msg-processed", payload: { msgId: "msg_id" } }],
];

it.each(singleCases)(
  "single subscriber to `%s` should receive notice",
  async (topic, notice) => {
    await pubsub.connect(CONNECTION_PARAMS);

    const receivedNotices: string[] = [];
    const deferred = createDeferredPromise<void>();

    await pubsub.subscribe(topic, (message) => {
      receivedNotices.push(message);
      deferred.resolve();
    });

    const noticeStr = JSON.stringify(notice);
    await pubsub.publish(topic, noticeStr);

    await deferred.promise;

    expect(receivedNotices).toHaveLength(1);
    expect(receivedNotices[0]).toBe(noticeStr);
  }
);

const multiCases: [Topic, string][] = [
  ["to_consumers", "test notice 1"],
  ["to_producers", "test notice 2"],
];

it.each(multiCases)(
  "multiple subscribers to `%s` should receive notices",
  async (topic, notice) => {
    await pubsub.connect(CONNECTION_PARAMS);

    const firstSubscriberMsgs: string[] = [];
    const secondSubscriberMsgs: string[] = [];
    const deferred = createDeferredPromise<void>();
    let receivedCount = 0;

    await pubsub.subscribe(topic, (msg) => {
      firstSubscriberMsgs.push(msg);
      receivedCount++;
      if (receivedCount === 2) deferred.resolve();
    });

    await pubsub.subscribe(topic, (msg) => {
      secondSubscriberMsgs.push(msg);
      receivedCount++;
      if (receivedCount === 2) deferred.resolve();
    });

    await pubsub.publish(topic, notice);

    await deferred.promise;

    expect(firstSubscriberMsgs).toHaveLength(1);
    expect(secondSubscriberMsgs).toHaveLength(1);
    expect(firstSubscriberMsgs[0]).toBe(notice);
    expect(secondSubscriberMsgs[0]).toBe(notice);
  }
);

type ChannelMismatchCase = [Topic, Topic, string];

const channelMismatchCases: ChannelMismatchCase[] = [
  ["to_producers", "to_consumers", "test notice 1"],
  ["to_consumers", "to_producers", "test notice 2"],
];

it.each(channelMismatchCases)(
  "subscriber to `%s` should not receive notices published to other channel",
  async (subscribeChannel, publishChannel, notice) => {
    await pubsub.connect(CONNECTION_PARAMS);

    const receivedNotices: string[] = [];
    const deferred = createDeferredPromise<void>();
    let timedOut = false;

    await pubsub.subscribe(subscribeChannel, (msg) => {
      receivedNotices.push(msg);
    });

    await pubsub.publish(publishChannel, notice);
    setTimeout(() => {
      timedOut = true;
      deferred.resolve();
    }, 100);

    await deferred.promise;

    expect(timedOut).toBe(true);
    expect(receivedNotices).toHaveLength(0);
  }
);
