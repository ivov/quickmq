import { Pool } from "pg";
import { PostgresPubSub } from "~/pubsub/postgres.pubsub";
import { createDeferredPromise } from "~/promises";
import type { Channel, Notice } from "~/types";
import { DEFAULT_POSTGRES_CONNECTION_PARAMS } from "~/constants";

const LIB_PREFIX = "test_pubsub";
const DB_NAME = "test_quick_postgres_pubsub";

let bootstrap: Pool;
let pool: Pool;
let pubsub: PostgresPubSub;

beforeAll(async () => {
  bootstrap = new Pool(DEFAULT_POSTGRES_CONNECTION_PARAMS);

  await bootstrap.query(`DROP DATABASE IF EXISTS ${DB_NAME};`);
  await bootstrap.query(`CREATE DATABASE ${DB_NAME};`);

  pubsub = new PostgresPubSub();
  pool = new Pool({
    ...DEFAULT_POSTGRES_CONNECTION_PARAMS,
    database: DB_NAME,
  });
  await pubsub.connect({ libPrefix: LIB_PREFIX, pool });
});

afterAll(async () => {
  await pubsub.disconnect();
  await pool.end();
  await bootstrap.query(`DROP DATABASE ${DB_NAME};`);
  await bootstrap.end();
});

type SingleSubscriberTestCase = [Channel, Notice];

const singleCases: SingleSubscriberTestCase[] = [
  ["to_consumers", { type: "msg-processed", payload: { msgId: "msg_id" } }],
  ["to_producers", { type: "abort-msg", payload: { msgId: "test-msg-123" } }],
];

it.each(singleCases)(
  "single subscriber to `%s` should receive notice",
  async (channel, notice) => {
    const receivedNotices: string[] = [];
    const deferred = createDeferredPromise<void>();

    await pubsub.subscribe(channel, (message) => {
      receivedNotices.push(message);
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
