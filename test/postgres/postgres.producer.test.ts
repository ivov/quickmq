import { Pool } from "pg";
import { Producer } from "~/actors/producer";
import {
  DEFAULT_QUEUE_NAME,
  DEFAULT_POSTGRES_CONNECTION_PARAMS,
} from "~/constants";
import { createDeferredPromise } from "~/promises";
import type {
  AbortMsgNotice,
  MsgProcessedNotice,
  Notice,
  ProducerConfig,
} from "~/types";

const DB_NAME = "test_quick_postgres_producer";
const LIB_PREFIX = "test_producer";
const TO_CONSUMERS_CHANNEL = `${LIB_PREFIX}:to_consumers`;
const TO_PRODUCERS_CHANNEL = `${LIB_PREFIX}:to_producers`;

const PRODUCER_CONFIG: ProducerConfig = {
  type: "postgres",
  queueName: DEFAULT_QUEUE_NAME,
  connectionParams: {
    ...DEFAULT_POSTGRES_CONNECTION_PARAMS,
    database: DB_NAME,
    libPrefix: LIB_PREFIX,
  },
};

const ENQUEUE_MSG_PARAMS = {
  queueName: DEFAULT_QUEUE_NAME,
  priority: 5,
};

let bootstrap: Pool;
let pool: Pool;
let producer: Producer;

beforeAll(async () => {
  bootstrap = new Pool(DEFAULT_POSTGRES_CONNECTION_PARAMS);
  await bootstrap.query(`DROP DATABASE IF EXISTS ${DB_NAME};`);
  await bootstrap.query(`CREATE DATABASE ${DB_NAME};`);

  pool = new Pool({
    ...DEFAULT_POSTGRES_CONNECTION_PARAMS,
    database: DB_NAME,
  });
});

beforeEach(async () => {
  producer = new Producer(PRODUCER_CONFIG);
  await producer.init();
});

afterEach(async () => {
  await producer.shutdown();
  await pool.query("TRUNCATE queues CASCADE;");
});

afterAll(async () => {
  await pool.end();
  await bootstrap.end();
});

describe("enqueue", () => {
  it("should enqueue message and return message ID", async () => {
    const { msgId } = await producer.enqueue("message", ENQUEUE_MSG_PARAMS);
    expect(msgId).toMatch(/^msg_[0-9a-fA-F]{32}$/);

    const result = await pool.query(
      "SELECT content FROM messages WHERE id = $1;",
      [msgId]
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].content).toBe("message");
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

    await pool.query(`SELECT pg_notify($1, $2)`, [
      TO_PRODUCERS_CHANNEL,
      JSON.stringify(notice),
    ]);

    await promise;

    expect(hasResolved).toBe(true);
  });
});

describe("remove", () => {
  it("should remove enqueued message", async () => {
    const { msgId } = await producer.enqueue("message", ENQUEUE_MSG_PARAMS);

    const resultPre = await pool.query(
      "SELECT id FROM messages WHERE id = $1;",
      [msgId]
    );
    expect(resultPre.rows).toHaveLength(1);

    const wasRemoved = await producer.remove(msgId);
    expect(wasRemoved).toBe(true);

    const resultPost = await pool.query(
      "SELECT id FROM messages WHERE id = $1;",
      [msgId]
    );
    expect(resultPost.rows).toHaveLength(0);
  });

  it("should return `false` when removing non-existent message", async () => {
    const wasRemoved = await producer.remove("non_existent_msg");
    expect(wasRemoved).toBe(false);
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

    await pool.query(`SELECT pg_notify($1, $2)`, [
      TO_PRODUCERS_CHANNEL,
      JSON.stringify(notice),
    ]);

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

    await pool.query(`SELECT pg_notify($1, $2)`, [
      TO_PRODUCERS_CHANNEL,
      JSON.stringify(notice),
    ]);

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

    const client = await pool.connect();
    await client.query(`LISTEN "${TO_CONSUMERS_CHANNEL}";`);

    client.on("notification", (msg) => {
      if (msg.payload) {
        receivedNotices.push(JSON.parse(msg.payload));
        deferred.resolve();
      }
    });

    const notice: AbortMsgNotice = {
      type: "abort-msg",
      payload: { msgId: "test_msg" },
    };

    await producer.notify(notice);
    await deferred.promise;

    expect(receivedNotices).toHaveLength(1);
    expect(receivedNotices[0]).toEqual(notice);

    client.release();
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
