import { Pool } from "pg";
import { Consumer } from "~/actors/consumer";
import {
  DEFAULT_QUEUE_NAME,
  DEFAULT_POSTGRES_CONNECTION_PARAMS,
} from "~/constants";
import { createDeferredPromise, sleep } from "~/promises";
import type {
  AbortMsgNotice,
  ConsumerConfig,
  Msg,
  MsgProcessedNotice,
  Notice,
  ProducerConfig,
} from "~/types";
import { Producer } from "~/actors/producer";

const DB_NAME = "test_quick_postgres_consumer";
const LIB_PREFIX = "test_consumer";
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

const CONSUMER_CONFIG: ConsumerConfig = {
  type: "postgres",
  queueName: DEFAULT_QUEUE_NAME,
  connectionParams: {
    ...DEFAULT_POSTGRES_CONNECTION_PARAMS,
    database: DB_NAME,
    libPrefix: LIB_PREFIX,
  },
  processorFn: jest.fn(),
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

    await pool.query(`SELECT pg_notify($1, $2)`, [
      TO_CONSUMERS_CHANNEL,
      JSON.stringify(notice),
    ]);

    await deferred.promise;

    expect(receivedNotices).toHaveLength(1);
    expect(receivedNotices[0]).toEqual(notice);

    await consumer.shutdown();
  });

  it("on() should not receive notices of non-target type", async () => {
    const consumer = new Consumer(CONSUMER_CONFIG);
    await consumer.init();

    const receivedNotices: Notice[] = [];
    const deferred = createDeferredPromise<void>();
    let timedOut = false;

    consumer.on("abort-msg", (notice) => {
      receivedNotices.push(notice);
    });

    const notice = {
      type: "other-type",
      payload: { msgId: "test_msg" },
    };

    await pool.query(`SELECT pg_notify($1, $2)`, [
      TO_CONSUMERS_CHANNEL,
      JSON.stringify(notice),
    ]);

    setTimeout(() => {
      timedOut = true;
      deferred.resolve();
    }, 100);

    await deferred.promise;

    expect(timedOut).toBe(true);
    expect(receivedNotices).toHaveLength(0);

    await consumer.shutdown();
  });

  it("notify() should publish notice to producers", async () => {
    const consumer = new Consumer(CONSUMER_CONFIG);
    await consumer.init();

    const receivedNotices: Notice[] = [];
    const deferred = createDeferredPromise<void>();

    const client = await pool.connect();
    await client.query(`LISTEN "${TO_PRODUCERS_CHANNEL}";`);

    client.on("notification", (msg) => {
      if (msg.payload) {
        receivedNotices.push(JSON.parse(msg.payload));
        deferred.resolve();
      }
    });

    const notice: MsgProcessedNotice = {
      type: "msg-processed",
      payload: { msgId: "test_msg" },
    };

    await consumer.notify(notice);
    await deferred.promise;

    expect(receivedNotices).toHaveLength(1);
    expect(receivedNotices[0]).toEqual(notice);

    client.release();
    await consumer.shutdown();
  });
});

describe("processing", () => {
  it("should poll queue and process messages in FIFO order", async () => {
    const processedMsgs: string[] = [];
    const firstProcessed = createDeferredPromise<void>();
    const secondProcessed = createDeferredPromise<void>();

    const processorFn = async (msg: Msg) => {
      processedMsgs.push(msg.content);
      if (msg.content === "first") firstProcessed.resolve();
      if (msg.content === "second") secondProcessed.resolve();
    };

    const consumer = new Consumer({
      ...CONSUMER_CONFIG,
      processorFn,
    });

    await consumer.init();

    await pool.query(
      `
      INSERT INTO messages (id, queue_name, content, priority, enqueued_at)
      VALUES ($1, $2, $3, $4, NOW());
    `,
      ["msg_1", DEFAULT_QUEUE_NAME, "first", 5]
    );

    await firstProcessed.promise;

    expect(processedMsgs).toHaveLength(1);
    expect(processedMsgs[0]).toBe("first");

    // Insert second message
    await pool.query(
      `
      INSERT INTO messages (id, queue_name, content, priority, enqueued_at)
      VALUES ($1, $2, $3, $4, NOW());
    `,
      ["msg_2", DEFAULT_QUEUE_NAME, "second", 5]
    );

    await secondProcessed.promise;

    expect(processedMsgs).toHaveLength(2);
    expect(processedMsgs).toEqual(["first", "second"]);

    await consumer.shutdown();
  });

  it("should process multiple messages concurrently", async () => {
    const processingStarts: number[] = [];
    const completionOrder: string[] = [];
    const allProcessed = createDeferredPromise<void>();

    const processorFn = async (msg: Msg) => {
      processingStarts.push(Date.now());
      await sleep(msg.content === "second" ? 100 : 50);
      completionOrder.push(msg.content);
      if (completionOrder.length === 3) allProcessed.resolve();
    };

    const consumer = new Consumer({
      ...CONSUMER_CONFIG,
      processorFn,
      concurrency: 3,
    });

    await consumer.init();

    const messages = ["first", "second", "third"];
    for (let i = 0; i < messages.length; i++) {
      await pool.query(
        `
        INSERT INTO messages (id, queue_name, content, priority, enqueued_at)
        VALUES ($1, $2, $3, $4, NOW() + interval '${i} milliseconds');
      `,
        [`msg_${i}`, DEFAULT_QUEUE_NAME, messages[i], 5]
      );
    }

    await allProcessed.promise;

    // All started processing within 100ms of each other
    const startTimeSpread =
      Math.max(...processingStarts) - Math.min(...processingStarts);
    expect(startTimeSpread).toBeLessThan(100);

    // Completed out of enqueue order - "second" completes last due to longer delay
    expect(completionOrder).toEqual(["first", "third", "second"]);

    await consumer.shutdown();
  });
});
