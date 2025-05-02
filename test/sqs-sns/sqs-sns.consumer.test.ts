import { Consumer } from "~/actors/consumer";
import { DEFAULT_AWS_CONNECTION_PARAMS } from "~/constants";
import { createDeferredPromise, sleep } from "~/promises";
import type {
  AbortMsgNotice,
  ConsumerConfig,
  Msg,
  MsgProcessedNotice,
  Notice,
} from "~/types";

const QUEUE_NAME = "test-sqs-sns-consumer";
const LIB_PREFIX = "test_consumer_sqs_sns";
const SERVER_PORT = 3003;

const CONSUMER_CONFIG: ConsumerConfig = {
  type: "sqs-sns",
  queueName: QUEUE_NAME,
  connectionParams: {
    sqs: {
      ...DEFAULT_AWS_CONNECTION_PARAMS,
      endpoint: "http://localhost:4566",
      accessKeyId: "test",
      secretAccessKey: "test",
    },
    sns: {
      ...DEFAULT_AWS_CONNECTION_PARAMS,
      endpoint: "http://localhost:4566",
      accessKeyId: "test",
      secretAccessKey: "test",
      serverUrl: "http://host.docker.internal",
      serverPort: SERVER_PORT,
      libPrefix: LIB_PREFIX,
    },
  },
  processorFn: jest.fn(),
};

let consumer: Consumer;

beforeEach(async () => {
  consumer = new Consumer(CONSUMER_CONFIG);
  await consumer.init();
});

afterEach(async () => {
  await consumer.shutdown();
  await sleep(100); // allow time for resources to clean up
});

describe("pubsub", () => {
  it("on() should receive notices of target type", async () => {
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

    // @ts-expect-error Repurpose the consumer's pubsub to publish messages to consumers
    const consumerPubsub = consumer.pubsub;
    await consumerPubsub.publish("to_consumers", JSON.stringify(notice));

    await deferred.promise;

    expect(receivedNotices).toHaveLength(1);
    expect(receivedNotices[0]).toEqual(notice);
  });

  it("on() should not receive notices of non-target type", async () => {
    const receivedNotices: Notice[] = [];
    const timeoutDeferred = createDeferredPromise<void>();
    let timedOut = false;

    consumer.on("abort-msg", (notice) => {
      receivedNotices.push(notice);
    });

    const notice = {
      type: "other-type",
      payload: { msgId: "test_msg" },
    };

    // @ts-expect-error Repurpose the consumer's pubsub to publish messages to consumers
    const consumerPubsub = consumer.pubsub;
    await consumerPubsub.publish("to_consumers", JSON.stringify(notice));

    setTimeout(() => {
      timedOut = true;
      timeoutDeferred.resolve();
    }, 100);

    await timeoutDeferred.promise;

    expect(timedOut).toBe(true);
    expect(receivedNotices).toHaveLength(0);
  });

  it("notify() should publish notice to producers", async () => {
    const receivedNotices: Notice[] = [];
    const deferred = createDeferredPromise<void>();

    // @ts-expect-error Repurpose the consumer's pubsub to listen for messages to producers
    const consumerPubsub = consumer.pubsub;

    await consumerPubsub.subscribe("to_producers", (message) => {
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
      connectionParams: {
        ...CONSUMER_CONFIG.connectionParams,
        sns: {
          ...CONSUMER_CONFIG.connectionParams.sns,
          serverPort: 0,
        },
      },
      processorFn,
    });

    await consumer.init();

    // @ts-expect-error Repurpose the underlying store for testing
    const sqsStore = consumer.store;
    await sqsStore.createQueue(QUEUE_NAME);
    await sqsStore.sendMsg("first", { queueName: QUEUE_NAME });
    await firstProcessed.promise;

    expect(processedMsgs).toHaveLength(1);
    expect(processedMsgs[0]).toBe("first");

    await sqsStore.sendMsg("second", { queueName: QUEUE_NAME });
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
      await (msg.content === "second" ? sleep(100) : sleep(50));
      completionOrder.push(msg.content);
      if (completionOrder.length === 3) allProcessed.resolve();
    };

    const consumer = new Consumer({
      ...CONSUMER_CONFIG,
      connectionParams: {
        ...CONSUMER_CONFIG.connectionParams,
        sns: {
          ...CONSUMER_CONFIG.connectionParams.sns,
          serverPort: SERVER_PORT + 1,
        },
      },
      processorFn,
      concurrency: 3,
    });

    await consumer.init();

    // @ts-expect-error Repurpose the underlying store for testing
    const sqsStore = consumer.store;
    await sqsStore.createQueue(QUEUE_NAME);

    // Send all messages at once
    await sqsStore.sendMsg("first", { queueName: QUEUE_NAME });
    await sqsStore.sendMsg("second", { queueName: QUEUE_NAME });
    await sqsStore.sendMsg("third", { queueName: QUEUE_NAME });

    await allProcessed.promise;

    const startTimeSpread =
      Math.max(...processingStarts) - Math.min(...processingStarts);
    expect(startTimeSpread).toBeLessThan(100);

    expect(completionOrder).toEqual(["first", "third", "second"]);

    await consumer.shutdown();
  });
});
