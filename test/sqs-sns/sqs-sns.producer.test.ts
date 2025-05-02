import { Producer } from "~/actors/producer";
import { DEFAULT_AWS_CONNECTION_PARAMS } from "~/constants";
import type { MsgProcessedNotice, Notice, ProducerConfig } from "~/types";
import { createDeferredPromise, sleep } from "~/promises";
import { HttpClient } from "~/http";
import { SqsStore } from "~/stores/sqs/sqs.store";

const QUEUE_NAME = "test-sqs-sns-producer";
const LIB_PREFIX = "test_producer_sqs_sns";
const SERVER_PORT = 3002;

const PRODUCER_CONFIG: ProducerConfig = {
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
      serverUrl: "http://host.docker.internal:3002",
      serverPort: SERVER_PORT,
      libPrefix: LIB_PREFIX,
    },
  },
};

const ENQUEUE_MSG_PARAMS = {
  queueName: QUEUE_NAME,
};

let producer: Producer;
let httpClient: HttpClient;
let sqsStore: SqsStore;

beforeAll(async () => {
  sqsStore = new SqsStore();
  await sqsStore.connect({
    region: "eu-west-3",
    endpoint: "http://localhost:4566",
    accessKeyId: "test",
    secretAccessKey: "test",
  });
  httpClient = new HttpClient();
});

beforeEach(async () => {
  await sqsStore.createQueue(QUEUE_NAME);
  producer = new Producer(PRODUCER_CONFIG);
  await producer.init();
});

afterEach(async () => {
  await producer.shutdown();
  await sqsStore.destroyQueue(QUEUE_NAME);
  await sleep(100); // allow time for resources to clean up @TODO
});

afterAll(async () => {
  await sqsStore.disconnect();
});

describe("enqueue", () => {
  it("should enqueue message and return message ID", async () => {
    const { msgId } = await producer.enqueue("message", ENQUEUE_MSG_PARAMS);
    expect(msgId).toMatch(/^msg_[0-9a-fA-F]{32}$/);

    // Check queue length to verify message was enqueued
    const queueLength = await producer.getQueueLength();
    expect(queueLength).toBeGreaterThan(0);
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

    // @ts-expect-error For testing
    await producer.pubsub.publish("to_producers", JSON.stringify(notice));

    await promise;
    expect(hasResolved).toBe(true);
  });
});

describe("remove", () => {
  it("should remove enqueued message", async () => {
    const { msgId } = await producer.enqueue("message", ENQUEUE_MSG_PARAMS);

    const lengthBefore = await producer.getQueueLength();
    expect(lengthBefore).toBeGreaterThan(0);

    const wasRemoved = await producer.remove(msgId);
    expect(wasRemoved).toBe(true);
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

    // @ts-expect-error For testing
    await producer.pubsub.publish("to_producers", JSON.stringify(notice));

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

    // Publish the notice to SNS
    await httpClient.request({
      method: "POST",
      url: "http://localhost:4566/",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Host: "localhost:4566",
      },
      data: new URLSearchParams({
        Action: "Publish",
        Version: "2010-03-31",
        TopicArn: `arn:aws:sns:eu-west-3:000000000000:${LIB_PREFIX}_to_producers`,
        Message: JSON.stringify(notice),
      }).toString(),
    });

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

    // @ts-expect-error Repurpose the producer's pubsub to listen for messages to consumers
    const producerPubsub = producer.pubsub;

    await producerPubsub.subscribe("to_consumers", (message) => {
      receivedNotices.push(JSON.parse(message));
      deferred.resolve();
    });

    const notice: Notice = {
      type: "abort-msg",
      payload: { msgId: "test_msg" },
    };

    await producer.notify(notice);

    await deferred.promise;

    expect(receivedNotices).toHaveLength(1);
    expect(receivedNotices[0]).toEqual(notice);
  });
});

describe("getQueueLength", () => {
  it("should return number of enqueued messages", async () => {
    await expect(producer.getQueueLength()).resolves.toBe(0);

    await producer.enqueue("first", ENQUEUE_MSG_PARAMS);
    await expect(producer.getQueueLength()).resolves.toBeGreaterThan(0);

    await producer.enqueue("second", ENQUEUE_MSG_PARAMS);
    const length = await producer.getQueueLength();
    expect(length).toBeGreaterThanOrEqual(2);
  });
});
