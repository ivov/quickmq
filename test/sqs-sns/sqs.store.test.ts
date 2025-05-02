import { SqsStore } from "~/stores/sqs/sqs.store";
import { MAX_MSG_SIZE } from "~/constants";
import { OversizeMsgError } from "~/errors/oversize-msg.error";
import { sleep } from "~/promises";
import assert from "node:assert";
import { UnsupportedPriorityError } from "~/errors/unsupported-priority.error";

// docker run -d --name quick-localstack -p 4566:4566 --add-host=host.docker.internal:host-gateway -e SERVICES=sqs,sns localstack/localstack

const QUEUE_NAME = "test-sqs-queue";

const ENQUEUE_MSG_PARAMS = {
  queueName: QUEUE_NAME,
};

let store: SqsStore;

beforeAll(async () => {
  store = new SqsStore();
  await store.connect({
    region: "eu-west-3",
    endpoint: "http://localhost:4566",
    accessKeyId: "test",
    secretAccessKey: "test",
  });
});

afterAll(async () => {
  await store.disconnect();
});

afterEach(async () => {
  await store.destroyQueue(QUEUE_NAME);
});

describe("createQueue", () => {
  it("should create queue with default config", async () => {
    const wasCreated = await store.createQueue(QUEUE_NAME);
    expect(wasCreated).toBe(true);
  });

  it("should create queue with custom name", async () => {
    const customQueueName = "custom-queue";
    const wasCreated = await store.createQueue(customQueueName);
    expect(wasCreated).toBe(true);

    await store.destroyQueue(customQueueName);
  });

  it("should return `false` when trying to create an already existing queue", async () => {
    await store.createQueue(QUEUE_NAME);
    const wasCreated = await store.createQueue(QUEUE_NAME);
    expect(wasCreated).toBe(false);
  });
});

describe("destroyQueue", () => {
  it("should return `true` when destroying queue", async () => {
    await store.createQueue(QUEUE_NAME);
    const wasDestroyed = await store.destroyQueue(QUEUE_NAME);
    expect(wasDestroyed).toBe(true);
  });

  it("should return `false` when trying to destroy non-existing queue", async () => {
    const wasDestroyed = await store.destroyQueue("non-existing-queue");
    expect(wasDestroyed).toBe(false);
  });
});

describe("sendMsg", () => {
  beforeEach(async () => {
    await store.createQueue(QUEUE_NAME);
  });

  it("should send message to queue", async () => {
    const msgId = await store.sendMsg("message", {
      ...ENQUEUE_MSG_PARAMS,
    });

    expect(msgId).toMatch(/^msg_[0-9a-fA-F]{32}$/);
  });

  it("should send message with custom ID", async () => {
    const customId = "custom_123";
    const msgId = await store.sendMsg("message", {
      ...ENQUEUE_MSG_PARAMS,
      customId,
    });

    expect(msgId).toBe(customId);
  });

  it("should throw `OversizeMsgError` when message exceeds max size", async () => {
    const oversizeMsg = Buffer.alloc(MAX_MSG_SIZE + 1, "a").toString();
    await expect(
      store.sendMsg(oversizeMsg, ENQUEUE_MSG_PARAMS)
    ).rejects.toThrow(OversizeMsgError);
  });
});

describe("nextMsg", () => {
  beforeEach(async () => {
    await store.createQueue(QUEUE_NAME);
  });

  it("should return `null` when queue is empty", async () => {
    const msg = await store.nextMsg({ queueName: QUEUE_NAME });
    expect(msg).toBeNull();
  });

  it("should return messages in FIFO order", async () => {
    await store.sendMsg("first", ENQUEUE_MSG_PARAMS);
    await sleep(10);
    await store.sendMsg("second", ENQUEUE_MSG_PARAMS);

    const firstMsg = await store.nextMsg({ queueName: QUEUE_NAME });
    assert(firstMsg);
    expect(firstMsg.content).toBe("first");

    const secondMsg = await store.nextMsg({ queueName: QUEUE_NAME });
    expect(secondMsg?.content).toBe("second");
  });

  it("should make message invisible on retrieval", async () => {
    await store.sendMsg("message", ENQUEUE_MSG_PARAMS);

    // retrieve message
    const msg = await store.nextMsg({ queueName: QUEUE_NAME });
    expect(msg?.content).toBe("message");
    expect(msg?.visibilityTs).toBeGreaterThan(Date.now());
    expect(msg?.receiveCount).toBe(1);

    // message should not be returned while invisible
    const nextMsg = await store.nextMsg({ queueName: QUEUE_NAME });
    expect(nextMsg).toBeNull();
  });
});

describe("removeMsg", () => {
  it("if message is absent, should not remove", async () => {
    await store.createQueue(QUEUE_NAME);

    const wasRemoved = await store.removeMsg("nonexistent_msg", QUEUE_NAME);
    expect(wasRemoved).toBe(false);
  });

  it("if message is present, should remove message after processing", async () => {
    await store.createQueue(QUEUE_NAME);
    const msgId = await store.sendMsg("message", ENQUEUE_MSG_PARAMS);

    const msg = await store.nextMsg({ queueName: QUEUE_NAME });
    assert(msg);

    const wasRemoved = await store.removeMsg(msgId, QUEUE_NAME);
    expect(wasRemoved).toBe(true);

    const queueLength = await store.getQueueLength(QUEUE_NAME);
    expect(queueLength).toBe(0);
  });
});

describe("getQueueLength", () => {
  beforeEach(async () => {
    await store.createQueue(QUEUE_NAME);
  });

  it("should return `0` when queue is empty", async () => {
    const length = await store.getQueueLength(QUEUE_NAME);
    expect(length).toBe(0);
  });

  it("should return correct count of messages in queue", async () => {
    await store.sendMsg("first", ENQUEUE_MSG_PARAMS);
    await store.sendMsg("second", ENQUEUE_MSG_PARAMS);
    await store.sendMsg("third", ENQUEUE_MSG_PARAMS);

    const length = await store.getQueueLength(QUEUE_NAME);
    expect(length).toBe(3);
  });

  it("should decrease count when messages are consumed", async () => {
    await store.sendMsg("first", ENQUEUE_MSG_PARAMS);
    await store.sendMsg("second", ENQUEUE_MSG_PARAMS);

    const lengthPre = await store.getQueueLength(QUEUE_NAME);
    expect(lengthPre).toBe(2);

    const msg = await store.nextMsg({ queueName: QUEUE_NAME });
    assert(msg);
    await store.removeMsg(msg.id, QUEUE_NAME);

    const lengthPost = await store.getQueueLength(QUEUE_NAME);
    expect(lengthPost).toBe(1);
  });
});

describe("priority", () => {
  beforeEach(async () => {
    await store.createQueue(QUEUE_NAME);
  });

  it("should disallow priority", async () => {
    for (const priority of [1, 5, 9]) {
      const promise = store.sendMsg("message", {
        ...ENQUEUE_MSG_PARAMS,
        priority,
      });
      await expect(promise).rejects.toThrow(UnsupportedPriorityError);
    }
  });
});
