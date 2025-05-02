import assert from "node:assert";
import Redis from "ioredis";
import { RedisStore } from "~/stores/redis/redis.store";
import {
  DEFAULT_QUEUE_NAME,
  DEFAULT_QUEUE_PROPS,
  MAX_MSG_SIZE,
} from "~/constants";
import { OversizeMsgError } from "~/errors/oversize-msg.error";
import { InvalidPriorityError } from "~/errors/invalid-priority.error";
import { sleep } from "~/promises";

// docker run -d --name quick-redis -p 6379:6379 redis

const LIB_PREFIX = "test_quick_redis_store";
const ALL_KEYS = `${LIB_PREFIX}:*`;
const HASH_KEY = `${LIB_PREFIX}:${DEFAULT_QUEUE_NAME}`;
const SORTED_SET_KEY = `${HASH_KEY}:sequence`;

let store: RedisStore;
let redis: Redis;

beforeAll(async () => {
  redis = new Redis();
  store = new RedisStore();
  await store.connect({ libPrefix: LIB_PREFIX });
});

afterAll(async () => {
  await store.disconnect();
  await redis.quit();
});

afterEach(async () => {
  const allKeys = await redis.keys(ALL_KEYS);
  if (allKeys.length > 0) await redis.del(allKeys);
});

const toInt = (value: string | null) => {
  assert(value);
  return parseInt(value, 10);
};

describe("createQueue", () => {
  it("should create queue with default config", async () => {
    const wasCreated = await store.createQueue(DEFAULT_QUEUE_NAME);

    expect(wasCreated).toBe(true);

    const [maxSize, createdAt] = await redis.hmget(
      HASH_KEY,
      "max_msg_size",
      "created_at"
    );

    expect(maxSize).toBe(MAX_MSG_SIZE.toString());
    expect(toInt(createdAt)).toBeGreaterThan(0);
  });

  it("should create queue with custom name", async () => {
    const customQueueName = "custom-queue";
    const customQueueHashKey = `${LIB_PREFIX}:${customQueueName}`;
    const wasCreated = await store.createQueue(customQueueName);

    expect(wasCreated).toBe(true);

    const [maxSize, createdAt] = await redis.hmget(
      customQueueHashKey,
      "max_msg_size",
      "created_at"
    );

    expect(maxSize).toBe(MAX_MSG_SIZE.toString());
    expect(toInt(createdAt)).toBeGreaterThan(0);
  });

  it("should return `false` when trying to create an already existing queue", async () => {
    await store.createQueue(DEFAULT_QUEUE_NAME);

    const wasCreated = await store.createQueue(DEFAULT_QUEUE_NAME);

    expect(wasCreated).toBe(false);
  });
});

describe("destroyQueue", () => {
  it("should return `true` when destroying queue", async () => {
    await store.createQueue(DEFAULT_QUEUE_NAME);

    const wasDestroyed = await store.destroyQueue(DEFAULT_QUEUE_NAME);

    expect(wasDestroyed).toBe(true);

    expect(redis.exists(HASH_KEY)).resolves.toBe(0);
    expect(redis.exists(SORTED_SET_KEY)).resolves.toBe(0);
  });

  it("should return `false` when trying to destroy non-existing queue", async () => {
    const wasDestroyed = await store.destroyQueue("non-existing-queue");

    expect(wasDestroyed).toBe(false);
  });
});

describe("sendMsg", () => {
  it("should send message to queue", async () => {
    await store.createQueue(DEFAULT_QUEUE_NAME);

    const msgId = await store.sendMsg("message", DEFAULT_QUEUE_PROPS);
    expect(msgId).toMatch(/^msg_[0-9a-fA-F]{32}$/);

    const msgContent = await redis.hget(HASH_KEY, msgId);
    expect(msgContent).toBe("message");

    const enqueuedAt = await redis.hget(HASH_KEY, `${msgId}:enqueued_at`);

    expect(toInt(enqueuedAt)).toBeGreaterThan(0);

    const members = await redis.zrange(SORTED_SET_KEY, 0, 0);
    expect(members).toHaveLength(1);

    const [member] = members;
    const [priority, timestamp, messageId] = member.split(":");

    expect(priority).toBe("5");
    expect(toInt(timestamp)).toBeGreaterThan(0);
    expect(messageId).toBe(msgId);
    expect(enqueuedAt).toBe(timestamp);
  });

  it("should send message with custom ID", async () => {
    await store.createQueue(DEFAULT_QUEUE_NAME);

    const customId = "custom_123";
    const msgId = await store.sendMsg("message", {
      ...DEFAULT_QUEUE_PROPS,
      customId,
    });

    expect(msgId).toBe(customId);

    const msgExists = await redis.hexists(HASH_KEY, customId);
    expect(msgExists).toBe(1);
  });

  it("should throw `OversizeMsgError` when message exceeds max size", async () => {
    await store.createQueue(DEFAULT_QUEUE_NAME);

    const oversizeMsg = Buffer.alloc(MAX_MSG_SIZE + 1, "a").toString();

    const promise = store.sendMsg(oversizeMsg, DEFAULT_QUEUE_PROPS);

    await expect(promise).rejects.toThrow(OversizeMsgError);
  });
});

describe("nextMsg", () => {
  it("should return `null` when queue is empty", async () => {
    await store.createQueue(DEFAULT_QUEUE_NAME);

    const msg = await store.nextMsg(DEFAULT_QUEUE_PROPS);

    expect(msg).toBeNull();
  });

  it("should return messages in FIFO order", async () => {
    await store.createQueue(DEFAULT_QUEUE_NAME);

    await store.sendMsg("first", DEFAULT_QUEUE_PROPS);
    await sleep(10);
    await store.sendMsg("second", DEFAULT_QUEUE_PROPS);

    const firstMsg = await store.nextMsg(DEFAULT_QUEUE_PROPS);
    expect(firstMsg?.content).toBe("first");

    const secondMsg = await store.nextMsg(DEFAULT_QUEUE_PROPS);
    expect(secondMsg?.content).toBe("second");
  });

  it("should make message invisible on retrieval", async () => {
    await store.createQueue(DEFAULT_QUEUE_NAME);

    const msgId = await store.sendMsg("message", DEFAULT_QUEUE_PROPS);

    // message exists before retrieval
    expect(redis.hexists(HASH_KEY, msgId)).resolves.toBe(1);
    expect(redis.hexists(HASH_KEY, `${msgId}:enqueued_at`)).resolves.toBe(1);
    const membersPre = await redis.zrange(SORTED_SET_KEY, 0, -1);
    expect(membersPre).toHaveLength(1);
    const originalMember = membersPre[0];
    expect(originalMember).toContain(`:${msgId}`);

    // retrieve message
    const msg = await store.nextMsg(DEFAULT_QUEUE_PROPS);
    expect(msg?.content).toBe("message");
    expect(msg?.visibilityTs).toBeGreaterThan(Date.now());
    expect(msg?.receiveCount).toBe(1);

    // message should still exist
    expect(redis.hexists(HASH_KEY, msgId)).resolves.toBe(1);
    expect(redis.hexists(HASH_KEY, `${msgId}:enqueued_at`)).resolves.toBe(1);
    expect(redis.hexists(HASH_KEY, `${msgId}:receive_count`)).resolves.toBe(1);
    const membersPost = await redis.zrange(SORTED_SET_KEY, 0, -1);
    expect(membersPost).toHaveLength(1);

    // but message should have future visibility timestamp
    const newMember = membersPost[0];
    expect(newMember).toContain(`:${msgId}`);
    expect(newMember).not.toBe(originalMember);
    const [_priority, visibilityTs, _id] = newMember.split(":");
    expect(parseInt(visibilityTs)).toBeGreaterThan(Date.now());

    // message should not be returned while invisible
    const nextMsg = await store.nextMsg(DEFAULT_QUEUE_PROPS);
    expect(nextMsg).toBeNull();
  });
});

describe("removeMsg", () => {
  it("if message is absent, should not remove", async () => {
    await store.createQueue(DEFAULT_QUEUE_NAME);

    const wasRemoved = await store.removeMsg(
      "nonexistent_msg",
      DEFAULT_QUEUE_NAME
    );

    expect(wasRemoved).toBe(false);
  });

  describe("if message is present", () => {
    it("with skipped ownership check, should remove", async () => {
      await store.createQueue(DEFAULT_QUEUE_NAME);
      const msgId = await store.sendMsg(
        "message",
        DEFAULT_QUEUE_PROPS
        /* no ownership check */
      );

      const wasRemoved = await store.removeMsg(msgId, DEFAULT_QUEUE_NAME);

      expect(wasRemoved).toBe(true);
      expect(redis.hexists(HASH_KEY, msgId)).resolves.toBe(0);
      expect(redis.hexists(HASH_KEY, `${msgId}:enqueued_at`)).resolves.toBe(0);
      expect(redis.zscore(SORTED_SET_KEY, msgId)).resolves.toBeNull();
    });

    it("with passing ownership check, should remove", async () => {
      await store.createQueue(DEFAULT_QUEUE_NAME);
      const msgId = await store.sendMsg("message", DEFAULT_QUEUE_PROPS);

      const msg = await store.nextMsg(DEFAULT_QUEUE_PROPS);
      assert(msg);

      const wasRemoved = await store.removeMsg(
        msgId,
        DEFAULT_QUEUE_NAME,
        msg.visibilityTs // correct timestamp, will pass ownership check
      );

      expect(wasRemoved).toBe(true);
      expect(redis.hexists(HASH_KEY, msgId)).resolves.toBe(0);
      expect(redis.hexists(HASH_KEY, `${msgId}:enqueued_at`)).resolves.toBe(0);
      expect(redis.hexists(HASH_KEY, `${msgId}:receive_count`)).resolves.toBe(
        0
      );
      expect(redis.zcard(SORTED_SET_KEY)).resolves.toBe(0);
    });

    it("with failing ownership check, should not remove", async () => {
      await store.createQueue(DEFAULT_QUEUE_NAME);
      const msgId = await store.sendMsg("message", DEFAULT_QUEUE_PROPS);

      const msg = await store.nextMsg(DEFAULT_QUEUE_PROPS);
      assert(msg);

      const wasRemoved = await store.removeMsg(
        msgId,
        DEFAULT_QUEUE_NAME,
        msg.visibilityTs + 1000 // timestamp has changed, will fail ownership check
      );

      expect(wasRemoved).toBe(false);
      expect(redis.hexists(HASH_KEY, msgId)).resolves.toBe(1);
      expect(redis.hexists(HASH_KEY, `${msgId}:enqueued_at`)).resolves.toBe(1);
      expect(redis.hexists(HASH_KEY, `${msgId}:receive_count`)).resolves.toBe(
        1
      );
      expect(redis.zcard(SORTED_SET_KEY)).resolves.toBe(1);
    });
  });
});

describe("getQueueLength", () => {
  it("should return `0` when queue is empty", async () => {
    await store.createQueue(DEFAULT_QUEUE_NAME);
    const length = await store.getQueueLength(DEFAULT_QUEUE_NAME);
    expect(length).toBe(0);
  });

  it("should return correct count of messages in queue", async () => {
    await store.createQueue(DEFAULT_QUEUE_NAME);

    await store.sendMsg("first", DEFAULT_QUEUE_PROPS);
    await store.sendMsg("second", DEFAULT_QUEUE_PROPS);
    await store.sendMsg("third", DEFAULT_QUEUE_PROPS);

    const length = await store.getQueueLength(DEFAULT_QUEUE_NAME);
    expect(length).toBe(3);
  });

  it("should decrease count when messages are consumed", async () => {
    await store.createQueue(DEFAULT_QUEUE_NAME);

    await store.sendMsg("first", DEFAULT_QUEUE_PROPS);
    await store.sendMsg("second", DEFAULT_QUEUE_PROPS);

    let length = await store.getQueueLength(DEFAULT_QUEUE_NAME);
    expect(length).toBe(2);

    await store.nextMsg(DEFAULT_QUEUE_PROPS);
    const msg = await store.nextMsg(DEFAULT_QUEUE_PROPS);
    assert(msg);
    await store.removeMsg(msg.id, DEFAULT_QUEUE_NAME, msg.visibilityTs);

    length = await store.getQueueLength(DEFAULT_QUEUE_NAME);
    expect(length).toBe(1);
  });

  it("should return `0` after all messages are consumed", async () => {
    await store.createQueue(DEFAULT_QUEUE_NAME);

    const msg1Id = await store.sendMsg("message 1", DEFAULT_QUEUE_PROPS);
    const msg2Id = await store.sendMsg("message 2", DEFAULT_QUEUE_PROPS);

    await store.removeMsg(msg1Id, DEFAULT_QUEUE_NAME);
    await store.removeMsg(msg2Id, DEFAULT_QUEUE_NAME);

    const length = await store.getQueueLength(DEFAULT_QUEUE_NAME);
    expect(length).toBe(0);
  });
});

describe("priority", () => {
  beforeEach(async () => {
    await store.createQueue(DEFAULT_QUEUE_NAME);
  });

  it("should honor priority", async () => {
    await store.sendMsg("lowest", { ...DEFAULT_QUEUE_PROPS, priority: 9 });
    await store.sendMsg("highest", { ...DEFAULT_QUEUE_PROPS, priority: 1 });
    await store.sendMsg("medium", DEFAULT_QUEUE_PROPS); // 5 by default

    const first = await store.nextMsg(DEFAULT_QUEUE_PROPS);
    assert(first);
    expect(first.content).toBe("highest");
    await store.removeMsg(first.id, DEFAULT_QUEUE_NAME, first.visibilityTs);

    const second = await store.nextMsg(DEFAULT_QUEUE_PROPS);
    assert(second);
    expect(second.content).toBe("medium");
    await store.removeMsg(second.id, DEFAULT_QUEUE_NAME, second.visibilityTs);

    const third = await store.nextMsg(DEFAULT_QUEUE_PROPS);
    assert(third);
    expect(third.content).toBe("lowest");
    await store.removeMsg(third.id, DEFAULT_QUEUE_NAME, third.visibilityTs);
  });

  it("if identical priority, should keep FIFO order", async () => {
    const priority = 3;

    await store.sendMsg("first", { ...DEFAULT_QUEUE_PROPS, priority });
    await sleep(10);
    await store.sendMsg("second", { ...DEFAULT_QUEUE_PROPS, priority });
    await sleep(10);
    await store.sendMsg("third", { ...DEFAULT_QUEUE_PROPS, priority });

    const first = await store.nextMsg(DEFAULT_QUEUE_PROPS);
    const second = await store.nextMsg(DEFAULT_QUEUE_PROPS);
    const third = await store.nextMsg(DEFAULT_QUEUE_PROPS);

    expect(first?.content).toBe("first");
    expect(second?.content).toBe("second");
    expect(third?.content).toBe("third");
  });

  it("should favor priority over timestamp", async () => {
    await store.sendMsg("older low priority", {
      ...DEFAULT_QUEUE_PROPS,
      priority: 9,
    });
    await sleep(10);
    await store.sendMsg("newer high priority", {
      ...DEFAULT_QUEUE_PROPS,
      priority: 1,
    });

    const first = await store.nextMsg(DEFAULT_QUEUE_PROPS);
    expect(first?.content).toBe("newer high priority");

    const second = await store.nextMsg(DEFAULT_QUEUE_PROPS);
    expect(second?.content).toBe("older low priority");
  });

  it("should reject invalid priority", async () => {
    for (const priority of [0, 10]) {
      const promise = store.sendMsg("message", {
        ...DEFAULT_QUEUE_PROPS,
        priority,
      });
      await expect(promise).rejects.toThrow(InvalidPriorityError);
    }
  });
});
