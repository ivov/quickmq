import { Pool } from "pg";
import { PostgresStore } from "~/stores/postgres/postgres.store";
import {
  DEFAULT_POSTGRES_CONNECTION_PARAMS,
  DEFAULT_QUEUE_NAME,
  DEFAULT_QUEUE_PROPS,
  MAX_MSG_SIZE,
} from "~/constants";
import { OversizeMsgError } from "~/errors/oversize-msg.error";
import { InvalidPriorityError } from "~/errors/invalid-priority.error";
import { sleep } from "~/promises";
import assert from "assert";

// docker run -d --name quick-postgres -e POSTGRES_PASSWORD=password -p 5432:5432 postgres

const DB_NAME = "test_quick_postgres_store";

let bootstrap: Pool;
let pool: Pool;
let store: PostgresStore;

beforeAll(async () => {
  bootstrap = new Pool(DEFAULT_POSTGRES_CONNECTION_PARAMS);

  await bootstrap.query(`DROP DATABASE IF EXISTS ${DB_NAME};`);
  await bootstrap.query(`CREATE DATABASE ${DB_NAME};`);

  pool = new Pool({
    ...DEFAULT_POSTGRES_CONNECTION_PARAMS,
    database: DB_NAME,
  });

  store = new PostgresStore();

  await store.connect(pool);
});

afterAll(async () => {
  await store.disconnect();
  await pool.end();
  await bootstrap.end();
});

afterEach(async () => {
  await pool.query("TRUNCATE queues CASCADE;");
});

describe("createQueue", () => {
  it("should create queue with default config", async () => {
    const wasCreated = await store.createQueue(DEFAULT_QUEUE_NAME);
    expect(wasCreated).toBe(true);

    const result = await pool.query(
      "SELECT max_msg_size, created_at FROM queues WHERE name = $1",
      [DEFAULT_QUEUE_NAME]
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].max_msg_size).toBe(MAX_MSG_SIZE);
    expect(result.rows[0].created_at).toBeInstanceOf(Date);
  });

  it("should create queue with custom name", async () => {
    const customQueueName = "custom-queue";
    const wasCreated = await store.createQueue(customQueueName);
    expect(wasCreated).toBe(true);

    const result = await pool.query("SELECT name FROM queues WHERE name = $1", [
      customQueueName,
    ]);
    expect(result.rows).toHaveLength(1);
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

    const result = await pool.query("SELECT name FROM queues WHERE name = $1", [
      DEFAULT_QUEUE_NAME,
    ]);
    expect(result.rows).toHaveLength(0);
  });

  it("should return `false` when trying to destroy non-existing queue", async () => {
    const wasDestroyed = await store.destroyQueue("non-existing-queue");
    expect(wasDestroyed).toBe(false);
  });
});

describe("sendMsg", () => {
  beforeEach(async () => {
    await store.createQueue(DEFAULT_QUEUE_NAME);
  });

  it("should send message to queue", async () => {
    const msgId = await store.sendMsg("message", DEFAULT_QUEUE_PROPS);
    expect(msgId).toMatch(/^msg_[0-9a-fA-F]{32}$/);

    const result = await pool.query(
      "SELECT content, enqueued_at FROM messages WHERE id = $1;",
      [msgId]
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].content).toBe("message");
    expect(result.rows[0].enqueued_at).toBeInstanceOf(Date);
  });

  it("should send message with custom ID", async () => {
    const customId = "custom_123";
    const msgId = await store.sendMsg("message", {
      ...DEFAULT_QUEUE_PROPS,
      customId,
    });

    expect(msgId).toBe(customId);

    const result = await pool.query("SELECT id FROM messages WHERE id = $1", [
      customId,
    ]);
    expect(result.rows).toHaveLength(1);
  });

  it("should throw `OversizeMsgError` when message exceeds max size", async () => {
    const oversizeMsg = Buffer.alloc(MAX_MSG_SIZE + 1, "a").toString();
    await expect(
      store.sendMsg(oversizeMsg, DEFAULT_QUEUE_PROPS)
    ).rejects.toThrow(OversizeMsgError);
  });
});

describe("nextMsg", () => {
  beforeEach(async () => {
    await store.createQueue(DEFAULT_QUEUE_NAME);
  });

  it("should return `null` when queue is empty", async () => {
    const msg = await store.nextMsg(DEFAULT_QUEUE_PROPS);
    expect(msg).toBeNull();
  });

  it("should return messages in FIFO order", async () => {
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
    const resultPre = await pool.query(
      "SELECT id, visibility_ts FROM messages WHERE id = $1",
      [msgId]
    );
    expect(resultPre.rows).toHaveLength(1);
    expect(resultPre.rows[0].visibility_ts).toBeNull();

    // retrieve message
    const msg = await store.nextMsg(DEFAULT_QUEUE_PROPS);
    expect(msg?.content).toBe("message");
    expect(msg?.visibilityTs).toBeGreaterThan(Date.now());
    expect(msg?.receiveCount).toBe(1);

    // message should still exist
    const resultPost = await pool.query(
      "SELECT id, visibility_ts, receive_count FROM messages WHERE id = $1",
      [msgId]
    );
    expect(resultPost.rows).toHaveLength(1);
    expect(resultPost.rows[0].visibility_ts).not.toBeNull();

    // but message should have future visibility timestamp
    expect(parseInt(resultPost.rows[0].visibility_ts)).toBeGreaterThan(
      Date.now()
    );

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

      const msgId = await store.sendMsg("message", DEFAULT_QUEUE_PROPS);
      const wasRemoved = await store.removeMsg(
        msgId,
        DEFAULT_QUEUE_NAME
        /* no ownership check */
      );

      expect(wasRemoved).toBe(true);

      const result = await pool.query(
        "SELECT id FROM messages WHERE id = $1;",
        [msgId]
      );
      expect(result.rows).toHaveLength(0);
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

      const result = await pool.query(
        "SELECT id FROM messages WHERE id = $1;",
        [msgId]
      );
      expect(result.rows).toHaveLength(0);
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

      const result = await pool.query(
        "SELECT id FROM messages WHERE id = $1;",
        [msgId]
      );
      expect(result.rows).toHaveLength(1);
    });
  });
});

describe("getQueueLength", () => {
  beforeEach(async () => {
    await store.createQueue(DEFAULT_QUEUE_NAME);
  });

  it("should return `0` when queue is empty", async () => {
    const length = await store.getQueueLength(DEFAULT_QUEUE_NAME);
    expect(length).toBe(0);
  });

  it("should return correct count of messages in queue", async () => {
    await store.sendMsg("first", DEFAULT_QUEUE_PROPS);
    await store.sendMsg("second", DEFAULT_QUEUE_PROPS);
    await store.sendMsg("third", DEFAULT_QUEUE_PROPS);

    const length = await store.getQueueLength(DEFAULT_QUEUE_NAME);
    expect(length).toBe(3);
  });

  it("should decrease count when messages are consumed", async () => {
    await store.sendMsg("first", DEFAULT_QUEUE_PROPS);
    await store.sendMsg("second", DEFAULT_QUEUE_PROPS);

    const lengthPre = await store.getQueueLength(DEFAULT_QUEUE_NAME);
    expect(lengthPre).toBe(2);

    await store.nextMsg(DEFAULT_QUEUE_PROPS);
    const msg = await store.nextMsg(DEFAULT_QUEUE_PROPS);
    assert(msg);
    await store.removeMsg(msg.id, DEFAULT_QUEUE_NAME, msg.visibilityTs);

    const lengthPost = await store.getQueueLength(DEFAULT_QUEUE_NAME);
    expect(lengthPost).toBe(1);
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
    const second = await store.nextMsg(DEFAULT_QUEUE_PROPS);
    const third = await store.nextMsg(DEFAULT_QUEUE_PROPS);

    expect(first?.content).toBe("highest");
    expect(second?.content).toBe("medium");
    expect(third?.content).toBe("lowest");
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
