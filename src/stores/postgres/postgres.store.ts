import path from "node:path";
import assert from "node:assert";
import { Pool } from "pg";
import { InvalidPriorityError } from "~/errors/invalid-priority.error";
import { OversizeMsgError } from "~/errors/oversize-msg.error";
import { MAX_MSG_SIZE, VISIBILITY_TIMEOUT } from "~/constants";
import { PostgresScriptRunner } from "./postgres.runner";
import type { Store, SendMsgParams, NextMsgParams, Msg } from "~/types";
import { loggerFactory } from "~/logger";
import { randomId } from "~/random";

export class PostgresStore implements Store {
  private pool: Pool;
  private runner: PostgresScriptRunner;

  constructor(private readonly logger = loggerFactory("postgres.store")) {}

  async connect(pool: Pool) {
    this.pool = pool;
    this.runner = new PostgresScriptRunner(this.pool);
    await this.runner.loadFromDir(path.join(__dirname, "scripts"));
    await this.runner.runScript("init");
  }

  async disconnect() {
    // pool is not store's responsibility
  }

  async createQueue(queueName: string) {
    try {
      await this.runner.runScript("create-queue", [queueName, MAX_MSG_SIZE]);
      return true;
    } catch (error) {
      assert(error instanceof Error);
      if ("code" in error && error.code === "23505") return false; // unique violation
      throw error;
    }
  }

  async destroyQueue(queueName: string) {
    const result = await this.runner.runScript<{ rowCount: number }>(
      "destroy-queue",
      [queueName]
    );

    return result.rowCount === 1;
  }

  async sendMsg(
    msg: string,
    { queueName, customId, priority = 5 }: SendMsgParams
  ) {
    if (priority < 1 || priority > 9) throw new InvalidPriorityError();

    if (Buffer.byteLength(msg, "utf-8") > MAX_MSG_SIZE) {
      throw new OversizeMsgError();
    }

    const msgId = customId ?? "msg_" + randomId();

    await this.runner.runScript("send-msg", [msgId, queueName, msg, priority]);

    return msgId;
  }

  async nextMsg({ queueName }: NextMsgParams): Promise<Msg | null> {
    type Rows = Array<{
      id: string;
      content: string;
      enqueued_at: string;
      visibility_ts: string;
      receive_count: number;
    }>;

    const result = await this.runner.runScript<{ rows: Rows }>("next-msg", [
      queueName,
      VISIBILITY_TIMEOUT.toString(),
    ]);

    if (result.rows.length === 0) return null;

    const [row] = result.rows;
    const { id, content, enqueued_at, visibility_ts, receive_count } = row;

    return {
      id,
      content,
      enqueuedAt: new Date(enqueued_at).getTime(), // to Unix timestamp (ms)
      visibilityTs: Number(visibility_ts), // pg converted BIGINT to string
      receiveCount: receive_count,
    };
  }

  async removeMsg(msgId: string, queueName: string, visibilityTs?: number) {
    const result = await this.runner.runScript<{ rowCount: number }>(
      "remove-msg",
      [msgId, queueName, visibilityTs?.toString() || "0"]
    );

    return result.rowCount === 1;
  }

  async getQueueLength(queueName: string) {
    const result = await this.runner.runScript<{
      rows: [{ count: string }];
    }>("get-queue-length", [queueName]);

    return parseInt(result.rows[0].count, 10);
  }
}
