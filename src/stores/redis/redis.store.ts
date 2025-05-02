import path from "node:path";
import Redis from "ioredis";
import { RedisScriptRunner } from "./redis.runner";
import {
  DEFAULT_REDIS_CONNECTION_PARAMS,
  MAX_MSG_SIZE,
  VISIBILITY_TIMEOUT,
} from "~/constants";
import { OversizeMsgError } from "~/errors/oversize-msg.error";
import { InvalidPriorityError } from "~/errors/invalid-priority.error";
import type {
  Store,
  NextMsgParams,
  SendMsgParams,
  NextMsgResult,
  RawPipelineResults,
  RedisConnectionParams,
} from "~/types";
import { loggerFactory } from "~/logger";
import { randomId } from "~/random";

export class RedisStore implements Store {
  private client: Redis;
  private libPrefix: string;
  private runner: RedisScriptRunner;

  constructor(private readonly logger = loggerFactory("redis.store")) {}

  async connect(params: Partial<RedisConnectionParams>) {
    const { host, port, libPrefix } = {
      ...DEFAULT_REDIS_CONNECTION_PARAMS,
      ...params,
    };

    this.client = new Redis({ host, port });
    this.client.on("error", (error) => this.logger.error(error));
    this.libPrefix = libPrefix;

    this.runner = new RedisScriptRunner(this.client);
    await this.runner.loadFromDir(path.join(__dirname, "scripts"));
  }

  async disconnect() {
    await this.client.quit();
  }

  async createQueue(queueName: string) {
    const hashKey = this.hashKey(queueName);

    const results = await this.client
      .pipeline()
      .hsetnx(hashKey, "max_msg_size", MAX_MSG_SIZE)
      .hsetnx(hashKey, "created_at", Date.now())
      .exec();

    const [hsetnx] = this.extractOrFail<number[]>(results);

    return hsetnx === 1;
  }

  async destroyQueue(queueName: string) {
    const results = await this.client
      .pipeline()
      .del(this.hashKey(queueName), this.sortedSetKey(queueName))
      .exec();

    type Results = [del: number, srem: number];

    const [del] = this.extractOrFail<Results>(results);

    return del === 1;
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
    const hashKey = this.hashKey(queueName);
    const sortedSetKey = this.sortedSetKey(queueName);

    await this.runner.runScript<number>(
      "send-msg",
      [hashKey, sortedSetKey],
      [msgId, msg, priority.toString()]
    );

    this.logger.debug("Sent message", { msgId });

    return msgId;
  }

  async nextMsg({ queueName }: NextMsgParams) {
    const results = await this.runner.runScript<NextMsgResult>(
      "next-msg",
      [this.hashKey(queueName)],
      [VISIBILITY_TIMEOUT.toString()]
    );

    if (results.length === 0) return null;

    const [id, content, enqueuedAt, visibilityTs, receiveCount] = results;

    this.logger.debug("Received message", { msgId: id });

    return {
      id,
      content,
      enqueuedAt,
      visibilityTs,
      receiveCount,
    };
  }

  async removeMsg(msgId: string, queueName: string, visibilityTs?: number) {
    const hashKey = this.hashKey(queueName);
    const sortedSetKey = this.sortedSetKey(queueName);

    const result = await this.runner.runScript<number>(
      "remove-msg",
      [hashKey, sortedSetKey],
      [msgId, visibilityTs?.toString() ?? "0"]
    );

    return result === 1;
  }

  async getQueueLength(queueName: string) {
    const sortedSetKey = this.sortedSetKey(queueName);
    return await this.client.zcard(sortedSetKey);
  }

  // ----------------------------------
  //         private methods
  // ----------------------------------

  private extractOrFail<PipelineResults extends unknown[]>(
    results: RawPipelineResults
  ) {
    if (!results) throw new Error("Redis pipeline returned null");

    return results.map(([error, data]) => {
      if (error) throw error;
      return data;
    }) as PipelineResults;
  }

  private hashKey(queueName: string) {
    return `${this.libPrefix}:${queueName}`;
  }

  private sortedSetKey(queueName: string) {
    return `${this.libPrefix}:${queueName}:sequence`;
  }
}
