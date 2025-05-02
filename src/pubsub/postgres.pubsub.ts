import assert from "node:assert";
import { Pool, type PoolClient } from "pg";
import { loggerFactory } from "~/logger";
import type { PubSub, Channel, RawNoticeHandler } from "~/types";

export class PostgresPubSub implements PubSub {
  private pool: Pool;
  private client: PoolClient | undefined;
  private libPrefix: string;
  private handlers: Map<string, ((msg: string) => void)[]> = new Map();

  constructor(private readonly logger = loggerFactory("postgres.pubsub")) {}

  async connect({ libPrefix, pool }: { libPrefix: string; pool: Pool }) {
    this.libPrefix = libPrefix;
    this.pool = pool;

    this.client = await this.pool.connect();

    this.client.on("notification", (msg) => {
      const { channel, payload } = msg;

      if (!channel || !payload) return;

      const handlers = this.handlers.get(channel) ?? [];
      handlers.forEach((handler) => handler(payload));
    });
  }

  async disconnect() {
    if (this.client) {
      this.client.release();
      this.client = undefined;
    }
  }

  async publish(channel: Channel, msg: string) {
    assert(this.pool);
    const channelName = `${this.libPrefix}:${channel}`;
    await this.pool.query("SELECT pg_notify($1, $2);", [channelName, msg]);
  }

  async subscribe(channel: Channel, handler: RawNoticeHandler) {
    assert(this.client);

    const channelName = `${this.libPrefix}:${channel}`;

    const handlers = this.handlers.get(channelName) ?? [];
    handlers.push(handler);
    this.handlers.set(channelName, handlers);

    await this.client.query(`LISTEN "${channelName}";`);
  }
}
