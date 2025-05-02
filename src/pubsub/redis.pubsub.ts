import Redis from "ioredis";
import type {
  PubSub,
  Channel,
  RedisConnectionParams,
  RawNoticeHandler,
} from "~/types";
import { DEFAULT_REDIS_CONNECTION_PARAMS } from "~/constants";
import { loggerFactory } from "~/logger";

export class RedisPubSub implements PubSub {
  private publisher: Redis;
  private subscriber: Redis;
  private libPrefix: string;

  constructor(private readonly logger = loggerFactory("redis.pubsub")) {}

  async connect(connectionParams: Partial<RedisConnectionParams>) {
    const { host, port, libPrefix } = {
      ...DEFAULT_REDIS_CONNECTION_PARAMS,
      ...connectionParams,
    };

    this.libPrefix = libPrefix;
    this.publisher = new Redis({ host, port });
    this.subscriber = new Redis({ host, port });

    this.publisher.on("error", (error) => this.logger.error(error));
    this.subscriber.on("error", (error) => this.logger.error(error));
  }

  async disconnect() {
    await Promise.all([this.publisher.quit(), this.subscriber.quit()]);
  }

  /** Publish a message into a channel. */
  async publish(channel: Channel, message: string) {
    await this.publisher.publish(`${this.libPrefix}:${channel}`, message);
  }

  /** Listen to a channel and register a handler to call for each notice received. */
  async subscribe(channel: Channel, handler: RawNoticeHandler) {
    const channelName = `${this.libPrefix}:${channel}`;
    await this.subscriber.subscribe(channelName);

    this.subscriber.on("message", (chan, message) => {
      if (chan === channelName) handler(message);
    });
  }
}
