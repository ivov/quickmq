import dotenv from "dotenv";

dotenv.config();

import type {
  PostgresConnectionParams,
  RedisConnectionParams,
  SnsConnectionParams,
  SqsConnectionParams,
} from "~/types";

export const DEFAULT_LIB_PREFIX = "quick";
export const DEFAULT_QUEUE_NAME = "jobs";

export const DEFAULT_QUEUE_PROPS = {
  queueName: DEFAULT_QUEUE_NAME,
  priority: 5,
};

export const DEFAULT_REDIS_CONNECTION_PARAMS: RedisConnectionParams = {
  host: "127.0.0.1",
  port: 6379,
  libPrefix: DEFAULT_LIB_PREFIX,
};

const {
  QUICK_POSTGRES_HOST,
  QUICK_POSTGRES_PORT,
  QUICK_POSTGRES_DATABASE,
  QUICK_POSTGRES_USER,
  QUICK_POSTGRES_PASSWORD,
} = process.env;

export const DEFAULT_POSTGRES_CONNECTION_PARAMS: PostgresConnectionParams = {
  host: QUICK_POSTGRES_HOST ?? "localhost",
  port: parseInt(QUICK_POSTGRES_PORT ?? "5432", 10),
  database: QUICK_POSTGRES_DATABASE ?? "postgres",
  user: QUICK_POSTGRES_USER ?? "postgres",
  password: QUICK_POSTGRES_PASSWORD,
  libPrefix: DEFAULT_LIB_PREFIX,
};

const {
  QUICK_AWS_REGION,
  AWS_ACCESS_KEY_ID,
  AWS_SECRET_ACCESS_KEY,
  QUICK_SQS_ENDPOINT,
  QUICK_SNS_SERVER_URL,
  QUICK_SNS_SERVER_PORT,
  QUICK_SNS_ENDPOINT,
} = process.env;

export const DEFAULT_AWS_CONNECTION_PARAMS: SqsConnectionParams = {
  region: QUICK_AWS_REGION ?? "eu-west-3",
  endpoint: QUICK_SQS_ENDPOINT ?? "",
  accessKeyId: AWS_ACCESS_KEY_ID ?? "",
  secretAccessKey: AWS_SECRET_ACCESS_KEY ?? "",
};

export const DEFAULT_SNS_CONNECTION_PARAMS: SnsConnectionParams = {
  ...DEFAULT_AWS_CONNECTION_PARAMS,
  endpoint: QUICK_SNS_ENDPOINT ?? "",
  serverUrl: QUICK_SNS_SERVER_URL ?? "http://localhost",
  serverPort: parseInt(QUICK_SNS_SERVER_PORT ?? "3000", 10),
  libPrefix: DEFAULT_LIB_PREFIX,
};

/**
 * How long (in ms) a consumer is entitled to process a message for.
 * The message is invisible to other consumers for this duration.
 */
export const VISIBILITY_TIMEOUT = 30 * 1000; // 30 seconds

export const MAX_MSG_SIZE = 65536; // bytes (64 KiB)

/** How many messages a consumer will process concurrently by default. */
export const DEFAULT_CONSUMER_CONCURRENCY = 5;

export const inTest = process.env.NODE_ENV === "test";

/** How long (in ms) a consumer will wait for its messages to finish processing before forcing shutdown. */
export const CONSUMER_FORCE_SHUTDOWN_TIMEOUT = inTest ? 100 : 30_000;

/** How often (in ms) a consumer polls for messages. */
export const POLLING_INTERVAL = inTest ? 50 : 1000;
