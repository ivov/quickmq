import { inTest } from "./constants";

export interface Logger {
  debug(message: string, ...meta: unknown[]): void;
  info(message: string, ...meta: unknown[]): void;
  warn(message: string, ...meta: unknown[]): void;
  error(error: Error, stack: string, ...meta: unknown[]): void;
}

export class ConsoleLogger implements Logger {
  constructor(private readonly namespace: string) {}

  debug(msg: string, ...meta: unknown[]) {
    console.debug(`[${this.namespace}] ${msg}`, ...meta);
  }

  info(msg: string, ...meta: unknown[]) {
    console.info(`[${this.namespace}] ${msg}`, ...meta);
  }

  warn(msg: string, ...meta: unknown[]) {
    console.warn(`[${this.namespace}] ${msg}`, ...meta);
  }

  error(error: Error, ...meta: unknown[]) {
    console.error(`[${this.namespace}] ${error.message}`, error.stack, ...meta);
  }
}

export class NoOpLogger implements Logger {
  debug(): void {}
  info(): void {}
  warn(): void {}
  error(): void {}
}

export function loggerFactory(namespace: string) {
  return inTest ? new NoOpLogger() : new ConsoleLogger(namespace);
}
