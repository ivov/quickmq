import { CONSUMER_FORCE_SHUTDOWN_TIMEOUT } from "~/constants";
import { ShutdownTimeoutError } from "~/errors/shutdown-timeout.error";
import type { DeferredPromise } from "~/types";

export function createDeferredPromise<T = void>(): DeferredPromise<T> {
  const deferred: Partial<DeferredPromise<T>> = {};
  deferred.promise = new Promise<T>((resolve, reject) => {
    deferred.resolve = resolve;
    deferred.reject = reject;
  });
  return deferred as DeferredPromise<T>;
}

export const forceShutdownTimeout = () => {
  return new Promise((_, reject) =>
    setTimeout(
      () => reject(new ShutdownTimeoutError()),
      CONSUMER_FORCE_SHUTDOWN_TIMEOUT
    )
  );
};

export const sleep = (ms: number) =>
  new Promise((resolve) => setTimeout(resolve, ms));
