export class ShutdownTimeoutError extends Error {
  constructor() {
    super("Forced shutdown - some messages may not have finished processing");
  }
}
