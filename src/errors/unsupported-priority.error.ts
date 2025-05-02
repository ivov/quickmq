export class UnsupportedPriorityError extends Error {
  constructor() {
    super(
      "SQS FIFO queues do not support message prioritization. All messages within a message group are processed in strict FIFO order. For priority support, consider using Redis or Postgres implementations instead."
    );
  }
}
