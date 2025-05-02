export class InvalidPriorityError extends Error {
  constructor() {
    super(`Priority must be between 1 and 9`);
  }
}
