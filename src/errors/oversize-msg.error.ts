import { MAX_MSG_SIZE } from "~/constants";

export class OversizeMsgError extends Error {
  constructor() {
    super(`Message exceeds max size of ${MAX_MSG_SIZE} bytes`);
  }
}
