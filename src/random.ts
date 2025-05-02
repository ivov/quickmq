import { randomUUID } from "node:crypto";

export const randomId = () => randomUUID().replace(/-/g, "");
