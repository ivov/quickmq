import fs from "node:fs/promises";
import path from "node:path";
import { Pool } from "pg";

type ScriptName =
  | "init"
  | "create-queue"
  | "destroy-queue"
  | "get-queue-length"
  | "next-msg"
  | "remove-msg"
  | "send-msg";

export class PostgresScriptRunner {
  private scripts: Record<string, string> = {};

  constructor(private readonly pool: Pool) {}

  async loadFromDir(dirPath: string) {
    const filepaths = (await fs.readdir(dirPath, { withFileTypes: true }))
      .filter((entry) => entry.isFile() && entry.name.endsWith(".sql"))
      .map((entry) => path.join(dirPath, entry.name));

    for (const filepath of filepaths) {
      const { name: scriptName } = path.parse(filepath);
      this.scripts[scriptName] = await fs.readFile(filepath, "utf-8");
    }
  }

  async runScript<T>(scriptName: ScriptName, params: unknown[] = []) {
    const script = this.scripts[scriptName];
    const result = await this.pool.query(script, params);
    return result as T;
  }
}
