import fs from "node:fs/promises";
import path from "node:path";
import assert from "node:assert";
import Redis from "ioredis";

type ScriptName = "next-msg" | "remove-msg" | "send-msg";

export class RedisScriptRunner {
  private scriptHashes: Record<string, string> = {};

  constructor(private readonly client: Redis) {}

  async loadFromDir(dirPath: string) {
    const filepaths = (await fs.readdir(dirPath, { withFileTypes: true }))
      .filter((entry) => entry.isFile() && entry.name.endsWith(".lua"))
      .map((entry) => path.join(dirPath, entry.name));

    const scripts: Record<string, string> = {};

    for (const filepath of filepaths) {
      const { name: scriptName } = path.parse(filepath);
      scripts[scriptName] = await fs.readFile(filepath, "utf-8");
    }

    for (const [scriptName, scriptBody] of Object.entries(scripts)) {
      const sha = await this.client.script("LOAD", scriptBody);
      assert(typeof sha === "string");
      this.scriptHashes[scriptName] = sha;
    }
  }

  async runScript<T>(scriptName: ScriptName, keys: string[], args: string[]) {
    const sha = this.scriptHashes[scriptName];

    assert(sha);

    return (await this.client.evalsha(sha, keys.length, ...keys, ...args)) as T;
  }
}
