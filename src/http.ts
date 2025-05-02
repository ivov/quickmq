import http from "node:http";
import https from "node:https";
import assert from "node:assert";
import { URL } from "node:url";
import type {
  RequestOptions,
  IncomingMessage,
  ServerResponse,
} from "node:http";

type RequestHandler = (req: IncomingMessage, res: ServerResponse) => void;

export class HttpServer {
  private server: http.Server | undefined;

  constructor(private readonly handler: RequestHandler) {}

  async listen(port: number) {
    this.server = http.createServer(this.handler);

    return new Promise<void>((resolve, reject) => {
      assert(this.server);
      this.server.listen(port, resolve);

      this.server.on("error", reject);
    });
  }

  async close() {
    return new Promise<void>((resolve, reject) => {
      if (!this.server) {
        resolve();
        return;
      }

      this.server.close((error) => {
        if (error) reject(error);
        else {
          this.server = undefined;
          resolve();
        }
      });
    });
  }
}

export async function parseRequestBody<T = unknown>(
  req: IncomingMessage
): Promise<T> {
  return new Promise((resolve, reject) => {
    let reqBody = "";

    req.on("data", (chunk) => {
      reqBody += chunk.toString();
    });

    req.on("end", () => {
      try {
        resolve(reqBody ? JSON.parse(reqBody) : null);
      } catch (error) {
        reject(error);
      }
    });
  });
}

export type HttpRequestConfig = {
  method: string;
  url: string;
  headers?: Record<string, string>;
  data?: Record<string, unknown> | string;
};

export type HttpResponse<T = unknown> = {
  data: T;
  status: number;
  statusText: string;
  headers: Record<string, string | string[]>;
};

export class HttpClient {
  request<T>(config: HttpRequestConfig): Promise<HttpResponse<T>> {
    return new Promise((resolve, reject) => {
      const url = new URL(config.url);
      const isHttps = url.protocol === "https:";

      const options: RequestOptions = {
        method: config.method,
        hostname: url.hostname,
        port: url.port || (isHttps ? 443 : 80),
        path: url.pathname + url.search,
        headers: config.headers,
      };

      const requestFn = isHttps ? https.request : http.request;

      const req = requestFn(options, (res) => {
        let resData = "";

        res.on("data", (chunk) => (resData += chunk));

        res.on("end", () => {
          try {
            let parsedData;

            if (this.isJson(res.headers["content-type"])) {
              parsedData = resData.length > 0 ? JSON.parse(resData) : null;
            } else {
              parsedData = resData;
            }

            resolve({
              data: parsedData,
              status: res.statusCode ?? 200,
              statusText: res.statusMessage ?? "OK",
              headers: res.headers as Record<string, string>,
            });
          } catch (error) {
            reject(error);
          }
        });
      });

      const reqData =
        typeof config.data === "string"
          ? config.data
          : JSON.stringify(config.data);

      if (reqData) req.write(reqData);

      req.on("error", (error) => reject(error));

      req.end();
    });
  }

  flatten(params: Record<string, unknown>, prefix = "") {
    const result: Record<string, string> = {};

    for (const [key, value] of Object.entries(params)) {
      const newKey = prefix ? `${prefix}.${key}` : key;

      if (typeof value === "object" && value !== null) {
        Object.assign(
          result,
          this.flatten(value as Record<string, unknown>, newKey)
        );
      } else {
        result[newKey] = String(value);
      }
    }

    return result;
  }

  // ----------------------------------
  //         private methods
  // ----------------------------------

  private isJson(contentType?: string) {
    if (!contentType) return false;

    return (
      contentType.includes("application/json") ||
      contentType.includes("x-amz-json-1.0")
    );
  }
}
