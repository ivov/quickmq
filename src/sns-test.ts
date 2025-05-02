
/**
 * 1. Run `ngrok http 3000`
 * 2. Visit `http://127.0.0.1:4040/inspect/http and get the URL
 * 3. Set `QUICK_SNS_SERVER_URL` in .env to ngrok's URL
 * 4. Run `pnpm sns`
 */

require("tsconfig-paths").register({
  baseUrl: "./dist",
  paths: {
    "~/*": ["./*"],
  },
});

import { SnsPubSub } from "~/pubsub/sns.pubsub";
import { sleep } from "~/promises";
import { loggerFactory } from "~/logger";
import type { Topic } from "./types";

async function testSnsPubSub() {
  console.log("Starting SNS PubSub test...");

  const logger = loggerFactory("sns-test");
  const pubsub = new SnsPubSub(logger);

  try {
    console.log("Connecting to SNS...");
    await pubsub.connect({});
    console.log("Connected successfully");

    const topics = ["to_consumers", "to_producers"] as Topic[];

    // Subscribe to topics
    for (const topic of topics) {
      console.log(`Subscribing to channel: ${topic}`);
      await pubsub.subscribe(topic, (message) => {
        console.log(`<- Received notice on ${topic}\n\t`, message, "\n");
      });
      console.log(`Subscribed to ${topic}`);
    }

    // Allow some time for subscription confirmation
    // console.log("Waiting for subscription confirmation...");
    await sleep(3000);

    // Publish string notices to topics
    for (const topic of topics) {
      const notice = `Test message for \`${topic}\` at ${new Date().toISOString()}`;
      await pubsub.publish(topic, notice);
      console.log(`-> Published notice to \`${topic}\``);
    }

    // Wait to see the received notices
    console.log("Waiting to receive notices...");
    await sleep(3000);

    // Publish object notices to topics
    for (const topic of topics) {
      const notice = JSON.stringify({
        type: topic === "to_consumers" ? "abort-msg" : "msg-processed",
        payload: {
          msgId: `test-msg-${Date.now()}`,
          timestamp: new Date().toISOString(),
        },
      });

      await pubsub.publish(topic, notice);
      console.log(`-> Published notice to \`${topic}\``);
    }

    // Wait again to see the received notices
    console.log("Waiting to receive notices...");
    await sleep(5000);
  } catch (error) {
    console.error("Test failed with error:", error);
  } finally {
    // Disconnect
    console.log("Disconnecting...");
    await pubsub.disconnect();
    console.log("Test completed");
  }
}

// Run the test
testSnsPubSub().catch(console.error);
