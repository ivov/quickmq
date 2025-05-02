// @TODO: Remove this file

require("tsconfig-paths").register({
  baseUrl: "./dist",
  paths: {
    "~/*": ["./*"],
  },
});

import { SqsStore } from "~/stores/sqs/sqs.store"; // Adjust the path as needed
// import { sleep } from "./promises";

async function testSQSStore() {
  console.log("Starting SQS Store test...");

  // console.log("process.env", process.env);

  // Create store instance
  const store = new SqsStore();

  // Queue name for testing
  const queueName = "test-queue";

  try {
    // Connect to SQS
    console.log("Connecting to SQS...");
    await store.connect({});
    console.log("Connected successfully");

    // Create queue
    console.log(`Creating queue: ${queueName}`);
    const wasCreated = await store.createQueue(queueName);
    console.log(
      `Queue creation ${wasCreated ? "successful" : "not done (already exists)"}`
    );

    // Send message
    console.log("Sending message");
    const msgContent = `Test message ${Date.now()}`;
    const msgId = await store.sendMsg(msgContent, {
      queueName,
    });
    console.log(`Message sent with ID: ${msgId}`);

    // await sleep(5000);

    // Check queue length
    const length = await store.getQueueLength(queueName);
    console.log(`Queue length: ${length}`);

    // Receive message
    console.log("Receiving message");
    const msg = await store.nextMsg({ queueName });

    if (msg) {
      console.log("Message received:");
      console.log(`- ID: ${msg.id}`);
      console.log(`- Content: ${msg.content}`);
      console.log(`- Enqueued at: ${new Date(msg.enqueuedAt).toISOString()}`);
      console.log(
        `- Invisible until: ${new Date(msg.visibilityTs).toISOString()}`
      );
      console.log(`- Receive count: ${msg.receiveCount}`);

      // Remove message
      console.log("Removing message");
      const removed = await store.removeMsg(msg.id, queueName);
      console.log(`Message removal ${removed ? "successful" : "failed"}`);
    } else {
      console.log("No message received");
    }

    // Optional: Clean up by destroying the queue
    if (process.env.CLEANUP === "true") {
      console.log("Cleaning up: destroying queue");
      const destroyed = await store.destroyQueue(queueName);
      console.log(`Queue destruction ${destroyed ? "successful" : "failed"}`);
    }
  } catch (error) {
    console.error("Test failed with error:", error);
  } finally {
    // Disconnect
    await store.disconnect();
    console.log("Test completed");
  }
}

// Run the test
testSQSStore().catch(console.error);
