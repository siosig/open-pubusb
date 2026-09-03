// Compatibility test (T066, User Story 4): proves the official
// `@google-cloud/pubsub` Node.js client's high-level streaming API
// (`subscription.on('message')`, `StreamingPull` under the hood) works
// against `open-pubusb` unmodified — ack and nack/redelivery.
//
// Requires `open-pubusb` running with `PUBSUB_EMULATOR_HOST` pointed at it (see
// `roundtrip.test.js`'s module docstring). Skipped when unset.

import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { PubSub } from "@google-cloud/pubsub";

const emulatorHost = process.env.PUBSUB_EMULATOR_HOST;
const projectId = process.env.PUBSUB_PROJECT_ID ?? "compat-node";

function uniqueName(prefix) {
  return `${prefix}-${randomUUID().slice(0, 12)}`;
}

function newHighLevelClient() {
  return new PubSub({ projectId });
}

async function withTopicAndSubscription(t) {
  const pubsub = newHighLevelClient();
  const topic = pubsub.topic(uniqueName("topic"));
  await topic.create();
  const subscription = topic.subscription(uniqueName("sub"), {
    ackDeadlineSeconds: 10,
  });
  await subscription.create();
  t.after(async () => {
    subscription.removeAllListeners();
    await subscription.close().catch(() => {});
    await subscription.delete().catch(() => {});
    await topic.delete().catch(() => {});
    pubsub.close();
  });
  return { pubsub, topic, subscription };
}

test(
  "subscription.on('message') receives and acks via streaming pull",
  { skip: !emulatorHost },
  async (t) => {
    const { topic, subscription } = await withTopicAndSubscription(t);

    const received = await new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error("did not receive the message within 15s")),
        15000,
      );
      subscription.on("message", (message) => {
        clearTimeout(timer);
        message.ack();
        resolve(message.data.toString());
      });
      subscription.on("error", reject);
      topic.publishMessage({ data: Buffer.from("hello via node streaming") });
    });

    assert.equal(received, "hello via node streaming");
  },
);

test(
  "message.nack() causes redelivery",
  { skip: !emulatorHost },
  async (t) => {
    const { topic, subscription } = await withTopicAndSubscription(t);

    let attempts = 0;
    const deliveredTwice = new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error("message was not redelivered after nack() within 15s")),
        15000,
      );
      subscription.on("message", (message) => {
        attempts += 1;
        if (attempts === 1) {
          message.nack();
        } else {
          message.ack();
          clearTimeout(timer);
          resolve();
        }
      });
      subscription.on("error", reject);
    });

    await topic.publishMessage({ data: Buffer.from("nack-then-ack") });
    await deliveredTwice;
    assert.equal(attempts, 2);
  },
);
