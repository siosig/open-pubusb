// Compatibility test (T029, User Story 1): proves official
// `@google-cloud/pubsub` Node.js client code works against `open-pubusb`
// unmodified, switching only the connection target via
// `PUBSUB_EMULATOR_HOST` — no application code change (spec.md SC-001).
//
// Two client shapes are exercised, matching what real Node applications
// use:
//   - the high-level `PubSub`/`Topic`/`Subscription` API (create/delete,
//     which *does* auto-detect `PUBSUB_EMULATOR_HOST`) for topic and
//     subscription management;
//   - the low-level generated `v1.SubscriberClient`/`v1.PublisherClient`
//     for unary Publish/Pull/Acknowledge/ModifyAckDeadline, since the
//     high-level `Subscription` object only exposes the streaming
//     `on('message')` API, not unary Pull (StreamingPull is User Story 4,
//     not yet implemented by `open-pubusb` — see the note at the bottom of this
//     file). The low-level clients don't auto-detect the emulator env
//     var the way the high-level `PubSub` class does, so this file reads
//     it explicitly and passes an insecure endpoint — this is normal
//     low-level-client usage, not a `open-pubusb`-specific workaround.
//
// Requires `open-pubusb` to be running with `PUBSUB_EMULATOR_HOST` pointed at
// it, e.g.:
//
//   open-pubusb serve --ephemeral --listen 127.0.0.1:8085 --admin-listen 127.0.0.1:8086 &
//   PUBSUB_EMULATOR_HOST=127.0.0.1:8085 npm test
//
// Every test is skipped (not failed) when PUBSUB_EMULATOR_HOST is unset.

import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { PubSub, v1 } from "@google-cloud/pubsub";
import { credentials } from "@grpc/grpc-js";

const emulatorHost = process.env.PUBSUB_EMULATOR_HOST;
const projectId = process.env.PUBSUB_PROJECT_ID ?? "compat-node";

function uniqueName(prefix) {
  return `${prefix}-${randomUUID().slice(0, 12)}`;
}

// No credentials, no endpoint override beyond PUBSUB_EMULATOR_HOST: this
// is exactly what an application already pointed at real Pub/Sub does,
// per SC-001 — only the environment variable changed.
function newHighLevelClient() {
  return new PubSub({ projectId });
}

function lowLevelClientOptions() {
  // The low-level gapic clients (unlike the high-level `PubSub` class)
  // do NOT parse `host:port` out of a single `apiEndpoint` string — an
  // embedded port is silently ignored and the client falls back to its
  // default (443), producing a hang (TRANSIENT_FAILURE retried forever)
  // rather than a clear connection error. `port` must be passed
  // separately. Confirmed via `GRPC_TRACE=connectivity_state`, which
  // showed the channel dialing `127.0.0.1:443` instead of the emulator
  // port until this was split out.
  const [host, port] = emulatorHost.split(":");
  return {
    apiEndpoint: host,
    port: Number(port),
    sslCreds: credentials.createInsecure(),
  };
}

async function withTopicAndSubscription(t) {
  const pubsub = newHighLevelClient();
  const topic = pubsub.topic(uniqueName("topic"));
  await topic.create();
  const subscription = topic.subscription(uniqueName("sub"));
  await subscription.create();
  t.after(async () => {
    await subscription.delete().catch(() => {});
    await topic.delete().catch(() => {});
    pubsub.close();
  });
  return { topic, subscription };
}

test("publish, pull, and acknowledge round trip", { skip: !emulatorHost }, async (t) => {
  const { topic, subscription } = await withTopicAndSubscription(t);

  const publisher = new v1.PublisherClient(lowLevelClientOptions());
  const subscriber = new v1.SubscriberClient(lowLevelClientOptions());
  t.after(async () => {
    await publisher.close();
    await subscriber.close();
  });

  const [publishResponse] = await publisher.publish({
    topic: topic.name,
    messages: [
      {
        data: Buffer.from("hello from node"),
        attributes: { origin: "compat-test", kind: "greeting" },
      },
    ],
  });
  const messageId = publishResponse.messageIds[0];
  assert.ok(messageId);

  const [pullResponse] = await subscriber.pull({
    subscription: subscription.name,
    maxMessages: 10,
  });
  assert.equal(pullResponse.receivedMessages.length, 1);
  const received = pullResponse.receivedMessages[0];
  assert.equal(received.message.data.toString(), "hello from node");
  assert.equal(received.message.attributes.origin, "compat-test");
  assert.equal(received.message.attributes.kind, "greeting");
  assert.equal(received.message.messageId, messageId);

  await subscriber.acknowledge({
    subscription: subscription.name,
    ackIds: [received.ackId],
  });

  const [again] = await subscriber.pull({
    subscription: subscription.name,
    maxMessages: 10,
  });
  assert.equal((again.receivedMessages ?? []).length, 0);
});

test("modifyAckDeadline(0) causes immediate redelivery", { skip: !emulatorHost }, async (t) => {
  const { topic, subscription } = await withTopicAndSubscription(t);

  const publisher = new v1.PublisherClient(lowLevelClientOptions());
  const subscriber = new v1.SubscriberClient(lowLevelClientOptions());
  t.after(async () => {
    await publisher.close();
    await subscriber.close();
  });

  await publisher.publish({
    topic: topic.name,
    messages: [{ data: Buffer.from("nack me") }],
  });

  const [first] = await subscriber.pull({
    subscription: subscription.name,
    maxMessages: 10,
  });
  assert.equal(first.receivedMessages.length, 1);

  await subscriber.modifyAckDeadline({
    subscription: subscription.name,
    ackIds: [first.receivedMessages[0].ackId],
    ackDeadlineSeconds: 0,
  });

  const [redelivered] = await subscriber.pull({
    subscription: subscription.name,
    maxMessages: 10,
  });
  assert.equal(redelivered.receivedMessages.length, 1);
  assert.equal(redelivered.receivedMessages[0].message.data.toString(), "nack me");
});

test("creating a duplicate topic fails with ALREADY_EXISTS", { skip: !emulatorHost }, async (t) => {
  const pubsub = newHighLevelClient();
  const topic = pubsub.topic(uniqueName("topic"));
  await topic.create();
  t.after(() => {
    topic.delete().catch(() => {});
    pubsub.close();
  });

  await assert.rejects(() => topic.create(), (err) => {
    assert.equal(err.code, 6); // google.rpc.Code.ALREADY_EXISTS
    return true;
  });
});

test("getting a missing topic fails with NOT_FOUND", { skip: !emulatorHost }, async (t) => {
  const pubsub = newHighLevelClient();
  t.after(() => pubsub.close());
  const topic = pubsub.topic(uniqueName("missing-topic"));

  await assert.rejects(() => topic.getMetadata(), (err) => {
    assert.equal(err.code, 5); // google.rpc.Code.NOT_FOUND
    return true;
  });
});

test("pulling on a deleted subscription fails with NOT_FOUND", { skip: !emulatorHost }, async (t) => {
  const { subscription } = await withTopicAndSubscription(t);
  await subscription.delete();

  const subscriber = new v1.SubscriberClient(lowLevelClientOptions());
  t.after(() => subscriber.close());

  await assert.rejects(
    () => subscriber.pull({ subscription: subscription.name, maxMessages: 10 }),
    (err) => {
      assert.equal(err.code, 5); // google.rpc.Code.NOT_FOUND
      return true;
    },
  );
});

// The high-level `subscription.on('message', ...)` API (StreamingPull
// under the hood) is intentionally NOT exercised here: `open-pubusb` returns
// UNIMPLEMENTED for StreamingPull until User Story 4 lands. T029's scope
// is unary Pull/Acknowledge/ModifyAckDeadline only (spec.md User Story
// 1); a streaming compat test belongs with the US4 implementation.

if (!emulatorHost) {
  test("PUBSUB_EMULATOR_HOST not set — compat suite skipped", () => {
    console.log(
      "Set PUBSUB_EMULATOR_HOST to a running open-pubusb instance to run the Node.js compat suite.",
    );
  });
}
