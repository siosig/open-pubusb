#!/usr/bin/env python3
"""Basic Pub/Sub operations against open-pubusb.

Uses the official Google Cloud client (google-cloud-pubsub) and only
redirects the endpoint to a local open-pubusb via `PUBSUB_EMULATOR_HOST`.
No application code changes are required.

What it does:
  1. Create a topic
  2. Create a subscription
  3. Publish a message (push)
  4. Pull the message and ack it (pull & ack)
  5. Delete the subscription
  6. Delete the topic

Prerequisites:
  - open-pubusb is running (for example, in another terminal):

      cargo run -p open-pubusb -- serve --ephemeral \\
          --listen 127.0.0.1:8085 --admin-listen 127.0.0.1:8086

  - google-cloud-pubsub is installed:
      pip install google-cloud-pubsub

Usage:
  PUBSUB_EMULATOR_HOST=127.0.0.1:8085 python3 sample/python/pubsub_demo.py
"""

from __future__ import annotations

import os
import sys
import uuid

from google.api_core import exceptions as gax_exceptions
from google.cloud import pubsub_v1

PROJECT_ID = os.environ.get("PUBSUB_PROJECT_ID", "sample-project")


def require_emulator_host() -> str:
    host = os.environ.get("PUBSUB_EMULATOR_HOST")
    if not host:
        print(
            "PUBSUB_EMULATOR_HOST is not set.\n"
            "Example: PUBSUB_EMULATOR_HOST=127.0.0.1:8085 python3 sample/python/pubsub_demo.py",
            file=sys.stderr,
        )
        sys.exit(1)
    return host


def create_topic(publisher: pubsub_v1.PublisherClient, topic_id: str) -> str:
    """Create a topic."""
    topic_path = publisher.topic_path(PROJECT_ID, topic_id)
    publisher.create_topic(request={"name": topic_path})
    print(f"[create] topic: {topic_path}")
    return topic_path


def delete_topic(publisher: pubsub_v1.PublisherClient, topic_path: str) -> None:
    """Delete a topic."""
    publisher.delete_topic(request={"topic": topic_path})
    print(f"[delete] topic: {topic_path}")


def create_subscription(
    subscriber: pubsub_v1.SubscriberClient, topic_path: str, subscription_id: str
) -> str:
    """Create a pull subscription attached to the topic."""
    subscription_path = subscriber.subscription_path(PROJECT_ID, subscription_id)
    subscriber.create_subscription(
        request={"name": subscription_path, "topic": topic_path}
    )
    print(f"[create] subscription: {subscription_path}")
    return subscription_path


def delete_subscription(
    subscriber: pubsub_v1.SubscriberClient, subscription_path: str
) -> None:
    """Delete a subscription."""
    subscriber.delete_subscription(request={"subscription": subscription_path})
    print(f"[delete] subscription: {subscription_path}")


def push_message(
    publisher: pubsub_v1.PublisherClient,
    topic_path: str,
    data: bytes,
    **attributes: str,
) -> str:
    """Publish a single message and return its message_id."""
    future = publisher.publish(topic_path, data, **attributes)
    message_id = future.result(timeout=10)
    print(f"[publish] message_id={message_id} data={data!r} attributes={attributes}")
    return message_id


def pull_and_ack(
    subscriber: pubsub_v1.SubscriberClient,
    subscription_path: str,
    max_messages: int = 10,
) -> list[bytes]:
    """Pull messages, ack everything received, and return the received payloads."""
    response = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": max_messages},
        timeout=10,
    )

    received_data: list[bytes] = []
    ack_ids: list[str] = []
    for received in response.received_messages:
        received_data.append(received.message.data)
        ack_ids.append(received.ack_id)
        print(
            f"[pull] message_id={received.message.message_id} "
            f"data={received.message.data!r} "
            f"attributes={dict(received.message.attributes)}"
        )

    if ack_ids:
        subscriber.acknowledge(
            request={"subscription": subscription_path, "ack_ids": ack_ids}
        )
        print(f"[ack] {len(ack_ids)} message(s) acknowledged")
    else:
        print("[pull] no messages available")

    return received_data


def main() -> None:
    require_emulator_host()

    run_id = uuid.uuid4().hex[:8]
    topic_id = f"sample-topic-{run_id}"
    subscription_id = f"sample-sub-{run_id}"

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    topic_path = create_topic(publisher, topic_id)
    try:
        subscription_path = create_subscription(subscriber, topic_path, subscription_id)
        try:
            push_message(publisher, topic_path, b"hello from open-pubusb sample", source="demo")
            pull_and_ack(subscriber, subscription_path)

            # Pull once more to confirm that acked messages are not redelivered.
            leftover = pull_and_ack(subscriber, subscription_path)
            assert not leftover, "acked message must not be redelivered"
        finally:
            delete_subscription(subscriber, subscription_path)
    finally:
        try:
            delete_topic(publisher, topic_path)
        except gax_exceptions.NotFound:
            pass

    print("done")


if __name__ == "__main__":
    main()
