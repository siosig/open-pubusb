"""Compatibility test (T028, User Story 1): proves the official
`google-cloud-pubsub` Python client works against `open-pubusb` unmodified,
switching only the connection target via `PUBSUB_EMULATOR_HOST` — no
application code change (spec.md SC-001).

Requires `open-pubusb` to be running with `PUBSUB_EMULATOR_HOST` pointed at it,
e.g.:

    open-pubusb serve --ephemeral --listen 127.0.0.1:8085 --admin-listen 127.0.0.1:8086 &
    PUBSUB_EMULATOR_HOST=127.0.0.1:8085 pytest tests/compat/python

Every test is skipped (not failed) when `PUBSUB_EMULATOR_HOST` is unset, so
`pytest` run without a live server still exits cleanly (e.g. in an
environment that only runs the Rust workspace tests).
"""

import os
import uuid

import pytest
from google.api_core import exceptions as gax_exceptions
from google.cloud import pubsub_v1

EMULATOR_HOST = os.environ.get("PUBSUB_EMULATOR_HOST")
PROJECT_ID = os.environ.get("PUBSUB_PROJECT_ID", "compat-python")

pytestmark = pytest.mark.skipif(
    not EMULATOR_HOST,
    reason="PUBSUB_EMULATOR_HOST is not set; start open-pubusb and set it to run this suite",
)


def unique_name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def publisher() -> pubsub_v1.PublisherClient:
    # No credentials, no endpoint override: exactly what an application
    # already pointed at real Pub/Sub does, per SC-001 — the only thing
    # that changed is the PUBSUB_EMULATOR_HOST environment variable.
    return pubsub_v1.PublisherClient()


@pytest.fixture
def subscriber() -> pubsub_v1.SubscriberClient:
    return pubsub_v1.SubscriberClient()


@pytest.fixture
def topic_path(publisher: pubsub_v1.PublisherClient):
    path = publisher.topic_path(PROJECT_ID, unique_name("topic"))
    publisher.create_topic(request={"name": path})
    yield path
    try:
        publisher.delete_topic(request={"topic": path})
    except gax_exceptions.GoogleAPICallError:
        pass


@pytest.fixture
def subscription_path(
    subscriber: pubsub_v1.SubscriberClient, topic_path: str
):
    path = subscriber.subscription_path(PROJECT_ID, unique_name("sub"))
    subscriber.create_subscription(request={"name": path, "topic": topic_path})
    yield path
    try:
        subscriber.delete_subscription(request={"subscription": path})
    except gax_exceptions.GoogleAPICallError:
        pass


def test_publish_pull_acknowledge_round_trip(
    publisher: pubsub_v1.PublisherClient,
    subscriber: pubsub_v1.SubscriberClient,
    topic_path: str,
    subscription_path: str,
) -> None:
    future = publisher.publish(
        topic_path, b"hello from python", origin="compat-test", kind="greeting"
    )
    message_id = future.result(timeout=10)
    assert message_id

    response = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": 10},
        timeout=10,
    )
    assert len(response.received_messages) == 1
    received = response.received_messages[0]
    assert received.message.data == b"hello from python"
    assert received.message.attributes["origin"] == "compat-test"
    assert received.message.attributes["kind"] == "greeting"
    assert received.message.message_id == message_id

    subscriber.acknowledge(
        request={
            "subscription": subscription_path,
            "ack_ids": [received.ack_id],
        }
    )

    # Re-pull: the acked message must not come back.
    again = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": 10},
        timeout=10,
    )
    assert len(again.received_messages) == 0


def test_modify_ack_deadline_zero_causes_redelivery(
    publisher: pubsub_v1.PublisherClient,
    subscriber: pubsub_v1.SubscriberClient,
    topic_path: str,
    subscription_path: str,
) -> None:
    future = publisher.publish(topic_path, b"nack me")
    future.result(timeout=10)

    first = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": 10},
        timeout=10,
    )
    assert len(first.received_messages) == 1
    ack_id = first.received_messages[0].ack_id

    subscriber.modify_ack_deadline(
        request={
            "subscription": subscription_path,
            "ack_ids": [ack_id],
            "ack_deadline_seconds": 0,
        }
    )

    redelivered = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": 10},
        timeout=10,
    )
    assert len(redelivered.received_messages) == 1
    assert redelivered.received_messages[0].message.data == b"nack me"


def test_create_topic_duplicate_is_already_exists(
    publisher: pubsub_v1.PublisherClient, topic_path: str
) -> None:
    with pytest.raises(gax_exceptions.AlreadyExists):
        publisher.create_topic(request={"name": topic_path})


def test_get_topic_missing_is_not_found(
    publisher: pubsub_v1.PublisherClient,
) -> None:
    missing = publisher.topic_path(PROJECT_ID, unique_name("missing"))
    with pytest.raises(gax_exceptions.NotFound):
        publisher.get_topic(request={"topic": missing})


def test_delete_subscription_then_pull_is_not_found(
    subscriber: pubsub_v1.SubscriberClient,
    subscription_path: str,
) -> None:
    subscriber.delete_subscription(request={"subscription": subscription_path})
    with pytest.raises(gax_exceptions.NotFound):
        subscriber.pull(
            request={"subscription": subscription_path, "max_messages": 10},
            timeout=10,
        )


# The high-level `subscriber.subscribe()` API (StreamingPull under the
# hood) is intentionally NOT exercised here: `open-pubusb` returns UNIMPLEMENTED
# for StreamingPull until User Story 4 lands. T028's scope is unary
# Pull/Acknowledge/ModifyAckDeadline only (spec.md User Story 1); a
# streaming compat test belongs with the US4 implementation instead of
# being added here and left permanently failing.
