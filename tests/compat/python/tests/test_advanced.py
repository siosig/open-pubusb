"""Compatibility test (T085, User Story 5): proves the official
`google-cloud-pubsub` Python client's advanced features work against
`open-pubusb` unmodified — filters, dead-letter policy, and Snapshot/Seek.

Ordering keys are deliberately **not** covered here: `delivery/ordering.rs`
(tasks.md T080) is not implemented yet (a documented, deliberate
sequencing decision — dead-letter/retry-backoff/Snapshot-Seek shipped
first since ordering needs a real redesign of the cursor model to support
un-acking already-acked messages within a nacked lane). This file adds
ordering coverage once T080 lands, rather than shipping a permanently
`#[ignore]`d/`xfail`ed placeholder for it now.

Requires `open-pubusb` to be running with `PUBSUB_EMULATOR_HOST` pointed at it —
see `tests/compat/python/tests/test_roundtrip.py`'s module doc comment for
the exact invocation; every test here is skipped (not failed) when
`PUBSUB_EMULATOR_HOST` is unset.
"""

import os
import time
import uuid

import pytest
from google.api_core import exceptions as gax_exceptions
from google.cloud import pubsub_v1
from google.protobuf import timestamp_pb2

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


def make_subscription(subscriber, topic_path, **kwargs):
    path = subscriber.subscription_path(PROJECT_ID, unique_name("sub"))
    request = {"name": path, "topic": topic_path, **kwargs}
    subscriber.create_subscription(request=request)
    return path


def test_filter_only_delivers_matching_messages(
    publisher: pubsub_v1.PublisherClient,
    subscriber: pubsub_v1.SubscriberClient,
    topic_path: str,
) -> None:
    sub_path = make_subscription(
        subscriber, topic_path, filter='attributes.kind = "keep"'
    )

    publisher.publish(topic_path, b"drop-me", kind="drop").result(timeout=10)
    publisher.publish(topic_path, b"keep-me", kind="keep").result(timeout=10)

    response = subscriber.pull(
        request={"subscription": sub_path, "max_messages": 10}, timeout=10
    )
    assert len(response.received_messages) == 1
    assert response.received_messages[0].message.data == b"keep-me"
    subscriber.acknowledge(
        request={
            "subscription": sub_path,
            "ack_ids": [m.ack_id for m in response.received_messages],
        }
    )

    # The filtered-out message never becomes deliverable either.
    time.sleep(0.5)
    response2 = subscriber.pull(
        request={"subscription": sub_path, "max_messages": 10}, timeout=5
    )
    assert len(response2.received_messages) == 0


def test_dead_letter_policy_forwards_after_max_delivery_attempts(
    publisher: pubsub_v1.PublisherClient,
    subscriber: pubsub_v1.SubscriberClient,
    topic_path: str,
) -> None:
    dlq_topic_path = publisher.topic_path(PROJECT_ID, unique_name("dlq-topic"))
    publisher.create_topic(request={"name": dlq_topic_path})
    dlq_sub_path = make_subscription(subscriber, dlq_topic_path)

    sub_path = make_subscription(
        subscriber,
        topic_path,
        ack_deadline_seconds=10,
        dead_letter_policy={
            "dead_letter_topic": dlq_topic_path,
            "max_delivery_attempts": 5,
        },
    )

    publisher.publish(topic_path, b"never-acked").result(timeout=10)

    # Nack every delivery (immediate redelivery via ModifyAckDeadline(0))
    # until it's dead-lettered instead of redelivered a 6th time.
    for _ in range(6):
        response = subscriber.pull(
            request={"subscription": sub_path, "max_messages": 10}, timeout=10
        )
        if not response.received_messages:
            break
        subscriber.modify_ack_deadline(
            request={
                "subscription": sub_path,
                "ack_ids": [m.ack_id for m in response.received_messages],
                "ack_deadline_seconds": 0,
            }
        )

    dlq_response = subscriber.pull(
        request={"subscription": dlq_sub_path, "max_messages": 10}, timeout=10
    )
    assert len(dlq_response.received_messages) == 1
    assert dlq_response.received_messages[0].message.data == b"never-acked"
    attrs = dlq_response.received_messages[0].message.attributes
    assert "CloudPubSubDeadLetterSourceDeliveryCount" in attrs
    assert attrs["CloudPubSubDeadLetterSourceSubscription"] == sub_path


def test_snapshot_and_seek_restores_acked_backlog(
    publisher: pubsub_v1.PublisherClient,
    subscriber: pubsub_v1.SubscriberClient,
    topic_path: str,
) -> None:
    sub_path = make_subscription(subscriber, topic_path)
    publisher.publish(topic_path, b"replay-me").result(timeout=10)

    snapshot_path = subscriber.snapshot_path(PROJECT_ID, unique_name("snap"))
    snapshot = subscriber.create_snapshot(
        request={"name": snapshot_path, "subscription": sub_path}
    )
    assert snapshot.topic == topic_path

    response = subscriber.pull(
        request={"subscription": sub_path, "max_messages": 10}, timeout=10
    )
    assert len(response.received_messages) == 1
    subscriber.acknowledge(
        request={
            "subscription": sub_path,
            "ack_ids": [m.ack_id for m in response.received_messages],
        }
    )
    assert (
        len(
            subscriber.pull(
                request={"subscription": sub_path, "max_messages": 10}, timeout=5
            ).received_messages
        )
        == 0
    )

    subscriber.seek(request={"subscription": sub_path, "snapshot": snapshot_path})

    replayed = subscriber.pull(
        request={"subscription": sub_path, "max_messages": 10}, timeout=10
    )
    assert len(replayed.received_messages) == 1
    assert replayed.received_messages[0].message.data == b"replay-me"
    subscriber.acknowledge(
        request={
            "subscription": sub_path,
            "ack_ids": [m.ack_id for m in replayed.received_messages],
        }
    )

    subscriber.delete_snapshot(request={"snapshot": snapshot_path})


def test_seek_to_time_marks_earlier_messages_acked(
    publisher: pubsub_v1.PublisherClient,
    subscriber: pubsub_v1.SubscriberClient,
    topic_path: str,
) -> None:
    sub_path = make_subscription(subscriber, topic_path)
    publisher.publish(topic_path, b"before-cutoff").result(timeout=10)

    time.sleep(0.2)
    cutoff = timestamp_pb2.Timestamp()
    cutoff.GetCurrentTime()

    subscriber.seek(request={"subscription": sub_path, "time": cutoff})

    response = subscriber.pull(
        request={"subscription": sub_path, "max_messages": 10}, timeout=5
    )
    assert len(response.received_messages) == 0
