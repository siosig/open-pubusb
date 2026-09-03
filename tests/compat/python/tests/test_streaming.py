"""Compatibility test (T066, User Story 4): proves the official
`google-cloud-pubsub` Python client's high-level `subscriber.subscribe()`
streaming API (`StreamingPull` under the hood) works against `open-pubusb`
unmodified — ack, nack/redelivery, and `flow_control`.

Requires `open-pubusb` to be running with `PUBSUB_EMULATOR_HOST` pointed at it
(see `test_roundtrip.py`'s module docstring). Every test is skipped when
`PUBSUB_EMULATOR_HOST` is unset.
"""

import os
import threading
import uuid

import pytest
from google.api_core import exceptions as gax_exceptions
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import FlowControl

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


@pytest.fixture
def subscription_path(subscriber: pubsub_v1.SubscriberClient, topic_path: str):
    path = subscriber.subscription_path(PROJECT_ID, unique_name("sub"))
    subscriber.create_subscription(
        request={"name": path, "topic": topic_path, "ack_deadline_seconds": 10}
    )
    yield path
    try:
        subscriber.delete_subscription(request={"subscription": path})
    except gax_exceptions.GoogleAPICallError:
        pass


def test_subscribe_receives_and_acks_via_callback(
    publisher: pubsub_v1.PublisherClient,
    subscriber: pubsub_v1.SubscriberClient,
    topic_path: str,
    subscription_path: str,
) -> None:
    received: list[bytes] = []
    done = threading.Event()

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        received.append(message.data)
        message.ack()
        done.set()

    future = subscriber.subscribe(
        subscription_path, callback=callback, flow_control=FlowControl(max_messages=10)
    )
    try:
        publisher.publish(topic_path, b"hello via streaming").result(timeout=10)
        assert done.wait(timeout=15), "message was not received via StreamingPull"
        assert received == [b"hello via streaming"]
    finally:
        future.cancel()
        future.result(timeout=10)


def test_nack_causes_redelivery(
    publisher: pubsub_v1.PublisherClient,
    subscriber: pubsub_v1.SubscriberClient,
    topic_path: str,
    subscription_path: str,
) -> None:
    attempts = 0
    lock = threading.Lock()
    delivered_twice = threading.Event()

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        nonlocal attempts
        with lock:
            attempts += 1
            n = attempts
        if n == 1:
            message.nack()
        else:
            message.ack()
            delivered_twice.set()

    future = subscriber.subscribe(
        subscription_path, callback=callback, flow_control=FlowControl(max_messages=10)
    )
    try:
        publisher.publish(topic_path, b"nack-then-ack").result(timeout=10)
        assert delivered_twice.wait(
            timeout=15
        ), "message was not redelivered after nack() within 15s"
        with lock:
            assert attempts == 2
    finally:
        future.cancel()
        future.result(timeout=10)
