#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Contract tests for `google.pubsub.v1.Subscriber/StreamingPull`, against
//! a real `open-pubusb serve --ephemeral` process.

use std::time::Duration;

use open_pubusb_contract_tests::harness::OpenPubusbHarness;
use open_pubusb_proto::pubsub::v1::{
    PublishRequest, PubsubMessage, PullRequest, StreamingPullRequest, Subscription, Topic,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;

fn topic(name: &str) -> Topic {
    Topic {
        name: name.to_string(),
        ..Default::default()
    }
}

fn subscription(name: &str, topic_name: &str) -> Subscription {
    Subscription {
        name: name.to_string(),
        topic: topic_name.to_string(),
        ack_deadline_seconds: 10,
        ..Default::default()
    }
}

fn first_request(subscription: &str, deadline_secs: i32) -> StreamingPullRequest {
    StreamingPullRequest {
        subscription: subscription.to_string(),
        stream_ack_deadline_seconds: deadline_secs,
        ..Default::default()
    }
}

/// Opens a client-to-server request channel and starts a `StreamingPull`
/// call with `first` as the first request already sent. Returns the
/// request sender (so the test can send further requests) and the
/// response stream.
async fn open_stream(
    harness: &OpenPubusbHarness,
    first: StreamingPullRequest,
) -> (
    mpsc::Sender<StreamingPullRequest>,
    tonic::Streaming<open_pubusb_proto::pubsub::v1::StreamingPullResponse>,
) {
    let (tx, rx) = mpsc::channel(16);
    tx.send(first).await.expect("failed to send first request");
    let mut subscriber = harness.subscriber_client();
    let response = subscriber
        .streaming_pull(ReceiverStream::new(rx))
        .await
        .expect("StreamingPull should accept a valid first request");
    (tx, response.into_inner())
}

#[tokio::test]
async fn missing_subscription_on_first_request_is_invalid_argument() {
    let harness = OpenPubusbHarness::start().await;
    let (tx, rx) = mpsc::channel(16);
    tx.send(StreamingPullRequest {
        stream_ack_deadline_seconds: 10,
        ..Default::default()
    })
    .await
    .expect("send should succeed");
    let mut subscriber = harness.subscriber_client();
    let err = subscriber
        .streaming_pull(ReceiverStream::new(rx))
        .await
        .expect_err("empty `subscription` on the first request must be rejected");
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn deadline_out_of_range_on_first_request_is_invalid_argument() {
    let harness = OpenPubusbHarness::start().await;
    let topic_name = "projects/p/topics/sp-deadline-src";
    let sub_name = "projects/p/subscriptions/sp-deadline";
    let mut publisher = harness.publisher_client();
    publisher
        .create_topic(topic(topic_name))
        .await
        .expect("CreateTopic should succeed");
    harness
        .subscriber_client()
        .create_subscription(subscription(sub_name, topic_name))
        .await
        .expect("CreateSubscription should succeed");

    let (tx, rx) = mpsc::channel(16);
    // 600s is the documented maximum; 601 must be rejected.
    tx.send(first_request(sub_name, 601))
        .await
        .expect("send should succeed");
    let mut subscriber = harness.subscriber_client();
    let err = subscriber
        .streaming_pull(ReceiverStream::new(rx))
        .await
        .expect_err("stream_ack_deadline_seconds out of [10, 600] must be rejected");
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn first_response_carries_subscription_properties_then_messages_flow() {
    let harness = OpenPubusbHarness::start().await;
    let topic_name = "projects/p/topics/sp-flow-src";
    let sub_name = "projects/p/subscriptions/sp-flow";
    let mut publisher = harness.publisher_client();
    publisher
        .create_topic(topic(topic_name))
        .await
        .expect("CreateTopic should succeed");
    let mut sub_req = subscription(sub_name, topic_name);
    sub_req.enable_message_ordering = false;
    harness
        .subscriber_client()
        .create_subscription(sub_req)
        .await
        .expect("CreateSubscription should succeed");

    let (_tx, mut responses) = open_stream(&harness, first_request(sub_name, 10)).await;

    let first = responses
        .message()
        .await
        .expect("reading the first response should not error")
        .expect("stream should not end before the first response");
    let props = first
        .subscription_properties
        .expect("first response must carry subscription_properties");
    assert!(!props.exactly_once_delivery_enabled);
    assert!(!props.message_ordering_enabled);
    assert!(first.received_messages.is_empty());

    publisher
        .publish(PublishRequest {
            topic: topic_name.to_string(),
            messages: vec![PubsubMessage {
                data: b"hello-stream".to_vec(),
                ..Default::default()
            }],
        })
        .await
        .expect("Publish should succeed");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "did not receive the published message via StreamingPull within 5s"
        );
        let resp = match tokio::time::timeout(Duration::from_secs(1), responses.message()).await {
            Ok(Ok(Some(r))) => Some(r),
            _ => None,
        };
        if let Some(resp) = resp {
            if let Some(m) = resp.received_messages.first() {
                assert_eq!(m.message.as_ref().unwrap().data, b"hello-stream");
                break;
            }
        }
    }
}

#[tokio::test]
async fn empty_keepalive_request_is_accepted() {
    let harness = OpenPubusbHarness::start().await;
    let topic_name = "projects/p/topics/sp-keepalive-src";
    let sub_name = "projects/p/subscriptions/sp-keepalive";
    harness
        .publisher_client()
        .create_topic(topic(topic_name))
        .await
        .expect("CreateTopic should succeed");
    harness
        .subscriber_client()
        .create_subscription(subscription(sub_name, topic_name))
        .await
        .expect("CreateSubscription should succeed");

    let (tx, mut responses) = open_stream(&harness, first_request(sub_name, 10)).await;
    // Consume the first (properties-only) response.
    responses.message().await.unwrap().unwrap();

    // An empty request (no subscription, no ack_ids, no modify-deadline
    // pairs) is a valid keepalive and must not abort the stream.
    tx.send(StreamingPullRequest::default())
        .await
        .expect("keepalive send should succeed");

    // Prove the stream is still alive: publish and confirm delivery still
    // works after the keepalive.
    harness
        .publisher_client()
        .publish(PublishRequest {
            topic: topic_name.to_string(),
            messages: vec![PubsubMessage {
                data: b"after-keepalive".to_vec(),
                ..Default::default()
            }],
        })
        .await
        .expect("Publish should succeed");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "stream did not survive the keepalive request"
        );
        if let Ok(Ok(Some(resp))) =
            tokio::time::timeout(Duration::from_secs(1), responses.message()).await
        {
            if !resp.received_messages.is_empty() {
                break;
            }
        }
    }
}

#[tokio::test]
async fn acks_via_stream_prevent_redelivery() {
    let harness = OpenPubusbHarness::start().await;
    let topic_name = "projects/p/topics/sp-ack-src";
    let sub_name = "projects/p/subscriptions/sp-ack";
    harness
        .publisher_client()
        .create_topic(topic(topic_name))
        .await
        .expect("CreateTopic should succeed");
    harness
        .subscriber_client()
        .create_subscription(subscription(sub_name, topic_name))
        .await
        .expect("CreateSubscription should succeed");

    let (tx, mut responses) = open_stream(&harness, first_request(sub_name, 10)).await;
    responses.message().await.unwrap().unwrap(); // properties-only first response

    harness
        .publisher_client()
        .publish(PublishRequest {
            topic: topic_name.to_string(),
            messages: vec![PubsubMessage {
                data: b"ack-me".to_vec(),
                ..Default::default()
            }],
        })
        .await
        .expect("Publish should succeed");

    let ack_id = loop {
        let resp = responses.message().await.unwrap().unwrap();
        if let Some(m) = resp.received_messages.first() {
            break m.ack_id.clone();
        }
    };

    tx.send(StreamingPullRequest {
        ack_ids: vec![ack_id],
        ..Default::default()
    })
    .await
    .expect("ack send should succeed");

    // Give the ack a moment to apply, then confirm a unary Pull sees
    // nothing outstanding (the message was acked, not just leased).
    tokio::time::sleep(Duration::from_millis(200)).await;
    let pulled = harness
        .subscriber_client()
        .pull(PullRequest {
            subscription: sub_name.to_string(),
            max_messages: 10,
            ..Default::default()
        })
        .await
        .expect("Pull should succeed")
        .into_inner();
    assert!(
        pulled.received_messages.is_empty(),
        "acked message must not be redelivered, got: {:?}",
        pulled.received_messages
    );
}

#[tokio::test]
async fn stream_lifetime_elapsing_ends_the_stream_with_unavailable() {
    let harness = OpenPubusbHarness::start_with_env(&[(
        "OPEN_PUBUSB__DELIVERY__STREAMING_PULL_MAX_LIFETIME_SECS",
        "1",
    )])
    .await;
    let topic_name = "projects/p/topics/sp-lifetime-src";
    let sub_name = "projects/p/subscriptions/sp-lifetime";
    harness
        .publisher_client()
        .create_topic(topic(topic_name))
        .await
        .expect("CreateTopic should succeed");
    harness
        .subscriber_client()
        .create_subscription(subscription(sub_name, topic_name))
        .await
        .expect("CreateSubscription should succeed");

    let (_tx, mut responses) = open_stream(&harness, first_request(sub_name, 10)).await;
    responses.message().await.unwrap().unwrap(); // properties-only first response

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "stream did not end with UNAVAILABLE within 5s of its 1s lifetime"
        );
        match responses.message().await {
            Err(status) => {
                assert_eq!(status.code(), Code::Unavailable);
                break;
            }
            Ok(None) => panic!("stream ended without an UNAVAILABLE status"),
            Ok(Some(_)) => {} // ignore any message that raced the timer
        }
    }
}

#[tokio::test]
async fn client_disconnect_makes_the_lease_immediately_available_again() {
    let harness = OpenPubusbHarness::start().await;
    let topic_name = "projects/p/topics/sp-disconnect-src";
    let sub_name = "projects/p/subscriptions/sp-disconnect";
    harness
        .publisher_client()
        .create_topic(topic(topic_name))
        .await
        .expect("CreateTopic should succeed");
    harness
        .subscriber_client()
        .create_subscription(subscription(sub_name, topic_name))
        .await
        .expect("CreateSubscription should succeed");

    harness
        .publisher_client()
        .publish(PublishRequest {
            topic: topic_name.to_string(),
            messages: vec![PubsubMessage {
                data: b"disconnect-me".to_vec(),
                ..Default::default()
            }],
        })
        .await
        .expect("Publish should succeed");

    {
        let (tx, mut responses) = open_stream(&harness, first_request(sub_name, 10)).await;
        responses.message().await.unwrap().unwrap(); // properties-only first response
        loop {
            let resp = responses.message().await.unwrap().unwrap();
            if !resp.received_messages.is_empty() {
                break;
            }
        }
        drop(tx);
        drop(responses);
    }

    // Give the server a moment to notice the disconnect and release the
    // lease, then confirm a unary Pull can see the message again
    // immediately (not stuck waiting out the original 10s ack deadline).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "message was not redelivered promptly after the stream disconnected"
        );
        let pulled = harness
            .subscriber_client()
            .pull(PullRequest {
                subscription: sub_name.to_string(),
                max_messages: 10,
                ..Default::default()
            })
            .await
            .expect("Pull should succeed")
            .into_inner();
        if !pulled.received_messages.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
