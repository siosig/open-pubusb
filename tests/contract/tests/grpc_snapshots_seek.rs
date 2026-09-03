#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Contract tests for the Snapshot RPCs
//! (`CreateSnapshot`/`GetSnapshot`/`ListSnapshots`/`UpdateSnapshot`/
//! `DeleteSnapshot`) and `Seek`, plus `ListTopicSnapshots`, against a real
//! `open-pubusb serve --ephemeral` process — the gRPC-layer counterpart to
//! `tests/integration/tests/snapshot_seek.rs`, which exercises the
//! same behavior one layer down (`PubSubService` directly). Also pins down
//! that the REST equivalents of these endpoints stay `501` (Seek and
//! Snapshot RPCs are gRPC-only; `rest_v1.rs`'s `unimplemented_custom_method_returns_501` already
//! covers `:seek` — this file adds the snapshot-resource REST paths).

use std::collections::HashMap;

use open_pubusb_contract_tests::harness::OpenPubusbHarness;
use open_pubusb_proto::pubsub::v1::{
    seek_request, CreateSnapshotRequest, DeleteSnapshotRequest, GetSnapshotRequest,
    GetSubscriptionRequest, ListSnapshotsRequest, ListTopicSnapshotsRequest, PublishRequest,
    PubsubMessage, PullRequest, SeekRequest, Snapshot, Subscription, Topic, UpdateSnapshotRequest,
};
use tonic::Code;

const PROJECT: &str = "snap-contract";

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
        ..Default::default()
    }
}

async fn setup(harness: &OpenPubusbHarness, suffix: &str) -> (String, String) {
    let mut publisher = harness.publisher_client();
    let mut subscriber = harness.subscriber_client();
    let topic_name = format!("projects/{PROJECT}/topics/topic-{suffix}");
    let sub_name = format!("projects/{PROJECT}/subscriptions/sub-{suffix}");
    publisher
        .create_topic(topic(&topic_name))
        .await
        .expect("CreateTopic should succeed");
    subscriber
        .create_subscription(subscription(&sub_name, &topic_name))
        .await
        .expect("CreateSubscription should succeed");
    (topic_name, sub_name)
}

#[tokio::test]
async fn create_get_list_update_delete_snapshot_round_trip() {
    let harness = OpenPubusbHarness::start().await;
    let (topic_name, sub_name) = setup(&harness, "crud").await;
    let mut subscriber = harness.subscriber_client();

    let snap_name = format!("projects/{PROJECT}/snapshots/snap-crud");
    let mut labels = HashMap::new();
    labels.insert("env".to_string(), "test".to_string());
    let created = subscriber
        .create_snapshot(CreateSnapshotRequest {
            name: snap_name.clone(),
            subscription: sub_name.clone(),
            labels: labels.clone(),
            tags: HashMap::new(),
        })
        .await
        .expect("CreateSnapshot should succeed")
        .into_inner();
    assert_eq!(created.name, snap_name);
    assert_eq!(created.topic, topic_name);
    assert_eq!(created.labels, labels);
    assert!(created.expire_time.is_some());

    let fetched = subscriber
        .get_snapshot(GetSnapshotRequest {
            snapshot: snap_name.clone(),
        })
        .await
        .expect("GetSnapshot should succeed")
        .into_inner();
    assert_eq!(fetched.name, snap_name);

    let listed = subscriber
        .list_snapshots(ListSnapshotsRequest {
            project: format!("projects/{PROJECT}"),
            page_size: 10,
            page_token: String::new(),
        })
        .await
        .expect("ListSnapshots should succeed")
        .into_inner();
    assert!(listed.snapshots.iter().any(|s| s.name == snap_name));

    let mut new_labels = HashMap::new();
    new_labels.insert("env".to_string(), "prod".to_string());
    let updated = subscriber
        .update_snapshot(UpdateSnapshotRequest {
            snapshot: Some(Snapshot {
                name: snap_name.clone(),
                labels: new_labels.clone(),
                ..Default::default()
            }),
            update_mask: Some(pbjson_types::FieldMask {
                paths: vec!["labels".to_string()],
            }),
        })
        .await
        .expect("UpdateSnapshot should succeed")
        .into_inner();
    assert_eq!(updated.labels, new_labels);

    subscriber
        .delete_snapshot(DeleteSnapshotRequest {
            snapshot: snap_name.clone(),
        })
        .await
        .expect("DeleteSnapshot should succeed");

    let err = subscriber
        .get_snapshot(GetSnapshotRequest {
            snapshot: snap_name,
        })
        .await
        .expect_err("GetSnapshot after delete should fail");
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn get_snapshot_missing_is_not_found() {
    let harness = OpenPubusbHarness::start().await;
    let mut subscriber = harness.subscriber_client();
    let err = subscriber
        .get_snapshot(GetSnapshotRequest {
            snapshot: format!("projects/{PROJECT}/snapshots/does-not-exist"),
        })
        .await
        .expect_err("GetSnapshot on a missing snapshot should fail");
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn create_snapshot_duplicate_name_is_already_exists() {
    let harness = OpenPubusbHarness::start().await;
    let (_topic_name, sub_name) = setup(&harness, "dup").await;
    let mut subscriber = harness.subscriber_client();
    let snap_name = format!("projects/{PROJECT}/snapshots/snap-dup");

    subscriber
        .create_snapshot(CreateSnapshotRequest {
            name: snap_name.clone(),
            subscription: sub_name.clone(),
            labels: HashMap::new(),
            tags: HashMap::new(),
        })
        .await
        .expect("first CreateSnapshot should succeed");

    let err = subscriber
        .create_snapshot(CreateSnapshotRequest {
            name: snap_name,
            subscription: sub_name,
            labels: HashMap::new(),
            tags: HashMap::new(),
        })
        .await
        .expect_err("duplicate CreateSnapshot should fail");
    assert_eq!(err.code(), Code::AlreadyExists);
}

#[tokio::test]
async fn list_topic_snapshots_returns_snapshots_for_that_topic() {
    let harness = OpenPubusbHarness::start().await;
    let (topic_name, sub_name) = setup(&harness, "topicsnaps").await;
    let mut subscriber = harness.subscriber_client();
    let mut publisher = harness.publisher_client();

    let snap_name = format!("projects/{PROJECT}/snapshots/snap-topicsnaps");
    subscriber
        .create_snapshot(CreateSnapshotRequest {
            name: snap_name.clone(),
            subscription: sub_name,
            labels: HashMap::new(),
            tags: HashMap::new(),
        })
        .await
        .expect("CreateSnapshot should succeed");

    let listed = publisher
        .list_topic_snapshots(ListTopicSnapshotsRequest {
            topic: topic_name,
            page_size: 10,
            page_token: String::new(),
        })
        .await
        .expect("ListTopicSnapshots should succeed")
        .into_inner();
    assert_eq!(listed.snapshots, vec![snap_name]);
}

#[tokio::test]
async fn seek_to_snapshot_unacks_a_previously_acked_message() {
    let harness = OpenPubusbHarness::start().await;
    let (topic_name, sub_name) = setup(&harness, "seeksnap").await;
    let mut publisher = harness.publisher_client();
    let mut subscriber = harness.subscriber_client();

    publisher
        .publish(PublishRequest {
            topic: topic_name,
            messages: vec![PubsubMessage {
                data: b"seek-me".to_vec(),
                ..Default::default()
            }],
        })
        .await
        .expect("Publish should succeed");

    let snap_name = format!("projects/{PROJECT}/snapshots/snap-seeksnap");
    subscriber
        .create_snapshot(CreateSnapshotRequest {
            name: snap_name.clone(),
            subscription: sub_name.clone(),
            labels: HashMap::new(),
            tags: HashMap::new(),
        })
        .await
        .expect("CreateSnapshot should succeed");

    let pulled = subscriber
        .pull(PullRequest {
            subscription: sub_name.clone(),
            max_messages: 10,
            ..Default::default()
        })
        .await
        .expect("Pull should succeed")
        .into_inner();
    assert_eq!(pulled.received_messages.len(), 1);
    subscriber
        .acknowledge(open_pubusb_proto::pubsub::v1::AcknowledgeRequest {
            subscription: sub_name.clone(),
            ack_ids: vec![pulled.received_messages[0].ack_id.clone()],
        })
        .await
        .expect("Acknowledge should succeed");

    subscriber
        .seek(SeekRequest {
            subscription: sub_name.clone(),
            target: Some(seek_request::Target::Snapshot(snap_name)),
        })
        .await
        .expect("Seek to snapshot should succeed");

    let redelivered = subscriber
        .pull(PullRequest {
            subscription: sub_name,
            max_messages: 10,
            ..Default::default()
        })
        .await
        .expect("Pull after seek should succeed")
        .into_inner();
    assert_eq!(redelivered.received_messages.len(), 1);
}

#[tokio::test]
async fn seek_to_time_in_the_future_leaves_nothing_deliverable() {
    let harness = OpenPubusbHarness::start().await;
    let (topic_name, sub_name) = setup(&harness, "seektime").await;
    let mut publisher = harness.publisher_client();
    let mut subscriber = harness.subscriber_client();

    publisher
        .publish(PublishRequest {
            topic: topic_name,
            messages: vec![PubsubMessage {
                data: b"before-cutoff".to_vec(),
                ..Default::default()
            }],
        })
        .await
        .expect("Publish should succeed");

    let far_future = pbjson_types::Timestamp {
        seconds: 4_102_444_800, // 2100-01-01T00:00:00Z
        nanos: 0,
    };
    subscriber
        .seek(SeekRequest {
            subscription: sub_name.clone(),
            target: Some(seek_request::Target::Time(far_future)),
        })
        .await
        .expect("Seek to a future time should succeed");

    let pulled = subscriber
        .pull(PullRequest {
            subscription: sub_name,
            max_messages: 10,
            ..Default::default()
        })
        .await
        .expect("Pull should succeed")
        .into_inner();
    assert!(pulled.received_messages.is_empty());
}

#[tokio::test]
async fn seek_without_a_target_is_invalid_argument() {
    let harness = OpenPubusbHarness::start().await;
    let (_topic_name, sub_name) = setup(&harness, "seeknotarget").await;
    let mut subscriber = harness.subscriber_client();

    let err = subscriber
        .seek(SeekRequest {
            subscription: sub_name,
            target: None,
        })
        .await
        .expect_err("Seek without a target should fail");
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn seek_to_snapshot_of_a_different_topic_is_invalid_argument() {
    let harness = OpenPubusbHarness::start().await;
    let (_t1, sub1) = setup(&harness, "seekcross1").await;
    let (_t2, sub2) = setup(&harness, "seekcross2").await;
    let mut subscriber = harness.subscriber_client();

    let snap_name = format!("projects/{PROJECT}/snapshots/snap-seekcross");
    subscriber
        .create_snapshot(CreateSnapshotRequest {
            name: snap_name.clone(),
            subscription: sub1,
            labels: HashMap::new(),
            tags: HashMap::new(),
        })
        .await
        .expect("CreateSnapshot should succeed");

    let err = subscriber
        .seek(SeekRequest {
            subscription: sub2,
            target: Some(seek_request::Target::Snapshot(snap_name)),
        })
        .await
        .expect_err("Seek to a snapshot of a different topic should fail");
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn get_subscription_is_unaffected_by_seek_wiring() {
    // Smoke check that wiring Seek/Snapshots didn't disturb an
    // already-passing unary method sharing this same `Subscriber` impl.
    let harness = OpenPubusbHarness::start().await;
    let (_topic_name, sub_name) = setup(&harness, "smoke").await;
    let mut subscriber = harness.subscriber_client();
    let fetched = subscriber
        .get_subscription(GetSubscriptionRequest {
            subscription: sub_name.clone(),
        })
        .await
        .expect("GetSubscription should still succeed")
        .into_inner();
    assert_eq!(fetched.name, sub_name);
}

#[tokio::test]
async fn rest_snapshot_endpoints_are_still_501() {
    let harness = OpenPubusbHarness::start().await;
    let base = harness.rest_base_url();
    let client = reqwest::Client::new();

    let get_resp = client
        .get(format!(
            "{base}/v1/projects/{PROJECT}/snapshots/does-not-matter"
        ))
        .send()
        .await
        .expect("GET snapshot request failed");
    assert_eq!(get_resp.status().as_u16(), 501);

    let create_resp = client
        .put(format!(
            "{base}/v1/projects/{PROJECT}/snapshots/does-not-matter"
        ))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("PUT snapshot request failed");
    assert_eq!(create_resp.status().as_u16(), 501);
}
