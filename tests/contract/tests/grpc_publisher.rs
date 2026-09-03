#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Contract tests for `google.pubsub.v1.Publisher` against a real
//! `open-pubusb serve --ephemeral` process.
//!
//! Any test marked `#[ignore]` here is written so that running
//! `cargo test -- --ignored` exercises the contract directly.

use open_pubusb_contract_tests::harness::OpenPubusbHarness;
use open_pubusb_proto::pubsub::v1::{
    DeleteTopicRequest, DetachSubscriptionRequest, GetTopicRequest, ListTopicSnapshotsRequest,
    ListTopicSubscriptionsRequest, ListTopicsRequest, PublishRequest, PubsubMessage,
    SchemaSettings, Subscription, Topic,
};
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
        ..Default::default()
    }
}

#[tokio::test]
async fn create_topic_then_get_topic_round_trip() {
    let harness = OpenPubusbHarness::start().await;
    let mut client = harness.publisher_client();
    let name = "projects/contract-test/topics/create-get";

    let created = client
        .create_topic(topic(name))
        .await
        .expect("CreateTopic should succeed")
        .into_inner();
    assert_eq!(created.name, name);

    let fetched = client
        .get_topic(GetTopicRequest {
            topic: name.to_string(),
        })
        .await
        .expect("GetTopic should succeed")
        .into_inner();
    assert_eq!(fetched.name, name);
}

#[tokio::test]
async fn create_topic_duplicate_is_already_exists() {
    let harness = OpenPubusbHarness::start().await;
    let mut client = harness.publisher_client();
    let name = "projects/contract-test/topics/duplicate";

    client
        .create_topic(topic(name))
        .await
        .expect("first CreateTopic should succeed");

    let err = client
        .create_topic(topic(name))
        .await
        .expect_err("duplicate CreateTopic should fail");
    assert_eq!(err.code(), Code::AlreadyExists);
}

#[tokio::test]
async fn get_topic_nonexistent_is_not_found() {
    let harness = OpenPubusbHarness::start().await;
    let mut client = harness.publisher_client();

    let err = client
        .get_topic(GetTopicRequest {
            topic: "projects/contract-test/topics/does-not-exist".to_string(),
        })
        .await
        .expect_err("GetTopic on a missing topic should fail");
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn create_topic_with_schema_settings_is_invalid_argument() {
    let harness = OpenPubusbHarness::start().await;
    let mut client = harness.publisher_client();

    let mut request = topic("projects/contract-test/topics/with-schema");
    request.schema_settings = Some(SchemaSettings {
        schema: "projects/contract-test/schemas/some-schema".to_string(),
        ..Default::default()
    });

    let err = client
        .create_topic(request)
        .await
        .expect_err("CreateTopic with schema_settings should fail");
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn list_topics_returns_created_topics() {
    let harness = OpenPubusbHarness::start().await;
    let mut client = harness.publisher_client();
    let name_a = "projects/contract-test/topics/list-a";
    let name_b = "projects/contract-test/topics/list-b";

    client
        .create_topic(topic(name_a))
        .await
        .expect("CreateTopic a should succeed");
    client
        .create_topic(topic(name_b))
        .await
        .expect("CreateTopic b should succeed");

    let listed = client
        .list_topics(ListTopicsRequest {
            project: "projects/contract-test".to_string(),
            ..Default::default()
        })
        .await
        .expect("ListTopics should succeed")
        .into_inner();

    let names: Vec<_> = listed.topics.iter().map(|t| t.name.as_str()).collect();
    assert!(names.contains(&name_a));
    assert!(names.contains(&name_b));
}

#[tokio::test]
async fn delete_topic_then_get_topic_is_not_found() {
    let harness = OpenPubusbHarness::start().await;
    let mut client = harness.publisher_client();
    let name = "projects/contract-test/topics/to-delete";

    client
        .create_topic(topic(name))
        .await
        .expect("CreateTopic should succeed");
    client
        .delete_topic(DeleteTopicRequest {
            topic: name.to_string(),
        })
        .await
        .expect("DeleteTopic should succeed");

    let err = client
        .get_topic(GetTopicRequest {
            topic: name.to_string(),
        })
        .await
        .expect_err("GetTopic after DeleteTopic should fail");
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn publish_to_nonexistent_topic_is_not_found() {
    let harness = OpenPubusbHarness::start().await;
    let mut client = harness.publisher_client();

    let err = client
        .publish(PublishRequest {
            topic: "projects/contract-test/topics/does-not-exist".to_string(),
            messages: vec![PubsubMessage {
                data: b"hello".to_vec(),
                ..Default::default()
            }],
        })
        .await
        .expect_err("Publish to a missing topic should fail");
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn publish_returns_one_message_id_per_message() {
    let harness = OpenPubusbHarness::start().await;
    let mut client = harness.publisher_client();
    let name = "projects/contract-test/topics/publish";

    client
        .create_topic(topic(name))
        .await
        .expect("CreateTopic should succeed");

    let messages = vec![
        PubsubMessage {
            data: b"one".to_vec(),
            ..Default::default()
        },
        PubsubMessage {
            data: b"two".to_vec(),
            ..Default::default()
        },
        PubsubMessage {
            data: b"three".to_vec(),
            ..Default::default()
        },
    ];
    let request_len = messages.len();

    let response = client
        .publish(PublishRequest {
            topic: name.to_string(),
            messages,
        })
        .await
        .expect("Publish should succeed")
        .into_inner();

    assert_eq!(response.message_ids.len(), request_len);
}

#[tokio::test]
async fn list_topic_subscriptions_and_snapshots_do_not_error() {
    let harness = OpenPubusbHarness::start().await;
    let mut client = harness.publisher_client();
    let name = "projects/contract-test/topics/list-children";

    client
        .create_topic(topic(name))
        .await
        .expect("CreateTopic should succeed");

    let subscriptions = client
        .list_topic_subscriptions(ListTopicSubscriptionsRequest {
            topic: name.to_string(),
            ..Default::default()
        })
        .await
        .expect("ListTopicSubscriptions should succeed")
        .into_inner();
    assert!(subscriptions.subscriptions.is_empty());

    let snapshots = client
        .list_topic_snapshots(ListTopicSnapshotsRequest {
            topic: name.to_string(),
            ..Default::default()
        })
        .await
        .expect("ListTopicSnapshots should succeed")
        .into_inner();
    assert!(snapshots.snapshots.is_empty());
}

#[tokio::test]
async fn detach_subscription_sets_detached_flag() {
    let harness = OpenPubusbHarness::start().await;
    let mut publisher = harness.publisher_client();
    let mut subscriber = harness.subscriber_client();
    let topic_name = "projects/contract-test/topics/detach-source";
    let sub_name = "projects/contract-test/subscriptions/detach-target";

    publisher
        .create_topic(topic(topic_name))
        .await
        .expect("CreateTopic should succeed");
    subscriber
        .create_subscription(subscription(sub_name, topic_name))
        .await
        .expect("CreateSubscription should succeed");

    publisher
        .detach_subscription(DetachSubscriptionRequest {
            subscription: sub_name.to_string(),
        })
        .await
        .expect("DetachSubscription should succeed");

    let fetched = subscriber
        .get_subscription(open_pubusb_proto::pubsub::v1::GetSubscriptionRequest {
            subscription: sub_name.to_string(),
        })
        .await
        .expect("GetSubscription should succeed")
        .into_inner();
    assert!(fetched.detached);
}
