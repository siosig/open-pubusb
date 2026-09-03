#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Contract tests for the unary methods of `google.pubsub.v1.Subscriber`,
//! plus `SchemaService`/`IAMPolicy` (both UNIMPLEMENTED) and the
//! ignored-metadata contract, against a real `open-pubusb serve --ephemeral`
//! process.
//!
//! `StreamingPull` is out of scope here (see `tests/contract/tests/grpc_streaming_pull.rs`).
//! `Seek` and the Snapshot RPCs are covered by
//! `tests/contract/tests/grpc_snapshots_seek.rs` instead of here.

use std::collections::HashMap;

use open_pubusb_contract_tests::harness::OpenPubusbHarness;
use open_pubusb_proto::iam::v1::iam_policy_client::IamPolicyClient;
use open_pubusb_proto::iam::v1::GetIamPolicyRequest;
use open_pubusb_proto::pubsub::v1::schema_service_client::SchemaServiceClient;
use open_pubusb_proto::pubsub::v1::{
    AcknowledgeRequest, DeleteSubscriptionRequest, GetSchemaRequest, GetSubscriptionRequest,
    ListSubscriptionsRequest, ModifyAckDeadlineRequest, ModifyPushConfigRequest, PublishRequest,
    PubsubMessage, PullRequest, PushConfig, Subscription, Topic, UpdateSubscriptionRequest,
};
use pbjson_types::FieldMask;
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
async fn create_get_list_delete_subscription_round_trip() {
    let harness = OpenPubusbHarness::start().await;
    let mut publisher = harness.publisher_client();
    let mut subscriber = harness.subscriber_client();
    let topic_name = "projects/contract-test/topics/sub-crud-source";
    let sub_name = "projects/contract-test/subscriptions/crud";

    publisher
        .create_topic(topic(topic_name))
        .await
        .expect("CreateTopic should succeed");

    let created = subscriber
        .create_subscription(subscription(sub_name, topic_name))
        .await
        .expect("CreateSubscription should succeed")
        .into_inner();
    assert_eq!(created.name, sub_name);
    assert_eq!(created.topic, topic_name);

    let fetched = subscriber
        .get_subscription(GetSubscriptionRequest {
            subscription: sub_name.to_string(),
        })
        .await
        .expect("GetSubscription should succeed")
        .into_inner();
    assert_eq!(fetched.name, sub_name);

    let listed = subscriber
        .list_subscriptions(ListSubscriptionsRequest {
            project: "projects/contract-test".to_string(),
            ..Default::default()
        })
        .await
        .expect("ListSubscriptions should succeed")
        .into_inner();
    assert!(listed.subscriptions.iter().any(|s| s.name == sub_name));

    subscriber
        .delete_subscription(DeleteSubscriptionRequest {
            subscription: sub_name.to_string(),
        })
        .await
        .expect("DeleteSubscription should succeed");

    let err = subscriber
        .get_subscription(GetSubscriptionRequest {
            subscription: sub_name.to_string(),
        })
        .await
        .expect_err("GetSubscription after DeleteSubscription should fail");
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn update_subscription_labels_succeeds() {
    let harness = OpenPubusbHarness::start().await;
    let mut publisher = harness.publisher_client();
    let mut subscriber = harness.subscriber_client();
    let topic_name = "projects/contract-test/topics/update-labels-source";
    let sub_name = "projects/contract-test/subscriptions/update-labels";

    publisher
        .create_topic(topic(topic_name))
        .await
        .expect("CreateTopic should succeed");
    subscriber
        .create_subscription(subscription(sub_name, topic_name))
        .await
        .expect("CreateSubscription should succeed");

    let mut updated = subscription(sub_name, topic_name);
    updated.labels = HashMap::from([("env".to_string(), "contract-test".to_string())]);

    let response = subscriber
        .update_subscription(UpdateSubscriptionRequest {
            subscription: Some(updated),
            update_mask: Some(FieldMask {
                paths: vec!["labels".to_string()],
            }),
        })
        .await
        .expect("UpdateSubscription(labels) should succeed")
        .into_inner();

    assert_eq!(
        response.labels.get("env").map(String::as_str),
        Some("contract-test")
    );
}

#[tokio::test]
async fn update_subscription_topic_is_invalid_argument() {
    let harness = OpenPubusbHarness::start().await;
    let mut publisher = harness.publisher_client();
    let mut subscriber = harness.subscriber_client();
    let topic_name = "projects/contract-test/topics/update-topic-source";
    let other_topic_name = "projects/contract-test/topics/update-topic-other";
    let sub_name = "projects/contract-test/subscriptions/update-topic";

    publisher
        .create_topic(topic(topic_name))
        .await
        .expect("CreateTopic should succeed");
    publisher
        .create_topic(topic(other_topic_name))
        .await
        .expect("CreateTopic (other) should succeed");
    subscriber
        .create_subscription(subscription(sub_name, topic_name))
        .await
        .expect("CreateSubscription should succeed");

    let mut updated = subscription(sub_name, topic_name);
    updated.topic = other_topic_name.to_string();

    let err = subscriber
        .update_subscription(UpdateSubscriptionRequest {
            subscription: Some(updated),
            update_mask: Some(FieldMask {
                paths: vec!["topic".to_string()],
            }),
        })
        .await
        .expect_err("UpdateSubscription(topic) should fail: topic is immutable");
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn publish_pull_ack_round_trip() {
    let harness = OpenPubusbHarness::start().await;
    let mut publisher = harness.publisher_client();
    let mut subscriber = harness.subscriber_client();
    let topic_name = "projects/contract-test/topics/pull-ack-source";
    let sub_name = "projects/contract-test/subscriptions/pull-ack";

    publisher
        .create_topic(topic(topic_name))
        .await
        .expect("CreateTopic should succeed");
    subscriber
        .create_subscription(subscription(sub_name, topic_name))
        .await
        .expect("CreateSubscription should succeed");

    publisher
        .publish(PublishRequest {
            topic: topic_name.to_string(),
            messages: vec![PubsubMessage {
                data: b"payload".to_vec(),
                ..Default::default()
            }],
        })
        .await
        .expect("Publish should succeed");

    let pulled = subscriber
        .pull(PullRequest {
            subscription: sub_name.to_string(),
            max_messages: 1,
            ..Default::default()
        })
        .await
        .expect("Pull should succeed")
        .into_inner();

    assert_eq!(pulled.received_messages.len(), 1);
    let ack_id = pulled.received_messages[0].ack_id.clone();

    // Also exercise ModifyAckDeadline before acking, per contract.
    subscriber
        .modify_ack_deadline(ModifyAckDeadlineRequest {
            subscription: sub_name.to_string(),
            ack_ids: vec![ack_id.clone()],
            ack_deadline_seconds: 30,
        })
        .await
        .expect("ModifyAckDeadline should succeed");

    subscriber
        .acknowledge(AcknowledgeRequest {
            subscription: sub_name.to_string(),
            ack_ids: vec![ack_id],
        })
        .await
        .expect("Acknowledge should succeed");
}

#[tokio::test]
async fn modify_push_config_switches_endpoint() {
    let harness = OpenPubusbHarness::start().await;
    let mut publisher = harness.publisher_client();
    let mut subscriber = harness.subscriber_client();
    let topic_name = "projects/contract-test/topics/push-config-source";
    let sub_name = "projects/contract-test/subscriptions/push-config";

    publisher
        .create_topic(topic(topic_name))
        .await
        .expect("CreateTopic should succeed");
    subscriber
        .create_subscription(subscription(sub_name, topic_name))
        .await
        .expect("CreateSubscription should succeed");

    subscriber
        .modify_push_config(ModifyPushConfigRequest {
            subscription: sub_name.to_string(),
            push_config: Some(PushConfig {
                push_endpoint: "http://localhost:9/push".to_string(),
                ..Default::default()
            }),
        })
        .await
        .expect("ModifyPushConfig should succeed");

    let fetched = subscriber
        .get_subscription(GetSubscriptionRequest {
            subscription: sub_name.to_string(),
        })
        .await
        .expect("GetSubscription should succeed")
        .into_inner();

    assert_eq!(
        fetched.push_config.map(|p| p.push_endpoint),
        Some("http://localhost:9/push".to_string())
    );
}

#[tokio::test]
async fn schema_service_is_unimplemented() {
    let harness = OpenPubusbHarness::start().await;
    let mut client = SchemaServiceClient::new(harness.channel());

    let err = client
        .get_schema(GetSchemaRequest {
            name: "projects/contract-test/schemas/does-not-matter".to_string(),
            view: 0,
        })
        .await
        .expect_err("SchemaService.GetSchema should be UNIMPLEMENTED");
    assert_eq!(err.code(), Code::Unimplemented);
}

#[tokio::test]
async fn iam_policy_is_unimplemented() {
    let harness = OpenPubusbHarness::start().await;
    let mut client = IamPolicyClient::new(harness.channel());

    let err = client
        .get_iam_policy(GetIamPolicyRequest {
            resource: "projects/contract-test/topics/does-not-matter".to_string(),
            options: None,
        })
        .await
        .expect_err("IAMPolicy.GetIamPolicy should be UNIMPLEMENTED");
    assert_eq!(err.code(), Code::Unimplemented);
}

#[tokio::test]
async fn bogus_authorization_header_is_ignored() {
    let harness = OpenPubusbHarness::start().await;
    let mut publisher = harness.publisher_client();
    let mut subscriber = harness.subscriber_client();
    let topic_name = "projects/contract-test/topics/auth-header-source";
    let sub_name = "projects/contract-test/subscriptions/auth-header";

    publisher
        .create_topic(topic(topic_name))
        .await
        .expect("CreateTopic should succeed");
    subscriber
        .create_subscription(subscription(sub_name, topic_name))
        .await
        .expect("CreateSubscription should succeed");

    let mut request = tonic::Request::new(GetSubscriptionRequest {
        subscription: sub_name.to_string(),
    });
    request
        .metadata_mut()
        .insert("authorization", "Bearer bogus".parse().unwrap());

    let fetched = subscriber
        .get_subscription(request)
        .await
        .expect("GetSubscription should succeed despite a bogus authorization header")
        .into_inner();
    assert_eq!(fetched.name, sub_name);
}
