#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Topic / Subscription CRUD integration tests.
//!
//! Covers User Story 1 acceptance scenarios 1 and 6, the Edge Cases
//! section, and this exact scope: create/get/list/delete topic &
//! subscription, `ALREADY_EXISTS`, `NOT_FOUND`, invalid names, `goog`
//! prefix, delete topic → subscription.topic == `_deleted-topic_`,
//! `ListTopicSubscriptions`, `DetachSubscription` → pull
//! `FAILED_PRECONDITION`, and `update_topic_labels` (the domain-level
//! counterpart of `UpdateTopic` with an update_mask limited to `labels`).
//!
//! `open_pubusb_core::service::PubSubService` does not exist yet. Until
//! then, every test here runs against
//! [`open_pubusb_integration_tests::target_api::StubService`], a documented
//! stand-in whose methods are `unimplemented!()`. Every test is therefore
//! `#[ignore]`d: `#[ignore]` only skips *running* the test, so this file
//! still proves today that the intended API shape compiles and that the
//! test bodies are complete and reviewable.
//!
//! TODO: once the real `PubSubService` lands, remove every
//! `#[ignore]`, swap `StubService::new_ephemeral()` for
//! `open_pubusb_core::service::PubSubService::new_ephemeral()`, and fix any
//! signature mismatches against `target_api::PubSubServiceApi`. See
//! `tests/integration/src/target_api.rs` for the full replacement plan,
//! including why this crate does not yet import `open_pubusb_core::Error`
//! directly.

use std::collections::HashMap;

use open_pubusb_integration_tests::target_api::{
    Error, PubSubServiceApi, StubService, SubscriptionOpts, DELETED_TOPIC,
};

/// Scenario 1: create topic → get returns matching labels; create again
/// with the same name → `Error::AlreadyExists`.
#[test]
fn create_topic_then_get_returns_labels_and_rejects_duplicate_create() {
    let svc = StubService::new_ephemeral();

    let topic = "projects/p/topics/my-topic";
    let mut labels = HashMap::new();
    labels.insert("env".to_string(), "test".to_string());

    svc.create_topic(topic, labels.clone()).unwrap();

    let info = svc.get_topic(topic).unwrap();
    assert_eq!(info.name, topic);
    assert_eq!(info.labels, labels);

    let err = svc.create_topic(topic, HashMap::new()).unwrap_err();
    assert!(matches!(err, Error::AlreadyExists { .. }));
}

/// Scenario 2: get / delete / list-subscriptions on a nonexistent topic →
/// `Error::NotFound` (the referenced resource is missing).
#[test]
fn operations_on_nonexistent_topic_return_not_found() {
    let svc = StubService::new_ephemeral();
    let topic = "projects/p/topics/does-not-exist";

    assert!(matches!(
        svc.get_topic(topic).unwrap_err(),
        Error::NotFound { .. }
    ));
    assert!(matches!(
        svc.delete_topic(topic).unwrap_err(),
        Error::NotFound { .. }
    ));
    assert!(matches!(
        svc.list_topic_subscriptions(topic).unwrap_err(),
        Error::NotFound { .. }
    ));
}

/// Scenario 3: invalid topic name (too short, bad characters) →
/// `Error::InvalidArgument` (names.rs `validate_resource_id`: 3-255 chars,
/// `[A-Za-z0-9-_.~+%]` only).
#[test]
fn invalid_topic_names_are_rejected() {
    let svc = StubService::new_ephemeral();

    // Too short: the trailing id must be at least 3 characters.
    let too_short = "projects/p/topics/ab";
    let err = svc.create_topic(too_short, HashMap::new()).unwrap_err();
    assert!(matches!(err, Error::InvalidArgument { .. }));

    // Bad characters: a space is not in [A-Za-z0-9-_.~+%].
    let bad_chars = "projects/p/topics/abc def";
    let err = svc.create_topic(bad_chars, HashMap::new()).unwrap_err();
    assert!(matches!(err, Error::InvalidArgument { .. }));
}

/// Scenario 4: a name starting with `goog` → `Error::InvalidArgument`
/// (names.rs `FORBIDDEN_ID_PREFIX`).
#[test]
fn topic_name_starting_with_goog_is_rejected() {
    let svc = StubService::new_ephemeral();

    let name = "projects/p/topics/googtopic";
    let err = svc.create_topic(name, HashMap::new()).unwrap_err();
    assert!(matches!(err, Error::InvalidArgument { .. }));
}

/// Scenario 5: create a subscription against a topic, delete the topic,
/// then get the subscription and assert its `topic` field equals
/// `DELETED_TOPIC` (`"_deleted-topic_"`): on deletion, the `topic` of every
/// attached Subscription is replaced with `_deleted-topic_`.
#[test]
fn deleting_topic_detaches_subscription_to_deleted_topic_sentinel() {
    let svc = StubService::new_ephemeral();
    let topic = "projects/proj/topics/topic1";
    let sub = "projects/proj/subscriptions/sub1";

    svc.create_topic(topic, HashMap::new()).unwrap();
    svc.create_subscription(sub, topic, SubscriptionOpts::default())
        .unwrap();

    svc.delete_topic(topic).unwrap();

    let info = svc.get_subscription(sub).unwrap();
    assert_eq!(info.topic, DELETED_TOPIC);
}

/// Scenario 6: `list_topic_subscriptions` after creating 2 subscriptions on
/// one topic and 1 on another returns exactly the 2 for the first
/// (`Publisher.ListTopicSubscriptions`).
#[test]
fn list_topic_subscriptions_returns_only_subscriptions_of_that_topic() {
    let svc = StubService::new_ephemeral();
    let topic_a = "projects/p/topics/topic-a";
    let topic_b = "projects/p/topics/topic-b";
    svc.create_topic(topic_a, HashMap::new()).unwrap();
    svc.create_topic(topic_b, HashMap::new()).unwrap();

    let sub_a1 = "projects/proj/subscriptions/suba1";
    let sub_a2 = "projects/proj/subscriptions/suba2";
    let sub_b1 = "projects/proj/subscriptions/subb1";
    svc.create_subscription(sub_a1, topic_a, SubscriptionOpts::default())
        .unwrap();
    svc.create_subscription(sub_a2, topic_a, SubscriptionOpts::default())
        .unwrap();
    svc.create_subscription(sub_b1, topic_b, SubscriptionOpts::default())
        .unwrap();

    let mut subs = svc.list_topic_subscriptions(topic_a).unwrap();
    subs.sort();
    let mut expected = vec![sub_a1.to_string(), sub_a2.to_string()];
    expected.sort();
    assert_eq!(subs, expected);
}

/// Scenario 7: `detach_subscription` then `pull` on it →
/// `Error::FailedPrecondition` (pull on a Detached subscription is
/// FAILED_PRECONDITION per the error mapping table).
#[test]
fn detached_subscription_pull_fails_precondition() {
    let svc = StubService::new_ephemeral();
    let topic = "projects/proj/topics/topic1";
    let sub = "projects/proj/subscriptions/sub1";
    svc.create_topic(topic, HashMap::new()).unwrap();
    svc.create_subscription(sub, topic, SubscriptionOpts::default())
        .unwrap();

    svc.detach_subscription(sub).unwrap();

    let err = svc.pull(sub, 10).unwrap_err();
    assert!(matches!(err, Error::FailedPrecondition { .. }));
}

/// Scenario 8: `update_topic_labels` changes labels without needing full
/// topic recreation (get before/after differ); `Publisher.UpdateTopic`'s
/// `labels` field is mutable.
#[test]
fn update_topic_labels_changes_labels_without_recreation() {
    let svc = StubService::new_ephemeral();
    let topic = "projects/proj/topics/topic1";
    let mut initial = HashMap::new();
    initial.insert("env".to_string(), "dev".to_string());
    svc.create_topic(topic, initial.clone()).unwrap();

    let before = svc.get_topic(topic).unwrap();
    assert_eq!(before.labels, initial);

    let mut updated = HashMap::new();
    updated.insert("env".to_string(), "prod".to_string());
    svc.update_topic_labels(topic, updated.clone()).unwrap();

    let after = svc.get_topic(topic).unwrap();
    assert_eq!(after.labels, updated);
    assert_ne!(before.labels, after.labels);
}
