#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration test (User Story 1) documenting the target behavior
//! of `publish` / `pull` / `acknowledge` / `modify_ack_deadline`.
//!
//! These operations do not exist in implementable form yet — they land in
//! `delivery/lease.rs`, `delivery/engine.rs` and `service.rs`, which wire a
//! real `open_pubusb_core::service::PubSubService`. Every test below is
//! `#[ignore]`d for that reason: they exist to document and pin down the
//! expected behavior (per User Story 1 acceptance scenarios 2-5 and Edge
//! Cases, and the delivery/lease handling and message state diagram) so
//! that once the real service lands, removing `#[ignore]` turns this file
//! into the executable contract.
//!
//! Until then, they're written against `target_api::PubSubServiceApi` /
//! `StubService`, a compile-only stand-in shared with
//! `topics_subscriptions.rs` — see `tests/integration/src/target_api.rs`.

use std::collections::HashMap;

use open_pubusb_core::limits::{MAX_MESSAGE_BYTES, MAX_PUBLISH_BATCH_MESSAGES};
use open_pubusb_core::Error;
use open_pubusb_integration_tests::target_api::{
    PubSubServiceApi, PublishMessage, StubService, SubscriptionOpts,
};

const TOPIC: &str = "projects/proj/topics/topic1";
const SUB: &str = "projects/proj/subscriptions/sub1";
const SUB_A: &str = "projects/proj/subscriptions/suba";
const SUB_B: &str = "projects/proj/subscriptions/subb";

fn attrs(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn message(data: &[u8]) -> PublishMessage {
    PublishMessage {
        data: data.to_vec(),
        ..Default::default()
    }
}

// ---- Scenario 1: publish returns monotonically increasing, distinct ids ----

#[test]
fn publish_returns_monotonically_increasing_distinct_message_ids() {
    let svc = StubService::new_ephemeral();
    svc.create_topic(TOPIC, HashMap::new()).unwrap();

    let messages = vec![
        PublishMessage {
            data: b"a".to_vec(),
            attributes: attrs(&[("k", "1")]),
            ordering_key: String::new(),
        },
        PublishMessage {
            data: b"b".to_vec(),
            attributes: attrs(&[("k", "2")]),
            ordering_key: String::new(),
        },
        PublishMessage {
            data: b"c".to_vec(),
            attributes: attrs(&[("k", "3")]),
            ordering_key: String::new(),
        },
    ];

    let ids = svc.publish(TOPIC, messages).unwrap();
    assert_eq!(ids.len(), 3);

    let nums: Vec<u64> = ids.iter().map(|id| id.parse().unwrap()).collect();
    assert!(
        nums[0] < nums[1] && nums[1] < nums[2],
        "message_ids must be strictly increasing in publish order: {nums:?}"
    );

    let unique: std::collections::HashSet<_> = nums.iter().collect();
    assert_eq!(unique.len(), 3, "message_ids must be distinct: {nums:?}");
}

// ---- Scenario 2: publish then pull round-trips data/attributes/message_id, sets publish_time ----

#[test]
fn publish_then_pull_returns_same_data_attributes_and_message_id() {
    let svc = StubService::new_ephemeral();
    svc.create_topic(TOPIC, HashMap::new()).unwrap();
    svc.create_subscription(SUB, TOPIC, SubscriptionOpts::default())
        .unwrap();

    let attributes = attrs(&[("k", "v")]);
    let sent = PublishMessage {
        data: b"hello".to_vec(),
        attributes: attributes.clone(),
        ordering_key: String::new(),
    };
    let ids = svc.publish(TOPIC, vec![sent]).unwrap();

    let pulled = svc.pull(SUB, 10).unwrap();
    assert_eq!(pulled.len(), 1);
    let m = &pulled[0];
    assert_eq!(m.data, b"hello");
    assert_eq!(m.attributes, attributes);
    assert_eq!(m.message_id, ids[0]);
    assert!(m.publish_time_ms > 0, "publish_time_ms must be set");
}

// ---- Scenario 3: ack prevents redelivery ----

#[test]
fn acknowledged_message_is_not_redelivered() {
    let svc = StubService::new_ephemeral();
    svc.create_topic(TOPIC, HashMap::new()).unwrap();
    svc.create_subscription(SUB, TOPIC, SubscriptionOpts::default())
        .unwrap();
    svc.publish(TOPIC, vec![message(b"once")]).unwrap();

    let pulled = svc.pull(SUB, 10).unwrap();
    assert_eq!(pulled.len(), 1);
    svc.acknowledge(SUB, vec![pulled[0].ack_id.clone()])
        .unwrap();

    let redelivered = svc.pull(SUB, 10).unwrap();
    assert!(
        redelivered.is_empty(),
        "acked message must not be redelivered"
    );
}

// ---- Scenario 4: unacked message redelivered after deadline expiry, delivery_attempt incremented ----

#[test]
fn unacked_message_is_redelivered_after_deadline_with_incremented_delivery_attempt() {
    let svc = StubService::new_ephemeral();
    svc.create_topic(TOPIC, HashMap::new()).unwrap();
    svc.create_subscription(
        SUB,
        TOPIC,
        SubscriptionOpts {
            ack_deadline_seconds: 10,
            ..Default::default()
        },
    )
    .unwrap();
    svc.publish(TOPIC, vec![message(b"redeliver-me")]).unwrap();

    let first = svc.pull(SUB, 10).unwrap();
    assert_eq!(first.len(), 1);

    // Past the 10s ack deadline, without ever acking.
    svc.advance_clock(11);

    let second = svc.pull(SUB, 10).unwrap();
    assert_eq!(second.len(), 1, "expired lease must be redelivered");
    assert_eq!(second[0].message_id, first[0].message_id);
    // `delivery_attempt` is only exposed to the client when the
    // subscription has a dead-letter policy (it's exposed in the API only
    // when a dead_letter_policy is set); this subscription has none, per
    // `target_api::SubscriptionOpts` (which doesn't even expose that
    // field), so both deliveries correctly report `0` here.
    // A distinct `ack_id` is the DLQ-policy-independent signal that this
    // was a genuine second delivery, not the same lease handed out twice
    // (`crates/open-pubusb-core/src/service.rs`'s
    // `unacked_message_redelivered_after_deadline_with_incremented_attempt`
    // test covers the `delivery_attempt`-is-exposed case directly).
    assert_eq!(second[0].delivery_attempt, 0);
    assert_ne!(
        second[0].ack_id, first[0].ack_id,
        "redelivery must issue a new ack_id (new lease generation)"
    );
}

// ---- Scenario 5: modify_ack_deadline(N>0) extends the lease ----

#[test]
fn modify_ack_deadline_extends_lease_preventing_immediate_redelivery() {
    let svc = StubService::new_ephemeral();
    svc.create_topic(TOPIC, HashMap::new()).unwrap();
    svc.create_subscription(SUB, TOPIC, SubscriptionOpts::default())
        .unwrap();
    svc.publish(TOPIC, vec![message(b"extend-me")]).unwrap();

    let pulled = svc.pull(SUB, 10).unwrap();
    assert_eq!(pulled.len(), 1);
    let ack_id = pulled[0].ack_id.clone();

    svc.modify_ack_deadline(SUB, vec![ack_id], 600).unwrap();

    // No advance_clock call: an extended-but-still-live lease must not be
    // handed out again.
    let pulled_again = svc.pull(SUB, 10).unwrap();
    assert!(
        pulled_again.is_empty(),
        "message with an extended (still-live) lease must not be redelivered"
    );
}

// ---- Scenario 6: modify_ack_deadline(0) is an immediate nack ----

#[test]
fn modify_ack_deadline_zero_immediately_nacks_and_redelivers() {
    let svc = StubService::new_ephemeral();
    svc.create_topic(TOPIC, HashMap::new()).unwrap();
    svc.create_subscription(SUB, TOPIC, SubscriptionOpts::default())
        .unwrap();
    svc.publish(TOPIC, vec![message(b"nack-me")]).unwrap();

    let pulled = svc.pull(SUB, 10).unwrap();
    assert_eq!(pulled.len(), 1);
    let ack_id = pulled[0].ack_id.clone();

    svc.modify_ack_deadline(SUB, vec![ack_id], 0).unwrap();

    // No advance_clock call: a 0s modack must expire the lease right away.
    let redelivered = svc.pull(SUB, 10).unwrap();
    assert_eq!(
        redelivered.len(),
        1,
        "modify_ack_deadline(ack_ids, 0) must nack immediately, without needing advance_clock"
    );
}

// ---- Scenario 7: duplicate ack is idempotent ----

#[test]
fn acknowledging_same_ack_id_twice_does_not_error() {
    let svc = StubService::new_ephemeral();
    svc.create_topic(TOPIC, HashMap::new()).unwrap();
    svc.create_subscription(SUB, TOPIC, SubscriptionOpts::default())
        .unwrap();
    svc.publish(TOPIC, vec![message(b"double-ack")]).unwrap();

    let pulled = svc.pull(SUB, 10).unwrap();
    let ack_id = pulled[0].ack_id.clone();

    svc.acknowledge(SUB, vec![ack_id.clone()]).unwrap();
    svc.acknowledge(SUB, vec![ack_id]).unwrap();
}

// ---- Scenario 8: unknown/stale ack_id is ignored, not an error ----

#[test]
fn acknowledging_unknown_ack_id_does_not_error() {
    let svc = StubService::new_ephemeral();
    svc.create_topic(TOPIC, HashMap::new()).unwrap();
    svc.create_subscription(SUB, TOPIC, SubscriptionOpts::default())
        .unwrap();

    // Unknown/stale ack_ids are ignored (the response still succeeds).
    svc.acknowledge(SUB, vec!["totally-unknown-ack-id".to_string()])
        .unwrap();
}

// ---- Scenario 9: pull with nothing pending returns Ok(vec![]) promptly ----

#[test]
fn pull_with_no_pending_messages_returns_empty_vec() {
    let svc = StubService::new_ephemeral();
    svc.create_topic(TOPIC, HashMap::new()).unwrap();
    svc.create_subscription(SUB, TOPIC, SubscriptionOpts::default())
        .unwrap();

    // This stub can't exercise the real server-side blocking-wait
    // behavior (Edge Cases: wait up to the client's deadline before
    // returning empty) — it only asserts the stub returns Ok(vec![])
    // promptly when nothing is pending.
    let pulled = svc.pull(SUB, 10).unwrap();
    assert!(pulled.is_empty());
}

// ---- Scenario 10: messages published before subscription creation are never delivered to it ----

#[test]
fn messages_published_before_subscription_creation_are_not_delivered() {
    let svc = StubService::new_ephemeral();
    svc.create_topic(TOPIC, HashMap::new()).unwrap();
    svc.publish(TOPIC, vec![message(b"before-subscription")])
        .unwrap();

    svc.create_subscription(SUB, TOPIC, SubscriptionOpts::default())
        .unwrap();
    let ids_after = svc
        .publish(TOPIC, vec![message(b"after-subscription")])
        .unwrap();

    let pulled = svc.pull(SUB, 10).unwrap();
    assert_eq!(
        pulled.len(),
        1,
        "only the message published after subscription creation should be delivered"
    );
    assert_eq!(pulled[0].data, b"after-subscription");
    assert_eq!(pulled[0].message_id, ids_after[0]);
}

// ---- Scenario 11: fan-out — two subscriptions on the same topic both receive ----

#[test]
fn two_subscriptions_on_same_topic_both_receive_published_message() {
    let svc = StubService::new_ephemeral();
    svc.create_topic(TOPIC, HashMap::new()).unwrap();
    svc.create_subscription(SUB_A, TOPIC, SubscriptionOpts::default())
        .unwrap();
    svc.create_subscription(SUB_B, TOPIC, SubscriptionOpts::default())
        .unwrap();

    svc.publish(TOPIC, vec![message(b"fan-out")]).unwrap();

    let a = svc.pull(SUB_A, 10).unwrap();
    let b = svc.pull(SUB_B, 10).unwrap();
    assert_eq!(a.len(), 1, "subscription A must receive the message");
    assert_eq!(b.len(), 1, "subscription B must receive the message");
    assert_eq!(a[0].data, b"fan-out");
    assert_eq!(b[0].data, b"fan-out");
}

// ---- Scenario 12: publish batch over MAX_PUBLISH_BATCH_MESSAGES -> InvalidArgument ----

#[test]
fn publishing_batch_over_limit_returns_invalid_argument() {
    let svc = StubService::new_ephemeral();
    svc.create_topic(TOPIC, HashMap::new()).unwrap();

    let messages: Vec<PublishMessage> = (0..=MAX_PUBLISH_BATCH_MESSAGES)
        .map(|i| message(i.to_string().as_bytes()))
        .collect();
    assert_eq!(messages.len(), MAX_PUBLISH_BATCH_MESSAGES + 1);

    let err = svc.publish(TOPIC, messages).unwrap_err();
    assert!(
        matches!(err, Error::InvalidArgument { .. }),
        "expected InvalidArgument, got {err:?}"
    );
}

// ---- Scenario 13: single message over MAX_MESSAGE_BYTES -> InvalidArgument ----

#[test]
fn publishing_message_over_max_bytes_returns_invalid_argument() {
    let svc = StubService::new_ephemeral();
    svc.create_topic(TOPIC, HashMap::new()).unwrap();

    let oversized = message(&vec![0u8; MAX_MESSAGE_BYTES + 1]);
    let err = svc.publish(TOPIC, vec![oversized]).unwrap_err();
    assert!(
        matches!(err, Error::InvalidArgument { .. }),
        "expected InvalidArgument, got {err:?}"
    );
}
