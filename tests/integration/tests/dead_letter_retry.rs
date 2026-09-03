//! Integration test for dead-lettering
//! (`crates/open-pubusb-core/src/delivery/dead_letter.rs`) and its
//! interaction with retry-policy backoff, against a real
//! `PubSubService<MemKv>` with a [`MockClock`] for deterministic
//! ack-deadline-expiry timing.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;

use open_pubusb_core::clock::MockClock;
use open_pubusb_core::delivery::dead_letter::{
    ATTR_DELIVERY_COUNT, ATTR_SUBSCRIPTION, ATTR_SUBSCRIPTION_PROJECT, ATTR_TOPIC_PUBLISH_TIME,
};
use open_pubusb_core::service::{PubSubService, PublishMessage};
use open_pubusb_core::store::kv::MemKv;
use open_pubusb_core::subscription::CreateSubscriptionOptions;

const TOPIC: &str = "projects/proj-x/topics/topic-a";
const DLQ_TOPIC: &str = "projects/proj-x/topics/dlq-a";
const SUB: &str = "projects/proj-x/subscriptions/sub-a";
const DLQ_SUB: &str = "projects/proj-x/subscriptions/dlq-observer";

fn service_with_dlq_observer(
    dead_letter_topic: Option<&str>,
) -> (Arc<PubSubService<MemKv>>, Arc<MockClock>) {
    let clock = Arc::new(MockClock::new(1_000));
    let svc = Arc::new(PubSubService::new_ephemeral_with_clock(clock.clone()));
    svc.create_topic(TOPIC, HashMap::new()).unwrap();
    if let Some(dlq) = dead_letter_topic {
        svc.create_topic(dlq, HashMap::new()).unwrap();
        // Attaches before anything is published to `dlq`, so it observes
        // every message later forwarded there.
        svc.create_subscription(DLQ_SUB, dlq, CreateSubscriptionOptions::default())
            .unwrap();
    }
    (svc, clock)
}

/// Never acking a message drives it through `max_delivery_attempts`
/// ack-deadline expiries (1 initial delivery + N automatic
/// redeliveries), then dead-letters it: forwarded to the DLQ topic with
/// the four `CloudPubSubDeadLetterSource*` attributes added (original
/// attributes preserved), removed from the source subscription.
#[test]
fn attempts_exceeding_max_delivery_attempts_lands_the_message_in_the_dlq_topic() {
    let (svc, clock) = service_with_dlq_observer(Some(DLQ_TOPIC));
    svc.create_subscription(
        SUB,
        TOPIC,
        CreateSubscriptionOptions {
            ack_deadline_secs: Some(10),
            dead_letter_topic: Some(DLQ_TOPIC.to_string()),
            max_delivery_attempts: Some(5),
            ..Default::default()
        },
    )
    .unwrap();
    let mut attrs = HashMap::new();
    attrs.insert("orig".to_string(), "yes".to_string());
    svc.publish(
        TOPIC,
        vec![PublishMessage {
            data: b"never-acked".to_vec(),
            attributes: attrs,
            ..Default::default()
        }],
    )
    .unwrap();

    // Attempts 1..=5: each redelivery just past the ack deadline, never
    // acked, still below the threshold (5) so still on the source sub.
    for attempt in 1..=5u32 {
        let pulled = svc.pull(SUB, 10).unwrap();
        assert_eq!(pulled.len(), 1, "attempt {attempt}: still deliverable");
        assert_eq!(pulled[0].delivery_attempt, attempt);
        clock.advance_secs(11);
    }

    // The 6th lease attempt (prior_attempts == max_delivery_attempts)
    // dead-letters instead of redelivering.
    let pulled = svc.pull(SUB, 10).unwrap();
    assert!(
        pulled.is_empty(),
        "message must be dead-lettered, not redelivered a 6th time"
    );
    // Removed from the source: nothing left outstanding or pending there.
    let still_pending = svc.pull(SUB, 10).unwrap();
    assert!(still_pending.is_empty());

    let dlq_pulled = svc.pull(DLQ_SUB, 10).unwrap();
    assert_eq!(dlq_pulled.len(), 1);
    let dead_lettered = &dlq_pulled[0];
    assert_eq!(dead_lettered.data, b"never-acked");
    assert_eq!(
        dead_lettered.attributes.get("orig"),
        Some(&"yes".to_string())
    );
    assert_eq!(
        dead_lettered.attributes.get(ATTR_DELIVERY_COUNT),
        Some(&"5".to_string())
    );
    assert_eq!(
        dead_lettered.attributes.get(ATTR_SUBSCRIPTION),
        Some(&SUB.to_string())
    );
    assert_eq!(
        dead_lettered.attributes.get(ATTR_SUBSCRIPTION_PROJECT),
        Some(&"proj-x".to_string())
    );
    assert!(dead_lettered
        .attributes
        .contains_key(ATTR_TOPIC_PUBLISH_TIME));
}

/// A `retry_policy` min/max backoff is still honored on the way to
/// dead-lettering: the message only becomes eligible for its next
/// attempt (or for dead-lettering) once both the ack deadline *and* the
/// backoff window have elapsed, not just the ack deadline.
#[test]
fn retry_policy_backoff_is_honored_en_route_to_dead_lettering() {
    let (svc, clock) = service_with_dlq_observer(Some(DLQ_TOPIC));
    svc.create_subscription(
        SUB,
        TOPIC,
        CreateSubscriptionOptions {
            ack_deadline_secs: Some(10),
            dead_letter_topic: Some(DLQ_TOPIC.to_string()),
            max_delivery_attempts: Some(5),
            min_retry_backoff_secs: Some(30),
            max_retry_backoff_secs: Some(30),
            ..Default::default()
        },
    )
    .unwrap();
    svc.publish(
        TOPIC,
        vec![PublishMessage {
            data: b"backed-off".to_vec(),
            ..Default::default()
        }],
    )
    .unwrap();

    let first = svc.pull(SUB, 10).unwrap();
    assert_eq!(first.len(), 1);

    // Only the 10s ack deadline has passed, not the 30s backoff — must
    // not be redelivered yet.
    clock.advance_secs(11);
    assert!(svc.pull(SUB, 10).unwrap().is_empty());

    // Now the backoff window (anchored to the deadline, per
    // `DeliveryEngine::lease_next`'s doc comment) has elapsed too.
    clock.advance_secs(30);
    let second = svc.pull(SUB, 10).unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].delivery_attempt, 2);
}

/// No `dead_letter_topic` configured (retry backoff only, no dead-lettering) never
/// dead-letters — an ack-deadline expiry always redelivers immediately,
/// regardless of how many attempts have accumulated.
#[test]
fn no_dead_letter_policy_redelivers_immediately_forever() {
    let (svc, clock) = service_with_dlq_observer(None);
    svc.create_subscription(
        SUB,
        TOPIC,
        CreateSubscriptionOptions {
            ack_deadline_secs: Some(10),
            ..Default::default()
        },
    )
    .unwrap();
    svc.publish(
        TOPIC,
        vec![PublishMessage {
            data: b"no-dlq".to_vec(),
            ..Default::default()
        }],
    )
    .unwrap();

    for attempt in 1..=8u32 {
        let pulled = svc.pull(SUB, 10).unwrap();
        assert_eq!(
            pulled.len(),
            1,
            "attempt {attempt}: no policy means never dead-lettered"
        );
        clock.advance_secs(11);
    }
}

/// A `dead_letter_topic` that doesn't exist (deleted after the
/// subscription was configured with it, or simply never created) is
/// "missing DLQ topic → keep + warn": the message keeps being redelivered
/// normally rather than being silently lost.
#[test]
fn missing_dlq_topic_keeps_the_message_instead_of_losing_it() {
    let (svc, clock) = service_with_dlq_observer(None);
    svc.create_subscription(
        SUB,
        TOPIC,
        CreateSubscriptionOptions {
            ack_deadline_secs: Some(10),
            dead_letter_topic: Some("projects/proj-x/topics/does-not-exist".to_string()),
            max_delivery_attempts: Some(5),
            ..Default::default()
        },
    )
    .unwrap();
    svc.publish(
        TOPIC,
        vec![PublishMessage {
            data: b"orphaned-dlq".to_vec(),
            ..Default::default()
        }],
    )
    .unwrap();

    // Drive well past the threshold (5) — should keep redelivering, not
    // vanish, since the configured DLQ topic never exists.
    for attempt in 1..=7u32 {
        let pulled = svc.pull(SUB, 10).unwrap();
        assert_eq!(
            pulled.len(),
            1,
            "attempt {attempt}: missing DLQ topic must not lose the message"
        );
        clock.advance_secs(11);
    }
}
