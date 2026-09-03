//! Integration test for Snapshot CRUD and Seek
//! (`crates/open-pubusb-core/src/delivery/snapshot.rs`;
//! `PubSubService::{create_snapshot,...,seek_to_snapshot,seek_to_time}`,
//! the service-layer half), against a real `PubSubService<MemKv>` with
//! a [`MockClock`].

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;

use open_pubusb_core::clock::{Clock, MockClock};
use open_pubusb_core::limits::{MAX_SNAPSHOT_LIFETIME_SECS, MIN_SNAPSHOT_REMAINING_LIFETIME_SECS};
use open_pubusb_core::service::{PubSubService, PublishMessage};
use open_pubusb_core::store::kv::MemKv;
use open_pubusb_core::subscription::CreateSubscriptionOptions;
use open_pubusb_core::Error;

const TOPIC: &str = "projects/proj-y/topics/topic-a";
const SUB: &str = "projects/proj-y/subscriptions/sub-a";
const SNAP: &str = "projects/proj-y/snapshots/snap-a";

fn service() -> (Arc<PubSubService<MemKv>>, Arc<MockClock>) {
    let clock = Arc::new(MockClock::new(1_000_000));
    let svc = Arc::new(PubSubService::new_ephemeral_with_clock(clock.clone()));
    svc.create_topic(TOPIC, HashMap::new()).unwrap();
    svc.create_subscription(SUB, TOPIC, CreateSubscriptionOptions::default())
        .unwrap();
    (svc, clock)
}

fn publish_n(svc: &PubSubService<MemKv>, n: usize) {
    for i in 0..n {
        svc.publish(
            TOPIC,
            vec![PublishMessage {
                data: format!("m{i}").into_bytes(),
                ..Default::default()
            }],
        )
        .unwrap();
    }
}

/// Acking messages, then seeking back to a snapshot taken before those
/// acks, makes them unacked (redeliverable) again with `delivery_attempt`
/// reset — this is the core Seek behavior.
#[test]
fn seek_to_snapshot_unacks_messages_acked_after_it_was_taken() {
    let (svc, _clock) = service();
    publish_n(&svc, 3);

    let snapshot = svc.create_snapshot(SNAP, SUB, HashMap::new()).unwrap();
    assert_eq!(snapshot.name, SNAP);

    // Pull and ack all 3 *after* the snapshot was taken.
    let pulled = svc.pull(SUB, 10).unwrap();
    assert_eq!(pulled.len(), 3);
    svc.acknowledge(SUB, pulled.iter().map(|m| m.ack_id.clone()).collect())
        .unwrap();
    assert!(svc.pull(SUB, 10).unwrap().is_empty());

    svc.seek_to_snapshot(SUB, SNAP).unwrap();

    let redelivered = svc.pull(SUB, 10).unwrap();
    assert_eq!(redelivered.len(), 3);
    // Fresh delivery after seek: attempts reset (not exposed without a
    // dead-letter policy, but the underlying persisted state has no
    // leftover `dlv` row — verified indirectly by never dead-lettering
    // even if this subscription had one; see the dead_letter_retry.rs
    // suite for that behavior directly).
}

/// Seeking to a point in time marks every message published before it as
/// done and every message from that point on as (re)deliverable.
#[test]
fn seek_to_time_splits_before_and_after() {
    let (svc, clock) = service();

    svc.publish(
        TOPIC,
        vec![PublishMessage {
            data: b"before".to_vec(),
            ..Default::default()
        }],
    )
    .unwrap();
    clock.advance_secs(10);
    let cutoff_ms = clock.now_ms();
    clock.advance_secs(10);
    svc.publish(
        TOPIC,
        vec![PublishMessage {
            data: b"after".to_vec(),
            ..Default::default()
        }],
    )
    .unwrap();

    // Ack both up front so the subscription starts fully caught up.
    let pulled = svc.pull(SUB, 10).unwrap();
    assert_eq!(pulled.len(), 2);
    svc.acknowledge(SUB, pulled.iter().map(|m| m.ack_id.clone()).collect())
        .unwrap();

    svc.seek_to_time(SUB, cutoff_ms).unwrap();

    let redelivered = svc.pull(SUB, 10).unwrap();
    assert_eq!(
        redelivered.len(),
        1,
        "only the message published at/after the cutoff"
    );
    assert_eq!(redelivered[0].data, b"after");
}

/// A fresh snapshot's `expire_time` is (creation time + 7 days) when the
/// subscription has nothing outstanding, and shrinks by the age of the
/// oldest unacked message otherwise.
#[test]
fn expire_time_reflects_oldest_unacked_message_age() {
    let (svc, clock) = service();
    // Nothing published/outstanding yet.
    let fully_caught_up = svc
        .create_snapshot("projects/proj-y/snapshots/snap-s1", SUB, HashMap::new())
        .unwrap();
    let now_ms = clock.now_ms();
    assert_eq!(
        fully_caught_up.expire_at_ms,
        now_ms + MAX_SNAPSHOT_LIFETIME_SECS * 1000
    );

    publish_n(&svc, 1);
    clock.advance_secs(3600); // the unacked message is now 1h old
    let with_backlog = svc
        .create_snapshot("projects/proj-y/snapshots/snap-s2", SUB, HashMap::new())
        .unwrap();
    let now_ms2 = clock.now_ms();
    assert_eq!(
        with_backlog.expire_at_ms,
        now_ms2 + (MAX_SNAPSHOT_LIFETIME_SECS - 3600) * 1000
    );
}

/// Creating a snapshot whose remaining lifetime would be under an hour
/// (the oldest unacked message is within an hour of the 7-day retention
/// horizon) is rejected with `FailedPrecondition`.
#[test]
fn remaining_lifetime_under_one_hour_is_rejected() {
    let (svc, clock) = service();
    publish_n(&svc, 1);
    clock.advance_secs(MAX_SNAPSHOT_LIFETIME_SECS - MIN_SNAPSHOT_REMAINING_LIFETIME_SECS + 60);

    let err = svc
        .create_snapshot("projects/proj-y/snapshots/too-old", SUB, HashMap::new())
        .unwrap_err();
    assert!(matches!(err, Error::FailedPrecondition { .. }));
}

/// List / update (labels) / delete round-trip.
#[test]
fn list_update_delete_round_trip() {
    let (svc, _clock) = service();
    publish_n(&svc, 1);
    svc.create_snapshot("projects/proj-y/snapshots/snap-aa", SUB, HashMap::new())
        .unwrap();
    svc.create_snapshot("projects/proj-y/snapshots/snap-bb", SUB, HashMap::new())
        .unwrap();

    let (page, _token) = svc.list_snapshots("proj-y", 10, None).unwrap();
    assert_eq!(page.len(), 2);

    let topic_snaps = svc.list_topic_snapshots(TOPIC).unwrap();
    assert_eq!(topic_snaps.len(), 2);

    let mut labels = HashMap::new();
    labels.insert("env".to_string(), "test".to_string());
    let updated = svc
        .update_snapshot_labels("projects/proj-y/snapshots/snap-aa", labels.clone())
        .unwrap();
    assert_eq!(updated.labels, labels);

    svc.delete_snapshot("projects/proj-y/snapshots/snap-aa")
        .unwrap();
    assert!(svc
        .get_snapshot("projects/proj-y/snapshots/snap-aa")
        .is_err());
    assert!(svc
        .get_snapshot("projects/proj-y/snapshots/snap-bb")
        .is_ok());
}

/// `retain_acked_messages` (either setting) enables seeking back over
/// already-acked messages: this implementation never deletes a message
/// before its topic-retention expiry regardless of ack state
/// (`crate::delivery::retention`'s module doc comment documents this
/// deliberate simplification), so Seek-to-snapshot/-time over acked
/// messages works the same whichever way `retain_acked_messages` is set —
/// this test pins that down for both settings rather than asserting a
/// difference this implementation doesn't have.
#[test]
fn seek_back_over_acked_messages_works_regardless_of_retain_acked_messages() {
    for retain in [true, false] {
        let clock = Arc::new(MockClock::new(1_000_000));
        let svc = Arc::new(PubSubService::new_ephemeral_with_clock(clock.clone()));
        svc.create_topic(TOPIC, HashMap::new()).unwrap();
        svc.create_subscription(
            SUB,
            TOPIC,
            CreateSubscriptionOptions {
                retain_acked_messages: retain,
                ..Default::default()
            },
        )
        .unwrap();
        publish_n(&svc, 1);
        let snapshot = svc.create_snapshot(SNAP, SUB, HashMap::new()).unwrap();
        let pulled = svc.pull(SUB, 10).unwrap();
        svc.acknowledge(SUB, vec![pulled[0].ack_id.clone()])
            .unwrap();
        assert!(svc.pull(SUB, 10).unwrap().is_empty());

        svc.seek_to_snapshot(SUB, SNAP).unwrap();
        assert_eq!(
            svc.pull(SUB, 10).unwrap().len(),
            1,
            "retain_acked_messages={retain}: seek back over an acked message must work"
        );
        assert_eq!(snapshot.topic, TOPIC);
    }
}
