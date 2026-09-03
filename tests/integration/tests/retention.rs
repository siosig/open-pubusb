//! Integration tests for retention / expiration sweeps and the on-disk
//! format-version guard.
//!
//! The sweep itself (`PubSubService::sweep_retention`) is `KvStore`-generic
//! (`crates/open-pubusb-core/src/delivery/retention.rs`'s module doc comment
//! explains why: an active sweep, not a fjall-specific compaction filter),
//! so these tests use the fast in-memory [`MemKv`] backend with a
//! [`MockClock`] for deterministic timing — except
//! [`newer_format_version_marker_fails_open_with_a_clear_error`], which is
//! inherently about the persistent [`FjallKv`] backend's on-disk state and
//! needs the real thing.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use open_pubusb_core::clock::MockClock;
use open_pubusb_core::service::{PubSubService, PublishMessage};
use open_pubusb_core::store::fjall::{FjallKv, OpenError, FORMAT_VERSION};
use open_pubusb_core::store::kv::{KvStore, MemKv};
use open_pubusb_core::subscription::CreateSubscriptionOptions;

const TOPIC: &str = "projects/p/topics/topic-a";
const SUB: &str = "projects/p/subscriptions/sub-a";

/// `sweep_retention` deletes messages whose `expire_at_ms` (computed once
/// at publish time from the topic's retention) has passed, regardless of
/// ack state, and does nothing to messages that have not yet expired.
#[test]
fn sweep_retention_deletes_only_expired_messages() {
    let clock = Arc::new(MockClock::new(0));
    let kv = Arc::new(MemKv::new());
    let svc = PubSubService::new(kv.clone(), clock.clone());

    svc.create_topic_full(
        TOPIC,
        Default::default(),
        Some(600), // 600s retention
        None,
        false,
        false,
    )
    .unwrap();
    svc.create_subscription(SUB, TOPIC, CreateSubscriptionOptions::default())
        .unwrap();
    svc.publish(TOPIC, vec![PublishMessage::default()]).unwrap(); // expires at 600_000ms

    clock.advance_secs(300); // 300s: not yet expired
    let stats = svc.sweep_retention();
    assert_eq!(stats.messages_expired, 0);
    assert_eq!(
        svc.pull(SUB, 10).unwrap().len(),
        1,
        "message must still be deliverable before its retention elapses"
    );
    // Re-lease it (redelivering the same message) so the next pull below
    // isn't blocked by its own still-outstanding lease from this pull.
    svc.acknowledge(
        SUB,
        svc.pull(SUB, 10)
            .unwrap()
            .into_iter()
            .map(|m| m.ack_id)
            .collect(),
    )
    .unwrap();

    clock.advance_secs(400); // now at 700s: past the 600s retention
    let stats = svc.sweep_retention();
    assert_eq!(
        stats.messages_expired, 1,
        "the sweep must delete the message once its retention has elapsed, \
         even though it was never acknowledged"
    );
}

/// `retain_acked_messages = true` on a subscription must not cause the
/// sweep to delete a message *before* its topic-level retention elapses —
/// acked or not, that message stays available (for a future Seek, once
/// implemented) until `expire_at_ms`, per this crate's design (see
/// `crate::delivery::retention`'s module doc comment).
#[test]
fn retain_acked_messages_keeps_acked_messages_until_expiry() {
    let clock = Arc::new(MockClock::new(0));
    let svc = PubSubService::new(Arc::new(MemKv::new()), clock.clone());

    svc.create_topic_full(TOPIC, Default::default(), Some(600), None, false, false)
        .unwrap();
    svc.create_subscription(
        SUB,
        TOPIC,
        CreateSubscriptionOptions {
            retain_acked_messages: true,
            ..Default::default()
        },
    )
    .unwrap();
    svc.publish(TOPIC, vec![PublishMessage::default()]).unwrap();

    let pulled = svc.pull(SUB, 10).unwrap();
    svc.acknowledge(SUB, vec![pulled[0].ack_id.clone()])
        .unwrap();

    clock.advance_secs(300); // well before the 600s retention elapses
    let stats = svc.sweep_retention();
    assert_eq!(
        stats.messages_expired, 0,
        "an acked message with retain_acked_messages=true must survive until \
         its topic-level retention actually elapses, not be swept early"
    );
}

/// `expiration_policy.ttl` (`Subscription.expiration_ttl_secs`): a
/// subscription with no Pull/Ack activity for longer than its TTL is
/// deleted by the sweep.
#[test]
fn idle_subscription_past_its_expiration_ttl_is_deleted() {
    let clock = Arc::new(MockClock::new(0));
    let svc = PubSubService::new(Arc::new(MemKv::new()), clock.clone());

    svc.create_topic(TOPIC, Default::default()).unwrap();
    svc.create_subscription(
        SUB,
        TOPIC,
        CreateSubscriptionOptions {
            expiration_ttl_secs: Some(Some(3600)), // 1 hour TTL
            ..Default::default()
        },
    )
    .unwrap();

    clock.advance_secs(1800); // 30 minutes of inactivity: not yet expired
    svc.sweep_retention();
    assert!(
        svc.get_subscription(SUB).is_ok(),
        "subscription must survive while still within its expiration TTL"
    );

    clock.advance_secs(1900); // total 3_700s: past the 1-hour TTL
    svc.sweep_retention();
    assert!(
        svc.get_subscription(SUB).is_err(),
        "subscription must be deleted once idle past its expiration_policy.ttl"
    );
}

/// A Pull (or Ack/ModifyAckDeadline) resets the idle clock —
/// `last_activity_ts_ms` is touched — so an actively-used subscription is
/// never swept just because it was created long ago.
#[test]
fn active_subscription_is_not_expired_despite_being_old() {
    let clock = Arc::new(MockClock::new(0));
    let svc = PubSubService::new(Arc::new(MemKv::new()), clock.clone());

    svc.create_topic(TOPIC, Default::default()).unwrap();
    svc.create_subscription(
        SUB,
        TOPIC,
        CreateSubscriptionOptions {
            expiration_ttl_secs: Some(Some(3600)),
            ..Default::default()
        },
    )
    .unwrap();

    clock.advance_secs(3000); // within the original TTL
    svc.pull(SUB, 1).unwrap(); // activity: resets last_activity_ts_ms

    clock.advance_secs(3000); // 3_000s since the Pull above — still < 3_600s TTL from that pull
    svc.sweep_retention();
    assert!(
        svc.get_subscription(SUB).is_ok(),
        "recent Pull activity must reset the expiration clock"
    );
}

/// A data directory whose `format_version` marker names a version newer
/// than this binary supports refuses to open, with a clear, specific
/// error rather than silent data corruption or a generic failure.
#[test]
fn newer_format_version_marker_fails_open_with_a_clear_error() {
    let dir = tempfile::tempdir().unwrap();
    {
        let kv = FjallKv::open(dir.path(), 8 * 1024 * 1024).unwrap();
        // Overwrite the marker `FjallKv::open` just wrote, with a version
        // one past what this binary supports.
        kv.put(
            "meta",
            b"__open_pubusb_format_version".to_vec(),
            (FORMAT_VERSION + 1).to_be_bytes().to_vec(),
        )
        .unwrap();
        kv.persist(fjall::PersistMode::SyncAll).unwrap();
    }

    match FjallKv::open(dir.path(), 8 * 1024 * 1024) {
        Err(OpenError::UnsupportedFormatVersion {
            found, supported, ..
        }) => {
            assert_eq!(found, FORMAT_VERSION + 1);
            assert_eq!(supported, FORMAT_VERSION);
        }
        other => panic!(
            "expected OpenError::UnsupportedFormatVersion, got: {}",
            other.is_ok()
        ),
    }
}
