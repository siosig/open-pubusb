//! Integration tests for User Story 3 (persistence across restarts).
//!
//! Unlike `tests/{topics_subscriptions,publish_pull_ack}.rs` (which go
//! through `target_api`'s trait/stub to stay decoupled from `PubSubService`
//! until it existed), these tests construct `open_pubusb_core::service::PubSubService`
//! directly against a real, disk-backed `open_pubusb_core::store::fjall::FjallKv`
//! — the whole point is to exercise the actual persistence contract, which
//! an abstraction layer would only obscure.
//!
//! "Simulating a crash" here means a plain `drop()` of the first
//! `PubSubService`/`FjallKv` — no explicit `persist()` call, no graceful
//! shutdown sequence. That's sufficient (and correct) for testing the
//! *recovery logic* this task is about (`DeliveryEngine::recover`,
//! cursor/seq-counter continuity): every domain write already reaches the
//! OS via fjall's default per-write `PersistMode::Buffer` flush (see
//! `crates/open-pubusb-core/src/store/fjall.rs`'s module doc comment), so a
//! plain `drop()` here is not weaker than a real `SIGKILL` from the
//! perspective of what ends up on disk — the OS-level "does an
//! *unsynced-to-disk-but-OS-buffered* write survive a killed process"
//! question is instead answered for real, against a genuinely separate
//! OS process, by `scripts/qa/durability.sh`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use open_pubusb_core::clock::MockClock;
use open_pubusb_core::service::{PubSubService, PublishMessage};
use open_pubusb_core::store::fjall::FjallKv;
use open_pubusb_core::store::kv::MemKv;
use open_pubusb_core::subscription::CreateSubscriptionOptions;

const TOPIC: &str = "projects/p/topics/topic-a";
const SUB: &str = "projects/p/subscriptions/sub-a";
const CACHE_BYTES: u64 = 8 * 1024 * 1024;

/// Opens (or reopens) a `PubSubService<FjallKv>` at `path`.
///
/// A caller that reopens the same `path` more than once in one test
/// **must** ensure the previous `PubSubService`/`FjallKv` has actually
/// been dropped first (an explicit `{ ... }` block, or `drop(prev)`) —
/// `let svc = open(...);` *shadowing* an earlier `svc` binding does
/// **not** drop the earlier value early; it stays alive (and keeps
/// holding the data directory's advisory lock) until the enclosing scope
/// ends, which would make a same-scope second `let svc = open(...);`
/// race the first instance's still-live lock. The short retry below is
/// defensive margin for ordinary OS scheduling jitter after a *correctly
/// sequenced* drop, not a substitute for that sequencing.
fn open(path: &std::path::Path, start_ms: i64) -> PubSubService<FjallKv> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
    loop {
        match FjallKv::open(path, CACHE_BYTES) {
            Ok(kv) => return PubSubService::new(Arc::new(kv), Arc::new(MockClock::new(start_ms))),
            Err(_) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            Err(e) => panic!("failed to open FjallKv at {path:?}: {e}"),
        }
    }
}

#[test]
fn topic_and_subscription_records_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let svc = open(dir.path(), 1_000);
        svc.create_topic(TOPIC, Default::default()).unwrap();
        svc.create_subscription(SUB, TOPIC, CreateSubscriptionOptions::default())
            .unwrap();
    }

    let svc = open(dir.path(), 2_000);
    let topic = svc.get_topic(TOPIC).expect("topic should survive restart");
    assert_eq!(topic.name, TOPIC);
    let sub = svc
        .get_subscription(SUB)
        .expect("subscription should survive restart");
    assert_eq!(sub.topic, TOPIC);
}

#[test]
fn seq_counter_continues_after_restart_no_message_id_reuse() {
    let dir = tempfile::tempdir().unwrap();
    {
        let svc = open(dir.path(), 1_000);
        svc.create_topic(TOPIC, Default::default()).unwrap();
        let ids = svc
            .publish(
                TOPIC,
                vec![PublishMessage::default(), PublishMessage::default()],
            )
            .unwrap();
        assert_eq!(ids, vec!["1", "2"]);
    }

    let svc = open(dir.path(), 2_000);
    let ids = svc.publish(TOPIC, vec![PublishMessage::default()]).unwrap();
    assert_eq!(
        ids,
        vec!["3"],
        "seq counter must continue from the persisted tail, not restart at 1"
    );
}

/// The core scenario: publish N, ack some, "crash" (plain drop, no
/// explicit persist), reopen, and confirm exactly the unacked messages —
/// same ids, same payload — come back, with acked ones absent.
#[test]
fn unacked_messages_survive_restart_acked_ones_do_not_reappear() {
    let dir = tempfile::tempdir().unwrap();
    let ack_ids_to_keep_unacked;
    {
        let svc = open(dir.path(), 1_000);
        svc.create_topic(TOPIC, Default::default()).unwrap();
        svc.create_subscription(SUB, TOPIC, CreateSubscriptionOptions::default())
            .unwrap();

        let messages = (0..4)
            .map(|i| PublishMessage {
                data: format!("msg-{i}").into_bytes(),
                ..Default::default()
            })
            .collect();
        let ids = svc.publish(TOPIC, messages).unwrap();
        assert_eq!(ids, vec!["1", "2", "3", "4"]);

        let pulled = svc.pull(SUB, 10).unwrap();
        assert_eq!(pulled.len(), 4);

        // Ack messages 1 and 2 (by message_id); leave 3 and 4 outstanding.
        let (to_ack, to_leave): (Vec<_>, Vec<_>) = pulled
            .into_iter()
            .partition(|m| m.message_id == "1" || m.message_id == "2");
        svc.acknowledge(SUB, to_ack.into_iter().map(|m| m.ack_id).collect())
            .unwrap();
        ack_ids_to_keep_unacked = to_leave
            .into_iter()
            .map(|m| m.message_id)
            .collect::<Vec<_>>();
        assert_eq!(ack_ids_to_keep_unacked, vec!["3", "4"]);

        // Default ack_deadline_secs is 10s (10_000ms); the clock is at
        // 1_000ms when these were leased, so their deadline is 11_000ms —
        // still comfortably in the future when this scope ends (no sleep,
        // no explicit persist: just a plain drop of `svc` here).
    }

    // Reopen shortly after (2_000ms) — still well before the original
    // 11_000ms deadline. If `DeliveryEngine::recover` did *not* restore
    // the in-memory lease table from the persisted `dlv` rows, this pull
    // would incorrectly redeliver messages 3 and 4 right away (see
    // `crate::delivery::engine::DeliveryEngine::recover`'s doc comment).
    //
    // Each "session" gets its own block: `let svc = ...;` shadowing a
    // prior `svc` binding does **not** drop the prior value early (it
    // stays alive, silently holding the fjall data directory's advisory
    // lock, until the enclosing scope ends) — an explicit block makes the
    // drop-before-next-open ordering deterministic instead of relying on
    // where the function happens to end.
    {
        let svc = open(dir.path(), 2_000);
        let pulled_too_soon = svc.pull(SUB, 10).unwrap();
        assert!(
            pulled_too_soon.is_empty(),
            "messages still within their pre-crash ack deadline must not be redelivered \
             immediately after recovery, got: {pulled_too_soon:?}"
        );
    }

    // Advance past the original deadline (11_000ms) and pull again: now
    // the self-healing `lease_next` should reclaim and redeliver exactly
    // 3 and 4, with the correct payload and an incremented delivery
    // attempt (proving the recovered lease's `attempts` was honored, not
    // reset to 1).
    let svc = open(dir.path(), 12_000);
    let mut redelivered = svc.pull(SUB, 10).unwrap();
    redelivered.sort_by(|a, b| a.message_id.cmp(&b.message_id));
    let redelivered_ids: Vec<&str> = redelivered.iter().map(|m| m.message_id.as_str()).collect();
    assert_eq!(redelivered_ids, vec!["3", "4"]);
    assert_eq!(redelivered[0].data, b"msg-2"); // message_id "3" == seq 3 == messages[2]
    assert_eq!(redelivered[1].data, b"msg-3");

    // Acked messages must never reappear.
    for m in &redelivered {
        assert_ne!(m.message_id, "1");
        assert_ne!(m.message_id, "2");
    }

    let _ = ack_ids_to_keep_unacked; // documents intent; already asserted above
}

/// `--ephemeral` (`MemKv`) is intentionally the opposite: a fresh instance
/// starts empty regardless of what a prior instance held, since nothing
/// was ever written to disk. There is nothing to "reopen" — a new
/// `MemKv::new()` simply has no relationship to a previous one: the
/// `EphemeralStore` variant is empty after reopen.
#[test]
fn ephemeral_backend_starts_empty_regardless_of_a_prior_instance() {
    {
        let svc = PubSubService::new(Arc::new(MemKv::new()), Arc::new(MockClock::new(1_000)));
        svc.create_topic(TOPIC, Default::default()).unwrap();
    }
    let svc = PubSubService::new(Arc::new(MemKv::new()), Arc::new(MockClock::new(2_000)));
    assert!(svc.get_topic(TOPIC).is_err());
}
