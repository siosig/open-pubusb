//! Message-retention sweep.
//!
//! ## Design: active sweep, not a fjall compaction filter
//!
//! The original design called for a fjall *compaction filter* on the `msg`
//! keyspace. This module instead performs an active, deterministic sweep
//! ([`sweep_expired_messages`]) invoked periodically
//! (`delivery.retention_sweep_interval_secs`, `crates/open-pubusb/src/main.rs`).
//! Two reasons:
//!
//! - A compaction filter only exists on [`crate::store::fjall::FjallKv`] —
//!   [`crate::store::kv::MemKv`] has no compaction concept at all, so
//!   retention behavior would silently differ between the ephemeral and
//!   persistent backends. An active sweep against the [`crate::store::kv::KvStore`]
//!   trait works identically on both.
//! - Compaction timing is opaque and not caller-controllable, which is
//!   awkward to assert against deterministically in a test — the
//!   expectation is that messages past retention are not delivered and
//!   `approx_size` shrinks *after sweep*, i.e. at a specific, callable
//!   moment, which is exactly what an active sweep provides and a
//!   compaction filter does not.
//!
//! ## What triggers deletion
//!
//! Every [`crate::store::codec::MessageRecord`] already carries its own
//! `expire_at_ms` (`publish time + retention_secs`, computed once at
//! publish time in `crate::topic::TopicStore::append` from the *topic's*
//! `message_retention_secs`) — that field alone is authoritative: once
//! `now_ms >= expire_at_ms`, the message is deleted unconditionally,
//! regardless of ack state. This automatically satisfies
//! `retain_acked_messages = true` ("keep acked messages around for Seek")
//! for free — an acked message is never deleted *before* `expire_at_ms`
//! either way, since nothing in this sweep deletes early. Deleting a
//! `retain_acked_messages = false` subscription's fully-acked messages
//! *before* `expire_at_ms` (a storage-reclaim optimization real GCP
//! Pub/Sub performs) is intentionally not implemented — correctness here
//! does not depend on it, and Seek (which is what `retain_acked_messages`
//! exists for) is a later user story (US5) in any case.

use crate::store::codec::MessageRecord;
use crate::store::keys;
use crate::store::kv::KvStore;
use crate::subscription::SubscriptionStore;

const MSG: &str = "msg";
const DLV: &str = "dlv";

/// Result of one [`sweep_expired_messages`] pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionStats {
    /// Number of `msg` rows deleted for having passed their `expire_at_ms`.
    pub messages_expired: u64,
}

/// Deletes every message past its `expire_at_ms`, across every topic, and
/// the stale `dlv` rows (if any) that referenced them. Safe to call
/// repeatedly (idempotent — a message already deleted is simply not found
/// again on the next pass) and cheap to call often on a small store, but
/// intended to run on `delivery.retention_sweep_interval_secs`
/// (`crates/open-pubusb/src/main.rs`), not per-request.
pub fn sweep_expired_messages<K: KvStore>(
    kv: &K,
    subs: &SubscriptionStore<K>,
    now_ms: i64,
) -> RetentionStats {
    let mut stats = RetentionStats::default();

    for (key, value) in kv.scan_prefix(MSG, &[]) {
        let Some((topic_id, seq)) = keys::parse_msg_key(&key) else {
            continue;
        };
        let Ok(record) = MessageRecord::decode(&value) else {
            continue;
        };
        if record.expire_at_ms > now_ms {
            continue;
        }
        if kv.delete(MSG, &key).is_err() {
            // Leave it for the next pass rather than losing the count —
            // `messages_expired` should reflect rows actually removed.
            continue;
        }
        stats.messages_expired += 1;

        if let Ok(sub_ids) = subs.list_by_topic_id(topic_id) {
            for sub_id in sub_ids {
                let _ = kv.delete(DLV, &keys::delivery_key(sub_id, seq));
                if let Ok(sub_record) = subs.get_by_id(sub_id) {
                    crate::metrics::record_expired(&sub_record.name);
                }
            }
        }
    }

    stats
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::store::kv::MemKv;
    use crate::subscription::CreateSubscriptionOptions;
    use std::sync::Arc;

    fn put_message(kv: &MemKv, topic_id: u64, seq: u64, expire_at_ms: i64) {
        let record = MessageRecord {
            publish_ts_ms: 0,
            ordering_key: String::new(),
            attributes: Default::default(),
            payload: b"x".to_vec(),
            expire_at_ms,
        };
        kv.put(MSG, keys::msg_key(topic_id, seq), record.encode())
            .unwrap();
    }

    #[test]
    fn expired_message_is_deleted_and_counted() {
        let kv = MemKv::new();
        put_message(&kv, 1, 1, 1_000);
        let subs = SubscriptionStore::new(Arc::new(MemKv::new()));

        let stats = sweep_expired_messages(&kv, &subs, 1_000);
        assert_eq!(stats.messages_expired, 1);
        assert_eq!(kv.get(MSG, &keys::msg_key(1, 1)), None);
    }

    #[test]
    fn not_yet_expired_message_survives() {
        let kv = MemKv::new();
        put_message(&kv, 1, 1, 5_000);
        let subs = SubscriptionStore::new(Arc::new(MemKv::new()));

        let stats = sweep_expired_messages(&kv, &subs, 1_000);
        assert_eq!(stats.messages_expired, 0);
        assert!(kv.get(MSG, &keys::msg_key(1, 1)).is_some());
    }

    #[test]
    fn acked_message_with_retain_acked_messages_survives_until_expiry() {
        // The sweep doesn't even look at ack state — this test documents
        // that behavior directly: an "acked" message (there is no ack
        // bookkeeping on `MessageRecord` itself, so this is really just
        // re-asserting `not_yet_expired_message_survives` under the name
        // this module's doc comment promises) is untouched before
        // `expire_at_ms`, satisfying `retain_acked_messages = true`.
        let kv = MemKv::new();
        put_message(&kv, 1, 1, 5_000);
        let sub_kv = Arc::new(MemKv::new());
        let subs = SubscriptionStore::new(sub_kv.clone());
        subs.create(
            "projects/p/subscriptions/sub-a",
            "projects/p/topics/topic-a",
            1,
            0,
            0,
            CreateSubscriptionOptions {
                retain_acked_messages: true,
                ..Default::default()
            },
        )
        .unwrap();

        let stats = sweep_expired_messages(&kv, &subs, 1_000);
        assert_eq!(stats.messages_expired, 0);
        assert!(kv.get(MSG, &keys::msg_key(1, 1)).is_some());
    }

    #[test]
    fn expiring_a_message_cleans_up_its_dlv_row() {
        let kv = Arc::new(MemKv::new());
        let subs = SubscriptionStore::new(kv.clone());
        let sub = subs
            .create(
                "projects/p/subscriptions/sub-a",
                "projects/p/topics/topic-a",
                1,
                0,
                0,
                CreateSubscriptionOptions::default(),
            )
            .unwrap();
        put_message(&kv, 1, 1, 1_000);
        kv.put(
            DLV,
            keys::delivery_key(sub.id, 1),
            b"leftover-lease".to_vec(),
        )
        .unwrap();

        let stats = sweep_expired_messages(&*kv, &subs, 1_000);
        assert_eq!(stats.messages_expired, 1);
        assert_eq!(kv.get(DLV, &keys::delivery_key(sub.id, 1)), None);
    }
}
