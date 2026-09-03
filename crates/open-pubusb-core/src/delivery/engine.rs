//! `DeliveryEngine`: leases messages out of a topic's message log to a
//! subscription's pull cursor, and applies Ack / ModifyAckDeadline against
//! those leases.
//!
//! Core scope: plain at-least-once Pull delivery with ack-deadline expiry
//! and redelivery. Ordering keys, attribute filters, dead-lettering, and
//! retry-policy backoff layer on top of this without changing the shapes
//! here.
//!
//! ## Attempt counting: a deliberate simplification
//!
//! The original design called for an expiry
//! task on `lease_scan_interval_ms` incrementing attempts. This module
//! instead makes [`DeliveryEngine::lease_next`] self-healing: when it scans
//! past a seq whose in-memory lease has already passed its deadline, it
//! reclaims that lease and increments `attempts` right there, rather than
//! waiting for a separate periodic sweep to do it first. This means a
//! single-threaded, fully-synchronous caller (as every integration test in
//! this crate is) observes correct expiry/redelivery/`delivery_attempt`
//! behavior without needing a real clock or a background task running
//! concurrently.
//!
//! A periodic background sweep ([`DeliveryEngine::sweep_expired`]) still
//! exists and is still wired up in production (`crates/open-pubusb/src/server.rs`)
//! — its job under this design is garbage-collecting in-memory
//! leases nobody has re-pulled in a long time (bounding `LeaseTable`
//! memory) and, in a future task, driving dead-letter transitions once
//! `attempts` exceeds a subscription's policy. It must not double-count
//! `attempts` for a seq that `lease_next` has already reclaimed, so it
//! only acts on leases still present in the in-memory table (an expired
//! lease `lease_next` already reclaimed is gone from the table by the time
//! the sweep runs).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use crate::delivery::lease::{decode_ack_id, AckOutcome, LeaseTable};
use crate::delivery::stream::{StreamInfo, StreamRegistry};
use crate::error::{Error, Result};
use crate::store::codec::{CursorRecord, DeliveryRecord, MessageRecord};
use crate::store::keys;
use crate::store::kv::KvStore;

const SUB: &str = "sub";
const DLV: &str = "dlv";
const MSG: &str = "msg";

/// One message handed back to a caller of [`DeliveryEngine::lease_next`].
#[derive(Debug, Clone)]
pub struct Delivered {
    /// Position in the topic's message log this delivery is for.
    pub seq: u64,
    /// Opaque token the client must present to Ack or ModifyAckDeadline
    /// this specific delivery.
    pub ack_id: String,
    /// The message payload/attributes/ordering key/publish time being
    /// delivered.
    pub message: MessageRecord,
    /// 1 + prior redeliveries of this message on this subscription.
    /// Whether to expose this to the client at all is the caller's call
    /// (only surfaced when the subscription has a
    /// dead-letter policy; `0` otherwise) — this field always carries the
    /// true count, uniformly.
    pub delivery_attempt: u32,
}

/// What [`DeliveryEngine::lease_next`]'s reclaim path needs from a
/// subscription's record — see [`DeliveryEngine::reclaim_policy`].
struct ReclaimPolicy {
    subscription_full_name: String,
    min_retry_backoff_secs: i64,
    max_retry_backoff_secs: i64,
    retry_policy_explicit: bool,
    dead_letter: crate::delivery::dead_letter::DeadLetterPolicy,
}

/// Leases messages out of topic message logs to subscription pull cursors.
/// Generic over [`KvStore`] so it works against [`crate::store::kv::MemKv`]
/// today and a real persistent store later without any call-site changes.
pub struct DeliveryEngine<K: KvStore> {
    kv: Arc<K>,
    leases: Mutex<LeaseTable>,
    waiters: Mutex<HashMap<u64, Arc<Notify>>>,
    streams: StreamRegistry,
    /// `sub_id -> Some(compiled filter)` or `sub_id -> None` ("no
    /// filter") — populated lazily on first use by [`Self::compiled_filter`]
    /// rather than eagerly at create/recovery time: a filter is immutable
    /// after creation, so "compile once, reuse forever" is equally correct
    /// either way, and lazy population handles the create-time and
    /// post-recovery cases uniformly with one code path instead of two.
    filters: Mutex<HashMap<u64, Option<crate::filter::CompiledFilter>>>,
}

impl<K: KvStore> DeliveryEngine<K> {
    /// Constructs an engine over `kv` with empty in-memory state — call
    /// [`Self::recover`] afterward to rebuild the lease table from
    /// whatever `kv` already persisted.
    pub fn new(kv: Arc<K>) -> Self {
        Self {
            kv,
            leases: Mutex::new(LeaseTable::new()),
            waiters: Mutex::new(HashMap::new()),
            streams: StreamRegistry::new(),
            filters: Mutex::new(HashMap::new()),
        }
    }

    /// Returns `sub_id`'s compiled filter (`None` = no filter), compiling
    /// and caching it on first use. A subscription whose record can't be
    /// read/decoded (e.g. it was deleted concurrently) is treated as
    /// having no filter — `lease_next` will simply find nothing to lease
    /// for it via the normal cursor/message lookup either way.
    fn compiled_filter(&self, sub_id: u64) -> Option<crate::filter::CompiledFilter> {
        {
            let cache = self.lock_filters();
            if let Some(cached) = cache.get(&sub_id) {
                return cached.clone();
            }
        }
        let filter_str = self
            .kv
            .get("meta", &keys::sub_key(sub_id))
            .and_then(|bytes| {
                serde_json::from_slice::<crate::subscription::SubscriptionRecord>(&bytes).ok()
            })
            .map(|record| record.filter)
            .unwrap_or_default();
        let compiled = crate::filter::compile(&filter_str).ok().flatten();
        self.lock_filters().insert(sub_id, compiled.clone());
        compiled
    }

    /// Everything [`Self::lease_next`]'s reclaim path needs from
    /// `sub_id`'s [`crate::subscription::SubscriptionRecord`] — retry
    /// backoff bounds and dead-letter policy — read
    /// together in one `kv` lookup. Falls back to the create-time retry
    /// defaults (10s/600s, `retry_policy_explicit = false`) and "no
    /// dead-letter policy" if the record can't be read (e.g. deleted
    /// concurrently — `lease_next` will find nothing to lease for it via
    /// the cursor/message lookup either way).
    ///
    /// Deliberately *not* cached like [`Self::compiled_filter`] — unlike
    /// `filter`, both of these are mutable after creation
    /// (`UpdateSubscription`), and this is read only once per
    /// self-healing reclaim (not once per message scanned), so a fresh
    /// read each time is cheap enough to just be correct by construction
    /// instead of needing cache invalidation.
    fn reclaim_policy(&self, sub_id: u64) -> ReclaimPolicy {
        self.kv
            .get("meta", &keys::sub_key(sub_id))
            .and_then(|bytes| {
                serde_json::from_slice::<crate::subscription::SubscriptionRecord>(&bytes).ok()
            })
            .map(|record| ReclaimPolicy {
                subscription_full_name: record.name,
                min_retry_backoff_secs: record.min_retry_backoff_secs,
                max_retry_backoff_secs: record.max_retry_backoff_secs,
                retry_policy_explicit: record.retry_policy_explicit,
                dead_letter: crate::delivery::dead_letter::DeadLetterPolicy {
                    topic: record.dead_letter_topic,
                    max_delivery_attempts: record.max_delivery_attempts,
                },
            })
            .unwrap_or(ReclaimPolicy {
                subscription_full_name: String::new(),
                min_retry_backoff_secs: 10,
                max_retry_backoff_secs: 600,
                retry_policy_explicit: false,
                dead_letter: crate::delivery::dead_letter::DeadLetterPolicy {
                    topic: None,
                    max_delivery_attempts: 0,
                },
            })
    }

    /// Appends `message` (with the `CloudPubSubDeadLetterSource*`
    /// attributes added) to `dead_letter_topic_name`'s log.
    /// Returns `Ok(false)` (not `Err`) when that topic doesn't exist — a
    /// missing DLQ topic means keep + warn: the
    /// caller must fall back to leasing the message normally rather than
    /// losing it.
    fn forward_to_dead_letter_topic(
        &self,
        dead_letter_topic_name: &str,
        message: &MessageRecord,
        policy: &ReclaimPolicy,
        delivery_count: u32,
        now_ms: i64,
    ) -> Result<bool> {
        let topics = crate::topic::TopicStore::new(Arc::clone(&self.kv));
        let dlq = match topics.get(dead_letter_topic_name) {
            Ok(record) => record,
            Err(Error::NotFound { .. }) => {
                tracing::warn!(
                    subscription = %policy.subscription_full_name,
                    dead_letter_topic = %dead_letter_topic_name,
                    "dead-letter topic not found; keeping message on the source subscription"
                );
                return Ok(false);
            }
            Err(e) => return Err(e),
        };
        let attributes = crate::delivery::dead_letter::build_attributes(
            &message.attributes,
            delivery_count,
            &policy.subscription_full_name,
            message.publish_ts_ms,
        );
        let retention_secs = if dlq.message_retention_secs > 0 {
            dlq.message_retention_secs
        } else {
            crate::limits::MAX_TOPIC_RETENTION_SECS
        };
        topics.append(
            dlq.id,
            vec![(
                message.payload.clone(),
                attributes,
                message.ordering_key.clone(),
            )],
            now_ms,
            retention_secs,
        )?;
        crate::metrics::record_dead_lettered(&policy.subscription_full_name);
        Ok(true)
    }

    fn lock_filters(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<u64, Option<crate::filter::CompiledFilter>>> {
        match self.filters.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Returns (creating if needed) the [`Notify`] that
    /// [`Self::notify_published`] signals for `sub_id`. Callers (the async
    /// gRPC/REST Pull handlers) `.notified()` on this while waiting for new
    /// messages instead of polling.
    pub fn waiter(&self, sub_id: u64) -> Arc<Notify> {
        let mut waiters = self.lock_waiters();
        waiters.entry(sub_id).or_default().clone()
    }

    /// Wakes anyone waiting on [`Self::waiter`] for every subscription in
    /// `sub_ids` (called after a successful Publish fans a message out to
    /// its subscriptions).
    pub fn notify_published(&self, sub_ids: &[u64]) {
        let waiters = self.lock_waiters();
        for sub_id in sub_ids {
            if let Some(notify) = waiters.get(sub_id) {
                notify.notify_waiters();
            }
        }
    }

    /// Leases up to `max_messages` currently-deliverable messages from
    /// `sub_id`'s topic (`topic_id`) log, starting just past its cursor.
    /// Returns promptly with however many are available right now (`0` if
    /// none) — this method never blocks; a caller wanting to wait for new
    /// messages does so via [`Self::waiter`] around a call to this method.
    pub fn lease_next(
        &self,
        sub_id: u64,
        topic_id: u64,
        max_messages: i32,
        now_ms: i64,
        ack_deadline_secs: i32,
    ) -> Result<Vec<Delivered>> {
        let max_messages = max_messages.max(0) as usize;
        if max_messages == 0 {
            return Ok(Vec::new());
        }

        let mut cursor = self.load_cursor(sub_id)?;
        let mut cursor_changed = false;
        let filter = self.compiled_filter(sub_id);
        let mut leases = self.lock_leases();
        let mut out = Vec::new();

        let prefix = keys::msg_key_prefix(topic_id);
        for (key, value) in self.kv.scan_prefix(MSG, &prefix) {
            if out.len() >= max_messages {
                break;
            }
            let Some((_, seq)) = keys::parse_msg_key(&key) else {
                continue;
            };
            if seq <= cursor.acked_floor || cursor.acked_above_floor.contains(&seq) {
                continue;
            }

            let Ok(message) = MessageRecord::decode(&value) else {
                continue;
            };
            if let Some(filter) = &filter {
                if !filter.matches(&message.attributes) {
                    // Filtered -> Acked — never delivered,
                    // never leased, immediately marked done so it's
                    // skipped on every future scan too.
                    mark_done(&mut cursor, seq);
                    cursor_changed = true;
                    continue;
                }
            }

            let mut prior_attempts = 0u32;
            if let Some(lease) = leases.get(sub_id, seq) {
                if lease.deadline_ms > now_ms {
                    // Still genuinely in flight to another puller.
                    continue;
                }
                // Ack deadline has passed. retry_policy
                // governs *this* path specifically — automatic
                // redelivery after ack-deadline expiry, and only when a
                // client explicitly configured retry_policy
                // (`retry_policy_explicit`) — not an explicit client Nack
                // (`ModifyAckDeadline(0)`, handled entirely separately in
                // `Self::modify_ack_deadline`/`LeaseTable::extend`, which
                // the proto contract requires to make the message
                // "immediately available", full stop, regardless of
                // retry_policy, per pubsub.proto's
                // `StreamingPullRequest.modify_deadline_seconds` doc
                // comment) and not a subscription that never configured
                // retry_policy at all (whose default 10s/600s bounds
                // would otherwise silently turn "redeliver once the ack
                // deadline passes" into "redeliver 10s after that").
                let policy = self.reclaim_policy(sub_id);
                if policy.retry_policy_explicit {
                    // Anchor the backoff to the deadline that just
                    // passed, not `now_ms`, so a caller polling less
                    // often than the backoff window doesn't get an
                    // artificially shorter effective backoff.
                    let backoff = crate::delivery::retry::backoff_for_attempts(
                        lease.attempts,
                        policy.min_retry_backoff_secs,
                        policy.max_retry_backoff_secs,
                    );
                    let eligible_at = lease.deadline_ms.saturating_add(backoff.as_millis() as i64);
                    if now_ms < eligible_at {
                        // Expired, but still within its retry backoff
                        // window — leave the lease exactly as-is (don't
                        // reclaim yet); the next scan re-checks the same
                        // condition.
                        continue;
                    }
                }
                // Past its ack deadline (and, if configured, its retry
                // backoff). Dead-letter instead of reclaiming, if
                // this delivery would exceed the policy's threshold.
                if crate::delivery::dead_letter::should_dead_letter(
                    lease.attempts,
                    &policy.dead_letter,
                ) {
                    // `policy.dead_letter.topic` is `Some` here per
                    // `should_dead_letter`'s contract.
                    let dlq_topic = policy.dead_letter.topic.clone().unwrap_or_default();
                    let forwarded = self.forward_to_dead_letter_topic(
                        &dlq_topic,
                        &message,
                        &policy,
                        lease.attempts,
                        now_ms,
                    )?;
                    if forwarded {
                        leases.remove(sub_id, seq);
                        self.kv.delete(DLV, &keys::delivery_key(sub_id, seq))?;
                        mark_done(&mut cursor, seq);
                        cursor_changed = true;
                        continue;
                    }
                    // Missing DLQ topic — fall through and reclaim
                    // normally instead of losing the message.
                }
                // Reclaim it right here (see module doc comment).
                leases.remove(sub_id, seq);
                prior_attempts = lease.attempts;
            } else if let Some(persisted) = self.persisted_delivery_record(sub_id, seq) {
                // No in-memory lease (e.g. `sweep_expired` already
                // dropped it for memory GC), but a `dlv` row survives —
                // apply the same backoff gate using the persisted
                // deadline/attempts, so memory GC can never silently
                // bypass retry_policy's backoff.
                let policy = self.reclaim_policy(sub_id);
                if policy.retry_policy_explicit {
                    let backoff = crate::delivery::retry::backoff_for_attempts(
                        persisted.attempts,
                        policy.min_retry_backoff_secs,
                        policy.max_retry_backoff_secs,
                    );
                    let eligible_at = persisted
                        .deadline_ms
                        .saturating_add(backoff.as_millis() as i64);
                    if now_ms < eligible_at {
                        continue;
                    }
                }
                if crate::delivery::dead_letter::should_dead_letter(
                    persisted.attempts,
                    &policy.dead_letter,
                ) {
                    let dlq_topic = policy.dead_letter.topic.clone().unwrap_or_default();
                    let forwarded = self.forward_to_dead_letter_topic(
                        &dlq_topic,
                        &message,
                        &policy,
                        persisted.attempts,
                        now_ms,
                    )?;
                    if forwarded {
                        self.kv.delete(DLV, &keys::delivery_key(sub_id, seq))?;
                        mark_done(&mut cursor, seq);
                        cursor_changed = true;
                        continue;
                    }
                }
                prior_attempts = persisted.attempts;
            }

            let attempts = prior_attempts.saturating_add(1);
            let deadline_ms = now_ms.saturating_add(i64::from(ack_deadline_secs.max(0)) * 1000);
            let ack_id = leases.insert(sub_id, seq, deadline_ms, attempts, u64::from(attempts));
            self.kv.put(
                DLV,
                keys::delivery_key(sub_id, seq),
                DeliveryRecord {
                    deadline_ms,
                    attempts,
                    generation: u64::from(attempts),
                }
                .encode(),
            )?;

            out.push(Delivered {
                seq,
                ack_id,
                message,
                delivery_attempt: attempts,
            });
        }

        if cursor_changed {
            self.save_cursor(sub_id, &cursor)?;
        }

        Ok(out)
    }

    /// Applies Acknowledge for `ack_ids` against `sub_id`'s outstanding
    /// leases. Unknown, stale, or malformed `ack_id`s are silently
    /// ignored — never an error.
    pub fn acknowledge(&self, sub_id: u64, ack_ids: &[String]) -> Result<()> {
        if ack_ids.is_empty() {
            return Ok(());
        }
        let mut cursor = self.load_cursor(sub_id)?;
        let mut leases = self.lock_leases();
        let mut cursor_changed = false;

        for ack_id in ack_ids {
            if let AckOutcome::Applied(lease) = leases.ack(ack_id) {
                self.kv
                    .delete(DLV, &keys::delivery_key(sub_id, lease.seq))?;
                mark_done(&mut cursor, lease.seq);
                cursor_changed = true;
            }
        }

        if cursor_changed {
            self.save_cursor(sub_id, &cursor)?;
        }
        Ok(())
    }

    /// Applies ModifyAckDeadline for `ack_ids` against `sub_id`'s
    /// outstanding leases. `seconds <= 0` is an immediate nack: the next
    /// [`Self::lease_next`] call (from anyone) will reclaim and redeliver
    /// it right away. Unknown/stale/malformed `ack_id`s are ignored, same
    /// as [`Self::acknowledge`].
    pub fn modify_ack_deadline(
        &self,
        sub_id: u64,
        ack_ids: &[String],
        now_ms: i64,
        seconds: i32,
    ) -> Result<()> {
        if ack_ids.is_empty() {
            return Ok(());
        }
        let mut leases = self.lock_leases();
        for ack_id in ack_ids {
            if let AckOutcome::Applied(lease) = leases.extend(ack_id, now_ms, seconds) {
                self.kv.put(
                    DLV,
                    keys::delivery_key(sub_id, lease.seq),
                    DeliveryRecord {
                        deadline_ms: lease.deadline_ms,
                        attempts: lease.attempts,
                        generation: lease.generation,
                    }
                    .encode(),
                )?;
            }
        }
        Ok(())
    }

    // -- StreamingPull sessions -------------------------------------------

    /// Opens a new `StreamingPull` session and returns its id.
    pub fn open_stream(
        &self,
        sub_id: u64,
        topic_id: u64,
        ack_deadline_secs: i32,
        max_outstanding_messages: i64,
        max_outstanding_bytes: i64,
    ) -> u64 {
        self.streams.open(StreamInfo {
            sub_id,
            topic_id,
            ack_deadline_secs,
            max_outstanding_messages,
            max_outstanding_bytes,
        })
    }

    /// Updates a stream's ack deadline / flow-control budget (a
    /// `StreamingPullRequest` after the first one may change these).
    pub fn update_stream(
        &self,
        stream_id: u64,
        ack_deadline_secs: Option<i32>,
        max_outstanding_messages: Option<i64>,
        max_outstanding_bytes: Option<i64>,
    ) {
        self.streams.update(
            stream_id,
            ack_deadline_secs,
            max_outstanding_messages,
            max_outstanding_bytes,
        );
    }

    /// Leases up to `stream_id`'s remaining flow-control budget worth of
    /// currently-deliverable messages. Returns `Err(Error::NotFound)` if
    /// `stream_id` is unknown (already closed).
    pub fn lease_for_stream(&self, stream_id: u64, now_ms: i64) -> Result<Vec<Delivered>> {
        let info = self
            .streams
            .info(stream_id)
            .ok_or_else(|| Error::NotFound {
                resource: format!("stream_id={stream_id}"),
            })?;
        let (remaining_msgs, remaining_bytes) = self.streams.remaining_budget(stream_id);
        if remaining_msgs <= 0 || remaining_bytes <= 0 {
            return Ok(Vec::new());
        }
        let max_messages = remaining_msgs.min(i64::from(i32::MAX)) as i32;
        let delivered = self.lease_next(
            info.sub_id,
            info.topic_id,
            max_messages,
            now_ms,
            info.ack_deadline_secs,
        )?;
        for d in &delivered {
            self.streams
                .record_leased(stream_id, d.seq, d.message.payload.len());
        }
        Ok(delivered)
    }

    /// Applies Acknowledge for `ack_ids` against `sub_id`'s leases (same
    /// as [`Self::acknowledge`]) and releases each successfully-decoded
    /// seq from `stream_id`'s outstanding budget. A message this server
    /// ever hands out is leased to at most one stream at a time (`lease_next`
    /// never re-leases an already-active seq), so it only needs releasing
    /// from the one `stream_id` that actually holds it — an ack_id for a
    /// seq this stream isn't tracking (e.g. it arrived via unary Ack, or
    /// via a different stream) simply no-ops in
    /// [`crate::delivery::stream::StreamRegistry::record_resolved`].
    pub fn stream_acknowledge(
        &self,
        stream_id: u64,
        sub_id: u64,
        ack_ids: &[String],
    ) -> Result<()> {
        self.acknowledge(sub_id, ack_ids)?;
        for ack_id in ack_ids {
            if let Some((_, seq, _)) = decode_ack_id(ack_id) {
                self.streams.record_resolved(stream_id, seq);
            }
        }
        Ok(())
    }

    /// Applies ModifyAckDeadline for `ack_ids` against `sub_id`'s leases
    /// (same as [`Self::modify_ack_deadline`]). `seconds <= 0` (an
    /// immediate nack) also releases the message from `stream_id`'s
    /// outstanding budget, same as an Ack; extending the deadline
    /// (`seconds > 0`) leaves it outstanding.
    pub fn stream_modify_ack_deadline(
        &self,
        stream_id: u64,
        sub_id: u64,
        ack_ids: &[String],
        now_ms: i64,
        seconds: i32,
    ) -> Result<()> {
        self.modify_ack_deadline(sub_id, ack_ids, now_ms, seconds)?;
        if seconds <= 0 {
            for ack_id in ack_ids {
                if let Some((_, seq, _)) = decode_ack_id(ack_id) {
                    self.streams.record_resolved(stream_id, seq);
                }
            }
        }
        Ok(())
    }

    /// Closes `stream_id` and immediately releases every lease it was
    /// still holding — dropping both the in-memory lease and its
    /// persisted `dlv` row, so those seqs are eligible for redelivery to
    /// anyone right away: the lease expires on client disconnect. A no-op
    /// if `stream_id` is already closed or unknown.
    pub fn on_stream_closed(&self, stream_id: u64) {
        let Some((sub_id, seqs)) = self.streams.close(stream_id) else {
            return;
        };
        if seqs.is_empty() {
            return;
        }
        let mut leases = self.lock_leases();
        for seq in seqs {
            leases.remove(sub_id, seq);
            let _ = self.kv.delete(DLV, &keys::delivery_key(sub_id, seq));
        }
    }

    /// Periodic maintenance: drops any in-memory lease for `sub_id` whose
    /// deadline is more than `grace_ms` past `now_ms`, on the assumption
    /// nobody is actively re-pulling it right now — the persisted `dlv`
    /// row is left as-is (it already carries the last-known `attempts`),
    /// so the next [`Self::lease_next`] call still redelivers it with the
    /// correct count. Returns how many leases were dropped, for metrics.
    ///
    /// This does **not** increment `attempts` (see the module doc comment
    /// for why: `lease_next` already does, exactly once, at the point a
    /// redelivery actually happens).
    pub fn sweep_expired(&self, sub_id: u64, now_ms: i64, grace_ms: i64) -> usize {
        let mut leases = self.lock_leases();
        let expired = leases.expire_up_to(now_ms.saturating_sub(grace_ms));
        let mut dropped = 0;
        for e in expired {
            if e.sub_id == sub_id {
                dropped += 1;
            } else {
                // Not ours to drop this pass; re-insert so another
                // subscription's sweep still finds it. (Leases are keyed
                // per-subscription already, so in practice this branch is
                // unreachable given callers pass their own sub_id, but
                // keep the table consistent regardless of call pattern.)
                leases.insert(e.sub_id, e.seq, now_ms, e.attempts, e.generation);
            }
        }
        dropped
    }

    /// Number of currently-outstanding leases across all subscriptions
    /// (for tests/metrics).
    pub fn lease_count(&self) -> usize {
        self.lock_leases().len()
    }

    /// Rebuilds the in-memory [`LeaseTable`] from every persisted `dlv`
    /// row.
    ///
    /// Everything else this engine relies on — subscription cursors
    /// (`sub`), the topic→subscription index (`idx`), per-topic seq
    /// counters (`meta`) — is read directly from `self.kv` on every call
    /// already (see this module's and `crate::topic`'s doc comments), so
    /// it needs no separate rebuild step: as soon as `self.kv` is backed
    /// by a persistent store, that state simply survives a restart on its
    /// own. `LeaseTable` is the one piece of state this engine keeps
    /// purely in memory — its ack-deadline timers live in memory only —
    /// so it is the only thing recovery needs to reconstruct.
    ///
    /// Without calling this, a fresh (empty) `LeaseTable` after a restart
    /// would make [`Self::lease_next`] treat every still-outstanding,
    /// not-yet-expired delivery as if it had never been leased at all
    /// (since its only "already leased, skip" check is against the
    /// in-memory table) — causing every unacked message to be redelivered
    /// immediately on the next Pull, even ones whose real ack deadline is
    /// still minutes away. Rebuilding the table from the persisted
    /// `deadline_ms`/`attempts`/`generation` restores the correct
    /// in-flight state, and the existing self-healing logic in
    /// [`Self::lease_next`] (see module doc comment) then transparently
    /// reclaims any lease that happens to have expired while the process
    /// was down, the next time something touches it.
    pub fn recover(&self) {
        let mut leases = self.lock_leases();
        for (key, value) in self.kv.scan_prefix(DLV, &[]) {
            let Some((sub_id, seq)) = keys::parse_delivery_key(&key) else {
                continue;
            };
            let Ok(record) = DeliveryRecord::decode(&value) else {
                continue;
            };
            leases.insert(
                sub_id,
                seq,
                record.deadline_ms,
                record.attempts,
                record.generation,
            );
        }
    }

    // -- internal helpers ---------------------------------------------

    fn lock_leases(&self) -> std::sync::MutexGuard<'_, LeaseTable> {
        match self.leases.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn lock_waiters(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Arc<Notify>>> {
        match self.waiters.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn persisted_delivery_record(&self, sub_id: u64, seq: u64) -> Option<DeliveryRecord> {
        self.kv
            .get(DLV, &keys::delivery_key(sub_id, seq))
            .and_then(|bytes| DeliveryRecord::decode(&bytes).ok())
    }

    fn load_cursor(&self, sub_id: u64) -> Result<CursorRecord> {
        let bytes = self
            .kv
            .get(SUB, &keys::cursor_key(sub_id))
            .ok_or_else(|| Error::NotFound {
                resource: format!("subscription cursor for id {sub_id}"),
            })?;
        CursorRecord::decode(&bytes).map_err(|e| Error::Internal {
            message: format!("corrupt cursor record for subscription {sub_id}: {e}"),
        })
    }

    fn save_cursor(&self, sub_id: u64, cursor: &CursorRecord) -> Result<()> {
        self.kv.put(SUB, keys::cursor_key(sub_id), cursor.encode())
    }

    /// A read-only copy of `sub_id`'s current cursor, used by
    /// `create_snapshot` — a Snapshot is defined as exactly this copy, a
    /// copy of this struct.
    pub fn cursor_snapshot(&self, sub_id: u64) -> Result<CursorRecord> {
        self.load_cursor(sub_id)
    }

    /// The publish timestamp (ms since epoch) of the oldest message on
    /// `topic_id` that `sub_id` has not yet fully processed (acked,
    /// filtered-out, or expired) — the smallest seq greater than
    /// `acked_floor` not present in `acked_above_floor`. `Ok(None)` when
    /// nothing is outstanding (every published message has been
    /// processed), in which case a fresh snapshot gets the maximum
    /// lifetime, per the Snapshot `expire_time` rule: created +
    /// (7d − age of the oldest unacked message).
    pub fn oldest_unacked_publish_ts_ms(&self, sub_id: u64, topic_id: u64) -> Result<Option<i64>> {
        let cursor = self.load_cursor(sub_id)?;
        let prefix = keys::msg_key_prefix(topic_id);
        for (key, value) in self.kv.scan_prefix(MSG, &prefix) {
            let Some((_, seq)) = keys::parse_msg_key(&key) else {
                continue;
            };
            if seq <= cursor.acked_floor || cursor.acked_above_floor.contains(&seq) {
                continue;
            }
            let Ok(message) = MessageRecord::decode(&value) else {
                continue;
            };
            return Ok(Some(message.publish_ts_ms));
        }
        Ok(None)
    }

    /// Replaces `sub_id`'s cursor wholesale — the mechanism behind Seek
    /// (`seek_to_snapshot`/`seek_to_time` in
    /// [`crate::delivery::snapshot`]). Invalidates every
    /// in-memory lease and persisted `dlv` row for the subscription first,
    /// so `delivery_attempt` resets to `0` for every message (there is no
    /// leftover attempts record for `lease_next` to find) and no
    /// in-flight lease survives contradicting the new cursor — matching
    /// real Pub/Sub's own documented Seek behavior (outstanding
    /// acks/leases are invalidated).
    pub fn restore_cursor(&self, sub_id: u64, cursor: CursorRecord) -> Result<()> {
        let removed_seqs = self.lock_leases().remove_all_for_subscription(sub_id);
        for seq in removed_seqs {
            self.kv.delete(DLV, &keys::delivery_key(sub_id, seq))?;
        }
        // A `dlv` row can also exist for a seq whose in-memory lease was
        // already GC'd by `sweep_expired` — scan-and-delete every
        // remaining `dlv` row for this subscription directly, not just
        // the ones the in-memory table still knew about.
        let stale: Vec<Vec<u8>> = self
            .kv
            .scan_prefix(DLV, &keys::delivery_key_prefix(sub_id))
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        for key in stale {
            self.kv.delete(DLV, &key)?;
        }
        self.save_cursor(sub_id, &cursor)
    }
}

/// Marks `seq` as done (acked, filtered-out, or expired-past-retention) in
/// `cursor`, advancing `acked_floor` over any now-contiguous run.
fn mark_done(cursor: &mut CursorRecord, seq: u64) {
    if seq <= cursor.acked_floor {
        return; // already done (duplicate ack) — no-op, not an error.
    }
    if seq == cursor.acked_floor + 1 {
        cursor.acked_floor += 1;
        while cursor.acked_above_floor.remove(&(cursor.acked_floor + 1)) {
            cursor.acked_floor += 1;
        }
    } else {
        cursor.acked_above_floor.insert(seq);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::kv::MemKv;

    fn kv() -> Arc<MemKv> {
        Arc::new(MemKv::new())
    }

    /// Writes a subscription cursor directly (bypassing `SubscriptionStore`,
    /// which owns creation) so these tests can exercise the engine in
    /// isolation.
    fn seed_cursor(kv: &Arc<MemKv>, sub_id: u64, attach_seq: u64) {
        let cursor = CursorRecord {
            attach_seq,
            acked_floor: attach_seq,
            acked_above_floor: Default::default(),
        };
        kv.put(SUB, keys::cursor_key(sub_id), cursor.encode())
            .unwrap();
    }

    fn seed_messages(kv: &Arc<MemKv>, topic_id: u64, count: u64, now_ms: i64) {
        for seq in 1..=count {
            let record = MessageRecord {
                publish_ts_ms: now_ms,
                ordering_key: String::new(),
                attributes: Default::default(),
                payload: format!("msg-{seq}").into_bytes(),
                expire_at_ms: now_ms + 7 * 24 * 3600 * 1000,
            };
            kv.put(MSG, keys::msg_key(topic_id, seq), record.encode())
                .unwrap();
        }
    }

    #[test]
    fn lease_next_returns_messages_in_seq_order() {
        let kv = kv();
        seed_cursor(&kv, 1, 0);
        seed_messages(&kv, 1, 3, 1_000);
        let engine = DeliveryEngine::new(kv);

        let delivered = engine.lease_next(1, 1, 10, 2_000, 10).unwrap();
        assert_eq!(delivered.len(), 3);
        let seqs: Vec<u64> = delivered.iter().map(|d| d.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
        assert_eq!(delivered[0].delivery_attempt, 1);
        assert_eq!(delivered[0].message.payload, b"msg-1");
    }

    #[test]
    fn lease_next_respects_max_messages() {
        let kv = kv();
        seed_cursor(&kv, 1, 0);
        seed_messages(&kv, 1, 5, 1_000);
        let engine = DeliveryEngine::new(kv);

        let delivered = engine.lease_next(1, 1, 2, 2_000, 10).unwrap();
        assert_eq!(delivered.len(), 2);
    }

    #[test]
    fn lease_next_skips_messages_before_attach_seq() {
        let kv = kv();
        seed_messages(&kv, 1, 3, 1_000);
        // Subscription attached after seq 1 was published.
        seed_cursor(&kv, 1, 1);
        let engine = DeliveryEngine::new(kv);

        let delivered = engine.lease_next(1, 1, 10, 2_000, 10).unwrap();
        let seqs: Vec<u64> = delivered.iter().map(|d| d.seq).collect();
        assert_eq!(seqs, vec![2, 3]);
    }

    #[test]
    fn lease_next_does_not_redeliver_a_live_lease() {
        let kv = kv();
        seed_cursor(&kv, 1, 0);
        seed_messages(&kv, 1, 1, 1_000);
        let engine = DeliveryEngine::new(kv);

        let first = engine.lease_next(1, 1, 10, 2_000, 10).unwrap();
        assert_eq!(first.len(), 1);
        // Still well within the ack deadline.
        let second = engine.lease_next(1, 1, 10, 2_500, 10).unwrap();
        assert!(second.is_empty());
    }

    #[test]
    fn lease_next_redelivers_after_deadline_with_incremented_attempt() {
        let kv = kv();
        seed_cursor(&kv, 1, 0);
        seed_messages(&kv, 1, 1, 1_000);
        let engine = DeliveryEngine::new(kv);

        let first = engine.lease_next(1, 1, 10, 2_000, 10).unwrap();
        assert_eq!(first[0].delivery_attempt, 1);

        // 11s later: past the 10s ack deadline.
        let second = engine.lease_next(1, 1, 10, 13_000, 10).unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].seq, first[0].seq);
        assert_eq!(second[0].delivery_attempt, 2);
        assert_ne!(second[0].ack_id, first[0].ack_id);
    }

    #[test]
    fn acknowledge_prevents_redelivery_and_advances_cursor() {
        let kv = kv();
        seed_cursor(&kv, 1, 0);
        seed_messages(&kv, 1, 1, 1_000);
        let engine = DeliveryEngine::new(kv);

        let delivered = engine.lease_next(1, 1, 10, 2_000, 10).unwrap();
        engine
            .acknowledge(1, &[delivered[0].ack_id.clone()])
            .unwrap();

        let redelivered = engine.lease_next(1, 1, 10, 100_000, 10).unwrap();
        assert!(redelivered.is_empty());
    }

    #[test]
    fn acknowledge_unknown_ack_id_does_not_error() {
        let kv = kv();
        seed_cursor(&kv, 1, 0);
        let engine = DeliveryEngine::new(kv);
        engine
            .acknowledge(1, &["not-a-real-ack-id".to_string()])
            .unwrap();
    }

    #[test]
    fn double_acknowledge_is_idempotent() {
        let kv = kv();
        seed_cursor(&kv, 1, 0);
        seed_messages(&kv, 1, 1, 1_000);
        let engine = DeliveryEngine::new(kv);

        let delivered = engine.lease_next(1, 1, 10, 2_000, 10).unwrap();
        engine
            .acknowledge(1, &[delivered[0].ack_id.clone()])
            .unwrap();
        // Second ack of the same (now-stale, since the lease is gone)
        // ack_id must not error.
        engine
            .acknowledge(1, &[delivered[0].ack_id.clone()])
            .unwrap();
    }

    #[test]
    fn modify_ack_deadline_extends_lease() {
        let kv = kv();
        seed_cursor(&kv, 1, 0);
        seed_messages(&kv, 1, 1, 1_000);
        let engine = DeliveryEngine::new(kv);

        let delivered = engine.lease_next(1, 1, 10, 2_000, 10).unwrap();
        engine
            .modify_ack_deadline(1, &[delivered[0].ack_id.clone()], 2_000, 600)
            .unwrap();

        // 11s later — past the *original* 10s deadline, but the extension
        // pushed it out to +600s, so it must still be live.
        let still_leased = engine.lease_next(1, 1, 10, 13_000, 10).unwrap();
        assert!(still_leased.is_empty());
    }

    #[test]
    fn modify_ack_deadline_zero_causes_immediate_redelivery() {
        let kv = kv();
        seed_cursor(&kv, 1, 0);
        seed_messages(&kv, 1, 1, 1_000);
        let engine = DeliveryEngine::new(kv);

        let delivered = engine.lease_next(1, 1, 10, 2_000, 10).unwrap();
        engine
            .modify_ack_deadline(1, &[delivered[0].ack_id.clone()], 2_000, 0)
            .unwrap();

        // No time needs to pass: the deadline was moved to "now" (2_000).
        let redelivered = engine.lease_next(1, 1, 10, 2_000, 10).unwrap();
        assert_eq!(redelivered.len(), 1);
        assert_eq!(redelivered[0].delivery_attempt, 2);
    }

    #[test]
    fn two_subscriptions_on_same_topic_both_receive_independently() {
        let kv = kv();
        seed_cursor(&kv, 1, 0);
        seed_cursor(&kv, 2, 0);
        seed_messages(&kv, 1, 1, 1_000);
        let engine = DeliveryEngine::new(kv);

        let a = engine.lease_next(1, 1, 10, 2_000, 10).unwrap();
        let b = engine.lease_next(2, 1, 10, 2_000, 10).unwrap();
        assert_eq!(a.len(), 1);
        assert_eq!(b.len(), 1);

        engine.acknowledge(1, &[a[0].ack_id.clone()]).unwrap();
        // Acking on subscription 1 must not affect subscription 2's copy.
        let still_there = engine.lease_next(2, 1, 10, 2_000, 10).unwrap();
        assert!(still_there.is_empty()); // still leased to sub 2 from the first pull
    }

    #[test]
    fn sparse_ack_above_floor_then_floor_ack_advances_over_both() {
        let kv = kv();
        seed_cursor(&kv, 1, 0);
        seed_messages(&kv, 1, 2, 1_000);
        let engine = DeliveryEngine::new(kv);

        let delivered = engine.lease_next(1, 1, 10, 2_000, 10).unwrap();
        assert_eq!(delivered.len(), 2);
        // Ack seq 2 first (sparse, above floor).
        engine
            .acknowledge(1, &[delivered[1].ack_id.clone()])
            .unwrap();
        // Then seq 1: floor should jump straight to 2, consuming the
        // sparse entry.
        engine
            .acknowledge(1, &[delivered[0].ack_id.clone()])
            .unwrap();

        let cursor: CursorRecord = CursorRecord::decode(&kv_get(&engine, 1)).unwrap();
        assert_eq!(cursor.acked_floor, 2);
        assert!(cursor.acked_above_floor.is_empty());
    }

    fn kv_get(engine: &DeliveryEngine<MemKv>, sub_id: u64) -> Vec<u8> {
        engine
            .kv
            .get(SUB, &keys::cursor_key(sub_id))
            .expect("cursor must exist")
    }
}
