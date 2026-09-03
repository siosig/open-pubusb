//! `PubSubService`: the single entry point the transport layers (gRPC,
//! REST — `crates/open-pubusb`) call into. Composes [`crate::topic::TopicStore`],
//! [`crate::subscription::SubscriptionStore`], and
//! [`crate::delivery::engine::DeliveryEngine`] behind one API that mirrors
//! the domain-level shapes of `google.pubsub.v1`'s REST/gRPC surface
//! (no proto types here — the transport layers convert to/from those).
//!
//! Core scope: Topic/Subscription CRUD, Publish, Pull, Acknowledge,
//! ModifyAckDeadline. Filtering, ordering, dead-lettering, retry-policy
//! backoff, Push delivery, StreamingPull, and Snapshot/Seek extend this
//! service without changing the shapes defined here.

use std::collections::HashMap;
use std::sync::Arc;

use crate::clock::{Clock, SystemClock};
use crate::delivery::engine::DeliveryEngine;
use crate::error::{Error, Result};
use crate::limits;
use crate::metrics;
use crate::names;
use crate::store::kv::{KvStore, MemKv};
use crate::subscription::{
    CreateSubscriptionOptions, SubscriptionRecord, SubscriptionStore, SubscriptionUpdatePatch,
};
use crate::topic::{TopicRecord, TopicStore};

/// A message as submitted to [`PubSubService::publish`].
#[derive(Debug, Clone, Default)]
pub struct PublishMessage {
    /// The message body.
    pub data: Vec<u8>,
    /// User-supplied attributes.
    pub attributes: HashMap<String, String>,
    /// Ordering key; empty string means unordered.
    pub ordering_key: String,
}

/// A message as returned by [`PubSubService::pull`].
#[derive(Debug, Clone, Default)]
pub struct PulledMessage {
    /// Opaque token the client must present to Ack or ModifyAckDeadline
    /// this specific delivery.
    pub ack_id: String,
    /// Server-assigned, monotonically increasing within the topic.
    pub message_id: String,
    /// The message body.
    pub data: Vec<u8>,
    /// User-supplied attributes.
    pub attributes: HashMap<String, String>,
    /// Ordering key; empty string means unordered.
    pub ordering_key: String,
    /// Milliseconds since the Unix epoch when the message was published.
    pub publish_time_ms: i64,
    /// `0` unless the subscription has a dead-letter policy: exposed via
    /// the API only when a dead_letter_policy is set.
    pub delivery_attempt: u32,
}

impl<K: KvStore> PubSubService<K> {
    // -- Topics ---------------------------------------------------------

    /// Creates a topic with just a name and labels (the REST/simple gRPC
    /// path) — see [`Self::create_topic_full`] for every `CreateTopic`
    /// field.
    pub fn create_topic(
        &self,
        full_name: &str,
        labels: HashMap<String, String>,
    ) -> Result<TopicRecord> {
        self.topics
            .create(full_name, labels, None, None, false, false)
    }

    /// Full-fidelity create, for the gRPC/REST layers that need every
    /// `CreateTopic` field (schema/ingestion rejection, retention, KMS).
    pub fn create_topic_full(
        &self,
        full_name: &str,
        labels: HashMap<String, String>,
        message_retention_secs: Option<i64>,
        kms_key_name: Option<String>,
        has_schema_settings: bool,
        has_ingestion_settings: bool,
    ) -> Result<TopicRecord> {
        self.topics.create(
            full_name,
            labels,
            message_retention_secs,
            kms_key_name,
            has_schema_settings,
            has_ingestion_settings,
        )
    }

    /// Resolves a full topic name to its record.
    pub fn get_topic(&self, full_name: &str) -> Result<TopicRecord> {
        self.topics.get(full_name)
    }

    /// Lists topics belonging to `project_id`, paginated.
    pub fn list_topics(
        &self,
        project_id: &str,
        page_size: usize,
        page_token: Option<&str>,
    ) -> Result<(Vec<TopicRecord>, Option<String>)> {
        self.topics.list(project_id, page_size, page_token)
    }

    /// Replaces a topic's labels, leaving every other field untouched.
    pub fn update_topic_labels(
        &self,
        full_name: &str,
        labels: HashMap<String, String>,
    ) -> Result<TopicRecord> {
        self.topics.update(full_name, Some(labels), None, None)
    }

    /// Full-fidelity update, applying only the `Some` fields given
    /// (`update_mask` allowlisting is the gRPC/REST layer's concern).
    pub fn update_topic_full(
        &self,
        full_name: &str,
        labels: Option<HashMap<String, String>>,
        message_retention_secs: Option<Option<i64>>,
        kms_key_name: Option<Option<String>>,
    ) -> Result<TopicRecord> {
        self.topics
            .update(full_name, labels, message_retention_secs, kms_key_name)
    }

    /// Deletes a topic and detaches every subscription attached to it
    /// (`topic` becomes [`names::DELETED_TOPIC`]), per the Topic state
    /// diagram.
    pub fn delete_topic(&self, full_name: &str) -> Result<()> {
        let record = self.topics.get(full_name)?;
        let sub_ids = self.subs.list_by_topic_id(record.id)?;
        for sub_id in sub_ids {
            self.subs.set_topic_to_deleted(sub_id)?;
        }
        self.topics.delete(full_name)
    }

    /// Full names of every subscription attached to `topic_full_name`.
    pub fn list_topic_subscriptions(&self, topic_full_name: &str) -> Result<Vec<String>> {
        let record = self.topics.get(topic_full_name)?;
        let sub_ids = self.subs.list_by_topic_id(record.id)?;
        let mut names_out = Vec::with_capacity(sub_ids.len());
        for id in sub_ids {
            names_out.push(self.subs.get_by_id(id)?.name);
        }
        names_out.sort();
        Ok(names_out)
    }

    // -- Publish / Pull / Ack --------------------------------------------

    /// Publishes `messages` to `topic_full_name`, in order, returning one
    /// message_id per input message (also in order). Fans out a wakeup to
    /// every attached subscription's pull waiter so a blocked Pull/
    /// StreamingPull notices immediately rather than on its next poll.
    pub fn publish(
        &self,
        topic_full_name: &str,
        messages: Vec<PublishMessage>,
    ) -> Result<Vec<String>> {
        let started = std::time::Instant::now();
        self.check_disk_guard()?;
        let record = self.topics.get(topic_full_name)?;
        let now_ms = self.clock.now_ms();
        let retention_secs = if record.message_retention_secs > 0 {
            record.message_retention_secs
        } else {
            // Unset: no explicit topic-level retention was requested.
            // Actual retention *enforcement* (sweeping expired messages) is
            // handled elsewhere; this only bounds the
            // `expire_at_ms` value stored alongside each message.
            limits::MAX_TOPIC_RETENTION_SECS
        };

        let raw: Vec<(Vec<u8>, HashMap<String, String>, String)> = messages
            .into_iter()
            .map(|m| (m.data, m.attributes, m.ordering_key))
            .collect();
        let seqs = self.topics.append(record.id, raw, now_ms, retention_secs)?;

        metrics::record_published(topic_full_name);

        let sub_ids = self.subs.list_by_topic_id(record.id)?;
        if !sub_ids.is_empty() {
            self.engine.notify_published(&sub_ids);
        }

        metrics::record_publish_latency(started.elapsed().as_secs_f64());
        Ok(seqs.into_iter().map(|seq| seq.to_string()).collect())
    }

    /// Pulls up to `max_messages` currently-deliverable messages from
    /// `subscription_full_name`. Returns promptly (`Ok(vec![])` if none
    /// are available right now) — never blocks. A caller that wants to
    /// wait for new messages should use [`Self::pull_waiter`] around a
    /// retry loop (the gRPC/REST Pull handlers do this to honor
    /// `pull_max_wait_secs`).
    pub fn pull(
        &self,
        subscription_full_name: &str,
        max_messages: i32,
    ) -> Result<Vec<PulledMessage>> {
        limits::validate_pull_max_messages(max_messages.max(1))?;
        let sub = self.subs.get(subscription_full_name)?;
        if sub.detached || sub.topic == names::DELETED_TOPIC {
            return Err(Error::FailedPrecondition {
                message: format!("subscription {subscription_full_name} is detached"),
            });
        }

        let now_ms = self.clock.now_ms();
        let delivered = self.engine.lease_next(
            sub.id,
            sub.topic_id,
            max_messages,
            now_ms,
            sub.ack_deadline_secs,
        )?;
        let _ = self.subs.touch(sub.id, now_ms);

        let expose_attempt = sub.dead_letter_topic.is_some();
        metrics::set_unacked(subscription_full_name, self.engine.lease_count() as f64);
        if !delivered.is_empty() {
            metrics::record_delivered(subscription_full_name, "pull");
        }

        Ok(delivered
            .into_iter()
            .map(|d| PulledMessage {
                ack_id: d.ack_id,
                message_id: d.seq.to_string(),
                data: d.message.payload,
                attributes: d.message.attributes,
                ordering_key: d.message.ordering_key,
                publish_time_ms: d.message.publish_ts_ms,
                delivery_attempt: if expose_attempt {
                    d.delivery_attempt
                } else {
                    0
                },
            })
            .collect())
    }

    /// The [`tokio::sync::Notify`] that wakes when a new message is
    /// published to (or redelivery becomes due on) `subscription_full_name`.
    /// Callers await this with a timeout, then retry [`Self::pull`], to
    /// implement a blocking Pull without polling.
    pub fn pull_waiter(&self, subscription_full_name: &str) -> Result<Arc<tokio::sync::Notify>> {
        let sub = self.subs.get(subscription_full_name)?;
        Ok(self.engine.waiter(sub.id))
    }

    /// Permanently acknowledges every message named by `ack_ids`. Unknown
    /// or stale `ack_id`s are silently ignored, never an error (per the
    /// proto contract).
    pub fn acknowledge(&self, subscription_full_name: &str, ack_ids: Vec<String>) -> Result<()> {
        let sub = self.subs.get(subscription_full_name)?;
        self.engine.acknowledge(sub.id, &ack_ids)?;
        let _ = self.subs.touch(sub.id, self.clock.now_ms());
        metrics::record_acked(subscription_full_name);
        Ok(())
    }

    /// Extends (or, with `seconds <= 0`, immediately expires — an explicit
    /// Nack) the ack deadline of every lease named by `ack_ids`.
    pub fn modify_ack_deadline(
        &self,
        subscription_full_name: &str,
        ack_ids: Vec<String>,
        seconds: i32,
    ) -> Result<()> {
        let sub = self.subs.get(subscription_full_name)?;
        let now_ms = self.clock.now_ms();
        self.engine
            .modify_ack_deadline(sub.id, &ack_ids, now_ms, seconds)?;
        let _ = self.subs.touch(sub.id, now_ms);
        Ok(())
    }

    /// Runs one pass of expired-lease garbage collection for `sub_id`
    /// (see [`DeliveryEngine::sweep_expired`]). Intended to be called
    /// periodically (`delivery.lease_scan_interval_ms`,
    /// `crates/open-pubusb/src/server.rs`, a later task) for every known
    /// subscription — not required for correctness (see
    /// `crate::delivery::engine`'s module doc comment on self-healing
    /// `lease_next`), only for bounding memory.
    pub fn sweep_subscription(&self, sub_id: u64, grace_ms: i64) -> usize {
        let now_ms = self.clock.now_ms();
        self.engine.sweep_expired(sub_id, now_ms, grace_ms)
    }

    /// Runs one retention sweep: deletes every message past its
    /// `expire_at_ms` (across every topic) and every subscription past its
    /// `expiration_policy.ttl`. Intended to be called
    /// periodically on `delivery.retention_sweep_interval_secs`
    /// (`crates/open-pubusb/src/main.rs`).
    pub fn sweep_retention(&self) -> crate::delivery::retention::RetentionStats {
        let now_ms = self.clock.now_ms();
        let stats = crate::delivery::retention::sweep_expired_messages(
            self.kv.as_ref(),
            &self.subs,
            now_ms,
        );
        match self.subs.sweep_expired(now_ms) {
            Ok(removed) => {
                for name in removed {
                    tracing::info!(subscription = %name, "expiration_policy.ttl elapsed; subscription deleted");
                }
            }
            Err(e) => {
                tracing::error!(error = ?e, "subscription expiration sweep failed");
            }
        }
        stats
    }

    // -- Subscriptions ----------------------------------------------------

    /// Full-fidelity create, mirroring `CreateSubscriptionRequest`.
    /// Resolves `topic_full_name` to its internal id and current tail
    /// (this subscription's `attach_seq`) itself.
    pub fn create_subscription(
        &self,
        full_name: &str,
        topic_full_name: &str,
        opts: CreateSubscriptionOptions,
    ) -> Result<SubscriptionRecord> {
        let topic = self.topics.get(topic_full_name)?;
        let attach_seq = self.topics.current_tail(topic.id);
        let now_ms = self.clock.now_ms();
        self.subs.create(
            full_name,
            topic_full_name,
            topic.id,
            attach_seq,
            now_ms,
            opts,
        )
    }

    /// Resolves a full subscription name to its record.
    pub fn get_subscription(&self, full_name: &str) -> Result<SubscriptionRecord> {
        self.subs.get(full_name)
    }

    /// Lists subscriptions belonging to `project_id`, paginated.
    pub fn list_subscriptions(
        &self,
        project_id: &str,
        page_size: usize,
        page_token: Option<&str>,
    ) -> Result<(Vec<SubscriptionRecord>, Option<String>)> {
        self.subs.list(project_id, page_size, page_token)
    }

    /// Every subscription across every project — for the push-dispatcher
    /// reconciliation loop (`crate::push::manager`), which needs to react
    /// to any subscription's push config regardless of project.
    pub fn list_all_subscriptions(&self) -> Vec<SubscriptionRecord> {
        self.subs.list_all()
    }

    /// Every topic across every project — for the `open_pubusb_topics` metrics
    /// gauge, sibling to [`Self::list_all_subscriptions`].
    pub fn list_all_topics(&self) -> Vec<TopicRecord> {
        self.topics.list_all()
    }

    /// Applies `patch`'s `Some` fields to a subscription's mutable state.
    pub fn update_subscription(
        &self,
        full_name: &str,
        patch: SubscriptionUpdatePatch,
    ) -> Result<SubscriptionRecord> {
        self.subs.update(full_name, patch)
    }

    /// Deletes a subscription.
    pub fn delete_subscription(&self, full_name: &str) -> Result<()> {
        self.subs.delete(full_name)
    }

    /// Marks a subscription detached — Pull/StreamingPull on it then fail
    /// `FailedPrecondition`, permanently.
    pub fn detach_subscription(&self, full_name: &str) -> Result<()> {
        self.subs.detach(full_name)
    }

    /// Switches a subscription's delivery mode: `Some` sets/replaces its
    /// push config (Push mode), `None` switches it to Pull.
    pub fn modify_push_config(
        &self,
        full_name: &str,
        push_config: Option<crate::subscription::PushConfig>,
    ) -> Result<()> {
        self.subs.set_push_config(full_name, push_config)
    }

    // -- StreamingPull ----------------------------------------------------

    /// Validates `subscription_full_name` and opens a new `StreamingPull`
    /// session against it. Returns the new stream's id and the
    /// subscription's current record (for the caller to build the first
    /// response's `subscription_properties`).
    pub fn open_stream(
        &self,
        subscription_full_name: &str,
        ack_deadline_secs: i32,
        max_outstanding_messages: i64,
        max_outstanding_bytes: i64,
    ) -> Result<(u64, SubscriptionRecord)> {
        let sub = self.subs.get(subscription_full_name)?;
        if sub.detached || sub.topic == names::DELETED_TOPIC {
            return Err(Error::FailedPrecondition {
                message: format!("subscription {subscription_full_name} is detached"),
            });
        }
        let stream_id = self.engine.open_stream(
            sub.id,
            sub.topic_id,
            ack_deadline_secs,
            max_outstanding_messages,
            max_outstanding_bytes,
        );
        Ok((stream_id, sub))
    }

    /// Updates a `StreamingPull` stream's ack deadline / flow-control
    /// budget mid-stream (a later `StreamingPullRequest` on the same
    /// stream) — only `Some` fields change.
    pub fn update_stream(
        &self,
        stream_id: u64,
        ack_deadline_secs: Option<i32>,
        max_outstanding_messages: Option<i64>,
        max_outstanding_bytes: Option<i64>,
    ) {
        self.engine.update_stream(
            stream_id,
            ack_deadline_secs,
            max_outstanding_messages,
            max_outstanding_bytes,
        );
    }

    /// Leases up to `stream_id`'s remaining flow-control budget worth of
    /// currently-deliverable messages.
    pub fn lease_for_stream(&self, stream_id: u64) -> Result<Vec<PulledMessage>> {
        let now_ms = self.clock.now_ms();
        let delivered = self.engine.lease_for_stream(stream_id, now_ms)?;
        Ok(delivered
            .into_iter()
            .map(|d| PulledMessage {
                ack_id: d.ack_id,
                message_id: d.seq.to_string(),
                data: d.message.payload,
                attributes: d.message.attributes,
                ordering_key: d.message.ordering_key,
                publish_time_ms: d.message.publish_ts_ms,
                delivery_attempt: d.delivery_attempt,
            })
            .collect())
    }

    /// The [`tokio::sync::Notify`] a `StreamingPull` send loop awaits
    /// between [`Self::lease_for_stream`] calls, same underlying
    /// per-subscription notifier unary Pull's [`Self::pull_waiter`] uses.
    pub fn stream_waiter(&self, sub_id: u64) -> Arc<tokio::sync::Notify> {
        self.engine.waiter(sub_id)
    }

    /// Ack, via a `StreamingPull` stream's own `AcknowledgeRequest`
    /// (as opposed to unary [`Self::acknowledge`]).
    pub fn stream_acknowledge(
        &self,
        stream_id: u64,
        sub_id: u64,
        ack_ids: Vec<String>,
    ) -> Result<()> {
        self.engine
            .stream_acknowledge(stream_id, sub_id, &ack_ids)?;
        let now_ms = self.clock.now_ms();
        let _ = self.subs.touch(sub_id, now_ms);
        if let Ok(sub) = self.subs.get_by_id(sub_id) {
            metrics::record_acked(&sub.name);
        }
        Ok(())
    }

    /// ModifyAckDeadline, via a `StreamingPull` stream's own
    /// `modify_deadline_seconds`/`modify_deadline_ack_ids`
    /// (as opposed to unary [`Self::modify_ack_deadline`]).
    pub fn stream_modify_ack_deadline(
        &self,
        stream_id: u64,
        sub_id: u64,
        ack_ids: Vec<String>,
        seconds: i32,
    ) -> Result<()> {
        let now_ms = self.clock.now_ms();
        self.engine
            .stream_modify_ack_deadline(stream_id, sub_id, &ack_ids, now_ms, seconds)?;
        let _ = self.subs.touch(sub_id, now_ms);
        Ok(())
    }

    /// Releases every lease `stream_id` was still holding, immediately
    /// making them eligible for redelivery — the lease expires on
    /// disconnect. Call when a `StreamingPull` stream ends, for
    /// any reason (client disconnect, lifetime timer, shutdown).
    pub fn close_stream(&self, stream_id: u64) {
        self.engine.on_stream_closed(stream_id);
    }

    // -- Snapshots & Seek -------------------------------------------------

    /// Captures `subscription_full_name`'s current cursor into a new
    /// snapshot. `expire_time` is computed per this rule:
    /// creation time + (7d − age of the oldest unacked message) —
    /// `FailedPrecondition` if that leaves under an hour of lifetime.
    pub fn create_snapshot(
        &self,
        full_name: &str,
        subscription_full_name: &str,
        labels: HashMap<String, String>,
    ) -> Result<crate::delivery::snapshot::SnapshotRecord> {
        let sub = self.subs.get(subscription_full_name)?;
        let cursor = self.engine.cursor_snapshot(sub.id)?;
        let oldest_unacked = self
            .engine
            .oldest_unacked_publish_ts_ms(sub.id, sub.topic_id)?;
        let now_ms = self.clock.now_ms();
        let expire_at_ms = crate::delivery::snapshot::compute_expire_at_ms(now_ms, oldest_unacked)?;
        self.snapshots.create(
            full_name,
            &sub.topic,
            sub.topic_id,
            labels,
            cursor,
            expire_at_ms,
        )
    }

    /// Resolves a full snapshot name to its record.
    pub fn get_snapshot(
        &self,
        full_name: &str,
    ) -> Result<crate::delivery::snapshot::SnapshotRecord> {
        self.snapshots.get(full_name)
    }

    /// Lists snapshots belonging to `project_id`, paginated.
    pub fn list_snapshots(
        &self,
        project_id: &str,
        page_size: usize,
        page_token: Option<&str>,
    ) -> Result<(
        Vec<crate::delivery::snapshot::SnapshotRecord>,
        Option<String>,
    )> {
        self.snapshots.list(project_id, page_size, page_token)
    }

    /// Snapshots captured from a subscription attached to `topic_full_name`
    /// (`ListTopicSnapshots`). No dedicated topic->snapshot index exists
    /// (snapshot counts are expected to be small relative to topics/subs),
    /// so this filters a full per-project listing rather than adding one.
    pub fn list_topic_snapshots(&self, topic_full_name: &str) -> Result<Vec<String>> {
        let project_id = crate::names::TopicName::parse(topic_full_name)?
            .project_id()
            .to_string();
        let (all, _) = self.snapshots.list(&project_id, usize::MAX, None)?;
        Ok(all
            .into_iter()
            .filter(|s| s.topic == topic_full_name)
            .map(|s| s.name)
            .collect())
    }

    /// Replaces a snapshot's labels — its only mutable field.
    pub fn update_snapshot_labels(
        &self,
        full_name: &str,
        labels: HashMap<String, String>,
    ) -> Result<crate::delivery::snapshot::SnapshotRecord> {
        self.snapshots.update_labels(full_name, labels)
    }

    /// Deletes a snapshot.
    pub fn delete_snapshot(&self, full_name: &str) -> Result<()> {
        self.snapshots.delete(full_name)
    }

    /// Periodic sweep deleting snapshots past their `expire_at_ms`
    /// (automatically deleted once reached), scoped to `project_id`.
    pub fn sweep_expired_snapshots(&self, project_id: &str) -> Result<usize> {
        let now_ms = self.clock.now_ms();
        self.snapshots.sweep_expired(project_id, now_ms)
    }

    /// Seeks `subscription_full_name` to `snapshot_full_name`'s captured
    /// cursor — every message the snapshot's cursor doesn't mark done
    /// becomes (re)deliverable with `delivery_attempt` reset, and every
    /// currently in-flight lease on the subscription is invalidated
    /// (`DeliveryEngine::restore_cursor`'s doc comment).
    pub fn seek_to_snapshot(
        &self,
        subscription_full_name: &str,
        snapshot_full_name: &str,
    ) -> Result<()> {
        let sub = self.subs.get(subscription_full_name)?;
        let snapshot = self.snapshots.get(snapshot_full_name)?;
        if snapshot.topic_id != sub.topic_id {
            return Err(Error::InvalidArgument {
                field: "snapshot".to_string(),
                message: format!(
                    "snapshot {snapshot_full_name} was not captured from a subscription of {subscription_full_name}'s topic"
                ),
            });
        }
        self.engine.restore_cursor(sub.id, snapshot.cursor)
    }

    /// Seeks `subscription_full_name` to `time_ms` (ms since epoch):
    /// messages published before `time_ms` become (re)marked done, and
    /// every message from `time_ms` on becomes (re)deliverable — same
    /// lease-invalidating/attempt-resetting semantics as
    /// [`Self::seek_to_snapshot`]. `time_ms` in the future is clamped to
    /// the topic's current tail (everything published so far becomes
    /// done); `time_ms` before the subscription's `attach_seq` is clamped
    /// to `attach_seq` (a subscription never sees messages published
    /// before it was created, seek or not).
    pub fn seek_to_time(&self, subscription_full_name: &str, time_ms: i64) -> Result<()> {
        let sub = self.subs.get(subscription_full_name)?;
        // The cursor's own `attach_seq` never changes across acks or
        // seeks — read the current one to preserve it in the replacement.
        let attach_seq = self.engine.cursor_snapshot(sub.id)?.attach_seq;
        let boundary_seq = self.topics.seq_boundary_for_time(sub.topic_id, time_ms);
        let acked_floor = boundary_seq.saturating_sub(1).max(attach_seq);
        self.engine.restore_cursor(
            sub.id,
            crate::store::codec::CursorRecord {
                attach_seq,
                acked_floor,
                acked_above_floor: Default::default(),
            },
        )
    }
}

/// The single entry point the transport layers call into. See the module
/// doc comment.
pub struct PubSubService<K: KvStore> {
    kv: Arc<K>,
    clock: Arc<dyn Clock>,
    topics: TopicStore<K>,
    subs: SubscriptionStore<K>,
    snapshots: crate::delivery::snapshot::SnapshotStore<K>,
    engine: DeliveryEngine<K>,
    /// `0` = unlimited (the default). See [`Self::with_max_disk_bytes`].
    max_disk_bytes: u64,
}

impl<K: KvStore> PubSubService<K> {
    /// Constructs a service over `kv`, immediately reconstructing any
    /// in-memory-only state (currently just the delivery engine's lease
    /// table — see [`DeliveryEngine::recover`]) from whatever `kv` already
    /// contains. A no-op on a fresh/empty store, so this is safe (and
    /// correct) to call unconditionally rather than requiring callers to
    /// remember a separate "recover" step only for the persistent-storage
    /// case.
    pub fn new(kv: Arc<K>, clock: Arc<dyn Clock>) -> Self {
        let engine = DeliveryEngine::new(kv.clone());
        engine.recover();
        Self {
            topics: TopicStore::new(kv.clone()),
            subs: SubscriptionStore::new(kv.clone()),
            snapshots: crate::delivery::snapshot::SnapshotStore::new(kv.clone()),
            engine,
            kv,
            clock,
            max_disk_bytes: 0,
        }
    }

    /// Sets the `storage.max_disk_bytes` guard (`0` = unlimited, the
    /// default): once `self.kv.approx_disk_bytes()` reaches this, further
    /// writes that grow storage (currently: `Publish`) are rejected with
    /// [`Error::ResourceExhausted`] instead of being attempted, so the
    /// underlying store is not driven into a real ENOSPC condition under
    /// normal operation.
    #[must_use]
    pub fn with_max_disk_bytes(mut self, max_disk_bytes: u64) -> Self {
        self.max_disk_bytes = max_disk_bytes;
        self
    }

    /// Current approximate on-disk usage (`open_pubusb_storage_disk_bytes`).
    pub fn disk_usage_bytes(&self) -> u64 {
        self.kv.approx_disk_bytes()
    }

    fn check_disk_guard(&self) -> Result<()> {
        if self.max_disk_bytes > 0 && self.kv.approx_disk_bytes() >= self.max_disk_bytes {
            return Err(Error::ResourceExhausted {
                message: format!(
                    "storage.max_disk_bytes ({}) reached; rejecting new writes",
                    self.max_disk_bytes
                ),
            });
        }
        Ok(())
    }
}

impl PubSubService<MemKv> {
    /// An in-memory service instance backed by the real wall clock — used
    /// for `--ephemeral` mode and any test that doesn't need to control
    /// time.
    pub fn new_ephemeral() -> Self {
        Self::new(Arc::new(MemKv::new()), Arc::new(SystemClock))
    }

    /// An in-memory service instance with an explicit clock (typically a
    /// [`crate::clock::MockClock`]), for tests that exercise ack-deadline
    /// expiry/redelivery deterministically.
    pub fn new_ephemeral_with_clock(clock: Arc<dyn Clock>) -> Self {
        Self::new(Arc::new(MemKv::new()), clock)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;

    fn labels() -> HashMap<String, String> {
        HashMap::new()
    }

    fn msg(data: &[u8]) -> PublishMessage {
        PublishMessage {
            data: data.to_vec(),
            ..Default::default()
        }
    }

    #[test]
    fn create_topic_then_get_round_trips_labels() {
        let svc = PubSubService::new_ephemeral();
        let mut l = HashMap::new();
        l.insert("env".to_string(), "prod".to_string());
        svc.create_topic("projects/proj/topics/top1", l.clone())
            .unwrap();
        let got = svc.get_topic("projects/proj/topics/top1").unwrap();
        assert_eq!(got.labels, l);

        let err = svc
            .create_topic("projects/proj/topics/top1", HashMap::new())
            .unwrap_err();
        assert!(matches!(err, Error::AlreadyExists { .. }));
    }

    #[test]
    fn operations_on_nonexistent_topic_are_not_found() {
        let svc = PubSubService::new_ephemeral();
        assert!(matches!(
            svc.get_topic("projects/proj/topics/missing1").unwrap_err(),
            Error::NotFound { .. }
        ));
        assert!(matches!(
            svc.delete_topic("projects/proj/topics/missing1")
                .unwrap_err(),
            Error::NotFound { .. }
        ));
    }

    #[test]
    fn deleting_topic_detaches_subscriptions_to_deleted_sentinel() {
        let svc = PubSubService::new_ephemeral();
        svc.create_topic("projects/proj/topics/top1", labels())
            .unwrap();
        svc.create_subscription(
            "projects/proj/subscriptions/sub1",
            "projects/proj/topics/top1",
            CreateSubscriptionOptions::default(),
        )
        .unwrap();

        svc.delete_topic("projects/proj/topics/top1").unwrap();

        let sub = svc
            .get_subscription("projects/proj/subscriptions/sub1")
            .unwrap();
        assert_eq!(sub.topic, names::DELETED_TOPIC);
    }

    #[test]
    fn list_topic_subscriptions_scopes_to_that_topic() {
        let svc = PubSubService::new_ephemeral();
        svc.create_topic("projects/proj/topics/topa", labels())
            .unwrap();
        svc.create_topic("projects/proj/topics/topb", labels())
            .unwrap();
        svc.create_subscription(
            "projects/proj/subscriptions/suba1",
            "projects/proj/topics/topa",
            CreateSubscriptionOptions::default(),
        )
        .unwrap();
        svc.create_subscription(
            "projects/proj/subscriptions/suba2",
            "projects/proj/topics/topa",
            CreateSubscriptionOptions::default(),
        )
        .unwrap();
        svc.create_subscription(
            "projects/proj/subscriptions/subb1",
            "projects/proj/topics/topb",
            CreateSubscriptionOptions::default(),
        )
        .unwrap();

        let subs = svc
            .list_topic_subscriptions("projects/proj/topics/topa")
            .unwrap();
        assert_eq!(
            subs,
            vec![
                "projects/proj/subscriptions/suba1".to_string(),
                "projects/proj/subscriptions/suba2".to_string(),
            ]
        );
    }

    #[test]
    fn detached_subscription_pull_fails_precondition() {
        let svc = PubSubService::new_ephemeral();
        svc.create_topic("projects/proj/topics/top1", labels())
            .unwrap();
        svc.create_subscription(
            "projects/proj/subscriptions/sub1",
            "projects/proj/topics/top1",
            CreateSubscriptionOptions::default(),
        )
        .unwrap();
        svc.detach_subscription("projects/proj/subscriptions/sub1")
            .unwrap();

        let err = svc
            .pull("projects/proj/subscriptions/sub1", 10)
            .unwrap_err();
        assert!(matches!(err, Error::FailedPrecondition { .. }));
    }

    #[test]
    fn update_topic_labels_changes_labels_without_recreation() {
        let svc = PubSubService::new_ephemeral();
        svc.create_topic("projects/proj/topics/top1", labels())
            .unwrap();
        let mut l = HashMap::new();
        l.insert("k".to_string(), "v".to_string());
        let updated = svc
            .update_topic_labels("projects/proj/topics/top1", l.clone())
            .unwrap();
        assert_eq!(updated.labels, l);
        assert_eq!(
            svc.get_topic("projects/proj/topics/top1").unwrap().labels,
            l
        );
    }

    #[test]
    fn publish_then_pull_round_trips_and_acks() {
        let svc = PubSubService::new_ephemeral();
        svc.create_topic("projects/proj/topics/top1", labels())
            .unwrap();
        svc.create_subscription(
            "projects/proj/subscriptions/sub1",
            "projects/proj/topics/top1",
            CreateSubscriptionOptions::default(),
        )
        .unwrap();

        let ids = svc
            .publish("projects/proj/topics/top1", vec![msg(b"hello")])
            .unwrap();
        assert_eq!(ids.len(), 1);

        let pulled = svc.pull("projects/proj/subscriptions/sub1", 10).unwrap();
        assert_eq!(pulled.len(), 1);
        assert_eq!(pulled[0].data, b"hello");
        assert_eq!(pulled[0].message_id, ids[0]);
        // No dead-letter policy on this subscription -> attempt hidden.
        assert_eq!(pulled[0].delivery_attempt, 0);

        svc.acknowledge(
            "projects/proj/subscriptions/sub1",
            vec![pulled[0].ack_id.clone()],
        )
        .unwrap();
        let again = svc.pull("projects/proj/subscriptions/sub1", 10).unwrap();
        assert!(again.is_empty());
    }

    #[test]
    fn messages_published_before_subscription_creation_are_not_delivered() {
        let svc = PubSubService::new_ephemeral();
        svc.create_topic("projects/proj/topics/top1", labels())
            .unwrap();
        svc.publish("projects/proj/topics/top1", vec![msg(b"before")])
            .unwrap();
        svc.create_subscription(
            "projects/proj/subscriptions/sub1",
            "projects/proj/topics/top1",
            CreateSubscriptionOptions::default(),
        )
        .unwrap();
        svc.publish("projects/proj/topics/top1", vec![msg(b"after")])
            .unwrap();

        let pulled = svc.pull("projects/proj/subscriptions/sub1", 10).unwrap();
        assert_eq!(pulled.len(), 1);
        assert_eq!(pulled[0].data, b"after");
    }

    #[test]
    fn two_subscriptions_on_same_topic_both_receive_published_message() {
        let svc = PubSubService::new_ephemeral();
        svc.create_topic("projects/proj/topics/top1", labels())
            .unwrap();
        svc.create_subscription(
            "projects/proj/subscriptions/suba",
            "projects/proj/topics/top1",
            CreateSubscriptionOptions::default(),
        )
        .unwrap();
        svc.create_subscription(
            "projects/proj/subscriptions/subb",
            "projects/proj/topics/top1",
            CreateSubscriptionOptions::default(),
        )
        .unwrap();

        svc.publish("projects/proj/topics/top1", vec![msg(b"fanout")])
            .unwrap();

        assert_eq!(
            svc.pull("projects/proj/subscriptions/suba", 10)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            svc.pull("projects/proj/subscriptions/subb", 10)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn unacked_message_redelivered_after_deadline_with_incremented_attempt() {
        let clock = Arc::new(MockClock::new(1_000));
        let svc = PubSubService::new_ephemeral_with_clock(clock.clone());
        svc.create_topic("projects/proj/topics/top1", labels())
            .unwrap();
        svc.create_subscription(
            "projects/proj/subscriptions/sub1",
            "projects/proj/topics/top1",
            CreateSubscriptionOptions {
                ack_deadline_secs: Some(10),
                dead_letter_topic: Some("projects/proj/topics/dlq1".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        svc.publish("projects/proj/topics/top1", vec![msg(b"redeliver")])
            .unwrap();

        let first = svc.pull("projects/proj/subscriptions/sub1", 10).unwrap();
        assert_eq!(first[0].delivery_attempt, 1); // DLQ policy present -> exposed

        clock.advance_secs(11);
        let second = svc.pull("projects/proj/subscriptions/sub1", 10).unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].message_id, first[0].message_id);
        assert_eq!(second[0].delivery_attempt, 2);
    }

    #[test]
    fn modify_ack_deadline_zero_immediately_nacks_and_redelivers() {
        let clock = Arc::new(MockClock::new(1_000));
        let svc = PubSubService::new_ephemeral_with_clock(clock);
        svc.create_topic("projects/proj/topics/top1", labels())
            .unwrap();
        svc.create_subscription(
            "projects/proj/subscriptions/sub1",
            "projects/proj/topics/top1",
            CreateSubscriptionOptions::default(),
        )
        .unwrap();
        svc.publish("projects/proj/topics/top1", vec![msg(b"nack-me")])
            .unwrap();

        let pulled = svc.pull("projects/proj/subscriptions/sub1", 10).unwrap();
        svc.modify_ack_deadline(
            "projects/proj/subscriptions/sub1",
            vec![pulled[0].ack_id.clone()],
            0,
        )
        .unwrap();

        let redelivered = svc.pull("projects/proj/subscriptions/sub1", 10).unwrap();
        assert_eq!(redelivered.len(), 1);
    }

    #[test]
    fn publishing_batch_over_limit_is_invalid_argument() {
        let svc = PubSubService::new_ephemeral();
        svc.create_topic("projects/proj/topics/top1", labels())
            .unwrap();
        let messages: Vec<PublishMessage> = (0..(limits::MAX_PUBLISH_BATCH_MESSAGES + 1))
            .map(|i| msg(format!("m{i}").as_bytes()))
            .collect();
        let err = svc
            .publish("projects/proj/topics/top1", messages)
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument { .. }));
    }

    #[test]
    fn invalid_topic_name_is_rejected() {
        let svc = PubSubService::new_ephemeral();
        let err = svc
            .create_topic("projects/proj/topics/ab", labels())
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument { .. }));
    }

    #[test]
    fn topic_name_starting_with_goog_is_rejected() {
        let svc = PubSubService::new_ephemeral();
        let err = svc
            .create_topic("projects/proj/topics/goog-reserved", labels())
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument { .. }));
    }
}
