//! Subscription domain logic: validation, persistence, and lookups for
//! `google.pubsub.v1.Subscription`-equivalent resources, matching the
//! Cloud Pub/Sub REST/gRPC error-code mapping for the operations it
//! implements.
//!
//! Persistence layout (see `store::keys` for the exact key builders):
//! - `meta/s/{sub_id}` — this module's [`SubscriptionRecord`], JSON-encoded.
//! - `meta/name/s/{full_name}` — `full_name` -> `sub_id` (8 big-endian bytes).
//! - `sub/{sub_id}` — the subscription's [`store::codec::CursorRecord`],
//!   initialized at creation time.
//! - `idx/{topic_id}{sub_id}` — reverse index from topic to subscription,
//!   used for Publish fan-out and topic-delete detach (empty value).

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Error;
use crate::limits;
use crate::names;
use crate::store::codec::CursorRecord;
use crate::store::keys::{self, NameKind};
use crate::store::kv::KvStore;

/// A subscription's push-delivery configuration.
/// `oidc_token`, if the client sent one, is intentionally *not* stored
/// here — this server accepts it (rather than rejecting the request) and
/// logs a one-time warning that it has no real OIDC-issuing backend, since
/// authenticated push isn't implemented; storing a value this server can
/// never act on would just be misleading state.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PushConfig {
    /// The URL every leased message is POSTed to.
    pub endpoint: String,
    /// `false` (the default/`PubsubWrapper`) sends the JSON-wrapped
    /// envelope; `true` (`NoWrapper`) sends the raw message body with
    /// `x-goog-pubsub-*` metadata headers.
    pub no_wrapper: bool,
    /// Only meaningful when `no_wrapper` is `true`.
    pub write_metadata: bool,
}

/// A Subscription's persisted, transport-agnostic state. Deliberately does
/// not depend on `open-pubusb-proto` — the gRPC/REST layers translate to/from
/// this shape.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SubscriptionRecord {
    /// Internal id, stable across renames (there are none) and separate
    /// from the resource name so that name reuse after deletion cannot
    /// collide with a still-referenced old id (e.g. in `idx` rows).
    pub id: u64,
    /// Full resource name: `projects/{project}/subscriptions/{name}`.
    pub name: String,
    /// Full topic resource name, or [`names::DELETED_TOPIC`] once the topic
    /// has been deleted.
    pub topic: String,
    /// Internal id of the topic this subscription is attached to. Retained
    /// even after the topic is deleted (when `topic` becomes the deleted
    /// sentinel) so `delete` can still clean up the `idx` row.
    pub topic_id: u64,
    /// Seconds a lease stays outstanding before automatic redelivery.
    pub ack_deadline_secs: i32,
    /// If `false` (the default), acked messages are eligible for deletion
    /// immediately; if `true`, they're kept until retention expiry so
    /// Seek can restore them.
    pub retain_acked_messages: bool,
    /// How long messages stay in the backlog, in seconds.
    pub message_retention_secs: i64,
    /// User-supplied labels.
    pub labels: HashMap<String, String>,
    /// Immutable after creation.
    pub enable_message_ordering: bool,
    /// `None` means no expiration policy is set — i.e. unbounded (the
    /// `ExpirationPolicy` with an unset `ttl`). `Some(secs)` means "expire
    /// `secs` after last use".
    pub expiration_ttl_secs: Option<i64>,
    /// Immutable after creation.
    pub filter: String,
    /// Full topic resource name messages are forwarded to once
    /// `max_delivery_attempts` is exceeded, or `None` to disable
    /// dead-lettering.
    pub dead_letter_topic: Option<String>,
    /// Only meaningful when `dead_letter_topic.is_some()`; `0` otherwise.
    pub max_delivery_attempts: i32,
    /// Minimum retry backoff, in seconds.
    pub min_retry_backoff_secs: i64,
    /// Maximum retry backoff, in seconds.
    pub max_retry_backoff_secs: i64,
    /// `true` iff the client explicitly set `min_retry_backoff_secs`
    /// and/or `max_retry_backoff_secs` (at create or update time), as
    /// opposed to `min_retry_backoff_secs`/`max_retry_backoff_secs`
    /// simply holding their create-time defaults (10s/600s). Gates
    /// whether automatic redelivery after ack-deadline expiry additionally
    /// waits out retry_policy's backoff window
    /// (`crate::delivery::engine`'s self-healing `lease_next` reclaim) —
    /// proto's own contract already requires an
    /// *explicit* `ModifyAckDeadline(0)`/Nack to make a message
    /// "immediately available" unconditionally, so this flag only needs
    /// to affect the *automatic* expiry path, not explicit Nack.
    pub retry_policy_explicit: bool,
    /// `true` once `Detach`ed (or its topic deleted) — Pull/StreamingPull
    /// on a detached subscription fail `FailedPrecondition`.
    pub detached: bool,
    /// Accepted and stored, but not actually enforced (no real
    /// exactly-once guarantee is implemented) — see the crate's
    /// **Deviations** documentation.
    pub enable_exactly_once_delivery: bool,
    /// `None` means Pull; `Some(config)` means Push.
    pub push_config: Option<PushConfig>,
    /// Milliseconds since the Unix epoch of the last Pull/Ack/ModifyAckDeadline
    /// (and, once implemented, StreamingPull activity) against this
    /// subscription. Initialized to its creation time; drives
    /// `expiration_ttl_secs` auto-deletion.
    pub last_activity_ts_ms: i64,
}

/// Optional parameters for [`SubscriptionStore::create`], bundling every
/// field that has a create-time default or requires validation.
#[derive(Debug, Clone, Default)]
pub struct CreateSubscriptionOptions {
    /// `None` -> default of 10s.
    pub ack_deadline_secs: Option<i32>,
    /// See [`SubscriptionRecord::retain_acked_messages`].
    pub retain_acked_messages: bool,
    /// `None` -> default of 7 days (604800s).
    pub message_retention_secs: Option<i64>,
    /// User-supplied labels.
    pub labels: HashMap<String, String>,
    /// Immutable after creation once set.
    pub enable_message_ordering: bool,
    /// Outer `None` -> default of 31 days. Outer `Some(None)` -> explicitly
    /// disabled (unbounded). Outer `Some(Some(secs))` -> explicit ttl.
    pub expiration_ttl_secs: Option<Option<i64>>,
    /// Immutable after creation.
    pub filter: String,
    /// See [`SubscriptionRecord::dead_letter_topic`].
    pub dead_letter_topic: Option<String>,
    /// Only validated/applied when `dead_letter_topic.is_some()`. `0` (or
    /// unset) substitutes the default of 5.
    pub max_delivery_attempts: Option<i32>,
    /// `None` -> default of 10s.
    pub min_retry_backoff_secs: Option<i64>,
    /// `None` -> default of 600s.
    pub max_retry_backoff_secs: Option<i64>,
    /// See [`SubscriptionRecord::enable_exactly_once_delivery`].
    pub enable_exactly_once_delivery: bool,
    /// `None` creates a Pull subscription; `Some` a Push subscription.
    pub push_config: Option<PushConfig>,
    /// Rejected with `InvalidArgument` when true — not supported by this
    /// server.
    pub has_bigquery_config: bool,
    /// Rejected with `InvalidArgument` when true — not supported by this
    /// server.
    pub has_cloud_storage_config: bool,
}

/// A patch applied by [`SubscriptionStore::update`] to an existing
/// subscription's mutable fields.
///
/// Deliberately has **no** field for `name`, `topic`,
/// `enable_message_ordering`, or `filter`: those four are immutable after
/// creation, so interpreting an `UpdateSubscription`
/// `update_mask` that names one of them into an `InvalidArgument` is a
/// higher-layer (gRPC/REST mapping) concern — this patch type simply has
/// no way to express such a change, which is the enforcement mechanism.
#[derive(Debug, Clone, Default)]
pub struct SubscriptionUpdatePatch {
    /// `Some` = change the ack deadline.
    pub ack_deadline_secs: Option<i32>,
    /// `Some` = change [`SubscriptionRecord::retain_acked_messages`].
    pub retain_acked_messages: Option<bool>,
    /// `Some` = change the message retention duration.
    pub message_retention_secs: Option<i64>,
    /// `Some` = replace the labels.
    pub labels: Option<HashMap<String, String>>,
    /// Outer `Some` = change the expiration policy; inner `None` = disable
    /// it (unbounded).
    pub expiration_ttl_secs: Option<Option<i64>>,
    /// Outer `Some` = change the dead-letter topic; inner `None` = clear
    /// the dead-letter policy.
    pub dead_letter_topic: Option<Option<String>>,
    /// `Some` = change the dead-letter max attempts threshold.
    pub max_delivery_attempts: Option<i32>,
    /// `Some` = change the minimum retry backoff.
    pub min_retry_backoff_secs: Option<i64>,
    /// `Some` = change the maximum retry backoff.
    pub max_retry_backoff_secs: Option<i64>,
    /// `Some` = change the detached flag (only used internally by
    /// `Detach`/topic deletion, not exposed through `UpdateSubscription`).
    pub detached: Option<bool>,
    /// `Some` = change [`SubscriptionRecord::enable_exactly_once_delivery`].
    pub enable_exactly_once_delivery: Option<bool>,
    /// Outer `Some` = change the push config; inner `None` = switch to
    /// Pull.
    pub push_config: Option<Option<PushConfig>>,
}

/// Persistence and validation for [`SubscriptionRecord`]s, backed by a
/// generic [`KvStore`].
pub struct SubscriptionStore<K: KvStore> {
    kv: Arc<K>,
}

impl<K: KvStore> SubscriptionStore<K> {
    /// Creates a store backed by `kv`.
    pub fn new(kv: Arc<K>) -> Self {
        Self { kv }
    }

    /// Validates and persists a new subscription.
    ///
    /// `topic_id` is the internal id of the already-resolved topic named
    /// `topic_full_name` (resolving a topic full name to its id is the
    /// caller's responsibility — see `TopicStore`, not composed here).
    /// `attach_seq` is the topic's log tail at creation time (the initial
    /// cursor's `attach_seq` / `acked_floor`).
    pub fn create(
        &self,
        full_name: &str,
        topic_full_name: &str,
        topic_id: u64,
        attach_seq: u64,
        now_ms: i64,
        opts: CreateSubscriptionOptions,
    ) -> crate::Result<SubscriptionRecord> {
        names::SubscriptionName::parse(full_name)?;
        names::TopicName::parse(topic_full_name)?;

        if opts.has_bigquery_config {
            return Err(Error::InvalidArgument {
                field: "bigquery_config".to_string(),
                message: "bigquery_config is not supported by this server".to_string(),
            });
        }
        if opts.has_cloud_storage_config {
            return Err(Error::InvalidArgument {
                field: "cloud_storage_config".to_string(),
                message: "cloud_storage_config is not supported by this server".to_string(),
            });
        }

        let ack_deadline_secs = opts.ack_deadline_secs.unwrap_or(10);
        limits::validate_ack_deadline_secs(ack_deadline_secs)?;

        let message_retention_secs = opts.message_retention_secs.unwrap_or(604_800);
        limits::validate_subscription_retention_secs(message_retention_secs)?;

        limits::validate_filter_len(&opts.filter)?;

        let max_delivery_attempts = if opts.dead_letter_topic.is_some() {
            let requested = opts.max_delivery_attempts.unwrap_or(0);
            limits::validate_dead_letter_max_attempts(requested)?;
            if requested == 0 {
                5
            } else {
                requested
            }
        } else {
            0
        };

        let retry_policy_explicit =
            opts.min_retry_backoff_secs.is_some() || opts.max_retry_backoff_secs.is_some();
        let min_retry_backoff_secs = match opts.min_retry_backoff_secs {
            Some(v) => {
                limits::validate_retry_backoff_secs(v)?;
                v
            }
            None => 10,
        };
        let max_retry_backoff_secs = match opts.max_retry_backoff_secs {
            Some(v) => {
                limits::validate_retry_backoff_secs(v)?;
                v
            }
            None => 600,
        };

        let expiration_ttl_secs = opts.expiration_ttl_secs.unwrap_or(Some(31 * 24 * 3600));

        let name_key_bytes = keys::name_key(NameKind::Subscription, full_name);
        if self.kv.get("meta", &name_key_bytes).is_some() {
            return Err(Error::AlreadyExists {
                resource: full_name.to_string(),
            });
        }

        let sub_id = self.next_id()?;

        let record = SubscriptionRecord {
            id: sub_id,
            name: full_name.to_string(),
            topic: topic_full_name.to_string(),
            topic_id,
            ack_deadline_secs,
            retain_acked_messages: opts.retain_acked_messages,
            message_retention_secs,
            labels: opts.labels,
            enable_message_ordering: opts.enable_message_ordering,
            expiration_ttl_secs,
            filter: opts.filter,
            dead_letter_topic: opts.dead_letter_topic,
            max_delivery_attempts,
            min_retry_backoff_secs,
            max_retry_backoff_secs,
            retry_policy_explicit,
            last_activity_ts_ms: now_ms,
            detached: false,
            enable_exactly_once_delivery: opts.enable_exactly_once_delivery,
            push_config: opts.push_config,
        };

        self.persist(&record)?;

        let cursor = CursorRecord {
            attach_seq,
            acked_floor: attach_seq,
            acked_above_floor: Default::default(),
        };
        self.kv
            .put("sub", keys::cursor_key(sub_id), cursor.encode())?;
        self.kv
            .put("idx", keys::topic_sub_idx_key(topic_id, sub_id), Vec::new())?;

        Ok(record)
    }

    /// Looks up a subscription by its full resource name.
    pub fn get(&self, full_name: &str) -> crate::Result<SubscriptionRecord> {
        let name_key_bytes = keys::name_key(NameKind::Subscription, full_name);
        let id_bytes = self
            .kv
            .get("meta", &name_key_bytes)
            .ok_or_else(|| Error::NotFound {
                resource: full_name.to_string(),
            })?;
        let id = decode_id(&id_bytes, full_name)?;
        self.load(id, full_name)
    }

    /// Lists subscriptions under `project_id`, paginated.
    ///
    /// `page_size` of `0` means "no limit". `page_token` is an opaque
    /// cursor: the full name of the last subscription returned by the
    /// previous call. Returns the page plus a `Some(next_token)` when more
    /// results remain.
    pub fn list(
        &self,
        project_id: &str,
        page_size: usize,
        page_token: Option<&str>,
    ) -> crate::Result<(Vec<SubscriptionRecord>, Option<String>)> {
        let prefix = keys::name_key(NameKind::Subscription, "");
        let entries = self.kv.scan_prefix("meta", &prefix);

        let project_prefix = format!("projects/{project_id}/subscriptions/");
        let mut names: Vec<String> = entries
            .into_iter()
            .filter_map(|(k, _)| String::from_utf8(k[prefix.len()..].to_vec()).ok())
            .filter(|name| name.starts_with(&project_prefix))
            .collect();
        names.sort();

        let start = match page_token {
            Some(token) => names
                .iter()
                .position(|n| n.as_str() > token)
                .unwrap_or(names.len()),
            None => 0,
        };
        let effective_page_size = if page_size == 0 {
            usize::MAX
        } else {
            page_size
        };
        let end = start.saturating_add(effective_page_size).min(names.len());

        let mut records = Vec::with_capacity(end.saturating_sub(start));
        for name in &names[start..end] {
            records.push(self.get(name)?);
        }

        let next_page_token = if end < names.len() {
            names.get(end.saturating_sub(1)).cloned()
        } else {
            None
        };

        Ok((records, next_page_token))
    }

    /// Returns the internal ids of every subscription attached to
    /// `topic_id`, via the `idx` reverse index.
    pub fn list_by_topic_id(&self, topic_id: u64) -> crate::Result<Vec<u64>> {
        let prefix = keys::topic_sub_idx_prefix(topic_id);
        let entries = self.kv.scan_prefix("idx", &prefix);
        let mut ids = Vec::with_capacity(entries.len());
        for (k, _) in entries {
            if k.len() != 16 {
                continue;
            }
            let Ok(sub_id_bytes) = <[u8; 8]>::try_from(&k[8..16]) else {
                continue;
            };
            ids.push(u64::from_be_bytes(sub_id_bytes));
        }
        Ok(ids)
    }

    /// Applies `patch` to the subscription's mutable fields, re-validating
    /// any changed numeric fields, and persists the result.
    pub fn update(
        &self,
        full_name: &str,
        patch: SubscriptionUpdatePatch,
    ) -> crate::Result<SubscriptionRecord> {
        let mut record = self.get(full_name)?;

        if let Some(v) = patch.ack_deadline_secs {
            limits::validate_ack_deadline_secs(v)?;
            record.ack_deadline_secs = v;
        }
        if let Some(v) = patch.retain_acked_messages {
            record.retain_acked_messages = v;
        }
        if let Some(v) = patch.message_retention_secs {
            limits::validate_subscription_retention_secs(v)?;
            record.message_retention_secs = v;
        }
        if let Some(v) = patch.labels {
            record.labels = v;
        }
        if let Some(v) = patch.expiration_ttl_secs {
            record.expiration_ttl_secs = v;
        }
        if let Some(v) = patch.dead_letter_topic {
            record.dead_letter_topic = v;
        }
        if let Some(v) = patch.max_delivery_attempts {
            limits::validate_dead_letter_max_attempts(v)?;
            record.max_delivery_attempts = if v == 0 { 5 } else { v };
        }
        if let Some(v) = patch.min_retry_backoff_secs {
            limits::validate_retry_backoff_secs(v)?;
            record.min_retry_backoff_secs = v;
            record.retry_policy_explicit = true;
        }
        if let Some(v) = patch.max_retry_backoff_secs {
            limits::validate_retry_backoff_secs(v)?;
            record.max_retry_backoff_secs = v;
            record.retry_policy_explicit = true;
        }
        if let Some(v) = patch.detached {
            record.detached = v;
        }
        if let Some(v) = patch.enable_exactly_once_delivery {
            record.enable_exactly_once_delivery = v;
        }
        if let Some(v) = patch.push_config {
            record.push_config = v;
        }

        self.persist(&record)?;
        Ok(record)
    }

    /// Sets `topic` to the [`names::DELETED_TOPIC`] sentinel for a
    /// subscription identified by internal id (used when its topic is
    /// deleted — the caller iterates [`Self::list_by_topic_id`] and calls
    /// this for each).
    pub fn set_topic_to_deleted(&self, sub_id: u64) -> crate::Result<()> {
        let mut record = self.load(sub_id, &format!("sub_id={sub_id}"))?;
        record.topic = names::DELETED_TOPIC.to_string();
        self.persist(&record)
    }

    /// Records `now_ms` as `last_activity_ts_ms` for `sub_id` — last activity
    /// is updated on pull/ack/streaming. A missing
    /// subscription (deleted concurrently) is silently ignored — this is
    /// best-effort bookkeeping, not a request the caller's own operation
    /// should fail over.
    pub fn touch(&self, sub_id: u64, now_ms: i64) -> crate::Result<()> {
        let Ok(mut record) = self.load(sub_id, &format!("sub_id={sub_id}")) else {
            return Ok(());
        };
        record.last_activity_ts_ms = now_ms;
        self.persist(&record)
    }

    /// Marks a subscription detached (one-way: there is no un-detach).
    pub fn detach(&self, full_name: &str) -> crate::Result<()> {
        let mut record = self.get(full_name)?;
        record.detached = true;
        self.persist(&record)
    }

    /// Sets or clears the push config (for `ModifyPushConfig`).
    /// `None` switches the subscription to Pull.
    pub fn set_push_config(
        &self,
        full_name: &str,
        push_config: Option<PushConfig>,
    ) -> crate::Result<()> {
        let mut record = self.get(full_name)?;
        record.push_config = push_config;
        self.persist(&record)
    }

    /// Removes a subscription entirely: its record, name index, cursor,
    /// and `idx` row.
    pub fn delete(&self, full_name: &str) -> crate::Result<()> {
        let record = self.get(full_name)?;
        self.kv.delete("meta", &keys::sub_key(record.id))?;
        self.kv.delete(
            "meta",
            &keys::name_key(NameKind::Subscription, &record.name),
        )?;
        self.kv.delete("sub", &keys::cursor_key(record.id))?;
        self.kv
            .delete("idx", &keys::topic_sub_idx_key(record.topic_id, record.id))?;
        Ok(())
    }

    /// Deletes every subscription whose `expiration_ttl_secs` has elapsed
    /// since `last_activity_ts_ms` (`expiration_policy.ttl` elapsed triggers
    /// automatic deletion). Returns the full names of the
    /// subscriptions removed, for logging/metrics/tests.
    ///
    /// Scans every subscription regardless of project (the `meta`
    /// keyspace's `s/` prefix, not the project-scoped name index that
    /// [`Self::list`] uses) since expiration is a global sweep, not a
    /// per-project listing.
    pub fn sweep_expired(&self, now_ms: i64) -> crate::Result<Vec<String>> {
        let mut removed = Vec::new();
        for record in self.list_all() {
            let Some(ttl_secs) = record.expiration_ttl_secs else {
                continue;
            };
            let expires_at_ms = record
                .last_activity_ts_ms
                .saturating_add(ttl_secs.saturating_mul(1000));
            if now_ms >= expires_at_ms {
                self.delete(&record.name)?;
                removed.push(record.name);
            }
        }
        Ok(removed)
    }

    /// Every subscription, regardless of project — the `meta` keyspace's
    /// `s/` prefix, not the project-scoped name index [`Self::list`]
    /// uses. Skips (rather than fails on) any corrupt record, matching
    /// the tolerance [`Self::sweep_expired`] already relied on before
    /// this was extracted from it. Used by global sweeps
    /// ([`Self::sweep_expired`]) and by push-dispatcher reconciliation
    /// (`crate::push::manager`), both of which need every subscription,
    /// not one project's page.
    pub fn list_all(&self) -> Vec<SubscriptionRecord> {
        self.kv
            .scan_prefix("meta", b"s/")
            .into_iter()
            .filter_map(|(_key, value)| serde_json::from_slice(&value).ok())
            .collect()
    }

    /// Writes `record` to both `meta/s/{id}` and `meta/name/s/{name}`.
    fn persist(&self, record: &SubscriptionRecord) -> crate::Result<()> {
        let bytes = serde_json::to_vec(record).map_err(|e| Error::Internal {
            message: format!("failed to encode subscription record: {e}"),
        })?;
        self.kv.put("meta", keys::sub_key(record.id), bytes)?;
        self.kv.put(
            "meta",
            keys::name_key(NameKind::Subscription, &record.name),
            record.id.to_be_bytes().to_vec(),
        )?;
        Ok(())
    }

    /// Loads a subscription record by internal id (e.g. for resolving the
    /// ids `Self::list_by_topic_id` returns into full records/names).
    pub fn get_by_id(&self, id: u64) -> crate::Result<SubscriptionRecord> {
        self.load(id, &format!("subscription id={id}"))
    }

    /// Loads a subscription record by internal id. `resource_for_err` is
    /// used only to build a readable `NotFound`/`Internal` message.
    fn load(&self, id: u64, resource_for_err: &str) -> crate::Result<SubscriptionRecord> {
        let bytes = self
            .kv
            .get("meta", &keys::sub_key(id))
            .ok_or_else(|| Error::NotFound {
                resource: resource_for_err.to_string(),
            })?;
        serde_json::from_slice(&bytes).map_err(|e| Error::Internal {
            message: format!("failed to decode subscription record: {e}"),
        })
    }

    /// Allocates the next id from the shared `id_seq` counter (`meta`
    /// keyspace), shared with `TopicStore`/`SnapshotStore` so ids never
    /// collide across resource kinds.
    fn next_id(&self) -> crate::Result<u64> {
        let key = keys::id_seq_key();
        let current = match self.kv.get("meta", &key) {
            Some(bytes) => decode_id(&bytes, "id_seq")?,
            None => 0,
        };
        let next = current
            .checked_add(1)
            .ok_or_else(|| Error::ResourceExhausted {
                message: "subscription id space exhausted".to_string(),
            })?;
        self.kv.put("meta", key, next.to_be_bytes().to_vec())?;
        Ok(next)
    }
}

/// Decodes an 8-byte big-endian id. `resource_for_err` is used only to
/// build a readable error message on corrupt data.
fn decode_id(bytes: &[u8], resource_for_err: &str) -> crate::Result<u64> {
    let arr: [u8; 8] = bytes.try_into().map_err(|_| Error::Internal {
        message: format!("corrupt id bytes for {resource_for_err}"),
    })?;
    Ok(u64::from_be_bytes(arr))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::store::kv::MemKv;

    fn new_store() -> SubscriptionStore<MemKv> {
        SubscriptionStore::new(Arc::new(MemKv::new()))
    }

    #[test]
    fn create_and_get_round_trip_with_initial_cursor() {
        let kv = Arc::new(MemKv::new());
        let store = SubscriptionStore::new(kv.clone());

        let record = store
            .create(
                "projects/p/subscriptions/sub-a",
                "projects/p/topics/topic-a",
                1,
                42,
                1_000,
                CreateSubscriptionOptions::default(),
            )
            .expect("create should succeed");

        assert_eq!(record.name, "projects/p/subscriptions/sub-a");
        assert_eq!(record.topic, "projects/p/topics/topic-a");
        assert_eq!(record.topic_id, 1);
        assert_eq!(record.ack_deadline_secs, 10);
        assert_eq!(record.message_retention_secs, 604_800);
        assert_eq!(record.expiration_ttl_secs, Some(31 * 24 * 3600));
        assert_eq!(record.max_delivery_attempts, 0);
        assert_eq!(record.min_retry_backoff_secs, 10);
        assert_eq!(record.max_retry_backoff_secs, 600);
        assert!(!record.detached);

        let fetched = store
            .get("projects/p/subscriptions/sub-a")
            .expect("get should succeed");
        assert_eq!(fetched.id, record.id);
        assert_eq!(fetched.name, record.name);

        let cursor_bytes = kv
            .get("sub", &keys::cursor_key(record.id))
            .expect("cursor record should be present");
        let cursor = CursorRecord::decode(&cursor_bytes).expect("cursor should decode");
        assert_eq!(cursor.attach_seq, 42);
        assert_eq!(cursor.acked_floor, 42);
        assert!(cursor.acked_above_floor.is_empty());
    }

    #[test]
    fn duplicate_create_is_already_exists() {
        let store = new_store();
        store
            .create(
                "projects/p/subscriptions/dup",
                "projects/p/topics/top",
                1,
                0,
                1_000,
                CreateSubscriptionOptions::default(),
            )
            .expect("first create should succeed");

        let err = store
            .create(
                "projects/p/subscriptions/dup",
                "projects/p/topics/top",
                1,
                0,
                1_000,
                CreateSubscriptionOptions::default(),
            )
            .expect_err("duplicate create should fail");
        assert!(matches!(err, Error::AlreadyExists { .. }));
    }

    #[test]
    fn invalid_ack_deadline_is_rejected() {
        let store = new_store();
        let opts = CreateSubscriptionOptions {
            ack_deadline_secs: Some(5),
            ..Default::default()
        };
        let err = store
            .create(
                "projects/p/subscriptions/sub",
                "projects/p/topics/top",
                1,
                0,
                1_000,
                opts,
            )
            .expect_err("should be rejected");
        assert!(matches!(err, Error::InvalidArgument { .. }));
    }

    #[test]
    fn bigquery_config_is_rejected() {
        let store = new_store();
        let opts = CreateSubscriptionOptions {
            has_bigquery_config: true,
            ..Default::default()
        };
        let err = store
            .create(
                "projects/p/subscriptions/sub",
                "projects/p/topics/top",
                1,
                0,
                1_000,
                opts,
            )
            .expect_err("should be rejected");
        assert!(matches!(err, Error::InvalidArgument { .. }));
    }

    #[test]
    fn cloud_storage_config_is_rejected() {
        let store = new_store();
        let opts = CreateSubscriptionOptions {
            has_cloud_storage_config: true,
            ..Default::default()
        };
        let err = store
            .create(
                "projects/p/subscriptions/sub",
                "projects/p/topics/top",
                1,
                0,
                1_000,
                opts,
            )
            .expect_err("should be rejected");
        assert!(matches!(err, Error::InvalidArgument { .. }));
    }

    #[test]
    fn filter_too_long_is_rejected() {
        let store = new_store();
        let opts = CreateSubscriptionOptions {
            filter: "a".repeat(limits::MAX_FILTER_CHARS + 1),
            ..Default::default()
        };
        let err = store
            .create(
                "projects/p/subscriptions/sub",
                "projects/p/topics/top",
                1,
                0,
                1_000,
                opts,
            )
            .expect_err("should be rejected");
        assert!(matches!(err, Error::InvalidArgument { .. }));
    }

    #[test]
    fn dead_letter_zero_max_attempts_substitutes_default() {
        let store = new_store();
        let opts = CreateSubscriptionOptions {
            dead_letter_topic: Some("projects/p/topics/dlq".to_string()),
            max_delivery_attempts: Some(0),
            ..Default::default()
        };
        let record = store
            .create(
                "projects/p/subscriptions/sub",
                "projects/p/topics/top",
                1,
                0,
                1_000,
                opts,
            )
            .expect("create should succeed");
        assert_eq!(record.max_delivery_attempts, 5);
    }

    #[test]
    fn list_by_topic_id_scopes_to_topic() {
        let store = new_store();
        let a = store
            .create(
                "projects/p/subscriptions/sub-a",
                "projects/p/topics/top1",
                1,
                0,
                1_000,
                CreateSubscriptionOptions::default(),
            )
            .expect("create a");
        let b = store
            .create(
                "projects/p/subscriptions/sub-b",
                "projects/p/topics/top1",
                1,
                0,
                1_000,
                CreateSubscriptionOptions::default(),
            )
            .expect("create b");
        let c = store
            .create(
                "projects/p/subscriptions/sub-c",
                "projects/p/topics/top2",
                2,
                0,
                1_000,
                CreateSubscriptionOptions::default(),
            )
            .expect("create c");

        let mut ids_for_topic_1 = store.list_by_topic_id(1).expect("list should succeed");
        ids_for_topic_1.sort_unstable();
        let mut expected = vec![a.id, b.id];
        expected.sort_unstable();
        assert_eq!(ids_for_topic_1, expected);

        let ids_for_topic_2 = store.list_by_topic_id(2).expect("list should succeed");
        assert_eq!(ids_for_topic_2, vec![c.id]);
    }

    #[test]
    fn delete_then_get_is_not_found() {
        let store = new_store();
        store
            .create(
                "projects/p/subscriptions/sub",
                "projects/p/topics/top",
                1,
                0,
                1_000,
                CreateSubscriptionOptions::default(),
            )
            .expect("create should succeed");
        store
            .delete("projects/p/subscriptions/sub")
            .expect("delete should succeed");
        let err = store
            .get("projects/p/subscriptions/sub")
            .expect_err("should be not found");
        assert!(matches!(err, Error::NotFound { .. }));
    }

    #[test]
    fn set_topic_to_deleted_updates_topic_field() {
        let store = new_store();
        let record = store
            .create(
                "projects/p/subscriptions/sub",
                "projects/p/topics/top",
                1,
                0,
                1_000,
                CreateSubscriptionOptions::default(),
            )
            .expect("create should succeed");
        store
            .set_topic_to_deleted(record.id)
            .expect("set_topic_to_deleted should succeed");
        let fetched = store
            .get("projects/p/subscriptions/sub")
            .expect("get should succeed");
        assert_eq!(fetched.topic, names::DELETED_TOPIC);
    }

    #[test]
    fn detach_sets_detached_true() {
        let store = new_store();
        store
            .create(
                "projects/p/subscriptions/sub",
                "projects/p/topics/top",
                1,
                0,
                1_000,
                CreateSubscriptionOptions::default(),
            )
            .expect("create should succeed");
        store
            .detach("projects/p/subscriptions/sub")
            .expect("detach should succeed");
        let fetched = store
            .get("projects/p/subscriptions/sub")
            .expect("get should succeed");
        assert!(fetched.detached);
    }

    #[test]
    fn set_push_config_updates_and_clears() {
        let store = new_store();
        store
            .create(
                "projects/p/subscriptions/sub",
                "projects/p/topics/top",
                1,
                0,
                1_000,
                CreateSubscriptionOptions::default(),
            )
            .expect("create should succeed");

        store
            .set_push_config(
                "projects/p/subscriptions/sub",
                Some(PushConfig {
                    endpoint: "https://example.com/push".to_string(),
                    no_wrapper: false,
                    write_metadata: false,
                }),
            )
            .expect("set_push_config should succeed");
        let fetched = store
            .get("projects/p/subscriptions/sub")
            .expect("get should succeed");
        assert_eq!(
            fetched.push_config.map(|c| c.endpoint),
            Some("https://example.com/push".to_string())
        );

        store
            .set_push_config("projects/p/subscriptions/sub", None)
            .expect("clearing push config should succeed");
        let fetched = store
            .get("projects/p/subscriptions/sub")
            .expect("get should succeed");
        assert_eq!(fetched.push_config, None);
    }

    #[test]
    fn update_applies_mutable_fields_and_revalidates() {
        let store = new_store();
        store
            .create(
                "projects/p/subscriptions/sub",
                "projects/p/topics/top",
                1,
                0,
                1_000,
                CreateSubscriptionOptions::default(),
            )
            .expect("create should succeed");

        let patch = SubscriptionUpdatePatch {
            ack_deadline_secs: Some(30),
            retain_acked_messages: Some(true),
            ..Default::default()
        };
        let updated = store
            .update("projects/p/subscriptions/sub", patch)
            .expect("update should succeed");
        assert_eq!(updated.ack_deadline_secs, 30);
        assert!(updated.retain_acked_messages);

        let bad_patch = SubscriptionUpdatePatch {
            ack_deadline_secs: Some(1),
            ..Default::default()
        };
        let err = store
            .update("projects/p/subscriptions/sub", bad_patch)
            .expect_err("invalid ack_deadline_secs should be rejected");
        assert!(matches!(err, Error::InvalidArgument { .. }));
    }

    #[test]
    fn list_paginates_within_project_and_excludes_others() {
        let store = new_store();
        for i in 0..3 {
            store
                .create(
                    &format!("projects/p/subscriptions/sub{i}"),
                    "projects/p/topics/top",
                    1,
                    0,
                    1_000,
                    CreateSubscriptionOptions::default(),
                )
                .expect("create should succeed");
        }
        store
            .create(
                "projects/other/subscriptions/sub0",
                "projects/other/topics/top",
                1,
                0,
                1_000,
                CreateSubscriptionOptions::default(),
            )
            .expect("create should succeed");

        let (page1, token1) = store.list("p", 2, None).expect("list should succeed");
        assert_eq!(page1.len(), 2);
        assert!(token1.is_some());

        let (page2, token2) = store
            .list("p", 2, token1.as_deref())
            .expect("list should succeed");
        assert_eq!(page2.len(), 1);
        assert!(token2.is_none());

        let all_names: Vec<&str> = page1
            .iter()
            .chain(page2.iter())
            .map(|r| r.name.as_str())
            .collect();
        assert!(all_names
            .iter()
            .all(|n| n.starts_with("projects/p/subscriptions/")));
    }
}
