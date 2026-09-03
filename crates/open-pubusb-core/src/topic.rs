//! Topic CRUD and message-log append.
//!
//! [`TopicStore`] is generic over [`crate::store::kv::KvStore`] rather than
//! a concrete storage engine: the real fjall-backed `Store` (a later task)
//! implements that same trait, so this module's logic does not change when
//! the backing store is swapped in.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::limits;
use crate::names::TopicName;
use crate::store::codec::MessageRecord;
use crate::store::keys::{self, NameKind};
use crate::store::kv::KvStore;

/// The `meta` keyspace: topic records, the name→id index, and the id/seq
/// counters.
const META: &str = "meta";
/// The `msg` keyspace: the per-topic append-only message log.
const MSG: &str = "msg";

/// The domain-level representation of a Pub/Sub Topic.
///
/// This is a plain core-layer struct, independent of the generated proto
/// types in `open-pubusb-proto` — conversion to/from `google.pubsub.v1.Topic`
/// happens in the `open-pubusb` binary crate.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TopicRecord {
    /// The topic's internal id (distinct from its name, so that
    /// rename/recreate cannot collide with a still-referenced old id).
    pub id: u64,
    /// The full resource name: `projects/{project}/topics/{topic}`.
    pub name: String,
    /// User-supplied labels.
    pub labels: HashMap<String, String>,
    /// `message_retention_duration`, in seconds. `0` means "unset" (no
    /// explicit topic-level retention was requested at create/update time).
    pub message_retention_secs: i64,
    /// `kms_key_name`, accepted and stored as-is (no encryption semantics
    /// implemented).
    pub kms_key_name: Option<String>,
}

/// Topic CRUD and message-log append, backed by a [`KvStore`].
pub struct TopicStore<K: KvStore> {
    kv: Arc<K>,
}

impl<K: KvStore> TopicStore<K> {
    /// Creates a `TopicStore` over the given [`KvStore`].
    pub fn new(kv: Arc<K>) -> Self {
        Self { kv }
    }

    /// Creates a new topic.
    ///
    /// Validates `full_name` and (when present) `message_retention_secs`,
    /// rejects `has_schema_settings` / `has_ingestion_settings` (unsupported
    /// features, per the contract), and rejects a duplicate name with
    /// [`Error::AlreadyExists`].
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        &self,
        full_name: &str,
        labels: HashMap<String, String>,
        message_retention_secs: Option<i64>,
        kms_key_name: Option<String>,
        has_schema_settings: bool,
        has_ingestion_settings: bool,
    ) -> Result<TopicRecord> {
        TopicName::parse(full_name)?;

        if has_schema_settings {
            return Err(Error::InvalidArgument {
                field: "schema_settings".to_string(),
                message: "schema_settings is not supported by this server".to_string(),
            });
        }
        if has_ingestion_settings {
            return Err(Error::InvalidArgument {
                field: "ingestion_data_source_settings".to_string(),
                message: "ingestion_data_source_settings is not supported by this server"
                    .to_string(),
            });
        }
        if let Some(secs) = message_retention_secs {
            limits::validate_topic_retention_secs(secs)?;
        }

        let name_key = keys::name_key(NameKind::Topic, full_name);
        if self.kv.get(META, &name_key).is_some() {
            return Err(Error::AlreadyExists {
                resource: full_name.to_string(),
            });
        }

        let id = self.next_id()?;
        let record = TopicRecord {
            id,
            name: full_name.to_string(),
            labels,
            message_retention_secs: message_retention_secs.unwrap_or(0),
            kms_key_name,
        };
        self.put_record(&record)?;
        self.kv.put(META, name_key, id.to_be_bytes().to_vec())?;
        Ok(record)
    }

    /// Resolves a full topic name to its record.
    pub fn get(&self, full_name: &str) -> Result<TopicRecord> {
        let id = self.resolve_id(full_name)?;
        self.load_record(id)
    }

    /// Every topic across every project — for the `open_pubusb_topics` metrics
    /// gauge (`crate::metrics::set_topics`), which has no reason to be
    /// project-scoped.
    ///
    /// The `t/` prefix also matches `topic_seq_counter_key` rows
    /// (`t/seq/{id}`, a raw big-endian `u64`, not JSON) alongside the
    /// `TopicRecord` rows this wants — `filter_map`'s `.ok()` silently
    /// drops those (they fail to parse as a `TopicRecord`), so this stays
    /// correct without a second, narrower prefix; scanning a few extra
    /// non-matching rows periodically is cheap at the topic counts this
    /// server targets (on the order of 1,000 topics).
    pub fn list_all(&self) -> Vec<TopicRecord> {
        self.kv
            .scan_prefix(META, b"t/")
            .into_iter()
            .filter_map(|(_key, value)| serde_json::from_slice(&value).ok())
            .collect()
    }

    /// Lists topics belonging to `project_id`, ordered by name, paginating
    /// with an opaque token (the last-returned name).
    ///
    /// Returns the page of records and, if more topics remain, a
    /// `Some(next_token)` to pass back as `page_token` on the next call.
    pub fn list(
        &self,
        project_id: &str,
        page_size: usize,
        page_token: Option<&str>,
    ) -> Result<(Vec<TopicRecord>, Option<String>)> {
        let scan_prefix =
            keys::name_key(NameKind::Topic, &format!("projects/{project_id}/topics/"));
        let entries = self.kv.scan_prefix(META, &scan_prefix);

        let mut records: Vec<TopicRecord> = Vec::with_capacity(entries.len());
        for (_key, value) in entries {
            let Some(id) = decode_u64(&value) else {
                continue;
            };
            records.push(self.load_record(id)?);
        }
        records.sort_by(|a, b| a.name.cmp(&b.name));

        let start = match page_token {
            Some(token) => records.partition_point(|r| r.name.as_str() <= token),
            None => 0,
        };
        let end = start.saturating_add(page_size).min(records.len());
        let page: Vec<TopicRecord> = records[start..end].to_vec();
        let next_token = if end < records.len() {
            page.last().map(|r| r.name.clone())
        } else {
            None
        };
        Ok((page, next_token))
    }

    /// Updates the mutable fields of a topic. Only fields passed as `Some`
    /// are changed; `update_mask` allowlisting is the gRPC-layer's
    /// responsibility (a later task) — this method simply applies whatever
    /// it is given.
    pub fn update(
        &self,
        full_name: &str,
        labels: Option<HashMap<String, String>>,
        message_retention_secs: Option<Option<i64>>,
        kms_key_name: Option<Option<String>>,
    ) -> Result<TopicRecord> {
        let mut record = self.get(full_name)?;

        if let Some(labels) = labels {
            record.labels = labels;
        }
        if let Some(retention) = message_retention_secs {
            match retention {
                Some(secs) => {
                    limits::validate_topic_retention_secs(secs)?;
                    record.message_retention_secs = secs;
                }
                None => record.message_retention_secs = 0,
            }
        }
        if let Some(kms) = kms_key_name {
            record.kms_key_name = kms;
        }

        self.put_record(&record)?;
        Ok(record)
    }

    /// Deletes a topic. Does not touch any attached subscriptions —
    /// composing this with `SubscriptionStore` to detach them (setting
    /// `topic` to [`crate::names::DELETED_TOPIC`]) belongs to a
    /// higher-level service.
    pub fn delete(&self, full_name: &str) -> Result<()> {
        let id = self.resolve_id(full_name)?;
        self.kv.delete(META, &keys::topic_key(id))?;
        self.kv
            .delete(META, &keys::name_key(NameKind::Topic, full_name))?;
        Ok(())
    }

    /// Appends a batch of messages to `topic_id`'s message log.
    ///
    /// `messages` are `(payload, attributes, ordering_key)` tuples, in
    /// publish order. Validates each message and the batch as a whole
    /// before writing anything. Returns the assigned `seq` numbers in
    /// input order; deriving `message_id` (`seq.to_string()`) is the
    /// caller's responsibility.
    pub fn append(
        &self,
        topic_id: u64,
        messages: Vec<(Vec<u8>, HashMap<String, String>, String)>,
        now_ms: i64,
        retention_secs: i64,
    ) -> Result<Vec<u64>> {
        let mut total_bytes: usize = 0;
        for (payload, attributes, ordering_key) in &messages {
            limits::validate_message(payload.len(), attributes, ordering_key)?;
            total_bytes = total_bytes.saturating_add(payload.len());
            for (key, value) in attributes {
                total_bytes = total_bytes
                    .saturating_add(key.len())
                    .saturating_add(value.len());
            }
        }
        limits::validate_publish_batch(messages.len(), total_bytes)?;

        let expire_at_ms = now_ms.saturating_add(retention_secs.saturating_mul(1000));
        let mut seqs = Vec::with_capacity(messages.len());
        for (payload, attributes, ordering_key) in messages {
            let seq = self.next_seq(topic_id)?;
            let record = MessageRecord {
                publish_ts_ms: now_ms,
                ordering_key,
                attributes,
                payload,
                expire_at_ms,
            };
            self.kv
                .put(MSG, keys::msg_key(topic_id, seq), record.encode())?;
            seqs.push(seq);
        }
        Ok(seqs)
    }

    // -- internal helpers ---------------------------------------------

    fn resolve_id(&self, full_name: &str) -> Result<u64> {
        TopicName::parse(full_name)?;
        let name_key = keys::name_key(NameKind::Topic, full_name);
        let id_bytes = self
            .kv
            .get(META, &name_key)
            .ok_or_else(|| Error::NotFound {
                resource: full_name.to_string(),
            })?;
        decode_u64(&id_bytes).ok_or_else(|| Error::Internal {
            message: format!("corrupt topic id index entry for {full_name:?}"),
        })
    }

    fn load_record(&self, id: u64) -> Result<TopicRecord> {
        let bytes = self
            .kv
            .get(META, &keys::topic_key(id))
            .ok_or_else(|| Error::Internal {
                message: format!("missing topic record for id {id}"),
            })?;
        serde_json::from_slice(&bytes).map_err(|e| Error::Internal {
            message: format!("failed to decode topic record for id {id}: {e}"),
        })
    }

    fn put_record(&self, record: &TopicRecord) -> Result<()> {
        let bytes = serde_json::to_vec(record).map_err(|e| Error::Internal {
            message: format!("failed to encode topic record for id {}: {e}", record.id),
        })?;
        self.kv.put(META, keys::topic_key(record.id), bytes)?;
        Ok(())
    }

    fn read_counter(&self, key: &[u8]) -> u64 {
        self.kv
            .get(META, key)
            .as_deref()
            .and_then(decode_u64)
            .unwrap_or(0)
    }

    fn next_id(&self) -> Result<u64> {
        let key = keys::id_seq_key();
        let next = self.read_counter(&key).saturating_add(1);
        self.kv.put(META, key, next.to_be_bytes().to_vec())?;
        Ok(next)
    }

    /// Allocates the next sequence number for `topic_id`, **1-indexed**
    /// (the first message ever published to a topic gets seq `1`, never
    /// `0`). This matters beyond cosmetics: `0` is reserved as the
    /// "nothing delivered/acked yet" sentinel for
    /// [`crate::store::codec::CursorRecord::acked_floor`] and
    /// `attach_seq` (a subscription's `attach_seq` is the
    /// log tail at creation time, and `seq > attach_seq` is the
    /// eligibility test `crate::delivery::engine::DeliveryEngine` uses) —
    /// a 0-indexed first message would collide with that sentinel and
    /// never be considered eligible for delivery to a subscription created
    /// before it was published.
    fn next_seq(&self, topic_id: u64) -> Result<u64> {
        let key = keys::topic_seq_counter_key(topic_id);
        let seq = self.read_counter(&key).saturating_add(1);
        self.kv.put(META, key, seq.to_be_bytes().to_vec())?;
        Ok(seq)
    }

    /// The topic's current log tail: the seq of the most recently
    /// published message, or `0` if none has been published yet. Used as
    /// a new subscription's `attach_seq` (the log tail at creation time)
    /// so it only ever receives messages published
    /// *after* it was created.
    pub fn current_tail(&self, topic_id: u64) -> u64 {
        self.read_counter(&keys::topic_seq_counter_key(topic_id))
    }

    /// The smallest seq on `topic_id`'s log whose `publish_ts_ms >=
    /// target_ts_ms`, or `current_tail(topic_id) + 1` (one past the tail)
    /// if every published message predates `target_ts_ms` — i.e. the
    /// exclusive upper boundary a Seek-to-time treats as
    /// "everything before this is done, everything from this point on is
    /// unacked".
    ///
    /// Binary search over `[1, current_tail]`: `publish_ts_ms` is
    /// non-decreasing in `seq` (each `append` call stamps every message in
    /// its batch with one `now_ms`, and the wall clock — real or a
    /// `MockClock` a caller only ever advances — never goes backward
    /// between calls), so the log is sorted by publish time exactly as it
    /// is by seq.
    pub fn seq_boundary_for_time(&self, topic_id: u64, target_ts_ms: i64) -> u64 {
        let tail = self.current_tail(topic_id);
        let (mut lo, mut hi) = (1u64, tail.saturating_add(1));
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let publish_ts_ms = self
                .kv
                .get(MSG, &keys::msg_key(topic_id, mid))
                .and_then(|bytes| MessageRecord::decode(&bytes).ok())
                .map(|m| m.publish_ts_ms)
                // A gap (already-retention-swept message) can't tell us
                // which side of `target_ts_ms` it fell on; treating it as
                // "before" is the safe direction — it only risks including
                // a couple of already-unavailable seqs in the "unacked"
                // side, which have nothing left to redeliver anyway.
                .unwrap_or(i64::MIN);
            if publish_ts_ms < target_ts_ms {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
}

fn decode_u64(bytes: &[u8]) -> Option<u64> {
    let arr: [u8; 8] = bytes.try_into().ok()?;
    Some(u64::from_be_bytes(arr))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::store::kv::MemKv;

    fn store() -> TopicStore<MemKv> {
        TopicStore::new(Arc::new(MemKv::new()))
    }

    fn labels() -> HashMap<String, String> {
        HashMap::new()
    }

    #[test]
    fn create_then_get_round_trip() {
        let ts = store();
        let created = ts
            .create("projects/p/topics/top1", labels(), None, None, false, false)
            .unwrap();
        assert_eq!(created.name, "projects/p/topics/top1");
        assert_eq!(created.id, 1);
        assert_eq!(created.message_retention_secs, 0);
        assert_eq!(created.kms_key_name, None);

        let fetched = ts.get("projects/p/topics/top1").unwrap();
        assert_eq!(fetched, created);
    }

    #[test]
    fn duplicate_create_errs_already_exists() {
        let ts = store();
        ts.create("projects/p/topics/top1", labels(), None, None, false, false)
            .unwrap();
        let err = ts
            .create("projects/p/topics/top1", labels(), None, None, false, false)
            .unwrap_err();
        assert!(matches!(err, Error::AlreadyExists { .. }));
    }

    #[test]
    fn get_missing_errs_not_found() {
        let ts = store();
        let err = ts.get("projects/p/topics/missing").unwrap_err();
        assert!(matches!(err, Error::NotFound { .. }));
    }

    #[test]
    fn create_invalid_name_errs_invalid_argument() {
        let ts = store();
        let err = ts
            .create("not-a-valid-name", labels(), None, None, false, false)
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument { .. }));
    }

    #[test]
    fn create_rejects_schema_settings() {
        let ts = store();
        let err = ts
            .create("projects/p/topics/top1", labels(), None, None, true, false)
            .unwrap_err();
        match err {
            Error::InvalidArgument { field, message } => {
                assert_eq!(field, "schema_settings");
                assert!(message.contains("not supported by this server"));
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn create_rejects_ingestion_settings() {
        let ts = store();
        let err = ts
            .create("projects/p/topics/top1", labels(), None, None, false, true)
            .unwrap_err();
        match err {
            Error::InvalidArgument { field, message } => {
                assert_eq!(field, "ingestion_data_source_settings");
                assert!(message.contains("not supported by this server"));
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn create_retention_out_of_range_rejected() {
        let ts = store();
        let err = ts
            .create(
                "projects/p/topics/top1",
                labels(),
                Some(1), // below MIN_TOPIC_RETENTION_SECS
                None,
                false,
                false,
            )
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument { .. }));
    }

    #[test]
    fn list_paginates_across_all_results_without_gaps_or_duplicates() {
        let ts = store();
        for i in 0..5 {
            ts.create(
                &format!("projects/p/topics/top{i}"),
                labels(),
                None,
                None,
                false,
                false,
            )
            .unwrap();
        }

        let mut seen: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let (page, next) = ts.list("p", 2, token.as_deref()).unwrap();
            assert!(page.len() <= 2);
            for r in &page {
                seen.push(r.name.clone());
            }
            match next {
                Some(t) => token = Some(t),
                None => break,
            }
        }

        seen.sort();
        seen.dedup();
        assert_eq!(seen.len(), 5);
        let expected: Vec<String> = (0..5)
            .map(|i| format!("projects/p/topics/top{i}"))
            .collect();
        assert_eq!(seen, expected);
    }

    #[test]
    fn update_labels_only_leaves_retention_unchanged() {
        let ts = store();
        ts.create(
            "projects/p/topics/top1",
            labels(),
            Some(700),
            None,
            false,
            false,
        )
        .unwrap();

        let mut new_labels = HashMap::new();
        new_labels.insert("env".to_string(), "prod".to_string());
        let updated = ts
            .update(
                "projects/p/topics/top1",
                Some(new_labels.clone()),
                None,
                None,
            )
            .unwrap();
        assert_eq!(updated.labels, new_labels);
        assert_eq!(updated.message_retention_secs, 700);
    }

    #[test]
    fn update_retention_only_leaves_labels_unchanged() {
        let ts = store();
        let mut initial_labels = HashMap::new();
        initial_labels.insert("env".to_string(), "dev".to_string());
        ts.create(
            "projects/p/topics/top1",
            initial_labels.clone(),
            None,
            None,
            false,
            false,
        )
        .unwrap();

        let updated = ts
            .update("projects/p/topics/top1", None, Some(Some(900)), None)
            .unwrap();
        assert_eq!(updated.labels, initial_labels);
        assert_eq!(updated.message_retention_secs, 900);
    }

    #[test]
    fn delete_then_get_errs_not_found() {
        let ts = store();
        ts.create("projects/p/topics/top1", labels(), None, None, false, false)
            .unwrap();
        ts.delete("projects/p/topics/top1").unwrap();
        let err = ts.get("projects/p/topics/top1").unwrap_err();
        assert!(matches!(err, Error::NotFound { .. }));
    }

    #[test]
    fn append_assigns_monotonically_increasing_seq_in_order() {
        let ts = store();
        let created = ts
            .create("projects/p/topics/top1", labels(), None, None, false, false)
            .unwrap();

        let messages = vec![
            (b"a".to_vec(), HashMap::new(), String::new()),
            (b"b".to_vec(), HashMap::new(), String::new()),
            (b"c".to_vec(), HashMap::new(), String::new()),
        ];
        let seqs = ts.append(created.id, messages, 1_000, 600).unwrap();
        assert_eq!(seqs.len(), 3);
        assert!(seqs[0] < seqs[1]);
        assert!(seqs[1] < seqs[2]);

        // A second batch continues from where the first left off.
        let more = vec![(b"d".to_vec(), HashMap::new(), String::new())];
        let more_seqs = ts.append(created.id, more, 2_000, 600).unwrap();
        assert!(more_seqs[0] > seqs[2]);
    }

    #[test]
    fn append_validates_batch_size_limit() {
        let ts = store();
        let created = ts
            .create("projects/p/topics/top1", labels(), None, None, false, false)
            .unwrap();

        let messages: Vec<(Vec<u8>, HashMap<String, String>, String)> = (0
            ..=limits::MAX_PUBLISH_BATCH_MESSAGES)
            .map(|_| (Vec::new(), HashMap::new(), String::new()))
            .collect();
        let err = ts.append(created.id, messages, 0, 600).unwrap_err();
        assert!(matches!(err, Error::InvalidArgument { .. }));
    }

    #[test]
    fn append_validates_oversized_single_message() {
        let ts = store();
        let created = ts
            .create("projects/p/topics/top1", labels(), None, None, false, false)
            .unwrap();

        let oversized = vec![(
            vec![0u8; limits::MAX_MESSAGE_BYTES + 1],
            HashMap::new(),
            String::new(),
        )];
        let err = ts.append(created.id, oversized, 0, 600).unwrap_err();
        assert!(matches!(err, Error::InvalidArgument { .. }));
    }
}
