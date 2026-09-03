//! Snapshot CRUD.
//!
//! Mirrors `crate::topic::TopicStore`/`crate::subscription::SubscriptionStore`'s
//! shape (id allocation, name index, `meta` keyspace record) but is
//! deliberately *not* the layer that captures/restores cursor state — a
//! snapshot's whole reason to exist is to interact with
//! `crate::delivery::engine::DeliveryEngine` (capturing a subscription's
//! cursor at create time, replacing one at seek time), and `TopicStore`
//! doesn't know about `DeliveryEngine` either (`crate::service::PubSubService`
//! is what composes the two) — so `Self::create`/`Self::seek_to_snapshot`'s
//! callers (`PubSubService`) pass in an already-captured
//! [`crate::store::codec::CursorRecord`] rather than this module reaching
//! into the engine itself.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::limits;
use crate::names::SnapshotName;
use crate::store::codec::CursorRecord;
use crate::store::keys::{self, NameKind};
use crate::store::kv::KvStore;

const META: &str = "meta";

pub use crate::store::codec::SnapshotRecord;

/// Snapshot CRUD, backed by a [`KvStore`]. See the module doc comment for
/// why this doesn't itself touch [`crate::delivery::engine::DeliveryEngine`].
pub struct SnapshotStore<K: KvStore> {
    kv: Arc<K>,
}

impl<K: KvStore> SnapshotStore<K> {
    /// Constructs a `SnapshotStore` over the given [`KvStore`].
    pub fn new(kv: Arc<K>) -> Self {
        Self { kv }
    }

    /// Creates a snapshot record from an already-captured cursor.
    /// `expire_at_ms` must already satisfy the rule that creation fails with
    /// `FAILED_PRECONDITION` if less than 1 hour of lifetime remains —
    /// callers (`PubSubService`) compute and validate it before calling
    /// this, since that computation needs
    /// [`crate::delivery::engine::DeliveryEngine::oldest_unacked_publish_ts_ms`],
    /// which this module has no access to.
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        &self,
        full_name: &str,
        topic_full_name: &str,
        topic_id: u64,
        labels: HashMap<String, String>,
        cursor: CursorRecord,
        expire_at_ms: i64,
    ) -> Result<SnapshotRecord> {
        SnapshotName::parse(full_name)?;

        let name_key = keys::name_key(NameKind::Snapshot, full_name);
        if self.kv.get(META, &name_key).is_some() {
            return Err(Error::AlreadyExists {
                resource: full_name.to_string(),
            });
        }

        let id = self.next_id()?;
        let record = SnapshotRecord {
            id,
            name: full_name.to_string(),
            topic: topic_full_name.to_string(),
            topic_id,
            labels,
            cursor,
            expire_at_ms,
        };
        self.put_record(&record)?;
        self.kv.put(META, name_key, id.to_be_bytes().to_vec())?;
        Ok(record)
    }

    /// Resolves a full snapshot name to its record.
    pub fn get(&self, full_name: &str) -> Result<SnapshotRecord> {
        let id = self.resolve_id(full_name)?;
        self.load_record(id)
    }

    /// Lists snapshots belonging to `project_id`, ordered by name,
    /// paginating with an opaque token (the last-returned name) — same
    /// shape as `TopicStore::list`/`SubscriptionStore::list`.
    pub fn list(
        &self,
        project_id: &str,
        page_size: usize,
        page_token: Option<&str>,
    ) -> Result<(Vec<SnapshotRecord>, Option<String>)> {
        let scan_prefix = keys::name_key(
            NameKind::Snapshot,
            &format!("projects/{project_id}/snapshots/"),
        );
        let entries = self.kv.scan_prefix(META, &scan_prefix);

        let mut records: Vec<SnapshotRecord> = Vec::with_capacity(entries.len());
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
        let page: Vec<SnapshotRecord> = records[start..end].to_vec();
        let next_token = if end < records.len() {
            page.last().map(|r| r.name.clone())
        } else {
            None
        };
        Ok((page, next_token))
    }

    /// Replaces a snapshot's labels — the only mutable field
    /// (`UpdateSnapshotRequest.update_mask` only ever names `labels`).
    pub fn update_labels(
        &self,
        full_name: &str,
        labels: HashMap<String, String>,
    ) -> Result<SnapshotRecord> {
        let mut record = self.get(full_name)?;
        record.labels = labels;
        self.put_record(&record)?;
        Ok(record)
    }

    /// Deletes a snapshot.
    pub fn delete(&self, full_name: &str) -> Result<()> {
        let id = self.resolve_id(full_name)?;
        self.kv.delete(META, &keys::snapshot_key(id))?;
        self.kv
            .delete(META, &keys::name_key(NameKind::Snapshot, full_name))?;
        Ok(())
    }

    /// Deletes every snapshot whose `expire_at_ms <= now_ms` (automatically
    /// deleted once reached). Returns the number removed.
    pub fn sweep_expired(&self, project_id: &str, now_ms: i64) -> Result<usize> {
        let (mut page, mut token) = self.list(project_id, usize::MAX, None)?;
        let mut removed = 0usize;
        loop {
            for record in &page {
                if record.expire_at_ms <= now_ms {
                    self.delete(&record.name)?;
                    removed += 1;
                }
            }
            let Some(next) = token else { break };
            let (next_page, next_token) = self.list(project_id, usize::MAX, Some(&next))?;
            page = next_page;
            token = next_token;
        }
        Ok(removed)
    }

    // -- internal helpers ---------------------------------------------

    fn resolve_id(&self, full_name: &str) -> Result<u64> {
        SnapshotName::parse(full_name)?;
        let name_key = keys::name_key(NameKind::Snapshot, full_name);
        let id_bytes = self
            .kv
            .get(META, &name_key)
            .ok_or_else(|| Error::NotFound {
                resource: full_name.to_string(),
            })?;
        decode_u64(&id_bytes).ok_or_else(|| Error::Internal {
            message: format!("corrupt snapshot id index entry for {full_name:?}"),
        })
    }

    fn load_record(&self, id: u64) -> Result<SnapshotRecord> {
        let bytes = self
            .kv
            .get(META, &keys::snapshot_key(id))
            .ok_or_else(|| Error::Internal {
                message: format!("missing snapshot record for id {id}"),
            })?;
        serde_json::from_slice(&bytes).map_err(|e| Error::Internal {
            message: format!("failed to decode snapshot record for id {id}: {e}"),
        })
    }

    fn put_record(&self, record: &SnapshotRecord) -> Result<()> {
        let bytes = serde_json::to_vec(record).map_err(|e| Error::Internal {
            message: format!("failed to encode snapshot record for id {}: {e}", record.id),
        })?;
        self.kv.put(META, keys::snapshot_key(record.id), bytes)?;
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
}

/// Computes a fresh snapshot's `expire_at_ms`: creation time + (7d − age of
/// the source subscription's oldest unacked message), and validates the
/// "FAILED_PRECONDITION if less than 1 hour of lifetime remains" gate.
/// `oldest_unacked_publish_ts_ms` is
/// [`crate::delivery::engine::DeliveryEngine::oldest_unacked_publish_ts_ms`]'s
/// result: `None` when the subscription has nothing outstanding, which
/// gets the maximum lifetime (no age to subtract).
pub fn compute_expire_at_ms(now_ms: i64, oldest_unacked_publish_ts_ms: Option<i64>) -> Result<i64> {
    let max_lifetime_ms = limits::MAX_SNAPSHOT_LIFETIME_SECS.saturating_mul(1000);
    let age_ms = oldest_unacked_publish_ts_ms
        .map(|ts| (now_ms.saturating_sub(ts)).max(0))
        .unwrap_or(0);
    let expire_at_ms = now_ms
        .saturating_add(max_lifetime_ms)
        .saturating_sub(age_ms);
    let remaining_ms = expire_at_ms.saturating_sub(now_ms);
    if remaining_ms < limits::MIN_SNAPSHOT_REMAINING_LIFETIME_SECS.saturating_mul(1000) {
        return Err(Error::FailedPrecondition {
            message: "snapshot's remaining lifetime would be under 1 hour".to_string(),
        });
    }
    Ok(expire_at_ms)
}

fn decode_u64(bytes: &[u8]) -> Option<u64> {
    let arr: [u8; 8] = bytes.try_into().ok()?;
    Some(u64::from_be_bytes(arr))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::store::codec::CursorRecord;
    use crate::store::kv::MemKv;

    fn store() -> SnapshotStore<MemKv> {
        SnapshotStore::new(Arc::new(MemKv::new()))
    }

    fn cursor() -> CursorRecord {
        CursorRecord {
            attach_seq: 0,
            acked_floor: 3,
            acked_above_floor: Default::default(),
        }
    }

    #[test]
    fn create_then_get_round_trip() {
        let s = store();
        let created = s
            .create(
                "projects/p/snapshots/snap-a",
                "projects/p/topics/topic-a",
                1,
                HashMap::new(),
                cursor(),
                1_000_000,
            )
            .unwrap();
        let fetched = s.get("projects/p/snapshots/snap-a").unwrap();
        assert_eq!(created, fetched);
        assert_eq!(fetched.cursor.acked_floor, 3);
    }

    #[test]
    fn duplicate_create_errs_already_exists() {
        let s = store();
        s.create(
            "projects/p/snapshots/snap-a",
            "projects/p/topics/topic-a",
            1,
            HashMap::new(),
            cursor(),
            1_000_000,
        )
        .unwrap();
        let err = s
            .create(
                "projects/p/snapshots/snap-a",
                "projects/p/topics/topic-a",
                1,
                HashMap::new(),
                cursor(),
                1_000_000,
            )
            .unwrap_err();
        assert!(matches!(err, Error::AlreadyExists { .. }));
    }

    #[test]
    fn get_missing_errs_not_found() {
        let s = store();
        let err = s.get("projects/p/snapshots/nope").unwrap_err();
        assert!(matches!(err, Error::NotFound { .. }));
    }

    #[test]
    fn delete_then_get_errs_not_found() {
        let s = store();
        s.create(
            "projects/p/snapshots/snap-a",
            "projects/p/topics/topic-a",
            1,
            HashMap::new(),
            cursor(),
            1_000_000,
        )
        .unwrap();
        s.delete("projects/p/snapshots/snap-a").unwrap();
        assert!(s.get("projects/p/snapshots/snap-a").is_err());
    }

    #[test]
    fn update_labels_changes_only_labels() {
        let s = store();
        s.create(
            "projects/p/snapshots/snap-a",
            "projects/p/topics/topic-a",
            1,
            HashMap::new(),
            cursor(),
            1_000_000,
        )
        .unwrap();
        let mut labels = HashMap::new();
        labels.insert("env".to_string(), "prod".to_string());
        let updated = s
            .update_labels("projects/p/snapshots/snap-a", labels.clone())
            .unwrap();
        assert_eq!(updated.labels, labels);
        assert_eq!(updated.cursor.acked_floor, 3);
    }

    #[test]
    fn list_paginates_within_project_and_excludes_others() {
        let s = store();
        for i in 0..3 {
            s.create(
                &format!("projects/p/snapshots/snap-{i}"),
                "projects/p/topics/topic-a",
                1,
                HashMap::new(),
                cursor(),
                1_000_000,
            )
            .unwrap();
        }
        s.create(
            "projects/other/snapshots/snap-x",
            "projects/other/topics/topic-a",
            2,
            HashMap::new(),
            cursor(),
            1_000_000,
        )
        .unwrap();

        let (page1, token1) = s.list("p", 2, None).unwrap();
        assert_eq!(page1.len(), 2);
        assert!(token1.is_some());
        let (page2, token2) = s.list("p", 2, token1.as_deref()).unwrap();
        assert_eq!(page2.len(), 1);
        assert!(token2.is_none());
    }

    #[test]
    fn sweep_expired_deletes_only_past_expiry() {
        let s = store();
        s.create(
            "projects/p/snapshots/expired",
            "projects/p/topics/topic-a",
            1,
            HashMap::new(),
            cursor(),
            1_000,
        )
        .unwrap();
        s.create(
            "projects/p/snapshots/alive",
            "projects/p/topics/topic-a",
            1,
            HashMap::new(),
            cursor(),
            9_999_999,
        )
        .unwrap();

        let removed = s.sweep_expired("p", 5_000).unwrap();
        assert_eq!(removed, 1);
        assert!(s.get("projects/p/snapshots/expired").is_err());
        assert!(s.get("projects/p/snapshots/alive").is_ok());
    }

    // -- compute_expire_at_ms ----------------------------------------

    #[test]
    fn no_oldest_unacked_gets_the_maximum_lifetime() {
        let expire = compute_expire_at_ms(1_000_000, None).unwrap();
        assert_eq!(
            expire,
            1_000_000 + limits::MAX_SNAPSHOT_LIFETIME_SECS * 1000
        );
    }

    #[test]
    fn oldest_unacked_age_shortens_the_lifetime() {
        let now_ms = 10 * 24 * 3600 * 1000; // day 10
        let oldest = now_ms - 2 * 24 * 3600 * 1000; // 2 days old
        let expire = compute_expire_at_ms(now_ms, Some(oldest)).unwrap();
        // 7d max minus 2d age = 5d remaining from now.
        assert_eq!(expire, now_ms + 5 * 24 * 3600 * 1000);
    }

    #[test]
    fn remaining_lifetime_under_one_hour_is_failed_precondition() {
        let now_ms = 10 * 24 * 3600 * 1000;
        // Oldest unacked message is (7d - 30min) old -> 30min remaining.
        let oldest = now_ms - (7 * 24 * 3600 - 1800) * 1000;
        let err = compute_expire_at_ms(now_ms, Some(oldest)).unwrap_err();
        assert!(matches!(err, Error::FailedPrecondition { .. }));
    }

    #[test]
    fn remaining_lifetime_of_exactly_one_hour_is_ok() {
        let now_ms = 10 * 24 * 3600 * 1000;
        let oldest = now_ms - (7 * 24 * 3600 - 3600) * 1000;
        assert!(compute_expire_at_ms(now_ms, Some(oldest)).is_ok());
    }
}
