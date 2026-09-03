//! Value structs for the fjall keyspaces (internal entities for delivery
//! state, and persistence key design).
//!
//! Encoding is `serde` derive + `serde_json::to_vec` / `from_slice`: it is
//! already a workspace dependency, correctness matters more than size at
//! this stage, and the JSON representation is trivial to inspect while
//! debugging the store directly. Every struct exposes `encode`/`decode`
//! rather than requiring callers to reach for `serde_json` directly, so the
//! wire format can be swapped later behind a stable interface.

use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

/// One entry in a topic's append-only message log (`msg` keyspace value).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageRecord {
    /// Server-assigned publish time, milliseconds since the Unix epoch.
    pub publish_ts_ms: i64,
    /// Ordering key, empty string if the message is unordered.
    pub ordering_key: String,
    /// User-supplied message attributes.
    pub attributes: HashMap<String, String>,
    /// Message body.
    pub payload: Vec<u8>,
    /// Absolute expiry time (publish time + retention), milliseconds since
    /// the Unix epoch. Used by the retention sweep.
    pub expire_at_ms: i64,
}

impl MessageRecord {
    /// Serializes this record to its persisted byte representation.
    pub fn encode(&self) -> Vec<u8> {
        // `serde_json::to_vec` only fails on non-string map keys or a
        // `Serialize` impl that itself errors; neither applies here, but we
        // still avoid `unwrap`/`expect` per the no-panic policy for
        // non-test code.
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Deserializes a record from its persisted byte representation.
    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// A subscription's progress through its topic's message log (`sub`
/// keyspace value, and the shape reused by `SnapshotRecord.cursor`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CursorRecord {
    /// Log tail at subscription-creation time; messages before this seq are
    /// never delivered to this subscription.
    pub attach_seq: u64,
    /// All seq <= this value are fully done (acked, filtered out, or
    /// expired).
    pub acked_floor: u64,
    /// Seq strictly above `acked_floor` that are individually done. Shrinks
    /// as `acked_floor` advances over contiguous entries.
    pub acked_above_floor: BTreeSet<u64>,
}

impl CursorRecord {
    /// Serializes this record to its persisted byte representation.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Deserializes a record from its persisted byte representation.
    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// An in-flight delivery/lease for one `(sub_id, seq)` pair (`dlv` keyspace
/// value). The row's presence *is* the "currently leased/tracked" signal —
/// deleting the row is how an Ack is recorded, so no `acked` field exists
/// here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeliveryRecord {
    /// Ack deadline, milliseconds since the Unix epoch. `ModifyAckDeadline`
    /// updates this in place.
    pub deadline_ms: i64,
    /// 1 + Nack count + expiry count so far for this seq.
    pub attempts: u32,
    /// Generation counter embedded in the `ack_id` issued for this
    /// delivery; an Ack/ModAck carrying an older generation is stale and
    /// ignored.
    pub generation: u64,
}

impl DeliveryRecord {
    /// Serializes this record to its persisted byte representation.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Deserializes a record from its persisted byte representation.
    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// Per-`ordering_key` serialization lane for a subscription (`okey`
/// keyspace value).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LaneRecord {
    /// Smallest seq not yet complete for this key.
    pub head_seq: u64,
    /// Start of the in-flight batch's seq range (inclusive).
    pub outstanding_from: u64,
    /// End of the in-flight batch's seq range (exclusive or inclusive per
    /// the engine's convention; stored as-is here).
    pub outstanding_to: u64,
    /// If set, this lane is blocked (after a Nack/expiry) until this time,
    /// milliseconds since the Unix epoch.
    pub blocked_until_ms: Option<i64>,
}

impl LaneRecord {
    /// Serializes this record to its persisted byte representation.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Deserializes a record from its persisted byte representation.
    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// A captured snapshot: a copy of a subscription's cursor at snapshot-time
/// plus its own resource identity and expiry.
///
/// One combined record (resource fields + internal cursor copy) under one
/// `meta` key — mirroring `TopicRecord`/`SubscriptionRecord`'s own "one
/// record has everything" shape — rather than splitting resource metadata
/// (`meta/snap/{id}`) from internal state (`snap/{id}`, the layout
/// `crate::store::keys::snapshot_state_key` was originally cut for): a
/// snapshot is read and written as a single unit everywhere it's used
/// (`crate::delivery::snapshot`), so one record avoids a second lookup on
/// every operation for no benefit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotRecord {
    /// Internal id, stable across the snapshot's lifetime.
    pub id: u64,
    /// Full resource name: `projects/{project}/snapshots/{name}`.
    pub name: String,
    /// Full resource name of the source subscription's topic — exposed as
    /// `Snapshot.topic`.
    pub topic: String,
    /// Internal id of the topic the source subscription was attached to
    /// (`seek_to_time`'s binary search over `msg` scans this topic's log).
    pub topic_id: u64,
    /// User-supplied labels.
    pub labels: HashMap<String, String>,
    /// Copy of the source subscription's cursor at capture time (or, after
    /// a mutation, its replacement — snapshots aren't mutated by anything
    /// other than expiry today, but the field name intentionally doesn't
    /// promise otherwise).
    pub cursor: CursorRecord,
    /// Absolute expiry time, milliseconds since the Unix epoch.
    pub expire_at_ms: i64,
}

impl SnapshotRecord {
    /// Serializes this record to its persisted byte representation.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Deserializes a record from its persisted byte representation.
    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_record_round_trip() {
        let mut attributes = HashMap::new();
        attributes.insert("k1".to_string(), "v1".to_string());
        attributes.insert("k2".to_string(), "v2".to_string());
        let record = MessageRecord {
            publish_ts_ms: 1_700_000_000_000,
            ordering_key: "order-a".to_string(),
            attributes,
            payload: vec![1, 2, 3, 4, 5],
            expire_at_ms: 1_700_600_000_000,
        };
        let bytes = record.encode();
        let decoded = MessageRecord::decode(&bytes).expect("decode should succeed");
        assert_eq!(record, decoded);
    }

    #[test]
    fn message_record_round_trip_empty_attributes_and_payload() {
        let record = MessageRecord {
            publish_ts_ms: 0,
            ordering_key: String::new(),
            attributes: HashMap::new(),
            payload: Vec::new(),
            expire_at_ms: 0,
        };
        let bytes = record.encode();
        let decoded = MessageRecord::decode(&bytes).expect("decode should succeed");
        assert_eq!(record, decoded);
    }

    #[test]
    fn cursor_record_round_trip_with_non_empty_set() {
        let mut acked_above_floor = BTreeSet::new();
        acked_above_floor.insert(10);
        acked_above_floor.insert(12);
        acked_above_floor.insert(15);
        let record = CursorRecord {
            attach_seq: 5,
            acked_floor: 9,
            acked_above_floor,
        };
        let bytes = record.encode();
        let decoded = CursorRecord::decode(&bytes).expect("decode should succeed");
        assert_eq!(record, decoded);
    }

    #[test]
    fn cursor_record_round_trip_empty_set() {
        let record = CursorRecord {
            attach_seq: 0,
            acked_floor: 0,
            acked_above_floor: BTreeSet::new(),
        };
        let bytes = record.encode();
        let decoded = CursorRecord::decode(&bytes).expect("decode should succeed");
        assert_eq!(record, decoded);
    }

    #[test]
    fn delivery_record_round_trip() {
        let record = DeliveryRecord {
            deadline_ms: 1_700_000_010_000,
            attempts: 3,
            generation: 7,
        };
        let bytes = record.encode();
        let decoded = DeliveryRecord::decode(&bytes).expect("decode should succeed");
        assert_eq!(record, decoded);
    }

    #[test]
    fn lane_record_round_trip_with_some_blocked_until() {
        let record = LaneRecord {
            head_seq: 42,
            outstanding_from: 42,
            outstanding_to: 50,
            blocked_until_ms: Some(1_700_000_020_000),
        };
        let bytes = record.encode();
        let decoded = LaneRecord::decode(&bytes).expect("decode should succeed");
        assert_eq!(record, decoded);
    }

    #[test]
    fn lane_record_round_trip_with_none_blocked_until() {
        let record = LaneRecord {
            head_seq: 0,
            outstanding_from: 0,
            outstanding_to: 0,
            blocked_until_ms: None,
        };
        let bytes = record.encode();
        let decoded = LaneRecord::decode(&bytes).expect("decode should succeed");
        assert_eq!(record, decoded);
    }

    #[test]
    fn snapshot_record_round_trip() {
        let mut acked_above_floor = BTreeSet::new();
        acked_above_floor.insert(3);
        let record = SnapshotRecord {
            id: 7,
            name: "projects/p/snapshots/snap-a".to_string(),
            topic: "projects/p/topics/topic-a".to_string(),
            topic_id: 99,
            labels: HashMap::new(),
            cursor: CursorRecord {
                attach_seq: 1,
                acked_floor: 2,
                acked_above_floor,
            },
            expire_at_ms: 1_700_700_000_000,
        };
        let bytes = record.encode();
        let decoded = SnapshotRecord::decode(&bytes).expect("decode should succeed");
        assert_eq!(record, decoded);
    }

    #[test]
    fn decode_rejects_garbage_bytes() {
        let err = MessageRecord::decode(b"not json");
        assert!(err.is_err());
    }
}
