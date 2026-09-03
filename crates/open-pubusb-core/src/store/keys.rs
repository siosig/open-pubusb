//! Big-endian, fixed-width binary key builders/parsers for the fjall
//! keyspaces (persistence key design).
//!
//! Every function here is pure (no I/O): it only builds or parses the
//! `Vec<u8>` key bytes used as fjall partition keys. Keys are big-endian so
//! that byte-lexicographic order matches numeric order, which range/prefix
//! scans (cursor Pull, retention sweep, etc.) rely on.

use std::fmt;

/// Discriminates the three name-indexed resource kinds sharing the `meta`
/// keyspace's `name/...` prefix (topic, subscription, snapshot).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NameKind {
    /// `name/t/{name}` — topic name -> topic_id resolution.
    Topic,
    /// `name/s/{name}` — subscription name -> sub_id resolution.
    Subscription,
    /// `name/snap/{name}` — snapshot name -> snap_id resolution.
    Snapshot,
}

impl NameKind {
    fn prefix(self) -> &'static [u8] {
        match self {
            NameKind::Topic => b"name/t/",
            NameKind::Subscription => b"name/s/",
            NameKind::Snapshot => b"name/snap/",
        }
    }
}

impl fmt::Display for NameKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NameKind::Topic => write!(f, "topic"),
            NameKind::Subscription => write!(f, "subscription"),
            NameKind::Snapshot => write!(f, "snapshot"),
        }
    }
}

// ---------------------------------------------------------------------
// `meta` keyspace
// ---------------------------------------------------------------------

/// `t/{topic_id}` — topic_id -> Topic proto record.
pub fn topic_key(topic_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + 8);
    key.extend_from_slice(b"t/");
    key.extend_from_slice(&topic_id.to_be_bytes());
    key
}

/// `s/{sub_id}` — sub_id -> Subscription proto record.
pub fn sub_key(sub_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + 8);
    key.extend_from_slice(b"s/");
    key.extend_from_slice(&sub_id.to_be_bytes());
    key
}

/// `snap/{snap_id}` — snap_id -> Snapshot proto record.
pub fn snapshot_key(snap_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(5 + 8);
    key.extend_from_slice(b"snap/");
    key.extend_from_slice(&snap_id.to_be_bytes());
    key
}

/// `name/t/{name}`, `name/s/{name}`, or `name/snap/{name}` depending on
/// `kind` — resource name -> id resolution.
pub fn name_key(kind: NameKind, name: &str) -> Vec<u8> {
    let prefix = kind.prefix();
    let mut key = Vec::with_capacity(prefix.len() + name.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(name.as_bytes());
    key
}

/// `id_seq` — the single counter key used by the id allocator.
pub fn id_seq_key() -> Vec<u8> {
    b"id_seq".to_vec()
}

// ---------------------------------------------------------------------
// `msg` keyspace
// ---------------------------------------------------------------------

/// `{topic_id u64 BE}{seq u64 BE}` — one message log entry.
pub fn msg_key(topic_id: u64, seq: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&topic_id.to_be_bytes());
    key.extend_from_slice(&seq.to_be_bytes());
    key
}

/// `{topic_id u64 BE}` prefix — for range-scanning all messages of a topic.
pub fn msg_key_prefix(topic_id: u64) -> Vec<u8> {
    topic_id.to_be_bytes().to_vec()
}

/// Decodes a `msg` keyspace key back into `(topic_id, seq)`. Returns `None`
/// if `key` is not exactly 16 bytes.
pub fn parse_msg_key(key: &[u8]) -> Option<(u64, u64)> {
    if key.len() != 16 {
        return None;
    }
    let topic_id = u64::from_be_bytes(key[0..8].try_into().ok()?);
    let seq = u64::from_be_bytes(key[8..16].try_into().ok()?);
    Some((topic_id, seq))
}

// ---------------------------------------------------------------------
// `sub` keyspace
// ---------------------------------------------------------------------

/// `{sub_id}` — SubscriptionCursor for a subscription.
pub fn cursor_key(sub_id: u64) -> Vec<u8> {
    sub_id.to_be_bytes().to_vec()
}

// ---------------------------------------------------------------------
// `dlv` keyspace
// ---------------------------------------------------------------------

/// `{sub_id u64 BE}{seq u64 BE}` — one in-flight delivery/lease record.
pub fn delivery_key(sub_id: u64, seq: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&sub_id.to_be_bytes());
    key.extend_from_slice(&seq.to_be_bytes());
    key
}

/// `{sub_id u64 BE}` prefix — for range-scanning all in-flight deliveries of
/// a subscription (e.g. startup lease-timer reconstruction).
pub fn delivery_key_prefix(sub_id: u64) -> Vec<u8> {
    sub_id.to_be_bytes().to_vec()
}

/// Decodes a `dlv` keyspace key back into `(sub_id, seq)`. Returns `None` if
/// `key` is not exactly 16 bytes.
pub fn parse_delivery_key(key: &[u8]) -> Option<(u64, u64)> {
    if key.len() != 16 {
        return None;
    }
    let sub_id = u64::from_be_bytes(key[0..8].try_into().ok()?);
    let seq = u64::from_be_bytes(key[8..16].try_into().ok()?);
    Some((sub_id, seq))
}

// ---------------------------------------------------------------------
// `okey` keyspace
// ---------------------------------------------------------------------

/// `{sub_id u64 BE}{xxh64(ordering_key) u64 BE}` — one ordering lane.
pub fn lane_key(sub_id: u64, ordering_key_hash: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&sub_id.to_be_bytes());
    key.extend_from_slice(&ordering_key_hash.to_be_bytes());
    key
}

// ---------------------------------------------------------------------
// `snap` keyspace
// ---------------------------------------------------------------------

/// `{snap_id}` — SnapshotRecord (cursor copy + expiry) for a snapshot.
pub fn snapshot_state_key(snap_id: u64) -> Vec<u8> {
    snap_id.to_be_bytes().to_vec()
}

// ---------------------------------------------------------------------
// `idx` keyspace
// ---------------------------------------------------------------------

/// `{topic_id u64 BE}{sub_id u64 BE}` — Topic -> Subscription reverse index
/// entry (empty value), used for Publish fan-out and topic-delete detach.
pub fn topic_sub_idx_key(topic_id: u64, sub_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&topic_id.to_be_bytes());
    key.extend_from_slice(&sub_id.to_be_bytes());
    key
}

/// `{topic_id u64 BE}` prefix — for range-scanning all subscriptions
/// attached to a topic.
pub fn topic_sub_idx_prefix(topic_id: u64) -> Vec<u8> {
    topic_id.to_be_bytes().to_vec()
}

// ---------------------------------------------------------------------
// `meta` keyspace (per-topic counters)
// ---------------------------------------------------------------------

/// `t/seq/{topic_id u64 BE}` — the per-topic message-log sequence counter
/// (next `seq` to assign in [`crate::topic::TopicStore::append`]).
pub fn topic_seq_counter_key(topic_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(6 + 8);
    key.extend_from_slice(b"t/seq/");
    key.extend_from_slice(&topic_id.to_be_bytes());
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_key_has_expected_prefix_and_length() {
        let key = topic_key(42);
        assert_eq!(&key[..2], b"t/");
        assert_eq!(key.len(), 10);
    }

    #[test]
    fn sub_key_has_expected_prefix_and_length() {
        let key = sub_key(42);
        assert_eq!(&key[..2], b"s/");
        assert_eq!(key.len(), 10);
    }

    #[test]
    fn snapshot_key_has_expected_prefix_and_length() {
        let key = snapshot_key(42);
        assert_eq!(&key[..5], b"snap/");
        assert_eq!(key.len(), 13);
    }

    #[test]
    fn name_key_uses_kind_specific_prefix() {
        assert_eq!(name_key(NameKind::Topic, "foo"), b"name/t/foo".to_vec());
        assert_eq!(
            name_key(NameKind::Subscription, "foo"),
            b"name/s/foo".to_vec()
        );
        assert_eq!(
            name_key(NameKind::Snapshot, "foo"),
            b"name/snap/foo".to_vec()
        );
    }

    #[test]
    fn id_seq_key_is_constant() {
        assert_eq!(id_seq_key(), b"id_seq".to_vec());
    }

    #[test]
    fn msg_key_round_trip() {
        let key = msg_key(7, 12345);
        assert_eq!(parse_msg_key(&key), Some((7, 12345)));
    }

    #[test]
    fn msg_key_round_trip_zero_and_max() {
        let key = msg_key(0, u64::MAX);
        assert_eq!(parse_msg_key(&key), Some((0, u64::MAX)));
    }

    #[test]
    fn msg_key_prefix_matches_msg_key_start() {
        let prefix = msg_key_prefix(7);
        let key = msg_key(7, 12345);
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn parse_msg_key_rejects_wrong_length() {
        assert_eq!(parse_msg_key(&[0u8; 15]), None);
        assert_eq!(parse_msg_key(&[0u8; 17]), None);
    }

    #[test]
    fn msg_key_orders_by_topic_then_seq() {
        // BE encoding must preserve numeric ordering under byte-lexicographic
        // comparison, since range scans depend on it.
        assert!(msg_key(1, 0) < msg_key(1, 1));
        assert!(msg_key(1, u64::MAX) < msg_key(2, 0));
    }

    #[test]
    fn cursor_key_round_trip_length() {
        let key = cursor_key(9);
        assert_eq!(key.len(), 8);
        assert_eq!(u64::from_be_bytes(key.try_into().unwrap()), 9);
    }

    #[test]
    fn delivery_key_round_trip() {
        let key = delivery_key(3, 99);
        assert_eq!(parse_delivery_key(&key), Some((3, 99)));
    }

    #[test]
    fn delivery_key_prefix_matches_delivery_key_start() {
        let prefix = delivery_key_prefix(3);
        let key = delivery_key(3, 99);
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn parse_delivery_key_rejects_wrong_length() {
        assert_eq!(parse_delivery_key(&[0u8; 15]), None);
        assert_eq!(parse_delivery_key(&[0u8; 17]), None);
    }

    #[test]
    fn lane_key_has_expected_length() {
        let key = lane_key(3, 0xdead_beef_1234_5678);
        assert_eq!(key.len(), 16);
        assert_eq!(&key[0..8], &3u64.to_be_bytes());
        assert_eq!(&key[8..16], &0xdead_beef_1234_5678u64.to_be_bytes());
    }

    #[test]
    fn snapshot_state_key_round_trip_length() {
        let key = snapshot_state_key(11);
        assert_eq!(key.len(), 8);
        assert_eq!(u64::from_be_bytes(key.try_into().unwrap()), 11);
    }

    #[test]
    fn topic_sub_idx_key_has_expected_length() {
        let key = topic_sub_idx_key(5, 6);
        assert_eq!(key.len(), 16);
        assert_eq!(&key[0..8], &5u64.to_be_bytes());
        assert_eq!(&key[8..16], &6u64.to_be_bytes());
    }

    #[test]
    fn topic_sub_idx_prefix_matches_idx_key_start() {
        let prefix = topic_sub_idx_prefix(5);
        let key = topic_sub_idx_key(5, 6);
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn topic_seq_counter_key_has_expected_prefix_and_length() {
        let key = topic_seq_counter_key(42);
        assert_eq!(&key[..6], b"t/seq/");
        assert_eq!(key.len(), 14);
    }

    #[test]
    fn topic_seq_counter_key_distinguishes_topics() {
        assert_ne!(topic_seq_counter_key(1), topic_seq_counter_key(2));
    }
}
