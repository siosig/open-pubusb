//! In-memory lease tracking for one server's currently-outstanding message
//! deliveries: which `(subscription, seq)` pairs are checked out to a
//! client, until when, and how many times.
//!
//! This is the fast, in-memory half of delivery state; [`crate::delivery::engine`]
//! is responsible for persisting the matching
//! [`crate::store::codec::DeliveryRecord`] rows and for rebuilding a
//! [`LeaseTable`] from them at startup: at startup, it scans `dlv/{sub}`
//! from the cursor to restore leases.

use std::collections::{BTreeMap, HashMap};

use base64::Engine as _;

/// One outstanding delivery: a specific `(subscription, seq)` currently
/// checked out to a client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lease {
    /// The subscription this delivery was leased to.
    pub sub_id: u64,
    /// Position in the topic's message log this delivery is for.
    pub seq: u64,
    /// Ack deadline, milliseconds since the Unix epoch.
    pub deadline_ms: i64,
    /// 1 + Nack count + expiry count so far for this seq.
    pub attempts: u32,
    /// Incremented every time this `(sub_id, seq)` is (re)leased. Embedded
    /// in the issued `ack_id` so a stale client's Ack/ModAck from an
    /// earlier delivery of the same message is recognized as such rather
    /// than silently applied to the current lease.
    pub generation: u64,
}

/// The result of applying an Ack or ModifyAckDeadline against an `ack_id`.
///
/// Unknown or expired `ack_id`s are ignored: matching the Pub/Sub REST/gRPC
/// contract, an unknown or stale-generation `ack_id` is **never** an error
/// — callers should treat every variant here as "the request succeeded",
/// and only use it for metrics/logging if they care to distinguish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    /// The `ack_id` named a live lease at the current generation; the
    /// requested effect (ack / deadline change) was applied.
    Applied(Lease),
    /// No lease exists for that `(sub_id, seq)` right now (already acked,
    /// already expired and superseded, or never existed).
    Unknown,
    /// A lease exists for that `(sub_id, seq)`, but at a newer generation
    /// than the `ack_id` names — the client is acting on a delivery that
    /// has since been superseded by a redelivery.
    StaleGeneration,
    /// The `ack_id` itself could not be decoded (malformed input, not a
    /// value this server ever issued).
    Malformed,
}

/// A single expired lease returned by [`LeaseTable::expire_up_to`]. Attempt
/// counting is the caller's responsibility: this only reports which
/// leases fell past their deadline and removes them from the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpiredLease {
    /// The subscription this delivery was leased to.
    pub sub_id: u64,
    /// Position in the topic's message log this delivery is for.
    pub seq: u64,
    /// 1 + Nack count + expiry count so far for this seq, as of expiry.
    pub attempts: u32,
    /// The generation the expired lease was at (see [`Lease::generation`]).
    pub generation: u64,
}

/// In-memory index of every currently-outstanding lease, ordered by
/// deadline for cheap expiry scans.
#[derive(Debug, Default)]
pub struct LeaseTable {
    by_deadline: BTreeMap<(i64, u64, u64), ()>,
    by_key: HashMap<(u64, u64), Lease>,
}

impl LeaseTable {
    /// An empty lease table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of currently-outstanding leases (for metrics/tests).
    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    /// Whether there are no currently-outstanding leases.
    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    /// Starts (or restarts, with a bumped generation) a lease for
    /// `(sub_id, seq)`, replacing any existing lease for that key. Returns
    /// the encoded `ack_id` for this lease.
    ///
    /// `generation` is provided by the caller, which tracks the next
    /// generation per `(sub_id, seq)`, including across restarts, since it
    /// must survive process crashes to keep stale-ack detection correct —
    /// see `crate::store::codec::DeliveryRecord::generation`).
    pub fn insert(
        &mut self,
        sub_id: u64,
        seq: u64,
        deadline_ms: i64,
        attempts: u32,
        generation: u64,
    ) -> String {
        let key = (sub_id, seq);
        if let Some(old) = self.by_key.remove(&key) {
            self.by_deadline.remove(&(old.deadline_ms, sub_id, seq));
        }
        let lease = Lease {
            sub_id,
            seq,
            deadline_ms,
            attempts,
            generation,
        };
        self.by_deadline.insert((deadline_ms, sub_id, seq), ());
        self.by_key.insert(key, lease);
        encode_ack_id(sub_id, seq, generation)
    }

    /// Reads the current lease for `(sub_id, seq)`, if any.
    pub fn get(&self, sub_id: u64, seq: u64) -> Option<Lease> {
        self.by_key.get(&(sub_id, seq)).copied()
    }

    /// Removes and returns the lease for `(sub_id, seq)`, if any (used
    /// when a message is acked or dead-lettered).
    pub fn remove(&mut self, sub_id: u64, seq: u64) -> Option<Lease> {
        let lease = self.by_key.remove(&(sub_id, seq))?;
        self.by_deadline.remove(&(lease.deadline_ms, sub_id, seq));
        Some(lease)
    }

    /// Applies an Acknowledge for the given `ack_id`: on a live lease at
    /// the matching generation, removes it and reports [`AckOutcome::Applied`].
    pub fn ack(&mut self, ack_id: &str) -> AckOutcome {
        let Some((sub_id, seq, generation)) = decode_ack_id(ack_id) else {
            return AckOutcome::Malformed;
        };
        let matches_generation = match self.by_key.get(&(sub_id, seq)) {
            None => return AckOutcome::Unknown,
            Some(lease) => lease.generation == generation,
        };
        if !matches_generation {
            return AckOutcome::StaleGeneration;
        }
        match self.remove(sub_id, seq) {
            Some(lease) => AckOutcome::Applied(lease),
            // Removed by a concurrent caller between the get() above and
            // here — this table isn't shared across threads today, but
            // treat it the same as "no longer live" rather than panicking.
            None => AckOutcome::Unknown,
        }
    }

    /// Applies a ModifyAckDeadline for the given `ack_id`: on a live lease
    /// at the matching generation, moves its deadline to `now_ms + secs`
    /// (or, per FR-008/contracts, treats `secs <= 0` as an immediate nack
    /// by setting the deadline to `now_ms`, making it eligible for
    /// redelivery on the next expiry scan) and reports the updated lease.
    pub fn extend(&mut self, ack_id: &str, now_ms: i64, secs: i32) -> AckOutcome {
        let Some((sub_id, seq, generation)) = decode_ack_id(ack_id) else {
            return AckOutcome::Malformed;
        };
        let Some(lease) = self.by_key.get(&(sub_id, seq)) else {
            return AckOutcome::Unknown;
        };
        if lease.generation != generation {
            return AckOutcome::StaleGeneration;
        }
        let old_deadline = lease.deadline_ms;
        let new_deadline = if secs <= 0 {
            now_ms
        } else {
            now_ms + i64::from(secs) * 1000
        };
        self.by_deadline.remove(&(old_deadline, sub_id, seq));
        self.by_deadline.insert((new_deadline, sub_id, seq), ());
        let Some(lease) = self.by_key.get_mut(&(sub_id, seq)) else {
            return AckOutcome::Unknown;
        };
        lease.deadline_ms = new_deadline;
        AckOutcome::Applied(*lease)
    }

    /// Same as [`Self::extend`] but for a nack applied directly via
    /// `(sub_id, seq, generation)` rather than a decoded `ack_id` — used
    /// by the ordering-lane redelivery path, which nacks every
    /// subsequent message for a blocked key without going through the
    /// wire-level ack_id encoding.
    pub fn nack_now(&mut self, sub_id: u64, seq: u64, now_ms: i64) -> Option<Lease> {
        let lease = self.by_key.get(&(sub_id, seq))?;
        let old_deadline = lease.deadline_ms;
        self.by_deadline.remove(&(old_deadline, sub_id, seq));
        self.by_deadline.insert((now_ms, sub_id, seq), ());
        let lease = self.by_key.get_mut(&(sub_id, seq))?;
        lease.deadline_ms = now_ms;
        Some(*lease)
    }

    /// Removes and returns every lease whose deadline is `<= now_ms`.
    /// Callers are responsible for incrementing `attempts` and
    /// deciding whether to redeliver or dead-letter each one — this
    /// method only reports which leases expired.
    pub fn expire_up_to(&mut self, now_ms: i64) -> Vec<ExpiredLease> {
        let expired_keys: Vec<(i64, u64, u64)> = self
            .by_deadline
            .range(..=(now_ms, u64::MAX, u64::MAX))
            .map(|(k, _)| *k)
            .collect();

        let mut out = Vec::with_capacity(expired_keys.len());
        for key @ (_, sub_id, seq) in expired_keys {
            self.by_deadline.remove(&key);
            if let Some(lease) = self.by_key.remove(&(sub_id, seq)) {
                out.push(ExpiredLease {
                    sub_id,
                    seq,
                    attempts: lease.attempts,
                    generation: lease.generation,
                });
            }
        }
        out
    }

    /// Removes every lease belonging to `sub_id` (used when a subscription
    /// is deleted). Returns the removed `seq`s.
    pub fn remove_all_for_subscription(&mut self, sub_id: u64) -> Vec<u64> {
        let seqs: Vec<u64> = self
            .by_key
            .keys()
            .filter(|(s, _)| *s == sub_id)
            .map(|(_, seq)| *seq)
            .collect();
        for seq in &seqs {
            self.remove(sub_id, *seq);
        }
        seqs
    }
}

/// Encodes `(sub_id, seq, generation)` as the opaque `ack_id` string handed
/// to clients: 24 raw bytes (three big-endian `u64`s), base64url without
/// padding. Not meant to be human-decodable by clients — just compact and
/// URL/header-safe.
pub fn encode_ack_id(sub_id: u64, seq: u64, generation: u64) -> String {
    let mut bytes = [0u8; 24];
    bytes[0..8].copy_from_slice(&sub_id.to_be_bytes());
    bytes[8..16].copy_from_slice(&seq.to_be_bytes());
    bytes[16..24].copy_from_slice(&generation.to_be_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Decodes an `ack_id` produced by [`encode_ack_id`]. Returns `None` for
/// anything malformed (wrong length, invalid base64, or — since this
/// server never issues anything else — simply not one of ours); callers
/// treat that the same as [`AckOutcome::Malformed`], which is itself
/// treated as a no-op success at the API boundary.
pub fn decode_ack_id(ack_id: &str) -> Option<(u64, u64, u64)> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(ack_id)
        .ok()?;
    if bytes.len() != 24 {
        return None;
    }
    let sub_id = u64::from_be_bytes(bytes[0..8].try_into().ok()?);
    let seq = u64::from_be_bytes(bytes[8..16].try_into().ok()?);
    let generation = u64::from_be_bytes(bytes[16..24].try_into().ok()?);
    Some((sub_id, seq, generation))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_id_round_trips() {
        let id = encode_ack_id(7, 42, 3);
        assert_eq!(decode_ack_id(&id), Some((7, 42, 3)));
    }

    #[test]
    fn decode_rejects_malformed_input() {
        assert_eq!(decode_ack_id("not-base64-!!!"), None);
        assert_eq!(
            decode_ack_id(&base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"short")),
            None
        );
    }

    #[test]
    fn insert_then_get_round_trip() {
        let mut t = LeaseTable::new();
        let ack_id = t.insert(1, 100, 5_000, 1, 0);
        let (sub_id, seq, generation) = decode_ack_id(&ack_id).unwrap();
        assert_eq!((sub_id, seq, generation), (1, 100, 0));
        let lease = t.get(1, 100).unwrap();
        assert_eq!(lease.deadline_ms, 5_000);
        assert_eq!(lease.attempts, 1);
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn ack_removes_lease_and_reports_applied() {
        let mut t = LeaseTable::new();
        let ack_id = t.insert(1, 100, 5_000, 1, 0);
        let outcome = t.ack(&ack_id);
        assert!(matches!(outcome, AckOutcome::Applied(_)));
        assert!(t.get(1, 100).is_none());
        assert!(t.is_empty());
    }

    #[test]
    fn ack_unknown_ack_id_is_unknown_not_error() {
        let mut t = LeaseTable::new();
        let bogus = encode_ack_id(99, 99, 0);
        assert_eq!(t.ack(&bogus), AckOutcome::Unknown);
    }

    #[test]
    fn ack_with_stale_generation_is_ignored() {
        let mut t = LeaseTable::new();
        // Lease at generation 0, then redelivered as generation 1.
        t.insert(1, 100, 1_000, 1, 0);
        let stale_ack_id = encode_ack_id(1, 100, 0);
        t.insert(1, 100, 2_000, 2, 1);

        assert_eq!(t.ack(&stale_ack_id), AckOutcome::StaleGeneration);
        // The live (generation-1) lease must still be present.
        assert!(t.get(1, 100).is_some());
    }

    #[test]
    fn ack_malformed_ack_id_is_ignored() {
        let mut t = LeaseTable::new();
        assert_eq!(t.ack("not a real ack id"), AckOutcome::Malformed);
    }

    #[test]
    fn double_ack_is_idempotent() {
        let mut t = LeaseTable::new();
        let ack_id = t.insert(1, 100, 5_000, 1, 0);
        assert!(matches!(t.ack(&ack_id), AckOutcome::Applied(_)));
        // Second ack of the same id: lease is gone, so Unknown — still not
        // an error at the API boundary.
        assert_eq!(t.ack(&ack_id), AckOutcome::Unknown);
    }

    #[test]
    fn extend_moves_deadline_forward() {
        let mut t = LeaseTable::new();
        let ack_id = t.insert(1, 100, 1_000, 1, 0);
        let outcome = t.extend(&ack_id, 1_000, 600);
        match outcome {
            AckOutcome::Applied(lease) => assert_eq!(lease.deadline_ms, 1_000 + 600_000),
            other => panic!("expected Applied, got {other:?}"),
        }
        assert_eq!(t.get(1, 100).unwrap().deadline_ms, 1_000 + 600_000);
    }

    #[test]
    fn extend_with_zero_seconds_is_immediate_nack() {
        let mut t = LeaseTable::new();
        let ack_id = t.insert(1, 100, 999_999, 1, 0);
        let outcome = t.extend(&ack_id, 5_000, 0);
        assert!(matches!(outcome, AckOutcome::Applied(_)));
        assert_eq!(t.get(1, 100).unwrap().deadline_ms, 5_000);

        let expired = t.expire_up_to(5_000);
        assert_eq!(expired.len(), 1);
        assert_eq!(
            expired[0],
            ExpiredLease {
                sub_id: 1,
                seq: 100,
                attempts: 1,
                generation: 0,
            }
        );
    }

    #[test]
    fn extend_unknown_ack_id_is_ignored() {
        let mut t = LeaseTable::new();
        let bogus = encode_ack_id(1, 1, 0);
        assert_eq!(t.extend(&bogus, 0, 10), AckOutcome::Unknown);
    }

    #[test]
    fn expire_up_to_returns_only_leases_past_deadline() {
        let mut t = LeaseTable::new();
        t.insert(1, 1, 1_000, 1, 0);
        t.insert(1, 2, 2_000, 1, 0);
        t.insert(1, 3, 3_000, 1, 0);

        let expired = t.expire_up_to(2_000);
        let mut seqs: Vec<u64> = expired.iter().map(|e| e.seq).collect();
        seqs.sort_unstable();
        assert_eq!(seqs, vec![1, 2]);
        assert_eq!(t.len(), 1);
        assert!(t.get(1, 3).is_some());
    }

    #[test]
    fn expire_up_to_is_idempotent_when_nothing_new_expired() {
        let mut t = LeaseTable::new();
        t.insert(1, 1, 1_000, 1, 0);
        assert_eq!(t.expire_up_to(1_000).len(), 1);
        assert_eq!(t.expire_up_to(1_000).len(), 0);
        assert_eq!(t.expire_up_to(999_999).len(), 0);
    }

    #[test]
    fn reinsert_replaces_deadline_index_entry() {
        let mut t = LeaseTable::new();
        t.insert(1, 1, 1_000, 1, 0);
        // Re-lease at a later deadline: the old (1_000, 1, 1) index entry
        // must not still cause an expiry at now=1_000.
        t.insert(1, 1, 5_000, 2, 1);
        assert_eq!(t.expire_up_to(1_000).len(), 0);
        assert_eq!(t.expire_up_to(5_000).len(), 1);
    }

    #[test]
    fn nack_now_moves_deadline_to_now_without_ack_id() {
        let mut t = LeaseTable::new();
        t.insert(1, 1, 999_999, 1, 0);
        let lease = t.nack_now(1, 1, 500).unwrap();
        assert_eq!(lease.deadline_ms, 500);
        assert_eq!(t.expire_up_to(500).len(), 1);
    }

    #[test]
    fn nack_now_on_missing_lease_returns_none() {
        let mut t = LeaseTable::new();
        assert!(t.nack_now(1, 1, 0).is_none());
    }

    #[test]
    fn remove_all_for_subscription_only_touches_that_subscription() {
        let mut t = LeaseTable::new();
        t.insert(1, 1, 1_000, 1, 0);
        t.insert(1, 2, 1_000, 1, 0);
        t.insert(2, 1, 1_000, 1, 0);

        let removed = t.remove_all_for_subscription(1);
        assert_eq!(removed.len(), 2);
        assert!(t.get(1, 1).is_none());
        assert!(t.get(1, 2).is_none());
        assert!(t.get(2, 1).is_some());
        assert_eq!(t.len(), 1);
    }
}
