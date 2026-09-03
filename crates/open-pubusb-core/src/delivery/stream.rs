//! `StreamingPull` session bookkeeping: tracks which
//! outstanding leases belong to which open stream, so a disconnected
//! stream's leases expire and are released immediately, and so
//! per-stream flow control (`max_outstanding_messages`/`max_outstanding_bytes`)
//! can bound how much [`super::engine::DeliveryEngine::lease_for_stream`]
//! hands out at once.
//!
//! Deliberately a thin layer *on top of* [`super::engine::DeliveryEngine`]'s
//! existing `lease_next`/`acknowledge`/`modify_ack_deadline`, not a parallel
//! implementation: a `StreamSession` is really just "a named budget plus a
//! set of `(sub_id, seq)` this particular caller is currently holding",
//! reusing the same [`super::lease::LeaseTable`] every unary Pull also
//! shares.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// One open `StreamingPull` stream's flow-control budget and identity.
#[derive(Debug, Clone, Copy)]
pub struct StreamInfo {
    /// The subscription this stream is pulling from.
    pub sub_id: u64,
    /// Internal id of `sub_id`'s topic.
    pub topic_id: u64,
    /// Ack deadline applied to every lease granted through this stream.
    pub ack_deadline_secs: i32,
    /// `<= 0` means unlimited (per the proto contract).
    pub max_outstanding_messages: i64,
    /// `<= 0` means unlimited.
    pub max_outstanding_bytes: i64,
}

struct SessionState {
    info: StreamInfo,
    /// `seq -> payload byte length`, so [`StreamRegistry::record_resolved`]
    /// can subtract the exact size recorded at lease time (not a caller-
    /// supplied one, which could drift from what was actually counted).
    outstanding: HashMap<u64, usize>,
    outstanding_bytes: i64,
}

/// Registry of currently-open `StreamingPull` sessions.
#[derive(Default)]
pub struct StreamRegistry {
    next_id: AtomicU64,
    sessions: Mutex<HashMap<u64, SessionState>>,
}

impl StreamRegistry {
    /// An empty registry with no open streams.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a new stream and returns its id.
    pub fn open(&self, info: StreamInfo) -> u64 {
        let stream_id = self.next_id.fetch_add(1, Ordering::SeqCst) + 1;
        self.lock().insert(
            stream_id,
            SessionState {
                info,
                outstanding: HashMap::new(),
                outstanding_bytes: 0,
            },
        );
        stream_id
    }

    /// Removes the session entirely. Returns the `(sub_id, seqs)` that were
    /// still outstanding, for the caller ([`super::engine::DeliveryEngine::on_stream_closed`])
    /// to release from the shared lease table.
    pub fn close(&self, stream_id: u64) -> Option<(u64, Vec<u64>)> {
        self.lock()
            .remove(&stream_id)
            .map(|s| (s.info.sub_id, s.outstanding.into_keys().collect()))
    }

    /// The current flow-control budget/identity for `stream_id`, or `None`
    /// if it's not open (already closed, or never existed).
    pub fn info(&self, stream_id: u64) -> Option<StreamInfo> {
        self.lock().get(&stream_id).map(|s| s.info)
    }

    /// Updates a stream's ack deadline / flow-control budget (a
    /// `StreamingPullRequest` after the first one may change these).
    pub fn update(
        &self,
        stream_id: u64,
        ack_deadline_secs: Option<i32>,
        max_outstanding_messages: Option<i64>,
        max_outstanding_bytes: Option<i64>,
    ) {
        if let Some(s) = self.lock().get_mut(&stream_id) {
            if let Some(v) = ack_deadline_secs {
                s.info.ack_deadline_secs = v;
            }
            if let Some(v) = max_outstanding_messages {
                s.info.max_outstanding_messages = v;
            }
            if let Some(v) = max_outstanding_bytes {
                s.info.max_outstanding_bytes = v;
            }
        }
    }

    /// Remaining message/byte budget right now (`i64::MAX` for an
    /// unlimited dimension).
    pub fn remaining_budget(&self, stream_id: u64) -> (i64, i64) {
        let guard = self.lock();
        let Some(s) = guard.get(&stream_id) else {
            return (0, 0);
        };
        let msgs = if s.info.max_outstanding_messages <= 0 {
            i64::MAX
        } else {
            (s.info.max_outstanding_messages - s.outstanding.len() as i64).max(0)
        };
        let bytes = if s.info.max_outstanding_bytes <= 0 {
            i64::MAX
        } else {
            (s.info.max_outstanding_bytes - s.outstanding_bytes).max(0)
        };
        (msgs, bytes)
    }

    /// Records that `seq` (of `payload_bytes` size) was just leased to this
    /// stream.
    pub fn record_leased(&self, stream_id: u64, seq: u64, payload_bytes: usize) {
        if let Some(s) = self.lock().get_mut(&stream_id) {
            s.outstanding.insert(seq, payload_bytes);
            s.outstanding_bytes = s.outstanding_bytes.saturating_add(payload_bytes as i64);
        }
    }

    /// Removes `seq` from a stream's outstanding set (acked, nacked, or
    /// its lease otherwise resolved) — best-effort: does nothing if
    /// `stream_id` is unknown or `seq` wasn't tracked (e.g. an ack_id from
    /// a different stream/unary Pull, which is valid per the proto:
    /// "acknowledging previously received messages (received on this
    /// stream or a different stream)"). The byte size subtracted is
    /// whatever [`Self::record_leased`] recorded for this `seq`, not a
    /// caller-supplied value, so the two can never drift apart.
    pub fn record_resolved(&self, stream_id: u64, seq: u64) {
        if let Some(s) = self.lock().get_mut(&stream_id) {
            if let Some(payload_bytes) = s.outstanding.remove(&seq) {
                s.outstanding_bytes = s.outstanding_bytes.saturating_sub(payload_bytes as i64);
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u64, SessionState>> {
        match self.sessions.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn info() -> StreamInfo {
        StreamInfo {
            sub_id: 1,
            topic_id: 1,
            ack_deadline_secs: 10,
            max_outstanding_messages: 2,
            max_outstanding_bytes: 100,
        }
    }

    #[test]
    fn budget_shrinks_as_messages_are_leased() {
        let reg = StreamRegistry::new();
        let id = reg.open(info());
        assert_eq!(reg.remaining_budget(id), (2, 100));
        reg.record_leased(id, 1, 40);
        assert_eq!(reg.remaining_budget(id), (1, 60));
        reg.record_leased(id, 2, 40);
        assert_eq!(reg.remaining_budget(id), (0, 20));
    }

    #[test]
    fn resolving_a_message_frees_its_budget() {
        let reg = StreamRegistry::new();
        let id = reg.open(info());
        reg.record_leased(id, 1, 40);
        reg.record_resolved(id, 1);
        assert_eq!(reg.remaining_budget(id), (2, 100));
    }

    #[test]
    fn unlimited_budget_is_i64_max() {
        let reg = StreamRegistry::new();
        let id = reg.open(StreamInfo {
            max_outstanding_messages: 0,
            max_outstanding_bytes: -1,
            ..info()
        });
        assert_eq!(reg.remaining_budget(id), (i64::MAX, i64::MAX));
    }

    #[test]
    fn close_returns_outstanding_seqs_for_release() {
        let reg = StreamRegistry::new();
        let id = reg.open(info());
        reg.record_leased(id, 1, 10);
        reg.record_leased(id, 2, 10);
        let (sub_id, mut seqs) = reg.close(id).expect("session should exist");
        seqs.sort_unstable();
        assert_eq!(sub_id, 1);
        assert_eq!(seqs, vec![1, 2]);
        assert!(reg.info(id).is_none());
    }
}
