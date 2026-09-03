//! Exponential retry backoff, used both by
//! [`crate::push::dispatcher`] (between push delivery attempts) and by
//! [`super::engine::DeliveryEngine`]'s explicit-Nack path (between
//! `ModifyAckDeadline(seconds<=0)` and the message becoming eligible for
//! redelivery again).
//!
//! ## Scope (documented simplification)
//!
//! Every subscription always has *some* `min_retry_backoff_secs`/
//! `max_retry_backoff_secs` — `crate::subscription::SubscriptionStore::create`
//! substitutes defaults (10s/600s) when a client doesn't set them
//! explicitly, and that distinction ("explicitly set" vs "defaulted") is
//! not tracked past that point. So rather than treating the no-policy
//! case as immediate redelivery, this module applies backoff uniformly
//! using whatever bounds the subscription record holds (explicit or
//! default) — arguably closer to real Pub/Sub's own behavior (its
//! documented defaults apply even when a caller never set `retry_policy`
//! explicitly) and avoids adding an extra "was this explicitly
//! configured" field purely to distinguish a case whose two outcomes
//! (immediate vs. a 10s-backed default backoff) are unlikely to matter in
//! practice.
//!
//! Applied to: [`crate::push::dispatcher`] (every push attempt) and
//! [`super::engine::DeliveryEngine`]'s *explicit* Nack path
//! (`ModifyAckDeadline`/`stream_modify_ack_deadline` with `seconds <= 0`).
//! *Not* applied to plain ack-deadline timeout (the self-healing
//! `lease_next` reclaim, `super::engine`'s module doc comment) — a
//! natural redelivery there is already bounded by `ack_deadline_secs`
//! itself, and retrofitting a second, independent backoff window onto
//! that self-healing reclaim would need lease state to track "reclaimed
//! but on hold until `next_eligible_at`" as a concept distinct from "has
//! an active lease", a real data-model change out of scope for this
//! pass.

use std::time::Duration;

/// Floor applied under `min_backoff_secs`, matching the "push default"
/// lower bound from the contract (100 ms) even when a subscription's
/// `min_retry_backoff_secs` is `0` (the documented minimum, per
/// `crate::limits::MIN_RETRY_BACKOFF_SECS`).
const MIN_POSSIBLE_BACKOFF: Duration = Duration::from_millis(100);

/// Exponential backoff from `attempts` (1-indexed —
/// `Delivered::delivery_attempt`/`PulledMessage::delivery_attempt`):
/// `min_backoff * 2^(attempts-1)`, capped at `max_backoff`.
pub fn backoff_for_attempts(
    attempts: u32,
    min_backoff_secs: i64,
    max_backoff_secs: i64,
) -> Duration {
    let min_backoff = Duration::from_secs(min_backoff_secs.max(0) as u64).max(MIN_POSSIBLE_BACKOFF);
    let max_backoff = Duration::from_secs(max_backoff_secs.max(0) as u64).max(min_backoff);
    let shift = attempts.saturating_sub(1).min(20); // 2^20 already dwarfs any real max_backoff
    min_backoff.saturating_mul(1u32 << shift).min(max_backoff)
}

/// Absolute time (ms since the Unix epoch) at which a message with
/// `attempts` prior deliveries becomes eligible for redelivery again,
/// starting from `now_ms`.
pub fn next_eligible_at_ms(
    now_ms: i64,
    attempts: u32,
    min_backoff_secs: i64,
    max_backoff_secs: i64,
) -> i64 {
    let backoff = backoff_for_attempts(attempts, min_backoff_secs, max_backoff_secs);
    now_ms.saturating_add(backoff.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_then_caps_at_max() {
        assert_eq!(backoff_for_attempts(1, 0, 60), Duration::from_millis(100));
        assert_eq!(backoff_for_attempts(2, 1, 60), Duration::from_secs(2));
        assert_eq!(backoff_for_attempts(3, 1, 60), Duration::from_secs(4));
        assert_eq!(backoff_for_attempts(20, 1, 60), Duration::from_secs(60));
    }

    #[test]
    fn next_eligible_at_is_now_plus_backoff() {
        assert_eq!(next_eligible_at_ms(1_000, 1, 1, 60), 2_000);
    }
}
