//! A small clock abstraction so delivery-timing logic (ack-deadline
//! expiry, redelivery) can be driven deterministically in tests instead of
//! depending on real wall-clock sleeps.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Anything that can report "now" as milliseconds since the Unix epoch.
pub trait Clock: Send + Sync {
    /// The current time, milliseconds since the Unix epoch.
    fn now_ms(&self) -> i64;
}

/// The real wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

/// A manually-advanced clock for tests: starts at a fixed instant and only
/// moves when [`MockClock::advance`] is called.
#[derive(Debug)]
pub struct MockClock(AtomicI64);

impl MockClock {
    /// Creates a clock starting at `start_ms` (ms since the Unix epoch).
    pub fn new(start_ms: i64) -> Self {
        Self(AtomicI64::new(start_ms))
    }

    /// Moves the clock forward by `ms` milliseconds and returns the new
    /// value.
    pub fn advance(&self, ms: i64) -> i64 {
        self.0.fetch_add(ms, Ordering::SeqCst) + ms
    }

    /// Moves the clock forward by `secs` seconds and returns the new
    /// value in milliseconds (convenience for test scenarios phrased in
    /// seconds, matching Given/When/Then style wording).
    pub fn advance_secs(&self, secs: i64) -> i64 {
        self.advance(secs.saturating_mul(1000))
    }
}

impl Clock for MockClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_clock_starts_at_given_value() {
        let c = MockClock::new(1_000);
        assert_eq!(c.now_ms(), 1_000);
    }

    #[test]
    fn mock_clock_advances() {
        let c = MockClock::new(1_000);
        c.advance(500);
        assert_eq!(c.now_ms(), 1_500);
        c.advance_secs(2);
        assert_eq!(c.now_ms(), 3_500);
    }

    #[test]
    fn system_clock_is_positive_and_roughly_now() {
        let now = SystemClock.now_ms();
        assert!(now > 1_700_000_000_000); // sometime in 2023+
    }
}
