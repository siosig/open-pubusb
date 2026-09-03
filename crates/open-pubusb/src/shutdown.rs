//! Readiness state tracking and graceful-shutdown signal handling.

use std::sync::atomic::{AtomicU8, Ordering};

/// Process readiness state, exposed via `/readyz` and consulted during shutdown.
///
/// Transitions monotonically forward in practice (`STARTING` -> `READY` -> `DRAINING`), but
/// this type does not itself enforce that ordering — callers set the state explicitly at the
/// appropriate points in the startup/shutdown sequence.
pub struct ReadyState(AtomicU8);

impl ReadyState {
    pub const STARTING: u8 = 0;
    pub const READY: u8 = 1;
    pub const DRAINING: u8 = 2;

    /// Construct a new `ReadyState`, starting in the `STARTING` state.
    pub fn new() -> Self {
        Self(AtomicU8::new(Self::STARTING))
    }

    /// Transition to `READY` (storage recovered, listeners bound).
    pub fn set_ready(&self) {
        self.0.store(Self::READY, Ordering::SeqCst);
    }

    /// Transition to `DRAINING` (shutdown in progress: `/readyz` should now return 503).
    pub fn set_draining(&self) {
        self.0.store(Self::DRAINING, Ordering::SeqCst);
    }

    /// True if currently `READY`.
    pub fn is_ready(&self) -> bool {
        self.0.load(Ordering::SeqCst) == Self::READY
    }

    /// True if currently `DRAINING`.
    #[allow(dead_code)] // exercised by tests; not yet read from production code
    pub fn is_draining(&self) -> bool {
        self.0.load(Ordering::SeqCst) == Self::DRAINING
    }

    /// Raw state value (one of `STARTING`, `READY`, `DRAINING`).
    #[allow(dead_code)] // exercised by tests; not yet read from production code
    pub fn load(&self) -> u8 {
        self.0.load(Ordering::SeqCst)
    }
}

impl Default for ReadyState {
    fn default() -> Self {
        Self::new()
    }
}

/// Construct a new root [`tokio_util::sync::CancellationToken`] for coordinating graceful
/// shutdown across tasks. Trivial wrapper so call sites don't need to import `tokio_util`
/// directly.
pub fn root_cancellation_token() -> tokio_util::sync::CancellationToken {
    tokio_util::sync::CancellationToken::new()
}

/// Await SIGTERM or SIGINT (Ctrl-C) and cancel `token` when either arrives.
///
/// If installing the SIGTERM handler fails (e.g. no signal support in the current
/// environment), this returns early without listening for anything, rather than panicking.
pub async fn listen_for_shutdown_signal(token: tokio_util::sync::CancellationToken) {
    let Ok(mut sigterm) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    else {
        return;
    };

    tokio::select! {
        _ = sigterm.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }

    tracing::info!("shutdown signal received");
    token.cancel();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_at_starting() {
        let state = ReadyState::new();
        assert_eq!(state.load(), ReadyState::STARTING);
        assert!(!state.is_ready());
        assert!(!state.is_draining());
    }

    #[test]
    fn set_ready_transitions_to_ready() {
        let state = ReadyState::new();
        state.set_ready();
        assert!(state.is_ready());
        assert!(!state.is_draining());
        assert_eq!(state.load(), ReadyState::READY);
    }

    #[test]
    fn set_draining_transitions_to_draining() {
        let state = ReadyState::new();
        state.set_ready();
        state.set_draining();
        assert!(state.is_draining());
        assert!(!state.is_ready());
        assert_eq!(state.load(), ReadyState::DRAINING);
    }
}
