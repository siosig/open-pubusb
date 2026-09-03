//! systemd `sd_notify` integration (`Type=notify` readiness/watchdog
//! protocol). Every function is a no-op (returns without error) when not
//! running under systemd — `sd-notify` detects this itself via the
//! `NOTIFY_SOCKET` environment variable and returns `Ok(())` without
//! sending anything, so this module never needs to special-case "not
//! under systemd" explicitly.

use tokio_util::sync::CancellationToken;

/// Sends `READY=1`. Call once, after the server has finished starting and
/// `/readyz` would return 200.
pub fn notify_ready() {
    if let Err(e) = sd_notify::notify(&[sd_notify::NotifyState::Ready]) {
        tracing::debug!(error = %e, "sd_notify READY failed (not running under systemd Type=notify)");
    }
}

/// Sends `STOPPING=1`. Call once, when graceful shutdown begins.
pub fn notify_stopping() {
    if let Err(e) = sd_notify::notify(&[sd_notify::NotifyState::Stopping]) {
        tracing::debug!(error = %e, "sd_notify STOPPING failed");
    }
}

/// Runs the `WATCHDOG=1` keepalive loop until `shutdown_token` is
/// cancelled. Pings at half the watchdog interval systemd configured
/// (`WatchdogSec` in the unit), per `sd_notify::watchdog_enabled`'s
/// contract. Returns immediately (no loop) if no watchdog is configured.
pub async fn run_watchdog_loop(shutdown_token: CancellationToken) {
    let Some(watchdog_interval) = sd_notify::watchdog_enabled() else {
        return;
    };
    let interval = watchdog_interval / 2;
    tracing::debug!(?interval, "sd_notify watchdog loop starting");

    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                if let Err(e) = sd_notify::notify(&[sd_notify::NotifyState::Watchdog]) {
                    tracing::debug!(error = %e, "sd_notify WATCHDOG ping failed");
                }
            }
            _ = shutdown_token.cancelled() => {
                break;
            }
        }
    }
}
