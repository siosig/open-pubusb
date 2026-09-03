//! Telemetry initialization: configures the global `tracing` subscriber per `[log].format`.
//!
//! Message payloads and attribute values must never be logged at any level — only resource
//! names, ids, and counts.

use anyhow::{anyhow, Context};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Initialize the global `tracing` subscriber.
///
/// `format` selects the output encoding: `"json"` (one JSON object per line on stdout),
/// `"text"` (human-readable), or `"journald"` (structured fields sent to the systemd
/// journal, falling back to `"text"` if no journal socket is available).
///
/// `filter` is a `RUST_LOG`-compatible filter string, e.g. `"info,open_pubusb::delivery=debug"`.
/// Callers should prefer the `RUST_LOG` env var over `[log].level` when both are present
/// (that precedence is resolved by the caller before this function is invoked; this
/// function simply parses whatever filter string it is given).
pub fn init(format: &str, filter: &str) -> Result<(), anyhow::Error> {
    match format {
        "json" => {
            let env_filter = EnvFilter::try_new(filter)
                .with_context(|| format!("invalid log filter: {filter}"))?;
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(env_filter)
                .try_init()
                .map_err(|e| anyhow!("failed to initialize json tracing subscriber: {e}"))
        }
        "text" => {
            let env_filter = EnvFilter::try_new(filter)
                .with_context(|| format!("invalid log filter: {filter}"))?;
            tracing_subscriber::fmt()
                .with_env_filter(env_filter)
                .try_init()
                .map_err(|e| anyhow!("failed to initialize text tracing subscriber: {e}"))
        }
        "journald" => match tracing_journald::layer() {
            Ok(journald_layer) => {
                let env_filter = EnvFilter::try_new(filter)
                    .with_context(|| format!("invalid log filter: {filter}"))?;
                tracing_subscriber::registry()
                    .with(env_filter)
                    .with(journald_layer)
                    .try_init()
                    .map_err(|e| anyhow!("failed to initialize journald tracing subscriber: {e}"))
            }
            Err(e) => {
                // tracing isn't initialized yet at this point, so we can't log through it.
                eprintln!(
                    "warning: journald socket unavailable ({e}); falling back to text log format"
                );
                init("text", filter)
            }
        },
        other => Err(anyhow!(
            "unknown log format: {other} (expected json|text|journald)"
        )),
    }
}
