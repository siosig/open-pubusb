//! `/healthz`, `/readyz`, `/metrics` — the operational endpoints. Served on
//! `server.admin_listen` (or merged onto the main listener when that's empty —
//! `crates/open-pubusb/src/main.rs` decides which).

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use axum::routing::get;
use axum::Router;
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::shutdown::ReadyState;

async fn healthz() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({ "status": "ok" }))
}

async fn readyz(
    axum::extract::State(state): axum::extract::State<Arc<ReadyState>>,
) -> axum::http::StatusCode {
    if state.is_ready() {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn metrics(axum::extract::State(handle): axum::extract::State<PrometheusHandle>) -> String {
    handle.render()
}

/// Builds the admin router. `ready_state` drives `/readyz`; `metrics_handle`
/// is `None` when `metrics.enabled = false` (the route then always 404s —
/// simpler than conditionally registering it, and matches "disabled"
/// closely enough for an internal endpoint).
pub fn router(ready_state: Arc<ReadyState>, metrics_handle: Option<PrometheusHandle>) -> Router {
    let mut router = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz).with_state(ready_state));
    if let Some(handle) = metrics_handle {
        router = router.route("/metrics", get(metrics).with_state(handle));
    }
    router
}

/// Binds `listen_addr` and serves the admin router until `shutdown_token`
/// is cancelled, then waits for in-flight requests to finish before the
/// returned [`tokio::task::JoinHandle`] resolves. See [`crate::server::serve`]'s
/// doc comment: bounding how long the caller waits on this handle is its
/// responsibility, not this function's.
pub async fn serve(
    listen_addr: SocketAddr,
    ready_state: Arc<ReadyState>,
    metrics_handle: Option<PrometheusHandle>,
    shutdown_token: CancellationToken,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let app = router(ready_state, metrics_handle);
    let listener = TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("failed to bind server.admin_listen address {listen_addr}"))?;
    tracing::info!(addr = %listen_addr, "listening (admin: healthz/readyz/metrics)");
    let handle = tokio::spawn(async move {
        let result = axum::serve(listener, app.into_make_service())
            .with_graceful_shutdown(shutdown_token.cancelled_owned())
            .await;
        if let Err(err) = result {
            tracing::error!(error = %err, "admin server error");
        }
    });
    Ok(handle)
}
