//! Merges the gRPC and REST routers onto one listener and serves them,
//! honoring graceful shutdown. `crates/open-pubusb/src/admin.rs` serves
//! `/healthz`, `/readyz`, `/metrics` separately (its own port, or this one
//! when `admin_listen` is empty — see `crates/open-pubusb/src/main.rs`).

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use open_pubusb_core::service::PubSubService;
use open_pubusb_core::store::kv::KvStore;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::grpc;

/// Binds `listen_addr` and serves the merged gRPC + REST router until
/// `shutdown_token` is cancelled, then waits for in-flight requests to
/// finish before the returned [`tokio::task::JoinHandle`] resolves.
///
/// The caller (`crates/open-pubusb/src/main.rs`) is responsible for bounding how
/// long it waits on that handle by `server.shutdown_grace_secs` — this
/// function itself does not time out, so a hung request would otherwise
/// block shutdown forever.
pub async fn serve<K: KvStore + 'static>(
    listen_addr: SocketAddr,
    svc: Arc<PubSubService<K>>,
    enable_reflection: bool,
    descriptor_set: &'static [u8],
    streaming_pull_max_lifetime_secs: u64,
    shutdown_token: CancellationToken,
) -> anyhow::Result<(grpc::HealthReporter, tokio::task::JoinHandle<()>)> {
    let (routes, health_reporter) = grpc::build_routes(
        svc.clone(),
        enable_reflection,
        descriptor_set,
        streaming_pull_max_lifetime_secs,
        shutdown_token.clone(),
    );
    // The gRPC `Routes::into_axum_router()` carries its own fallback
    // (UNIMPLEMENTED for unmatched gRPC paths); axum panics if both sides
    // of a `.merge()` set one, so the REST router intentionally sets none
    // and the 501 fallback is applied once, here, after the merge.
    let app = crate::rest::router::router(svc)
        .merge(routes.into_axum_router())
        .fallback(crate::rest::fallback);

    let listener = TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("failed to bind server.listen address {listen_addr}"))?;
    tracing::info!(addr = %listen_addr, "listening (gRPC + REST)");

    let handle = tokio::spawn(async move {
        let result = axum::serve(listener, app.into_make_service())
            .with_graceful_shutdown(shutdown_token.cancelled_owned())
            .await;
        if let Err(err) = result {
            tracing::error!(error = %err, "server error");
        }
    });

    Ok((health_reporter, handle))
}
