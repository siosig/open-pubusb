#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
//! `open-pubusb`: a Google Cloud Pub/Sub v1-compatible message broker.
//!
//! CLI surface: `serve`, `check-config`, `health`, `version`.

mod admin;
mod config;
mod grpc;
mod rest;
mod server;
mod shutdown;
mod systemd;
mod telemetry;

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use open_pubusb_core::service::PubSubService;

#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(name = "open-pubusb", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Starts the server.
    Serve {
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        listen: Option<String>,
        #[arg(long = "admin-listen")]
        admin_listen: Option<String>,
        #[arg(long = "data-dir")]
        data_dir: Option<String>,
        #[arg(long)]
        ephemeral: bool,
    },
    /// Loads and validates configuration, then exits (0 = valid, 2 = invalid).
    CheckConfig {
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Checks `/readyz` on a running server (for container `HEALTHCHECK`).
    Health {
        #[arg(long, default_value = "http://127.0.0.1:8086/readyz")]
        url: String,
        #[arg(long, default_value_t = 5)]
        timeout: u64,
    },
    /// Prints version information.
    Version,
}

fn resolve_config(cli_flag: Option<PathBuf>) -> Option<PathBuf> {
    cli_flag
        .or_else(|| std::env::var("OPEN_PUBUSB_CONFIG").ok().map(PathBuf::from))
        .or_else(|| {
            let default = PathBuf::from("/etc/open-pubusb/config.toml");
            default.exists().then_some(default)
        })
}

fn apply_cli_overrides(
    mut cfg: config::Config,
    listen: Option<String>,
    admin_listen: Option<String>,
    data_dir: Option<String>,
    ephemeral: bool,
) -> config::Config {
    if let Some(v) = listen {
        cfg.server.listen = v;
    }
    if let Some(v) = admin_listen {
        cfg.server.admin_listen = v;
    }
    if let Some(v) = data_dir {
        cfg.storage.data_dir = v;
    }
    if ephemeral {
        cfg.storage.ephemeral = true;
    }
    cfg
}

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Version => {
            println!(
                "open-pubusb {} ({})",
                env!("CARGO_PKG_VERSION"),
                std::env::consts::ARCH,
            );
            std::process::ExitCode::SUCCESS
        }
        Command::CheckConfig {
            config: config_path,
        } => match config::Config::load(resolve_config(config_path).as_deref()) {
            Ok(cfg) => match cfg.validate() {
                Ok(()) => {
                    println!("config is valid");
                    std::process::ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("invalid config: {e}");
                    std::process::ExitCode::from(2)
                }
            },
            Err(e) => {
                eprintln!("failed to load config: {e}");
                std::process::ExitCode::from(2)
            }
        },
        Command::Health { url, timeout } => {
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("failed to start async runtime: {e}");
                    return std::process::ExitCode::from(1);
                }
            };
            rt.block_on(run_health_check(url, timeout))
        }
        Command::Serve {
            config: config_path,
            listen,
            admin_listen,
            data_dir,
            ephemeral,
        } => {
            let cfg = match config::Config::load(resolve_config(config_path).as_deref()) {
                Ok(cfg) => cfg,
                Err(e) => {
                    eprintln!("failed to load config: {e}");
                    return std::process::ExitCode::from(2);
                }
            };
            let cfg = apply_cli_overrides(cfg, listen, admin_listen, data_dir, ephemeral);
            if let Err(e) = cfg.validate() {
                eprintln!("invalid config ({}): {e}", e.config_key().unwrap_or("?"));
                return std::process::ExitCode::from(2);
            }

            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("failed to start async runtime: {e}");
                    return std::process::ExitCode::from(1);
                }
            };
            rt.block_on(run_serve(cfg))
        }
    }
}

async fn run_health_check(url: String, timeout_secs: u64) -> std::process::ExitCode {
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(timeout_secs))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to build HTTP client: {e}");
            return std::process::ExitCode::from(1);
        }
    };
    match client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => std::process::ExitCode::SUCCESS,
        Ok(resp) => {
            eprintln!("{url} returned {}", resp.status());
            std::process::ExitCode::from(1)
        }
        Err(e) => {
            eprintln!("{url}: {e}");
            std::process::ExitCode::from(1)
        }
    }
}

async fn run_serve(cfg: config::Config) -> std::process::ExitCode {
    if let Err(e) = telemetry::init(&cfg.log.format, &config::effective_log_filter(&cfg.log)) {
        eprintln!("failed to initialize logging: {e}");
        return std::process::ExitCode::from(1);
    }

    let ready_state = Arc::new(shutdown::ReadyState::new());
    let shutdown_token = shutdown::root_cancellation_token();

    let kv: Arc<open_pubusb_core::store::kv::AnyKv> = if cfg.storage.ephemeral {
        Arc::new(open_pubusb_core::store::kv::AnyKv::Mem(
            open_pubusb_core::store::kv::MemKv::new(),
        ))
    } else {
        let data_dir = std::path::Path::new(&cfg.storage.data_dir);
        match open_pubusb_core::store::fjall::FjallKv::open(data_dir, cfg.storage.cache_size_bytes)
        {
            Ok(fjall) => Arc::new(open_pubusb_core::store::kv::AnyKv::Fjall(fjall)),
            Err(e) => {
                // Exit 1 is the fatal-runtime-error code (e.g. storage
                // corruption) — a data directory that can't be opened (or a
                // format version newer than this binary supports) is that
                // category, not the pre-startup config/env bucket (2).
                tracing::error!(
                    data_dir = %cfg.storage.data_dir,
                    error = %e,
                    "failed to open persistent storage"
                );
                return std::process::ExitCode::from(1);
            }
        }
    };

    let svc = Arc::new(
        PubSubService::new(kv.clone(), Arc::new(open_pubusb_core::clock::SystemClock))
            .with_max_disk_bytes(cfg.storage.max_disk_bytes),
    );

    let listen_addr = match cfg.server.listen.parse() {
        Ok(addr) => addr,
        Err(e) => {
            tracing::error!(addr = %cfg.server.listen, error = %e, "invalid server.listen address");
            return std::process::ExitCode::from(2);
        }
    };

    let metrics_handle = if cfg.metrics.enabled {
        match metrics_exporter_prometheus::PrometheusBuilder::new().install_recorder() {
            Ok(handle) => {
                open_pubusb_core::metrics::describe_all();
                Some(handle)
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to install Prometheus recorder; /metrics disabled");
                None
            }
        }
    } else {
        None
    };

    let merge_admin = cfg.server.admin_listen.trim().is_empty();
    let admin_addr = if merge_admin {
        None
    } else {
        match cfg.server.admin_listen.parse() {
            Ok(addr) => Some(addr),
            Err(e) => {
                tracing::error!(addr = %cfg.server.admin_listen, error = %e, "invalid server.admin_listen address");
                return std::process::ExitCode::from(2);
            }
        }
    };

    let (health_reporter, server_handle) = match server::serve(
        listen_addr,
        svc.clone(),
        cfg.server.enable_reflection,
        open_pubusb_proto::FILE_DESCRIPTOR_SET,
        cfg.delivery.streaming_pull_max_lifetime_secs,
        shutdown_token.clone(),
    )
    .await
    {
        Ok(pair) => pair,
        Err(e) => {
            // A bind failure (port in use, invalid address, ...) is a
            // pre-startup environment problem, not a runtime fault — exit
            // code 2, matching `check-config`'s failure code. `{e:?}` (not
            // `{e}`) so the `.with_context(...)` chain in `server::serve`
            // (which names the offending address) is included.
            tracing::error!(error = ?e, "failed to start gRPC/REST server");
            return std::process::ExitCode::from(2);
        }
    };

    let admin_handle = if let Some(admin_addr) = admin_addr {
        match admin::serve(
            admin_addr,
            ready_state.clone(),
            metrics_handle,
            shutdown_token.clone(),
        )
        .await
        {
            Ok(handle) => Some(handle),
            Err(e) => {
                tracing::error!(error = ?e, "failed to start admin server");
                return std::process::ExitCode::from(2);
            }
        }
    } else {
        // When merged (admin_listen empty), /healthz /readyz /metrics are
        // not yet reachable on the main listener — that merge is a later
        // refinement; for now an empty admin_listen simply means "no
        // separate admin server" rather than a hard error, since
        // --ephemeral/dev usage doesn't require it.
        None
    };

    // `PubSubService::new` (above) already recovered any in-memory-only
    // state (the delivery engine's lease table) synchronously from `kv`
    // before returning, so it's safe to flip health/readiness now.
    let storage_sync_handle = tokio::spawn(run_storage_sync_loop(
        kv.clone(),
        cfg.storage.sync_interval_ms,
        shutdown_token.clone(),
    ));
    let push_manager_handle = tokio::spawn(run_push_manager_loop(
        svc.clone(),
        cfg.delivery.push_timeout_secs,
        cfg.delivery.push_max_concurrency_per_sub,
        shutdown_token.clone(),
    ));
    let retention_sweep_handle = tokio::spawn(run_retention_sweep_loop(
        svc.clone(),
        cfg.delivery.retention_sweep_interval_secs,
        shutdown_token.clone(),
    ));

    health_reporter
        .set_serving::<open_pubusb_proto::pubsub::v1::publisher_server::PublisherServer<
            grpc::publisher::PublisherService<open_pubusb_core::store::kv::AnyKv>,
        >>()
        .await;
    health_reporter
        .set_serving::<open_pubusb_proto::pubsub::v1::subscriber_server::SubscriberServer<
            grpc::subscriber::SubscriberService<open_pubusb_core::store::kv::AnyKv>,
        >>()
        .await;
    ready_state.set_ready();
    systemd::notify_ready();
    let watchdog_handle = tokio::spawn(systemd::run_watchdog_loop(shutdown_token.clone()));
    tracing::info!("ready");

    shutdown::listen_for_shutdown_signal(shutdown_token.clone()).await;

    // Order matters here: stop advertising readiness and gRPC health
    // *before* waiting for the
    // servers to actually finish draining, so a client that checks
    // `/readyz`/`Health.Check` right after the signal already sees the
    // service as going away, even while in-flight requests are still
    // being allowed to complete underneath.
    ready_state.set_draining();
    health_reporter
        .set_not_serving::<open_pubusb_proto::pubsub::v1::publisher_server::PublisherServer<
            grpc::publisher::PublisherService<open_pubusb_core::store::kv::AnyKv>,
        >>()
        .await;
    health_reporter
        .set_not_serving::<open_pubusb_proto::pubsub::v1::subscriber_server::SubscriberServer<
            grpc::subscriber::SubscriberService<open_pubusb_core::store::kv::AnyKv>,
        >>()
        .await;
    systemd::notify_stopping();
    tracing::info!(
        grace_secs = cfg.server.shutdown_grace_secs,
        "draining, waiting for in-flight requests"
    );

    let grace = std::time::Duration::from_secs(cfg.server.shutdown_grace_secs);
    let drain = async {
        let _ = server_handle.await;
        if let Some(admin_handle) = admin_handle {
            let _ = admin_handle.await;
        }
    };
    if tokio::time::timeout(grace, drain).await.is_err() {
        tracing::warn!("shutdown_grace_secs elapsed with requests still in flight; exiting anyway");
    }
    let _ = watchdog_handle.await;
    let _ = storage_sync_handle.await;
    let _ = push_manager_handle.await;
    let _ = retention_sweep_handle.await;

    // One final `SyncAll` fsync before exit, on top of the periodic
    // group-sync timer, so a clean SIGTERM never loses the last
    // <sync_interval_ms worth of writes to the OS page cache.
    if let Err(e) = kv.persist(fjall::PersistMode::SyncAll) {
        tracing::warn!(error = ?e, "final storage persist on shutdown failed");
    }

    std::process::ExitCode::SUCCESS
}

/// Every `sync_interval_ms`, flushes accumulated writes
/// (`fjall::PersistMode::SyncData` — a group fsync, FR-016) and updates the
/// `open_pubusb_storage_disk_bytes` gauge. Exits promptly once `shutdown_token`
/// is cancelled (the caller performs one final `SyncAll` persist itself).
async fn run_storage_sync_loop(
    kv: Arc<open_pubusb_core::store::kv::AnyKv>,
    sync_interval_ms: u64,
    shutdown_token: tokio_util::sync::CancellationToken,
) {
    use open_pubusb_core::store::kv::KvStore;

    let interval = std::time::Duration::from_millis(sync_interval_ms.max(1));
    loop {
        tokio::select! {
            () = tokio::time::sleep(interval) => {
                let start = std::time::Instant::now();
                if let Err(e) = kv.persist(fjall::PersistMode::SyncData) {
                    tracing::error!(error = ?e, "group-sync persist failed");
                }
                open_pubusb_core::metrics::record_storage_sync_duration(start.elapsed().as_secs_f64());
                open_pubusb_core::metrics::set_storage_disk_bytes(kv.approx_disk_bytes() as f64);
            }
            () = shutdown_token.cancelled() => return,
        }
    }
}

/// How long past its ack deadline an in-memory lease must sit before
/// [`run_retention_sweep_loop`] garbage-collects it — bounding
/// `LeaseTable` memory only (not required for delivery correctness; see
/// `DeliveryEngine::sweep_expired`'s doc comment and the module-level doc
/// comment on `crate::delivery::engine`'s self-healing `lease_next`). A
/// full hour comfortably outlives any subscription's ack deadline
/// (max 600s) and retry backoff (max 600s) so this can never race a
/// redelivery still legitimately in flight.
const LEASE_GC_GRACE_MS: i64 = 3_600_000;

/// Periodically (`delivery.retention_sweep_interval_secs`) runs message
/// retention + subscription-TTL expiry (`PubSubService::sweep_retention`)
/// and in-memory lease-table garbage collection
/// (`PubSubService::sweep_subscription` per subscription) —
/// both were implemented and unit-tested but never actually wired into a
/// running loop (their own doc comments said so: "periodically... a later
/// task"). Also refreshes the `open_pubusb_topics`/`open_pubusb_subscriptions`
/// gauges here, since it's already paying the cost of listing every
/// subscription for the lease sweep.
async fn run_retention_sweep_loop(
    svc: Arc<open_pubusb_core::service::PubSubService<open_pubusb_core::store::kv::AnyKv>>,
    retention_sweep_interval_secs: u64,
    shutdown_token: tokio_util::sync::CancellationToken,
) {
    let interval = std::time::Duration::from_secs(retention_sweep_interval_secs.max(1));
    loop {
        tokio::select! {
            () = tokio::time::sleep(interval) => {
                let stats = svc.sweep_retention();
                if stats.messages_expired > 0 {
                    tracing::info!(expired = stats.messages_expired, "retention sweep: messages expired");
                }
                let subs = svc.list_all_subscriptions();
                for sub in &subs {
                    svc.sweep_subscription(sub.id, LEASE_GC_GRACE_MS);
                }
                open_pubusb_core::metrics::set_subscriptions(subs.len() as f64);
                open_pubusb_core::metrics::set_topics(svc.list_all_topics().len() as f64);
            }
            () = shutdown_token.cancelled() => return,
        }
    }
}

/// Reconciles running push dispatchers against subscriptions' current
/// `push_config` every few seconds, and stops every dispatcher on
/// shutdown.
const PUSH_RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3);

async fn run_push_manager_loop(
    svc: Arc<open_pubusb_core::service::PubSubService<open_pubusb_core::store::kv::AnyKv>>,
    push_timeout_secs: u64,
    push_max_concurrency_per_sub: u32,
    shutdown_token: tokio_util::sync::CancellationToken,
) {
    let mut manager = open_pubusb_core::push::manager::PushManager::new(
        push_timeout_secs,
        push_max_concurrency_per_sub,
    );
    loop {
        manager.reconcile(&svc).await;
        tokio::select! {
            () = tokio::time::sleep(PUSH_RECONCILE_INTERVAL) => {}
            () = shutdown_token.cancelled() => break,
        }
    }
    manager.shutdown().await;
}
