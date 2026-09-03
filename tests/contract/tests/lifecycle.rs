//! Contract tests for `open-pubusb`'s process lifecycle: CLI subcommands and
//! signal handling.
//!
//! Unlike the sibling `grpc_publisher.rs` / `grpc_subscriber_unary.rs` /
//! `rest_v1.rs` contract tests (which exercise RPC behavior against a
//! running server via `OpenPubusbHarness`'s gRPC/REST clients), these tests
//! exercise the `open-pubusb` *binary itself*: process exit codes, stdout/stderr
//! content, and its response to SIGTERM. All spawn the real compiled
//! `open-pubusb` binary (`open_pubusb_contract_tests::harness::open_pubusb_bin_path()`), none
//! are `#[ignore]`d, and every wait is bounded well under the default
//! `shutdown_grace_secs = 30` so the suite stays fast.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::time::Duration;

use open_pubusb_contract_tests::harness::{
    free_local_addr, open_pubusb_bin_path, OpenPubusbHarness,
};

/// SIGTERM with no in-flight requests: the process must stop advertising
/// readiness and exit 0 promptly (well under `shutdown_grace_secs`).
///
/// On SIGTERM / SIGINT the server performs a graceful stop: readyz→503,
/// reject new connections, ..., exit code 0.
#[tokio::test]
async fn sigterm_causes_prompt_exit_with_code_zero() {
    let mut harness = OpenPubusbHarness::start().await;

    // `OpenPubusbHarness::start()` already polled `/readyz` until it returned a
    // success status (or timed out) — re-check explicitly here so a
    // regression that breaks readiness fails loudly at this assertion
    // rather than silently inside the harness.
    let readyz_url = format!("http://{}/readyz", harness.admin_addr);
    let resp = reqwest::get(&readyz_url)
        .await
        .expect("GET /readyz should succeed while the server is up");
    assert!(
        resp.status().is_success(),
        "/readyz should be 2xx before shutdown, got {}",
        resp.status()
    );

    harness.send_sigterm();

    // Bounded wait, well under the default `shutdown_grace_secs = 30`.
    let status = harness
        .wait_for_exit(Duration::from_secs(5))
        .await
        .expect("process should exit within 5s of SIGTERM with no in-flight requests");
    assert!(
        status.success(),
        "expected exit code 0 after SIGTERM, got {status:?}"
    );
}

/// `open-pubusb health --url <readyz-url>` exits 0 while the server is serving,
/// and exits non-zero once the server has stopped.
///
/// `open-pubusb health` exits 0 on 200.
#[tokio::test]
async fn health_subcommand_reflects_server_liveness() {
    let mut harness = OpenPubusbHarness::start().await;
    let readyz_url = format!("http://{}/readyz", harness.admin_addr);

    let status_up = Command::new(open_pubusb_bin_path())
        .arg("health")
        .arg("--url")
        .arg(&readyz_url)
        .arg("--timeout")
        .arg("5")
        .status()
        .expect("failed to run `open-pubusb health` while the server is up");
    assert!(
        status_up.success(),
        "`open-pubusb health` should exit 0 while the server is up, got {status_up:?}"
    );

    harness.send_sigterm();
    harness
        .wait_for_exit(Duration::from_secs(5))
        .await
        .expect("server should exit within 5s of SIGTERM");

    let status_down = Command::new(open_pubusb_bin_path())
        .arg("health")
        .arg("--url")
        .arg(&readyz_url)
        .arg("--timeout")
        .arg("2")
        .status()
        .expect("failed to run `open-pubusb health` after the server stopped");
    assert!(
        !status_down.success(),
        "`open-pubusb health` should exit non-zero once the server has stopped, got {status_down:?}"
    );
}

/// `open-pubusb version` prints output containing the crate's semver.
///
/// `open-pubusb version` prints semver + git sha + build target.
/// `open-pubusb-contract-tests` and `open-pubusb` share the same workspace
/// `[workspace.package] version` (`Cargo.toml`), so `env!("CARGO_PKG_VERSION")`
/// evaluated in *this* crate is the exact string `open-pubusb version` must emit.
#[test]
fn version_subcommand_prints_crate_version() {
    let output = Command::new(open_pubusb_bin_path())
        .arg("version")
        .output()
        .expect("failed to run `open-pubusb version`");
    assert!(
        output.status.success(),
        "`open-pubusb version` should exit 0, got {:?}",
        output.status
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(env!("CARGO_PKG_VERSION")),
        "expected `open-pubusb version` stdout to contain '{}', got: {stdout:?}",
        env!("CARGO_PKG_VERSION")
    );
}

/// Binding to an already-occupied `server.listen` address fails fast, and
/// the failure is surfaced in the process's own log output.
///
/// A configuration/environment error before startup exits with code 2, and
/// the offending address is named in the log. `crates/open-pubusb/src/server.rs::serve`
/// attaches the offending address via `anyhow::Context`
/// (`.with_context(|| format!("failed to bind server.listen address {listen_addr}"))`)
/// and `crates/open-pubusb/src/main.rs::run_serve` maps a bind failure to exit
/// code 2 (not the generic runtime-error bucket, 1) — this was fixed after
/// this test first caught the mismatch empirically; see git history for the
/// prior "DEVIATION FROM SPEC" version of this comment if curious.
#[tokio::test]
async fn listen_port_already_in_use_fails_fast_and_logs_the_conflict() {
    let occupied_listener =
        TcpListener::bind("127.0.0.1:0").expect("failed to bind a port to occupy");
    let occupied_addr = occupied_listener
        .local_addr()
        .expect("failed to read occupied listener's local addr")
        .to_string();

    let admin_addr = free_local_addr();

    let child = Command::new(open_pubusb_bin_path())
        .arg("serve")
        .arg("--ephemeral")
        .arg("--listen")
        .arg(&occupied_addr)
        .arg("--admin-listen")
        .arg(&admin_addr)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn open-pubusb serve");

    let output = tokio::task::spawn_blocking(move || {
        child
            .wait_with_output()
            .expect("failed to wait for open-pubusb serve to exit")
    })
    .await
    .expect("wait_with_output task panicked");

    // Keep `occupied_listener` alive (and thus the port genuinely occupied)
    // for the whole spawn+wait above; only drop it now that the child has
    // already observed the conflict and exited.
    drop(occupied_listener);

    assert_eq!(
        output.status.code(),
        Some(2),
        "bind failure on an occupied port must exit 2, got {:?}",
        output.status
    );

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let combined_lower = combined.to_lowercase();
    assert!(
        combined_lower.contains("already in use") || combined_lower.contains("address in use"),
        "expected startup failure output to mention the bind conflict, got: {combined:?}"
    );
    assert!(
        combined.contains(&occupied_addr),
        "expected startup failure output to name the occupied address {occupied_addr:?}, got: {combined:?}"
    );
}

/// An unknown top-level config key fails `check-config` with exit code 2,
/// and the failing key is named in the output.
///
/// An unknown key fails validation at startup with exit code 2
/// (configuration/environment error, pre-startup); mirrors the `config::Config::load`
/// unit-level behavior already covered in `crates/open-pubusb/src/config.rs`'s own
/// tests, but here end-to-end through the real `open-pubusb check-config` process.
#[test]
fn unknown_config_key_fails_check_config_with_exit_code_two() {
    let config_path = std::env::temp_dir().join(format!(
        "open-pubusb-lifecycle-unknown-key-{}-{}.toml",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before UNIX_EPOCH")
            .as_nanos()
    ));
    std::fs::write(&config_path, "not_a_real_key = 1\n").expect("failed to write temp config file");

    let output = Command::new(open_pubusb_bin_path())
        .arg("check-config")
        .arg("--config")
        .arg(&config_path)
        .output()
        .expect("failed to run `open-pubusb check-config`");

    let _ = std::fs::remove_file(&config_path);

    assert_eq!(
        output.status.code(),
        Some(2),
        "expected exit code 2 for an unknown config key, got {:?}",
        output.status
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("not_a_real_key"),
        "expected output to mention the offending key 'not_a_real_key', got: {combined:?}"
    );
}
