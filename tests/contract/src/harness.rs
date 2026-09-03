//! Test harness that spawns a real `open-pubusb serve --ephemeral` process on two
//! free local ports and exposes ready-to-use gRPC/REST clients to it.
//!
//! `start()` tolerates the spawned process never becoming ready: it polls
//! `/readyz` for a bounded amount of time and returns the harness regardless
//! of the outcome. Every test that actually exercises the harness's clients
//! is `#[ignore]`d until those tasks land.

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::Duration;

use open_pubusb_proto::pubsub::v1::publisher_client::PublisherClient;
use open_pubusb_proto::pubsub::v1::subscriber_client::SubscriberClient;
use tonic::transport::Channel;

/// How long `start()` will wait for `/readyz` to return 200 before giving up
/// and returning the harness anyway.
const READY_TIMEOUT: Duration = Duration::from_secs(5);
const READY_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Owns a spawned `open-pubusb serve --ephemeral` child process and the addresses
/// it was told to listen on. Killed on drop.
pub struct OpenPubusbHarness {
    child: Child,
    /// `host:port` the gRPC+REST multiplexed listener was started on.
    pub grpc_addr: String,
    /// `host:port` the admin (`/healthz`, `/readyz`, `/metrics`) listener was
    /// started on.
    pub admin_addr: String,
}

impl Drop for OpenPubusbHarness {
    fn drop(&mut self) {
        // Best-effort: the process may have already exited (e.g. today's
        // placeholder binary exits immediately), so ignore all errors.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl OpenPubusbHarness {
    /// Spawns `open-pubusb serve --ephemeral` on two free local ports and waits
    /// (briefly, best-effort) for it to report readiness.
    pub async fn start() -> OpenPubusbHarness {
        Self::start_with_env(&[]).await
    }

    /// Like [`Self::start`], but with additional `OPEN_PUBUSB__...` environment
    /// variables set on the spawned process — for tests that need a
    /// non-default config value (e.g. a short
    /// `OPEN_PUBUSB__DELIVERY__STREAMING_PULL_MAX_LIFETIME_SECS` to exercise the
    /// `StreamingPull` lifetime timer without a real 30-minute wait).
    pub async fn start_with_env(envs: &[(&str, &str)]) -> OpenPubusbHarness {
        let grpc_addr = free_local_addr();
        let admin_addr = free_local_addr();

        let bin = open_pubusb_bin_path();
        let child = Command::new(&bin)
            .arg("serve")
            .arg("--ephemeral")
            .arg("--listen")
            .arg(&grpc_addr)
            .arg("--admin-listen")
            .arg(&admin_addr)
            .envs(envs.iter().copied())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| {
                panic!(
                    "failed to spawn open-pubusb binary at {}: {e} \
                     (build it first with `cargo build -p open-pubusb`)",
                    bin.display()
                )
            });

        let harness = OpenPubusbHarness {
            child,
            grpc_addr,
            admin_addr,
        };

        harness.wait_ready().await;
        harness
    }

    /// Polls `GET {admin_addr}/readyz` for up to `READY_TIMEOUT`. Never
    /// panics: today's placeholder `open-pubusb` binary does not open this port at
    /// all, so timing out here is expected and not a failure of the harness
    /// itself.
    async fn wait_ready(&self) {
        let url = format!("http://{}/readyz", self.admin_addr);
        let client = reqwest::Client::new();
        let deadline = tokio::time::Instant::now() + READY_TIMEOUT;

        while tokio::time::Instant::now() < deadline {
            if let Ok(resp) = client.get(&url).send().await {
                if resp.status().is_success() {
                    return;
                }
            }
            tokio::time::sleep(READY_POLL_INTERVAL).await;
        }
    }

    /// Lazily-connecting gRPC channel pointed at the multiplexed
    /// gRPC+REST port. Exposed so tests can build clients for services this
    /// harness doesn't have a dedicated constructor for (e.g.
    /// `SchemaServiceClient`, `IamPolicyClient`).
    pub fn channel(&self) -> Channel {
        tonic::transport::Endpoint::new(format!("http://{}", self.grpc_addr))
            .expect("invalid grpc endpoint URI")
            .connect_lazy()
    }

    /// A `PublisherClient` connected (lazily) to this harness's server.
    pub fn publisher_client(&self) -> PublisherClient<Channel> {
        PublisherClient::new(self.channel())
    }

    /// A `SubscriberClient` connected (lazily) to this harness's server.
    pub fn subscriber_client(&self) -> SubscriberClient<Channel> {
        SubscriberClient::new(self.channel())
    }

    /// Base URL for REST calls. gRPC and REST are multiplexed on the *same*
    /// port (`grpc_addr`); `admin_addr` is a separate port reserved for
    /// `/healthz`, `/readyz`, `/metrics` only.
    pub fn rest_base_url(&self) -> String {
        format!("http://{}", self.grpc_addr)
    }

    /// OS process id of the spawned `open-pubusb` child.
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Sends a real SIGTERM to the spawned child (
    /// `Child::kill()` sends SIGKILL on Unix, which bypasses the graceful
    /// shutdown path entirely — tests exercising graceful shutdown need the
    /// real signal instead).
    pub fn send_sigterm(&self) {
        // SAFETY: `libc::kill` with a pid this process itself spawned (and
        // has not yet reaped) and the well-defined `SIGTERM` signal number
        // has no memory-safety implications; it's a plain syscall wrapper.
        unsafe {
            libc::kill(self.pid() as libc::pid_t, libc::SIGTERM);
        }
    }

    /// Non-blocking check for whether the child has exited yet, per
    /// [`Child::try_wait`].
    pub fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    /// Polls [`Self::try_wait`] until the child exits or `timeout` elapses.
    /// Returns `None` on timeout (the child is still running).
    pub async fn wait_for_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Ok(Some(status)) = self.try_wait() {
                return Some(status);
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// Binds a `TcpListener` to port 0 to let the OS assign a free local port,
/// reads back the assigned address, then drops the listener so the port is
/// free again for the spawned process to bind to. Not fully race-free
/// (another process could grab the port between drop and spawn), but that's
/// an acceptable risk for a test harness and avoids an extra dependency.
pub fn free_local_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind free port");
    let addr = listener.local_addr().expect("failed to read local addr");
    drop(listener);
    addr.to_string()
}

/// Resolves the path to the compiled `open-pubusb` binary.
///
/// `open-pubusb` (the CLI binary crate) and `open-pubusb-contract-tests` (this crate) are
/// separate packages in the same Cargo workspace, so `env!("CARGO_BIN_EXE_open-pubusb")`
/// cannot be used here: Cargo only defines `CARGO_BIN_EXE_<name>` for a
/// *package's own* `[[bin]]` targets when compiling that same package's
/// integration tests/benches — verified empirically: adding
/// `open-pubusb = { path = "../../crates/open-pubusb" }` under `[dev-dependencies]` in
/// `Cargo.toml` makes Cargo print `ignoring invalid dependency \`open-pubusb\`
/// which is missing a lib target` (because `open-pubusb` has no `[lib]`, only
/// `[[bin]]`) and it still leaves `CARGO_BIN_EXE_open-pubusb` undefined at compile
/// time, so `env!(...)` fails with `environment variable \`CARGO_BIN_EXE_open-pubusb\`
/// not defined at compile time` even inside a real `tests/*.rs` integration
/// test binary.
///
/// Instead, resolve it at runtime relative to this test binary's own path:
/// `current_exe()` for a test binary is `target/<profile>/deps/<name>-<hash>`
/// (or occasionally directly under `target/<profile>/`); every workspace
/// member's build artifacts, including `open-pubusb`'s binary, are placed in that
/// same `target/<profile>/` directory. This is the standard technique for
/// invoking one workspace package's binary from another package's tests.
pub fn open_pubusb_bin_path() -> PathBuf {
    let mut path = std::env::current_exe().expect("failed to resolve current_exe()");
    path.pop(); // drop this test binary's own file name
    if path.ends_with("deps") {
        path.pop();
    }
    path.push(if cfg!(windows) {
        "open-pubusb.exe"
    } else {
        "open-pubusb"
    });
    path
}
