//! Contract test: the `open-pubusb` Docker image runs as a non-root user,
//! per `Dockerfile`'s `gcr.io/distroless/static-debian12:nonroot`
//! final stage and `USER nonroot` directive.
//!
//! Building the image (`docker build .`) takes minutes, so this is not run
//! as part of the default `cargo test` / CI `test` job — it looks for a
//! pre-built image (`OPEN_PUBUSB_DOCKER_IMAGE`, default `open-pubusb:local`) and skips
//! (not fails) when `docker` itself isn't available or that image hasn't
//! been built, matching this workspace's compat-suite convention (skip
//! rather than fail when the prerequisite environment isn't set up, since
//! running this against Docker is optional in CI). To run for real:
//!
//! ```bash
//! docker build -t open-pubusb:local .
//! cargo test -p open-pubusb-contract-tests --test docker_nonroot
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::process::Command;

fn docker_image() -> String {
    std::env::var("OPEN_PUBUSB_DOCKER_IMAGE").unwrap_or_else(|_| "open-pubusb:local".to_string())
}

fn docker_available_with_image(image: &str) -> bool {
    let Ok(which) = Command::new("docker").arg("--version").output() else {
        return false;
    };
    if !which.status.success() {
        return false;
    }
    Command::new("docker")
        .args(["image", "inspect", image])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// A unique container name per test run, so repeated/parallel invocations
/// never collide.
fn container_name(suffix: &str) -> String {
    format!("open-pubusb-contract-{suffix}-{}", std::process::id())
}

#[test]
fn container_process_runs_as_a_non_root_uid() {
    let image = docker_image();
    if !docker_available_with_image(&image) {
        eprintln!(
            "skipping: docker not available or image {image:?} not built \
             (build it with `docker build -t {image} .` to run this test)"
        );
        return;
    }

    let name = container_name("nonroot");
    // distroless has no shell, so `docker exec` can't run `id` inside the
    // container — inspect the *running* process's UID from the host side
    // instead (`docker top` reports the container's own process table with
    // host-visible UIDs, which is exactly "what user is this actually
    // running as").
    let run = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            &name,
            "-P",
            &image,
            "serve",
            "--ephemeral",
        ])
        .output()
        .expect("failed to run `docker run`");
    assert!(
        run.status.success(),
        "docker run failed: {}",
        String::from_utf8_lossy(&run.stderr)
    );

    let cleanup = |name: &str| {
        let _ = Command::new("docker").args(["kill", name]).output();
    };

    // No `-o` format spec: this docker version's `top` requires the
    // default column set (a custom `-o` selection can fail with "Couldn't
    // find PID field in ps output" even when the requested columns are
    // individually valid). The default output's first column is `UID`.
    let top = Command::new("docker").args(["top", &name]).output();
    let top = match top {
        Ok(o) if o.status.success() => o,
        Ok(o) => {
            cleanup(&name);
            panic!("docker top failed: {}", String::from_utf8_lossy(&o.stderr));
        }
        Err(e) => {
            cleanup(&name);
            panic!("failed to run `docker top`: {e}");
        }
    };
    cleanup(&name);

    let stdout = String::from_utf8_lossy(&top.stdout);
    let data_row = stdout
        .lines()
        .nth(1) // line 0 is the "UID PID PPID ..." header
        .unwrap_or_default();
    let user_line = data_row.split_whitespace().next().unwrap_or_default();
    assert!(!user_line.is_empty(), "docker top produced no process row");
    assert_ne!(
        user_line, "root",
        "open-pubusb container process must not run as root, `docker top` reported user {user_line:?}"
    );
    // distroless's `nonroot` user is uid 65532 by convention; assert the
    // numeric form too in case `docker top` reports a raw uid rather than
    // a resolved name (distroless has no /etc/passwd-driven name lookup on
    // the host side).
    assert_ne!(
        user_line, "0",
        "open-pubusb container process must not run as uid 0"
    );
}
