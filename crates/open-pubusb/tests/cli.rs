//! Smoke test for the compiled `open-pubusb` executable.
//!
//! Besides checking `open-pubusb version`, this file has a structural job:
//! Cargo only builds a package's `[[bin]]` targets during `cargo test` /
//! `cargo nextest run` when the package has at least one integration test
//! (that is what makes `CARGO_BIN_EXE_<name>` available). The contract tests
//! in `tests/contract` are a separate package that locates the binary in the
//! workspace `target/` directory at runtime, so this test is what guarantees
//! the binary exists before they run on a clean checkout.

use std::process::Command;

#[test]
fn version_subcommand_prints_the_crate_version() {
    let output = match Command::new(env!("CARGO_BIN_EXE_open-pubusb"))
        .arg("version")
        .output()
    {
        Ok(output) => output,
        Err(e) => panic!("failed to run `open-pubusb version`: {e}"),
    };
    assert!(output.status.success(), "exit status: {:?}", output.status);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(env!("CARGO_PKG_VERSION")),
        "stdout does not mention the crate version {}: {stdout:?}",
        env!("CARGO_PKG_VERSION")
    );
}
