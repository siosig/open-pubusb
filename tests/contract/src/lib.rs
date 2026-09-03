// This whole crate is test infrastructure (`publish = false`, only ever
// used from `tests/*.rs` binaries), so unlike a normal library crate we
// don't gate this behind `cfg(test)` — the lib target itself is compiled
// in non-test mode when linked into an integration-test binary, and
// panicking on a genuinely-broken test harness (bad port, spawn failure)
// is the correct behavior, matching this project's "tests and test helpers"
// exception for `unwrap`/`expect`.
#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Contract-test harness: spawns a real `open-pubusb serve --ephemeral` process and
//! exposes ready-to-use gRPC/REST clients to the tests in `tests/`.

pub mod harness;
