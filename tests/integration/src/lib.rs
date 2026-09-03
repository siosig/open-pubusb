#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
//! In-process integration tests for `open-pubusb-core`, exercised directly
//! against domain types (no gRPC/REST layer).

pub mod target_api;
