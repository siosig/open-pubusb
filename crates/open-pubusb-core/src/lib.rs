#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
#![warn(missing_docs)]
//! Core domain logic for open-pubusb (storage, delivery engine, filters), independent of the transport layer.

pub mod clock;
pub mod delivery;
pub mod error;
pub mod filter;
pub mod limits;
pub mod metrics;
pub mod names;
pub mod push;
pub mod service;
pub mod store;
pub mod subscription;
pub mod topic;

pub use error::{Error, Result};
