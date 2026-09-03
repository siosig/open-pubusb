//! Generated Pub/Sub v1 protobuf/gRPC bindings and their proto3-JSON (pbjson) mappings.
//!
//! The module tree mirrors the proto package hierarchy (`google.*`) because the
//! tonic/prost-generated code references sibling packages via `super::super::...`.

// Everything below is emitted by prost/tonic/pbjson at build time. Generated
// code is not held to this crate's clippy configuration: new lints in newer
// toolchains (e.g. `redundant reference in write! argument` in the pbjson
// serde output) would otherwise break `cargo clippy -D warnings` without any
// change on our side.
#[allow(clippy::all)]
pub mod google {
    pub mod api {
        include!(concat!(env!("OUT_DIR"), "/google.api.rs"));
    }

    pub mod r#type {
        include!(concat!(env!("OUT_DIR"), "/google.r#type.rs"));
    }

    pub mod pubsub {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/google.pubsub.v1.rs"));
            include!(concat!(env!("OUT_DIR"), "/google.pubsub.v1.serde.rs"));
        }
    }

    pub mod iam {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/google.iam.v1.rs"));
        }
    }
}

pub use google::iam;
pub use google::pubsub;

pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/open_pubusb_descriptor.bin"));
