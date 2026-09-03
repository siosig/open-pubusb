//! Assembles every gRPC service this server exposes into one
//! [`tonic::service::Routes`], for `crates/open-pubusb/src/server.rs` to
//! merge with the REST router and serve on one port.

pub mod convert;
pub mod iam;
pub mod publisher;
pub mod schema;
pub mod status;
pub mod streaming;
pub mod subscriber;

use std::sync::Arc;

use open_pubusb_core::service::PubSubService;
use open_pubusb_core::store::kv::KvStore;
use open_pubusb_proto::iam::v1::iam_policy_server::IamPolicyServer;
use open_pubusb_proto::pubsub::v1::publisher_server::PublisherServer;
use open_pubusb_proto::pubsub::v1::schema_service_server::SchemaServiceServer;
use open_pubusb_proto::pubsub::v1::subscriber_server::SubscriberServer;
use tokio_util::sync::CancellationToken;

/// The health reporter handle, kept by the caller so it can flip services
/// to `SERVING` once startup finishes and back to `NOT_SERVING` while
/// draining on shutdown.
pub type HealthReporter = tonic_health::server::HealthReporter;

/// Builds the full set of gRPC routes and the health reporter that
/// controls them. Every `google.pubsub.v1` and `google.iam.v1` service
/// starts registered but reporting `NOT_SERVING` — the caller flips
/// Publisher/Subscriber to `SERVING` once the store has finished opening
/// (`crates/open-pubusb/src/main.rs`). `SchemaService` and `IAMPolicy` are
/// out of scope entirely and are left `NOT_SERVING` forever (their RPCs
/// still individually return `UNIMPLEMENTED` — health status and RPC status
/// are reported independently here, which is standard grpc.health.v1
/// practice for a deliberately-unimplemented service).
pub fn build_routes<K: KvStore + 'static>(
    svc: Arc<PubSubService<K>>,
    enable_reflection: bool,
    descriptor_set: &'static [u8],
    streaming_pull_max_lifetime_secs: u64,
    shutdown_token: CancellationToken,
) -> (tonic::service::Routes, HealthReporter) {
    let (health_reporter, health_service) = tonic_health::server::health_reporter();

    let mut builder = tonic::service::Routes::builder();
    builder
        .add_service(PublisherServer::new(publisher::PublisherService::new(
            svc.clone(),
        )))
        .add_service(SubscriberServer::new(subscriber::SubscriberService::new(
            svc,
            streaming_pull_max_lifetime_secs,
            shutdown_token,
        )))
        .add_service(SchemaServiceServer::new(schema::SchemaServiceImpl))
        .add_service(IamPolicyServer::new(iam::IamPolicyImpl))
        .add_service(health_service);

    if enable_reflection {
        if let Ok(reflection_service) = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(descriptor_set)
            .build_v1()
        {
            builder.add_service(reflection_service);
        }
    }

    (builder.routes(), health_reporter)
}
