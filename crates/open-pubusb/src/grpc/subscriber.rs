//! `google.pubsub.v1.Subscriber` gRPC service implementation.
//!
//! Core scope: the 9 unary methods (Create/Get/Update/List/DeleteSubscription,
//! ModifyAckDeadline, Acknowledge, Pull, ModifyPushConfig). `StreamingPull`
//! (`crates/open-pubusb/src/grpc/streaming.rs`) and the Snapshot/Seek family
//! are implemented elsewhere in this file without having changed the 9
//! methods above.

use std::pin::Pin;
use std::sync::Arc;

use open_pubusb_core::service::PubSubService;
use open_pubusb_core::store::kv::KvStore;
use open_pubusb_core::subscription::{
    CreateSubscriptionOptions, SubscriptionRecord, SubscriptionUpdatePatch,
};
use open_pubusb_proto::pubsub::v1 as pb;
use tonic::{Request, Response, Status};

use crate::grpc::convert::{duration_to_secs, ms_to_timestamp, secs_to_duration, timestamp_to_ms};
use crate::grpc::status::to_status;

pub struct SubscriberService<K: KvStore> {
    svc: Arc<PubSubService<K>>,
    streaming_pull_max_lifetime_secs: u64,
    shutdown_token: tokio_util::sync::CancellationToken,
}

impl<K: KvStore> SubscriberService<K> {
    pub fn new(
        svc: Arc<PubSubService<K>>,
        streaming_pull_max_lifetime_secs: u64,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            svc,
            streaming_pull_max_lifetime_secs,
            shutdown_token,
        }
    }
}

/// Converts one [`open_pubusb_core::service::PulledMessage`] into a
/// `ReceivedMessage`, shared by the unary `Pull` handler and the
/// `StreamingPull` send loop (`crates/open-pubusb/src/grpc/streaming.rs`).
pub(crate) fn pulled_to_received_message(
    m: open_pubusb_core::service::PulledMessage,
) -> pb::ReceivedMessage {
    pb::ReceivedMessage {
        ack_id: m.ack_id,
        message: Some(pb::PubsubMessage {
            data: m.data,
            attributes: m.attributes,
            message_id: m.message_id,
            publish_time: Some(ms_to_timestamp(m.publish_time_ms)),
            ordering_key: m.ordering_key,
        }),
        delivery_attempt: m.delivery_attempt as i32,
    }
}

/// Converts a domain [`open_pubusb_core::subscription::PushConfig`] to the proto
/// shape, shared by `subscription_to_proto` and (indirectly, via its
/// inverse) every RPC that accepts a `PushConfig`.
fn push_config_to_proto(cfg: &open_pubusb_core::subscription::PushConfig) -> pb::PushConfig {
    pb::PushConfig {
        push_endpoint: cfg.endpoint.clone(),
        attributes: Default::default(),
        // OIDC tokens are accepted-and-warned, never persisted (see
        // `PushConfig`'s doc comment) — so there is nothing to echo back
        // here.
        authentication_method: None,
        wrapper: Some(if cfg.no_wrapper {
            pb::push_config::Wrapper::NoWrapper(pb::push_config::NoWrapper {
                write_metadata: cfg.write_metadata,
            })
        } else {
            pb::push_config::Wrapper::PubsubWrapper(pb::push_config::PubsubWrapper {})
        }),
    }
}

/// Converts a proto `PushConfig` to the domain shape. Logs a one-time-per-call
/// warning if `oidc_token` was set (accepted, not acted upon — see
/// [`open_pubusb_core::subscription::PushConfig`]'s doc comment).
fn push_config_from_proto(pc: pb::PushConfig) -> open_pubusb_core::subscription::PushConfig {
    if pc.authentication_method.is_some() {
        tracing::warn!(
            "PushConfig.oidc_token was set but this server has no OIDC-issuing backend; \
             accepting the request, but no Authorization header will actually be attached \
             to push requests"
        );
    }
    let (no_wrapper, write_metadata) = match pc.wrapper {
        Some(pb::push_config::Wrapper::NoWrapper(nw)) => (true, nw.write_metadata),
        _ => (false, false),
    };
    open_pubusb_core::subscription::PushConfig {
        endpoint: pc.push_endpoint,
        no_wrapper,
        write_metadata,
    }
}

pub(crate) fn subscription_to_proto(record: &SubscriptionRecord) -> pb::Subscription {
    pb::Subscription {
        name: record.name.clone(),
        topic: record.topic.clone(),
        push_config: record.push_config.as_ref().map(push_config_to_proto),
        bigquery_config: None,
        cloud_storage_config: None,
        bigtable_config: None,
        ack_deadline_seconds: record.ack_deadline_secs,
        retain_acked_messages: record.retain_acked_messages,
        message_retention_duration: Some(secs_to_duration(record.message_retention_secs)),
        labels: record.labels.clone(),
        enable_message_ordering: record.enable_message_ordering,
        expiration_policy: Some(pb::ExpirationPolicy {
            ttl: record.expiration_ttl_secs.map(secs_to_duration),
        }),
        filter: record.filter.clone(),
        dead_letter_policy: record
            .dead_letter_topic
            .clone()
            .map(|topic| pb::DeadLetterPolicy {
                dead_letter_topic: topic,
                max_delivery_attempts: record.max_delivery_attempts,
            }),
        retry_policy: Some(pb::RetryPolicy {
            minimum_backoff: Some(secs_to_duration(record.min_retry_backoff_secs)),
            maximum_backoff: Some(secs_to_duration(record.max_retry_backoff_secs)),
        }),
        detached: record.detached,
        enable_exactly_once_delivery: record.enable_exactly_once_delivery,
        topic_message_retention_duration: None,
        state: pb::subscription::State::Active as i32,
        analytics_hub_subscription_info: None,
        message_transforms: Vec::new(),
        tags: Default::default(),
    }
}

fn create_options_from_proto(sub: &pb::Subscription) -> CreateSubscriptionOptions {
    CreateSubscriptionOptions {
        ack_deadline_secs: (sub.ack_deadline_seconds != 0).then_some(sub.ack_deadline_seconds),
        retain_acked_messages: sub.retain_acked_messages,
        message_retention_secs: sub
            .message_retention_duration
            .as_ref()
            .map(duration_to_secs),
        labels: sub.labels.clone(),
        enable_message_ordering: sub.enable_message_ordering,
        expiration_ttl_secs: sub
            .expiration_policy
            .as_ref()
            .map(|p| p.ttl.as_ref().map(duration_to_secs)),
        filter: sub.filter.clone(),
        dead_letter_topic: sub
            .dead_letter_policy
            .as_ref()
            .map(|p| p.dead_letter_topic.clone()),
        max_delivery_attempts: sub
            .dead_letter_policy
            .as_ref()
            .map(|p| p.max_delivery_attempts),
        min_retry_backoff_secs: sub
            .retry_policy
            .as_ref()
            .and_then(|p| p.minimum_backoff.as_ref())
            .map(duration_to_secs),
        max_retry_backoff_secs: sub
            .retry_policy
            .as_ref()
            .and_then(|p| p.maximum_backoff.as_ref())
            .map(duration_to_secs),
        enable_exactly_once_delivery: sub.enable_exactly_once_delivery,
        push_config: sub
            .push_config
            .clone()
            .filter(|p| !p.push_endpoint.is_empty())
            .map(push_config_from_proto),
        has_bigquery_config: sub.bigquery_config.is_some(),
        has_cloud_storage_config: sub.cloud_storage_config.is_some(),
    }
}

const DEFAULT_PAGE_SIZE: usize = 100;

fn strip_projects_prefix(project: &str) -> &str {
    project.strip_prefix("projects/").unwrap_or(project)
}

fn snapshot_to_proto(
    record: &open_pubusb_core::delivery::snapshot::SnapshotRecord,
) -> pb::Snapshot {
    pb::Snapshot {
        name: record.name.clone(),
        topic: record.topic.clone(),
        expire_time: Some(ms_to_timestamp(record.expire_at_ms)),
        labels: record.labels.clone(),
    }
}

#[tonic::async_trait]
impl<K: KvStore + 'static> pb::subscriber_server::Subscriber for SubscriberService<K> {
    async fn create_subscription(
        &self,
        request: Request<pb::Subscription>,
    ) -> Result<Response<pb::Subscription>, Status> {
        let sub = request.into_inner();
        let opts = create_options_from_proto(&sub);
        let record = self
            .svc
            .create_subscription(&sub.name, &sub.topic, opts)
            .map_err(to_status)?;
        Ok(Response::new(subscription_to_proto(&record)))
    }

    async fn get_subscription(
        &self,
        request: Request<pb::GetSubscriptionRequest>,
    ) -> Result<Response<pb::Subscription>, Status> {
        let req = request.into_inner();
        let record = self
            .svc
            .get_subscription(&req.subscription)
            .map_err(to_status)?;
        Ok(Response::new(subscription_to_proto(&record)))
    }

    async fn update_subscription(
        &self,
        request: Request<pb::UpdateSubscriptionRequest>,
    ) -> Result<Response<pb::Subscription>, Status> {
        let req = request.into_inner();
        let sub = req
            .subscription
            .ok_or_else(|| Status::invalid_argument("subscription is required"))?;
        let paths: Vec<String> = req.update_mask.map(|m| m.paths).unwrap_or_default();

        for immutable in ["name", "topic", "enable_message_ordering", "filter"] {
            if paths.iter().any(|p| p == immutable) {
                return Err(to_status(open_pubusb_core::Error::InvalidArgument {
                    field: immutable.to_string(),
                    message: format!("{immutable} is immutable after creation"),
                }));
            }
        }

        let mut patch = SubscriptionUpdatePatch::default();
        if paths.iter().any(|p| p == "ack_deadline_seconds") {
            patch.ack_deadline_secs = Some(sub.ack_deadline_seconds);
        }
        if paths.iter().any(|p| p == "retain_acked_messages") {
            patch.retain_acked_messages = Some(sub.retain_acked_messages);
        }
        if paths.iter().any(|p| p == "message_retention_duration") {
            patch.message_retention_secs = sub
                .message_retention_duration
                .as_ref()
                .map(duration_to_secs);
        }
        if paths.iter().any(|p| p == "labels") {
            patch.labels = Some(sub.labels.clone());
        }
        if paths.iter().any(|p| p == "expiration_policy") {
            patch.expiration_ttl_secs = Some(
                sub.expiration_policy
                    .as_ref()
                    .and_then(|p| p.ttl.as_ref())
                    .map(duration_to_secs),
            );
        }
        if paths.iter().any(|p| p == "dead_letter_policy") {
            patch.dead_letter_topic = Some(
                sub.dead_letter_policy
                    .as_ref()
                    .map(|p| p.dead_letter_topic.clone()),
            );
            if let Some(dlp) = &sub.dead_letter_policy {
                patch.max_delivery_attempts = Some(dlp.max_delivery_attempts);
            }
        }
        if paths.iter().any(|p| p == "retry_policy") {
            if let Some(rp) = &sub.retry_policy {
                patch.min_retry_backoff_secs = rp.minimum_backoff.as_ref().map(duration_to_secs);
                patch.max_retry_backoff_secs = rp.maximum_backoff.as_ref().map(duration_to_secs);
            }
        }
        if paths.iter().any(|p| p == "detached") {
            patch.detached = Some(sub.detached);
        }
        if paths.iter().any(|p| p == "enable_exactly_once_delivery") {
            patch.enable_exactly_once_delivery = Some(sub.enable_exactly_once_delivery);
        }
        if paths.iter().any(|p| p == "push_config") {
            patch.push_config = Some(
                sub.push_config
                    .clone()
                    .filter(|p| !p.push_endpoint.is_empty())
                    .map(push_config_from_proto),
            );
        }

        let record = self
            .svc
            .update_subscription(&sub.name, patch)
            .map_err(to_status)?;
        Ok(Response::new(subscription_to_proto(&record)))
    }

    async fn list_subscriptions(
        &self,
        request: Request<pb::ListSubscriptionsRequest>,
    ) -> Result<Response<pb::ListSubscriptionsResponse>, Status> {
        let req = request.into_inner();
        let page_size = if req.page_size > 0 {
            req.page_size as usize
        } else {
            DEFAULT_PAGE_SIZE
        };
        let page_token = (!req.page_token.is_empty()).then_some(req.page_token.as_str());
        let (records, next) = self
            .svc
            .list_subscriptions(strip_projects_prefix(&req.project), page_size, page_token)
            .map_err(to_status)?;
        Ok(Response::new(pb::ListSubscriptionsResponse {
            subscriptions: records.iter().map(subscription_to_proto).collect(),
            next_page_token: next.unwrap_or_default(),
        }))
    }

    async fn delete_subscription(
        &self,
        request: Request<pb::DeleteSubscriptionRequest>,
    ) -> Result<Response<pbjson_types::Empty>, Status> {
        let req = request.into_inner();
        self.svc
            .delete_subscription(&req.subscription)
            .map_err(to_status)?;
        Ok(Response::new(pbjson_types::Empty {}))
    }

    async fn modify_ack_deadline(
        &self,
        request: Request<pb::ModifyAckDeadlineRequest>,
    ) -> Result<Response<pbjson_types::Empty>, Status> {
        let req = request.into_inner();
        self.svc
            .modify_ack_deadline(&req.subscription, req.ack_ids, req.ack_deadline_seconds)
            .map_err(to_status)?;
        Ok(Response::new(pbjson_types::Empty {}))
    }

    async fn acknowledge(
        &self,
        request: Request<pb::AcknowledgeRequest>,
    ) -> Result<Response<pbjson_types::Empty>, Status> {
        let req = request.into_inner();
        self.svc
            .acknowledge(&req.subscription, req.ack_ids)
            .map_err(to_status)?;
        Ok(Response::new(pbjson_types::Empty {}))
    }

    async fn pull(
        &self,
        request: Request<pb::PullRequest>,
    ) -> Result<Response<pb::PullResponse>, Status> {
        let req = request.into_inner();
        let max_messages = if req.max_messages > 0 {
            req.max_messages
        } else {
            1
        };

        let mut delivered = self
            .svc
            .pull(&req.subscription, max_messages)
            .map_err(to_status)?;
        if delivered.is_empty() {
            // Honor a bounded wait so clients that expect Pull to block a
            // little rather than busy-poll get *some* relief; the actual
            // `pull_max_wait_secs` ceiling and `grpc-timeout` handling is
            // refined by the server wiring elsewhere, this is a minimal
            // single-retry wait sufficient for an "empty pull waits briefly"
            // behavior without a full retry loop yet.
            if let Ok(waiter) = self.svc.pull_waiter(&req.subscription) {
                let _ =
                    tokio::time::timeout(std::time::Duration::from_millis(200), waiter.notified())
                        .await;
                delivered = self
                    .svc
                    .pull(&req.subscription, max_messages)
                    .map_err(to_status)?;
            }
        }

        let received_messages = delivered
            .into_iter()
            .map(pulled_to_received_message)
            .collect();
        Ok(Response::new(pb::PullResponse { received_messages }))
    }

    type StreamingPullStream = Pin<
        Box<
            dyn tonic::codegen::tokio_stream::Stream<
                    Item = Result<pb::StreamingPullResponse, Status>,
                > + Send
                + 'static,
        >,
    >;

    async fn streaming_pull(
        &self,
        request: Request<tonic::Streaming<pb::StreamingPullRequest>>,
    ) -> Result<Response<Self::StreamingPullStream>, Status> {
        crate::grpc::streaming::streaming_pull(
            self.svc.clone(),
            self.streaming_pull_max_lifetime_secs,
            self.shutdown_token.clone(),
            request.into_inner(),
        )
        .await
    }

    async fn modify_push_config(
        &self,
        request: Request<pb::ModifyPushConfigRequest>,
    ) -> Result<Response<pbjson_types::Empty>, Status> {
        let req = request.into_inner();
        let push_config = req
            .push_config
            .filter(|c| !c.push_endpoint.is_empty())
            .map(push_config_from_proto);
        self.svc
            .modify_push_config(&req.subscription, push_config)
            .map_err(to_status)?;
        Ok(Response::new(pbjson_types::Empty {}))
    }

    async fn get_snapshot(
        &self,
        request: Request<pb::GetSnapshotRequest>,
    ) -> Result<Response<pb::Snapshot>, Status> {
        let req = request.into_inner();
        let record = self.svc.get_snapshot(&req.snapshot).map_err(to_status)?;
        Ok(Response::new(snapshot_to_proto(&record)))
    }

    async fn list_snapshots(
        &self,
        request: Request<pb::ListSnapshotsRequest>,
    ) -> Result<Response<pb::ListSnapshotsResponse>, Status> {
        let req = request.into_inner();
        let page_size = if req.page_size > 0 {
            req.page_size as usize
        } else {
            DEFAULT_PAGE_SIZE
        };
        let page_token = (!req.page_token.is_empty()).then_some(req.page_token.as_str());
        let (records, next) = self
            .svc
            .list_snapshots(strip_projects_prefix(&req.project), page_size, page_token)
            .map_err(to_status)?;
        Ok(Response::new(pb::ListSnapshotsResponse {
            snapshots: records.iter().map(snapshot_to_proto).collect(),
            next_page_token: next.unwrap_or_default(),
        }))
    }

    async fn create_snapshot(
        &self,
        request: Request<pb::CreateSnapshotRequest>,
    ) -> Result<Response<pb::Snapshot>, Status> {
        let req = request.into_inner();
        let record = self
            .svc
            .create_snapshot(&req.name, &req.subscription, req.labels)
            .map_err(to_status)?;
        Ok(Response::new(snapshot_to_proto(&record)))
    }

    async fn update_snapshot(
        &self,
        request: Request<pb::UpdateSnapshotRequest>,
    ) -> Result<Response<pb::Snapshot>, Status> {
        let req = request.into_inner();
        let snapshot = req
            .snapshot
            .ok_or_else(|| Status::invalid_argument("snapshot is required"))?;
        let paths: Vec<String> = req.update_mask.map(|m| m.paths).unwrap_or_default();
        if paths.iter().any(|p| p == "name" || p == "topic") {
            return Err(to_status(open_pubusb_core::Error::InvalidArgument {
                field: "name/topic".to_string(),
                message: "name and topic are immutable after creation".to_string(),
            }));
        }
        // `labels` is the only mutable field a `Snapshot` has — apply it
        // whenever named (or, per the common "no mask means everything"
        // convention other handlers in this file don't special-case
        // because their patches are all `Option`-gated, applied here
        // explicitly since `update_snapshot_labels` always overwrites).
        let record = if paths.is_empty() || paths.iter().any(|p| p == "labels") {
            self.svc
                .update_snapshot_labels(&snapshot.name, snapshot.labels)
                .map_err(to_status)?
        } else {
            self.svc.get_snapshot(&snapshot.name).map_err(to_status)?
        };
        Ok(Response::new(snapshot_to_proto(&record)))
    }

    async fn delete_snapshot(
        &self,
        request: Request<pb::DeleteSnapshotRequest>,
    ) -> Result<Response<pbjson_types::Empty>, Status> {
        let req = request.into_inner();
        self.svc.delete_snapshot(&req.snapshot).map_err(to_status)?;
        Ok(Response::new(pbjson_types::Empty {}))
    }

    async fn seek(
        &self,
        request: Request<pb::SeekRequest>,
    ) -> Result<Response<pb::SeekResponse>, Status> {
        let req = request.into_inner();
        match req.target {
            Some(pb::seek_request::Target::Time(ts)) => {
                self.svc
                    .seek_to_time(&req.subscription, timestamp_to_ms(&ts))
                    .map_err(to_status)?;
            }
            Some(pb::seek_request::Target::Snapshot(snapshot)) => {
                self.svc
                    .seek_to_snapshot(&req.subscription, &snapshot)
                    .map_err(to_status)?;
            }
            None => {
                return Err(Status::invalid_argument(
                    "seek requires either time or snapshot",
                ))
            }
        }
        Ok(Response::new(pb::SeekResponse {}))
    }
}
