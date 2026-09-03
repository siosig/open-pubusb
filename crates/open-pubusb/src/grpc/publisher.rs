//! `google.pubsub.v1.Publisher` gRPC service implementation. Maps proto
//! request/response types to and from [`open_pubusb_core::service::PubSubService`]
//! calls; see `crates/open-pubusb/src/grpc/convert.rs` for `Timestamp`/`Duration`
//! helpers and `crates/open-pubusb/src/grpc/status.rs` for the error mapping.

use std::sync::Arc;

use open_pubusb_core::service::{PubSubService, PublishMessage};
use open_pubusb_core::store::kv::KvStore;
use open_pubusb_proto::pubsub::v1 as pb;
use tonic::{Request, Response, Status};

use crate::grpc::convert::{duration_to_secs, secs_to_duration};
use crate::grpc::status::to_status;

pub struct PublisherService<K: KvStore> {
    svc: Arc<PubSubService<K>>,
}

impl<K: KvStore> PublisherService<K> {
    pub fn new(svc: Arc<PubSubService<K>>) -> Self {
        Self { svc }
    }
}

pub(crate) fn topic_to_proto(record: &open_pubusb_core::topic::TopicRecord) -> pb::Topic {
    pb::Topic {
        name: record.name.clone(),
        labels: record.labels.clone(),
        message_storage_policy: None,
        kms_key_name: String::new(),
        schema_settings: None,
        satisfies_pzs: false,
        message_retention_duration: if record.message_retention_secs > 0 {
            Some(secs_to_duration(record.message_retention_secs))
        } else {
            None
        },
        state: pb::topic::State::Active as i32,
        ingestion_data_source_settings: None,
        message_transforms: Vec::new(),
        tags: Default::default(),
    }
}

/// `projects/{id}` -> `{id}`, per every List* request's `project` field
/// format.
fn strip_projects_prefix(project: &str) -> &str {
    project.strip_prefix("projects/").unwrap_or(project)
}

const DEFAULT_PAGE_SIZE: usize = 100;

#[tonic::async_trait]
impl<K: KvStore + 'static> pb::publisher_server::Publisher for PublisherService<K> {
    async fn create_topic(
        &self,
        request: Request<pb::Topic>,
    ) -> Result<Response<pb::Topic>, Status> {
        let topic = request.into_inner();
        let record = self
            .svc
            .create_topic_full(
                &topic.name,
                topic.labels,
                topic
                    .message_retention_duration
                    .as_ref()
                    .map(duration_to_secs),
                (!topic.kms_key_name.is_empty()).then_some(topic.kms_key_name),
                topic.schema_settings.is_some(),
                topic.ingestion_data_source_settings.is_some(),
            )
            .map_err(to_status)?;
        Ok(Response::new(topic_to_proto(&record)))
    }

    async fn update_topic(
        &self,
        request: Request<pb::UpdateTopicRequest>,
    ) -> Result<Response<pb::Topic>, Status> {
        let req = request.into_inner();
        let topic = req
            .topic
            .ok_or_else(|| Status::invalid_argument("topic is required"))?;
        let paths: Vec<String> = req.update_mask.map(|m| m.paths).unwrap_or_default();

        let labels = paths
            .iter()
            .any(|p| p == "labels")
            .then(|| topic.labels.clone());
        let retention = paths
            .iter()
            .any(|p| p == "message_retention_duration")
            .then(|| {
                topic
                    .message_retention_duration
                    .as_ref()
                    .map(duration_to_secs)
            });
        let kms = paths
            .iter()
            .any(|p| p == "kms_key_name")
            .then(|| (!topic.kms_key_name.is_empty()).then_some(topic.kms_key_name.clone()));

        let record = self
            .svc
            .update_topic_full(&topic.name, labels, retention, kms)
            .map_err(to_status)?;
        Ok(Response::new(topic_to_proto(&record)))
    }

    async fn publish(
        &self,
        request: Request<pb::PublishRequest>,
    ) -> Result<Response<pb::PublishResponse>, Status> {
        let req = request.into_inner();
        let messages = req
            .messages
            .into_iter()
            .map(|m| PublishMessage {
                data: m.data,
                attributes: m.attributes,
                ordering_key: m.ordering_key,
            })
            .collect();
        let message_ids = self.svc.publish(&req.topic, messages).map_err(to_status)?;
        Ok(Response::new(pb::PublishResponse { message_ids }))
    }

    async fn get_topic(
        &self,
        request: Request<pb::GetTopicRequest>,
    ) -> Result<Response<pb::Topic>, Status> {
        let req = request.into_inner();
        let record = self.svc.get_topic(&req.topic).map_err(to_status)?;
        Ok(Response::new(topic_to_proto(&record)))
    }

    async fn list_topics(
        &self,
        request: Request<pb::ListTopicsRequest>,
    ) -> Result<Response<pb::ListTopicsResponse>, Status> {
        let req = request.into_inner();
        let page_size = if req.page_size > 0 {
            req.page_size as usize
        } else {
            DEFAULT_PAGE_SIZE
        };
        let page_token = (!req.page_token.is_empty()).then_some(req.page_token.as_str());
        let (records, next) = self
            .svc
            .list_topics(strip_projects_prefix(&req.project), page_size, page_token)
            .map_err(to_status)?;
        Ok(Response::new(pb::ListTopicsResponse {
            topics: records.iter().map(topic_to_proto).collect(),
            next_page_token: next.unwrap_or_default(),
        }))
    }

    async fn list_topic_subscriptions(
        &self,
        request: Request<pb::ListTopicSubscriptionsRequest>,
    ) -> Result<Response<pb::ListTopicSubscriptionsResponse>, Status> {
        let req = request.into_inner();
        let subscriptions = self
            .svc
            .list_topic_subscriptions(&req.topic)
            .map_err(to_status)?;
        Ok(Response::new(pb::ListTopicSubscriptionsResponse {
            subscriptions,
            next_page_token: String::new(),
        }))
    }

    async fn list_topic_snapshots(
        &self,
        request: Request<pb::ListTopicSnapshotsRequest>,
    ) -> Result<Response<pb::ListTopicSnapshotsResponse>, Status> {
        let req = request.into_inner();
        self.svc.get_topic(&req.topic).map_err(to_status)?;
        let snapshots = self
            .svc
            .list_topic_snapshots(&req.topic)
            .map_err(to_status)?;
        Ok(Response::new(pb::ListTopicSnapshotsResponse {
            snapshots,
            next_page_token: String::new(),
        }))
    }

    async fn delete_topic(
        &self,
        request: Request<pb::DeleteTopicRequest>,
    ) -> Result<Response<pbjson_types::Empty>, Status> {
        let req = request.into_inner();
        self.svc.delete_topic(&req.topic).map_err(to_status)?;
        Ok(Response::new(pbjson_types::Empty {}))
    }

    async fn detach_subscription(
        &self,
        request: Request<pb::DetachSubscriptionRequest>,
    ) -> Result<Response<pb::DetachSubscriptionResponse>, Status> {
        let req = request.into_inner();
        self.svc
            .detach_subscription(&req.subscription)
            .map_err(to_status)?;
        Ok(Response::new(pb::DetachSubscriptionResponse {}))
    }
}
