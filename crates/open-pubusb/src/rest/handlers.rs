//! Handlers for the REST (HTTP/JSON) subset of the API.
//!
//! Response bodies reuse the gRPC layer's `topic_to_proto`/
//! `subscription_to_proto` converters and the generated `pb::*` types'
//! `pbjson`-derived `Serialize` impls, so the wire format (camelCase,
//! base64 `bytes`, RFC 3339 `Timestamp`, `"600s"` `Duration`) matches the
//! contract exactly without hand-rolling it. Request bodies, which the
//! contract only specifies loosely (`{"messages":[...]}`,
//! `{"topic":"..."}`, ...), use small local structs instead of the full
//! generated request types (whose shapes don't match what a REST caller
//! actually sends — e.g. `PublishRequest` also carries `topic`, which here
//! comes from the URL path instead).

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::Json;
use open_pubusb_core::service::PubSubService;
use open_pubusb_core::store::kv::KvStore;
use serde::{Deserialize, Serialize};

use crate::grpc::publisher::topic_to_proto;
use crate::grpc::subscriber::subscription_to_proto;
use crate::rest::error::RestError;

fn topic_name(project: &str, topic: &str) -> String {
    format!("projects/{project}/topics/{topic}")
}

fn sub_name(project: &str, sub: &str) -> String {
    format!("projects/{project}/subscriptions/{sub}")
}

/// axum 0.8's router does not allow a path parameter followed by literal
/// text in the same segment (`{topic}:publish` is rejected at
/// route-registration time: "Only one parameter is allowed per path
/// segment" — verified in `rest::router::tests::router_builds_without_panicking`,
/// plan.md D5). So every `POST .../{resource}:verb` endpoint is registered
/// on the *plain* `{resource}` path pattern, and the handler splits the
/// captured segment on `:` itself. Returns `(resource_id, None)` when
/// there's no `:`, `(resource_id, Some(verb))` when there is.
fn split_verb(raw: &str) -> (&str, Option<&str>) {
    match raw.split_once(':') {
        Some((id, verb)) => (id, Some(verb)),
        None => (raw, None),
    }
}

/// Parses a POST body as JSON, treating an empty body as `null` (so a
/// bodyless request, e.g. `curl -X POST .../:pull` with no `-d`, still
/// reaches the handler and gets `PullBody`'s all-`#[serde(default)]`
/// fields rather than a 400). A genuinely malformed non-empty body is
/// still an error.
fn parse_body_lenient(bytes: &[u8]) -> Result<serde_json::Value, RestError> {
    if bytes.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    serde_json::from_slice(bytes).map_err(|e| {
        RestError::Domain(open_pubusb_core::Error::InvalidArgument {
            field: "body".to_string(),
            message: format!("invalid JSON: {e}"),
        })
    })
}

// -- Topics --------------------------------------------------------------

#[derive(Deserialize, Default)]
pub struct CreateTopicBody {
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

pub async fn create_topic<K: KvStore + 'static>(
    State(svc): State<Arc<PubSubService<K>>>,
    Path((project, topic)): Path<(String, String)>,
    body: Option<Json<CreateTopicBody>>,
) -> Result<impl IntoResponse, RestError> {
    let labels = body.map(|Json(b)| b.labels).unwrap_or_default();
    let record = svc.create_topic(&topic_name(&project, &topic), labels)?;
    Ok(Json(topic_to_proto(&record)))
}

pub async fn get_topic<K: KvStore + 'static>(
    State(svc): State<Arc<PubSubService<K>>>,
    Path((project, topic)): Path<(String, String)>,
) -> Result<impl IntoResponse, RestError> {
    let record = svc.get_topic(&topic_name(&project, &topic))?;
    Ok(Json(topic_to_proto(&record)))
}

#[derive(Serialize)]
pub struct ListTopicsResponseBody {
    pub topics: Vec<open_pubusb_proto::pubsub::v1::Topic>,
    #[serde(rename = "nextPageToken", skip_serializing_if = "String::is_empty")]
    pub next_page_token: String,
}

pub async fn list_topics<K: KvStore + 'static>(
    State(svc): State<Arc<PubSubService<K>>>,
    Path(project): Path<String>,
) -> Result<impl IntoResponse, RestError> {
    let (records, next) = svc.list_topics(&project, 100, None)?;
    Ok(Json(ListTopicsResponseBody {
        topics: records.iter().map(topic_to_proto).collect(),
        next_page_token: next.unwrap_or_default(),
    }))
}

pub async fn delete_topic<K: KvStore + 'static>(
    State(svc): State<Arc<PubSubService<K>>>,
    Path((project, topic)): Path<(String, String)>,
) -> Result<impl IntoResponse, RestError> {
    svc.delete_topic(&topic_name(&project, &topic))?;
    Ok(Json(serde_json::json!({})))
}

#[derive(Deserialize)]
pub struct PublishBody {
    pub messages: Vec<PublishMessageBody>,
}

#[derive(Deserialize)]
pub struct PublishMessageBody {
    #[serde(default)]
    pub data: String,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
    #[serde(default, rename = "orderingKey")]
    pub ordering_key: String,
}

#[derive(Serialize)]
pub struct PublishResponseBody {
    #[serde(rename = "messageIds")]
    pub message_ids: Vec<String>,
}

async fn publish<K: KvStore + 'static>(
    svc: &PubSubService<K>,
    full_name: &str,
    body: serde_json::Value,
) -> Result<impl IntoResponse, RestError> {
    let body: PublishBody = serde_json::from_value(body).map_err(|e| {
        RestError::Domain(open_pubusb_core::Error::InvalidArgument {
            field: "body".to_string(),
            message: e.to_string(),
        })
    })?;
    use base64::Engine as _;
    let mut messages = Vec::with_capacity(body.messages.len());
    for m in body.messages {
        let data = base64::engine::general_purpose::STANDARD
            .decode(&m.data)
            .map_err(|e| {
                RestError::Domain(open_pubusb_core::Error::InvalidArgument {
                    field: "data".to_string(),
                    message: format!("invalid base64: {e}"),
                })
            })?;
        messages.push(open_pubusb_core::service::PublishMessage {
            data,
            attributes: m.attributes,
            ordering_key: m.ordering_key,
        });
    }
    let message_ids = svc.publish(full_name, messages)?;
    Ok(Json(PublishResponseBody { message_ids }))
}

/// Dispatches `POST /v1/projects/{p}/topics/{topic}:verb` (only
/// `:publish` exists in this server's REST subset — anything else is a
/// REST-layer 501, same as `rest::fallback`).
pub async fn topic_post<K: KvStore + 'static>(
    State(svc): State<Arc<PubSubService<K>>>,
    Path((project, topic_and_verb)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> Result<axum::response::Response, RestError> {
    let (topic, verb) = split_verb(&topic_and_verb);
    let full_name = topic_name(&project, topic);
    // Dispatch on the verb *before* touching the body: an unrecognized
    // verb must 501 even with no/invalid body (e.g. a bare `POST .../t:seek`
    // with no Content-Type) rather than 415/400ing on the body first —
    // confirmed live (not just `cargo check`) via manual smoke testing,
    // per plan.md's "not done until the behavior is proven" rule.
    match verb {
        Some("publish") => {
            let json = parse_body_lenient(&body)?;
            Ok(publish(&svc, &full_name, json).await?.into_response())
        }
        _ => Err(RestError::Unimplemented {
            message: format!("no such method on this resource: {topic_and_verb:?}"),
        }),
    }
}

// -- Subscriptions ---------------------------------------------------------

#[derive(Deserialize, Default)]
pub struct CreateSubscriptionBody {
    pub topic: String,
    #[serde(default, rename = "ackDeadlineSeconds")]
    pub ack_deadline_seconds: Option<i32>,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

pub async fn create_subscription<K: KvStore + 'static>(
    State(svc): State<Arc<PubSubService<K>>>,
    Path((project, sub)): Path<(String, String)>,
    Json(body): Json<CreateSubscriptionBody>,
) -> Result<impl IntoResponse, RestError> {
    let opts = open_pubusb_core::subscription::CreateSubscriptionOptions {
        ack_deadline_secs: body.ack_deadline_seconds,
        labels: body.labels,
        ..Default::default()
    };
    let record = svc.create_subscription(&sub_name(&project, &sub), &body.topic, opts)?;
    Ok(Json(subscription_to_proto(&record)))
}

pub async fn get_subscription<K: KvStore + 'static>(
    State(svc): State<Arc<PubSubService<K>>>,
    Path((project, sub)): Path<(String, String)>,
) -> Result<impl IntoResponse, RestError> {
    let record = svc.get_subscription(&sub_name(&project, &sub))?;
    Ok(Json(subscription_to_proto(&record)))
}

#[derive(Serialize)]
pub struct ListSubscriptionsResponseBody {
    pub subscriptions: Vec<open_pubusb_proto::pubsub::v1::Subscription>,
    #[serde(rename = "nextPageToken", skip_serializing_if = "String::is_empty")]
    pub next_page_token: String,
}

pub async fn list_subscriptions<K: KvStore + 'static>(
    State(svc): State<Arc<PubSubService<K>>>,
    Path(project): Path<String>,
) -> Result<impl IntoResponse, RestError> {
    let (records, next) = svc.list_subscriptions(&project, 100, None)?;
    Ok(Json(ListSubscriptionsResponseBody {
        subscriptions: records.iter().map(subscription_to_proto).collect(),
        next_page_token: next.unwrap_or_default(),
    }))
}

pub async fn delete_subscription<K: KvStore + 'static>(
    State(svc): State<Arc<PubSubService<K>>>,
    Path((project, sub)): Path<(String, String)>,
) -> Result<impl IntoResponse, RestError> {
    svc.delete_subscription(&sub_name(&project, &sub))?;
    Ok(Json(serde_json::json!({})))
}

#[derive(Deserialize)]
pub struct PullBody {
    #[serde(default, rename = "maxMessages")]
    pub max_messages: i32,
}

#[derive(Serialize)]
pub struct PullResponseBody {
    #[serde(rename = "receivedMessages")]
    pub received_messages: Vec<open_pubusb_proto::pubsub::v1::ReceivedMessage>,
}

async fn pull<K: KvStore + 'static>(
    svc: &PubSubService<K>,
    full_name: &str,
    body: serde_json::Value,
) -> Result<impl IntoResponse, RestError> {
    let max_messages = serde_json::from_value::<PullBody>(body)
        .map(|b| b.max_messages)
        .unwrap_or(0)
        .max(1);
    let delivered = svc.pull(full_name, max_messages)?;
    let received_messages = delivered
        .into_iter()
        .map(|m| open_pubusb_proto::pubsub::v1::ReceivedMessage {
            ack_id: m.ack_id,
            message: Some(open_pubusb_proto::pubsub::v1::PubsubMessage {
                data: m.data,
                attributes: m.attributes,
                message_id: m.message_id,
                publish_time: Some(crate::grpc::convert::ms_to_timestamp(m.publish_time_ms)),
                ordering_key: m.ordering_key,
            }),
            delivery_attempt: m.delivery_attempt as i32,
        })
        .collect();
    Ok(Json(PullResponseBody { received_messages }))
}

#[derive(Deserialize)]
pub struct AckIdsBody {
    #[serde(default, rename = "ackIds")]
    pub ack_ids: Vec<String>,
}

async fn acknowledge<K: KvStore + 'static>(
    svc: &PubSubService<K>,
    full_name: &str,
    body: serde_json::Value,
) -> Result<impl IntoResponse, RestError> {
    let body: AckIdsBody = serde_json::from_value(body).unwrap_or(AckIdsBody {
        ack_ids: Vec::new(),
    });
    svc.acknowledge(full_name, body.ack_ids)?;
    Ok(Json(serde_json::json!({})))
}

#[derive(Deserialize)]
pub struct ModifyAckDeadlineBody {
    #[serde(default, rename = "ackIds")]
    pub ack_ids: Vec<String>,
    #[serde(default, rename = "ackDeadlineSeconds")]
    pub ack_deadline_seconds: i32,
}

async fn modify_ack_deadline<K: KvStore + 'static>(
    svc: &PubSubService<K>,
    full_name: &str,
    body: serde_json::Value,
) -> Result<impl IntoResponse, RestError> {
    let body: ModifyAckDeadlineBody = serde_json::from_value(body).map_err(|e| {
        RestError::Domain(open_pubusb_core::Error::InvalidArgument {
            field: "body".to_string(),
            message: e.to_string(),
        })
    })?;
    svc.modify_ack_deadline(full_name, body.ack_ids, body.ack_deadline_seconds)?;
    Ok(Json(serde_json::json!({})))
}

/// Dispatches every `POST /v1/projects/{p}/subscriptions/{sub}:verb`
/// request (see `router::router`'s doc comment for why they all share one
/// route registration). An unrecognized verb (or none) is a REST-layer
/// 501, same as `rest::fallback` — e.g. `:seek` (User Story 5, not yet
/// implemented over REST).
pub async fn subscription_post<K: KvStore + 'static>(
    State(svc): State<Arc<PubSubService<K>>>,
    Path((project, sub_and_verb)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> Result<axum::response::Response, RestError> {
    let (sub, verb) = split_verb(&sub_and_verb);
    let full_name = sub_name(&project, sub);
    // See `topic_post` for why verb dispatch happens before body parsing.
    match verb {
        Some("pull") => {
            let json = parse_body_lenient(&body)?;
            Ok(pull(&svc, &full_name, json).await?.into_response())
        }
        Some("acknowledge") => {
            let json = parse_body_lenient(&body)?;
            Ok(acknowledge(&svc, &full_name, json).await?.into_response())
        }
        Some("modifyAckDeadline") => {
            let json = parse_body_lenient(&body)?;
            Ok(modify_ack_deadline(&svc, &full_name, json)
                .await?
                .into_response())
        }
        _ => Err(RestError::Unimplemented {
            message: format!("no such method on this resource: {sub_and_verb:?}"),
        }),
    }
}
