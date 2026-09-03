//! Push delivery envelope construction.
//!
//! Two wire formats, matching `google.pubsub.v1.PushConfig`'s `wrapper`
//! oneof:
//!
//! - **`PubsubWrapper`** (the default): a JSON body
//!   `{"message": {...}, "subscription": "..."}`, with the message using
//!   **both** camelCase and snake_case keys for every field
//!   (`messageId`+`message_id`, `publishTime`+`publish_time`,
//!   `orderingKey`+`ordering_key`) — real GCP Pub/Sub's push payload does
//!   this so receivers written against either convention work.
//! - **`NoWrapper`**: the raw message body as the HTTP body, with
//!   metadata carried in `x-goog-pubsub-*` headers instead (and, if
//!   `write_metadata` is set, each attribute as its own header).

use crate::service::PulledMessage;

/// What to send: a body plus whatever headers the format requires beyond
/// the caller's own (`Content-Type`, `Authorization`, ...).
pub struct PushRequestBody {
    /// The HTTP request body bytes.
    pub body: Vec<u8>,
    /// The `Content-Type` header value.
    pub content_type: &'static str,
    /// Additional headers beyond `Content-Type` (e.g. `x-goog-pubsub-*`
    /// metadata headers for `NoWrapper`).
    pub extra_headers: Vec<(String, String)>,
}

fn rfc3339(ms_since_epoch: i64) -> String {
    jiff::Timestamp::from_millisecond(ms_since_epoch)
        .map(|t| t.to_string())
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

/// Builds the wrapped-JSON envelope (`PubsubWrapper`, the default).
///
/// `delivery_attempt` is only included (as `deliveryAttempt`/
/// `delivery_attempt`) when `Some`: `deliveryAttempt` is exposed via the
/// API only when a dead_letter_policy is set, mirrored here for push.
pub fn build_wrapped(
    subscription_full_name: &str,
    msg: &PulledMessage,
    delivery_attempt: Option<u32>,
) -> PushRequestBody {
    use base64::Engine as _;
    let data_b64 = base64::engine::general_purpose::STANDARD.encode(&msg.data);
    let publish_time = rfc3339(msg.publish_time_ms);

    let mut message = serde_json::json!({
        "data": data_b64,
        "attributes": msg.attributes,
        "messageId": msg.message_id,
        "message_id": msg.message_id,
        "publishTime": publish_time,
        "publish_time": publish_time,
    });
    if !msg.ordering_key.is_empty() {
        message["orderingKey"] = serde_json::Value::String(msg.ordering_key.clone());
        message["ordering_key"] = serde_json::Value::String(msg.ordering_key.clone());
    }
    if let Some(attempt) = delivery_attempt {
        message["deliveryAttempt"] = serde_json::Value::from(attempt);
        message["delivery_attempt"] = serde_json::Value::from(attempt);
    }

    let envelope = serde_json::json!({
        "message": message,
        "subscription": subscription_full_name,
    });

    PushRequestBody {
        body: serde_json::to_vec(&envelope).unwrap_or_default(),
        content_type: "application/json",
        extra_headers: Vec::new(),
    }
}

/// Builds the unwrapped body (`NoWrapper`): the raw message data as the
/// HTTP body, with `x-goog-pubsub-*` metadata headers and, if
/// `write_metadata`, each attribute as its own header.
pub fn build_no_wrapper(
    subscription_full_name: &str,
    msg: &PulledMessage,
    write_metadata: bool,
) -> PushRequestBody {
    let mut headers = vec![
        (
            "x-goog-pubsub-subscription-name".to_string(),
            subscription_full_name.to_string(),
        ),
        (
            "x-goog-pubsub-message-id".to_string(),
            msg.message_id.clone(),
        ),
        (
            "x-goog-pubsub-publish-time".to_string(),
            rfc3339(msg.publish_time_ms),
        ),
    ];
    if !msg.ordering_key.is_empty() {
        headers.push((
            "x-goog-pubsub-ordering-key".to_string(),
            msg.ordering_key.clone(),
        ));
    }
    if write_metadata {
        for (k, v) in &msg.attributes {
            headers.push((k.clone(), v.clone()));
        }
    }

    PushRequestBody {
        body: msg.data.clone(),
        content_type: "application/octet-stream",
        extra_headers: headers,
    }
}

/// Builds the body/headers for `msg` per `no_wrapper`/`write_metadata`.
pub fn build(
    subscription_full_name: &str,
    msg: &PulledMessage,
    delivery_attempt: Option<u32>,
    no_wrapper: bool,
    write_metadata: bool,
) -> PushRequestBody {
    if no_wrapper {
        build_no_wrapper(subscription_full_name, msg, write_metadata)
    } else {
        build_wrapped(subscription_full_name, msg, delivery_attempt)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn sample_message() -> PulledMessage {
        PulledMessage {
            ack_id: "ack-1".to_string(),
            message_id: "42".to_string(),
            data: b"hello".to_vec(),
            attributes: HashMap::from([("k".to_string(), "v".to_string())]),
            ordering_key: "order-a".to_string(),
            publish_time_ms: 1_700_000_000_000,
            delivery_attempt: 1,
        }
    }

    #[test]
    fn wrapped_envelope_has_both_key_styles() {
        let body = build_wrapped("projects/p/subscriptions/s", &sample_message(), None);
        let v: serde_json::Value = serde_json::from_slice(&body.body).unwrap();
        assert_eq!(v["message"]["messageId"], "42");
        assert_eq!(v["message"]["message_id"], "42");
        assert!(v["message"].get("publishTime").is_some());
        assert!(v["message"].get("publish_time").is_some());
        assert_eq!(v["message"]["orderingKey"], "order-a");
        assert_eq!(v["message"]["ordering_key"], "order-a");
        assert_eq!(v["subscription"], "projects/p/subscriptions/s");
        // No dead-letter policy on this call -> no delivery attempt exposed.
        assert!(v["message"].get("deliveryAttempt").is_none());
    }

    #[test]
    fn wrapped_envelope_includes_delivery_attempt_when_given() {
        let body = build_wrapped("projects/p/subscriptions/s", &sample_message(), Some(3));
        let v: serde_json::Value = serde_json::from_slice(&body.body).unwrap();
        assert_eq!(v["message"]["deliveryAttempt"], 3);
        assert_eq!(v["message"]["delivery_attempt"], 3);
    }

    #[test]
    fn no_wrapper_sends_raw_body_and_metadata_headers() {
        let body = build_no_wrapper("projects/p/subscriptions/s", &sample_message(), false);
        assert_eq!(body.body, b"hello");
        assert!(body
            .extra_headers
            .iter()
            .any(|(k, v)| k == "x-goog-pubsub-message-id" && v == "42"));
        assert!(!body.extra_headers.iter().any(|(k, _)| k == "k"));
    }

    #[test]
    fn no_wrapper_with_write_metadata_includes_attribute_headers() {
        let body = build_no_wrapper("projects/p/subscriptions/s", &sample_message(), true);
        assert!(body.extra_headers.iter().any(|(k, v)| k == "k" && v == "v"));
    }
}
