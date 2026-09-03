//! Contract tests for the REST (HTTP/JSON) subset, including its error
//! mapping table.
//!
//! Uses `open_pubusb_contract_tests::harness::OpenPubusbHarness::start().await`
//! (`rest_base_url()` and `admin_addr` from `tests/contract/src/harness.rs`).
//!
//! Any test marked `#[ignore]` here can be run with `cargo test -- --ignored`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use open_pubusb_contract_tests::harness::OpenPubusbHarness;
use serde_json::{json, Value};

/// Minimal standard-alphabet base64 encoder (no external crate: this crate's
/// `Cargo.toml` does not currently pull in `base64`, and adding a dependency
/// here isn't worth it for one helper). Matches proto3 JSON `bytes` mapping (RFC 4648 standard
/// alphabet, `=` padding).
fn b64_encode(input: &str) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_bytes();
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        let n = (u32::from(b0) << 16) | (u32::from(b1) << 8) | u32::from(b2);
        out.push(ALPHABET[((n >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[((n >> 6) & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(n & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// Minimal standard-alphabet base64 decoder, paired with [`b64_encode`].
fn b64_decode(input: &str) -> Vec<u8> {
    fn val(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some(u32::from(c - b'A')),
            b'a'..=b'z' => Some(u32::from(c - b'a') + 26),
            b'0'..=b'9' => Some(u32::from(c - b'0') + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let clean: Vec<u8> = input.bytes().filter(|&c| c != b'=').collect();
    let mut out = Vec::new();
    for chunk in clean.chunks(4) {
        let vals: Vec<u32> = chunk
            .iter()
            .map(|&c| val(c).expect("invalid base64 character"))
            .collect();
        let n = vals
            .iter()
            .enumerate()
            .fold(0u32, |acc, (i, &v)| acc | (v << (18 - 6 * i)));
        out.push((n >> 16) as u8);
        if vals.len() > 2 {
            out.push((n >> 8) as u8);
        }
        if vals.len() > 3 {
            out.push(n as u8);
        }
    }
    out
}

/// Structural RFC 3339 validation without an external date/time crate:
/// `YYYY-MM-DDTHH:MM:SS[.fraction](Z|+HH:MM|-HH:MM)` with each numeric field
/// range-checked. Good enough to catch a malformed/missing timestamp without
/// adding a new workspace dependency.
fn assert_rfc3339(s: &str) {
    let bytes = s.as_bytes();
    assert!(bytes.len() >= 20, "timestamp too short for RFC 3339: {s}");
    let digit = |i: usize| -> bool { bytes[i].is_ascii_digit() };
    assert!(
        (0..4).all(digit) && bytes[4] == b'-' && (5..7).all(digit) && bytes[7] == b'-',
        "expected YYYY-MM-DD prefix, got: {s}"
    );
    assert!((8..10).all(digit), "expected DD in date, got: {s}");
    assert!(
        bytes[10] == b'T' || bytes[10] == b't',
        "expected 'T' date/time separator, got: {s}"
    );
    assert!(
        (11..13).all(digit)
            && bytes[13] == b':'
            && (14..16).all(digit)
            && bytes[16] == b':'
            && (17..19).all(digit),
        "expected HH:MM:SS, got: {s}"
    );
    let month: u32 = s[5..7].parse().expect("month not numeric");
    let day: u32 = s[8..10].parse().expect("day not numeric");
    let hour: u32 = s[11..13].parse().expect("hour not numeric");
    let minute: u32 = s[14..16].parse().expect("minute not numeric");
    let second: u32 = s[17..19].parse().expect("second not numeric");
    assert!((1..=12).contains(&month), "month out of range: {s}");
    assert!((1..=31).contains(&day), "day out of range: {s}");
    assert!(hour <= 23, "hour out of range: {s}");
    assert!(minute <= 59, "minute out of range: {s}");
    assert!(second <= 60, "second out of range: {s}"); // allow leap second
    let rest = &s[19..];
    let tz_part = rest.trim_start_matches(|c: char| c == '.' || c.is_ascii_digit());
    assert!(
        tz_part == "Z" || tz_part == "z" || tz_part.starts_with('+') || tz_part.starts_with('-'),
        "expected 'Z' or +/-HH:MM offset, got: {s}"
    );
}

fn unique_id(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        uuid_like_suffix(std::time::SystemTime::now())
    )
}

/// Cheap unique suffix without pulling in a `uuid` dependency: nanos since
/// epoch plus the low bits of the thread id's debug representation.
fn uuid_like_suffix(t: std::time::SystemTime) -> String {
    let nanos = t
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before epoch")
        .as_nanos();
    format!("{nanos:x}")
}

#[tokio::test]
async fn put_topic_creates_topic_with_matching_name() {
    let harness = OpenPubusbHarness::start().await;
    let base = harness.rest_base_url();
    let client = reqwest::Client::new();

    let project = "proj-rest";
    let topic = unique_id("topic-create");
    let url = format!("{base}/v1/projects/{project}/topics/{topic}");

    let resp = client
        .put(&url)
        .json(&json!({}))
        .send()
        .await
        .expect("PUT topic request failed");

    assert_eq!(resp.status().as_u16(), 200);
    let body: Value = resp.json().await.expect("response body was not JSON");
    let expected_name = format!("projects/{project}/topics/{topic}");
    assert_eq!(
        body.get("name").and_then(Value::as_str),
        Some(expected_name.as_str())
    );
}

#[tokio::test]
async fn get_topic_returns_it_as_json() {
    let harness = OpenPubusbHarness::start().await;
    let base = harness.rest_base_url();
    let client = reqwest::Client::new();

    let project = "proj-rest";
    let topic = unique_id("topic-get");
    let create_url = format!("{base}/v1/projects/{project}/topics/{topic}");
    client
        .put(&create_url)
        .json(&json!({}))
        .send()
        .await
        .expect("PUT topic request failed");

    let resp = client
        .get(&create_url)
        .send()
        .await
        .expect("GET topic request failed");

    assert_eq!(resp.status().as_u16(), 200);
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .expect("missing Content-Type header")
        .to_str()
        .expect("Content-Type header was not valid UTF-8");
    assert!(
        content_type.starts_with("application/json"),
        "expected application/json, got {content_type}"
    );

    let body: Value = resp.json().await.expect("response body was not JSON");
    let expected_name = format!("projects/{project}/topics/{topic}");
    assert_eq!(
        body.get("name").and_then(Value::as_str),
        Some(expected_name.as_str())
    );
}

#[tokio::test]
async fn list_topics_includes_created_topic() {
    let harness = OpenPubusbHarness::start().await;
    let base = harness.rest_base_url();
    let client = reqwest::Client::new();

    let project = "proj-rest";
    let topic = unique_id("topic-list");
    let create_url = format!("{base}/v1/projects/{project}/topics/{topic}");
    client
        .put(&create_url)
        .json(&json!({}))
        .send()
        .await
        .expect("PUT topic request failed");

    let list_url = format!("{base}/v1/projects/{project}/topics");
    let resp = client
        .get(&list_url)
        .send()
        .await
        .expect("GET list topics request failed");

    assert_eq!(resp.status().as_u16(), 200);
    let body: Value = resp.json().await.expect("response body was not JSON");
    let expected_name = format!("projects/{project}/topics/{topic}");
    let topics = body
        .get("topics")
        .and_then(Value::as_array)
        .expect("response missing `topics` array");
    let found = topics
        .iter()
        .any(|t| t.get("name").and_then(Value::as_str) == Some(expected_name.as_str()));
    assert!(found, "created topic not found in list response: {body}");
}

#[tokio::test]
async fn publish_returns_one_message_id() {
    let harness = OpenPubusbHarness::start().await;
    let base = harness.rest_base_url();
    let client = reqwest::Client::new();

    let project = "proj-rest";
    let topic = unique_id("topic-publish");
    let create_url = format!("{base}/v1/projects/{project}/topics/{topic}");
    client
        .put(&create_url)
        .json(&json!({}))
        .send()
        .await
        .expect("PUT topic request failed");

    let publish_url = format!("{base}/v1/projects/{project}/topics/{topic}:publish");
    let resp = client
        .post(&publish_url)
        .json(&json!({ "messages": [{ "data": b64_encode("hello") }] }))
        .send()
        .await
        .expect("publish request failed");

    assert_eq!(resp.status().as_u16(), 200);
    let body: Value = resp.json().await.expect("response body was not JSON");
    let ids = body
        .get("messageIds")
        .and_then(Value::as_array)
        .expect("response missing `messageIds` array");
    assert_eq!(ids.len(), 1);
    assert!(ids[0].as_str().is_some_and(|s| !s.is_empty()));
}

#[tokio::test]
async fn put_subscription_creates_pull_subscription() {
    let harness = OpenPubusbHarness::start().await;
    let base = harness.rest_base_url();
    let client = reqwest::Client::new();

    let project = "proj-rest";
    let topic = unique_id("topic-sub-create");
    let sub = unique_id("sub-create");
    let topic_url = format!("{base}/v1/projects/{project}/topics/{topic}");
    client
        .put(&topic_url)
        .json(&json!({}))
        .send()
        .await
        .expect("PUT topic request failed");

    let sub_url = format!("{base}/v1/projects/{project}/subscriptions/{sub}");
    let topic_name = format!("projects/{project}/topics/{topic}");
    let resp = client
        .put(&sub_url)
        .json(&json!({ "topic": topic_name }))
        .send()
        .await
        .expect("PUT subscription request failed");

    assert_eq!(resp.status().as_u16(), 200);
    let body: Value = resp.json().await.expect("response body was not JSON");
    let expected_name = format!("projects/{project}/subscriptions/{sub}");
    assert_eq!(
        body.get("name").and_then(Value::as_str),
        Some(expected_name.as_str())
    );
}

#[tokio::test]
async fn pull_returns_published_message_camel_case() {
    let harness = OpenPubusbHarness::start().await;
    let base = harness.rest_base_url();
    let client = reqwest::Client::new();

    let project = "proj-rest";
    let topic = unique_id("topic-pull");
    let sub = unique_id("sub-pull");
    let topic_url = format!("{base}/v1/projects/{project}/topics/{topic}");
    client
        .put(&topic_url)
        .json(&json!({}))
        .send()
        .await
        .expect("PUT topic request failed");

    let sub_url = format!("{base}/v1/projects/{project}/subscriptions/{sub}");
    let topic_name = format!("projects/{project}/topics/{topic}");
    client
        .put(&sub_url)
        .json(&json!({ "topic": topic_name }))
        .send()
        .await
        .expect("PUT subscription request failed");

    let publish_url = format!("{base}/v1/projects/{project}/topics/{topic}:publish");
    client
        .post(&publish_url)
        .json(&json!({ "messages": [{ "data": b64_encode("hello") }] }))
        .send()
        .await
        .expect("publish request failed");

    let pull_url = format!("{base}/v1/projects/{project}/subscriptions/{sub}:pull");
    let resp = client
        .post(&pull_url)
        .json(&json!({ "maxMessages": 10 }))
        .send()
        .await
        .expect("pull request failed");

    assert_eq!(resp.status().as_u16(), 200);
    let raw = resp.text().await.expect("response body was not text");
    assert!(
        raw.contains("\"receivedMessages\""),
        "expected camelCase `receivedMessages` key, got: {raw}"
    );
    assert!(
        !raw.contains("\"received_messages\""),
        "response used snake_case `received_messages`, got: {raw}"
    );

    let body: Value = serde_json::from_str(&raw).expect("response body was not JSON");
    let received = body
        .get("receivedMessages")
        .and_then(Value::as_array)
        .expect("response missing `receivedMessages` array");
    assert_eq!(received.len(), 1, "expected exactly one received message");

    let received_message = received[0]
        .get("message")
        .expect("received message missing `message` field");
    let data = received_message
        .get("data")
        .and_then(Value::as_str)
        .expect("message missing `data` field");
    let decoded = b64_decode(data);
    assert_eq!(
        String::from_utf8(decoded).expect("decoded data not UTF-8"),
        "hello"
    );

    let publish_time = received_message
        .get("publishTime")
        .and_then(Value::as_str)
        .expect("message missing `publishTime` field");
    assert_rfc3339(publish_time);
}

#[tokio::test]
async fn acknowledge_returns_empty_object() {
    let harness = OpenPubusbHarness::start().await;
    let base = harness.rest_base_url();
    let client = reqwest::Client::new();

    let project = "proj-rest";
    let topic = unique_id("topic-ack");
    let sub = unique_id("sub-ack");
    let topic_url = format!("{base}/v1/projects/{project}/topics/{topic}");
    client
        .put(&topic_url)
        .json(&json!({}))
        .send()
        .await
        .expect("PUT topic request failed");

    let sub_url = format!("{base}/v1/projects/{project}/subscriptions/{sub}");
    let topic_name = format!("projects/{project}/topics/{topic}");
    client
        .put(&sub_url)
        .json(&json!({ "topic": topic_name }))
        .send()
        .await
        .expect("PUT subscription request failed");

    let publish_url = format!("{base}/v1/projects/{project}/topics/{topic}:publish");
    client
        .post(&publish_url)
        .json(&json!({ "messages": [{ "data": b64_encode("hello") }] }))
        .send()
        .await
        .expect("publish request failed");

    let pull_url = format!("{base}/v1/projects/{project}/subscriptions/{sub}:pull");
    let pull_resp = client
        .post(&pull_url)
        .json(&json!({ "maxMessages": 10 }))
        .send()
        .await
        .expect("pull request failed");
    let pull_body: Value = pull_resp.json().await.expect("pull response was not JSON");
    let received = pull_body
        .get("receivedMessages")
        .and_then(Value::as_array)
        .expect("response missing `receivedMessages` array");
    let ack_id = received[0]
        .get("ackId")
        .and_then(Value::as_str)
        .expect("received message missing `ackId` field")
        .to_string();

    let ack_url = format!("{base}/v1/projects/{project}/subscriptions/{sub}:acknowledge");
    let resp = client
        .post(&ack_url)
        .json(&json!({ "ackIds": [ack_id] }))
        .send()
        .await
        .expect("acknowledge request failed");

    assert_eq!(resp.status().as_u16(), 200);
    let body: Value = resp.json().await.expect("response body was not JSON");
    assert_eq!(body, json!({}));
}

#[tokio::test]
async fn delete_subscription_then_get_returns_404() {
    let harness = OpenPubusbHarness::start().await;
    let base = harness.rest_base_url();
    let client = reqwest::Client::new();

    let project = "proj-rest";
    let topic = unique_id("topic-sub-delete");
    let sub = unique_id("sub-delete");
    let topic_url = format!("{base}/v1/projects/{project}/topics/{topic}");
    client
        .put(&topic_url)
        .json(&json!({}))
        .send()
        .await
        .expect("PUT topic request failed");

    let sub_url = format!("{base}/v1/projects/{project}/subscriptions/{sub}");
    let topic_name = format!("projects/{project}/topics/{topic}");
    client
        .put(&sub_url)
        .json(&json!({ "topic": topic_name }))
        .send()
        .await
        .expect("PUT subscription request failed");

    let delete_resp = client
        .delete(&sub_url)
        .send()
        .await
        .expect("DELETE subscription request failed");
    assert_eq!(delete_resp.status().as_u16(), 200);

    let get_resp = client
        .get(&sub_url)
        .send()
        .await
        .expect("GET subscription request failed");
    assert_eq!(get_resp.status().as_u16(), 404);

    let body: Value = get_resp.json().await.expect("response body was not JSON");
    let error = body.get("error").expect("response missing `error` object");
    assert_eq!(error.get("code").and_then(Value::as_i64), Some(404));
    assert_eq!(
        error.get("status").and_then(Value::as_str),
        Some("NOT_FOUND")
    );
}

#[tokio::test]
async fn delete_topic_then_get_returns_404() {
    let harness = OpenPubusbHarness::start().await;
    let base = harness.rest_base_url();
    let client = reqwest::Client::new();

    let project = "proj-rest";
    let topic = unique_id("topic-delete");
    let topic_url = format!("{base}/v1/projects/{project}/topics/{topic}");
    client
        .put(&topic_url)
        .json(&json!({}))
        .send()
        .await
        .expect("PUT topic request failed");

    let delete_resp = client
        .delete(&topic_url)
        .send()
        .await
        .expect("DELETE topic request failed");
    assert_eq!(delete_resp.status().as_u16(), 200);

    let get_resp = client
        .get(&topic_url)
        .send()
        .await
        .expect("GET topic request failed");
    assert_eq!(get_resp.status().as_u16(), 404);

    let body: Value = get_resp.json().await.expect("response body was not JSON");
    let error = body.get("error").expect("response missing `error` object");
    assert_eq!(error.get("code").and_then(Value::as_i64), Some(404));
    assert_eq!(
        error.get("status").and_then(Value::as_str),
        Some("NOT_FOUND")
    );
}

#[tokio::test]
async fn unimplemented_custom_method_returns_501() {
    let harness = OpenPubusbHarness::start().await;
    let base = harness.rest_base_url();
    let client = reqwest::Client::new();

    let project = "proj-rest";
    let topic = unique_id("topic-seek");
    let sub = unique_id("sub-seek");
    let topic_url = format!("{base}/v1/projects/{project}/topics/{topic}");
    client
        .put(&topic_url)
        .json(&json!({}))
        .send()
        .await
        .expect("PUT topic request failed");

    let sub_url = format!("{base}/v1/projects/{project}/subscriptions/{sub}");
    let topic_name = format!("projects/{project}/topics/{topic}");
    client
        .put(&sub_url)
        .json(&json!({ "topic": topic_name }))
        .send()
        .await
        .expect("PUT subscription request failed");

    let seek_url = format!("{base}/v1/projects/{project}/subscriptions/{sub}:seek");
    let resp = client
        .post(&seek_url)
        .json(&json!({}))
        .send()
        .await
        .expect("seek request failed");

    assert_eq!(resp.status().as_u16(), 501);
    let body: Value = resp.json().await.expect("response body was not JSON");
    let error = body.get("error").expect("response missing `error` object");
    assert_eq!(error.get("code").and_then(Value::as_i64), Some(501));
}

#[tokio::test]
async fn create_topic_with_invalid_name_returns_400() {
    let harness = OpenPubusbHarness::start().await;
    let base = harness.rest_base_url();
    let client = reqwest::Client::new();

    let project = "proj-rest";
    // Topic ids must satisfy Pub/Sub's naming rules (min length 3); "ab" is
    // deliberately too short.
    let url = format!("{base}/v1/projects/{project}/topics/ab");

    let resp = client
        .put(&url)
        .json(&json!({}))
        .send()
        .await
        .expect("PUT topic request failed");

    assert_eq!(resp.status().as_u16(), 400);
    let body: Value = resp.json().await.expect("response body was not JSON");
    let error = body.get("error").expect("response missing `error` object");
    assert_eq!(
        error.get("status").and_then(Value::as_str),
        Some("INVALID_ARGUMENT")
    );
}

#[tokio::test]
async fn create_duplicate_topic_returns_409() {
    let harness = OpenPubusbHarness::start().await;
    let base = harness.rest_base_url();
    let client = reqwest::Client::new();

    let project = "proj-rest";
    let topic = unique_id("topic-dup");
    let url = format!("{base}/v1/projects/{project}/topics/{topic}");

    let first = client
        .put(&url)
        .json(&json!({}))
        .send()
        .await
        .expect("first PUT topic request failed");
    assert_eq!(first.status().as_u16(), 200);

    let second = client
        .put(&url)
        .json(&json!({}))
        .send()
        .await
        .expect("second PUT topic request failed");

    assert_eq!(second.status().as_u16(), 409);
    let body: Value = second.json().await.expect("response body was not JSON");
    let error = body.get("error").expect("response missing `error` object");
    assert_eq!(
        error.get("status").and_then(Value::as_str),
        Some("ALREADY_EXISTS")
    );
}

/// Decoding the create body as the generated `Subscription` is what makes
/// `pushConfig` work over REST at all: the hand-written subset struct that
/// preceded it had no such field, so serde dropped it and the caller got a
/// Pull subscription back with a `200`.
#[tokio::test]
async fn create_subscription_honors_push_config() {
    let harness = OpenPubusbHarness::start().await;
    let base = harness.rest_base_url();
    let client = reqwest::Client::new();
    let endpoint = "http://127.0.0.1:9/push";

    let project = "proj-rest";
    let topic = unique_id("topic-push-create");
    let sub = unique_id("sub-push-create");
    client
        .put(format!("{base}/v1/projects/{project}/topics/{topic}"))
        .json(&json!({}))
        .send()
        .await
        .expect("PUT topic request failed");

    let sub_url = format!("{base}/v1/projects/{project}/subscriptions/{sub}");
    let resp = client
        .put(&sub_url)
        .json(&json!({
            "topic": format!("projects/{project}/topics/{topic}"),
            "pushConfig": { "pushEndpoint": endpoint },
        }))
        .send()
        .await
        .expect("PUT subscription request failed");

    assert_eq!(resp.status().as_u16(), 200);
    let created: Value = resp.json().await.expect("response body was not JSON");
    assert_eq!(
        created
            .pointer("/pushConfig/pushEndpoint")
            .and_then(Value::as_str),
        Some(endpoint),
        "the create response must echo the push config back: {created}"
    );

    let fetched: Value = client
        .get(&sub_url)
        .send()
        .await
        .expect("GET subscription request failed")
        .json()
        .await
        .expect("response body was not JSON");
    assert_eq!(
        fetched
            .pointer("/pushConfig/pushEndpoint")
            .and_then(Value::as_str),
        Some(endpoint),
        "the push config must be persisted, not merely echoed: {fetched}"
    );
}

/// Same mechanism as the push config, on a second field, to pin down that
/// the fix is "every field the gRPC path honors" rather than a one-off
/// special case for `pushConfig`.
#[tokio::test]
async fn create_subscription_honors_filter() {
    let harness = OpenPubusbHarness::start().await;
    let base = harness.rest_base_url();
    let client = reqwest::Client::new();
    let filter = "attributes.color = \"blue\"";

    let project = "proj-rest";
    let topic = unique_id("topic-filter-create");
    let sub = unique_id("sub-filter-create");
    client
        .put(format!("{base}/v1/projects/{project}/topics/{topic}"))
        .json(&json!({}))
        .send()
        .await
        .expect("PUT topic request failed");

    let sub_url = format!("{base}/v1/projects/{project}/subscriptions/{sub}");
    let resp = client
        .put(&sub_url)
        .json(&json!({
            "topic": format!("projects/{project}/topics/{topic}"),
            "filter": filter,
        }))
        .send()
        .await
        .expect("PUT subscription request failed");

    assert_eq!(resp.status().as_u16(), 200);
    let created: Value = resp.json().await.expect("response body was not JSON");
    assert_eq!(
        created.get("filter").and_then(Value::as_str),
        Some(filter),
        "the filter must survive the create: {created}"
    );
}

/// The generated `Deserialize` rejects unknown fields, which is the half of
/// the contract that turns a typo (or a field this server does not model)
/// into a loud `400` instead of a silently ignored one.
#[tokio::test]
async fn create_subscription_rejects_unknown_field() {
    let harness = OpenPubusbHarness::start().await;
    let base = harness.rest_base_url();
    let client = reqwest::Client::new();

    let project = "proj-rest";
    let topic = unique_id("topic-unknown-field");
    let sub = unique_id("sub-unknown-field");
    client
        .put(format!("{base}/v1/projects/{project}/topics/{topic}"))
        .json(&json!({}))
        .send()
        .await
        .expect("PUT topic request failed");

    let resp = client
        .put(format!("{base}/v1/projects/{project}/subscriptions/{sub}"))
        .json(&json!({
            "topic": format!("projects/{project}/topics/{topic}"),
            "pushConfigTypo": { "pushEndpoint": "http://127.0.0.1:9/push" },
        }))
        .send()
        .await
        .expect("PUT subscription request failed");

    assert_eq!(resp.status().as_u16(), 400);
    let body: Value = resp.json().await.expect("response body was not JSON");
    assert_eq!(
        body.pointer("/error/status").and_then(Value::as_str),
        Some("INVALID_ARGUMENT"),
        "unknown fields must be rejected, not dropped: {body}"
    );
}

/// `POST .../subscriptions/{sub}:modifyPushConfig`, including the emptiness
/// rule the gRPC method uses: an empty `pushConfig` switches back to Pull.
#[tokio::test]
async fn modify_push_config_sets_then_clears_endpoint() {
    let harness = OpenPubusbHarness::start().await;
    let base = harness.rest_base_url();
    let client = reqwest::Client::new();
    let endpoint = "http://127.0.0.1:9/push";

    let project = "proj-rest";
    let topic = unique_id("topic-modify-push");
    let sub = unique_id("sub-modify-push");
    client
        .put(format!("{base}/v1/projects/{project}/topics/{topic}"))
        .json(&json!({}))
        .send()
        .await
        .expect("PUT topic request failed");

    let sub_url = format!("{base}/v1/projects/{project}/subscriptions/{sub}");
    client
        .put(&sub_url)
        .json(&json!({ "topic": format!("projects/{project}/topics/{topic}") }))
        .send()
        .await
        .expect("PUT subscription request failed");

    let modify_url = format!("{sub_url}:modifyPushConfig");
    let resp = client
        .post(&modify_url)
        .json(&json!({ "pushConfig": { "pushEndpoint": endpoint } }))
        .send()
        .await
        .expect("POST :modifyPushConfig request failed");
    assert_eq!(resp.status().as_u16(), 200);

    let fetched: Value = client
        .get(&sub_url)
        .send()
        .await
        .expect("GET subscription request failed")
        .json()
        .await
        .expect("response body was not JSON");
    assert_eq!(
        fetched
            .pointer("/pushConfig/pushEndpoint")
            .and_then(Value::as_str),
        Some(endpoint),
        "the subscription must have switched to push: {fetched}"
    );

    let resp = client
        .post(&modify_url)
        .json(&json!({ "pushConfig": {} }))
        .send()
        .await
        .expect("POST :modifyPushConfig (clear) request failed");
    assert_eq!(resp.status().as_u16(), 200);

    let cleared: Value = client
        .get(&sub_url)
        .send()
        .await
        .expect("GET subscription request failed")
        .json()
        .await
        .expect("response body was not JSON");
    assert_eq!(
        cleared.get("pushConfig"),
        None,
        "an empty push config must switch the subscription back to pull: {cleared}"
    );
}
