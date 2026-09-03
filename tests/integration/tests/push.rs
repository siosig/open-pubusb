//! Integration tests for push delivery, against a local
//! axum test receiver and `PubSubService<MemKv>` directly (no gRPC/REST
//! layer, no real `open-pubusb` process — `crate::push::dispatcher` operates
//! purely against the domain layer).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use open_pubusb_core::clock::SystemClock;
use open_pubusb_core::push::dispatcher;
use open_pubusb_core::service::{PubSubService, PublishMessage};
use open_pubusb_core::store::kv::MemKv;
use open_pubusb_core::subscription::{CreateSubscriptionOptions, PushConfig};

const TOPIC: &str = "projects/p/topics/topic-a";
const SUB: &str = "projects/p/subscriptions/sub-a";

#[derive(Debug, Clone)]
struct Captured {
    content_type: Option<String>,
    body: Vec<u8>,
    headers: HeaderMap,
}

#[derive(Clone)]
struct ReceiverState {
    captured: Arc<Mutex<Vec<Captured>>>,
    /// Requests with a 1-based index `<= fail_first` return 500; the rest
    /// return `success_status`.
    fail_first: usize,
    success_status: StatusCode,
    request_count: Arc<AtomicUsize>,
}

async fn receive(
    State(state): State<ReceiverState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> StatusCode {
    let n = state.request_count.fetch_add(1, Ordering::SeqCst) + 1;
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    state.captured.lock().unwrap().push(Captured {
        content_type,
        body: body.to_vec(),
        headers,
    });
    if n <= state.fail_first {
        StatusCode::INTERNAL_SERVER_ERROR
    } else {
        state.success_status
    }
}

/// Starts a local receiver on an OS-assigned port and returns its base URL
/// plus a handle to the captured requests.
async fn start_receiver(
    fail_first: usize,
    success_status: StatusCode,
) -> (String, Arc<Mutex<Vec<Captured>>>) {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let state = ReceiverState {
        captured: captured.clone(),
        fail_first,
        success_status,
        request_count: Arc::new(AtomicUsize::new(0)),
    };
    let app = axum::Router::new()
        .route("/push", post(receive))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind local receiver");
    let addr = listener.local_addr().expect("failed to read local addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}/push"), captured)
}

async fn wait_for<F: Fn() -> bool>(cond: F, what: &str) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while !cond() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

fn service() -> Arc<PubSubService<MemKv>> {
    Arc::new(PubSubService::new(
        Arc::new(MemKv::new()),
        Arc::new(SystemClock),
    ))
}

#[tokio::test]
async fn wrapped_envelope_has_both_key_styles_and_is_acked_on_200() {
    let svc = service();
    let (endpoint, captured) = start_receiver(0, StatusCode::OK).await;

    svc.create_topic(TOPIC, HashMap::new()).unwrap();
    let sub = svc
        .create_subscription(
            SUB,
            TOPIC,
            CreateSubscriptionOptions {
                push_config: Some(PushConfig {
                    endpoint: endpoint.clone(),
                    no_wrapper: false,
                    write_metadata: false,
                }),
                min_retry_backoff_secs: Some(0),
                max_retry_backoff_secs: Some(1),
                ..Default::default()
            },
        )
        .unwrap();

    let handle = dispatcher::spawn(
        svc.clone(),
        SUB.to_string(),
        sub.id,
        sub.ack_deadline_secs,
        sub.min_retry_backoff_secs,
        sub.max_retry_backoff_secs,
        sub.dead_letter_topic.is_some(),
        sub.push_config.clone().unwrap(),
        5,
        4,
    );

    svc.publish(
        TOPIC,
        vec![PublishMessage {
            data: b"hello-push".to_vec(),
            ..Default::default()
        }],
    )
    .unwrap();

    wait_for(
        || !captured.lock().unwrap().is_empty(),
        "first push delivery",
    )
    .await;
    handle.stop().await;

    let requests = captured.lock().unwrap();
    assert_eq!(
        requests.len(),
        1,
        "success on the first attempt should not retry"
    );
    let req = &requests[0];
    assert_eq!(req.content_type.as_deref(), Some("application/json"));
    let v: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
    assert_eq!(v["message"]["messageId"], "1");
    assert_eq!(v["message"]["message_id"], "1");
    assert!(v["message"].get("publishTime").is_some());
    assert!(v["message"].get("publish_time").is_some());
    assert_eq!(v["subscription"], SUB);

    // 200 must have been treated as success: the message is no longer
    // outstanding (a unary Pull sees nothing).
    let pulled = svc.pull(SUB, 10).unwrap();
    assert!(pulled.is_empty());
}

#[tokio::test]
async fn no_wrapper_mode_sends_raw_body_with_metadata_headers() {
    let svc = service();
    let (endpoint, captured) = start_receiver(0, StatusCode::OK).await;

    svc.create_topic(TOPIC, HashMap::new()).unwrap();
    let sub = svc
        .create_subscription(
            SUB,
            TOPIC,
            CreateSubscriptionOptions {
                push_config: Some(PushConfig {
                    endpoint: endpoint.clone(),
                    no_wrapper: true,
                    write_metadata: true,
                }),
                ..Default::default()
            },
        )
        .unwrap();

    let handle = dispatcher::spawn(
        svc.clone(),
        SUB.to_string(),
        sub.id,
        sub.ack_deadline_secs,
        sub.min_retry_backoff_secs,
        sub.max_retry_backoff_secs,
        false,
        sub.push_config.clone().unwrap(),
        5,
        4,
    );

    svc.publish(
        TOPIC,
        vec![PublishMessage {
            data: b"raw-body".to_vec(),
            attributes: HashMap::from([("k".to_string(), "v".to_string())]),
            ..Default::default()
        }],
    )
    .unwrap();

    wait_for(
        || !captured.lock().unwrap().is_empty(),
        "no_wrapper delivery",
    )
    .await;
    handle.stop().await;

    let requests = captured.lock().unwrap();
    let req = &requests[0];
    assert_eq!(req.body, b"raw-body");
    assert_eq!(req.headers.get("x-goog-pubsub-message-id").unwrap(), "1");
    assert_eq!(req.headers.get("k").unwrap(), "v");
}

#[tokio::test]
async fn failure_then_success_retries_with_backoff_and_delivers_once() {
    let svc = service();
    // Fail the first 2 attempts, succeed on the 3rd.
    let (endpoint, captured) = start_receiver(2, StatusCode::OK).await;

    svc.create_topic(TOPIC, HashMap::new()).unwrap();
    let sub = svc
        .create_subscription(
            SUB,
            TOPIC,
            CreateSubscriptionOptions {
                push_config: Some(PushConfig {
                    endpoint: endpoint.clone(),
                    no_wrapper: false,
                    write_metadata: false,
                }),
                min_retry_backoff_secs: Some(0), // floors to 100ms
                max_retry_backoff_secs: Some(1),
                ..Default::default()
            },
        )
        .unwrap();

    let handle = dispatcher::spawn(
        svc.clone(),
        SUB.to_string(),
        sub.id,
        sub.ack_deadline_secs,
        sub.min_retry_backoff_secs,
        sub.max_retry_backoff_secs,
        false,
        sub.push_config.clone().unwrap(),
        5,
        4,
    );

    svc.publish(
        TOPIC,
        vec![PublishMessage {
            data: b"retry-me".to_vec(),
            ..Default::default()
        }],
    )
    .unwrap();

    wait_for(
        || captured.lock().unwrap().len() >= 3,
        "3 delivery attempts",
    )
    .await;
    handle.stop().await;

    let requests = captured.lock().unwrap();
    assert_eq!(
        requests.len(),
        3,
        "exactly 3 attempts: 2 failures + 1 success, no further retries after success"
    );
    for req in requests.iter() {
        let v: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        assert_eq!(
            v["message"]["messageId"], "1",
            "every attempt is the same message"
        );
    }
}

/// A non-success but "2xx-ish" code that is *not* on the documented
/// success-code list (102/200/201/202/204) must still be treated as a
/// failure needing retry.
#[tokio::test]
async fn undocumented_status_code_is_treated_as_failure() {
    let svc = service();
    // 203 (Non-Authoritative Information) is *not* on the documented
    // success list (102/200/201/202/204), unlike its 2xx neighbors —
    // proves the dispatcher checks the exact list, not "any 2xx".
    let (endpoint, _captured) = start_receiver(0, StatusCode::NON_AUTHORITATIVE_INFORMATION).await;

    svc.create_topic(TOPIC, HashMap::new()).unwrap();
    let sub = svc
        .create_subscription(
            SUB,
            TOPIC,
            CreateSubscriptionOptions {
                push_config: Some(PushConfig {
                    endpoint,
                    no_wrapper: false,
                    write_metadata: false,
                }),
                min_retry_backoff_secs: Some(0),
                max_retry_backoff_secs: Some(1),
                ..Default::default()
            },
        )
        .unwrap();

    let handle = dispatcher::spawn(
        svc.clone(),
        SUB.to_string(),
        sub.id,
        sub.ack_deadline_secs,
        sub.min_retry_backoff_secs,
        sub.max_retry_backoff_secs,
        false,
        sub.push_config.clone().unwrap(),
        5,
        4,
    );

    svc.publish(
        TOPIC,
        vec![PublishMessage {
            data: b"not-really-success".to_vec(),
            ..Default::default()
        }],
    )
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    handle.stop().await;

    // A message that was never acked (because 203 never counts as
    // success) must still be outstanding/redeliverable via unary Pull
    // once its lease is released by stopping the dispatcher.
    let pulled = svc.pull(SUB, 10).unwrap();
    assert_eq!(
        pulled.len(),
        1,
        "the message must still be outstanding since 203 is not a documented success code"
    );
}
