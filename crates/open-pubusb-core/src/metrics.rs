//! Prometheus metric names, descriptions, and recording helpers for open-pubusb.
//!
//! This module owns the *definition* of every metric this service exposes.
//! It does not install a recorder/exporter itself — that is
//! the responsibility of the `open-pubusb` binary crate (via
//! `metrics-exporter-prometheus`), which should call [`describe_all`] once
//! at startup after installing its recorder.
//!
//! Cardinality control: per the contract, once a resource count (topics or
//! subscriptions) exceeds 1000, per-resource-labeled series must be cut off
//! and `open_pubusb_metrics_truncated` set to 1. [`CardinalityGuard`] implements
//! that rule; callers should consult it before emitting per-resource-labeled
//! metrics such as [`set_unacked`] or [`set_oldest_unacked_age`].

/// Maximum number of distinct resources (topics/subscriptions) for which
/// per-resource-labeled series are still emitted, per the contract.
const CARDINALITY_LIMIT: usize = 1000;

// ---------------------------------------------------------------------
// Metric name constants
// ---------------------------------------------------------------------

/// `open_pubusb_topics` (gauge) — number of topics
pub const TOPICS: &str = "open_pubusb_topics";
/// `open_pubusb_subscriptions` (gauge) — number of subscriptions
pub const SUBSCRIPTIONS: &str = "open_pubusb_subscriptions";
/// `open_pubusb_messages_published_total` (counter, label `topic`) — messages published
pub const MESSAGES_PUBLISHED_TOTAL: &str = "open_pubusb_messages_published_total";
/// `open_pubusb_messages_delivered_total` (counter, labels `subscription`, `mode`) — messages
/// delivered (including redeliveries)
pub const MESSAGES_DELIVERED_TOTAL: &str = "open_pubusb_messages_delivered_total";
/// `open_pubusb_messages_acked_total` (counter, label `subscription`) — messages acked
pub const MESSAGES_ACKED_TOTAL: &str = "open_pubusb_messages_acked_total";
/// `open_pubusb_messages_expired_total` (counter, label `subscription`) — messages dropped
/// because their retention period elapsed
pub const MESSAGES_EXPIRED_TOTAL: &str = "open_pubusb_messages_expired_total";
/// `open_pubusb_messages_dead_lettered_total` (counter, label `subscription`) — messages
/// forwarded to the dead-letter topic
pub const MESSAGES_DEAD_LETTERED_TOTAL: &str = "open_pubusb_messages_dead_lettered_total";
/// `open_pubusb_unacked_messages` (gauge, label `subscription`) — unacked messages
/// (awaiting delivery + leased)
pub const UNACKED_MESSAGES: &str = "open_pubusb_unacked_messages";
/// `open_pubusb_oldest_unacked_age_seconds` (gauge, label `subscription`) — age in seconds of
/// the oldest unacked message
pub const OLDEST_UNACKED_AGE_SECONDS: &str = "open_pubusb_oldest_unacked_age_seconds";
/// `open_pubusb_publish_latency_seconds` (histogram) — Publish RPC duration
pub const PUBLISH_LATENCY_SECONDS: &str = "open_pubusb_publish_latency_seconds";
/// `open_pubusb_grpc_requests_total` (counter, labels `method`, `code`) — gRPC call count
pub const GRPC_REQUESTS_TOTAL: &str = "open_pubusb_grpc_requests_total";
/// `open_pubusb_push_requests_total` (counter, labels `subscription`, `result`) — push delivery
/// outcomes
pub const PUSH_REQUESTS_TOTAL: &str = "open_pubusb_push_requests_total";
/// `open_pubusb_storage_sync_duration_seconds` (histogram) — group fsync duration
pub const STORAGE_SYNC_DURATION_SECONDS: &str = "open_pubusb_storage_sync_duration_seconds";
/// `open_pubusb_storage_disk_bytes` (gauge) — data_dir disk usage
pub const STORAGE_DISK_BYTES: &str = "open_pubusb_storage_disk_bytes";
/// `open_pubusb_streaming_pull_streams` (gauge) — number of connected streams
pub const STREAMING_PULL_STREAMS: &str = "open_pubusb_streaming_pull_streams";
/// `open_pubusb_metrics_truncated` (gauge) — 1 when per-resource-labeled series have
/// been cut off due to exceeding the cardinality limit, 0 otherwise.
pub const METRICS_TRUNCATED: &str = "open_pubusb_metrics_truncated";

/// Registers descriptions (and types) for every metric with the currently
/// installed `metrics` recorder. Call once at process startup, after
/// installing the exporter recorder (e.g. `PrometheusBuilder::install`) and
/// before serving traffic.
pub fn describe_all() {
    metrics::describe_gauge!(TOPICS, "Number of topics");
    metrics::describe_gauge!(SUBSCRIPTIONS, "Number of subscriptions");
    metrics::describe_counter!(MESSAGES_PUBLISHED_TOTAL, "Number of messages published");
    metrics::describe_counter!(
        MESSAGES_DELIVERED_TOTAL,
        "Number of messages delivered (including redeliveries)"
    );
    metrics::describe_counter!(MESSAGES_ACKED_TOTAL, "Number of messages acknowledged");
    metrics::describe_counter!(
        MESSAGES_EXPIRED_TOTAL,
        "Number of messages discarded after their retention period expired"
    );
    metrics::describe_counter!(
        MESSAGES_DEAD_LETTERED_TOTAL,
        "Number of messages forwarded to a dead-letter topic"
    );
    metrics::describe_gauge!(
        UNACKED_MESSAGES,
        "Number of unacknowledged messages (pending delivery plus leased)"
    );
    metrics::describe_gauge!(
        OLDEST_UNACKED_AGE_SECONDS,
        "Age in seconds of the oldest unacknowledged message"
    );
    metrics::describe_histogram!(PUBLISH_LATENCY_SECONDS, "Publish RPC duration in seconds");
    metrics::describe_counter!(GRPC_REQUESTS_TOTAL, "Number of gRPC requests handled");
    metrics::describe_counter!(PUSH_REQUESTS_TOTAL, "Result of push delivery attempts");
    metrics::describe_histogram!(
        STORAGE_SYNC_DURATION_SECONDS,
        "Duration of a group fsync in seconds"
    );
    metrics::describe_gauge!(STORAGE_DISK_BYTES, "Disk usage of data_dir in bytes");
    metrics::describe_gauge!(
        STREAMING_PULL_STREAMS,
        "Number of currently connected StreamingPull streams"
    );
    metrics::describe_gauge!(
        METRICS_TRUNCATED,
        "1 if per-resource-labeled metric series have been truncated due to \
         exceeding the cardinality limit, 0 otherwise"
    );
}

/// Enforces the cardinality rule from the ops-config contract: once a
/// resource count (topics or subscriptions) exceeds 1000, per-resource
/// labeled series should be cut off and `open_pubusb_metrics_truncated` set to 1.
///
/// Each call to [`should_label`](Self::should_label) also updates the
/// `open_pubusb_metrics_truncated` gauge as a side effect, reflecting the latest
/// known resource count.
#[derive(Debug, Default, Clone, Copy)]
pub struct CardinalityGuard;

impl CardinalityGuard {
    /// Creates a new guard. Stateless: the guard only interprets the
    /// resource count passed to [`should_label`](Self::should_label) at each
    /// call site.
    pub fn new() -> Self {
        Self
    }

    /// Returns `true` if per-resource-labeled series should still be
    /// emitted for the given resource count, i.e. `resource_count <= 1000`.
    ///
    /// As a side effect, sets the `open_pubusb_metrics_truncated` gauge to `1.0`
    /// when the limit is exceeded, `0.0` otherwise.
    pub fn should_label(&self, resource_count: usize) -> bool {
        let truncated = resource_count > CARDINALITY_LIMIT;
        metrics::gauge!(METRICS_TRUNCATED).set(if truncated { 1.0 } else { 0.0 });
        !truncated
    }
}

// ---------------------------------------------------------------------
// Recording helpers
// ---------------------------------------------------------------------

/// Records one published message for `topic`.
pub fn record_published(topic: &str) {
    metrics::counter!(MESSAGES_PUBLISHED_TOTAL, "topic" => topic.to_string()).increment(1);
}

/// Records one delivered message for `subscription` via `mode`
/// (`pull`/`streaming`/`push`).
pub fn record_delivered(subscription: &str, mode: &str) {
    metrics::counter!(
        MESSAGES_DELIVERED_TOTAL,
        "subscription" => subscription.to_string(),
        "mode" => mode.to_string()
    )
    .increment(1);
}

/// Records one acknowledged message for `subscription`.
pub fn record_acked(subscription: &str) {
    metrics::counter!(MESSAGES_ACKED_TOTAL, "subscription" => subscription.to_string())
        .increment(1);
}

/// Records one message discarded for `subscription` after its retention
/// period expired.
pub fn record_expired(subscription: &str) {
    metrics::counter!(MESSAGES_EXPIRED_TOTAL, "subscription" => subscription.to_string())
        .increment(1);
}

/// Records one message forwarded to a dead-letter topic for `subscription`.
pub fn record_dead_lettered(subscription: &str) {
    metrics::counter!(MESSAGES_DEAD_LETTERED_TOTAL, "subscription" => subscription.to_string())
        .increment(1);
}

/// Sets the current number of unacknowledged messages for `subscription`.
pub fn set_unacked(subscription: &str, value: f64) {
    metrics::gauge!(UNACKED_MESSAGES, "subscription" => subscription.to_string()).set(value);
}

/// Sets the age in seconds of the oldest unacknowledged message for
/// `subscription`.
pub fn set_oldest_unacked_age(subscription: &str, seconds: f64) {
    metrics::gauge!(OLDEST_UNACKED_AGE_SECONDS, "subscription" => subscription.to_string())
        .set(seconds);
}

/// Records one Publish RPC duration, in seconds.
pub fn record_publish_latency(seconds: f64) {
    metrics::histogram!(PUBLISH_LATENCY_SECONDS).record(seconds);
}

/// Records one gRPC request for `method`, completed with status `code`.
pub fn record_grpc_request(method: &str, code: &str) {
    metrics::counter!(
        GRPC_REQUESTS_TOTAL,
        "method" => method.to_string(),
        "code" => code.to_string()
    )
    .increment(1);
}

/// Records one push delivery attempt for `subscription`, with `result`
/// being `ok` or `fail`.
pub fn record_push_request(subscription: &str, result: &str) {
    metrics::counter!(
        PUSH_REQUESTS_TOTAL,
        "subscription" => subscription.to_string(),
        "result" => result.to_string()
    )
    .increment(1);
}

/// Records one group fsync duration, in seconds.
pub fn record_storage_sync_duration(seconds: f64) {
    metrics::histogram!(STORAGE_SYNC_DURATION_SECONDS).record(seconds);
}

/// Sets the current disk usage of `data_dir`, in bytes.
pub fn set_storage_disk_bytes(value: f64) {
    metrics::gauge!(STORAGE_DISK_BYTES).set(value);
}

/// Sets the current number of connected StreamingPull streams.
pub fn set_streaming_pull_streams(value: f64) {
    metrics::gauge!(STREAMING_PULL_STREAMS).set(value);
}

/// Sets the current number of topics.
pub fn set_topics(value: f64) {
    metrics::gauge!(TOPICS).set(value);
}

/// Sets the current number of subscriptions.
pub fn set_subscriptions(value: f64) {
    metrics::gauge!(SUBSCRIPTIONS).set(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_helpers_do_not_panic_without_a_recorder() {
        // No recorder is installed in unit tests; `metrics` falls back to a
        // no-op recorder, so these calls must simply not panic.
        record_published("projects/p/topics/t");
        record_delivered("projects/p/subscriptions/s", "pull");
        set_topics(1.0);
        set_subscriptions(1.0);
        let guard = CardinalityGuard::new();
        assert!(guard.should_label(1));
        assert!(!guard.should_label(CARDINALITY_LIMIT + 1));
    }
}
