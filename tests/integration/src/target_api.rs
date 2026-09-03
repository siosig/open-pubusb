//! Documents the `PubSubService` surface the User Story 1 integration tests
//! exercise (topic/subscription CRUD, publish/pull/ack), unioned into a
//! single trait + stub.
//!
//! Implemented for real by a later task
//! (`crates/open-pubusb-core/src/service.rs`). This file exists so
//! `tests/integration/tests/{topics_subscriptions,publish_pull_ack}.rs` can
//! be written and reviewed today, and will be deleted/replaced once
//! `open_pubusb_core::service::PubSubService` actually exists — at that point the
//! tests should be updated to import the real type instead and every
//! `#[ignore]` removed.
//!
//! `open_pubusb_core::{Error, Result}` now exist and are used directly here
//! — earlier drafts of this file defined a local mirror of `Error`; that
//! mirror has been replaced.

use std::collections::HashMap;

pub use open_pubusb_core::{Error, Result};

/// Mirrors `open_pubusb_core::names::DELETED_TOPIC` for readability at call
/// sites; re-exported so tests don't need a second `use`.
pub use open_pubusb_core::names::DELETED_TOPIC;

/// Domain-level view of a `google.pubsub.v1.Topic`, trimmed to the fields
/// these tests care about.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TopicInfo {
    pub name: String,
    pub labels: HashMap<String, String>,
}

/// Domain-level view of a `google.pubsub.v1.Subscription`, trimmed to the
/// fields these tests care about.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubscriptionInfo {
    pub name: String,
    /// The subscription's topic, or [`DELETED_TOPIC`] once that topic has
    /// been deleted.
    pub topic: String,
    pub labels: HashMap<String, String>,
    pub detached: bool,
}

/// Subset of `CreateSubscriptionRequest` fields these tests need to pass
/// at creation time. Union of what the CRUD/labels/ordering/filter tests
/// and the ack-deadline tests each needed.
#[derive(Debug, Clone)]
pub struct SubscriptionOpts {
    pub ack_deadline_seconds: i32,
    pub labels: HashMap<String, String>,
    pub enable_message_ordering: bool,
    pub filter: Option<String>,
}

impl Default for SubscriptionOpts {
    fn default() -> Self {
        // Matches open_pubusb_core::limits::MIN_ACK_DEADLINE_SECS (10s).
        Self {
            ack_deadline_seconds: 10,
            labels: HashMap::new(),
            enable_message_ordering: false,
            filter: None,
        }
    }
}

/// A message as submitted to `publish`.
#[derive(Debug, Clone, Default)]
pub struct PublishMessage {
    pub data: Vec<u8>,
    pub attributes: HashMap<String, String>,
    pub ordering_key: String,
}

/// Domain-level view of one pulled `google.pubsub.v1.PubsubMessage` plus
/// its `ack_id` and delivery metadata. Union of the fields needed for the
/// ordering_key (a detached-pull FAILED_PRECONDITION check) and for
/// publish_time_ms / delivery_attempt (redelivery assertions).
#[derive(Debug, Clone, Default)]
pub struct PulledMessage {
    pub ack_id: String,
    pub message_id: String,
    pub data: Vec<u8>,
    pub attributes: HashMap<String, String>,
    pub ordering_key: String,
    pub publish_time_ms: i64,
    pub delivery_attempt: u32,
}

/// Documents the domain-level surface of the eventual
/// `open_pubusb_core::service::PubSubService` that the User Story 1
/// integration tests exercise. No proto types appear here — only the
/// domain-level request/response shapes, matching the gRPC
/// Publisher/Subscriber method tables.
///
/// Every method body on [`StubService`] below is `unimplemented!()`: this
/// trait exists purely as a typed contract for the `#[ignore]`d tests to
/// compile against, not as a working implementation.
pub trait PubSubServiceApi {
    fn create_topic(&self, full_name: &str, labels: HashMap<String, String>) -> Result<()>;
    fn get_topic(&self, full_name: &str) -> Result<TopicInfo>;
    fn list_topics(&self, project_id: &str) -> Result<Vec<TopicInfo>>;
    fn delete_topic(&self, full_name: &str) -> Result<()>;
    fn list_topic_subscriptions(&self, topic_full_name: &str) -> Result<Vec<String>>;
    fn update_topic_labels(&self, full_name: &str, labels: HashMap<String, String>) -> Result<()>;

    fn create_subscription(
        &self,
        full_name: &str,
        topic_full_name: &str,
        opts: SubscriptionOpts,
    ) -> Result<()>;
    fn get_subscription(&self, full_name: &str) -> Result<SubscriptionInfo>;
    fn delete_subscription(&self, full_name: &str) -> Result<()>;
    fn detach_subscription(&self, full_name: &str) -> Result<()>;

    /// Publishes `messages` to the given topic, returning one message_id
    /// per input message, in input order.
    fn publish(&self, topic_full_name: &str, messages: Vec<PublishMessage>) -> Result<Vec<String>>;

    /// Pulls up to `max_messages` currently-deliverable messages from the
    /// given subscription. Returns promptly (no blocking wait) in this
    /// stub shape.
    fn pull(&self, subscription_full_name: &str, max_messages: i32) -> Result<Vec<PulledMessage>>;

    /// Acknowledges the given ack_ids. Unknown/stale/duplicate ack_ids are
    /// ignored (never an error).
    fn acknowledge(&self, subscription_full_name: &str, ack_ids: Vec<String>) -> Result<()>;

    /// Extends (or, with `seconds == 0`, immediately expires/nacks) the
    /// ack deadline for the given ack_ids.
    fn modify_ack_deadline(
        &self,
        subscription_full_name: &str,
        ack_ids: Vec<String>,
        seconds: i32,
    ) -> Result<()>;

    /// Test-only: fast-forwards the mock clock so ack-deadline expiry can
    /// be exercised deterministically without real sleeps.
    fn advance_clock(&self, seconds: i64);
}

/// Adapts the real `open_pubusb_core::service::PubSubService` (backed by
/// `MemKv` + a `MockClock` so [`PubSubServiceApi::advance_clock`] works)
/// to this trait's simplified request/response shapes. `StubService` was
/// this type's earlier name (an `unimplemented!()`-bodied placeholder);
/// kept as an alias so the test files that construct
/// `StubService::new_ephemeral()` did not need to change when the real
/// service landed.
pub struct RealService {
    svc: open_pubusb_core::service::PubSubService<open_pubusb_core::store::kv::MemKv>,
    clock: std::sync::Arc<open_pubusb_core::clock::MockClock>,
}

/// See [`RealService`]'s doc comment.
pub type StubService = RealService;

impl RealService {
    /// Mirrors the intended `PubSubService::new_ephemeral() -> Self`. Uses
    /// a `MockClock` (starting at an arbitrary fixed instant, not real
    /// wall-clock time) internally so `advance_clock` has something to
    /// move — tests that never call `advance_clock` are unaffected by
    /// this starting point.
    pub fn new_ephemeral() -> Self {
        let clock = std::sync::Arc::new(open_pubusb_core::clock::MockClock::new(1_700_000_000_000));
        Self {
            svc: open_pubusb_core::service::PubSubService::new_ephemeral_with_clock(clock.clone()),
            clock,
        }
    }
}

fn topic_info(record: &open_pubusb_core::topic::TopicRecord) -> TopicInfo {
    TopicInfo {
        name: record.name.clone(),
        labels: record.labels.clone(),
    }
}

fn subscription_info(
    record: &open_pubusb_core::subscription::SubscriptionRecord,
) -> SubscriptionInfo {
    SubscriptionInfo {
        name: record.name.clone(),
        topic: record.topic.clone(),
        labels: record.labels.clone(),
        detached: record.detached,
    }
}

impl PubSubServiceApi for RealService {
    fn create_topic(&self, full_name: &str, labels: HashMap<String, String>) -> Result<()> {
        self.svc.create_topic(full_name, labels)?;
        Ok(())
    }

    fn get_topic(&self, full_name: &str) -> Result<TopicInfo> {
        Ok(topic_info(&self.svc.get_topic(full_name)?))
    }

    fn list_topics(&self, project_id: &str) -> Result<Vec<TopicInfo>> {
        let (records, _next) = self.svc.list_topics(project_id, usize::MAX, None)?;
        Ok(records.iter().map(topic_info).collect())
    }

    fn delete_topic(&self, full_name: &str) -> Result<()> {
        self.svc.delete_topic(full_name)
    }

    fn list_topic_subscriptions(&self, topic_full_name: &str) -> Result<Vec<String>> {
        self.svc.list_topic_subscriptions(topic_full_name)
    }

    fn update_topic_labels(&self, full_name: &str, labels: HashMap<String, String>) -> Result<()> {
        self.svc.update_topic_labels(full_name, labels)?;
        Ok(())
    }

    fn create_subscription(
        &self,
        full_name: &str,
        topic_full_name: &str,
        opts: SubscriptionOpts,
    ) -> Result<()> {
        let core_opts = open_pubusb_core::subscription::CreateSubscriptionOptions {
            ack_deadline_secs: Some(opts.ack_deadline_seconds),
            labels: opts.labels,
            enable_message_ordering: opts.enable_message_ordering,
            filter: opts.filter.unwrap_or_default(),
            ..Default::default()
        };
        self.svc
            .create_subscription(full_name, topic_full_name, core_opts)?;
        Ok(())
    }

    fn get_subscription(&self, full_name: &str) -> Result<SubscriptionInfo> {
        Ok(subscription_info(&self.svc.get_subscription(full_name)?))
    }

    fn delete_subscription(&self, full_name: &str) -> Result<()> {
        self.svc.delete_subscription(full_name)
    }

    fn detach_subscription(&self, full_name: &str) -> Result<()> {
        self.svc.detach_subscription(full_name)
    }

    fn publish(&self, topic_full_name: &str, messages: Vec<PublishMessage>) -> Result<Vec<String>> {
        let core_messages = messages
            .into_iter()
            .map(|m| open_pubusb_core::service::PublishMessage {
                data: m.data,
                attributes: m.attributes,
                ordering_key: m.ordering_key,
            })
            .collect();
        self.svc.publish(topic_full_name, core_messages)
    }

    fn pull(&self, subscription_full_name: &str, max_messages: i32) -> Result<Vec<PulledMessage>> {
        let delivered = self.svc.pull(subscription_full_name, max_messages)?;
        Ok(delivered
            .into_iter()
            .map(|m| PulledMessage {
                ack_id: m.ack_id,
                message_id: m.message_id,
                data: m.data,
                attributes: m.attributes,
                ordering_key: m.ordering_key,
                publish_time_ms: m.publish_time_ms,
                delivery_attempt: m.delivery_attempt,
            })
            .collect())
    }

    fn acknowledge(&self, subscription_full_name: &str, ack_ids: Vec<String>) -> Result<()> {
        self.svc.acknowledge(subscription_full_name, ack_ids)
    }

    fn modify_ack_deadline(
        &self,
        subscription_full_name: &str,
        ack_ids: Vec<String>,
        seconds: i32,
    ) -> Result<()> {
        self.svc
            .modify_ack_deadline(subscription_full_name, ack_ids, seconds)
    }

    fn advance_clock(&self, seconds: i64) {
        self.clock.advance_secs(seconds);
    }
}

/// Placeholder domain error surfaced by [`Error::InvalidArgument`] et al.
/// Re-exported so call sites can write `target_api::Error` if they prefer
/// (identical to `open_pubusb_core::Error`).
pub type TargetError = Error;
