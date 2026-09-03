//! Dead-lettering: once a message's delivery attempts on
//! a subscription exceed its `dead_letter_policy.max_delivery_attempts`,
//! [`super::engine::DeliveryEngine::lease_next`] append-forwards it to the
//! configured dead-letter topic (with the `CloudPubSubDeadLetterSource*`
//! attributes real Pub/Sub adds) and marks it acked on the source
//! subscription, instead of leasing it out again.
//!
//! This module holds the pure decision/attribute-building logic; the
//! actual `kv` reads/writes (loading the policy, appending to the DLQ
//! topic via [`crate::topic::TopicStore`], acking the source) live in
//! `engine.rs`'s `lease_next`, next to the retry-backoff gate ([`super::retry`])
//! it composes with — a message becomes dead-letter-eligible using the
//! exact same `prior_attempts` count that `lease_next` already computes
//! for that gate.

use std::collections::HashMap;

/// A subscription's dead-letter configuration, as stored on
/// [`crate::subscription::SubscriptionRecord`].
#[derive(Debug, Clone)]
pub struct DeadLetterPolicy {
    /// `None` when no `dead_letter_topic` is configured — dead-lettering
    /// is disabled for this subscription regardless of `max_delivery_attempts`.
    pub topic: Option<String>,
    /// Only meaningful when `topic.is_some()`.
    pub max_delivery_attempts: i32,
}

/// Attribute key: the message's `delivery_attempt` count at the moment it
/// was dead-lettered.
pub const ATTR_DELIVERY_COUNT: &str = "CloudPubSubDeadLetterSourceDeliveryCount";
/// Attribute key: the full resource name of the source subscription.
pub const ATTR_SUBSCRIPTION: &str = "CloudPubSubDeadLetterSourceSubscription";
/// Attribute key: the project id parsed out of the source subscription's
/// full resource name.
pub const ATTR_SUBSCRIPTION_PROJECT: &str = "CloudPubSubDeadLetterSourceSubscriptionProject";
/// Attribute key: the message's original publish time, RFC 3339 UTC.
pub const ATTR_TOPIC_PUBLISH_TIME: &str = "CloudPubSubDeadLetterSourceTopicPublishTime";

/// Whether a message that has already been delivered `prior_attempts`
/// times (0 the first time it's ever leased) should be dead-lettered
/// instead of leased again.
///
/// `max_delivery_attempts <= 0` (the create-time "unset" sentinel — see
/// [`crate::subscription::SubscriptionRecord::max_delivery_attempts`]'s
/// doc comment) never dead-letters, even with a topic configured, since a
/// policy isn't actually in effect without a positive threshold.
pub fn should_dead_letter(prior_attempts: u32, policy: &DeadLetterPolicy) -> bool {
    policy.topic.is_some()
        && policy.max_delivery_attempts > 0
        && prior_attempts >= policy.max_delivery_attempts as u32
}

/// Returns `original` plus the four `CloudPubSubDeadLetterSource*`
/// attributes real Pub/Sub adds when forwarding to a dead-letter topic
/// (see the [Pub/Sub dead-letter docs](https://cloud.google.com/pubsub/docs/handling-failures#dead_letter_topic)).
///
/// `source_subscription_full_name` is `projects/{project}/subscriptions/{name}`;
/// the project id is split back out of it for `ATTR_SUBSCRIPTION_PROJECT`.
pub fn build_attributes(
    original: &HashMap<String, String>,
    delivery_count: u32,
    source_subscription_full_name: &str,
    source_topic_publish_ts_ms: i64,
) -> HashMap<String, String> {
    let mut attrs = original.clone();
    attrs.insert(ATTR_DELIVERY_COUNT.to_string(), delivery_count.to_string());
    attrs.insert(
        ATTR_SUBSCRIPTION.to_string(),
        source_subscription_full_name.to_string(),
    );
    let project = source_subscription_full_name
        .strip_prefix("projects/")
        .and_then(|rest| rest.split('/').next())
        .unwrap_or_default();
    attrs.insert(ATTR_SUBSCRIPTION_PROJECT.to_string(), project.to_string());
    attrs.insert(
        ATTR_TOPIC_PUBLISH_TIME.to_string(),
        format_rfc3339_ms(source_topic_publish_ts_ms),
    );
    attrs
}

/// Formats a ms-since-epoch timestamp as RFC 3339 UTC with millisecond
/// precision (`2024-01-02T03:04:05.678Z`), matching the format real
/// Pub/Sub uses for `publishTime`-derived attribute values. Written by
/// hand (no `chrono`/`time` dependency in this crate) using the
/// civil-from-days algorithm (Howard Hinnant's
/// `http://howardhinnant.github.io/date_algorithms.html#civil_from_days`).
fn format_rfc3339_ms(ms: i64) -> String {
    let millis = ms.rem_euclid(1000);
    let secs_total = ms.div_euclid(1000);
    let days = secs_total.div_euclid(86_400);
    let secs_of_day = secs_total.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = if month <= 2 { y + 1 } else { y };
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_topic_never_dead_letters() {
        let policy = DeadLetterPolicy {
            topic: None,
            max_delivery_attempts: 5,
        };
        assert!(!should_dead_letter(100, &policy));
    }

    #[test]
    fn unset_max_attempts_never_dead_letters() {
        let policy = DeadLetterPolicy {
            topic: Some("projects/p/topics/dlq".to_string()),
            max_delivery_attempts: 0,
        };
        assert!(!should_dead_letter(100, &policy));
    }

    #[test]
    fn dead_letters_once_prior_attempts_reach_the_threshold() {
        let policy = DeadLetterPolicy {
            topic: Some("projects/p/topics/dlq".to_string()),
            max_delivery_attempts: 5,
        };
        assert!(!should_dead_letter(3, &policy));
        assert!(!should_dead_letter(4, &policy));
        assert!(should_dead_letter(5, &policy));
        assert!(should_dead_letter(6, &policy));
    }

    #[test]
    fn build_attributes_adds_the_four_source_keys_and_keeps_original() {
        let mut original = HashMap::new();
        original.insert("k".to_string(), "v".to_string());
        let attrs = build_attributes(
            &original,
            6,
            "projects/proj-1/subscriptions/sub-a",
            1_700_000_000_123,
        );
        assert_eq!(attrs.get("k"), Some(&"v".to_string()));
        assert_eq!(attrs.get(ATTR_DELIVERY_COUNT), Some(&"6".to_string()));
        assert_eq!(
            attrs.get(ATTR_SUBSCRIPTION),
            Some(&"projects/proj-1/subscriptions/sub-a".to_string())
        );
        assert_eq!(
            attrs.get(ATTR_SUBSCRIPTION_PROJECT),
            Some(&"proj-1".to_string())
        );
        assert!(attrs.get(ATTR_TOPIC_PUBLISH_TIME).unwrap().ends_with('Z'));
    }

    #[test]
    fn rfc3339_formatting_is_correct_for_a_known_instant() {
        // 2023-11-14T22:13:20.123Z
        assert_eq!(
            format_rfc3339_ms(1_700_000_000_123),
            "2023-11-14T22:13:20.123Z"
        );
        assert_eq!(format_rfc3339_ms(0), "1970-01-01T00:00:00.000Z");
    }
}
