//! Size, count, and duration limits enforced across the Pub/Sub API surface,
//! and the validators that check request data against them.
//!
//! Values and semantics follow the validation rules (FR-005) and the
//! per-field constraints defined for each resource (Topic / Subscription /
//! PubsubMessage).

use std::collections::HashMap;

use crate::error::{Error, Result};

/// Maximum combined size (bytes) of a single message's `data` + all
/// `attributes` (keys and values).
pub const MAX_MESSAGE_BYTES: usize = 10 * 1024 * 1024;

/// Maximum number of messages in a single Publish request.
pub const MAX_PUBLISH_BATCH_MESSAGES: usize = 1000;

/// Maximum combined size (bytes) of all messages in a single Publish request.
pub const MAX_PUBLISH_BATCH_BYTES: usize = 10 * 1024 * 1024;

/// Maximum number of attributes on a single message.
pub const MAX_ATTRIBUTES: usize = 100;

/// Maximum size (bytes) of a single attribute key.
pub const MAX_ATTRIBUTE_KEY_BYTES: usize = 256;

/// Maximum size (bytes) of a single attribute value.
pub const MAX_ATTRIBUTE_VALUE_BYTES: usize = 1024;

/// Maximum size (bytes) of a message's `ordering_key`.
pub const MAX_ORDERING_KEY_BYTES: usize = 1024;

/// Minimum allowed `ack_deadline_seconds` on a Subscription.
pub const MIN_ACK_DEADLINE_SECS: i32 = 10;

/// Maximum allowed `ack_deadline_seconds` on a Subscription (also the
/// maximum for `ModifyAckDeadline`).
pub const MAX_ACK_DEADLINE_SECS: i32 = 600;

/// Minimum allowed Subscription `message_retention_duration`, in seconds
/// (10 minutes).
pub const MIN_SUBSCRIPTION_RETENTION_SECS: i64 = 600;

/// Maximum allowed Subscription `message_retention_duration`, in seconds
/// (7 days).
pub const MAX_SUBSCRIPTION_RETENTION_SECS: i64 = 7 * 24 * 3600;

/// Minimum allowed Topic `message_retention_duration`, in seconds
/// (10 minutes).
pub const MIN_TOPIC_RETENTION_SECS: i64 = 600;

/// Maximum allowed Topic `message_retention_duration`, in seconds
/// (31 days).
pub const MAX_TOPIC_RETENTION_SECS: i64 = 31 * 24 * 3600;

/// Maximum lifetime of a Snapshot, in seconds (7 days): `expire_time` =
/// creation time + (7d − age of the source
/// subscription's oldest unacked message). Same value as
/// [`MAX_SUBSCRIPTION_RETENTION_SECS`] (real Pub/Sub ties both to the same
/// 7-day figure) but named separately since the two aren't the same
/// concept — reusing the subscription-retention constant here would read
/// as a coincidence rather than the deliberate reuse it is.
pub const MAX_SNAPSHOT_LIFETIME_SECS: i64 = MAX_SUBSCRIPTION_RETENTION_SECS;

/// Minimum remaining lifetime a Snapshot must have at creation time, in
/// seconds (1 hour): creation fails with
/// FAILED_PRECONDITION if less than 1 hour of lifetime remains.
pub const MIN_SNAPSHOT_REMAINING_LIFETIME_SECS: i64 = 3600;

/// Minimum allowed `dead_letter_policy.max_delivery_attempts`.
pub const MIN_DEAD_LETTER_MAX_ATTEMPTS: i32 = 5;

/// Maximum allowed `dead_letter_policy.max_delivery_attempts`.
pub const MAX_DEAD_LETTER_MAX_ATTEMPTS: i32 = 100;

/// Minimum allowed `retry_policy` backoff (minimum or maximum), in seconds.
pub const MIN_RETRY_BACKOFF_SECS: i64 = 0;

/// Maximum allowed `retry_policy` backoff (minimum or maximum), in seconds.
pub const MAX_RETRY_BACKOFF_SECS: i64 = 600;

/// Maximum length (in characters, not bytes) of a Subscription `filter`.
pub const MAX_FILTER_CHARS: usize = 256;

/// Minimum allowed `max_messages` on a Pull request.
pub const MIN_PULL_MAX_MESSAGES: i32 = 1;

/// Maximum allowed `max_messages` on a Pull request.
pub const MAX_PULL_MAX_MESSAGES: i32 = 1000;

/// Attribute keys starting with this prefix are reserved and rejected.
pub const ATTRIBUTE_KEY_FORBIDDEN_PREFIX: &str = "goog";

/// Validates a single message's `data` size, `attributes`, and
/// `ordering_key` against the limits above.
///
/// `data_len` is the byte length of the message payload (`data`).
pub fn validate_message(
    data_len: usize,
    attributes: &HashMap<String, String>,
    ordering_key: &str,
) -> Result<()> {
    if attributes.len() > MAX_ATTRIBUTES {
        return Err(Error::InvalidArgument {
            field: "attributes".to_string(),
            message: format!(
                "attributes count {} exceeds maximum of {MAX_ATTRIBUTES}",
                attributes.len()
            ),
        });
    }

    let mut attributes_bytes: usize = 0;
    for (key, value) in attributes {
        if key.len() > MAX_ATTRIBUTE_KEY_BYTES {
            return Err(Error::InvalidArgument {
                field: "attributes.key".to_string(),
                message: format!(
                    "attribute key {key:?} is {} bytes, exceeding maximum of {MAX_ATTRIBUTE_KEY_BYTES}",
                    key.len()
                ),
            });
        }
        if key.starts_with(ATTRIBUTE_KEY_FORBIDDEN_PREFIX) {
            return Err(Error::InvalidArgument {
                field: "attributes.key".to_string(),
                message: format!(
                    "attribute key {key:?} must not start with {ATTRIBUTE_KEY_FORBIDDEN_PREFIX:?}"
                ),
            });
        }
        if value.len() > MAX_ATTRIBUTE_VALUE_BYTES {
            return Err(Error::InvalidArgument {
                field: "attributes.value".to_string(),
                message: format!(
                    "attribute value for key {key:?} is {} bytes, exceeding maximum of {MAX_ATTRIBUTE_VALUE_BYTES}",
                    value.len()
                ),
            });
        }
        attributes_bytes = attributes_bytes
            .saturating_add(key.len())
            .saturating_add(value.len());
    }

    let total_bytes = data_len.saturating_add(attributes_bytes);
    if total_bytes > MAX_MESSAGE_BYTES {
        return Err(Error::InvalidArgument {
            field: "data".to_string(),
            message: format!(
                "message size {total_bytes} bytes (data + attributes) exceeds maximum of {MAX_MESSAGE_BYTES}"
            ),
        });
    }

    if ordering_key.len() > MAX_ORDERING_KEY_BYTES {
        return Err(Error::InvalidArgument {
            field: "ordering_key".to_string(),
            message: format!(
                "ordering_key is {} bytes, exceeding maximum of {MAX_ORDERING_KEY_BYTES}",
                ordering_key.len()
            ),
        });
    }

    Ok(())
}

/// Validates a Publish request's aggregate message count and total byte
/// size against the batch limits.
pub fn validate_publish_batch(message_count: usize, total_bytes: usize) -> Result<()> {
    if message_count > MAX_PUBLISH_BATCH_MESSAGES {
        return Err(Error::InvalidArgument {
            field: "messages".to_string(),
            message: format!(
                "batch contains {message_count} messages, exceeding maximum of {MAX_PUBLISH_BATCH_MESSAGES}"
            ),
        });
    }
    if total_bytes > MAX_PUBLISH_BATCH_BYTES {
        return Err(Error::InvalidArgument {
            field: "messages".to_string(),
            message: format!(
                "batch total size {total_bytes} bytes exceeds maximum of {MAX_PUBLISH_BATCH_BYTES}"
            ),
        });
    }
    Ok(())
}

/// Validates `ack_deadline_seconds` (Subscription create/update, and
/// `ModifyAckDeadline`'s `ack_deadline_seconds`, both share the same
/// 10..=600 range).
pub fn validate_ack_deadline_secs(v: i32) -> Result<()> {
    if !(MIN_ACK_DEADLINE_SECS..=MAX_ACK_DEADLINE_SECS).contains(&v) {
        return Err(Error::InvalidArgument {
            field: "ack_deadline_seconds".to_string(),
            message: format!(
                "ack_deadline_seconds {v} is outside the allowed range {MIN_ACK_DEADLINE_SECS}..={MAX_ACK_DEADLINE_SECS}"
            ),
        });
    }
    Ok(())
}

/// Validates a Subscription's `message_retention_duration`, in seconds.
pub fn validate_subscription_retention_secs(v: i64) -> Result<()> {
    if !(MIN_SUBSCRIPTION_RETENTION_SECS..=MAX_SUBSCRIPTION_RETENTION_SECS).contains(&v) {
        return Err(Error::InvalidArgument {
            field: "message_retention_duration".to_string(),
            message: format!(
                "message_retention_duration {v}s is outside the allowed range {MIN_SUBSCRIPTION_RETENTION_SECS}..={MAX_SUBSCRIPTION_RETENTION_SECS}"
            ),
        });
    }
    Ok(())
}

/// Validates a Topic's `message_retention_duration`, in seconds.
pub fn validate_topic_retention_secs(v: i64) -> Result<()> {
    if !(MIN_TOPIC_RETENTION_SECS..=MAX_TOPIC_RETENTION_SECS).contains(&v) {
        return Err(Error::InvalidArgument {
            field: "message_retention_duration".to_string(),
            message: format!(
                "message_retention_duration {v}s is outside the allowed range {MIN_TOPIC_RETENTION_SECS}..={MAX_TOPIC_RETENTION_SECS}"
            ),
        });
    }
    Ok(())
}

/// Validates `dead_letter_policy.max_delivery_attempts`.
///
/// `0` means "unspecified": the caller substitutes the default of 5
/// elsewhere, so `0` is silently accepted here rather
/// than rejected as below the minimum.
pub fn validate_dead_letter_max_attempts(v: i32) -> Result<()> {
    if v != 0 && !(MIN_DEAD_LETTER_MAX_ATTEMPTS..=MAX_DEAD_LETTER_MAX_ATTEMPTS).contains(&v) {
        return Err(Error::InvalidArgument {
            field: "dead_letter_policy.max_delivery_attempts".to_string(),
            message: format!(
                "max_delivery_attempts {v} is outside the allowed range {MIN_DEAD_LETTER_MAX_ATTEMPTS}..={MAX_DEAD_LETTER_MAX_ATTEMPTS}"
            ),
        });
    }
    Ok(())
}

/// Validates a single `retry_policy` backoff value (`minimum_backoff` or
/// `maximum_backoff`), in seconds.
pub fn validate_retry_backoff_secs(v: i64) -> Result<()> {
    if !(MIN_RETRY_BACKOFF_SECS..=MAX_RETRY_BACKOFF_SECS).contains(&v) {
        return Err(Error::InvalidArgument {
            field: "retry_policy.backoff".to_string(),
            message: format!(
                "backoff {v}s is outside the allowed range {MIN_RETRY_BACKOFF_SECS}..={MAX_RETRY_BACKOFF_SECS}"
            ),
        });
    }
    Ok(())
}

/// Validates a Subscription `filter` string's length, measured in
/// characters (not bytes).
pub fn validate_filter_len(filter: &str) -> Result<()> {
    let char_count = filter.chars().count();
    if char_count > MAX_FILTER_CHARS {
        return Err(Error::InvalidArgument {
            field: "filter".to_string(),
            message: format!(
                "filter is {char_count} characters, exceeding maximum of {MAX_FILTER_CHARS}"
            ),
        });
    }
    Ok(())
}

/// Validates a Pull request's `max_messages`.
pub fn validate_pull_max_messages(v: i32) -> Result<()> {
    if !(MIN_PULL_MAX_MESSAGES..=MAX_PULL_MAX_MESSAGES).contains(&v) {
        return Err(Error::InvalidArgument {
            field: "max_messages".to_string(),
            message: format!(
                "max_messages {v} is outside the allowed range {MIN_PULL_MAX_MESSAGES}..={MAX_PULL_MAX_MESSAGES}"
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attrs(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    // ---- validate_message: data + attributes total size ----

    #[test]
    fn message_at_max_bytes_ok() {
        let data_len = MAX_MESSAGE_BYTES;
        assert!(validate_message(data_len, &HashMap::new(), "").is_ok());
    }

    #[test]
    fn message_over_max_bytes_errs() {
        let data_len = MAX_MESSAGE_BYTES + 1;
        assert!(matches!(
            validate_message(data_len, &HashMap::new(), ""),
            Err(Error::InvalidArgument { .. })
        ));
    }

    #[test]
    fn message_total_size_includes_attributes() {
        // data alone is within budget, but data + attribute bytes tips over.
        let attributes = attrs(&[("k", &"v".repeat(10))]);
        let attr_bytes = 1 + 10; // key "k" + 10 "v"s
        let data_len = MAX_MESSAGE_BYTES - attr_bytes; // exactly at max combined
        assert!(validate_message(data_len, &attributes, "").is_ok());
        assert!(validate_message(data_len + 1, &attributes, "").is_err());
    }

    // ---- validate_message: attribute count ----

    #[test]
    fn attribute_count_at_max_ok() {
        let attributes: HashMap<String, String> = (0..MAX_ATTRIBUTES)
            .map(|i| (format!("k{i}"), "v".to_string()))
            .collect();
        assert!(validate_message(0, &attributes, "").is_ok());
    }

    #[test]
    fn attribute_count_over_max_errs() {
        let attributes: HashMap<String, String> = (0..=MAX_ATTRIBUTES)
            .map(|i| (format!("k{i}"), "v".to_string()))
            .collect();
        assert!(matches!(
            validate_message(0, &attributes, ""),
            Err(Error::InvalidArgument { .. })
        ));
    }

    // ---- validate_message: attribute key length ----

    #[test]
    fn attribute_key_at_max_bytes_ok() {
        let key = "k".repeat(MAX_ATTRIBUTE_KEY_BYTES);
        let attributes = attrs(&[(key.as_str(), "v")]);
        assert!(validate_message(0, &attributes, "").is_ok());
    }

    #[test]
    fn attribute_key_over_max_bytes_errs() {
        let key = "k".repeat(MAX_ATTRIBUTE_KEY_BYTES + 1);
        let attributes = attrs(&[(key.as_str(), "v")]);
        assert!(matches!(
            validate_message(0, &attributes, ""),
            Err(Error::InvalidArgument { .. })
        ));
    }

    // ---- validate_message: forbidden "goog" attribute key prefix ----

    #[test]
    fn attribute_key_with_goog_prefix_errs() {
        let attributes = attrs(&[("googreserved", "v")]);
        assert!(matches!(
            validate_message(0, &attributes, ""),
            Err(Error::InvalidArgument { .. })
        ));
    }

    #[test]
    fn attribute_key_without_goog_prefix_ok() {
        let attributes = attrs(&[("mykey", "v")]);
        assert!(validate_message(0, &attributes, "").is_ok());
    }

    // ---- validate_message: attribute value length ----

    #[test]
    fn attribute_value_at_max_bytes_ok() {
        let value = "v".repeat(MAX_ATTRIBUTE_VALUE_BYTES);
        let attributes = attrs(&[("k", value.as_str())]);
        assert!(validate_message(0, &attributes, "").is_ok());
    }

    #[test]
    fn attribute_value_over_max_bytes_errs() {
        let value = "v".repeat(MAX_ATTRIBUTE_VALUE_BYTES + 1);
        let attributes = attrs(&[("k", value.as_str())]);
        assert!(matches!(
            validate_message(0, &attributes, ""),
            Err(Error::InvalidArgument { .. })
        ));
    }

    // ---- validate_message: ordering_key length ----

    #[test]
    fn ordering_key_at_max_bytes_ok() {
        let key = "k".repeat(MAX_ORDERING_KEY_BYTES);
        assert!(validate_message(0, &HashMap::new(), &key).is_ok());
    }

    #[test]
    fn ordering_key_over_max_bytes_errs() {
        let key = "k".repeat(MAX_ORDERING_KEY_BYTES + 1);
        assert!(matches!(
            validate_message(0, &HashMap::new(), &key),
            Err(Error::InvalidArgument { .. })
        ));
    }

    // ---- validate_publish_batch ----

    #[test]
    fn publish_batch_message_count_at_max_ok() {
        assert!(validate_publish_batch(MAX_PUBLISH_BATCH_MESSAGES, 0).is_ok());
    }

    #[test]
    fn publish_batch_message_count_over_max_errs() {
        assert!(matches!(
            validate_publish_batch(MAX_PUBLISH_BATCH_MESSAGES + 1, 0),
            Err(Error::InvalidArgument { .. })
        ));
    }

    #[test]
    fn publish_batch_bytes_at_max_ok() {
        assert!(validate_publish_batch(0, MAX_PUBLISH_BATCH_BYTES).is_ok());
    }

    #[test]
    fn publish_batch_bytes_over_max_errs() {
        assert!(matches!(
            validate_publish_batch(0, MAX_PUBLISH_BATCH_BYTES + 1),
            Err(Error::InvalidArgument { .. })
        ));
    }

    // ---- validate_ack_deadline_secs ----

    #[test]
    fn ack_deadline_min_ok() {
        assert!(validate_ack_deadline_secs(MIN_ACK_DEADLINE_SECS).is_ok());
    }

    #[test]
    fn ack_deadline_below_min_errs() {
        assert!(matches!(
            validate_ack_deadline_secs(MIN_ACK_DEADLINE_SECS - 1),
            Err(Error::InvalidArgument { .. })
        ));
    }

    #[test]
    fn ack_deadline_max_ok() {
        assert!(validate_ack_deadline_secs(MAX_ACK_DEADLINE_SECS).is_ok());
    }

    #[test]
    fn ack_deadline_above_max_errs() {
        assert!(matches!(
            validate_ack_deadline_secs(MAX_ACK_DEADLINE_SECS + 1),
            Err(Error::InvalidArgument { .. })
        ));
    }

    // ---- validate_subscription_retention_secs ----

    #[test]
    fn subscription_retention_min_ok() {
        assert!(validate_subscription_retention_secs(MIN_SUBSCRIPTION_RETENTION_SECS).is_ok());
    }

    #[test]
    fn subscription_retention_below_min_errs() {
        assert!(matches!(
            validate_subscription_retention_secs(MIN_SUBSCRIPTION_RETENTION_SECS - 1),
            Err(Error::InvalidArgument { .. })
        ));
    }

    #[test]
    fn subscription_retention_max_ok() {
        assert!(validate_subscription_retention_secs(MAX_SUBSCRIPTION_RETENTION_SECS).is_ok());
    }

    #[test]
    fn subscription_retention_above_max_errs() {
        assert!(matches!(
            validate_subscription_retention_secs(MAX_SUBSCRIPTION_RETENTION_SECS + 1),
            Err(Error::InvalidArgument { .. })
        ));
    }

    // ---- validate_topic_retention_secs ----

    #[test]
    fn topic_retention_min_ok() {
        assert!(validate_topic_retention_secs(MIN_TOPIC_RETENTION_SECS).is_ok());
    }

    #[test]
    fn topic_retention_below_min_errs() {
        assert!(matches!(
            validate_topic_retention_secs(MIN_TOPIC_RETENTION_SECS - 1),
            Err(Error::InvalidArgument { .. })
        ));
    }

    #[test]
    fn topic_retention_max_ok() {
        assert!(validate_topic_retention_secs(MAX_TOPIC_RETENTION_SECS).is_ok());
    }

    #[test]
    fn topic_retention_above_max_errs() {
        assert!(matches!(
            validate_topic_retention_secs(MAX_TOPIC_RETENTION_SECS + 1),
            Err(Error::InvalidArgument { .. })
        ));
    }

    // ---- validate_dead_letter_max_attempts ----

    #[test]
    fn dead_letter_zero_is_unspecified_and_ok() {
        assert!(validate_dead_letter_max_attempts(0).is_ok());
    }

    #[test]
    fn dead_letter_min_ok() {
        assert!(validate_dead_letter_max_attempts(MIN_DEAD_LETTER_MAX_ATTEMPTS).is_ok());
    }

    #[test]
    fn dead_letter_below_min_nonzero_errs() {
        assert!(matches!(
            validate_dead_letter_max_attempts(MIN_DEAD_LETTER_MAX_ATTEMPTS - 1),
            Err(Error::InvalidArgument { .. })
        ));
    }

    #[test]
    fn dead_letter_max_ok() {
        assert!(validate_dead_letter_max_attempts(MAX_DEAD_LETTER_MAX_ATTEMPTS).is_ok());
    }

    #[test]
    fn dead_letter_above_max_errs() {
        assert!(matches!(
            validate_dead_letter_max_attempts(MAX_DEAD_LETTER_MAX_ATTEMPTS + 1),
            Err(Error::InvalidArgument { .. })
        ));
    }

    // ---- validate_retry_backoff_secs ----

    #[test]
    fn retry_backoff_min_ok() {
        assert!(validate_retry_backoff_secs(MIN_RETRY_BACKOFF_SECS).is_ok());
    }

    #[test]
    fn retry_backoff_below_min_errs() {
        assert!(matches!(
            validate_retry_backoff_secs(MIN_RETRY_BACKOFF_SECS - 1),
            Err(Error::InvalidArgument { .. })
        ));
    }

    #[test]
    fn retry_backoff_max_ok() {
        assert!(validate_retry_backoff_secs(MAX_RETRY_BACKOFF_SECS).is_ok());
    }

    #[test]
    fn retry_backoff_above_max_errs() {
        assert!(matches!(
            validate_retry_backoff_secs(MAX_RETRY_BACKOFF_SECS + 1),
            Err(Error::InvalidArgument { .. })
        ));
    }

    // ---- validate_filter_len ----

    #[test]
    fn filter_at_max_chars_ok() {
        let filter = "a".repeat(MAX_FILTER_CHARS);
        assert!(validate_filter_len(&filter).is_ok());
    }

    #[test]
    fn filter_over_max_chars_errs() {
        let filter = "a".repeat(MAX_FILTER_CHARS + 1);
        assert!(matches!(
            validate_filter_len(&filter),
            Err(Error::InvalidArgument { .. })
        ));
    }

    #[test]
    fn filter_len_counts_chars_not_bytes() {
        // "€" is 3 bytes in UTF-8 but 1 char; MAX_FILTER_CHARS of them
        // should be accepted even though the byte length is much larger.
        let filter = "€".repeat(MAX_FILTER_CHARS);
        assert!(validate_filter_len(&filter).is_ok());
        let filter_over = "€".repeat(MAX_FILTER_CHARS + 1);
        assert!(validate_filter_len(&filter_over).is_err());
    }

    // ---- validate_pull_max_messages ----

    #[test]
    fn pull_max_messages_min_ok() {
        assert!(validate_pull_max_messages(MIN_PULL_MAX_MESSAGES).is_ok());
    }

    #[test]
    fn pull_max_messages_below_min_errs() {
        assert!(matches!(
            validate_pull_max_messages(MIN_PULL_MAX_MESSAGES - 1),
            Err(Error::InvalidArgument { .. })
        ));
    }

    #[test]
    fn pull_max_messages_max_ok() {
        assert!(validate_pull_max_messages(MAX_PULL_MAX_MESSAGES).is_ok());
    }

    #[test]
    fn pull_max_messages_above_max_errs() {
        assert!(matches!(
            validate_pull_max_messages(MAX_PULL_MAX_MESSAGES + 1),
            Err(Error::InvalidArgument { .. })
        ));
    }
}
