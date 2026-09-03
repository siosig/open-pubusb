//! Resource name newtypes and validation.
//!
//! Pub/Sub resources are addressed by their *full* resource path, e.g.
//! `projects/{project}/topics/{topic}`. Each newtype in this module wraps
//! the complete path (never just the trailing id) so that callers cannot
//! accidentally mix up a bare id with a full name.

use std::fmt;

use crate::error::{Error, Result};

/// Sentinel value used as a subscription's `topic` field once the topic it
/// pointed at has been deleted. It is not a real resource path and is
/// always considered a valid [`TopicName`].
pub const DELETED_TOPIC: &str = "_deleted-topic_";

const MIN_ID_LEN: usize = 3;
const MAX_ID_LEN: usize = 255;
const FORBIDDEN_ID_PREFIX: &str = "goog";

fn is_valid_id_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '~' | '+' | '%')
}

/// Validates a bare resource id (the trailing `{id}` segment of a full
/// name): 3-255 chars, starts with an ASCII letter, only
/// `[A-Za-z0-9\-_.~+%]`, and must not start with the literal `goog` prefix.
fn validate_resource_id(id: &str) -> Result<()> {
    let len = id.chars().count();
    if !(MIN_ID_LEN..=MAX_ID_LEN).contains(&len) {
        return Err(Error::InvalidArgument {
            field: "name".into(),
            message: format!("resource id must be {MIN_ID_LEN}-{MAX_ID_LEN} characters, got {len}"),
        });
    }
    let first = match id.chars().next() {
        Some(c) => c,
        None => {
            return Err(Error::InvalidArgument {
                field: "name".into(),
                message: "resource id must not be empty".into(),
            });
        }
    };
    if !first.is_ascii_alphabetic() {
        return Err(Error::InvalidArgument {
            field: "name".into(),
            message: "resource id must start with an ASCII letter".into(),
        });
    }
    if !id.chars().all(is_valid_id_char) {
        return Err(Error::InvalidArgument {
            field: "name".into(),
            message: "resource id must only contain [A-Za-z0-9-_.~+%]".into(),
        });
    }
    if id.starts_with(FORBIDDEN_ID_PREFIX) {
        return Err(Error::InvalidArgument {
            field: "name".into(),
            message: format!("resource id must not start with \"{FORBIDDEN_ID_PREFIX}\""),
        });
    }
    Ok(())
}

/// Splits and validates the structural shape of a full resource name of the
/// form `projects/{project}/{kind}/{id}`, returning the trailing `{id}`
/// segment. Does not validate the id's character set — callers should
/// follow up with [`validate_resource_id`].
fn parse_full_name<'a>(full_name: &'a str, kind: &str) -> Result<&'a str> {
    let parts: Vec<&str> = full_name.split('/').collect();
    if parts.len() != 4 || parts[0] != "projects" || parts[2] != kind {
        return Err(Error::InvalidArgument {
            field: "name".into(),
            message: format!(
                "name must match projects/{{project}}/{kind}/{{id}}, got \"{full_name}\""
            ),
        });
    }
    let project_id = parts[1];
    let id = parts[3];
    if project_id.is_empty() {
        return Err(Error::InvalidArgument {
            field: "name".into(),
            message: "project_id must not be empty".into(),
        });
    }
    Ok(id)
}

/// Splits an already-validated `projects/{project}/{kind}/{id}` string into
/// its `(project_id, id)` parts. Falls back to empty strings if the shape
/// is unexpected rather than panicking; every constructor in this module
/// validates the shape before storing the string, so that fallback is not
/// expected to be exercised in practice.
fn split_full_name(full_name: &str) -> (&str, &str) {
    let mut parts = full_name.splitn(4, '/');
    let _projects = parts.next();
    let project_id = parts.next().unwrap_or("");
    let _kind = parts.next();
    let id = parts.next().unwrap_or("");
    (project_id, id)
}

macro_rules! resource_name_newtype {
    ($(#[$meta:meta])* $name:ident, $kind:literal) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq, Hash)]
        pub struct $name(String);

        impl $name {
            /// Parses and validates a full resource name of the form
            /// `projects/{project}/
            #[doc = $kind]
            /// /{id}`.
            pub fn parse(full_name: &str) -> Result<Self> {
                let id = parse_full_name(full_name, $kind)?;
                validate_resource_id(id)?;
                Ok(Self(full_name.to_string()))
            }

            /// The full resource path, e.g. `projects/p/{kind}/id`.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// The `{project}` segment of the full name.
            pub fn project_id(&self) -> &str {
                split_full_name(&self.0).0
            }

            /// The trailing `{id}` segment of the full name.
            pub fn resource_id(&self) -> &str {
                split_full_name(&self.0).1
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

resource_name_newtype!(
    /// A fully-qualified subscription name: `projects/{project}/subscriptions/{subscription}`.
    SubscriptionName,
    "subscriptions"
);
resource_name_newtype!(
    /// A fully-qualified snapshot name: `projects/{project}/snapshots/{snapshot}`.
    SnapshotName,
    "snapshots"
);

/// A fully-qualified topic name: `projects/{project}/topics/{topic}`.
///
/// [`DELETED_TOPIC`] is also a valid value: it is the sentinel a
/// subscription's `topic` field takes on once its topic has been deleted.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TopicName(String);

impl TopicName {
    /// Parses and validates a full topic name, or accepts the
    /// [`DELETED_TOPIC`] sentinel as-is.
    pub fn parse(full_name: &str) -> Result<Self> {
        if full_name == DELETED_TOPIC {
            return Ok(Self(full_name.to_string()));
        }
        let id = parse_full_name(full_name, "topics")?;
        validate_resource_id(id)?;
        Ok(Self(full_name.to_string()))
    }

    /// The full resource path, or [`DELETED_TOPIC`].
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// True if this is the `_deleted-topic_` sentinel rather than a real
    /// topic name.
    pub fn is_deleted_sentinel(&self) -> bool {
        self.0 == DELETED_TOPIC
    }

    /// The `{project}` segment of the full name, or `""` for the
    /// [`DELETED_TOPIC`] sentinel.
    pub fn project_id(&self) -> &str {
        if self.is_deleted_sentinel() {
            ""
        } else {
            split_full_name(&self.0).0
        }
    }

    /// The trailing `{id}` segment of the full name, or [`DELETED_TOPIC`]
    /// itself for the sentinel value.
    pub fn resource_id(&self) -> &str {
        if self.is_deleted_sentinel() {
            DELETED_TOPIC
        } else {
            split_full_name(&self.0).1
        }
    }
}

impl fmt::Display for TopicName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// An implicit project identifier: `projects/{project_id}`.
///
/// Projects have no create/delete API and no character-set validation
/// beyond non-emptiness.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProjectId(String);

impl ProjectId {
    /// Parses `projects/{project_id}`, requiring a non-empty `project_id`.
    pub fn parse(full_name: &str) -> Result<Self> {
        let parts: Vec<&str> = full_name.split('/').collect();
        if parts.len() != 2 || parts[0] != "projects" {
            return Err(Error::InvalidArgument {
                field: "name".into(),
                message: format!("name must match projects/{{project_id}}, got \"{full_name}\""),
            });
        }
        if parts[1].is_empty() {
            return Err(Error::InvalidArgument {
                field: "name".into(),
                message: "project_id must not be empty".into(),
            });
        }
        Ok(Self(full_name.to_string()))
    }

    /// The full resource path: `projects/{project_id}`.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The `{project_id}` segment of the full name.
    pub fn project_id(&self) -> &str {
        self.0.strip_prefix("projects/").unwrap_or("")
    }

    /// The `{project_id}` segment of the full name (same as
    /// [`ProjectId::project_id`] — a `ProjectId`'s "resource" is the
    /// project itself).
    pub fn resource_id(&self) -> &str {
        self.project_id()
    }
}

impl fmt::Display for ProjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn topic_name_valid() {
        let n = TopicName::parse("projects/my-proj/topics/my-topic").unwrap();
        assert_eq!(n.as_str(), "projects/my-proj/topics/my-topic");
        assert_eq!(n.project_id(), "my-proj");
        assert_eq!(n.resource_id(), "my-topic");
        assert!(!n.is_deleted_sentinel());
    }

    #[test]
    fn topic_name_deleted_sentinel() {
        let n = TopicName::parse(DELETED_TOPIC).unwrap();
        assert_eq!(n.as_str(), DELETED_TOPIC);
        assert!(n.is_deleted_sentinel());
        assert_eq!(n.resource_id(), DELETED_TOPIC);
        assert_eq!(n.project_id(), "");
    }

    #[test]
    fn subscription_name_valid() {
        let n = SubscriptionName::parse("projects/p/subscriptions/sub.1-2_3~4+5%36").unwrap();
        assert_eq!(n.project_id(), "p");
        assert_eq!(n.resource_id(), "sub.1-2_3~4+5%36");
    }

    #[test]
    fn snapshot_name_valid() {
        let n = SnapshotName::parse("projects/p/snapshots/snap-abc").unwrap();
        assert_eq!(n.project_id(), "p");
        assert_eq!(n.resource_id(), "snap-abc");
    }

    #[test]
    fn project_id_valid() {
        let p = ProjectId::parse("projects/my-proj").unwrap();
        assert_eq!(p.as_str(), "projects/my-proj");
        assert_eq!(p.project_id(), "my-proj");
        assert_eq!(p.resource_id(), "my-proj");
    }

    #[test]
    fn project_id_empty_rejected() {
        assert!(ProjectId::parse("projects/").is_err());
        assert!(ProjectId::parse("projects").is_err());
        assert!(ProjectId::parse("not-projects/p").is_err());
    }

    #[test]
    fn rejects_wrong_kind_segment() {
        assert!(TopicName::parse("projects/p/subscriptions/t").is_err());
        assert!(SubscriptionName::parse("projects/p/topics/s").is_err());
        assert!(SnapshotName::parse("projects/p/topics/s").is_err());
    }

    #[test]
    fn rejects_missing_or_extra_segments() {
        assert!(TopicName::parse("projects/p/topics").is_err());
        assert!(TopicName::parse("topics/t").is_err());
        assert!(TopicName::parse("projects/p/topics/a/b").is_err());
        assert!(TopicName::parse("projects//topics/t").is_err());
    }

    #[test]
    fn rejects_id_too_short() {
        assert!(TopicName::parse("projects/p/topics/ab").is_err());
    }

    #[test]
    fn rejects_id_too_long() {
        let id: String = std::iter::once('a')
            .chain(std::iter::repeat_n('b', 255))
            .collect();
        assert_eq!(id.len(), 256);
        let full = format!("projects/p/topics/{id}");
        assert!(TopicName::parse(&full).is_err());
    }

    #[test]
    fn accepts_id_at_length_bounds() {
        let min_id = "abc"; // 3 chars
        assert!(TopicName::parse(&format!("projects/p/topics/{min_id}")).is_ok());

        let max_id: String = std::iter::once('a')
            .chain(std::iter::repeat_n('b', 254))
            .collect();
        assert_eq!(max_id.len(), 255);
        assert!(TopicName::parse(&format!("projects/p/topics/{max_id}")).is_ok());
    }

    #[test]
    fn rejects_id_not_starting_with_letter() {
        assert!(TopicName::parse("projects/p/topics/1abc").is_err());
        assert!(TopicName::parse("projects/p/topics/-abc").is_err());
        assert!(TopicName::parse("projects/p/topics/_abc").is_err());
    }

    #[test]
    fn rejects_invalid_characters() {
        assert!(TopicName::parse("projects/p/topics/abc def").is_err());
        assert!(TopicName::parse("projects/p/topics/abc/def").is_err());
        assert!(TopicName::parse("projects/p/topics/abc$def").is_err());
        assert!(TopicName::parse("projects/p/topics/abc*def").is_err());
    }

    #[test]
    fn rejects_goog_prefix_case_sensitive() {
        assert!(TopicName::parse("projects/p/topics/googtopic").is_err());
        // Case-sensitive: "Goog..." is not the forbidden literal prefix.
        assert!(TopicName::parse("projects/p/topics/Googtopic").is_ok());
    }

    #[test]
    fn accepts_full_valid_charset() {
        let id = "abc-DEF_123.4~5+6%37";
        let full = format!("projects/p/topics/{id}");
        let n = TopicName::parse(&full).unwrap();
        assert_eq!(n.resource_id(), id);
    }

    #[test]
    fn display_matches_as_str() {
        let n = TopicName::parse("projects/p/topics/abc").unwrap();
        assert_eq!(n.to_string(), n.as_str());
    }

    #[test]
    fn equality_and_hash() {
        use std::collections::HashSet;
        let a = TopicName::parse("projects/p/topics/abc").unwrap();
        let b = TopicName::parse("projects/p/topics/abc").unwrap();
        assert_eq!(a, b);
        let mut set = HashSet::new();
        set.insert(a);
        assert!(set.contains(&b));
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    fn id_char() -> impl Strategy<Value = char> {
        prop_oneof![
            Just('-'),
            Just('_'),
            Just('.'),
            Just('~'),
            Just('+'),
            Just('%'),
            proptest::char::range('a', 'z'),
            proptest::char::range('A', 'Z'),
            proptest::char::range('0', '9'),
        ]
    }

    /// An ASCII letter that is never 'g'/'G', so a string starting with it
    /// can never begin with the forbidden "goog" prefix.
    fn first_char() -> impl Strategy<Value = char> {
        prop_oneof![
            proptest::char::range('a', 'f'),
            proptest::char::range('h', 'z'),
            proptest::char::range('A', 'F'),
            proptest::char::range('H', 'Z'),
        ]
    }

    /// A valid resource id: length 3-255, ASCII-letter first char (never
    /// "goog"-prefixed), remaining chars from the allowed alphabet.
    fn valid_id() -> impl Strategy<Value = String> {
        (first_char(), proptest::collection::vec(id_char(), 2..254)).prop_map(|(first, rest)| {
            let mut s = String::with_capacity(rest.len() + 1);
            s.push(first);
            s.extend(rest);
            s
        })
    }

    proptest! {
        #[test]
        fn topic_name_roundtrips(id in valid_id()) {
            prop_assert!(id.chars().count() >= 3 && id.chars().count() <= 255);
            let full = format!("projects/proj-1/topics/{id}");
            let parsed = TopicName::parse(&full)?;
            prop_assert_eq!(parsed.as_str(), full.as_str());
            prop_assert_eq!(parsed.project_id(), "proj-1");
            prop_assert_eq!(parsed.resource_id(), id.as_str());
        }

        #[test]
        fn subscription_name_roundtrips(id in valid_id()) {
            let full = format!("projects/proj-1/subscriptions/{id}");
            let parsed = SubscriptionName::parse(&full)?;
            prop_assert_eq!(parsed.as_str(), full.as_str());
            prop_assert_eq!(parsed.project_id(), "proj-1");
            prop_assert_eq!(parsed.resource_id(), id.as_str());
        }

        #[test]
        fn snapshot_name_roundtrips(id in valid_id()) {
            let full = format!("projects/proj-1/snapshots/{id}");
            let parsed = SnapshotName::parse(&full)?;
            prop_assert_eq!(parsed.as_str(), full.as_str());
            prop_assert_eq!(parsed.project_id(), "proj-1");
            prop_assert_eq!(parsed.resource_id(), id.as_str());
        }
    }
}
