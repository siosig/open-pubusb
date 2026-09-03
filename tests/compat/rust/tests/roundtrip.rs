//! Compatibility test: proves community/official Rust Pub/Sub client
//! crates work against `open-pubusb` unmodified, switching only the
//! connection target — no application code change required.
//!
//! Two crates are exercised:
//!   - `gcloud-pubsub` (yoshidan) — reads `PUBSUB_EMULATOR_HOST` itself via
//!     `ClientConfig::default()`, exactly like the Python/Node official
//!     clients do. This is the primary path and covers the full
//!     publish/pull/ack round trip.
//!   - Google's official `google-cloud-pubsub` crate — does NOT read
//!     `PUBSUB_EMULATOR_HOST` (verified by inspecting its source), so this
//!     test builds each client's endpoint from the same
//!     env var explicitly instead, with anonymous credentials. This
//!     crate's design splits admin (`TopicAdmin`/`SubscriptionAdmin`) from
//!     data-plane (`Publisher`/`Subscriber`) clients, each independently
//!     configured — so this secondary/smoke path exercises topic and
//!     subscription CRUD only (the part every one of those clients shares
//!     the same simple endpoint-override construction for); the full
//!     publish/pull round trip is already covered by the primary path,
//!     Python, and Node.js.
//!
//! Requires `open-pubusb` to be running with `PUBSUB_EMULATOR_HOST` pointed at
//! it, e.g.:
//!
//!   open-pubusb serve --ephemeral --listen 127.0.0.1:8085 --admin-listen 127.0.0.1:8086 &
//!   PUBSUB_EMULATOR_HOST=127.0.0.1:8085 cargo test
//!
//! Every test returns early (treated as a pass, printing a notice) when
//! `PUBSUB_EMULATOR_HOST` is unset.

fn emulator_host() -> Option<String> {
    std::env::var("PUBSUB_EMULATOR_HOST").ok()
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}-{}", &uuid::Uuid::new_v4().simple().to_string()[..12])
}

macro_rules! skip_without_emulator {
    () => {
        let Some(_host) = emulator_host() else {
            eprintln!(
                "PUBSUB_EMULATOR_HOST is not set; start open-pubusb and set it to run this suite"
            );
            return;
        };
    };
}

mod gcloud_pubsub_primary {
    //! Primary path: `gcloud-pubsub` (yoshidan), which reads
    //! `PUBSUB_EMULATOR_HOST` on its own via `ClientConfig::default()` —
    //! no endpoint plumbing in this test at all, matching how the
    //! Python/Node compat tests use their official clients.

    use super::*;
    use gcloud_googleapis::pubsub::v1::PubsubMessage;
    use gcloud_pubsub::client::{Client, ClientConfig};
    use gcloud_pubsub::publisher::PublisherConfig;
    use gcloud_pubsub::subscription::SubscriptionConfig;
    use std::collections::HashMap;

    async fn client() -> Client {
        let config = ClientConfig::default()
            .with_auth()
            .await
            .unwrap_or_else(|_| ClientConfig::default());
        Client::new(config).await.expect(
            "Client::new must succeed against open-pubusb with PUBSUB_EMULATOR_HOST set",
        )
    }

    #[tokio::test]
    async fn publish_pull_ack_round_trip() {
        skip_without_emulator!();
        let client = client().await;

        let topic = client.topic(&unique_name("topic"));
        topic.create(None, None).await.expect("create_topic");

        let subscription = client.subscription(&unique_name("sub"));
        subscription
            .create(topic.fully_qualified_name(), SubscriptionConfig::default(), None)
            .await
            .expect("create_subscription");

        let mut publisher = topic.new_publisher(Some(PublisherConfig::default()));
        let mut attributes = HashMap::new();
        attributes.insert("origin".to_string(), "compat-test".to_string());
        let awaiter = publisher
            .publish(PubsubMessage {
                data: b"hello from rust".to_vec(),
                attributes,
                ..Default::default()
            })
            .await;
        let message_id = awaiter.get().await.expect("publish result");
        assert!(!message_id.is_empty());

        let received = subscription.pull(10, None).await.expect("pull");
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].message.data, b"hello from rust");
        assert_eq!(
            received[0].message.attributes.get("origin"),
            Some(&"compat-test".to_string())
        );

        subscription
            .ack(vec![received[0].ack_id.clone()])
            .await
            .expect("ack");

        let again = subscription.pull(10, None).await.expect("pull again");
        assert!(again.is_empty(), "acked message must not be redelivered");

        subscription.delete(None).await.expect("delete_subscription");
        topic.delete(None).await.expect("delete_topic");
        publisher.shutdown().await;
    }

    #[tokio::test]
    async fn create_duplicate_topic_is_already_exists() {
        skip_without_emulator!();
        let client = client().await;
        let topic = client.topic(&unique_name("topic"));
        topic.create(None, None).await.expect("create_topic");

        let err = topic
            .create(None, None)
            .await
            .expect_err("duplicate create must fail");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("AlreadyExists") || msg.contains("already exists"),
            "expected AlreadyExists, got: {msg}"
        );

        topic.delete(None).await.expect("delete_topic");
    }
}

mod google_cloud_pubsub_secondary {
    //! Secondary/smoke path: Google's official `google-cloud-pubsub`
    //! crate. It does not read `PUBSUB_EMULATOR_HOST`, so the endpoint is
    //! built from it explicitly here, with anonymous
    //! credentials — this is the one piece of "connection wiring" code
    //! this test intentionally contains, not an application-level change.
    //! Scope: topic/subscription CRUD via `TopicAdmin`/`SubscriptionAdmin`
    //! (see this module's doc comment above for why publish/pull aren't
    //! also exercised through this particular crate).

    use super::*;
    use google_cloud_auth::credentials::anonymous;
    use google_cloud_pubsub::client::{SubscriptionAdmin, TopicAdmin};
    use google_cloud_pubsub::model::{Subscription, Topic};

    fn endpoint() -> Option<String> {
        emulator_host().map(|h| format!("http://{h}"))
    }

    async fn topic_admin() -> Option<TopicAdmin> {
        let endpoint = endpoint()?;
        Some(
            TopicAdmin::builder()
                .with_endpoint(endpoint)
                .with_credentials(anonymous::Builder::new().build())
                .build()
                .await
                .expect("TopicAdmin::builder().build() must succeed against open-pubusb"),
        )
    }

    async fn subscription_admin() -> Option<SubscriptionAdmin> {
        let endpoint = endpoint()?;
        Some(
            SubscriptionAdmin::builder()
                .with_endpoint(endpoint)
                .with_credentials(anonymous::Builder::new().build())
                .build()
                .await
                .expect("SubscriptionAdmin::builder().build() must succeed against open-pubusb"),
        )
    }

    #[tokio::test]
    async fn create_get_delete_topic_and_subscription() {
        skip_without_emulator!();
        let Some(topics) = topic_admin().await else {
            return;
        };
        let Some(subs) = subscription_admin().await else {
            return;
        };

        let project = std::env::var("PUBSUB_PROJECT_ID").unwrap_or_else(|_| "compat-rust".into());
        let topic_name = format!("projects/{project}/topics/{}", unique_name("topic"));
        let sub_name = format!("projects/{project}/subscriptions/{}", unique_name("sub"));

        topics
            .create_topic()
            .set_name(&topic_name)
            .send()
            .await
            .expect("create_topic");

        let fetched = topics
            .get_topic()
            .set_topic(&topic_name)
            .send()
            .await
            .expect("get_topic");
        assert_eq!(fetched.name, topic_name);

        subs.create_subscription()
            .set_name(&sub_name)
            .set_topic(&topic_name)
            .send()
            .await
            .expect("create_subscription");

        let fetched_sub = subs
            .get_subscription()
            .set_subscription(&sub_name)
            .send()
            .await
            .expect("get_subscription");
        assert_eq!(fetched_sub.name, sub_name);
        assert_eq!(fetched_sub.topic, topic_name);

        subs.delete_subscription()
            .set_subscription(&sub_name)
            .send()
            .await
            .expect("delete_subscription");
        topics
            .delete_topic()
            .set_topic(&topic_name)
            .send()
            .await
            .expect("delete_topic");
    }

    #[allow(dead_code)]
    fn types_are_referenced(_t: Topic, _s: Subscription) {}
}
