//! Builds the REST `axum::Router` for the REST (HTTP/JSON) subset of the API.
//!
//! Design note (plan.md D5): axum 0.8's router (`matchit` under the hood)
//! does **not** allow a path parameter followed by literal text in the
//! same segment — `/topics/{topic}:publish` panics at route-registration
//! time with "Only one parameter is allowed per path segment"
//! (`router_builds_without_panicking` below pins this down; it was the
//! first design tried and failed exactly this way). So every
//! `POST .../{resource}:verb` endpoint is registered on the *plain*
//! `{resource}` path pattern, and `handlers::{topic_post,
//! subscription_post}` split the captured segment on `:` themselves —
//! see `handlers::split_verb`.

use std::sync::Arc;

use axum::routing::{get, put};
use axum::Router;
use open_pubusb_core::service::PubSubService;
use open_pubusb_core::store::kv::KvStore;

use crate::rest::handlers;

pub fn router<K: KvStore + 'static>(svc: Arc<PubSubService<K>>) -> Router {
    Router::new()
        .route(
            "/v1/projects/{project}/topics/{topic}",
            put(handlers::create_topic::<K>)
                .get(handlers::get_topic::<K>)
                .delete(handlers::delete_topic::<K>)
                .post(handlers::topic_post::<K>),
        )
        .route(
            "/v1/projects/{project}/topics",
            get(handlers::list_topics::<K>),
        )
        .route(
            "/v1/projects/{project}/subscriptions/{sub}",
            put(handlers::create_subscription::<K>)
                .get(handlers::get_subscription::<K>)
                .delete(handlers::delete_subscription::<K>)
                .post(handlers::subscription_post::<K>),
        )
        .route(
            "/v1/projects/{project}/subscriptions",
            get(handlers::list_subscriptions::<K>),
        )
        .with_state(svc)
    // No `.fallback(...)` here: this router gets `.merge()`d with the gRPC
    // `Routes::into_axum_router()` in `crates/open-pubusb/src/server.rs`, and
    // axum panics ("Cannot merge two `Router`s that both have a
    // fallback") if both sides set one — that panic surfaced for real the
    // first time this server was actually started, not just
    // `cargo check`ed, which is exactly why plan.md's rule to smoke-test
    // real behavior (not just tests) matters. The 501 fallback is applied
    // once, after the merge, in `server.rs`.
}

#[cfg(test)]
mod tests {
    use super::*;
    use open_pubusb_core::store::kv::MemKv;

    #[test]
    fn router_builds_without_panicking() {
        let svc = Arc::new(PubSubService::new_ephemeral());
        let _router: Router = router::<MemKv>(svc);
    }
}
