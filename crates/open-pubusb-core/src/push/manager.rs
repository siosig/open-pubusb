//! Reconciles running push dispatchers against subscriptions' current
//! `push_config`, starting or stopping a dispatcher on
//! create/modify_push_config/delete.
//!
//! Deliberately reconciliation-based (periodic `reconcile()` calls, e.g.
//! from a `crates/open-pubusb/src/main.rs` timer) rather than an event hook
//! wired into every `PubSubService` mutation method: `PubSubService`
//! stays a plain domain type with no operational/task-spawning
//! responsibilities, and reconciliation is naturally idempotent (a no-op
//! if nothing changed) and self-healing (recovers from e.g. a dispatcher
//! task that panicked without needing dedicated crash-recovery logic —
//! the next `reconcile()` just restarts it).

use std::collections::HashMap;
use std::sync::Arc;

use crate::push::dispatcher::{self, DispatcherHandle};
use crate::service::PubSubService;
use crate::store::kv::KvStore;
use crate::subscription::PushConfig;

struct Entry {
    push_config: PushConfig,
    handle: DispatcherHandle,
}

/// Owns every currently-running push dispatcher, keyed by subscription id.
pub struct PushManager {
    entries: HashMap<u64, Entry>,
    push_timeout_secs: u64,
    push_max_concurrency_per_sub: u32,
}

impl PushManager {
    /// An empty manager (no dispatchers running yet) with the given
    /// per-request timeout and per-subscription concurrency cap applied
    /// to every dispatcher it starts.
    pub fn new(push_timeout_secs: u64, push_max_concurrency_per_sub: u32) -> Self {
        Self {
            entries: HashMap::new(),
            push_timeout_secs,
            push_max_concurrency_per_sub,
        }
    }

    /// Compares every subscription's current `push_config` against what's
    /// running and starts/stops/restarts dispatchers to match. Safe (and
    /// cheap when nothing changed) to call on a fixed interval.
    pub async fn reconcile<K: KvStore + 'static>(&mut self, svc: &Arc<PubSubService<K>>) {
        let subs = svc.list_all_subscriptions();
        let mut seen = std::collections::HashSet::with_capacity(subs.len());

        for sub in subs {
            seen.insert(sub.id);
            let needs_restart = match (&sub.push_config, self.entries.get(&sub.id)) {
                (Some(cfg), Some(entry)) => &entry.push_config != cfg,
                (Some(_), None) => true,
                (None, Some(_)) => true, // push disabled -> stop, don't restart
                (None, None) => false,
            };
            if !needs_restart {
                continue;
            }
            if let Some(entry) = self.entries.remove(&sub.id) {
                entry.handle.stop().await;
            }
            if let Some(cfg) = sub.push_config {
                let handle = dispatcher::spawn(
                    svc.clone(),
                    sub.name.clone(),
                    sub.id,
                    sub.ack_deadline_secs,
                    sub.min_retry_backoff_secs,
                    sub.max_retry_backoff_secs,
                    sub.dead_letter_topic.is_some(),
                    cfg.clone(),
                    self.push_timeout_secs,
                    self.push_max_concurrency_per_sub,
                );
                tracing::info!(subscription = %sub.name, endpoint = %cfg.endpoint, "push dispatcher started");
                self.entries.insert(
                    sub.id,
                    Entry {
                        push_config: cfg,
                        handle,
                    },
                );
            }
        }

        let stale: Vec<u64> = self
            .entries
            .keys()
            .filter(|id| !seen.contains(id))
            .copied()
            .collect();
        for id in stale {
            if let Some(entry) = self.entries.remove(&id) {
                entry.handle.stop().await;
            }
        }
    }

    /// Stops every running dispatcher. Call once, on shutdown.
    pub async fn shutdown(&mut self) {
        for (_, entry) in self.entries.drain() {
            entry.handle.stop().await;
        }
    }
}
