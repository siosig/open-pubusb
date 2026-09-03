//! Push delivery dispatcher: one background task per
//! push-configured subscription, leasing messages through the same
//! internal-stream machinery `StreamingPull` uses
//! ([`PubSubService::open_stream`]/[`PubSubService::lease_for_stream`])
//! and POSTing each to its `push_endpoint`, acknowledging on success and
//! extending the ack deadline (exponential backoff) on failure so the
//! self-healing lease reclaim in [`crate::delivery::engine::DeliveryEngine`]
//! redelivers it.
//!
//! Success codes: `102/200/201/202/204`, per the proto contract (this is
//! *not* "any 2xx" — e.g. 203/205/206 are not on the documented list, so
//! they count as failures needing a retry).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::push::envelope;
use crate::service::{PubSubService, PulledMessage};
use crate::store::kv::KvStore;
use crate::subscription::PushConfig;

const SUCCESS_CODES: [u16; 5] = [102, 200, 201, 202, 204];
const POLL_INTERVAL_ON_EMPTY: Duration = Duration::from_millis(200);

/// Handle to a running dispatcher task. Dropping this **does not** stop
/// the task (it would keep delivering) — call [`Self::stop`] explicitly.
pub struct DispatcherHandle {
    cancel: CancellationToken,
    join: tokio::task::JoinHandle<()>,
}

impl DispatcherHandle {
    /// Cancels the dispatcher and waits for it to actually finish
    /// (closing its internal stream, releasing any outstanding leases).
    pub async fn stop(self) {
        self.cancel.cancel();
        let _ = self.join.await;
    }
}

/// Spawns a dispatcher for `sub_id` (already known to have `push_config`
/// set) and returns a handle to it.
#[allow(clippy::too_many_arguments)]
pub fn spawn<K: KvStore + 'static>(
    svc: Arc<PubSubService<K>>,
    subscription_full_name: String,
    sub_id: u64,
    ack_deadline_secs: i32,
    min_retry_backoff_secs: i64,
    max_retry_backoff_secs: i64,
    dead_letter_configured: bool,
    push_config: PushConfig,
    push_timeout_secs: u64,
    max_concurrency: u32,
) -> DispatcherHandle {
    let cancel = CancellationToken::new();
    let join = tokio::spawn(run(
        svc,
        subscription_full_name,
        sub_id,
        ack_deadline_secs,
        min_retry_backoff_secs,
        max_retry_backoff_secs,
        dead_letter_configured,
        push_config,
        push_timeout_secs,
        max_concurrency,
        cancel.clone(),
    ));
    DispatcherHandle { cancel, join }
}

#[allow(clippy::too_many_arguments)]
async fn run<K: KvStore + 'static>(
    svc: Arc<PubSubService<K>>,
    subscription_full_name: String,
    sub_id: u64,
    ack_deadline_secs: i32,
    min_retry_backoff_secs: i64,
    max_retry_backoff_secs: i64,
    dead_letter_configured: bool,
    push_config: PushConfig,
    push_timeout_secs: u64,
    max_concurrency: u32,
    cancel: CancellationToken,
) {
    let max_concurrency = max_concurrency.max(1);
    let stream_id = match svc.open_stream(
        &subscription_full_name,
        ack_deadline_secs,
        i64::from(max_concurrency),
        0,
    ) {
        Ok((id, _)) => id,
        Err(e) => {
            tracing::error!(subscription = %subscription_full_name, error = ?e, "push dispatcher failed to open stream");
            return;
        }
    };

    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(push_timeout_secs.max(1)))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(subscription = %subscription_full_name, error = ?e, "push dispatcher failed to build HTTP client");
            svc.close_stream(stream_id);
            return;
        }
    };

    let sem = Arc::new(Semaphore::new(max_concurrency as usize));
    let waiter = svc.stream_waiter(sub_id);

    loop {
        if cancel.is_cancelled() {
            break;
        }
        // `lease_for_stream` is synchronous (every `KvStore` op is), so
        // this loop drives it directly rather than through
        // `tokio::select!` (which needs a real future per branch) —
        // cancellation is instead checked at the top of the loop and in
        // the wait branch below.
        match svc.lease_for_stream(stream_id) {
            Ok(messages) if !messages.is_empty() => {
                for msg in messages {
                    let Ok(permit) = sem.clone().acquire_owned().await else {
                        continue;
                    };
                    let svc = svc.clone();
                    let client = client.clone();
                    let subscription_full_name = subscription_full_name.clone();
                    let push_config = push_config.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        deliver_one(
                            &svc,
                            stream_id,
                            sub_id,
                            &client,
                            &subscription_full_name,
                            &push_config,
                            msg,
                            dead_letter_configured,
                            min_retry_backoff_secs,
                            max_retry_backoff_secs,
                        )
                        .await;
                    });
                }
            }
            Ok(_) => {
                tokio::select! {
                    () = cancel.cancelled() => break,
                    () = tokio::time::sleep(POLL_INTERVAL_ON_EMPTY) => {}
                    () = waiter.notified() => {}
                }
            }
            Err(e) => {
                tracing::error!(subscription = %subscription_full_name, error = ?e, "push dispatcher lease failed");
                break;
            }
        }
    }
    // Each in-flight `deliver_one` task (spawned separately, below) holds
    // one semaphore permit until it finishes acking/nacking. Acquiring
    // every permit here blocks until all of them have — closing the
    // stream (and releasing every remaining lease) while a delivery is
    // still in flight would race that task's own eventual
    // ack/nack, which could otherwise land after (and silently undo)
    // `close_stream`'s release.
    let _ = sem.acquire_many(max_concurrency).await;
    svc.close_stream(stream_id);
}

#[allow(clippy::too_many_arguments)]
async fn deliver_one<K: KvStore + 'static>(
    svc: &Arc<PubSubService<K>>,
    stream_id: u64,
    sub_id: u64,
    client: &reqwest::Client,
    subscription_full_name: &str,
    push_config: &PushConfig,
    msg: PulledMessage,
    dead_letter_configured: bool,
    min_retry_backoff_secs: i64,
    max_retry_backoff_secs: i64,
) {
    let attempt = msg.delivery_attempt;
    let ack_id = msg.ack_id.clone();
    let body = envelope::build(
        subscription_full_name,
        &msg,
        dead_letter_configured.then_some(attempt),
        push_config.no_wrapper,
        push_config.write_metadata,
    );

    let mut request = client
        .post(&push_config.endpoint)
        .header("Content-Type", body.content_type)
        .body(body.body);
    for (name, value) in body.extra_headers {
        request = request.header(name, value);
    }

    let outcome = request.send().await;
    let success = matches!(
        &outcome,
        Ok(resp) if SUCCESS_CODES.contains(&resp.status().as_u16())
    );

    if success {
        let _ = svc.stream_acknowledge(stream_id, sub_id, vec![ack_id]);
        crate::metrics::record_push_request(subscription_full_name, "ok");
    } else {
        if let Err(e) = &outcome {
            tracing::debug!(subscription = %subscription_full_name, error = %e, "push delivery failed");
        }
        let backoff = crate::delivery::retry::backoff_for_attempts(
            attempt,
            min_retry_backoff_secs,
            max_retry_backoff_secs,
        );
        // `stream_modify_ack_deadline`/`LeaseTable::extend` treats
        // `seconds <= 0` as an *immediate* nack (per the proto contract
        // for an explicit `ModifyAckDeadline(0)`), so a sub-second
        // backoff must never round down to 0 here — that would silently
        // collapse the whole exponential backoff into "retry
        // immediately" for every low-attempt-count delivery, since
        // `backoff_for_attempts`'s 100ms floor and early doublings
        // (100ms/200ms/400ms/800ms) all truncate to a whole-second 0.
        let deadline_secs = backoff.as_secs().max(1).min(i64::MAX as u64) as i32;
        let _ = svc.stream_modify_ack_deadline(stream_id, sub_id, vec![ack_id], deadline_secs);
        crate::metrics::record_push_request(subscription_full_name, "fail");
    }
}
