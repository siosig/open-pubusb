//! `StreamingPull`.
//!
//! Two tasks per stream, coordinated by a [`CancellationToken`] private to
//! that stream (not the process-wide shutdown token — that one only
//! *triggers* this one, via the `shutdown_token.cancelled()` branch in the
//! send loop):
//!
//! - **recv task**: reads subsequent `StreamingPullRequest`s (ack_ids /
//!   modify-deadline pairs, empty keepalive requests are simply no-ops
//!   here since an empty request applies zero acks/modifies) and applies
//!   them via [`PubSubService::stream_acknowledge`]/
//!   [`PubSubService::stream_modify_ack_deadline`]. Ends (and cancels the
//!   stream) on client half-close, a transport error, or a validation
//!   failure (aborts with `INVALID_ARGUMENT`, per the proto contract for
//!   a malformed `modify_deadline_*` pairing).
//! - **send task**: sends the first response (`subscription_properties`
//!   only), then repeatedly leases and forwards newly-deliverable
//!   messages until the stream is cancelled (by the recv task, the
//!   process shutting down, or its own lifetime timer), closing the
//!   stream (releasing every outstanding lease, since leases expire on
//!   disconnect) exactly once, in its own cleanup, regardless of which of
//!   those ended it.

use std::sync::Arc;
use std::time::Duration;

use open_pubusb_core::service::PubSubService;
use open_pubusb_core::store::kv::KvStore;
use open_pubusb_proto::pubsub::v1 as pb;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Response, Status, Streaming};

use super::status::to_status;
use super::subscriber::pulled_to_received_message;

const RESPONSE_CHANNEL_CAPACITY: usize = 16;
/// How long the send loop waits for a publish notification before polling
/// again — bounds staleness if a notify is ever missed (e.g. a message
/// became eligible via ack-deadline expiry rather than a fresh publish,
/// which does not itself fire a notify).
const POLL_INTERVAL_ON_EMPTY: Duration = Duration::from_millis(200);

type BoxStream = std::pin::Pin<
    Box<
        dyn tonic::codegen::tokio_stream::Stream<Item = Result<pb::StreamingPullResponse, Status>>
            + Send
            + 'static,
    >,
>;

pub async fn streaming_pull<K: KvStore + 'static>(
    svc: Arc<PubSubService<K>>,
    max_lifetime_secs: u64,
    shutdown_token: CancellationToken,
    mut incoming: Streaming<pb::StreamingPullRequest>,
) -> Result<Response<BoxStream>, Status> {
    let first = incoming
        .message()
        .await
        .map_err(|e| Status::invalid_argument(format!("failed to read first request: {e}")))?
        .ok_or_else(|| Status::invalid_argument("stream closed before any request"))?;

    if first.subscription.is_empty() {
        return Err(Status::invalid_argument(
            "the first request must set `subscription`",
        ));
    }
    if !(10..=600).contains(&first.stream_ack_deadline_seconds) {
        return Err(Status::invalid_argument(
            "stream_ack_deadline_seconds must be between 10 and 600",
        ));
    }

    let (stream_id, sub) = svc
        .open_stream(
            &first.subscription,
            first.stream_ack_deadline_seconds,
            first.max_outstanding_messages,
            first.max_outstanding_bytes,
        )
        .map_err(to_status)?;
    let sub_id = sub.id;

    if let Err(status) = apply_request(&svc, stream_id, sub_id, &first) {
        svc.close_stream(stream_id);
        return Err(status);
    }

    let (tx, rx) = mpsc::channel(RESPONSE_CHANNEL_CAPACITY);
    let cancel = CancellationToken::new();

    tokio::spawn(recv_loop(
        svc.clone(),
        stream_id,
        sub_id,
        incoming,
        tx.clone(),
        cancel.clone(),
    ));
    tokio::spawn(send_loop(
        svc,
        stream_id,
        sub_id,
        sub.enable_exactly_once_delivery,
        sub.enable_message_ordering,
        max_lifetime_secs,
        tx,
        cancel,
        shutdown_token,
    ));

    Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
}

/// Applies one request's `ack_ids` and `modify_deadline_seconds`/
/// `modify_deadline_ack_ids` pair. Per the proto contract, a length
/// mismatch between the two parallel arrays aborts the stream with
/// `INVALID_ARGUMENT`.
fn apply_request<K: KvStore>(
    svc: &PubSubService<K>,
    stream_id: u64,
    sub_id: u64,
    req: &pb::StreamingPullRequest,
) -> Result<(), Status> {
    if req.modify_deadline_seconds.len() != req.modify_deadline_ack_ids.len() {
        return Err(Status::invalid_argument(
            "modify_deadline_seconds and modify_deadline_ack_ids must have the same length",
        ));
    }
    if !req.ack_ids.is_empty() {
        svc.stream_acknowledge(stream_id, sub_id, req.ack_ids.clone())
            .map_err(to_status)?;
    }
    // The domain layer applies one `seconds` value to a whole batch;
    // `StreamingPullRequest` allows a different value per ack_id in the
    // same request (parallel arrays), so each pair is applied
    // individually rather than trying to batch same-value pairs — simpler
    // and correctness-first over the (rare) large-batch case.
    for (ack_id, seconds) in req
        .modify_deadline_ack_ids
        .iter()
        .zip(req.modify_deadline_seconds.iter())
    {
        svc.stream_modify_ack_deadline(stream_id, sub_id, vec![ack_id.clone()], *seconds)
            .map_err(to_status)?;
    }
    Ok(())
}

async fn recv_loop<K: KvStore + 'static>(
    svc: Arc<PubSubService<K>>,
    stream_id: u64,
    sub_id: u64,
    mut incoming: Streaming<pb::StreamingPullRequest>,
    tx: mpsc::Sender<Result<pb::StreamingPullResponse, Status>>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            msg = incoming.message() => match msg {
                Ok(Some(req)) => {
                    if let Err(status) = apply_request(&svc, stream_id, sub_id, &req) {
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                }
                Ok(None) => break, // client half-closed: normal end of stream
                Err(_) => break,   // transport error reading from the client
            },
        }
    }
    cancel.cancel();
}

#[allow(clippy::too_many_arguments)]
async fn send_loop<K: KvStore + 'static>(
    svc: Arc<PubSubService<K>>,
    stream_id: u64,
    sub_id: u64,
    exactly_once_delivery_enabled: bool,
    message_ordering_enabled: bool,
    max_lifetime_secs: u64,
    tx: mpsc::Sender<Result<pb::StreamingPullResponse, Status>>,
    cancel: CancellationToken,
    shutdown_token: CancellationToken,
) {
    let first_response = pb::StreamingPullResponse {
        subscription_properties: Some(pb::streaming_pull_response::SubscriptionProperties {
            exactly_once_delivery_enabled,
            message_ordering_enabled,
        }),
        ..Default::default()
    };
    if tx.send(Ok(first_response)).await.is_err() {
        svc.close_stream(stream_id);
        return;
    }

    let waiter = svc.stream_waiter(sub_id);
    let deadline = tokio::time::sleep(Duration::from_secs(max_lifetime_secs.max(1)));
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            () = shutdown_token.cancelled() => {
                let _ = tx.send(Err(Status::unavailable("server is shutting down"))).await;
                break;
            }
            () = &mut deadline => {
                let _ = tx.send(Err(Status::unavailable(
                    "StreamingPull stream lifetime elapsed; reconnect",
                ))).await;
                break;
            }
            leased = svc_lease_then_wait(&svc, stream_id, &waiter) => {
                match leased {
                    Ok(Some(received_messages)) => {
                        let resp = pb::StreamingPullResponse {
                            received_messages,
                            ..Default::default()
                        };
                        if tx.send(Ok(resp)).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => {} // nothing available this pass; loop (already waited inside)
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                }
            }
        }
    }
    svc.close_stream(stream_id);
}

/// Leases whatever is currently available for `stream_id`. If nothing is,
/// waits (bounded by [`POLL_INTERVAL_ON_EMPTY`]) for a publish
/// notification before returning `Ok(None)`, so the `send_loop`'s
/// `select!` doesn't busy-spin this branch while genuinely idle.
async fn svc_lease_then_wait<K: KvStore>(
    svc: &PubSubService<K>,
    stream_id: u64,
    waiter: &tokio::sync::Notify,
) -> Result<Option<Vec<pb::ReceivedMessage>>, Status> {
    let delivered = svc.lease_for_stream(stream_id).map_err(to_status)?;
    if delivered.is_empty() {
        let _ = tokio::time::timeout(POLL_INTERVAL_ON_EMPTY, waiter.notified()).await;
        return Ok(None);
    }
    Ok(Some(
        delivered
            .into_iter()
            .map(pulled_to_received_message)
            .collect(),
    ))
}
