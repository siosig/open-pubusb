//! `open-pubusb-bench`: a load generator against a running `open-pubusb` (or any real
//! Pub/Sub-compatible) server.
//!
//! `open-pubusb-bench publish` creates `--topics` topics (each with
//! `--subscribers` attached subscriptions, so Publish pays the same
//! fan-out notification cost a real deployment would), then publishes
//! `--msg-size`-byte messages round-robin across those topics at
//! `--rate` messages/sec for `--duration` seconds, timing every unary
//! `Publish` RPC round trip. Reports achieved throughput and p50/p99
//! publish latency.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use open_pubusb_proto::pubsub::v1::publisher_client::PublisherClient;
use open_pubusb_proto::pubsub::v1::subscriber_client::SubscriberClient;
use open_pubusb_proto::pubsub::v1::{PublishRequest, PubsubMessage, Subscription, Topic};

#[derive(Parser)]
#[command(name = "open-pubusb-bench", about = "Load generator for open-pubusb")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Publish a synthetic load and report throughput/latency.
    Publish {
        /// Server address, e.g. `127.0.0.1:8085`.
        #[arg(long, default_value = "127.0.0.1:8085")]
        endpoint: String,
        /// Project id topics/subscriptions are created under.
        #[arg(long, default_value = "open-pubusb-bench")]
        project: String,
        /// Payload size of each published message, in bytes.
        #[arg(long, default_value_t = 1024)]
        msg_size: usize,
        /// Target aggregate publish rate, in messages/second.
        #[arg(long, default_value_t = 1000)]
        rate: u64,
        /// How long to run the load, in seconds.
        #[arg(long, default_value_t = 30)]
        duration: u64,
        /// Number of subscriptions attached per topic (fan-out cost).
        #[arg(long, default_value_t = 1)]
        subscribers: u32,
        /// Number of topics to spread the load round-robin across.
        #[arg(long, default_value_t = 1)]
        topics: u32,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Publish {
            endpoint,
            project,
            msg_size,
            rate,
            duration,
            subscribers,
            topics,
        } => {
            if let Err(e) = run_publish(
                &endpoint,
                &project,
                msg_size,
                rate,
                duration,
                subscribers,
                topics,
            )
            .await
            {
                eprintln!("open-pubusb-bench: {e}");
                std::process::exit(1);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_publish(
    endpoint: &str,
    project: &str,
    msg_size: usize,
    rate: u64,
    duration_secs: u64,
    subscribers: u32,
    topic_count: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("http://{endpoint}");
    let channel = tonic::transport::Endpoint::new(url)?.connect_lazy();
    let mut publisher = PublisherClient::new(channel.clone());
    let mut subscriber = SubscriberClient::new(channel);

    let run_id = std::process::id();
    let topic_names: Vec<String> = (0..topic_count.max(1))
        .map(|i| format!("projects/{project}/topics/bench-{run_id}-{i}"))
        .collect();
    for topic_name in &topic_names {
        publisher
            .create_topic(Topic {
                name: topic_name.clone(),
                ..Default::default()
            })
            .await?;
        for s in 0..subscribers {
            let sub_name = format!(
                "projects/{project}/subscriptions/bench-{run_id}-{}-{s}",
                topic_name.rsplit('/').next().unwrap_or("t")
            );
            subscriber
                .create_subscription(Subscription {
                    name: sub_name,
                    topic: topic_name.clone(),
                    ..Default::default()
                })
                .await?;
        }
    }

    let payload = vec![b'x'; msg_size];
    let rate = rate.max(1);
    let interval = Duration::from_secs_f64(1.0 / rate as f64);
    let deadline = Instant::now() + Duration::from_secs(duration_secs.max(1));

    // A single h2 connection multiplexes many concurrent RPCs already
    // (that's what `tonic::transport::Channel` gives every clone of
    // `publisher` below), so sustaining `rate` doesn't need more TCP
    // connections — it needs *not waiting for each response* before
    // scheduling the next send. Each tick spawns its own publish as a
    // task; a semaphore caps how many can be in flight at once so an
    // unreachable/slow server can't spawn unboundedly many tasks.
    let max_in_flight = (rate as usize).clamp(64, 4096);
    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(max_in_flight));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Option<f64>>();

    let mut next_send = Instant::now();
    let mut topic_idx = 0usize;
    let started = Instant::now();
    let mut sent = 0u64;

    while Instant::now() < deadline {
        if Instant::now() < next_send {
            tokio::time::sleep(next_send - Instant::now()).await;
        }
        let topic_name = topic_names[topic_idx % topic_names.len()].clone();
        topic_idx += 1;
        sent += 1;

        let Ok(permit) = sem.clone().acquire_owned().await else {
            break;
        };
        let mut publisher = publisher.clone();
        let payload = payload.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let sent_at = Instant::now();
            let result = publisher
                .publish(PublishRequest {
                    topic: topic_name,
                    messages: vec![PubsubMessage {
                        data: payload,
                        attributes: HashMap::new(),
                        ..Default::default()
                    }],
                })
                .await;
            let elapsed_ms = sent_at.elapsed().as_secs_f64() * 1000.0;
            match result {
                Ok(_) => {
                    let _ = tx.send(Some(elapsed_ms));
                }
                Err(e) => {
                    eprintln!("publish failed: {e}");
                    let _ = tx.send(None);
                }
            }
        });

        next_send += interval;
    }
    drop(tx);

    // Drain every in-flight publish's result rather than dropping them at
    // the deadline — a request already sent should still count.
    let mut latencies_ms: Vec<f64> = Vec::with_capacity(sent as usize);
    let _ = sem.acquire_many(max_in_flight as u32).await;
    while let Ok(result) = rx.try_recv() {
        if let Some(ms) = result {
            latencies_ms.push(ms);
        }
    }

    let total_elapsed = started.elapsed();
    report(&latencies_ms, total_elapsed);
    Ok(())
}

fn report(latencies_ms: &[f64], elapsed: Duration) {
    if latencies_ms.is_empty() {
        println!("no successful publishes");
        return;
    }
    let mut sorted = latencies_ms.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p50 = percentile(&sorted, 0.50);
    let p99 = percentile(&sorted, 0.99);
    let throughput = sorted.len() as f64 / elapsed.as_secs_f64();

    println!("published:   {}", sorted.len());
    println!("elapsed:     {:.2}s", elapsed.as_secs_f64());
    println!("throughput:  {throughput:.1} msg/s");
    println!("p50 latency: {p50:.2} ms");
    println!("p99 latency: {p99:.2} ms");
}

/// `sorted` must already be sorted ascending. `p` in `[0.0, 1.0]`.
fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_of_sorted_data_matches_expected_index() {
        let data: Vec<f64> = (1..=100).map(f64::from).collect();
        assert_eq!(percentile(&data, 0.50), 51.0);
        assert_eq!(percentile(&data, 0.99), 99.0);
        assert_eq!(percentile(&data, 0.0), 1.0);
    }

    #[test]
    fn percentile_of_empty_data_is_zero() {
        assert_eq!(percentile(&[], 0.50), 0.0);
    }
}
