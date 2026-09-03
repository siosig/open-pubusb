# open-pubusb

> Japanese: [README.ja.md](README.ja.md)

A GCP Pub/Sub v1-compatible messaging service written in Rust. Runs on localhost as a systemd service or a Docker container.

## Table of Contents

- [Overview](#overview)
- [Setup](#setup)
- [Quick Start](#quick-start)
- [Connecting Clients (by Language)](#connecting-clients-by-language)
- [Supported APIs](#supported-apis)
  - [gRPC](#grpc)
  - [REST (Subset)](#rest-subset)
- [Configuration](#configuration)
- [Running with Docker](#running-with-docker)
- [Deploying with Ansible](#deploying-with-ansible)
- [Operational Endpoints](#operational-endpoints)
- [Benchmarks](#benchmarks)
- [Deviations from Production Pub/Sub](#deviations-from-production-pubsub)
- [Testing](#testing)
- [Further Documentation](#further-documentation)
- [License](#license)

## Overview

`open-pubusb` is a single-binary implementation that runs a Google Cloud Pub/Sub v1 gRPC / REST API-compatible server on localhost. The official client libraries (Python / Node.js / Go / Rust, etc.) work unmodified; you only need to point them at a different endpoint. It supports topic / subscription CRUD, Publish/Pull/Ack, StreamingPull, push delivery, filters, DLQ (dead-letter queues), retry backoff, and Snapshot/Seek. Persistence uses an embedded LSM store (fjall), and it can also run in memory only with `--ephemeral`. It can be deployed as a systemd service or a Docker container via the Ansible role.

## Setup

```bash
git submodule update --init third_party/googleapis
cargo check --workspace
```

## Quick Start

```bash
# In-memory only (for development and testing)
cargo run -p open-pubusb -- serve --ephemeral

# With persistence (default ports: 8085 = gRPC+REST, 8086 = admin)
cargo run -p open-pubusb -- serve --data-dir ./data
```

Once started, the server is ready when `GET http://127.0.0.1:8086/readyz` returns `200`.

## Connecting Clients (by Language)

`open-pubusb` uses no TLS and accepts anonymous connections (the `authorization` header is accepted but ignored).

| Language | How to connect |
|---|---|
| Python (`google-cloud-pubsub`) | Just set the environment variable `PUBSUB_EMULATOR_HOST=127.0.0.1:8085`. No application code changes required |
| Node.js (`@google-cloud/pubsub`) | Same as above, `PUBSUB_EMULATOR_HOST=127.0.0.1:8085` |
| Rust (`gcloud-pubsub` / `google-cloud-pubsub`) | `gcloud-pubsub` honors `PUBSUB_EMULATOR_HOST`. Google's official Rust SDK `google-cloud-pubsub` does not read the environment variable, so connect with `Client::builder().with_endpoint("http://127.0.0.1:8085")` plus anonymous credentials |
| Other (raw gRPC) | Connect any gRPC client to `http://127.0.0.1:8085` over an insecure channel. `grpc.reflection.v1` is enabled by default, so `grpcurl` works out of the box |

## Supported APIs

The service definitions come from the `google.pubsub.v1` protos in the `third_party/googleapis` submodule. The key points are summarized below.

### gRPC

All methods of `google.pubsub.v1.Publisher` / `Subscriber` are implemented (Topic/Subscription CRUD, Publish, Pull, StreamingPull, ModifyPushConfig, the Snapshot family, and Seek). `google.pubsub.v1.SchemaService` and `google.iam.v1.IAMPolicy` return `UNIMPLEMENTED` (see [Deviations](#deviations-from-production-pubsub)). `grpc.health.v1.Health` and `grpc.reflection.v1` are also supported.

### REST (Subset)

Using the same paths and JSON shapes (proto3 JSON mapping) as the Cloud Pub/Sub REST API, only the following are implemented. Any other `/v1/...` path returns `501 Not Implemented`.

| HTTP | Path | Corresponding gRPC method |
|---|---|---|
| PUT | `/v1/projects/{p}/topics/{t}` | CreateTopic |
| GET | `/v1/projects/{p}/topics/{t}` | GetTopic |
| GET | `/v1/projects/{p}/topics` | ListTopics |
| DELETE | `/v1/projects/{p}/topics/{t}` | DeleteTopic |
| POST | `/v1/projects/{p}/topics/{t}:publish` | Publish |
| PUT | `/v1/projects/{p}/subscriptions/{s}` | CreateSubscription |
| GET | `/v1/projects/{p}/subscriptions/{s}` | GetSubscription |
| GET | `/v1/projects/{p}/subscriptions` | ListSubscriptions |
| DELETE | `/v1/projects/{p}/subscriptions/{s}` | DeleteSubscription |
| POST | `/v1/projects/{p}/subscriptions/{s}:pull` | Pull |
| POST | `/v1/projects/{p}/subscriptions/{s}:acknowledge` | Acknowledge |
| POST | `/v1/projects/{p}/subscriptions/{s}:modifyAckDeadline` | ModifyAckDeadline |
| POST | `/v1/projects/{p}/subscriptions/{s}:modifyPushConfig` | ModifyPushConfig |

The `PUT .../subscriptions/{s}` body is the full `Subscription` message, decoded with the same proto3 JSON mapping and mapped through the same conversion the gRPC `CreateSubscription` uses. So push delivery, filters, dead-letter policies, retry policies, and the other subscription fields are configurable over REST exactly as they are over gRPC. A body carrying a field this server does not model is rejected with `400 INVALID_ARGUMENT` rather than being silently ignored.

StreamingPull and Snapshot/Seek remain gRPC-only (over REST they return `501`).

## Configuration

Settings can be supplied via CLI flags, a configuration file (TOML), and environment variables (in the `OPEN_PUBUSB__<section>__<key>` form). They are applied in the order defaults < configuration file < environment variables < CLI flags, with later sources overriding earlier ones. The full list of options is defined in `crates/open-pubusb/src/config.rs`.

```bash
open-pubusb serve        [--config <path>] [--listen <addr>] [--admin-listen <addr>] [--data-dir <path>] [--ephemeral]
open-pubusb check-config [--config <path>]
open-pubusb health       [--url <http://127.0.0.1:8086/readyz>] [--timeout <secs>]
open-pubusb version
```

## Running with Docker

```bash
docker build -t open-pubusb:local .
docker run --rm -p 8085:8085 -p 8086:8086 \
  -v open-pubusb-data:/data \
  open-pubusb:local
```

A statically linked binary on a `gcr.io/distroless/static-debian12:nonroot` base, running as a non-root user. The `HEALTHCHECK` uses `open-pubusb health`. See the `Dockerfile` for details.

## Deploying with Ansible

The Ansible role `open_pubusb` deploys the service either as a systemd service or as a Docker container.

```bash
cd ansible
ansible-galaxy collection install -r requirements.yml
ansible-playbook -i inventory/hosts.yml site.yml -e open_pubusb_deploy_mode=systemd
# or
ansible-playbook -i inventory/hosts.yml site.yml -e open_pubusb_deploy_mode=docker
```

See [`ansible/README.md`](ansible/README.md) for the public variables, tags, and idempotency details.

## Operational Endpoints

| Path | Description |
|---|---|
| `GET /healthz` | Process liveness check. Always returns `200` |
| `GET /readyz` | Readiness check. Returns `200` once store recovery has completed and the listeners are up; returns `503` during shutdown |
| `GET /metrics` | Metrics in Prometheus text format |

## Benchmarks

```bash
cargo run -p open-pubusb-bench --release -- publish \
  --endpoint 127.0.0.1:8085 --msg-size 1024 --rate 1000 --duration 30 \
  --topics 4 --subscribers 2

cargo bench -p open-pubusb-bench
```

The `publish` subcommand measures Publish throughput and p50/p99 latency. `cargo bench` runs the micro-benchmarks (criterion) for filter evaluation and key encoding.

## Deviations from Production Pub/Sub

| Item | Behavior |
|---|---|
| `enable_exactly_once_delivery` | The value is accepted and stored, but delivery is always at-least-once (exactly-once semantics are not implemented) |
| `SchemaService` | All methods return `UNIMPLEMENTED` (no schema validation) |
| `IAMPolicy` | All methods return `UNIMPLEMENTED` (no access control) |
| TLS / authentication | None. Plaintext transport; the `authorization` header is ignored (anonymous connections assumed) |
| Node topology | Single process, single node only. No clustering or replication |
| Durability level | Without `--ephemeral`, writes reach the OS buffers immediately (surviving `kill -9`), but fsync is batched every `storage.sync_interval_ms` — full durability against power loss is not guaranteed |
| `bigquery_config` / `cloud_storage_config` / `schema_settings` / `ingestion_data_source_settings` | Specifying any of these returns `INVALID_ARGUMENT` (unsupported) |

## Testing

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings -W clippy::unwrap_used -W clippy::expect_used
tests/compat/run_all.sh   # Official-client compatibility suite in three languages (Python/Node.js/Rust)
```

## Further Documentation

- Operations runbook: [`docs/operations.md`](docs/operations.md)
- Architecture: [`docs/architecture.md`](docs/architecture.md)

## License

Dual-licensed under the MIT License and the Apache License 2.0 (`MIT OR Apache-2.0`). You may use it under either license at your option.

- [`LICENSE-MIT`](LICENSE-MIT)
- [`LICENSE-APACHE`](LICENSE-APACHE)
