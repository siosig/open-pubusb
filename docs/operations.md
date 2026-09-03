# Operations Runbook

> Japanese: [operations.ja.md](operations.ja.md)

## Table of Contents

- [Overview](#overview)
- [Running under systemd](#running-under-systemd)
- [Running under Docker](#running-under-docker)
- [Logging](#logging)
- [Metrics](#metrics)
- [Shutdown behavior (signals and exit codes)](#shutdown-behavior-signals-and-exit-codes)
- [Backing up data_dir](#backing-up-data_dir)
- [Upgrade procedure and format version](#upgrade-procedure-and-format-version)
- [Troubleshooting](#troubleshooting)

## Overview

This page collects the day-to-day operations (start/stop, log inspection, metrics, backup, upgrade) for running `open-pubusb` as a systemd service or a Docker container. For the deployment procedure itself see [`ansible/README.md`](../ansible/README.md); the full list of configuration options is defined in `crates/open-pubusb/src/config.rs`.

## Running under systemd

```bash
systemctl status open-pubusb
systemctl restart open-pubusb
journalctl -u open-pubusb -f                 # follow the log
journalctl -u open-pubusb -o json --since "10 min ago"   # include the structured fields
```

- `Type=notify`: the service sends `READY=1` only after storage recovery has finished and the listener is accepting connections, so `systemctl start` blocks until the service can actually serve requests.
- `WatchdogSec=30`: if the process stops responding, systemd restarts it automatically.
- The data directory is pinned to `/var/lib/open-pubusb` via `DynamicUser=true` + `StateDirectory=open-pubusb` (injected automatically as `OPEN_PUBUSB__STORAGE__DATA_DIR`).
- `SIGTERM` (`systemctl stop`) follows the graceful shutdown sequence described in [Shutdown behavior](#shutdown-behavior-signals-and-exit-codes). `TimeoutStopSec` is `shutdown_grace_secs + 5` seconds.

## Running under Docker

```bash
docker logs -f open-pubusb
docker exec open-pubusb /usr/local/bin/open-pubusb health --url http://127.0.0.1:8086/readyz
docker restart open-pubusb
```

- Data is persisted in a named volume (default path `/data`, `OPEN_PUBUSB__STORAGE__DATA_DIR=/data`). As long as the volume is kept, the data survives removing and re-creating the container.
- `HEALTHCHECK` runs `open-pubusb health` every 10 seconds (inspect it with `docker inspect --format='{{json .State.Health}}' open-pubusb`).
- The process runs as a non-root user (distroless `nonroot`).

## Logging

- One JSON object per line on stdout (under systemd the default is `format=journald`, stored as structured journal fields).
- Fields: `timestamp` (RFC3339), `level`, `target`, `message`, plus the structured fields `topic` / `subscription` / `message_id` / `ack_id` / `stream_id` / `grpc.method` / `grpc.code` / `elapsed_ms`.
- Request logs (gRPC method, code, elapsed time) are emitted at `info`, one line per request. High-frequency calls such as Publish/Pull are logged at `debug`.
- **Message payloads and attribute values are never logged at any level** (a deliberate design constraint to prevent PII leakage).

## Metrics

`GET /metrics` (on the admin port, 8086 by default) serves Prometheus text format with the `open_pubusb_` prefix.

| Name | Type | Labels | Meaning |
|---|---|---|---|
| `open_pubusb_topics` | gauge | — | Number of topics |
| `open_pubusb_subscriptions` | gauge | — | Number of subscriptions |
| `open_pubusb_messages_published_total` | counter | `topic` | Messages published |
| `open_pubusb_messages_delivered_total` | counter | `subscription`, `mode` | Messages delivered (including redeliveries; mode=pull/streaming/push) |
| `open_pubusb_messages_acked_total` | counter | `subscription` | Messages acknowledged |
| `open_pubusb_messages_expired_total` | counter | `subscription` | Messages discarded because the retention period elapsed |
| `open_pubusb_messages_dead_lettered_total` | counter | `subscription` | Messages forwarded to the DLQ |
| `open_pubusb_unacked_messages` | gauge | `subscription` | Unacked messages (awaiting delivery + leased) |
| `open_pubusb_oldest_unacked_age_seconds` | gauge | `subscription` | Age in seconds of the oldest unacked message |
| `open_pubusb_publish_latency_seconds` | histogram | — | Publish RPC latency |
| `open_pubusb_grpc_requests_total` | counter | `method`, `code` | gRPC calls |
| `open_pubusb_push_requests_total` | counter | `subscription`, `result` | Push delivery outcomes (ok/fail) |
| `open_pubusb_storage_sync_duration_seconds` | histogram | — | Group fsync duration |
| `open_pubusb_storage_disk_bytes` | gauge | — | data_dir usage |
| `open_pubusb_streaming_pull_streams` | gauge | — | Number of open StreamingPull streams |

The `subscription`/`topic` labels carry the full resource name. Once the number of resources exceeds 1,000, the labeled series are truncated and `open_pubusb_metrics_truncated=1` is set (a guard against cardinality explosion).

**Useful starting points for alerting**:
- A sharp rise in `open_pubusb_oldest_unacked_age_seconds` — subscribers are falling behind
- `open_pubusb_storage_disk_bytes` approaching `storage.max_disk_bytes` — write rejection (`RESOURCE_EXHAUSTED`) is imminent
- The rate of increase of `open_pubusb_grpc_requests_total{code!="OK"}`

## Shutdown behavior (signals and exit codes)

| Signal | Behavior |
|---|---|
| `SIGTERM` / `SIGINT` | Graceful shutdown: `/readyz` switches to `503` → new connections are refused → in-flight `StreamingPull` streams receive `UNAVAILABLE` → in-flight RPCs are awaited for up to `shutdown_grace_secs` (default 30 seconds) → the store is fsynced one last time → exit code `0` |
| Grace period exceeded | Remaining work is abandoned, a final fsync is performed, and the process exits with code `0` (leaving a `warn` log entry) |
| `SIGHUP` | Ignored (there is no dynamic configuration reload; configuration changes take effect on restart) |

| Exit code | Meaning |
|---|---|
| `0` | Normal termination |
| `1` | Fatal runtime error (e.g. storage corruption) |
| `2` | Configuration or environment error (validation failed before startup) |

Within the shutdown grace period, no message that has been accepted and acknowledged to the client is ever lost (acceptance means the write to the OS buffer has completed).

## Backing up data_dir

`data_dir` (systemd: `/var/lib/open-pubusb`; Docker: the named volume) consists solely of the `fjall` keyspace files.

- **Hot backups of a running instance are not recommended** — `fjall` is an LSM store spread across multiple files, and copying those files from the outside while writes are in progress can produce an inconsistent snapshot. Take backups in one of the following ways:
  1. Stop the service temporarily (`systemctl stop open-pubusb` / `docker stop open-pubusb`), copy the whole `data_dir`, then start the service again.
  2. Use a filesystem- or block-device-level snapshot facility (LVM snapshot, ZFS/btrfs snapshot, cloud disk snapshot, etc.). This is safe even while the service is running, because no application-level quiescent point is needed.
- To restore, put the `data_dir` back while the service is stopped, then start it. Make sure the format version matches (see the next section).
- When running in `--ephemeral` mode, `data_dir` is a fresh empty temporary directory on every start (nothing is persisted), so there is nothing to back up in the first place.

## Upgrade procedure and format version

The `meta` keyspace in `data_dir` holds a `__open_pubusb_format_version` marker. If the binary being started finds a marker newer than the highest format version it supports, it refuses to start with an error (to prevent misbehavior and data corruption after a downgrade).

Recommended upgrade procedure:

1. Check the release notes of the new version for format version changes.
2. Take a [backup of `data_dir`](#backing-up-data_dir).
3. Stop the service.
4. Replace the binary (systemd) or the image (Docker) with the new version (Ansible: update `open_pubusb_version` / `open_pubusb_image_tag` and re-run the playbook).
5. Start the service and confirm that `/readyz` returns `200`.
6. If anything goes wrong, roll back to the previous binary/image using the backup from step 2.

A downgrade (opening data written in a newer format with an older binary) fails with an explicit error thanks to the startup check above — silent data corruption does not occur.

## Troubleshooting

| Symptom | What to check |
|---|---|
| `/readyz` stays at `503` | Storage recovery has not finished (`data_dir` is huge, or corrupted). Check the startup errors in the log |
| Exit code `2` right after startup | Configuration error. Validate ahead of time with `open-pubusb check-config --config <path>` |
| Exit code `1` right after startup | Possible storage corruption. A format version mismatch also lands here (the error message has the details) |
| `RESOURCE_EXHAUSTED` is returned | `storage.max_disk_bytes` has been reached. Check the `open_pubusb_storage_disk_bytes` metric and consider expanding the disk or shortening the retention period |
| Push deliveries never arrive | Check `open_pubusb_push_requests_total{result="fail"}`. Also check the endpoint's logs and the timeout setting (default 10 seconds) |
