#!/usr/bin/env bash
# Durability QA script.
#
# Two scenarios, each: start open-pubusb on a fresh data dir -> publish 1000
# messages -> pull all -> ack the first 500 -> SIGKILL the process
# immediately (no grace, no sleep, deliberately inside
# storage.sync_interval_ms's window) -> restart on the same data dir ->
# verify.
#
#   1. Persistent (default): exactly the 500 never-acked messages come
#      back, same ids and payload; the 500 acked ones never reappear.
#   2. `--ephemeral`: everything is gone after the restart (nothing was
#      ever written to disk in the first place).
#
# Usage: scripts/qa/durability.sh
# Env overrides: OPEN_PUBUSB_BIN (path to a prebuilt binary; built via
# `cargo build --release -p open-pubusb` if unset and not already built).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

BIN="${OPEN_PUBUSB_BIN:-target/release/open-pubusb}"
if [[ ! -x "$BIN" ]]; then
  echo "[durability] building open-pubusb (release)..." >&2
  cargo build --release -p open-pubusb
fi

PROJECT="qa-durability"
TOPIC="topic-durability"
SUB="sub-durability"
MESSAGE_COUNT=1000
LISTEN_PORT=18185
ADMIN_PORT=18186

# Best-effort: kill anything already listening on our fixed ports before
# starting. Without this, a process orphaned by a previous *failed* run
# of this script (e.g. one that `set -e`-aborted before its own cleanup
# ran) can still be alive and healthy on these ports — `wait_for_ready`
# would then happily report readiness against that *stale* server
# instead of the one this run just started, silently talking to the
# wrong process for the rest of the scenario.
free_test_ports() {
  local pid
  for port in "$LISTEN_PORT" "$ADMIN_PORT"; do
    pid="$(ss -ltnp 2>/dev/null | awk -v p=":${port}$" '$4 ~ p {print $0}' | grep -oP 'pid=\K[0-9]+' | head -1 || true)"
    if [[ -n "${pid:-}" ]]; then
      echo "[durability] killing stale process ${pid} already listening on port ${port}" >&2
      kill -9 "$pid" 2>/dev/null || true
    fi
  done
  sleep 0.2
}

wait_for_ready() {
  local admin_addr="$1" pid="$2" log_file="$3"
  for _ in $(seq 1 60); do
    if curl -sf "http://${admin_addr}/readyz" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "[durability] server process ${pid} exited before becoming ready; log:" >&2
      cat "$log_file" >&2
      return 1
    fi
    sleep 0.2
  done
  echo "[durability] server at ${admin_addr} did not become ready in time" >&2
  return 1
}


# Sets the global `SERVER_PID` to the started server's pid. Deliberately
# *not* `pid=$(start_server ...)`: command substitution runs the function
# in a subshell, which (a) makes the backgrounded server process a child
# of that subshell rather than of this script — so a later `wait "$pid"`
# in this script fails ("not a child of this shell") and silently does
# nothing behind `|| true`, meaning the caller never actually blocks for
# the process to be reaped before reusing its port — and (b) risks the
# server's own inherited stdout interleaving with (and corrupting) the
# `echo $!` output the substitution is trying to capture. A global
# variable avoids both: the server is started directly in this shell (a
# real, waitable child), and its own output goes to `log_file`, not this
# script's stdout.
start_server() {
  local data_dir="$1" listen="$2" admin="$3" ephemeral="$4" log_file="$5"
  local args=(serve --data-dir "$data_dir" --listen "$listen" --admin-listen "$admin")
  if [[ "$ephemeral" == "true" ]]; then
    args+=(--ephemeral)
  fi
  "$BIN" "${args[@]}" >"$log_file" 2>&1 &
  SERVER_PID=$!
}

run_scenario() {
  local ephemeral="$1"
  local label
  if [[ "$ephemeral" == "true" ]]; then
    label="--ephemeral"
  else
    label="persistent"
  fi
  echo "=== durability scenario: ${label} ==="

  local data_dir listen admin base_url state_file log1 log2 pid
  data_dir="$(mktemp -d)"
  listen="127.0.0.1:${LISTEN_PORT}"
  admin="127.0.0.1:${ADMIN_PORT}"
  base_url="http://${listen}"
  state_file="$(mktemp)"
  log1="$(mktemp)"
  log2="$(mktemp)"

  free_test_ports
  start_server "$data_dir" "$listen" "$admin" "$ephemeral" "$log1"
  pid="$SERVER_PID"
  wait_for_ready "$admin" "$pid" "$log1"

  python3 scripts/qa/durability_client.py setup "$base_url" "$PROJECT" "$TOPIC" "$SUB"
  python3 scripts/qa/durability_client.py publish-pull-ack-half \
    "$base_url" "$PROJECT" "$TOPIC" "$SUB" "$MESSAGE_COUNT" "$state_file"

  echo "[durability] SIGKILL-ing pid ${pid} immediately (no grace period)"
  kill -9 "$pid"
  wait "$pid" 2>/dev/null || true

  start_server "$data_dir" "$listen" "$admin" "$ephemeral" "$log2"
  pid="$SERVER_PID"
  wait_for_ready "$admin" "$pid" "$log2"

  if [[ "$ephemeral" == "true" ]]; then
    python3 scripts/qa/durability_client.py verify-empty "$base_url" "$PROJECT" "$SUB"
  else
    python3 scripts/qa/durability_client.py verify-recovered "$base_url" "$PROJECT" "$SUB" "$state_file"
  fi

  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  rm -rf "$data_dir" "$state_file" "$log1" "$log2"
  echo "=== ${label} scenario: PASSED ==="
}

run_scenario false
run_scenario true

echo "[durability] ALL DURABILITY CHECKS PASSED"
