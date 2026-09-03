#!/usr/bin/env bash
# Runs the full 3-language compatibility suite (basic pub/sub round trip,
# StreamingPull, and the filter/dead-letter/Snapshot-Seek advanced suite)
# against a freshly-started `open-pubusb --ephemeral` instance.
#
# Usage: tests/compat/run_all.sh
#
# Requires: cargo (to build open-pubusb), python3 with google-cloud-pubsub and
# pytest installed, node with @google-cloud/pubsub and @grpc/grpc-js
# installed under tests/compat/node/node_modules (`npm install` there
# first), and a Rust toolchain for `cargo test` under tests/compat/rust.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

GRPC_PORT="${OPEN_PUBUSB_COMPAT_GRPC_PORT:-18085}"
ADMIN_PORT="${OPEN_PUBUSB_COMPAT_ADMIN_PORT:-18086}"
export PUBSUB_EMULATOR_HOST="127.0.0.1:${GRPC_PORT}"
export PUBSUB_PROJECT_ID="compat-suite"

echo "==> Building open-pubusb"
cargo build -p open-pubusb

echo "==> Starting open-pubusb --ephemeral on ${PUBSUB_EMULATOR_HOST}"
./target/debug/open-pubusb serve --ephemeral \
    --listen "127.0.0.1:${GRPC_PORT}" \
    --admin-listen "127.0.0.1:${ADMIN_PORT}" &
SERVER_PID=$!
trap 'kill "${SERVER_PID}" 2>/dev/null || true' EXIT

echo "==> Waiting for /readyz"
for _ in $(seq 1 50); do
    if curl -sf "http://127.0.0.1:${ADMIN_PORT}/readyz" > /dev/null; then
        break
    fi
    sleep 0.2
done
if ! curl -sf "http://127.0.0.1:${ADMIN_PORT}/readyz" > /dev/null; then
    echo "open-pubusb never became ready" >&2
    exit 1
fi

status=0

echo "==> Python compat suite"
if ! python3 -m pytest tests/compat/python/tests/ -v; then
    status=1
fi

echo "==> Node.js compat suite"
if ! (cd tests/compat/node && npm test); then
    status=1
fi

echo "==> Rust compat suite"
if ! (cd tests/compat/rust && cargo test); then
    status=1
fi

exit "${status}"
