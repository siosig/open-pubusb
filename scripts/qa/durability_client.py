#!/usr/bin/env python3
"""REST client helper for `scripts/qa/durability.sh` (tasks.md T058).

Deliberately stdlib-only (`urllib.request`, `json`, `base64`) — no
dependency on `tests/compat/python`'s installed package set, so this
script can run standalone with just `python3`.

Subcommands:
  setup <base_url> <project> <topic> <sub>
      Creates the topic and subscription (idempotent: ignores 409).

  publish-pull-ack-half <base_url> <project> <topic> <sub> <count> <state_file>
      Publishes `count` distinct messages, pulls all of them, acks the
      first half (by message_id, numerically) and leaves the second half
      outstanding. Writes the expected-still-unacked (message_id, data)
      pairs to `state_file` as JSON, for the post-restart check to
      compare against.

  verify-recovered <base_url> <sub> <state_file>
      Pulls from `sub` until no more messages are returned (bounded
      retries — a fresh process needs a moment to become ready and,
      depending on timing, the self-healing lease reclaim needs the mock/real
      clock to have actually passed the original ack deadline). Asserts
      the set of (message_id, data) pulled matches `state_file` exactly
      — same ids, same payload, nothing extra, nothing missing. Exits
      non-zero with a clear message on mismatch.

  verify-empty <base_url> <sub>
      Asserts a Pull against `sub` returns zero messages (the
      `--ephemeral` variant: nothing should have survived the restart).
"""

import base64
import json
import sys
import time
import urllib.error
import urllib.request


def _request(method: str, url: str, body: dict | None = None) -> dict:
    data = None
    if method != "GET":
        data = json.dumps(body).encode("utf-8") if body is not None else b"{}"
    req = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        raise SystemExit(f"{method} {url} -> HTTP {e.code}: {raw}")


def _http_status(method: str, url: str, body: dict | None = None) -> int:
    """Like `_request`, but returns the HTTP status code instead of
    raising/parsing — for a probe that expects (and is fine with) a 404."""
    data = json.dumps(body).encode("utf-8") if body is not None and method != "GET" else None
    req = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code


def cmd_setup(base_url: str, project: str, topic: str, sub: str) -> None:
    topic_path = f"projects/{project}/topics/{topic}"
    try:
        _request("PUT", f"{base_url}/v1/projects/{project}/topics/{topic}", {})
    except SystemExit as e:
        if "409" not in str(e):
            raise
    try:
        _request(
            "PUT",
            f"{base_url}/v1/projects/{project}/subscriptions/{sub}",
            {"topic": topic_path},
        )
    except SystemExit as e:
        if "409" not in str(e):
            raise
    print(f"setup: topic={topic_path} sub={sub} ready")


def cmd_publish_pull_ack_half(
    base_url: str, project: str, topic: str, sub: str, count: int, state_file: str
) -> None:
    messages = [
        {"data": base64.b64encode(f"payload-{i}".encode()).decode()}
        for i in range(count)
    ]
    resp = _request(
        "POST",
        f"{base_url}/v1/projects/{project}/topics/{topic}:publish",
        {"messages": messages},
    )
    message_ids = resp["messageIds"]
    assert len(message_ids) == count, f"expected {count} message ids, got {len(message_ids)}"
    print(f"published {count} messages: ids {message_ids[0]}..{message_ids[-1]}")

    pulled = []
    remaining = count
    # `maxMessages` is capped at 1000 server-side; loop defensively in case
    # `count` ever exceeds that or a single Pull doesn't return everything
    # at once.
    for _ in range(count + 5):
        if remaining <= 0:
            break
        resp = _request(
            "POST",
            f"{base_url}/v1/projects/{project}/subscriptions/{sub}:pull",
            {"maxMessages": min(remaining, 1000)},
        )
        batch = resp.get("receivedMessages", [])
        if not batch:
            break
        pulled.extend(batch)
        remaining -= len(batch)
    assert len(pulled) == count, f"expected to pull {count} messages, got {len(pulled)}"

    pulled.sort(key=lambda m: int(m["message"]["messageId"]))
    half = count // 2
    to_ack, to_leave = pulled[:half], pulled[half:]

    ack_ids = [m["ackId"] for m in to_ack]
    for i in range(0, len(ack_ids), 1000):
        _request(
            "POST",
            f"{base_url}/v1/projects/{project}/subscriptions/{sub}:acknowledge",
            {"ackIds": ack_ids[i : i + 1000]},
        )
    print(f"acked {len(to_ack)} messages, leaving {len(to_leave)} outstanding")

    expected = sorted(
        [
            {
                "messageId": m["message"]["messageId"],
                "data": m["message"]["data"],
            }
            for m in to_leave
        ],
        key=lambda m: int(m["messageId"]),
    )
    with open(state_file, "w") as f:
        json.dump(expected, f)
    print(f"wrote expected post-restart state ({len(expected)} messages) to {state_file}")


def cmd_verify_recovered(base_url: str, project: str, sub: str, state_file: str) -> None:
    with open(state_file) as f:
        expected = json.load(f)

    pulled: list[dict] = []
    seen_ids: set[str] = set()
    # Bounded retry loop: a message whose ack deadline hasn't elapsed yet
    # (relative to the new process's clock) won't be redelivered
    # immediately after recovery — that's correct behavior (see
    # `tests/integration/tests/recovery.rs`), not a bug, but this script
    # doesn't control server-side timing, so it polls.
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and len(pulled) < len(expected):
        resp = _request(
            "POST",
            f"{base_url}/v1/projects/{project}/subscriptions/{sub}:pull",
            {"maxMessages": 1000},
        )
        batch = resp.get("receivedMessages", [])
        for m in batch:
            mid = m["message"]["messageId"]
            if mid not in seen_ids:
                seen_ids.add(mid)
                pulled.append(m)
        if not batch:
            time.sleep(0.5)

    actual = sorted(
        [
            {"messageId": m["message"]["messageId"], "data": m["message"]["data"]}
            for m in pulled
        ],
        key=lambda m: int(m["messageId"]),
    )

    if actual != expected:
        expected_ids = {m["messageId"] for m in expected}
        actual_ids = {m["messageId"] for m in actual}
        missing = sorted(expected_ids - actual_ids, key=int)
        extra = sorted(actual_ids - expected_ids, key=int)
        raise SystemExit(
            "post-restart recovery mismatch: "
            f"expected {len(expected)} messages, got {len(actual)}. "
            f"missing (should have been redelivered but weren't): {missing[:10]}"
            f"{'...' if len(missing) > 10 else ''}. "
            f"extra (should not have reappeared — likely an acked message "
            f"came back): {extra[:10]}{'...' if len(extra) > 10 else ''}"
        )
    print(
        f"OK: exactly {len(actual)} unacked messages recovered, "
        "same ids and payload, none of the acked ones reappeared"
    )


def cmd_verify_empty(base_url: str, project: str, sub: str) -> None:
    # With `--ephemeral`, the subscription itself does not survive a
    # restart (nothing was ever written to disk) — so the meaningful
    # assertion is that it is gone entirely (404), not merely that a Pull
    # against a still-existing subscription happens to return nothing.
    status = _http_status(
        "GET", f"{base_url}/v1/projects/{project}/subscriptions/{sub}"
    )
    if status != 404:
        raise SystemExit(
            f"expected the subscription to be gone (404) after an --ephemeral "
            f"restart, got HTTP {status}"
        )
    print("OK: subscription (and everything else) is gone after --ephemeral restart, as expected")


def main() -> None:
    args = sys.argv[1:]
    if not args:
        raise SystemExit(__doc__)
    cmd, rest = args[0], args[1:]
    if cmd == "setup":
        cmd_setup(*rest)
    elif cmd == "publish-pull-ack-half":
        base_url, project, topic, sub, count, state_file = rest
        cmd_publish_pull_ack_half(base_url, project, topic, sub, int(count), state_file)
    elif cmd == "verify-recovered":
        cmd_verify_recovered(*rest)
    elif cmd == "verify-empty":
        cmd_verify_empty(*rest)
    else:
        raise SystemExit(f"unknown subcommand: {cmd}\n\n{__doc__}")


if __name__ == "__main__":
    main()
