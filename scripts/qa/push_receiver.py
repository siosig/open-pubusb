#!/usr/bin/env python3
"""Minimal push-endpoint receiver for manual/QA testing (tasks.md T067).

Listens on `--port`, accepts POST requests carrying either a wrapped
(`PubsubWrapper`) JSON envelope or an unwrapped (`NoWrapper`) raw body with
`x-goog-pubsub-*` headers, and prints each received envelope as one JSON
line to stdout — so it composes with `jq`/`grep` for quick inspection.

`--fail-first N` makes the first N requests return HTTP 500 (a push
failure, triggering open-pubusb's backoff/retry), and the (N+1)th request onward
return 200 — handy for watching a message actually get retried and then
finally acknowledged.

Usage:
    python3 scripts/qa/push_receiver.py --port 9000 --fail-first 2
"""

import argparse
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def make_handler(fail_first: int):
    request_count = {"n": 0}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):  # noqa: A002 - stdlib signature
            pass  # keep stdout clean for the JSON-lines output

        def do_POST(self):  # noqa: N802 - stdlib method name
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length) if length else b""
            content_type = self.headers.get("Content-Type", "")

            record = {
                "path": self.path,
                "content_type": content_type,
                "headers": {
                    k: v
                    for k, v in self.headers.items()
                    if k.lower().startswith("x-goog-pubsub-") or k.lower() not in ("host", "content-length", "content-type", "user-agent", "accept-encoding", "connection")
                },
            }
            if "application/json" in content_type:
                try:
                    record["envelope"] = json.loads(body)
                except json.JSONDecodeError:
                    record["raw_body_base64_decode_failed"] = True
            else:
                record["raw_body"] = body.decode("utf-8", errors="replace")

            print(json.dumps(record), flush=True)

            request_count["n"] += 1
            if request_count["n"] <= fail_first:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(b"simulated failure")
            else:
                self.send_response(200)
                self.end_headers()

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument(
        "--fail-first",
        type=int,
        default=0,
        help="return HTTP 500 for this many requests before succeeding",
    )
    args = parser.parse_args()

    server = ThreadingHTTPServer(("0.0.0.0", args.port), make_handler(args.fail_first))
    print(
        f"push_receiver: listening on :{args.port}, failing the first {args.fail_first} request(s)",
        file=sys.stderr,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
