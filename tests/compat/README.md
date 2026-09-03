# Compatibility Tests (3 Languages)

> Japanese: [README.ja.md](README.ja.md)

Point `PUBSUB_EMULATOR_HOST` at `open-pubusb` and verify that the official (or semi-official) client libraries work with nothing but an endpoint change.

- `python/` — `google-cloud-pubsub` + pytest
- `node/` — `@google-cloud/pubsub` + node:test
- `rust/` — `gcloud-pubsub` (primary) + `google-cloud-pubsub` (secondary, endpoint override)
