# 互換性テスト（3 言語）

> English: [README.md](README.md)

`PUBSUB_EMULATOR_HOST` を `open-pubusb` に向けて、公式（または準公式）クライアントライブラリが接続先設定の変更のみで動作することを確認する。

- `python/` — `google-cloud-pubsub` + pytest
- `node/` — `@google-cloud/pubsub` + node:test
- `rust/` — `gcloud-pubsub`（主）+ `google-cloud-pubsub`（副、endpoint 上書き）
