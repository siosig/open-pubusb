# open-pubusb Python サンプル

> English: [README.md](README.md)

## 目次

- [概要](#概要)
- [セットアップ](#セットアップ)
- [実行方法](#実行方法)
- [サンプルの内容](#サンプルの内容)

## 概要

Google Cloud 公式クライアント（`google-cloud-pubsub`）を使い、接続先だけ
`PUBSUB_EMULATOR_HOST` でローカルの `open-pubusb` に向けて、Topic / Subscription
の作成・削除とメッセージの発行（push）・取得＆ack（pull & ack）を行う最小サンプル。

## セットアップ

```bash
pip install -r sample/python/requirements.txt
```

`open-pubusb` を起動しておく（別ターミナル、リポジトリルートで）:

```bash
cargo run -p open-pubusb -- serve --ephemeral --listen 127.0.0.1:8085 --admin-listen 127.0.0.1:8086
```

## 実行方法

```bash
PUBSUB_EMULATOR_HOST=127.0.0.1:8085 python3 sample/python/pubsub_demo.py
```

## サンプルの内容

`pubsub_demo.py` は以下を順番に実行する。

1. トピック作成（`create_topic`）
2. サブスクリプション作成（`create_subscription`）
3. メッセージ発行（`push_message`）
4. メッセージ取得＆ack（`pull_and_ack`）— 再度 pull しても ack 済みメッセージが返らないことを確認
5. サブスクリプション削除（`delete_subscription`）
6. トピック削除（`delete_topic`）
