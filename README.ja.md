# open-pubusb

> English: [README.md](README.md)

GCP Pub/Sub v1 互換のメッセージングサービス（Rust 実装）。ローカルホストで systemd サービスまたは Docker コンテナとして動作する。

## 目次

- [概要](#概要)
- [セットアップ](#セットアップ)
- [クイックスタート](#クイックスタート)
- [クライアント接続方法（言語別）](#クライアント接続方法言語別)
- [対応 API](#対応-api)
  - [gRPC](#grpc)
  - [REST（部分集合）](#rest部分集合)
- [設定](#設定)
- [Docker での実行](#docker-での実行)
- [Ansible でのデプロイ](#ansible-でのデプロイ)
- [運用エンドポイント](#運用エンドポイント)
- [ベンチマーク](#ベンチマーク)
- [本番 Pub/Sub との差分（Deviations）](#本番-pubsub-との差分deviations)
- [テスト](#テスト)
- [詳細ドキュメント](#詳細ドキュメント)
- [ライセンス](#ライセンス)

## 概要

`open-pubusb` は、Google Cloud Pub/Sub v1 の gRPC / REST API 互換サーバーをローカルホスト上で動かすための単一バイナリ実装。公式クライアントライブラリ（Python / Node.js / Go / Rust など）を無変更のまま接続先だけ切り替えて使える。トピック／サブスクリプション CRUD、Publish/Pull/Ack、StreamingPull、Push 配信、フィルタ、DLQ（デッドレターキュー）、再試行バックオフ、Snapshot/Seek に対応する。永続化には組み込み LSM ストア（fjall）を使い、`--ephemeral` 指定でメモリのみでも動く。systemd サービスまたは Docker コンテナとして、Ansible ロール経由でデプロイできる。

## セットアップ

```bash
git submodule update --init third_party/googleapis
cargo check --workspace
```

## クイックスタート

```bash
# メモリのみで起動（開発・テスト向け）
cargo run -p open-pubusb -- serve --ephemeral

# 永続化して起動（既定ポート 8085 = gRPC+REST、8086 = 管理用）
cargo run -p open-pubusb -- serve --data-dir ./data
```

起動後、`GET http://127.0.0.1:8086/readyz` が `200` を返せば準備完了。

## クライアント接続方法（言語別）

`open-pubusb` は TLS なし・匿名接続（`authorization` ヘッダは受理するが無視）。

| 言語 | 接続方法 |
|---|---|
| Python（`google-cloud-pubsub`） | 環境変数 `PUBSUB_EMULATOR_HOST=127.0.0.1:8085` を設定するだけ。アプリケーションコードの変更は不要 |
| Node.js（`@google-cloud/pubsub`） | 同上、`PUBSUB_EMULATOR_HOST=127.0.0.1:8085` |
| Rust（`gcloud-pubsub` / `google-cloud-pubsub`） | `gcloud-pubsub` は `PUBSUB_EMULATOR_HOST` 対応。Google 公式 Rust SDK `google-cloud-pubsub` は環境変数非対応のため `Client::builder().with_endpoint("http://127.0.0.1:8085")` + 匿名認証で接続する |
| その他（gRPC 直接） | 任意の gRPC クライアントで `http://127.0.0.1:8085` に insecure channel 接続。`grpcurl` 利用時は `grpc.reflection.v1` が既定で有効 |

## 対応 API

サービス定義は `third_party/googleapis` サブモジュールの `google.pubsub.v1` proto に基づく。要点のみ以下に示す。

### gRPC

`google.pubsub.v1.Publisher` / `Subscriber` の全メソッドを実装（Topic/Subscription CRUD、Publish、Pull、StreamingPull、ModifyPushConfig、Snapshot 系、Seek）。`google.pubsub.v1.SchemaService` と `google.iam.v1.IAMPolicy` は `UNIMPLEMENTED` を返す（[差分](#本番-pubsub-との差分deviations)参照）。`grpc.health.v1.Health` と `grpc.reflection.v1` にも対応。

### REST（部分集合）

Cloud Pub/Sub REST と同じパス・JSON 形（proto3 JSON マッピング）で、以下のみ実装。それ以外の `/v1/...` パスは `501 Not Implemented` を返す。

| HTTP | パス | 対応 gRPC メソッド |
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

StreamingPull・Push・Snapshot/Seek・DLQ・フィルタ・再試行バックオフは gRPC のみ（REST では未提供、`501`）。

## 設定

CLI フラグ・設定ファイル（TOML）・環境変数（`OPEN_PUBUSB__<section>__<key>` 形式）に対応し、既定値 < 設定ファイル < 環境変数 < CLI フラグの順で上書きされる。全項目の定義は `crates/open-pubusb/src/config.rs` を参照。

```bash
open-pubusb serve        [--config <path>] [--listen <addr>] [--admin-listen <addr>] [--data-dir <path>] [--ephemeral]
open-pubusb check-config [--config <path>]
open-pubusb health       [--url <http://127.0.0.1:8086/readyz>] [--timeout <secs>]
open-pubusb version
```

## Docker での実行

```bash
docker build -t open-pubusb:local .
docker run --rm -p 8085:8085 -p 8086:8086 \
  -v open-pubusb-data:/data \
  open-pubusb:local
```

`gcr.io/distroless/static-debian12:nonroot` ベースの静的バイナリ、非 root ユーザーで実行。`HEALTHCHECK` に `open-pubusb health` を使用。詳細は `Dockerfile` を参照。

## Ansible でのデプロイ

systemd サービスまたは Docker コンテナとしてのデプロイを Ansible ロール `open_pubusb` が提供する。

```bash
cd ansible
ansible-galaxy collection install -r requirements.yml
ansible-playbook -i inventory/hosts.yml site.yml -e open_pubusb_deploy_mode=systemd
# または
ansible-playbook -i inventory/hosts.yml site.yml -e open_pubusb_deploy_mode=docker
```

公開変数・タグ・冪等性の詳細は [`ansible/README.ja.md`](ansible/README.ja.md) を参照。

## 運用エンドポイント

| パス | 内容 |
|---|---|
| `GET /healthz` | プロセス生存確認。常に `200` |
| `GET /readyz` | 準備完了確認。ストア復旧完了・待受開始後 `200`、停止処理中は `503` |
| `GET /metrics` | Prometheus テキスト形式のメトリクス |

## ベンチマーク

```bash
cargo run -p open-pubusb-bench --release -- publish \
  --endpoint 127.0.0.1:8085 --msg-size 1024 --rate 1000 --duration 30 \
  --topics 4 --subscribers 2

cargo bench -p open-pubusb-bench
```

`publish` サブコマンドは Publish のスループットと p50/p99 レイテンシを計測する。`cargo bench` はフィルタ評価・キー符号化のマイクロベンチマーク（criterion）。

## 本番 Pub/Sub との差分（Deviations）

| 項目 | 挙動 |
|---|---|
| `enable_exactly_once_delivery` | 値は受理・保持するが、実際の配信は常に at-least-once（exactly-once セマンティクスは実装しない） |
| `SchemaService` | 全メソッド `UNIMPLEMENTED`（スキーマ検証なし） |
| `IAMPolicy` | 全メソッド `UNIMPLEMENTED`（アクセス制御なし） |
| TLS / 認証 | なし。平文通信、`authorization` ヘッダは無視（匿名接続前提） |
| ノード構成 | 単一プロセス・単一ノードのみ。クラスタリング・レプリケーションなし |
| 永続化レベル | `--ephemeral` 未指定時、書き込みは OS バッファへ即時反映（`kill -9` に耐える）が、fsync は `storage.sync_interval_ms` ごとのバッチ実行 — 電源断に対する完全な耐久性は保証しない |
| `bigquery_config` / `cloud_storage_config` / `schema_settings` / `ingestion_data_source_settings` | 指定すると `INVALID_ARGUMENT`（未対応） |

## テスト

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings -W clippy::unwrap_used -W clippy::expect_used
tests/compat/run_all.sh   # 3言語（Python/Node.js/Rust）公式クライアント互換スイート
```

## 詳細ドキュメント

- 運用ランブック: [`docs/operations.ja.md`](docs/operations.ja.md)
- アーキテクチャ: [`docs/architecture.ja.md`](docs/architecture.ja.md)

## ライセンス

MIT License と Apache License 2.0 のデュアルライセンス（`MIT OR Apache-2.0`）。どちらか一方を選択して利用できる。

- [`LICENSE-MIT`](LICENSE-MIT)
- [`LICENSE-APACHE`](LICENSE-APACHE)
