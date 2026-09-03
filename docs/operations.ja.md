# 運用ランブック

> English: [operations.md](operations.md)

## 目次

- [概要](#概要)
- [systemd での運用](#systemd-での運用)
- [Docker での運用](#docker-での運用)
- [ログ](#ログ)
- [メトリクス](#メトリクス)
- [停止挙動（シグナル・終了コード）](#停止挙動シグナル終了コード)
- [data_dir のバックアップ](#data_dir-のバックアップ)
- [アップグレード手順とフォーマットバージョン](#アップグレード手順とフォーマットバージョン)
- [トラブルシューティング](#トラブルシューティング)

## 概要

`open-pubusb` を systemd サービスまたは Docker コンテナとして運用する際の日常操作（起動・停止・ログ確認・メトリクス確認・バックアップ・アップグレード）をまとめる。デプロイ自体の手順は [`ansible/README.ja.md`](../ansible/README.ja.md)、設定項目の全リファレンスは `crates/open-pubusb/src/config.rs` を参照。

## systemd での運用

```bash
systemctl status open-pubusb
systemctl restart open-pubusb
journalctl -u open-pubusb -f                 # ログ追跡
journalctl -u open-pubusb -o json --since "10 min ago"   # 構造化フィールド込みで参照
```

- `Type=notify`: ストレージ復旧完了・待受開始後に `READY=1` を通知するので、`systemctl start` はサービスが実際に受け付け可能になるまでブロックする。
- `WatchdogSec=30`: プロセスが応答しなくなった場合、systemd が自動再起動する。
- データディレクトリは `DynamicUser=true` + `StateDirectory=open-pubusb` により `/var/lib/open-pubusb` に固定される（`OPEN_PUBUSB__STORAGE__DATA_DIR` として自動注入）。
- `SIGTERM`（`systemctl stop`）は [停止挙動](#停止挙動シグナル終了コード) の優雅停止シーケンスに従う。`TimeoutStopSec` は `shutdown_grace_secs + 5` 秒。

## Docker での運用

```bash
docker logs -f open-pubusb
docker exec open-pubusb /usr/local/bin/open-pubusb health --url http://127.0.0.1:8086/readyz
docker restart open-pubusb
```

- データは名前付きボリューム（既定パス `/data`、`OPEN_PUBUSB__STORAGE__DATA_DIR=/data`）に永続化する。コンテナを削除してもボリュームを維持すれば再作成後にデータを引き継げる。
- `HEALTHCHECK` は `open-pubusb health` を10秒間隔で実行する（`docker inspect --format='{{json .State.Health}}' open-pubusb` で確認可能）。
- 非 root ユーザー（distroless の `nonroot`）で実行される。

## ログ

- 1 行 1 JSON を stdout へ出力（systemd 配下では `format=journald` が既定、journal の構造化フィールドとして格納される）。
- フィールド: `timestamp`（RFC3339）、`level`、`target`、`message`、および構造化フィールド `topic` / `subscription` / `message_id` / `ack_id` / `stream_id` / `grpc.method` / `grpc.code` / `elapsed_ms`。
- リクエストログ（gRPC メソッド・コード・所要時間）は `info` レベルで 1 リクエスト 1 行。Publish/Pull などの高頻度呼び出しは `debug`。
- **メッセージ本文・属性値はいかなるログレベルでも出力しない**（PII 漏洩防止のための設計上の制約）。

## メトリクス

`GET /metrics`（既定では管理ポート 8086）で Prometheus テキスト形式、接頭辞 `open_pubusb_`。

| 名前 | 型 | ラベル | 意味 |
|---|---|---|---|
| `open_pubusb_topics` | gauge | — | トピック数 |
| `open_pubusb_subscriptions` | gauge | — | サブスクリプション数 |
| `open_pubusb_messages_published_total` | counter | `topic` | 発行件数 |
| `open_pubusb_messages_delivered_total` | counter | `subscription`, `mode` | 配信件数（再配信含む、mode=pull/streaming/push） |
| `open_pubusb_messages_acked_total` | counter | `subscription` | Ack 件数 |
| `open_pubusb_messages_expired_total` | counter | `subscription` | 保持期間切れで破棄 |
| `open_pubusb_messages_dead_lettered_total` | counter | `subscription` | DLQ 転送 |
| `open_pubusb_unacked_messages` | gauge | `subscription` | 未 Ack（配信待ち + リース中） |
| `open_pubusb_oldest_unacked_age_seconds` | gauge | `subscription` | 最古未 Ack の経過秒 |
| `open_pubusb_publish_latency_seconds` | histogram | — | Publish RPC 所要時間 |
| `open_pubusb_grpc_requests_total` | counter | `method`, `code` | gRPC 呼び出し数 |
| `open_pubusb_push_requests_total` | counter | `subscription`, `result` | Push 配送結果（ok/fail） |
| `open_pubusb_storage_sync_duration_seconds` | histogram | — | グループ fsync 所要時間 |
| `open_pubusb_storage_disk_bytes` | gauge | — | data_dir 使用量 |
| `open_pubusb_streaming_pull_streams` | gauge | — | 接続中 StreamingPull ストリーム数 |

ラベル `subscription`/`topic` は完全名。リソース数が 1,000 を超えるとラベル付き系列を打ち切り、`open_pubusb_metrics_truncated=1` を立てる（カーディナリティ爆発対策）。

**アラート監視の起点として有用な指標**:
- `open_pubusb_oldest_unacked_age_seconds` の急増 — サブスクライバー側の消化遅延
- `open_pubusb_storage_disk_bytes` が `storage.max_disk_bytes` に接近 — 書き込み拒否（`RESOURCE_EXHAUSTED`）が近い
- `open_pubusb_grpc_requests_total{code!="OK"}` の増加率

## 停止挙動（シグナル・終了コード）

| シグナル | 挙動 |
|---|---|
| `SIGTERM` / `SIGINT` | 優雅停止: `/readyz` が `503` に切り替わる → 新規接続を拒否 → 進行中の `StreamingPull` へ `UNAVAILABLE` を送る → 進行中 RPC を `shutdown_grace_secs`（既定 30 秒）まで待つ → ストレージを最終 fsync → 終了コード `0` |
| 猶予超過 | 残処理を打ち切り最終 fsync を実行して終了コード `0`（`warn` ログを残す） |
| `SIGHUP` | 無視（設定の動的再読込はしない。設定変更は再起動で反映） |

| 終了コード | 意味 |
|---|---|
| `0` | 正常終了 |
| `1` | 実行時致命エラー（ストレージ破損など） |
| `2` | 設定・環境エラー（起動前の検証失敗） |

停止猶予内であれば、受理済み・応答済みのメッセージが失われることはない（受理は OS バッファへの書き込み完了が条件）。

## data_dir のバックアップ

`data_dir`（systemd: `/var/lib/open-pubusb`、Docker: 名前付きボリューム）は `fjall` のキースペースファイル群のみで構成される。

- **稼働中のホットバックアップは推奨しない** — `fjall` は複数ファイルにまたがる LSM ストアであり、書き込み中に外部からファイルコピーすると整合性のないスナップショットになりうる。バックアップは以下のいずれかで行う:
  1. サービスを一時停止（`systemctl stop open-pubusb` / `docker stop open-pubusb`）してから `data_dir` をまるごとコピーし、再開する。
  2. ファイルシステム／ブロックデバイスレベルのスナップショット機能（LVM snapshot、ZFS/btrfs snapshot、クラウドディスクのスナップショット等）を使う（アプリケーション側の静止点を意識する必要がないため、稼働中でも安全）。
- リストアは、停止した状態のサービスに対して `data_dir` を復元後 → 起動、の順で行う。フォーマットバージョンが一致していることを確認する（次節）。
- `--ephemeral` モードで運用している場合、`data_dir` は起動のたびに空の一時ディレクトリになる（永続化されない）ため、バックアップ対象がそもそも存在しない。

## アップグレード手順とフォーマットバージョン

`data_dir` の `meta` キースペースには `__open_pubusb_format_version` マーカーが書き込まれており、起動したバイナリが対応する最大フォーマットバージョンより新しいマーカーが見つかった場合、起動はエラーで拒否される（ダウングレード後の誤動作・データ破損を防ぐため）。

推奨アップグレード手順:

1. 新バージョンのリリースノートでフォーマットバージョンの変更有無を確認する。
2. [`data_dir` のバックアップ](#data_dir-のバックアップ)を取得する。
3. サービスを停止する。
4. バイナリ（systemd）またはイメージ（Docker）を新バージョンに入れ替える（Ansible: `open_pubusb_version` / `open_pubusb_image_tag` を更新して再実行）。
5. 起動し、`/readyz` が `200` になることを確認する。
6. 問題があれば手順 2 のバックアップから旧バージョンのバイナリ/イメージでロールバックする。

ダウングレード（新フォーマットのデータを旧バイナリで開く）は上記の起動時チェックにより明示的なエラーで失敗する — サイレントなデータ破損は起きない。

## トラブルシューティング

| 症状 | 確認ポイント |
|---|---|
| `/readyz` が `503` のまま | ストレージ復旧が完了していない（`data_dir` が巨大、または破損）。ログの起動時エラーを確認 |
| 起動直後に終了コード `2` | 設定エラー。`open-pubusb check-config --config <path>` で事前検証できる |
| 起動直後に終了コード `1` | ストレージ破損の可能性。フォーマットバージョン不一致もこのケース（エラーメッセージに詳細が出る） |
| `RESOURCE_EXHAUSTED` が返る | `storage.max_disk_bytes` に到達。`open_pubusb_storage_disk_bytes` メトリクスで確認し、ディスク拡張または保持期間短縮を検討 |
| Push 配信が届かない | `open_pubusb_push_requests_total{result="fail"}` を確認。エンドポイント側のログ、タイムアウト設定（既定 10 秒）も確認 |
