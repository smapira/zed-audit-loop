# zed-audit-loop

Zed の外部スレッドメッセージブリッジ（Unix domain socket / NDJSON）を使い、
**監査 1 サイクルを回し続けるクライアント側デーモン**です。
ブリッジ本体（Zed 側）は別リポジトリで実装済みで、このリポジトリは変更しません。

- 仕様の正: [`docs/external-bridge-spec.md`](docs/external-bridge-spec.md)（§3〜§7, §10, §11）
- 実装計画と BDD タスク: [`docs/plan/20261108-audit-daemon-plan.md`](docs/plan/20261108-audit-daemon-plan.md)
- 要件対応表: [`docs/requirements-traceability.md`](docs/requirements-traceability.md)

対象プラットフォームは **macOS のみ**です（ブリッジの peer uid 検証が macOS 固有のため）。

## 1. 何をするか

1 サイクルごとに、次の順で動きます（仕様書 §8 のシーケンス）。

```
1. 対象スレッドを決める（明示 thread_id / list_threads で最新を選ぶ）
2. 専用接続で watch_thread          → 以後この接続にイベントが流れる
3. get_thread_status                → state を確認（awaiting_approval なら送らずに待つ）
4. send_message（mode: prompt）     → delivered / queued
5. イベントを読む                   → status_changed / entry_added / turn_stopped …
6. entry.text から応答本文を組み立てて保存
7. サイクル間隔だけ待つ
```

`send_message` の `delivered` は「スレッド履歴に積んだ」の意味で、応答完了ではありません。
応答本文は `watch_thread` のイベントにしか存在せず、購読は過去を再生しないため、
**受け取った `entry.text` はその場でディスクに書きます**。

## 2. 前提（Zed 側）

```json
{
  "agent_external_bridge": {
    "enabled": true,
    "transport": "unix_socket",
    "allow_prompt": true,
    "allow_notify": false
  }
}
```

- ブリッジは既定で無効です。有効化すると socket が作られます。
- socket は `~/Library/Application Support/Zed/agent-bridge-<channel>.sock`。
  ローカルビルドの Zed なら `dev`（本デーモンでは `[bridge] channel = "dev"`）。

## 3. ビルド

```sh
cargo build --release
# 生成物: target/release/zed-audit-loop
```

## 4. 事前疎通確認（実装前に必ず）

Zed 側の `zed-bridge` CLI で、ブリッジが生きていることを確認します（仕様書 §9）。

```sh
zed-bridge list-threads                       # 一覧が出れば疎通 OK
ZED_BRIDGE_CHANNEL=dev zed-bridge list-threads # dev ビルドの Zed
```

本デーモンにも同じ確認用のサブコマンドがあります。

```sh
zed-audit-loop --config config.toml list-threads
zed-audit-loop --config config.toml thread-status <thread-id>
```

socket が無い・接続を拒否される場合は、クライアントからは「ブリッジ無効」か
「Zed 未起動」かを区別できません。エラーメッセージに両方の候補を出します。

## 5. 設定

例: [`examples/config.toml`](examples/config.toml)。全項目に既定値があり、
変えたい値だけ書けば済みます。未知のキーは起動時にエラーになります（タイポ検出）。

| キー | 既定 | 意味 |
| --- | --- | --- |
| `bridge.channel` | `stable` | Zed のリリースチャンネル。socket 名に使う |
| `bridge.socket` | （なし） | 明示的な socket パス。`channel` より優先。`~/` 展開可 |
| `bridge.connect_timeout_ms` | `3000` | 接続タイムアウト |
| `bridge.request_timeout_ms` | `20000` | 1 要求の応答待ちタイムアウト |
| `target.thread_id` | （なし） | 監査対象の ThreadId（UUID） |
| `target.select` | `explicit` | `latest` にすると `list_threads` の先頭（archived/draft を除く） |
| `cycle.interval_secs` | `300` | サイクル間隔 |
| `cycle.await_response_secs` | `900` | 応答待ちの上限 |
| `cycle.approval_poll_ms` | `30000` | 承認待ちのポーリング間隔 |
| `cycle.max_approval_wait_secs` | `3600` | 承認待ちの上限 |
| `cycle.backoff_initial_ms` / `backoff_max_ms` | `500` / `60000` | `queue_full` 等の指数バックオフ |
| `cycle.max_send_attempts` | `6` | 1 メッセージの最大試行回数 |
| `storage.dir` | `~/Library/Application Support/zed-audit-loop` | 状態保存先 |
| `finding.source` | `file` | `file` または `command` |
| `finding.file` | （なし） | `file` のときの JSONL パス |
| `finding.command` / `args` | （なし） | `command` のときの実行コマンド |
| `finding.max_message_bytes` | `48000` | 1 メッセージの上限（64 KiB 行長の内側に収める） |
| `log.level` | `info` | `debug` / `info` / `warn` / `error` |
| `log.file` | （なし） | JSONL ログの追記先 |
| `log.stderr` | `true` | stderr にも出すか |

## 6. 起動

```sh
zed-audit-loop --config config.toml run
```

- `SIGINT` / `SIGTERM` でサイクルの切れ目に終了します（終了コード 0）。
- ログは 1 行 1 JSON で、`cycle_id` / `thread_id` / `request_id` で追跡できます。

```json
{"ts":"2026-11-08T07:33:48.980Z","level":"info","event":"send_accepted",
 "cycle_id":"c1-1794123228921","thread_id":"6f1c…","request_id":"audit-6f1c2d3e-8f8f06f53cd4-0","queued":"false"}
```

## 7. 保存されるもの

`storage.dir` 配下:

| ファイル | 内容 |
| --- | --- |
| `ledger.jsonl` | 送信台帳。**送信前に書く**（write-ahead）。`request_id`・送信内容・最終状態 |
| `events/<thread>.jsonl` | 受信した全イベント（`entry.text` 含む。購読は過去を再生しないため必須） |
| `turns/<thread>.jsonl` | ターン完了ごとに、エージェントの応答本文を連結して 1 行 |
| `findings.cursor.json` | 監査結果ファイルのどこまで消費したか |
| `daemon.jsonl` | （設定した場合）構造化ログ |

### `request_id` の一意性と再送抑止

`request_id = audit-<thread 先頭8>-<内容ハッシュ12>-<attempt>` です。

- 内容（送信本文）から決まるので、**クラッシュ後に同じ内容を作り直すと同じ id に戻り**、
  台帳が再送を止めます。これがブリッジ側台帳（メモリ保持）に頼らない二重配送防止です。
- `attempt` を進めるのは **ブリッジ側が台帳に記録する拒否**（`queue_full` /
  `awaiting_approval` / `thread_archived`）のときだけ。これらは同じ id で再送すると
  `duplicate_request` になり「成功」と誤認するため、新しい id が必要です。
- 記録されない拒否（`thread_unavailable` / `thread_not_found` / `unsupported_thread_type` /
  設定系 / 転送エラー）は同じ id のまま再試行します。

### トレードオフ（at-most-once）

送信直後に落ちて応答を記録できなかった場合（台帳が `sending` のまま）、デーモンは
**そのメッセージを再送しません**。起動時に `get_delivery_status` で照合し、ブリッジが
知らなければ `unknown` として警告ログを残します。
監査ノートの二重配送より、稀な取りこぼしを選ぶ判断です。

## 8. 監査内容の供給（`FindingSource`）

「何を監査し、どんな文面を送るか」は仕様書の範囲外なので、このリポジトリは
**境界だけ**を提供します。

```rust
pub trait FindingSource {
    fn read_batch(&mut self) -> Result<FindingBatch>;   // 読むだけ（前進しない）
    fn commit(&mut self, cursor: &FindingCursor) -> Result<()>;  // 送信後に前進
    fn describe(&self) -> String;
}
```

- `file`: JSONL を読み、消費位置を `findings.cursor.json` に永続化。追記途中の不完全行は次回に回す。
- `command`: サイクルごとにコマンドを 1 回実行し、stdout の JSONL を読む（終了コード非 0 はそのサイクルをスキップ）。
- 2 相コミットなので、送信に失敗したサイクルでは結果が消費されません。

`Finding` の形:

```json
{"id":"F-12","severity":"warning","summary":"仕様 §6 と実装が一致しない",
 "detail":"`deliverable` を送信可否の判断に使っている","location":"src/daemon.rs:120"}
```

**LLM による監査判断や Issue 管理は実装していません。** それらは `file` / `command` の
外側で行い、結果を JSONL で流し込む想定です。

## 9. 既知の制約

- `awaiting_approval` は迂回しません（仕様どおり）。承認は Zed の UI が持ちます。
- `notify` モードは未実装なので使いません（常に `prompt`）。
- サイクル間はイベントを読みません。購読接続は保持しますが、ターン外のイベントは
  次に読むまでソケットバッファに溜まります（`storage` のイベント列はターン中のみ）。
- `command` ソースはサイクルごとに同期実行です（1 回/サイクルなので許容）。
- ソケットパスは Unix socket の長さ制限（`SUN_LEN`）を受けるため、深いディレクトリに置けません。
- 復元時に Zed の Agent Panel 表示が対象スレッドへ切り替わる場合があります（ブリッジ側の性質）。

## 10. 未検証事項（成功扱いにしないこと）

この環境には実スレッドを持つ Zed が無いため、次は**未検証**です。

1. **実 Zed の実スレッドへの配送と次ターン開始**: 実際にメッセージが履歴に増え、
   `generating` → `idle` になること。
2. **実 `watch_thread` のイベント受信**: 実装とテスト（fake bridge）のみ。
3. **実機の `queue_full`**: 16 件キューを埋める必要がある。テストは fake で分岐のみ確認。
4. **`LOCAL_PEERCRED` による uid 検証**: サーバー側の機能で、クライアントからは観測不可。

実機での確認手順は仕様書 §11 のとおりです（Zed を `enabled: true` で起動 → スレッド作成 →
`zed-bridge send --thread <T>` → 本デーモンで `list-threads` / `thread-status`）。

## 11. テスト

```sh
cargo test          # 92 tests: lib 55 / daemon_cycle 21 / protocol_conformance 16
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

実 Zed が無くてもクライアントの振る舞いを検証できるよう、
**fake bridge server**（[`tests/support/mod.rs`](tests/support/mod.rs)）を立てて、
NDJSON・行長上限・1 接続あたりの要求数・購読イベントを仕様どおりに再現しています。

主なテスト:

| テスト | 何を保証するか |
| --- | --- |
| `protocol_conformance::*` | 5 アクション、`message_too_large` 後の切断、不正 JSON/未知 action、購読イベント、接続ローテート、64 KiB のローカル拒否 |
| `daemon_cycle::a_cycle_sends_findings_and_stores_the_answer` | 1 サイクルが送信し、イベントと応答本文を残す |
| `daemon_cycle::a_full_queue_is_retried_with_a_new_request_id` | `queue_full` のバックオフと attempt の前進 |
| `daemon_cycle::an_awaiting_approval_rejection_waits_and_then_retries` | 承認待ちを迂回しない |
| `daemon_cycle::thread_unavailable_is_retried_with_the_same_request_id` | 未記録拒否は同一 id で再試行 |
| `daemon_cycle::a_delivered_message_is_not_sent_again_after_a_restart` | 再起動しても二重配送しない |
| `daemon_cycle::a_lost_watch_is_rebuilt_and_the_state_reread` | 切断 → 再接続 → 状態読み直し |
| `daemon_cycle::a_watch_that_keeps_dropping_ends_the_cycle` | 再接続の上限（無限ループしない） |
| `daemon_cycle::a_turn_without_a_generating_event_still_finishes` | `generating` を観測できなくても完了できる |

## 12. ライセンス

MIT
