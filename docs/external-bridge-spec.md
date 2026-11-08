# Zed External Thread Message Bridge — 外部デーモン向け仕様書

最終更新: 2026-11-08 / 実装: このリポジトリの `main`（コミット `d750faca3c` 時点）

対象読者: この IPC を使って Zed の既存 Agent Thread にメッセージを送り、
エージェントの応答を受け取りたい外部プロセス（監査デーモン等）の実装者。

---

## 0. この文書の位置づけ

- 仕様の正は実装（`crates/agent_bridge_protocol`、`crates/agent_bridge`、`crates/zed_bridge`）。
  本文書は実装から書き起こした外部向けの要約。
- 単体・統合テストは通っている（protocol 12 / bridge 8 / CLI 6）。
  ただし **実スレッドへの配送と受信の E2E は未検証**（§11）。デーモン側で最初に
  疎通確認すること。

---

## 1. できること / できないこと

できること:

- ローカルの Unix domain socket 経由で、Zed が管理する Agent Thread の一覧取得・状態照会。
- 既存スレッド（非表示・別ワークスペースのものを含む）へのユーザーメッセージ配送。
- エージェント実行中はキューに積み、Idle 復帰後に配送（**実行中のターンは中断しない**）。
- スレッドの変化（新しいエントリ、エージェントの応答本文、状態遷移、ツール承認要求）を
  購読して受け取る（`watch_thread`）。

できないこと（重要）:

- `notify` モード（エージェントを走らせず履歴に表示だけする）は未実装。
  `mode: "notify"` は `rejected` / `notify_disabled` を返す。
- 承認（ツール実行の許可）を代行することはできない。承認は常に Zed の UI が持つ。
  デーモンは `tool_authorization_requested` を観測するだけ。
- 過去イベントの再生（カーソル）。`watch_thread` は接続中に起きた変化だけを流す。
- 配送台帳と重複排除はメモリ保持。Zed 再起動で消える。
- Linux / Windows。macOS のみ（peer uid 検証が `LOCAL_PEERCRED`）。

---

## 2. 前提と有効化

1. Zed の設定でブリッジを有効にする（**既定は無効**）。

   ```json [settings]
   {
     "agent_external_bridge": {
       "enabled": true,
       "transport": "unix_socket",
       "allow_prompt": true,
       "allow_notify": false
     }
   }
   ```

   - `enabled` が false の間は socket を作らず、既存の socket ファイルにも触らない。
   - 設定は実行中でも反映される（true→false で listen 停止と socket 削除、
     false→true で listen 開始）。
   - `transport` は `unix_socket` のみ。未知の値は既定にフォールバックする。

2. socket のパスは Zed のデータディレクトリ配下、リリースチャンネルごとに分かれる。

   ```
   ~/Library/Application Support/Zed/agent-bridge-<channel>.sock
   ```

   `<channel>` は `stable` / `preview` / `nightly` / `dev`。
   このフォークをローカルビルドした場合は `dev`（`ZED_BRIDGE_CHANNEL=dev` を CLI に渡す）。

3. 権限と配置。

   - socket は bind 直後に `0600`。同一ユーザーのプロセスだけが接続できる。
   - accept 後に `LOCAL_PEERCRED` で `cr_uid` を照合し、異なる uid は 1 バイトも読まずに切断。
   - TCP は開かない。外部ネットワークには公開されない。
   - 正常終了時に socket を削除。異常終了で残った場合は次回起動時に置き換える
     （生きたソケットが既にある場合は起動エラーとしてログに残し、奪わない）。

---

## 3. トランスポートとフレーミング

- Unix domain **stream** socket。
- メッセージ境界は改行（`\n`）区切りの JSON（NDJSON）。1 行 1 メッセージ。
- 1 行の上限は **64 KiB**（`MAX_LINE_BYTES`）。超過すると
  `rejected` / `message_too_large` を返して**その接続を閉じる**。
- 1 接続で処理する要求は最大 **100**（`MAX_REQUESTS_PER_CONNECTION`）。
  超えるとサーバー側が接続を閉じる。購読用の接続は 1 要求で足りるので実質無制限。

---

## 4. リクエスト共通形

```json
{
  "version": 1,
  "request_id": "audit-001",
  "action": "send_message",
  "thread_id": "existing-thread-id",
  "mode": "prompt",
  "delivery": "after_current_turn",
  "message": "【独立監査】仕様との不一致が見つかりました。確認してください。"
}
```

| フィールド | 必須 | 内容 |
| --- | --- | --- |
| `version` | 必須 | `1` 固定。違う値は `unsupported_version`。 |
| `request_id` | 必須 | 呼び出し側が決める一意な ID。重複送信の防止に使う（§10）。 |
| `action` | 必須 | `list_threads` / `get_thread_status` / `send_message` / `get_delivery_status` / `watch_thread`。未知は `unknown_action`。 |
| `thread_id` | 対象指定 | Zed の `ThreadId`（UUID 文字列）。`session_id` と排他。 |
| `session_id` | 対象指定 | ACP のセッション ID。`thread_id` と排他。 |
| `mode` | 任意 | `prompt`（既定）/ `notify`。 |
| `delivery` | 任意 | `after_current_turn`（既定・唯一の値）。 |
| `message` | `send_message` で必須 | 空文字・空白のみは `invalid_request`。 |

未知のフィールドは無視される（前方互換）。ただし未知の `action` / `mode` /
`delivery` / `version` はエラーにする（既定動作に勝手に落とさない）。

---

## 5. アクション

応答はすべて 1 行の JSON。共通フィールドは `version` / `request_id` /
`thread_id` / `status`、必要に応じて `message_id` / `error` / `threads` /
`thread` / `delivery` / `event`。

### 5.1 `list_threads`

```json
{ "version": 1, "request_id": "r1", "action": "list_threads" }
```

```json
{
  "version": 1,
  "request_id": "r1",
  "status": "accepted",
  "threads": [
    {
      "thread_id": "6f1c2d3e-4b5a-4c7d-8e9f-0a1b2c3d4e5f",
      "session_id": "acp-session-id",
      "title": "Refactor the parser",
      "agent_id": "zed-agent",
      "archived": false,
      "state": "idle",
      "worktree_paths": ["/Users/me/project"],
      "updated_at": null
    }
  ]
}
```

- 並びは `updated_at` の新しい順。
- `state` は `draft` / `idle` / `generating` / `awaiting_approval` / `unconfirmed` / `unknown`。
  `draft` はセッション未作成（＝配送不可）。
- `updated_at` は現在 `null` 固定。

### 5.2 `get_thread_status`

```json
{ "version": 1, "request_id": "r2", "action": "get_thread_status", "thread_id": "…" }
```

```json
{
  "version": 1,
  "request_id": "r2",
  "thread_id": "…",
  "status": "accepted",
  "thread": {
    "thread_id": "…",
    "found": true,
    "archived": false,
    "state": "generating",
    "deliverable": false,
    "loaded": true
  }
}
```

- 存在しない場合は `status: "thread_not_found"` と `thread.found: false`。
- `deliverable` は「いま `send_message` を受け付けるか」。実行中でもキューに積めるので、
  `generating` でも受け付ける（`deliverable` は archived / draft のときに false）。
- `loaded` は Zed がメモリに保持しているか。

### 5.3 `send_message`

```json
{
  "version": 1,
  "request_id": "audit-001",
  "action": "send_message",
  "thread_id": "…",
  "mode": "prompt",
  "message": "監査結果を確認してください"
}
```

応答は 2 通り。

待機中（配送済み）:

```json
{
  "version": 1,
  "request_id": "audit-001",
  "thread_id": "…",
  "status": "delivered",
  "message_id": "12"
}
```

生成中（キュー登録）:

```json
{
  "version": 1,
  "request_id": "audit-001",
  "thread_id": "…",
  "status": "queued"
}
```

- `delivered` は「スレッド履歴に積み、ターン開始を要求した」の意味。
  **エージェントの応答完了ではない。** 完了や本文は `watch_thread` で受け取る。
- `queued` はブリッジ側の保留キュー（スレッドあたり最大 16 件）。Idle 復帰時に
  自動で配送され、台帳の状態が `delivered` に変わる。溢れると `queue_full`。
- `message_id` は Zed が発行する submission ID（数値の文字列）。
- 生成中のスレッドへ送っても現在のターンは中断しない。
- 承認待ち（`state: awaiting_approval`）のスレッドへは送らず
  `rejected` / `awaiting_approval` を返す（承認の迂回防止）。
- アーカイブ済みは `rejected` / `thread_archived`。セッション未作成は
  `unsupported_thread_type`。台帳にある `request_id` の再利用は `duplicate_request`。

### 5.4 `get_delivery_status`

```json
{ "version": 1, "request_id": "r3", "action": "get_delivery_status" }
```

```json
{
  "version": 1,
  "request_id": "r3",
  "status": "accepted",
  "delivery": {
    "request_id": "audit-001",
    "thread_id": "…",
    "status": "delivered",
    "message_id": "12"
  }
}
```

- 台帳は直近 1024 件をメモリ保持。Zed 再起動後は
  `rejected` / `unknown_request`。

### 5.5 `watch_thread`（受信ストリーム）

```json
{ "version": 1, "request_id": "watch-1", "action": "watch_thread", "thread_id": "…" }
```

最初の応答:

```json
{
  "version": 1,
  "request_id": "watch-1",
  "thread_id": "…",
  "status": "accepted",
  "thread": { "thread_id": "…", "found": true, "archived": false, "state": "idle", "deliverable": true, "loaded": true }
}
```

以後、同じ接続にイベント行が流れ続ける:

```json
{
  "version": 1,
  "request_id": "watch-1",
  "thread_id": "…",
  "status": "event",
  "event": {
    "watch_id": "watch-1",
    "thread_id": "…",
    "kind": "entry_added",
    "entry": { "index": 7, "kind": "assistant_message", "text": "## Assistant\n\n確認しました。\n" }
  }
}
```

イベント種別:

| `kind` | 追加フィールド | 意味 |
| --- | --- | --- |
| `entry_added` | `entry` | 新しいエントリ（ユーザー発話・エージェント応答など）。 |
| `entry_updated` | `entry` | 既存エントリの更新（応答の追記など）。`entry.index` で同一性を判断。 |
| `entry_removed` | `detail`（`"start..end"`） | エントリ削除。 |
| `status_changed` | `state` | 実行状態の変化（`idle` / `generating` / `awaiting_approval` …）。 |
| `title_updated` | `detail` | スレッドタイトル。 |
| `tool_authorization_requested` | `detail` | ツール実行の承認要求。承認は Zed UI が行う。 |
| `tool_authorization_received` | `detail` | 上記の判断が下った。 |
| `turn_stopped` | `detail` | ターン終了（停止理由が分かる場合のみ）。 |

`entry`:

```json
{ "index": 7, "kind": "assistant_message", "text": "## Assistant\n\n…" }
```

- `kind` は `user_message` / `assistant_message` / `session_message` /
  `tool_call` / `elicitation` / `context_compaction`。
- `text` はユーザー／アシスタント／セッションメッセージのみ Markdown で入る。
  ツール呼び出し等は `kind` と `index` だけ（本文は返さない）。
- 購読は接続と結び付く。接続が切れると解除される。同時購読は全体で 8 本まで
  （超えると `rejected` / `queue_full`）。
- 未ロードのスレッドを指定した場合も復元を試みる（`send_message` と同じ制約）。
- 再接続時に過去イベントは再生されない。本文を後から取り直す API（例:
  `get_thread_entries`）は現時点で無いので、必要な `entry.text` はデーモン側で
  保存しておくこと。溜めない場合は接続を切らない運用にする。

---

## 6. ステータス

| `status` | 意味 |
| --- | --- |
| `accepted` | 要求を受理（照会系・購読の応答）。 |
| `queued` | スレッドの保留キューに登録。Idle 復帰後に配送。 |
| `delivered` | `AcpThread::send` を実行し履歴に積んだ（応答完了ではない）。 |
| `event` | `watch_thread` のストリーム行。 |
| `thread_not_found` | 対象スレッドが存在しない。 |
| `thread_unavailable` | 存在するが配送できない（開いているワークスペースが無い等）。 |
| `unsupported_thread_type` | セッション未作成（draft）など、配送対象外。 |
| `rejected` | 明示的な拒否。`error.code` を見る。 |
| `error` | 内部エラー。 |

受理・キュー登録・履歴反映・ターン完了は別の状態として扱う。

---

## 7. エラーコード（`error.code`）

| code | 発生条件 | デーモンの推奨動作 |
| --- | --- | --- |
| `invalid_json` | JSON として壊れている / UTF-8 でない | バグ。ログに残して停止。 |
| `unsupported_version` | `version` が 1 以外 | 実装を見直す。 |
| `unknown_action` | 未知の `action` | 同上。 |
| `invalid_request` | 必須欠落、`thread_id` と `session_id` の同時指定、空メッセージ等 | 同上。 |
| `message_too_large` | 1 行が 64 KiB 超 | メッセージを分割。**接続は閉じられる。** |
| `unknown_request` | `get_delivery_status` で未知の `request_id` | 送信履歴が無い/再起動後。再送を検討。 |
| `duplicate_request` | 同一 `request_id` の再送 | 送信済み。結果は `get_delivery_status` で確認。 |
| `queue_full` | 保留キュー（16 件）または購読（8 本）が満杯 | 時間を置いて再試行。 |
| `thread_archived` | アーカイブ済みスレッド | スレッドを選び直す。 |
| `thread_unavailable` | 復元できるワークスペースが無い | ユーザーに Zed でそのプロジェクトを開いてもらう。 |
| `awaiting_approval` | 承認待ち | 承認は UI。待ってから再送。 |
| `notify_disabled` | `mode: notify` | `prompt` を使う。 |
| `prompt_disabled` | 設定で `allow_prompt: false` | 設定を確認。 |
| `bridge_disabled` | 設定で `enabled: false` | 設定を確認。 |
| `internal` | 想定外 | ログを添えて報告。 |

---

## 8. 典型的なシーケンス

監査デーモンの 1 サイクル:

```
1. list_threads                       → 対象スレッドを決める（thread_id）
2. watch_thread --thread <T>          → 同じ接続で購読開始（accepted）
3. get_thread_status --thread <T>     → state を確認（idle / generating / awaiting_approval）
4. send_message --thread <T> --mode prompt --message "…"
                                       → delivered（idle だった） / queued（生成中だった）
5. イベントを読む:
     status_changed -> generating
     entry_added     (agent_message_chunk 相当が entry 単位で流れる)
     status_changed -> idle
     turn_stopped
6. 結果本文は entry_added / entry_updated の entry.text から取得
```

注意: 2 と 3/4 は別接続でよいが、購読用の接続は開いたままにする
（1 接続 100 要求の上限があるため、購読専用の接続を推奨）。

---

## 9. 参照実装（CLI）

`zed-bridge` が最小のクライアント実装。デーモンの実装前に挙動確認に使える。

```sh
# 一覧（既定は stable の socket）
zed-bridge list-threads

# dev ビルドの Zed に繋ぐ
ZED_BRIDGE_CHANNEL=dev zed-bridge list-threads

# 状態照会
zed-bridge thread-status 6f1c2d3e-4b5a-4c7d-8e9f-0a1b2c3d4e5f

# 配送（--request-id 省略時は自動生成）
zed-bridge send --thread <T> --mode prompt --message "監査結果を確認してください"

# 配送状態（Zed 再起動後は unknown_request）
zed-bridge delivery-status audit-001

# 受信（Ctrl-C まで購読）
zed-bridge watch --thread <T>

# 生 JSON を見る
zed-bridge --json --socket ~/Library/Application\ Support/Zed/agent-bridge-dev.sock list-threads
```

終了コード: `send` が `delivered` / `queued` 以外なら 1。`thread-status` の
`thread_not_found` は 0（照会結果であって失敗ではない）。

---

## 10. デーモン実装上の推奨

- **`request_id` は永続化して一意に**。クラッシュ再送時の二重配送は
  `duplicate_request` で防げるが、台帳はメモリ保持なので Zed 再起動後は
  区別できない。送信内容と `request_id` をデーモン側に記録しておく。
- **1 目的 1 接続**。購読用の接続は開きっぱなしにし、照会・送信用は都度つなぐ。
- **`watch_thread` と `get_delivery_status` の役割を分ける**。前者は変化の通知、
  後者は送信結果の確認。どちらも「エージェントの応答内容」はイベント側にしかない。
- **再接続時は状態を取り直す**。購読は過去を再生しないので、`list_threads` /
  `get_thread_status` で現在値を読み、必要なら `entry.text` を保存しておく。
- **`generating` 中の連投を避ける**。ブリッジは積んでくれるが、キューは 16 件で
  溢れると `queue_full`。溢れたら指数バックオフで再試行。
- **承認待ちは待つ**。`awaiting_approval` / `tool_authorization_requested` は
  ユーザーの操作待ち。迂回する方法は無い（意図的に用意していない）。
- **ソケットパスの解決**はチャンネル依存。監視対象の Zed が dev ビルドなら
  `agent-bridge-dev.sock`。存在しなければ「ブリッジ無効」か「Zed 未起動」。

---

## 11. 制約・未検証事項

未検証（デーモン実装前に確認すること）:

1. **実スレッドへの配送と次ターン開始**。テスト環境にスレッドが無く、GUI 操作も
   自動化できなかったため未確認。実機では次で確認できる:
   Zed を `enabled: true` で起動 → Agent Panel でスレッド A/B を作成 →
   `zed-bridge send --thread <B>` → B にだけユーザーメッセージが増える →
   `thread-status` が `generating` → `idle` に戻る。
2. 生成中のキューイング、承認待ち拒否の実機挙動（分岐と単体テストのみ）。
3. `watch_thread` のイベントを実スレッドで受けること（実装・単体テストのみ）。

既知の制約:

- ブリッジ側の保留キューはメモリ保持で、Agent Panel のキュー UI には出ない。
- 復元時は Agent Panel の表示が対象スレッドへ切り替わる。
- 強制終了時は socket ファイルが残る（次回起動で置換）。
- 台帳・重複排除・購読状態は再起動で失われる。

---

## 12. 参照

- プロトコル仕様（リポジトリ内の詳細版）: `docs/plan/20261107-ipc-protocol.md`
- 実装計画と調査メモ: `docs/plan/20261107-implementation-plan.md`
- 実装:
  - `crates/agent_bridge_protocol/src/agent_bridge_protocol.rs`（wire 型・検証）
  - `crates/agent_bridge/src/agent_bridge.rs`（受付・配送・購読）
  - `crates/agent_bridge/src/delivery.rs`（スレッド解決・状態分類）
  - `crates/agent_bridge/src/server.rs`（socket・権限・行長上限）
  - `crates/zed_bridge/src/main.rs`（CLI）
