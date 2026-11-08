# 監査デーモン（zed-audit-loop）実装計画

作成: 2026-11-08 / 対象: 外部スレッドメッセージブリッジ（IPC）のクライアント側デーモン

## 0. 入力（仕様の正）

| 種別 | 場所 | 用途 |
| --- | --- | --- |
| 外部向け仕様書 | `docs/external-bridge-spec.md`（原本 `/Users/bookair18/Desktop/zed-agent-bridge-spec.md`） | §3〜§7, §10, §11 が実装基準 |
| 詳細プロトコル | Zed 側 `d750faca3c:docs/plan/20261107-ipc-protocol.md`（読み取りのみ） | ステータス／エラーコードの全量 |
| 実装計画 | Zed 側 `d750faca3c:docs/plan/20261107-implementation-plan.md`（読み取りのみ） | サーバー側の意図確認 |
| 参照クライアント | Zed 側 `crates/zed_bridge/src/main.rs` | 接続・行長・購読の最小実装 |
| wire 型 | Zed 側 `crates/agent_bridge_protocol/src/agent_bridge_protocol.rs` | JSON 形状（`Response` は全フィールド任意） |
| サーバー挙動 | Zed 側 `crates/agent_bridge/src/server.rs`, `src/agent_bridge.rs` | 行長超過時の応答、100 要求上限、`queue_full` が台帳に記録される点 |

調査に使った手段: `terminal` の `git show`（Zed 側リポジトリ）＋ 本ワークツリーの探索。
`serena` / `context7` / `codebase-memory-mcp` / `graphify` は本環境では未接続（MCP サーバーなし）のため、
同等の情報を `git show` と実ファイル読解で取得した。新規リポジトリのため既存実装の再利用可否の探索対象は無い。

## 1. 仕様から読み取った実装上の制約（重要）

1. **行長 64 KiB**。超過時サーバーは `rejected` + `error.code = message_too_large` を返し、
   その直後に接続を閉じる。このとき `request_id` は空文字（`Response::rejected(None, ..)`）。
   クライアントは「空の request_id の拒否」を許容しなければならない。
2. **1 接続 100 要求**。超過でサーバーが接続を閉じる（応答は返らない）。長時間運用では
   照会用接続を使い回すなら 100 要求でローテートが必要。
3. **購読専用接続**。`watch_thread` は 1 要求で足りるので専用接続を保持する。
   接続断で購読解除。過去イベントの再生は無い。
4. **`queue_full` は台帳に記録される**（`reject_facts` → `record`）。したがって同じ
   `request_id` で再送すると `duplicate_request` になる。**再試行は新しい `request_id`** を
   使わなければならない（`duplicate_request` を成功と誤認しないため）。同様に
   `awaiting_approval` / `thread_archived` も台帳に記録される。
5. **台帳はメモリ保持**。Zed 再起動で `get_delivery_status` は `unknown_request` になる。
   二重配送防止はデーモン側の台帳が唯一の根拠。
6. `get_thread_status` で **セッションはあるが未ロード** のスレッドは `state: unknown` を返す
   （`send_message` は復元を試み、失敗すると `thread_unavailable`）。デーモンは `unknown` を
   「送信不可・様子見」として扱う。
7. `deliverable` は archived / draft のとき false。`generating` でも送信自体は受け付ける
   （キュー登録）ので、送信可否の判断に `deliverable` だけを使わない。
8. socket が無い / 接続拒否は「ブリッジ無効」か「Zed 未起動」。クライアントはこの 2 つを
   区別できないので、原因候補を添えた単一のエラー種別にまとめる。

## 2. 技術選定

- **Rust**（第一候補どおり）。理由: wire 型・参照実装が Rust、常駐デーモン、本マシンでビルド可能。
- 依存: `tokio`（net/io-util/time/macros/rt-multi-thread/signal）, `serde`, `serde_json`,
  `clap`（derive）, `anyhow`。dev 依存に `tempfile`。
  - `tracing`/`chrono` は入れない（依存最小化）。構造化ログは自前の 1 行 JSON、
    タイムスタンプは自前の RFC3339 変換（単体テストで担保）。
  - 設定は **TOML**（人間が書く前提。`toml` クレートを 1 つ追加する判断は「設定例が
    読みやすい」ため）。→ 実装時に TOML が重すぎる場合は JSON にフォールバックする。
- 構成は単一クレート（lib + bin）。`mod.rs` は作らず `src/protocol.rs` + `src/protocol/*.rs` 形式。

### クレート構成（予定）

```
src/
  lib.rs            # モジュール公開
  main.rs           # CLI: run / list-threads / thread-status
  config.rs         # TOML 設定・既定値・パス解決（channel → socket パス）
  time.rs           # unix 秒 → RFC3339（ログ用）
  logging.rs        # 1 行 1 JSON のログ（stderr + ファイル）
  ledger.rs         # request_id 台帳（JSONL 追記・状態遷移・重複抑止）
  store.rs          # イベント/応答本文の JSONL 保存
  finding.rs        # FindingSource 抽象 + file / command 実装
  daemon.rs         # 監査 1 サイクルのループ・バックオフ・再接続
  protocol.rs       # wire 型・定数・エラー分類（再エクスポート）
  protocol/types.rs # Request/Response/Status/ErrorCode/Event...
  protocol/client.rs# 接続・1 要求 1 応答・購読ストリーム
  protocol/error.rs # クライアント側エラー型と「推奨動作」対応
```

## 3. 出力（このリポジトリが提供するもの）

- `zed-audit-loop run --config <path>`: 監査サイクルを回すデーモン。
- `zed-audit-loop list-threads` / `thread-status <id>`: §9 の事前疎通確認に使う最小 CLI。
- `FindingSource`: 「何を監査するか」の境界。LLM 監査は実装しない。

## 4. BDD タスク一覧（振る舞い単位）

各タスクは「入力 → 期待する振る舞い」で書く。受け入れは fake bridge server で検証する。

### T1. socket パス解決（`config`）

- `channel = "stable"`（既定）→ `~/Library/Application Support/Zed/agent-bridge-stable.sock`
- `channel = "dev"` → `...-dev.sock`
- 明示 `socket` 指定 → そのパスを優先（`~` 展開あり）
- 未知 channel 文字列 → パスにそのまま埋める（サーバー側と同じ規則）／空文字 → `stable` に正規化
- `HOME` 未設定 → エラー（`--socket` 明示を促す）

### T2. 要求のエンコード（`protocol`）

- `list_threads` 要求 → `{"version":1,"request_id":..,"action":"list_threads","mode":"prompt","delivery":"after_current_turn"}`
- `send_message` 要求 → `thread_id` と `message` を含み、`session_id` を含まない
- `thread_id` と `session_id` は排他（同時指定は構築時にエラー）
- 64 KiB 超の行 → 送信前に `LineTooLarge` で失敗（サーバーを待たない）

### T3. 1 要求 1 応答（`protocol::client`）

- 正常応答 1 行 → 型付き `Response` に復号
- 応答が来る前に EOF → `UnexpectedEof`（再試行可）
- socket 不在 / 接続拒否 → `NotConnected`（「ブリッジ無効 or Zed 未起動」の候補を detail に）
- 応答が壊れた JSON → `MalformedResponse`（生の行を detail に）
- 応答待ちが `request_timeout` を超える → `Timeout`（再試行可）
- 1 接続で 100 要求を超える → 新しい接続に自動で切り替える（fake server は上限超で切断）

### T4. ステータス／エラーの型付け（`protocol`）

- `status` 9 種を enum で表現（`accepted`/`queued`/`delivered`/`event`/`thread_not_found`/
  `thread_unavailable`/`unsupported_thread_type`/`rejected`/`error`）
- `error.code` 15 種を enum で表現（§7 の表と同じ並び）
- 各コードに「デーモンの推奨動作」を持たせる（`Retryable` / `Fatal` / `Wait` / `Reselect` /
  `Stop`）。§7 の表をそのまま写像する。
- `rejected` でも `error` が無い応答 → `error` 欠落をエラーにせず `None` として扱う（サーバー実装差に耐える）

### T5. 購読（`protocol::client::Subscription`）

- `watch_thread` → 最初の応答 `accepted`（`thread` 付き）を返し、以後 `event` 行をストリーム
- `entry_added` / `entry_updated` → `entry.index` / `entry.kind` / `entry.text` を保持
- `status_changed` → `state` を持つ
- `turn_stopped` → `detail` を持つ
- 未知 `kind` → エラーにせず無視（前方互換。ログに残す）
- イベント行の JSON が壊れている → `MalformedResponse`（購読は継続）
- 接続が閉じる → `next_event` が `None`（デーモンは再接続して現在状態を読み直す）
- `watch_thread` が `rejected` / `thread_not_found` → 購読を開始せず型付き理由を返す

### T6. request_id 台帳（`ledger`）

- `request_id = audit-<thread 先頭 8>-<content_hash 先頭 12>-<attempt>`（attempt は 0 始まり）
  - 同じ（thread, メッセージ内容）→ 同じ attempt の id（再現可能）
  - **attempt を進めるのは「ブリッジ側台帳に記録される拒否」のときだけ**（`queue_full` /
    `awaiting_approval` / `thread_archived`）。これらは再送すると `duplicate_request` になるため
  - `thread_unavailable` / `thread_not_found` / `unsupported_thread_type` / 設定系拒否 /
    パース段階拒否 / 転送エラーは**未記録**なので同一 request_id で再試行する
- 送信前に `sending` として JSONL に追記（write-ahead）、応答で `delivered`/`queued`/`rejected` に更新
- 起動時に JSONL を読み、`(thread, content_hash, attempt)` → 最終状態を復元
- `delivered` / `queued` の id は再送しない（プロセス再起動後も）
- `sending` のまま残った id（クラッシュ窓）は再送せず `get_delivery_status` で照合し、
  `unknown_request` なら `unknown` に落として警告ログ（at-most-once。二重配送を選ばない）
- `duplicate_request` 応答 → 「送信済み」として成功扱い
- 壊れた JSONL 行 → その行をスキップし警告（起動を止めない）

### T7. `entry.text` 保存（`store`）

- `events/<thread_id>.jsonl` に 1 イベント 1 行で追記（時刻・kind・index・entry_kind・text・state・detail）
- ターン完了（`turn_stopped` または `status_changed:idle`）で `turns/<thread_id>.jsonl` に
  そのターンの assistant 本文を連結して 1 行追記（`request_id` 付き）
- 追記は `open(append) + sync_data`（イベント取りこぼしを最小化）
- ディレクトリは起動時に作成。書けない場合は致命的（起動失敗）

### T8. 監査サイクル（`daemon`）

- **正常系**: 対象決定（明示 thread_id）→ `watch_thread` → `get_thread_status`（idle）→
  finding を `send_message`（`delivered`）→ イベント購読 → `turn_stopped` で応答本文を保存 → interval 待ち- **finding ゼロ件**: 送信せず interval 待ち（ログに `cycle_skipped`）
- **queue_full**: 指数バックオフ（initial ×2^n, 上限あり）で同一内容を **attempt+1** で再送
- **awaiting_approval**: `get_thread_status` が `awaiting_approval` の間は送らない。
  戻るまで待機（ポーリング）。イベント `tool_authorization_requested` は記録のみ
- **thread_archived**: 送らずにログ（`Reselect`）。次サイクルで対象を選び直す
- **unsupported_thread_type**: 送らずにログ。対象を選び直す
- **thread_not_found / state=unknown**: `thread_not_found` は送らずにログ（対象再選）。
  `state=unknown`（未ロード）は**送信を試みる**（ブリッジが復元する）。復元失敗は
  `thread_unavailable` として扱う
- **thread_unavailable**: 送らずにログ。バックオフして次サイクルで再解決
- **購読断**: 再接続して `get_thread_status` を読み直し、サイクルを続行
- **応答タイムアウト**: `await_response_timeout` を超えたら諦めてログ（`turn_timeout`）。
  購読は維持し、次サイクルへ
- **SIGINT / SIGTERM**: サイクルの切れ目で終了、台帳を flush
- 対象の決め方: `target.thread_id` 明示、または `target.select = "latest"`（`list_threads` の
  先頭＝最新、archived 除外）

### T9. FindingSource（監査内容の供給）

- `FindingSource::next_batch() -> Result<Vec<Finding>>`。`Finding { id, severity, summary, detail, path }`
- `file`: JSONL を読み、消費位置（カーソルファイル）を永続化。既読は再送しない。
  末尾の不完全行は次回に回す
- `command`: コマンドを 1 回実行し、stdout の JSONL を読む。終了コード非 0 はそのサイクルをスキップ
- どちらも 0 件なら空ベクタ（送信しない）
- メッセージ整形は 1 関数（`render_message`）に集約。LLM・Issue 管理は実装しない

### T10. CLI

- `run --config <path>`: デーモン起動。設定不備は起動前にエラー終了
- `list-threads`: `list_threads` を 1 回叩いて表を出す（§9 の事前確認）
- `thread-status <id>`: `get_thread_status` を 1 回叩く
- 接続不能時の終了コードは非 0、`thread_not_found` は 0（照会結果）

### T11. fake bridge server（テスト基盤）

- `tests/support/mod.rs` に Unix socket を立てるサーバー。NDJSON を読み、
  スクリプト化した応答（固定応答・状態機械）を返す
- 受信した要求を記録してテストから検証できる
- 行長超過 → `rejected`/`message_too_large` を返して切断
- 100 要求で切断（テスト用に上限を下げられる）

### T12. 適合テスト（受け入れ条件）

1. 5 アクションの正常系（`accepted` / `delivered` / `queued` / `event`）
2. `message_too_large` 後の切断（拒否理由の型付け + `UnexpectedEof` にならないこと）
3. `queue_full` → バックオフ再送（attempt が進む）
4. `awaiting_approval` → 送らない（待機）
5. `thread_not_found`
6. 不正 JSON / 未知 action の応答
7. 購読中のイベント受信（4 種）
8. 切断 → 再接続（現在状態の読み直し）
9. `request_id` 永続化による再送抑止（プロセス再起動相当）
10. 設定・パス解決・台帳の単体テスト

## 5. Plan Architect レビュー（自己レビュー）と計画修正

コードベースは空なので、「既存規約との突き合わせ」ではなく
**仕様（§3〜§7, §10, §11）とサーバー実装の乖離リスク**をレビュー軸にした。
下表の A〜F を検出し、計画に反映済み。

| # | 指摘 | 対応 |
| --- | --- | --- |
| A | 1 行 64 KiB を超える監査結果を 1 通で送ると `message_too_large` で切断される。§7 は「分割」を推奨 | `render_batches(findings, max_message_bytes)` で送信前に分割し、各バッチを別メッセージ（別 content hash）として順に送る。切り捨てない |
| B | finding のカーソルを送信前に進めると、送信失敗で検知結果が消える | 2 相コミット: `read_batch()` が返すカーソルは**送信結果を台帳に記録した後**に `commit()` する。クラッシュしても同じ content hash → 同じ attempt 0 の request_id になり台帳で抑止される |
| C | `turn_stopped` は「停止理由が分かる場合のみ」なので必ずしも来ない | 完了判定を「① `turn_stopped` ② 送信後に見た `generating` から `idle` への遷移」の OR にし、どちらも来なければ `await_response_timeout` で打ち切ってログ |
| D | 購読を毎サイクル張り直すとイベントの空白ができる | 対象スレッドが変わらない間は購読接続を保持。対象変更・切断時のみ張り直す |
| E | `get_thread_status` の `state=unknown`（未ロード）を「送信不可」と誤判定する危険 | `unknown` は送信を試みる（ブリッジが復元を試みる）。送信を止めるのは `awaiting_approval` のみ |
| F | 構造化ログが `request_id` で追えないと障害調査ができない | 全レコードに `cycle_id` / `thread_id` / `request_id` を可能な限り付け、1 イベント 1 行 JSON でファイルにも出す |
| G | （親レビュー）**台帳に記録される拒否とされない拒否がある**。`reject_facts` 経由（`awaiting_approval` / `thread_archived` / `queue_full`）と成功（`delivered` / `queued`）は記録され、同一 request_id の再送は `duplicate_request` になる。一方 `notify_disabled` / `prompt_disabled` / `bridge_disabled` / パース段階の拒否 / `thread_not_found` / `unsupported_thread_type` / `thread_unavailable` は記録されない | attempt サフィックスは**記録される拒否のときだけ**進める。それ以外（`thread_unavailable` でプロジェクトが開かれるのを待つ等）は同一 request_id で再試行する |
| H | （親レビュー）`bridge_disabled` は `request_id` すら返らない（`Response::rejected(None, ..)`） | `message_too_large` と同じく `request_id` 空文字を許容してパースする |
| I | （親レビュー）`get_delivery_status` の台帳には `rejected` レコード（`error` 付き）も載る | reconcile は `delivery.status` と `delivery.error` の両方を読み、`unknown_request` だけを「記録なし」と判定する |

追加で確認した点:

- `queue_full` / `awaiting_approval` / `thread_archived` はサーバー側で台帳に記録されるため、
  **同一 request_id での再試行は `duplicate_request` になり成功と誤認する**。
  → request_id は attempt ごとに変える（T6）。これが今回の設計の要。
- `message_too_large` の拒否応答は `request_id` が空文字で返る。パースは
  「`request_id` 空文字を受け入れる」ようにする。
- fake server は「サーバーの実際の応答順（拒否 → 即切断）」を再現する必要がある。

## 6. 未検証事項（この環境では不可）

- 実 Zed の実スレッドへの配送・応答受信の E2E（ブリッジを有効化した Zed と実スレッドが必要）
- `LOCAL_PEERCRED` による uid 検証（サーバー側機能。クライアントからは観測不能）
- 実環境での `queue_full` 発生（16 件キューを実機で埋める必要がある）

README の「未検証事項」に明記し、成功扱いにしない。

## 7. 実装順（Phase 2〜4 の対応）

- Phase 2: T2, T3, T4, T5, T11, および T12-1/2/5/6/7 のテスト（クライアント + fake で疎通）
- Phase 3: T6, T7, T8, T9, T10, T1（設定）+ T12-3/4/8/9
- Phase 4: README、要件対応表、`cargo fmt --check` / `cargo clippy -- -D warnings` / `cargo test`

## 8. 実装中に決めたこと（計画からの差分）

計画を進める中で必要になった判断。仕様からの逸脱は無し。

| # | 決定 | 理由 |
| --- | --- | --- |
| 1 | 分割したバッチは 1 サイクルで順に全部送る（各バッチごとにターン完了を待つ） | 一部だけ送って残りを次サイクルに回すと、 finding の取りこぼしと順序の入れ替わりが起きる |
| 2 | ターン本文にはエージェントの発話（`assistant_message` / `session_message`）だけを入れる | `user_message` は自分が送った文のエコーで、応答ではない。生イベントは別ファイルに全部残る |
| 3 | 使い回し接続が死んでいた場合、**接続/書き込み段階の失敗に限り** 1 回だけ新しい接続で再試行する | その段階の失敗は「ブリッジが受け取っていない」ことが確実なので安全。読み取り段階は再試行しない |
| 4 | 購読の再接続は 5 回で打ち切る（`MAX_WATCH_RECONNECTS`） | 落ち続ける購読で応答待ちタイムアウトまで接続を繰り返さない |
| 5 | `idle` 遷移 + （`generating` を観測 or エージェント本文を受信）でターン完了とみなす | 短いターンでは `generating` が観測できないことがある |
| 6 | findings カーソルの前進は `Sent` / `Idle`（送信済み）のときだけ | `Failed` / `Deferred` で消費すると、未送信の指摘が消える |
| 7 | clippy は `all = deny` + `pedantic = allow` | pedantic の整数キャスト系は、範囲が自明な時刻・バイト数計算にノイズしか出さない |
| 8 | 送信内容（全文）を台帳に保存する | 仕様書 §10 の「送信内容と `request_id` を記録」をそのまま満たす。量はサイクルあたり数 KB |

## 9. 独立レビュー（Reviewer サブエージェント）の指摘と対応

初回実装を通しでレビューさせ、次の指摘を受けて修正した（すべてテストで固定済み）。

| 指摘 | 対応 |
| --- | --- |
| **M1** `queued` 送信で「先行ターンの回答」を新しい `request_id` に記録してしまう | `generating` / `unconfirmed` 中は送らない（次のサイクルへ）。万一 `queued` になった場合は先行ターンの終了を境界にし、それ以前のイベントを本文に取り込まない |
| **M2** ローカル拒否の `RequestTooLarge` が「届いたかもしれない」扱いになり、finding が永久に捨てられる | エンコード段階の失敗を `never_reached_bridge()` に含め、`Deferred`（カーソルは進めない）にした |
| **M3** 台帳末尾が破損していると、直後に書いた write-ahead レコードが次回起動で消える | 次の書き込みで改行を補ってから追記。torn tail → `begin` → 再起動のテストを追加 |
| **M4** 受信側にも 64 KiB 上限を適用していたため、長い `entry.text` で購読ごと失う | イベント行の上限を 4 MiB に分離。超過行はその行だけ捨てて購読を維持し、`line_too_large` として記録 |
| **m1** 「停止」推奨の拒否でループし続ける | `CycleOutcome::Stopped` を追加し `run()` を抜ける |
| **m2** シグナルがサイクル途中で割り込む | シグナルはフラグを立てるだけにし、サイクル境界で終了 |
| **m3** `reconcile_pending` にテストが無く、起動時しか呼ばれない | 3 分岐（delivered / 未知 / ブリッジが忘れた）をテスト化 |
| **m5** 未知の送信ステータスを「未配送」扱い | `unknown`（再送しない）に倒した |
| **m7** アーカイブ済みスレッドでも購読を開いていた | 状態確認 → 判定 → 購読の順に変更 |
| **m8** 本文なしターンがタイムアウトまで待つ | `entry`（ツール呼び出し含む）を生存の証拠に加えた |
| **m9** 購読ストリーム上の拒否行を黙って捨てる | `WatchRejected` として返し、再接続の警告を出す |
| **m10** `error_code` にプロトコル外の値 | `local:` 接頭辞に統一 |
| **m11** 二重起動の防止が無い | `InstanceLock`（flock）を `run` で取得 |
| **m12** `HOME` 未設定時に `--socket` でも通らない | クエリ系は `HOME` 無しでも動くようにした |
| **n1** 64 KiB 境界の 1 バイト差 | エンコード後のサイズで予算を取り、4096 バイトの余裕を設定検証で確保 |
| **n2** 部品番号の予算が 2 桁固定 | 3 桁（999）で見積もる |
| **n3** `request_id` 切り詰めの衝突で台帳を上書きし得る | 同一 id で別内容の `begin` をエラーにした |
| **m4（未対応）** 台帳が無制限に成長 | 制約として README に明記（世代剪定は将来作業）。`finish` 行から本文を落として増加を半分にした |
| **n4/n5, m6** | 設計判断として現状維持（理由はコードのコメントと README に記載） |

検証結果（最終）: `cargo test` 125 件成功（lib 73 / daemon_cycle 29 / protocol_conformance 23）、
`cargo fmt --all -- --check` と `cargo clippy --all-targets -- -D warnings` は無警告。
実 Zed / 実スレッドの E2E と `LOCAL_PEERCRED` 検証は未検証（§6）。
