# 要件対応表（仕様書 → 実装 → テスト）

対象仕様: [`docs/external-bridge-spec.md`](external-bridge-spec.md)（元: 依頼者の
`zed-agent-bridge-spec.md`）。
「実装」はこのリポジトリ内のシンボル、「テスト」は `cargo test` で走るテスト名。

## §2 前提と有効化

| 要件 | 実装 | テスト |
| --- | --- | --- |
| socket は `~/Library/Application Support/Zed/agent-bridge-<channel>.sock` | `config::default_socket_path`, `Config::socket_path` | `config::tests::defaults_resolve_to_the_stable_channel`, `a_channel_selects_its_own_socket` |
| 空の channel は `stable` に正規化 | `config::normalized_channel` | `config::tests::a_blank_channel_falls_back_to_stable` |
| socket の明示指定（`~` 展開） | `config::expand_tilde`, `BridgeConfig::socket` | `config::tests::an_explicit_socket_wins_and_expands_a_tilde` |
| 設定で無効 (`bridge_disabled`) は型付きで扱う | `ErrorCode::BridgeDisabled`, `Response::request_id` を任意としてパース | `protocol::types::tests::every_documented_error_code_round_trips`, `tests/protocol_conformance.rs::a_bridge_disabled_rejection_carries_no_request_id` |

## §3 トランスポートとフレーミング

| 要件 | 実装 | テスト |
| --- | --- | --- |
| Unix domain stream、NDJSON（`\n` 区切り） | `protocol::client::LineReader`, `Connection::exchange` | `protocol::client::tests::reads_newline_delimited_lines` |
| 1 行 64 KiB 上限、超過時 `message_too_large` + 切断 | `protocol::MAX_LINE_BYTES`, `Request::to_line`, `LineReader` | `protocol::client::tests::rejects_lines_over_the_limit`, `tests/protocol_conformance.rs::an_oversized_message_is_refused_without_touching_the_socket`, `a_message_too_large_rejection_is_reported_and_the_client_recovers` |
| 送信前にエンコード後の長さで上限を守る（JSON エスケープ込み） | `finding::wire_len`（`render_batches` の予算）, `ErrorCode`/`ClientError::RequestTooLarge` | `finding::tests::a_message_of_many_newlines_fits_the_encoded_budget`, `protocol::types::tests::oversized_message_is_refused_before_sending` |
| 1 接続 100 要求。購読は専用接続 | `ClientConfig::max_requests_per_connection`, `BridgeClient::ensure_connection`, `BridgeClient::watch` | `tests/protocol_conformance.rs::the_client_reopens_after_its_request_budget`, `watching_streams_every_event_kind` |
| 切り詰められた行を応答として解釈しない | `LineReader::next_line`（EOF 途中は `UnexpectedEof`） | `protocol::client::tests::a_truncated_final_line_is_an_error_not_a_response` |

## §4 リクエスト共通形

| 要件 | 実装 | テスト |
| --- | --- | --- |
| `version: 1` / `request_id` / `action` を必ず送る | `Request::to_line` | `protocol::types::tests::send_request_carries_thread_and_message_only` |
| `thread_id` と `session_id` は排他 | `ThreadTarget` enum（型で排他） | `protocol::types::tests::a_session_target_uses_session_id_and_not_thread_id` |
| `mode: prompt` 固定で送る（`notify` は未実装） | `Request::to_line`, `Daemon::send_with_retries` | `send_request_carries_thread_and_message_only`（`mode` が `prompt`） |
| `delivery: after_current_turn` | `Request::to_line` | 同上 |

## §5 アクション

| 要件 | 実装 | テスト |
| --- | --- | --- |
| §5.1 `list_threads`（`accepted` + `threads`） | `BridgeClient::list_threads` | `tests/protocol_conformance.rs::list_threads_returns_the_threads_in_order` |
| §5.2 `get_thread_status`（`found` / `deliverable` / `loaded`） | `BridgeClient::thread_status`, `Daemon::thread_status` | `get_thread_status_reports_state_and_deliverability` |
| §5.2 存在しない → `thread_not_found`（`found: false`） | `Daemon::thread_status` が `found` を見る | `a_missing_thread_is_a_status_not_a_transport_error`, `tests/daemon_cycle.rs::a_missing_thread_defers_without_sending` |
| §5.3 `delivered` + `message_id` / `queued` | `Daemon::send_with_retries` | `send_message_reports_delivery_and_the_message_id`, `send_message_can_be_queued` |
| §5.3 承認待ちへ送らない（`awaiting_approval`） | `Daemon::run_cycle`（送信前チェック）, `Daemon::wait_for_approval` | `tests/daemon_cycle.rs::an_awaiting_approval_thread_is_left_alone`, `an_awaiting_approval_rejection_waits_and_then_retries` |
| §5.3 アーカイブ済み / draft / 重複 / キュー満杯 | `Daemon::send_with_retries` + `ErrorCode::recommended_action` | `an_archived_thread_is_skipped`, `a_draft_thread_defers_the_cycle`, `a_full_queue_is_retried_with_a_new_request_id` |
| §5.4 `get_delivery_status` / `unknown_request` | `BridgeClient::delivery_status`, `Daemon::reconcile_pending` | `tests/protocol_conformance.rs::get_delivery_status_surfaces_unknown_request` |
| §5.5 `watch_thread` の `accepted` とイベント行 | `BridgeClient::watch`, `Subscription::next_event` | `watching_streams_every_event_kind` |
| §5.5 イベント種別（`entry_added` / `entry_updated` / `entry_removed` / `status_changed` / `title_updated` / `tool_authorization_*` / `turn_stopped`） | `protocol::EventKind`, `Daemon::await_turn` | `tests/protocol_conformance.rs::every_documented_event_kind_decodes`, `watching_streams_every_event_kind`, `tests/daemon_cycle.rs::events_arriving_after_the_turn_are_still_stored` |
| §5.5 未知の `kind` でも壊れない | `EventKind::Other` | `protocol::types::tests::unknown_wire_values_are_preserved`, `tests/protocol_conformance.rs::an_unknown_event_kind_does_not_break_the_stream` |
| §5.5 長すぎるイベント行で購読を失わない | `MAX_EVENT_LINE_BYTES`, `LineReader::skip_line` | `tests/protocol_conformance.rs::a_long_event_line_is_discarded_without_losing_the_subscription` |
| §5.5 購読ストリーム上の拒否を握りつぶさない | `Subscription::next_event`（拒否行は `WatchRejected`） | `tests/protocol_conformance.rs::a_rejection_on_the_watch_stream_is_reported` |
| §5.5 接続断で購読解除・過去は再生されない | `Daemon::await_turn`（再接続 + `get_thread_status` 読み直し） | `tests/daemon_cycle.rs::a_lost_watch_is_rebuilt_and_the_state_reread` |
| §5.5 購読が落ち続けても無限ループしない | `daemon::MAX_WATCH_RECONNECTS` | `tests/daemon_cycle.rs::a_watch_that_keeps_dropping_ends_the_cycle` |

## §6 ステータス

| 要件 | 実装 | テスト |
| --- | --- | --- |
| 9 種の `status` を型付きで扱う（未知値も保持） | `protocol::Status` | `protocol::types::tests::every_documented_status_and_state_round_trips`, `unknown_wire_values_are_preserved` |
| `event` 行は応答と区別する | `Subscription::next_event` が `event` 以外を読み飛ばす | `watching_streams_every_event_kind` |
| 未知の `status` は「届いたかもしれない」として扱う | `Daemon::send_with_retries`（`Status::Other` → 台帳 `unknown`） | `tests/daemon_cycle.rs::an_unknown_send_status_is_never_resent` |

## §7 エラーコード

| 要件 | 実装 | テスト |
| --- | --- | --- |
| 15 種の `error.code` を型付きで扱う | `protocol::ErrorCode` | `protocol::types::tests::every_documented_error_code_round_trips` |
| 「推奨動作」を表として写像 | `ErrorCode::recommended_action` | `protocol::types::tests::every_error_code_has_a_recommended_action` |
| `message_too_large` → 分割して再送 | `finding::render_batches`（送信前に分割・1 件が大きすぎる場合は切り詰め）, `RecommendedAction::Split` | `finding::tests::batches_never_exceed_the_budget_and_keep_every_finding`, `a_finding_larger_than_the_budget_is_truncated_not_dropped`, `tests/daemon_cycle.rs::large_findings_are_split_across_messages` |
| `queue_full` → 時間を置いて再試行 | `Daemon::send_with_retries`（指数バックオフ） | `a_full_queue_is_retried_with_a_new_request_id` |
| `duplicate_request` → 送信済み扱い | `RecommendedAction::AlreadySent`（台帳を `delivered` に更新） | `tests/protocol_conformance.rs::duplicate_request_is_reported_as_already_handled`, `tests/daemon_cycle.rs::a_duplicate_request_counts_as_delivered` |
| `invalid_json` / `unknown_action` / `invalid_request` を握りつぶさない | `ClientError::MalformedResponse`, `Response::error_code`, `CycleOutcome::Stopped` | `tests/protocol_conformance.rs::a_malformed_response_line_is_an_error_not_a_panic`, `an_unknown_action_rejection_is_surfaced_with_its_code`, `an_invalid_json_rejection_keeps_going`, `an_invalid_request_rejection_is_surfaced`; 停止動作は `tests/daemon_cycle.rs::a_protocol_bug_stops_the_daemon_instead_of_looping` |
| `bridge_disabled` は `request_id` 空で返る | `Response::request_id` を任意としてパース | `protocol::types::tests::response_accepts_empty_request_id_and_missing_file_fields` |

## §8 典型的なシーケンス（監査 1 サイクル）

| 要件 | 実装 | テスト |
| --- | --- | --- |
| 1→7 の順序（対象決定・購読・状態・送信・イベント・本文取得） | `Daemon::run_cycle` → `deliver_batch` → `await_turn` | `tests/daemon_cycle.rs::a_cycle_sends_findings_and_stores_the_answer` |
| 購読は専用接続で保持（100 要求上限の回避） | `Daemon::ensure_subscription` | `tests/daemon_cycle.rs::a_cycle_sends_findings_and_stores_the_answer`（`watch_thread` 1 回・`get_thread_status` 1 回・接続 2 本以上を断言） |
| 応答本文は `entry.text` から取得 | `store::TurnCapture` | `a_cycle_sends_findings_and_stores_the_answer`, `store::tests::a_turn_concatenates_entries_in_index_order` |
| `turn_stopped` が来ない場合の完了判定 | `store::is_finished_state` + `status_changed: idle` | `tests/daemon_cycle.rs::events_arriving_after_the_turn_are_still_stored` |
| 応答が来ないまま時間切れでも送信は成立させる | `TurnOutcome::TimedOut` | `a_silent_agent_does_not_block_the_next_cycle` |
| 短いターン（`generating` を観測できない）でも完了する | `Daemon::await_turn`（エージェント本文 + `idle` で完了） | `a_turn_without_a_generating_event_still_finishes` |
| サイクル間のイベントをターン本文と混同しない | `Daemon::drain_pending_events`（送信前に読み切る） | `events_from_before_the_send_are_not_taken_as_the_answer` |
| 前のターンの残りイベントが次のターンを終わらせない | `daemon::turn_finishes`（`generating` かエージェント本文を必要条件にする） | `daemon::tests::a_stale_stop_does_not_end_a_turn_that_never_started`, `a_stop_ends_a_turn_that_showed_life`, `only_idle_ends_a_turn_through_a_state_change` |

## §9 参照実装（CLI）

| 要件 | 実装 | テスト |
| --- | --- | --- |
| `list-threads` / `thread-status` で事前疎通確認 | `main.rs`（`list_threads`, `thread_status` サブコマンド） | 手動スモーク（fake bridge に対して実行、本報告に出力記録）。自動テストはない |
| `thread_not_found` は終了コード 0（照会結果） | `main.rs::thread_status` | 自動テストなし（終了コードは手動確認） |

## §10 デーモン実装上の推奨

| 要件 | 実装 | テスト |
| --- | --- | --- |
| `request_id` を永続化して一意に | `ledger::Ledger`（write-ahead、`request_id` は内容ハッシュ + attempt、衝突は拒否） | `ledger::tests::a_record_survives_a_restart`, `request_ids_are_readable_and_attempt_scoped`, `reusing_an_id_for_other_content_is_refused`, `a_torn_tail_does_not_swallow_the_next_record` |
| 二重起動で二重配送しない | `daemon::InstanceLock`（flock） | `daemon::tests::one_daemon_per_storage_directory` |
| 再起動後の再送で二重配送しない | `Ledger::find_blocking` + `Daemon::send_with_retries` | `tests/daemon_cycle.rs::a_delivered_message_is_not_sent_again_after_a_restart` |
| 記録される拒否のときだけ新しい `request_id` | `ErrorCode::is_recorded_by_bridge`, `Ledger::next_attempt` | `tests/daemon_cycle.rs::a_full_queue_is_retried_with_a_new_request_id`, `thread_unavailable_is_retried_with_the_same_request_id` |
| 1 目的 1 接続 | `BridgeClient`（照会用の使い回し + 100 要求でローテート）, `Subscription`（専用） | `the_client_reopens_after_its_request_budget`, `watching_streams_every_event_kind`, `tests/daemon_cycle.rs::a_cycle_sends_findings_and_stores_the_answer`（デーモンが接続を分けていることを断言） |
| 購読と `get_delivery_status` の役割分担 | `Daemon::await_turn`（変化）, `Daemon::reconcile_pending`（送信結果） | `a_lost_watch_is_rebuilt_and_the_state_reread`, `tests/daemon_cycle.rs::a_pending_send_is_reconciled_against_the_bridge_ledger`, `a_pending_send_the_bridge_forgot_is_marked_unknown` |
| 再接続時は状態を取り直す | `Daemon::await_turn` の再接続ブロック | 同上 |
| `generating` 中の連投を避け、`queue_full` はバックオフ | `Daemon::run_cycle`（busy なら送らない）, `Daemon::send_with_retries`（バックオフ） | `tests/daemon_cycle.rs::a_busy_agent_is_not_messaged`, `a_full_queue_is_retried_with_a_new_request_id` |
| 承認待ちは迂回せず待つ | `Daemon::wait_for_approval` | `an_awaiting_approval_thread_is_left_alone` |
| socket パスはチャンネル依存 | `config::resolve` 相当（`Config::socket_path`） | `config::tests::a_channel_selects_its_own_socket` |

## §11 制約・未検証事項

| 項目 | 本リポジトリでの扱い |
| --- | --- |
| 実スレッドへの配送・ターン開始の E2E | **未検証**（README §10 に明記） |
| 実機のキューイング / 承認待ち拒否 | 分岐と fake bridge のテストのみ |
| 実 `watch_thread` のイベント受信 | fake bridge のテストのみ |
| 過去イベントの非再生 | 設計で前提化（`entry.text` を都度保存）→ `store` |
| ブリッジ側台帳のメモリ保持 | デーモン側台帳で代替（at-most-once） |

## 意図的に実装していないもの

| 項目 | 理由 |
| --- | --- |
| 何を監査するかの判断（LLM など） | 依頼のスコープ外。`FindingSource` の境界のみ提供 |
| Issue 管理・通知 | 同上 |
| `notify` モード | ブリッジ側が未実装（`notify_disabled`） |
| Linux / Windows 対応 | 対象は macOS のみ |
| 承認の代行 | 仕様が意図的に提供していない |
