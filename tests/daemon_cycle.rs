//! The audit cycle, driven against a fake bridge.
//!
//! These tests are the closest thing to an end-to-end run this environment
//! allows: a real Zed with a real thread is not available, so every rule about
//! sending, waiting, retrying, and persisting is checked against the fake bridge
//! instead.

mod support;

use std::path::{Path, PathBuf};
use std::time::Duration;

use support::{
    FakeBridge, Script, SendReply, ThreadFacts, TurnScript, entry_added, status_changed,
};
use zed_audit_loop::config::{Config, FindingSourceKind, TargetSelect};
use zed_audit_loop::daemon::{CycleOutcome, Daemon};
use zed_audit_loop::ledger::LedgerState;
use zed_audit_loop::logging::Logger;

const THREAD: &str = "thread-a";

/// One test run: a fake bridge, a home directory, and a findings file.
struct Harness {
    bridge: FakeBridge,
    home: tempfile::TempDir,
    findings: PathBuf,
}

impl Harness {
    fn new(script: Script, findings: &[&str]) -> Self {
        let home = tempfile::tempdir().expect("home dir");
        let findings_path = home.path().join("findings.jsonl");
        let mut contents = String::new();
        for line in findings {
            contents.push_str(line);
            contents.push('\n');
        }
        std::fs::write(&findings_path, contents).expect("write findings");

        Self {
            bridge: FakeBridge::start(script),
            home,
            findings: findings_path,
        }
    }

    fn config(&self) -> Config {
        let mut config = Config::default();
        config.bridge.socket = Some(self.bridge.path().to_path_buf());
        config.target.thread_id = Some(THREAD.to_string());
        config.finding.file = Some(self.findings.clone());
        config.storage.dir = Some(self.home.path().join("state"));
        config.cycle.interval_secs = 1;
        config.cycle.await_response_secs = 1;
        config.cycle.approval_poll_ms = 5;
        config.cycle.max_approval_wait_secs = 1;
        config.cycle.backoff_initial_ms = 1;
        config.cycle.backoff_max_ms = 4;
        config.cycle.max_send_attempts = 3;
        config
    }

    fn daemon(&self) -> Daemon {
        Daemon::new(
            self.config(),
            Logger::silent(),
            self.home.path().to_path_buf(),
        )
        .expect("build daemon")
    }

    fn state_dir(&self) -> PathBuf {
        self.home.path().join("state")
    }

    fn cursor_path(&self) -> PathBuf {
        self.state_dir().join("findings.cursor.json")
    }

    fn read_state_file(&self, relative: &str) -> String {
        let path = self.state_dir().join(relative);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()))
    }

    /// Pretends the daemon crashed before it accepted the findings, which is the
    /// only way the same message can legitimately be re-derived.
    fn rewind_findings_cursor(&self) {
        let _ = std::fs::remove_file(self.cursor_path());
    }
}

fn finding(summary: &str) -> String {
    format!(r#"{{"summary":"{summary}","severity":"warning","location":"src/lib.rs:1"}}"#)
}

fn script_with_turn(answer: &str) -> Script {
    Script {
        thread: Some(ThreadFacts::idle(THREAD)),
        turn: Some(TurnScript::standard(THREAD, answer)),
        ..Script::default()
    }
}

#[tokio::test]
async fn a_cycle_sends_findings_and_stores_the_answer() {
    let harness = Harness::new(
        script_with_turn("## Assistant\n\nConfirmed."),
        &[&finding("mismatch")],
    );
    let mut daemon = harness.daemon();

    let outcome = daemon.run_once().await;

    let CycleOutcome::Sent {
        request_id,
        queued,
        entries,
        text_bytes,
    } = outcome.clone()
    else {
        panic!("expected a send, got {}", outcome.summary());
    };
    assert!(!queued);
    // Only the agent entry counts as the answer; the `user_message` echo of what
    // the daemon sent does not.
    assert_eq!(entries, 1);
    assert!(text_bytes > 0);

    // The request carries the audit header and the finding.
    let sends = harness.bridge.requests_for("send_message");
    assert_eq!(sends.len(), 1);
    assert_eq!(sends[0].request_id, request_id);
    assert_eq!(sends[0].thread_id.as_deref(), Some(THREAD));
    assert_eq!(sends[0].raw["mode"], "prompt");
    let message = sends[0].message.as_deref().expect("message present");
    assert!(message.contains("【独立監査】"));
    assert!(message.contains("mismatch"));
    assert!(sends[0].request_id.starts_with("audit-"));

    // The watcher was opened before the send, on its own connection.
    assert_eq!(harness.bridge.requests_for("watch_thread").len(), 1);

    // The events and the answer are on disk.
    let events = harness.read_state_file(&format!("events/{THREAD}.jsonl"));
    assert!(events.contains("entry_added"));
    // `entry_added: idle` finishes the turn, so `turn_stopped` may not be read:
    // the bridge only sends it when the stop reason is known.
    assert!(events.contains("status_changed"));
    let turns = harness.read_state_file(&format!("turns/{THREAD}.jsonl"));
    assert!(turns.contains("Confirmed."));
    assert!(turns.contains(&request_id));
    assert!(
        !turns.contains("audit please"),
        "the echo of our own message is not part of the answer"
    );

    // The ledger says it was delivered.
    let ledger = harness.read_state_file("ledger.jsonl");
    assert!(ledger.contains("\"delivered\""));

    // The findings were consumed: the next cycle has nothing to do.
    let next = daemon.run_once().await;
    assert!(
        matches!(next, CycleOutcome::Idle(_)),
        "got {}",
        next.summary()
    );
    assert_eq!(harness.bridge.requests_for("send_message").len(), 1);
}

#[tokio::test]
async fn no_findings_sends_nothing() {
    let harness = Harness::new(script_with_turn("ignored"), &[]);
    let mut daemon = harness.daemon();

    let outcome = daemon.run_once().await;

    assert!(
        matches!(outcome, CycleOutcome::Idle(_)),
        "got {}",
        outcome.summary()
    );
    assert!(harness.bridge.requests_for("send_message").is_empty());
}

#[tokio::test]
async fn a_full_queue_is_retried_with_a_new_request_id() {
    let harness = Harness::new(
        Script {
            thread: Some(ThreadFacts::idle(THREAD)),
            send_replies: vec![
                SendReply::Rejected(zed_audit_loop::protocol::ErrorCode::QueueFull),
                SendReply::Delivered,
            ],
            turn: Some(TurnScript::standard(THREAD, "done")),
            ..Script::default()
        },
        &[&finding("queued first")],
    );
    let mut daemon = harness.daemon();

    let outcome = daemon.run_once().await;

    assert!(
        matches!(outcome, CycleOutcome::Sent { .. }),
        "got {}",
        outcome.summary()
    );
    let sends = harness.bridge.requests_for("send_message");
    assert_eq!(sends.len(), 2, "queue_full must be retried once");
    assert_ne!(
        sends[0].request_id, sends[1].request_id,
        "a recorded rejection poisons the id, so the retry needs a new one"
    );
    assert!(sends[1].request_id.ends_with(&format!("-{}", 1)));
}

#[tokio::test]
async fn an_awaiting_approval_rejection_waits_and_then_retries() {
    let harness = Harness::new(
        Script {
            thread: Some(ThreadFacts::idle(THREAD)),
            send_replies: vec![
                SendReply::Rejected(zed_audit_loop::protocol::ErrorCode::AwaitingApproval),
                SendReply::Delivered,
            ],
            turn: Some(TurnScript::standard(THREAD, "approved")),
            ..Script::default()
        },
        &[&finding("needs approval")],
    );
    let mut daemon = harness.daemon();

    let outcome = daemon.run_once().await;

    assert!(
        matches!(outcome, CycleOutcome::Sent { .. }),
        "got {}",
        outcome.summary()
    );
    let sends = harness.bridge.requests_for("send_message");
    assert_eq!(sends.len(), 2);
    assert_ne!(sends[0].request_id, sends[1].request_id);
}

#[tokio::test]
async fn an_awaiting_approval_thread_is_left_alone() {
    let mut facts = ThreadFacts::idle(THREAD);
    facts.state = "awaiting_approval".to_string();
    let harness = Harness::new(
        Script {
            thread: Some(facts),
            ..Script::default()
        },
        &[&finding("must wait")],
    );
    let mut daemon = harness.daemon();

    let outcome = daemon.run_once().await;

    assert!(
        matches!(outcome, CycleOutcome::Deferred(_)),
        "got {}",
        outcome.summary()
    );
    assert!(
        harness.bridge.requests_for("send_message").is_empty(),
        "approval must never be bypassed"
    );
}

#[tokio::test]
async fn an_archived_thread_is_skipped() {
    let mut facts = ThreadFacts::idle(THREAD);
    facts.archived = true;
    let harness = Harness::new(
        Script {
            thread: Some(facts),
            ..Script::default()
        },
        &[&finding("archived")],
    );
    let mut daemon = harness.daemon();

    let outcome = daemon.run_once().await;

    assert!(
        matches!(outcome, CycleOutcome::Deferred(_)),
        "got {}",
        outcome.summary()
    );
    assert!(harness.bridge.requests_for("send_message").is_empty());
}

#[tokio::test]
async fn a_draft_thread_defers_the_cycle() {
    let harness = Harness::new(
        Script {
            thread: Some(ThreadFacts::idle(THREAD)),
            send_replies: vec![SendReply::Status(
                zed_audit_loop::protocol::Status::UnsupportedThreadType,
            )],
            ..Script::default()
        },
        &[&finding("draft")],
    );
    let mut daemon = harness.daemon();

    let outcome = daemon.run_once().await;

    assert!(
        matches!(outcome, CycleOutcome::Deferred(_)),
        "got {}",
        outcome.summary()
    );
    assert_eq!(harness.bridge.requests_for("send_message").len(), 1);
    // The findings stay unread, so a later cycle can report them again.
    assert!(!harness.cursor_path().exists());
}

#[tokio::test]
async fn a_missing_thread_defers_without_sending() {
    let harness = Harness::new(
        Script {
            thread: Some(ThreadFacts::missing(THREAD)),
            ..Script::default()
        },
        &[&finding("missing")],
    );
    let mut daemon = harness.daemon();

    let outcome = daemon.run_once().await;

    assert!(
        matches!(outcome, CycleOutcome::Deferred(_)),
        "got {}",
        outcome.summary()
    );
    assert!(harness.bridge.requests_for("send_message").is_empty());
}

#[tokio::test]
async fn thread_unavailable_is_retried_with_the_same_request_id() {
    let harness = Harness::new(
        Script {
            thread: Some(ThreadFacts::idle(THREAD)),
            send_replies: vec![
                SendReply::Status(zed_audit_loop::protocol::Status::ThreadUnavailable),
                SendReply::Delivered,
            ],
            turn: Some(TurnScript::standard(THREAD, "restored")),
            ..Script::default()
        },
        &[&finding("unavailable")],
    );
    let mut daemon = harness.daemon();

    let first = daemon.run_once().await;
    assert!(
        matches!(first, CycleOutcome::Deferred(_)),
        "got {}",
        first.summary()
    );

    // The bridge does not record `thread_unavailable`, so the id is reusable.
    let second = daemon.run_once().await;
    assert!(
        matches!(second, CycleOutcome::Sent { .. }),
        "got {}",
        second.summary()
    );

    let sends = harness.bridge.requests_for("send_message");
    assert_eq!(sends.len(), 2);
    assert_eq!(sends[0].request_id, sends[1].request_id);
}

#[tokio::test]
async fn a_lost_watch_is_rebuilt_and_the_state_reread() {
    let harness = Harness::new(
        Script {
            thread: Some(ThreadFacts::idle(THREAD)),
            turn: Some(TurnScript {
                events: vec![status_changed(THREAD, "generating")],
            }),
            close_watch_after_send: true,
            ..Script::default()
        },
        &[&finding("watch drops")],
    );
    let mut daemon = harness.daemon();

    let outcome = daemon.run_once().await;

    assert!(
        matches!(outcome, CycleOutcome::Sent { .. }),
        "got {}",
        outcome.summary()
    );
    // A second watch was opened after the connection dropped, and the current
    // state was read again because events are never replayed.
    let watches = harness.bridge.requests_for("watch_thread");
    assert!(watches.len() >= 2, "watches: {}", watches.len());
    let statuses = harness.bridge.requests_for("get_thread_status");
    assert!(statuses.len() >= 2, "status reads: {}", statuses.len());
}

#[tokio::test]
async fn a_silent_agent_does_not_block_the_next_cycle() {
    let harness = Harness::new(
        Script {
            thread: Some(ThreadFacts::idle(THREAD)),
            turn: Some(TurnScript::silent()),
            ..Script::default()
        },
        &[&finding("silence")],
    );
    let mut daemon = harness.daemon();
    let started = std::time::Instant::now();

    let outcome = daemon.run_once().await;

    assert!(
        matches!(outcome, CycleOutcome::Sent { .. }),
        "got {}",
        outcome.summary()
    );
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(harness.bridge.requests_for("send_message").len(), 1);
    // A delivery without an answer is still a delivery.
    let ledger = harness.read_state_file("ledger.jsonl");
    assert!(ledger.contains("\"delivered\""));
}

#[tokio::test]
async fn a_delivered_message_is_not_sent_again_after_a_restart() {
    let harness = Harness::new(script_with_turn("first answer"), &[&finding("once")]);
    let outcome = harness.daemon().run_once().await;
    assert!(
        matches!(outcome, CycleOutcome::Sent { .. }),
        "got {}",
        outcome.summary()
    );
    assert_eq!(harness.bridge.requests_for("send_message").len(), 1);

    // A crash before the findings cursor moved leaves the same findings in the
    // file, which is exactly how a duplicate would happen.
    harness.rewind_findings_cursor();
    let mut restarted = harness.daemon();
    let outcome = restarted.run_once().await;

    assert!(
        matches!(outcome, CycleOutcome::Idle(_)),
        "got {}",
        outcome.summary()
    );
    assert_eq!(
        harness.bridge.requests_for("send_message").len(),
        1,
        "the ledger must suppress the second send"
    );
}

#[tokio::test]
async fn large_findings_are_split_across_messages() {
    let detail = "x".repeat(400);
    let findings: Vec<String> = (0..8)
        .map(|index| {
            format!(r#"{{"summary":"problem {index}","detail":"{detail}","severity":"error"}}"#)
        })
        .collect();
    let finding_refs: Vec<&str> = findings.iter().map(String::as_str).collect();
    let harness = Harness::new(
        Script {
            thread: Some(ThreadFacts::idle(THREAD)),
            turn: Some(TurnScript::standard(THREAD, "ack")),
            ..Script::default()
        },
        &finding_refs,
    );
    let mut config = harness.config();
    // A small budget forces the findings into several messages.
    config.finding.max_message_bytes = 1_500;
    let mut daemon = Daemon::new(config, Logger::silent(), harness.home.path().to_path_buf())
        .expect("build daemon");

    let outcome = daemon.run_once().await;

    assert!(
        matches!(outcome, CycleOutcome::Sent { .. }),
        "got {}",
        outcome.summary()
    );
    let sends = harness.bridge.requests_for("send_message");
    assert!(
        sends.len() >= 2,
        "expected a split, got {} sends",
        sends.len()
    );
    for send in &sends {
        let message = send.message.as_deref().unwrap_or_default();
        assert!(
            message.len() < zed_audit_loop::protocol::MAX_LINE_BYTES,
            "a batch must fit the bridge line limit"
        );
    }
    // Every finding reached the bridge exactly once.
    let all: String = sends
        .iter()
        .filter_map(|send| send.message.clone())
        .collect();
    for index in 0..8 {
        assert_eq!(
            all.matches(&format!("problem {index}")).count(),
            1,
            "finding {index} appears once"
        );
    }
}

#[tokio::test]
async fn the_latest_thread_is_selected_when_asked() {
    let mut archived = ThreadFacts::idle("thread-archived");
    archived.archived = true;
    let harness = Harness::new(
        Script {
            threads: vec![archived, ThreadFacts::idle(THREAD)],
            thread: Some(ThreadFacts::idle(THREAD)),
            turn: Some(TurnScript::standard(THREAD, "latest")),
            ..Script::default()
        },
        &[&finding("latest target")],
    );
    let mut config = harness.config();
    config.target.thread_id = None;
    config.target.select = TargetSelect::Latest;
    let mut daemon = Daemon::new(config, Logger::silent(), harness.home.path().to_path_buf())
        .expect("build daemon");

    let outcome = daemon.run_once().await;

    assert!(
        matches!(outcome, CycleOutcome::Sent { .. }),
        "got {}",
        outcome.summary()
    );
    let sends = harness.bridge.requests_for("send_message");
    assert_eq!(sends.len(), 1);
    assert_eq!(sends[0].thread_id.as_deref(), Some(THREAD));
}

#[tokio::test]
async fn a_command_source_feeds_the_cycle() {
    let harness = Harness::new(script_with_turn("from command"), &[]);
    let mut config = harness.config();
    config.finding.source = FindingSourceKind::Command;
    config.finding.command = Some("sh".to_string());
    config.finding.args = vec![
        "-c".to_string(),
        r#"printf '{"summary":"command finding"}\n'"#.to_string(),
    ];
    let mut daemon = Daemon::new(config, Logger::silent(), harness.home.path().to_path_buf())
        .expect("build daemon");

    let outcome = daemon.run_once().await;

    assert!(
        matches!(outcome, CycleOutcome::Sent { .. }),
        "got {}",
        outcome.summary()
    );
    let sends = harness.bridge.requests_for("send_message");
    assert_eq!(sends.len(), 1);
    assert!(
        sends[0]
            .message
            .as_deref()
            .unwrap_or_default()
            .contains("command finding")
    );
}

#[tokio::test]
async fn an_unreachable_bridge_defers_instead_of_failing() {
    let home = tempfile::tempdir().expect("home dir");
    let findings = home.path().join("findings.jsonl");
    std::fs::write(&findings, format!("{}\n", finding("offline"))).expect("write findings");
    let mut config = Config::default();
    config.bridge.socket = Some(home.path().join("no-bridge.sock"));
    config.target.thread_id = Some(THREAD.to_string());
    config.finding.file = Some(findings);
    config.storage.dir = Some(home.path().join("state"));
    config.bridge.connect_timeout_ms = 200;
    let mut daemon =
        Daemon::new(config, Logger::silent(), home.path().to_path_buf()).expect("build daemon");

    let outcome = daemon.run_once().await;

    assert!(
        matches!(outcome, CycleOutcome::Deferred(_)),
        "got {}",
        outcome.summary()
    );
    // Nothing was sent, so the findings are still pending.
    assert!(daemon.ledger().is_empty());
    assert!(!home.path().join("state/findings.cursor.json").exists());
}

#[tokio::test]
async fn events_arriving_after_the_turn_are_still_stored() {
    let harness = Harness::new(
        Script {
            thread: Some(ThreadFacts::idle(THREAD)),
            turn: Some(TurnScript {
                events: vec![
                    status_changed(THREAD, "generating"),
                    entry_added(THREAD, 5, "assistant_message", "partial"),
                    entry_added(THREAD, 5, "assistant_message", "final"),
                    status_changed(THREAD, "idle"),
                ],
            }),
            ..Script::default()
        },
        &[&finding("updated entry")],
    );
    let mut daemon = harness.daemon();

    let outcome = daemon.run_once().await;

    assert!(
        matches!(outcome, CycleOutcome::Sent { .. }),
        "got {}",
        outcome.summary()
    );
    let turns = harness.read_state_file(&format!("turns/{THREAD}.jsonl"));
    assert!(turns.contains("final"));
    assert!(
        !turns.contains("partial"),
        "an updated entry replaces its text"
    );
}

#[test]
fn a_config_without_a_target_is_rejected() {
    let config = Config::default();
    assert!(config.validate().is_err());
}

#[test]
fn the_ledger_state_after_a_restart_is_readable() {
    let directory = tempfile::tempdir().expect("temp dir");
    let ledger = zed_audit_loop::ledger::Ledger::open(directory.path()).expect("ledger");
    assert!(ledger.is_empty());
    assert_eq!(ledger.skipped_lines(), 0);
    assert!(LedgerState::Delivered.blocks_resend());
}

fn _assert_path_types(_: &Path) {}
