//! The audit loop.
//!
//! One cycle follows the bridge spec's example sequence: pick a thread, watch it on
//! a dedicated connection, read its state, send what the [`FindingSource`]
//! produced, then read events until the agent's turn ends. Everything the loop
//! does that is not obvious is a rule from the spec:
//!
//! - a message is never sent twice (the [`Ledger`] is written before sending);
//! - `awaiting_approval` is waited out, never bypassed;
//! - a full queue is retried with backoff and a *fresh* `request_id`, because the
//!   bridge records that rejection and would answer a repeat with
//!   `duplicate_request`;
//! - a lost subscription is rebuilt and the current state re-read, because the
//!   bridge never replays past events.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use tokio::time::{sleep, timeout};

use crate::config::{Config, TargetSelect};
use crate::finding::{
    CommandFindingSource, FileFindingSource, FindingBatch, FindingSource, render_batches,
};
use crate::ledger::{Ledger, LedgerState};
use crate::logging::Logger;
use crate::protocol::{
    BridgeClient, ErrorCode, EventKind, RecommendedAction, Status, Subscription, ThreadState,
    ThreadStatusInfo,
};
use crate::store::{Store, TurnCapture, is_finished_state};
use crate::time::unix_millis;

/// File under the storage directory that remembers how many findings were read.
pub const FINDINGS_CURSOR_FILE: &str = "findings.cursor.json";

/// How many times one turn may lose its watch before the cycle gives up. A
/// bridge that keeps dropping the subscription must not turn the response wait
/// into a reconnect storm.
pub const MAX_WATCH_RECONNECTS: u32 = 5;

/// What one cycle achieved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CycleOutcome {
    /// Nothing to report, or nothing was sent.
    Idle(String),
    /// A message reached the bridge.
    Sent {
        /// The `request_id` of the send.
        request_id: String,
        /// Whether the bridge queued it behind a running turn.
        queued: bool,
        /// Entries collected after the send.
        entries: usize,
        /// Bytes of answer text persisted.
        text_bytes: usize,
    },
    /// Transient trouble; the next cycle tries again.
    Deferred(String),
    /// The operator has to fix something.
    Failed(String),
}

impl CycleOutcome {
    /// A one-line description for logs and tests.
    pub fn summary(&self) -> String {
        match self {
            Self::Idle(reason) => format!("idle: {reason}"),
            Self::Sent {
                request_id,
                queued,
                entries,
                text_bytes,
            } => format!(
                "sent {request_id} ({}), {entries} entries, {text_bytes} bytes of answer",
                if *queued { "queued" } else { "delivered" }
            ),
            Self::Deferred(reason) => format!("deferred: {reason}"),
            Self::Failed(reason) => format!("failed: {reason}"),
        }
    }
}

/// What happened when one message was offered to the bridge.
enum SendOutcome {
    /// The bridge accepted it.
    Accepted { request_id: String, queued: bool },
    /// It was sent in an earlier run; sending again would duplicate it.
    AlreadySent { request_id: String },
    /// The thread cannot take messages; pick another one.
    Blocked(String),
    /// Worth retrying on a later cycle.
    Deferred(String),
    /// Give up on this cycle.
    Failed(String),
}

/// What happened while waiting for the agent's answer.
enum TurnOutcome {
    /// The turn ended, with the answer captured.
    Completed { entries: usize, text_bytes: usize },
    /// Nothing arrived in time.
    TimedOut,
    /// The watch kept failing.
    Lost(String),
}

/// The audit daemon.
pub struct Daemon {
    config: Config,
    logger: Logger,
    client: BridgeClient,
    ledger: Ledger,
    store: Store,
    source: Box<dyn FindingSource>,
    subscription: Option<Subscription>,
    cycle_seq: u64,
}

impl Daemon {
    /// Builds a daemon from a validated config.
    ///
    /// Opening the storage directory happens here so a permissions problem
    /// surfaces at start-up rather than on the first cycle.
    pub fn new(config: Config, logger: Logger, home: PathBuf) -> Result<Self> {
        config.validate()?;
        let storage_dir = config.storage_dir(&home);
        let ledger = Ledger::open(&storage_dir)
            .with_context(|| format!("opening the ledger in {}", storage_dir.display()))?;
        let store = Store::open(&storage_dir)
            .with_context(|| format!("opening the event store in {}", storage_dir.display()))?;
        let client = BridgeClient::new(config.client_config(&home));
        let source = build_source(&config, &home, &storage_dir)?;

        Ok(Self {
            config,
            logger,
            client,
            ledger,
            store,
            source,
            subscription: None,
            cycle_seq: 0,
        })
    }

    /// The config the daemon runs with.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The send ledger, for tests and for `--print-state` style reporting.
    pub fn ledger(&self) -> &Ledger {
        &self.ledger
    }

    /// The event store, for tests.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// The bridge client, for tests that need to change its socket.
    pub fn client_mut(&mut self) -> &mut BridgeClient {
        &mut self.client
    }

    /// Reports sends that were written down but never answered.
    ///
    /// Those are the only records whose fate is unknown, and the daemon does not
    /// resend them: double-delivering an audit note is worse than dropping one,
    /// and the bridge keeps no ledger across its own restarts to ask.
    pub async fn reconcile_pending(&mut self) -> Result<()> {
        let pending: Vec<(String, String)> = self
            .ledger
            .unanswered()
            .iter()
            .map(|record| (record.request_id.clone(), record.thread_id.clone()))
            .collect();
        if pending.is_empty() {
            return Ok(());
        }

        for (request_id, thread_id) in pending {
            match self.client.delivery_status(&request_id).await {
                Ok(response) => match response.delivery {
                    Some(delivery) if delivery.status == Status::Delivered => {
                        self.ledger.finish(
                            &request_id,
                            LedgerState::Delivered,
                            delivery.message_id,
                            None,
                        )?;
                        self.logger.info(
                            "reconciled_send",
                            &[
                                ("request_id", &request_id),
                                ("thread_id", &thread_id),
                                ("state", "delivered"),
                            ],
                        );
                    }
                    Some(delivery) if delivery.status == Status::Queued => {
                        self.ledger
                            .finish(&request_id, LedgerState::Queued, None, None)?;
                        self.logger.info(
                            "reconciled_send",
                            &[
                                ("request_id", &request_id),
                                ("thread_id", &thread_id),
                                ("state", "queued"),
                            ],
                        );
                    }
                    Some(delivery) => {
                        let code = delivery.error.as_ref().map(|error| error.code.to_string());
                        self.ledger.finish(
                            &request_id,
                            LedgerState::Rejected,
                            None,
                            code.clone(),
                        )?;
                        self.logger.warn(
                            "reconciled_send",
                            &[
                                ("request_id", &request_id),
                                ("thread_id", &thread_id),
                                ("state", "rejected"),
                                ("error_code", code.as_deref().unwrap_or("unknown")),
                            ],
                        );
                    }
                    None => {
                        // The bridge answered but knows nothing about the send,
                        // so it never reached the thread history.
                        self.ledger
                            .finish(&request_id, LedgerState::Unknown, None, None)?;
                        self.logger.warn(
                            "send_outcome_unknown",
                            &[
                                ("request_id", &request_id),
                                ("thread_id", &thread_id),
                                ("reason", "bridge_has_no_record"),
                            ],
                        );
                    }
                },
                Err(error) => {
                    // Zed may have restarted, which empties its ledger. Keep the
                    // record as "sending" and refuse to resend it.
                    self.logger.warn(
                        "reconcile_failed",
                        &[
                            ("request_id", &request_id),
                            ("thread_id", &thread_id),
                            ("error", &error.to_string()),
                        ],
                    );
                }
            }
        }
        Ok(())
    }

    /// Runs the loop until a termination signal arrives.
    pub async fn run(&mut self) -> Result<()> {
        use tokio::signal::unix::{SignalKind, signal};

        let mut interrupt = signal(SignalKind::interrupt()).context("listening for SIGINT")?;
        let mut terminate = signal(SignalKind::terminate()).context("listening for SIGTERM")?;

        self.logger.info(
            "daemon_started",
            &[
                ("socket", &self.client.socket_path().display().to_string()),
                ("source", &self.source.describe()),
                ("ledger_records", &self.ledger.len().to_string()),
            ],
        );
        if let Err(error) = self.reconcile_pending().await {
            self.logger
                .error("reconcile_error", &[("error", &format!("{error:#}"))]);
        }

        loop {
            // `run_once` logs its own outcome, so the loop only needs to know
            // whether it was interrupted.
            tokio::select! {
                _ = interrupt.recv() => break,
                _ = terminate.recv() => break,
                _ = self.run_once() => {}
            }

            let interval = Duration::from_secs(self.config.cycle.interval_secs);
            tokio::select! {
                _ = interrupt.recv() => break,
                _ = terminate.recv() => break,
                () = sleep(interval) => {}
            }
        }

        self.logger.info("daemon_stopped", &[]);
        Ok(())
    }

    /// Runs one audit cycle.
    pub async fn run_once(&mut self) -> CycleOutcome {
        self.cycle_seq += 1;
        let cycle_id = format!(
            "c{}-{}",
            self.cycle_seq,
            unix_millis(std::time::SystemTime::now())
        );
        let outcome = self.run_cycle(&cycle_id).await;
        self.log_outcome_with_cycle(&outcome, &cycle_id);
        outcome
    }

    async fn run_cycle(&mut self, cycle_id: &str) -> CycleOutcome {
        self.logger.info("cycle_started", &[("cycle_id", cycle_id)]);

        let thread_id = match self.resolve_target(cycle_id).await {
            Ok(thread_id) => thread_id,
            Err(reason) => return CycleOutcome::Deferred(reason),
        };

        if let Err(error) = self.ensure_subscription(&thread_id, cycle_id).await {
            return CycleOutcome::Deferred(format!("watch failed: {error}"));
        }

        let status = match self.thread_status(&thread_id).await {
            Ok(status) => status,
            Err(reason) => return CycleOutcome::Deferred(reason),
        };
        self.log_thread_state(cycle_id, &thread_id, &status);

        // Anything the watch buffered since the last cycle belongs to an earlier
        // turn: it is stored, but must not be mistaken for the answer to the
        // message this cycle is about to send.
        let drained = self.drain_pending_events(cycle_id, &thread_id).await;
        if drained > 0 {
            self.logger.info(
                "events_drained",
                &[
                    ("cycle_id", cycle_id),
                    ("thread_id", &thread_id),
                    ("count", &drained.to_string()),
                ],
            );
        }

        if status.archived {
            return CycleOutcome::Deferred("thread is archived".to_string());
        }
        if status.state == ThreadState::AwaitingApproval {
            self.logger.warn(
                "approval_pending",
                &[("cycle_id", cycle_id), ("thread_id", &thread_id)],
            );
            return CycleOutcome::Deferred("thread is waiting for user approval".to_string());
        }

        let batch = match self.source.read_batch() {
            Ok(batch) => batch,
            Err(error) => {
                self.logger.error(
                    "findings_error",
                    &[("cycle_id", cycle_id), ("error", &format!("{error:#}"))],
                );
                return CycleOutcome::Failed(format!("reading findings failed: {error:#}"));
            }
        };
        if batch.is_empty() {
            self.logger.info(
                "cycle_idle",
                &[("cycle_id", cycle_id), ("reason", "no findings")],
            );
            return CycleOutcome::Idle("no findings".to_string());
        }
        self.logger.info(
            "findings_read",
            &[
                ("cycle_id", cycle_id),
                ("count", &batch.findings.len().to_string()),
                ("source", &self.source.describe()),
            ],
        );

        let outcome = self.deliver_batch(cycle_id, &thread_id, &batch).await;

        // The cursor moves only when the message actually reached the thread (or
        // was already there): a deferred or failed cycle keeps the findings, so
        // the next attempt reports them again instead of losing them.
        if matches!(outcome, CycleOutcome::Sent { .. } | CycleOutcome::Idle(_))
            && let Err(error) = self.source.commit(&batch.cursor)
        {
            self.logger.error(
                "findings_commit_error",
                &[("cycle_id", cycle_id), ("error", &format!("{error:#}"))],
            );
        }
        outcome
    }

    async fn deliver_batch(
        &mut self,
        cycle_id: &str,
        thread_id: &str,
        batch: &FindingBatch,
    ) -> CycleOutcome {
        let chunks = render_batches(&batch.findings, self.config.finding.max_message_bytes);
        let total = chunks.len();
        let mut last_request_id = String::new();
        let mut queued = false;
        let mut entries = 0usize;
        let mut text_bytes = 0usize;
        let mut sent_any = false;

        for (index, message) in chunks.into_iter().enumerate() {
            match self.send_with_retries(cycle_id, thread_id, &message).await {
                SendOutcome::Accepted {
                    request_id,
                    queued: was_queued,
                } => {
                    sent_any = true;
                    last_request_id = request_id.clone();
                    queued = was_queued;
                    self.logger.info(
                        "send_accepted",
                        &[
                            ("cycle_id", cycle_id),
                            ("thread_id", thread_id),
                            ("request_id", &request_id),
                            ("part", &format!("{}/{total}", index + 1)),
                            ("queued", if was_queued { "true" } else { "false" }),
                        ],
                    );

                    match self.await_turn(cycle_id, thread_id, &request_id).await {
                        TurnOutcome::Completed {
                            entries: captured,
                            text_bytes: text,
                        } => {
                            entries += captured;
                            text_bytes += text;
                        }
                        // The message is in the thread even when nothing came
                        // back in time, so the send still counts.
                        TurnOutcome::TimedOut => {}
                        TurnOutcome::Lost(reason) => return CycleOutcome::Deferred(reason),
                    }
                }
                SendOutcome::AlreadySent { request_id } => {
                    self.logger.info(
                        "send_suppressed",
                        &[
                            ("cycle_id", cycle_id),
                            ("thread_id", thread_id),
                            ("request_id", &request_id),
                            ("part", &format!("{}/{total}", index + 1)),
                        ],
                    );
                }
                SendOutcome::Blocked(reason) => return CycleOutcome::Deferred(reason),
                SendOutcome::Deferred(reason) => return CycleOutcome::Deferred(reason),
                SendOutcome::Failed(reason) => return CycleOutcome::Failed(reason),
            }
        }

        if sent_any {
            CycleOutcome::Sent {
                request_id: last_request_id,
                queued,
                entries,
                text_bytes,
            }
        } else {
            CycleOutcome::Idle("every message was already sent earlier".to_string())
        }
    }

    /// Sends one message, retrying only when retrying cannot duplicate it.
    async fn send_with_retries(
        &mut self,
        cycle_id: &str,
        thread_id: &str,
        message: &str,
    ) -> SendOutcome {
        let content_hash = Ledger::content_hash(message);

        if let Some(record) = self.ledger.find_blocking(thread_id, &content_hash) {
            return SendOutcome::AlreadySent {
                request_id: record.request_id.clone(),
            };
        }

        let mut attempt = self.ledger.next_attempt(thread_id, &content_hash);
        let mut backoff = self.config.cycle.backoff_initial_ms;
        let max_attempts = self.config.cycle.max_send_attempts;

        for _ in 0..max_attempts {
            let request_id = Ledger::request_id(thread_id, &content_hash, attempt);
            if let Err(error) = self.ledger.begin(
                &request_id,
                thread_id,
                &content_hash,
                attempt,
                cycle_id,
                message,
            ) {
                return SendOutcome::Failed(format!("writing the ledger failed: {error:#}"));
            }

            let response = self
                .client
                .send_message(&request_id, thread_id, message)
                .await;
            match response {
                Ok(response) => match response.status.clone() {
                    Status::Delivered | Status::Queued => {
                        let queued = response.status == Status::Queued;
                        let state = if queued {
                            LedgerState::Queued
                        } else {
                            LedgerState::Delivered
                        };
                        if let Err(error) =
                            self.ledger
                                .finish(&request_id, state, response.message_id, None)
                        {
                            return SendOutcome::Failed(format!(
                                "writing the ledger failed: {error:#}"
                            ));
                        }
                        return SendOutcome::Accepted { request_id, queued };
                    }
                    Status::ThreadNotFound => {
                        self.finish_rejected(&request_id, "thread_not_found");
                        return SendOutcome::Blocked("thread no longer exists".to_string());
                    }
                    Status::UnsupportedThreadType => {
                        self.finish_rejected(&request_id, "unsupported_thread_type");
                        return SendOutcome::Blocked(
                            "thread has no session and cannot receive messages".to_string(),
                        );
                    }
                    Status::ThreadUnavailable => {
                        self.finish_rejected(&request_id, "thread_unavailable");
                        return SendOutcome::Deferred(
                            "thread cannot be restored; open its project in Zed".to_string(),
                        );
                    }
                    Status::Rejected | Status::Error => {
                        let code = response
                            .error_code()
                            .cloned()
                            .unwrap_or(ErrorCode::Internal);
                        self.finish_rejected(&request_id, code.as_wire());
                        let action = code.recommended_action();
                        self.logger.warn(
                            "send_rejected",
                            &[
                                ("cycle_id", cycle_id),
                                ("thread_id", thread_id),
                                ("request_id", &request_id),
                                ("error_code", code.as_wire()),
                                ("attempt", &attempt.to_string()),
                                ("action", &format!("{action:?}")),
                            ],
                        );
                        match action {
                            RecommendedAction::AlreadySent => {
                                // The bridge already handled this id, so the
                                // message is in the thread. Record that and stop.
                                if let Err(error) = self.ledger.finish(
                                    &request_id,
                                    LedgerState::Delivered,
                                    None,
                                    None,
                                ) {
                                    return SendOutcome::Failed(format!(
                                        "writing the ledger failed: {error:#}"
                                    ));
                                }
                                return SendOutcome::Accepted {
                                    request_id,
                                    queued: false,
                                };
                            }
                            RecommendedAction::BackOff => {
                                // The bridge recorded this id, so a retry needs a
                                // new one or it would be a duplicate.
                                if code.is_recorded_by_bridge() {
                                    attempt += 1;
                                }
                                sleep(Duration::from_millis(backoff)).await;
                                backoff = (backoff * 2).min(self.config.cycle.backoff_max_ms);
                            }
                            RecommendedAction::WaitForApproval => {
                                if !self.wait_for_approval(cycle_id, thread_id).await {
                                    return SendOutcome::Deferred(
                                        "approval did not arrive in time".to_string(),
                                    );
                                }
                                if code.is_recorded_by_bridge() {
                                    attempt += 1;
                                }
                            }
                            RecommendedAction::Reselect => {
                                return SendOutcome::Blocked(
                                    "thread is archived; choose another one".to_string(),
                                );
                            }
                            RecommendedAction::OpenProject => {
                                return SendOutcome::Deferred(
                                    "thread cannot be restored; open its project in Zed"
                                        .to_string(),
                                );
                            }
                            RecommendedAction::Resend => {}
                            RecommendedAction::Stop
                            | RecommendedAction::Split
                            | RecommendedAction::UsePrompt
                            | RecommendedAction::FixSettings => {
                                return SendOutcome::Failed(format!(
                                    "bridge refused the request with {code}"
                                ));
                            }
                        }
                    }
                    Status::Accepted | Status::Event | Status::Other(_) => {
                        self.finish_rejected(&request_id, "unexpected_status");
                        return SendOutcome::Deferred(format!(
                            "unexpected send status {}",
                            response.status
                        ));
                    }
                },
                Err(error) => {
                    if error.never_reached_bridge() {
                        // The bytes never arrived, so the id is still unused.
                        self.finish_rejected(&request_id, "transport");
                        self.logger.warn(
                            "send_transport_error",
                            &[
                                ("cycle_id", cycle_id),
                                ("thread_id", thread_id),
                                ("request_id", &request_id),
                                ("error", &error.to_string()),
                            ],
                        );
                        sleep(Duration::from_millis(backoff)).await;
                        backoff = (backoff * 2).min(self.config.cycle.backoff_max_ms);
                    } else {
                        // The request may have been processed. Two deliveries are
                        // worse than one lost note, so it is never resent.
                        if let Err(ledger_error) = self.ledger.finish(
                            &request_id,
                            LedgerState::Unknown,
                            None,
                            Some("outcome_unknown".to_string()),
                        ) {
                            return SendOutcome::Failed(format!(
                                "writing the ledger failed: {ledger_error:#}"
                            ));
                        }
                        self.logger.warn(
                            "send_outcome_unknown",
                            &[
                                ("cycle_id", cycle_id),
                                ("thread_id", thread_id),
                                ("request_id", &request_id),
                                ("error", &error.to_string()),
                            ],
                        );
                        return SendOutcome::Deferred(format!("send outcome unknown: {error}"));
                    }
                }
            }
        }

        SendOutcome::Deferred(format!("gave up after {max_attempts} attempts"))
    }

    /// Polls the thread state until the user clears the approval prompt.
    async fn wait_for_approval(&mut self, cycle_id: &str, thread_id: &str) -> bool {
        let deadline =
            Instant::now() + Duration::from_secs(self.config.cycle.max_approval_wait_secs);
        let poll = Duration::from_millis(self.config.cycle.approval_poll_ms.max(1));

        while Instant::now() < deadline {
            sleep(poll).await;
            match self.thread_status(thread_id).await {
                Ok(status) if status.state != ThreadState::AwaitingApproval => {
                    self.logger.info(
                        "approval_cleared",
                        &[
                            ("cycle_id", cycle_id),
                            ("thread_id", thread_id),
                            ("state", status.state.as_wire()),
                        ],
                    );
                    return true;
                }
                Ok(status) => {
                    self.logger.debug(
                        "approval_wait",
                        &[("cycle_id", cycle_id), ("state", status.state.as_wire())],
                    );
                }
                Err(error) => {
                    self.logger
                        .warn("approval_poll_failed", &[("error", &error)]);
                }
            }
        }
        false
    }

    /// Moves events that arrived between cycles from the socket into the store.
    ///
    /// The subscription stays open across cycles, so a thread that changed while
    /// the daemon was idle has its events waiting. Reading them here keeps the
    /// event log complete and stops an old answer being mistaken for this turn's.
    async fn drain_pending_events(&mut self, cycle_id: &str, thread_id: &str) -> usize {
        let mut drained = 0usize;
        loop {
            let Some(mut subscription) = self.subscription.take() else {
                break;
            };
            // A zero-length wait still polls once, so buffered events come out
            // without ever blocking the cycle.
            let received = timeout(Duration::from_millis(1), subscription.next_event()).await;
            match received {
                Err(_) => {
                    self.subscription = Some(subscription);
                    break;
                }
                Ok(Ok(Some(event))) => {
                    self.subscription = Some(subscription);
                    drained += 1;
                    if let Err(error) = self.store.record_event(thread_id, &event) {
                        self.logger.warn(
                            "event_store_failed",
                            &[("cycle_id", cycle_id), ("error", &format!("{error:#}"))],
                        );
                    }
                }
                Ok(Ok(None)) | Ok(Err(_)) => {
                    // Closed or broken: the next ensure_subscription rebuilds it.
                    self.subscription = None;
                    break;
                }
            }
        }
        drained
    }

    /// Reads events until the turn the send started has finished.
    async fn await_turn(
        &mut self,
        cycle_id: &str,
        thread_id: &str,
        request_id: &str,
    ) -> TurnOutcome {
        let deadline = Instant::now() + Duration::from_secs(self.config.cycle.await_response_secs);
        let mut capture = TurnCapture::default();
        let mut seen_generating = false;
        let mut lost = 0u32;

        loop {
            if self.subscription.is_none() {
                if let Err(error) = self.ensure_subscription(thread_id, cycle_id).await {
                    return TurnOutcome::Lost(format!("watch failed: {error}"));
                }
                // The bridge never replays, so the current state is the only way
                // to know whether the turn already finished.
                if let Ok(status) = self.thread_status(thread_id).await {
                    if status.state == ThreadState::Generating {
                        seen_generating = true;
                    }
                    if is_finished_state(&status.state)
                        && (seen_generating || capture.saw_agent_text())
                    {
                        return self.complete_turn(thread_id, request_id, capture, None);
                    }
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                self.logger.warn(
                    "turn_timeout",
                    &[
                        ("cycle_id", cycle_id),
                        ("thread_id", thread_id),
                        ("request_id", request_id),
                    ],
                );
                return TurnOutcome::TimedOut;
            }

            // Taken out of `self` so the store and logger stay usable below.
            let mut subscription = match self.subscription.take() {
                Some(subscription) => subscription,
                None => continue,
            };
            let received = timeout(remaining, subscription.next_event()).await;

            match received {
                Err(_) => {
                    self.subscription = Some(subscription);
                    self.logger.warn(
                        "turn_timeout",
                        &[
                            ("cycle_id", cycle_id),
                            ("thread_id", thread_id),
                            ("request_id", request_id),
                        ],
                    );
                    return TurnOutcome::TimedOut;
                }
                Ok(Err(error)) => {
                    // The stream is broken; reconnect on the next pass.
                    self.subscription = None;
                    lost += 1;
                    if lost >= MAX_WATCH_RECONNECTS {
                        return TurnOutcome::Lost(format!(
                            "the watch kept failing ({lost} times): {error}"
                        ));
                    }
                    self.logger.warn(
                        "watch_error",
                        &[
                            ("cycle_id", cycle_id),
                            ("thread_id", thread_id),
                            ("error", &error.to_string()),
                            ("reconnects", &lost.to_string()),
                        ],
                    );
                    continue;
                }
                Ok(Ok(None)) => {
                    self.subscription = None;
                    lost += 1;
                    if lost >= MAX_WATCH_RECONNECTS {
                        return TurnOutcome::Lost(format!(
                            "the bridge closed the watch {lost} times"
                        ));
                    }
                    self.logger.info(
                        "watch_closed",
                        &[
                            ("cycle_id", cycle_id),
                            ("thread_id", thread_id),
                            ("reconnects", &lost.to_string()),
                        ],
                    );
                    continue;
                }
                Ok(Ok(Some(event))) => {
                    self.subscription = Some(subscription);
                    if let Err(error) = self.store.record_event(thread_id, &event) {
                        self.logger
                            .warn("event_store_failed", &[("error", &format!("{error:#}"))]);
                    }

                    match &event.kind {
                        EventKind::EntryAdded | EventKind::EntryUpdated => {
                            if let Some(entry) = &event.entry {
                                capture.observe(entry);
                            }
                        }
                        EventKind::EntryRemoved => {
                            if let Some(detail) = &event.detail
                                && let Some(index) = parse_removed_index(detail)
                            {
                                capture.remove(index);
                            }
                        }
                        EventKind::StatusChanged => {
                            if let Some(state) = &event.state {
                                if *state == ThreadState::Generating {
                                    seen_generating = true;
                                }
                                if *state == ThreadState::AwaitingApproval {
                                    self.logger.warn(
                                        "approval_pending",
                                        &[("cycle_id", cycle_id), ("thread_id", thread_id)],
                                    );
                                }
                                if is_finished_state(state)
                                    && (seen_generating || capture.saw_agent_text())
                                {
                                    let subscription = self.subscription.take();
                                    let outcome =
                                        self.complete_turn(thread_id, request_id, capture, None);
                                    self.subscription = subscription;
                                    return outcome;
                                }
                            }
                        }
                        EventKind::TurnStopped => {
                            let subscription = self.subscription.take();
                            let outcome = self.complete_turn(
                                thread_id,
                                request_id,
                                capture,
                                event.detail.clone(),
                            );
                            self.subscription = subscription;
                            return outcome;
                        }
                        EventKind::ToolAuthorizationRequested => {
                            self.logger.warn(
                                "approval_requested",
                                &[
                                    ("cycle_id", cycle_id),
                                    ("thread_id", thread_id),
                                    ("detail", event.detail.as_deref().unwrap_or("")),
                                ],
                            );
                        }
                        EventKind::ToolAuthorizationReceived => {
                            self.logger.info(
                                "approval_resolved",
                                &[("cycle_id", cycle_id), ("thread_id", thread_id)],
                            );
                        }
                        EventKind::TitleUpdated => {}
                        EventKind::Other(kind) => {
                            self.logger
                                .debug("event_ignored", &[("cycle_id", cycle_id), ("kind", kind)]);
                        }
                    }
                }
            }
        }
    }

    fn complete_turn(
        &mut self,
        thread_id: &str,
        request_id: &str,
        capture: TurnCapture,
        stopped: Option<String>,
    ) -> TurnOutcome {
        let entries = capture.indexes().len();
        let turn = capture.into_turn(thread_id, request_id, stopped.as_deref());
        let text_bytes = turn.text.len();
        match self.store.record_turn(&turn) {
            Ok(()) => TurnOutcome::Completed {
                entries,
                text_bytes,
            },
            Err(error) => TurnOutcome::Lost(format!("storing the turn failed: {error:#}")),
        }
    }

    async fn resolve_target(&mut self, cycle_id: &str) -> Result<String, String> {
        match self.config.target.select {
            TargetSelect::Explicit => self
                .config
                .target
                .thread_id
                .clone()
                .filter(|id| !id.trim().is_empty())
                .ok_or_else(|| "no target thread configured".to_string()),
            TargetSelect::Latest => {
                let request_id = format!("list-{cycle_id}");
                match self.client.list_threads(&request_id).await {
                    Ok(response) if response.is_rejection() => Err(format!(
                        "list_threads was refused with {}",
                        response
                            .error_code()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| response.status.to_string())
                    )),
                    Ok(response) => response
                        .threads
                        .unwrap_or_default()
                        .into_iter()
                        .find(|thread| !thread.archived && thread.state != ThreadState::Draft)
                        .map(|thread| thread.thread_id)
                        .ok_or_else(|| "no deliverable thread found".to_string()),
                    Err(error) => Err(format!("list_threads failed: {error}")),
                }
            }
        }
    }

    async fn ensure_subscription(&mut self, thread_id: &str, cycle_id: &str) -> Result<(), String> {
        if let Some(subscription) = &self.subscription
            && subscription.thread_id() == thread_id
        {
            return Ok(());
        }
        self.subscription = None;

        let short: String = thread_id
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .take(8)
            .collect();
        let watch_id = format!("watch-{short}-{cycle_id}");
        match self.client.watch(&watch_id, thread_id).await {
            Ok(subscription) => {
                self.logger.info(
                    "watch_started",
                    &[
                        ("cycle_id", cycle_id),
                        ("thread_id", thread_id),
                        ("watch_id", &watch_id),
                    ],
                );
                self.subscription = Some(subscription);
                Ok(())
            }
            Err(error) => Err(error.to_string()),
        }
    }

    async fn thread_status(&mut self, thread_id: &str) -> Result<ThreadStatusInfo, String> {
        let request_id = format!("status-{}", unix_millis(std::time::SystemTime::now()));
        match self.client.thread_status(&request_id, thread_id).await {
            Ok(response) => match response.thread {
                Some(status) if status.found => Ok(status),
                _ => Err("thread not found".to_string()),
            },
            Err(error) => Err(format!("get_thread_status failed: {error}")),
        }
    }

    fn finish_rejected(&mut self, request_id: &str, error_code: &str) {
        if let Err(error) = self.ledger.finish(
            request_id,
            LedgerState::Rejected,
            None,
            Some(error_code.to_string()),
        ) {
            self.logger
                .error("ledger_write_failed", &[("error", &format!("{error:#}"))]);
        }
    }

    fn log_thread_state(&self, cycle_id: &str, thread_id: &str, status: &ThreadStatusInfo) {
        self.logger.info(
            "thread_state",
            &[
                ("cycle_id", cycle_id),
                ("thread_id", thread_id),
                ("state", status.state.as_wire()),
                (
                    "deliverable",
                    if status.deliverable { "true" } else { "false" },
                ),
                ("loaded", if status.loaded { "true" } else { "false" }),
            ],
        );
    }

    fn log_outcome_with_cycle(&self, outcome: &CycleOutcome, cycle_id: &str) {
        let detail = outcome.summary();
        let fields: &[(&str, &str)] = &[("cycle_id", cycle_id), ("outcome", &detail)];
        match outcome {
            CycleOutcome::Idle(_) => self.logger.info("cycle_finished", fields),
            CycleOutcome::Sent { .. } => self.logger.info("cycle_finished", fields),
            CycleOutcome::Deferred(_) => self.logger.info("cycle_finished", fields),
            CycleOutcome::Failed(_) => self.logger.error("cycle_finished", fields),
        }
    }
}

/// Builds the configured finding source.
fn build_source(
    config: &Config,
    home: &std::path::Path,
    storage: &std::path::Path,
) -> Result<Box<dyn FindingSource>> {
    match config.finding.source {
        crate::config::FindingSourceKind::File => {
            let path = config
                .finding_file(home)
                .ok_or_else(|| anyhow::anyhow!("finding.file is not set"))?;
            Ok(Box::new(FileFindingSource::new(
                path,
                storage.join(FINDINGS_CURSOR_FILE),
            )?))
        }
        crate::config::FindingSourceKind::Command => {
            let command = config
                .finding
                .command
                .clone()
                .ok_or_else(|| anyhow::anyhow!("finding.command is not set"))?;
            Ok(Box::new(CommandFindingSource::new(
                command,
                config.finding.args.clone(),
            )))
        }
    }
}

/// Parses `"start..end"` from an `entry_removed` event.
fn parse_removed_index(detail: &str) -> Option<u64> {
    detail.split("..").next()?.trim().parse().ok()
}
