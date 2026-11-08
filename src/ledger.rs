//! The daemon's own record of what it sent.
//!
//! The bridge keeps its delivery ledger in memory only, so `duplicate_request`
//! cannot be relied on across a Zed restart. This ledger is therefore the only
//! guarantee against sending the same audit message twice: a message is
//! identified by its content, written here before it is sent, and never sent
//! again while a record for that content says it was accepted.

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};

use crate::time::now_rfc3339;

/// File name of the append-only ledger inside the storage directory.
pub const LEDGER_FILE: &str = "ledger.jsonl";

/// What happened to one send attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LedgerState {
    /// Written down before the request went out; no answer was recorded.
    Sending,
    /// The bridge queued the message behind the running turn.
    Queued,
    /// The bridge put the message into the thread history.
    Delivered,
    /// The bridge refused. `error_code` says why.
    Rejected,
    /// The answer was lost (timeout, disconnect). Treated as "possibly
    /// delivered" and never retried, so the daemon cannot double-deliver.
    Unknown,
}

impl LedgerState {
    /// Whether a record in this state forbids sending the same content again.
    pub fn blocks_resend(self) -> bool {
        matches!(
            self,
            Self::Sending | Self::Queued | Self::Delivered | Self::Unknown
        )
    }
}

/// One ledger line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerRecord {
    /// The `request_id` used on the wire.
    pub request_id: String,
    pub thread_id: String,
    /// Hash of the message body, so identical content is recognised.
    pub content_hash: String,
    /// Retry counter; only grows for rejections the bridge records.
    pub attempt: u32,
    /// Which daemon cycle produced it, for log correlation.
    pub cycle_id: String,
    pub state: LedgerState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    /// The message body, so an operator can see what was actually sent. Only the
    /// `begin` line of an attempt carries it; state changes leave it out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default)]
    pub message_bytes: usize,
    pub recorded_at: String,
}

/// An append-only ledger, loaded into memory at start-up.
pub struct Ledger {
    path: PathBuf,
    records: BTreeMap<String, LedgerRecord>,
    skipped_lines: usize,
    /// A crash can leave the file without a final newline. The next record has to
    /// start on a line of its own, or it would merge into the torn one and be
    /// lost on the next start - exactly the record write-ahead exists to keep.
    needs_newline: bool,
}

impl Ledger {
    /// Opens the ledger in `directory`, creating it when missing.
    pub fn open(directory: &Path) -> Result<Self> {
        std::fs::create_dir_all(directory)
            .with_context(|| format!("creating storage directory {}", directory.display()))?;
        let path = directory.join(LEDGER_FILE);

        let mut records = BTreeMap::new();
        let mut skipped_lines = 0;
        let mut needs_newline = false;
        match std::fs::read_to_string(&path) {
            Ok(text) => {
                needs_newline = !text.is_empty() && !text.ends_with('\n');
                for line in text.lines() {
                    if line.trim().is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<LedgerRecord>(line) {
                        Ok(record) => {
                            records.insert(record.request_id.clone(), record);
                        }
                        // A torn line from a crash must not stop the daemon.
                        Err(_) => skipped_lines += 1,
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| format!("reading {}", path.display()));
            }
        }

        Ok(Self {
            path,
            records,
            skipped_lines,
            needs_newline,
        })
    }

    /// Lines dropped while loading because they did not parse.
    pub fn skipped_lines(&self) -> usize {
        self.skipped_lines
    }

    /// How many requests the ledger knows about.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the ledger is empty.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// A stable hash of a message body.
    ///
    /// FNV-1a is used instead of `DefaultHasher` because the value must survive
    /// a restart and a toolchain upgrade: it is what makes a re-derived message
    /// recognisable as the same message.
    pub fn content_hash(message: &str) -> String {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in message.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        format!("{hash:016x}")
    }

    /// The `request_id` for one attempt.
    ///
    /// Deriving it from the content keeps it reproducible: a cycle re-run after a
    /// crash produces the same id, which the ledger then suppresses.
    pub fn request_id(thread_id: &str, content_hash: &str, attempt: u32) -> String {
        let thread: String = thread_id
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .take(8)
            .collect();
        let thread = if thread.is_empty() {
            "thread".to_string()
        } else {
            thread
        };
        let short_hash: String = content_hash.chars().take(12).collect();
        format!("audit-{thread}-{short_hash}-{attempt}")
    }

    /// Looks a request up by id.
    pub fn get(&self, request_id: &str) -> Option<&LedgerRecord> {
        self.records.get(request_id)
    }

    /// The highest attempt recorded for this content, if any.
    pub fn last_attempt(&self, thread_id: &str, content_hash: &str) -> Option<u32> {
        self.records
            .values()
            .filter(|record| record.thread_id == thread_id && record.content_hash == content_hash)
            .map(|record| record.attempt)
            .max()
    }

    /// The attempt number the next send of this content must use.
    ///
    /// A rejection the bridge records poisons its `request_id`, so the retry
    /// steps to a fresh one. A rejection the bridge does not record (an unloaded
    /// thread, a setting, a transport failure) leaves the id reusable, which
    /// keeps one logical message on one id for as long as possible.
    pub fn next_attempt(&self, thread_id: &str, content_hash: &str) -> u32 {
        let attempts: Vec<&LedgerRecord> = self
            .records
            .values()
            .filter(|record| record.thread_id == thread_id && record.content_hash == content_hash)
            .collect();

        let after_poisoned = attempts
            .iter()
            .filter(|record| is_recorded_rejection(record))
            .map(|record| record.attempt + 1)
            .max();
        match after_poisoned {
            Some(attempt) => attempt,
            None => attempts
                .iter()
                .map(|record| record.attempt)
                .max()
                .unwrap_or(0),
        }
    }

    /// A record for this content that forbids sending it again.
    pub fn find_blocking(&self, thread_id: &str, content_hash: &str) -> Option<&LedgerRecord> {
        self.records
            .values()
            .filter(|record| record.thread_id == thread_id && record.content_hash == content_hash)
            .find(|record| record.state.blocks_resend())
    }

    /// Every attempt made for this content, oldest first.
    pub fn attempts_for(&self, thread_id: &str, content_hash: &str) -> Vec<&LedgerRecord> {
        let mut records: Vec<&LedgerRecord> = self
            .records
            .values()
            .filter(|record| record.thread_id == thread_id && record.content_hash == content_hash)
            .collect();
        records.sort_by_key(|record| record.attempt);
        records
    }

    /// Records that were written before sending and never answered, which means
    /// a crash happened in between.
    pub fn unanswered(&self) -> Vec<&LedgerRecord> {
        self.records
            .values()
            .filter(|record| record.state == LedgerState::Sending)
            .collect()
    }

    /// Writes down a send *before* it goes out, so a crash cannot hide it.
    pub fn begin(
        &mut self,
        request_id: &str,
        thread_id: &str,
        content_hash: &str,
        attempt: u32,
        cycle_id: &str,
        message: &str,
    ) -> Result<()> {
        if let Some(existing) = self.records.get(request_id)
            && (existing.thread_id != thread_id
                || existing.content_hash != content_hash
                || existing.attempt != attempt)
        {
            // The id is derived from (thread, content, attempt); a different
            // record under the same id means the derivation collided, and
            // overwriting would silently drop a blocking record.
            anyhow::bail!("request id {request_id} is already used by another message");
        }

        let record = LedgerRecord {
            request_id: request_id.to_string(),
            thread_id: thread_id.to_string(),
            content_hash: content_hash.to_string(),
            attempt,
            cycle_id: cycle_id.to_string(),
            state: LedgerState::Sending,
            message_id: None,
            error_code: None,
            message: Some(message.to_string()),
            message_bytes: message.len(),
            recorded_at: now_rfc3339(),
        };
        self.persist(&record)?;
        self.records.insert(record.request_id.clone(), record);
        Ok(())
    }

    /// Records the outcome of a send.
    pub fn finish(
        &mut self,
        request_id: &str,
        state: LedgerState,
        message_id: Option<String>,
        error_code: Option<String>,
    ) -> Result<()> {
        let Some(existing) = self.records.get(request_id).cloned() else {
            anyhow::bail!("no ledger record for request {request_id}");
        };
        let record = LedgerRecord {
            state,
            message_id,
            error_code,
            recorded_at: now_rfc3339(),
            // The body is already on the `begin` line; repeating it on every
            // state change would double the ledger for no extra information.
            message: None,
            ..existing
        };
        self.persist(&record)?;
        self.records.insert(record.request_id.clone(), record);
        Ok(())
    }

    fn persist(&mut self, record: &LedgerRecord) -> Result<()> {
        let line = serde_json::to_string(record).context("encoding a ledger record")?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .with_context(|| format!("opening ledger {}", self.path.display()))?;
        if self.needs_newline {
            // Start on a fresh line so this record is not glued to a torn one.
            file.write_all(b"\n")
                .with_context(|| format!("repairing {}", self.path.display()))?;
        }
        writeln!(file, "{line}").with_context(|| format!("writing {}", self.path.display()))?;
        file.flush()
            .with_context(|| format!("flushing {}", self.path.display()))?;
        // The ledger is the only protection against double delivery, so it has
        // to reach the disk before the request does.
        file.sync_data()
            .with_context(|| format!("syncing {}", self.path.display()))?;
        self.needs_newline = false;
        Ok(())
    }
}

fn is_recorded_rejection(record: &LedgerRecord) -> bool {
    record
        .error_code
        .as_deref()
        .map(|code| crate::protocol::ErrorCode::from_wire(code).is_recorded_by_bridge())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ledger() -> (tempfile::TempDir, Ledger) {
        let directory = tempfile::tempdir().expect("temp dir");
        let ledger = Ledger::open(directory.path()).expect("open ledger");
        (directory, ledger)
    }

    #[test]
    fn content_hash_is_stable_and_distinguishes_messages() {
        assert_eq!(Ledger::content_hash("abc"), Ledger::content_hash("abc"));
        assert_ne!(Ledger::content_hash("abc"), Ledger::content_hash("abd"));
        assert_eq!(Ledger::content_hash("abc").len(), 16);
    }

    #[test]
    fn request_ids_are_readable_and_attempt_scoped() {
        let hash = Ledger::content_hash("message");
        let first = Ledger::request_id("6f1c2d3e-4b5a-4c7d-8e9f-0a1b2c3d4e5f", &hash, 0);
        let second = Ledger::request_id("6f1c2d3e-4b5a-4c7d-8e9f-0a1b2c3d4e5f", &hash, 1);

        assert!(first.starts_with("audit-6f1c2d3e-"));
        assert!(first.ends_with("-0"));
        assert_ne!(first, second);
        // A thread id without alphanumerics still yields a usable id.
        let empty = Ledger::request_id("---", &hash, 0);
        assert!(empty.starts_with("audit-thread-"));
    }

    #[test]
    fn a_record_survives_a_restart() {
        let (directory, mut ledger) = ledger();
        let hash = Ledger::content_hash("message");
        let request_id = Ledger::request_id("thread-a", &hash, 0);

        ledger
            .begin(&request_id, "thread-a", &hash, 0, "cycle-1", "message")
            .expect("begin");
        ledger
            .finish(&request_id, LedgerState::Delivered, Some("12".into()), None)
            .expect("finish");
        drop(ledger);

        let reloaded = Ledger::open(directory.path()).expect("reopen");
        let record = reloaded.get(&request_id).expect("record present");
        assert_eq!(record.state, LedgerState::Delivered);
        assert_eq!(record.message_id.as_deref(), Some("12"));
        assert_eq!(record.message_bytes, "message".len());
        // The body is on the write-ahead line, which is what an operator reads.
        let contents =
            std::fs::read_to_string(directory.path().join(LEDGER_FILE)).expect("read ledger");
        assert!(contents.contains("\"message\":\"message\""));
        assert!(reloaded.find_blocking("thread-a", &hash).is_some());
    }

    #[test]
    fn a_rejected_attempt_does_not_block_a_retry() {
        let (_directory, mut ledger) = ledger();
        let hash = Ledger::content_hash("message");
        let request_id = Ledger::request_id("thread-a", &hash, 0);

        ledger
            .begin(&request_id, "thread-a", &hash, 0, "cycle-1", "message")
            .expect("begin");
        ledger
            .finish(
                &request_id,
                LedgerState::Rejected,
                None,
                Some("queue_full".into()),
            )
            .expect("finish");

        assert!(ledger.find_blocking("thread-a", &hash).is_none());
        assert_eq!(ledger.last_attempt("thread-a", &hash), Some(0));
    }

    #[test]
    fn an_unknown_outcome_blocks_a_resend() {
        let (_directory, mut ledger) = ledger();
        let hash = Ledger::content_hash("message");
        let request_id = Ledger::request_id("thread-a", &hash, 0);

        ledger
            .begin(&request_id, "thread-a", &hash, 0, "cycle-1", "message")
            .expect("begin");
        ledger
            .finish(&request_id, LedgerState::Unknown, None, None)
            .expect("finish");

        assert!(ledger.find_blocking("thread-a", &hash).is_some());
        assert_eq!(ledger.unanswered().len(), 0);
    }

    #[test]
    fn a_crash_after_beginning_leaves_an_unanswered_record() {
        let (_directory, mut ledger) = ledger();
        let hash = Ledger::content_hash("message");
        let request_id = Ledger::request_id("thread-a", &hash, 0);

        ledger
            .begin(&request_id, "thread-a", &hash, 0, "cycle-1", "message")
            .expect("begin");

        assert_eq!(ledger.unanswered().len(), 1);
        assert!(ledger.find_blocking("thread-a", &hash).is_some());
    }

    #[test]
    fn a_torn_line_is_skipped_not_fatal() {
        let directory = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            directory.path().join(LEDGER_FILE),
            "{\"request_id\":\"a\",\"torn\n{\"request_id\":\"b\",\"thread_id\":\"t\",\"content_hash\":\"h\",\"attempt\":0,\"cycle_id\":\"c\",\"state\":\"delivered\",\"recorded_at\":\"now\"}\n",
        )
        .expect("write ledger file");

        let ledger = Ledger::open(directory.path()).expect("open");
        assert_eq!(ledger.skipped_lines(), 1);
        assert_eq!(ledger.len(), 1);
    }

    #[test]
    fn a_torn_tail_does_not_swallow_the_next_record() {
        let directory = tempfile::tempdir().expect("temp dir");
        std::fs::write(directory.path().join(LEDGER_FILE), "{\"request_id\":\"torn")
            .expect("write a torn tail");

        let mut ledger = Ledger::open(directory.path()).expect("open");
        assert_eq!(ledger.skipped_lines(), 1);
        let hash = Ledger::content_hash("message");
        let request_id = Ledger::request_id("thread-a", &hash, 0);
        ledger
            .begin(&request_id, "thread-a", &hash, 0, "cycle-1", "message")
            .expect("begin");
        drop(ledger);

        let reloaded = Ledger::open(directory.path()).expect("reopen");
        let record = reloaded.get(&request_id).expect("record survived");
        assert_eq!(record.state, LedgerState::Sending);
    }

    #[test]
    fn reusing_an_id_for_other_content_is_refused() {
        let (_directory, mut ledger) = ledger();
        let hash = Ledger::content_hash("message");
        let request_id = Ledger::request_id("thread-a", &hash, 0);
        ledger
            .begin(&request_id, "thread-a", &hash, 0, "cycle-1", "message")
            .expect("begin");

        let error = ledger
            .begin(&request_id, "thread-b", &hash, 0, "cycle-2", "message")
            .expect_err("must refuse a colliding id");

        assert!(format!("{error:#}").contains("already used"));
    }

    #[test]
    fn attempts_are_listed_for_one_content_hash_only() {
        let (_directory, mut ledger) = ledger();
        let hash = Ledger::content_hash("message");
        for attempt in 0..2 {
            let request_id = Ledger::request_id("thread-a", &hash, attempt);
            ledger
                .begin(&request_id, "thread-a", &hash, attempt, "c", "message")
                .expect("begin");
        }
        let other = Ledger::request_id("thread-a", &Ledger::content_hash("other"), 0);
        ledger
            .begin(
                &other,
                "thread-a",
                &Ledger::content_hash("other"),
                0,
                "c",
                "other",
            )
            .expect("begin");

        assert_eq!(ledger.attempts_for("thread-a", &hash).len(), 2);
    }
}
