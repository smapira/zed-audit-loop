//! Persistence for what the bridge streams.
//!
//! `watch_thread` never replays the past, so anything not written down while it
//! arrives is gone. Two files are kept per thread: every event, and one line per
//! completed turn with the agent's answer text.

use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};

use crate::protocol::{EntrySummary, ThreadEvent, ThreadState};
use crate::time::now_rfc3339;

/// Subdirectory holding raw event streams.
pub const EVENTS_DIR: &str = "events";
/// Subdirectory holding one record per completed turn.
pub const TURNS_DIR: &str = "turns";

/// One persisted event line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredEvent {
    pub recorded_at: String,
    pub thread_id: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_text: Option<String>,
}

/// One persisted turn: the message that started it and the answer it produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredTurn {
    pub recorded_at: String,
    pub thread_id: String,
    /// The `request_id` of the `send_message` that started the turn.
    pub request_id: String,
    /// Why the turn ended, when the bridge said.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped: Option<String>,
    /// The agent's text, concatenated across entries by index.
    pub text: String,
    /// Entries the answer is made of, in order.
    #[serde(default)]
    pub entry_indexes: Vec<u64>,
}

/// The per-thread event and turn files.
pub struct Store {
    events_dir: PathBuf,
    turns_dir: PathBuf,
}

impl Store {
    /// Creates (or opens) the event and turn directories under `directory`.
    pub fn open(directory: &Path) -> Result<Self> {
        let events_dir = directory.join(EVENTS_DIR);
        let turns_dir = directory.join(TURNS_DIR);
        for dir in [&events_dir, &turns_dir] {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("creating storage directory {}", dir.display()))?;
        }
        Ok(Self {
            events_dir,
            turns_dir,
        })
    }

    /// The event file for one thread.
    pub fn event_path(&self, thread_id: &str) -> PathBuf {
        self.events_dir
            .join(format!("{}.jsonl", sanitize(thread_id)))
    }

    /// The turn file for one thread.
    pub fn turn_path(&self, thread_id: &str) -> PathBuf {
        self.turns_dir
            .join(format!("{}.jsonl", sanitize(thread_id)))
    }

    /// Appends one event.
    pub fn record_event(&self, thread_id: &str, event: &ThreadEvent) -> Result<()> {
        let entry: Option<&EntrySummary> = event.entry.as_ref();
        let stored = StoredEvent {
            recorded_at: now_rfc3339(),
            thread_id: thread_id.to_string(),
            kind: event.kind.as_wire().to_string(),
            state: event
                .state
                .as_ref()
                .map(|state| state.as_wire().to_string()),
            detail: event.detail.clone(),
            entry_index: entry.map(|entry| entry.index),
            entry_kind: entry.map(|entry| entry.kind.clone()),
            entry_text: entry.and_then(|entry| entry.text.clone()),
        };
        self.append(&self.event_path(thread_id), &stored)
    }

    /// Appends one completed turn.
    pub fn record_turn(&self, turn: &StoredTurn) -> Result<()> {
        self.append(&self.turn_path(&turn.thread_id), turn)
    }

    fn append<T: Serialize>(&self, path: &Path, value: &T) -> Result<()> {
        let line = serde_json::to_string(value).context("encoding a storage record")?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("opening {}", path.display()))?;
        writeln!(file, "{line}").with_context(|| format!("writing {}", path.display()))?;
        file.flush()
            .with_context(|| format!("flushing {}", path.display()))?;
        file.sync_data()
            .with_context(|| format!("syncing {}", path.display()))?;
        Ok(())
    }
}

/// Collects the agent's text of one turn as events arrive.
///
/// Only agent entries are kept: the `user_message` echo of what the daemon sent
/// carries no answer, and the raw event file already has it.
///
/// `entry_updated` replaces the text of its index, which is how the bridge
/// reports a message that grew while it streamed.
#[derive(Debug, Default)]
pub struct TurnCapture {
    entries: std::collections::BTreeMap<u64, String>,
}

impl TurnCapture {
    /// Applies one entry to the capture.
    pub fn observe(&mut self, entry: &EntrySummary) {
        if !entry.is_agent_text() {
            return;
        }
        let Some(text) = &entry.text else {
            return;
        };
        self.entries.insert(entry.index, text.clone());
    }

    /// Removes an entry the bridge dropped.
    pub fn remove(&mut self, index: u64) {
        self.entries.remove(&index);
    }

    /// Whether the agent produced any text at all.
    pub fn saw_agent_text(&self) -> bool {
        !self.entries.is_empty()
    }

    /// The answer text, concatenated in entry order.
    pub fn text(&self) -> String {
        let mut text = String::new();
        for value in self.entries.values() {
            if !text.is_empty() && !text.ends_with('\n') {
                text.push('\n');
            }
            text.push_str(value);
        }
        text
    }

    /// The entry indexes the answer is made of.
    pub fn indexes(&self) -> Vec<u64> {
        self.entries.keys().copied().collect()
    }

    /// Turns the capture into a persistable record.
    pub fn into_turn(self, thread_id: &str, request_id: &str, stopped: Option<&str>) -> StoredTurn {
        StoredTurn {
            recorded_at: now_rfc3339(),
            thread_id: thread_id.to_string(),
            request_id: request_id.to_string(),
            stopped: stopped.map(ToString::to_string),
            text: self.text(),
            entry_indexes: self.indexes(),
        }
    }
}

/// Whether a state means "the agent finished".
///
/// `awaiting_approval` also means the turn is paused, not finished, so it is
/// excluded deliberately.
pub fn is_finished_state(state: &ThreadState) -> bool {
    matches!(state, ThreadState::Idle)
}

/// Keeps a thread id usable as a file name.
fn sanitize(thread_id: &str) -> String {
    let mut name: String = thread_id
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect();
    if name.is_empty() {
        name.push_str("thread");
    }
    name
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_are_appended_per_thread() {
        let directory = tempfile::tempdir().expect("temp dir");
        let store = Store::open(directory.path()).expect("open store");
        let event = ThreadEvent {
            watch_id: "watch-1".into(),
            thread_id: "thread-a".into(),
            kind: crate::protocol::EventKind::EntryAdded,
            entry: Some(EntrySummary {
                index: 7,
                kind: "assistant_message".into(),
                text: Some("done".into()),
            }),
            state: None,
            detail: None,
        };

        store.record_event("thread-a", &event).expect("record");
        store.record_event("thread-a", &event).expect("record");

        let contents = std::fs::read_to_string(store.event_path("thread-a")).expect("read");
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        let first: StoredEvent = serde_json::from_str(lines[0]).expect("decode");
        assert_eq!(first.kind, "entry_added");
        assert_eq!(first.entry_index, Some(7));
        assert_eq!(first.entry_text.as_deref(), Some("done"));
    }

    #[test]
    fn thread_ids_become_safe_file_names() {
        let directory = tempfile::tempdir().expect("temp dir");
        let store = Store::open(directory.path()).expect("open store");

        let path = store.event_path("../escape/me");
        let name = path.file_name().expect("file name").to_string_lossy();
        assert!(name.ends_with(".jsonl"));
        assert!(
            !name.contains('/'),
            "a thread id must not escape the directory: {name}"
        );
        assert!(
            !name.contains(".."),
            "a thread id must not traverse: {name}"
        );
    }

    #[test]
    fn a_turn_concatenates_entries_in_index_order() {
        let mut capture = TurnCapture::default();
        capture.observe(&EntrySummary {
            index: 9,
            kind: "assistant_message".into(),
            text: Some("second".into()),
        });
        capture.observe(&EntrySummary {
            index: 8,
            kind: "assistant_message".into(),
            text: Some("first".into()),
        });
        capture.observe(&EntrySummary {
            index: 10,
            kind: "tool_call".into(),
            text: None,
        });

        assert!(capture.saw_agent_text());
        assert_eq!(capture.text(), "first\nsecond");
        assert_eq!(capture.indexes(), vec![8, 9]);
    }

    #[test]
    fn an_updated_entry_replaces_its_text() {
        let mut capture = TurnCapture::default();
        capture.observe(&EntrySummary {
            index: 3,
            kind: "assistant_message".into(),
            text: Some("partial".into()),
        });
        capture.observe(&EntrySummary {
            index: 3,
            kind: "assistant_message".into(),
            text: Some("complete".into()),
        });
        assert_eq!(capture.text(), "complete");

        capture.remove(3);
        assert_eq!(capture.text(), "");
    }

    #[test]
    fn a_user_message_echo_is_not_part_of_the_answer() {
        let mut capture = TurnCapture::default();
        capture.observe(&EntrySummary {
            index: 1,
            kind: "user_message".into(),
            text: Some("audit please".into()),
        });
        capture.observe(&EntrySummary {
            index: 2,
            kind: "assistant_message".into(),
            text: Some("the answer".into()),
        });

        assert!(capture.saw_agent_text());
        assert_eq!(capture.text(), "the answer");
        assert_eq!(capture.indexes(), vec![2]);
    }

    #[test]
    fn only_idle_finishes_a_turn() {
        assert!(is_finished_state(&ThreadState::Idle));
        assert!(!is_finished_state(&ThreadState::AwaitingApproval));
        assert!(!is_finished_state(&ThreadState::Generating));
    }
}
