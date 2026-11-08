//! One JSON object per line, so every record can be grepped by `request_id`.

use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::Path;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::time::now_rfc3339;

/// Log severity, ordered so a configured level filters out lower ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    /// Per-event detail.
    Debug,
    /// Cycle milestones.
    Info,
    /// Recoverable trouble worth reading.
    Warn,
    /// Something the operator has to fix.
    Error,
}

impl Level {
    /// The `level` field value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

/// A structured logger writing JSON lines to stderr and, optionally, a file.
#[derive(Debug)]
pub struct Logger {
    level: Level,
    file: Option<Mutex<File>>,
    stderr: bool,
}

impl Logger {
    /// A logger that only writes to stderr.
    pub fn stderr(level: Level) -> Self {
        Self {
            level,
            file: None,
            stderr: true,
        }
    }

    /// A logger that discards everything, for tests that only assert on files.
    pub fn silent() -> Self {
        Self {
            level: Level::Error,
            file: None,
            stderr: false,
        }
    }

    /// Adds a log file. The file is created if missing and appended to
    /// otherwise.
    pub fn with_file(mut self, path: &Path) -> std::io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        self.file = Some(Mutex::new(file));
        Ok(self)
    }

    /// Stops or starts the stderr copy.
    pub fn with_stderr(mut self, enabled: bool) -> Self {
        self.stderr = enabled;
        self
    }

    /// Drops stderr output, which keeps daemon tests readable.
    pub fn quiet(mut self) -> Self {
        self.stderr = false;
        self
    }

    /// The configured level.
    pub fn level(&self) -> Level {
        self.level
    }

    /// Whether a record at this level would be written.
    pub fn enabled(&self, level: Level) -> bool {
        level >= self.level
    }

    /// Writes one record.
    ///
    /// Fields are added verbatim, so callers can pass `("request_id", &id)`.
    pub fn record(&self, level: Level, event: &str, fields: &[(&str, &str)]) {
        if !self.enabled(level) {
            return;
        }

        let mut object = serde_json::Map::new();
        object.insert("ts".to_string(), serde_json::Value::from(now_rfc3339()));
        object.insert("level".to_string(), serde_json::Value::from(level.as_str()));
        object.insert("event".to_string(), serde_json::Value::from(event));
        for (key, value) in fields {
            object.insert((*key).to_string(), serde_json::Value::from(*value));
        }
        let Ok(line) = serde_json::to_string(&object) else {
            return;
        };

        if let Some(file) = &self.file
            && let Ok(mut file) = file.lock()
        {
            let _ = writeln!(file, "{line}");
            let _ = file.flush();
        }
        if self.stderr {
            eprintln!("{line}");
        }
    }

    /// A debug record.
    pub fn debug(&self, event: &str, fields: &[(&str, &str)]) {
        self.record(Level::Debug, event, fields);
    }

    /// An info record.
    pub fn info(&self, event: &str, fields: &[(&str, &str)]) {
        self.record(Level::Info, event, fields);
    }

    /// A warning record.
    pub fn warn(&self, event: &str, fields: &[(&str, &str)]) {
        self.record(Level::Warn, event, fields);
    }

    /// An error record.
    pub fn error(&self, event: &str, fields: &[(&str, &str)]) {
        self.record(Level::Error, event, fields);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_one_json_object_per_record() {
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("daemon.jsonl");
        let logger = Logger::stderr(Level::Debug)
            .with_file(&path)
            .expect("log file")
            .quiet();

        logger.info("cycle_started", &[("cycle_id", "7")]);
        logger.debug("skipped", &[]);

        let contents = std::fs::read_to_string(&path).expect("read log");
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);

        let first: serde_json::Value = serde_json::from_str(lines[0]).expect("json record");
        assert_eq!(first["event"], "cycle_started");
        assert_eq!(first["level"], "info");
        assert_eq!(first["cycle_id"], "7");
        assert!(first["ts"].as_str().is_some_and(|ts| ts.ends_with('Z')));
    }

    #[test]
    fn level_filters_lower_records() {
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("daemon.jsonl");
        let logger = Logger::stderr(Level::Warn)
            .with_file(&path)
            .expect("log file")
            .quiet();

        logger.info("hidden", &[]);
        logger.warn("shown", &[]);

        let contents = std::fs::read_to_string(&path).expect("read log");
        assert!(!contents.contains("hidden"));
        assert!(contents.contains("shown"));
    }
}
