//! The daemon configuration file.
//!
//! Every section and every field has a default, so a config only names what the
//! operator wants to change. Unknown keys are an error: a typo in a long-running
//! daemon is otherwise silent.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};

use crate::logging::Level;
use crate::protocol::ClientConfig;

/// The default Zed release channel whose socket the daemon talks to.
const DEFAULT_CHANNEL: &str = "stable";

/// The whole configuration.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// How to reach the bridge.
    pub bridge: BridgeConfig,
    /// Which thread to audit.
    pub target: TargetConfig,
    /// Timing of one audit cycle.
    pub cycle: CycleConfig,
    /// Where the daemon keeps its state.
    pub storage: StorageConfig,
    /// Where findings come from.
    pub finding: FindingConfig,
    /// Logging.
    pub log: LogConfig,
}

/// How to reach the bridge socket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BridgeConfig {
    /// Zed release channel: `stable`, `preview`, `nightly`, or `dev`.
    pub channel: String,
    /// An explicit socket path, which wins over `channel`.
    pub socket: Option<PathBuf>,
    /// Connection timeout in milliseconds.
    pub connect_timeout_ms: u64,
    /// One request/response exchange timeout in milliseconds.
    pub request_timeout_ms: u64,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            channel: DEFAULT_CHANNEL.to_string(),
            socket: None,
            connect_timeout_ms: 3_000,
            request_timeout_ms: 20_000,
        }
    }
}

/// How the audited thread is chosen.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TargetConfig {
    /// An explicit thread id (a hyphenated UUID).
    pub thread_id: Option<String>,
    /// How to choose when `thread_id` is absent.
    pub select: TargetSelect,
}

impl Default for TargetConfig {
    fn default() -> Self {
        Self {
            thread_id: None,
            select: TargetSelect::Explicit,
        }
    }
}

/// The available target selection modes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetSelect {
    /// Use `target.thread_id`.
    #[default]
    Explicit,
    /// Use the most recently updated non-archived thread from `list_threads`.
    Latest,
}

/// Timing of the audit loop.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CycleConfig {
    /// Idle time between cycles, in seconds.
    pub interval_secs: u64,
    /// How long to wait for the agent's turn to finish, in seconds.
    pub await_response_secs: u64,
    /// How often to re-read the thread state while it waits for approval, in
    /// milliseconds.
    pub approval_poll_ms: u64,
    /// How long to keep waiting for an approval before giving the cycle up.
    pub max_approval_wait_secs: u64,
    /// First retry delay when the bridge is full, in milliseconds.
    pub backoff_initial_ms: u64,
    /// Retry delay ceiling, in milliseconds.
    pub backoff_max_ms: u64,
    /// Retries of one message before the cycle gives up.
    pub max_send_attempts: u32,
}

impl Default for CycleConfig {
    fn default() -> Self {
        Self {
            interval_secs: 300,
            await_response_secs: 900,
            approval_poll_ms: 30_000,
            max_approval_wait_secs: 3_600,
            backoff_initial_ms: 500,
            backoff_max_ms: 60_000,
            max_send_attempts: 6,
        }
    }
}

/// Where the daemon keeps its ledger, event log, and findings cursor.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StorageConfig {
    /// Directory for state files. Defaults to
    /// `~/Library/Application Support/zed-audit-loop`.
    pub dir: Option<PathBuf>,
}

/// Where findings come from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct FindingConfig {
    /// Which [`FindingSource`](crate::finding::FindingSource) to build.
    pub source: FindingSourceKind,
    /// JSONL file read by the `file` source.
    pub file: Option<PathBuf>,
    /// Program run by the `command` source.
    pub command: Option<String>,
    /// Arguments for `command`.
    pub args: Vec<String>,
    /// Upper bound on one rendered message, below the bridge's 64 KiB line
    /// limit so the JSON envelope always fits.
    pub max_message_bytes: usize,
}

impl Default for FindingConfig {
    fn default() -> Self {
        Self {
            source: FindingSourceKind::File,
            file: None,
            command: None,
            args: Vec::new(),
            max_message_bytes: 48_000,
        }
    }
}

/// The finding sources that exist.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingSourceKind {
    /// Read a JSON Lines file, remembering how far it has been consumed.
    #[default]
    File,
    /// Run a command and read JSON Lines from its stdout.
    Command,
}

/// Logging.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LogConfig {
    /// Minimum level to write.
    pub level: Level,
    /// File to append JSON lines to, in addition to stderr.
    pub file: Option<PathBuf>,
    /// Whether records also go to stderr.
    pub stderr: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: Level::Info,
            file: None,
            stderr: true,
        }
    }
}

impl Config {
    /// Reads and validates a TOML config file.
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        Self::parse(&text).with_context(|| format!("parsing config {}", path.display()))
    }

    /// Parses and validates a TOML document.
    pub fn parse(text: &str) -> Result<Self> {
        let config: Self = toml::from_str(text).context("invalid TOML")?;
        config.validate()?;
        Ok(config)
    }

    /// Checks the combinations that cannot be expressed by types.
    pub fn validate(&self) -> Result<()> {
        match self.target.select {
            TargetSelect::Explicit
                if self
                    .target
                    .thread_id
                    .as_deref()
                    .is_none_or(|id| id.trim().is_empty()) =>
            {
                bail!("target.thread_id is required unless target.select = \"latest\"");
            }
            TargetSelect::Latest if self.target.thread_id.is_some() => {
                bail!("target.thread_id cannot be combined with target.select = \"latest\"");
            }
            _ => {}
        }

        match self.finding.source {
            FindingSourceKind::File => {
                if self.finding.file.is_none() {
                    bail!("finding.file is required when finding.source = \"file\"");
                }
            }
            FindingSourceKind::Command => {
                if self
                    .finding
                    .command
                    .as_deref()
                    .is_none_or(|command| command.trim().is_empty())
                {
                    bail!("finding.command is required when finding.source = \"command\"");
                }
            }
        }

        if self.finding.max_message_bytes < 1_024 {
            bail!("finding.max_message_bytes must be at least 1024");
        }
        if self.finding.max_message_bytes > crate::protocol::MAX_LINE_BYTES - 4_096 {
            bail!(
                "finding.max_message_bytes must leave room for the JSON envelope below the {} byte line limit",
                crate::protocol::MAX_LINE_BYTES
            );
        }
        if self.cycle.interval_secs == 0 {
            bail!("cycle.interval_secs must be at least 1");
        }
        if self.cycle.max_send_attempts == 0 {
            bail!("cycle.max_send_attempts must be at least 1");
        }
        if self.cycle.backoff_max_ms < self.cycle.backoff_initial_ms {
            bail!("cycle.backoff_max_ms must not be smaller than cycle.backoff_initial_ms");
        }

        Ok(())
    }

    /// The bridge socket this config points at.
    pub fn socket_path(&self, home: &Path) -> PathBuf {
        match &self.bridge.socket {
            Some(path) => expand_tilde(path, home),
            None => default_socket_path(home, normalized_channel(&self.bridge.channel)),
        }
    }

    /// The directory holding the ledger, event log, and findings cursor.
    pub fn storage_dir(&self, home: &Path) -> PathBuf {
        match &self.storage.dir {
            Some(path) => expand_tilde(path, home),
            None => home
                .join("Library/Application Support")
                .join("zed-audit-loop"),
        }
    }

    /// The client settings for this config.
    pub fn client_config(&self, home: &Path) -> ClientConfig {
        ClientConfig {
            socket_path: self.socket_path(home),
            connect_timeout: Duration::from_millis(self.bridge.connect_timeout_ms),
            request_timeout: Duration::from_millis(self.bridge.request_timeout_ms),
            max_requests_per_connection: crate::protocol::MAX_REQUESTS_PER_CONNECTION,
        }
    }

    /// The expanded `finding.file`, if the config names one.
    pub fn finding_file(&self, home: &Path) -> Option<PathBuf> {
        self.finding
            .file
            .as_deref()
            .map(|path| expand_tilde(path, home))
    }

    /// The expanded `log.file`, if the config names one.
    pub fn log_file(&self, home: &Path) -> Option<PathBuf> {
        self.log
            .file
            .as_deref()
            .map(|path| expand_tilde(path, home))
    }
}

/// The bridge socket path for a Zed release channel.
pub fn default_socket_path(home: &Path, channel: &str) -> PathBuf {
    home.join("Library/Application Support")
        .join("Zed")
        .join(format!("agent-bridge-{channel}.sock"))
}

/// An empty or blank channel means `stable`, matching the bridge's default.
fn normalized_channel(channel: &str) -> &str {
    let trimmed = channel.trim();
    if trimmed.is_empty() {
        DEFAULT_CHANNEL
    } else {
        trimmed
    }
}

/// Expands a leading `~` using `home`.
pub fn expand_tilde(path: &Path, home: &Path) -> PathBuf {
    let text = path.to_string_lossy();
    if text == "~" {
        return home.to_path_buf();
    }
    match text.strip_prefix("~/") {
        Some(rest) => home.join(rest),
        None => path.to_path_buf(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn home() -> PathBuf {
        PathBuf::from("/Users/tester")
    }

    #[test]
    fn defaults_resolve_to_the_stable_channel() {
        let config = Config::parse(
            "target = { thread_id = \"thread-a\" }\n[finding]\nfile = \"findings.jsonl\"\n",
        )
        .expect("valid config");

        assert_eq!(config.bridge.channel, "stable");
        assert_eq!(
            config.socket_path(&home()),
            PathBuf::from("/Users/tester/Library/Application Support/Zed/agent-bridge-stable.sock")
        );
        assert_eq!(
            config.storage_dir(&home()),
            PathBuf::from("/Users/tester/Library/Application Support/zed-audit-loop")
        );
        assert_eq!(config.cycle.interval_secs, 300);
        assert_eq!(config.cycle.max_send_attempts, 6);
    }

    #[test]
    fn a_channel_selects_its_own_socket() {
        let config = Config::parse(
            "target = { thread_id = \"t\" }\n[bridge]\nchannel = \"dev\"\n[finding]\nfile = \"f.jsonl\"\n",
        )
        .expect("valid config");

        assert_eq!(
            config.socket_path(&home()),
            PathBuf::from("/Users/tester/Library/Application Support/Zed/agent-bridge-dev.sock")
        );
    }

    #[test]
    fn a_blank_channel_falls_back_to_stable() {
        let config = Config::parse(
            "target = { thread_id = \"t\" }\n[bridge]\nchannel = \"  \"\n[finding]\nfile = \"f.jsonl\"\n",
        )
        .expect("valid config");

        assert!(
            config
                .socket_path(&home())
                .ends_with("agent-bridge-stable.sock")
        );
    }

    #[test]
    fn an_explicit_socket_wins_and_expands_a_tilde() {
        let config = Config::parse(
            "target = { thread_id = \"t\" }\n[bridge]\nchannel = \"dev\"\nsocket = \"~/tmp/bridge.sock\"\n[finding]\nfile = \"f.jsonl\"\n",
        )
        .expect("valid config");

        assert_eq!(
            config.socket_path(&home()),
            PathBuf::from("/Users/tester/tmp/bridge.sock")
        );
    }

    #[test]
    fn a_bare_tilde_expands_to_the_home_directory() {
        assert_eq!(expand_tilde(Path::new("~"), &home()), home());
        assert_eq!(
            expand_tilde(Path::new("/absolute.sock"), &home()),
            PathBuf::from("/absolute.sock")
        );
    }

    #[test]
    fn an_explicit_target_is_required_unless_latest_is_selected() {
        let error = Config::parse("");
        assert!(error.is_err(), "a config without a target must fail");

        let latest =
            Config::parse("[target]\nselect = \"latest\"\n[finding]\nfile = \"f.jsonl\"\n")
                .expect("latest needs no thread id");
        assert_eq!(latest.target.select, TargetSelect::Latest);
    }

    #[test]
    fn latest_and_a_thread_id_are_mutually_exclusive() {
        let error = Config::parse("[target]\nthread_id = \"t\"\nselect = \"latest\"\n")
            .expect_err("must refuse");

        assert!(format!("{error:#}").contains("cannot be combined"));
    }

    #[test]
    fn the_file_source_needs_a_file_and_the_command_source_a_command() {
        let error = Config::parse("target = { thread_id = \"t\" }\n")
            .expect_err("file source without a file must fail");
        assert!(format!("{error:#}").contains("finding.file"));

        Config::parse("target = { thread_id = \"t\" }\n[finding]\nfile = \"findings.jsonl\"\n")
            .expect("file source with a file is fine");

        let error =
            Config::parse("target = { thread_id = \"t\" }\n[finding]\nsource = \"command\"\n")
                .expect_err("command source without a command must fail");
        assert!(format!("{error:#}").contains("finding.command"));
    }

    #[test]
    fn unknown_keys_are_rejected() {
        let error = Config::parse("target = { thread_id = \"t\", targte = 1 }\n")
            .expect_err("a typo must be loud");

        assert!(format!("{error:#}").contains("parse"));
    }

    #[test]
    fn message_budget_stays_below_the_line_limit() {
        let error = Config::parse(
            "target = { thread_id = \"t\" }\n[finding]\nfile = \"f.jsonl\"\nmax_message_bytes = 100000\n",
        )
        .expect_err("must refuse");
        assert!(format!("{error:#}").contains("max_message_bytes"));
    }

    #[test]
    fn client_config_uses_the_socket_and_timeouts() {
        let config = Config::parse(
            "target = { thread_id = \"t\" }\n[bridge]\nchannel = \"dev\"\nconnect_timeout_ms = 10\nrequest_timeout_ms = 20\n[finding]\nfile = \"f.jsonl\"\n",
        )
        .expect("valid config");
        let client = config.client_config(&home());

        assert!(client.socket_path.ends_with("agent-bridge-dev.sock"));
        assert_eq!(client.connect_timeout, Duration::from_millis(10));
        assert_eq!(client.request_timeout, Duration::from_millis(20));
    }
}
