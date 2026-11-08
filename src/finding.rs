//! The seam where "what to audit" enters the daemon.
//!
//! Deciding *what* is wrong is the job of whatever produces findings: a linter, a
//! script, a human, or an LLM. None of that lives here. The daemon only needs a
//! batch of findings, which is why this module defines one trait and two trivial
//! implementations rather than an audit engine.

use std::path::PathBuf;

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};

/// How serious a finding is.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// Worth knowing, nothing is broken.
    #[default]
    Info,
    /// A disagreement with the specification.
    Warning,
    /// Something is definitely wrong.
    Error,
}

impl Severity {
    /// The word used in the rendered message.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }
}

/// One problem to report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Finding {
    /// Optional producer-side identifier, echoed into the message.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default)]
    pub severity: Severity,
    /// One line describing the problem.
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Where the problem is, e.g. a file path or a spec section.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
}

/// How much of a source has been consumed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindingCursor {
    /// The file the offset refers to, so a different file restarts at zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    /// Bytes already consumed.
    #[serde(default)]
    pub offset: u64,
}

/// Findings read in one go, plus the cursor to commit once they were handled.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FindingBatch {
    pub findings: Vec<Finding>,
    pub cursor: FindingCursor,
}

impl FindingBatch {
    /// Whether there is nothing to report.
    pub fn is_empty(&self) -> bool {
        self.findings.is_empty()
    }
}

/// Where findings come from.
///
/// `read_batch` must not move the source forward by itself: the daemon calls
/// [`FindingSource::commit`] only after the findings reached the bridge, so a
/// crash in between re-reads the same findings and the ledger suppresses the
/// duplicate send.
pub trait FindingSource {
    /// Reads the findings available right now.
    fn read_batch(&mut self) -> Result<FindingBatch>;

    /// Accepts the cursor, so those findings are not reported again.
    fn commit(&mut self, cursor: &FindingCursor) -> Result<()>;

    /// A human-readable description for the log.
    fn describe(&self) -> String;
}

/// Reads a JSON Lines file, remembering how far it has been consumed.
pub struct FileFindingSource {
    path: PathBuf,
    cursor_path: PathBuf,
    cursor: FindingCursor,
}

impl FileFindingSource {
    /// Opens a JSONL file, loading the persisted cursor when it refers to the
    /// same file.
    pub fn new(path: impl Into<PathBuf>, cursor_path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let cursor_path = cursor_path.into();
        let cursor = match std::fs::read_to_string(&cursor_path) {
            Ok(text) => serde_json::from_str::<FindingCursor>(&text).unwrap_or_default(),
            Err(_) => FindingCursor::default(),
        };
        Ok(Self {
            path,
            cursor_path,
            cursor,
        })
    }
}

impl FindingSource for FileFindingSource {
    fn read_batch(&mut self) -> Result<FindingBatch> {
        let contents = match std::fs::read(&self.path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => {
                return Err(error).with_context(|| format!("reading {}", self.path.display()));
            }
        };

        let file_name = self.path.display().to_string();
        // A shorter file means it was truncated or replaced, so start over
        // rather than skipping the new content.
        let mut offset = if self.cursor.file.as_deref() == Some(file_name.as_str()) {
            usize::try_from(self.cursor.offset).unwrap_or(0)
        } else {
            0
        };
        if offset > contents.len() {
            offset = 0;
        }

        let rest = contents.get(offset..).unwrap_or_default();
        // Only complete lines are consumed: a writer appending concurrently may
        // leave half a line behind, which must be read next time.
        let consumed = match rest.iter().rposition(|byte| *byte == b'\n') {
            Some(position) => position + 1,
            None => 0,
        };
        let text = std::str::from_utf8(rest.get(..consumed).unwrap_or_default())
            .with_context(|| format!("{} is not valid UTF-8", self.path.display()))?;
        let findings =
            parse_findings(text).with_context(|| format!("parsing {}", self.path.display()))?;

        Ok(FindingBatch {
            findings,
            cursor: FindingCursor {
                file: Some(file_name),
                offset: (offset + consumed) as u64,
            },
        })
    }

    fn commit(&mut self, cursor: &FindingCursor) -> Result<()> {
        self.cursor = cursor.clone();
        if let Some(parent) = self.cursor_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        let text = serde_json::to_string(&self.cursor).context("encoding the findings cursor")?;
        std::fs::write(&self.cursor_path, format!("{text}\n"))
            .with_context(|| format!("writing {}", self.cursor_path.display()))
    }

    fn describe(&self) -> String {
        format!("file:{}", self.path.display())
    }
}

/// Runs a command once per cycle and reads JSON Lines from its stdout.
///
/// The command runs synchronously on the runtime thread; at one launch per audit
/// cycle that is cheaper than the complexity of a supervised child process.
pub struct CommandFindingSource {
    command: String,
    args: Vec<String>,
}

impl CommandFindingSource {
    /// Creates a source that runs `command` with `args`.
    pub fn new(command: impl Into<String>, args: Vec<String>) -> Self {
        Self {
            command: command.into(),
            args,
        }
    }
}

impl FindingSource for CommandFindingSource {
    fn read_batch(&mut self) -> Result<FindingBatch> {
        let output = std::process::Command::new(&self.command)
            .args(&self.args)
            .stdin(std::process::Stdio::null())
            .output()
            .with_context(|| format!("running {}", self.describe()))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!(
                "{} exited with {}: {}",
                self.describe(),
                output.status,
                stderr.trim()
            );
        }
        let text = String::from_utf8(output.stdout)
            .with_context(|| format!("{} wrote invalid UTF-8", self.describe()))?;
        let findings =
            parse_findings(&text).with_context(|| format!("parsing {}", self.describe()))?;

        Ok(FindingBatch {
            findings,
            // A command has no read position to remember.
            cursor: FindingCursor::default(),
        })
    }

    fn commit(&mut self, _cursor: &FindingCursor) -> Result<()> {
        Ok(())
    }

    fn describe(&self) -> String {
        if self.args.is_empty() {
            format!("command:{}", self.command)
        } else {
            format!("command:{} {}", self.command, self.args.join(" "))
        }
    }
}

/// Parses JSON Lines, ignoring blank lines.
pub fn parse_findings(text: &str) -> Result<Vec<Finding>> {
    let mut findings = Vec::new();
    for (number, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let finding: Finding = serde_json::from_str(line)
            .with_context(|| format!("line {}: {}", number + 1, line.trim()))?;
        if finding.summary.trim().is_empty() {
            bail!("line {}: finding has no summary", number + 1);
        }
        findings.push(finding);
    }
    Ok(findings)
}

/// Renders findings into messages that each fit in `max_bytes`.
///
/// The bridge closes the connection on a line over 64 KiB, so an audit result
/// that does not fit is split instead of truncated. Each batch is a separate
/// message with its own `request_id`.
#[must_use]
pub fn render_batches(findings: &[Finding], max_bytes: usize) -> Vec<String> {
    if findings.is_empty() {
        return Vec::new();
    }

    // The bridge limits the encoded line, not the text, and every newline or
    // quote in a message takes two bytes once escaped. Sizing on the encoded form
    // is what keeps a message under the limit.
    // The part marker grows with the number of batches; `999` is far more than
    // one cycle ever produces and keeps the arithmetic honest.
    let header_room = wire_len(&render_message(&[], 1, 999));
    let budget = max_bytes.saturating_sub(header_room).max(1);

    let mut batches: Vec<Vec<String>> = Vec::new();
    let mut current: Vec<String> = Vec::new();
    let mut current_len = 0usize;
    for finding in findings {
        let mut block = render_finding(finding);
        if wire_len(&block) > budget {
            // One finding can be larger than a whole message. Truncating it is
            // better than failing the send forever: the send would be refused
            // locally and the finding would never be consumed.
            block = truncate_block(block, budget);
        }
        let block_len = wire_len(&block);
        if !current.is_empty() && current_len + block_len > budget {
            batches.push(std::mem::take(&mut current));
            current_len = 0;
        }
        current_len += block_len;
        current.push(block);
    }
    if !current.is_empty() {
        batches.push(current);
    }

    let total = batches.len();
    batches
        .into_iter()
        .enumerate()
        .map(|(index, blocks)| render_message(&blocks, index + 1, total))
        .collect()
}

/// One message: a header plus the findings it carries.
#[must_use]
pub fn render_message(blocks: &[String], part: usize, parts: usize) -> String {
    let mut message =
        String::from("【独立監査】仕様との不一致が見つかりました。確認してください。");
    if parts > 1 {
        message.push_str(&format!("（{part}/{parts}）"));
    }
    message.push_str("\n\n");
    for block in blocks {
        message.push_str(block);
    }
    message
}

/// The size a string occupies inside a JSON line.
fn wire_len(text: &str) -> usize {
    serde_json::to_string(text).map_or(text.len(), |encoded| encoded.len())
}

/// Shortens one oversized finding so it fits a message on its own.
///
/// The target is in encoded bytes, so the search halves the raw length until the
/// escaped form fits; it converges in a handful of steps and never splits a
/// multi-byte character.
fn truncate_block(mut block: String, budget: usize) -> String {
    const MARKER: &str = "\n  ...(truncated)\n";
    if wire_len(&block) <= budget {
        return block;
    }
    let target = budget.saturating_sub(wire_len(MARKER));

    let mut end = target.min(block.len());
    loop {
        while end > 0 && !block.is_char_boundary(end) {
            end -= 1;
        }
        if wire_len(block.get(..end).unwrap_or_default()) <= target || end == 0 {
            break;
        }
        end /= 2;
    }

    block.truncate(end);
    block.push_str(MARKER);
    block
}

/// One finding as a bullet.
fn render_finding(finding: &Finding) -> String {
    let mut block = String::new();
    block.push_str(&format!(
        "- [{}] {}",
        finding.severity.as_str(),
        finding.summary.trim()
    ));
    if let Some(location) = &finding.location {
        block.push_str(&format!(" ({location})"));
    }
    if !finding.id.is_empty() {
        block.push_str(&format!(" [{}]", finding.id));
    }
    block.push('\n');
    if let Some(detail) = &finding.detail {
        for line in detail.trim().lines() {
            block.push_str("  ");
            block.push_str(line);
            block.push('\n');
        }
    }
    block
}

#[cfg(test)]
mod tests {
    use super::*;
    fn finding(summary: &str) -> Finding {
        Finding {
            id: "F-1".into(),
            severity: Severity::Warning,
            summary: summary.into(),
            detail: Some("detail line".into()),
            location: Some("docs/spec.md:42".into()),
        }
    }

    #[test]
    fn a_file_is_reported_once() {
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("findings.jsonl");
        let cursor = directory.path().join("cursor.json");
        std::fs::write(
            &path,
            "{\"summary\":\"first\"}\n{\"summary\":\"second\",\"severity\":\"error\"}\n",
        )
        .expect("write findings");

        let mut source = FileFindingSource::new(&path, &cursor).expect("source");
        let batch = source.read_batch().expect("read");
        assert_eq!(batch.findings.len(), 2);
        assert_eq!(batch.findings[1].severity, Severity::Error);

        source.commit(&batch.cursor).expect("commit");
        assert!(source.read_batch().expect("read again").is_empty());

        // The cursor survives a restart.
        let mut reopened = FileFindingSource::new(&path, &cursor).expect("source");
        assert!(reopened.read_batch().expect("read").is_empty());
    }

    #[test]
    fn a_partial_trailing_line_waits_for_the_next_read() {
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("findings.jsonl");
        let cursor = directory.path().join("cursor.json");
        std::fs::write(&path, "{\"summary\":\"first\"}\n{\"summ").expect("write");

        let mut source = FileFindingSource::new(&path, &cursor).expect("source");
        let batch = source.read_batch().expect("read");
        assert_eq!(batch.findings.len(), 1);
        source.commit(&batch.cursor).expect("commit");

        std::fs::write(&path, "{\"summary\":\"first\"}\n{\"summary\":\"second\"}\n")
            .expect("append");
        let batch = source.read_batch().expect("read");
        assert_eq!(batch.findings.len(), 1);
        assert_eq!(batch.findings[0].summary, "second");
    }

    #[test]
    fn a_missing_file_reports_nothing() {
        let directory = tempfile::tempdir().expect("temp dir");
        let mut source = FileFindingSource::new(
            directory.path().join("absent.jsonl"),
            directory.path().join("cursor.json"),
        )
        .expect("source");

        assert!(source.read_batch().expect("read").is_empty());
    }

    #[test]
    fn a_truncated_file_is_read_from_the_start() {
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("findings.jsonl");
        let cursor = directory.path().join("cursor.json");
        std::fs::write(&path, "{\"summary\":\"a longer first line\"}\n").expect("write");
        let mut source = FileFindingSource::new(&path, &cursor).expect("source");
        let batch = source.read_batch().expect("read");
        source.commit(&batch.cursor).expect("commit");

        std::fs::write(&path, "{\"summary\":\"x\"}\n").expect("rewrite shorter");
        let batch = source.read_batch().expect("read");
        assert_eq!(batch.findings.len(), 1);
        assert_eq!(batch.findings[0].summary, "x");
    }

    #[test]
    fn a_broken_line_is_loud() {
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("findings.jsonl");
        std::fs::write(&path, "{not json}\n").expect("write");
        let mut source =
            FileFindingSource::new(&path, directory.path().join("cursor.json")).expect("source");

        let error = source.read_batch().expect_err("must fail");
        assert!(format!("{error:#}").contains("line 1"));
    }

    #[test]
    fn a_command_produces_findings() {
        let mut source = CommandFindingSource::new(
            "sh",
            vec![
                "-c".into(),
                "printf '{\\\"summary\\\":\\\"from command\\\"}\\n'".into(),
            ],
        );

        let batch = source.read_batch().expect("read");
        assert_eq!(batch.findings.len(), 1);
        assert_eq!(batch.findings[0].summary, "from command");
    }

    #[test]
    fn a_failing_command_is_an_error() {
        let mut source = CommandFindingSource::new("sh", vec!["-c".into(), "exit 3".into()]);

        let error = source.read_batch().expect_err("must fail");
        assert!(format!("{error:#}").contains("exited with"));
    }

    #[test]
    fn batches_never_exceed_the_budget_and_keep_every_finding() {
        let findings: Vec<Finding> = (0..40).map(|n| finding(&format!("problem {n}"))).collect();
        let batches = render_batches(&findings, 600);

        assert!(batches.len() > 1);
        for batch in &batches {
            assert!(batch.len() <= 600, "batch too large: {}", batch.len());
        }
        for n in 0..40 {
            // The needle includes the location so `problem 1` cannot match
            // `problem 10`.
            let needle = format!("] problem {n} (");
            assert_eq!(
                batches
                    .iter()
                    .filter(|batch| batch.contains(&needle))
                    .count(),
                1,
                "finding {n} must appear exactly once"
            );
        }
        assert!(batches[0].contains("1/"));
    }

    #[test]
    fn one_batch_has_no_part_marker() {
        let batches = render_batches(&[finding("only")], 10_000);

        assert_eq!(batches.len(), 1);
        assert!(!batches[0].contains("1/1"));
        assert!(batches[0].contains("【独立監査】"));
    }

    #[test]
    fn a_finding_larger_than_the_budget_is_truncated_not_dropped() {
        let huge = Finding {
            id: "F-huge".into(),
            severity: Severity::Error,
            summary: "one very long finding".into(),
            detail: Some("y".repeat(5_000)),
            location: None,
        };

        let batches = render_batches(&[huge], 500);

        assert_eq!(batches.len(), 1);
        assert!(batches[0].len() <= 500, "length: {}", batches[0].len());
        assert!(batches[0].contains("one very long finding"));
        assert!(batches[0].contains("truncated"));
    }

    #[test]
    fn truncation_keeps_utf8_intact() {
        let wide = Finding {
            summary: "日本語".repeat(200),
            ..finding("x")
        };

        let batches = render_batches(&[wide], 400);

        // `String::truncate` would have panicked on a character boundary; the
        // marker proves the truncation path ran on a wide-character block.
        assert!(!batches[0].contains('\u{fffd}'));
        assert!(batches[0].contains("truncated"));
        assert!(batches[0].contains("日本語"));
    }

    #[test]
    fn a_message_of_many_newlines_fits_the_encoded_budget() {
        // Newlines double once JSON-escaped, which is what used to push a
        // message past the bridge's 64 KiB line limit.
        let wordy = Finding {
            id: String::new(),
            severity: Severity::Info,
            summary: "many short lines".into(),
            detail: Some("one line\n".repeat(4_000)),
            location: None,
        };

        let batches = render_batches(&[wordy], 4_000);

        assert!(!batches.is_empty());
        for batch in &batches {
            assert!(
                wire_len(batch) <= 4_000,
                "encoded batch is {} bytes",
                wire_len(batch)
            );
        }
    }

    #[test]
    fn no_findings_render_nothing() {
        assert!(render_batches(&[], 10_000).is_empty());
    }

    #[test]
    fn a_summary_of_spaces_is_rejected() {
        let error = parse_findings("{\"summary\":\"   \"}\n").expect_err("must fail");
        assert!(format!("{error:#}").contains("no summary"));
    }

    #[test]
    fn blank_lines_are_skipped() {
        let findings = parse_findings("\n{\"summary\":\"a\"}\n\n").expect("parses");
        assert_eq!(findings.len(), 1);
    }
}
