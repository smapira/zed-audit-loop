//! Client-side failures, kept separate from protocol-level rejections.
//!
//! A `rejected` response is a successful exchange: the bridge answered. These
//! errors mean the exchange itself failed, and whether retrying is safe depends
//! on [`ClientStage`].

use std::fmt;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use super::{ErrorBody, Status};

/// Where in an exchange a failure happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientStage {
    /// Opening the socket.
    Connect,
    /// Writing the request line.
    Write,
    /// Reading the response line.
    Read,
}

impl fmt::Display for ClientStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect => formatter.write_str("connect"),
            Self::Write => formatter.write_str("write"),
            Self::Read => formatter.write_str("read"),
        }
    }
}

/// Everything that can go wrong before a response is decoded.
#[derive(Debug)]
pub enum ClientError {
    /// The socket is missing, refuses connections, or belongs to another user.
    /// The daemon cannot tell "bridge disabled" from "Zed not running", so the
    /// message names both.
    NotConnected { path: PathBuf, source: io::Error },
    /// The pooled connection went away between calls.
    Disconnected,
    /// Reading or writing failed.
    Io {
        stage: ClientStage,
        source: io::Error,
    },
    /// The stage did not finish in time.
    Timeout { stage: ClientStage, after: Duration },
    /// A line exceeded the 64 KiB bridge limit.
    LineTooLarge { bytes: usize, limit: usize },
    /// The bridge closed the connection without answering.
    UnexpectedEof { stage: ClientStage },
    /// The line was not UTF-8 or not a response object.
    MalformedResponse { line: String, detail: String },
    /// A request could not be serialized.
    Encode(String),
    /// `watch_thread` was refused, so there is no stream to read.
    WatchRejected {
        status: Status,
        error: Option<ErrorBody>,
    },
    /// A request was refused. The bridge answered, but the answer forbids the
    /// operation rather than describing a result.
    Rejected {
        status: Status,
        error: Option<ErrorBody>,
    },
}

impl ClientError {
    /// Whether retrying the same request could plausibly succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::NotConnected { .. }
            | Self::Disconnected
            | Self::Timeout { .. }
            | Self::Io { .. } => true,
            // The bridge answered with a line we cannot decode; a retry with the
            // same bytes would fail the same way.
            Self::LineTooLarge { .. } | Self::MalformedResponse { .. } | Self::Encode(_) => false,
            // The bridge closed without answering: it may have processed the
            // request, so the caller must reconcile instead of assuming.
            Self::UnexpectedEof { .. } => false,
            Self::WatchRejected { .. } => false,
            Self::Rejected { .. } => false,
        }
    }

    /// Where the failure happened, when it happened during transport.
    pub fn stage(&self) -> Option<ClientStage> {
        match self {
            Self::NotConnected { .. } => Some(ClientStage::Connect),
            Self::Io { stage, .. } | Self::Timeout { stage, .. } => Some(*stage),
            Self::UnexpectedEof { stage } => Some(*stage),
            Self::Disconnected | Self::LineTooLarge { .. } => Some(ClientStage::Read),
            Self::MalformedResponse { .. } | Self::Encode(_) => None,
            Self::WatchRejected { .. } | Self::Rejected { .. } => None,
        }
    }

    /// Whether the bridge certainly never saw the request, which is what makes
    /// reusing the same `request_id` safe.
    pub fn never_reached_bridge(&self) -> bool {
        matches!(
            self.stage(),
            Some(ClientStage::Connect | ClientStage::Write)
        )
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotConnected { path, source } => write!(
                formatter,
                "cannot connect to {} ({source}); is the bridge enabled \
                 (agent_external_bridge.enabled = true) and Zed running?",
                path.display()
            ),
            Self::Disconnected => formatter.write_str("the bridge connection was closed"),
            Self::Io { stage, source } => write!(formatter, "bridge {stage} failed: {source}"),
            Self::Timeout { stage, after } => {
                write!(formatter, "bridge {stage} timed out after {after:?}")
            }
            Self::LineTooLarge { bytes, limit } => {
                write!(
                    formatter,
                    "line of {bytes} bytes exceeds the {limit} byte limit"
                )
            }
            Self::UnexpectedEof { stage } => {
                write!(
                    formatter,
                    "bridge closed the connection during {stage} without answering"
                )
            }
            Self::MalformedResponse { line, detail } => {
                write!(formatter, "unexpected response line ({detail}): {line}")
            }
            Self::Encode(detail) => write!(formatter, "cannot encode the request: {detail}"),
            Self::WatchRejected { status, error } => match error {
                Some(error) => write!(
                    formatter,
                    "bridge refused to watch: {status}/{}",
                    error.code
                ),
                None => write!(formatter, "bridge refused to watch: {status}"),
            },
            Self::Rejected { status, error } => match error {
                Some(error) => write!(
                    formatter,
                    "bridge refused the request: {status}/{}",
                    error.code
                ),
                None => write!(formatter, "bridge refused the request: {status}"),
            },
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotConnected { source, .. } | Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}
