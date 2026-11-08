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
    /// A line exceeded the 64 KiB bridge limit: a response we could not read.
    LineTooLarge { bytes: usize, limit: usize },
    /// The request would not fit in one bridge line. Detected before writing, so
    /// the bridge certainly never saw it.
    RequestTooLarge { bytes: usize, limit: usize },
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
}

impl ClientError {
    /// Whether retrying the same request could plausibly succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::NotConnected { .. }
            | Self::Disconnected
            | Self::Timeout { .. }
            | Self::Io { .. } => true,
            // The bridge answered with a line we cannot decode, or the request
            // cannot be encoded; a retry with the same bytes fails the same way.
            Self::LineTooLarge { .. }
            | Self::RequestTooLarge { .. }
            | Self::MalformedResponse { .. }
            | Self::Encode(_) => false,
            // The bridge closed without answering: it may have processed the
            // request, so the caller must reconcile instead of assuming.
            Self::UnexpectedEof { .. } => false,
            Self::WatchRejected { .. } => false,
        }
    }

    /// Where the failure happened, when it happened during transport.
    pub fn stage(&self) -> Option<ClientStage> {
        match self {
            Self::NotConnected { .. } => Some(ClientStage::Connect),
            Self::Io { stage, .. } | Self::Timeout { stage, .. } => Some(*stage),
            Self::UnexpectedEof { stage } => Some(*stage),
            Self::Disconnected | Self::LineTooLarge { .. } => Some(ClientStage::Read),
            Self::RequestTooLarge { .. } | Self::MalformedResponse { .. } | Self::Encode(_) => None,
            Self::WatchRejected { .. } => None,
        }
    }

    /// Whether the bridge certainly never saw the request, which is what makes
    /// reusing the same `request_id` safe.
    pub fn never_reached_bridge(&self) -> bool {
        matches!(
            self.stage(),
            Some(ClientStage::Connect | ClientStage::Write)
        ) || matches!(self, Self::RequestTooLarge { .. } | Self::Encode(_))
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
                    "response line of {bytes} bytes exceeds the {limit} byte limit"
                )
            }
            Self::RequestTooLarge { bytes, limit } => {
                write!(
                    formatter,
                    "request line of {bytes} bytes exceeds the {limit} byte limit; split the message"
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
        }
    }
}

impl std::error::Error for ClientError {
    /// The I/O cause is already inside the message, so it is not offered again as
    /// a source: `anyhow`'s `{:#}` would otherwise print it twice, which is what
    /// an operator sees first when the bridge is not there.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_that_never_fit_is_never_treated_as_delivered() {
        let error = ClientError::RequestTooLarge {
            bytes: 70_000,
            limit: 65_536,
        };

        assert!(error.never_reached_bridge());
        assert!(!error.is_retryable());
        assert!(error.to_string().contains("split the message"));
    }

    #[test]
    fn a_read_side_failure_may_have_reached_the_bridge() {
        for error in [
            ClientError::Timeout {
                stage: ClientStage::Read,
                after: Duration::from_secs(1),
            },
            ClientError::UnexpectedEof {
                stage: ClientStage::Read,
            },
            ClientError::LineTooLarge {
                bytes: 70_000,
                limit: 65_536,
            },
            ClientError::MalformedResponse {
                line: "{".to_string(),
                detail: "unexpected end of input".to_string(),
            },
        ] {
            assert!(!error.never_reached_bridge(), "{error}");
        }
    }

    #[test]
    fn the_io_cause_is_printed_once() {
        let error = anyhow::Error::new(ClientError::NotConnected {
            path: std::path::PathBuf::from("/tmp/missing.sock"),
            source: std::io::Error::from(std::io::ErrorKind::ConnectionRefused),
        });

        let rendered = format!("{error:#}");
        assert_eq!(
            rendered
                .to_lowercase()
                .matches("connection refused")
                .count(),
            1,
            "{rendered}"
        );
    }

    #[test]
    fn a_connect_or_write_failure_never_reached_the_bridge() {
        for error in [
            ClientError::Timeout {
                stage: ClientStage::Connect,
                after: Duration::from_secs(1),
            },
            ClientError::Io {
                stage: ClientStage::Write,
                source: std::io::Error::from(std::io::ErrorKind::BrokenPipe),
            },
            ClientError::NotConnected {
                path: std::path::PathBuf::from("/tmp/missing.sock"),
                source: std::io::Error::from(std::io::ErrorKind::NotFound),
            },
        ] {
            assert!(error.never_reached_bridge(), "{error}");
        }
    }
}
