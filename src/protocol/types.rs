//! Request and response types of the bridge wire protocol.

use serde::{Deserialize, Serialize};

use super::{MAX_LINE_BYTES, PROTOCOL_VERSION};

/// Defines a string-valued wire enum that tolerates values this client has not
/// learned yet, so a newer Zed cannot break the daemon with a new status.
macro_rules! wire_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $( $variant:ident => $wire:literal ),* $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        $vis enum $name {
            $( $variant, )*
            /// A value this build does not know. Preserved instead of rejected.
            Other(String),
        }

        impl $name {
            /// The exact string used on the wire.
            pub fn as_wire(&self) -> &str {
                match self {
                    $( Self::$variant => $wire, )*
                    Self::Other(value) => value,
                }
            }

            /// Parses a wire value, keeping unknown ones.
            pub fn from_wire(value: &str) -> Self {
                match value {
                    $( $wire => Self::$variant, )*
                    other => Self::Other(other.to_string()),
                }
            }

            /// Whether this build understands the wire value.
            pub fn is_known(&self) -> bool {
                !matches!(self, Self::Other(_))
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(self.as_wire())
            }
        }

        impl Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_wire())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(
                deserializer: D,
            ) -> Result<Self, D::Error> {
                let value = String::deserialize(deserializer)?;
                Ok(Self::from_wire(&value))
            }
        }
    };
}

/// Request actions the bridge serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    ListThreads,
    GetThreadStatus,
    SendMessage,
    GetDeliveryStatus,
    WatchThread,
}

impl Action {
    /// The exact string used on the wire.
    pub fn as_wire(self) -> &'static str {
        match self {
            Self::ListThreads => "list_threads",
            Self::GetThreadStatus => "get_thread_status",
            Self::SendMessage => "send_message",
            Self::GetDeliveryStatus => "get_delivery_status",
            Self::WatchThread => "watch_thread",
        }
    }
}

/// How the target thread should react to a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Start a turn with the message.
    Prompt,
    /// Not implemented by the bridge; always rejected with `notify_disabled`.
    Notify,
}

/// When the bridge should deliver a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryTiming {
    AfterCurrentTurn,
}

wire_enum! {
    /// Outcome of a request, or `event` for a line streamed by `watch_thread`.
    pub enum Status {
        Accepted => "accepted",
        Queued => "queued",
        Delivered => "delivered",
        Event => "event",
        ThreadNotFound => "thread_not_found",
        ThreadUnavailable => "thread_unavailable",
        UnsupportedThreadType => "unsupported_thread_type",
        Rejected => "rejected",
        Error => "error",
    }
}

wire_enum! {
    /// Reason attached to a `rejected` or `error` status.
    pub enum ErrorCode {
        InvalidJson => "invalid_json",
        UnsupportedVersion => "unsupported_version",
        UnknownAction => "unknown_action",
        InvalidRequest => "invalid_request",
        MessageTooLarge => "message_too_large",
        UnknownRequest => "unknown_request",
        DuplicateRequest => "duplicate_request",
        QueueFull => "queue_full",
        ThreadArchived => "thread_archived",
        ThreadUnavailable => "thread_unavailable",
        AwaitingApproval => "awaiting_approval",
        NotifyDisabled => "notify_disabled",
        PromptDisabled => "prompt_disabled",
        BridgeDisabled => "bridge_disabled",
        Internal => "internal",
    }
}

impl ErrorCode {
    /// What the daemon should do about this error, following the bridge spec's
    /// error table.
    pub fn recommended_action(&self) -> RecommendedAction {
        match self {
            Self::InvalidJson
            | Self::UnsupportedVersion
            | Self::UnknownAction
            | Self::InvalidRequest
            | Self::Internal => RecommendedAction::Stop,
            Self::MessageTooLarge => RecommendedAction::Split,
            Self::UnknownRequest => RecommendedAction::Resend,
            Self::DuplicateRequest => RecommendedAction::AlreadySent,
            Self::QueueFull => RecommendedAction::BackOff,
            Self::ThreadArchived => RecommendedAction::Reselect,
            Self::ThreadUnavailable => RecommendedAction::OpenProject,
            Self::AwaitingApproval => RecommendedAction::WaitForApproval,
            Self::NotifyDisabled => RecommendedAction::UsePrompt,
            Self::PromptDisabled | Self::BridgeDisabled => RecommendedAction::FixSettings,
            Self::Other(_) => RecommendedAction::Stop,
        }
    }

    /// Whether the bridge records this rejection in its delivery ledger.
    ///
    /// A recorded rejection makes any later request with the same `request_id`
    /// answer `duplicate_request`, so a retry must use a fresh id. Rejections
    /// the bridge does not record can be retried with the id they were sent
    /// with; that keeps the daemon's ledger (and the bridge's) unambiguous.
    pub fn is_recorded_by_bridge(&self) -> bool {
        matches!(
            self,
            Self::QueueFull
                | Self::AwaitingApproval
                | Self::ThreadArchived
                | Self::DuplicateRequest
        )
    }
}

/// The daemon-side reading of an [`ErrorCode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecommendedAction {
    /// A bug on our side; stop rather than loop.
    Stop,
    /// Split the message and resend (never sent as one line).
    Split,
    /// The bridge has no record; resend.
    Resend,
    /// The bridge already handled it; treat as sent.
    AlreadySent,
    /// Wait and retry (queue is full).
    BackOff,
    /// Pick a different thread.
    Reselect,
    /// The user has to open the project in Zed.
    OpenProject,
    /// The user has to approve in the Zed UI.
    WaitForApproval,
    /// Send with `mode: prompt`.
    UsePrompt,
    /// The Zed setting disables the bridge or prompts.
    FixSettings,
}

wire_enum! {
    /// What the agent is doing right now.
    pub enum ThreadState {
        Draft => "draft",
        Idle => "idle",
        Generating => "generating",
        AwaitingApproval => "awaiting_approval",
        Unconfirmed => "unconfirmed",
        Unknown => "unknown",
    }
}

wire_enum! {
    /// What kind of change an event line reports.
    pub enum EventKind {
        EntryAdded => "entry_added",
        EntryUpdated => "entry_updated",
        EntryRemoved => "entry_removed",
        StatusChanged => "status_changed",
        TitleUpdated => "title_updated",
        ToolAuthorizationRequested => "tool_authorization_requested",
        ToolAuthorizationReceived => "tool_authorization_received",
        TurnStopped => "turn_stopped",
    }
}

/// An error body; `detail` exists for humans reading logs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub code: ErrorCode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// One thread entry as the bridge reports it.
///
/// `text` is Markdown for user, assistant, and session messages only; tool
/// calls carry their kind and index alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntrySummary {
    pub index: u64,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

impl EntrySummary {
    /// Whether this entry carries Markdown the bridge publishes.
    pub fn carries_message_text(&self) -> bool {
        matches!(
            self.kind.as_str(),
            "user_message" | "assistant_message" | "session_message"
        )
    }

    /// Whether this entry is the agent speaking, as opposed to the echo of a
    /// message a client sent.
    pub fn is_agent_text(&self) -> bool {
        matches!(self.kind.as_str(), "assistant_message" | "session_message")
    }
}

/// One line of a `watch_thread` stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadEvent {
    /// The `request_id` of the request that opened the stream.
    #[serde(default)]
    pub watch_id: String,
    #[serde(default)]
    pub thread_id: String,
    pub kind: EventKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry: Option<EntrySummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<ThreadState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// A thread as `list_threads` reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadSummary {
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default)]
    pub agent_id: String,
    #[serde(default)]
    pub archived: bool,
    pub state: ThreadState,
    #[serde(default)]
    pub worktree_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

/// A thread as `get_thread_status` and the `watch_thread` reply report it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadStatusInfo {
    pub thread_id: String,
    pub found: bool,
    #[serde(default)]
    pub archived: bool,
    pub state: ThreadState,
    /// Whether a `send_message` would currently be accepted.
    pub deliverable: bool,
    /// Whether Zed already holds the thread in memory.
    #[serde(default)]
    pub loaded: bool,
}

/// A `get_delivery_status` record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryRecord {
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    pub status: Status,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorBody>,
}

/// One response line. Every payload field is optional because which ones are
/// present depends on the action, and a rejection may carry none.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response {
    #[serde(default)]
    pub version: u32,
    /// Echoes the request id. Empty for rejections the bridge cannot correlate
    /// (`message_too_large`, `bridge_disabled`), so never treat it as required.
    #[serde(default)]
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    pub status: Status,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorBody>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threads: Option<Vec<ThreadSummary>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread: Option<ThreadStatusInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<DeliveryRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<ThreadEvent>,
}

impl Response {
    /// The error code, if this response is a rejection.
    pub fn error_code(&self) -> Option<&ErrorCode> {
        self.error.as_ref().map(|error| &error.code)
    }

    /// Whether the bridge refused the request.
    pub fn is_rejection(&self) -> bool {
        matches!(self.status, Status::Rejected | Status::Error)
    }
}

/// Which thread a request targets. The bridge treats the two as exclusive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadTarget {
    ThreadId(String),
    SessionId(String),
}

impl ThreadTarget {
    /// The identifier without its kind.
    pub fn as_str(&self) -> &str {
        match self {
            Self::ThreadId(value) | Self::SessionId(value) => value,
        }
    }
}

/// One request line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub request_id: String,
    pub action: Action,
    pub target: Option<ThreadTarget>,
    pub message: Option<String>,
}

impl Request {
    /// `list_threads`
    pub fn list_threads(request_id: impl Into<String>) -> Self {
        Self {
            request_id: request_id.into(),
            action: Action::ListThreads,
            target: None,
            message: None,
        }
    }

    /// `get_thread_status`
    pub fn get_thread_status(request_id: impl Into<String>, thread_id: impl Into<String>) -> Self {
        Self {
            request_id: request_id.into(),
            action: Action::GetThreadStatus,
            target: Some(ThreadTarget::ThreadId(thread_id.into())),
            message: None,
        }
    }

    /// `send_message` in the only mode the bridge implements (`prompt`).
    pub fn send_message(
        request_id: impl Into<String>,
        thread_id: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            action: Action::SendMessage,
            target: Some(ThreadTarget::ThreadId(thread_id.into())),
            message: Some(message.into()),
        }
    }

    /// `get_delivery_status`
    pub fn get_delivery_status(request_id: impl Into<String>) -> Self {
        Self {
            request_id: request_id.into(),
            action: Action::GetDeliveryStatus,
            target: None,
            message: None,
        }
    }

    /// `watch_thread`
    pub fn watch_thread(request_id: impl Into<String>, thread_id: impl Into<String>) -> Self {
        Self {
            request_id: request_id.into(),
            action: Action::WatchThread,
            target: Some(ThreadTarget::ThreadId(thread_id.into())),
            message: None,
        }
    }

    /// Serializes to one NDJSON line, refusing oversized lines locally so an
    /// oversized message never costs a connection.
    pub fn to_line(&self) -> Result<String, crate::protocol::ClientError> {
        let mut object = serde_json::Map::new();
        object.insert(
            "version".to_string(),
            serde_json::Value::from(PROTOCOL_VERSION),
        );
        object.insert(
            "request_id".to_string(),
            serde_json::Value::from(self.request_id.clone()),
        );
        object.insert(
            "action".to_string(),
            serde_json::Value::from(self.action.as_wire()),
        );
        match &self.target {
            Some(ThreadTarget::ThreadId(thread_id)) => {
                object.insert(
                    "thread_id".to_string(),
                    serde_json::Value::from(thread_id.clone()),
                );
            }
            Some(ThreadTarget::SessionId(session_id)) => {
                object.insert(
                    "session_id".to_string(),
                    serde_json::Value::from(session_id.clone()),
                );
            }
            None => {}
        }
        object.insert("mode".to_string(), serde_json::Value::from("prompt"));
        object.insert(
            "delivery".to_string(),
            serde_json::Value::from("after_current_turn"),
        );
        if let Some(message) = &self.message {
            object.insert(
                "message".to_string(),
                serde_json::Value::from(message.clone()),
            );
        }

        let line = serde_json::to_string(&object)
            .map_err(|error| crate::protocol::ClientError::Encode(error.to_string()))?;
        if line.len() > MAX_LINE_BYTES {
            return Err(crate::protocol::ClientError::RequestTooLarge {
                bytes: line.len(),
                limit: MAX_LINE_BYTES,
            });
        }
        Ok(line)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_request_carries_thread_and_message_only() {
        let line = Request::send_message("audit-1", "thread-a", "hello")
            .to_line()
            .expect("encodes");
        let value: serde_json::Value = serde_json::from_str(&line).expect("valid json");

        assert_eq!(value["version"], 1);
        assert_eq!(value["request_id"], "audit-1");
        assert_eq!(value["action"], "send_message");
        assert_eq!(value["thread_id"], "thread-a");
        assert_eq!(value["message"], "hello");
        assert_eq!(value["mode"], "prompt");
        assert_eq!(value["delivery"], "after_current_turn");
        assert!(value.get("session_id").is_none());
    }

    #[test]
    fn list_request_omits_target() {
        let line = Request::list_threads("r1").to_line().expect("encodes");
        let value: serde_json::Value = serde_json::from_str(&line).expect("valid json");

        assert_eq!(value["action"], "list_threads");
        assert!(value.get("thread_id").is_none());
        assert!(value.get("message").is_none());
    }

    #[test]
    fn oversized_message_is_refused_before_sending() {
        let message = "x".repeat(MAX_LINE_BYTES + 1);
        let error = Request::send_message("audit-1", "thread-a", message)
            .to_line()
            .expect_err("must refuse");

        assert!(matches!(
            error,
            crate::protocol::ClientError::RequestTooLarge { .. }
        ));
    }

    #[test]
    fn response_accepts_empty_request_id_and_missing_file_fields() {
        let response: Response = serde_json::from_str(
            r#"{"version":1,"request_id":"","status":"rejected",
                "error":{"code":"message_too_large"}}"#,
        )
        .expect("parses a correlated-less rejection");

        assert_eq!(response.request_id, "");
        assert_eq!(response.error_code(), Some(&ErrorCode::MessageTooLarge));
    }

    #[test]
    fn unknown_wire_values_are_preserved() {
        assert_eq!(
            Status::from_wire("paused_for_coffee"),
            Status::Other("paused_for_coffee".to_string())
        );
        assert!(!Status::from_wire("paused_for_coffee").is_known());
        assert_eq!(EventKind::from_wire("entry_added"), EventKind::EntryAdded);
    }

    #[test]
    fn recorded_rejections_are_the_ones_needing_a_fresh_request_id() {
        for code in [
            ErrorCode::QueueFull,
            ErrorCode::AwaitingApproval,
            ErrorCode::ThreadArchived,
            ErrorCode::DuplicateRequest,
        ] {
            assert!(code.is_recorded_by_bridge(), "{code}");
        }
        for code in [
            ErrorCode::ThreadUnavailable,
            ErrorCode::PromptDisabled,
            ErrorCode::BridgeDisabled,
            ErrorCode::Internal,
        ] {
            assert!(!code.is_recorded_by_bridge(), "{code}");
        }
    }

    #[test]
    fn event_line_decodes_entry_text() {
        let response: Response = serde_json::from_str(
            r#"{"version":1,"request_id":"watch-1","thread_id":"thread-a",
                "status":"event",
                "event":{"watch_id":"watch-1","thread_id":"thread-a",
                          "kind":"entry_added",
                          "entry":{"index":7,"kind":"assistant_message",
                                    "text":"done"}}}"#,
        )
        .expect("parses an event");
        let event = response.event.expect("event present");

        assert_eq!(event.kind, EventKind::EntryAdded);
        assert_eq!(
            event
                .entry
                .map(|entry| (entry.index, entry.kind, entry.text)),
            Some((7, "assistant_message".to_string(), Some("done".to_string())))
        );
    }
}
