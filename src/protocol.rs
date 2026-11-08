//! Wire protocol of Zed's external thread message bridge.
//!
//! The shapes mirror `crates/agent_bridge_protocol` in Zed so that the daemon
//! speaks exactly what the bridge accepts, without depending on Zed's crates.
//! Everything the server may add later is modelled permissively: an unknown
//! status, state, error code, or event kind is preserved as [`WireEnum`]-style
//! `Other` rather than failing the line.

mod client;
mod error;
mod types;

pub use client::{BridgeClient, ClientConfig, LINE_TOO_LARGE_EVENT, LineReader, Subscription};
pub use error::{ClientError, ClientStage};
pub use types::{
    Action, DeliveryRecord, DeliveryTiming, EntrySummary, ErrorBody, ErrorCode, EventKind, Mode,
    RecommendedAction, Request, Response, Status, ThreadEvent, ThreadState, ThreadStatusInfo,
    ThreadSummary, ThreadTarget,
};

/// Only version 1 exists; the bridge rejects anything else with
/// `unsupported_version` instead of guessing.
pub const PROTOCOL_VERSION: u32 = 1;

/// Upper bound on one newline-delimited request or response line.
pub const MAX_LINE_BYTES: usize = 64 * 1024;

/// Upper bound on one line the bridge streams to a subscriber.
///
/// The bridge caps the lines it *accepts*, not the ones it writes, and an
/// `entry.text` is a whole Markdown message: a long answer can be far bigger
/// than a request. Reading with the request limit would drop the subscription on
/// every long answer, so event lines get their own, larger ceiling.
pub const MAX_EVENT_LINE_BYTES: usize = 4 * 1024 * 1024;

/// Requests the bridge serves per connection before it closes it.
pub const MAX_REQUESTS_PER_CONNECTION: usize = 100;

/// Pending deliveries the bridge holds per thread when the agent is busy.
pub const MAX_PENDING_DELIVERIES_PER_THREAD: usize = 16;

/// Concurrent `watch_thread` subscriptions one Zed instance serves.
pub const MAX_WATCHES: usize = 8;
