//! A fake bridge server, so the client and the daemon can be tested without Zed.
//!
//! It speaks the same NDJSON protocol as `crates/agent_bridge`: one thread per
//! connection, a hard line limit, a request budget per connection, and `event`
//! lines pushed to a `watch_thread` subscriber.

#![allow(dead_code)]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use serde_json::{Value, json};
use zed_audit_loop::protocol::{
    ErrorBody, ErrorCode, PROTOCOL_VERSION, Response, Status, ThreadEvent, ThreadStatusInfo,
    ThreadSummary,
};

/// One received request line, decoded just enough for assertions.
#[derive(Debug, Clone)]
pub struct ReceivedRequest {
    pub action: String,
    pub request_id: String,
    pub thread_id: Option<String>,
    pub message: Option<String>,
    pub raw: Value,
}

impl ReceivedRequest {
    fn parse(line: &str) -> Value {
        serde_json::from_str(line).unwrap_or_else(|_| json!({ "raw": line }))
    }

    fn from_value(value: Value) -> Self {
        Self {
            action: value["action"].as_str().unwrap_or_default().to_string(),
            request_id: value["request_id"].as_str().unwrap_or_default().to_string(),
            thread_id: value["thread_id"].as_str().map(ToString::to_string),
            message: value["message"].as_str().map(ToString::to_string),
            raw: value,
        }
    }
}

/// The thread state the fake bridge reports.
#[derive(Debug, Clone)]
pub struct ThreadFacts {
    pub thread_id: String,
    pub state: String,
    pub archived: bool,
    pub found: bool,
    pub loaded: bool,
    pub session: bool,
}

impl ThreadFacts {
    /// A healthy idle thread.
    pub fn idle(thread_id: &str) -> Self {
        Self {
            thread_id: thread_id.to_string(),
            state: "idle".to_string(),
            archived: false,
            found: true,
            loaded: true,
            session: true,
        }
    }

    /// A thread that does not exist.
    pub fn missing(thread_id: &str) -> Self {
        Self {
            found: false,
            ..Self::idle(thread_id)
        }
    }

    fn summary(&self) -> ThreadSummary {
        ThreadSummary {
            thread_id: self.thread_id.clone(),
            session_id: self.session.then(|| format!("session-{}", self.thread_id)),
            title: Some(format!("thread {}", self.thread_id)),
            agent_id: "zed-agent".to_string(),
            archived: self.archived,
            state: zed_audit_loop::protocol::ThreadState::from_wire(&self.state),
            worktree_paths: vec!["/tmp/project".to_string()],
            updated_at: None,
        }
    }

    fn status(&self) -> ThreadStatusInfo {
        ThreadStatusInfo {
            thread_id: self.thread_id.clone(),
            found: self.found,
            archived: self.archived,
            state: zed_audit_loop::protocol::ThreadState::from_wire(&self.state),
            deliverable: self.found && !self.archived && self.session,
            loaded: self.loaded,
        }
    }
}

/// How the fake bridge answers a `send_message`.
#[derive(Debug, Clone)]
pub enum SendReply {
    /// `delivered` with a message id.
    Delivered,
    /// `queued`.
    Queued,
    /// `rejected` with an error code.
    Rejected(ErrorCode),
    /// A bare status without an error body.
    Status(Status),
}

impl SendReply {
    fn to_response(&self, request_id: &str, thread_id: &str) -> Response {
        match self {
            Self::Delivered => Response {
                message_id: Some("12".to_string()),
                thread_id: Some(thread_id.to_string()),
                ..Response {
                    version: PROTOCOL_VERSION,
                    request_id: request_id.to_string(),
                    status: Status::Delivered,
                    thread_id: None,
                    message_id: None,
                    error: None,
                    threads: None,
                    thread: None,
                    delivery: None,
                    event: None,
                }
            },
            Self::Queued => Response {
                thread_id: Some(thread_id.to_string()),
                ..Response {
                    version: PROTOCOL_VERSION,
                    request_id: request_id.to_string(),
                    status: Status::Queued,
                    thread_id: None,
                    message_id: None,
                    error: None,
                    threads: None,
                    thread: None,
                    delivery: None,
                    event: None,
                }
            },
            Self::Rejected(code) => Response {
                thread_id: Some(thread_id.to_string()),
                error: Some(ErrorBody {
                    code: code.clone(),
                    detail: Some(format!("{code} from the fake bridge")),
                }),
                ..Response {
                    version: PROTOCOL_VERSION,
                    request_id: request_id.to_string(),
                    status: Status::Rejected,
                    thread_id: None,
                    message_id: None,
                    error: None,
                    threads: None,
                    thread: None,
                    delivery: None,
                    event: None,
                }
            },
            Self::Status(status) => Response {
                thread_id: Some(thread_id.to_string()),
                ..Response {
                    version: PROTOCOL_VERSION,
                    request_id: request_id.to_string(),
                    status: status.clone(),
                    thread_id: None,
                    message_id: None,
                    error: None,
                    threads: None,
                    thread: None,
                    delivery: None,
                    event: None,
                }
            },
        }
    }
}

/// What the fake bridge sends after accepting a `send_message`.
#[derive(Debug, Clone, Default)]
pub struct TurnScript {
    /// Event lines pushed to the watcher, in order.
    pub events: Vec<ThreadEvent>,
}

impl TurnScript {
    /// The usual shape: the turn starts, the answer arrives, the turn ends.
    pub fn standard(thread_id: &str, answer: &str) -> Self {
        Self {
            events: vec![
                status_changed(thread_id, "generating"),
                entry_added(thread_id, 7, "user_message", "audit please"),
                entry_added(thread_id, 8, "assistant_message", answer),
                status_changed(thread_id, "idle"),
                ThreadEvent {
                    watch_id: "watch".to_string(),
                    thread_id: thread_id.to_string(),
                    kind: zed_audit_loop::protocol::EventKind::TurnStopped,
                    entry: None,
                    state: None,
                    detail: Some("end_turn".to_string()),
                },
            ],
        }
    }

    /// No events at all, which is what a silent agent looks like.
    pub fn silent() -> Self {
        Self::default()
    }
}

/// A `status_changed` event.
#[must_use]
pub fn status_changed(thread_id: &str, state: &str) -> ThreadEvent {
    ThreadEvent {
        watch_id: "watch".to_string(),
        thread_id: thread_id.to_string(),
        kind: zed_audit_loop::protocol::EventKind::StatusChanged,
        entry: None,
        state: Some(zed_audit_loop::protocol::ThreadState::from_wire(state)),
        detail: None,
    }
}

/// An `entry_added` event.
#[must_use]
pub fn entry_added(thread_id: &str, index: u64, kind: &str, text: &str) -> ThreadEvent {
    ThreadEvent {
        watch_id: "watch".to_string(),
        thread_id: thread_id.to_string(),
        kind: zed_audit_loop::protocol::EventKind::EntryAdded,
        entry: Some(zed_audit_loop::protocol::EntrySummary {
            index,
            kind: kind.to_string(),
            text: Some(text.to_string()),
        }),
        state: None,
        detail: None,
    }
}

/// Behaviour shared with the running server thread.
#[derive(Default)]
pub struct Script {
    pub thread: Option<ThreadFacts>,
    /// Threads `list_threads` returns, in order.
    pub threads: Vec<ThreadFacts>,
    /// Replies to `send_message`, consumed in order; the last one repeats.
    pub send_replies: Vec<SendReply>,
    /// Sent after each accepted `send_message`.
    pub turn: Option<TurnScript>,
    /// Reply to `get_thread_status` overrides the thread facts when set.
    pub status_override: Option<ThreadStatusInfo>,
    /// Verdict for `watch_thread`; `None` accepts.
    pub watch_rejection: Option<ErrorCode>,
    /// Answer `watched` connections with a raw line that is not valid JSON.
    pub send_malformed_event: bool,
    /// Close every connection after answering once (a bridge that went away).
    pub close_after_request: bool,
    /// Refuse the next request of any action, then clear itself.
    pub reject_next_request: Option<ErrorCode>,
    /// Answer the next request with a line that is not JSON, then clear itself.
    pub malformed_next_reply: bool,
    /// Drop the watcher sockets right after streaming a send's events, which is
    /// what a bridge that restarted looks like to the client.
    pub close_watch_after_send: bool,
    /// Never register the first watcher, so its connection ends immediately.
    pub close_first_watch: bool,
    /// Never register any watcher, so every watch connection ends at once.
    pub close_all_watches: bool,
    /// How many `watch_thread` requests were served.
    pub watches_served: usize,
}

/// State the tests inspect.
#[derive(Default)]
pub struct Recorded {
    pub requests: Vec<ReceivedRequest>,
    pub connections: usize,
    pub dropped_oversized: usize,
    /// Raw lines written to watchers.
    pub events_written: usize,
}

/// A fake bridge listening on a private socket path.
pub struct FakeBridge {
    path: PathBuf,
    script: Arc<Mutex<Script>>,
    recorded: Arc<Mutex<Recorded>>,
    watchers: Arc<Mutex<Vec<Arc<Mutex<UnixStream>>>>>,
    stop: Arc<Mutex<bool>>,
    handle: Option<JoinHandle<()>>,
    /// Socket directory, kept short: a Unix socket path must fit in `SUN_LEN`.
    directory: PathBuf,
}

impl FakeBridge {
    /// Starts a bridge in a fresh temporary directory.
    pub fn start(script: Script) -> Self {
        let directory = short_socket_directory();
        let path = directory.join("bridge.sock");
        let listener = UnixListener::bind(&path).expect("bind fake bridge");

        let script = Arc::new(Mutex::new(script));
        let recorded = Arc::new(Mutex::new(Recorded::default()));
        let watchers: Arc<Mutex<Vec<Arc<Mutex<UnixStream>>>>> = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(Mutex::new(false));

        let script_handle = Arc::clone(&script);
        let recorded_handle = Arc::clone(&recorded);
        let watchers_handle = Arc::clone(&watchers);
        let stop_handle = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            for stream in listener.incoming() {
                if *stop_handle.lock().expect("stop flag") {
                    break;
                }
                let Ok(stream) = stream else { break };
                recorded_handle.lock().expect("recorded").connections += 1;
                let script = Arc::clone(&script_handle);
                let recorded = Arc::clone(&recorded_handle);
                let watchers = Arc::clone(&watchers_handle);
                thread::spawn(move || serve_connection(stream, script, recorded, watchers));
            }
        });

        Self {
            path,
            script,
            recorded,
            watchers,
            stop,
            handle: Some(handle),
            directory,
        }
    }

    /// The socket path to hand to a client.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// All requests received so far.
    pub fn requests(&self) -> Vec<ReceivedRequest> {
        self.recorded.lock().expect("recorded").requests.clone()
    }

    /// Requests for one action, in order.
    pub fn requests_for(&self, action: &str) -> Vec<ReceivedRequest> {
        self.requests()
            .into_iter()
            .filter(|request| request.action == action)
            .collect()
    }

    /// How many connections were accepted.
    pub fn connections(&self) -> usize {
        self.recorded.lock().expect("recorded").connections
    }

    /// How many oversized lines were rejected with `message_too_large`.
    pub fn dropped_oversized(&self) -> usize {
        self.recorded.lock().expect("recorded").dropped_oversized
    }

    /// Replaces the thread facts a subsequent request sees.
    pub fn set_thread(&self, facts: ThreadFacts) {
        self.script.lock().expect("script").thread = Some(facts);
    }

    /// Replaces the replies a subsequent `send_message` receives.
    pub fn set_send_replies(&self, replies: Vec<SendReply>) {
        self.script.lock().expect("script").send_replies = replies;
    }

    /// Replaces what is streamed after an accepted send.
    pub fn set_turn(&self, turn: TurnScript) {
        self.script.lock().expect("script").turn = Some(turn);
    }

    /// Adds one event to the stream used by later sends.
    pub fn set_events(&self, events: Vec<ThreadEvent>) {
        let mut script = self.script.lock().expect("script");
        script.turn = Some(TurnScript { events });
    }

    /// Pushes event lines to every registered watcher, as the bridge does when a
    /// watched thread changes.
    pub fn push_events(&self, thread_id: &str, events: &[ThreadEvent]) {
        push_events(&self.watchers, thread_id, events);
    }

    /// Stops accepting connections.
    pub fn stop(&mut self) {
        if let Ok(mut flag) = self.stop.lock() {
            *flag = true;
        }
        // Unblock `incoming()` by connecting once.
        let _ = UnixStream::connect(&self.path);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// A socket directory short enough for `SUN_LEN`, under `/tmp`.
fn short_socket_directory() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let directory = PathBuf::from("/tmp").join(format!(
        "zed-audit-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&directory).expect("create the socket directory");
    directory
}

impl Drop for FakeBridge {
    fn drop(&mut self) {
        if let Ok(mut flag) = self.stop.lock() {
            *flag = true;
        }
        let _ = UnixStream::connect(&self.path);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn serve_connection(
    stream: UnixStream,
    script: Arc<Mutex<Script>>,
    recorded: Arc<Mutex<Recorded>>,
    watchers: Arc<Mutex<Vec<Arc<Mutex<UnixStream>>>>>,
) {
    let read_half = stream.try_clone().expect("clone for reading");
    let mut writer = stream;
    let mut reader = BufReader::new(read_half);
    let mut served = 0usize;
    let mut line = String::new();

    loop {
        line.clear();
        let read = match reader.read_line(&mut line) {
            Ok(read) => read,
            Err(_) => return,
        };
        if read == 0 {
            return;
        }

        // The real bridge answers an oversized line and closes the connection.
        if line.len() > zed_audit_loop::protocol::MAX_LINE_BYTES {
            recorded.lock().expect("recorded").dropped_oversized += 1;
            let response = Response {
                version: PROTOCOL_VERSION,
                request_id: String::new(),
                thread_id: None,
                status: Status::Rejected,
                message_id: None,
                error: Some(ErrorBody {
                    code: ErrorCode::MessageTooLarge,
                    detail: Some("line exceeds the 64 KiB limit".to_string()),
                }),
                threads: None,
                thread: None,
                delivery: None,
                event: None,
            };
            let _ = write_response(&mut writer, &response);
            return;
        }

        served += 1;
        let value = ReceivedRequest::parse(line.trim_end());
        let request = ReceivedRequest::from_value(value);
        recorded
            .lock()
            .expect("recorded")
            .requests
            .push(request.clone());

        if request.action == "watch_thread" {
            let (reply, register) = {
                let script = script.lock().expect("script");
                let facts = script
                    .thread
                    .clone()
                    .unwrap_or_else(|| ThreadFacts::idle("thread-a"));
                match script.watch_rejection.clone() {
                    Some(code) => (
                        Some(send_status_with_error(
                            &request.request_id,
                            &code,
                            Status::Rejected,
                        )),
                        false,
                    ),
                    None => {
                        let reply = Response {
                            version: PROTOCOL_VERSION,
                            request_id: request.request_id.clone(),
                            thread_id: Some(facts.thread_id.clone()),
                            status: Status::Accepted,
                            message_id: None,
                            error: None,
                            threads: None,
                            thread: Some(facts.status()),
                            delivery: None,
                            event: None,
                        };
                        (Some(reply), true)
                    }
                }
            };
            if let Some(reply) = reply {
                let _ = write_response(&mut writer, &reply);
            }
            let first = {
                let mut script = script.lock().expect("script");
                let first = script.watches_served == 0;
                script.watches_served += 1;
                first
            };
            let skip_registration = {
                let script = script.lock().expect("script");
                script.close_all_watches || (first && script.close_first_watch)
            };
            if register && !skip_registration {
                // A clone keeps the socket open after this thread returns, so
                // the client still sees a live subscription.
                let sink = writer.try_clone().expect("clone the watch socket");
                watchers
                    .lock()
                    .expect("watchers")
                    .push(Arc::new(Mutex::new(sink)));
            }
            return;
        }

        {
            let mut script = script.lock().expect("script");
            if script.malformed_next_reply {
                script.malformed_next_reply = false;
                let _ = writer.write_all(b"{ this is not json\n");
                let _ = writer.flush();
                continue;
            }
            if let Some(code) = script.reject_next_request.take() {
                let response = send_status_with_error(&request.request_id, &code, Status::Rejected);
                let _ = write_response(&mut writer, &response);
                continue;
            }
        }

        let response = {
            let mut script = script.lock().expect("script");
            match request.action.as_str() {
                "list_threads" => list_threads_response(&request.request_id, &script.threads),
                "get_thread_status" => {
                    let facts = script
                        .thread
                        .clone()
                        .unwrap_or_else(|| ThreadFacts::idle("thread-a"));
                    match &script.status_override {
                        Some(status) => Response {
                            version: PROTOCOL_VERSION,
                            request_id: request.request_id.clone(),
                            thread_id: Some(status.thread_id.clone()),
                            status: if status.found {
                                Status::Accepted
                            } else {
                                Status::ThreadNotFound
                            },
                            message_id: None,
                            error: None,
                            threads: None,
                            thread: Some(status.clone()),
                            delivery: None,
                            event: None,
                        },
                        None if !facts.found => Response {
                            version: PROTOCOL_VERSION,
                            request_id: request.request_id.clone(),
                            thread_id: request.thread_id.clone(),
                            status: Status::ThreadNotFound,
                            message_id: None,
                            error: None,
                            threads: None,
                            thread: Some(facts.status()),
                            delivery: None,
                            event: None,
                        },
                        None => Response {
                            version: PROTOCOL_VERSION,
                            request_id: request.request_id.clone(),
                            thread_id: Some(facts.thread_id.clone()),
                            status: Status::Accepted,
                            message_id: None,
                            error: None,
                            threads: None,
                            thread: Some(facts.status()),
                            delivery: None,
                            event: None,
                        },
                    }
                }
                "send_message" => {
                    let reply = match script.send_replies.len() {
                        0 => SendReply::Delivered,
                        1 => script.send_replies[0].clone(),
                        _ => script.send_replies.remove(0),
                    };
                    let thread_id = request.thread_id.clone().unwrap_or_default();
                    reply.to_response(&request.request_id, &thread_id)
                }
                "get_delivery_status" => Response {
                    version: PROTOCOL_VERSION,
                    request_id: request.request_id.clone(),
                    thread_id: None,
                    status: Status::Rejected,
                    message_id: None,
                    error: Some(ErrorBody {
                        code: ErrorCode::UnknownRequest,
                        detail: Some("no delivery is tracked".to_string()),
                    }),
                    threads: None,
                    thread: None,
                    delivery: None,
                    event: None,
                },
                other => send_status_with_error(
                    &request.request_id,
                    &ErrorCode::UnknownAction,
                    Status::Rejected,
                )
                .with_action(other),
            }
        };

        let _ = write_response(&mut writer, &response);

        if request.action == "send_message"
            && matches!(response.status, Status::Delivered | Status::Queued)
        {
            let guard = script.lock().expect("script");
            let streamed = guard.turn.as_ref().map(|turn| turn.events.clone());
            let close_after = guard.close_watch_after_send;
            drop(guard);
            if let Some(events) = streamed {
                let thread_id = request.thread_id.clone().unwrap_or_default();
                push_events(&watchers, &thread_id, &events);
                if close_after {
                    // Dropping every sink closes the watcher sockets.
                    watchers.lock().expect("watchers").clear();
                }
            }
        }

        if served >= zed_audit_loop::protocol::MAX_REQUESTS_PER_CONNECTION {
            return;
        }
        let close = script.lock().expect("script").close_after_request;
        if close {
            return;
        }
    }
}

fn list_threads_response(request_id: &str, threads: &[ThreadFacts]) -> Response {
    Response {
        version: PROTOCOL_VERSION,
        request_id: request_id.to_string(),
        thread_id: None,
        status: Status::Accepted,
        message_id: None,
        error: None,
        threads: Some(threads.iter().map(ThreadFacts::summary).collect()),
        thread: None,
        delivery: None,
        event: None,
    }
}

fn send_status_with_error(request_id: &str, code: &ErrorCode, status: Status) -> Response {
    Response {
        version: PROTOCOL_VERSION,
        request_id: request_id.to_string(),
        thread_id: None,
        status,
        message_id: None,
        error: Some(ErrorBody {
            code: code.clone(),
            detail: None,
        }),
        threads: None,
        thread: None,
        delivery: None,
        event: None,
    }
}

trait WithAction {
    fn with_action(self, action: &str) -> Self;
}

impl WithAction for Response {
    fn with_action(mut self, action: &str) -> Self {
        if let Some(error) = &mut self.error
            && error.detail.is_none()
        {
            error.detail = Some(format!("unknown action {action}"));
        }
        self
    }
}

fn push_events(
    watchers: &Arc<Mutex<Vec<Arc<Mutex<UnixStream>>>>>,
    thread_id: &str,
    events: &[ThreadEvent],
) {
    let sinks: Vec<Arc<Mutex<UnixStream>>> = watchers
        .lock()
        .expect("watchers")
        .iter()
        .map(Arc::clone)
        .collect();
    for event in events {
        for sink in &sinks {
            let Ok(mut stream) = sink.lock() else {
                continue;
            };
            let watch_id = event.watch_id.clone();
            let _ = write_event(&mut stream, thread_id, &watch_id, event);
        }
    }
}

fn write_response(writer: &mut UnixStream, response: &Response) -> std::io::Result<()> {
    let line = serde_json::to_string(response).map_err(std::io::Error::other)?;
    writer.write_all(line.as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()
}

/// Writes one event line to a watcher, as the bridge does after `watch_thread`.
pub fn write_event(
    writer: &mut UnixStream,
    thread_id: &str,
    watch_id: &str,
    event: &ThreadEvent,
) -> std::io::Result<()> {
    let response = Response {
        version: PROTOCOL_VERSION,
        request_id: watch_id.to_string(),
        thread_id: Some(thread_id.to_string()),
        status: Status::Event,
        message_id: None,
        error: None,
        threads: None,
        thread: None,
        delivery: None,
        event: Some(event.clone()),
    };
    write_response(writer, &response)
}
