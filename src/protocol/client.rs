//! The socket client the daemon uses: one request per connection for queries
//! and sends, one long-lived connection per watched thread.

use std::time::Duration;

use tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::time::timeout;

use super::error::{ClientError, ClientStage};
use super::types::{EventKind, Request, Response, Status, ThreadEvent};
use super::{MAX_EVENT_LINE_BYTES, MAX_LINE_BYTES};

/// Everything needed to reach one bridge socket.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Absolute path of the bridge socket for the target Zed channel.
    pub socket_path: std::path::PathBuf,
    /// How long to wait for the socket to accept a connection.
    pub connect_timeout: Duration,
    /// How long one request/response exchange may take.
    pub request_timeout: Duration,
    /// Requests to serve on one connection before opening a new one. The bridge
    /// closes a connection after its own limit, so staying below it avoids
    /// racing a close we cannot see coming.
    pub max_requests_per_connection: usize,
}

impl ClientConfig {
    /// A config for one socket with conservative timeouts.
    pub fn new(socket_path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
            connect_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(20),
            max_requests_per_connection: super::MAX_REQUESTS_PER_CONNECTION,
        }
    }
}

/// Reads newline-delimited lines with a hard size limit.
///
/// The partial line is kept in the struct, so cancelling a read (for example by
/// a `select!` timeout) loses nothing: the next call continues the same line.
pub struct LineReader<R> {
    reader: BufReader<R>,
    buffer: Vec<u8>,
    limit: usize,
}

impl<R: AsyncRead + Unpin> LineReader<R> {
    /// Wraps a stream, reading lines of at most `limit` bytes.
    pub fn new(reader: R, limit: usize) -> Self {
        Self {
            reader: BufReader::new(reader),
            buffer: Vec::new(),
            limit,
        }
    }

    /// Discards the rest of the current line.
    ///
    /// Used when a line is too long to keep: the subscription stays usable, and
    /// only that line is lost.
    pub async fn skip_line(&mut self) -> Result<(), ClientError> {
        self.buffer.clear();
        loop {
            let (consumed, complete) = {
                let available = self
                    .reader
                    .fill_buf()
                    .await
                    .map_err(|source| ClientError::Io {
                        stage: ClientStage::Read,
                        source,
                    })?;
                if available.is_empty() {
                    return Ok(());
                }
                match available.iter().position(|byte| *byte == b'\n') {
                    Some(index) => (index + 1, true),
                    None => (available.len(), false),
                }
            };
            self.reader.consume(consumed);
            if complete {
                return Ok(());
            }
        }
    }

    /// Reads one line without its trailing newline.
    ///
    /// Returns `Ok(None)` at a clean end of stream, and an error when the stream
    /// ends mid-line: a truncated line must never be parsed as a response.
    pub async fn next_line(&mut self) -> Result<Option<String>, ClientError> {
        loop {
            let (chunk, consumed, complete) = {
                let available = self
                    .reader
                    .fill_buf()
                    .await
                    .map_err(|source| ClientError::Io {
                        stage: ClientStage::Read,
                        source,
                    })?;
                if available.is_empty() {
                    (Vec::new(), 0, None)
                } else {
                    match available.iter().position(|byte| *byte == b'\n') {
                        Some(index) => (
                            available.get(..index).unwrap_or_default().to_vec(),
                            index + 1,
                            Some(true),
                        ),
                        None => (available.to_vec(), available.len(), Some(false)),
                    }
                }
            };

            if self.buffer.len() + chunk.len() > self.limit {
                return Err(ClientError::LineTooLarge {
                    bytes: self.buffer.len() + chunk.len(),
                    limit: self.limit,
                });
            }
            self.buffer.extend_from_slice(&chunk);
            self.reader.consume(consumed);

            match complete {
                None => {
                    if self.buffer.is_empty() {
                        return Ok(None);
                    }
                    return Err(ClientError::UnexpectedEof {
                        stage: ClientStage::Read,
                    });
                }
                Some(true) => {
                    let line = std::mem::take(&mut self.buffer);
                    return Ok(Some(decode_line(line)?));
                }
                Some(false) => {}
            }
        }
    }
}

/// Event kind used for a line the subscriber had to discard.
pub const LINE_TOO_LARGE_EVENT: &str = "line_too_large";

fn decode_line(bytes: Vec<u8>) -> Result<String, ClientError> {
    match String::from_utf8(bytes) {
        Ok(line) => Ok(line.trim_end_matches('\r').to_string()),
        Err(error) => Err(ClientError::MalformedResponse {
            line: String::from_utf8_lossy(error.as_bytes()).into_owned(),
            detail: "line is not valid UTF-8".to_string(),
        }),
    }
}

fn decode_response(line: &str) -> Result<Response, ClientError> {
    serde_json::from_str(line).map_err(|error| ClientError::MalformedResponse {
        line: line.to_string(),
        detail: error.to_string(),
    })
}

struct Connection {
    lines: LineReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    served: usize,
}

impl Connection {
    async fn connect(
        socket_path: &std::path::Path,
        connect_timeout: Duration,
    ) -> Result<Self, ClientError> {
        Self::connect_with_limit(socket_path, connect_timeout, MAX_LINE_BYTES).await
    }

    async fn connect_with_limit(
        socket_path: &std::path::Path,
        connect_timeout: Duration,
        line_limit: usize,
    ) -> Result<Self, ClientError> {
        let stream = match timeout(connect_timeout, UnixStream::connect(socket_path)).await {
            Err(_) => {
                return Err(ClientError::Timeout {
                    stage: ClientStage::Connect,
                    after: connect_timeout,
                });
            }
            Ok(Err(source)) => {
                return Err(ClientError::NotConnected {
                    path: socket_path.to_path_buf(),
                    source,
                });
            }
            Ok(Ok(stream)) => stream,
        };
        let (read, write) = stream.into_split();
        Ok(Self {
            lines: LineReader::new(read, line_limit),
            writer: write,
            served: 0,
        })
    }

    async fn exchange(&mut self, line: &str, budget: Duration) -> Result<Response, ClientError> {
        let write = timeout(budget, async {
            self.writer.write_all(line.as_bytes()).await?;
            self.writer.write_all(b"\n").await?;
            self.writer.flush().await
        })
        .await;
        match write {
            Err(_) => {
                return Err(ClientError::Timeout {
                    stage: ClientStage::Write,
                    after: budget,
                });
            }
            Ok(Err(source)) => {
                return Err(ClientError::Io {
                    stage: ClientStage::Write,
                    source,
                });
            }
            Ok(Ok(())) => {}
        }

        let read = timeout(budget, self.lines.next_line()).await;
        let response_line = match read {
            Err(_) => {
                return Err(ClientError::Timeout {
                    stage: ClientStage::Read,
                    after: budget,
                });
            }
            Ok(Err(error)) => return Err(error),
            Ok(Ok(None)) => {
                return Err(ClientError::UnexpectedEof {
                    stage: ClientStage::Read,
                });
            }
            Ok(Ok(Some(line))) => line,
        };
        self.served += 1;
        decode_response(&response_line)
    }
}

/// A client for one bridge socket.
///
/// Conversations with the bridge are sequential by design: the bridge serves one
/// request at a time per connection, and the daemon has nothing to pipeline.
pub struct BridgeClient {
    config: ClientConfig,
    connection: Option<Connection>,
}

impl BridgeClient {
    /// Creates a client; nothing connects until the first request.
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config,
            connection: None,
        }
    }

    /// The socket this client talks to.
    pub fn socket_path(&self) -> &std::path::Path {
        &self.config.socket_path
    }

    /// Drops any pooled connection, for example after a failure.
    pub fn close(&mut self) {
        self.connection = None;
    }

    async fn ensure_connection(&mut self) -> Result<(), ClientError> {
        let stale = match &self.connection {
            None => true,
            Some(connection) => connection.served >= self.config.max_requests_per_connection,
        };
        if stale {
            self.connection = None;
            let connection =
                Connection::connect(&self.config.socket_path, self.config.connect_timeout).await?;
            self.connection = Some(connection);
        }
        Ok(())
    }

    /// Sends one request and returns the decoded response.
    ///
    /// Protocol rejections are returned as [`Response`] values, not errors: the
    /// bridge answered, and the caller decides what the answer means.
    ///
    /// A reused connection that turns out to be closed is retried once on a
    /// fresh one. That is safe only because a failure while connecting or
    /// writing means the bridge never received the request; a request that might
    /// have been received is reported instead of repeated.
    pub async fn request(&mut self, request: &Request) -> Result<Response, ClientError> {
        let line = request.to_line()?;
        let reused = self.connection.is_some();

        match self.exchange(&line).await {
            Ok(response) => Ok(response),
            Err(error) if reused && error.never_reached_bridge() => {
                self.connection = None;
                self.exchange(&line).await
            }
            Err(error) => Err(error),
        }
    }

    async fn exchange(&mut self, line: &str) -> Result<Response, ClientError> {
        let budget = self.config.request_timeout;
        self.ensure_connection().await?;

        let outcome = match self.connection.as_mut() {
            Some(connection) => connection.exchange(line, budget).await,
            None => Err(ClientError::Disconnected),
        };
        if outcome.is_err() {
            // The stream may hold half a line now, so it is never reused.
            self.connection = None;
        }
        outcome
    }

    /// `list_threads`: every thread the running Zed knows about.
    pub async fn list_threads(&mut self, request_id: &str) -> Result<Response, ClientError> {
        self.request(&Request::list_threads(request_id)).await
    }

    /// `get_thread_status` for one thread.
    pub async fn thread_status(
        &mut self,
        request_id: &str,
        thread_id: &str,
    ) -> Result<Response, ClientError> {
        self.request(&Request::get_thread_status(request_id, thread_id))
            .await
    }

    /// `send_message` in `prompt` mode.
    pub async fn send_message(
        &mut self,
        request_id: &str,
        thread_id: &str,
        message: &str,
    ) -> Result<Response, ClientError> {
        self.request(&Request::send_message(request_id, thread_id, message))
            .await
    }

    /// `get_delivery_status` for one earlier send.
    pub async fn delivery_status(&mut self, request_id: &str) -> Result<Response, ClientError> {
        self.request(&Request::get_delivery_status(request_id))
            .await
    }

    /// `watch_thread` on its own connection, which stays open for the life of
    /// the [`Subscription`].
    pub async fn watch(
        &mut self,
        request_id: &str,
        thread_id: &str,
    ) -> Result<Subscription, ClientError> {
        let request = Request::watch_thread(request_id, thread_id);
        let line = request.to_line()?;
        // Event lines carry whole messages, so this connection reads with the
        // larger ceiling from the start.
        let mut connection = Connection::connect_with_limit(
            &self.config.socket_path,
            self.config.connect_timeout,
            MAX_EVENT_LINE_BYTES,
        )
        .await?;

        let write = timeout(self.config.request_timeout, async {
            connection.writer.write_all(line.as_bytes()).await?;
            connection.writer.write_all(b"\n").await?;
            connection.writer.flush().await
        })
        .await;
        match write {
            Err(_) => {
                return Err(ClientError::Timeout {
                    stage: ClientStage::Write,
                    after: self.config.request_timeout,
                });
            }
            Ok(Err(source)) => {
                return Err(ClientError::Io {
                    stage: ClientStage::Write,
                    source,
                });
            }
            Ok(Ok(())) => {}
        }

        let read = timeout(self.config.request_timeout, connection.lines.next_line()).await;
        let response_line = match read {
            Err(_) => {
                return Err(ClientError::Timeout {
                    stage: ClientStage::Read,
                    after: self.config.request_timeout,
                });
            }
            Ok(Err(error)) => return Err(error),
            Ok(Ok(None)) => {
                return Err(ClientError::UnexpectedEof {
                    stage: ClientStage::Read,
                });
            }
            Ok(Ok(Some(line))) => line,
        };
        let response = decode_response(&response_line)?;
        let Connection { lines, writer, .. } = connection;

        if response.status != Status::Accepted {
            return Err(ClientError::WatchRejected {
                status: response.status,
                error: response.error,
            });
        }
        Ok(Subscription {
            lines,
            // Kept so the socket stays open: the bridge drops a watch when its
            // connection closes.
            _writer: writer,
            request_id: request_id.to_string(),
            thread_id: thread_id.to_string(),
            initial: response,
            skipped_lines: 0,
        })
    }
}

/// A live `watch_thread` stream.
pub struct Subscription {
    lines: LineReader<OwnedReadHalf>,
    _writer: OwnedWriteHalf,
    request_id: String,
    thread_id: String,
    initial: Response,
    skipped_lines: usize,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Subscription")
            .field("request_id", &self.request_id)
            .field("thread_id", &self.thread_id)
            .field("initial", &self.initial)
            .finish_non_exhaustive()
    }
}

impl Subscription {
    /// The `request_id` that opened this stream (the bridge calls it
    /// `watch_id`).
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    /// The watched thread.
    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }

    /// The initial `accepted` reply, which already carries the thread's state.
    pub fn initial(&self) -> &Response {
        &self.initial
    }

    /// Reads the next event.
    ///
    /// `Ok(None)` means the bridge closed the connection, so the watch is gone
    /// and the daemon has to reconnect. Other lines are skipped: a rejection is
    /// returned as an error so the caller learns why, and an oversized line is
    /// discarded with a synthetic `line_too_large` event so the loss is visible
    /// instead of killing the subscription.
    pub async fn next_event(&mut self) -> Result<Option<ThreadEvent>, ClientError> {
        loop {
            let line = match self.lines.next_line().await {
                Ok(Some(line)) => line,
                Ok(None) => return Ok(None),
                Err(ClientError::LineTooLarge { bytes, limit }) => {
                    self.lines.skip_line().await?;
                    self.skipped_lines += 1;
                    return Ok(Some(ThreadEvent {
                        watch_id: self.request_id.clone(),
                        thread_id: self.thread_id.clone(),
                        kind: EventKind::from_wire(LINE_TOO_LARGE_EVENT),
                        entry: None,
                        state: None,
                        detail: Some(format!(
                            "discarded a {bytes} byte event line (limit {limit})"
                        )),
                    }));
                }
                Err(error) => return Err(error),
            };
            let response = decode_response(&line)?;
            if let Some(event) = response.event {
                return Ok(Some(event));
            }
            if response.is_rejection() {
                return Err(ClientError::WatchRejected {
                    status: response.status,
                    error: response.error,
                });
            }
        }
    }

    /// How many event lines were discarded for being too long.
    pub fn skipped_lines(&self) -> usize {
        self.skipped_lines
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reader(input: &[u8], limit: usize) -> LineReader<&[u8]> {
        LineReader::new(input, limit)
    }

    #[tokio::test]
    async fn reads_newline_delimited_lines() {
        let mut lines = reader(b"{\"a\":1}\n{\"b\":2}\n", 1024);

        assert_eq!(
            lines.next_line().await.unwrap().as_deref(),
            Some("{\"a\":1}")
        );
        assert_eq!(
            lines.next_line().await.unwrap().as_deref(),
            Some("{\"b\":2}")
        );
        assert_eq!(lines.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_truncated_final_line_is_an_error_not_a_response() {
        let mut lines = reader(b"{\"a\":1}", 1024);

        let error = lines
            .next_line()
            .await
            .expect_err("must not parse a partial line");
        assert!(matches!(
            error,
            ClientError::UnexpectedEof {
                stage: ClientStage::Read
            }
        ));
    }

    #[tokio::test]
    async fn rejects_lines_over_the_limit() {
        let mut lines = reader(b"xxxxxxxx\n", 4);

        let error = lines.next_line().await.expect_err("must refuse");
        assert!(matches!(
            error,
            ClientError::LineTooLarge { bytes: 8, limit: 4 }
        ));
    }

    #[tokio::test]
    async fn a_line_of_exactly_the_limit_is_accepted() {
        let mut lines = reader(b"abcd\n", 4);

        assert_eq!(lines.next_line().await.unwrap().as_deref(), Some("abcd"));
    }

    #[tokio::test]
    async fn carriage_returns_are_stripped() {
        let mut lines = reader(b"abc\r\n", 16);

        assert_eq!(lines.next_line().await.unwrap().as_deref(), Some("abc"));
    }
}
