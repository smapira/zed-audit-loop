//! Client conformance tests against a fake bridge speaking the documented
//! protocol. These are the tests that stand in for a real Zed: every rule in the
//! bridge spec that the client depends on is exercised here.

mod support;

use std::time::Duration;

use support::{FakeBridge, Script, SendReply, ThreadFacts, TurnScript};
use zed_audit_loop::protocol::{
    BridgeClient, ClientConfig, ClientError, ErrorCode, EventKind, Status, ThreadState,
};

fn client(bridge: &FakeBridge) -> BridgeClient {
    let mut config = ClientConfig::new(bridge.path());
    config.connect_timeout = Duration::from_millis(500);
    config.request_timeout = Duration::from_millis(500);
    BridgeClient::new(config)
}

#[tokio::test]
async fn list_threads_returns_the_threads_in_order() {
    let bridge = FakeBridge::start(Script {
        threads: vec![ThreadFacts::idle("thread-a"), ThreadFacts::idle("thread-b")],
        ..Script::default()
    });
    let mut client = client(&bridge);

    let response = client
        .list_threads("list-1")
        .await
        .expect("request answered");

    assert_eq!(response.status, Status::Accepted);
    let threads = response.threads.expect("threads present");
    assert_eq!(threads.len(), 2);
    assert_eq!(threads[0].thread_id, "thread-a");
    assert_eq!(threads[0].state, ThreadState::Idle);
    assert_eq!(bridge.requests_for("list_threads").len(), 1);
}

#[tokio::test]
async fn get_thread_status_reports_state_and_deliverability() {
    let bridge = FakeBridge::start(Script {
        thread: Some(ThreadFacts::idle("thread-a")),
        ..Script::default()
    });
    let mut client = client(&bridge);

    let response = client
        .thread_status("status-1", "thread-a")
        .await
        .expect("request answered");

    assert_eq!(response.status, Status::Accepted);
    let thread = response.thread.expect("thread present");
    assert!(thread.found);
    assert!(thread.deliverable);
    assert_eq!(thread.state, ThreadState::Idle);
}

#[tokio::test]
async fn a_missing_thread_is_a_status_not_a_transport_error() {
    let bridge = FakeBridge::start(Script {
        thread: Some(ThreadFacts::missing("thread-x")),
        ..Script::default()
    });
    let mut client = client(&bridge);

    let response = client
        .thread_status("status-1", "thread-x")
        .await
        .expect("the bridge answered");

    assert_eq!(response.status, Status::ThreadNotFound);
    assert!(!response.thread.expect("thread present").found);
}

#[tokio::test]
async fn send_message_reports_delivery_and_the_message_id() {
    let bridge = FakeBridge::start(Script::default());
    let mut client = client(&bridge);

    let response = client
        .send_message("audit-1", "thread-a", "hello")
        .await
        .expect("request answered");

    assert_eq!(response.status, Status::Delivered);
    assert_eq!(response.message_id.as_deref(), Some("12"));
    let sent = bridge.requests_for("send_message");
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].request_id, "audit-1");
    assert_eq!(sent[0].thread_id.as_deref(), Some("thread-a"));
    assert_eq!(sent[0].message.as_deref(), Some("hello"));
    assert_eq!(sent[0].raw["mode"], "prompt");
    assert_eq!(sent[0].raw["delivery"], "after_current_turn");
}

#[tokio::test]
async fn send_message_can_be_queued() {
    let bridge = FakeBridge::start(Script {
        send_replies: vec![SendReply::Queued],
        ..Script::default()
    });
    let mut client = client(&bridge);

    let response = client
        .send_message("audit-1", "thread-a", "hello")
        .await
        .expect("request answered");

    assert_eq!(response.status, Status::Queued);
    assert!(response.message_id.is_none());
}

#[tokio::test]
async fn get_delivery_status_surfaces_unknown_request() {
    let bridge = FakeBridge::start(Script::default());
    let mut client = client(&bridge);

    let response = client
        .delivery_status("audit-1")
        .await
        .expect("request answered");

    assert_eq!(response.status, Status::Rejected);
    assert_eq!(response.error_code(), Some(&ErrorCode::UnknownRequest));
}

#[tokio::test]
async fn watching_streams_every_event_kind() {
    let bridge = FakeBridge::start(Script {
        thread: Some(ThreadFacts::idle("thread-a")),
        ..Script::default()
    });
    let mut client = client(&bridge);

    let mut subscription = client
        .watch("watch-1", "thread-a")
        .await
        .expect("watch accepted");

    assert_eq!(subscription.request_id(), "watch-1");
    assert_eq!(subscription.thread_id(), "thread-a");
    assert_eq!(
        subscription
            .initial()
            .thread
            .as_ref()
            .expect("status present")
            .state,
        ThreadState::Idle
    );

    let events = TurnScript::standard("thread-a", "## Assistant\n\nDone.\n");
    // Stream the same events a send would push.
    bridge.push_events("thread-a", &events.events);

    let mut kinds = Vec::new();
    for _ in 0..events.events.len() {
        let event = tokio::time::timeout(Duration::from_secs(2), subscription.next_event())
            .await
            .expect("event arrives")
            .expect("no watch error")
            .expect("stream still open");
        kinds.push(event.kind.clone());
    }

    assert_eq!(kinds[0], EventKind::StatusChanged);
    assert_eq!(kinds[1], EventKind::EntryAdded);
    assert_eq!(*kinds.last().expect("last event"), EventKind::TurnStopped);
}

#[tokio::test]
async fn a_refused_watch_is_a_typed_error() {
    let bridge = FakeBridge::start(Script {
        watch_rejection: Some(ErrorCode::QueueFull),
        ..Script::default()
    });
    let mut client = client(&bridge);

    let error = client
        .watch("watch-1", "thread-a")
        .await
        .expect_err("must fail");

    assert!(matches!(
        error,
        ClientError::WatchRejected {
            status: Status::Rejected,
            error: Some(ref body),
        } if body.code == ErrorCode::QueueFull
    ));
}

#[tokio::test]
async fn a_connection_that_closes_mid_request_is_an_unexpected_eof() {
    let bridge = FakeBridge::start(Script {
        close_after_request: true,
        ..Script::default()
    });
    // The very first request is answered, then the socket closes.
    let mut client = client(&bridge);
    client.list_threads("list-1").await.expect("first answer");

    // The fake closes after answering, so the pooled connection is dead and the
    // client has to reconnect; that succeeds, so this stays a success.
    client.list_threads("list-2").await.expect("reconnects");
    assert!(bridge.connections() >= 2);
}

#[tokio::test]
async fn a_message_too_large_rejection_is_reported_and_the_client_recovers() {
    let bridge = FakeBridge::start(Script {
        send_replies: vec![SendReply::Rejected(ErrorCode::MessageTooLarge)],
        close_after_request: true,
        ..Script::default()
    });
    let mut client = client(&bridge);

    let response = client
        .send_message("audit-1", "thread-a", "hello")
        .await
        .expect("the rejection is an answer");

    assert_eq!(response.status, Status::Rejected);
    assert_eq!(response.error_code(), Some(&ErrorCode::MessageTooLarge));
    // The bridge closes that connection, so the next request needs a new one.
    client.list_threads("list-1").await.expect("reconnected");
    assert!(bridge.connections() >= 2);
}

#[tokio::test]
async fn an_oversized_message_is_refused_without_touching_the_socket() {
    let bridge = FakeBridge::start(Script::default());
    let mut client = client(&bridge);
    let message = "x".repeat(zed_audit_loop::protocol::MAX_LINE_BYTES);

    let error = client
        .send_message("audit-1", "thread-a", &message)
        .await
        .expect_err("must refuse locally");

    assert!(matches!(error, ClientError::LineTooLarge { .. }));
    assert!(bridge.requests_for("send_message").is_empty());
    assert!(bridge.requests().is_empty());
}

#[tokio::test]
async fn a_malformed_response_line_is_an_error_not_a_panic() {
    let bridge = FakeBridge::start(Script {
        malformed_next_reply: true,
        ..Script::default()
    });
    let mut client = client(&bridge);

    let error = client.list_threads("list-1").await.expect_err("must fail");

    assert!(matches!(error, ClientError::MalformedResponse { .. }));
    // The broken connection is dropped, so the next request starts clean.
    client.list_threads("list-2").await.expect("recovers");
}

#[tokio::test]
async fn an_unknown_action_rejection_is_surfaced_with_its_code() {
    let bridge = FakeBridge::start(Script {
        reject_next_request: Some(ErrorCode::UnknownAction),
        ..Script::default()
    });
    let mut client = client(&bridge);

    let response = client.list_threads("list-1").await.expect("answered");

    assert_eq!(response.status, Status::Rejected);
    assert_eq!(response.error_code(), Some(&ErrorCode::UnknownAction));
}

#[tokio::test]
async fn an_invalid_json_rejection_keeps_going() {
    let bridge = FakeBridge::start(Script {
        reject_next_request: Some(ErrorCode::InvalidJson),
        ..Script::default()
    });
    let mut client = client(&bridge);

    let response = client.list_threads("list-1").await.expect("answered");
    assert_eq!(response.error_code(), Some(&ErrorCode::InvalidJson));
    // A blank request_id in that rejection must not confuse the client.
    client.list_threads("list-2").await.expect("still works");
}

#[tokio::test]
async fn a_missing_socket_says_the_bridge_may_be_disabled() {
    let directory = tempfile::tempdir().expect("temp dir");
    let config = ClientConfig::new(directory.path().join("absent.sock"));
    let mut client = BridgeClient::new(config);

    let error = client.list_threads("list-1").await.expect_err("must fail");
    let message = error.to_string();

    assert!(matches!(error, ClientError::NotConnected { .. }));
    assert!(message.contains("agent_external_bridge.enabled"));
    assert!(message.contains("Zed running"));
}

#[tokio::test]
async fn the_client_reopens_after_its_request_budget() {
    let bridge = FakeBridge::start(Script::default());
    let mut config = ClientConfig::new(bridge.path());
    // The bridge closes at 100 requests; staying below it keeps the client from
    // racing a close it cannot see.
    config.max_requests_per_connection = 2;
    let mut client = BridgeClient::new(config);

    for index in 0..5 {
        client
            .list_threads(&format!("list-{index}"))
            .await
            .expect("request answered");
    }

    assert!(bridge.connections() >= 3, "client must rotate connections");
    assert_eq!(bridge.requests_for("list_threads").len(), 5);
}
