//! Integration tests for the Runway ML video-generation adapter using wiremock.
//!
//! Runway is an async job adapter: `execute()` first POSTs a submit request to
//! `{base}/tasks` (default model `gen-4`) authenticated with a bearer token,
//! then polls `GET {base}/tasks/{id}` until the returned `status` is
//! `"SUCCEEDED"`. Because `JobConfig::default()` uses a 3-second poll interval
//! and `poll_until_complete` only sleeps when a poll returns "still processing"
//! (`PENDING`/`RUNNING`), every mocked poll endpoint here returns a terminal
//! status on its FIRST response so the tests stay sub-second.
//!
//! NOTE on error mappings: unlike some adapters, the Runway adapter does NOT
//! route the submit path through the shared `http_json` helper. It checks
//! `resp.status().is_success()` directly and maps EVERY non-success submit
//! response — including 401/403 and 429 — to
//! `GatewayError::ProviderError { status: Some(code), .. }`. These tests assert
//! that ACTUAL behaviour rather than the generic auth/rate-limit mappings.

use std::collections::HashMap;

use gateway::types::capability::Capability;
use gateway::types::config::RouterConfig;
use gateway::types::error::GatewayError;
use gateway::types::request::{InferenceRequest, Message, MessageRole, Payload};

use gateway::adapters::InferenceAdapter;
use gateway::adapters::runway::RunwayAdapter;

use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const SAMPLE_URL: &str = "https://cdn.runwayml.com/output/generated-video.mp4";
const DEFAULT_MODEL: &str = "gen-4";
const TASK_ID: &str = "task-abc-123";

fn router_config(url: &str) -> RouterConfig {
    RouterConfig {
        url: url.to_string(),
        api_key: Some("test-key".into()),
        api_key_env: None,
        enabled: true,
        timeout_ms: Some(5000),
        headers: HashMap::new(),
    }
}

fn video_request() -> InferenceRequest {
    InferenceRequest {
        capability: Capability::VideoGenerate,
        model: None,
        router: None,
        chain: None,
        payload: Payload::VideoGenerate {
            prompt: "A timelapse of a blooming flower".to_string(),
            duration_secs: Some(5),
            resolution: Some("1080p".to_string()),
        },
        budget: None,
    }
}

fn chat_request() -> InferenceRequest {
    InferenceRequest {
        capability: Capability::TextChat,
        model: Some("gen-4".to_string()),
        router: None,
        chain: None,
        payload: Payload::Chat {
            messages: vec![Message::text(MessageRole::User, "hello")],
            system: None,
            max_tokens: Some(64),
            temperature: Some(0.5),
            tools: Vec::new(),
        },
        budget: None,
    }
}

/// Mount the submit endpoint returning a task id.
async fn mount_submit(server: &MockServer, task_id: &str) {
    Mock::given(method("POST"))
        .and(path("/tasks"))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({ "id": task_id })),
        )
        .mount(server)
        .await;
}

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn runway_execute_happy_path() {
    let server = MockServer::start().await;

    // 1. Submit task -> returns an id.
    mount_submit(&server, TASK_ID).await;

    // 2. Poll -> returns a terminal "SUCCEEDED" status on the FIRST call.
    Mock::given(method("GET"))
        .and(path(format!("/tasks/{TASK_ID}")))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "SUCCEEDED",
            "output": [SAMPLE_URL, "https://cdn.runwayml.com/output/second.mp4"],
            "failure": null
        })))
        .mount(&server)
        .await;

    let adapter = RunwayAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let response = adapter.execute(&config, &request).await.unwrap();

    assert!(response.success);
    assert_eq!(response.model.as_deref(), Some(DEFAULT_MODEL));

    let videos = response.videos.expect("expected videos in response");
    assert_eq!(videos.len(), 1);
    assert_eq!(videos[0].url.as_deref(), Some(SAMPLE_URL));
    // duration_secs (5u32) is carried through as an f32.
    assert!((videos[0].duration_secs.unwrap() - 5.0).abs() < f32::EPSILON);
}

// ---------------------------------------------------------------------------
// Submit error mappings — all non-success maps to ProviderError.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn runway_submit_401_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/tasks"))
        .respond_with(ResponseTemplate::new(401).set_body_string("invalid api key"))
        .mount(&server)
        .await;

    let adapter = RunwayAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(
            err,
            GatewayError::ProviderError {
                status: Some(401),
                ..
            }
        ),
        "expected ProviderError with status 401, got: {err:?}",
    );
}

#[tokio::test]
async fn runway_submit_403_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/tasks"))
        .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&server)
        .await;

    let adapter = RunwayAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(
            err,
            GatewayError::ProviderError {
                status: Some(403),
                ..
            }
        ),
        "expected ProviderError with status 403, got: {err:?}",
    );
}

#[tokio::test]
async fn runway_submit_429_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/tasks"))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        .mount(&server)
        .await;

    let adapter = RunwayAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(
            err,
            GatewayError::ProviderError {
                status: Some(429),
                ..
            }
        ),
        "expected ProviderError with status 429, got: {err:?}",
    );
}

#[tokio::test]
async fn runway_submit_500_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/tasks"))
        .respond_with(ResponseTemplate::new(500).set_body_string("internal server error"))
        .mount(&server)
        .await;

    let adapter = RunwayAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(
            err,
            GatewayError::ProviderError {
                status: Some(500),
                ..
            }
        ),
        "expected ProviderError with status 500, got: {err:?}",
    );
}

#[tokio::test]
async fn runway_submit_unparseable_body_maps_to_provider_error() {
    let server = MockServer::start().await;

    // Submit succeeds (200) but the body is not a valid RunwayTaskResponse,
    // exercising the JSON-parse error branch on the submit path.
    Mock::given(method("POST"))
        .and(path("/tasks"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&server)
        .await;

    let adapter = RunwayAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(
            err,
            GatewayError::ProviderError {
                status: Some(200),
                ..
            }
        ),
        "expected ProviderError from unparseable submit body, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// Poll-phase failures
// ---------------------------------------------------------------------------

#[tokio::test]
async fn runway_poll_failed_status_maps_to_provider_error() {
    let server = MockServer::start().await;

    mount_submit(&server, "task-fail-1").await;

    // First (and only) poll returns a terminal "FAILED" status with a
    // failure message; the adapter surfaces it as a ProviderError.
    Mock::given(method("GET"))
        .and(path("/tasks/task-fail-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "FAILED",
            "output": null,
            "failure": "content policy violation"
        })))
        .mount(&server)
        .await;

    let adapter = RunwayAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::ProviderError { status: None, .. }),
        "expected ProviderError from FAILED poll, got: {err:?}",
    );
}

#[tokio::test]
async fn runway_poll_http_error_maps_to_provider_error() {
    let server = MockServer::start().await;

    mount_submit(&server, "task-fail-2").await;

    // The poll endpoint itself returns a non-success HTTP status.
    Mock::given(method("GET"))
        .and(path("/tasks/task-fail-2"))
        .respond_with(ResponseTemplate::new(500).set_body_string("poll server error"))
        .mount(&server)
        .await;

    let adapter = RunwayAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::ProviderError { status: None, .. }),
        "expected ProviderError from failed poll HTTP status, got: {err:?}",
    );
}

#[tokio::test]
async fn runway_poll_unparseable_body_maps_to_provider_error() {
    let server = MockServer::start().await;

    mount_submit(&server, "task-fail-3").await;

    // Poll returns 200 but an un-parseable status body.
    Mock::given(method("GET"))
        .and(path("/tasks/task-fail-3"))
        .respond_with(ResponseTemplate::new(200).set_body_string("still not json"))
        .mount(&server)
        .await;

    let adapter = RunwayAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::ProviderError { status: None, .. }),
        "expected ProviderError from unparseable poll body, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// Happy path with no output URL — extraction yields None.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn runway_succeeded_without_output_yields_none_url() {
    let server = MockServer::start().await;

    mount_submit(&server, "task-empty").await;

    // Terminal SUCCEEDED but with an empty output list -> url is None.
    Mock::given(method("GET"))
        .and(path("/tasks/task-empty"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "SUCCEEDED",
            "output": [],
            "failure": null
        })))
        .mount(&server)
        .await;

    let adapter = RunwayAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let response = adapter.execute(&config, &request).await.unwrap();
    assert!(response.success);
    let videos = response.videos.expect("expected videos in response");
    assert_eq!(videos.len(), 1);
    assert!(videos[0].url.is_none());
}

// ---------------------------------------------------------------------------
// Wrong payload type -> early return before any HTTP call.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn runway_wrong_payload_returns_provider_error() {
    let server = MockServer::start().await;

    // No mocks mounted: the adapter must return before making any HTTP call.
    let adapter = RunwayAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = chat_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::ProviderError { status: None, .. }),
        "expected ProviderError for wrong payload, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// stream() is unsupported.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn runway_stream_returns_error() {
    let server = MockServer::start().await;

    let adapter = RunwayAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let result = adapter.stream(&config, &request).await;
    assert!(result.is_err(), "runway stream() must return an error");
    let err = result.err().unwrap();
    assert!(
        matches!(err, GatewayError::ProviderError { status: None, .. }),
        "expected ProviderError from stream(), got: {err:?}",
    );
}
