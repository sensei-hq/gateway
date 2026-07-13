//! Integration tests for the Kling video-generation adapter using wiremock.
//!
//! Kling is an async job adapter: `execute()` first POSTs a submit request to
//! `{base}/videos/text2video` authenticated with a bearer token, receiving
//! `{"data":{"task_id":"..."}}`, then polls
//! `GET {base}/videos/text2video/{task_id}` until the returned
//! `data.task_status` is `"succeed"`. Because `JobConfig::default()` uses a
//! 3-second poll interval and `poll_until_complete` only sleeps when a poll
//! returns "still processing", every mocked poll endpoint here returns a
//! terminal status on its FIRST response so the tests stay sub-second.
//!
//! NOTE: unlike the FLUX adapter (which routes submit errors through
//! `http_json` and thus maps 401/403 -> Authentication and 429 -> RateLimit),
//! Kling's `execute()` maps EVERY non-success submit status directly to
//! `GatewayError::ProviderError { status: Some(code), .. }`. These tests assert
//! that ACTUAL behavior.

use std::collections::HashMap;

use gateway::types::capability::Capability;
use gateway::types::config::RouterConfig;
use gateway::types::error::GatewayError;
use gateway::types::request::{InferenceRequest, Message, MessageRole, Payload};

use gateway::adapters::InferenceAdapter;
use gateway::adapters::kling::KlingAdapter;

use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const SAMPLE_URL: &str = "https://cdn.klingai.com/output/generated-video.mp4";
const DEFAULT_MODEL: &str = "kling-v2";
const SUBMIT_PATH: &str = "/videos/text2video";

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
            prompt: "a timelapse of a blooming flower".to_string(),
            duration_secs: Some(5),
            resolution: Some("1080p".to_string()),
        },
        budget: None,
    }
}

fn chat_request() -> InferenceRequest {
    InferenceRequest {
        capability: Capability::TextChat,
        model: Some("kling-v2".to_string()),
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

/// Mount the submit endpoint to return a task id with the given HTTP status.
async fn mount_submit_ok(server: &MockServer, task_id: &str) {
    Mock::given(method("POST"))
        .and(path(SUBMIT_PATH))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"data": {"task_id": task_id}})),
        )
        .mount(server)
        .await;
}

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn kling_execute_happy_path() {
    let server = MockServer::start().await;

    // 1. Submit task -> returns a task_id.
    mount_submit_ok(&server, "task-abc-123").await;

    // 2. Poll -> returns a terminal "succeed" status with the video on the
    //    FIRST call.
    Mock::given(method("GET"))
        .and(path(format!("{SUBMIT_PATH}/task-abc-123")))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": {
                "task_status": "succeed",
                "task_result": {
                    "videos": [
                        { "url": SAMPLE_URL, "duration": 5.0 }
                    ]
                }
            }
        })))
        .mount(&server)
        .await;

    let adapter = KlingAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let response = adapter.execute(&config, &request).await.unwrap();

    assert!(response.success);
    assert_eq!(response.model.as_deref(), Some(DEFAULT_MODEL));

    let videos = response.videos.expect("expected videos in response");
    assert_eq!(videos.len(), 1);
    assert_eq!(videos[0].url.as_deref(), Some(SAMPLE_URL));
    assert!((videos[0].duration_secs.unwrap() - 5.0).abs() < f32::EPSILON);
}

/// The `succeed` branch with an empty result must still succeed, falling back
/// to the requested duration and a `None` URL.
#[tokio::test]
async fn kling_execute_succeed_without_video_falls_back() {
    let server = MockServer::start().await;

    mount_submit_ok(&server, "task-no-video").await;

    Mock::given(method("GET"))
        .and(path(format!("{SUBMIT_PATH}/task-no-video")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": {
                "task_status": "succeed",
                "task_result": null
            }
        })))
        .mount(&server)
        .await;

    let adapter = KlingAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let response = adapter.execute(&config, &request).await.unwrap();

    assert!(response.success);
    let videos = response.videos.expect("expected a videos vec");
    assert_eq!(videos.len(), 1);
    assert!(videos[0].url.is_none());
    // Falls back to the requested duration (5s -> 5.0).
    assert!((videos[0].duration_secs.unwrap() - 5.0).abs() < f32::EPSILON);
}

// ---------------------------------------------------------------------------
// Submit error mappings
//
// Kling's execute() maps ALL non-success submit statuses directly to
// ProviderError { status: Some(code) }. There is no Authentication/RateLimit
// special-casing on this path.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn kling_submit_401_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(SUBMIT_PATH))
        .respond_with(ResponseTemplate::new(401).set_body_string("invalid api key"))
        .mount(&server)
        .await;

    let adapter = KlingAdapter::new().unwrap();
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
async fn kling_submit_403_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(SUBMIT_PATH))
        .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&server)
        .await;

    let adapter = KlingAdapter::new().unwrap();
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
async fn kling_submit_429_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(SUBMIT_PATH))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        .mount(&server)
        .await;

    let adapter = KlingAdapter::new().unwrap();
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
async fn kling_submit_500_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(SUBMIT_PATH))
        .respond_with(ResponseTemplate::new(500).set_body_string("internal server error"))
        .mount(&server)
        .await;

    let adapter = KlingAdapter::new().unwrap();
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

/// A 200 submit response whose body can't be parsed as the task envelope must
/// surface as a ProviderError.
#[tokio::test]
async fn kling_submit_unparseable_body_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(SUBMIT_PATH))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"unexpected": true})),
        )
        .mount(&server)
        .await;

    let adapter = KlingAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::ProviderError { .. }),
        "expected ProviderError parsing submit response, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// Poll-phase failures
// ---------------------------------------------------------------------------

/// The poll returns a terminal `"failed"` status -> mapped ProviderError.
#[tokio::test]
async fn kling_poll_failed_status_maps_to_provider_error() {
    let server = MockServer::start().await;

    mount_submit_ok(&server, "task-fail-1").await;

    Mock::given(method("GET"))
        .and(path(format!("{SUBMIT_PATH}/task-fail-1")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": {
                "task_status": "failed",
                "task_result": null
            }
        })))
        .mount(&server)
        .await;

    let adapter = KlingAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::ProviderError { .. }),
        "expected ProviderError from failed poll, got: {err:?}",
    );
}

/// A non-success HTTP status on the poll endpoint -> mapped ProviderError.
#[tokio::test]
async fn kling_poll_http_error_maps_to_provider_error() {
    let server = MockServer::start().await;

    mount_submit_ok(&server, "task-poll-500").await;

    Mock::given(method("GET"))
        .and(path(format!("{SUBMIT_PATH}/task-poll-500")))
        .respond_with(ResponseTemplate::new(500).set_body_string("poll blew up"))
        .mount(&server)
        .await;

    let adapter = KlingAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::ProviderError { .. }),
        "expected ProviderError from poll HTTP error, got: {err:?}",
    );
}

/// A 200 poll response whose body can't be parsed as the status envelope must
/// surface as a ProviderError.
#[tokio::test]
async fn kling_poll_unparseable_body_maps_to_provider_error() {
    let server = MockServer::start().await;

    mount_submit_ok(&server, "task-poll-garbage").await;

    Mock::given(method("GET"))
        .and(path(format!("{SUBMIT_PATH}/task-poll-garbage")))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&server)
        .await;

    let adapter = KlingAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::ProviderError { .. }),
        "expected ProviderError parsing poll status, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// Wrong payload type -> early return
// ---------------------------------------------------------------------------

#[tokio::test]
async fn kling_wrong_payload_returns_provider_error() {
    let server = MockServer::start().await;

    // No mocks mounted: the adapter must return before making any HTTP call.
    let adapter = KlingAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = chat_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::ProviderError { status: None, .. }),
        "expected ProviderError for wrong payload, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// stream() is unsupported
// ---------------------------------------------------------------------------

#[tokio::test]
async fn kling_stream_returns_error() {
    let server = MockServer::start().await;

    let adapter = KlingAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let result = adapter.stream(&config, &request).await;
    assert!(result.is_err(), "kling stream() must return an error");
    let err = result.err().unwrap();
    assert!(
        matches!(err, GatewayError::ProviderError { .. }),
        "expected ProviderError from stream(), got: {err:?}",
    );
}
