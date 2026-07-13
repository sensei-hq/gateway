//! Integration tests for the Luma AI Dream Machine video-generation adapter
//! using wiremock.
//!
//! Luma is an async job adapter: `execute()` first POSTs a submit request to
//! `{base}/generations` (default model `ray-2`) authenticated with a bearer
//! `Authorization: Bearer <key>` header, then polls
//! `GET {base}/generations/{id}` until the returned `state` is `"completed"`.
//! Because `JobConfig::default()` uses a 3-second poll interval and
//! `poll_until_complete` only sleeps when a poll returns "still processing"
//! (`state` neither `completed` nor `failed`), every mocked poll endpoint here
//! returns a terminal `completed`/`failed` state on its FIRST response so the
//! tests stay sub-second.
//!
//! NOTE on error mappings: unlike the shared `base::http_json` helper, the Luma
//! adapter does NOT special-case 401/403 → Authentication or 429 → RateLimit
//! on the submit call. It maps EVERY non-success submit status to
//! `GatewayError::ProviderError` carrying `status: Some(<code>)`. The tests
//! below assert that ACTUAL behavior.

use std::collections::HashMap;

use gateway::types::capability::Capability;
use gateway::types::config::RouterConfig;
use gateway::types::error::GatewayError;
use gateway::types::request::{InferenceRequest, Message, MessageRole, Payload};

use gateway::adapters::InferenceAdapter;
use gateway::adapters::luma::LumaAdapter;

use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const SAMPLE_URL: &str = "https://storage.lumalabs.ai/dream-machine/video1.mp4";
const DEFAULT_MODEL: &str = "ray-2";

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
            prompt: "A timelapse of a city skyline at dusk".to_string(),
            duration_secs: Some(5),
            resolution: Some("1080p".to_string()),
        },
        budget: None,
    }
}

fn chat_request() -> InferenceRequest {
    InferenceRequest {
        capability: Capability::TextChat,
        model: Some("ray-2".to_string()),
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

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn luma_execute_happy_path() {
    let server = MockServer::start().await;

    // 1. Submit generation -> returns an id + a non-terminal state.
    Mock::given(method("POST"))
        .and(path("/generations"))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "gen-abc-123",
            "state": "queued"
        })))
        .mount(&server)
        .await;

    // 2. Poll -> returns a terminal "completed" state with the asset on the
    //    FIRST call so no 3s poll sleep is incurred.
    Mock::given(method("GET"))
        .and(path("/generations/gen-abc-123"))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "gen-abc-123",
            "state": "completed",
            "assets": {"video": SAMPLE_URL},
            "failure_reason": null
        })))
        .mount(&server)
        .await;

    let adapter = LumaAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let response = adapter.execute(&config, &request).await.unwrap();

    assert!(response.success);
    assert_eq!(response.model.as_deref(), Some(DEFAULT_MODEL));

    let videos = response.videos.expect("expected videos in response");
    assert_eq!(videos.len(), 1);
    assert_eq!(videos[0].url.as_deref(), Some(SAMPLE_URL));
    // duration_secs on the request was 5 -> surfaced as 5.0 on the result.
    assert!((videos[0].duration_secs.unwrap() - 5.0).abs() < f32::EPSILON);
}

// ---------------------------------------------------------------------------
// Submit error mappings
//
// The Luma adapter maps EVERY non-success submit status to ProviderError with
// the numeric status attached — it does NOT translate 401/403/429 to
// Authentication/RateLimit like the shared http_json helper.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn luma_submit_401_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/generations"))
        .respond_with(ResponseTemplate::new(401).set_body_string("invalid api key"))
        .mount(&server)
        .await;

    let adapter = LumaAdapter::new().unwrap();
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
async fn luma_submit_403_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/generations"))
        .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&server)
        .await;

    let adapter = LumaAdapter::new().unwrap();
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
async fn luma_submit_429_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/generations"))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        .mount(&server)
        .await;

    let adapter = LumaAdapter::new().unwrap();
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
async fn luma_submit_500_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/generations"))
        .respond_with(ResponseTemplate::new(500).set_body_string("internal server error"))
        .mount(&server)
        .await;

    let adapter = LumaAdapter::new().unwrap();
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

// ---------------------------------------------------------------------------
// Submit response body that fails to parse -> ProviderError
// ---------------------------------------------------------------------------

#[tokio::test]
async fn luma_submit_unparseable_body_maps_to_provider_error() {
    let server = MockServer::start().await;

    // 200 OK but the body is not a valid LumaGenerationResponse.
    Mock::given(method("POST"))
        .and(path("/generations"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json at all"))
        .mount(&server)
        .await;

    let adapter = LumaAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::ProviderError { .. }),
        "expected ProviderError from unparseable submit body, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// Poll failure state
// ---------------------------------------------------------------------------

#[tokio::test]
async fn luma_poll_failed_state_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/generations"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "gen-fail-1",
            "state": "queued"
        })))
        .mount(&server)
        .await;

    // First (and only) poll returns a terminal "failed" state carrying a
    // failure_reason, which is surfaced as the ProviderError message.
    Mock::given(method("GET"))
        .and(path("/generations/gen-fail-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "gen-fail-1",
            "state": "failed",
            "assets": null,
            "failure_reason": "content policy violation"
        })))
        .mount(&server)
        .await;

    let adapter = LumaAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::ProviderError { .. }),
        "expected ProviderError from failed poll, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// Poll HTTP error -> ProviderError
// ---------------------------------------------------------------------------

#[tokio::test]
async fn luma_poll_http_error_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/generations"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "gen-poll-500",
            "state": "queued"
        })))
        .mount(&server)
        .await;

    // Poll endpoint returns a non-success status; the adapter maps this to a
    // ProviderError (with status: None inside the poll closure).
    Mock::given(method("GET"))
        .and(path("/generations/gen-poll-500"))
        .respond_with(ResponseTemplate::new(500).set_body_string("poll failed"))
        .mount(&server)
        .await;

    let adapter = LumaAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::ProviderError { .. }),
        "expected ProviderError from failed poll HTTP status, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// Completed but no video asset -> success with a None url
// ---------------------------------------------------------------------------

#[tokio::test]
async fn luma_execute_completed_without_assets_returns_none_url() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/generations"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "gen-no-asset",
            "state": "queued"
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/generations/gen-no-asset"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "gen-no-asset",
            "state": "completed",
            "assets": null,
            "failure_reason": null
        })))
        .mount(&server)
        .await;

    let adapter = LumaAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let response = adapter.execute(&config, &request).await.unwrap();
    assert!(response.success);
    let videos = response.videos.expect("expected a videos vec");
    assert_eq!(videos.len(), 1);
    assert!(
        videos[0].url.is_none(),
        "expected no url when assets are absent, got: {:?}",
        videos[0].url,
    );
}

// ---------------------------------------------------------------------------
// Wrong payload type -> early return before any HTTP call
// ---------------------------------------------------------------------------

#[tokio::test]
async fn luma_wrong_payload_returns_provider_error() {
    let server = MockServer::start().await;

    // No mocks mounted: the adapter must return before making any HTTP call.
    let adapter = LumaAdapter::new().unwrap();
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
async fn luma_stream_returns_error() {
    let server = MockServer::start().await;

    let adapter = LumaAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let result = adapter.stream(&config, &request).await;
    assert!(result.is_err(), "luma stream() must return an error");
    let err = result.err().unwrap();
    assert!(
        matches!(err, GatewayError::ProviderError { .. }),
        "expected ProviderError from stream(), got: {err:?}",
    );
}
