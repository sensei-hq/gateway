//! Integration tests for the fal.ai adapter's typed capability methods
//! (`generate_video()` / `generate_image()`) using wiremock.
//!
//! These tests spin up an in-process HTTP mock server and verify that
//! `FalAdapter` correctly serialises requests, drives the submit ->
//! poll -> fetch-result async-job flow, and maps error conditions to
//! the exact `GatewayError` variants the adapter produces.
//!
//! Important behavioural notes derived from `src/adapters/fal.rs`:
//!   * fal uses `Authorization: Key {api_key}` (NOT Bearer / x-api-key).
//!   * Submit-phase HTTP failures (401/403/429/500) are ALL mapped
//!     inline to `GatewayError::ProviderError { status: Some(..) }` —
//!     fal does *not* special-case auth/rate-limit at submit time.
//!   * The poll endpoint must return a terminal status
//!     ("COMPLETED"/"FAILED") on the FIRST response, because
//!     `JobConfig::default()` uses a 3-second poll interval and a
//!     non-terminal first poll would sleep 3s before the second call.

use std::collections::HashMap;

use kernel::types::config::RouterConfig;
use kernel::types::error::GatewayError;
use kernel::types::io::{ImageRequest, VideoRequest};

use cloud_providers::fal::FalAdapter;
use kernel::adapters::capability::{ImageModel, Model, VideoModel};

use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// RouterConfig with a literal api_key so `resolve_api_key` returns it
/// directly (no env var involved).
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

fn video_request(model: Option<&str>) -> VideoRequest {
    VideoRequest {
        model: model.map(|m| m.to_string()),
        prompt: "A cat playing piano".to_string(),
        duration_secs: Some(5),
        resolution: None,
    }
}

fn image_request(model: Option<&str>) -> ImageRequest {
    ImageRequest {
        model: model.map(|m| m.to_string()),
        prompt: "A sunset over mountains".to_string(),
        size: None,
        quality: None,
        style: None,
        n: 1,
    }
}

// ---------------------------------------------------------------------------
// Video happy path — default model (fal-ai/veo3)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fal_video_generate_happy_path() {
    let server = MockServer::start().await;

    // 1. Submit -> queue response with request_id.
    Mock::given(method("POST"))
        .and(path("/fal-ai/veo3"))
        .and(header("authorization", "Key test-key"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"request_id": "req-video-1"})),
        )
        .mount(&server)
        .await;

    // 2. Poll status -> COMPLETED on the FIRST response (sub-second).
    Mock::given(method("GET"))
        .and(path("/requests/req-video-1/status"))
        .and(header("authorization", "Key test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "COMPLETED"
        })))
        .mount(&server)
        .await;

    // 3. Fetch result -> video url.
    Mock::given(method("GET"))
        .and(path("/requests/req-video-1"))
        .and(header("authorization", "Key test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "video": {"url": "https://fal.media/video1.mp4"}
        })))
        .mount(&server)
        .await;

    let adapter = FalAdapter::from_config(&router_config(&server.uri())).unwrap();
    let config = router_config(&server.uri());
    let request = video_request(None);

    let response = adapter.generate_video(&config, &request).await.unwrap();
    let videos = response.videos;
    assert_eq!(videos.len(), 1);
    assert_eq!(
        videos[0].url.as_deref(),
        Some("https://fal.media/video1.mp4"),
    );
    assert_eq!(videos[0].duration_secs, Some(5.0));
}

// ---------------------------------------------------------------------------
// Image happy path — default model (fal-ai/flux-pro/v1.1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fal_image_generate_happy_path() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/fal-ai/flux-pro/v1.1"))
        .and(header("authorization", "Key test-key"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"request_id": "req-image-1"})),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/requests/req-image-1/status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "COMPLETED"
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/requests/req-image-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "images": [{"url": "https://fal.media/image1.png"}]
        })))
        .mount(&server)
        .await;

    let adapter = FalAdapter::from_config(&router_config(&server.uri())).unwrap();
    let config = router_config(&server.uri());
    let request = image_request(None);

    let response = adapter.generate_image(&config, &request).await.unwrap();
    let images = response.images;
    assert_eq!(images.len(), 1);
    assert_eq!(
        images[0].url.as_deref(),
        Some("https://fal.media/image1.png"),
    );
    assert!(images[0].b64_json.is_none());
}

// ---------------------------------------------------------------------------
// Image happy path — explicit model override (verified via the POST path).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fal_image_generate_custom_model() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/fal-ai/recraft-v3"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"request_id": "req-image-2"})),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/requests/req-image-2/status"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "COMPLETED"})),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/requests/req-image-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "images": [{"url": "https://fal.media/image2.png"}]
        })))
        .mount(&server)
        .await;

    let adapter = FalAdapter::from_config(&router_config(&server.uri())).unwrap();
    let config = router_config(&server.uri());
    let request = image_request(Some("fal-ai/recraft-v3"));

    // The mock only matches `/fal-ai/recraft-v3`; a wrong model URL would
    // 404 and the call would error — so success proves the override is used.
    let response = adapter.generate_image(&config, &request).await.unwrap();
    assert_eq!(response.images.len(), 1);
}

// ---------------------------------------------------------------------------
// Missing API key -> Authentication error (no submit attempted)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fal_missing_api_key_authentication_error() {
    let server = MockServer::start().await;

    let config = RouterConfig {
        url: server.uri(),
        api_key: None,
        api_key_env: None,
        enabled: true,
        timeout_ms: Some(5000),
        headers: HashMap::new(),
    };
    let adapter = FalAdapter::from_config(&config).unwrap();
    let request = video_request(None);

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::Authentication { .. }),
        "expected Authentication error, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// Submit-phase HTTP errors — fal maps ALL of these to ProviderError
// (it does NOT special-case 401/403 -> Authentication or 429 ->
// RateLimit at submit time).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fal_submit_401_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/fal-ai/veo3"))
        .respond_with(
            ResponseTemplate::new(401)
                .set_body_json(serde_json::json!({"error": "invalid api key"})),
        )
        .mount(&server)
        .await;

    let adapter = FalAdapter::from_config(&router_config(&server.uri())).unwrap();
    let config = router_config(&server.uri());
    let request = video_request(None);

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
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
async fn fal_submit_403_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/fal-ai/veo3"))
        .respond_with(
            ResponseTemplate::new(403).set_body_json(serde_json::json!({"error": "forbidden"})),
        )
        .mount(&server)
        .await;

    let adapter = FalAdapter::from_config(&router_config(&server.uri())).unwrap();
    let config = router_config(&server.uri());
    let request = video_request(None);

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
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
async fn fal_submit_429_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/fal-ai/veo3"))
        .respond_with(
            ResponseTemplate::new(429).set_body_json(serde_json::json!({"error": "rate limited"})),
        )
        .mount(&server)
        .await;

    let adapter = FalAdapter::from_config(&router_config(&server.uri())).unwrap();
    let config = router_config(&server.uri());
    let request = video_request(None);

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
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
async fn fal_submit_500_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/fal-ai/veo3"))
        .respond_with(
            ResponseTemplate::new(500)
                .set_body_json(serde_json::json!({"error": "internal server error"})),
        )
        .mount(&server)
        .await;

    let adapter = FalAdapter::from_config(&router_config(&server.uri())).unwrap();
    let config = router_config(&server.uri());
    let request = video_request(None);

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
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
async fn fal_image_submit_500_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/fal-ai/flux-pro/v1.1"))
        .respond_with(
            ResponseTemplate::new(500).set_body_json(serde_json::json!({"error": "boom"})),
        )
        .mount(&server)
        .await;

    let adapter = FalAdapter::from_config(&router_config(&server.uri())).unwrap();
    let config = router_config(&server.uri());
    let request = image_request(None);

    let err = adapter.generate_image(&config, &request).await.unwrap_err();
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
// Submit returns a body that isn't a valid queue response -> ProviderError
// ("failed to parse queue response").
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fal_submit_bad_queue_body_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/fal-ai/veo3"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"not_request_id": "oops"})),
        )
        .mount(&server)
        .await;

    let adapter = FalAdapter::from_config(&router_config(&server.uri())).unwrap();
    let config = router_config(&server.uri());
    let request = video_request(None);

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    match err {
        GatewayError::ProviderError { message, .. } => {
            assert!(
                message.contains("failed to parse queue response"),
                "unexpected message: {message}",
            );
        }
        other => panic!("expected ProviderError, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Poll phase — status "FAILED" -> ProviderError ("fal.ai request failed").
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fal_poll_failed_status_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/fal-ai/veo3"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"request_id": "req-failed"})),
        )
        .mount(&server)
        .await;

    // First (and only) poll returns terminal FAILED.
    Mock::given(method("GET"))
        .and(path("/requests/req-failed/status"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "FAILED"})),
        )
        .mount(&server)
        .await;

    let adapter = FalAdapter::from_config(&router_config(&server.uri())).unwrap();
    let config = router_config(&server.uri());
    let request = video_request(None);

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    match err {
        GatewayError::ProviderError { message, .. } => {
            assert_eq!(message, "fal.ai request failed");
        }
        other => panic!("expected ProviderError, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Poll phase — non-200 status response -> ProviderError (status: None).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fal_poll_http_error_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/fal-ai/veo3"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"request_id": "req-poll-err"})),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/requests/req-poll-err/status"))
        .respond_with(
            ResponseTemplate::new(500).set_body_json(serde_json::json!({"error": "status boom"})),
        )
        .mount(&server)
        .await;

    let adapter = FalAdapter::from_config(&router_config(&server.uri())).unwrap();
    let config = router_config(&server.uri());
    let request = video_request(None);

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::ProviderError { status: None, .. }),
        "expected ProviderError with status None, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// Result phase — fetch returns non-200 -> ProviderError (status: Some).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fal_result_http_error_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/fal-ai/veo3"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"request_id": "req-result-err"})),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/requests/req-result-err/status"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "COMPLETED"})),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/requests/req-result-err"))
        .respond_with(
            ResponseTemplate::new(500).set_body_json(serde_json::json!({"error": "result boom"})),
        )
        .mount(&server)
        .await;

    let adapter = FalAdapter::from_config(&router_config(&server.uri())).unwrap();
    let config = router_config(&server.uri());
    let request = video_request(None);

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
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
// Result phase — result body has no `video` field -> a VideoResult whose
// url is None.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fal_video_result_missing_video_yields_none_url() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/fal-ai/veo3"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"request_id": "req-no-video"})),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/requests/req-no-video/status"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "COMPLETED"})),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/requests/req-no-video"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;

    let adapter = FalAdapter::from_config(&router_config(&server.uri())).unwrap();
    let config = router_config(&server.uri());
    let request = video_request(None);

    let response = adapter.generate_video(&config, &request).await.unwrap();
    let videos = response.videos;
    assert_eq!(videos.len(), 1);
    assert!(videos[0].url.is_none());
}

// ---------------------------------------------------------------------------
// Identity check through the capability-trait surface.
// ---------------------------------------------------------------------------

#[test]
fn fal_id() {
    let adapter = FalAdapter::new().unwrap();
    assert_eq!(Model::id(&adapter), "fal");
}
