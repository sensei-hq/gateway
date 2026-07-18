//! Integration tests for the Replicate adapter's typed capability methods
//! (`generate_video()` / `generate_image()`) using wiremock.
//!
//! `ReplicateAdapter` implements its own two-phase flow (create prediction
//! via POST `/predictions`, then poll GET `/predictions/{id}`) and — unlike
//! adapters that route through `base::http_json` — does NOT special-case
//! 401/403/429. Every non-success HTTP status on the create call therefore
//! surfaces as `GatewayError::ProviderError { status: Some(..) }`. The tests
//! below assert that actual behavior.
//!
//! Poll responses always return a *terminal* state ("succeeded" / "failed" /
//! "canceled") on the first poll so the 3-second `JobConfig::default()` poll
//! interval is never reached — tests stay sub-second.

use std::collections::HashMap;

use gateway::types::config::RouterConfig;
use gateway::types::error::GatewayError;
use gateway::types::io::{ImageRequest, VideoRequest};

use gateway::adapters::capability::{ImageModel, VideoModel};
use gateway::adapters::replicate::ReplicateAdapter;

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
        prompt: "A dog surfing a wave".to_string(),
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

/// A create-prediction POST response — the adapter only reads `id` here.
fn create_body(id: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "status": "starting",
        "output": null,
        "error": null,
    })
}

// ---------------------------------------------------------------------------
// Happy path — video
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replicate_video_succeeds() {
    let server = MockServer::start().await;

    // 1. Create prediction.
    Mock::given(method("POST"))
        .and(path("/predictions"))
        .respond_with(ResponseTemplate::new(201).set_body_json(create_body("pred-vid-1")))
        .mount(&server)
        .await;

    // 2. First (and only) poll returns a terminal "succeeded" state with
    //    an array output — extract_video_url takes the first element.
    let succeeded = serde_json::json!({
        "id": "pred-vid-1",
        "status": "succeeded",
        "output": ["https://replicate.delivery/video1.mp4"],
        "error": null,
    });
    Mock::given(method("GET"))
        .and(path("/predictions/pred-vid-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&succeeded))
        .mount(&server)
        .await;

    let adapter = ReplicateAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request(Some("tencent/hunyuan-video"));

    let response = adapter.generate_video(&config, &request).await.unwrap();
    let videos = response.videos;
    assert_eq!(videos.len(), 1);
    assert_eq!(
        videos[0].url.as_deref(),
        Some("https://replicate.delivery/video1.mp4"),
    );
    // duration_secs from the request (5) is echoed back as f32.
    assert_eq!(videos[0].duration_secs, Some(5.0));
}

#[tokio::test]
async fn replicate_video_succeeds_with_string_output() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/predictions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(create_body("pred-vid-2")))
        .mount(&server)
        .await;

    // String output — extract_video_url returns it directly.
    let succeeded = serde_json::json!({
        "id": "pred-vid-2",
        "status": "succeeded",
        "output": "https://replicate.delivery/single.mp4",
        "error": null,
    });
    Mock::given(method("GET"))
        .and(path("/predictions/pred-vid-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&succeeded))
        .mount(&server)
        .await;

    let adapter = ReplicateAdapter::new().unwrap();
    let config = router_config(&server.uri());
    // No model on the request -> DEFAULT_MODEL is used.
    let request = video_request(None);

    let response = adapter.generate_video(&config, &request).await.unwrap();
    let videos = response.videos;
    assert_eq!(
        videos[0].url.as_deref(),
        Some("https://replicate.delivery/single.mp4"),
    );
}

// ---------------------------------------------------------------------------
// Happy path — image
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replicate_image_succeeds() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/predictions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(create_body("pred-img-1")))
        .mount(&server)
        .await;

    let succeeded = serde_json::json!({
        "id": "pred-img-1",
        "status": "succeeded",
        "output": [
            "https://replicate.delivery/img1.png",
            "https://replicate.delivery/img2.png"
        ],
        "error": null,
    });
    Mock::given(method("GET"))
        .and(path("/predictions/pred-img-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&succeeded))
        .mount(&server)
        .await;

    let adapter = ReplicateAdapter::new().unwrap();
    let config = router_config(&server.uri());
    // No model -> image branch default "black-forest-labs/flux-schnell".
    let request = image_request(None);

    let response = adapter.generate_image(&config, &request).await.unwrap();
    let images = response.images;
    assert_eq!(images.len(), 2);
    assert_eq!(
        images[0].url.as_deref(),
        Some("https://replicate.delivery/img1.png"),
    );
    assert_eq!(
        images[1].url.as_deref(),
        Some("https://replicate.delivery/img2.png"),
    );
}

// ---------------------------------------------------------------------------
// Create-call HTTP error mapping — all become ProviderError with the status.
// (ReplicateAdapter does its own status handling; it does NOT map 401/403/429
//  to Authentication/RateLimit the way base::http_json does.)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replicate_create_401_is_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/predictions"))
        .respond_with(
            ResponseTemplate::new(401)
                .set_body_json(serde_json::json!({"detail": "invalid token"})),
        )
        .mount(&server)
        .await;

    let adapter = ReplicateAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request(Some("tencent/hunyuan-video"));

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    assert!(
        matches!(
            err,
            GatewayError::ProviderError {
                status: Some(401),
                ..
            }
        ),
        "expected ProviderError status 401, got: {err:?}",
    );
}

#[tokio::test]
async fn replicate_create_403_is_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/predictions"))
        .respond_with(
            ResponseTemplate::new(403).set_body_json(serde_json::json!({"detail": "forbidden"})),
        )
        .mount(&server)
        .await;

    let adapter = ReplicateAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request(Some("tencent/hunyuan-video"));

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    assert!(
        matches!(
            err,
            GatewayError::ProviderError {
                status: Some(403),
                ..
            }
        ),
        "expected ProviderError status 403, got: {err:?}",
    );
}

#[tokio::test]
async fn replicate_create_429_is_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/predictions"))
        .respond_with(
            ResponseTemplate::new(429).set_body_json(serde_json::json!({"detail": "rate limited"})),
        )
        .mount(&server)
        .await;

    let adapter = ReplicateAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request(Some("tencent/hunyuan-video"));

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    assert!(
        matches!(
            err,
            GatewayError::ProviderError {
                status: Some(429),
                ..
            }
        ),
        "expected ProviderError status 429, got: {err:?}",
    );
}

#[tokio::test]
async fn replicate_create_500_is_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/predictions"))
        .respond_with(
            ResponseTemplate::new(500)
                .set_body_json(serde_json::json!({"detail": "internal error"})),
        )
        .mount(&server)
        .await;

    let adapter = ReplicateAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request(Some("tencent/hunyuan-video"));

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    assert!(
        matches!(
            err,
            GatewayError::ProviderError {
                status: Some(500),
                ..
            }
        ),
        "expected ProviderError status 500, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// Poll-call errors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replicate_poll_failed_state_is_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/predictions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(create_body("pred-fail-1")))
        .mount(&server)
        .await;

    // Terminal "failed" state — the adapter surfaces `error` as the message.
    let failed = serde_json::json!({
        "id": "pred-fail-1",
        "status": "failed",
        "output": null,
        "error": "model crashed",
    });
    Mock::given(method("GET"))
        .and(path("/predictions/pred-fail-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&failed))
        .mount(&server)
        .await;

    let adapter = ReplicateAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request(Some("tencent/hunyuan-video"));

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    match err {
        GatewayError::ProviderError {
            status, message, ..
        } => {
            assert_eq!(status, None, "poll-side failures carry no status");
            assert_eq!(message, "model crashed");
        }
        other => panic!("expected ProviderError, got: {other:?}"),
    }
}

#[tokio::test]
async fn replicate_poll_canceled_state_is_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/predictions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(create_body("pred-cancel-1")))
        .mount(&server)
        .await;

    // Terminal "canceled" state with no `error` field -> default message.
    let canceled = serde_json::json!({
        "id": "pred-cancel-1",
        "status": "canceled",
        "output": null,
        "error": null,
    });
    Mock::given(method("GET"))
        .and(path("/predictions/pred-cancel-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&canceled))
        .mount(&server)
        .await;

    let adapter = ReplicateAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request(Some("tencent/hunyuan-video"));

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    match err {
        GatewayError::ProviderError {
            status, message, ..
        } => {
            assert_eq!(status, None);
            assert_eq!(message, "prediction failed");
        }
        other => panic!("expected ProviderError, got: {other:?}"),
    }
}

#[tokio::test]
async fn replicate_poll_http_error_is_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/predictions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(create_body("pred-poll-500")))
        .mount(&server)
        .await;

    // The poll GET itself returns a non-success status.
    Mock::given(method("GET"))
        .and(path("/predictions/pred-poll-500"))
        .respond_with(
            ResponseTemplate::new(500).set_body_json(serde_json::json!({"detail": "boom"})),
        )
        .mount(&server)
        .await;

    let adapter = ReplicateAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request(Some("tencent/hunyuan-video"));

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    // Poll-side HTTP failures are mapped with status: None.
    assert!(
        matches!(err, GatewayError::ProviderError { status: None, .. }),
        "expected ProviderError status None, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// Missing API key -> Authentication (require_api_key is the only Auth path).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replicate_missing_api_key_is_authentication_error() {
    let server = MockServer::start().await;

    let adapter = ReplicateAdapter::new().unwrap();
    let mut config = router_config(&server.uri());
    // Strip both key sources so resolve_api_key returns None.
    config.api_key = None;
    config.api_key_env = None;
    let request = video_request(Some("tencent/hunyuan-video"));

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::Authentication { .. }),
        "expected Authentication error, got: {err:?}",
    );
}
