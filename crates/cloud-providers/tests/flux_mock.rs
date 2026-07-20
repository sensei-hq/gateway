//! Integration tests for the FLUX image-generation adapter using wiremock.
//!
//! FLUX is an async job adapter: `generate_image()` first POSTs a submit request
//! to `{base}/{model}` (default model `flux-pro-1.1`) authenticated with the
//! `x-key` header, then polls `GET {base}/get_result?id={id}` until the
//! returned `status` is `"Ready"`. Because `JobConfig::default()` uses a
//! 3-second poll interval and `poll_until_complete` only sleeps when a poll
//! returns "still processing", every mocked poll endpoint here returns a
//! terminal status on its FIRST response so the tests stay sub-second.

use std::collections::HashMap;

use kernel::types::config::RouterConfig;
use kernel::types::error::GatewayError;
use kernel::types::io::ImageRequest;

use kernel::adapters::capability::ImageModel;
use cloud_providers::flux::FluxAdapter;

use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const SAMPLE_URL: &str = "https://bfl.ai/output/generated-image.png";
const DEFAULT_MODEL: &str = "flux-pro-1.1";

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

fn image_request() -> ImageRequest {
    ImageRequest {
        model: None,
        prompt: "a red fox in a snowy forest".to_string(),
        size: Some("512x512".to_string()),
        quality: None,
        style: None,
        n: 1,
    }
}

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flux_generate_image_happy_path() {
    let server = MockServer::start().await;

    // 1. Submit job -> returns an id.
    Mock::given(method("POST"))
        .and(path(format!("/{DEFAULT_MODEL}")))
        .and(header("x-key", "test-key"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": "task-abc-123"})),
        )
        .mount(&server)
        .await;

    // 2. Poll -> returns a terminal "Ready" result on the FIRST call.
    Mock::given(method("GET"))
        .and(path("/get_result"))
        .and(query_param("id", "task-abc-123"))
        .and(header("x-key", "test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "Ready",
            "result": {"sample": SAMPLE_URL}
        })))
        .mount(&server)
        .await;

    let adapter = FluxAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = image_request();

    let response = adapter.generate_image(&config, &request).await.unwrap();

    let images = response.images;
    assert_eq!(images.len(), 1);
    assert_eq!(images[0].url.as_deref(), Some(SAMPLE_URL));
}

// ---------------------------------------------------------------------------
// Submit error mappings
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flux_submit_401_maps_to_authentication() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(format!("/{DEFAULT_MODEL}")))
        .respond_with(ResponseTemplate::new(401).set_body_string("invalid api key"))
        .mount(&server)
        .await;

    let adapter = FluxAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = image_request();

    let err = adapter.generate_image(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::Authentication { .. }),
        "expected Authentication error, got: {err:?}",
    );
}

#[tokio::test]
async fn flux_submit_403_maps_to_authentication() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(format!("/{DEFAULT_MODEL}")))
        .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&server)
        .await;

    let adapter = FluxAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = image_request();

    let err = adapter.generate_image(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::Authentication { .. }),
        "expected Authentication error, got: {err:?}",
    );
}

#[tokio::test]
async fn flux_submit_429_maps_to_rate_limit() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(format!("/{DEFAULT_MODEL}")))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        .mount(&server)
        .await;

    let adapter = FluxAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = image_request();

    let err = adapter.generate_image(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::RateLimit { .. }),
        "expected RateLimit error, got: {err:?}",
    );
}

#[tokio::test]
async fn flux_submit_500_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(format!("/{DEFAULT_MODEL}")))
        .respond_with(ResponseTemplate::new(500).set_body_string("internal server error"))
        .mount(&server)
        .await;

    let adapter = FluxAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = image_request();

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
// Poll failure status
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flux_poll_error_status_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(format!("/{DEFAULT_MODEL}")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": "task-fail-1"})),
        )
        .mount(&server)
        .await;

    // First (and only) poll returns a terminal failure status.
    Mock::given(method("GET"))
        .and(path("/get_result"))
        .and(query_param("id", "task-fail-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "Error",
            "result": null
        })))
        .mount(&server)
        .await;

    let adapter = FluxAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = image_request();

    let err = adapter.generate_image(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::ProviderError { .. }),
        "expected ProviderError from failed poll, got: {err:?}",
    );
}

#[tokio::test]
async fn flux_poll_failed_status_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(format!("/{DEFAULT_MODEL}")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": "task-fail-2"})),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/get_result"))
        .and(query_param("id", "task-fail-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "Failed",
            "result": null
        })))
        .mount(&server)
        .await;

    let adapter = FluxAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = image_request();

    let err = adapter.generate_image(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::ProviderError { .. }),
        "expected ProviderError from failed poll, got: {err:?}",
    );
}
