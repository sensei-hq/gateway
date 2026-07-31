//! Integration tests for the FLUX image-generation adapter using wiremock.
//!
//! FLUX is an async job adapter: `generate_image()` first POSTs a submit request
//! to `{base}/{model}` (default model `flux-pro-1.1`) authenticated with the
//! `x-key` header, then polls `GET {base}/get_result?id={id}` until the
//! returned `status` is `"Ready"`. Because `JobConfig::default()` uses a
//! 3-second poll interval and `poll_until_complete` only sleeps when a poll
//! returns "still processing", every mocked poll endpoint here returns a
//! terminal status on its FIRST response so the tests stay sub-second.

use kernel::types::io::ImageRequest;

use cloud_providers::flux::FluxAdapter;
use kernel::adapters::capability::ImageModel;

use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[macro_use]
mod common;
use common::{assert_is_provider_error, router_config};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const SAMPLE_URL: &str = "https://bfl.ai/output/generated-image.png";
const DEFAULT_MODEL: &str = "flux-pro-1.1";

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
// Submit error mappings — FLUX maps 401/403 → Authentication, 429 → RateLimit,
// 500 → ProviderError (distinct mapping, unlike the fal/kling/luma/runway/replicate
// adapters that route every submit status through ProviderError).
// ---------------------------------------------------------------------------

http_error_tests! {
    call: |config| async move {
        FluxAdapter::new().unwrap().generate_image(&config, &image_request()).await
    },
    method: "POST",
    path: format!("/{DEFAULT_MODEL}"),
    cases: {
        flux_submit_401_maps_to_authentication => (401, "invalid api key", common::ErrKind::Auth),
        flux_submit_403_maps_to_authentication => (403, "forbidden", common::ErrKind::Auth),
        flux_submit_429_maps_to_rate_limit => (429, "rate limited", common::ErrKind::RateLimit),
        flux_submit_500_maps_to_provider_error => (500, "internal server error", common::ErrKind::Provider(Some(500))),
    }
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
    assert_is_provider_error(&err);
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
    assert_is_provider_error(&err);
}
