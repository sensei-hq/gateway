//! Shared wiremock scaffolding for the cloud-providers adapter mock tests.
//!
//! Each `tests/*_mock.rs` file is a separate integration-test binary that
//! exercises a different provider adapter (fal, flux, kling, luma, runway,
//! replicate, grok, together, …). They all build the same `RouterConfig`
//! shape and repeat the same handful of "mount an error response, then
//! assert the mapped `GatewayError` variant" patterns. This module factors
//! ONLY that truly-identical scaffolding — provider-specific wire quirks
//! (submit paths, poll bodies, status field names, which statuses map to
//! which error variant, etc.) stay in each `*_mock.rs` file where they
//! belong; see the `//! NOTE:` comments in those files for why the
//! differences are intentional.
//!
//! Not every helper here is used by every consumer, since each `*_mock.rs`
//! file only needs the subset that matches its adapter's behavior.

#![allow(dead_code)]

use std::collections::HashMap;

use kernel::types::config::RouterConfig;
use kernel::types::error::GatewayError;

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// `RouterConfig` with a literal `api_key` so `resolve_api_key` returns it
/// directly (no env var involved). This exact shape is the default fixture
/// for every provider mock test.
pub fn router_config(url: &str) -> RouterConfig {
    RouterConfig {
        url: url.to_string(),
        api_key: Some("test-key".into()),
        api_key_env: None,
        enabled: true,
        timeout_ms: Some(5000),
        headers: HashMap::new(),
    }
}

/// `RouterConfig` with neither a literal key nor an env var configured, so
/// `resolve_api_key` returns `None` and adapters should surface
/// `GatewayError::Authentication` before any HTTP call is made.
pub fn router_config_missing_key(url: &str) -> RouterConfig {
    RouterConfig {
        url: url.to_string(),
        api_key: None,
        api_key_env: None,
        enabled: true,
        timeout_ms: Some(5000),
        headers: HashMap::new(),
    }
}

/// Mount a single-shot `http_method` + `http_path` mock returning `status`
/// with a plain string body. Covers the many "submit/poll returns a 4xx/5xx"
/// error-mapping tests shared across providers, where the response body's
/// content doesn't matter — only the status code being mapped correctly.
pub async fn mount_status(
    server: &MockServer,
    http_method: &'static str,
    http_path: impl Into<String>,
    status: u16,
    body: &str,
) {
    Mock::given(method(http_method))
        .and(path(http_path))
        .respond_with(ResponseTemplate::new(status).set_body_string(body))
        .mount(server)
        .await;
}

/// Same as [`mount_status`], but with a JSON body — for adapters/tests that
/// specifically exercise a JSON error envelope.
pub async fn mount_status_json(
    server: &MockServer,
    http_method: &'static str,
    http_path: impl Into<String>,
    status: u16,
    body: serde_json::Value,
) {
    Mock::given(method(http_method))
        .and(path(http_path))
        .respond_with(ResponseTemplate::new(status).set_body_json(body))
        .mount(server)
        .await;
}

/// Assert `err` is `GatewayError::Authentication`.
#[track_caller]
pub fn assert_authentication_error(err: &GatewayError) {
    assert!(
        matches!(err, GatewayError::Authentication { .. }),
        "expected Authentication error, got: {err:?}",
    );
}

/// Assert `err` is `GatewayError::RateLimit`.
#[track_caller]
pub fn assert_rate_limit_error(err: &GatewayError) {
    assert!(
        matches!(err, GatewayError::RateLimit { .. }),
        "expected RateLimit error, got: {err:?}",
    );
}

/// Assert `err` is `GatewayError::ProviderError` (status unconstrained).
#[track_caller]
pub fn assert_is_provider_error(err: &GatewayError) {
    assert!(
        matches!(err, GatewayError::ProviderError { .. }),
        "expected ProviderError, got: {err:?}",
    );
}

/// Assert `err` is `GatewayError::ProviderError` carrying exactly `status`
/// (pass `None` for the poll-phase branches that don't attach one).
#[track_caller]
pub fn assert_provider_error_status(err: &GatewayError, status: Option<u16>) {
    match err {
        GatewayError::ProviderError { status: s, .. } => {
            assert_eq!(*s, status, "unexpected ProviderError status, got: {err:?}");
        }
        other => panic!("expected ProviderError, got: {other:?}"),
    }
}
