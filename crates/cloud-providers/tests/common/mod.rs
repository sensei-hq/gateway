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

/// Which `GatewayError` variant a mapped HTTP status is expected to produce — the one
/// axis that genuinely varies per provider (flux/grok/together map 401/403→Authentication,
/// 429→RateLimit; the async-job adapters fal/kling/luma/runway/replicate route every submit
/// status through `ProviderError`). Passed to [`assert_http_error`] / the
/// [`http_error_tests!`](crate::http_error_tests) macro so the shared harness asserts the
/// right variant.
pub enum ErrKind {
    Auth,
    RateLimit,
    /// `ProviderError` carrying exactly this status (`None` = no status attached).
    Provider(Option<u16>),
    /// `ProviderError` with any/unspecified status.
    ProviderAny,
}

/// Assert `err` matches the expected [`ErrKind`].
#[track_caller]
pub fn assert_err_kind(err: &GatewayError, kind: &ErrKind) {
    match kind {
        ErrKind::Auth => assert_authentication_error(err),
        ErrKind::RateLimit => assert_rate_limit_error(err),
        ErrKind::Provider(status) => assert_provider_error_status(err, *status),
        ErrKind::ProviderAny => assert_is_provider_error(err),
    }
}

/// One mounted error response for [`assert_http_error`]: the `method`+`path` to intercept and
/// the `status`+`body` to return. Groups the four "which error to mock" fields so the harness
/// takes a spec rather than a long positional parameter list.
pub struct HttpErrorCase {
    pub method: &'static str,
    pub path: String,
    pub status: u16,
    pub body: &'static str,
}

/// The shared body of every "mount an error status at one endpoint, call the adapter, assert
/// the mapped `GatewayError`" test: start a server, mount `case` (`status` at `method`+`path`),
/// run `call` against a [`router_config`] pointed at it, and assert the error is `kind`. `call`
/// receives the config and returns the adapter's own result (generic over the Ok type, which
/// is discarded — only the error is checked). Drives the [`http_error_tests!`] macro.
pub async fn assert_http_error<T, F, Fut>(case: HttpErrorCase, kind: ErrKind, call: F)
where
    F: FnOnce(RouterConfig) -> Fut,
    Fut: std::future::Future<Output = Result<T, GatewayError>>,
{
    let server = MockServer::start().await;
    mount_status(&server, case.method, case.path, case.status, case.body).await;
    let err = match call(router_config(&server.uri())).await {
        Ok(_) => panic!("expected a GatewayError, got Ok"),
        Err(e) => e,
    };
    assert_err_kind(&err, &kind);
}

/// Generate the standard per-status HTTP-error-mapping tests for a provider adapter from one
/// spec, instead of hand-writing four near-identical `#[tokio::test]`s per provider. Each case
/// keeps its own provider-prefixed function name (e.g. `flux_submit_401_maps_to_authentication`)
/// so a failure still names the provider, and the status→variant mapping is explicit per case
/// (the genuine per-provider difference). `call` is a closure `|RouterConfig| -> impl Future`
/// that builds the adapter + request and invokes the capability under test.
///
/// ```ignore
/// http_error_tests! {
///     call: |cfg| async move { FluxAdapter::new().unwrap().generate_image(&cfg, &image_request()).await },
///     method: "POST",
///     path: format!("/{DEFAULT_MODEL}"),
///     cases: {
///         flux_submit_401_maps_to_authentication => (401, "invalid api key", common::ErrKind::Auth),
///         flux_submit_500_maps_to_provider_error => (500, "boom", common::ErrKind::Provider(Some(500))),
///     }
/// }
/// ```
#[macro_export]
macro_rules! http_error_tests {
    (
        call: $call:expr,
        method: $method:literal,
        path: $path:expr,
        cases: { $( $name:ident => ($status:literal, $body:literal, $kind:expr) ),+ $(,)? }
    ) => {
        $(
            #[tokio::test]
            async fn $name() {
                common::assert_http_error(
                    common::HttpErrorCase {
                        method: $method,
                        path: ($path).into(),
                        status: $status,
                        body: $body,
                    },
                    $kind,
                    $call,
                )
                .await;
            }
        )+
    };
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

/// RAII guard that sets an env var for the life of the guard and removes it
/// on drop — even if the test panics partway through. Replaces the
/// hand-paired `unsafe { std::env::set_var(..) }` / `unsafe {
/// std::env::remove_var(..) }` bookends that adapter tests use to exercise
/// the `api_key_env` resolution path without leaking test env vars into
/// other tests.
pub struct EnvVarGuard {
    key: &'static str,
}

impl EnvVarGuard {
    /// Set `key=value` for the current process, returning a guard that
    /// removes it again on drop.
    pub fn set(key: &'static str, value: &str) -> Self {
        // SAFETY: test-only; the mock tests that use this run single env
        // vars that are unique per test (`__TEST_*_KEY_*__`), so concurrent
        // mutation of the *same* key from other threads isn't a concern.
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        // SAFETY: see `set` above.
        unsafe {
            std::env::remove_var(self.key);
        }
    }
}
