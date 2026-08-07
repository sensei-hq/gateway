use std::collections::HashMap;

use reqwest::Client;

use kernel::types::config::RouterConfig;
use kernel::types::error::GatewayError;

/// Build a reqwest Client for a router config with optional timeout.
pub fn build_client(config: &RouterConfig) -> Result<Client, GatewayError> {
    let mut builder = Client::builder();
    if let Some(timeout_ms) = config.timeout_ms {
        builder = builder.timeout(std::time::Duration::from_millis(timeout_ms));
    }
    builder.build().map_err(|e| GatewayError::ProviderError {
        adapter: "http".into(),
        message: e.to_string(),
        status: None,
    })
}

/// Resolve an API key for an adapter request.
///
/// Precedence:
///   1. `config.api_key` (literal — the daemon populates this after
///      reading the Keychain).
///   2. `config.api_key_env` (env var name — original behaviour).
///   3. None.
pub fn resolve_api_key(config: &RouterConfig) -> Option<String> {
    if let Some(literal) = config.api_key.as_ref() {
        return Some(literal.clone());
    }
    config
        .api_key_env
        .as_ref()
        .and_then(|env_var| std::env::var(env_var).ok())
}

/// [`resolve_api_key`], mapping a missing key to `GatewayError::Authentication`
/// tagged with `adapter`. Shared by every async-job / REST adapter whose only
/// auth failure mode at this point is "no key configured" — they call this
/// before building the submit request so a missing key short-circuits
/// without touching the wire.
pub fn require_api_key(config: &RouterConfig, adapter: &str) -> Result<String, GatewayError> {
    resolve_api_key(config).ok_or_else(|| GatewayError::Authentication {
        adapter: adapter.into(),
        message: "missing API key — set the env var specified in api_key_env".into(),
    })
}

/// `config.url` with a trailing slash trimmed, falling back to `default`
/// when the operator left `url` empty (e.g. the zero-value `RouterConfig`
/// adapters build in `new()`/tests). Shared by every adapter that targets a
/// fixed provider base URL but allows a per-router override.
pub fn base_url_or<'a>(config: &'a RouterConfig, default: &'a str) -> &'a str {
    let url = config.url.trim_end_matches('/');
    if url.is_empty() { default } else { url }
}

/// Target + auth for an [`http_json`] call. Groups the parameters that
/// always travel together (the endpoint a request is aimed at) so the
/// function itself only takes the `client` doing the work and the `body`
/// being sent, rather than five positional arguments.
pub struct JsonEndpoint<'a> {
    pub base_url: &'a str,
    pub path: &'a str,
    pub api_key: Option<&'a str>,
    pub extra_headers: &'a HashMap<String, String>,
}

/// POST JSON to a provider endpoint, return parsed response.
pub async fn http_json<T: serde::de::DeserializeOwned>(
    client: &Client,
    endpoint: JsonEndpoint<'_>,
    body: &impl serde::Serialize,
) -> Result<T, GatewayError> {
    let url = format!(
        "{}{}",
        endpoint.base_url.trim_end_matches('/'),
        endpoint.path
    );
    let mut req = client.post(&url).json(body);

    if let Some(key) = endpoint.api_key {
        req = req.bearer_auth(key);
    }
    for (k, v) in endpoint.extra_headers {
        req = req.header(k.as_str(), v.as_str());
    }

    let response = req.send().await?;
    let status = response.status();

    if !status.is_success() {
        let retry_after_ms = parse_retry_after_ms(
            response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
        );
        let body_text = response.text().await.unwrap_or_default();
        let message = extract_error_message(&body_text).unwrap_or(body_text);

        // Mirror `map_status_error`: 401 → Authentication, 429 → RateLimit
        // (threading Retry-After), and every other code — including 403 —
        // → ProviderError carrying the status, using the extracted message.
        return Err(match status.as_u16() {
            401 => GatewayError::Authentication {
                adapter: "http".into(),
                message,
            },
            429 => GatewayError::RateLimit {
                adapter: "http".into(),
                retry_after_ms,
            },
            code => GatewayError::ProviderError {
                adapter: "http".into(),
                message,
                status: Some(code),
            },
        });
    }

    response
        .json::<T>()
        .await
        .map_err(|e| GatewayError::ProviderError {
            adapter: "http".into(),
            message: format!("failed to parse response: {}", e),
            status: Some(status.as_u16()),
        })
}

/// Map a **non-success** provider HTTP response to a [`GatewayError`], tagged
/// with `adapter`, consuming the response to use its body as the error message.
///
/// Shared form of the status-mapping the adapters' streaming and multipart paths
/// repeat: `401` → [`GatewayError::Authentication`], `429` →
/// [`GatewayError::RateLimit`] (carrying any parsed `Retry-After`), and every
/// other code — **including `403`** — → [`GatewayError::ProviderError`] carrying
/// the status code, so the lockout classifier can disambiguate a 403 quota /
/// credits / forbidden from its body. Uses the raw body as the message (callers
/// wanting the JSON `error.message` extracted use [`http_json`]). Call only
/// after checking `!status.is_success()`.
pub async fn error_from_response(adapter: &str, response: reqwest::Response) -> GatewayError {
    let status = response.status().as_u16();
    let retry_after_ms = parse_retry_after_ms(
        response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok()),
    );
    let body_text = response.text().await.unwrap_or_default();
    map_status_error(adapter, status, body_text, retry_after_ms)
}

/// Parse a `Retry-After` header value (integer seconds) to milliseconds.
/// The HTTP-date form is not supported (returns `None`); callers then fall
/// back to the lockout policy's synthetic backoff.
fn parse_retry_after_ms(header: Option<&str>) -> Option<u64> {
    header?
        .trim()
        .parse::<u64>()
        .ok()
        .map(|secs| secs.saturating_mul(1000))
}

/// Pure mapping of a non-success HTTP status code + response body to a
/// [`GatewayError`]. Split from [`error_from_response`] so the mapping is
/// unit-testable without constructing a live [`reqwest::Response`].
fn map_status_error(
    adapter: &str,
    status: u16,
    body_text: String,
    retry_after_ms: Option<u64>,
) -> GatewayError {
    match status {
        401 => GatewayError::Authentication {
            adapter: adapter.into(),
            message: body_text,
        },
        429 => GatewayError::RateLimit {
            adapter: adapter.into(),
            retry_after_ms,
        },
        // 403 (and every other non-success) preserves the status + body so the
        // lockout classifier can disambiguate quota / credits / forbidden.
        code => GatewayError::ProviderError {
            adapter: adapter.into(),
            message: body_text,
            status: Some(code),
        },
    }
}

/// POST `body` as JSON with bearer auth and parse the response as `R`.
///
/// A non-success status maps to [`GatewayError::ProviderError`] tagged with
/// `adapter`, carrying the raw response body and the HTTP status; a JSON parse
/// failure maps the same way. This is the shared "submit" half of the async-job
/// (video) adapters — see [`get_json_bearer`] for the poll half.
pub async fn post_json_bearer<B, R>(
    client: &Client,
    url: &str,
    api_key: &str,
    adapter: &str,
    body: &B,
) -> Result<R, GatewayError>
where
    B: serde::Serialize,
    R: serde::de::DeserializeOwned,
{
    let resp = client
        .post(url)
        .json(body)
        .bearer_auth(api_key)
        .send()
        .await?;
    let status = resp.status();
    if !status.is_success() {
        let body_text = resp.text().await.unwrap_or_default();
        return Err(GatewayError::ProviderError {
            adapter: adapter.into(),
            message: body_text,
            status: Some(status.as_u16()),
        });
    }
    resp.json::<R>()
        .await
        .map_err(|e| GatewayError::ProviderError {
            adapter: adapter.into(),
            message: format!("failed to parse response: {e}"),
            status: Some(status.as_u16()),
        })
}

/// GET with bearer auth and parse the response as `R`.
///
/// A non-success status maps to [`GatewayError::ProviderError`] tagged with
/// `adapter` (raw body as the message, no HTTP status attached — matching the
/// poll-loop convention); a JSON parse failure maps the same way. This is the
/// shared "poll" half of the async-job adapters — see [`post_json_bearer`].
pub async fn get_json_bearer<R>(
    client: &Client,
    url: &str,
    api_key: &str,
    adapter: &str,
) -> Result<R, GatewayError>
where
    R: serde::de::DeserializeOwned,
{
    let resp = client.get(url).bearer_auth(api_key).send().await?;
    if !resp.status().is_success() {
        let body_text = resp.text().await.unwrap_or_default();
        return Err(GatewayError::ProviderError {
            adapter: adapter.into(),
            message: body_text,
            status: None,
        });
    }
    resp.json::<R>()
        .await
        .map_err(|e| GatewayError::ProviderError {
            adapter: adapter.into(),
            message: format!("failed to parse response: {e}"),
            status: None,
        })
}

/// Extract error message from various provider JSON error formats.
fn extract_error_message(body: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    // OpenAI/Anthropic: { "error": { "message": "..." } }
    v.get("error")
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .map(|s| s.to_string())
        // Fallback: { "error": "string" }
        .or_else(|| {
            v.get("error")
                .and_then(|e| e.as_str())
                .map(|s| s.to_string())
        })
        // FastAPI: { "detail": "..." }
        .or_else(|| {
            v.get("detail")
                .and_then(|d| d.as_str())
                .map(|s| s.to_string())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use kernel::types::config::RouterConfig;
    use std::collections::HashMap;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn fixture(api_key: Option<String>, env: Option<String>) -> RouterConfig {
        RouterConfig {
            url: "https://x".into(),
            api_key,
            api_key_env: env,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        }
    }

    #[test]
    fn literal_api_key_takes_precedence() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { std::env::set_var("RESOLVE_TEST_KEY", "from-env") };
        let cfg = fixture(Some("from-literal".into()), Some("RESOLVE_TEST_KEY".into()));
        assert_eq!(resolve_api_key(&cfg).as_deref(), Some("from-literal"));
        unsafe { std::env::remove_var("RESOLVE_TEST_KEY") };
    }

    #[test]
    fn falls_back_to_env_var_when_literal_absent() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { std::env::set_var("RESOLVE_TEST_FALLBACK", "from-env") };
        let cfg = fixture(None, Some("RESOLVE_TEST_FALLBACK".into()));
        assert_eq!(resolve_api_key(&cfg).as_deref(), Some("from-env"));
        unsafe { std::env::remove_var("RESOLVE_TEST_FALLBACK") };
    }

    #[test]
    fn returns_none_when_neither_source_has_a_key() {
        let _guard = ENV_LOCK.lock().unwrap();
        let cfg = fixture(None, Some("RESOLVE_TEST_MISSING".into()));
        assert_eq!(resolve_api_key(&cfg), None);
    }

    #[test]
    fn extract_error_message_openai_format() {
        let body = r#"{"error":{"message":"rate limit","type":"rate_limit_error"}}"#;
        assert_eq!(extract_error_message(body), Some("rate limit".to_string()),);
    }

    #[test]
    fn extract_error_message_string_format() {
        let body = r#"{"error":"bad request"}"#;
        assert_eq!(extract_error_message(body), Some("bad request".to_string()),);
    }

    #[test]
    fn extract_error_message_invalid_json() {
        let body = "not json";
        assert_eq!(extract_error_message(body), None);
    }

    #[test]
    fn map_status_error_maps_401_to_authentication_403_to_provider_error() {
        match map_status_error("acme", 401, "bad key".into(), None) {
            GatewayError::Authentication { adapter, message } => {
                assert_eq!(adapter, "acme");
                assert_eq!(message, "bad key");
            }
            other => panic!("expected Authentication for 401, got {other:?}"),
        }
        match map_status_error("acme", 403, "quota exceeded".into(), None) {
            GatewayError::ProviderError {
                adapter,
                message,
                status,
            } => {
                assert_eq!(adapter, "acme");
                assert_eq!(message, "quota exceeded");
                assert_eq!(status, Some(403));
            }
            other => panic!("expected ProviderError for 403, got {other:?}"),
        }
    }

    #[test]
    fn map_status_error_429_threads_retry_after() {
        match map_status_error("acme", 429, "slow down".into(), Some(1500)) {
            GatewayError::RateLimit {
                adapter,
                retry_after_ms,
            } => {
                assert_eq!(adapter, "acme");
                assert_eq!(retry_after_ms, Some(1500));
            }
            other => panic!("expected RateLimit, got {other:?}"),
        }
    }

    #[test]
    fn parse_retry_after_seconds_and_missing() {
        assert_eq!(parse_retry_after_ms(Some("2")), Some(2000));
        assert_eq!(parse_retry_after_ms(Some("0")), Some(0));
        // HTTP-date form unsupported → None (falls back to policy backoff)
        assert_eq!(parse_retry_after_ms(Some("not-a-number")), None);
        assert_eq!(parse_retry_after_ms(None), None);
        // Large-but-valid integer saturates on the * 1000 rather than
        // panicking (debug overflow-checks) or wrapping (release).
        assert_eq!(
            parse_retry_after_ms(Some("99999999999999999")),
            Some(u64::MAX)
        );
    }

    #[test]
    fn map_status_error_maps_other_codes_to_provider_error() {
        match map_status_error("acme", 500, "boom".into(), None) {
            GatewayError::ProviderError {
                adapter,
                message,
                status,
            } => {
                assert_eq!(adapter, "acme");
                assert_eq!(message, "boom");
                assert_eq!(status, Some(500));
            }
            other => panic!("expected ProviderError, got {other:?}"),
        }
    }
}
