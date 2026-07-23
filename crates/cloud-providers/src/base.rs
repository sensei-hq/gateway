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

/// POST JSON to a provider endpoint, return parsed response.
pub async fn http_json<T: serde::de::DeserializeOwned>(
    client: &Client,
    base_url: &str,
    path: &str,
    body: &impl serde::Serialize,
    api_key: Option<&str>,
    extra_headers: &HashMap<String, String>,
) -> Result<T, GatewayError> {
    let url = format!("{}{}", base_url.trim_end_matches('/'), path);
    let mut req = client.post(&url).json(body);

    if let Some(key) = api_key {
        req = req.bearer_auth(key);
    }
    for (k, v) in extra_headers {
        req = req.header(k.as_str(), v.as_str());
    }

    let response = req.send().await?;
    let status = response.status();

    if !status.is_success() {
        let body_text = response.text().await.unwrap_or_default();
        let message = extract_error_message(&body_text).unwrap_or(body_text);

        if status.as_u16() == 429 {
            return Err(GatewayError::RateLimit {
                adapter: "http".into(),
                retry_after_ms: None,
            });
        }
        if status.as_u16() == 401 || status.as_u16() == 403 {
            return Err(GatewayError::Authentication {
                adapter: "http".into(),
                message,
            });
        }
        return Err(GatewayError::ProviderError {
            adapter: "http".into(),
            message,
            status: Some(status.as_u16()),
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
/// repeat: `401`/`403` → [`GatewayError::Authentication`], `429` →
/// [`GatewayError::RateLimit`], anything else → [`GatewayError::ProviderError`]
/// carrying the status code. Uses the raw body as the message (callers wanting
/// the JSON `error.message` extracted use [`http_json`]). Call only after
/// checking `!status.is_success()`.
pub async fn error_from_response(adapter: &str, response: reqwest::Response) -> GatewayError {
    let status = response.status().as_u16();
    let body_text = response.text().await.unwrap_or_default();
    map_status_error(adapter, status, body_text)
}

/// Pure mapping of a non-success HTTP status code + response body to a
/// [`GatewayError`]. Split from [`error_from_response`] so the mapping is
/// unit-testable without constructing a live [`reqwest::Response`].
fn map_status_error(adapter: &str, status: u16, body_text: String) -> GatewayError {
    match status {
        401 | 403 => GatewayError::Authentication {
            adapter: adapter.into(),
            message: body_text,
        },
        429 => GatewayError::RateLimit {
            adapter: adapter.into(),
            retry_after_ms: None,
        },
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
    fn map_status_error_maps_401_403_to_authentication() {
        for code in [401u16, 403] {
            match map_status_error("acme", code, "bad key".into()) {
                GatewayError::Authentication { adapter, message } => {
                    assert_eq!(adapter, "acme");
                    assert_eq!(message, "bad key");
                }
                other => panic!("expected Authentication for {code}, got {other:?}"),
            }
        }
    }

    #[test]
    fn map_status_error_maps_429_to_rate_limit() {
        match map_status_error("acme", 429, "slow down".into()) {
            GatewayError::RateLimit {
                adapter,
                retry_after_ms,
            } => {
                assert_eq!(adapter, "acme");
                assert_eq!(retry_after_ms, None);
            }
            other => panic!("expected RateLimit, got {other:?}"),
        }
    }

    #[test]
    fn map_status_error_maps_other_codes_to_provider_error() {
        match map_status_error("acme", 500, "boom".into()) {
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
