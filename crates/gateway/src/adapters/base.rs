use std::collections::HashMap;

use reqwest::Client;

use crate::types::config::RouterConfig;
use crate::types::error::GatewayError;

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
    use crate::types::config::RouterConfig;
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
}
