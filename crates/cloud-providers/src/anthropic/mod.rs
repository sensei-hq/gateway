use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use futures::stream::StreamExt;
use reqwest::Client;

use crate::base::{build_client, error_from_response, resolve_api_key};
use kernel::types::config::RouterConfig;
use kernel::types::error::GatewayError;
use kernel::types::io::{ChatRequest, ChatResponse};
use kernel::types::request::{Message, MessageRole, StreamChunk};

mod convert;
use convert::{
    AnthropicRequest, AnthropicResponse, AnthropicStreamState, build_messages, build_tools,
    extract_system, extract_text, extract_tool_calls, process_stream_bytes, usage_from_anthropic,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const DEFAULT_MODEL: &str = "claude-haiku-4-5-20250414";
const ANTHROPIC_VERSION: &str = "2023-06-01";
const DEFAULT_MAX_TOKENS: u32 = 1024;

/// Header name for the Anthropic API version. Operators can override the
/// default [`ANTHROPIC_VERSION`] by setting this key in
/// `RouterConfig.headers` — so the version can be bumped via config
/// without a rebuild.
const ANTHROPIC_VERSION_HEADER: &str = "anthropic-version";

/// Header carrying Anthropic beta opt-ins. In OAuth (bearer) mode the oauth marker is sent
/// here; an operator can override it via `config.headers` (like the version).
const ANTHROPIC_BETA_HEADER: &str = "anthropic-beta";
/// Default beta marker for the Anthropic OAuth auth mode (overridable via `config.headers`).
const ANTHROPIC_OAUTH_BETA: &str = "oauth-2025-04-20";

/// Apply the fixed Anthropic headers plus any operator-supplied `config.headers` to an
/// outbound request builder.
///
/// The auth header depends on the credential kind (F3 OAuth — per the kernel credentials-channel
/// contract): an OAuth-marked value (`oauth:<token>`) is sent as `Authorization: Bearer <token>`
/// plus the [`ANTHROPIC_BETA_HEADER`] oauth marker; a plain api_key goes in `x-api-key`.
///
/// `anthropic-version` (and, in OAuth mode, `anthropic-beta`) default here but an operator can
/// override via `config.headers`; the resolved value wins and is skipped during the
/// `config.headers` pass so exactly one of each is emitted. Every other custom header is
/// forwarded verbatim, matching the openai/gemini/grok adapters.
fn apply_request_headers(
    builder: reqwest::RequestBuilder,
    credential: &str,
    config: &RouterConfig,
) -> reqwest::RequestBuilder {
    let anthropic_version = config
        .headers
        .get(ANTHROPIC_VERSION_HEADER)
        .map(String::as_str)
        .unwrap_or(ANTHROPIC_VERSION);
    let mut builder = builder
        .header(ANTHROPIC_VERSION_HEADER, anthropic_version)
        .header("content-type", "application/json");
    let oauth_mode = match kernel::oauth_token(credential) {
        Some(token) => {
            let beta = config
                .headers
                .get(ANTHROPIC_BETA_HEADER)
                .map(String::as_str)
                .unwrap_or(ANTHROPIC_OAUTH_BETA);
            builder = builder
                .header("authorization", format!("Bearer {token}"))
                .header(ANTHROPIC_BETA_HEADER, beta);
            true
        }
        None => {
            builder = builder.header("x-api-key", credential);
            false
        }
    };
    for (k, v) in &config.headers {
        // Skip headers already applied above (caller value won) so none is emitted twice.
        if k.eq_ignore_ascii_case(ANTHROPIC_VERSION_HEADER) {
            continue;
        }
        if oauth_mode && k.eq_ignore_ascii_case(ANTHROPIC_BETA_HEADER) {
            continue;
        }
        builder = builder.header(k.as_str(), v.as_str());
    }
    builder
}

// ---------------------------------------------------------------------------
// AnthropicAdapter
// ---------------------------------------------------------------------------

/// Adapter for the Anthropic Messages API.
///
/// Supports chat completions via `POST /v1/messages`. Anthropic does not expose
/// an embedding endpoint, so only the Chat capability is supported.
pub struct AnthropicAdapter {
    client: Client,
}

impl AnthropicAdapter {
    pub fn new() -> Result<Self, GatewayError> {
        Ok(Self {
            client: Client::new(),
        })
    }

    /// Create an adapter from a pre-built client (e.g. with timeout from config).
    pub fn from_config(config: &RouterConfig) -> Result<Self, GatewayError> {
        Ok(Self {
            client: build_client(config)?,
        })
    }
}

// ---------------------------------------------------------------------------
// Capability traits (target model). Traits + RegisterInto referenced by full path. The
// SSE pipeline (process_stream_bytes / process_sse_line / AnthropicStreamState)
// is shared verbatim with the legacy stream() method above.
// ---------------------------------------------------------------------------

impl kernel::adapters::capability::Model for AnthropicAdapter {
    fn id(&self) -> &str {
        "anthropic"
    }
}

#[async_trait]
impl kernel::adapters::capability::ChatModel for AnthropicAdapter {
    async fn chat(
        &self,
        config: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError> {
        let api_key = resolve_api_key(config).ok_or_else(|| GatewayError::Authentication {
            adapter: "anthropic".into(),
            message: "missing API key — set the env var specified in api_key_env".into(),
        })?;

        let model = req
            .model
            .clone()
            .unwrap_or_else(|| DEFAULT_MODEL.to_string());
        let extracted_system = extract_system(&req.messages, &req.system);

        let body = AnthropicRequest {
            model: model.clone(),
            messages: build_messages(&req.messages),
            max_tokens: req.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
            system: extracted_system,
            temperature: req.temperature,
            stream: false,
            tools: build_tools(&req.tools),
        };

        let url = format!("{}/v1/messages", config.url.trim_end_matches('/'));
        let resp = apply_request_headers(self.client.post(&url), &api_key, config)
            .json(&body)
            .send()
            .await?;

        let status = resp.status();

        if !status.is_success() {
            return Err(error_from_response("anthropic", resp).await);
        }

        let anthropic_resp: AnthropicResponse =
            resp.json().await.map_err(|e| GatewayError::ProviderError {
                adapter: "anthropic".into(),
                message: format!("failed to parse response: {}", e),
                status: Some(status.as_u16()),
            })?;

        let content = extract_text(&anthropic_resp.content);
        let tool_calls = extract_tool_calls(&anthropic_resp.content);
        let usage = usage_from_anthropic(&anthropic_resp.usage);

        Ok(ChatResponse {
            content: if content.is_empty() {
                None
            } else {
                Some(content)
            },
            tool_calls,
            usage: Some(usage),
            model: Some(model),
            degraded: false,
        })
    }

    async fn chat_stream(
        &self,
        config: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>
    {
        let api_key = resolve_api_key(config).ok_or_else(|| GatewayError::Authentication {
            adapter: "anthropic".into(),
            message: "missing API key — set the env var specified in api_key_env".into(),
        })?;

        let model = req
            .model
            .clone()
            .unwrap_or_else(|| DEFAULT_MODEL.to_string());
        let extracted_system = extract_system(&req.messages, &req.system);

        let body = AnthropicRequest {
            model,
            messages: build_messages(&req.messages),
            max_tokens: req.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
            system: extracted_system,
            temperature: req.temperature,
            stream: true,
            // Streaming + tool calling is deferred — Anthropic emits
            // `input_json_delta` events for tool arguments that need
            // accumulation in the stream layer. v1 ships tools through
            // execute() only.
            tools: Vec::new(),
        };

        let url = format!("{}/v1/messages", config.url.trim_end_matches('/'));
        let response = apply_request_headers(self.client.post(&url), &api_key, config)
            .json(&body)
            .send()
            .await?;

        let status = response.status();

        if !status.is_success() {
            return Err(error_from_response("anthropic", response).await);
        }

        let byte_stream: Pin<Box<dyn Stream<Item = _> + Send>> = Box::pin(response.bytes_stream());
        let initial = AnthropicStreamState {
            byte_stream,
            line_buf: String::new(),
            tool_calls: BTreeMap::new(),
            pending: VecDeque::new(),
            eof: false,
        };

        let stream = futures::stream::unfold(initial, |mut state| async move {
            loop {
                if let Some(item) = state.pending.pop_front() {
                    return Some((item, state));
                }
                if state.eof {
                    return None;
                }
                match state.byte_stream.next().await {
                    Some(Ok(bytes)) => process_stream_bytes(&mut state, &bytes),
                    Some(Err(e)) => {
                        state.pending.push_back(Err(GatewayError::ProviderError {
                            adapter: "anthropic".into(),
                            message: format!("anthropic stream error: {e}"),
                            status: None,
                        }));
                        state.eof = true;
                    }
                    None => state.eof = true,
                }
            }
        });

        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl kernel::adapters::RegisterInto for AnthropicAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &kernel::adapters::AdapterRegistry) {
        reg.register_chat(self).await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anthropic_id_and_supports() {
        let adapter = AnthropicAdapter::new().unwrap();
        assert_eq!(
            kernel::adapters::capability::Model::id(&adapter),
            "anthropic"
        );
    }

    fn config_with_headers(headers: std::collections::HashMap<String, String>) -> RouterConfig {
        RouterConfig {
            url: "https://api.anthropic.com".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers,
        }
    }

    #[test]
    fn apply_request_headers_forwards_custom_headers_and_defaults_version() {
        let headers =
            std::collections::HashMap::from([("x-custom".to_string(), "abc".to_string())]);
        let config = config_with_headers(headers);
        let req = apply_request_headers(
            Client::new().post("https://api.anthropic.com/v1/messages"),
            "sk-test",
            &config,
        )
        .build()
        .unwrap();
        let h = req.headers();
        assert_eq!(h.get("x-api-key").unwrap(), "sk-test");
        assert_eq!(h.get("content-type").unwrap(), "application/json");
        // The custom header reaches the outbound request.
        assert_eq!(h.get("x-custom").unwrap(), "abc");
        // No override present → the const fallback is used.
        assert_eq!(h.get("anthropic-version").unwrap(), ANTHROPIC_VERSION);
    }

    #[test]
    fn apply_request_headers_uses_bearer_for_an_oauth_credential() {
        // An oauth-marked credential authenticates via Authorization: Bearer + the oauth beta
        // marker, and does NOT send x-api-key. (F3 OAuth — O-1.)
        let config = config_with_headers(std::collections::HashMap::new());
        let req = apply_request_headers(
            Client::new().post("https://api.anthropic.com/v1/messages"),
            "oauth:tok-XYZ",
            &config,
        )
        .build()
        .unwrap();
        let h = req.headers();
        assert!(
            h.get("x-api-key").is_none(),
            "oauth mode must not send x-api-key"
        );
        assert_eq!(h.get("authorization").unwrap(), "Bearer tok-XYZ");
        assert_eq!(h.get("anthropic-beta").unwrap(), ANTHROPIC_OAUTH_BETA);
        // A plain api_key still uses x-api-key and no bearer.
        let req2 = apply_request_headers(
            Client::new().post("https://api.anthropic.com/v1/messages"),
            "sk-ant-static",
            &config,
        )
        .build()
        .unwrap();
        assert_eq!(req2.headers().get("x-api-key").unwrap(), "sk-ant-static");
        assert!(req2.headers().get("authorization").is_none());
    }

    #[test]
    fn apply_request_headers_lets_config_override_the_anthropic_version() {
        let headers = std::collections::HashMap::from([
            ("anthropic-version".to_string(), "2099-01-01".to_string()),
            ("x-extra".to_string(), "1".to_string()),
        ]);
        let config = config_with_headers(headers);
        let req = apply_request_headers(
            Client::new().post("https://api.anthropic.com/v1/messages"),
            "sk-test",
            &config,
        )
        .build()
        .unwrap();
        let h = req.headers();
        // The operator's version wins over the compiled-in default…
        assert_eq!(h.get("anthropic-version").unwrap(), "2099-01-01");
        // …and exactly one anthropic-version header is emitted (no dupe).
        assert_eq!(h.get_all("anthropic-version").iter().count(), 1);
        assert_eq!(h.get("x-extra").unwrap(), "1");
    }

    #[test]
    fn anthropic_capability_model_id() {
        let adapter = AnthropicAdapter::new().unwrap();
        // Reference `Model::id` by full path
        // the capability Model trait.
        assert_eq!(
            kernel::adapters::capability::Model::id(&adapter),
            "anthropic"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn anthropic_chat_integration() {
        use kernel::adapters::capability::ChatModel;
        // Requires ANTHROPIC_API_KEY env var
        let adapter = AnthropicAdapter::new().unwrap();
        let config = RouterConfig {
            url: "https://api.anthropic.com".to_string(),
            api_key_env: Some("ANTHROPIC_API_KEY".to_string()),
            api_key: None,
            enabled: true,
            timeout_ms: Some(30000),
            headers: std::collections::HashMap::new(),
        };
        let req = kernel::types::io::ChatRequest {
            model: Some("claude-haiku-4-5-20250414".to_string()),
            messages: vec![Message::text(
                MessageRole::User,
                "Say hello in one sentence.",
            )],
            system: None,
            max_tokens: Some(64),
            temperature: Some(0.3),
            tools: Vec::new(),
        };

        let response = adapter.chat(&config, &req).await.unwrap();
        assert!(response.content.is_some());
        assert!(!response.content.unwrap().is_empty());
        assert!(response.usage.is_some());
    }
}
