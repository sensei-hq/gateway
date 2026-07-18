use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use reqwest::Client;

use super::base::{build_client, resolve_api_key};
use super::openai_compat;
use crate::types::config::RouterConfig;
use crate::types::error::GatewayError;
use crate::types::io::{ChatRequest, ChatResponse, EmbedRequest, EmbedResponse};
use crate::types::request::StreamChunk;

/// Default base URL — Hugging Face's OpenAI-compatible Inference **router**
/// (multi-provider, serverless, pay-as-you-go). Override via `RouterConfig.url`
/// to target a dedicated HF **Inference Endpoint** (same wire format).
const BASE_URL: &str = "https://router.huggingface.co/v1";

/// Fallback model only. The gateway injects the resolved model per request
/// (from config/chain or a caller pin), so this is essentially never used —
/// HF hosts thousands of models and has no meaningful universal default. Kept
/// so a bare, model-less direct call still resolves to something rather than
/// panicking; operators drive the real model through config.
const DEFAULT_MODEL: &str = "meta-llama/Llama-3.3-70B-Instruct";

/// Adapter for the Hugging Face Inference router (`router.huggingface.co/v1`) —
/// an OpenAI-compatible endpoint authenticated with a bearer HF token and
/// metered pay-as-you-go. Chat, streaming, tool-calling, and embeddings all
/// ride the shared [`openai_compat`] core.
///
/// Embeddings depend on the target model/provider exposing `/v1/embeddings`;
/// models without it return a provider error at call time.
pub struct HuggingFaceAdapter {
    client: Client,
}

impl HuggingFaceAdapter {
    pub fn new() -> Result<Self, GatewayError> {
        Ok(Self {
            client: Client::new(),
        })
    }

    /// Build from config (honours `RouterConfig::timeout_ms`).
    pub fn from_config(config: &RouterConfig) -> Result<Self, GatewayError> {
        Ok(Self {
            client: build_client(config)?,
        })
    }
}

/// Resolve the base URL: operator-supplied `config.url`, else the router default.
fn base_url(config: &RouterConfig) -> &str {
    let url = config.url.trim_end_matches('/');
    if url.is_empty() { BASE_URL } else { url }
}

/// HF requires a token — fail before any network I/O when none is present.
fn require_api_key(config: &RouterConfig) -> Result<String, GatewayError> {
    resolve_api_key(config).ok_or_else(|| GatewayError::Authentication {
        adapter: "huggingface".into(),
        message: "missing API key — set the env var specified in api_key_env".into(),
    })
}

impl crate::adapters::capability::Model for HuggingFaceAdapter {
    fn id(&self) -> &str {
        "huggingface"
    }
}

#[async_trait]
impl crate::adapters::capability::ChatModel for HuggingFaceAdapter {
    async fn chat(
        &self,
        config: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError> {
        require_api_key(config)?;
        openai_compat::chat(&self.client, base_url(config), DEFAULT_MODEL, config, req).await
    }

    async fn chat_stream(
        &self,
        config: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>
    {
        require_api_key(config)?;
        openai_compat::chat_stream(&self.client, base_url(config), DEFAULT_MODEL, config, req).await
    }
}

#[async_trait]
impl crate::adapters::capability::EmbedModel for HuggingFaceAdapter {
    async fn embed(
        &self,
        config: &RouterConfig,
        req: &EmbedRequest,
    ) -> Result<EmbedResponse, GatewayError> {
        require_api_key(config)?;
        openai_compat::embed(&self.client, base_url(config), DEFAULT_MODEL, config, req).await
    }
}

#[async_trait]
impl crate::adapters::RegisterInto for HuggingFaceAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &crate::adapters::AdapterRegistry) {
        reg.register_chat(self.clone()).await;
        reg.register_embed(self).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn cfg(url: &str, key: Option<&str>) -> RouterConfig {
        RouterConfig {
            url: url.to_string(),
            api_key_env: None,
            api_key: key.map(Into::into),
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        }
    }

    #[test]
    fn huggingface_id() {
        let a = HuggingFaceAdapter::new().unwrap();
        assert_eq!(crate::adapters::capability::Model::id(&a), "huggingface");
    }

    #[test]
    fn base_url_defaults_to_router_and_honours_override() {
        assert_eq!(base_url(&cfg("", None)), BASE_URL);
        assert_eq!(
            base_url(&cfg("https://my-endpoint.hf.space/v1/", None)),
            "https://my-endpoint.hf.space/v1"
        );
    }

    #[tokio::test]
    async fn chat_missing_api_key_returns_auth_error() {
        use crate::adapters::capability::ChatModel;
        let a = HuggingFaceAdapter::new().unwrap();
        let req = ChatRequest {
            model: Some("meta-llama/Llama-3.3-70B-Instruct".into()),
            messages: Vec::new(),
            system: None,
            max_tokens: None,
            temperature: None,
            tools: Vec::new(),
        };
        let err = a
            .chat(&cfg("http://localhost", None), &req)
            .await
            .unwrap_err();
        assert!(matches!(err, GatewayError::Authentication { .. }));
    }

    #[tokio::test]
    async fn embed_missing_api_key_returns_auth_error() {
        use crate::adapters::capability::EmbedModel;
        let a = HuggingFaceAdapter::new().unwrap();
        let req = EmbedRequest {
            model: Some("BAAI/bge-small-en-v1.5".into()),
            texts: vec!["hi".into()],
        };
        let err = a
            .embed(&cfg("http://localhost", None), &req)
            .await
            .unwrap_err();
        assert!(matches!(err, GatewayError::Authentication { .. }));
    }
}
