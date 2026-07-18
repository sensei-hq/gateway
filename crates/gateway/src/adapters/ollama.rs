use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use reqwest::Client;

use super::base::build_client;
use super::openai_compat;
use crate::types::config::RouterConfig;
use crate::types::error::GatewayError;
use crate::types::io::{ChatRequest, ChatResponse};
use crate::types::request::StreamChunk;

// ---------------------------------------------------------------------------
// Helpers
//
// The chat / embed / streaming wire types + helpers live in the shared
// `openai_compat` module. Ollama speaks the OpenAI-compatible
// `/v1/chat/completions` and `/v1/embeddings` wire format, so this adapter's
// `ChatModel` / `EmbedModel` methods delegate straight to that core.
// ---------------------------------------------------------------------------

const DEFAULT_MODEL: &str = "gemma3:27b";

// ---------------------------------------------------------------------------
// OllamaAdapter
// ---------------------------------------------------------------------------

/// Adapter for Ollama's OpenAI-compatible inference endpoints.
///
/// Ollama exposes `/v1/chat/completions` and `/v1/embeddings` that follow the
/// OpenAI wire format, so no auth is typically required for local instances.
pub struct OllamaAdapter {
    client: Client,
}

/// Default per-request timeout when the adapter is built without explicit
/// config. A bare `reqwest::Client` has NO timeout, so a wedged Ollama
/// connection (accepted but never answered) hangs the caller forever; this
/// bounds it. Configured callers (`from_config`) override via
/// `RouterConfig::timeout_ms`.
const DEFAULT_TIMEOUT_SECS: u64 = 120;

impl OllamaAdapter {
    pub fn new() -> Result<Self, GatewayError> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .build()
            .map_err(|e| GatewayError::ProviderError {
                adapter: "ollama".into(),
                message: e.to_string(),
                status: None,
            })?;
        Ok(Self { client })
    }

    /// Create an adapter from a pre-built client (e.g. with timeout from config).
    pub fn from_config(config: &RouterConfig) -> Result<Self, GatewayError> {
        Ok(Self {
            client: build_client(config)?,
        })
    }
}

// ---------------------------------------------------------------------------
// Capability traits (target model). Traits + RegisterInto referenced by full path.
// ---------------------------------------------------------------------------

impl crate::adapters::capability::Model for OllamaAdapter {
    fn id(&self) -> &str {
        "ollama"
    }
}

#[async_trait]
impl crate::adapters::capability::ChatModel for OllamaAdapter {
    async fn chat(
        &self,
        config: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError> {
        // Ollama is keyless/local: delegate directly. The shared core's auth
        // is optional and sends no bearer when `resolve_api_key` yields none,
        // matching the previous behaviour.
        openai_compat::chat(&self.client, &config.url, DEFAULT_MODEL, config, req).await
    }

    async fn chat_stream(
        &self,
        config: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>
    {
        openai_compat::chat_stream(&self.client, &config.url, DEFAULT_MODEL, config, req).await
    }
}

#[async_trait]
impl crate::adapters::capability::EmbedModel for OllamaAdapter {
    async fn embed(
        &self,
        config: &RouterConfig,
        req: &crate::types::io::EmbedRequest,
    ) -> Result<crate::types::io::EmbedResponse, GatewayError> {
        openai_compat::embed(&self.client, &config.url, DEFAULT_MODEL, config, req).await
    }
}

#[async_trait]
impl crate::adapters::RegisterInto for OllamaAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &crate::adapters::AdapterRegistry) {
        reg.register_chat(self.clone()).await;
        reg.register_embed(self).await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::request::{Message, MessageRole};

    #[tokio::test]
    async fn embed_capability_times_out_against_a_silent_server() {
        use crate::adapters::capability::EmbedModel;
        // Same silent-server setup as the execute-path timeout test, but
        // driving the typed EmbedModel::embed method.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let mut held = Vec::new();
            for s in listener.incoming().flatten() {
                held.push(s);
            }
        });

        let config = RouterConfig {
            url: format!("http://{}", addr),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: Some(300),
            headers: std::collections::HashMap::new(),
        };
        let adapter = OllamaAdapter::from_config(&config).unwrap();
        let req = crate::types::io::EmbedRequest {
            model: Some("all-minilm".to_string()),
            texts: vec!["hello".to_string()],
        };

        let start = std::time::Instant::now();
        let result = adapter.embed(&config, &req).await;
        let elapsed = start.elapsed();
        assert!(result.is_err(), "silent server must error, not hang");
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "must time out promptly, took {elapsed:?}"
        );
    }

    #[test]
    fn ollama_id_and_supports() {
        let adapter = OllamaAdapter::new().unwrap();
        assert_eq!(crate::adapters::capability::Model::id(&adapter), "ollama");
    }

    #[tokio::test]
    #[ignore]
    async fn ollama_chat_integration() {
        use crate::adapters::capability::ChatModel;
        let adapter = OllamaAdapter::new().unwrap();
        let config = RouterConfig {
            url: "http://localhost:11434".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: Some(60000),
            headers: std::collections::HashMap::new(),
        };
        let req = crate::types::io::ChatRequest {
            model: Some("llama3.2:latest".to_string()),
            messages: vec![Message::text(
                MessageRole::User,
                "Say hello in one sentence.".to_string(),
            )],
            system: None,
            max_tokens: Some(64),
            temperature: Some(0.3),
            tools: Vec::new(),
        };

        let response = adapter.chat(&config, &req).await.unwrap();
        assert!(response.content.is_some());
        assert!(!response.content.unwrap().is_empty());
    }

    #[tokio::test]
    async fn embed_times_out_against_a_silent_server() {
        use crate::adapters::capability::EmbedModel;
        // A server that accepts the connection but never sends a response. A
        // no-timeout client (the old Client::new()) would hang here forever and
        // wedge the worker; with a per-request timeout the call must return an
        // error promptly instead. Uses std::net so no tokio "net" feature is
        // required.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let mut held = Vec::new();
            // Hold each accepted connection open, never write a response.
            for s in listener.incoming().flatten() {
                held.push(s);
            }
        });

        let config = RouterConfig {
            url: format!("http://{}", addr),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: Some(300),
            headers: std::collections::HashMap::new(),
        };
        let adapter = OllamaAdapter::from_config(&config).unwrap();
        let req = crate::types::io::EmbedRequest {
            model: Some("all-minilm".to_string()),
            texts: vec!["hello".to_string()],
        };

        let start = std::time::Instant::now();
        let result = adapter.embed(&config, &req).await;
        let elapsed = start.elapsed();
        assert!(
            result.is_err(),
            "a silent server must produce an error, not hang"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "request must time out promptly, took {elapsed:?}"
        );
    }
}
