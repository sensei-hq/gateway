use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use reqwest::Client;

use crate::base::build_client;
use crate::openai_compat;
use kernel::types::config::RouterConfig;
use kernel::types::error::GatewayError;
use kernel::types::io::{ChatRequest, ChatResponse};
use kernel::types::request::StreamChunk;

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

impl kernel::adapters::capability::Model for OllamaAdapter {
    fn id(&self) -> &str {
        "ollama"
    }
}

#[async_trait]
impl kernel::adapters::capability::ChatModel for OllamaAdapter {
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
impl kernel::adapters::capability::EmbedModel for OllamaAdapter {
    async fn embed(
        &self,
        config: &RouterConfig,
        req: &kernel::types::io::EmbedRequest,
    ) -> Result<kernel::types::io::EmbedResponse, GatewayError> {
        openai_compat::embed(&self.client, &config.url, DEFAULT_MODEL, config, req).await
    }
}

/// Default decision model when the request pins none (the engine always pins
/// the candidate's `api_model_id`, so this only serves direct adapter callers).
const DEFAULT_DECISION_MODEL: &str = "nimble";

/// Oldest Ollama with `/v1/systemone`.
const MIN_DECISION_VERSION: (u64, u64) = (0, 35);

#[async_trait]
impl kernel::adapters::capability::DecisionModel for OllamaAdapter {
    /// System One decisions via `{url}/v1/systemone`. Local-only: Ollama
    /// rejects cloud models here with a 400, which falls back like any other.
    async fn decide(
        &self,
        config: &RouterConfig,
        req: &kernel::types::io::DecisionRequest,
    ) -> Result<kernel::types::io::DecisionResponse, GatewayError> {
        let model = req.model.as_deref().unwrap_or(DEFAULT_DECISION_MODEL);
        crate::systemone::decide(
            &self.client,
            &config.url,
            "ollama",
            model,
            config,
            req,
            crate::systemone::ImageEncoding::Bare,
        )
        .await?
            .map_err(|r| {
                // Ollama answers 404 for two unrelated reasons. A JSON error
                // means the model is not pulled; gin's bare "404 page not
                // found" means the route itself does not exist — a server
                // older than System One. Say which, so nobody pulls a model
                // to fix an outdated server, or upgrades to fix a missing pull.
                let message = match (r.status, r.is_json_error()) {
                    (404, true) => format!(
                        "decision model '{model}' is not pulled on this Ollama — run `ollama pull {model}` ({})",
                        r.message()
                    ),
                    (404, false) => format!(
                        "this Ollama has no /v1/systemone endpoint — System One decisions need Ollama >= {}.{} ({})",
                        MIN_DECISION_VERSION.0,
                        MIN_DECISION_VERSION.1,
                        r.message().trim()
                    ),
                    _ => r.message(),
                };
                r.into_error("ollama", message)
            })
    }
}

impl OllamaAdapter {
    /// Probe whether `model` can serve System One decisions on this server,
    /// telling "Ollama too old" from "model not pulled" from "not a decision
    /// model" (gh#72). Reads `GET /api/version`, then `POST /api/show`, whose
    /// `capabilities` lists `"decision"` for decision models.
    pub async fn probe_decision_model(
        &self,
        config: &RouterConfig,
        model: &str,
    ) -> Result<crate::systemone::DecisionModelStatus, GatewayError> {
        use crate::systemone::DecisionModelStatus;

        #[derive(serde::Deserialize)]
        struct Version {
            version: String,
        }
        #[derive(serde::Deserialize)]
        struct Show {
            #[serde(default)]
            capabilities: Vec<String>,
        }

        let base = config.url.trim_end_matches('/');
        let version: Version = self
            .client
            .get(format!("{base}/api/version"))
            .timeout(crate::systemone::request_timeout(config))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        // Dev builds report 0.0.0 and unparseable strings carry no evidence —
        // only a version that parses AND is older counts as too old.
        if parse_major_minor(&version.version).is_some_and(|v| v < MIN_DECISION_VERSION) {
            return Ok(DecisionModelStatus::ServerTooOld {
                version: version.version,
            });
        }

        let show = self
            .client
            .post(format!("{base}/api/show"))
            .timeout(crate::systemone::request_timeout(config))
            .json(&serde_json::json!({ "model": model }))
            .send()
            .await?;
        if show.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(DecisionModelStatus::NotPulled);
        }
        if !show.status().is_success() {
            return Err(crate::base::error_from_response("ollama", show).await);
        }
        let show: Show = show.json().await?;
        Ok(if show.capabilities.iter().any(|c| c == "decision") {
            DecisionModelStatus::Ready
        } else {
            DecisionModelStatus::NotADecisionModel
        })
    }
}

/// `"0.35.1-rc0"` → `(0, 35)`. `None` for anything that is not `N.N…`, and for
/// the `0.0.0` dev-build placeholder.
fn parse_major_minor(version: &str) -> Option<(u64, u64)> {
    let mut parts = version.trim_start_matches('v').split(['.', '-']);
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    ((major, minor) != (0, 0)).then_some((major, minor))
}

#[async_trait]
impl kernel::adapters::RegisterInto for OllamaAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &kernel::adapters::AdapterRegistry) {
        reg.register_chat(self.clone()).await;
        reg.register_embed(self.clone()).await;
        reg.register_decision(self).await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use kernel::types::request::{Message, MessageRole};

    #[tokio::test]
    async fn embed_capability_times_out_against_a_silent_server() {
        use kernel::adapters::capability::EmbedModel;
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
        let req = kernel::types::io::EmbedRequest {
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
        assert_eq!(kernel::adapters::capability::Model::id(&adapter), "ollama");
    }

    #[tokio::test]
    #[ignore]
    async fn ollama_chat_integration() {
        use kernel::adapters::capability::ChatModel;
        let adapter = OllamaAdapter::new().unwrap();
        let config = RouterConfig {
            url: "http://localhost:11434".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: Some(60000),
            headers: std::collections::HashMap::new(),
        };
        let req = kernel::types::io::ChatRequest {
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
        use kernel::adapters::capability::EmbedModel;
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
        let req = kernel::types::io::EmbedRequest {
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
