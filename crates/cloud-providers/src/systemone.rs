//! System One decision calls (gh#72) — `POST {base}/v1/systemone`.
//!
//! The wire format is TypeSafe's System One API, served verbatim by Ollama
//! (≥ 0.35), OpenRouter (`https://openrouter.ai/api`) and TypeSafe
//! (`https://api.typesafe.ai`). This module holds the shared call and
//! [`SystemOneAdapter`], an id-configurable adapter for any bearer-auth host
//! of the endpoint; Ollama's adapter delegates to the same core and adds its
//! own reading of the 404s it returns.

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::base::{
    base_url_or, extract_error_message, map_status_error, parse_retry_after_ms, resolve_api_key,
};
use kernel::types::config::RouterConfig;
use kernel::types::cost::TokenUsage;
use kernel::types::decision::{DecisionAnswers, DecisionContent, DecisionQuestions};
use kernel::types::error::GatewayError;
use kernel::types::io::{DecisionRequest, DecisionResponse};

/// OpenRouter's API base; `/v1/systemone` is appended.
pub const OPENROUTER_BASE_URL: &str = "https://openrouter.ai/api";
/// TypeSafe's API base (the reference implementation of System One).
pub const TYPESAFE_BASE_URL: &str = "https://api.typesafe.ai";

const PATH: &str = "/v1/systemone";

/// Bound on one decision call when the router sets no `timeout_ms`. A bare
/// `reqwest::Client` has NO timeout, so a host that accepts the connection and
/// never answers would hang the caller forever (the defect `OllamaAdapter::new`
/// already fixed for chat with the same 120s).
const DEFAULT_TIMEOUT_SECS: u64 = 120;

/// The deadline applied to every decision request: the router's `timeout_ms`,
/// else [`DEFAULT_TIMEOUT_SECS`]. Applied per request rather than per client so
/// it holds however the adapter was built (`with_id` has no config to read).
pub(crate) fn request_timeout(cfg: &RouterConfig) -> std::time::Duration {
    cfg.timeout_ms
        .map(std::time::Duration::from_millis)
        .unwrap_or(std::time::Duration::from_secs(DEFAULT_TIMEOUT_SECS))
}

/// Whether a local Ollama can serve a decision model right now — the
/// distinctions gh#72 asks probing to make.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecisionModelStatus {
    /// The model is pulled, advertises the `decision` capability, and the
    /// server has the endpoint.
    Ready,
    /// The server predates System One (`/v1/systemone` arrived in Ollama 0.35).
    ServerTooOld { version: String },
    /// The server is new enough but the model is not pulled.
    NotPulled,
    /// The model is pulled but is not a decision model.
    NotADecisionModel,
}

#[derive(Serialize)]
struct WireRequest<'a> {
    model: &'a str,
    state: &'a DecisionContent,
    questions: &'a DecisionQuestions,
    #[serde(skip_serializing_if = "<[String]>::is_empty")]
    images: &'a [String],
    #[serde(skip_serializing_if = "Option::is_none")]
    keep_alive: Option<&'a serde_json::Value>,
}

/// Unknown fields (OpenRouter's `id`, `provider`, `usage.cost`) are ignored.
#[derive(Deserialize)]
struct WireResponse {
    #[serde(default)]
    model: Option<String>,
    answers: DecisionAnswers,
    #[serde(default)]
    usage: Option<WireUsage>,
}

#[derive(Deserialize)]
struct WireUsage {
    #[serde(default)]
    input_tokens: u32,
    #[serde(default)]
    output_tokens: u32,
}

/// A non-success response, before it becomes a [`GatewayError`] — kept apart
/// so a caller can reword a status it knows more about (Ollama's two 404s).
pub(crate) struct Rejection {
    pub status: u16,
    pub body: String,
    retry_after_ms: Option<u64>,
}

impl Rejection {
    /// The provider's own message: the JSON `error` when present, else the raw body.
    pub fn message(&self) -> String {
        extract_error_message(&self.body).unwrap_or_else(|| self.body.clone())
    }

    /// Whether the body is a JSON error envelope (as opposed to a bare router
    /// 404 page from a server that has no such endpoint at all).
    pub fn is_json_error(&self) -> bool {
        extract_error_message(&self.body).is_some()
    }

    pub fn into_error(self, adapter: &str, message: String) -> GatewayError {
        map_status_error(adapter, self.status, message, self.retry_after_ms)
    }
}

/// POST one decision call to `{base_url}/v1/systemone`. A bearer is sent only
/// when [`resolve_api_key`] yields one (local Ollama is keyless).
pub(crate) async fn decide(
    client: &Client,
    base_url: &str,
    adapter: &str,
    model: &str,
    cfg: &RouterConfig,
    req: &DecisionRequest,
) -> Result<Result<DecisionResponse, Rejection>, GatewayError> {
    let body = WireRequest {
        model,
        state: &req.state,
        questions: &req.questions,
        images: &req.images,
        keep_alive: req.keep_alive.as_ref(),
    };
    let mut http = client
        .post(format!("{}{PATH}", base_url.trim_end_matches('/')))
        .timeout(request_timeout(cfg))
        .json(&body);
    if let Some(key) = resolve_api_key(cfg) {
        http = http.bearer_auth(key);
    }
    for (k, v) in &cfg.headers {
        http = http.header(k.as_str(), v.as_str());
    }

    let response = http.send().await?;
    let status = response.status();
    if !status.is_success() {
        let retry_after_ms = parse_retry_after_ms(
            response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
        );
        return Ok(Err(Rejection {
            status: status.as_u16(),
            body: response.text().await.unwrap_or_default(),
            retry_after_ms,
        }));
    }

    let wire: WireResponse = response
        .json()
        .await
        .map_err(|e| GatewayError::ProviderError {
            adapter: adapter.into(),
            message: format!("failed to parse System One response: {e}"),
            status: Some(status.as_u16()),
        })?;
    // A 2xx with holes is not a success: report it as a failed attempt so the
    // engine records it and falls back, rather than `success: true` over
    // questions nobody answered.
    let missing: Vec<&str> = req
        .questions
        .keys()
        .filter(|name| !wire.answers.contains_key(*name))
        .map(String::as_str)
        .collect();
    if !missing.is_empty() {
        return Err(GatewayError::ProviderError {
            adapter: adapter.into(),
            message: format!("System One response has no answer for {missing:?}"),
            status: Some(status.as_u16()),
        });
    }

    Ok(Ok(DecisionResponse {
        answers: wire.answers,
        usage: wire.usage.map(|u| TokenUsage {
            input_tokens: u.input_tokens,
            output_tokens: u.output_tokens,
            total_tokens: u.input_tokens.saturating_add(u.output_tokens),
        }),
        model: wire.model.or_else(|| Some(model.to_string())),
        degraded: false,
    }))
}

/// Generic System One adapter for any bearer-auth host of the endpoint —
/// OpenRouter, TypeSafe, or a self-hosted server (llama.cpp, SGLang) via `url`.
/// Decision-only: register a chat adapter under the same id separately.
pub struct SystemOneAdapter {
    client: Client,
    id: String,
}

impl SystemOneAdapter {
    /// Build an adapter registered under `id`. The id should match the
    /// router key in `GatewayConfig::routers`, since the engine dispatches by
    /// router id.
    pub fn with_id(id: impl Into<String>) -> Result<Self, GatewayError> {
        Ok(Self {
            client: Client::new(),
            id: id.into(),
        })
    }

    /// Same as [`Self::with_id`] with the router's timeout applied.
    pub fn from_config_with_id(
        id: impl Into<String>,
        config: &RouterConfig,
    ) -> Result<Self, GatewayError> {
        Ok(Self {
            client: crate::base::build_client(config)?,
            id: id.into(),
        })
    }

    /// `config.url`, else the well-known base for `openrouter` / `typesafe`.
    fn base_url<'a>(&self, config: &'a RouterConfig) -> Result<&'a str, GatewayError> {
        let default = match self.id.as_str() {
            "openrouter" => OPENROUTER_BASE_URL,
            "typesafe" => TYPESAFE_BASE_URL,
            _ => "",
        };
        match base_url_or(config, default) {
            "" => Err(GatewayError::InvalidConfig(format!(
                "router '{}' needs a `url` for its System One endpoint",
                self.id
            ))),
            url => Ok(url),
        }
    }
}

impl kernel::adapters::capability::Model for SystemOneAdapter {
    fn id(&self) -> &str {
        &self.id
    }
}

#[async_trait]
impl kernel::adapters::capability::DecisionModel for SystemOneAdapter {
    async fn decide(
        &self,
        config: &RouterConfig,
        req: &DecisionRequest,
    ) -> Result<DecisionResponse, GatewayError> {
        // A hosted router has no sensible default decision model; guessing one
        // would bill the caller for a model they never chose.
        let Some(model) = req.model.as_deref() else {
            return Err(GatewayError::InvalidRequest {
                message: format!("a decision call to '{}' needs a model", self.id),
            });
        };
        let base_url = self.base_url(config)?;
        decide(&self.client, base_url, &self.id, model, config, req)
            .await?
            .map_err(|r| {
                let message = r.message();
                r.into_error(&self.id, message)
            })
    }
}

#[async_trait]
impl kernel::adapters::RegisterInto for SystemOneAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &kernel::adapters::AdapterRegistry) {
        reg.register_decision(self).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn cfg(url: &str) -> RouterConfig {
        RouterConfig {
            url: url.into(),
            api_key: None,
            api_key_env: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        }
    }

    /// A router with no `timeout_ms` still gets a bound: a wedged host must
    /// not hang `Gateway::execute` (the defect `OllamaAdapter::new` fixed).
    #[test]
    fn requests_are_bounded_even_without_a_configured_timeout() {
        assert_eq!(
            request_timeout(&cfg("http://h")),
            std::time::Duration::from_secs(120)
        );
        let mut c = cfg("http://h");
        c.timeout_ms = Some(2500);
        assert_eq!(request_timeout(&c), std::time::Duration::from_millis(2500));
    }

    #[test]
    fn well_known_ids_default_their_base_url_and_others_require_one() {
        let base = |id: &str, url: &str| {
            SystemOneAdapter::with_id(id)
                .unwrap()
                .base_url(&cfg(url))
                .map(str::to_string)
        };
        assert_eq!(base("openrouter", "").unwrap(), OPENROUTER_BASE_URL);
        assert_eq!(base("typesafe", "").unwrap(), TYPESAFE_BASE_URL);
        assert_eq!(
            base("openrouter", "https://proxy.example/api/").unwrap(),
            "https://proxy.example/api"
        );
        assert!(matches!(
            base("llamacpp", ""),
            Err(GatewayError::InvalidConfig(m)) if m.contains("llamacpp")
        ));
    }
}
