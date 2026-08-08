//! Test-only fixtures for the executor's tests: a recording chat adapter plus a
//! minimal single-chain [`Gateway`], modeled on the gateway crate's own
//! adapter/reference-chain test harness (`gateway::engine::tests` /
//! `gateway::catalog::presets`). Kept behind `#[cfg(test)]`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use gateway::Gateway;
use gateway::adapters::AdapterRegistry;
use gateway::adapters::capability::{ChatModel, Model};
use gateway::circuit_breaker::{CircuitBreakerConfig, CircuitBreakerManager};
use kernel::types::capability::Capability;
use kernel::types::config::{
    ChainEntry, FallbackChainConfig, GatewayConfig, ModelConfig, RouterConfig,
};
use kernel::types::error::GatewayError;
use kernel::types::io::{ChatRequest, ChatResponse};

/// One recorded gateway call: the resolved model id the adapter was dispatched
/// with, plus a fingerprint of the payload (the first user message's text).
/// Enough to prove distinct payloads reached the adapter, in order.
pub type RecordedCall = (Option<String>, String);

/// Shared, clonable log of recorded calls.
pub type CallLog = Arc<Mutex<Vec<RecordedCall>>>;

/// Chat adapter that records each call into a shared log and returns a canned,
/// non-degraded (successful) response. `fail_after` is the crash injector for
/// the resume test: `Some(n)` ⇒ succeed for the first `n` calls then error on
/// every call after that; `None` ⇒ always succeed.
pub struct RecordingAdapter {
    calls: CallLog,
    fail_after: Option<usize>,
}

impl Model for RecordingAdapter {
    fn id(&self) -> &str {
        "r"
    }
}

#[async_trait]
impl ChatModel for RecordingAdapter {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError> {
        let prompt = req
            .messages
            .first()
            .map(|m| m.as_text().to_string())
            .unwrap_or_default();
        // Record the call first (so the log counts even a failed dispatch), then
        // read the 1-based index of *this* call to decide success vs. failure.
        let call_index = {
            let mut calls = self.calls.lock().unwrap_or_else(|e| e.into_inner());
            calls.push((req.model.clone(), prompt));
            calls.len()
        };
        // Crash injector: model a provider dying mid-run by erroring once the
        // call count passes `fail_after`. A single-candidate chain surfaces this
        // straight to `Gateway::execute` as an `Err`.
        if let Some(succeed) = self.fail_after
            && call_index > succeed
        {
            return Err(GatewayError::ProviderError {
                adapter: self.id().to_string(),
                message: "injected mid-run failure".to_string(),
                status: Some(500),
            });
        }
        Ok(ChatResponse {
            content: Some("canned-response".into()),
            tool_calls: Vec::new(),
            usage: None,
            model: req.model.clone(),
            degraded: false,
        })
    }
}

/// Build a minimal gateway whose chain `"c"` resolves `TextChat` to the
/// recording adapter (router `"r"`, model `"m"`), returning the gateway and the
/// shared call log. `fail_after` is threaded into the adapter's crash injector.
/// The adapter is registered into the `AdapterRegistry` before `Gateway::new`
/// because `Gateway::adapters` is crate-private.
async fn build_gateway(fail_after: Option<usize>) -> (Gateway, CallLog) {
    let mut routers = HashMap::new();
    routers.insert(
        "r".to_string(),
        RouterConfig {
            url: "http://localhost".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        },
    );

    let mut models = HashMap::new();
    models.insert(
        "m".to_string(),
        ModelConfig {
            id: "m".to_string(),
            api_model_id: None,
            provider: "r".to_string(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window: 4096,
            max_output_tokens: 1024,
            pricing: None,
            catalog: None,
        },
    );

    let mut chains = HashMap::new();
    chains.insert(
        "c".to_string(),
        FallbackChainConfig {
            id: "c".to_string(),
            capability: Capability::TextChat,
            models: vec![ChainEntry {
                model: "m".to_string(),
                router: Some("r".to_string()),
                api_model_id: None,
                priority: 1,
            }],
            fallback_triggers: Vec::new(),
        },
    );

    let config = GatewayConfig {
        routers,
        models,
        chains,
        constraints: Default::default(),
        panels: Default::default(),
        consensus: Default::default(),
    };

    let calls: CallLog = Arc::new(Mutex::new(Vec::new()));
    let adapters = AdapterRegistry::new();
    adapters
        .register_chat(Arc::new(RecordingAdapter {
            calls: calls.clone(),
            fail_after,
        }))
        .await;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    (Gateway::new(config, adapters, cb), calls)
}

/// A gateway whose recording adapter always succeeds.
pub async fn recording_gateway() -> (Gateway, CallLog) {
    build_gateway(None).await
}

/// A gateway whose recording adapter succeeds for its first `succeed` calls and
/// errors thereafter — the crash injector for the resume-without-re-spend test.
pub async fn failing_after_gateway(succeed: usize) -> (Gateway, CallLog) {
    build_gateway(Some(succeed)).await
}
