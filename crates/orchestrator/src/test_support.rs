//! Test-only fixtures for the executor's tests: a recording chat adapter plus a
//! minimal single-chain [`Gateway`], modeled on the gateway crate's own
//! adapter/reference-chain test harness (`gateway::engine::tests` /
//! `gateway::catalog::presets`). Kept behind `#[cfg(test)]`.

use std::collections::{HashMap, VecDeque};
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
use kernel::types::request::{MessageRole, ToolCall};

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

/// The shared single-chain `GatewayConfig` for the test harness: chain `"c"`
/// resolves `TextChat` to router `"r"` / model `"m"` (context window 4096).
/// Factored out of `build_gateway` so `scripted_gateway` (a different adapter,
/// same topology) doesn't duplicate the routers/models/chains assembly.
fn single_chain_config() -> GatewayConfig {
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

    GatewayConfig {
        routers,
        models,
        chains,
        constraints: Default::default(),
        panels: Default::default(),
        consensus: Default::default(),
    }
}

/// Build a minimal gateway whose chain `"c"` resolves `TextChat` to the
/// recording adapter (router `"r"`, model `"m"`), returning the gateway and the
/// shared call log. `fail_after` is threaded into the adapter's crash injector.
/// The adapter is registered into the `AdapterRegistry` before `Gateway::new`
/// because `Gateway::adapters` is crate-private.
async fn build_gateway(fail_after: Option<usize>) -> (Gateway, CallLog) {
    let config = single_chain_config();

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

/// Chat adapter that replays a scripted queue of responses (one per turn), so a
/// test can drive a multi-turn ReAct loop: e.g. [turn0: a `calc` tool_call, turn1:
/// a final text]. Records each call's prompt like `RecordingAdapter`.
pub struct ScriptedAdapter {
    calls: CallLog,
    script: Mutex<VecDeque<ChatResponse>>,
}

impl Model for ScriptedAdapter {
    fn id(&self) -> &str {
        "r"
    }
}

#[async_trait]
impl ChatModel for ScriptedAdapter {
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
        self.calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((req.model.clone(), prompt));
        let next = self
            .script
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop_front();
        next.ok_or_else(|| GatewayError::ProviderError {
            adapter: "r".into(),
            message: "script exhausted".into(),
            status: Some(500),
        })
    }
}

/// Chat adapter that succeeds unless the first user message contains the marker
/// substring `"FAIL"`, in which case it errors. Lets a fan-out test pick which
/// specific children fail **by item content** — deterministic regardless of the
/// nondeterministic concurrent dispatch order (unlike `ScriptedAdapter`, which
/// is keyed on call order).
pub struct ContentGatedAdapter {
    calls: CallLog,
}

impl Model for ContentGatedAdapter {
    fn id(&self) -> &str {
        "r"
    }
}

#[async_trait]
impl ChatModel for ContentGatedAdapter {
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
        self.calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((req.model.clone(), prompt.clone()));
        if prompt.contains("FAIL") {
            return Err(GatewayError::ProviderError {
                adapter: "r".into(),
                message: format!("content-gated failure for {prompt:?}"),
                status: Some(500),
            });
        }
        Ok(ChatResponse {
            content: Some(format!("ok:{prompt}")),
            tool_calls: Vec::new(),
            usage: None,
            model: req.model.clone(),
            degraded: false,
        })
    }
}

/// A gateway on chain `"c"` whose adapter fails any request whose prompt
/// contains `"FAIL"` and succeeds otherwise — the fan-out partial-failure fixture.
pub async fn content_gated_gateway() -> (Gateway, CallLog) {
    let calls: CallLog = Arc::new(Mutex::new(Vec::new()));
    let adapters = AdapterRegistry::new();
    adapters
        .register_chat(Arc::new(ContentGatedAdapter {
            calls: calls.clone(),
        }))
        .await;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    (Gateway::new(single_chain_config(), adapters, cb), calls)
}

/// A gateway on chain "c" whose scripted adapter yields `responses` in order.
pub async fn scripted_gateway(responses: Vec<ChatResponse>) -> (Gateway, CallLog) {
    let calls: CallLog = Arc::new(Mutex::new(Vec::new()));
    let adapters = AdapterRegistry::new();
    adapters
        .register_chat(Arc::new(ScriptedAdapter {
            calls: calls.clone(),
            script: Mutex::new(responses.into()),
        }))
        .await;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    (Gateway::new(single_chain_config(), adapters, cb), calls)
}

/// Convenience: a `ChatResponse` carrying a single tool call.
pub fn tool_call_response(id: &str, name: &str, arguments: &str) -> ChatResponse {
    ChatResponse {
        content: Some(String::new()),
        tool_calls: vec![ToolCall {
            id: id.into(),
            name: name.into(),
            arguments: arguments.into(),
        }],
        usage: None,
        model: Some("m".into()),
        degraded: false,
    }
}

/// Convenience: a final text `ChatResponse` (no tool calls).
pub fn final_response(text: &str) -> ChatResponse {
    ChatResponse {
        content: Some(text.into()),
        tool_calls: Vec::new(),
        usage: None,
        model: Some("m".into()),
        degraded: false,
    }
}

/// A local stand-in for a live Ollama runner, used by the reference-chain
/// end-to-end test. Registered under the router id `"ollama"`, it serves
/// `TextChat` with no network I/O and returns `model: None` so the engine fills
/// `ChatResponse.model` from the *selected candidate* (`llama3.1-local`). Each
/// call is recorded so the test can prove the chain actually reached the local
/// adapter (rather than short-circuiting earlier).
pub struct LocalOllamaAdapter {
    calls: CallLog,
}

impl Model for LocalOllamaAdapter {
    fn id(&self) -> &str {
        "ollama"
    }
}

#[async_trait]
impl ChatModel for LocalOllamaAdapter {
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
        self.calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((req.model.clone(), prompt));
        Ok(ChatResponse {
            content: Some("served by local llama3.1".into()),
            tool_calls: Vec::new(),
            usage: None,
            // `None` ⇒ the engine records the selected candidate's id on the
            // response, which is the fallover proof the e2e asserts on.
            model: None,
            degraded: false,
        })
    }
}

/// Build the REAL gateway from the illustrative demo catalog
/// (`gateway::catalog::assemble(demo_catalog())`) with a [`LocalOllamaAdapter`]
/// registered for the `"ollama"` router ONLY — the credential-gated cloud
/// entries at the head of `research.bulk` (`groq-llama-free`, `deepseek-chat`)
/// have no adapter, so the chain falls over to the local `llama3.1-local`
/// model. Returns the gateway plus the adapter's shared call log.
pub async fn demo_reference_gateway() -> (Gateway, CallLog) {
    let config = gateway::catalog::assemble(gateway::catalog::demo_catalog())
        .expect("demo catalog assembles");
    let calls: CallLog = Arc::new(Mutex::new(Vec::new()));
    let adapters = AdapterRegistry::new();
    adapters
        .register_chat(Arc::new(LocalOllamaAdapter {
            calls: calls.clone(),
        }))
        .await;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    (Gateway::new(config, adapters, cb), calls)
}

/// A stateless local adapter (like [`LocalOllamaAdapter`]) that DRIVES a ReAct
/// loop deterministically: on a turn where a tool is offered and the transcript
/// has no prior tool result, it emits ONE `tool_call` to the first offered tool
/// (args shaped by tool name — `search`⇒`{query}`, `record_note`⇒`{note}`); once
/// a tool result is present (or no tools are offered) it returns a final answer.
/// `model: None` ⇒ the engine records the fallover candidate id. Stateless, so it
/// is safe under a `Map`'s concurrent fan-out (a flat script would race). Used by
/// the slice-4 e2e to exercise Observation/Mutation tools THROUGH the real
/// reference chain without a live model.
pub struct ToolEmittingOllamaAdapter {
    calls: CallLog,
}

impl Model for ToolEmittingOllamaAdapter {
    fn id(&self) -> &str {
        "ollama"
    }
}

#[async_trait]
impl ChatModel for ToolEmittingOllamaAdapter {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError> {
        let prompt = req
            .messages
            .iter()
            .find(|m| m.role == MessageRole::User)
            .map(|m| m.as_text().to_string())
            .unwrap_or_default();
        self.calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((req.model.clone(), prompt.clone()));

        let tool_result_seen = req.messages.iter().any(|m| m.role == MessageRole::Tool);
        match req.tools.first() {
            Some(tool) if !tool_result_seen => {
                let arguments = match tool.name.as_str() {
                    "search" => serde_json::json!({ "query": prompt }).to_string(),
                    "record_note" => serde_json::json!({ "note": prompt }).to_string(),
                    _ => "{}".to_string(),
                };
                Ok(ChatResponse {
                    content: Some(String::new()),
                    tool_calls: vec![ToolCall {
                        id: "tc".into(),
                        name: tool.name.clone(),
                        arguments,
                    }],
                    usage: None,
                    model: None,
                    degraded: false,
                })
            }
            _ => Ok(ChatResponse {
                content: Some("synthesized locally".into()),
                tool_calls: Vec::new(),
                usage: None,
                model: None,
                degraded: false,
            }),
        }
    }
}

/// The REAL gateway (demo catalog / `research.bulk` fallover, as
/// [`demo_reference_gateway`]) wired to a [`ToolEmittingOllamaAdapter`], so an
/// agent's ReAct loop actually calls its Observation/Mutation tools end-to-end
/// through the reference chain.
pub async fn demo_reference_tool_gateway() -> (Gateway, CallLog) {
    let config = gateway::catalog::assemble(gateway::catalog::demo_catalog())
        .expect("demo catalog assembles");
    let calls: CallLog = Arc::new(Mutex::new(Vec::new()));
    let adapters = AdapterRegistry::new();
    adapters
        .register_chat(Arc::new(ToolEmittingOllamaAdapter {
            calls: calls.clone(),
        }))
        .await;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    (Gateway::new(config, adapters, cb), calls)
}

/// A chat adapter that echoes the request's **system** prompt back as its answer
/// (no tool calls). Lets a test assert what an agent's assembled system prompt
/// contained — e.g. a dependency's output injected from the blackboard. `id`
/// `"r"` binds it to the single-chain harness config.
pub struct EchoSystemAdapter {
    calls: CallLog,
}

impl Model for EchoSystemAdapter {
    fn id(&self) -> &str {
        "r"
    }
}

#[async_trait]
impl ChatModel for EchoSystemAdapter {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError> {
        let system = req.system.clone().unwrap_or_default();
        self.calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((req.model.clone(), system.clone()));
        Ok(ChatResponse {
            content: Some(system),
            tool_calls: Vec::new(),
            usage: None,
            model: req.model.clone(),
            degraded: false,
        })
    }
}

/// A single-chain gateway whose adapter echoes the system prompt back — the
/// blackboard read-path fixture.
pub async fn echo_system_gateway() -> (Gateway, CallLog) {
    let calls: CallLog = Arc::new(Mutex::new(Vec::new()));
    let adapters = AdapterRegistry::new();
    adapters
        .register_chat(Arc::new(EchoSystemAdapter {
            calls: calls.clone(),
        }))
        .await;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    (Gateway::new(single_chain_config(), adapters, cb), calls)
}

/// A chat adapter (`id = "r"`) that always times out — a transport fault that
/// cools its router. Warm-gating a single-candidate chain (one `execute`) makes
/// the NEXT `execute` return `AllGated{resume_after: Some(_)}`.
pub struct TimeoutAdapter;
impl Model for TimeoutAdapter {
    fn id(&self) -> &str {
        "r"
    }
}
#[async_trait]
impl ChatModel for TimeoutAdapter {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        _req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError> {
        Err(GatewayError::Timeout {
            adapter: "r".into(),
            model: "m".into(),
            duration_ms: 1,
        })
    }
}

/// A single-candidate gateway whose only router times out. One warm-up `execute`
/// cools it; the next `execute` finds the sole candidate gated → `AllGated`.
pub async fn timeout_gateway() -> Gateway {
    let adapters = AdapterRegistry::new();
    adapters.register_chat(Arc::new(TimeoutAdapter)).await;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    Gateway::new(single_chain_config(), adapters, cb)
}
