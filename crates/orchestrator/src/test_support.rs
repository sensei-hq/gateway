//! Test-only fixtures for the executor's tests: a recording chat adapter plus a
//! minimal single-chain [`Gateway`], modeled on the gateway crate's own
//! adapter/reference-chain test harness (`gateway::engine::tests` /
//! `gateway::catalog::presets`). Compiled only under `#[cfg(test)]` or the dev-only
//! `test-support` feature, which lets another crate's dev-dependencies reuse the same
//! doubles (torii's cross-process e2e).

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gateway::Gateway;
use gateway::adapters::AdapterRegistry;
use gateway::adapters::capability::{ChatModel, Model};
use gateway::circuit_breaker::{CircuitBreakerConfig, CircuitBreakerManager};
use kernel::types::capability::Capability;
use kernel::types::config::{
    ChainEntry, FallbackChainConfig, GatewayConfig, ModelConfig, RouterConfig,
};
use kernel::types::cost::TokenUsage;
use kernel::types::error::GatewayError;
use kernel::types::io::{ChatRequest, ChatResponse};
use kernel::types::request::{InferenceRequest, Message, MessageRole, Payload, ToolCall};
use orchestrator_core::Clock;

/// One recorded gateway call: the resolved model id the adapter was dispatched
/// with, plus a fingerprint of the payload (the first user message's text).
/// Enough to prove distinct payloads reached the adapter, in order.
pub type RecordedCall = (Option<String>, String);

/// Shared, clonable log of recorded calls.
pub type CallLog = Arc<Mutex<Vec<RecordedCall>>>;

/// One recorded call including its SYSTEM prompt: `(system, first user message)`.
/// See [`prompt_recording_gateway`].
pub type RecordedPrompt = (Option<String>, String);

/// Shared, clonable log of recorded prompts.
pub type PromptLog = Arc<Mutex<Vec<RecordedPrompt>>>;

/// Chat adapter recording BOTH halves of the prompt — `(system, first user
/// message)` — for one call.
///
/// The existing pair each sees only one half: [`RecordingAdapter`] logs the user
/// text, [`EchoSystemAdapter`] logs (and echoes) the system prompt. Reach for this
/// one when a single call must be shown to carry both, as the planner selector's
/// must: its capability menu is the user text and its instruction ("answer with
/// ONLY the exact agent name") is the system prompt, and `SelectorDispatch`
/// hand-builds its `InferenceRequest` precisely because the shared `build_request`
/// hardcodes `system: None`.
pub struct PromptRecordingAdapter {
    prompts: PromptLog,
    content: String,
}

impl Model for PromptRecordingAdapter {
    fn id(&self) -> &str {
        "r"
    }
}

#[async_trait]
impl ChatModel for PromptRecordingAdapter {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError> {
        let user = req
            .messages
            .first()
            .map(|m| m.as_text().to_string())
            .unwrap_or_default();
        self.prompts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((req.system.clone(), user));
        Ok(ChatResponse {
            content: Some(self.content.clone()),
            tool_calls: Vec::new(),
            usage: None,
            model: req.model.clone(),
            degraded: false,
        })
    }
}

/// A gateway whose adapter records `(system, user)` for every call and always
/// answers `content`.
pub async fn prompt_recording_gateway(content: &str) -> (Gateway, PromptLog) {
    let prompts: PromptLog = Arc::new(Mutex::new(Vec::new()));
    let adapters = AdapterRegistry::new();
    adapters
        .register_chat(Arc::new(PromptRecordingAdapter {
            prompts: prompts.clone(),
            content: content.to_string(),
        }))
        .await;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    (Gateway::new(single_chain_config(), adapters, cb), prompts)
}

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

/// The fixture chain's model output limit, and the ceiling the SP-DATA-5 clamp must
/// respect.
///
/// Named rather than repeated because two places must agree on it and a drift between
/// them would make the ceiling test pass for the wrong reason: `single_chain_config`
/// declares it as the model's `max_output_tokens` (what `Gateway::min_max_output_tokens`
/// reads), and [`ClampObservingAdapter`] enforces it as the provider would.
///
/// 1024 is the value `anthropic` and `bedrock` substitute for a `None` `max_tokens`
/// (`cloud-providers/src/anthropic/mod.rs:27`, `bedrock/mod.rs:62`), which makes it a
/// realistic figure for the fixture even though nothing here depends on the coincidence.
///
/// `gemini.rs:43` declares the same constant and does NOT reach it from this
/// orchestrator: its `generationConfig` is built only when `max_tokens` or `temperature`
/// is `Some`, and every producer here sends both as `None` on an unbudgeted call, so no
/// `generationConfig` is sent at all. The two families that genuinely default to 1024 are
/// anthropic and bedrock; the local engine defaults to 512 and `openai_compat` omits the
/// field entirely. See `dispatch.rs`'s clamp comment for the full five-family survey —
/// an earlier version of this sentence listed gemini among the substituting three.
pub const FIXTURE_MAX_OUTPUT_TOKENS: u32 = 1024;

/// The shared single-chain `GatewayConfig` for the test harness: chain `"c"`
/// resolves `TextChat` to router `"r"` / model `"m"` (context window 4096,
/// [`FIXTURE_MAX_OUTPUT_TOKENS`] of output).
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
            max_output_tokens: FIXTURE_MAX_OUTPUT_TOKENS,
            pricing: None,
            catalog: None,
        },
    );

    // The embedding twin of `"m"`, on the same router. Present so a test can dispatch
    // the ONE payload kind the SP-DATA-5 clamp deliberately skips — `Payload::Embed`
    // has no `max_tokens` to set — and show it is still metered and still gated.
    // Inert for every other fixture: nothing else resolves chain `"emb"`.
    models.insert(
        "me".to_string(),
        ModelConfig {
            id: "me".to_string(),
            api_model_id: None,
            provider: "r".to_string(),
            family: None,
            capabilities: vec![Capability::TextEmbed],
            context_window: 4096,
            max_output_tokens: FIXTURE_MAX_OUTPUT_TOKENS,
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
    chains.insert(
        "emb".to_string(),
        FallbackChainConfig {
            id: "emb".to_string(),
            capability: Capability::TextEmbed,
            models: vec![ChainEntry {
                model: "me".to_string(),
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

/// The two windows of [`two_window_chain_config`], named so a test can compute the
/// prompt size it needs from them instead of repeating literals that could drift apart.
///
/// `SMALL` matches [`single_chain_config`]'s only model, so a prompt sized against one
/// fixture behaves the same way in the other.
pub const TWO_WINDOW_SMALL: u32 = 4096;
/// See [`TWO_WINDOW_SMALL`]. Large enough that no fixture prompt in this repo approaches
/// it, so "over the small window" and "under the big one" is a wide target.
pub const TWO_WINDOW_BIG: u32 = 200_000;

/// A HETEROGENEOUS chain `"c"`: `small` (window [`TWO_WINDOW_SMALL`]) at priority 1 and
/// `big` (window [`TWO_WINDOW_BIG`]) at priority 2, both `TextChat` on router `"r"`.
///
/// SP-7a's whole point, as a fixture. [`single_chain_config`] — the only gateway config
/// this harness had — has one chat model, so every orchestrator test of the window
/// behaviour could only exercise the over-EVERY-window FAILURE. The scenario the slice
/// exists to unblock (a prompt over the chain MINIMUM but under the primary's window,
/// which the deleted `over_budget` pre-check refused outright) had no fixture above the
/// gateway crate at all, and the deleted pre-check was the orchestrator's.
///
/// `small` is priority 1 deliberately: it is the entry selection returns FIRST, so a test
/// that ends up on `big` did so because the window gate removed `small`, not because of
/// ordering luck.
pub fn two_window_chain_config() -> GatewayConfig {
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
    for (id, context_window) in [("small", TWO_WINDOW_SMALL), ("big", TWO_WINDOW_BIG)] {
        models.insert(
            id.to_string(),
            ModelConfig {
                id: id.to_string(),
                api_model_id: None,
                provider: "r".to_string(),
                family: None,
                capabilities: vec![Capability::TextChat],
                context_window,
                max_output_tokens: FIXTURE_MAX_OUTPUT_TOKENS,
                pricing: None,
                catalog: None,
            },
        );
    }

    let mut chains = HashMap::new();
    chains.insert(
        "c".to_string(),
        FallbackChainConfig {
            id: "c".to_string(),
            capability: Capability::TextChat,
            models: vec![
                ChainEntry {
                    model: "small".to_string(),
                    router: Some("r".to_string()),
                    api_model_id: None,
                    priority: 1,
                },
                ChainEntry {
                    model: "big".to_string(),
                    router: Some("r".to_string()),
                    api_model_id: None,
                    priority: 2,
                },
            ],
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

/// [`recording_gateway`] over [`two_window_chain_config`]: the chain name is still
/// `"c"`, so every agent fixture keyed on that chain works unchanged and the only
/// difference is which models it resolves to.
pub async fn two_window_chain_gateway() -> (Gateway, CallLog) {
    let calls: CallLog = Arc::new(Mutex::new(Vec::new()));
    let adapters = AdapterRegistry::new();
    adapters
        .register_chat(Arc::new(RecordingAdapter {
            calls: calls.clone(),
            fail_after: None,
        }))
        .await;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    (Gateway::new(two_window_chain_config(), adapters, cb), calls)
}

/// [`scripted_gateway`] over [`two_window_chain_config`].
///
/// Exists for the AC9 memo-replay test, whose second drive must resolve chain `"c"` to a
/// DIFFERENT model than its first — the shape a config reload between two processes
/// produces, and the only way to exercise "selection may differ" for real rather than by
/// relabelling a response.
pub async fn two_window_scripted_gateway(responses: Vec<ChatResponse>) -> (Gateway, CallLog) {
    let calls: CallLog = Arc::new(Mutex::new(Vec::new()));
    let adapters = AdapterRegistry::new();
    adapters
        .register_chat(Arc::new(ScriptedAdapter {
            calls: calls.clone(),
            script: Mutex::new(responses.into()),
        }))
        .await;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    (Gateway::new(two_window_chain_config(), adapters, cb), calls)
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

/// Chat adapter that behaves like [`RecordingAdapter`] (records + always succeeds)
/// but reports the given `usage` on every response. SP-DATA-5: every OTHER double
/// in this module hardcodes `usage: None` — so every existing test runs unmetered
/// by construction (fine; none of them set a budget), but a test that wants to
/// prove reported usage reaches the journal needs THIS double instead.
pub struct MeteredAdapter {
    calls: CallLog,
    usage: Option<TokenUsage>,
}

impl Model for MeteredAdapter {
    fn id(&self) -> &str {
        "r"
    }
}

#[async_trait]
impl ChatModel for MeteredAdapter {
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
            content: Some("canned-response".into()),
            tool_calls: Vec::new(),
            usage: self.usage.clone(),
            model: req.model.clone(),
            degraded: false,
        })
    }
}

/// A gateway on chain `"c"` whose adapter always succeeds and reports `usage` on
/// every response (`None` reproduces the other doubles' unmetered behaviour, so a
/// caller can also use this to exercise the additivity path explicitly).
pub async fn metered_gateway(usage: Option<TokenUsage>) -> (Gateway, CallLog) {
    let calls: CallLog = Arc::new(Mutex::new(Vec::new()));
    let adapters = AdapterRegistry::new();
    adapters
        .register_chat(Arc::new(MeteredAdapter {
            calls: calls.clone(),
            usage,
        }))
        .await;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    (Gateway::new(single_chain_config(), adapters, cb), calls)
}

/// Chat adapter that behaves like [`MeteredAdapter`] but **actually suspends** —
/// `tokio::time::sleep(delay)` — before returning.
///
/// This is not a convenience. EVERY other double in this module returns without a
/// single suspension point, which makes a `Map`'s `join_all` degenerate to strictly
/// SEQUENTIAL execution: each child future runs to completion on its first poll, so
/// no two children are ever in flight together and any concurrency defect is
/// structurally invisible to the suite. That is exactly how SP-DATA-5 shipped a
/// budget gate that a 6-wide fan-out walks straight through — the same test yields
/// 1 call against this double and 6 against the non-awaiting one.
///
/// Reach for this whenever a test's claim is about what happens *between* two
/// concurrent gateway calls (fan-out gating, shared-state races, ordering); the
/// zero-latency doubles are fine for everything else.
pub struct LatencyMeteredAdapter {
    calls: CallLog,
    usage: Option<TokenUsage>,
    delay: std::time::Duration,
}

impl Model for LatencyMeteredAdapter {
    fn id(&self) -> &str {
        "r"
    }
}

#[async_trait]
impl ChatModel for LatencyMeteredAdapter {
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
        // Record BEFORE suspending: a test that counts calls must see a call that is
        // in flight, not only one that has returned.
        self.calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((req.model.clone(), prompt));
        tokio::time::sleep(self.delay).await;
        Ok(ChatResponse {
            content: Some("canned-response".into()),
            tool_calls: Vec::new(),
            usage: self.usage.clone(),
            model: req.model.clone(),
            degraded: false,
        })
    }
}

/// A gateway on chain `"c"` whose adapter reports `usage` and **awaits** `delay`
/// before responding — see [`LatencyMeteredAdapter`] for why a suspension point is
/// load-bearing rather than cosmetic.
pub async fn metered_latency_gateway(
    usage: Option<TokenUsage>,
    delay: std::time::Duration,
) -> (Gateway, CallLog) {
    let calls: CallLog = Arc::new(Mutex::new(Vec::new()));
    let adapters = AdapterRegistry::new();
    adapters
        .register_chat(Arc::new(LatencyMeteredAdapter {
            calls: calls.clone(),
            usage,
            delay,
        }))
        .await;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    (Gateway::new(single_chain_config(), adapters, cb), calls)
}

/// One entry per call: the `max_tokens` that call's request carried.
/// See [`ClampObservingAdapter`].
pub type MaxTokensLog = Arc<Mutex<Vec<Option<u32>>>>;

/// Records the `max_tokens` each call carried, and HONOURS it in the usage it reports
/// — `output_tokens = min(scripted_output, max_tokens)`.
///
/// All three legs are load-bearing, and no other double in this module has any of
/// them. The existing adapters record some slice of the prompt — the user text, the
/// system text, or both — and `max_tokens` is read by none of them, so before this one
/// **no test in the suite could see a clamp at all**.
///
/// Recording is what lets a test assert the clamp was applied. Honouring is what makes
/// "a budgeted run's spend stays within its cap plus the estimate error" a MEASUREMENT
/// rather than a reading-back of a number we just watched ourselves compute — the
/// difference matters because a test of the first kind cannot tell a correct clamp from
/// one the provider ignored. Both claims were checked by mutation rather than asserted:
/// report `scripted_output` regardless of `max_tokens` and FIVE tests redden —
/// `a_budgeted_run_does_not_overshoot_beyond_the_estimate_error` with "spend 5010 must
/// stay within cap 2000", `a_clamped_call_still_journals_its_real_usage` on the journaled
/// count, `both_clamp_signals_fire_when_the_clamp_bit_and_the_estimate_was_low` and
/// `the_clamp_bit_signal_fires_against_the_value_sent_not_the_allowance` on the reported
/// `output_tokens`, and this module's own
/// `the_clamp_observing_fixture_records_and_honours_max_tokens`.
///
/// Re-measured here rather than carried forward. Both this list and the "only test that
/// depends on honouring" claim in `a_budgeted_run_does_not_overshoot_beyond_the_estimate_error`
/// were stale within the slice that wrote them; a mutation result is only evidence for as
/// long as it reproduces, so if a later change makes this list wrong, re-run the mutation
/// rather than trusting the sentence.
///
/// It is a stand-in for a provider's behaviour, not a model of one: a real provider
/// stops GENERATING at `max_tokens`, so `output_tokens` lands at or below it. `min` is
/// the same observable, without pretending to know where a real reply would have
/// ended.
///
/// The one place it does model a provider faithfully is `max_output`: a real provider
/// REJECTS a `max_tokens` above the model's own output limit rather than quietly
/// honouring it (Anthropic answers a 400 `invalid_request_error`), and every adapter in
/// this repo forwards the value verbatim. A double that accepted any number would let
/// the clamp emit a request no real provider would run, which is exactly the regression
/// the whole-slice review found.
pub struct ClampObservingAdapter {
    seen: MaxTokensLog,
    input_tokens: u32,
    /// What the model WOULD emit unclamped; the reported output is capped by
    /// `max_tokens`.
    scripted_output: u32,
    /// The model's own output limit. A request asking for more is REFUSED, as a real
    /// provider refuses it — see the struct doc.
    max_output: u32,
}

impl Model for ClampObservingAdapter {
    fn id(&self) -> &str {
        "r"
    }
}

#[async_trait]
impl ChatModel for ClampObservingAdapter {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError> {
        self.seen
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(req.max_tokens);
        if req.max_tokens.is_some_and(|m| m > self.max_output) {
            return Err(GatewayError::ProviderError {
                adapter: self.id().to_string(),
                message: format!(
                    "max_tokens: {} > {}, the maximum output tokens for this model",
                    req.max_tokens.unwrap_or_default(),
                    self.max_output
                ),
                status: Some(400),
            });
        }
        let output_tokens = match req.max_tokens {
            Some(cap) => self.scripted_output.min(cap),
            None => self.scripted_output,
        };
        Ok(ChatResponse {
            content: Some("canned-response".into()),
            tool_calls: Vec::new(),
            usage: Some(TokenUsage {
                input_tokens: self.input_tokens,
                output_tokens,
                total_tokens: self.input_tokens.saturating_add(output_tokens),
            }),
            model: req.model.clone(),
            degraded: false,
        })
    }
}

/// A gateway on chain `"c"` whose adapter records and honours `max_tokens`, and
/// refuses one above [`FIXTURE_MAX_OUTPUT_TOKENS`] the way a real provider does.
/// Returns the shared log so a test can assert what actually reached the provider —
/// see [`ClampObservingAdapter`] for why each half matters.
pub async fn clamp_observing_gateway(
    input_tokens: u32,
    scripted_output: u32,
) -> (Gateway, MaxTokensLog) {
    let seen: MaxTokensLog = Arc::new(Mutex::new(Vec::new()));
    let adapters = AdapterRegistry::new();
    adapters
        .register_chat(Arc::new(ClampObservingAdapter {
            seen: seen.clone(),
            input_tokens,
            scripted_output,
            max_output: FIXTURE_MAX_OUTPUT_TOKENS,
        }))
        .await;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    (Gateway::new(single_chain_config(), adapters, cb), seen)
}

/// A metered EMBEDDING double for chain `"emb"`, counting the calls that reach it.
///
/// `Payload::Embed` is the one payload kind the SP-DATA-5 clamp skips — there is no
/// `max_tokens` on it to set — which makes it the only place left where the plain
/// `spent >= cap` gate is observably the ONLY thing between a budgeted run and the
/// provider. That is why this exists: it is the home for the `>=`-versus-`>` boundary
/// guard the clamp took away from `spending_exactly_the_cap_stops_the_run`.
pub struct MeteredEmbedAdapter {
    calls: Arc<Mutex<usize>>,
    total_tokens: u32,
}

impl Model for MeteredEmbedAdapter {
    fn id(&self) -> &str {
        "r"
    }
}

#[async_trait]
impl gateway::adapters::capability::EmbedModel for MeteredEmbedAdapter {
    async fn embed(
        &self,
        _cfg: &RouterConfig,
        req: &kernel::types::io::EmbedRequest,
    ) -> Result<kernel::types::io::EmbedResponse, GatewayError> {
        *self.calls.lock().unwrap_or_else(|e| e.into_inner()) += 1;
        Ok(kernel::types::io::EmbedResponse {
            embeddings: req.texts.iter().map(|_| vec![0.0_f32; 3]).collect(),
            usage: Some(TokenUsage {
                input_tokens: self.total_tokens,
                output_tokens: 0,
                total_tokens: self.total_tokens,
            }),
            degraded: false,
        })
    }
}

/// A gateway whose chain `"emb"` resolves `TextEmbed` to [`MeteredEmbedAdapter`].
/// Returns the shared call counter, so a test can tell "refused before the provider"
/// from "dispatched" — the distinction the whole non-`Chat` arm rests on.
pub async fn metered_embed_gateway(total_tokens: u32) -> (Gateway, Arc<Mutex<usize>>) {
    let calls = Arc::new(Mutex::new(0usize));
    let adapters = AdapterRegistry::new();
    adapters
        .register_embed(Arc::new(MeteredEmbedAdapter {
            calls: calls.clone(),
            total_tokens,
        }))
        .await;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    (Gateway::new(single_chain_config(), adapters, cb), calls)
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

/// A [`timeout_gateway`] that has ALREADY been warmed: one `execute` against chain `"c"`
/// is spent cooling its sole router, so the very NEXT `execute` finds every candidate
/// gated and returns `AllGated { resume_after }` — which is what makes a run over this
/// gateway *pause* (resumable, with a deadline) instead of failing outright.
///
/// The warm-up belongs with the fixture rather than at each call site: a caller that
/// forgets it silently gets a FAILED run instead of a paused one, which is a different
/// test. The request must target the same chain and capability the executor's own
/// `build_request` compiles a `ModelCall` into, since that is what decides which router
/// cools — hence the deliberate mirroring below (the executor's helper is private to its
/// module, and widening that module's visibility for a test fixture would make a private
/// type reachable at `pub(crate)`).
pub async fn gated_gateway() -> Gateway {
    let gw = timeout_gateway().await;
    let _ = gw
        .execute(&InferenceRequest {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("c".to_string()),
            payload: Payload::Chat {
                messages: vec![Message::text(MessageRole::User, "warm")],
                system: None,
                max_tokens: None,
                temperature: None,
                tools: Vec::new(),
            },
            budget: None,
            auth: None,
            panel: None,
            consensus: None,
            allow_fallback: true,
            credentials: Default::default(),
        })
        .await;
    gw
}

/// A settable [`Clock`] for deterministic scheduler/wake tests — advance time by hand (no real
/// sleeps) to make a `Scheduler::tick` fire.
pub struct FakeClock(Mutex<DateTime<Utc>>);
impl FakeClock {
    pub fn new(t: DateTime<Utc>) -> Arc<Self> {
        Arc::new(Self(Mutex::new(t)))
    }
    pub fn set(&self, t: DateTime<Utc>) {
        *self.0.lock().unwrap() = t;
    }
}
impl Clock for FakeClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A chat request on the fixture chain carrying `max_tokens`, shaped exactly like
    /// the one the executor's `build_request` compiles a `ModelCall` into — so this
    /// exercises the same path a real clamped dispatch takes.
    fn chat_request(max_tokens: Option<u32>) -> InferenceRequest {
        InferenceRequest {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("c".to_string()),
            payload: Payload::Chat {
                messages: vec![Message::text(MessageRole::User, "hi")],
                system: None,
                max_tokens,
                temperature: None,
                tools: Vec::new(),
            },
            budget: None,
            auth: None,
            panel: None,
            consensus: None,
            allow_fallback: true,
            credentials: Default::default(),
        }
    }

    /// The fixture's own proof, and it is not ceremony: every clamp assertion in the
    /// executor suite is only as good as this double.
    ///
    /// It checks all three legs. Recording is what lets a test see the clamp at all —
    /// and it also proves the value survives `InferenceRequest` → `ChatRequest`
    /// (`gateway::dispatch::to_chat_request`), which is the precondition that makes
    /// the clamp observable end-to-end rather than only inside the executor.
    /// Honouring is what
    /// `a_budgeted_run_does_not_overshoot_beyond_the_estimate_error` and
    /// `a_clamped_call_still_journals_its_real_usage` rest on: both fail if this
    /// double reports its scripted output regardless, which is how we know the claim
    /// is a measurement and not a reading-back of a number the clamp just computed.
    /// Refusing an over-large `max_tokens` is what makes
    /// `a_budgeted_call_is_never_clamped_above_the_models_output_limit` a claim about a
    /// provider's contract rather than about the fixture's own arithmetic.
    ///
    /// The `None` leg matters too — it is the unbudgeted/additivity path, and a
    /// fixture that capped output unconditionally would make an unclamped run look
    /// clamped.
    #[tokio::test]
    async fn the_clamp_observing_fixture_records_and_honours_max_tokens() {
        let (gw, seen) = clamp_observing_gateway(10, 5_000).await;

        let clamped = gw
            .execute(&chat_request(Some(64)))
            .await
            .expect("the fixture chain dispatches");
        assert_eq!(
            clamped
                .usage
                .expect("the fixture always reports usage")
                .output_tokens,
            64,
            "the scripted 5000 is capped BY max_tokens — without this the double \
             cannot witness a clamp being honoured"
        );

        let unclamped = gw
            .execute(&chat_request(None))
            .await
            .expect("the fixture chain dispatches");
        assert_eq!(
            unclamped
                .usage
                .expect("the fixture always reports usage")
                .output_tokens,
            5_000,
            "with no max_tokens the full scripted output is reported"
        );

        // And the third half: a `max_tokens` above the model's own output limit is
        // REFUSED, as a real provider refuses it. This is what makes
        // `a_budgeted_call_is_never_clamped_above_the_models_output_limit` a claim about
        // the provider's contract rather than about a number the double invented.
        let refused = gw
            .execute(&chat_request(Some(FIXTURE_MAX_OUTPUT_TOKENS + 1)))
            .await
            .expect_err("a max_tokens above the model's output limit is rejected");
        assert!(
            refused.to_string().contains("max_tokens"),
            "the refusal names the offending parameter, as a provider 400 does: {refused}"
        );

        assert_eq!(
            *seen.lock().unwrap_or_else(|e| e.into_inner()),
            vec![Some(64), None, Some(FIXTURE_MAX_OUTPUT_TOKENS + 1)],
            "all three calls are recorded, in order, with the max_tokens each carried — \
             including the refused one, so a test can tell 'never sent' from 'sent and \
             rejected'"
        );
    }
}
