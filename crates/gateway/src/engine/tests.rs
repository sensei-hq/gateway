use super::*;
use crate::adapters::noop::NoopAdapter;
use crate::circuit_breaker::CircuitBreakerConfig;
use crate::gates::RouterHealthRead;
use crate::store::InMemoryStore;
use crate::types::capability::Capability;
use crate::types::config::{
    ChainEntry, ConstraintsConfig, FallbackChainConfig, FallbackTrigger, GatewayConfig, MeterUnit,
    ModelConfig, ModelPricing, QuotaLimit, RouterConfig, TierConstraints, Window,
};
use crate::types::cost::TokenUsage;
use crate::types::request::{AuthContext, Message, MessageRole, Payload};
use std::collections::HashMap;
use std::sync::Arc;

use crate::test_support::noop_chat_chain;

fn test_config_with_noop() -> GatewayConfig {
    let mut routers = HashMap::new();
    routers.insert(
        "noop".to_string(),
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
        "noop".to_string(),
        ModelConfig {
            id: "noop".to_string(),
            api_model_id: None,
            provider: "noop".to_string(),
            family: None,
            capabilities: vec![Capability::TextChat, Capability::TextEmbed],
            context_window: 4096,
            max_output_tokens: 1024,
            pricing: None,
            catalog: None,
        },
    );

    let mut chains = HashMap::new();
    chains.insert("chat_chain".to_string(), noop_chat_chain());

    GatewayConfig {
        routers,
        models,
        chains,
        constraints: Default::default(),
        panels: Default::default(),
        consensus: Default::default(),
    }
}

fn test_gateway() -> Gateway {
    let config = test_config_with_noop();
    let adapters = AdapterRegistry::new();
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());

    Gateway::new(config, adapters, cb)
}

async fn register_noop(gw: &Gateway) {
    use crate::adapters::RegisterInto;
    // NoopAdapter implements every capability trait + RegisterInto, so this
    // lands it in all six capability maps in one call.
    Arc::new(NoopAdapter).register_into(&gw.adapters).await;
}

fn chat_request() -> InferenceRequest {
    InferenceRequest {
        capability: Capability::TextChat,
        model: None,
        router: None,
        chain: None,
        payload: Payload::Chat {
            messages: vec![Message::text(MessageRole::User, "Hello, world!")],
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
        routing: None,
    }
}

/// A GatewayConfig with a single "priced" router+model (`TextChat`, no
/// chain, priced at $0.0008/1k input and $0.004/1k output), shared by the
/// tests that assert on `Cost::from_usage x pricing` / persisted spend.
fn priced_gateway_config() -> GatewayConfig {
    let mut routers = HashMap::new();
    routers.insert(
        "priced".to_string(),
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
        "priced".to_string(),
        ModelConfig {
            id: "priced".to_string(),
            api_model_id: None,
            provider: "priced".to_string(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window: 4096,
            max_output_tokens: 1024,
            pricing: Some(ModelPricing {
                input_per_1k: 0.0008,
                output_per_1k: 0.004,
                per_request: None,
            }),
            catalog: None,
        },
    );
    GatewayConfig {
        routers,
        models,
        chains: HashMap::new(),
        constraints: Default::default(),
        panels: Default::default(),
        consensus: Default::default(),
    }
}

/// A chat request pinned directly at the "priced" router/model (no chain).
fn priced_chat_request() -> InferenceRequest {
    InferenceRequest {
        capability: Capability::TextChat,
        model: Some("priced".to_string()),
        router: Some("priced".to_string()),
        chain: None,
        payload: Payload::Chat {
            messages: vec![Message::text(MessageRole::User, "hi")],
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
        routing: None,
    }
}

/// Chat adapter that reports a fixed, non-trivial token usage (1000 in /
/// 500 out), so callers can assert cost = usage x pricing (or persisted
/// spend) without a live provider.
struct UsageAdapter;
impl crate::adapters::capability::Model for UsageAdapter {
    fn id(&self) -> &str {
        "priced"
    }
}
#[async_trait::async_trait]
impl crate::adapters::capability::ChatModel for UsageAdapter {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<crate::types::io::ChatResponse, GatewayError> {
        Ok(crate::types::io::ChatResponse {
            content: Some("ok".to_string()),
            tool_calls: Vec::new(),
            usage: Some(TokenUsage {
                input_tokens: 1000,
                output_tokens: 500,
                total_tokens: 1500,
            }),
            model: Some("priced".to_string()),
            degraded: false,
        })
    }
}

// Adapter that records the model it receives via `chat`, so we can assert the
// engine injects the chain-selected api_model_id rather than passing None.
struct RecordingAdapter {
    seen_model: Arc<std::sync::Mutex<Option<String>>>,
}

impl crate::adapters::capability::Model for RecordingAdapter {
    fn id(&self) -> &str {
        "noop"
    }
}

#[async_trait::async_trait]
impl crate::adapters::capability::ChatModel for RecordingAdapter {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        req: &crate::types::io::ChatRequest,
    ) -> Result<crate::types::io::ChatResponse, GatewayError> {
        *self.seen_model.lock().unwrap() = req.model.clone();
        Ok(crate::types::io::ChatResponse {
            content: Some("ok".to_string()),
            tool_calls: Vec::new(),
            usage: None,
            model: req.model.clone(),
            degraded: false,
        })
    }
}

#[tokio::test]
async fn chain_selection_injects_resolved_api_model_id() {
    // Model whose api_model_id ("noop-v2") differs from its registry id
    // ("noop"); the chain entry leaves api_model_id None so it must resolve
    // from the model config. The caller pins no model.
    let mut routers = HashMap::new();
    routers.insert(
        "noop".to_string(),
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
        "noop".to_string(),
        ModelConfig {
            id: "noop".to_string(),
            api_model_id: Some("noop-v2".to_string()),
            provider: "noop".to_string(),
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
        "chat_chain".to_string(),
        FallbackChainConfig {
            id: "chat_chain".to_string(),
            capability: Capability::TextChat,
            models: vec![ChainEntry {
                model: "noop".to_string(),
                router: Some("noop".to_string()),
                api_model_id: None,
                priority: 1,
            }],
            fallback_triggers: vec![],
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
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(config, AdapterRegistry::new(), cb);

    let seen_model = Arc::new(std::sync::Mutex::new(None));
    gw.adapters
        .register_chat(Arc::new(RecordingAdapter {
            seen_model: seen_model.clone(),
        }))
        .await;

    let request = InferenceRequest {
        capability: Capability::TextChat,
        model: None,
        router: None,
        chain: Some("chat_chain".to_string()),
        payload: Payload::Chat {
            messages: vec![Message::text(MessageRole::User, "hi")],
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
        routing: None,
    };
    gw.execute(&request).await.unwrap();

    assert_eq!(
        seen_model.lock().unwrap().clone(),
        Some("noop-v2".to_string()),
        "adapter should receive the chain-resolved api_model_id, not None or a default"
    );
}

#[tokio::test]
async fn execute_fills_actual_cost_from_usage_and_pricing() {
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(priced_gateway_config(), AdapterRegistry::new(), cb);
    gw.adapters.register_chat(Arc::new(UsageAdapter)).await;

    let response = gw.execute(&priced_chat_request()).await.unwrap();

    // input 1000/1000 * 0.0008 = 0.0008; output 500/1000 * 0.004 = 0.002; total 0.0028
    let cost = response
        .actual_cost
        .expect("actual_cost should be computed from usage x pricing");
    assert!(
        (cost.total_cost - 0.0028).abs() < 1e-9,
        "got {}",
        cost.total_cost
    );
    assert_eq!(cost.input_tokens, 1000);
    // The recorded attempt carries the same dollar cost.
    assert_eq!(response.attempts[0].cost, Some(cost.total_cost));
}

#[tokio::test]
async fn execute_records_successful_call_into_store() {
    // With a store attached, a successful call is persisted so burn-rate
    // (`get_spend_since`) has data — the deferred store-wiring, now live.
    use crate::store::{GatewayStore, InMemoryStore};

    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let store = Arc::new(InMemoryStore::default());
    let gw =
        Gateway::new(priced_gateway_config(), AdapterRegistry::new(), cb).with_store(store.clone());
    gw.adapters.register_chat(Arc::new(UsageAdapter)).await;

    gw.execute(&priced_chat_request()).await.unwrap();

    // input 1000/1000*0.0008 + output 500/1000*0.004 = 0.0028; a row was
    // persisted, so the windowed spend reflects it.
    let since = chrono::Utc::now() - chrono::Duration::hours(1);
    let spend = store.get_spend_since(since).await.unwrap();
    assert!(
        (spend - 0.0028).abs() < 1e-9,
        "recorded spend should match the call cost, got {spend}"
    );
}

#[tokio::test]
async fn execute_without_store_is_unchanged() {
    // No store attached ⇒ recording is a no-op and execute behaves as before.
    let gw = test_gateway();
    register_noop(&gw).await;
    let response = gw.execute(&chat_request()).await.unwrap();
    assert!(!response.attempts.is_empty());
}

// --- AUTH quota enforcement ---

/// A noop gateway with a store and the given constraints attached.
fn constrained_gateway(constraints: ConstraintsConfig, store: Arc<InMemoryStore>) -> Gateway {
    let mut config = test_config_with_noop();
    config.constraints = constraints;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    Gateway::new(config, AdapterRegistry::new(), cb).with_store(store)
}

fn authed(subject: Uuid, tier: Option<&str>) -> InferenceRequest {
    let mut r = chat_request();
    r.auth = Some(AuthContext {
        subject_id: subject,
        tier: tier.map(Into::into),
    });
    r
}

fn requests_per_day(limit: u64) -> Vec<QuotaLimit> {
    vec![QuotaLimit {
        unit: MeterUnit::Requests,
        window: Window::Day,
        limit,
    }]
}

#[tokio::test]
async fn execute_enforces_request_quota_as_hard_stop() {
    // Tier "free": 1 request/day. First authed call records one request;
    // the second is refused pre-flight with QuotaExceeded.
    let store = Arc::new(InMemoryStore::default());
    let constraints = ConstraintsConfig {
        tiers: HashMap::from([(
            "free".to_string(),
            TierConstraints {
                quota: requests_per_day(1),
                per_capability: HashMap::new(),
            },
        )]),
        default: None,
    };
    let gw = constrained_gateway(constraints, store.clone());
    register_noop(&gw).await;

    let subject = Uuid::new_v4();
    let req = authed(subject, Some("free"));

    gw.execute(&req).await.unwrap(); // within quota, records 1
    let err = gw.execute(&req).await.unwrap_err(); // exceeds
    assert!(
        matches!(
            err,
            GatewayError::QuotaExceeded {
                unit: MeterUnit::Requests,
                window: Window::Day,
                limit: 1,
                used: 1,
            }
        ),
        "expected QuotaExceeded, got {err:?}"
    );
    // The blocked call contacted no provider, so only the first recorded.
    let usage = store
        .get_usage_since(subject, Utc::now() - chrono::Duration::hours(1))
        .await
        .unwrap();
    assert_eq!(usage.requests, 1);
}

#[tokio::test]
async fn execute_without_auth_bypasses_quota() {
    // A default tier exists, but an unauthenticated request is never
    // quota-checked — recorded without a subject, always allowed.
    let store = Arc::new(InMemoryStore::default());
    let constraints = ConstraintsConfig {
        tiers: HashMap::new(),
        default: Some(TierConstraints {
            quota: requests_per_day(1),
            per_capability: HashMap::new(),
        }),
    };
    let gw = constrained_gateway(constraints, store);
    register_noop(&gw).await;
    for _ in 0..3 {
        gw.execute(&chat_request()).await.unwrap();
    }
}

#[tokio::test]
async fn execute_uses_default_tier_when_tier_absent() {
    // The request's tier isn't in the catalog, so the default applies.
    let store = Arc::new(InMemoryStore::default());
    let constraints = ConstraintsConfig {
        tiers: HashMap::new(),
        default: Some(TierConstraints {
            quota: requests_per_day(1),
            per_capability: HashMap::new(),
        }),
    };
    let gw = constrained_gateway(constraints, store);
    register_noop(&gw).await;

    let subject = Uuid::new_v4();
    let req = authed(subject, Some("no-such-tier"));
    gw.execute(&req).await.unwrap();
    let err = gw.execute(&req).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::QuotaExceeded { .. }),
        "got {err:?}"
    );
}

#[tokio::test]
async fn execute_enforces_per_capability_override() {
    // No tier-wide quota, but a per-capability cap on TextChat of 1/day.
    let store = Arc::new(InMemoryStore::default());
    let constraints = ConstraintsConfig {
        tiers: HashMap::from([(
            "free".to_string(),
            TierConstraints {
                quota: Vec::new(),
                per_capability: HashMap::from([(Capability::TextChat, requests_per_day(1))]),
            },
        )]),
        default: None,
    };
    let gw = constrained_gateway(constraints, store);
    register_noop(&gw).await;

    let subject = Uuid::new_v4();
    let req = authed(subject, Some("free"));
    gw.execute(&req).await.unwrap();
    let err = gw.execute(&req).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::QuotaExceeded { .. }),
        "got {err:?}"
    );
}

#[tokio::test]
async fn reserved_capability_returns_unsupported_not_no_adapter() {
    // A model may declare a reserved capability (e.g. ImageEdit); a request
    // for it must surface an honest "not yet supported" error rather than the
    // misleading "no adapter registered".
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
            capabilities: vec![Capability::ImageEdit],
            context_window: 4096,
            max_output_tokens: 1024,
            pricing: None,
            catalog: None,
        },
    );
    let config = GatewayConfig {
        routers,
        models,
        chains: HashMap::new(),
        constraints: Default::default(),
        panels: Default::default(),
        consensus: Default::default(),
    };
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(config, AdapterRegistry::new(), cb);

    let request = InferenceRequest {
        capability: Capability::ImageEdit,
        model: Some("m".to_string()),
        router: Some("r".to_string()),
        chain: None,
        // No ImageEdit payload exists; any constructible payload works — the
        // reserved arm errors before the payload is inspected.
        payload: Payload::ImageGenerate {
            prompt: "x".to_string(),
            size: None,
            quality: None,
            style: None,
            n: 1,
        },
        budget: None,
        auth: None,
        panel: None,
        consensus: None,
        allow_fallback: true,
        credentials: Default::default(),
        routing: None,
    };
    let msg = gw.execute(&request).await.unwrap_err().to_string();
    assert!(
        msg.contains("not yet supported") || msg.contains("reserved"),
        "expected an honest reserved-capability error, got: {msg}"
    );
}

#[tokio::test]
async fn execute_with_noop_adapter() {
    let gw = test_gateway();
    register_noop(&gw).await;

    let response = gw.execute(&chat_request()).await.unwrap();

    // The noop marks its responses `degraded: true`, which the dispatch
    // boundary propagates to `success: false` — the placeholder is not a
    // real provider result. The canned "No inference provider" content is
    // still returned.
    assert!(!response.success);
    assert!(
        response
            .content
            .as_ref()
            .unwrap()
            .contains("No inference provider")
    );
}

#[tokio::test]
async fn execute_with_only_noop_reports_degraded_success_false() {
    // With only the NoopAdapter registered, the response is a placeholder,
    // so `success` must be false (degraded) even though the call is `Ok`.
    let gw = test_gateway();
    register_noop(&gw).await;

    let response = gw.execute(&chat_request()).await.unwrap();

    assert!(!response.success);
}

#[tokio::test]
async fn execute_no_candidates_errors() {
    let gw = test_gateway();
    register_noop(&gw).await;

    // VoiceStt has no chain configured
    let request = InferenceRequest {
        capability: Capability::AudioTranscribe,
        model: None,
        router: None,
        chain: None,
        payload: Payload::Chat {
            messages: vec![Message::text(MessageRole::User, "transcribe".to_string())],
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
        routing: None,
    };

    let result = gw.execute(&request).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        GatewayError::NoCandidates { capability } => {
            assert_eq!(capability, Capability::AudioTranscribe);
        }
        other => panic!("Expected NoCandidates, got: {other}"),
    }
}

/// A caller's `routing` preferences must actually REACH selection. The two
/// production `SelectionCriteria` sites (`execute.rs`, `stream.rs`) are the only
/// path, and before SP-ROUTE-1 Task 10 wired them they hardcoded
/// `preferences: None` — so the whole feature could be built and stay inert with
/// a green suite. Un-ignored at Task 10 Step 4; it is now the regression guard
/// that catches the entire feature going inert again.
#[tokio::test]
async fn a_requests_routing_preferences_reach_selection() {
    let gw = test_gateway();
    register_noop(&gw).await;

    // `test_config_with_noop`'s only chain has exactly one candidate: router
    // "noop", model "noop". An `only` naming a router that does not exist
    // excludes it — and once excluded, nothing is left to try.
    let mut request = chat_request();
    request.routing = Some(crate::types::request::RoutingPreferences {
        only: Some(crate::types::request::CandidateSet {
            routers: vec!["nonexistent".to_string()],
            models: vec![],
        }),
        ..Default::default()
    });

    let result = gw.execute(&request).await;
    assert!(
        result.is_err(),
        "the only-router filter names a router absent from the chain, so the sole \
         candidate must be excluded and the call must fail rather than succeed \
         (degraded or not) against the noop adapter: {result:?}"
    );
}

#[tokio::test]
async fn execute_with_direct_model() {
    let gw = test_gateway();
    register_noop(&gw).await;

    let request = InferenceRequest {
        capability: Capability::TextChat,
        model: Some("noop".to_string()),
        router: Some("noop".to_string()),
        chain: None,
        payload: Payload::Chat {
            messages: vec![Message::text(MessageRole::User, "Hello".to_string())],
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
        routing: None,
    };

    let response = gw.execute(&request).await.unwrap();
    assert_eq!(response.model, Some("noop".to_string()));
}

#[tokio::test]
async fn execute_records_attempts() {
    let gw = test_gateway();
    register_noop(&gw).await;

    let response = gw.execute(&chat_request()).await.unwrap();

    assert!(!response.attempts.is_empty());
    assert_eq!(response.attempts[0].adapter, "noop");
}

/// A `HealthRecorder` that just counts how many outcomes it observed. Wired
/// in via direct access to `Gateway::recorders` — this module is a
/// descendant of `engine`, so the private field is visible here without
/// adding any public injection API (that's plan (f)'s `with_recorder`).
struct CountingRecorder(Arc<std::sync::atomic::AtomicUsize>);

impl crate::gates::HealthRecorder for CountingRecorder {
    fn on_outcome(
        &self,
        _outcome: &crate::gates::AttemptOutcome<'_>,
    ) -> Option<std::time::Instant> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        None
    }
}

/// A `HealthRecorder` that records every `AttemptPhase` it observed, in
/// order. Wired in the same way as `CountingRecorder`. Exists because every
/// `performance.rs` test hand-constructs an `AttemptOutcome` and calls
/// `PerformanceRecorder` directly — nothing drove a REAL request or stream
/// through the engine and asserted which phase it actually dispatched, so a
/// call site tagging the wrong phase (e.g. `stream.rs`'s acquisition dispatch
/// reverted to `Complete`, reinstating the very bug the phase split fixes)
/// had nothing pinning it.
struct PhaseRecorder(Arc<std::sync::Mutex<Vec<crate::gates::AttemptPhase>>>);

impl crate::gates::HealthRecorder for PhaseRecorder {
    fn on_outcome(&self, outcome: &crate::gates::AttemptOutcome<'_>) -> Option<std::time::Instant> {
        self.0.lock().unwrap().push(outcome.phase);
        None
    }
}

/// A `HealthRecorder` that records `(phase, success, output_tokens)` for
/// every dispatch. SP-ROUTE-1 Task 5 review (Important 1): the throughput
/// test originally asserted `mean_tokens_per_sec > 0.0`, which cannot fail —
/// `throughput_samples == 1`, checked two lines earlier, already forces
/// `ms > 0 && t > 0` (see `PerformanceRecorder::on_outcome`), so ANY positive
/// token count derives a positive rate. Swapping `output_tokens.output_tokens`
/// for `.input_tokens` at the dispatch site (500 → 1000) survived the whole
/// suite. This recorder captures the dispatched count directly and
/// deterministically, independent of wall-clock arithmetic.
type RecordedOutcomes = Arc<std::sync::Mutex<Vec<(crate::gates::AttemptPhase, bool, Option<u32>)>>>;

struct OutcomeRecorder(RecordedOutcomes);

impl crate::gates::HealthRecorder for OutcomeRecorder {
    fn on_outcome(&self, outcome: &crate::gates::AttemptOutcome<'_>) -> Option<std::time::Instant> {
        self.0
            .lock()
            .unwrap()
            .push((outcome.phase, outcome.success, outcome.output_tokens));
        None
    }
}

#[tokio::test]
async fn execute_fans_outcome_out_to_registered_recorders() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let mut gw = test_gateway();
    register_noop(&gw).await;
    let count = Arc::new(AtomicUsize::new(0));
    gw.recorders.push(Arc::new(CountingRecorder(count.clone())));

    let response = gw.execute(&chat_request()).await.unwrap();

    assert_eq!(response.attempts.len(), 1);
    // One successful attempt ⇒ every registered recorder sees exactly one
    // outcome (the pre-existing breaker sink plus this counting recorder).
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

/// Kills the `execute.rs` success-branch phase flip: a successful
/// non-streaming attempt must dispatch exactly one `Complete` outcome.
#[tokio::test]
async fn execute_tags_its_outcome_complete() {
    let mut gw = test_gateway();
    register_noop(&gw).await;
    let phases = Arc::new(std::sync::Mutex::new(Vec::new()));
    gw.recorders.push(Arc::new(PhaseRecorder(phases.clone())));

    gw.execute(&chat_request()).await.unwrap();

    assert_eq!(
        *phases.lock().unwrap(),
        vec![crate::gates::AttemptPhase::Complete],
        "a successful non-streaming attempt must dispatch exactly one Complete outcome"
    );
}

/// Kills the `execute.rs` error-branch phase flip: a failed non-streaming
/// attempt must ALSO dispatch `Complete` — a setup/response failure is final,
/// not a mid-stream observation. "noop" has no adapter registered, so the
/// chain exhausts after this one real attempt; the overall `Err` is expected
/// and irrelevant here — the failed attempt's tag is what's pinned.
#[tokio::test]
async fn execute_tags_a_failed_attempt_complete() {
    let mut gw = test_gateway_with_chain();
    register_failing(
        &gw,
        GatewayError::ProviderError {
            adapter: "failing".into(),
            message: "boom".into(),
            status: Some(500),
        },
    )
    .await;
    let phases = Arc::new(std::sync::Mutex::new(Vec::new()));
    gw.recorders.push(Arc::new(PhaseRecorder(phases.clone())));

    let _ = gw.execute(&chat_request()).await;

    assert_eq!(
        *phases.lock().unwrap(),
        vec![crate::gates::AttemptPhase::Complete],
        "a failed non-streaming attempt must dispatch Complete, not some other phase"
    );
}

/// Kills the `stream.rs` acquisition-dispatch phase flip back to `Complete`
/// (literally reinstating the C2 bug this slice fixed). Must DRIVE the
/// stream with `collect_stream`, not merely await `execute_stream` — the
/// dispatch happens inside the `async_stream` generator body, which only runs
/// once the stream is polled; awaiting the setup alone records zero outcomes
/// and the test would pass vacuously.
///
/// SP-ROUTE-1 Task 5 review (Minor 1): this asserts ONLY `phases[0]` — the
/// acquisition tag — even though a full successful drive now also casts a
/// trailing `StreamCompleted` (see `completed_stream_is_tagged_stream_completed_not_complete`).
/// Splitting the two catchers matters: a single test asserting the whole
/// two-element vec invites a later "simplification" to `phases[0]` that
/// would silently delete the completion-tag guard along with it.
#[tokio::test]
async fn stream_acquisition_is_tagged_stream_acquired_not_complete() {
    let mut gw = test_gateway();
    gw.adapters
        .register_chat(Arc::new(FakeStreamer {
            id: "noop".to_string(),
        }))
        .await;
    let phases = Arc::new(std::sync::Mutex::new(Vec::new()));
    gw.recorders.push(Arc::new(PhaseRecorder(phases.clone())));

    let _ = collect_stream(&gw, &chat_request()).await;

    let recorded = phases.lock().unwrap();
    assert_eq!(
        recorded.first().copied(),
        Some(crate::gates::AttemptPhase::StreamAcquired),
        "a stream acquisition must be tagged StreamAcquired, not Complete: {recorded:?}"
    );
}

/// SP-ROUTE-1 Task 5 review (Minor 1) — the sibling catcher this split
/// preserves: a successfully-drained stream's completion outcome must be
/// tagged `StreamCompleted`, never `Complete` (which would inject generation
/// time into `mean_latency_ms` — exactly the span-pooling Task 4 fixed).
#[tokio::test]
async fn completed_stream_is_tagged_stream_completed_not_complete() {
    let mut gw = test_gateway();
    gw.adapters
        .register_chat(Arc::new(FakeStreamer {
            id: "noop".to_string(),
        }))
        .await;
    let phases = Arc::new(std::sync::Mutex::new(Vec::new()));
    gw.recorders.push(Arc::new(PhaseRecorder(phases.clone())));

    let _ = collect_stream(&gw, &chat_request()).await;

    assert_eq!(
        *phases.lock().unwrap(),
        vec![
            crate::gates::AttemptPhase::StreamAcquired,
            crate::gates::AttemptPhase::StreamCompleted
        ],
        "a successfully-drained stream must cast a trailing StreamCompleted outcome, not Complete"
    );
}

/// Kills the `stream.rs` setup-failure phase flip. Same driving requirement
/// as above: `collect_stream` actually polls the generator body where the
/// setup-failure dispatch lives. "B" has no adapter registered, so the walk
/// exhausts (a terminal Error event) after "A"'s one real setup failure.
#[tokio::test]
async fn stream_setup_failure_is_tagged_complete() {
    let mut gw = ab_gateway(ab_chain_config(vec![]));
    register_stream_err(&gw, "A", || GatewayError::ProviderError {
        adapter: "A".into(),
        message: "boom".into(),
        status: Some(500),
    })
    .await;
    let phases = Arc::new(std::sync::Mutex::new(Vec::new()));
    gw.recorders.push(Arc::new(PhaseRecorder(phases.clone())));

    let _ = collect_stream(&gw, &chat_request()).await;

    assert_eq!(
        *phases.lock().unwrap(),
        vec![crate::gates::AttemptPhase::Complete],
        "a stream setup failure is final and must be tagged Complete"
    );
}

/// SP-ROUTE-1 Task 5 review (Critical 1) — the mid-stream failure terminus
/// has no phase-tag test at all: `AttemptPhase::StreamCompleted` → `Complete`
/// on that dispatch survived the whole suite. `Complete` contributes a
/// LATENCY of generation time, so a stream dying after 9s would inject
/// 9000ms into `mean_latency_ms` — precisely the span-pooling Task 4 fixed,
/// and what Task 9 sorts on.
#[tokio::test]
async fn stream_mid_failure_is_tagged_stream_completed_not_complete() {
    let mut gw = test_gateway();
    gw.adapters
        .register_chat(Arc::new(FakeStreamMidFailer {
            id: "noop".to_string(),
        }))
        .await;
    let phases = Arc::new(std::sync::Mutex::new(Vec::new()));
    gw.recorders.push(Arc::new(PhaseRecorder(phases.clone())));

    let _ = collect_stream(&gw, &chat_request()).await;

    assert_eq!(
        *phases.lock().unwrap(),
        vec![
            crate::gates::AttemptPhase::StreamAcquired,
            crate::gates::AttemptPhase::StreamCompleted
        ],
        "a stream dying mid-way must tag its completion StreamCompleted, not Complete"
    );
}

/// Task 4 review (Important 2): deleting `PerformanceRecorder` from
/// `build_recorders` passed 342/342 — nothing exercised the wiring between
/// `Gateway::record_outcome` and `Gateway::performance_stats`. This proves the
/// whole path end-to-end on a real `Gateway`: a real outcome in, the exact
/// units out (50 tokens / 1000ms == 50 tok/s) at the boundary a caller sees.
#[test]
fn record_outcome_feeds_performance_stats_through_the_real_gateway() {
    let gw = test_gateway();

    // Asserted BEFORE any outcome is recorded, so the later `expect` cannot be
    // satisfied by some fallback/default — only a real write makes this `Some`.
    assert!(
        gw.performance_stats("r:m").is_none(),
        "no attempt has been recorded yet"
    );

    gw.record_outcome(&crate::gates::AttemptOutcome {
        endpoint: "r:m",
        router: "r",
        success: true,
        error: None,
        duration_ms: 1000,
        output_tokens: Some(50),
        phase: crate::gates::AttemptPhase::Complete,
    });

    let stats = gw
        .performance_stats("r:m")
        .expect("the recorder just wrote a live sample");
    assert_eq!(stats.samples, 1);
    assert!(
        (stats.mean_tokens_per_sec - 50.0).abs() < 1e-9,
        "50 tokens in 1000ms is 50 tok/s at the Gateway boundary, got {}",
        stats.mean_tokens_per_sec
    );
}

#[tokio::test]
async fn execute_update_config_takes_effect() {
    let gw = test_gateway();
    register_noop(&gw).await;

    // First: verify execute works
    let response = gw.execute(&chat_request()).await;
    assert!(response.is_ok());

    // Update config to empty — no routers, no models, no chains
    gw.update_config(GatewayConfig::default()).await;

    // Now execute should fail with NotConfigured (empty config)
    let result = gw.execute(&chat_request()).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), GatewayError::NotConfigured));
}

// --- FailingAdapter for error/fallback path tests ---

struct FailingAdapter {
    error: GatewayError,
}

impl crate::adapters::capability::Model for FailingAdapter {
    fn id(&self) -> &str {
        "failing"
    }
}

#[async_trait::async_trait]
impl crate::adapters::capability::ChatModel for FailingAdapter {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<crate::types::io::ChatResponse, GatewayError> {
        // Clone the configured error to return each time.
        match &self.error {
            GatewayError::ProviderError {
                adapter,
                message,
                status,
            } => Err(GatewayError::ProviderError {
                adapter: adapter.clone(),
                message: message.clone(),
                status: *status,
            }),
            GatewayError::Authentication { adapter, message } => {
                Err(GatewayError::Authentication {
                    adapter: adapter.clone(),
                    message: message.clone(),
                })
            }
            GatewayError::RateLimit {
                adapter,
                retry_after_ms,
            } => Err(GatewayError::RateLimit {
                adapter: adapter.clone(),
                retry_after_ms: *retry_after_ms,
            }),
            GatewayError::Timeout {
                adapter,
                model,
                duration_ms,
            } => Err(GatewayError::Timeout {
                adapter: adapter.clone(),
                model: model.clone(),
                duration_ms: *duration_ms,
            }),
            _ => Err(GatewayError::ProviderError {
                adapter: "failing".into(),
                message: "generic failure".into(),
                status: None,
            }),
        }
    }
}

/// Config with a failing adapter as primary and noop as fallback.
fn test_config_with_failing_and_noop() -> GatewayConfig {
    let mut routers = HashMap::new();
    routers.insert(
        "failing".to_string(),
        RouterConfig {
            url: "http://localhost".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        },
    );
    routers.insert(
        "noop".to_string(),
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
        "fail-model".to_string(),
        ModelConfig {
            id: "fail-model".to_string(),
            api_model_id: None,
            provider: "failing".to_string(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window: 4096,
            max_output_tokens: 1024,
            pricing: None,
            catalog: None,
        },
    );
    models.insert(
        "noop".to_string(),
        ModelConfig {
            id: "noop".to_string(),
            api_model_id: None,
            provider: "noop".to_string(),
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
        "chat_chain".to_string(),
        FallbackChainConfig {
            id: "chat_chain".to_string(),
            capability: Capability::TextChat,
            models: vec![
                ChainEntry {
                    model: "fail-model".to_string(),
                    router: Some("failing".to_string()),
                    api_model_id: None,
                    priority: 1,
                },
                ChainEntry {
                    model: "noop".to_string(),
                    router: Some("noop".to_string()),
                    api_model_id: None,
                    priority: 2,
                },
            ],
            fallback_triggers: vec![
                FallbackTrigger::ProviderError,
                FallbackTrigger::Timeout,
                FallbackTrigger::RateLimit,
            ],
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

/// Helper: gateway with a failing adapter registered.
fn test_gateway_with_chain() -> Gateway {
    let config = test_config_with_failing_and_noop();
    let adapters = AdapterRegistry::new();
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    Gateway::new(config, adapters, cb)
}

async fn register_failing(gw: &Gateway, error: GatewayError) {
    gw.adapters
        .register_chat(Arc::new(FailingAdapter { error }))
        .await;
}

/// Like `test_gateway_with_chain` but with caller-chosen fallback triggers on
/// the `failing`→`noop` chain, so a test can prove the classify()-driven
/// in-flight fallover is independent of the configured trigger set (design
/// §3.1).
fn gateway_with_triggers(triggers: Vec<FallbackTrigger>) -> Gateway {
    let mut config = test_config_with_failing_and_noop();
    config
        .chains
        .get_mut("chat_chain")
        .expect("chat_chain exists")
        .fallback_triggers = triggers;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    Gateway::new(config, AdapterRegistry::new(), cb)
}

/// Readiness probe returning canned phases per model (Absent for unknown ids).
struct FakeProbe {
    phases: HashMap<String, kernel::ProvisionPhase>,
}

#[async_trait::async_trait]
impl kernel::ReadinessProbe for FakeProbe {
    async fn phase(&self, model: &str) -> kernel::ProvisionPhase {
        self.phases
            .get(model)
            .cloned()
            .unwrap_or(kernel::ProvisionPhase::Absent)
    }
    async fn status_all(&self) -> Vec<(String, kernel::ProvisionPhase)> {
        self.phases
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[tokio::test]
async fn exhaustion_with_in_flight_candidate_degrades_to_model_not_ready() {
    // Single-candidate chain, no adapter registered → the candidate fails
    // (no adapter) and the chain exhausts. With a probe reporting the model
    // still downloading, exhaustion degrades to a terminal ModelNotReady
    // rather than the generic AllAttemptsFailed.
    let mut phases = HashMap::new();
    phases.insert(
        "noop".to_string(),
        kernel::ProvisionPhase::Downloading {
            done: 3,
            total: Some(10),
        },
    );
    let gw = test_gateway().with_readiness(Arc::new(FakeProbe { phases }));

    match gw.execute(&chat_request()).await.unwrap_err() {
        GatewayError::ModelNotReady { model, phase } => {
            assert_eq!(model, "noop");
            assert_eq!(
                phase,
                kernel::ProvisionPhase::Downloading {
                    done: 3,
                    total: Some(10)
                }
            );
        }
        other => panic!("expected ModelNotReady, got: {other}"),
    }
}

#[tokio::test]
async fn ready_fallback_succeeds_even_with_probe_attached() {
    // Primary candidate ("failing" router) has no adapter registered — still
    // provisioning — so the walk falls through to the registered noop. The
    // request succeeds before exhaustion, so the probe is never consulted and
    // no ModelNotReady is produced.
    let gw = test_gateway_with_chain();
    register_noop(&gw).await; // registers "noop"; "failing" is left unregistered
    let mut phases = HashMap::new();
    phases.insert(
        "fail-model".to_string(),
        kernel::ProvisionPhase::Downloading {
            done: 1,
            total: Some(4),
        },
    );
    phases.insert("noop".to_string(), kernel::ProvisionPhase::Ready);
    let gw = gw.with_readiness(Arc::new(FakeProbe { phases }));

    let response = gw.execute(&chat_request()).await.unwrap();
    assert_eq!(response.model, Some("noop".to_string()));
    assert!(
        response.attempts.len() >= 2,
        "primary was attempted, then fell back to the ready candidate"
    );
}

#[tokio::test]
async fn exhaustion_without_probe_still_returns_all_attempts_failed() {
    // No probe attached → byte-identical to before this seam: chain
    // exhaustion with no adapter yields AllAttemptsFailed, not ModelNotReady.
    let gw = test_gateway();
    match gw.execute(&chat_request()).await.unwrap_err() {
        GatewayError::AllAttemptsFailed { .. } => {}
        other => panic!("expected AllAttemptsFailed, got: {other}"),
    }
}

#[tokio::test]
async fn execute_fallback_on_provider_error() {
    let gw = test_gateway_with_chain();
    register_failing(
        &gw,
        GatewayError::ProviderError {
            adapter: "failing".into(),
            message: "server error".into(),
            status: Some(500),
        },
    )
    .await;
    register_noop(&gw).await;

    let response = gw.execute(&chat_request()).await.unwrap();
    // Should have fallen back to noop after failing adapter errors
    assert_eq!(response.model, Some("noop".to_string()));
    assert!(response.attempts.len() >= 2);
    // First attempt should be failed
    assert_eq!(
        response.attempts[0].status,
        crate::types::trace::AttemptStatus::Failed
    );
    assert!(response.attempts[0].fallback_triggered);
    // Second attempt should be the noop success
    assert_eq!(
        response.attempts[1].status,
        crate::types::trace::AttemptStatus::Success
    );
}

#[tokio::test]
async fn timeout_cools_router_and_next_selection_skips_it() {
    // A `Timeout` outcome on "failing" is a transport-level fault: the
    // write-side `ConnectionCooldownSink` (registered in `Gateway::new`)
    // should cool the whole router, so the read-side `ConnectionCooldownGate`
    // skips its candidate on the very next selection.
    let gw = test_gateway_with_chain();
    register_failing(
        &gw,
        GatewayError::Timeout {
            adapter: "failing".into(),
            model: "fail-model".into(),
            duration_ms: 1,
        },
    )
    .await;
    register_noop(&gw).await;

    // First execute: "failing" times out (a configured fallback trigger), the
    // chain falls over to noop, and the outcome cools router "failing".
    let response = gw.execute(&chat_request()).await.unwrap();
    assert_eq!(response.model, Some("noop".to_string()));
    assert_eq!(response.attempts.len(), 2);
    assert_eq!(response.attempts[0].adapter, "failing");

    let until = gw.cooldown.cooling_until("failing");
    assert!(
        until.is_some_and(|u| u > Instant::now()),
        "a Timeout outcome should start an active cooldown for router 'failing'"
    );

    // Second execute: "failing" is now skipped at selection (Cooling), so
    // only noop is attempted — one successful attempt, no fallback needed.
    let response2 = gw.execute(&chat_request()).await.unwrap();
    assert_eq!(response2.model, Some("noop".to_string()));
    assert_eq!(response2.attempts.len(), 1);
    assert_eq!(response2.attempts[0].adapter, "noop");
}

#[tokio::test]
async fn with_resilience_overrides_cooldown_base() {
    // A gateway tuned with a very short `cooldown_base` (50ms) must cool the
    // router for ~50ms, NOT the 30s default: `with_resilience` rebuilds the
    // recorder pipeline from the config while preserving the SAME shared
    // `cooldown` store, so the gate reads what the (reconfigured) sink writes.
    let gw = test_gateway_with_chain().with_resilience(crate::resilience::ResilienceConfig {
        cooldown_base: std::time::Duration::from_millis(50),
        ..Default::default()
    });
    register_failing(
        &gw,
        GatewayError::Timeout {
            adapter: "failing".into(),
            model: "fail-model".into(),
            duration_ms: 1,
        },
    )
    .await;
    register_noop(&gw).await;

    // Execute so "failing" times out and gets cooled by the rebuilt sink.
    let response = gw.execute(&chat_request()).await.unwrap();
    assert_eq!(response.model, Some("noop".to_string()));

    let until = gw.cooldown.cooling_until("failing").expect("cooled");
    let now = std::time::Instant::now();
    assert!(
        until <= now + std::time::Duration::from_millis(500),
        "custom 50ms base, not 30s"
    );
}

/// `with_resilience` rebuilds the `PerformanceStore` (M3), unlike
/// cooldown/lockout which it reuses in place — and nothing exercised that
/// rebuild path. Two mutations survive without this test: (a) swapping the
/// store-rebuild above `build_recorders` so the recorders write to an
/// ORPHANED store — `performance_stats` then returns `None` forever on any
/// `.with_resilience(...)` gateway; (b) using the `DEFAULT_PERF_*` constants
/// instead of the config's fields, silently ignoring operator tuning.
#[test]
fn with_resilience_rewires_and_applies_the_performance_store() {
    let gw = test_gateway_with_chain().with_resilience(crate::resilience::ResilienceConfig {
        perf_samples: 1,
        ..Default::default()
    });

    gw.record_outcome(&crate::gates::AttemptOutcome {
        endpoint: "r:m",
        router: "r",
        success: true,
        error: None,
        duration_ms: 100,
        output_tokens: None,
        phase: crate::gates::AttemptPhase::Complete,
    });
    let s = gw
        .performance_stats("r:m")
        .expect("the rebuilt recorder must write to the store the Gateway reads");
    assert_eq!(s.samples, 1);

    gw.record_outcome(&crate::gates::AttemptOutcome {
        endpoint: "r:m",
        router: "r",
        success: true,
        error: None,
        duration_ms: 900,
        output_tokens: None,
        phase: crate::gates::AttemptPhase::Complete,
    });
    let s = gw.performance_stats("r:m").expect("still live");
    assert_eq!(s.samples, 1, "the configured perf_samples=1 caps the ring");
    assert!(
        (s.mean_latency_ms - 900.0).abs() < 1e-9,
        "the ring kept only the newest sample, got {}",
        s.mean_latency_ms
    );
}

#[tokio::test]
async fn default_gateway_still_uses_30s_cooldown() {
    // The SAME setup WITHOUT `with_resilience` preserves today's behavior: the
    // default recorder pipeline cools the router for the 30s default.
    let gw = test_gateway_with_chain();
    register_failing(
        &gw,
        GatewayError::Timeout {
            adapter: "failing".into(),
            model: "fail-model".into(),
            duration_ms: 1,
        },
    )
    .await;
    register_noop(&gw).await;

    let response = gw.execute(&chat_request()).await.unwrap();
    assert_eq!(response.model, Some("noop".to_string()));

    let until = gw.cooldown.cooling_until("failing").expect("cooled");
    let now = std::time::Instant::now();
    assert!(
        until > now + std::time::Duration::from_secs(25),
        "the 30s default cooldown is preserved"
    );
}

#[tokio::test]
async fn quota_403_locks_model_and_next_selection_skips_it() {
    // A 403-quota outcome on "failing" is a classified provider limit: the
    // write-side `ModelLockoutSink` (registered in `Gateway::new`) locks the
    // endpoint `failing:fail-model`, so the read-side `ModelLockoutGate` skips
    // that candidate on the very next selection and the chain never attempts it.
    let gw = test_gateway_with_chain();
    register_failing(
        &gw,
        GatewayError::ProviderError {
            adapter: "failing".into(),
            message: "quota exceeded".into(),
            status: Some(403),
        },
    )
    .await;
    register_noop(&gw).await;

    // First execute: "failing" returns the 403-quota (a configured ProviderError
    // fallback trigger), the chain falls over to noop, and the sink locks the
    // endpoint. Both candidates are attempted this time.
    let response = gw.execute(&chat_request()).await.unwrap();
    assert_eq!(response.model, Some("noop".to_string()));
    assert_eq!(response.attempts.len(), 2);
    assert_eq!(response.attempts[0].adapter, "failing");

    // The sink classified the 403-quota body and wrote the SHARED lockout store:
    // a timed (recoverable) QuotaExhausted lock, active into the future.
    let entry = gw
        .model_lockout
        .get("failing:fail-model")
        .expect("sink should have locked failing:fail-model");
    assert_eq!(
        entry.reason,
        crate::gates::lockout::LockReason::QuotaExhausted
    );
    assert!(
        entry.until.is_some_and(|u| u > Instant::now()),
        "a quota lock is timed and currently active"
    );

    // Second execute: the endpoint is now skipped at selection (LockedOut), so
    // only noop is attempted — a single successful attempt, no fallback. This is
    // the non-vacuous proof: if the sink had not fired or the store were not
    // shared with the gate, "failing" would be attempted first (two attempts).
    let response2 = gw.execute(&chat_request()).await.unwrap();
    assert_eq!(response2.model, Some("noop".to_string()));
    assert_eq!(response2.attempts.len(), 1);
    assert_eq!(response2.attempts[0].adapter, "noop");
}

#[tokio::test]
async fn provider_500_does_not_lock_model() {
    // A 500 `ProviderError` triggers fallback but is NOT a provider limit, so
    // `classify` returns `None` and the sink locks nothing — "failing" is still
    // attempted first on the next selection.
    let gw = test_gateway_with_chain();
    register_failing(
        &gw,
        GatewayError::ProviderError {
            adapter: "failing".into(),
            message: "server error".into(),
            status: Some(500),
        },
    )
    .await;
    register_noop(&gw).await;

    let response = gw.execute(&chat_request()).await.unwrap();
    assert_eq!(response.model, Some("noop".to_string()));
    assert_eq!(response.attempts.len(), 2);
    assert!(
        gw.model_lockout.get("failing:fail-model").is_none(),
        "a 500 is not a provider limit → nothing locked"
    );

    // Second execute: with nothing locked, "failing" is still attempted first
    // and again falls over to noop (two attempts) — proving the absence of a
    // lock is behavioral, not just an empty store.
    let response2 = gw.execute(&chat_request()).await.unwrap();
    assert_eq!(response2.model, Some("noop".to_string()));
    assert_eq!(response2.attempts.len(), 2);
    assert_eq!(response2.attempts[0].adapter, "failing");
}

#[tokio::test]
async fn no_fallback_when_disabled_stops_at_primary() {
    // `allow_fallback = false` (workspace "Automatic fallback" off): a
    // ProviderError on the primary is normally fallback-eligible, but with
    // fallback disabled the engine must NOT step down to the ready noop —
    // it returns the primary's failure with a single attempt. Guards the
    // admin toggle against becoming cosmetic.
    let gw = test_gateway_with_chain();
    register_failing(
        &gw,
        GatewayError::ProviderError {
            adapter: "failing".into(),
            message: "server error".into(),
            status: Some(500),
        },
    )
    .await;
    register_noop(&gw).await; // registered + ready, but must never be reached

    let req = InferenceRequest {
        allow_fallback: false,
        ..chat_request()
    };
    match gw.execute(&req).await.unwrap_err() {
        GatewayError::AllAttemptsFailed { attempts, .. } => {
            assert_eq!(
                attempts, 1,
                "only the primary candidate is attempted when fallback is disabled"
            );
        }
        other => panic!("expected AllAttemptsFailed (no fallback), got: {other}"),
    }
}

// --- Task 3: classify()-driven in-flight fallover (design §3.1) ---
//
// The candidate walk's fallover decision is driven by the SAME `classify()`
// used for next-request lockout: a RECOVERABLE provider limit (429 / 403-quota)
// demotes to the next candidate on THIS request even without a configured
// trigger; a TERMINAL one (401 / 403-credits) stops; a non-limit error keeps
// the configured `should_trigger_fallback` semantics. "Served by B" == the
// request completes on the second candidate (`response.model == "noop"`).

/// A 429 with NO `RateLimit` trigger configured now falls over to the next
/// candidate (recoverable). Pre-§3.1 this STOPPED at the primary.
#[tokio::test]
async fn recoverable_rate_limit_falls_over_without_trigger() {
    let gw = gateway_with_triggers(vec![]);
    register_failing(
        &gw,
        GatewayError::RateLimit {
            adapter: "failing".into(),
            retry_after_ms: None,
        },
    )
    .await;
    register_noop(&gw).await;

    let response = gw.execute(&chat_request()).await.unwrap();
    assert_eq!(
        response.model,
        Some("noop".to_string()),
        "a recoverable 429 must demote to the next candidate (served by B)"
    );
    assert_eq!(response.attempts.len(), 2);
    assert_eq!(response.attempts[0].adapter, "failing");
    assert!(response.attempts[0].fallback_triggered);
}

/// A 403-quota body with NO `ProviderError` trigger configured now falls over
/// (recoverable). Pre-§3.1 this STOPPED at the primary.
#[tokio::test]
async fn recoverable_quota_403_falls_over_without_trigger() {
    let gw = gateway_with_triggers(vec![]);
    register_failing(
        &gw,
        GatewayError::ProviderError {
            adapter: "failing".into(),
            message: "quota exceeded".into(),
            status: Some(403),
        },
    )
    .await;
    register_noop(&gw).await;

    let response = gw.execute(&chat_request()).await.unwrap();
    assert_eq!(
        response.model,
        Some("noop".to_string()),
        "a recoverable 403-quota must demote to the next candidate (served by B)"
    );
    assert_eq!(response.attempts.len(), 2);
    assert_eq!(response.attempts[0].adapter, "failing");
    assert!(response.attempts[0].fallback_triggered);
}

/// A terminal 403-credits body does NOT fall over — even WITH `ProviderError`
/// configured as a trigger, a terminal limit stops the walk (the ready noop is
/// never tried).
#[tokio::test]
async fn terminal_credits_403_stops_even_with_trigger() {
    let gw = gateway_with_triggers(vec![FallbackTrigger::ProviderError]);
    register_failing(
        &gw,
        GatewayError::ProviderError {
            adapter: "failing".into(),
            message: "insufficient credits".into(),
            status: Some(403),
        },
    )
    .await;
    register_noop(&gw).await;

    match gw.execute(&chat_request()).await.unwrap_err() {
        GatewayError::AllAttemptsFailed { attempts, .. } => assert_eq!(
            attempts, 1,
            "a terminal credits limit stops; the ready noop is never tried"
        ),
        other => panic!("expected AllAttemptsFailed, got: {other}"),
    }
}

/// **SP-OPS-1.3 — `AllAttemptsFailed.retryable` discriminates the two ways exhaustion
/// is reached.** The gateway already classifies every attempt
/// (`exhaustion::contribution_for` → `Timed` / `Terminal` / `HardFailure`) and used to
/// discard that, leaving a caller to string-match provider prose in `errors`. These pin
/// the typed answer instead.
///
/// Both cases raise the SAME variant with the SAME attempt count, which is exactly why
/// the flag is needed — nothing else in the error tells them apart.
///
/// Terminal credits: a 403-credits `Stop`s the walk with no hard fault, so exhaustion is
/// reached via `attempted_all == false`. Waiting cannot fix it.
#[tokio::test]
async fn a_terminal_credits_exhaustion_is_not_retryable() {
    let gw = gateway_with_triggers(vec![FallbackTrigger::ProviderError]);
    register_failing(
        &gw,
        GatewayError::ProviderError {
            adapter: "failing".into(),
            message: "insufficient credits".into(),
            status: Some(403),
        },
    )
    .await;
    register_noop(&gw).await;

    match gw.execute(&chat_request()).await.unwrap_err() {
        GatewayError::AllAttemptsFailed { retryable, .. } => assert!(
            !retryable,
            "a terminal credits limit never clears on its own — retrying only burns money"
        ),
        other => panic!("expected AllAttemptsFailed, got: {other}"),
    }
}

/// The other half: a 500 is a `HardFailure`, which may clear on its own. With fallback
/// disabled the single candidate exhausts, so this reaches `AllAttemptsFailed` by the
/// hard-fault route rather than the early-`Stop` one.
#[tokio::test]
async fn a_hard_fault_exhaustion_is_retryable() {
    let gw = gateway_with_triggers(vec![FallbackTrigger::ProviderError]);
    register_failing(
        &gw,
        GatewayError::ProviderError {
            adapter: "failing".into(),
            message: "boom".into(),
            status: Some(500),
        },
    )
    .await;
    register_noop(&gw).await;

    let req = InferenceRequest {
        allow_fallback: false,
        ..chat_request()
    };
    match gw.execute(&req).await.unwrap_err() {
        GatewayError::AllAttemptsFailed { retryable, .. } => assert!(
            retryable,
            "a 5xx is a non-limit fault that may clear — the orchestrator must be able to retry it"
        ),
        other => panic!("expected AllAttemptsFailed, got: {other}"),
    }
}

/// An unclassified 500 keeps the configured trigger semantics: WITH a
/// `ProviderError` trigger it falls over (the `classify()==None` branch defers
/// to `should_trigger_fallback`).
#[tokio::test]
async fn unclassified_500_falls_over_with_trigger() {
    let gw = gateway_with_triggers(vec![FallbackTrigger::ProviderError]);
    register_failing(
        &gw,
        GatewayError::ProviderError {
            adapter: "failing".into(),
            message: "boom".into(),
            status: Some(500),
        },
    )
    .await;
    register_noop(&gw).await;

    let response = gw.execute(&chat_request()).await.unwrap();
    assert_eq!(response.model, Some("noop".to_string()));
    assert_eq!(response.attempts.len(), 2);
    assert_eq!(response.attempts[0].adapter, "failing");
    assert!(response.attempts[0].fallback_triggered);
}

/// The other half of the pin: an unclassified 500 WITHOUT a `ProviderError`
/// trigger STOPS — proving the `classify()==None` branch still honors
/// `should_trigger_fallback` (not a blanket fallover).
#[tokio::test]
async fn unclassified_500_stops_without_trigger() {
    let gw = gateway_with_triggers(vec![]);
    register_failing(
        &gw,
        GatewayError::ProviderError {
            adapter: "failing".into(),
            message: "boom".into(),
            status: Some(500),
        },
    )
    .await;
    register_noop(&gw).await;

    match gw.execute(&chat_request()).await.unwrap_err() {
        GatewayError::AllAttemptsFailed { attempts, .. } => assert_eq!(
            attempts, 1,
            "an unclassified 500 with no matching trigger stops (classify()==None)"
        ),
        other => panic!("expected AllAttemptsFailed, got: {other}"),
    }
}

/// Minimal config: one keyless router "cap" with one TextChat model on it.
fn cap_config() -> GatewayConfig {
    let mut routers = HashMap::new();
    routers.insert(
        "cap".to_string(),
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
        "cap-model".to_string(),
        ModelConfig {
            id: "cap-model".to_string(),
            api_model_id: None,
            provider: "cap".to_string(),
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
        "cap_chain".to_string(),
        FallbackChainConfig {
            id: "cap_chain".to_string(),
            capability: Capability::TextChat,
            models: vec![ChainEntry {
                model: "cap-model".to_string(),
                router: Some("cap".to_string()),
                api_model_id: None,
                priority: 1,
            }],
            fallback_triggers: vec![],
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

#[tokio::test]
async fn per_call_credential_reaches_the_adapter() {
    use std::sync::Mutex;
    // Adapter that records the api_key on the RouterConfig it was dispatched with.
    struct Capturing {
        seen: Arc<Mutex<Option<String>>>,
    }
    impl crate::adapters::capability::Model for Capturing {
        fn id(&self) -> &str {
            "cap"
        }
    }
    #[async_trait::async_trait]
    impl crate::adapters::capability::ChatModel for Capturing {
        async fn chat(
            &self,
            cfg: &RouterConfig,
            _req: &crate::types::io::ChatRequest,
        ) -> Result<crate::types::io::ChatResponse, GatewayError> {
            *self.seen.lock().unwrap() = cfg.api_key.clone();
            Ok(crate::types::io::ChatResponse {
                content: Some("ok".into()),
                model: Some("cap-model".into()),
                ..Default::default()
            })
        }
    }

    let seen = Arc::new(Mutex::new(None));
    let adapters = AdapterRegistry::new();
    adapters
        .register_chat(Arc::new(Capturing { seen: seen.clone() }))
        .await;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(cap_config(), adapters, cb);

    // The wrapper resolves the tenant's key and hands it to the engine per call.
    let mut req = chat_request();
    req.credentials
        .insert("cap".to_string(), "sekret-per-call".to_string());

    let resp = gw.execute(&req).await.unwrap();
    assert!(resp.success);
    assert_eq!(
        *seen.lock().unwrap(),
        Some("sekret-per-call".to_string()),
        "the per-call credential must reach the adapter as RouterConfig.api_key"
    );
}

#[tokio::test]
async fn per_call_credential_reaches_the_stream_adapter() {
    use crate::types::request::StreamChunk;
    use std::sync::Mutex;
    // Streaming adapter that records the api_key on the config it streams with.
    struct CapturingStreamer {
        seen: Arc<Mutex<Option<String>>>,
    }
    impl crate::adapters::capability::Model for CapturingStreamer {
        fn id(&self) -> &str {
            "cap"
        }
    }
    #[async_trait::async_trait]
    impl crate::adapters::capability::ChatModel for CapturingStreamer {
        async fn chat(
            &self,
            _cfg: &RouterConfig,
            _req: &crate::types::io::ChatRequest,
        ) -> Result<crate::types::io::ChatResponse, GatewayError> {
            Ok(crate::types::io::ChatResponse::default())
        }
        async fn chat_stream(
            &self,
            cfg: &RouterConfig,
            _req: &crate::types::io::ChatRequest,
        ) -> Result<
            std::pin::Pin<
                Box<dyn futures::Stream<Item = Result<StreamChunk, GatewayError>> + Send>,
            >,
            GatewayError,
        > {
            *self.seen.lock().unwrap() = cfg.api_key.clone();
            let chunks: Vec<Result<StreamChunk, GatewayError>> = vec![Ok(StreamChunk {
                content: "ok".to_string(),
                finish_reason: Some("stop".to_string()),
                usage: None,
                tool_calls: Vec::new(),
            })];
            Ok(Box::pin(futures::stream::iter(chunks)))
        }
    }

    let seen = Arc::new(Mutex::new(None));
    let adapters = AdapterRegistry::new();
    adapters
        .register_chat(Arc::new(CapturingStreamer { seen: seen.clone() }))
        .await;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(cap_config(), adapters, cb);

    let mut req = chat_request();
    req.credentials
        .insert("cap".to_string(), "stream-sekret".to_string());

    let events = collect_stream(&gw, &req).await;
    assert!(
        events.iter().any(|e| matches!(e, StreamEvent::Done { .. })),
        "stream should complete with Done: {events:?}"
    );
    assert_eq!(
        *seen.lock().unwrap(),
        Some("stream-sekret".to_string()),
        "the per-call credential must reach the stream adapter as RouterConfig.api_key"
    );
}

#[tokio::test]
async fn execute_stops_on_auth_error() {
    // Authentication error should NOT trigger fallback — it breaks the loop
    let gw = test_gateway_with_chain();
    register_failing(
        &gw,
        GatewayError::Authentication {
            adapter: "failing".into(),
            message: "bad key".into(),
        },
    )
    .await;
    register_noop(&gw).await;

    let result = gw.execute(&chat_request()).await;
    // Should be AllAttemptsFailed because auth error is not a fallback trigger
    assert!(result.is_err());
    match result.unwrap_err() {
        GatewayError::AllAttemptsFailed { attempts, .. } => {
            assert_eq!(attempts, 1);
        }
        other => panic!("Expected AllAttemptsFailed, got: {other}"),
    }
}

#[tokio::test]
async fn execute_all_fail_returns_error() {
    // Both adapters are failing — all candidates fail
    let mut routers = HashMap::new();
    routers.insert(
        "failing".to_string(),
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
        "fail-model".to_string(),
        ModelConfig {
            id: "fail-model".to_string(),
            api_model_id: None,
            provider: "failing".to_string(),
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
        "chat_chain".to_string(),
        FallbackChainConfig {
            id: "chat_chain".to_string(),
            capability: Capability::TextChat,
            models: vec![ChainEntry {
                model: "fail-model".to_string(),
                router: Some("failing".to_string()),
                api_model_id: None,
                priority: 1,
            }],
            fallback_triggers: vec![FallbackTrigger::ProviderError],
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
    let adapters = AdapterRegistry::new();
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(config, adapters, cb);
    register_failing(
        &gw,
        GatewayError::ProviderError {
            adapter: "failing".into(),
            message: "error".into(),
            status: Some(500),
        },
    )
    .await;

    let result = gw.execute(&chat_request()).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        GatewayError::AllAttemptsFailed { attempts, .. } => {
            assert_eq!(attempts, 1);
        }
        other => panic!("Expected AllAttemptsFailed, got: {other}"),
    }
}

#[tokio::test]
async fn execute_adapter_not_found() {
    // Config references a router "ghost" but no adapter is registered for it
    let mut routers = HashMap::new();
    routers.insert(
        "ghost".to_string(),
        RouterConfig {
            url: "http://localhost".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        },
    );
    routers.insert(
        "noop".to_string(),
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
        "ghost-model".to_string(),
        ModelConfig {
            id: "ghost-model".to_string(),
            api_model_id: None,
            provider: "ghost".to_string(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window: 4096,
            max_output_tokens: 1024,
            pricing: None,
            catalog: None,
        },
    );
    models.insert(
        "noop".to_string(),
        ModelConfig {
            id: "noop".to_string(),
            api_model_id: None,
            provider: "noop".to_string(),
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
        "chat_chain".to_string(),
        FallbackChainConfig {
            id: "chat_chain".to_string(),
            capability: Capability::TextChat,
            models: vec![
                ChainEntry {
                    model: "ghost-model".to_string(),
                    router: Some("ghost".to_string()),
                    api_model_id: None,
                    priority: 1,
                },
                ChainEntry {
                    model: "noop".to_string(),
                    router: Some("noop".to_string()),
                    api_model_id: None,
                    priority: 2,
                },
            ],
            fallback_triggers: vec![FallbackTrigger::ProviderError],
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
    let adapters = AdapterRegistry::new();
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(config, adapters, cb);
    // Only register noop — "ghost" has no adapter
    register_noop(&gw).await;

    let response = gw.execute(&chat_request()).await.unwrap();
    // Ghost adapter was skipped, noop should have handled it
    assert_eq!(response.model, Some("noop".to_string()));
    assert!(response.attempts.len() >= 2);
    assert!(
        response.attempts[0]
            .error
            .as_ref()
            .unwrap()
            .contains("no adapter registered")
    );
}

#[tokio::test]
async fn execute_all_fail_populates_attempts_detail() {
    // Chain: a failing provider (ProviderError, a fallback trigger) followed
    // by a router with no registered adapter. Both fail, so the terminal
    // AllAttemptsFailed must carry the full structured Attempt records.
    let mut routers = HashMap::new();
    for id in ["failing", "ghost"] {
        routers.insert(
            id.to_string(),
            RouterConfig {
                url: "http://localhost".to_string(),
                api_key_env: None,
                api_key: None,
                enabled: true,
                timeout_ms: None,
                headers: HashMap::new(),
            },
        );
    }

    let mut models = HashMap::new();
    models.insert(
        "fail-model".to_string(),
        ModelConfig {
            id: "fail-model".to_string(),
            api_model_id: None,
            provider: "failing".to_string(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window: 4096,
            max_output_tokens: 1024,
            pricing: None,
            catalog: None,
        },
    );
    models.insert(
        "ghost-model".to_string(),
        ModelConfig {
            id: "ghost-model".to_string(),
            api_model_id: None,
            provider: "ghost".to_string(),
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
        "chat_chain".to_string(),
        FallbackChainConfig {
            id: "chat_chain".to_string(),
            capability: Capability::TextChat,
            models: vec![
                ChainEntry {
                    model: "fail-model".to_string(),
                    router: Some("failing".to_string()),
                    api_model_id: None,
                    priority: 1,
                },
                ChainEntry {
                    model: "ghost-model".to_string(),
                    router: Some("ghost".to_string()),
                    api_model_id: None,
                    priority: 2,
                },
            ],
            fallback_triggers: vec![FallbackTrigger::ProviderError],
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
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(config, AdapterRegistry::new(), cb);
    register_failing(
        &gw,
        GatewayError::ProviderError {
            adapter: "failing".into(),
            message: "server error".into(),
            status: Some(500),
        },
    )
    .await;

    match gw.execute(&chat_request()).await.unwrap_err() {
        GatewayError::AllAttemptsFailed {
            attempts,
            attempts_detail,
            ..
        } => {
            // Both the count and the structured detail agree.
            assert_eq!(attempts, attempts_detail.len());
            assert_eq!(attempts_detail.len(), 2);

            // First attempt: the provider error, which triggered fallback.
            let first = &attempts_detail[0];
            assert_eq!(first.status, crate::types::trace::AttemptStatus::Failed);
            assert_eq!(first.adapter, "failing");
            assert!(first.fallback_triggered);
            assert!(first.error.as_ref().unwrap().contains("server error"));

            // Second attempt: no adapter registered for "ghost".
            let second = &attempts_detail[1];
            assert_eq!(second.status, crate::types::trace::AttemptStatus::Failed);
            assert_eq!(second.adapter, "ghost");
            assert!(
                second
                    .error
                    .as_ref()
                    .unwrap()
                    .contains("no adapter registered")
            );
        }
        other => panic!("Expected AllAttemptsFailed, got: {other}"),
    }
}

#[test]
fn try_new_accepts_valid_config() {
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    // test_config_with_noop() is internally consistent (router/model/chain).
    let result = Gateway::try_new(test_config_with_noop(), AdapterRegistry::new(), cb);
    assert!(result.is_ok(), "valid config should pass validation");
}

#[test]
fn try_new_rejects_config_with_dangling_chain_model() {
    // Chain references a model that isn't configured — obviously invalid.
    let mut config = test_config_with_noop();
    config.chains.get_mut("chat_chain").unwrap().models[0].model = "does-not-exist".to_string();

    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    match Gateway::try_new(config, AdapterRegistry::new(), cb) {
        Err(GatewayError::InvalidConfig(msg)) => {
            assert!(
                msg.contains("unknown model") && msg.contains("does-not-exist"),
                "unexpected message: {msg}"
            );
        }
        Err(other) => panic!("Expected InvalidConfig, got: {other:?}"),
        Ok(_) => panic!("Expected InvalidConfig, got Ok(Gateway)"),
    }
}

#[test]
fn try_new_rejects_empty_config() {
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    match Gateway::try_new(GatewayConfig::default(), AdapterRegistry::new(), cb) {
        Err(GatewayError::InvalidConfig(msg)) => {
            assert!(msg.contains("no routers"), "unexpected message: {msg}");
        }
        Err(other) => panic!("Expected InvalidConfig, got: {other:?}"),
        Ok(_) => panic!("Expected InvalidConfig, got Ok(Gateway)"),
    }
}

#[tokio::test]
async fn try_update_config_rejects_invalid_and_preserves_current() {
    let gw = test_gateway();
    register_noop(&gw).await;

    // Invalid swap is rejected...
    let err = gw
        .try_update_config(GatewayConfig::default())
        .await
        .unwrap_err();
    assert!(matches!(err, GatewayError::InvalidConfig(_)));

    // ...and the original (valid) config is still in place.
    assert!(gw.is_configured().await);
    assert!(gw.execute(&chat_request()).await.is_ok());

    // A valid swap succeeds.
    gw.try_update_config(test_config_with_noop())
        .await
        .expect("valid config should be accepted");
    assert!(gw.is_configured().await);
}

// --- Streaming (execute_stream) fakes + tests ---

/// Chat adapter whose `chat_stream` yields two content chunks then a
/// terminal (empty-content) chunk carrying `TokenUsage`.
struct FakeStreamer {
    id: String,
}

impl crate::adapters::capability::Model for FakeStreamer {
    fn id(&self) -> &str {
        &self.id
    }
}

#[async_trait::async_trait]
impl crate::adapters::capability::ChatModel for FakeStreamer {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<crate::types::io::ChatResponse, GatewayError> {
        Ok(crate::types::io::ChatResponse::default())
    }

    async fn chat_stream(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<
        std::pin::Pin<
            Box<
                dyn futures::Stream<Item = Result<crate::types::request::StreamChunk, GatewayError>>
                    + Send,
            >,
        >,
        GatewayError,
    > {
        use crate::types::cost::TokenUsage;
        use crate::types::request::StreamChunk;
        let chunks: Vec<Result<StreamChunk, GatewayError>> = vec![
            Ok(StreamChunk {
                content: "Hello, ".to_string(),
                finish_reason: None,
                usage: None,
                tool_calls: Vec::new(),
            }),
            Ok(StreamChunk {
                content: "world!".to_string(),
                finish_reason: None,
                usage: None,
                tool_calls: Vec::new(),
            }),
            Ok(StreamChunk {
                content: String::new(),
                finish_reason: Some("stop".to_string()),
                usage: Some(TokenUsage {
                    input_tokens: 1000,
                    output_tokens: 500,
                    total_tokens: 1500,
                }),
                tool_calls: Vec::new(),
            }),
        ];
        Ok(Box::pin(futures::stream::iter(chunks)))
    }
}

/// Chat adapter whose `chat_stream` fails at setup (before any chunk)
/// with a `ProviderError` carrying the given HTTP status.
struct FakeStreamFailer {
    id: String,
    status: u16,
}

impl crate::adapters::capability::Model for FakeStreamFailer {
    fn id(&self) -> &str {
        &self.id
    }
}

#[async_trait::async_trait]
impl crate::adapters::capability::ChatModel for FakeStreamFailer {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<crate::types::io::ChatResponse, GatewayError> {
        Ok(crate::types::io::ChatResponse::default())
    }

    async fn chat_stream(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<
        std::pin::Pin<
            Box<
                dyn futures::Stream<Item = Result<crate::types::request::StreamChunk, GatewayError>>
                    + Send,
            >,
        >,
        GatewayError,
    > {
        Err(GatewayError::ProviderError {
            adapter: self.id.clone(),
            message: "stream setup failed".to_string(),
            status: Some(self.status),
        })
    }
}

/// Chat adapter whose `chat_stream` yields one good chunk then an error — a
/// stream that dies AFTER the caller has committed to it. Distinct from
/// `FakeStreamFailer`, which fails at setup and is already covered.
struct FakeStreamMidFailer {
    id: String,
}

impl crate::adapters::capability::Model for FakeStreamMidFailer {
    fn id(&self) -> &str {
        &self.id
    }
}

#[async_trait::async_trait]
impl crate::adapters::capability::ChatModel for FakeStreamMidFailer {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<crate::types::io::ChatResponse, GatewayError> {
        Ok(crate::types::io::ChatResponse::default())
    }

    async fn chat_stream(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<
        std::pin::Pin<
            Box<
                dyn futures::Stream<Item = Result<crate::types::request::StreamChunk, GatewayError>>
                    + Send,
            >,
        >,
        GatewayError,
    > {
        use crate::types::request::StreamChunk;
        let chunks: Vec<Result<StreamChunk, GatewayError>> = vec![
            Ok(StreamChunk {
                content: "partial".to_string(),
                finish_reason: None,
                usage: None,
                tool_calls: Vec::new(),
            }),
            Err(GatewayError::ProviderError {
                adapter: self.id.clone(),
                message: "connection reset mid-stream".to_string(),
                status: Some(500),
            }),
        ];
        Ok(Box::pin(futures::stream::iter(chunks)))
    }
}

/// Same chunk shape as `FakeStreamer` (two content chunks then a terminal
/// chunk carrying `TokenUsage { output_tokens: 500 }`), but with a real
/// (tiny) delay before the terminal chunk.
///
/// Why this exists rather than reusing `FakeStreamer` directly: the
/// `StreamCompleted` dispatch times generation with `std::time::Instant`,
/// a real wall clock that tokio's mock-time (`test-util`) cannot advance.
/// `FakeStreamer`'s chunks come from a synchronous `futures::stream::iter`,
/// so a whole attempt completes in low-microseconds — `duration_ms` always
/// truncates to 0, and `PerformanceRecorder::on_outcome`'s `ms > 0` guard
/// then (correctly) declines to invent a throughput sample. That guard is
/// exactly right in production (no rate should ever be reported over an
/// unmeasurable span); it just means a throughput assertion needs a fixture
/// where measurable time genuinely elapses.
struct FakeStreamerWithRealDelay {
    id: String,
}

impl crate::adapters::capability::Model for FakeStreamerWithRealDelay {
    fn id(&self) -> &str {
        &self.id
    }
}

#[async_trait::async_trait]
impl crate::adapters::capability::ChatModel for FakeStreamerWithRealDelay {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<crate::types::io::ChatResponse, GatewayError> {
        Ok(crate::types::io::ChatResponse::default())
    }

    async fn chat_stream(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<
        std::pin::Pin<
            Box<
                dyn futures::Stream<Item = Result<crate::types::request::StreamChunk, GatewayError>>
                    + Send,
            >,
        >,
        GatewayError,
    > {
        use crate::types::cost::TokenUsage;
        use crate::types::request::StreamChunk;
        let stream = async_stream::stream! {
            yield Ok(StreamChunk {
                content: "Hello, ".to_string(),
                finish_reason: None,
                usage: None,
                tool_calls: Vec::new(),
            });
            yield Ok(StreamChunk {
                content: "world!".to_string(),
                finish_reason: None,
                usage: None,
                tool_calls: Vec::new(),
            });
            // The real, unmocked delay this fixture exists for.
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            yield Ok(StreamChunk {
                content: String::new(),
                finish_reason: Some("stop".to_string()),
                usage: Some(TokenUsage {
                    input_tokens: 1000,
                    output_tokens: 500,
                    total_tokens: 1500,
                }),
                tool_calls: Vec::new(),
            });
        };
        Ok(Box::pin(stream))
    }
}

/// Like `FakeStreamMidFailer`, but the mid-stream error is caller-supplied.
/// `FakeStreamMidFailer` hardcodes a `ProviderError`; the SP-ROUTE-1 Task 5
/// review's cooldown/lockout/escalation tests need OTHER error kinds
/// (`Timeout` to cool the router, `RateLimit` to lock the endpoint) — the
/// same closure convention `FakeStreamErrAdapter` already uses for setup
/// failures, applied to the mid-stream terminus instead.
struct FakeStreamMidFailerWith {
    id: String,
    err: Arc<dyn Fn() -> GatewayError + Send + Sync>,
}

impl crate::adapters::capability::Model for FakeStreamMidFailerWith {
    fn id(&self) -> &str {
        &self.id
    }
}

#[async_trait::async_trait]
impl crate::adapters::capability::ChatModel for FakeStreamMidFailerWith {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<crate::types::io::ChatResponse, GatewayError> {
        Ok(crate::types::io::ChatResponse::default())
    }

    async fn chat_stream(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<
        std::pin::Pin<
            Box<
                dyn futures::Stream<Item = Result<crate::types::request::StreamChunk, GatewayError>>
                    + Send,
            >,
        >,
        GatewayError,
    > {
        use crate::types::request::StreamChunk;
        let chunks: Vec<Result<StreamChunk, GatewayError>> = vec![
            Ok(StreamChunk {
                content: "partial".to_string(),
                finish_reason: None,
                usage: None,
                tool_calls: Vec::new(),
            }),
            Err((self.err)()),
        ];
        Ok(Box::pin(futures::stream::iter(chunks)))
    }
}

async fn register_stream_mid_err(
    gw: &Gateway,
    id: &str,
    err: impl Fn() -> GatewayError + Send + Sync + 'static,
) {
    gw.adapters
        .register_chat(Arc::new(FakeStreamMidFailerWith {
            id: id.to_string(),
            err: Arc::new(err),
        }))
        .await;
}

/// A mid-stream failer whose one good chunk carries `usage` — a provider that
/// reports token counts and then dies. Real (tiny) delay before the error, for
/// the same reason `FakeStreamerWithRealDelay` needs one: without it,
/// `duration_ms` truncates to 0 and the M3 mutation (restoring a throughput
/// sample to the failure dispatch) would be masked by the `ms > 0` guard
/// rather than caught by the assertion.
struct FakeStreamMidFailerWithUsage {
    id: String,
}

impl crate::adapters::capability::Model for FakeStreamMidFailerWithUsage {
    fn id(&self) -> &str {
        &self.id
    }
}

#[async_trait::async_trait]
impl crate::adapters::capability::ChatModel for FakeStreamMidFailerWithUsage {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<crate::types::io::ChatResponse, GatewayError> {
        Ok(crate::types::io::ChatResponse::default())
    }

    async fn chat_stream(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<
        std::pin::Pin<
            Box<
                dyn futures::Stream<Item = Result<crate::types::request::StreamChunk, GatewayError>>
                    + Send,
            >,
        >,
        GatewayError,
    > {
        use crate::types::cost::TokenUsage;
        use crate::types::request::StreamChunk;
        let id = self.id.clone();
        let stream = async_stream::stream! {
            yield Ok(StreamChunk {
                content: "partial".to_string(),
                finish_reason: None,
                usage: Some(TokenUsage {
                    input_tokens: 100,
                    output_tokens: 50,
                    total_tokens: 150,
                }),
                tool_calls: Vec::new(),
            });
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            yield Err(GatewayError::ProviderError {
                adapter: id.clone(),
                message: "connection reset mid-stream".to_string(),
                status: Some(500),
            });
        };
        Ok(Box::pin(stream))
    }
}

async fn collect_stream(gw: &Gateway, request: &InferenceRequest) -> Vec<StreamEvent> {
    use futures::StreamExt;
    let mut stream = gw
        .execute_stream(request)
        .await
        .expect("stream should start");
    let mut events = Vec::new();
    while let Some(ev) = stream.next().await {
        events.push(ev);
    }
    events
}

/// Drive one streaming attempt if the endpoint currently admits one, silently
/// doing nothing if selection is gated (e.g. the breaker is already Open) —
/// unlike `collect_stream`, which `.expect()`s a stream and panics on `Err`.
/// Needed to drive an endpoint PAST the point its breaker opens without the
/// test panicking on the very call that (correctly) starts failing selection.
async fn try_drain_stream(gw: &Gateway, request: &InferenceRequest) {
    use futures::StreamExt;
    if let Ok(mut stream) = gw.execute_stream(request).await {
        while stream.next().await.is_some() {}
    }
}

#[tokio::test]
async fn execute_stream_yields_chunks_then_done_with_cost() {
    use crate::types::config::ModelPricing;

    let mut routers = HashMap::new();
    routers.insert(
        "priced".to_string(),
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
        "priced".to_string(),
        ModelConfig {
            id: "priced".to_string(),
            api_model_id: None,
            provider: "priced".to_string(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window: 4096,
            max_output_tokens: 1024,
            pricing: Some(ModelPricing {
                input_per_1k: 0.0008,
                output_per_1k: 0.004,
                per_request: None,
            }),
            catalog: None,
        },
    );
    let config = GatewayConfig {
        routers,
        models,
        chains: HashMap::new(),
        constraints: Default::default(),
        panels: Default::default(),
        consensus: Default::default(),
    };
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(config, AdapterRegistry::new(), cb);
    gw.adapters
        .register_chat(Arc::new(FakeStreamer {
            id: "priced".to_string(),
        }))
        .await;

    let request = InferenceRequest {
        capability: Capability::TextChat,
        model: Some("priced".to_string()),
        router: Some("priced".to_string()),
        chain: None,
        payload: Payload::Chat {
            messages: vec![Message::text(MessageRole::User, "hi")],
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
        routing: None,
    };

    let events = collect_stream(&gw, &request).await;

    assert_eq!(events.len(), 3, "two chunks + one done, got {events:?}");
    match &events[0] {
        StreamEvent::Chunk { content } => assert_eq!(content, "Hello, "),
        other => panic!("expected first Chunk, got {other:?}"),
    }
    match &events[1] {
        StreamEvent::Chunk { content } => assert_eq!(content, "world!"),
        other => panic!("expected second Chunk, got {other:?}"),
    }
    match &events[2] {
        StreamEvent::Done {
            model,
            tokens,
            cost,
        } => {
            assert_eq!(model, "priced");
            assert_eq!(tokens.input_tokens, 1000);
            assert_eq!(tokens.output_tokens, 500);
            // input 1000/1000*0.0008 = 0.0008; output 500/1000*0.004 = 0.002; total 0.0028
            assert!((cost - 0.0028).abs() < 1e-9, "got {cost}");
        }
        other => panic!("expected Done, got {other:?}"),
    }
}

#[tokio::test]
async fn execute_stream_falls_back_before_first_byte() {
    // Chain: a failing streamer (ProviderError 500 at setup) then a
    // healthy streamer. ProviderError is in the fallback triggers, so the
    // stream must switch providers before the first byte.
    let mut routers = HashMap::new();
    for id in ["failing", "good"] {
        routers.insert(
            id.to_string(),
            RouterConfig {
                url: "http://localhost".to_string(),
                api_key_env: None,
                api_key: None,
                enabled: true,
                timeout_ms: None,
                headers: HashMap::new(),
            },
        );
    }
    let mut models = HashMap::new();
    models.insert(
        "fail-model".to_string(),
        ModelConfig {
            id: "fail-model".to_string(),
            api_model_id: None,
            provider: "failing".to_string(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window: 4096,
            max_output_tokens: 1024,
            pricing: None,
            catalog: None,
        },
    );
    models.insert(
        "good".to_string(),
        ModelConfig {
            id: "good".to_string(),
            api_model_id: None,
            provider: "good".to_string(),
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
        "chat_chain".to_string(),
        FallbackChainConfig {
            id: "chat_chain".to_string(),
            capability: Capability::TextChat,
            models: vec![
                ChainEntry {
                    model: "fail-model".to_string(),
                    router: Some("failing".to_string()),
                    api_model_id: None,
                    priority: 1,
                },
                ChainEntry {
                    model: "good".to_string(),
                    router: Some("good".to_string()),
                    api_model_id: None,
                    priority: 2,
                },
            ],
            fallback_triggers: vec![FallbackTrigger::ProviderError],
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
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(config, AdapterRegistry::new(), cb);
    gw.adapters
        .register_chat(Arc::new(FakeStreamFailer {
            id: "failing".to_string(),
            status: 500,
        }))
        .await;
    gw.adapters
        .register_chat(Arc::new(FakeStreamer {
            id: "good".to_string(),
        }))
        .await;

    let events = collect_stream(&gw, &chat_request()).await;

    // Sequence: ProviderSwitch (failing -> good), then good's two chunks,
    // then Done. No Error, since fallback succeeded before any byte.
    match &events[0] {
        StreamEvent::ProviderSwitch {
            from_adapter,
            from_model,
            to_adapter,
            to_model,
            ..
        } => {
            assert_eq!(from_adapter, "failing");
            assert_eq!(from_model, "fail-model");
            assert_eq!(to_adapter, "good");
            assert_eq!(to_model, "good");
        }
        other => panic!("expected leading ProviderSwitch, got {other:?}"),
    }
    let chunks = events
        .iter()
        .filter(|e| matches!(e, StreamEvent::Chunk { .. }))
        .count();
    assert_eq!(chunks, 2, "expected the second adapter's two chunks");
    assert!(
        matches!(events.last().unwrap(), StreamEvent::Done { model, .. } if model == "good"),
        "expected a terminal Done for the good model, got {:?}",
        events.last()
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, StreamEvent::Error { .. })),
        "successful fallback must not emit an Error event"
    );
}

#[tokio::test]
async fn execute_stream_non_chat_capability_errors_up_front() {
    // Embedding is not a chat capability, so streaming is rejected before
    // any stream is produced.
    let gw = test_gateway();
    register_noop(&gw).await;

    let request = InferenceRequest {
        capability: Capability::TextEmbed,
        model: None,
        router: None,
        chain: None,
        payload: Payload::Embed {
            texts: vec!["hello".to_string()],
        },
        budget: None,
        auth: None,
        panel: None,
        consensus: None,
        allow_fallback: true,
        credentials: Default::default(),
        routing: None,
    };

    match gw.execute_stream(&request).await {
        Err(GatewayError::Unsupported { adapter, what }) => {
            assert_eq!(adapter, "gateway");
            assert!(what.contains("streaming"), "unexpected `what`: {what}");
        }
        Err(other) => panic!("expected Unsupported, got: {other}"),
        Ok(_) => panic!("expected Err(Unsupported), got a stream"),
    }
}

// --- Task 5: execute_stream AllGated + §3.1 fallover + retained-error gap ---
//
// `execute_stream` reaches parity with `execute`: selection-empty where every
// candidate is gated returns `Err(AllGated)`; a recoverable provider limit at
// stream setup falls over on THIS request (§3.1, independent of the configured
// triggers); stream-setup exhaustion where every attempt was gated yields a
// terminal `StreamEvent::Error { code: "all_gated", resume_after: Some(_) }`;
// and — the gap this closes — the REAL `GatewayError` now reaches the recorder
// sinks at setup, so a setup failure cools/locks its router/endpoint.

/// A chat adapter whose `chat_stream` fails at setup with a freshly-built
/// error (the streaming analogue of `ChatErrAdapter`), so distinct routers can
/// return distinct setup failures in one gateway.
struct FakeStreamErrAdapter {
    id: String,
    err: Arc<dyn Fn() -> GatewayError + Send + Sync>,
}

impl crate::adapters::capability::Model for FakeStreamErrAdapter {
    fn id(&self) -> &str {
        &self.id
    }
}

#[async_trait::async_trait]
impl crate::adapters::capability::ChatModel for FakeStreamErrAdapter {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<crate::types::io::ChatResponse, GatewayError> {
        Ok(crate::types::io::ChatResponse::default())
    }

    async fn chat_stream(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<
        std::pin::Pin<
            Box<
                dyn futures::Stream<Item = Result<crate::types::request::StreamChunk, GatewayError>>
                    + Send,
            >,
        >,
        GatewayError,
    > {
        Err((self.err)())
    }
}

async fn register_stream_err(
    gw: &Gateway,
    id: &str,
    err: impl Fn() -> GatewayError + Send + Sync + 'static,
) {
    gw.adapters
        .register_chat(Arc::new(FakeStreamErrAdapter {
            id: id.to_string(),
            err: Arc::new(err),
        }))
        .await;
}

/// (1) Every candidate pre-locked (timed quota) at selection → `execute_stream`
/// returns `Err(AllGated)` BEFORE the stream, `resume_after` = min over the two
/// expiries (B at ~1800s). The streaming analogue of the `execute` selection
/// case.
#[tokio::test]
async fn execute_stream_all_gated_at_selection_returns_allgated() {
    use crate::gates::lockout::LockReason;

    let gw = ab_gateway(ab_chain_config(vec![]));
    gw.apply_lockout(
        "A:a-model",
        LockReason::QuotaExhausted,
        Some(Instant::now() + std::time::Duration::from_secs(3600)),
    );
    gw.apply_lockout(
        "B:b-model",
        LockReason::QuotaExhausted,
        Some(Instant::now() + std::time::Duration::from_secs(1800)),
    );

    match gw.execute_stream(&chat_request()).await {
        Err(GatewayError::AllGated {
            resume_after,
            human_action,
            ..
        }) => {
            assert_resume_near(resume_after, 1800, 150); // the nearer of 3600 / 1800
            assert!(
                human_action.is_none(),
                "a timed retry means no human action is surfaced"
            );
        }
        Err(other) => panic!("expected Err(AllGated), got: {other}"),
        Ok(_) => panic!("expected Err(AllGated) before any stream, got a stream"),
    }
}

/// (2) §3.1 in the stream: `[A (429 at setup), B (ok)]` with NO configured
/// triggers → A's recoverable rate-limit demotes to B on THIS request (a
/// `ProviderSwitch` then B's chunks then `Done { model: "b-model" }`), where
/// pre-§3.1 a 429 without a `RateLimit` trigger terminated the stream.
#[tokio::test]
async fn execute_stream_recoverable_limit_falls_over_without_trigger() {
    let gw = ab_gateway(ab_chain_config(vec![]));
    register_stream_err(&gw, "A", || GatewayError::RateLimit {
        adapter: "A".into(),
        retry_after_ms: None,
    })
    .await;
    gw.adapters
        .register_chat(Arc::new(FakeStreamer {
            id: "B".to_string(),
        }))
        .await;

    let events = collect_stream(&gw, &chat_request()).await;

    assert!(
        matches!(events.first(), Some(StreamEvent::ProviderSwitch { from_adapter, to_adapter, .. }) if from_adapter == "A" && to_adapter == "B"),
        "a recoverable 429 at setup must demote to B (leading ProviderSwitch), got {events:?}"
    );
    assert!(
        matches!(events.last(), Some(StreamEvent::Done { model, .. }) if model == "b-model"),
        "the request must complete on B (Done for b-model), got {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, StreamEvent::Error { .. })),
        "a successful §3.1 fallover must not emit an Error event, got {events:?}"
    );
}

/// (3) Stream-setup exhaustion where every attempt was gated: `[A (429), B
/// (429)]` with NO configured triggers → both fall over at setup and each is
/// locked in-flight, so the LAST event is a terminal
/// `StreamEvent::Error { code: "all_gated", resume_after: Some(_) }` (~60s
/// rate-limit base, the min over the just-written timed locks).
#[tokio::test]
async fn execute_stream_setup_exhaustion_all_recoverable_is_all_gated() {
    let gw = ab_gateway(ab_chain_config(vec![]));
    register_stream_err(&gw, "A", || GatewayError::RateLimit {
        adapter: "A".into(),
        retry_after_ms: None,
    })
    .await;
    register_stream_err(&gw, "B", || GatewayError::RateLimit {
        adapter: "B".into(),
        retry_after_ms: None,
    })
    .await;

    let events = collect_stream(&gw, &chat_request()).await;

    match events.last() {
        Some(StreamEvent::Error {
            code, resume_after, ..
        }) => {
            assert_eq!(code, "all_gated", "every attempt gated ⇒ all_gated code");
            assert_resume_near(*resume_after, 60, 40); // rate-limit base ~60s
        }
        other => panic!("expected a terminal all_gated Error, got {other:?}"),
    }
}

/// (4) Gap-closer: a stream-setup `Timeout` on A now reaches the recorder
/// sinks, so router "A" is cooled (previously the setup error was dropped and
/// `dispatch_outcome` was passed `None`, so nothing cooled). `[A (Timeout), B
/// (ok)]` with a `Timeout` trigger falls over to B; the cooldown on "A" proves
/// the real error was dispatched.
#[tokio::test]
async fn execute_stream_setup_failure_cools_router_closing_the_gap() {
    let gw = ab_gateway(ab_chain_config(vec![FallbackTrigger::Timeout]));
    register_stream_err(&gw, "A", || GatewayError::Timeout {
        adapter: "A".into(),
        model: "a-model".into(),
        duration_ms: 1,
    })
    .await;
    gw.adapters
        .register_chat(Arc::new(FakeStreamer {
            id: "B".to_string(),
        }))
        .await;

    let events = collect_stream(&gw, &chat_request()).await;
    // Sanity: A's Timeout fell over to B and the stream completed on B.
    assert!(
        matches!(events.last(), Some(StreamEvent::Done { model, .. }) if model == "b-model"),
        "expected the stream to complete on B, got {events:?}"
    );

    // The gap: the real Timeout reached the ConnectionCooldownSink, cooling "A".
    let until = gw.cooldown.cooling_until("A");
    assert!(
        until.is_some_and(|u| u > Instant::now()),
        "a stream-setup Timeout must cool router 'A' (the real error now reaches the sinks)"
    );
}

/// (5) Guard: an early terminal stop leaves a PLAIN error, not all_gated.
/// `[A (401 auth), B (ok)]` → A's terminal auth stops the walk with B still
/// untried, so the terminal `StreamEvent::Error` carries `resume_after: None`
/// and is NOT coded `all_gated` (mirrors `execute_stops_on_auth_error`'s
/// guard: an untried, still-eligible candidate means "not every candidate was
/// gated").
#[tokio::test]
async fn execute_stream_early_terminal_stop_is_not_all_gated() {
    let gw = ab_gateway(ab_chain_config(vec![]));
    register_stream_err(&gw, "A", || GatewayError::Authentication {
        adapter: "A".into(),
        message: "bad key".into(),
    })
    .await;
    gw.adapters
        .register_chat(Arc::new(FakeStreamer {
            id: "B".to_string(),
        }))
        .await;

    let events = collect_stream(&gw, &chat_request()).await;

    assert!(
        !events.iter().any(|e| matches!(e, StreamEvent::Done { .. })),
        "the untried B must never stream (auth stops the walk), got {events:?}"
    );
    match events.last() {
        Some(StreamEvent::Error {
            code, resume_after, ..
        }) => {
            assert_ne!(code, "all_gated", "an early terminal stop is NOT all_gated");
            assert_eq!(code, "authentication");
            assert!(
                resume_after.is_none(),
                "a non-fallback stop with an untried candidate carries no resume_after"
            );
        }
        other => panic!("expected a terminal plain Error, got {other:?}"),
    }
}

// --- Panel fan-out (execute_panel) ---

fn router(enabled: bool) -> RouterConfig {
    RouterConfig {
        url: "http://localhost".to_string(),
        api_key_env: None,
        api_key: None,
        enabled,
        timeout_ms: None,
        headers: HashMap::new(),
    }
}

fn chat_model(id: &str, provider: &str, family: &str) -> ModelConfig {
    ModelConfig {
        id: id.to_string(),
        api_model_id: None,
        provider: provider.to_string(),
        family: Some(family.to_string()),
        capabilities: vec![Capability::TextChat],
        context_window: 4096,
        max_output_tokens: 1024,
        pricing: None,
        catalog: None,
    }
}

fn one_model_chain(id: &str, model: &str, router: &str) -> FallbackChainConfig {
    FallbackChainConfig {
        id: id.to_string(),
        capability: Capability::TextChat,
        models: vec![ChainEntry {
            model: model.to_string(),
            router: Some(router.to_string()),
            api_model_id: None,
            priority: 1,
        }],
        fallback_triggers: vec![],
    }
}

#[tokio::test]
async fn execute_panel_fans_out_and_isolates_slot_failures() {
    use crate::types::config::{DistinctBy, PanelConfig, PanelSlot};

    // gemma + qwen are served by the registered noop adapter; the "wildcard"
    // slot routes to "ghost" (no adapter) so it fails alone.
    let routers = HashMap::from([
        ("noop".to_string(), router(true)),
        ("ghost".to_string(), router(true)),
    ]);
    let models = HashMap::from([
        (
            "m-gemma".to_string(),
            chat_model("m-gemma", "noop", "gemma"),
        ),
        ("m-qwen".to_string(), chat_model("m-qwen", "noop", "qwen")),
        ("m-ghost".to_string(), chat_model("m-ghost", "ghost", "phi")),
    ]);
    let chains = HashMap::from([
        (
            "c-gemma".to_string(),
            one_model_chain("c-gemma", "m-gemma", "noop"),
        ),
        (
            "c-qwen".to_string(),
            one_model_chain("c-qwen", "m-qwen", "noop"),
        ),
        (
            "c-ghost".to_string(),
            one_model_chain("c-ghost", "m-ghost", "ghost"),
        ),
    ]);
    let config = GatewayConfig {
        routers,
        models,
        chains,
        constraints: Default::default(),
        panels: Default::default(),
        consensus: Default::default(),
    };
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(config, AdapterRegistry::new(), cb);
    register_noop(&gw).await; // "noop" only; "ghost" has no adapter

    let panel = PanelConfig {
        id: "consensus".to_string(),
        capability: Capability::TextChat,
        distinct_by: DistinctBy::Family,
        strict: false,
        slots: vec![
            PanelSlot {
                chain: "c-gemma".to_string(),
                label: Some("proposer".to_string()),
                system_prompt: None,
            },
            PanelSlot {
                chain: "c-qwen".to_string(),
                label: Some("challenger".to_string()),
                system_prompt: None,
            },
            PanelSlot {
                chain: "c-ghost".to_string(),
                label: Some("wildcard".to_string()),
                system_prompt: None,
            },
        ],
    };

    let resp = gw.execute_panel(&chat_request(), &panel).await.unwrap();
    assert_eq!(resp.slots.len(), 3);

    let slot = |label: &str| {
        resp.slots
            .iter()
            .find(|s| s.label.as_deref() == Some(label))
            .unwrap_or_else(|| panic!("missing slot {label}"))
    };
    assert!(slot("proposer").result.is_ok());
    assert!(slot("challenger").result.is_ok());
    // The ghost slot has no adapter → its execute() fails, but in isolation.
    assert!(slot("wildcard").result.is_err());
    assert_eq!(slot("proposer").family.as_deref(), Some("gemma"));
    assert_eq!(slot("challenger").family.as_deref(), Some("qwen"));
    assert!(
        resp.collisions.is_empty(),
        "distinct families should not collide, got {:?}",
        resp.collisions
    );
}

#[tokio::test]
async fn execute_panel_rejects_same_family_at_formation() {
    use crate::types::config::{DistinctBy, PanelConfig, PanelSlot};

    // Both slots resolve to a gemma-family primary → formation must reject
    // before any inference under distinct_by = family.
    let routers = HashMap::from([("noop".to_string(), router(true))]);
    let models = HashMap::from([
        ("m1".to_string(), chat_model("m1", "noop", "gemma")),
        ("m2".to_string(), chat_model("m2", "noop", "gemma")),
    ]);
    let chains = HashMap::from([
        ("c1".to_string(), one_model_chain("c1", "m1", "noop")),
        ("c2".to_string(), one_model_chain("c2", "m2", "noop")),
    ]);
    let config = GatewayConfig {
        routers,
        models,
        chains,
        constraints: Default::default(),
        panels: Default::default(),
        consensus: Default::default(),
    };
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(config, AdapterRegistry::new(), cb);
    register_noop(&gw).await;

    let panel = PanelConfig {
        id: "bad".to_string(),
        capability: Capability::TextChat,
        distinct_by: DistinctBy::Family,
        strict: false,
        slots: vec![
            PanelSlot {
                chain: "c1".to_string(),
                label: None,
                system_prompt: None,
            },
            PanelSlot {
                chain: "c2".to_string(),
                label: None,
                system_prompt: None,
            },
        ],
    };

    let err = gw.execute_panel(&chat_request(), &panel).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::InvalidConfig(_)),
        "same-family panel must fail formation, got {err:?}"
    );
}

#[tokio::test]
async fn execute_panel_applies_per_slot_system_prompt() {
    use crate::types::config::{DistinctBy, PanelConfig, PanelSlot};

    // A chat adapter that records the system prompt of every request it
    // serves, so we can prove each slot's persona reached *its own* outgoing
    // request (gh#18) rather than every slot getting the identical request.
    struct SystemRecorder {
        seen: Arc<std::sync::Mutex<Vec<Option<String>>>>,
    }
    impl crate::adapters::capability::Model for SystemRecorder {
        fn id(&self) -> &str {
            "noop"
        }
    }
    #[async_trait::async_trait]
    impl crate::adapters::capability::ChatModel for SystemRecorder {
        async fn chat(
            &self,
            _cfg: &RouterConfig,
            req: &crate::types::io::ChatRequest,
        ) -> Result<crate::types::io::ChatResponse, GatewayError> {
            self.seen.lock().unwrap().push(req.system.clone());
            Ok(crate::types::io::ChatResponse {
                content: Some("ok".to_string()),
                tool_calls: Vec::new(),
                usage: None,
                model: req.model.clone(),
                degraded: false,
            })
        }
    }

    let routers = HashMap::from([("noop".to_string(), router(true))]);
    let models = HashMap::from([
        (
            "m-gemma".to_string(),
            chat_model("m-gemma", "noop", "gemma"),
        ),
        ("m-qwen".to_string(), chat_model("m-qwen", "noop", "qwen")),
    ]);
    let chains = HashMap::from([
        (
            "c-gemma".to_string(),
            one_model_chain("c-gemma", "m-gemma", "noop"),
        ),
        (
            "c-qwen".to_string(),
            one_model_chain("c-qwen", "m-qwen", "noop"),
        ),
    ]);
    let config = GatewayConfig {
        routers,
        models,
        chains,
        constraints: Default::default(),
        panels: Default::default(),
        consensus: Default::default(),
    };
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(config, AdapterRegistry::new(), cb);
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    gw.adapters
        .register_chat(Arc::new(SystemRecorder { seen: seen.clone() }))
        .await;

    let panel = PanelConfig {
        id: "p".to_string(),
        capability: Capability::TextChat,
        distinct_by: DistinctBy::Family,
        strict: false,
        slots: vec![
            PanelSlot {
                chain: "c-gemma".to_string(),
                label: Some("proposer".to_string()),
                system_prompt: Some("You are the proposer.".to_string()),
            },
            PanelSlot {
                chain: "c-qwen".to_string(),
                label: Some("challenger".to_string()),
                system_prompt: Some("You are the challenger.".to_string()),
            },
        ],
    };

    // Base request carries no system prompt, so each slot's outgoing system
    // is exactly that slot's persona.
    let resp = gw.execute_panel(&chat_request(), &panel).await.unwrap();
    assert!(resp.slots.iter().all(|s| s.result.is_ok()));

    let mut got = seen.lock().unwrap().clone();
    got.sort();
    assert_eq!(
        got,
        vec![
            Some("You are the challenger.".to_string()),
            Some("You are the proposer.".to_string()),
        ],
        "each slot's system prompt must reach its own outgoing request",
    );
}

// --- Consensus workflow (execute_consensus) ---

#[tokio::test]
async fn execute_consensus_debate_synthesize_judge() {
    use crate::types::config::{ConsensusConfig, DistinctBy, PanelConfig, PanelSlot, RoleSpec};

    let routers = HashMap::from([("noop".to_string(), router(true))]);
    let models = HashMap::from([
        (
            "m-gemma".to_string(),
            chat_model("m-gemma", "noop", "gemma"),
        ),
        ("m-qwen".to_string(), chat_model("m-qwen", "noop", "qwen")),
        (
            "m-synth".to_string(),
            chat_model("m-synth", "noop", "mixtral"),
        ),
        ("m-judge".to_string(), chat_model("m-judge", "noop", "phi")),
    ]);
    let chains = HashMap::from([
        (
            "c-gemma".to_string(),
            one_model_chain("c-gemma", "m-gemma", "noop"),
        ),
        (
            "c-qwen".to_string(),
            one_model_chain("c-qwen", "m-qwen", "noop"),
        ),
        (
            "c-synth".to_string(),
            one_model_chain("c-synth", "m-synth", "noop"),
        ),
        (
            "c-judge".to_string(),
            one_model_chain("c-judge", "m-judge", "noop"),
        ),
    ]);
    let config = GatewayConfig {
        routers,
        models,
        chains,
        constraints: Default::default(),
        panels: Default::default(),
        consensus: Default::default(),
    };
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(config, AdapterRegistry::new(), cb);
    register_noop(&gw).await;

    let spec = ConsensusConfig {
        id: "consensus".to_string(),
        capability: Capability::TextChat,
        panel: PanelConfig {
            id: "debate".to_string(),
            capability: Capability::TextChat,
            distinct_by: DistinctBy::Family,
            strict: false,
            slots: vec![
                PanelSlot {
                    chain: "c-gemma".to_string(),
                    label: Some("proposer".to_string()),
                    system_prompt: None,
                },
                PanelSlot {
                    chain: "c-qwen".to_string(),
                    label: Some("challenger".to_string()),
                    system_prompt: None,
                },
            ],
        },
        synthesizer: RoleSpec {
            chain: "c-synth".to_string(),
            system_prompt: Some("Merge the proposals.".to_string()),
        },
        judge: Some(RoleSpec {
            chain: "c-judge".to_string(),
            system_prompt: Some("Score the synthesis.".to_string()),
        }),
        judge_quorum: None,
    };

    let result = gw.execute_consensus(&spec, "What is 2 + 2?").await.unwrap();

    assert_eq!(result.debate.len(), 2);
    assert!(result.debate.iter().all(|s| s.result.is_ok()));
    // noop returns canned content, but every phase produced output.
    assert!(!result.synthesis_output.is_empty());
    assert!(result.judgment.is_some());
    assert!(!result.judgment_output.unwrap().is_empty());
}

#[tokio::test]
async fn execute_consensus_rejects_non_independent_judge() {
    use crate::types::config::{ConsensusConfig, DistinctBy, PanelConfig, PanelSlot, RoleSpec};

    // The judge chain's primary is gemma-family — the same as the proposer —
    // so it must be rejected before any inference.
    let routers = HashMap::from([("noop".to_string(), router(true))]);
    let models = HashMap::from([
        (
            "m-gemma".to_string(),
            chat_model("m-gemma", "noop", "gemma"),
        ),
        ("m-qwen".to_string(), chat_model("m-qwen", "noop", "qwen")),
        (
            "m-synth".to_string(),
            chat_model("m-synth", "noop", "mixtral"),
        ),
        (
            "m-gemma2".to_string(),
            chat_model("m-gemma2", "noop", "gemma"),
        ),
    ]);
    let chains = HashMap::from([
        (
            "c-gemma".to_string(),
            one_model_chain("c-gemma", "m-gemma", "noop"),
        ),
        (
            "c-qwen".to_string(),
            one_model_chain("c-qwen", "m-qwen", "noop"),
        ),
        (
            "c-synth".to_string(),
            one_model_chain("c-synth", "m-synth", "noop"),
        ),
        (
            "c-gemma2".to_string(),
            one_model_chain("c-gemma2", "m-gemma2", "noop"),
        ),
    ]);
    let config = GatewayConfig {
        routers,
        models,
        chains,
        constraints: Default::default(),
        panels: Default::default(),
        consensus: Default::default(),
    };
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(config, AdapterRegistry::new(), cb);
    register_noop(&gw).await;

    let spec = ConsensusConfig {
        id: "bad".to_string(),
        capability: Capability::TextChat,
        panel: PanelConfig {
            id: "debate".to_string(),
            capability: Capability::TextChat,
            distinct_by: DistinctBy::Family,
            strict: false,
            slots: vec![
                PanelSlot {
                    chain: "c-gemma".to_string(),
                    label: None,
                    system_prompt: None,
                },
                PanelSlot {
                    chain: "c-qwen".to_string(),
                    label: None,
                    system_prompt: None,
                },
            ],
        },
        synthesizer: RoleSpec {
            chain: "c-synth".to_string(),
            system_prompt: None,
        },
        judge: Some(RoleSpec {
            chain: "c-gemma2".to_string(),
            system_prompt: None,
        }),
        judge_quorum: None,
    };

    let err = gw.execute_consensus(&spec, "q").await.unwrap_err();
    assert!(
        matches!(err, GatewayError::InvalidConfig(ref m) if m.contains("independent")),
        "non-independent judge must be rejected, got {err:?}"
    );
}

// --- Config-addressed panels / consensus (gh#19) ---

#[tokio::test]
async fn execute_panel_addressed_resolves_from_config() {
    use crate::types::config::{DistinctBy, PanelConfig, PanelSlot};

    let routers = HashMap::from([("noop".to_string(), router(true))]);
    let models = HashMap::from([
        (
            "m-gemma".to_string(),
            chat_model("m-gemma", "noop", "gemma"),
        ),
        ("m-qwen".to_string(), chat_model("m-qwen", "noop", "qwen")),
    ]);
    let chains = HashMap::from([
        (
            "c-gemma".to_string(),
            one_model_chain("c-gemma", "m-gemma", "noop"),
        ),
        (
            "c-qwen".to_string(),
            one_model_chain("c-qwen", "m-qwen", "noop"),
        ),
    ]);
    let panels = HashMap::from([(
        "board".to_string(),
        PanelConfig {
            id: "board".to_string(),
            capability: Capability::TextChat,
            distinct_by: DistinctBy::Family,
            strict: false,
            slots: vec![
                PanelSlot {
                    chain: "c-gemma".to_string(),
                    label: Some("proposer".to_string()),
                    system_prompt: None,
                },
                PanelSlot {
                    chain: "c-qwen".to_string(),
                    label: Some("challenger".to_string()),
                    system_prompt: None,
                },
            ],
        },
    )]);
    let config = GatewayConfig {
        routers,
        models,
        chains,
        constraints: Default::default(),
        panels,
        consensus: Default::default(),
    };
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(config, AdapterRegistry::new(), cb);
    register_noop(&gw).await;

    // Address the panel by id from the request; no explicit PanelConfig.
    let mut req = chat_request();
    req.panel = Some("board".to_string());
    let resp = gw.execute_panel_addressed(&req).await.unwrap();
    assert_eq!(resp.slots.len(), 2);
    assert!(resp.slots.iter().all(|s| s.result.is_ok()));
}

#[tokio::test]
async fn execute_panel_addressed_unknown_id_errors() {
    let gw = test_gateway();
    register_noop(&gw).await;
    let mut req = chat_request();
    req.panel = Some("nope".to_string());
    let err = gw.execute_panel_addressed(&req).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::InvalidConfig(ref m) if m.contains("unknown panel")),
        "unknown panel id must fail fast, got {err:?}"
    );
}

#[tokio::test]
async fn execute_consensus_addressed_resolves_from_config() {
    use crate::types::config::{ConsensusConfig, DistinctBy, PanelConfig, PanelSlot, RoleSpec};

    let routers = HashMap::from([("noop".to_string(), router(true))]);
    let models = HashMap::from([
        (
            "m-gemma".to_string(),
            chat_model("m-gemma", "noop", "gemma"),
        ),
        ("m-qwen".to_string(), chat_model("m-qwen", "noop", "qwen")),
        (
            "m-synth".to_string(),
            chat_model("m-synth", "noop", "mixtral"),
        ),
    ]);
    let chains = HashMap::from([
        (
            "c-gemma".to_string(),
            one_model_chain("c-gemma", "m-gemma", "noop"),
        ),
        (
            "c-qwen".to_string(),
            one_model_chain("c-qwen", "m-qwen", "noop"),
        ),
        (
            "c-synth".to_string(),
            one_model_chain("c-synth", "m-synth", "noop"),
        ),
    ]);
    let consensus = HashMap::from([(
        "debate".to_string(),
        ConsensusConfig {
            id: "debate".to_string(),
            capability: Capability::TextChat,
            panel: PanelConfig {
                id: "debate-panel".to_string(),
                capability: Capability::TextChat,
                distinct_by: DistinctBy::Family,
                strict: false,
                slots: vec![
                    PanelSlot {
                        chain: "c-gemma".to_string(),
                        label: Some("proposer".to_string()),
                        system_prompt: None,
                    },
                    PanelSlot {
                        chain: "c-qwen".to_string(),
                        label: Some("challenger".to_string()),
                        system_prompt: None,
                    },
                ],
            },
            synthesizer: RoleSpec {
                chain: "c-synth".to_string(),
                system_prompt: Some("Merge.".to_string()),
            },
            judge: None,
            judge_quorum: None,
        },
    )]);
    let config = GatewayConfig {
        routers,
        models,
        chains,
        constraints: Default::default(),
        panels: Default::default(),
        consensus,
    };
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(config, AdapterRegistry::new(), cb);
    register_noop(&gw).await;

    // The prompt comes from the request payload; the workflow from its id.
    let mut req = chat_request();
    req.consensus = Some("debate".to_string());
    let result = gw.execute_consensus_addressed(&req).await.unwrap();
    assert_eq!(result.debate.len(), 2);
    assert!(!result.synthesis_output.is_empty());
}

#[tokio::test]
async fn execute_consensus_addressed_unknown_id_errors() {
    let gw = test_gateway();
    register_noop(&gw).await;
    let mut req = chat_request();
    req.consensus = Some("nope".to_string());
    let err = gw.execute_consensus_addressed(&req).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::InvalidConfig(ref m) if m.contains("unknown consensus")),
        "unknown consensus id must fail fast, got {err:?}"
    );
}

// --- Judge quorum (gh#20) ---

/// Config for the quorum tests: gemma/qwen debaters, a mixtral synthesizer,
/// and judge chains whose families are given by `judges` (id, family).
fn quorum_config(judges: &[(&str, &str)]) -> GatewayConfig {
    let mut models = HashMap::from([
        (
            "m-gemma".to_string(),
            chat_model("m-gemma", "noop", "gemma"),
        ),
        ("m-qwen".to_string(), chat_model("m-qwen", "noop", "qwen")),
        (
            "m-synth".to_string(),
            chat_model("m-synth", "noop", "mixtral"),
        ),
    ]);
    let mut chains = HashMap::from([
        (
            "c-gemma".to_string(),
            one_model_chain("c-gemma", "m-gemma", "noop"),
        ),
        (
            "c-qwen".to_string(),
            one_model_chain("c-qwen", "m-qwen", "noop"),
        ),
        (
            "c-synth".to_string(),
            one_model_chain("c-synth", "m-synth", "noop"),
        ),
    ]);
    for (chain, family) in judges {
        let model = format!("m-{chain}");
        models.insert(model.clone(), chat_model(&model, "noop", family));
        chains.insert((*chain).to_string(), one_model_chain(chain, &model, "noop"));
    }
    GatewayConfig {
        routers: HashMap::from([("noop".to_string(), router(true))]),
        models,
        chains,
        constraints: Default::default(),
        panels: Default::default(),
        consensus: Default::default(),
    }
}

fn quorum_panel(slots: &[&str]) -> crate::types::config::PanelConfig {
    use crate::types::config::{DistinctBy, PanelConfig, PanelSlot};
    PanelConfig {
        id: "jury".to_string(),
        capability: Capability::TextChat,
        distinct_by: DistinctBy::Family,
        strict: false,
        slots: slots
            .iter()
            .map(|c| PanelSlot {
                chain: (*c).to_string(),
                label: Some((*c).to_string()),
                system_prompt: Some("Score 1-10.".to_string()),
            })
            .collect(),
    }
}

fn debate_panel() -> crate::types::config::PanelConfig {
    use crate::types::config::{DistinctBy, PanelConfig, PanelSlot};
    PanelConfig {
        id: "debate".to_string(),
        capability: Capability::TextChat,
        distinct_by: DistinctBy::Family,
        strict: false,
        slots: vec![
            PanelSlot {
                chain: "c-gemma".to_string(),
                label: Some("proposer".to_string()),
                system_prompt: None,
            },
            PanelSlot {
                chain: "c-qwen".to_string(),
                label: Some("challenger".to_string()),
                system_prompt: None,
            },
        ],
    }
}

fn gw_from(config: GatewayConfig) -> Gateway {
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    Gateway::new(config, AdapterRegistry::new(), cb)
}

#[tokio::test]
async fn execute_consensus_judge_quorum_fans_out() {
    use crate::types::config::{ConsensusConfig, RoleSpec};

    // Two family-distinct judges (phi, llama), both independent of the
    // gemma/qwen debaters and the mixtral synthesizer.
    let gw = gw_from(quorum_config(&[("c-phi", "phi"), ("c-llama", "llama")]));
    register_noop(&gw).await;

    let spec = ConsensusConfig {
        id: "consensus".to_string(),
        capability: Capability::TextChat,
        panel: debate_panel(),
        synthesizer: RoleSpec {
            chain: "c-synth".to_string(),
            system_prompt: Some("Merge.".to_string()),
        },
        judge: None,
        judge_quorum: Some(quorum_panel(&["c-phi", "c-llama"])),
    };

    let result = gw.execute_consensus(&spec, "What is 2 + 2?").await.unwrap();
    assert_eq!(result.debate.len(), 2);
    assert!(!result.synthesis_output.is_empty());
    // The single-judge fields stay empty; the quorum panel carries the votes.
    assert!(result.judgment.is_none());
    let jury = result.judge_quorum.expect("quorum results present");
    assert_eq!(jury.slots.len(), 2);
    assert!(jury.slots.iter().all(|s| s.result.is_ok()));
}

#[tokio::test]
async fn execute_consensus_rejects_non_independent_quorum_member() {
    use crate::types::config::{ConsensusConfig, RoleSpec};

    // One quorum judge (c-gemma2) shares the proposer's gemma family →
    // rejected before any inference.
    let gw = gw_from(quorum_config(&[("c-phi", "phi"), ("c-gemma2", "gemma")]));
    register_noop(&gw).await;

    let spec = ConsensusConfig {
        id: "bad".to_string(),
        capability: Capability::TextChat,
        panel: debate_panel(),
        synthesizer: RoleSpec {
            chain: "c-synth".to_string(),
            system_prompt: None,
        },
        judge: None,
        judge_quorum: Some(quorum_panel(&["c-phi", "c-gemma2"])),
    };

    let err = gw.execute_consensus(&spec, "q").await.unwrap_err();
    assert!(
        matches!(err, GatewayError::InvalidConfig(ref m) if m.contains("quorum") && m.contains("independent")),
        "non-independent quorum member must be rejected, got {err:?}"
    );
}

#[tokio::test]
async fn execute_consensus_rejects_both_judge_and_quorum() {
    use crate::types::config::{ConsensusConfig, RoleSpec};

    // Setting both a single judge and a quorum is a config error.
    let gw = gw_from(quorum_config(&[("c-phi", "phi")]));
    register_noop(&gw).await;

    let spec = ConsensusConfig {
        id: "bad".to_string(),
        capability: Capability::TextChat,
        panel: debate_panel(),
        synthesizer: RoleSpec {
            chain: "c-synth".to_string(),
            system_prompt: None,
        },
        judge: Some(RoleSpec {
            chain: "c-phi".to_string(),
            system_prompt: None,
        }),
        judge_quorum: Some(quorum_panel(&["c-phi"])),
    };

    let err = gw.execute_consensus(&spec, "q").await.unwrap_err();
    assert!(
        matches!(err, GatewayError::InvalidConfig(ref m) if m.contains("not both")),
        "both judge and quorum must be rejected, got {err:?}"
    );
}

// --- Strict runtime distinctness (gh#21) ---

#[tokio::test]
async fn execute_panel_strict_drops_runtime_family_convergence() {
    use crate::types::config::{
        ChainEntry, DistinctBy, FallbackChainConfig, PanelConfig, PanelSlot,
    };

    // Slot A's primary is phi on router "ghost" (no adapter → skipped), so it
    // falls back to a gemma model; slot B is gemma. Formation passes (phi vs
    // gemma *primaries*), but at runtime both answer with gemma — the
    // convergence strict mode must prevent.
    let routers = HashMap::from([
        ("noop".to_string(), router(true)),
        ("ghost".to_string(), router(true)),
    ]);
    let models = HashMap::from([
        ("m-a1".to_string(), chat_model("m-a1", "ghost", "phi")),
        ("m-a2".to_string(), chat_model("m-a2", "noop", "gemma")),
        ("m-b1".to_string(), chat_model("m-b1", "noop", "gemma")),
    ]);
    let c_a = FallbackChainConfig {
        id: "c-a".to_string(),
        capability: Capability::TextChat,
        models: vec![
            ChainEntry {
                model: "m-a1".to_string(),
                router: Some("ghost".to_string()),
                api_model_id: None,
                priority: 1,
            },
            ChainEntry {
                model: "m-a2".to_string(),
                router: Some("noop".to_string()),
                api_model_id: None,
                priority: 2,
            },
        ],
        fallback_triggers: vec![],
    };
    let chains = HashMap::from([
        ("c-a".to_string(), c_a),
        ("c-b".to_string(), one_model_chain("c-b", "m-b1", "noop")),
    ]);
    let config = GatewayConfig {
        routers,
        models,
        chains,
        constraints: Default::default(),
        panels: Default::default(),
        consensus: Default::default(),
    };
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(config, AdapterRegistry::new(), cb);
    register_noop(&gw).await;

    let make_panel = |strict: bool| PanelConfig {
        id: "p".to_string(),
        capability: Capability::TextChat,
        distinct_by: DistinctBy::Family,
        strict,
        slots: vec![
            PanelSlot {
                chain: "c-a".to_string(),
                label: Some("A".to_string()),
                system_prompt: None,
            },
            PanelSlot {
                chain: "c-b".to_string(),
                label: Some("B".to_string()),
                system_prompt: None,
            },
        ],
    };

    // Non-strict: both slots succeed on gemma; the convergence is only flagged.
    let lax = gw
        .execute_panel(&chat_request(), &make_panel(false))
        .await
        .unwrap();
    assert!(lax.slots.iter().all(|s| s.result.is_ok()));
    assert!(
        !lax.collisions.is_empty(),
        "non-strict must flag the runtime collision"
    );

    // Strict: slot A (first) keeps gemma; slot B is dropped to an error, so no
    // two *returned* slots share a family.
    let strict = gw
        .execute_panel(&chat_request(), &make_panel(true))
        .await
        .unwrap();
    let a = strict
        .slots
        .iter()
        .find(|s| s.label.as_deref() == Some("A"))
        .unwrap();
    let b = strict
        .slots
        .iter()
        .find(|s| s.label.as_deref() == Some("B"))
        .unwrap();
    assert!(a.result.is_ok(), "the first slot keeps its family");
    assert!(
        b.result.is_err(),
        "the converging slot must be dropped under strict"
    );
    assert_eq!(
        strict.slots.iter().filter(|s| s.result.is_ok()).count(),
        1,
        "strict leaves exactly one successful slot per family"
    );
    assert!(
        strict.collisions.iter().any(|c| c.contains("dropped")),
        "strict records the drop, got {:?}",
        strict.collisions
    );
}

// --- Task 6: on_lockout callback + apply/clear + terminal-lock lifecycle ---

/// A best-effort `SelectionObserver` that records every announced lockout as
/// `(endpoint, reason, until.is_some())`. Playing the *caller*, it is what
/// "persists" — the gateway only announces (design §5c).
struct RecordingObserver(
    Arc<std::sync::Mutex<Vec<(String, crate::gates::lockout::LockReason, bool)>>>,
);

impl crate::gates::lockout::SelectionObserver for RecordingObserver {
    fn on_lockout(
        &self,
        endpoint: &str,
        reason: crate::gates::lockout::LockReason,
        until: Option<Instant>,
    ) {
        self.0
            .lock()
            .unwrap()
            .push((endpoint.to_string(), reason, until.is_some()));
    }
}

/// (a) The gateway FIRES `on_lockout` when the sink locks an endpoint; the
/// observer (the caller) is what records it. A 403-quota failover locks
/// `failing:fail-model` (timed → `until.is_some()`), and the observer sees it.
#[tokio::test]
async fn on_lockout_callback_fires_caller_persists() {
    use crate::gates::lockout::LockReason;

    let recorded = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observer = Arc::new(RecordingObserver(recorded.clone()));
    let gw = test_gateway_with_chain().with_observer(observer.clone());
    register_failing(
        &gw,
        GatewayError::ProviderError {
            adapter: "failing".into(),
            message: "quota exceeded".into(),
            status: Some(403),
        },
    )
    .await;
    register_noop(&gw).await;

    gw.execute(&chat_request()).await.unwrap();

    let recorded = recorded.lock().unwrap();
    assert!(
        recorded.contains(&(
            "failing:fail-model".to_string(),
            LockReason::QuotaExhausted,
            true
        )),
        "the gateway must announce the timed quota lock to the observer, got {recorded:?}"
    );
}

/// (b) `apply_lockout` re-seeds a persisted lock on a fresh instance (no prior
/// failure): the gate then skips `failing:fail-model`, so only noop is
/// attempted. `clear_lockout` restores it — the endpoint is tried first again.
#[tokio::test]
async fn apply_and_clear_lockout_reseed_and_restore() {
    use crate::gates::lockout::LockReason;

    let gw = test_gateway_with_chain();
    register_failing(
        &gw,
        GatewayError::ProviderError {
            adapter: "failing".into(),
            message: "quota exceeded".into(),
            status: Some(403),
        },
    )
    .await;
    register_noop(&gw).await;

    // Re-seed a timed quota lock without ever failing: the caller persisted it
    // and hands it back on this fresh instance.
    gw.apply_lockout(
        "failing:fail-model",
        LockReason::QuotaExhausted,
        Some(Instant::now() + std::time::Duration::from_secs(3600)),
    );

    // The locked endpoint is skipped at selection → only noop is attempted.
    let response = gw.execute(&chat_request()).await.unwrap();
    assert_eq!(response.model, Some("noop".to_string()));
    assert_eq!(
        response.attempts.len(),
        1,
        "the re-seeded lock must skip failing:fail-model (noop only)"
    );
    assert_eq!(response.attempts[0].adapter, "noop");

    // Clearing the lock makes the endpoint eligible again: it is attempted
    // first (403-quota), then falls over to noop — two attempts.
    gw.clear_lockout("failing:fail-model");
    let response2 = gw.execute(&chat_request()).await.unwrap();
    assert_eq!(response2.model, Some("noop".to_string()));
    assert_eq!(
        response2.attempts.len(),
        2,
        "after clear, failing:fail-model is tried first again"
    );
    assert_eq!(response2.attempts[0].adapter, "failing");
}

/// (c) `refresh_router_keys` clears TERMINAL (`Auth`/`Credits`) locks on every
/// configured router's endpoints — a fresh credential may fix them — but leaves
/// TIMED locks intact (a rate/quota reset is unrelated to the key). Selectivity
/// is proven by a timed lock on a *different*, unconfigured endpoint surviving.
#[tokio::test]
async fn refresh_router_keys_clears_terminal_lock_but_keeps_timed() {
    use crate::gates::lockout::LockReason;

    let gw = test_gateway_with_chain();

    // Terminal Auth lock on a configured router's endpoint...
    gw.apply_lockout("failing:fail-model", LockReason::Auth, None);
    // ...and a timed quota lock on a different (unconfigured) endpoint, which
    // must survive — proving the clear is both terminal-only and router-scoped.
    gw.apply_lockout(
        "other:other-model",
        LockReason::QuotaExhausted,
        Some(Instant::now() + std::time::Duration::from_secs(3600)),
    );

    gw.refresh_router_keys(|_| Some("new-key".to_string()))
        .await;

    assert!(
        gw.model_lockout.get("failing:fail-model").is_none(),
        "a fresh credential must clear the terminal Auth lock"
    );
    assert!(
        gw.model_lockout.get("other:other-model").is_some(),
        "a timed (rate/quota) lock is unrelated to the key and must survive"
    );
}

// --- Task 4: AllGated{resume_after} at execute exhaustion (design §3.3) ---
//
// AllGated fires iff EVERY candidate was gated (skipped by a health gate at
// selection, or attempted-and-classified as a recoverable/terminal provider
// limit) and NONE hard-failed. `resume_after` is the wall-clock min over the
// TIMED gates only; all-terminal ⇒ `None` + a `human_action` remedy.

/// Two-candidate TextChat chain `[a-model@A, b-model@B]` with the given
/// fallback triggers. Endpoints resolve to `"A:a-model"` and `"B:b-model"`.
fn ab_chain_config(triggers: Vec<FallbackTrigger>) -> GatewayConfig {
    ab_chain_config_priced(triggers, None)
}

/// Like [`ab_chain_config`], but both models carry the given optional pricing
/// (used by the all-over-budget case).
fn ab_chain_config_priced(
    triggers: Vec<FallbackTrigger>,
    pricing: Option<ModelPricing>,
) -> GatewayConfig {
    let mut routers = HashMap::new();
    for id in ["A", "B"] {
        routers.insert(
            id.to_string(),
            RouterConfig {
                url: "http://localhost".to_string(),
                api_key_env: None,
                api_key: None,
                enabled: true,
                timeout_ms: None,
                headers: HashMap::new(),
            },
        );
    }
    let mut models = HashMap::new();
    for (model, provider) in [("a-model", "A"), ("b-model", "B")] {
        models.insert(
            model.to_string(),
            ModelConfig {
                id: model.to_string(),
                api_model_id: None,
                provider: provider.to_string(),
                family: None,
                capabilities: vec![Capability::TextChat],
                context_window: 4096,
                max_output_tokens: 1024,
                pricing: pricing.clone(),
                catalog: None,
            },
        );
    }
    let mut chains = HashMap::new();
    chains.insert(
        "chat_chain".to_string(),
        FallbackChainConfig {
            id: "chat_chain".to_string(),
            capability: Capability::TextChat,
            models: vec![
                ChainEntry {
                    model: "a-model".to_string(),
                    router: Some("A".to_string()),
                    api_model_id: None,
                    priority: 1,
                },
                ChainEntry {
                    model: "b-model".to_string(),
                    router: Some("B".to_string()),
                    api_model_id: None,
                    priority: 2,
                },
            ],
            fallback_triggers: triggers,
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

fn ab_gateway(config: GatewayConfig) -> Gateway {
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    Gateway::new(config, AdapterRegistry::new(), cb)
}

/// A chat adapter with a caller-chosen `id` that always fails with a
/// freshly-built error, so distinct routers can return distinct provider
/// failures in one gateway.
struct ChatErrAdapter {
    id: String,
    err: Arc<dyn Fn() -> GatewayError + Send + Sync>,
}

impl crate::adapters::capability::Model for ChatErrAdapter {
    fn id(&self) -> &str {
        &self.id
    }
}

#[async_trait::async_trait]
impl crate::adapters::capability::ChatModel for ChatErrAdapter {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<crate::types::io::ChatResponse, GatewayError> {
        Err((self.err)())
    }
}

async fn register_chat_err(
    gw: &Gateway,
    id: &str,
    err: impl Fn() -> GatewayError + Send + Sync + 'static,
) {
    gw.adapters
        .register_chat(Arc::new(ChatErrAdapter {
            id: id.to_string(),
            err: Arc::new(err),
        }))
        .await;
}

/// Assert a wall-clock `resume_after` is `Some` and within `±tol_secs` of
/// `now + expected_secs`. A tolerance window (not an exact instant) keeps the
/// assertion robust against wall-clock drift, yet tight enough to reject `None`
/// and the wrong endpoint's expiry.
fn assert_resume_near(
    resume_after: Option<chrono::DateTime<chrono::Utc>>,
    expected_secs: i64,
    tol_secs: i64,
) {
    let now = chrono::Utc::now();
    let t = resume_after.expect("resume_after should be Some");
    let lo = now + chrono::Duration::seconds(expected_secs - tol_secs);
    let hi = now + chrono::Duration::seconds(expected_secs + tol_secs);
    assert!(
        t > lo && t < hi,
        "resume_after {t} not within ±{tol_secs}s of now+{expected_secs}s (lo={lo}, hi={hi})"
    );
}

/// (1) Every candidate pre-locked (timed quota) at selection → `AllGated` whose
/// `resume_after` is the MIN over the two expiries (the nearer, B at ~1800s),
/// and no `human_action` (a timed retry exists).
#[tokio::test]
async fn all_gated_at_selection_returns_allgated_with_min_resume() {
    use crate::gates::lockout::LockReason;

    let gw = ab_gateway(ab_chain_config(vec![]));
    gw.apply_lockout(
        "A:a-model",
        LockReason::QuotaExhausted,
        Some(Instant::now() + std::time::Duration::from_secs(3600)),
    );
    gw.apply_lockout(
        "B:b-model",
        LockReason::QuotaExhausted,
        Some(Instant::now() + std::time::Duration::from_secs(1800)),
    );

    match gw.execute(&chat_request()).await.unwrap_err() {
        GatewayError::AllGated {
            resume_after,
            human_action,
            ..
        } => {
            assert_resume_near(resume_after, 1800, 150); // the nearer of 3600 / 1800
            assert!(
                human_action.is_none(),
                "a timed retry means no human action is surfaced"
            );
        }
        other => panic!("expected AllGated, got: {other}"),
    }
}

/// (2) Every candidate terminally locked (auth + credits) → `resume_after: None`
/// and a `human_action` remedy (never pause forever).
#[tokio::test]
async fn all_gated_all_terminal_returns_none_resume_with_human_action() {
    use crate::gates::lockout::LockReason;

    let gw = ab_gateway(ab_chain_config(vec![]));
    gw.apply_lockout("A:a-model", LockReason::Auth, None);
    gw.apply_lockout("B:b-model", LockReason::CreditsExhausted, None);

    match gw.execute(&chat_request()).await.unwrap_err() {
        GatewayError::AllGated {
            resume_after,
            human_action,
            ..
        } => {
            assert!(resume_after.is_none(), "all-terminal ⇒ no resume time");
            assert!(
                human_action.is_some(),
                "a terminal remedy (top-up / rotate) must be surfaced"
            );
        }
        other => panic!("expected AllGated, got: {other}"),
    }
}

/// (3) Mixed terminal (A, credits) + timed (B, quota) → `resume_after` is the
/// min over the TIMED gates only (B ~1800s), so the terminal A is excluded from the
/// wake; A's remedy is still REPORTED beside it.
///
/// This test asserted `human_action.is_none()` here — "a timed retry wins over the
/// terminal remedy" — until SP-7a's review. The two fields answer different questions:
/// `resume_after` decides whether the caller pauses (and only the timed gates can
/// contribute to it, which is the half this test has always been about), while
/// `human_action` is the diagnosis of what will STILL be wrong after the wake. Nulling
/// the second because the first exists loses information no caller can recover, and it
/// bites hardest on the reason SP-7a introduced: no deadline makes a context window
/// bigger, so an over-window candidate beside a timed one produced a wake, a retry, and
/// only then the truth.
#[tokio::test]
async fn all_gated_mixed_terminal_and_timed_uses_min_over_timed_only() {
    use crate::gates::lockout::LockReason;
    use crate::types::error::HumanAction;

    let gw = ab_gateway(ab_chain_config(vec![]));
    gw.apply_lockout("A:a-model", LockReason::CreditsExhausted, None); // terminal
    gw.apply_lockout(
        "B:b-model",
        LockReason::QuotaExhausted,
        Some(Instant::now() + std::time::Duration::from_secs(1800)),
    ); // timed

    match gw.execute(&chat_request()).await.unwrap_err() {
        GatewayError::AllGated {
            resume_after,
            human_action,
            ..
        } => {
            assert_resume_near(resume_after, 1800, 150); // terminal A excluded from the min
            assert_eq!(
                human_action,
                Some(HumanAction::TopUpCredits),
                "and A's remedy travels with the wake rather than being discarded by \
                 it — B's quota clears in 30 minutes, A's credits never do"
            );
        }
        other => panic!("expected AllGated, got: {other}"),
    }
}

/// (4) Every candidate's breaker tripped Open at selection → `AllGated` whose
/// `resume_after` comes from the breaker `next_retry` (~300s default timeout).
#[tokio::test]
async fn all_gated_all_breaker_open_resume_from_next_retry() {
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default()); // threshold 5, timeout 300s
    for ep in ["A:a-model", "B:b-model"] {
        cb.can_execute(ep); // initialize
        for _ in 0..5 {
            cb.record_failure(ep);
        }
        assert!(!cb.can_execute(ep), "breaker for {ep} should be open");
    }
    let gw = Gateway::new(ab_chain_config(vec![]), AdapterRegistry::new(), cb);

    match gw.execute(&chat_request()).await.unwrap_err() {
        GatewayError::AllGated { resume_after, .. } => {
            assert_resume_near(resume_after, 300, 120); // breaker default timeout
        }
        other => panic!("expected AllGated, got: {other}"),
    }
}

/// (5) Both candidates attempted and each fails with a recoverable 429 (no
/// configured trigger; §3.1 fallover) → each is locked in-flight, so the walk
/// exhausts to `AllGated` on THIS request, `resume_after` ~ the 60s rate-limit
/// base (the min over the just-written timed locks).
#[tokio::test]
async fn attempted_exhaustion_all_recoverable_returns_allgated() {
    let gw = ab_gateway(ab_chain_config(vec![]));
    register_chat_err(&gw, "A", || GatewayError::RateLimit {
        adapter: "A".into(),
        retry_after_ms: None,
    })
    .await;
    register_chat_err(&gw, "B", || GatewayError::RateLimit {
        adapter: "B".into(),
        retry_after_ms: None,
    })
    .await;

    match gw.execute(&chat_request()).await.unwrap_err() {
        GatewayError::AllGated { resume_after, .. } => {
            assert_resume_near(resume_after, 60, 40); // rate-limit base ~60s
        }
        other => panic!("expected AllGated, got: {other}"),
    }
}

/// (6) A hard failure among the attempts keeps `AllAttemptsFailed`: A (429,
/// recoverable → locked) falls over to B (500, unclassified → hard failure), so
/// NOT every candidate was gated. Pins that AllGated does not over-fire.
#[tokio::test]
async fn attempted_exhaustion_with_hard_failure_keeps_all_attempts_failed() {
    let gw = ab_gateway(ab_chain_config(vec![]));
    register_chat_err(&gw, "A", || GatewayError::RateLimit {
        adapter: "A".into(),
        retry_after_ms: None,
    })
    .await;
    register_chat_err(&gw, "B", || GatewayError::ProviderError {
        adapter: "B".into(),
        message: "boom".into(),
        status: Some(500),
    })
    .await;

    match gw.execute(&chat_request()).await.unwrap_err() {
        GatewayError::AllAttemptsFailed { attempts, .. } => {
            assert_eq!(
                attempts, 2,
                "both candidates were attempted; B hard-failed ⇒ not all-gated"
            );
        }
        other => panic!("expected AllAttemptsFailed, got: {other}"),
    }
}

/// (7) A chain whose entries all reference missing models → every skip is
/// Structural, so `AllGated` never fires and `NoCandidates` is preserved.
#[tokio::test]
async fn all_structural_selection_stays_no_candidates() {
    let mut routers = HashMap::new();
    routers.insert(
        "A".to_string(),
        RouterConfig {
            url: "http://localhost".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        },
    );
    let models = HashMap::new(); // no models — every chain entry is ModelNotFound
    let mut chains = HashMap::new();
    chains.insert(
        "chat_chain".to_string(),
        FallbackChainConfig {
            id: "chat_chain".to_string(),
            capability: Capability::TextChat,
            models: vec![
                ChainEntry {
                    model: "ghost-a".to_string(),
                    router: Some("A".to_string()),
                    api_model_id: None,
                    priority: 1,
                },
                ChainEntry {
                    model: "ghost-b".to_string(),
                    router: Some("A".to_string()),
                    api_model_id: None,
                    priority: 2,
                },
            ],
            fallback_triggers: vec![],
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
    let gw = ab_gateway(config);

    match gw.execute(&chat_request()).await.unwrap_err() {
        GatewayError::NoCandidates { capability } => {
            assert_eq!(capability, Capability::TextChat);
        }
        other => panic!("expected NoCandidates (all-structural), got: {other}"),
    }
}

/// (8) Every candidate priced over a tiny budget → `AllGated` with
/// `resume_after: None` + `human_action: RaiseBudget`. A DELIBERATE change from
/// the pre-(e) `NoCandidates` for an all-over-budget selection (OverBudget is a
/// `Terminal(RaiseBudget)` gate, per design §3.3).
#[tokio::test]
async fn all_over_budget_returns_allgated_raise_budget() {
    use crate::types::error::HumanAction;

    let config = ab_chain_config_priced(
        vec![],
        Some(ModelPricing {
            input_per_1k: 0.0008,
            output_per_1k: 0.004,
            per_request: None,
        }),
    );
    let gw = ab_gateway(config);

    // Every candidate's estimate (output term ~0.004 alone) exceeds this budget.
    let req = InferenceRequest {
        budget: Some(0.0001),
        ..chat_request()
    };

    match gw.execute(&req).await.unwrap_err() {
        GatewayError::AllGated {
            resume_after,
            human_action,
            ..
        } => {
            assert!(
                resume_after.is_none(),
                "over-budget is terminal ⇒ no resume"
            );
            assert_eq!(human_action, Some(HumanAction::RaiseBudget));
        }
        other => panic!("expected AllGated (raise budget), got: {other}"),
    }
}

// --- Task 6: acceptance scenarios (quota-demote-to-tier.md) ---
//
// Named end-to-end tests for the `quota-demote-to-tier.md` Gherkins whose
// observable behavior is not already pinned by Tasks 3–5. The rest of that
// feature doc's scenarios are covered by existing named tests (see the mapping
// in this crate's Task-6 report): "provider 403 quota falls over on the SAME
// request" = `recoverable_quota_403_falls_over_without_trigger`; "all tiers
// gated returns resume_after" = `all_gated_at_selection_returns_allgated_with_min_resume`;
// "mixed terminal + timed" = `all_gated_mixed_terminal_and_timed_uses_min_over_timed_only`;
// "all candidates terminal → None + human action" = `all_gated_all_terminal_returns_none_resume_with_human_action`;
// "all candidates circuit-open" = `all_gated_all_breaker_open_resume_from_next_retry`.

/// A minimal recorded `InferenceCall` attributed to `subject` (one Success
/// request on tier "free", now), used to pre-seed a store so `get_usage_since`
/// already reports usage at a tier cap — without routing a live call through
/// the engine (which would itself attempt a model).
fn seed_request_call(subject: Uuid) -> crate::store::InferenceCall {
    crate::store::InferenceCall {
        id: Uuid::new_v4(),
        session_id: None,
        project_id: None,
        capability: Capability::TextChat,
        chain_id: None,
        adapter: "seed".to_string(),
        model: "seed".to_string(),
        api_model_id: None,
        input_tokens: Some(0),
        output_tokens: Some(0),
        cost_usd: 0.0,
        cost_estimated: None,
        duration_ms: 0,
        status: crate::store::CallStatus::Success,
        error_type: None,
        fallback_sequence: 0,
        recorded_at: Utc::now(),
        subject_id: Some(subject),
        tier: Some("free".to_string()),
    }
}

/// A chat adapter with a caller-chosen `id` that succeeds and bumps a shared
/// counter, so a test can prove whether the engine ever attempted it. Both A
/// and B are "healthy/ready" (a live provider would serve the request), which
/// is what makes the subscription-quota guard non-vacuous: if the hard stop
/// did NOT short-circuit, one of these WOULD be attempted.
struct CountingOkAdapter {
    id: String,
    hits: Arc<std::sync::atomic::AtomicUsize>,
}

impl crate::adapters::capability::Model for CountingOkAdapter {
    fn id(&self) -> &str {
        &self.id
    }
}

#[async_trait::async_trait]
impl crate::adapters::capability::ChatModel for CountingOkAdapter {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<crate::types::io::ChatResponse, GatewayError> {
        self.hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(crate::types::io::ChatResponse {
            content: Some("ok".to_string()),
            model: Some(self.id.clone()),
            ..Default::default()
        })
    }
}

/// Scenario: "Subscription quota exhaustion does NOT demote" (the demote-vs-
/// hard-stop guard). The caller's per-subject/tier `QuotaExceeded` — a hard
/// stop raised by `check_quota` BEFORE selection — must NOT fall over to the
/// next tier and must NOT become `AllGated`: no model is attempted and nothing
/// is locked out. This is the end-to-end distinction between the subscription
/// hard stop and a provider-side limit (which DOES demote via §3.1 / lockout).
#[tokio::test]
async fn subscription_quota_exhaustion_does_not_demote_or_lock() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Chain [A, B]; a store pre-seeded with one request for `subject` so a
    // 1/day tier cap is already exhausted for the very next authed call.
    let store = Arc::new(InMemoryStore::default());
    let subject = Uuid::new_v4();
    store
        .insert_inference_call(&seed_request_call(subject))
        .await
        .unwrap();

    let mut config = ab_chain_config(vec![]);
    config.constraints = ConstraintsConfig {
        tiers: HashMap::from([(
            "free".to_string(),
            TierConstraints {
                quota: requests_per_day(1),
                per_capability: HashMap::new(),
            },
        )]),
        default: None,
    };
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(config, AdapterRegistry::new(), cb).with_store(store);

    // Both candidates are healthy + ready: a shared counter proves neither
    // `chat` is reached once the subscription quota short-circuits selection.
    let hits = Arc::new(AtomicUsize::new(0));
    for id in ["A", "B"] {
        gw.adapters
            .register_chat(Arc::new(CountingOkAdapter {
                id: id.to_string(),
                hits: hits.clone(),
            }))
            .await;
    }

    let mut req = chat_request();
    req.auth = Some(AuthContext {
        subject_id: subject,
        tier: Some("free".to_string()),
    });

    // (a) The subscription hard stop surfaces as QuotaExceeded, NOT AllGated.
    match gw.execute(&req).await.unwrap_err() {
        GatewayError::QuotaExceeded {
            unit,
            window,
            limit,
            used,
        } => {
            assert_eq!(unit, MeterUnit::Requests);
            assert_eq!(window, Window::Day);
            assert_eq!(limit, 1);
            assert_eq!(used, 1);
        }
        other => panic!(
            "a subscription quota must hard-stop as QuotaExceeded, never demote/AllGated, got: {other}"
        ),
    }

    // (b) It fires BEFORE selection: no provider was contacted...
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "no model may be attempted on a subscription hard stop"
    );
    // (c) ...and NOTHING was locked out — a subject hard stop is not a
    // provider-side limit, so it must not demote-by-lockout either.
    assert!(
        gw.model_lockout.get("A:a-model").is_none(),
        "the subscription hard stop must not lock model A"
    );
    assert!(
        gw.model_lockout.get("B:b-model").is_none(),
        "the subscription hard stop must not lock model B"
    );
}

/// Scenario: "A durable consumer pauses and resumes at resume_after". An
/// all-gated selection surfaces `AllGated { resume_after: Some(t), .. }` whose
/// `t` is a USABLE FUTURE wall-clock instant (`t > Utc::now()`) — the concrete
/// wake-up time the orchestrator records as a durable pause. There is no
/// orchestrator here, so this asserts the pause INPUT's shape/value (a future
/// `DateTime<Utc>`), which is the contract a durable consumer depends on.
#[tokio::test]
async fn all_gated_resume_after_is_a_future_pause_instant() {
    use crate::gates::lockout::LockReason;

    let gw = ab_gateway(ab_chain_config(vec![]));
    gw.apply_lockout(
        "A:a-model",
        LockReason::QuotaExhausted,
        Some(Instant::now() + std::time::Duration::from_secs(3600)),
    );
    gw.apply_lockout(
        "B:b-model",
        LockReason::QuotaExhausted,
        Some(Instant::now() + std::time::Duration::from_secs(1800)),
    );

    match gw.execute(&chat_request()).await.unwrap_err() {
        GatewayError::AllGated { resume_after, .. } => {
            let t = resume_after.expect("a timed all-gated exposes a wake-up instant");
            assert!(
                t > chrono::Utc::now(),
                "resume_after {t} must be a FUTURE instant a durable consumer can pause until"
            );
        }
        other => panic!("expected AllGated, got: {other}"),
    }
}

// -----------------------------------------------------------------------------
// SP-7a — the engine boundary: `engine::execute` computing the pessimistic
// estimate and putting it on `SelectionCriteria`.
//
// This is the one link neither `gates/context_window.rs` (which hand-sets the
// number) nor `engine/util.rs` (which builds a `SelectionCtx` by hand) can see.
// Both of those pass unchanged if `execute` never computes the figure at all, or
// computes it and assigns the COST one instead — which compiles, because the two
// fields have the same type.
// -----------------------------------------------------------------------------

/// A `TextChat` chain of two `noop`-served models differing only in context window,
/// with the SMALL one at priority 1.
///
/// The priority order is the point: with the window gate absent or misfed, selection
/// returns `small` and the assertions below fail on the model name rather than on some
/// derived property.
fn window_chain_config(big: u32, small: u32) -> GatewayConfig {
    let mut routers = HashMap::new();
    routers.insert(
        "noop".to_string(),
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
    for (id, context_window) in [("big", big), ("small", small)] {
        models.insert(
            id.to_string(),
            ModelConfig {
                id: id.to_string(),
                api_model_id: None,
                provider: "noop".to_string(),
                family: None,
                capabilities: vec![Capability::TextChat],
                context_window,
                max_output_tokens: 1024,
                pricing: None,
                catalog: None,
            },
        );
    }
    let mut chains = HashMap::new();
    chains.insert(
        "win_chain".to_string(),
        FallbackChainConfig {
            id: "win_chain".to_string(),
            capability: Capability::TextChat,
            models: vec![
                ChainEntry {
                    model: "small".to_string(),
                    router: Some("noop".to_string()),
                    api_model_id: None,
                    priority: 1,
                },
                ChainEntry {
                    model: "big".to_string(),
                    router: Some("noop".to_string()),
                    api_model_id: None,
                    priority: 2,
                },
            ],
            fallback_triggers: vec![],
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

/// A chat payload whose bulk is entirely TOOL SCHEMAS: 80 tools at ~410 bytes of
/// serialized schema each, ~35 KB of JSON, against 14 bytes of prose. The same shape as
/// `util::tool_schemas_alone_push_a_request_over_a_small_candidates_window`.
///
/// This is the payload that separates the two estimates, and the assertions here are
/// the fixture's own contract: the COST figure fits an 8192-token window and the
/// PESSIMISTIC one does not. Every test below that claims to tell the two apart depends
/// on both halves holding, so they are asserted once, here, rather than assumed at three
/// call sites. If a future change to either estimator collapses the gap, these fire and
/// name the reason instead of the callers silently losing their discriminating power.
fn schema_heavy_chat_payload() -> Payload {
    use crate::types::request::ToolDefinition;

    let tools: Vec<ToolDefinition> = (0..80)
        .map(|i| ToolDefinition {
            name: format!("tool_{i}"),
            description: Some("does a thing".into()),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "an absolute path to the file this tool should operate on, which must exist" },
                    "contents": { "type": "string", "description": "the bytes to write, encoded as UTF-8 text, with no length limit imposed here" },
                    "mode": { "type": "string", "enum": ["append", "overwrite", "create"], "description": "how an existing file is treated" }
                },
                "required": ["path", "contents"]
            }),
        })
        .collect();
    let payload = Payload::Chat {
        messages: vec![Message::text(MessageRole::User, "write the file")],
        system: None,
        max_tokens: None,
        temperature: None,
        tools,
    };
    assert!(
        estimate_input_tokens(&payload) < 8_192,
        "the COST estimate must FIT the small model, or no test built on this fixture \
         can tell the two estimates apart: {}",
        estimate_input_tokens(&payload)
    );
    assert!(
        super::util::estimate_input_tokens_pessimistic(&payload) > 8_192,
        "and the PESSIMISTIC one must not: {}",
        super::util::estimate_input_tokens_pessimistic(&payload)
    );
    payload
}

/// A `TextChat` request over `win_chain` whose sole user message is `bytes` long.
///
/// Bytes rather than tokens because bytes are what the estimator divides; the tests
/// that care about the token figure assert it explicitly rather than trusting the name.
fn chat_request_of_length(bytes: usize) -> InferenceRequest {
    InferenceRequest {
        chain: Some("win_chain".to_string()),
        payload: Payload::Chat {
            messages: vec![Message::text(MessageRole::User, "x".repeat(bytes))],
            system: None,
            max_tokens: None,
            temperature: None,
            tools: Vec::new(),
        },
        ..chat_request()
    }
}

/// The engine passes the PESSIMISTIC estimate to selection, not the cost one.
///
/// The mutation this exists to catch is one character wide: `input_tokens_pessimistic:
/// Some(input_tokens)` in `engine/execute.rs` compiles, keeps every other test in the
/// workspace green, and silently admits exactly the over-window requests the slice was
/// written to catch — because the cost estimate omits tool schemas and divides by 4.
///
/// So the fixture is a payload whose TOOL SCHEMAS are all of its bulk and whose prose is
/// negligible. The two `assert!`s below the payload state the trap as arithmetic: the
/// cost figure fits the small model, the pessimistic one does not. Whichever the engine
/// forwards decides which model answers, and the model that answers is what is asserted.
#[tokio::test]
async fn the_engine_selects_on_the_pessimistic_estimate_not_the_cost_one() {
    let payload = schema_heavy_chat_payload();
    let gw = ab_gateway(window_chain_config(128_000, 8_192));
    register_noop(&gw).await;
    let response = gw
        .execute(&InferenceRequest {
            payload,
            ..chat_request_of_length(0)
        })
        .await
        .expect("the 128k candidate can serve this request");
    assert_eq!(
        response.model,
        Some("big".to_string()),
        "`small` is the priority-1 entry and the cost estimate fits it, so it is \
         selected unless the engine forwarded the PESSIMISTIC figure"
    );
}

/// A candidate that is BOTH over-window and circuit-open still lets the run PAUSE — and
/// the terminal remedy travels with the pause instead of being discarded.
///
/// Two properties, in the one place their consequence is visible.
///
/// **(a) The gate's registration position.** `ContextWindowGate` is registered after the
/// health gates, so `small` — over-window AND breaker-open — reports `CircuitOpen`, which
/// is `Timed`, which is what puts a `resume_after` on the `AllGated`. That `Some(t)` is
/// the SELF-HEALING pause: the orchestrator's `classify_gateway_error` schedules a wake
/// at `t` and the scheduler returns the run unaided. Reporting the window instead yields
/// `resume_after: None`, which since the M1 reversal is the indefinite HOTL pause — so a
/// transient provider outage would leave the run waiting on a human with nothing to fix
/// (and, before that reversal, would have killed it outright). Moving the gate to the
/// front of `ModelSelectionService::new`'s vector left the whole workspace green before
/// this test existed.
///
/// **(b) `human_action` survives a `resume_after`.** `all_gated_error` used to null the
/// remedy whenever any timed gate set a wake, on the reasoning that "a timed retry wins
/// over the terminal remedy". SP-7a is what makes that lossy: the window is the first
/// terminal reason no elapsed time can EVER clear, so this run wakes in five minutes,
/// finds `big` still too small, and only then fails — having never told the operator the
/// one thing that was true from the start. The wake is still scheduled (waking can help;
/// `small`'s breaker may close onto a request that fits), but the message now carries
/// both halves.
#[tokio::test]
async fn an_over_window_candidate_whose_breaker_is_open_still_lets_the_run_pause() {
    use crate::types::error::HumanAction;

    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default()); // threshold 5, timeout 300s
    cb.can_execute("noop:small");
    for _ in 0..5 {
        cb.record_failure("noop:small");
    }
    assert!(
        !cb.can_execute("noop:small"),
        "the fixture needs `small`'s breaker genuinely open"
    );
    let gw = Gateway::new(
        window_chain_config(128_000, 8_192),
        AdapterRegistry::new(),
        cb,
    );

    // 600 KB ⇒ 200 000 tokens: over BOTH windows, so `big` is terminal-gated and
    // `small` is gated twice over.
    let err = gw
        .execute(&chat_request_of_length(600_000))
        .await
        .expect_err("nothing in the chain can hold 200k tokens");
    let GatewayError::AllGated {
        skipped,
        human_action,
        resume_after,
    } = &err
    else {
        panic!("expected AllGated, got: {err:?}");
    };

    assert!(
        skipped
            .iter()
            .any(|s| s.contains("small") && s.contains("circuit breaker open")),
        "`small` must report the BREAKER, not the window — the breaker clears by \
         itself and the window never does: {skipped:?}"
    );
    assert_resume_near(*resume_after, 300, 120);
    assert_eq!(
        *human_action,
        Some(HumanAction::UseLargerContextWindow),
        "and the unclearable half must not be thrown away just because a wake was \
         scheduled: waking cannot make `big` bigger, and the operator needs to know \
         that now rather than in five minutes"
    );
    let rendered = format!("{err}");
    assert!(
        rendered.contains("resume after") && rendered.contains("larger context window"),
        "both must survive Display, which is the only channel that reaches a \
         NodeFailed: {rendered}"
    );
}

/// AC3, the half the selection-level test cannot see: an all-gated selection must reach
/// the caller as an `AllGated` naming the cause and the remedy, not as a bare
/// `NoCandidates`. Asserted at the engine boundary, because `all_gated_error` is what
/// makes the distinction and it lives here.
///
/// Both the typed variant AND the rendered string, deliberately. The typed check is the
/// contract; the rendered one is what an operator actually gets, because the
/// orchestrator's `classify_gateway_error` builds its journaled reason from
/// `err.to_string()` and nothing downstream destructures the error. Since the M1 reversal
/// that reason lands in a `RunPaused` rather than a `NodeFailed`, which makes it the text
/// `list_paused` shows the person who has to act before `force_wake` will help — so the
/// `human_action` assertion below is guarding recoverability, not just wording.
///
/// This is the improvement over the deleted `OrchestratorError::PromptOverBudget`, and
/// the only place it is visible: that halt named ONE number, the chain minimum, which
/// may belong to a model the request never wanted. This names each candidate's own.
#[tokio::test]
async fn a_request_over_every_window_is_all_gated_with_the_numbers() {
    use crate::types::error::HumanAction;

    let gw = ab_gateway(window_chain_config(128_000, 8_192));
    register_noop(&gw).await;
    // 600 KB of prose ⇒ 200 000 tokens at ceil(bytes/3), over both windows.
    let req = chat_request_of_length(600_000);
    let err = gw
        .execute(&req)
        .await
        .expect_err("nothing in the chain can hold 200k tokens");

    let GatewayError::AllGated {
        skipped,
        human_action,
        resume_after,
    } = &err
    else {
        panic!(
            "an all-gated selection must not degrade to another error — NoCandidates is \
             the structural 'nothing is configured' case and tells an operator nothing \
             about what to do: {err:?}"
        );
    };
    assert_eq!(
        *human_action,
        Some(HumanAction::UseLargerContextWindow),
        "the remedy is the window, not money and not a credential"
    );
    assert!(
        resume_after.is_none(),
        "no deadline makes a window bigger, so there is nothing to pause until: \
         {resume_after:?}"
    );
    assert!(
        skipped
            .iter()
            .any(|s| s.contains("8192-token context window")),
        "the diagnostics must name a candidate's OWN window: {skipped:?}"
    );
    assert!(
        skipped
            .iter()
            .any(|s| s.contains("128000-token context window")),
        "including the large one — an operator widening the chain needs to know 128k \
         was tried too: {skipped:?}"
    );
    let rendered = format!("{err}");
    assert!(
        rendered.contains("8192-token context window"),
        "and it must survive Display, which is the only channel that reaches a \
         NodeFailed: {rendered}"
    );
    assert!(
        rendered.contains("route to a model with a larger context window"),
        "as must the remedy: {rendered}"
    );
}

/// The STREAMING path gates on the window too, on the same estimate.
///
/// `execute_stream` builds its own `SelectionCriteria` from the same payload, in its own
/// copy of the block `execute` uses. Nothing makes the two agree — a slice that wires
/// only `execute` leaves streaming callers exactly as unprotected as before, and the
/// duplicated prose in `stream.rs`'s selection-empty comment (which names the gates that
/// can cause an `AllGated`) becomes false. So parity is asserted rather than assumed,
/// mirroring `execute_stream_all_gated_at_selection_returns_allgated`.
///
/// Streaming is where an unfit candidate costs most: `execute` can still return an error,
/// but a stream that has begun has already committed the caller, and the provider's 400
/// arrives mid-flight.
///
/// Two claims, because one of them is weak alone. The `Done` model is the discriminating
/// half — the schema-heavy payload FITS the small model on the cost estimate, so
/// forwarding the wrong figure streams from `small` — while the over-everything half
/// pins the terminal shape a caller receives, which a `Done` assertion cannot show.
#[tokio::test]
async fn execute_stream_gates_on_the_context_window_like_execute() {
    use crate::types::error::HumanAction;

    let gw = ab_gateway(window_chain_config(128_000, 8_192));
    register_noop(&gw).await;

    // (a) A request the LARGE model can hold streams from the large model, though
    // `small` is the priority-1 entry and the cost estimate fits it.
    let events = collect_stream(
        &gw,
        &InferenceRequest {
            payload: schema_heavy_chat_payload(),
            ..chat_request_of_length(0)
        },
    )
    .await;
    let done_model = events.iter().find_map(|e| match e {
        StreamEvent::Done { model, .. } => Some(model.clone()),
        _ => None,
    });
    assert_eq!(
        done_model,
        Some("big".to_string()),
        "the stream must come from the 128k candidate — streaming from `small` means \
         `execute_stream` forwarded the COST estimate: {events:?}"
    );

    // (b) A request NO candidate can hold never starts a stream at all.
    match gw.execute_stream(&chat_request_of_length(600_000)).await {
        Err(GatewayError::AllGated {
            human_action,
            skipped,
            ..
        }) => {
            assert_eq!(human_action, Some(HumanAction::UseLargerContextWindow));
            assert!(
                skipped
                    .iter()
                    .any(|s| s.contains("8192-token context window")),
                "with the same per-candidate diagnostics `execute` gets: {skipped:?}"
            );
        }
        Err(other) => panic!("expected Err(AllGated) before any stream, got: {other}"),
        Ok(_) => panic!(
            "a request no candidate's window can hold must not start streaming — the \
             provider would answer 400 mid-stream, after the caller has committed"
        ),
    }
}

/// SP-ROUTE-1 Task 10 — the STREAMING half of the routing-preferences wiring,
/// which nothing else covered.
///
/// `execute_stream` builds its OWN `SelectionCriteria`, so
/// `a_requests_routing_preferences_reach_selection` — which drives `execute` —
/// says nothing about it. Verified rather than assumed: reverting `stream.rs`
/// alone to `preferences: None` left the entire 416-test gateway suite green, so
/// half of SP-ROUTE-1 could have gone inert unnoticed. Filed beside
/// `execute_stream_gates_on_the_context_window_like_execute`, which exists for
/// exactly this reason on exactly this seam and shares its fixture.
///
/// BOTH halves are asserted, and the first is what makes the second mean
/// something: unfiltered, this request streams from `small`, so a test asserting
/// only "it streamed from `big`" could not tell a working `ignore` from a
/// fixture that never had a choice.
#[tokio::test]
async fn execute_stream_honours_routing_preferences_like_execute() {
    let gw = ab_gateway(window_chain_config(128_000, 8_192));
    register_noop(&gw).await;

    fn done_model(events: &[StreamEvent]) -> Option<String> {
        events.iter().find_map(|e| match e {
            StreamEvent::Done { model, .. } => Some(model.clone()),
            _ => None,
        })
    }

    // `small` is the priority-1 entry and holds an empty request comfortably, so
    // it is what an unpreferenced stream comes from.
    let plain = collect_stream(&gw, &chat_request_of_length(0)).await;
    assert_eq!(
        done_model(&plain),
        Some("small".to_string()),
        "the BEFORE state: with no preferences the priority-1 candidate serves: \
         {plain:?}"
    );

    // `ignore` naming that candidate must demote the stream to `big`.
    let mut request = chat_request_of_length(0);
    request.routing = Some(crate::types::request::RoutingPreferences {
        ignore: Some(crate::types::request::CandidateSet {
            routers: vec![],
            models: vec!["small".to_string()],
        }),
        ..Default::default()
    });
    let filtered = collect_stream(&gw, &request).await;
    assert_eq!(
        done_model(&filtered),
        Some("big".to_string()),
        "`ignore` must reach selection through `execute_stream` too — streaming \
         from `small` means the caller's preferences were dropped on this path: \
         {filtered:?}"
    );
}

/// AC9 — a stream that fails after its first chunk must reach the health
/// recorders as a FAILURE.
///
/// Before this fix the mid-stream error path returned without dispatching at
/// all, while the acquisition dispatch had already fired — so an endpoint
/// failing every stream halfway looked perfectly healthy.
///
/// `success_rate` must be 0.0, NOT 0.5. Task 4's `StreamAcquired` phase casts
/// no verdict precisely so that one attempt yields one vote; 0.5 was the floor
/// that made it impossible for Task 7's reliability multiplier to de-weight a
/// totally broken endpoint.
#[tokio::test]
async fn a_mid_stream_failure_is_recorded_as_a_failure() {
    let mut routers = HashMap::new();
    routers.insert(
        "mid".to_string(),
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
        "mid".to_string(),
        ModelConfig {
            id: "mid".to_string(),
            api_model_id: None,
            provider: "mid".to_string(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window: 4096,
            max_output_tokens: 1024,
            pricing: None,
            catalog: None,
        },
    );
    let config = GatewayConfig {
        routers,
        models,
        chains: HashMap::new(),
        constraints: Default::default(),
        panels: Default::default(),
        consensus: Default::default(),
    };
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(config, AdapterRegistry::new(), cb);
    gw.adapters
        .register_chat(Arc::new(FakeStreamMidFailer {
            id: "mid".to_string(),
        }))
        .await;

    let request = InferenceRequest {
        capability: Capability::TextChat,
        model: Some("mid".to_string()),
        router: Some("mid".to_string()),
        chain: None,
        payload: Payload::Chat {
            messages: vec![Message::text(MessageRole::User, "hi")],
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
        routing: None,
    };

    let events = collect_stream(&gw, &request).await;
    assert!(
        matches!(events.last(), Some(StreamEvent::Error { .. })),
        "the fixture must actually fail mid-stream: {events:?}"
    );

    let stats = gw
        .performance_stats("mid:mid")
        .expect("the attempt must be recorded");
    assert_eq!(
        stats.verdict_samples, 1,
        "one attempt casts exactly one verdict"
    );
    assert!(
        (stats.success_rate - 0.0).abs() < 1e-9,
        "the single verdict is a failure: {stats:?}"
    );
}

/// The positive mirror, and the reason it exists: a test that only checks a
/// failed stream records a failure also passes against an implementation that
/// records EVERY stream as a failure.
#[tokio::test]
async fn a_completed_stream_is_recorded_as_a_success_with_its_throughput() {
    use crate::types::config::ModelPricing;

    let mut routers = HashMap::new();
    routers.insert(
        "priced".to_string(),
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
        "priced".to_string(),
        ModelConfig {
            id: "priced".to_string(),
            api_model_id: None,
            provider: "priced".to_string(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window: 4096,
            max_output_tokens: 1024,
            pricing: Some(ModelPricing {
                input_per_1k: 0.0008,
                output_per_1k: 0.004,
                per_request: None,
            }),
            catalog: None,
        },
    );
    let config = GatewayConfig {
        routers,
        models,
        chains: HashMap::new(),
        constraints: Default::default(),
        panels: Default::default(),
        consensus: Default::default(),
    };
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let mut gw = Gateway::new(config, AdapterRegistry::new(), cb);
    gw.adapters
        .register_chat(Arc::new(FakeStreamerWithRealDelay {
            id: "priced".to_string(),
        }))
        .await;
    let outcomes = Arc::new(std::sync::Mutex::new(Vec::new()));
    gw.recorders
        .push(Arc::new(OutcomeRecorder(outcomes.clone())));

    let request = InferenceRequest {
        capability: Capability::TextChat,
        model: Some("priced".to_string()),
        router: Some("priced".to_string()),
        chain: None,
        payload: Payload::Chat {
            messages: vec![Message::text(MessageRole::User, "hi")],
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
        routing: None,
    };

    let events = collect_stream(&gw, &request).await;
    assert!(matches!(events.last(), Some(StreamEvent::Done { .. })));

    let stats = gw.performance_stats("priced:priced").expect("recorded");
    assert_eq!(stats.verdict_samples, 1, "one attempt, one verdict");
    assert!(
        (stats.success_rate - 1.0).abs() < 1e-9,
        "and it succeeded: {stats:?}"
    );
    assert_eq!(
        stats.throughput_samples, 1,
        "completion carries the token count — proves the wiring"
    );

    // Deterministic, wall-clock-independent pin on the exact count dispatched
    // (Important 1): `mean_tokens_per_sec > 0.0` cannot fail once
    // `throughput_samples == 1` already forces a positive rate, so a scrambled
    // (but still positive) token count would ship silently. 500 is FakeStreamer's
    // `output_tokens`; 1000 is its `input_tokens` — a wrong-field swap at the
    // dispatch site must be visible here.
    let recorded = outcomes.lock().unwrap();
    let completion = recorded
        .iter()
        .find(|(phase, ..)| *phase == crate::gates::AttemptPhase::StreamCompleted)
        .expect("a StreamCompleted outcome must have been dispatched");
    assert_eq!(
        completion.2,
        Some(500),
        "the completion dispatch must carry OUTPUT tokens (500), not input tokens (1000): {recorded:?}"
    );
}

// --- SP-ROUTE-1 Task 5 review fixes: one attempt casts one verdict to every
// health recorder, not just `PerformanceRecorder` ---
//
// The review found that `AttemptPhase::StreamAcquired` — deliberately a
// latency-only observation, per Task 4 — was read ONLY by
// `PerformanceRecorder`. Every other recorder (`CircuitBreakerSink`,
// `ModelLockoutSink`, `ConnectionCooldownSink`) treated it as a full
// observation, so the acquisition dispatch's `success: true` reached
// `record_success` / `store.clear()` on EVERY streaming attempt — a real
// production defect (Critical 3 below). The tests in this section drove the
// mutation that proved it, then pin the `AttemptPhase::is_verdict()` fix.

/// A single-router/single-model ("mid"/"mid") config with NO fallback
/// candidate — used below so the point of each test is watching ONE
/// endpoint's health state evolve across repeated attempts, not fallback.
fn mid_gateway_config() -> GatewayConfig {
    let mut routers = HashMap::new();
    routers.insert(
        "mid".to_string(),
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
        "mid".to_string(),
        ModelConfig {
            id: "mid".to_string(),
            api_model_id: None,
            provider: "mid".to_string(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window: 4096,
            max_output_tokens: 1024,
            pricing: None,
            catalog: None,
        },
    );
    GatewayConfig {
        routers,
        models,
        chains: HashMap::new(),
        constraints: Default::default(),
        panels: Default::default(),
        consensus: Default::default(),
    }
}

/// A chat request pinned directly at the "mid" router/model (no chain).
fn mid_chat_request() -> InferenceRequest {
    InferenceRequest {
        capability: Capability::TextChat,
        model: Some("mid".to_string()),
        router: Some("mid".to_string()),
        chain: None,
        payload: Payload::Chat {
            messages: vec![Message::text(MessageRole::User, "hi")],
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
        routing: None,
    }
}

/// Critical 3, primary symptom. Before the `is_verdict()` fix, the
/// acquisition dispatch (`success: true`, phase `StreamAcquired`) reached
/// `CircuitBreakerSink` and called `record_success`, which RESETS
/// `Closed { failure_count }` to 0. The mid-stream failure then
/// re-incremented it to 1. Every attempt: 0 → 1 → 0 → 1 — the breaker could
/// never reach `threshold` no matter how many streams failed halfway.
#[tokio::test]
async fn repeated_mid_stream_failures_open_the_breaker() {
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig {
        threshold: 5,
        ..Default::default()
    });
    let gw = Gateway::new(mid_gateway_config(), AdapterRegistry::new(), cb.clone());
    gw.adapters
        .register_chat(Arc::new(FakeStreamMidFailer {
            id: "mid".to_string(),
        }))
        .await;
    let request = mid_chat_request();

    // try_drain_stream tolerates the breaker opening partway through: once
    // Open, selection gates the candidate and `execute_stream` returns Err
    // before any stream — a no-op for the loop, not a panic.
    for _ in 0..10 {
        try_drain_stream(&gw, &request).await;
    }

    assert_eq!(
        cb.get_state("mid:mid").name(),
        "open",
        "an endpoint failing every stream halfway must eventually be skipped"
    );
}

/// Critical 3, second symptom. A successful stream dispatches TWO
/// `success: true` outcomes (acquisition, then completion); in `HalfOpen`,
/// `record_success` increments `success_count` on each — so
/// `half_open_max_requests: 3` would close the breaker after two streaming
/// attempts, not three.
#[tokio::test]
async fn a_streaming_success_casts_one_breaker_vote_not_two() {
    use crate::circuit_breaker::BreakerState;

    let cb = CircuitBreakerManager::new(CircuitBreakerConfig {
        threshold: 1,
        timeout: std::time::Duration::from_millis(0),
        half_open_max_requests: 3,
    });
    // Drive "mid:mid" into HalfOpen directly: one failure to Open, then one
    // `can_execute` (timeout already expired) for the Open -> HalfOpen
    // transition — mirrors `circuit_breaker.rs`'s own tests.
    cb.can_execute("mid:mid");
    cb.record_failure("mid:mid");
    assert!(cb.can_execute("mid:mid"), "expired Open must admit");
    match cb.get_state("mid:mid") {
        BreakerState::HalfOpen { success_count } => assert_eq!(success_count, 0),
        other => panic!(
            "expected HalfOpen{{success_count:0}} before the attempt, got {}",
            other.name()
        ),
    }

    let gw = Gateway::new(mid_gateway_config(), AdapterRegistry::new(), cb.clone());
    gw.adapters
        .register_chat(Arc::new(FakeStreamer {
            id: "mid".to_string(),
        }))
        .await;

    let _ = collect_stream(&gw, &mid_chat_request()).await;

    match cb.get_state("mid:mid") {
        BreakerState::HalfOpen { success_count } => assert_eq!(
            success_count, 1,
            "one successful streaming attempt must cast exactly one breaker vote"
        ),
        other => panic!(
            "expected still HalfOpen{{success_count:1}}, got {}",
            other.name()
        ),
    }
}

/// Critical 3, third symptom. A consumer that drops a stream after one chunk
/// (a normal SSE client disconnect) leaves only the acquisition dispatch on
/// the wire. That dispatch must NOT hand the breaker a free `record_success`
/// for an attempt whose outcome nobody knows — proven by pre-seeding a
/// NONZERO failure count and confirming an abandoned stream does not reset
/// it. A latency sample IS still legitimately recorded (`PerformanceRecorder`
/// treats `StreamAcquired` as a latency-only observation, unaffected by this
/// fix).
#[tokio::test]
async fn an_abandoned_stream_casts_no_verdict_at_all() {
    use crate::circuit_breaker::BreakerState;
    use futures::StreamExt;

    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default()); // threshold 5
    cb.can_execute("mid:mid");
    cb.record_failure("mid:mid");
    cb.record_failure("mid:mid");
    match cb.get_state("mid:mid") {
        BreakerState::Closed { failure_count } => assert_eq!(failure_count, 2),
        other => panic!(
            "expected Closed{{failure_count:2}} before the attempt, got {}",
            other.name()
        ),
    }

    let gw = Gateway::new(mid_gateway_config(), AdapterRegistry::new(), cb.clone());
    gw.adapters
        .register_chat(Arc::new(FakeStreamer {
            id: "mid".to_string(),
        }))
        .await;

    let mut stream = gw
        .execute_stream(&mid_chat_request())
        .await
        .expect("stream should start");
    let first = stream.next().await;
    assert!(
        matches!(first, Some(StreamEvent::Chunk { .. })),
        "expected the first chunk, got {first:?}"
    );
    drop(stream); // abandon mid-stream — never polled again

    match cb.get_state("mid:mid") {
        BreakerState::Closed { failure_count } => assert_eq!(
            failure_count, 2,
            "an abandoned stream must not reset the failure count via a phantom record_success"
        ),
        other => panic!("the breaker must stay Closed{{2}}, got {}", other.name()),
    }

    let stats = gw
        .performance_stats("mid:mid")
        .expect("the acquisition latency observation IS legitimately recorded");
    assert_eq!(
        stats.verdict_samples, 0,
        "nobody knows whether the abandoned stream succeeded or failed"
    );
    assert_eq!(
        stats.samples, 1,
        "the acquisition dispatch still contributes a latency sample"
    );
}

/// Critical 2 — half of AC9 had no test at all: mutating the mid-stream
/// dispatch's `error: Some(&e)` to `error: None` survived the whole suite
/// (358/358). `ConnectionCooldownSink` matches on `o.error`, so a mid-stream
/// transport fault must reach it exactly as a setup-time one already does.
#[tokio::test]
async fn a_mid_stream_timeout_cools_the_router() {
    let gw = Gateway::new(
        mid_gateway_config(),
        AdapterRegistry::new(),
        CircuitBreakerManager::new(CircuitBreakerConfig::default()),
    );
    register_stream_mid_err(&gw, "mid", || GatewayError::Timeout {
        adapter: "mid".to_string(),
        model: "mid".to_string(),
        duration_ms: 1,
    })
    .await;

    let _ = collect_stream(&gw, &mid_chat_request()).await;

    assert!(
        gw.cooldown.cooling_until("mid").is_some(),
        "a mid-stream Timeout must cool the router exactly like a setup-time one"
    );
}

/// Critical 2, second half — the same gap on the lockout sink: a mid-stream
/// 429 must lock the endpoint exactly like a setup-time one.
#[tokio::test]
async fn a_mid_stream_rate_limit_locks_the_endpoint() {
    use crate::gates::lockout::{LockReason, ModelLockoutRead};

    let gw = Gateway::new(
        mid_gateway_config(),
        AdapterRegistry::new(),
        CircuitBreakerManager::new(CircuitBreakerConfig::default()),
    );
    register_stream_mid_err(&gw, "mid", || GatewayError::RateLimit {
        adapter: "mid".to_string(),
        retry_after_ms: Some(2000),
    })
    .await;

    let _ = collect_stream(&gw, &mid_chat_request()).await;

    let locked = gw
        .model_lockout
        .locked("mid:mid")
        .expect("a mid-stream 429 must lock the endpoint exactly like a setup-time one");
    assert_eq!(locked.reason, LockReason::RateLimit);
}

/// Important 3 — a single mid-stream 429 now locks the endpoint, so on a
/// single-candidate config the NEXT `execute_stream` is gated before any
/// stream starts: `Err(AllGated { resume_after: Some(_) })`, which the
/// orchestrator turns into a durable pause. A defensible policy, but a new
/// user-visible escalation with no test before this — pinned here.
#[tokio::test]
async fn a_mid_stream_rate_limit_pauses_the_next_stream_request() {
    let gw = Gateway::new(
        mid_gateway_config(),
        AdapterRegistry::new(),
        CircuitBreakerManager::new(CircuitBreakerConfig::default()),
    );
    register_stream_mid_err(&gw, "mid", || GatewayError::RateLimit {
        adapter: "mid".to_string(),
        retry_after_ms: Some(2000),
    })
    .await;

    let _ = collect_stream(&gw, &mid_chat_request()).await; // locks "mid:mid"

    match gw.execute_stream(&mid_chat_request()).await {
        Err(GatewayError::AllGated { resume_after, .. }) => {
            assert!(
                resume_after.is_some(),
                "a timed 429 lock must carry a resume instant, not fail outright"
            );
        }
        Err(other) => panic!("expected AllGated, got: {other}"),
        Ok(_) => {
            panic!("expected the SECOND stream request to be paused as AllGated, not admitted")
        }
    }
}

/// Important 3, secondary — the OTHER practical effect of the C3 fix. Before
/// it, the acquisition dispatch's `store.clear(o.endpoint)` in
/// `ModelLockoutSink` (its `success: true` branch) ran on EVERY streaming
/// attempt, wiping the escalation counter immediately before the mid-stream
/// failure dispatch that followed in the SAME attempt — so a relock after a
/// prior lock expired always looked like a fresh lock (escalation reset to
/// 0) and could never escalate past the base backoff.
#[tokio::test]
async fn a_second_mid_stream_rate_limit_escalates_past_the_first() {
    let gw = Gateway::new(
        mid_gateway_config(),
        AdapterRegistry::new(),
        CircuitBreakerManager::new(CircuitBreakerConfig::default()),
    );
    register_stream_mid_err(&gw, "mid", || GatewayError::RateLimit {
        adapter: "mid".to_string(),
        retry_after_ms: None, // synthetic backoff, so escalation shows up in `until`
    })
    .await;
    let now = Instant::now();

    // Pre-seed as though a first lock -> release cycle already happened: an
    // EXPIRED rate-limit lock at escalation 0 — exactly what a real first
    // mid-stream 429 leaves behind once its base cooldown elapses.
    gw.apply_lockout(
        "mid:mid",
        crate::gates::lockout::LockReason::RateLimit,
        Some(now - std::time::Duration::from_secs(1)),
    );

    let _ = collect_stream(&gw, &mid_chat_request()).await; // a genuine relock

    use crate::gates::lockout::ModelLockoutRead;
    let until = gw
        .model_lockout
        .locked("mid:mid")
        .expect("relocked")
        .until
        .expect("timed lock");
    let base = crate::gates::lockout::ModelLockoutPolicy::default().rate_limit_base;
    assert!(
        until > now + base + std::time::Duration::from_secs(30),
        "a genuine relock must escalate strictly past the base backoff (~60s): \
         until={until:?} now+base={:?}",
        now + base
    );
}

/// Minor 2 — the completion dispatch sits BEFORE `yield StreamEvent::Done` in
/// `stream.rs`, and that ordering is load-bearing: in an `async_stream`
/// generator, code after a `yield` runs only on the NEXT poll. A real SSE
/// handler that stops polling once it observes the terminal `Done` event (as
/// any sane consumer does) would never resume the generator far enough to
/// run a dispatch placed AFTER that yield — the verdict would silently
/// vanish. `collect_stream` drains to `None` and would not catch a
/// regression here; this test stops exactly where a real consumer stops.
#[tokio::test]
async fn a_consumer_that_stops_at_done_still_sees_its_verdict_recorded() {
    use futures::StreamExt;

    let gw = Gateway::new(
        mid_gateway_config(),
        AdapterRegistry::new(),
        CircuitBreakerManager::new(CircuitBreakerConfig::default()),
    );
    gw.adapters
        .register_chat(Arc::new(FakeStreamer {
            id: "mid".to_string(),
        }))
        .await;

    let mut stream = gw
        .execute_stream(&mid_chat_request())
        .await
        .expect("stream should start");
    loop {
        match stream.next().await {
            Some(StreamEvent::Done { .. }) => break,
            Some(_) => continue,
            None => panic!("stream ended without a terminal Done event"),
        }
    }
    drop(stream); // stop exactly where a real consumer stops — no further polling

    let stats = gw
        .performance_stats("mid:mid")
        .expect("recorded by the time Done was observed");
    assert_eq!(
        stats.verdict_samples, 1,
        "the verdict must already be recorded once Done is observed, not only after the \
         stream is fully drained to None"
    );
}

/// Minor 3 — a mid-stream failure's dispatch must carry `output_tokens:
/// None`, even when the provider's last good chunk reported usage. A failed
/// attempt has no meaningful rate; contributing a throughput sample for it
/// would let Task 9 average in a broken attempt's partial output.
#[tokio::test]
async fn a_mid_stream_failure_with_usage_contributes_no_throughput() {
    let gw = Gateway::new(
        mid_gateway_config(),
        AdapterRegistry::new(),
        CircuitBreakerManager::new(CircuitBreakerConfig::default()),
    );
    gw.adapters
        .register_chat(Arc::new(FakeStreamMidFailerWithUsage {
            id: "mid".to_string(),
        }))
        .await;

    let events = collect_stream(&gw, &mid_chat_request()).await;
    assert!(matches!(events.last(), Some(StreamEvent::Error { .. })));

    let stats = gw.performance_stats("mid:mid").expect("recorded");
    assert_eq!(
        stats.throughput_samples, 0,
        "a failed attempt must not contribute a throughput sample, even though its last \
         chunk carried usage: {stats:?}"
    );
}

/// SP-ROUTE-1 Task 6 review — Important 1. `with_random`/`with_performance`
/// have exactly one call site in the repo today (a unit test): production
/// (`execute`, `execute_stream`) builds its `ModelSelectionService` via
/// `Gateway::selection_service`, which calls `ModelSelectionService::new` and
/// nothing else. So today every production request routes off the fixed-seed
/// `DEFAULT_RNG`, and this asserts exactly that against the REAL production
/// construction path — `selection_service` is the one place both `execute`
/// and `execute_stream` build the service, extracted for this reason — not a
/// hand-mirrored copy of it.
///
/// `#[ignore]`d because it is expected to fail until SP-ROUTE-1 Task 10 adds
/// `.with_random(entropy_source)` (and, per the plan, `.with_performance`)
/// inside `selection_service`. Un-ignoring it then turns it into the
/// regression guard: if a future edit to `selection_service` drops the
/// `with_random` call, this goes red again.
#[test]
fn production_selection_never_uses_the_fixed_seed_default() {
    let gw = test_gateway();
    let config = test_config_with_noop();
    let svc = gw.selection_service(&config);
    assert!(
        !svc.uses_default_rng(),
        "production must pass an entropy-seeded source via with_random; the fixed-seed \
         DEFAULT_RNG makes every process draw the identical sequence, so weighted routing \
         synchronises across the fleet instead of spreading"
    );
}

/// SP-ROUTE-1 Task 10 — the OTHER half of `selection_service`'s wiring, which no
/// tripwire covered: `.with_performance`.
///
/// Rule 2 from Task 9's Critical: **a fixture that returns a constant cannot
/// test a live source.** Production reads the gateway's REAL `PerformanceStore`
/// — the one every completing attempt mutates through `record_outcome` — so this
/// asserts the answer CHANGES: same gateway, same criteria, a different route
/// once observations land. Every metric test in `selection.rs` and `strategy.rs`
/// uses a fixed fixture and is structurally unable to make that claim, so
/// `.with_performance` could be dropped from `selection_service` and all of them
/// would stay green.
///
/// Both halves are asserted, and the BEFORE half is what makes the AFTER half
/// mean anything: a test asserting only the final order would pass against a
/// chain that was already in that order. Here the before and after orders are
/// REVERSES of each other, so no fixed answer satisfies both.
#[test]
fn production_selection_reads_the_gateways_live_performance_store() {
    let gw = test_gateway_with_chain();
    let config = test_config_with_failing_and_noop();
    // `chat_chain`: fail-model@failing is priority 1, noop@noop is priority 2 —
    // so priority order and the observed-latency order below disagree.
    let criteria = SelectionCriteria {
        capability: Capability::TextChat,
        model: None,
        router: None,
        chain: Some("chat_chain".to_string()),
        budget: None,
        input_tokens: None,
        input_tokens_pessimistic: None,
        preferences: Some(crate::types::request::RoutingPreferences {
            sort: Some(crate::types::request::SortKey::Latency),
            ..Default::default()
        }),
    };
    let route = || -> Vec<String> {
        gw.selection_service(&config)
            .select_all(&criteria)
            .all_candidates
            .iter()
            .map(|c| c.model.clone())
            .collect()
    };

    assert_eq!(
        route(),
        vec!["fail-model", "noop"],
        "with nothing observed a latency sort degrades to priority order (AC7) — \
         this is the BEFORE state a live read has to move away from"
    );

    // The gateway's OWN write path: `record_outcome` fans out to the
    // `PerformanceRecorder`, which holds a clone of the very store
    // `selection_service` has to read. `min_samples` is 3, so three each.
    let observe = |endpoint: &str, router: &str, duration_ms: u64| {
        gw.record_outcome(&crate::gates::AttemptOutcome {
            endpoint,
            router,
            success: true,
            error: None,
            duration_ms,
            output_tokens: Some(10),
            phase: crate::gates::AttemptPhase::Complete,
        });
    };
    for _ in 0..3 {
        observe("failing:fail-model", "failing", 500);
        observe("noop:noop", "noop", 10);
    }

    assert_eq!(
        route(),
        vec!["noop", "fail-model"],
        "`noop` is observed at 10ms against `fail-model`'s 500ms, so a latency \
         sort must now put it first DESPITE its worse priority. Same gateway and \
         same criteria as the assertion above — only the live store moved, which \
         is the one thing a constant fixture cannot demonstrate"
    );
}
