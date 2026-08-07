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
        credentials: Default::default(),
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
/// min over the TIMED gates only (B ~1800s); the terminal A is excluded, and a
/// timed retry wins over the human action.
#[tokio::test]
async fn all_gated_mixed_terminal_and_timed_uses_min_over_timed_only() {
    use crate::gates::lockout::LockReason;

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
            assert!(
                human_action.is_none(),
                "a timed retry wins over the terminal remedy"
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
