//! Batteries-included composition root — one call to stand up a [`Gateway`]
//! with cloud providers registered (feature `cloud`) and the local provisioning
//! supervisor wired (feature `local`), over a single shared [`AdapterRegistry`].
//!
//! The low-level [`Gateway::new`] / [`Gateway::with_store`] /
//! [`Gateway::with_readiness`] stay public for hand-wiring; this builder is the
//! ergonomic default. Cloud adapters that need setup the builder can't do from
//! config alone (e.g. `bedrock`'s AWS SDK) are registered by the caller into the
//! shared [`FacadeBuilder::registry`] before [`FacadeBuilder::build`].

#[cfg(any(feature = "cloud", feature = "local"))]
use std::sync::Arc;

use crate::adapters::AdapterRegistry;
use crate::circuit_breaker::{CircuitBreakerConfig, CircuitBreakerManager};
use crate::engine::Gateway;
use crate::types::config::GatewayConfig;
#[cfg(feature = "cloud")]
use crate::types::error::GatewayError;

#[cfg(feature = "local")]
use local_engine::{ProvisionPlan, ProvisioningSupervisor};
#[cfg(feature = "local")]
use std::collections::HashMap;

/// Fluent composition root. See the module docs.
pub struct FacadeBuilder {
    config: GatewayConfig,
    breaker: CircuitBreakerConfig,
    registry: AdapterRegistry,
    #[cfg(feature = "local")]
    plans: HashMap<String, ProvisionPlan>,
    #[cfg(feature = "local")]
    max_concurrent_provisioning: usize,
}

/// The composed [`Gateway`], plus (feature `local`) the supervisor handle the
/// caller drives with `ensure`/`status` and whose `ProvisionHandle::events()`
/// it relays to clients.
pub struct Facade {
    /// The wired gateway — ready to `execute` / `execute_stream`.
    pub gateway: Gateway,
    /// The provisioning supervisor backing the gateway's readiness probe.
    #[cfg(feature = "local")]
    pub supervisor: Arc<ProvisioningSupervisor>,
}

impl FacadeBuilder {
    /// Start from a [`GatewayConfig`] (validate it yourself via
    /// [`GatewayBuilder`](crate::GatewayBuilder) or [`Gateway::try_new`] first if
    /// you want the checked path).
    pub fn new(config: GatewayConfig) -> Self {
        Self {
            config,
            breaker: CircuitBreakerConfig::default(),
            registry: AdapterRegistry::new(),
            #[cfg(feature = "local")]
            plans: HashMap::new(),
            #[cfg(feature = "local")]
            max_concurrent_provisioning: 2,
        }
    }

    /// Override the circuit-breaker policy (defaults to
    /// [`CircuitBreakerConfig::default`]).
    pub fn circuit_breaker(mut self, breaker: CircuitBreakerConfig) -> Self {
        self.breaker = breaker;
        self
    }

    /// The shared registry. Register any adapters the auto-wiring doesn't cover
    /// (e.g. `bedrock`, or custom routers) here before [`Self::build`]; the
    /// composed gateway dispatches to whatever ends up registered.
    pub fn registry(&self) -> &AdapterRegistry {
        &self.registry
    }

    /// Provisioning plans keyed by model id (feature `local`).
    #[cfg(feature = "local")]
    pub fn plans(mut self, plans: HashMap<String, ProvisionPlan>) -> Self {
        self.plans = plans;
        self
    }

    /// Cap on concurrent provisioning jobs (feature `local`; default 2).
    #[cfg(feature = "local")]
    pub fn max_concurrent_provisioning(mut self, n: usize) -> Self {
        self.max_concurrent_provisioning = n;
        self
    }

    /// Compose the gateway: register cloud providers from config (feature
    /// `cloud`), then wire the provisioning supervisor via
    /// [`Gateway::with_readiness`] (feature `local`), all over the one shared
    /// registry. Cloud adapters that fail to build (e.g. a missing key) are
    /// logged and skipped — construction never fails on a single bad router.
    pub async fn build(self) -> Facade {
        #[cfg(feature = "cloud")]
        for (router, err) in register_cloud_from_config(&self.registry, &self.config).await {
            tracing::warn!(router = %router, error = %err, "cloud adapter not registered (skipped)");
        }

        // A model whose price cannot be COMPARED is not safely routable, so drop
        // it. This is the production path — `Gateway::new` below is unchecked by
        // documented design, and `collect_validation_errors` guards only the
        // checked entry points — so without this a programmatically-assembled
        // `GatewayConfig` carrying a negative or non-finite price reaches routing
        // untouched. (A config FILE cannot: `ModelPricing`'s deserializer already
        // refuses one.)
        //
        // DROP rather than null its pricing. The tempting repair is
        // `model.pricing = None` — carry on without the bad price — and that is
        // the hazard, not the fix: `None` means FREE, `PriceStrategy`'s
        // `price_key` maps it to `0.0`, and free sorts FIRST, so "cleaning" a
        // broken price hands it the cheapest slot. It is the same reasoning
        // SP-ROUTE-1 used when it mapped a non-finite `PriceStrategy` key to
        // `+inf` rather than to zero. A chain still referencing a dropped model
        // reports `SkipReason::ModelNotFound`, which is `Structural` and surfaces
        // in the selection diagnostics — traceable, and it cannot silently win
        // anything.
        //
        // Matches the stance the facade already takes for a cloud router that
        // fails to build: logged at `warn`, skipped, construction still succeeds.
        // `build`'s signature is unchanged; it may simply build with fewer models.
        // Pinned by `pricing_tests::a_dropped_model_never_outranks_a_priced_one`.
        let mut config = self.config;
        config.models.retain(
            |id, model| match model.pricing.as_ref().map(|p| p.validate()) {
                Some(Err(reason)) => {
                    tracing::warn!(model = %id, error = %reason,
                        "model dropped: its pricing cannot be compared");
                    false
                }
                _ => true,
            },
        );

        let breaker = CircuitBreakerManager::new(self.breaker);
        let gateway = Gateway::new(config, self.registry.clone(), breaker);

        #[cfg(feature = "local")]
        {
            let supervisor = Arc::new(ProvisioningSupervisor::new(
                self.plans,
                self.max_concurrent_provisioning,
            ));
            let gateway = gateway.with_readiness(supervisor.clone());
            Facade {
                gateway,
                supervisor,
            }
        }
        #[cfg(not(feature = "local"))]
        {
            Facade { gateway }
        }
    }
}

/// Register the cloud adapter matching each well-known router name from config,
/// returning `(router, error)` for any that failed to build. `bedrock` (which
/// needs explicit AWS SDK setup) and unrecognised router names are skipped for
/// the caller to register manually via the shared registry.
#[cfg(feature = "cloud")]
async fn register_cloud_from_config(
    registry: &AdapterRegistry,
    config: &GatewayConfig,
) -> Vec<(String, GatewayError)> {
    use crate::adapters::RegisterInto;
    use cloud_providers as cp;

    // Build then register an adapter, or bubble the build error.
    async fn reg<A: RegisterInto + 'static>(
        registry: &AdapterRegistry,
        built: Result<A, GatewayError>,
    ) -> Result<(), GatewayError> {
        registry.register(Arc::new(built?)).await;
        Ok(())
    }

    let mut failures = Vec::new();
    for (id, router) in &config.routers {
        let outcome: Option<Result<(), GatewayError>> = match id.as_str() {
            "anthropic" => Some(
                reg(
                    registry,
                    cp::anthropic::AnthropicAdapter::from_config(router),
                )
                .await,
            ),
            "openai" => Some(reg(registry, cp::openai::OpenAIAdapter::from_config(router)).await),
            "gemini" => Some(reg(registry, cp::gemini::GeminiAdapter::from_config(router)).await),
            "grok" => Some(reg(registry, cp::grok::GrokAdapter::from_config(router)).await),
            "ollama" => Some(reg(registry, cp::ollama::OllamaAdapter::from_config(router)).await),
            "huggingface" => Some(
                reg(
                    registry,
                    cp::huggingface::HuggingFaceAdapter::from_config(router),
                )
                .await,
            ),
            "together" => {
                Some(reg(registry, cp::together::TogetherAdapter::from_config(router)).await)
            }
            "fal" => Some(reg(registry, cp::fal::FalAdapter::from_config(router)).await),
            "flux" => Some(reg(registry, cp::flux::FluxAdapter::from_config(router)).await),
            "kling" => Some(reg(registry, cp::kling::KlingAdapter::from_config(router)).await),
            "luma" => Some(reg(registry, cp::luma::LumaAdapter::from_config(router)).await),
            "runway" => Some(reg(registry, cp::runway::RunwayAdapter::from_config(router)).await),
            "stability" => Some(
                reg(
                    registry,
                    cp::stability::StabilityAdapter::from_config(router),
                )
                .await,
            ),
            "recraft" => {
                Some(reg(registry, cp::recraft::RecraftAdapter::from_config(router)).await)
            }
            "replicate" => Some(
                reg(
                    registry,
                    cp::replicate::ReplicateAdapter::from_config(router),
                )
                .await,
            ),
            _ => None,
        };
        if let Some(Err(e)) = outcome {
            failures.push((id.clone(), e));
        }
    }
    failures
}

#[cfg(all(test, feature = "local"))]
mod tests {
    use super::*;
    use crate::pruning::Availability;
    use crate::types::capability::Capability;
    use crate::types::config::{ChainEntry, FallbackChainConfig, ModelConfig, RouterConfig};
    use crate::types::error::GatewayError;
    use crate::types::request::{InferenceRequest, Message, MessageRole, Payload};
    use local_engine::{EnsureOpts, ScriptedPlan};

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

    fn model(id: &str, provider: &str) -> ModelConfig {
        ModelConfig {
            id: id.to_string(),
            api_model_id: None,
            provider: provider.to_string(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window: 4096,
            max_output_tokens: 1024,
            pricing: None,
            catalog: None,
        }
    }

    /// Chain [m@local (provisioning), c1@cloudy (unavailable)].
    fn two_candidate_config() -> GatewayConfig {
        let routers = HashMap::from([
            ("local".to_string(), router(true)),
            ("cloudy".to_string(), router(true)),
        ]);
        let models = HashMap::from([
            ("m".to_string(), model("m", "local")),
            ("c1".to_string(), model("c1", "cloudy")),
        ]);
        let chains = HashMap::from([(
            "chat".to_string(),
            FallbackChainConfig {
                id: "chat".to_string(),
                capability: Capability::TextChat,
                models: vec![
                    ChainEntry {
                        model: "m".to_string(),
                        router: Some("local".to_string()),
                        api_model_id: None,
                        priority: 1,
                    },
                    ChainEntry {
                        model: "c1".to_string(),
                        router: Some("cloudy".to_string()),
                        api_model_id: None,
                        priority: 2,
                    },
                ],
                fallback_triggers: vec![],
            },
        )]);
        GatewayConfig {
            routers,
            models,
            chains,
            constraints: Default::default(),
            panels: Default::default(),
            consensus: Default::default(),
        }
    }

    fn chat_request_on_chain(chain: &str) -> InferenceRequest {
        InferenceRequest {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some(chain.to_string()),
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

    #[tokio::test]
    async fn facade_wires_readiness_probe_and_pruning() {
        // A plan that parks the model in `Downloading` (in-flight, never Ready).
        let plans = HashMap::from([(
            "m".to_string(),
            ProvisionPlan::Scripted(ScriptedPlan::new(vec![
                kernel::ProvisionPhase::Downloading {
                    done: 1,
                    total: Some(2),
                },
            ])),
        )]);

        let facade = FacadeBuilder::new(two_candidate_config())
            .plans(plans)
            .build()
            .await;

        // Kick provisioning: the model is now in-flight (Queued → Downloading).
        facade.supervisor.ensure("m", EnsureOpts::default());

        // No adapters are registered, so both candidates miss and the chain
        // exhausts — but the wired probe degrades the in-flight `m` to a terminal
        // ModelNotReady rather than AllAttemptsFailed. This proves `with_readiness`
        // was wired through the facade.
        match facade.gateway.execute(&chat_request_on_chain("chat")).await {
            Err(GatewayError::ModelNotReady { model, phase }) => {
                assert_eq!(model, "m");
                assert!(phase.is_in_flight());
            }
            other => panic!("expected ModelNotReady, got: {other:?}"),
        }

        // Pruning runs through the composed gateway: the judge marks the cloudy
        // router unavailable (no key), so its candidate is dropped with a warning.
        let warnings = facade
            .gateway
            .prune_unavailable(|router, _model| {
                if router == "cloudy" {
                    Availability::Unavailable {
                        reason: "no api key".to_string(),
                    }
                } else {
                    Availability::Pending
                }
            })
            .await;
        assert!(
            warnings
                .iter()
                .any(|w| w.router == "cloudy" && w.model == "c1"),
            "expected a warning for the pruned cloudy/c1 candidate, got: {warnings:?}"
        );
    }
}

// Cloud-path tests run under the default (`cloud`) feature set — the one the
// coverage job measures — exercising `register_cloud_from_config` + the
// non-`local` `build` branch.
#[cfg(all(test, feature = "cloud"))]
mod cloud_tests {
    use super::*;
    use crate::types::config::RouterConfig;
    use std::collections::HashMap;

    fn router() -> RouterConfig {
        RouterConfig {
            url: "https://example.test".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn build_registers_known_cloud_routers_and_skips_unknown() {
        // `openai` / `anthropic` are recognised (their `from_config` builds a
        // client without needing a key); an unrecognised router is skipped, not
        // treated as a failure.
        let routers = HashMap::from([
            ("openai".to_string(), router()),
            ("anthropic".to_string(), router()),
            ("totally-unknown-router".to_string(), router()),
        ]);
        let config = GatewayConfig {
            routers,
            ..Default::default()
        };

        let facade = FacadeBuilder::new(config).build().await;
        let adapters = facade.gateway.list_adapters().await;

        assert!(
            adapters.iter().any(|a| a == "openai"),
            "openai should register, got {adapters:?}"
        );
        assert!(
            adapters.iter().any(|a| a == "anthropic"),
            "anthropic should register, got {adapters:?}"
        );
        assert!(
            !adapters.iter().any(|a| a == "totally-unknown-router"),
            "unknown router must be skipped, got {adapters:?}"
        );
    }

    #[tokio::test]
    async fn build_with_no_routers_yields_a_gateway_with_no_adapters() {
        let facade = FacadeBuilder::new(GatewayConfig::default()).build().await;
        assert!(facade.gateway.list_adapters().await.is_empty());
    }

    #[tokio::test]
    async fn circuit_breaker_override_is_accepted() {
        // Exercises the builder override + the non-local `build` branch.
        let facade = FacadeBuilder::new(GatewayConfig::default())
            .circuit_breaker(CircuitBreakerConfig::default())
            .build()
            .await;
        assert!(!facade.gateway.is_configured().await);
    }
}

/// SP-ROUTE-1.1 Task 4 — the production path drops a model whose price cannot be
/// compared, and must NOT make it free.
///
/// Deliberately a plain `#[cfg(test)]` module rather than joining the
/// `#[cfg(all(test, feature = "local"))]` one above: that module is not compiled
/// by a default `cargo test`, so tests placed there would never run and would
/// guard nothing. This module compiles under every feature combination.
#[cfg(test)]
mod pricing_tests {
    use super::*;
    use crate::types::capability::Capability;
    use crate::types::config::{
        ChainEntry, FallbackChainConfig, ModelConfig, ModelPricing, RouterConfig,
    };
    use crate::types::error::GatewayError;
    use crate::types::request::{
        InferenceRequest, Message, MessageRole, Payload, RoutingPreferences, SortKey,
    };
    use std::collections::HashMap;

    /// A router whose id matches no well-known cloud provider, so
    /// `register_cloud_from_config` registers no adapter for it under the default
    /// `cloud` feature. Every candidate therefore fails with "no adapter
    /// registered" — which still records an `Attempt`, in the order the strategy
    /// produced.
    fn bench_router() -> RouterConfig {
        RouterConfig {
            url: "http://localhost".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        }
    }

    fn model(id: &str, pricing: Option<ModelPricing>) -> ModelConfig {
        ModelConfig {
            id: id.to_string(),
            api_model_id: None,
            provider: "bench".to_string(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window: 4096,
            max_output_tokens: 1024,
            pricing,
            catalog: None,
        }
    }

    /// `good` is genuinely priced (`0.5`/1k output over a 1024-token ceiling ⇒ an
    /// estimate of ~0.512); `bad` carries a NEGATIVE price, which no config file
    /// can express since SP-ROUTE-1.1 Task 2 but a programmatic `GatewayConfig`
    /// still can — the exact input `Facade::build` is the last line against.
    ///
    /// The chain authors `good` FIRST (priority 1). That is deliberate: it means
    /// a passing AC5 cannot be explained by chain order alone — anything that
    /// puts `bad` in front had to be the price sort actively promoting it.
    fn config_with_a_bad_price() -> GatewayConfig {
        let models = HashMap::from([
            (
                "good".to_string(),
                model(
                    "good",
                    Some(ModelPricing {
                        input_per_1k: 0.5,
                        output_per_1k: 0.5,
                        per_request: None,
                    }),
                ),
            ),
            (
                "bad".to_string(),
                model(
                    "bad",
                    Some(ModelPricing {
                        input_per_1k: -1.0,
                        output_per_1k: -1.0,
                        per_request: None,
                    }),
                ),
            ),
        ]);
        let chains = HashMap::from([(
            "chat".to_string(),
            FallbackChainConfig {
                id: "chat".to_string(),
                capability: Capability::TextChat,
                models: vec![
                    ChainEntry {
                        model: "good".to_string(),
                        router: Some("bench".to_string()),
                        api_model_id: None,
                        priority: 1,
                    },
                    ChainEntry {
                        model: "bad".to_string(),
                        router: Some("bench".to_string()),
                        api_model_id: None,
                        priority: 2,
                    },
                ],
                fallback_triggers: vec![],
            },
        )]);
        GatewayConfig {
            routers: HashMap::from([("bench".to_string(), bench_router())]),
            models,
            chains,
            constraints: Default::default(),
            panels: Default::default(),
            consensus: Default::default(),
        }
    }

    fn price_sorted_chat_request() -> InferenceRequest {
        InferenceRequest {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat".to_string()),
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
            routing: Some(RoutingPreferences {
                sort: Some(SortKey::Price),
                ..Default::default()
            }),
        }
    }

    /// AC4 — a programmatically-constructed bad price does not reach routing.
    ///
    /// The facade drops the model and still builds, matching how it already logs
    /// and skips a cloud router that fails to construct. Its signature is
    /// unchanged: `build` still returns `Facade`, not `Result`.
    #[tokio::test]
    async fn the_facade_drops_a_model_whose_pricing_cannot_be_compared() {
        let facade = FacadeBuilder::new(config_with_a_bad_price()).build().await;

        let models = facade
            .gateway
            .list_models()
            .await
            .expect("the facade must still build and serve");
        let ids: Vec<String> = models
            .iter()
            .map(|m| m["id"].as_str().unwrap().to_string())
            .collect();

        assert!(
            !ids.contains(&"bad".to_string()),
            "the unusable model must be dropped: {ids:?}"
        );
        assert!(
            ids.contains(&"good".to_string()),
            "and the rest must survive: {ids:?}"
        );
    }

    /// AC5 — the SHARP one. A dropped model must not come back as FREE.
    ///
    /// `pricing: None` means free, `price_key` maps it to `0.0`, and free sorts
    /// FIRST — so "repairing" a bad price by nulling it would hand it the
    /// cheapest slot. That is the exact hazard SP-ROUTE-1 avoided when it chose
    /// `+inf` over `0.0` for a non-finite `PriceStrategy` key.
    ///
    /// The observable is the walk order the engine actually attempted, read off
    /// `AllAttemptsFailed::attempts_detail` (no adapter is registered for the
    /// `bench` router, so every candidate is attempted and recorded in strategy
    /// order). Three outcomes are distinguishable:
    ///
    /// - drop (correct): `bad` is not a model at all, the chain entry skips as
    ///   `ModelNotFound`, and the order is `["good"]`;
    /// - `pricing = None` (the tempting repair): `bad` is free, sorts ahead of
    ///   `good`'s 0.512, order is `["bad", "good"]`;
    /// - no handling at all: `bad`'s negative estimate also sorts first, order is
    ///   `["bad", "good"]`.
    ///
    /// Every other test in this slice fails when the feature is ABSENT. This one
    /// fails when the feature is implemented the WRONG way, which is why it
    /// exists.
    #[tokio::test]
    async fn a_dropped_model_never_outranks_a_priced_one() {
        let facade = FacadeBuilder::new(config_with_a_bad_price()).build().await;

        let order: Vec<String> = match facade.gateway.execute(&price_sorted_chat_request()).await {
            Err(GatewayError::AllAttemptsFailed {
                attempts_detail, ..
            }) => attempts_detail.iter().map(|a| a.model.clone()).collect(),
            other => panic!("expected AllAttemptsFailed carrying the walk order, got: {other:?}"),
        };

        assert_eq!(
            order,
            vec!["good".to_string()],
            "the dropped model must be absent, not free-and-first"
        );
    }
}
