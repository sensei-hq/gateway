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

        let breaker = CircuitBreakerManager::new(self.breaker);
        let gateway = Gateway::new(self.config, self.registry.clone(), breaker);

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
            capabilities: vec![Capability::TextChat],
            context_window: 4096,
            max_output_tokens: 1024,
            pricing: None,
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
