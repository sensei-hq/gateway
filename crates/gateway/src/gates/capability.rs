use super::{AdmissionGate, CandidateView, GateVerdict, SelectionCtx};
use crate::skip_reason::SkipReason;

/// Gate: the candidate model must declare the requested capability.
pub struct CapabilityGate;

impl AdmissionGate for CapabilityGate {
    fn name(&self) -> &'static str {
        "capability"
    }

    fn evaluate(&self, c: &CandidateView<'_>, x: &SelectionCtx<'_>) -> GateVerdict {
        if c.model_config.capabilities.contains(&x.capability) {
            GateVerdict::Admit
        } else {
            GateVerdict::Skip(SkipReason::UnsupportedCapability(x.capability.clone()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gates::EndpointHealthRead;
    use crate::types::capability::Capability;
    use crate::types::config::{GatewayConfig, ModelConfig, RouterConfig};
    use std::collections::HashMap;
    use std::time::Instant;

    /// Health port stub for tests that don't exercise the circuit breaker.
    struct NeverOpen;
    impl EndpointHealthRead for NeverOpen {
        fn open_until(&self, _endpoint: &str) -> Option<Instant> {
            None
        }
    }

    /// Router health port stub for tests that don't exercise cooldown.
    struct NeverCooling;
    impl crate::gates::RouterHealthRead for NeverCooling {
        fn cooling_until(&self, _router: &str) -> Option<Instant> {
            None
        }
    }

    fn embed_only_model_config() -> ModelConfig {
        ModelConfig {
            id: "all-minilm".to_string(),
            api_model_id: None,
            provider: "ollama".to_string(),
            family: None,
            capabilities: vec![Capability::TextEmbed],
            context_window: 512,
            max_output_tokens: 0,
            pricing: None,
            catalog: None,
        }
    }

    fn test_router_config() -> RouterConfig {
        RouterConfig {
            url: "http://localhost:11434".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        }
    }

    #[test]
    fn skips_unsupported_capability() {
        let model_config = embed_only_model_config();
        let router_config = test_router_config();
        let cand = CandidateView {
            model: "all-minilm",
            router: "ollama",
            endpoint: "ollama:all-minilm".to_string(),
            model_config: &model_config,
            router_config: &router_config,
        };
        let gateway_config = GatewayConfig::default();
        let health = NeverOpen;
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let ctx = SelectionCtx {
            capability: Capability::AudioTranscribe,
            budget: None,
            input_tokens: None,
            // Not a window test — `None` admits, so this fixture is unaffected by
            // `ContextWindowGate`.
            input_tokens_pessimistic: None,
            health: &health,
            now: Instant::now(),
            config: &gateway_config,
            router_health: &NeverCooling,
            model_lockout: &lockout,
        };

        let verdict = CapabilityGate.evaluate(&cand, &ctx);
        assert!(matches!(
            verdict,
            GateVerdict::Skip(SkipReason::UnsupportedCapability(_))
        ));
    }

    #[test]
    fn admits_supported_capability() {
        let model_config = embed_only_model_config();
        let router_config = test_router_config();
        let cand = CandidateView {
            model: "all-minilm",
            router: "ollama",
            endpoint: "ollama:all-minilm".to_string(),
            model_config: &model_config,
            router_config: &router_config,
        };
        let gateway_config = GatewayConfig::default();
        let health = NeverOpen;
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let ctx = SelectionCtx {
            capability: Capability::TextEmbed,
            budget: None,
            input_tokens: None,
            // Not a window test — `None` admits, so this fixture is unaffected by
            // `ContextWindowGate`.
            input_tokens_pessimistic: None,
            health: &health,
            now: Instant::now(),
            config: &gateway_config,
            router_health: &NeverCooling,
            model_lockout: &lockout,
        };

        let verdict = CapabilityGate.evaluate(&cand, &ctx);
        assert!(matches!(verdict, GateVerdict::Admit));
    }
}
