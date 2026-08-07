use super::{AdmissionGate, CandidateView, GateVerdict, SelectionCtx};
use crate::skip_reason::SkipReason;

/// Gate: the candidate's endpoint must not be circuit-broken open.
pub struct CircuitBreakerGate;

impl AdmissionGate for CircuitBreakerGate {
    fn name(&self) -> &'static str {
        "circuit_breaker"
    }

    fn evaluate(&self, c: &CandidateView<'_>, x: &SelectionCtx<'_>) -> GateVerdict {
        match x.health.open_until(&c.endpoint) {
            Some(until) => GateVerdict::Skip(SkipReason::CircuitOpen { until }),
            None => GateVerdict::Admit,
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

    /// Fake health port returning a fixed value regardless of endpoint.
    struct FakeHealth(Option<Instant>);
    impl EndpointHealthRead for FakeHealth {
        fn open_until(&self, _endpoint: &str) -> Option<Instant> {
            self.0
        }
    }

    fn test_model_config() -> ModelConfig {
        ModelConfig {
            id: "gemma3:27b".to_string(),
            api_model_id: None,
            provider: "ollama".to_string(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window: 128000,
            max_output_tokens: 8192,
            pricing: None,
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
    fn skips_when_endpoint_reports_open_until() {
        let model_config = test_model_config();
        let router_config = test_router_config();
        let cand = CandidateView {
            model: "gemma3:27b",
            router: "ollama",
            endpoint: "ollama:gemma3:27b".to_string(),
            model_config: &model_config,
            router_config: &router_config,
        };
        let gateway_config = GatewayConfig::default();
        let health = FakeHealth(Some(Instant::now()));
        let ctx = SelectionCtx {
            capability: Capability::TextChat,
            budget: None,
            input_tokens: None,
            health: &health,
            now: Instant::now(),
            config: &gateway_config,
        };

        let verdict = CircuitBreakerGate.evaluate(&cand, &ctx);
        assert!(matches!(
            verdict,
            GateVerdict::Skip(SkipReason::CircuitOpen { .. })
        ));
    }

    #[test]
    fn admits_when_endpoint_reports_none() {
        let model_config = test_model_config();
        let router_config = test_router_config();
        let cand = CandidateView {
            model: "gemma3:27b",
            router: "ollama",
            endpoint: "ollama:gemma3:27b".to_string(),
            model_config: &model_config,
            router_config: &router_config,
        };
        let gateway_config = GatewayConfig::default();
        let health = FakeHealth(None);
        let ctx = SelectionCtx {
            capability: Capability::TextChat,
            budget: None,
            input_tokens: None,
            health: &health,
            now: Instant::now(),
            config: &gateway_config,
        };

        let verdict = CircuitBreakerGate.evaluate(&cand, &ctx);
        assert!(matches!(verdict, GateVerdict::Admit));
    }
}
