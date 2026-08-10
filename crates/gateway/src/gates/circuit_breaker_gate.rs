use super::{
    AdmissionGate, AttemptOutcome, CandidateView, GateVerdict, HealthRecorder, SelectionCtx,
};
use crate::circuit_breaker::CircuitBreakerManager;
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

/// Behavior-preserving breaker write-side: success→record_success, failure→record_failure.
pub struct CircuitBreakerSink {
    breaker: CircuitBreakerManager,
}

impl CircuitBreakerSink {
    pub fn new(breaker: CircuitBreakerManager) -> Self {
        Self { breaker }
    }
}

impl HealthRecorder for CircuitBreakerSink {
    fn on_outcome(&self, o: &AttemptOutcome<'_>) -> Option<std::time::Instant> {
        if o.success {
            self.breaker.record_success(o.endpoint);
            None
        } else {
            self.breaker.record_failure(o.endpoint);
            // Read the resulting state PURELY via `get_state` (no Open→HalfOpen
            // side effect, unlike `open_until`/`can_execute`). If this failure
            // just tripped the breaker Open, surface its `next_retry`.
            match self.breaker.get_state(o.endpoint) {
                crate::circuit_breaker::BreakerState::Open { next_retry } => Some(next_retry),
                _ => None,
            }
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

    /// Router health port stub for tests that don't exercise cooldown.
    struct NeverCooling;
    impl crate::gates::RouterHealthRead for NeverCooling {
        fn cooling_until(&self, _router: &str) -> Option<Instant> {
            None
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
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let ctx = SelectionCtx {
            capability: Capability::TextChat,
            budget: None,
            input_tokens: None,
            health: &health,
            now: Instant::now(),
            config: &gateway_config,
            router_health: &NeverCooling,
            model_lockout: &lockout,
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
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let ctx = SelectionCtx {
            capability: Capability::TextChat,
            budget: None,
            input_tokens: None,
            health: &health,
            now: Instant::now(),
            config: &gateway_config,
            router_health: &NeverCooling,
            model_lockout: &lockout,
        };

        let verdict = CircuitBreakerGate.evaluate(&cand, &ctx);
        assert!(matches!(verdict, GateVerdict::Admit));
    }

    #[test]
    fn circuit_breaker_sink_records_success_and_failure() {
        use crate::circuit_breaker::{BreakerState, CircuitBreakerConfig, CircuitBreakerManager};
        let cb = CircuitBreakerManager::new(CircuitBreakerConfig {
            threshold: 1,
            ..Default::default()
        });
        cb.can_execute("r:m"); // init
        let sink = CircuitBreakerSink::new(cb.clone());
        // A failure that trips the breaker to Open returns the `next_retry` it
        // just wrote (read purely via `get_state`, not the side-effecting reads).
        let returned = sink.on_outcome(&AttemptOutcome {
            endpoint: "r:m",
            router: "r",
            success: false,
            error: None,
        });
        assert_eq!(cb.get_state("r:m").name(), "open"); // threshold 1 → opens on one failure
        let next_retry = match cb.get_state("r:m") {
            BreakerState::Open { next_retry } => next_retry,
            other => panic!("expected Open, got {}", other.name()),
        };
        assert_eq!(returned, Some(next_retry));

        cb.can_execute("r:n");
        let returned = sink.on_outcome(&AttemptOutcome {
            endpoint: "r:n",
            router: "r",
            success: true,
            error: None,
        });
        assert_eq!(returned, None, "success returns no wake-up instant");
        assert_eq!(cb.get_state("r:n").name(), "closed");

        // A below-threshold failure (higher threshold) stays Closed → None.
        let cb2 = CircuitBreakerManager::new(CircuitBreakerConfig {
            threshold: 5,
            ..Default::default()
        });
        cb2.can_execute("r:o");
        let sink2 = CircuitBreakerSink::new(cb2.clone());
        let returned = sink2.on_outcome(&AttemptOutcome {
            endpoint: "r:o",
            router: "r",
            success: false,
            error: None,
        });
        assert_eq!(cb2.get_state("r:o").name(), "closed");
        assert_eq!(
            returned, None,
            "below-threshold failure stays closed → None"
        );
    }
}
