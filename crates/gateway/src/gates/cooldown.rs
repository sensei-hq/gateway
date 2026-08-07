use super::{AdmissionGate, CandidateView, GateVerdict, RouterHealthRead, SelectionCtx};
use crate::skip_reason::SkipReason;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// In-memory per-router cooldown state (read by the gate, written by the sink).
/// Arc-backed + Clone so a read reference and an owned sink copy share one map.
#[derive(Clone, Default)]
pub struct ConnectionCooldownStore {
    cooling: Arc<Mutex<HashMap<String, Instant>>>,
}
impl ConnectionCooldownStore {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn start(&self, router: &str, until: Instant) {
        self.cooling
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(router.to_string(), until);
    }
}
impl RouterHealthRead for ConnectionCooldownStore {
    fn cooling_until(&self, router: &str) -> Option<Instant> {
        self.cooling
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(router)
            .copied()
    }
}

/// Gate: the candidate's router must not be in an active connection cooldown.
/// Read-side only — nothing writes cooldowns yet (Task 3), so this always
/// admits against an empty [`ConnectionCooldownStore`].
pub struct ConnectionCooldownGate;

impl AdmissionGate for ConnectionCooldownGate {
    fn name(&self) -> &'static str {
        "connection_cooldown"
    }

    fn evaluate(&self, c: &CandidateView<'_>, x: &SelectionCtx<'_>) -> GateVerdict {
        match x.router_health.cooling_until(c.router) {
            Some(until) if until > x.now => GateVerdict::Skip(SkipReason::Cooling { until }),
            _ => GateVerdict::Admit,
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
    use std::time::Duration;

    #[test]
    fn store_reports_cooling_until_set() {
        let s = ConnectionCooldownStore::new();
        assert!(s.cooling_until("r").is_none()); // unknown → not cooling
        let until = Instant::now() + Duration::from_secs(60);
        s.start("r", until);
        assert_eq!(s.cooling_until("r"), Some(until)); // recorded
    }

    /// Fake endpoint health port returning None regardless of endpoint (not
    /// under test here; the ctx needs one to construct).
    struct FakeEndpointHealth;
    impl EndpointHealthRead for FakeEndpointHealth {
        fn open_until(&self, _endpoint: &str) -> Option<Instant> {
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

    /// Cooling router → `Skip(Cooling)`; not-cooling router → `Admit`;
    /// expired cooldown (`until` in the past relative to `now`) → `Admit`.
    /// One store + one ctx reused across scenarios (the store's interior
    /// mutability lets `start` change state between `evaluate` calls).
    #[test]
    fn gate_reads_cooldown_store() {
        let store = ConnectionCooldownStore::new();
        let now = Instant::now();

        let model_config = test_model_config();
        let router_config = test_router_config();
        let cand = CandidateView {
            model: "gemma3:27b",
            router: "r",
            endpoint: "r:gemma3:27b".to_string(),
            model_config: &model_config,
            router_config: &router_config,
        };
        let gateway_config = GatewayConfig::default();
        let endpoint_health = FakeEndpointHealth;
        let ctx = SelectionCtx {
            capability: Capability::TextChat,
            budget: None,
            input_tokens: None,
            health: &endpoint_health,
            now,
            config: &gateway_config,
            router_health: &store,
        };

        // Not cooling yet → Admit.
        assert!(matches!(
            ConnectionCooldownGate.evaluate(&cand, &ctx),
            GateVerdict::Admit
        ));

        // Cooling until after `now` → Skip(Cooling).
        store.start("r", now + Duration::from_secs(60));
        assert!(matches!(
            ConnectionCooldownGate.evaluate(&cand, &ctx),
            GateVerdict::Skip(SkipReason::Cooling { .. })
        ));

        // Cooldown expired (until before `now`) → Admit.
        store.start("r", now - Duration::from_secs(1));
        assert!(matches!(
            ConnectionCooldownGate.evaluate(&cand, &ctx),
            GateVerdict::Admit
        ));
    }
}
