use super::{
    AdmissionGate, AttemptOutcome, CandidateView, GateVerdict, HealthRecorder, RouterHealthRead,
    SelectionCtx,
};
use crate::skip_reason::SkipReason;
use crate::types::error::GatewayError;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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

/// Default router cooldown after a transport fault (operator-configurable via
/// ResilienceConfig in plan (f); a constant for now).
pub const DEFAULT_CONNECTION_COOLDOWN: Duration = Duration::from_secs(30);

/// Write side: on a transport-level fault (`Network`/`Timeout`), cool the
/// whole router for the configured cooldown duration so the (read-side) gate
/// skips all of its models on the next selection. Other errors do NOT cool —
/// they're provider/request faults, not evidence the router itself is
/// unreachable.
pub struct ConnectionCooldownSink {
    store: ConnectionCooldownStore,
    cooldown: Duration,
}

impl ConnectionCooldownSink {
    pub fn new(store: ConnectionCooldownStore, cooldown: Duration) -> Self {
        Self { store, cooldown }
    }
}

impl HealthRecorder for ConnectionCooldownSink {
    fn on_outcome(&self, o: &AttemptOutcome<'_>) {
        // Transport-level fault → cool the whole router (Network = connection failure;
        // Timeout = endpoint unreachable/too slow). Other errors do NOT cool.
        if matches!(
            o.error,
            Some(GatewayError::Network(_)) | Some(GatewayError::Timeout { .. })
        ) {
            self.store.start(o.router, Instant::now() + self.cooldown);
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

    /// Transport faults (`Timeout`) cool the router; a non-transport error
    /// (`ProviderError`) does NOT; neither does a successful outcome.
    #[test]
    fn sink_cools_only_on_transport_fault() {
        let store = ConnectionCooldownStore::new();
        let sink = ConnectionCooldownSink::new(store.clone(), Duration::from_secs(30));
        let now = Instant::now();

        let timeout_err = GatewayError::Timeout {
            adapter: "a".into(),
            model: "m".into(),
            duration_ms: 1,
        };
        sink.on_outcome(&AttemptOutcome {
            endpoint: "A:m",
            router: "A",
            success: false,
            error: Some(&timeout_err),
        });
        let until = store.cooling_until("A");
        assert!(until.is_some());
        assert!(until.unwrap() > now);

        let provider_err = GatewayError::ProviderError {
            adapter: "a".into(),
            message: "x".into(),
            status: Some(500),
        };
        sink.on_outcome(&AttemptOutcome {
            endpoint: "B:m",
            router: "B",
            success: false,
            error: Some(&provider_err),
        });
        assert!(store.cooling_until("B").is_none());

        sink.on_outcome(&AttemptOutcome {
            endpoint: "C:m",
            router: "C",
            success: true,
            error: None,
        });
        assert!(store.cooling_until("C").is_none());
    }
}
