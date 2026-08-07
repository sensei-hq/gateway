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
    /// Number of tracked routers (active + expired-but-not-yet-evicted).
    pub fn len(&self) -> usize {
        self.cooling.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
    /// Whether any router is tracked.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Over `cap` entries, drop those whose cooldown has already elapsed
    /// (`until <= now`). Active cooldowns are never dropped, so the cap is soft
    /// (bounded by concurrently-active routers). Prevents unbounded growth from
    /// many short-lived routers.
    pub fn evict_expired_over_cap(&self, cap: usize) {
        let mut m = self.cooling.lock().unwrap_or_else(|e| e.into_inner());
        if m.len() <= cap {
            return;
        }
        let now = Instant::now();
        m.retain(|_, until| *until > now);
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
    eviction_cap: usize,
    /// Deterministic per-router jitter fraction added to the SYNTHETIC cooldown
    /// to spread retry storms (Task 4). `0.0` ⇒ off (exactly `now + cooldown`).
    jitter_fraction: f64,
}

impl ConnectionCooldownSink {
    pub fn new(
        store: ConnectionCooldownStore,
        cooldown: Duration,
        eviction_cap: usize,
        jitter_fraction: f64,
    ) -> Self {
        Self {
            store,
            cooldown,
            eviction_cap,
            jitter_fraction,
        }
    }
}

impl HealthRecorder for ConnectionCooldownSink {
    fn on_outcome(&self, o: &AttemptOutcome<'_>) -> Option<Instant> {
        // Transport-level fault → cool the whole router (Network = connection failure;
        // Timeout = endpoint unreachable/too slow). Other errors do NOT cool.
        if matches!(
            o.error,
            Some(GatewayError::Network(_)) | Some(GatewayError::Timeout { .. })
        ) {
            // Deterministic per-router jitter spreads simultaneous router faults
            // across the cooldown window (`jitter_fraction = 0.0` ⇒ exactly
            // `now + cooldown`, today's behavior).
            let until = Instant::now()
                + self.cooldown
                + crate::resilience::deterministic_jitter(
                    o.router,
                    self.cooldown,
                    self.jitter_fraction,
                );
            self.store.start(o.router, until);
            // Bound the map on the write path only (reads/successes never grow it).
            self.store.evict_expired_over_cap(self.eviction_cap);
            Some(until)
        } else {
            None
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
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let ctx = SelectionCtx {
            capability: Capability::TextChat,
            budget: None,
            input_tokens: None,
            health: &endpoint_health,
            now,
            config: &gateway_config,
            router_health: &store,
            model_lockout: &lockout,
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
        let sink = ConnectionCooldownSink::new(store.clone(), Duration::from_secs(30), 4096, 0.0);
        let now = Instant::now();

        let timeout_err = GatewayError::Timeout {
            adapter: "a".into(),
            model: "m".into(),
            duration_ms: 1,
        };
        let returned = sink.on_outcome(&AttemptOutcome {
            endpoint: "A:m",
            router: "A",
            success: false,
            error: Some(&timeout_err),
        });
        // The sink returns the `until` it just wrote — a future instant equal to
        // what the store now reports.
        assert!(returned.is_some());
        assert!(returned.unwrap() > now);
        assert_eq!(returned, store.cooling_until("A"));

        let provider_err = GatewayError::ProviderError {
            adapter: "a".into(),
            message: "x".into(),
            status: Some(500),
        };
        let returned = sink.on_outcome(&AttemptOutcome {
            endpoint: "B:m",
            router: "B",
            success: false,
            error: Some(&provider_err),
        });
        assert!(returned.is_none(), "non-transport error does not cool");
        assert!(store.cooling_until("B").is_none());

        let returned = sink.on_outcome(&AttemptOutcome {
            endpoint: "C:m",
            router: "C",
            success: true,
            error: None,
        });
        assert!(returned.is_none(), "success does not cool");
        assert!(store.cooling_until("C").is_none());
    }

    /// Over `cap`, EXPIRED cooldowns are dropped but ACTIVE ones are never
    /// dropped — the load-bearing invariant. 3 expired + 2 active, cap = 2.
    #[test]
    fn evicts_expired_over_cap_keeps_active() {
        let s = ConnectionCooldownStore::new();
        let now = Instant::now();
        // 3 expired + 2 active, cap = 2
        s.start("expired1", now - Duration::from_secs(1));
        s.start("expired2", now - Duration::from_secs(1));
        s.start("expired3", now - Duration::from_secs(1));
        s.start("active1", now + Duration::from_secs(60));
        s.start("active2", now + Duration::from_secs(60));
        s.evict_expired_over_cap(2);
        // expired gone, active kept
        assert!(s.cooling_until("expired1").is_none());
        assert!(s.cooling_until("expired2").is_none());
        assert!(s.cooling_until("expired3").is_none());
        assert!(s.cooling_until("active1").is_some());
        assert!(s.cooling_until("active2").is_some());
    }

    /// At/below the cap, nothing is evicted — even an expired entry is kept.
    #[test]
    fn evict_no_op_when_at_or_below_cap() {
        let s = ConnectionCooldownStore::new();
        let now = Instant::now();
        s.start("a", now - Duration::from_secs(1)); // expired but under cap → kept
        s.evict_expired_over_cap(4096);
        assert!(s.cooling_until("a").is_some());
    }

    /// On the WRITE path, a tiny `eviction_cap` prunes expired entries while the
    /// active cooldown (and the just-written one) survive; the map stays bounded.
    #[test]
    fn sink_evicts_expired_over_cap_on_write_keeps_active() {
        let store = ConnectionCooldownStore::new();
        let now = Instant::now();
        // Pre-seed two expired entries and one active entry.
        store.start("expired1", now - Duration::from_secs(1));
        store.start("expired2", now - Duration::from_secs(1));
        store.start("active", now + Duration::from_secs(60));
        // Tiny cap so the next write trips eviction.
        let sink = ConnectionCooldownSink::new(store.clone(), Duration::from_secs(30), 1, 0.0);

        let timeout_err = GatewayError::Timeout {
            adapter: "a".into(),
            model: "m".into(),
            duration_ms: 1,
        };
        // Transport fault on a NEW router → writes "new" then evicts expired over cap.
        sink.on_outcome(&AttemptOutcome {
            endpoint: "new:m",
            router: "new",
            success: false,
            error: Some(&timeout_err),
        });

        // Expired pruned; the active cooldown and just-written one survive.
        assert!(store.cooling_until("expired1").is_none());
        assert!(store.cooling_until("expired2").is_none());
        assert!(
            store.cooling_until("active").is_some(),
            "active cooldown never evicted"
        );
        assert!(
            store.cooling_until("new").is_some(),
            "just-written cooldown present"
        );
        // Map bounded to the non-expired entries (active + new).
        assert!(store.len() <= 2);
    }

    /// A transport fault carried by every scenario here.
    fn timeout_err() -> GatewayError {
        GatewayError::Timeout {
            adapter: "a".into(),
            model: "m".into(),
            duration_ms: 1,
        }
    }

    /// With `jitter_fraction = 0.0` (the default) the cooldown deadline is exactly
    /// `now + base` — behavior-preserving, NOT widened. Asserts `until` sits in a
    /// tiny scheduling window at `now + base`, never larger (a stray jitter would
    /// push it past `now + base`).
    #[test]
    fn sink_no_jitter_when_fraction_zero() {
        let store = ConnectionCooldownStore::new();
        let base = Duration::from_secs(60);
        let sink = ConnectionCooldownSink::new(store.clone(), base, 4096, 0.0);
        let now = Instant::now();
        let err = timeout_err();
        let until = sink
            .on_outcome(&AttemptOutcome {
                endpoint: "R:m",
                router: "R",
                success: false,
                error: Some(&err),
            })
            .expect("transport fault cools");
        assert!(until >= now + base, "at least the full base cooldown");
        assert!(
            until < now + base + Duration::from_secs(1),
            "exactly base, no jitter (within a tiny scheduling margin)"
        );
    }

    /// With `jitter_fraction > 0` the cooldown deadline is `now + base +
    /// jitter(router)`, equal to the deterministic per-router offset (flake-free)
    /// and within `[now+base, now+base*1.5)`.
    #[test]
    fn sink_applies_deterministic_jitter_when_fraction_positive() {
        let store = ConnectionCooldownStore::new();
        let base = Duration::from_secs(1000); // large base ⇒ wide 500s jitter span
        let sink = ConnectionCooldownSink::new(store.clone(), base, 4096, 0.5);
        let now = Instant::now();
        let err = timeout_err();
        let until = sink
            .on_outcome(&AttemptOutcome {
                endpoint: "R:m",
                router: "R",
                success: false,
                error: Some(&err),
            })
            .expect("transport fault cools");
        let jitter = crate::resilience::deterministic_jitter("R", base, 0.5);
        // Deadline carries exactly the deterministic per-router jitter (± margin).
        assert!(
            until >= now + base + jitter,
            "carries the deterministic per-router jitter"
        );
        assert!(
            until < now + base + jitter + Duration::from_secs(1),
            "no more than base + deterministic jitter"
        );
        // Within [now+base, now+base*1.5).
        assert!(until >= now + base);
        assert!(until < now + base + base / 2);
        // Deterministic: the sink wrote exactly this deadline to the store.
        assert_eq!(store.cooling_until("R"), Some(until));
    }
}
