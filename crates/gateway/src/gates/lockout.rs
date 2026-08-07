use crate::types::error::GatewayError;

/// Why a `router:model` is (or should be) locked out. **Provider-side only** —
/// distinct from the caller's subscription `GatewayError::QuotaExceeded`, which
/// is a subject hard-stop and never produces a lockout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockReason {
    /// 429 — recovers fast (short cooldown, honoring Retry-After if present).
    RateLimit,
    /// Provider quota for this model exhausted (403 + quota body) — recovers at a
    /// reset boundary; demotes to the next tier.
    QuotaExhausted,
    /// Out of credits / billing (403 credit body, or 402) — terminal until a
    /// human tops up.
    CreditsExhausted,
    /// 401, or a bare 403 forbidden — terminal until the credential changes.
    Auth,
}

impl LockReason {
    /// Recoverable reasons demote-and-retry (the model comes back). Terminal
    /// reasons need human action (top-up / rotate key) and carry `until: None`.
    pub fn is_recoverable(&self) -> bool {
        matches!(self, LockReason::RateLimit | LockReason::QuotaExhausted)
    }
    pub fn is_terminal(&self) -> bool {
        !self.is_recoverable()
    }
}

/// Classify a provider error into a lockout reason, or `None` when it is not a
/// provider limit signal. The single source of truth shared by the (future)
/// in-flight walk and the next-request `ModelLockoutSink` — they cannot disagree.
/// Reads the boundary-enriched error (Task 1): 403 preserves status+body; 429
/// carries retry-after.
pub fn classify(err: &GatewayError) -> Option<LockReason> {
    match err {
        GatewayError::RateLimit { .. } => Some(LockReason::RateLimit),
        GatewayError::Authentication { .. } => Some(LockReason::Auth), // 401
        GatewayError::ProviderError {
            status: Some(402), ..
        } => Some(LockReason::CreditsExhausted),
        GatewayError::ProviderError {
            status: Some(403),
            message,
            ..
        } => Some(classify_403_body(message)),
        _ => None,
    }
}

/// A 403 is ambiguous: quota vs credits vs a plain forbidden. Disambiguate by
/// body keywords (providers throttle via non-standard 403 bodies). Order
/// matters: credits/billing before quota, since "you have exceeded your credit
/// limit" contains both senses and the billing sense is terminal.
fn classify_403_body(message: &str) -> LockReason {
    let m = message.to_ascii_lowercase();
    if m.contains("credit")
        || m.contains("billing")
        || m.contains("payment")
        || m.contains("insufficient")
    {
        LockReason::CreditsExhausted
    } else if m.contains("quota")
        || m.contains("exceed")
        || m.contains("exhaust")
        || m.contains("rate limit")
    {
        LockReason::QuotaExhausted
    } else {
        LockReason::Auth // bare forbidden → terminal (preserves pre-(d) 403 semantics)
    }
}

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// A single endpoint's lockout state; `until = None` is terminal. Task 5 (the
/// sink) re-adds an `escalation` generation counter here alongside its reader.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LockEntry {
    pub reason: LockReason,
    pub until: Option<Instant>,
}

/// What the gate reads for a candidate: the active lock's reason + deadline.
#[derive(Debug, Clone, Copy)]
pub struct LockView {
    pub reason: LockReason,
    pub until: Option<Instant>, // None = terminal
}

/// Read port for endpoint model-lockout state (read by the gate, later task).
pub trait ModelLockoutRead: Send + Sync {
    /// The endpoint's lock entry (reason + deadline), or `None` if not tracked.
    /// Expiry is NOT applied here — the gate compares `until` to its injected
    /// `now` (mirrors `RouterHealthRead::cooling_until`), and the entry is
    /// retained past expiry so escalation memory survives a release.
    fn locked(&self, endpoint: &str) -> Option<LockView>;
}

/// In-memory per-endpoint (`"router:model"`) lockout state, Arc-backed + Clone so
/// the gate's read reference, the sink's owned copy, and `Gateway`'s apply/clear
/// share one map. Same pattern as `ConnectionCooldownStore`.
#[derive(Clone, Default)]
pub struct ModelLockoutStore {
    locks: Arc<Mutex<HashMap<String, LockEntry>>>,
}

impl ModelLockoutStore {
    pub fn new() -> Self {
        Self::default()
    }
    /// Insert/replace the endpoint's lock (used by the sink and `apply_lockout`).
    pub fn set(&self, endpoint: &str, reason: LockReason, until: Option<Instant>) {
        self.locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(endpoint.to_string(), LockEntry { reason, until });
    }
    /// Remove the endpoint's lock (success / `clear_lockout`).
    pub fn clear(&self, endpoint: &str) {
        self.locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(endpoint);
    }
}

impl ModelLockoutRead for ModelLockoutStore {
    fn locked(&self, endpoint: &str) -> Option<LockView> {
        self.locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(endpoint)
            .map(|e| LockView {
                reason: e.reason,
                until: e.until,
            })
    }
}

use super::{AdmissionGate, CandidateView, GateVerdict, SelectionCtx};
use crate::skip_reason::SkipReason;

/// Gate: the candidate's `router:model` must not be under an active lockout —
/// terminal (`until = None`) or timed (`until > now`). An expired timed lock
/// admits (the entry lingers only for the sink's escalation memory, later task).
pub struct ModelLockoutGate;

impl AdmissionGate for ModelLockoutGate {
    fn name(&self) -> &'static str {
        "model_lockout"
    }
    fn evaluate(&self, c: &CandidateView<'_>, x: &SelectionCtx<'_>) -> GateVerdict {
        match x.model_lockout.locked(&c.endpoint) {
            Some(v) => match v.until {
                None => GateVerdict::Skip(SkipReason::LockedOut {
                    reason: v.reason,
                    until: None,
                }),
                Some(until) if until > x.now => GateVerdict::Skip(SkipReason::LockedOut {
                    reason: v.reason,
                    until: Some(until),
                }),
                _ => GateVerdict::Admit, // expired timed lock
            },
            None => GateVerdict::Admit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gates::EndpointHealthRead;
    use crate::gates::cooldown::ConnectionCooldownStore;
    use crate::types::capability::Capability;
    use crate::types::config::{GatewayConfig, ModelConfig, RouterConfig};
    use crate::types::error::GatewayError;
    use std::time::Duration;

    fn provider(status: u16, msg: &str) -> GatewayError {
        GatewayError::ProviderError {
            adapter: "a".into(),
            message: msg.into(),
            status: Some(status),
        }
    }

    #[test]
    fn classify_maps_provider_limits() {
        let rl = GatewayError::RateLimit {
            adapter: "a".into(),
            retry_after_ms: Some(1000),
        };
        assert_eq!(classify(&rl), Some(LockReason::RateLimit));
        assert!(LockReason::RateLimit.is_recoverable());

        assert_eq!(
            classify(&provider(403, "You exceeded your quota")),
            Some(LockReason::QuotaExhausted)
        );
        assert!(LockReason::QuotaExhausted.is_recoverable());

        assert_eq!(
            classify(&provider(403, "insufficient credits, please add billing")),
            Some(LockReason::CreditsExhausted)
        );
        assert!(LockReason::CreditsExhausted.is_terminal());

        assert_eq!(
            classify(&provider(402, "payment required")),
            Some(LockReason::CreditsExhausted)
        );

        assert_eq!(
            classify(&provider(403, "forbidden")),
            Some(LockReason::Auth)
        );

        let auth = GatewayError::Authentication {
            adapter: "a".into(),
            message: "bad key".into(),
        };
        assert_eq!(classify(&auth), Some(LockReason::Auth));
        assert!(LockReason::Auth.is_terminal());

        assert_eq!(classify(&provider(500, "boom")), None);
        assert_eq!(
            classify(&GatewayError::Timeout {
                adapter: "a".into(),
                model: "m".into(),
                duration_ms: 1
            }),
            None
        );
        assert_eq!(
            classify(&GatewayError::ModelUnavailable {
                adapter: "a".into(),
                model: "m".into()
            }),
            None
        );
    }

    #[test]
    fn store_records_and_reads_locks() {
        let s = ModelLockoutStore::new();
        assert!(s.locked("r:m").is_none()); // unknown → not locked
        let until = std::time::Instant::now() + std::time::Duration::from_secs(60);
        s.set("r:m", LockReason::RateLimit, Some(until));
        let v = s.locked("r:m").expect("locked");
        assert_eq!(v.reason, LockReason::RateLimit);
        assert_eq!(v.until, Some(until));
        // terminal lock: until = None
        s.set("r:x", LockReason::Auth, None);
        assert_eq!(s.locked("r:x").unwrap().until, None);
        // clear removes it
        s.clear("r:m");
        assert!(s.locked("r:m").is_none());
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
            id: "m".to_string(),
            api_model_id: None,
            provider: "r".to_string(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window: 128000,
            max_output_tokens: 8192,
            pricing: None,
        }
    }

    fn test_router_config() -> RouterConfig {
        RouterConfig {
            url: "http://localhost".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        }
    }

    /// Unknown endpoint → `Admit`; terminal lock (`until = None`) →
    /// `Skip(LockedOut { until: None })`; timed lock in the future →
    /// `Skip(LockedOut { until: Some(_) })`; expired timed lock → `Admit`.
    /// One store + one ctx reused across scenarios (the store's interior
    /// mutability lets `set` change state between `evaluate` calls).
    #[test]
    fn gate_reads_lockout_store() {
        let store = ModelLockoutStore::new();
        let now = Instant::now();

        let model_config = test_model_config();
        let router_config = test_router_config();
        let cand = CandidateView {
            model: "m",
            router: "r",
            endpoint: "r:m".to_string(),
            model_config: &model_config,
            router_config: &router_config,
        };
        let gateway_config = GatewayConfig::default();
        let endpoint_health = FakeEndpointHealth;
        let router_health = ConnectionCooldownStore::new();
        let ctx = SelectionCtx {
            capability: Capability::TextChat,
            budget: None,
            input_tokens: None,
            health: &endpoint_health,
            now,
            config: &gateway_config,
            router_health: &router_health,
            model_lockout: &store,
        };

        // Unknown endpoint → Admit.
        assert!(matches!(
            ModelLockoutGate.evaluate(&cand, &ctx),
            GateVerdict::Admit
        ));

        // Terminal lock (until = None) → Skip(LockedOut { until: None }).
        store.set("r:m", LockReason::Auth, None);
        assert!(matches!(
            ModelLockoutGate.evaluate(&cand, &ctx),
            GateVerdict::Skip(SkipReason::LockedOut { until: None, .. })
        ));

        // Timed lock in the future → Skip(LockedOut { until: Some(_) }).
        store.set(
            "r:m",
            LockReason::RateLimit,
            Some(now + Duration::from_secs(60)),
        );
        assert!(matches!(
            ModelLockoutGate.evaluate(&cand, &ctx),
            GateVerdict::Skip(SkipReason::LockedOut { until: Some(_), .. })
        ));

        // Expired timed lock (until before now) → Admit.
        store.set(
            "r:m",
            LockReason::RateLimit,
            Some(now - Duration::from_secs(1)),
        );
        assert!(matches!(
            ModelLockoutGate.evaluate(&cand, &ctx),
            GateVerdict::Admit
        ));
    }
}
