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
use std::time::{Duration, Instant};

/// A single endpoint's lockout state; `until = None` is terminal. Carries an
/// `escalation` generation counter the sink reads to grow the backoff window
/// once per lock→release→relock cycle (not per concurrent failure).
#[derive(Debug, Clone, Copy)]
pub(crate) struct LockEntry {
    pub reason: LockReason,
    pub until: Option<Instant>,
    pub escalation: u32,
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
    pub fn set(&self, endpoint: &str, reason: LockReason, until: Option<Instant>, escalation: u32) {
        self.locks.lock().unwrap_or_else(|e| e.into_inner()).insert(
            endpoint.to_string(),
            LockEntry {
                reason,
                until,
                escalation,
            },
        );
    }
    /// The endpoint's raw lock entry (reason + deadline + escalation), or `None`.
    /// Read by the sink to decide whether a fresh failure escalates the window.
    pub(crate) fn get(&self, endpoint: &str) -> Option<LockEntry> {
        self.locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(endpoint)
            .copied()
    }
    /// Remove the endpoint's lock (success / `clear_lockout`).
    pub fn clear(&self, endpoint: &str) {
        self.locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(endpoint);
    }
    /// Clear terminal (`Auth`/`Credits`) locks on all of `router`'s endpoints
    /// (keys `"{router}:*"`). Used after `refresh_router_keys` — a new credential
    /// may fix an auth/credits lock. Timed locks (rate/quota) are left intact.
    pub fn clear_terminal_for_router(&self, router: &str) {
        let prefix = format!("{router}:");
        self.locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|k, e| !(k.starts_with(&prefix) && e.reason.is_terminal()));
    }
    /// Number of tracked endpoints (active + terminal + expired-not-yet-evicted).
    pub fn len(&self) -> usize {
        self.locks.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
    /// Whether any endpoint is tracked.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Over `cap`, drop expired TIMED locks (`Some(u)` with `u <= now`). Active
    /// timed locks and TERMINAL locks (`until: None`) are never dropped — the
    /// design's "never evict mid-lock". Prevents unbounded growth from many
    /// short-lived endpoints.
    pub fn evict_expired_over_cap(&self, cap: usize) {
        let mut m = self.locks.lock().unwrap_or_else(|e| e.into_inner());
        if m.len() <= cap {
            return;
        }
        let now = Instant::now();
        m.retain(|_, e| match e.until {
            Some(u) => u > now, // active timed → keep; expired timed → drop
            None => true,       // terminal → always keep
        });
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

use super::{AttemptOutcome, HealthRecorder};

/// Best-effort, isolated observer of gateway health decisions. The gateway
/// **announces** a lockout; the caller **persists** it (the tenant-agnostic core
/// never persists — design §5c). The `Gateway::with_observer` wiring lands in
/// Task 6. More methods arrive with later observability work.
pub trait SelectionObserver: Send + Sync {
    /// `until = None` ⇒ terminal (surface a human-action hint — top-up / rotate
    /// key — not a wake-up time).
    fn on_lockout(&self, endpoint: &str, reason: LockReason, until: Option<Instant>);
}

/// Arc-backed, Clone registry shared between `Gateway` (registers via
/// `with_observer`, Task 6) and the sink (fires). Empty until an observer is
/// registered. Same shared-handle pattern as `ModelLockoutStore`.
#[derive(Clone, Default)]
pub struct LockoutBroadcaster {
    observers: Arc<Mutex<Vec<Arc<dyn SelectionObserver>>>>,
}

impl LockoutBroadcaster {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn register(&self, obs: Arc<dyn SelectionObserver>) {
        self.observers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(obs);
    }
    /// Fire best-effort. Clone the handles out before calling so an observer
    /// can't deadlock by re-entering the registry.
    pub fn fire(&self, endpoint: &str, reason: LockReason, until: Option<Instant>) {
        let observers = self
            .observers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        for o in observers {
            o.on_lockout(endpoint, reason, until);
        }
    }
}

/// Per-reason lockout durations. Operator-configurable via `ResilienceConfig` in
/// plan (f); defaults here. (Quota uses a fixed default; the calendar-clock
/// reset-boundary refinement is plan (e).)
#[derive(Debug, Clone)]
pub struct ModelLockoutPolicy {
    pub rate_limit_base: Duration,
    pub quota_default: Duration,
    pub max_cooldown: Duration,
}

impl Default for ModelLockoutPolicy {
    fn default() -> Self {
        Self {
            rate_limit_base: Duration::from_secs(60),
            quota_default: Duration::from_secs(3600),
            max_cooldown: Duration::from_secs(6 * 3600),
        }
    }
}

/// Write side: classify a failed outcome and lock the endpoint accordingly; a
/// success clears it. Terminal reasons (`Auth`/`Credits`) lock with `until: None`.
/// Timed reasons escalate **once per lock→release→relock cycle** (not per
/// concurrent failure): escalation grows only when the prior lock had already
/// expired. A real `Retry-After` is honored exactly (never clamped); synthetic
/// backoff is clamped to `max_cooldown`.
pub struct ModelLockoutSink {
    store: ModelLockoutStore,
    policy: ModelLockoutPolicy,
    observers: LockoutBroadcaster,
    eviction_cap: usize,
}

impl ModelLockoutSink {
    pub fn new(
        store: ModelLockoutStore,
        policy: ModelLockoutPolicy,
        observers: LockoutBroadcaster,
        eviction_cap: usize,
    ) -> Self {
        Self {
            store,
            policy,
            observers,
            eviction_cap,
        }
    }

    /// Compute the lock deadline. Terminal → `None`. A real retry-after is exact
    /// (never clamped). Otherwise synthetic backoff = base * 2^escalation, clamped.
    fn deadline(
        &self,
        reason: LockReason,
        retry_after: Option<Duration>,
        escalation: u32,
        now: Instant,
    ) -> Option<Instant> {
        if reason.is_terminal() {
            return None;
        }
        if let Some(exact) = retry_after {
            return Some(now + exact); // honored verbatim, never clamped
        }
        let base = match reason {
            LockReason::RateLimit => self.policy.rate_limit_base,
            LockReason::QuotaExhausted => self.policy.quota_default,
            _ => unreachable!("terminal handled above"),
        };
        let factor = 1u32.checked_shl(escalation.min(16)).unwrap_or(u32::MAX);
        let backoff = base.saturating_mul(factor).min(self.policy.max_cooldown);
        Some(now + backoff)
    }
}

impl HealthRecorder for ModelLockoutSink {
    fn on_outcome(&self, o: &AttemptOutcome<'_>) -> Option<Instant> {
        if o.success {
            self.store.clear(o.endpoint); // success clears lock + escalation
            return None;
        }
        let err = o.error?; // no error carried → nothing to lock
        let reason = classify(err)?; // not a provider-limit signal → no lock

        let now = Instant::now();
        let prior = self.store.get(o.endpoint);
        // Generation guard: escalate only on a genuine relock (prior lock expired),
        // not on a concurrent failure during an active lock.
        let escalation = match prior {
            Some(p) if matches!(p.until, Some(u) if u <= now) => p.escalation.saturating_add(1),
            Some(p) => p.escalation, // still-active or terminal prior → keep
            None => 0,
        };

        let until = self.deadline(reason, retry_after(err), escalation, now);
        self.store.set(o.endpoint, reason, until, escalation);
        // Bound the map on the write path only (a success `clear`s and shrinks).
        self.store.evict_expired_over_cap(self.eviction_cap);
        self.observers.fire(o.endpoint, reason, until);
        // `Some(deadline)` for a timed lock, `None` for a terminal lock — exactly
        // the recorder contract (a terminal lock has no wake-up instant).
        until
    }
}

/// A real `Retry-After` (429 only, here) → exact duration; otherwise `None`.
fn retry_after(err: &GatewayError) -> Option<Duration> {
    match err {
        GatewayError::RateLimit {
            retry_after_ms: Some(ms),
            ..
        } => Some(Duration::from_millis(*ms)),
        _ => None,
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
        s.set("r:m", LockReason::RateLimit, Some(until), 0);
        let v = s.locked("r:m").expect("locked");
        assert_eq!(v.reason, LockReason::RateLimit);
        assert_eq!(v.until, Some(until));
        // terminal lock: until = None
        s.set("r:x", LockReason::Auth, None, 0);
        assert_eq!(s.locked("r:x").unwrap().until, None);
        // clear removes it
        s.clear("r:m");
        assert!(s.locked("r:m").is_none());
    }

    /// `clear_terminal_for_router` is both terminal-only AND router-scoped:
    /// with a terminal and a timed lock on the SAME router, only the terminal
    /// one is removed (proving the `is_terminal()` predicate, not mere router
    /// scoping); a terminal lock on a different router is left untouched.
    #[test]
    fn clear_terminal_for_router_removes_only_terminal_on_that_router() {
        let s = ModelLockoutStore::new();
        let future = std::time::Instant::now() + std::time::Duration::from_secs(60);
        s.set("r1:auth-model", LockReason::Auth, None, 0); // terminal, r1
        s.set(
            "r1:quota-model",
            LockReason::QuotaExhausted,
            Some(future),
            0,
        ); // timed, r1
        s.set("r2:auth-model", LockReason::Auth, None, 0); // terminal, r2 (untouched)
        s.clear_terminal_for_router("r1");
        assert!(
            s.locked("r1:auth-model").is_none(),
            "terminal on r1 cleared"
        );
        assert!(s.locked("r1:quota-model").is_some(), "timed on r1 survives");
        assert!(
            s.locked("r2:auth-model").is_some(),
            "other router untouched"
        );
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
        store.set("r:m", LockReason::Auth, None, 0);
        assert!(matches!(
            ModelLockoutGate.evaluate(&cand, &ctx),
            GateVerdict::Skip(SkipReason::LockedOut { until: None, .. })
        ));

        // Timed lock in the future → Skip(LockedOut { until: Some(_) }).
        store.set(
            "r:m",
            LockReason::RateLimit,
            Some(now + Duration::from_secs(60)),
            0,
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
            0,
        );
        assert!(matches!(
            ModelLockoutGate.evaluate(&cand, &ctx),
            GateVerdict::Admit
        ));
    }

    // ---- ModelLockoutSink (write side) --------------------------------------

    /// Fresh store + a default-policy sink sharing it, and no observers.
    fn sink_with_default() -> (ModelLockoutStore, ModelLockoutSink) {
        let store = ModelLockoutStore::new();
        let sink = ModelLockoutSink::new(
            store.clone(),
            ModelLockoutPolicy::default(),
            LockoutBroadcaster::new(),
            4096,
        );
        (store, sink)
    }

    /// A failed attempt outcome on endpoint `"r:m"` carrying `err`.
    fn fail(err: &GatewayError) -> AttemptOutcome<'_> {
        AttemptOutcome {
            endpoint: "r:m",
            router: "r",
            success: false,
            error: Some(err),
        }
    }

    /// A real `Retry-After` (429) is honored verbatim (~2s), NOT clamped up to
    /// the 60s base.
    #[test]
    fn sink_honors_retry_after_exactly() {
        let (store, sink) = sink_with_default();
        let now = Instant::now();
        let err = GatewayError::RateLimit {
            adapter: "a".into(),
            retry_after_ms: Some(2000),
        };
        let returned = sink.on_outcome(&fail(&err));

        let v = store.locked("r:m").expect("locked");
        assert_eq!(v.reason, LockReason::RateLimit);
        let until = v.until.expect("timed lock");
        // The sink returns the timed deadline it just wrote — equal to `store.get`.
        assert_eq!(returned, Some(until));
        assert!(
            until > now + Duration::from_millis(1500),
            "retry-after honored (>1.5s)"
        );
        assert!(
            until < now + Duration::from_secs(3),
            "retry-after exact, not clamped up to the 60s base (<3s)"
        );
    }

    /// No retry-after → base ~60s at escalation 0; a genuine relock after the
    /// prior lock expired escalates to a strictly-longer window (~120s), never
    /// exceeding `max_cooldown`.
    #[test]
    fn sink_times_lock_at_base_then_escalates_on_relock() {
        let (store, sink) = sink_with_default();
        let now = Instant::now();
        let err = GatewayError::RateLimit {
            adapter: "a".into(),
            retry_after_ms: None,
        };

        sink.on_outcome(&fail(&err));
        let first = store.locked("r:m").expect("locked").until.expect("timed");
        assert!(first > now + Duration::from_secs(55), "base ~60s (>55s)");
        assert!(first < now + Duration::from_secs(70), "base ~60s (<70s)");
        assert_eq!(store.get("r:m").unwrap().escalation, 0);

        // Pre-seed an EXPIRED lock so the next failure is a genuine relock.
        store.set(
            "r:m",
            LockReason::RateLimit,
            Some(now - Duration::from_secs(1)),
            0,
        );
        sink.on_outcome(&fail(&err));

        let entry = store.get("r:m").unwrap();
        assert_eq!(entry.escalation, 1, "relock after expiry escalates once");
        let second = entry.until.expect("timed");
        assert!(
            second > now + Duration::from_secs(90),
            "escalated window strictly longer than the base (>90s)"
        );
        assert!(
            second <= now + ModelLockoutPolicy::default().max_cooldown,
            "escalated window never exceeds max_cooldown"
        );
    }

    /// A 403 quota body → timed lock at the ~1h quota default.
    #[test]
    fn sink_locks_quota_at_default() {
        let (store, sink) = sink_with_default();
        let now = Instant::now();
        let err = provider(403, "You have exceeded your quota for this model");
        let returned = sink.on_outcome(&fail(&err));

        let v = store.locked("r:m").expect("locked");
        assert_eq!(v.reason, LockReason::QuotaExhausted);
        let until = v.until.expect("timed lock");
        // The sink returns the timed deadline it just wrote — equal to `store.get`.
        assert_eq!(returned, Some(until));
        assert!(
            until > now + Duration::from_secs(3000),
            "quota ~1h (>3000s)"
        );
        assert!(
            until < now + Duration::from_secs(4000),
            "quota ~1h (<4000s)"
        );
    }

    /// 401 and a 403-credits body → terminal locks with `until: None`.
    #[test]
    fn sink_locks_terminal_reasons_with_no_deadline() {
        let (store, sink) = sink_with_default();

        let auth = GatewayError::Authentication {
            adapter: "a".into(),
            message: "bad key".into(),
        };
        let returned = sink.on_outcome(&fail(&auth));
        assert_eq!(returned, None, "terminal lock has no wake-up instant");
        let v = store.locked("r:m").expect("locked");
        assert_eq!(v.reason, LockReason::Auth);
        assert_eq!(v.until, None, "terminal → no wake-up time");

        let credits = provider(403, "insufficient credits, please add billing");
        let returned = sink.on_outcome(&fail(&credits));
        assert_eq!(returned, None, "terminal lock has no wake-up instant");
        let v = store.locked("r:m").expect("locked");
        assert_eq!(v.reason, LockReason::CreditsExhausted);
        assert_eq!(v.until, None, "terminal → no wake-up time");
    }

    /// A successful outcome clears the endpoint's lock (and escalation).
    #[test]
    fn sink_success_clears_lock() {
        let (store, sink) = sink_with_default();
        let now = Instant::now();
        store.set(
            "r:m",
            LockReason::RateLimit,
            Some(now + Duration::from_secs(60)),
            3,
        );
        let returned = sink.on_outcome(&AttemptOutcome {
            endpoint: "r:m",
            router: "r",
            success: true,
            error: None,
        });
        assert_eq!(returned, None, "success returns no wake-up instant");
        assert!(
            store.locked("r:m").is_none(),
            "success clears lock + escalation"
        );
    }

    /// Non-limit faults (500 `ProviderError`, `Timeout`) never lock.
    #[test]
    fn sink_ignores_non_limit_faults() {
        let (store, sink) = sink_with_default();

        let returned = sink.on_outcome(&fail(&provider(500, "boom")));
        assert_eq!(returned, None, "non-limit fault returns no wake-up instant");
        assert!(store.locked("r:m").is_none(), "500 is not a provider limit");

        let timeout = GatewayError::Timeout {
            adapter: "a".into(),
            model: "m".into(),
            duration_ms: 1,
        };
        let returned = sink.on_outcome(&fail(&timeout));
        assert_eq!(returned, None, "non-limit fault returns no wake-up instant");
        assert!(
            store.locked("r:m").is_none(),
            "timeout is a transport fault, not a provider limit"
        );
    }

    /// A 403-quota over an already-active rate-limit lock upgrades the reason.
    #[test]
    fn sink_403_quota_upgrades_active_rate_limit() {
        let (store, sink) = sink_with_default();
        let now = Instant::now();
        store.set(
            "r:m",
            LockReason::RateLimit,
            Some(now + Duration::from_secs(60)),
            0,
        );
        sink.on_outcome(&fail(&provider(403, "quota exceeded")));
        assert_eq!(
            store.locked("r:m").unwrap().reason,
            LockReason::QuotaExhausted,
            "403-quota upgrades the active rate-limit lock"
        );
    }

    /// Over `cap`, expired TIMED locks are evicted; a TERMINAL lock (`until:
    /// None`) and an ACTIVE timed lock (`until` future) are NEVER evicted — the
    /// load-bearing "never evict mid-lock" invariant.
    #[test]
    fn evicts_expired_timed_locks_keeps_terminal_and_active() {
        let s = ModelLockoutStore::new();
        let now = Instant::now();
        s.set(
            "expired",
            LockReason::RateLimit,
            Some(now - Duration::from_secs(1)),
            0,
        );
        s.set("terminal", LockReason::Auth, None, 0);
        s.set(
            "active",
            LockReason::QuotaExhausted,
            Some(now + Duration::from_secs(60)),
            0,
        );
        // add filler expired entries to exceed a small cap
        for i in 0..5 {
            s.set(
                &format!("e{i}"),
                LockReason::RateLimit,
                Some(now - Duration::from_secs(1)),
                0,
            );
        }
        s.evict_expired_over_cap(2);
        assert!(s.locked("expired").is_none());
        assert!(s.locked("terminal").is_some(), "terminal never evicted");
        assert!(s.locked("active").is_some(), "active never evicted");
    }

    /// At/below the cap, nothing is evicted — even an expired timed lock is kept.
    #[test]
    fn lockout_evict_no_op_when_at_or_below_cap() {
        let s = ModelLockoutStore::new();
        let now = Instant::now();
        s.set(
            "expired",
            LockReason::RateLimit,
            Some(now - Duration::from_secs(1)),
            0,
        );
        s.evict_expired_over_cap(4096);
        assert!(s.locked("expired").is_some());
    }

    /// On the WRITE path, a tiny `eviction_cap` prunes expired timed locks while
    /// the terminal and active locks (and the just-written one) survive.
    #[test]
    fn sink_evicts_expired_timed_locks_on_write_keeps_terminal_and_active() {
        let store = ModelLockoutStore::new();
        let now = Instant::now();
        // Pre-seed expired timed, terminal, and active locks.
        store.set(
            "expired",
            LockReason::RateLimit,
            Some(now - Duration::from_secs(1)),
            0,
        );
        store.set("terminal", LockReason::Auth, None, 0);
        store.set(
            "active",
            LockReason::QuotaExhausted,
            Some(now + Duration::from_secs(60)),
            0,
        );
        // Tiny cap so the next write trips eviction.
        let sink = ModelLockoutSink::new(
            store.clone(),
            ModelLockoutPolicy::default(),
            LockoutBroadcaster::new(),
            1,
        );

        // A fresh rate-limit failure on a NEW endpoint → writes then evicts.
        let err = GatewayError::RateLimit {
            adapter: "a".into(),
            retry_after_ms: Some(1000),
        };
        sink.on_outcome(&AttemptOutcome {
            endpoint: "new:m",
            router: "new",
            success: false,
            error: Some(&err),
        });

        assert!(
            store.locked("expired").is_none(),
            "expired timed lock evicted"
        );
        assert!(store.locked("terminal").is_some(), "terminal never evicted");
        assert!(
            store.locked("active").is_some(),
            "active timed lock never evicted"
        );
        assert!(store.locked("new:m").is_some(), "just-written lock present");
        // Map bounded to the non-expired entries (terminal + active + new).
        assert!(store.len() <= 3);
    }
}
