use crate::gates::lockout::ModelLockoutPolicy;
use std::time::Duration;

/// Max entries retained per in-memory health store before EXPIRED entries are
/// evicted (SP-0 (f) Task 3). Generous — normal operation (fewer distinct endpoints)
/// never trips it; it only bounds leakage from many short-lived endpoints.
/// Active/terminal gates are never dropped.
pub const DEFAULT_EVICTION_CAP: usize = 4096;

/// Samples retained per endpoint in the rolling performance window
/// (SP-ROUTE-1 Task 4).
pub const DEFAULT_PERF_SAMPLES: usize = 64;
/// How long a performance sample stays live (SP-ROUTE-1 Task 4).
pub const DEFAULT_PERF_WINDOW: Duration = Duration::from_secs(300);
/// Minimum live observations before a metric sort trusts a candidate's mean.
/// Matches the value `ModelSelectionService::new` installs, so wiring this
/// through `Gateway` changes nothing by default.
pub const DEFAULT_MIN_SAMPLES: u32 = 3;

/// Operator-tunable resilience policy applied at construction via
/// `Gateway::with_resilience` (SP-0 (f) Task 2). `Default` reproduces the
/// pre-(f) hardcoded behavior exactly, so an absent config changes nothing.
///
/// `#[non_exhaustive]` because this is a published crate's public surface and
/// this struct grows a field per slice (`min_samples` arrived with SP-ROUTE-1).
/// Without it, every added field is a breaking change for any downstream struct
/// literal. Applied the same slice the field landed in — it is breaking exactly
/// once, and doing it later only makes the break bigger.
///
/// **The attribute forbids EVERY struct expression outside this crate,
/// functional-update syntax INCLUDED.** `ResilienceConfig { min_samples: 5,
/// ..Default::default() }` is `error[E0639]: cannot create non-exhaustive
/// struct using struct expression` for any consumer, however few fields it
/// names — `..Default::default()` buys nothing here, and the three docs that
/// printed it were wrong. Only code inside `sensei-gateway` may write that
/// form, which is exactly why a whole slice shipped without noticing. Build the
/// value and then assign:
///
/// ```
/// use gateway::resilience::ResilienceConfig;
///
/// let mut resilience = ResilienceConfig::default();
/// resilience.min_samples = 5;
/// ```
///
/// That doctest compiles as a downstream crate, and
/// `tests/reexport_paths.rs::non_exhaustive_config_is_built_the_way_the_docs_say`
/// pins the same shape from the integration-test side.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ResilienceConfig {
    /// Base router cooldown after a transport fault (`Network`/`Timeout`).
    pub cooldown_base: Duration,
    /// Per-reason model-lockout durations + escalation clamp.
    pub lockout: ModelLockoutPolicy,
    /// Per-store retention cap; over it, expired entries are evicted
    /// (SP-0 (f) Task 3).
    pub eviction_cap: usize,
    /// Deterministic jitter fraction in `[0.0, 1.0)` added to SYNTHETIC timed
    /// deadlines to spread retries across endpoints (SP-0 (f) Task 4). `0.0` ⇒
    /// off (today's behavior). A real upstream `Retry-After` is never jittered.
    pub jitter_fraction: f64,
    /// Samples retained per endpoint in the rolling performance window
    /// (SP-ROUTE-1 Task 4 — a DIFFERENT slice's Task 4 from `jitter_fraction`
    /// above, which is SP-0 (f)'s). Applying a change requires
    /// `Gateway::with_resilience` to rebuild the `PerformanceStore` (its
    /// capacity is fixed at construction), discarding any samples recorded
    /// before the rebuild.
    pub perf_samples: usize,
    /// How long a performance sample stays live (SP-ROUTE-1 Task 4). Same
    /// rebuild caveat as `perf_samples`.
    pub perf_window: Duration,
    /// The minimum count of live observations a metric sort requires before it
    /// trusts a candidate's mean as "measured" rather than leaving it in
    /// priority order (SP-ROUTE-1). Reaches the strategies as
    /// [`crate::strategy::StrategyCtx::min_samples`], whose doc carries the
    /// hazard: each metric has its OWN counter on `EndpointStats`, and this
    /// threshold must be compared against the one belonging to the metric being
    /// sorted on.
    ///
    /// Defaults to [`DEFAULT_MIN_SAMPLES`] (3) — the smallest count at which a
    /// mean is not simply the last observation. The cost of getting it wrong is
    /// bounded in both directions: too low and the sort reacts to noise, too
    /// high and it degrades to priority order, which is the documented fallback
    /// anyway.
    ///
    /// **Do NOT set it to `0`** — but the mechanism is narrower than "a cold
    /// process breaks", and an earlier draft of this doc named a mechanism the
    /// code does not have.
    ///
    /// A NEVER-observed endpoint is unmeasured at EVERY threshold, zero
    /// included. `PerformanceStore::stats` returns `None` before any `>=`
    /// comparison can run — `m.get(endpoint)?` for an endpoint with no ring at
    /// all, then `if live.is_empty() { return None }` for one whose samples have
    /// aged out — and `None` takes the unmeasured path regardless. So a cold
    /// process routes normally at `min_samples: 0`; it does not weigh every
    /// candidate at zero.
    ///
    /// What zero DOES reach is an endpoint whose ring holds live samples while
    /// the counter being read sits at `0` beside a mean of `0.0`. The three
    /// counters are independent (neither superset nor subset), so that is an
    /// ordinary state rather than a corner:
    ///
    /// - **Reliability.** One `StreamAcquired` with no completion yet ⇒
    ///   `samples: 1, verdict_samples: 0, success_rate: 0.0`. At zero the
    ///   verdict gate passes, so `GroupedWeightedStrategy` records
    ///   `reliability: Some(0.0)` and `weight: Some(0.0)` — dead last in its
    ///   group, for an endpoint whose only sin is not having finished a request.
    /// - **Latency.** A FAILED `Complete` casts a verdict but contributes no
    ///   latency observation ⇒ `samples: 0, mean_latency_ms: 0.0`. At zero
    ///   `sort: latency` reads that `0.0` as a measurement, and the endpoint
    ///   wins every race it has never run.
    ///
    /// Any value `>= 1` makes both unreachable, and the default of 3 keeps a
    /// margin past that. Pinned by
    /// `engine::tests::resilience_min_samples_reaches_the_metric_sort`.
    ///
    /// A COUNT, unlike `perf_samples` (a retention capacity) and `perf_window`
    /// (a retention age) — those two decide what stays in the window, this one
    /// decides how much of it is enough to act on. Unlike them it needs no
    /// rebuild: it is read per request, so `with_resilience` simply replaces it.
    pub min_samples: u32,
}

impl Default for ResilienceConfig {
    fn default() -> Self {
        Self {
            cooldown_base: Duration::from_secs(30),
            lockout: ModelLockoutPolicy::default(),
            eviction_cap: DEFAULT_EVICTION_CAP,
            jitter_fraction: 0.0,
            perf_samples: DEFAULT_PERF_SAMPLES,
            perf_window: DEFAULT_PERF_WINDOW,
            min_samples: DEFAULT_MIN_SAMPLES,
        }
    }
}

/// Deterministic per-key jitter in `[0, base * fraction)`. Uses `DefaultHasher`
/// (fixed keys ⇒ stable across runs, unlike `RandomState`), so the SAME key
/// always gets the SAME offset (flake-free tests) while DIFFERENT keys spread
/// out (thundering-herd mitigation). `fraction <= 0` or `base == 0` ⇒ zero.
pub(crate) fn deterministic_jitter(key: &str, base: Duration, fraction: f64) -> Duration {
    if fraction <= 0.0 || base.is_zero() {
        return Duration::ZERO;
    }
    let span_ms = (base.as_millis() as f64 * fraction.min(1.0)) as u64;
    if span_ms == 0 {
        return Duration::ZERO;
    }
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    Duration::from_millis(h.finish() % span_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn default_matches_todays_hardcoded_behavior() {
        let r = ResilienceConfig::default();
        assert_eq!(r.cooldown_base, Duration::from_secs(30)); // == old DEFAULT_CONNECTION_COOLDOWN
        assert_eq!(r.lockout.rate_limit_base, Duration::from_secs(60));
        assert_eq!(r.lockout.quota_default, Duration::from_secs(3600));
        assert_eq!(r.lockout.max_cooldown, Duration::from_secs(6 * 3600));
        assert_eq!(r.jitter_fraction, 0.0); // off ⇒ behavior-preserving
        assert!(r.eviction_cap >= 1024); // bounded but generous
        // Pinned to literals, like the neighbouring assertions — comparing
        // to the constants themselves would pass even if the constant (and
        // this default) silently drifted together.
        assert_eq!(r.perf_samples, 64);
        assert_eq!(r.perf_window, Duration::from_secs(300));
    }

    #[test]
    fn jitter_off_and_zero_base() {
        use std::time::Duration;
        assert_eq!(
            deterministic_jitter("k", Duration::from_secs(60), 0.0),
            Duration::ZERO
        );
        assert_eq!(
            deterministic_jitter("k", Duration::ZERO, 0.5),
            Duration::ZERO
        );
    }

    #[test]
    fn jitter_deterministic_bounded_and_spread() {
        use std::time::Duration;
        let base = Duration::from_secs(60);
        let a1 = deterministic_jitter("router-a", base, 0.5);
        let a2 = deterministic_jitter("router-a", base, 0.5);
        assert_eq!(a1, a2, "same key → same offset (flake-free)");
        assert!(a1 < Duration::from_secs(30), "within [0, base*fraction)");
        let b = deterministic_jitter("router-b", base, 0.5);
        // At least one of several distinct keys must differ from a1 (spread).
        let c = deterministic_jitter("router-c", base, 0.5);
        assert!(b != a1 || c != a1, "different keys generally spread out");
    }
}
