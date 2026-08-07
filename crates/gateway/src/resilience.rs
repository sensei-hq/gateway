use crate::gates::lockout::ModelLockoutPolicy;
use std::time::Duration;

/// Max entries retained per in-memory health store before EXPIRED entries are
/// evicted (Task 3). Generous — normal operation (fewer distinct endpoints)
/// never trips it; it only bounds leakage from many short-lived endpoints.
/// Active/terminal gates are never dropped.
pub const DEFAULT_EVICTION_CAP: usize = 4096;

/// Operator-tunable resilience policy applied at construction via
/// `Gateway::with_resilience` (Task 2). `Default` reproduces the pre-(f)
/// hardcoded behavior exactly, so an absent config changes nothing.
#[derive(Debug, Clone)]
pub struct ResilienceConfig {
    /// Base router cooldown after a transport fault (`Network`/`Timeout`).
    pub cooldown_base: Duration,
    /// Per-reason model-lockout durations + escalation clamp.
    pub lockout: ModelLockoutPolicy,
    /// Per-store retention cap; over it, expired entries are evicted (Task 3).
    pub eviction_cap: usize,
    /// Deterministic jitter fraction in `[0.0, 1.0)` added to SYNTHETIC timed
    /// deadlines to spread retries across endpoints (Task 4). `0.0` ⇒ off
    /// (today's behavior). A real upstream `Retry-After` is never jittered.
    pub jitter_fraction: f64,
}

impl Default for ResilienceConfig {
    fn default() -> Self {
        Self {
            cooldown_base: Duration::from_secs(30),
            lockout: ModelLockoutPolicy::default(),
            eviction_cap: DEFAULT_EVICTION_CAP,
            jitter_fraction: 0.0,
        }
    }
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
    }
}
