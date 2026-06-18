use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Configuration for the circuit breaker.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening the circuit.
    pub threshold: usize,
    /// How long to wait in the Open state before transitioning to HalfOpen.
    pub timeout: Duration,
    /// Number of consecutive successes in HalfOpen before closing the circuit.
    pub half_open_max_requests: usize,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            threshold: 5,
            timeout: Duration::from_secs(300),
            half_open_max_requests: 3,
        }
    }
}

/// The three states of a circuit breaker.
#[derive(Debug)]
pub enum BreakerState {
    Closed { failure_count: usize },
    Open { next_retry: Instant },
    HalfOpen { success_count: usize },
}

impl BreakerState {
    /// Returns a human-readable name for the current state.
    pub fn name(&self) -> &'static str {
        match self {
            BreakerState::Closed { .. } => "closed",
            BreakerState::Open { .. } => "open",
            BreakerState::HalfOpen { .. } => "half_open",
        }
    }
}

/// Per-endpoint circuit breaker manager.
///
/// State is in-memory only (ephemeral). Uses `Arc<Mutex<HashMap>>` for
/// thread-safe shared state. Unknown endpoints are lazily initialized
/// to `Closed { failure_count: 0 }` on first access.
#[derive(Debug, Clone)]
pub struct CircuitBreakerManager {
    states: Arc<Mutex<HashMap<String, BreakerState>>>,
    config: CircuitBreakerConfig,
}

impl CircuitBreakerManager {
    /// Create a new manager with the given configuration.
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            states: Arc::new(Mutex::new(HashMap::new())),
            config,
        }
    }

    /// Check whether a request to `endpoint` is allowed.
    ///
    /// - **Closed** — always returns `true`.
    /// - **Open** — returns `false` unless the timeout has expired, in which
    ///   case the state transitions to HalfOpen and returns `true`.
    /// - **HalfOpen** — returns `true` (probe requests are allowed).
    pub fn can_execute(&self, endpoint: &str) -> bool {
        let mut states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        let state = states
            .entry(endpoint.to_string())
            .or_insert(BreakerState::Closed { failure_count: 0 });

        match state {
            BreakerState::Closed { .. } => true,
            BreakerState::Open { next_retry } => {
                if Instant::now() >= *next_retry {
                    *state = BreakerState::HalfOpen { success_count: 0 };
                    true
                } else {
                    false
                }
            }
            BreakerState::HalfOpen { .. } => true,
        }
    }

    /// Record a successful response for `endpoint`.
    ///
    /// - **Closed** — resets the failure count to zero.
    /// - **HalfOpen** — increments the success count; transitions to Closed
    ///   once `half_open_max_requests` successes are reached.
    /// - **Open** — no-op (shouldn't happen in normal flow).
    pub fn record_success(&self, endpoint: &str) {
        let mut states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        let Some(state) = states.get_mut(endpoint) else {
            return;
        };

        match state {
            BreakerState::Closed { failure_count } => {
                *failure_count = 0;
            }
            BreakerState::HalfOpen { success_count } => {
                *success_count += 1;
                if *success_count >= self.config.half_open_max_requests {
                    *state = BreakerState::Closed { failure_count: 0 };
                }
            }
            BreakerState::Open { .. } => {}
        }
    }

    /// Record a failed response for `endpoint`.
    ///
    /// - **Closed** — increments the failure count; transitions to Open once
    ///   `threshold` is reached.
    /// - **HalfOpen** — immediately transitions to Open.
    /// - **Open** — no-op (shouldn't happen in normal flow).
    pub fn record_failure(&self, endpoint: &str) {
        let mut states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        let Some(state) = states.get_mut(endpoint) else {
            return;
        };

        match state {
            BreakerState::Closed { failure_count } => {
                *failure_count += 1;
                if *failure_count >= self.config.threshold {
                    *state = BreakerState::Open {
                        next_retry: Instant::now() + self.config.timeout,
                    };
                }
            }
            BreakerState::HalfOpen { .. } => {
                *state = BreakerState::Open {
                    next_retry: Instant::now() + self.config.timeout,
                };
            }
            BreakerState::Open { .. } => {}
        }
    }

    /// Return the current state for `endpoint`.
    ///
    /// Returns `Closed { failure_count: 0 }` for unknown endpoints.
    pub fn get_state(&self, endpoint: &str) -> BreakerState {
        let states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        match states.get(endpoint) {
            Some(BreakerState::Closed { failure_count }) => BreakerState::Closed {
                failure_count: *failure_count,
            },
            Some(BreakerState::Open { next_retry }) => BreakerState::Open {
                next_retry: *next_retry,
            },
            Some(BreakerState::HalfOpen { success_count }) => BreakerState::HalfOpen {
                success_count: *success_count,
            },
            None => BreakerState::Closed { failure_count: 0 },
        }
    }

    /// Remove state for a single endpoint, resetting it to the default Closed.
    pub fn reset(&self, endpoint: &str) {
        let mut states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        states.remove(endpoint);
    }

    /// Remove state for all endpoints.
    pub fn reset_all(&self) {
        let mut states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        states.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn make_manager(threshold: usize, timeout_ms: u64) -> CircuitBreakerManager {
        CircuitBreakerManager::new(CircuitBreakerConfig {
            threshold,
            timeout: Duration::from_millis(timeout_ms),
            half_open_max_requests: 3,
        })
    }

    #[test]
    fn starts_closed_allows_execution() {
        let mgr = make_manager(5, 5000);
        assert!(mgr.can_execute("ep1"));
        assert_eq!(mgr.get_state("ep1").name(), "closed");
    }

    #[test]
    fn stays_closed_below_threshold() {
        let mgr = make_manager(5, 5000);
        mgr.can_execute("ep1");
        for _ in 0..4 {
            mgr.record_failure("ep1");
        }
        assert_eq!(mgr.get_state("ep1").name(), "closed");
        assert!(mgr.can_execute("ep1"));
    }

    #[test]
    fn transitions_to_open_at_threshold() {
        let mgr = make_manager(3, 5000);
        mgr.can_execute("ep1");
        for _ in 0..3 {
            mgr.record_failure("ep1");
        }
        assert_eq!(mgr.get_state("ep1").name(), "open");
        assert!(!mgr.can_execute("ep1"));
    }

    #[test]
    fn open_blocks_execution() {
        let mgr = make_manager(1, 60_000);
        mgr.can_execute("ep1");
        mgr.record_failure("ep1");
        assert!(!mgr.can_execute("ep1"));
        assert!(!mgr.can_execute("ep1"));
        assert!(!mgr.can_execute("ep1"));
    }

    #[test]
    fn open_transitions_to_half_open_after_timeout() {
        let mgr = make_manager(1, 0);
        mgr.can_execute("ep1");
        mgr.record_failure("ep1");
        assert_eq!(mgr.get_state("ep1").name(), "open");
        // Timeout is 0ms, so next can_execute should transition to HalfOpen.
        assert!(mgr.can_execute("ep1"));
        assert_eq!(mgr.get_state("ep1").name(), "half_open");
    }

    #[test]
    fn half_open_transitions_to_closed_after_successes() {
        let mgr = make_manager(1, 0);
        mgr.can_execute("ep1");
        mgr.record_failure("ep1");
        // Transition Open -> HalfOpen
        mgr.can_execute("ep1");
        assert_eq!(mgr.get_state("ep1").name(), "half_open");
        mgr.record_success("ep1");
        mgr.record_success("ep1");
        mgr.record_success("ep1");
        assert_eq!(mgr.get_state("ep1").name(), "closed");
    }

    #[test]
    fn half_open_transitions_to_open_on_failure() {
        let mgr = make_manager(1, 0);
        mgr.can_execute("ep1");
        mgr.record_failure("ep1");
        // Transition Open -> HalfOpen
        mgr.can_execute("ep1");
        assert_eq!(mgr.get_state("ep1").name(), "half_open");
        mgr.record_failure("ep1");
        assert_eq!(mgr.get_state("ep1").name(), "open");
    }

    #[test]
    fn success_resets_failure_count_in_closed() {
        let mgr = make_manager(5, 5000);
        mgr.can_execute("ep1");
        mgr.record_failure("ep1");
        mgr.record_failure("ep1");
        mgr.record_success("ep1"); // resets to 0
        mgr.record_failure("ep1");
        mgr.record_failure("ep1");
        // Total failures since last reset: 2, threshold is 5 → still closed
        assert_eq!(mgr.get_state("ep1").name(), "closed");
        assert!(mgr.can_execute("ep1"));
    }

    #[test]
    fn independent_endpoints() {
        let mgr = make_manager(1, 60_000);
        mgr.can_execute("ep1");
        mgr.can_execute("ep2");
        mgr.record_failure("ep1");
        assert_eq!(mgr.get_state("ep1").name(), "open");
        assert_eq!(mgr.get_state("ep2").name(), "closed");
        assert!(mgr.can_execute("ep2"));
    }

    #[test]
    fn reset_clears_single_endpoint() {
        let mgr = make_manager(1, 60_000);
        mgr.can_execute("ep1");
        mgr.can_execute("ep2");
        mgr.record_failure("ep1");
        mgr.record_failure("ep2");
        assert_eq!(mgr.get_state("ep1").name(), "open");
        assert_eq!(mgr.get_state("ep2").name(), "open");
        mgr.reset("ep1");
        assert_eq!(mgr.get_state("ep1").name(), "closed");
        assert_eq!(mgr.get_state("ep2").name(), "open");
    }

    #[test]
    fn reset_all_clears_everything() {
        let mgr = make_manager(1, 60_000);
        mgr.can_execute("ep1");
        mgr.can_execute("ep2");
        mgr.record_failure("ep1");
        mgr.record_failure("ep2");
        assert_eq!(mgr.get_state("ep1").name(), "open");
        assert_eq!(mgr.get_state("ep2").name(), "open");
        mgr.reset_all();
        assert_eq!(mgr.get_state("ep1").name(), "closed");
        assert_eq!(mgr.get_state("ep2").name(), "closed");
    }

    #[test]
    fn half_open_allows_execution() {
        // Transition to HalfOpen, then verify can_execute returns true without
        // the transition itself (i.e. already in HalfOpen).
        let mgr = make_manager(1, 0);
        mgr.can_execute("ep1"); // initialize
        mgr.record_failure("ep1"); // -> Open
        assert_eq!(mgr.get_state("ep1").name(), "open");
        // Timeout is 0ms, so next can_execute transitions Open -> HalfOpen
        assert!(mgr.can_execute("ep1"));
        assert_eq!(mgr.get_state("ep1").name(), "half_open");
        // Now call can_execute again while already in HalfOpen — line 87
        assert!(mgr.can_execute("ep1"));
        assert_eq!(mgr.get_state("ep1").name(), "half_open");
    }

    #[test]
    fn record_success_unknown_endpoint() {
        // record_success on an endpoint that was never seen should early-return
        // without panic — line 100
        let mgr = make_manager(5, 5000);
        mgr.record_success("never_seen_endpoint");
        // No panic, state is default Closed
        assert_eq!(mgr.get_state("never_seen_endpoint").name(), "closed");
    }

    #[test]
    fn record_success_open_is_noop() {
        // record_success while Open should be a no-op — line 113
        let mgr = make_manager(1, 60_000);
        mgr.can_execute("ep1");
        mgr.record_failure("ep1"); // -> Open
        assert_eq!(mgr.get_state("ep1").name(), "open");
        // record_success in Open state should not change state
        mgr.record_success("ep1");
        assert_eq!(mgr.get_state("ep1").name(), "open");
    }

    #[test]
    fn record_failure_unknown_endpoint() {
        // record_failure on an endpoint that was never seen should early-return
        // without panic — line 126
        let mgr = make_manager(5, 5000);
        mgr.record_failure("never_seen_endpoint");
        // No panic, state is default Closed (never initialized)
        assert_eq!(mgr.get_state("never_seen_endpoint").name(), "closed");
    }

    #[test]
    fn record_failure_open_is_noop() {
        // record_failure while Open should be a no-op — line 143
        let mgr = make_manager(1, 60_000);
        mgr.can_execute("ep1");
        mgr.record_failure("ep1"); // -> Open
        assert_eq!(mgr.get_state("ep1").name(), "open");
        // record_failure in Open state should not change state
        mgr.record_failure("ep1");
        assert_eq!(mgr.get_state("ep1").name(), "open");
    }
}
