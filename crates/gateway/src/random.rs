use std::sync::atomic::{AtomicU64, Ordering};

/// Randomness for weighted routing, injected as a port.
///
/// `&self` with interior mutability, matching `CircuitBreakerManager`'s `Mutex`
/// style — it keeps `RoutingStrategy::order` taking `&self` and makes a seeded
/// source trivial to substitute in tests (the `FakeClock` precedent, SP-DATA-3).
pub trait RandomSource: Send + Sync {
    fn next_u64(&self) -> u64;
}

/// SplitMix64. Chosen over adding a crate: the workspace has no `rand`
/// dependency, and `uuid` (already a dependency, CSPRNG-backed for v4) supplies
/// the seed. Quality is ample for weighting a handful of candidates.
pub struct SplitMix64 {
    state: AtomicU64,
}

const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

impl SplitMix64 {
    /// `const` so it can back a `static` default.
    pub const fn seeded(seed: u64) -> Self {
        Self {
            state: AtomicU64::new(seed),
        }
    }

    /// Seeded from a v4 UUID's bytes, so two processes diverge — otherwise every
    /// worker would make the same first choice and "load balancing" would
    /// synchronise rather than spread.
    pub fn from_entropy() -> Self {
        let b = uuid::Uuid::new_v4().into_bytes();
        let seed = u64::from_le_bytes(b[0..8].try_into().expect("16 bytes contains 8"));
        Self::seeded(seed)
    }
}

impl RandomSource for SplitMix64 {
    fn next_u64(&self) -> u64 {
        // `fetch_add` returns the PREVIOUS value and wraps on overflow, so add
        // GOLDEN back to get the advanced state SplitMix64 specifies.
        let z = self
            .state
            .fetch_add(GOLDEN, Ordering::Relaxed)
            .wrapping_add(GOLDEN);
        let z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Seeded ⇒ reproducible. This is what makes every weighted-strategy test in
    /// Tasks 7-9 deterministic rather than flaky.
    #[test]
    fn the_same_seed_yields_the_same_sequence() {
        let a = SplitMix64::seeded(42);
        let b = SplitMix64::seeded(42);
        let xs: Vec<u64> = (0..8).map(|_| a.next_u64()).collect();
        let ys: Vec<u64> = (0..8).map(|_| b.next_u64()).collect();
        assert_eq!(xs, ys);
    }

    /// The sequence must be the REAL SplitMix64 one, pinned by value. Asserting
    /// only that draws differ would pass against a counter, which has terrible
    /// distribution and would quietly wreck Task 7's weighting.
    #[test]
    fn the_sequence_is_splitmix64_not_merely_varying() {
        let r = SplitMix64::seeded(0);
        // Reference vectors for SplitMix64 seeded at 0, golden gamma
        // 0x9E3779B97F4A7C15. Verify these against the published algorithm
        // before trusting them — if your implementation disagrees, work out
        // which one is wrong rather than editing the expectation.
        let got: Vec<u64> = (0..3).map(|_| r.next_u64()).collect();
        assert_eq!(
            got,
            vec![
                0xE220A8397B1DCDAF_u64,
                0x6E789E6AA1B965F4_u64,
                0x06C45D188009454F_u64,
            ],
            "the draw sequence must be SplitMix64's, not merely non-constant"
        );
    }

    #[test]
    fn successive_draws_differ() {
        let r = SplitMix64::seeded(7);
        assert_ne!(
            r.next_u64(),
            r.next_u64(),
            "state must advance through &self"
        );
    }

    /// Two processes must not make the same first routing decision, or
    /// "load balancing" would synchronise every worker instead of spreading.
    #[test]
    fn from_entropy_differs_across_instances() {
        let a = SplitMix64::from_entropy();
        let b = SplitMix64::from_entropy();
        assert_ne!(a.next_u64(), b.next_u64());
    }
}
