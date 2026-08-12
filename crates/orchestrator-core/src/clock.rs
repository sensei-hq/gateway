//! The wall-clock seam (§7.1). Only Observation freshness + provenance read it,
//! so a resume is a pure function of `(journal, clock)`; Pure effects never do.

use chrono::{DateTime, Utc};

/// A source of "now". Injected into the executor so tests can control TTL time.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

/// The default clock — real wall time.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_advances() {
        let c = SystemClock;
        let a = c.now();
        let b = c.now();
        assert!(b >= a, "monotonic non-decreasing");
    }
}
