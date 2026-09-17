use super::{AttemptOutcome, HealthRecorder};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Observed performance for one endpoint over the live window.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EndpointStats {
    /// Live samples in the window. Every sample carries a latency.
    pub samples: u32,
    /// Of those, how many carried token counts. Counted SEPARATELY because a
    /// throughput sort must not treat a latency-only history as measured.
    pub throughput_samples: u32,
    pub mean_latency_ms: f64,
    /// Mean over `throughput_samples`; `0.0` when there are none.
    pub mean_tokens_per_sec: f64,
    /// Over `samples`. Feeds the default strategy's reliability multiplier.
    pub success_rate: f64,
}

/// Synchronous read port. Selection is not async (`ModelSelectionService::select`),
/// so routing cannot query the async `GatewayStore` inline — this mirrors the
/// existing `EndpointHealthRead` / `RouterHealthRead` / `ModelLockoutRead` trio.
pub trait EndpointPerformanceRead: Send + Sync {
    fn stats(&self, endpoint: &str) -> Option<EndpointStats>;
}

/// The null port: the default wherever performance was never wired, so absent
/// wiring is byte-identical to before this slice.
pub struct NoPerformance;
impl EndpointPerformanceRead for NoPerformance {
    fn stats(&self, _endpoint: &str) -> Option<EndpointStats> {
        None
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Sample {
    pub at: Instant,
    pub latency_ms: u64,
    /// `None` for an attempt that produced no token counts (a setup failure, or
    /// the stream-acquisition dispatch that fires before any tokens exist).
    pub tokens_per_sec: Option<f64>,
    pub success: bool,
}

/// In-memory rolling window per endpoint. Arc-backed + `Clone` so the read
/// reference held by selection and the owned copy inside the recorder share one
/// map — the same pattern as `ConnectionCooldownStore`.
#[derive(Clone)]
pub struct PerformanceStore {
    inner: Arc<Mutex<HashMap<String, VecDeque<Sample>>>>,
    max_samples: usize,
    max_age: Duration,
}

impl PerformanceStore {
    pub fn new(max_samples: usize, max_age: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            max_samples,
            max_age,
        }
    }

    pub(crate) fn record(&self, endpoint: &str, s: Sample) {
        let mut m = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let ring = m.entry(endpoint.to_string()).or_default();
        ring.push_back(s);
        while ring.len() > self.max_samples {
            ring.pop_front();
        }
    }

    /// Drop endpoints whose every sample has aged out, once over `cap`. Active
    /// endpoints are never dropped, so the cap is soft — the same bounded-memory
    /// discipline as `ConnectionCooldownStore::evict_expired_over_cap`.
    pub fn evict_stale_over_cap(&self, cap: usize) {
        let mut m = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if m.len() <= cap {
            return;
        }
        let now = Instant::now();
        let max_age = self.max_age;
        m.retain(|_, ring| {
            ring.iter()
                .any(|s| now.saturating_duration_since(s.at) <= max_age)
        });
    }
}

impl EndpointPerformanceRead for PerformanceStore {
    fn stats(&self, endpoint: &str) -> Option<EndpointStats> {
        let m = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let ring = m.get(endpoint)?;
        let now = Instant::now();
        let live: Vec<&Sample> = ring
            .iter()
            .filter(|s| now.saturating_duration_since(s.at) <= self.max_age)
            .collect();
        if live.is_empty() {
            return None;
        }

        let n = live.len() as f64;
        let mean_latency_ms = live.iter().map(|s| s.latency_ms as f64).sum::<f64>() / n;
        let success_rate = live.iter().filter(|s| s.success).count() as f64 / n;

        let tps: Vec<f64> = live.iter().filter_map(|s| s.tokens_per_sec).collect();
        let mean_tokens_per_sec = if tps.is_empty() {
            0.0
        } else {
            tps.iter().sum::<f64>() / tps.len() as f64
        };

        Some(EndpointStats {
            samples: live.len() as u32,
            throughput_samples: tps.len() as u32,
            mean_latency_ms,
            mean_tokens_per_sec,
            success_rate,
        })
    }
}

/// Write side. Never gates, so it always returns `None` — it contributes no
/// deadline to `AllGated.resume_after`.
pub struct PerformanceRecorder {
    store: PerformanceStore,
    eviction_cap: usize,
}

impl PerformanceRecorder {
    pub fn new(store: PerformanceStore, eviction_cap: usize) -> Self {
        Self {
            store,
            eviction_cap,
        }
    }
}

impl HealthRecorder for PerformanceRecorder {
    fn on_outcome(&self, o: &AttemptOutcome<'_>) -> Option<Instant> {
        let tokens_per_sec = match (o.output_tokens, o.duration_ms) {
            (Some(t), ms) if ms > 0 && t > 0 => Some(t as f64 * 1000.0 / ms as f64),
            _ => None,
        };
        self.store.record(
            o.endpoint,
            Sample {
                at: Instant::now(),
                latency_ms: o.duration_ms,
                tokens_per_sec,
                success: o.success,
            },
        );
        self.store.evict_stale_over_cap(self.eviction_cap);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(latency_ms: u64, tps: Option<f64>, success: bool) -> Sample {
        Sample {
            at: Instant::now(),
            latency_ms,
            tokens_per_sec: tps,
            success,
        }
    }

    #[test]
    fn stats_average_latency_and_success_over_the_window() {
        let store = PerformanceStore::new(8, Duration::from_secs(60));
        store.record("r:m", sample(100, Some(10.0), true));
        store.record("r:m", sample(300, Some(30.0), true));
        store.record("r:m", sample(200, None, false));

        let s = store.stats("r:m").expect("three samples recorded");
        assert_eq!(s.samples, 3);
        assert!((s.mean_latency_ms - 200.0).abs() < f64::EPSILON);
        assert!((s.success_rate - 2.0 / 3.0).abs() < 1e-9);
    }

    /// Throughput is counted SEPARATELY from latency. An endpoint with plenty of
    /// latency samples but no token counts must not look measured to a
    /// throughput sort — otherwise it sorts on a mean over zero observations.
    #[test]
    fn throughput_samples_are_counted_separately_from_latency_samples() {
        let store = PerformanceStore::new(8, Duration::from_secs(60));
        store.record("r:m", sample(100, None, true));
        store.record("r:m", sample(100, None, true));
        store.record("r:m", sample(100, Some(50.0), true));

        let s = store.stats("r:m").unwrap();
        assert_eq!(s.samples, 3, "all three carry latency");
        assert_eq!(s.throughput_samples, 1, "only one carries token counts");
        assert!((s.mean_tokens_per_sec - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn the_ring_is_bounded_by_length() {
        let store = PerformanceStore::new(2, Duration::from_secs(60));
        for _ in 0..10 {
            store.record("r:m", sample(100, None, true));
        }
        assert_eq!(store.stats("r:m").unwrap().samples, 2, "capped at 2");
    }

    /// The ring keeps the NEWEST samples, not the oldest — a cap that dropped
    /// the wrong end would make the store report stale numbers forever.
    #[test]
    fn the_ring_evicts_the_oldest_and_keeps_the_newest() {
        let store = PerformanceStore::new(2, Duration::from_secs(60));
        store.record("r:m", sample(999, None, true));
        store.record("r:m", sample(100, None, true));
        store.record("r:m", sample(100, None, true));
        let s = store.stats("r:m").unwrap();
        assert!(
            (s.mean_latency_ms - 100.0).abs() < f64::EPSILON,
            "the 999ms sample must have been evicted, got {}",
            s.mean_latency_ms
        );
    }

    /// Samples older than the window do not count. Uses a zero-length window so
    /// every recorded sample is instantly stale.
    #[test]
    fn samples_older_than_the_window_are_not_counted() {
        let store = PerformanceStore::new(8, Duration::ZERO);
        store.record("r:m", sample(100, None, true));
        std::thread::sleep(Duration::from_millis(5));
        assert!(
            store.stats("r:m").is_none(),
            "an all-stale endpoint reports no stats at all"
        );
    }

    #[test]
    fn an_unknown_endpoint_has_no_stats() {
        let store = PerformanceStore::new(8, Duration::from_secs(60));
        assert!(store.stats("never:seen").is_none());
    }

    /// The null port used as the default wherever performance is not wired, so
    /// absent wiring is byte-identical to before this slice.
    #[test]
    fn the_null_port_never_reports_stats() {
        assert!(NoPerformance.stats("r:m").is_none());
    }

    /// The RECORDER is the write side, and it must derive throughput from the
    /// outcome rather than being handed it. Both branches matter: an outcome
    /// with tokens yields a rate, one without yields None.
    #[test]
    fn the_recorder_derives_throughput_only_when_tokens_are_present() {
        let store = PerformanceStore::new(8, Duration::from_secs(60));
        let rec = PerformanceRecorder::new(store.clone(), 4096);

        let with_tokens = crate::gates::AttemptOutcome {
            endpoint: "r:m",
            router: "r",
            success: true,
            error: None,
            duration_ms: 1000,
            output_tokens: Some(50),
        };
        assert!(
            rec.on_outcome(&with_tokens).is_none(),
            "never writes a deadline"
        );

        let s = store.stats("r:m").unwrap();
        assert_eq!(s.throughput_samples, 1);
        assert!(
            (s.mean_tokens_per_sec - 50.0).abs() < f64::EPSILON,
            "50 tokens in 1000ms is 50 tok/s, got {}",
            s.mean_tokens_per_sec
        );

        let without_tokens = crate::gates::AttemptOutcome {
            endpoint: "r2:m2",
            router: "r2",
            success: false,
            error: None,
            duration_ms: 250,
            output_tokens: None,
        };
        rec.on_outcome(&without_tokens);
        let s2 = store.stats("r2:m2").unwrap();
        assert_eq!(s2.samples, 1, "latency is still recorded");
        assert_eq!(s2.throughput_samples, 0, "but no throughput is invented");
        assert!((s2.mean_latency_ms - 250.0).abs() < f64::EPSILON);
        assert!(
            (s2.success_rate - 0.0).abs() < f64::EPSILON,
            "a failure is recorded as such"
        );
    }
}
