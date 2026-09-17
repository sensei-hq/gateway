use super::{AttemptOutcome, AttemptPhase, HealthRecorder};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Observed performance for one endpoint over the live window.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EndpointStats {
    /// Live samples contributing a LATENCY observation (`Complete` and
    /// `StreamAcquired` phases; NOT `StreamCompleted`, whose duration is
    /// generation time, a different quantity).
    pub samples: u32,
    /// Of the live samples, how many carried token counts. Counted SEPARATELY
    /// from `samples` because a throughput sort must not treat a latency-only
    /// history as measured.
    pub throughput_samples: u32,
    /// Mean over `samples`; `0.0` when there are none.
    pub mean_latency_ms: f64,
    /// Mean over `throughput_samples`; `0.0` when there are none.
    pub mean_tokens_per_sec: f64,
    /// Fraction of VERDICT-bearing live samples (`Complete` and
    /// `StreamCompleted`; NOT `StreamAcquired`, which is a latency observation
    /// with no final verdict) that succeeded. `0.0` when there are none. Feeds
    /// the default strategy's reliability multiplier — one attempt casts
    /// exactly one verdict here, however many phases it dispatched.
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

/// One observation. A single streaming attempt writes TWO of these (one
/// `StreamAcquired` then one `StreamCompleted`), so `latency_ms` and `success`
/// are each optional and independent — exactly one sample carries the verdict,
/// and `StreamCompleted`'s `duration_ms` (generation time) is never a latency
/// observation at all. See [`AttemptPhase`] for why.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Sample {
    pub at: Instant,
    /// `None` for a `StreamCompleted` sample — its duration is generation
    /// time, not comparable to the other phases' latency.
    pub latency_ms: Option<u64>,
    pub tokens_per_sec: Option<f64>,
    /// `None` for `StreamAcquired` — it is a latency observation, not a
    /// verdict. Exactly one sample per attempt carries `Some`.
    pub success: Option<bool>,
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

    /// Number of tracked endpoints (live + stale-but-not-yet-evicted). Mirrors
    /// `ConnectionCooldownStore::len` — exists so a test can prove an entry was
    /// actually REMOVED by `evict_stale_over_cap`, which `stats()` alone cannot:
    /// `stats()` returns `None` for both "evicted" and "present but every
    /// sample is stale".
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
    /// Whether any endpoint is tracked.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drop endpoints whose every sample has aged out, once over `cap`. An
    /// endpoint with even ONE live sample is never dropped, so the cap is soft
    /// — the same bounded-memory discipline as
    /// `ConnectionCooldownStore::evict_expired_over_cap`. The predicate is
    /// `.any(live)`, not `.all(live)`: an endpoint that mixes a stale sample
    /// with a live one must survive, since `.all` would delete the performance
    /// history of an actively-used endpoint the moment its ring holds a single
    /// aged-out entry.
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
        // `None` only when the ring has NO live samples at all — a field with
        // no contributors still reports (0, 0.0), it does not fail the whole
        // read. Tested on a MIXED (live + stale) ring, not just an all-stale
        // one: dividing by `ring.len()` instead of `live.len()` passes an
        // all-stale-only check but under-reports latency by the stale
        // fraction on a mixed one.
        if live.is_empty() {
            return None;
        }

        let latencies: Vec<f64> = live
            .iter()
            .filter_map(|s| s.latency_ms.map(|v| v as f64))
            .collect();
        let mean_latency_ms = if latencies.is_empty() {
            0.0
        } else {
            latencies.iter().sum::<f64>() / latencies.len() as f64
        };

        let tps: Vec<f64> = live.iter().filter_map(|s| s.tokens_per_sec).collect();
        let mean_tokens_per_sec = if tps.is_empty() {
            0.0
        } else {
            tps.iter().sum::<f64>() / tps.len() as f64
        };

        let verdicts: Vec<bool> = live.iter().filter_map(|s| s.success).collect();
        let success_rate = if verdicts.is_empty() {
            0.0
        } else {
            verdicts.iter().filter(|v| **v).count() as f64 / verdicts.len() as f64
        };

        Some(EndpointStats {
            samples: latencies.len() as u32,
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
        // One attempt, one latency observation, one verdict — never both from
        // the same phase. `StreamAcquired` is a latency-only observation (the
        // completion dispatch that always follows carries the real verdict);
        // `StreamCompleted`'s `duration_ms` is generation time, not latency.
        let (latency_ms, success) = match o.phase {
            AttemptPhase::Complete => (Some(o.duration_ms), Some(o.success)),
            AttemptPhase::StreamAcquired => (Some(o.duration_ms), None),
            AttemptPhase::StreamCompleted => (None, Some(o.success)),
        };
        self.store.record(
            o.endpoint,
            Sample {
                at: Instant::now(),
                latency_ms,
                tokens_per_sec,
                success,
            },
        );
        self.store.evict_stale_over_cap(self.eviction_cap);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A "complete" sample: both a latency and a verdict, as every
    /// non-streaming attempt (and every setup failure) produces.
    fn sample(latency_ms: u64, tps: Option<f64>, success: bool) -> Sample {
        Sample {
            at: Instant::now(),
            latency_ms: Some(latency_ms),
            tokens_per_sec: tps,
            success: Some(success),
        }
    }

    /// A sample as old as `age`, for deterministic mixed live/stale fixtures —
    /// no sleeping.
    fn aged_sample(age: Duration, latency_ms: u64, tps: Option<f64>, success: bool) -> Sample {
        Sample {
            at: Instant::now()
                .checked_sub(age)
                .expect("age fits before now"),
            latency_ms: Some(latency_ms),
            tokens_per_sec: tps,
            success: Some(success),
        }
    }

    /// Build an `AttemptOutcome` for the given phase (endpoint `"r:m"`, router `"r"`).
    fn outcome(
        endpoint: &str,
        success: bool,
        duration_ms: u64,
        output_tokens: Option<u32>,
        phase: AttemptPhase,
    ) -> AttemptOutcome<'_> {
        AttemptOutcome {
            endpoint,
            router: "r",
            success,
            error: None,
            duration_ms,
            output_tokens,
            phase,
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
        assert!((s.mean_latency_ms - 200.0).abs() < 1e-9);
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
        assert!((s.mean_tokens_per_sec - 50.0).abs() < 1e-9);
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
            (s.mean_latency_ms - 100.0).abs() < 1e-9,
            "the 999ms sample must have been evicted, got {}",
            s.mean_latency_ms
        );
    }

    /// Samples older than the window do not count. Uses a zero-length window so
    /// every recorded sample is instantly stale — the degenerate all-stale case.
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

    /// The load-bearing MIXED case: live and stale samples in the SAME ring.
    /// An all-stale-only check (above) cannot distinguish `live.len()` from
    /// `ring.len()` in the denominator — both are the same number when
    /// everything is stale. This fixture requires the store to filter,
    /// not merely to detect "all stale".
    #[test]
    fn stale_samples_are_excluded_from_every_reported_field() {
        let store = PerformanceStore::new(8, Duration::from_secs(60));
        // Two stale samples (older than the 60s window) that would drag every
        // mean the wrong way and inflate every count if counted.
        store.record(
            "r:m",
            aged_sample(Duration::from_secs(120), 900, Some(900.0), false),
        );
        store.record(
            "r:m",
            aged_sample(Duration::from_secs(90), 900, Some(900.0), false),
        );
        // Two live samples — the only ones that should be reflected.
        store.record("r:m", sample(100, Some(10.0), true));
        store.record("r:m", sample(300, Some(30.0), true));

        let s = store.stats("r:m").unwrap();
        assert_eq!(s.samples, 2, "only the two live samples count");
        assert_eq!(
            s.throughput_samples, 2,
            "only the live throughput samples count"
        );
        assert!(
            (s.mean_latency_ms - 200.0).abs() < 1e-9,
            "stale 900ms samples must not pull the mean down, got {}",
            s.mean_latency_ms
        );
        assert!(
            (s.mean_tokens_per_sec - 20.0).abs() < 1e-9,
            "stale 900 tok/s samples must not pull the mean up, got {}",
            s.mean_tokens_per_sec
        );
        assert!(
            (s.success_rate - 1.0).abs() < 1e-9,
            "stale failures must not drag success_rate down, got {}",
            s.success_rate
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

        let with_tokens = outcome("r:m", true, 1000, Some(50), AttemptPhase::Complete);
        assert!(
            rec.on_outcome(&with_tokens).is_none(),
            "never writes a deadline"
        );

        let s = store.stats("r:m").unwrap();
        assert_eq!(s.throughput_samples, 1);
        assert!(
            (s.mean_tokens_per_sec - 50.0).abs() < 1e-9,
            "50 tokens in 1000ms is 50 tok/s, got {}",
            s.mean_tokens_per_sec
        );

        let without_tokens = outcome("r2:m2", false, 250, None, AttemptPhase::Complete);
        rec.on_outcome(&without_tokens);
        let s2 = store.stats("r2:m2").unwrap();
        assert_eq!(s2.samples, 1, "latency is still recorded");
        assert_eq!(s2.throughput_samples, 0, "but no throughput is invented");
        assert!((s2.mean_latency_ms - 250.0).abs() < 1e-9);
        assert!(
            (s2.success_rate - 0.0).abs() < 1e-9,
            "a failure is recorded as such"
        );
    }

    /// One streaming attempt must contribute ONE latency observation and ONE
    /// verdict, not two of each. Before the phase split, a stream that died
    /// mid-way scored 0.5 forever because its acquisition success was counted
    /// alongside its failure — so Task 7's reliability multiplier could never
    /// de-weight a totally broken endpoint.
    #[test]
    fn a_streaming_attempt_contributes_one_latency_and_one_verdict() {
        let store = PerformanceStore::new(8, Duration::from_secs(60));
        let rec = PerformanceRecorder::new(store.clone(), 4096);

        // Acquisition: fast, succeeded, no tokens yet.
        rec.on_outcome(&outcome(
            "r:m",
            true,
            200,
            None,
            AttemptPhase::StreamAcquired,
        ));
        // Completion: the stream then died after 9s having emitted nothing useful.
        rec.on_outcome(&outcome(
            "r:m",
            false,
            9_000,
            None,
            AttemptPhase::StreamCompleted,
        ));

        let s = store.stats("r:m").unwrap();
        assert_eq!(s.samples, 1, "only the acquisition contributes a latency");
        assert!(
            (s.mean_latency_ms - 200.0).abs() < 1e-9,
            "the 9s generation span must NOT be pooled into latency, got {}",
            s.mean_latency_ms
        );
        assert!(
            (s.success_rate - 0.0).abs() < 1e-9,
            "one attempt casts ONE verdict, and it failed — got {}",
            s.success_rate
        );
    }

    /// Over `cap`, endpoints with NO live sample are dropped, but an endpoint
    /// that mixes a stale sample with one still-live sample is NEVER dropped —
    /// the load-bearing "any life is enough" invariant. A `.all(live)`
    /// predicate would wrongly evict the mixed endpoint (its stale sample
    /// fails `all`), silently deleting the performance history of an
    /// actively-used endpoint.
    #[test]
    fn evict_stale_over_cap_drops_all_stale_keeps_any_live() {
        let store = PerformanceStore::new(8, Duration::from_secs(60));
        // Three endpoints with only a stale sample.
        store.record(
            "stale1",
            aged_sample(Duration::from_secs(120), 100, None, true),
        );
        store.record(
            "stale2",
            aged_sample(Duration::from_secs(120), 100, None, true),
        );
        store.record(
            "stale3",
            aged_sample(Duration::from_secs(120), 100, None, true),
        );
        // One endpoint with a STALE sample plus one LIVE sample — must survive.
        store.record(
            "mixed",
            aged_sample(Duration::from_secs(120), 100, None, true),
        );
        store.record("mixed", sample(100, None, true));

        store.evict_stale_over_cap(2);

        assert_eq!(
            store.len(),
            1,
            "only the mixed (any-live) endpoint survives"
        );
        assert!(
            store.stats("mixed").is_some(),
            "an endpoint with even one live sample must never be evicted"
        );
    }

    /// At/below the cap, nothing is evicted — even an all-stale endpoint is
    /// kept (until the cap is actually exceeded on some later write).
    #[test]
    fn evict_stale_over_cap_no_op_when_at_or_below_cap() {
        let store = PerformanceStore::new(8, Duration::from_secs(60));
        store.record(
            "stale",
            aged_sample(Duration::from_secs(120), 100, None, true),
        );
        store.evict_stale_over_cap(4096);
        assert_eq!(
            store.len(),
            1,
            "at/below cap, even an all-stale endpoint is kept"
        );
    }
}
