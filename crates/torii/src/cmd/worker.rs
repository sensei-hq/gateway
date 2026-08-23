//! The worker: the first production caller of `Scheduler::tick`.

use crate::cmd::Outcome;
use crate::errors::CliError;
use orchestrator_core::OrchestratorError;
use std::time::Duration;

/// The tick surface, injected so the loop's resilience policy is testable without
/// a database or a gateway.
#[async_trait::async_trait]
pub trait Ticker: Send + Sync {
    async fn tick(&self) -> Result<usize, OrchestratorError>;
}

#[async_trait::async_trait]
impl Ticker for orchestrator::Scheduler {
    async fn tick(&self) -> Result<usize, OrchestratorError> {
        orchestrator::Scheduler::tick(self).await
    }
}

pub const MAX_CONSECUTIVE_FAILURES: u32 = 5;

pub struct ServeOpts {
    pub interval: Duration,
    pub once: bool,
}

/// Parse `5s` / `2m` / `500ms` into a Duration.
pub fn parse_interval(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let (num, unit) = if let Some(n) = s.strip_suffix("ms") {
        (n, "ms")
    } else if let Some(n) = s.strip_suffix('s') {
        (n, "s")
    } else if let Some(n) = s.strip_suffix('m') {
        (n, "m")
    } else {
        return Err(format!(
            "invalid interval {s:?}: expected a unit, e.g. 500ms, 5s, 2m"
        ));
    };
    let v: u64 = num
        .parse()
        .map_err(|_| format!("invalid interval {s:?}: {num:?} is not a number"))?;
    let duration = match unit {
        "ms" => Duration::from_millis(v),
        "s" => Duration::from_secs(v),
        _ => {
            let secs = v
                .checked_mul(60)
                .ok_or_else(|| format!("invalid interval {s:?}: overflow"))?;
            Duration::from_secs(secs)
        }
    };
    if duration.is_zero() {
        return Err(format!(
            "invalid interval {s:?}: a zero poll interval is not a meaningful value"
        ));
    }
    Ok(duration)
}

/// Poll until shutdown. A store fault is retried with bounded backoff; after
/// MAX_CONSECUTIVE_FAILURES the worker exits non-zero so a supervisor restarts it
/// and an alert fires — a dead database must never read as healthy. In `--once`
/// mode there is no next attempt to retry on, so a single store fault is fatal
/// immediately rather than deferred until the cap.
///
/// `shutdown` is a LEVEL, not a one-shot event: the caller increments the
/// `watch` value once per received signal (SIGINT, or on unix also SIGTERM). A
/// `watch::Receiver` is what lets a single external signal source be observed
/// TWICE — a plain `Future` fires once by construction, and `Notify` stores at
/// most one permit (two signals landing back-to-back before this loop is ever
/// polled would silently coalesce into one). Reading `*shutdown.borrow()` as a
/// level rather than counting `changed()` events sidesteps that hazard: even if
/// both signals land before this loop is polled even once, the value already
/// reads >= 2 the first time it's checked, and is treated exactly like two
/// signals observed one after another.
///
/// The tick itself is raced against `shutdown`, not just the sleep between
/// ticks — a tick's duration is unbounded because `tick()` drives due runs
/// inline via `Executor::start`, so racing only the sleep could never observe a
/// second signal until the tick had already finished on its own. The FIRST
/// signal is deliberately NOT acted on while a tick is in flight — it is noted,
/// and the tick is allowed to finish, so a partial drive is not wasted — and is
/// honored immediately once that tick completes. The SECOND signal received
/// before that same tick finishes abandons it: the future is dropped at its
/// next await point (mid-`claim_due` or mid-`Executor::start`, whichever the
/// tick happened to be in). That is safe by construction — whatever
/// `scheduled_runs` row(s) that tick had already claimed stay `waking`, not
/// terminal, and the scheduler's lease reclaims each of them after its lease
/// duration — but it is a deliberate act an operator took by signalling twice,
/// not silent data loss, so the outcome text says so explicitly (without
/// naming a row count: `tick()` claims a whole batch before driving any of
/// them, and this loop only ever sees its final `Ok(n)`/`Err`, never how far a
/// dropped attempt got — a specific number here would be false precision).
/// The exit code is still 0: an operator who signals twice asked for exactly
/// this, so it is not an error outcome — an unexpected process death would be
/// signalled some other way (e.g. a non-zero exit from a crash), and this one
/// is a clean, deliberate stop.
pub async fn serve(
    ticker: &dyn Ticker,
    opts: ServeOpts,
    mut shutdown: tokio::sync::watch::Receiver<u64>,
) -> Result<Outcome, CliError> {
    let mut consecutive_failures: u32 = 0;
    let mut woken_total: usize = 0;

    loop {
        // Race the SAME tick future against `shutdown` across repeated selects: a
        // signal that only reaches level 1 must not touch `tick_fut` at all (it
        // keeps being polled, unmodified, next iteration), so the in-flight drive
        // survives exactly one signal but not two.
        let mut tick_fut = ticker.tick();
        let tick_result = loop {
            tokio::select! {
                result = &mut tick_fut => break Some(result),
                _ = shutdown.changed() => {
                    if *shutdown.borrow() >= 2 {
                        break None;
                    }
                    // Exactly one signal so far: noted, not acted on.
                }
            }
        };

        let Some(tick_result) = tick_result else {
            // Deliberately no specific count here: `Ticker::tick()` is opaque to this
            // loop (it returns a run COUNT only on success, never how many it had
            // claimed so far), and `Scheduler::tick()` claims up to a whole batch in
            // one `claim_due` call before driving any of them — so an abandoned tick
            // could be holding anywhere from zero (cancelled mid-claim, before any
            // row was claimed — confirmed against a live database: the cancelled
            // claim query reported `rows_affected=0`) up to a full batch. Naming a
            // specific number here would be false precision.
            return Ok(Outcome::ok(format!(
                "shutdown (tick abandoned): {woken_total} run(s) woken this session; \
                 the in-flight tick was abandoned before it finished — any run(s) it had \
                 claimed remain 'waking', and the scheduler lease will reclaim them"
            )));
        };

        match tick_result {
            Ok(n) => {
                consecutive_failures = 0;
                woken_total += n;
                if n > 0 {
                    tracing::info!(woken = n, "tick woke runs");
                }
            }
            Err(e) => {
                consecutive_failures += 1;
                // Loud, with the full chain — never swallowed.
                tracing::error!(
                    error = %e,
                    consecutive = consecutive_failures,
                    "store fault during tick"
                );
                if opts.once {
                    // No next attempt to retry on: the exit code IS the health
                    // signal for a cron job or liveness probe, so a fault must
                    // be fatal on the very first failure, not deferred to the
                    // cap (which only makes sense across repeated attempts).
                    return Err(CliError::from(e));
                }
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                    return Err(CliError::error(format!(
                        "giving up after {consecutive_failures} consecutive store faults: {e}"
                    )));
                }
            }
        }

        if opts.once {
            return Ok(Outcome::ok(format!(
                "tick complete: {woken_total} run(s) woken"
            )));
        }

        // A signal landed during that tick and was allowed to finish gracefully —
        // honor it now rather than starting another tick or sleeping.
        if *shutdown.borrow() >= 1 {
            return Ok(Outcome::ok(format!(
                "shutdown: {woken_total} run(s) woken this session"
            )));
        }

        // Back off on failure so a dead database is not hammered; otherwise poll.
        let delay = if consecutive_failures == 0 {
            opts.interval
        } else {
            opts.interval
                * 2u32.saturating_pow(consecutive_failures.min(MAX_CONSECUTIVE_FAILURES - 1))
        };

        tokio::select! {
            _ = shutdown.changed() => {
                return Ok(Outcome::ok(format!(
                    "shutdown: {woken_total} run(s) woken this session"
                )));
            }
            _ = tokio::time::sleep(delay) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A shutdown receiver that never reaches level 1 — for tests where shutdown
    /// must not interrupt the loop within the test's own lifetime. The sender is
    /// returned too and MUST be kept alive by the caller: dropping it closes the
    /// channel, and a closed `watch::Receiver::changed()` resolves immediately
    /// (as an `Err`), which `serve`'s `select!` would misread as a signal.
    fn never_shuts_down() -> (
        tokio::sync::watch::Sender<u64>,
        tokio::sync::watch::Receiver<u64>,
    ) {
        tokio::sync::watch::channel(0u64)
    }

    struct FakeTicker {
        calls: AtomicUsize,
        /// Return Err for the first `fail_first` calls, then Ok(0).
        fail_first: usize,
        always_fail: bool,
    }

    impl FakeTicker {
        fn ok() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                fail_first: 0,
                always_fail: false,
            }
        }
        fn failing_forever() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                fail_first: 0,
                always_fail: true,
            }
        }
        fn failing_then_ok(n: usize) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                fail_first: n,
                always_fail: false,
            }
        }
    }

    #[async_trait::async_trait]
    impl Ticker for FakeTicker {
        async fn tick(&self) -> Result<usize, OrchestratorError> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.always_fail || n < self.fail_first {
                return Err(OrchestratorError::Store("pool timeout".into()));
            }
            Ok(1)
        }
    }

    #[test]
    fn parse_interval_accepts_seconds_minutes_and_millis() {
        assert_eq!(parse_interval("5s"), Ok(Duration::from_secs(5)));
        assert_eq!(parse_interval("2m"), Ok(Duration::from_secs(120)));
        assert_eq!(parse_interval("500ms"), Ok(Duration::from_millis(500)));
    }

    #[test]
    fn parse_interval_rejects_garbage_loudly() {
        assert!(parse_interval("soon").is_err());
        assert!(parse_interval("5").is_err(), "a bare number has no unit");
        assert!(parse_interval("").is_err());
    }

    /// A zero poll interval has no floor to stop it hammering the store as fast
    /// as the runtime can cycle — the exact failure the backoff exists to
    /// prevent, just on the happy path instead of the failure path.
    #[test]
    fn parse_interval_rejects_a_zero_interval() {
        assert!(parse_interval("0s").is_err());
        assert!(parse_interval("0ms").is_err());
        assert!(parse_interval("0m").is_err());
    }

    /// `v * 60` for minutes must not silently overflow (a release-mode wraparound
    /// producing a bogus Duration) or panic (a debug-mode overflow check abort) —
    /// it must fail cleanly like every other invalid input.
    #[test]
    fn parse_interval_rejects_a_minutes_overflow_instead_of_panicking() {
        assert!(parse_interval("308000000000000000m").is_err());
    }

    #[tokio::test]
    async fn once_runs_exactly_one_tick() {
        let t = FakeTicker::ok();
        let (_tx, rx) = never_shuts_down();
        let out = serve(
            &t,
            ServeOpts {
                interval: Duration::from_secs(5),
                once: true,
            },
            rx,
        )
        .await
        .expect("serves");
        assert_eq!(t.calls.load(Ordering::SeqCst), 1);
        assert_eq!(out.code, crate::errors::EXIT_OK);
    }

    /// `--once` is precisely the mode a cron job or a health check uses, where
    /// the exit code IS the entire signal. A store fault here must NOT be
    /// reported as success just because the single-tick budget was spent.
    #[tokio::test]
    async fn once_propagates_a_store_fault_instead_of_reporting_success() {
        let t = FakeTicker::failing_forever();
        let (_tx, rx) = never_shuts_down();
        let err = serve(
            &t,
            ServeOpts {
                interval: Duration::from_secs(5),
                once: true,
            },
            rx,
        )
        .await
        .expect_err("a store fault during --once must be fatal, not reported as success");
        assert_eq!(err.code, crate::errors::EXIT_ERROR);
        assert_eq!(
            t.calls.load(Ordering::SeqCst),
            1,
            "--once must not retry — one tick, one fault, fatal immediately"
        );
    }

    /// A transient store fault must NOT kill the worker — Postgres failover is
    /// survivable.
    #[tokio::test(start_paused = true)]
    async fn a_transient_store_fault_is_retried_not_fatal() {
        let t = FakeTicker::failing_then_ok(2);
        let (tx, rx) = tokio::sync::watch::channel(0u64);
        let handle = tokio::spawn(async move {
            // Stop once we have seen a success after the failures.
            tokio::time::sleep(Duration::from_secs(60)).await;
            tx.send_modify(|n| *n += 1);
        });
        let out = serve(
            &t,
            ServeOpts {
                interval: Duration::from_millis(10),
                once: false,
            },
            rx,
        )
        .await
        .expect("survives transient faults");
        handle.abort();
        assert_eq!(out.code, crate::errors::EXIT_OK);
        assert!(
            t.calls.load(Ordering::SeqCst) > 2,
            "must have retried past the failures: {}",
            t.calls.load(Ordering::SeqCst)
        );
    }

    /// A persistently dead database must NOT be silently tolerated.
    #[tokio::test(start_paused = true)]
    async fn a_persistent_store_fault_exits_non_zero_after_the_cap() {
        let t = FakeTicker::failing_forever();
        let (_tx, rx) = never_shuts_down();
        let err = serve(
            &t,
            ServeOpts {
                interval: Duration::from_millis(10),
                once: false,
            },
            rx,
        )
        .await
        .expect_err("a dead store must be fatal eventually");
        assert_eq!(err.code, crate::errors::EXIT_ERROR);
        assert!(err.message.contains("consecutive"), "{}", err.message);
        assert_eq!(
            t.calls.load(Ordering::SeqCst),
            MAX_CONSECUTIVE_FAILURES as usize,
            "exactly the cap, then give up"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_stops_the_loop_cleanly() {
        let t = FakeTicker::ok();
        let (tx, rx) = tokio::sync::watch::channel(0u64);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            tx.send_modify(|n| *n += 1);
        });
        let out = serve(
            &t,
            ServeOpts {
                interval: Duration::from_secs(1),
                once: false,
            },
            rx,
        )
        .await
        .expect("clean shutdown");
        assert_eq!(out.code, crate::errors::EXIT_OK);
        assert!(t.calls.load(Ordering::SeqCst) >= 1);
    }

    /// Fails on 4 of every 9 calls, then recovers — never 5 *consecutive*
    /// failures, but well past `MAX_CONSECUTIVE_FAILURES` failures in total
    /// across the run. This discriminates the `consecutive_failures = 0` reset
    /// on success from a monotonic (never-reset) counter: every other test here
    /// is monotonic (always ok, only fails then permanently recovers, or always
    /// fails), so none of them would notice a dropped or misplaced reset.
    struct PeriodicFlakyTicker {
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Ticker for PeriodicFlakyTicker {
        async fn tick(&self) -> Result<usize, OrchestratorError> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n % 9 < 4 {
                Err(OrchestratorError::Store("pool timeout".into()))
            } else {
                Ok(1)
            }
        }
    }

    /// Proves the reset: without it, failures accumulate across the periodic
    /// dips instead of clearing on each recovery, and the cap trips after ~10
    /// calls even though no single streak ever reaches 5 in a row.
    #[tokio::test(start_paused = true)]
    async fn a_periodic_recovering_failure_pattern_never_trips_the_cap() {
        let t = PeriodicFlakyTicker {
            calls: AtomicUsize::new(0),
        };
        let (tx, rx) = tokio::sync::watch::channel(0u64);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            tx.send_modify(|n| *n += 1);
        });
        let out = serve(
            &t,
            ServeOpts {
                interval: Duration::from_millis(10),
                once: false,
            },
            rx,
        )
        .await
        .expect("a periodic, self-recovering failure pattern must never trip the cap");
        assert_eq!(out.code, crate::errors::EXIT_OK);
        assert!(
            t.calls.load(Ordering::SeqCst) > 27,
            "must have survived multiple 9-call cycles: {}",
            t.calls.load(Ordering::SeqCst)
        );
    }

    /// A ticker whose `tick()` never returns on its own — the genuine shape of an
    /// unbounded tick blocked on, e.g., `LOCK TABLE … ACCESS EXCLUSIVE`. Notifies
    /// `started` right before parking forever, so a test can wait until the tick is
    /// GENUINELY in flight before racing signals against it.
    struct BlockingTicker {
        started: std::sync::Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl Ticker for BlockingTicker {
        async fn tick(&self) -> Result<usize, OrchestratorError> {
            self.started.notify_one();
            std::future::pending::<()>().await;
            unreachable!("pending() never resolves");
        }
    }

    /// A tick's duration is unbounded (it drives runs inline), so the FIRST signal
    /// cannot interrupt it. The second must, or an operator watching a slow tick has
    /// no way to stop the process short of SIGKILL.
    #[tokio::test(start_paused = true)]
    async fn a_second_signal_abandons_an_in_flight_tick() {
        let started = std::sync::Arc::new(tokio::sync::Notify::new());
        let ticker = BlockingTicker {
            started: started.clone(),
        };
        let (tx, rx) = tokio::sync::watch::channel(0u64);

        let handle = tokio::spawn(async move {
            serve(
                &ticker,
                ServeOpts {
                    interval: Duration::from_secs(5),
                    once: false,
                },
                rx,
            )
            .await
        });

        // Wait until the tick has GENUINELY started (not just been scheduled).
        started.notified().await;

        // First signal: must NOT abandon the tick — it is allowed to finish (it
        // never will, here, which is exactly the point: `serve` must still be
        // running afterward).
        tx.send_modify(|n| *n += 1);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(
            !handle.is_finished(),
            "a single signal must not abandon an in-flight tick"
        );

        // Second signal: must abandon it and return promptly, without the tick ever
        // completing.
        tx.send_modify(|n| *n += 1);
        let out = handle
            .await
            .expect("the serve task must not panic")
            .expect("serve returns Ok even though the tick never finished");

        assert_eq!(out.code, crate::errors::EXIT_OK);
        assert!(
            out.text.contains("abandoned"),
            "the outcome must distinguish an abandoned tick from a clean shutdown: {}",
            out.text
        );
        assert!(out.text.contains("shutdown"), "{}", out.text);
    }
}
