//! SP-DATA-3: the durable-scheduler driver. Drives an injected [`Executor`] and records each run's
//! pause/terminal into a [`SchedulerStore`]; [`tick`](Scheduler::tick) atomically claims due wakes and
//! re-drives `Executor::start`. Reads the pause deadline from the durable journal — the `Executor` is
//! unchanged. Observe (`status`/`list_paused`) + intervene (`cancel`/`force_wake`) delegate to the store.
//!
//! A double-drive is harmless (idempotent resume: fold + memo, zero re-spend), so the store's atomic
//! `claim_due` prevents a thundering herd while a crash between drive and record self-heals on the next
//! tick.
//!
//! AG-3: a wake that keeps failing is not re-driven forever — each claimed wake is counted, a failed
//! or lost one is backed off ([`WakeRetryPolicy`]), and past `max_attempts` the run is filed `Failed`.
//!
//! "Zero re-spend" has one honest exception, and a retry must not widen it. A journal fault AFTER a
//! paid model call — the provider answered, the append of its `EffectRecorded { usage }` failed —
//! leaves a call the durable ledger never saw and no memo to replay, so a re-drive would buy it
//! again past every cap. The executor raises that as [`OrchestratorError::SpendUnrecorded`], which
//! is NOT retryable: the run is filed `Failed` naming the unrecorded spend, for an operator. A fault
//! before any paid dispatch stays retryable. What no classification can cover is a PROCESS crash in
//! the same window (between the provider's response and the append): the lost worker's lease is
//! reclaimed and the re-drive re-buys that one call — the pre-existing at-least-once edge
//! `durable-journal.md` states, bounded by `max_attempts` because every reclaim is a counted attempt.

use crate::executor::{Executor, RunOutcome};
use orchestrator_core::{
    Clock, ExecutionJournal, Graph, JournalError, JournalEvent, OrchestratorError, RunBudget,
    RunId, RunStatus, ScheduledRun, SchedulerStore, Seq, TokenBudget, WakeAttempt,
};
use std::sync::Arc;

const DEFAULT_LEASE_SECS: i64 = 60;
const CLAIM_BATCH: usize = 64;

/// AG-3: how [`Scheduler::tick`] retries a wake that failed, and when it gives up.
///
/// A wake FAILS when its drive returns a retryable error (a journal or store backend fault —
/// see [`Scheduler::tick`]) or when its worker is lost mid-drive and the stale lease is
/// reclaimed. Each failure re-schedules the run at `now + backoff(attempt)`; the attempt that
/// would exceed `max_attempts` is never driven — the run is recorded terminal-`Failed` with a
/// reason naming the attempt count and the last error. A successful drive resets the count.
#[derive(Debug, Clone, PartialEq)]
pub struct WakeRetryPolicy {
    /// Total wake attempts a run gets between successful drives (`0` is treated as `1`).
    /// Default 5.
    pub max_attempts: u32,
    /// The delay after the first failed attempt; it doubles per attempt. Default 30s.
    pub base_backoff: chrono::Duration,
    /// The ceiling the doubling delay is clamped to. Default 1h.
    pub max_backoff: chrono::Duration,
    /// Fraction of each delay that is jittered AWAY, clamped to `[0, 0.5]` — a delay `d` becomes
    /// a value in `[d·(1-jitter), d]`, so the spacing still strictly grows attempt over attempt.
    /// Default 0.2.
    pub jitter: f64,
    /// Keys the jitter. The jitter is a pure function of `(jitter_seed, run, attempt)` — no
    /// randomness — so a deadline is reproducible, and runs that failed together (one outage)
    /// retry spread apart rather than in lock-step. Default 0.
    pub jitter_seed: u64,
}

impl Default for WakeRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_backoff: chrono::Duration::seconds(30),
            max_backoff: chrono::Duration::hours(1),
            jitter: 0.2,
            jitter_seed: 0,
        }
    }
}

impl WakeRetryPolicy {
    /// The delay before retrying `run` after its wake attempt `attempt` (1-based) failed:
    /// `base_backoff · 2^(attempt-1)`, clamped to `max_backoff`, less the deterministic jitter.
    pub fn backoff(&self, run: RunId, attempt: u32) -> chrono::Duration {
        let base = self.base_backoff.num_milliseconds().max(0);
        let max = self.max_backoff.num_milliseconds().max(0);
        let shift = attempt.saturating_sub(1).min(62);
        let full = base.checked_mul(1i64 << shift).unwrap_or(i64::MAX).min(max);
        let jitter = if self.jitter.is_nan() {
            0.0
        } else {
            self.jitter.clamp(0.0, 0.5)
        };
        let cut = (full as f64 * jitter * unit_interval(self.jitter_seed, run, attempt)) as i64;
        chrono::Duration::milliseconds(full - cut.clamp(0, full))
    }

    fn cap(&self) -> u32 {
        self.max_attempts.max(1)
    }

    /// `now + backoff(run, attempt)`, SATURATING at the end of `DateTime<Utc>`'s range: an
    /// overflow panic here would fire inside `tick` on every claim of the run — a poison pill
    /// of its own. A saturated deadline parks the run: `cancel` always reaches it, and
    /// `force_wake` reaches it once it is `paused` (a failed drive re-schedules it `paused`; a
    /// LOST drive leaves it `waking`, where only `cancel` applies — reachable only with a
    /// backoff configured past the year 262143).
    fn retry_at(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        run: RunId,
        attempt: u32,
    ) -> chrono::DateTime<chrono::Utc> {
        now.checked_add_signed(self.backoff(run, attempt))
            .unwrap_or(chrono::DateTime::<chrono::Utc>::MAX_UTC)
    }
}

/// A deterministic value in `[0, 1)` keyed by `(seed, run, attempt)` — SplitMix64 finalisers
/// over the inputs, so it is stable across builds and toolchains (unlike `DefaultHasher`).
fn unit_interval(seed: u64, run: RunId, attempt: u32) -> f64 {
    fn mix(mut z: u64) -> u64 {
        z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    let (hi, lo) = run.0.as_u64_pair();
    let h = mix(mix(mix(seed ^ hi) ^ lo) ^ u64::from(attempt));
    (h >> 11) as f64 / (1u64 << 53) as f64
}

/// AG-3: the drive errors a retry can plausibly cure — a journal or store BACKEND fault (a
/// database blip, a dropped connection). Every other drive error is deterministic for this
/// run and this build (a config or `format_version` fence, a determinism violation, an invalid
/// graph, …): retrying cannot change its answer, so it stays terminal at once, exactly as
/// before AG-3, rather than delaying the operator's signal by the whole backoff budget. An
/// ALLOWLIST, so a new error variant defaults to terminal rather than to a retry loop.
///
/// [`OrchestratorError::SpendUnrecorded`] is deliberately absent even though a backend fault
/// usually causes it: it means a PAID call's spend never reached the journal, and a retry
/// would re-dispatch and re-pay that call outside the cap.
fn is_retryable(e: &OrchestratorError) -> bool {
    matches!(
        e,
        OrchestratorError::Journal(JournalError::Backend(_)) | OrchestratorError::Store(_)
    )
}

/// Drives paused runs to their durable wakes over a [`SchedulerStore`].
pub struct Scheduler {
    store: Arc<dyn SchedulerStore>,
    executor: Executor,
    journal: Arc<dyn ExecutionJournal>,
    clock: Arc<dyn Clock>,
    lease: chrono::Duration,
    retry: WakeRetryPolicy,
}

impl Scheduler {
    /// The executor this scheduler drives. A test seam, not public API — gated so it
    /// cannot become one by accident. `Scheduler` takes the `Executor` by value, so a
    /// caller that built one (`torii::boot::heavy`) has no other way to assert what it
    /// wired.
    #[cfg(any(test, feature = "test-support"))]
    pub fn executor(&self) -> &Executor {
        &self.executor
    }

    /// A scheduler over `store`, driving `executor`, reading pause deadlines from `journal` (the SAME
    /// journal the executor holds), timed by `clock`. Default lease 60s (stale-`waking` reclaim window).
    pub fn new(
        store: Arc<dyn SchedulerStore>,
        executor: Executor,
        journal: Arc<dyn ExecutionJournal>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            store,
            executor,
            journal,
            clock,
            lease: chrono::Duration::seconds(DEFAULT_LEASE_SECS),
            retry: WakeRetryPolicy::default(),
        }
    }

    pub fn with_lease(mut self, lease: chrono::Duration) -> Self {
        self.lease = lease;
        self
    }

    /// AG-3: replace the default [`WakeRetryPolicy`].
    pub fn with_wake_retry(mut self, retry: WakeRetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Enqueue the graph, drive a FRESH run, record the outcome. Returns the [`RunOutcome`].
    /// Unbudgeted — delegates to [`submit_budgeted`](Self::submit_budgeted) with
    /// `None`, so every existing caller stays byte-identical.
    pub async fn submit(&self, run: RunId, graph: Graph) -> Result<RunOutcome, OrchestratorError> {
        self.submit_budgeted(run, graph, None).await
    }

    /// SP-DATA-5 Task 5: like [`submit`](Self::submit), but journals a per-run token
    /// cap on `RunStarted` (via [`Executor::run_budgeted`]) — the operator-facing
    /// `torii run submit --budget-tokens N` path.
    pub async fn submit_budgeted(
        &self,
        run: RunId,
        graph: Graph,
        budget: Option<TokenBudget>,
    ) -> Result<RunOutcome, OrchestratorError> {
        self.submit_with_budget(
            run,
            graph,
            RunBudget {
                tokens: budget,
                money: None,
            },
        )
        .await
    }

    /// AG-12: like [`submit_budgeted`](Self::submit_budgeted), with a token cap, a money
    /// cap, both or neither (via [`Executor::run_with_budget`]) — the path torii takes
    /// once it derives a run's dollar limit from its caps (torii#41/#49).
    pub async fn submit_with_budget(
        &self,
        run: RunId,
        graph: Graph,
        budget: RunBudget,
    ) -> Result<RunOutcome, OrchestratorError> {
        self.store.enqueue(run, &graph, self.clock.now()).await?;
        let since = match self.watermark(run).await {
            Ok(s) => s,
            // The row is already enqueued, so returning here without recording would leave
            // it durably unclassified. See `tick` for why a journal fault is a DRIVE
            // failure and not a store one.
            Err(e) => {
                let outcome = Err(e);
                self.record(run, 0, &outcome).await?;
                return outcome;
            }
        };
        // SP-OPS-1.4: `submit` drives inline and holds the row `waking` for the whole drive, so
        // it is a driver like any other and must exclude concurrent ones. A fresh run id is
        // rarely contended — `enqueue` already rejects a duplicate — but taking the lock here is
        // what makes "exactly one driver per run" a property of the scheduler rather than of
        // which entry point happened to be used.
        let Some(lock) = self.store.try_lock_run(run).await? else {
            return Err(OrchestratorError::Store(format!(
                "run {} is already being driven",
                run.0
            )));
        };
        let outcome = self.executor.run_with_budget(run, &graph, budget).await;
        let recorded = self.record(run, since, &outcome).await;
        lock.release().await?;
        recorded?;
        outcome
    }

    /// Claim due wakes and re-drive each via `Executor::start`; record each outcome; return the count
    /// woken. A STORE failure aborts loudly; a drive's own failure is recorded, not propagated.
    ///
    /// AG-3: every claimed wake is counted ([`SchedulerStore::begin_wake_attempt`]) before it is
    /// driven, arming a retry deadline so a drive whose worker dies is reclaimed only after its
    /// backoff. A RETRYABLE drive error (a journal/store backend fault) re-schedules the run at
    /// `now + backoff`; a deterministic one stays terminal-`Failed` at once. The attempt that
    /// would exceed [`WakeRetryPolicy::max_attempts`] is not driven: the run is recorded `Failed`
    /// with a reason naming the count and the last error. A store that does not count attempts
    /// (`begin_wake_attempt` → `None`) gets the pre-AG-3 behaviour unchanged.
    pub async fn tick(&self) -> Result<usize, OrchestratorError> {
        let due = self
            .store
            .claim_due(self.clock.now(), self.lease, CLAIM_BATCH)
            .await?;
        let mut driven = 0usize;
        for (run, graph) in due {
            // SP-OPS-1.4: the exclusive hold, taken BEFORE any drive work. `None` ⇒ another
            // worker is mid-drive on this run, so skip it entirely — do NOT record anything,
            // because that drive owns the outcome and `record_*` is conditional on `waking`.
            // The row keeps its fresh `claimed_at`, which is now inert: the lock, not the
            // lease, is what excludes.
            let Some(lock) = self.store.try_lock_run(run).await? else {
                continue;
            };
            // AG-3: count the attempt and arm its retry BEFORE any drive work, so a drive that
            // takes its worker down is already counted and already spaced out.
            let now = self.clock.now();
            let schedule = |attempt: u32| self.retry.retry_at(now, run, attempt);
            let attempt = match self.store.begin_wake_attempt(run, &schedule).await {
                Ok(a) => a,
                Err(e) => {
                    lock.release().await?;
                    return Err(e);
                }
            };
            if let Some(a) = attempt.as_ref().filter(|a| a.attempt > self.retry.cap()) {
                let last = a.last_error.as_deref().unwrap_or(
                    "the drive never recorded an outcome — its worker was lost mid-drive and \
                     its lease was reclaimed",
                );
                let reason = format!(
                    "gave up after {} failed wake attempts; last error: {last}",
                    a.attempt - 1
                );
                let recorded = self
                    .store
                    .record_terminal(run, RunStatus::Failed, Some(&reason))
                    .await;
                lock.release().await?;
                recorded?;
                driven += 1;
                continue;
            }
            // A journal that will not load is a DRIVE failure, not a store failure, and the
            // distinction is the whole contract above. `?`-ing it here aborted the entire
            // CLAIMED batch on one bad run: the run was never recorded terminal, so
            // `claim_due` left its `next_wake` in the past and reclaimed its stale `waking`
            // lease on every later tick — a poison pill — while every run behind it in the
            // batch sat undriven and `worker serve` exited on `MAX_CONSECUTIVE_FAILURES`.
            // One run whose durable `format_version` a rolling deploy had bumped therefore
            // stalled the whole paused fleet, which is the same blast radius the same
            // fence produced in `torii run list-paused`.
            //
            // Before this slice there was no pre-drive load and the drive's own `load` hit
            // the identical error, which `record`'s `Err` arm filed terminal-`Failed`; that
            // is the behaviour restored here. `since` is unread on the `Err` arm, so `0` is
            // inert rather than a window claim. (AG-3: a RETRYABLE load fault is now backed
            // off instead — see `record_wake`; the `format_version` fence is not retryable.)
            let since = match self.watermark(run).await {
                Ok(s) => s,
                Err(e) => {
                    let recorded = self.record_wake(run, 0, &Err(e), attempt.as_ref()).await;
                    lock.release().await?;
                    recorded?;
                    driven += 1;
                    continue;
                }
            };
            let outcome = self.executor.start(run, &graph).await;
            let recorded = self
                .record_wake(run, since, &outcome, attempt.as_ref())
                .await;
            // Release before propagating: a store fault must not also strand the run behind a
            // lock that outlives this tick.
            lock.release().await?;
            recorded?;
            driven += 1;
        }
        Ok(driven)
    }

    pub async fn status(&self, run: RunId) -> Result<Option<ScheduledRun>, OrchestratorError> {
        self.store.status(run).await
    }
    pub async fn list_paused(&self) -> Result<Vec<ScheduledRun>, OrchestratorError> {
        self.store.list_paused().await
    }
    pub async fn cancel(&self, run: RunId) -> Result<(), OrchestratorError> {
        self.store.cancel(run).await
    }
    pub async fn force_wake(&self, run: RunId) -> Result<(), OrchestratorError> {
        self.store.force_wake(run, self.clock.now()).await
    }

    /// AG-3: classify a WAKE's drive result. A retryable error on a counted attempt is backed
    /// off (`record_wake_failed` at `now + backoff`), or — on the last allowed attempt — filed
    /// terminal-`Failed` naming the count and the error. Everything else is [`record`](Self::record)
    /// exactly as before, including every outcome on a store that does not count attempts.
    async fn record_wake(
        &self,
        run: RunId,
        since: Seq,
        outcome: &Result<RunOutcome, OrchestratorError>,
        attempt: Option<&WakeAttempt>,
    ) -> Result<(), OrchestratorError> {
        if let (Err(e), Some(a)) = (outcome, attempt)
            && is_retryable(e)
        {
            let cap = self.retry.cap();
            if a.attempt >= cap {
                let reason = format!(
                    "gave up after {} failed wake attempts; last error: {e}",
                    a.attempt
                );
                return self
                    .store
                    .record_terminal(run, RunStatus::Failed, Some(&reason))
                    .await;
            }
            let retry_at = self.retry.retry_at(self.clock.now(), run, a.attempt);
            let error = format!(
                "wake attempt {} of {cap} failed (retrying at {retry_at}): {e}",
                a.attempt
            );
            return self.store.record_wake_failed(run, retry_at, &error).await;
        }
        self.record(run, since, outcome).await
    }

    /// Classify a drive result into the store. A drive's own error (e.g. a config-fence mismatch) is
    /// recorded terminal-`Failed` (loud in the store, not propagated); only a STORE failure returns `Err`.
    ///
    /// `since` is the journal watermark taken BEFORE the drive — see
    /// [`earliest_resume_after`](Self::earliest_resume_after), which needs it to tell this drive's
    /// pauses from every pause the run has ever taken.
    async fn record(
        &self,
        run: RunId,
        since: Seq,
        outcome: &Result<RunOutcome, OrchestratorError>,
    ) -> Result<(), OrchestratorError> {
        match outcome {
            Ok(o) if o.paused.is_some() => {
                let next_wake = self.earliest_resume_after(run, since).await?;
                let reason = o
                    .paused
                    .as_ref()
                    .map(|p| p.reason.clone())
                    .unwrap_or_default();
                self.store.record_paused(run, next_wake, &reason).await
            }
            Ok(o) if o.failed.is_some() => {
                let reason = o.failed.as_ref().map(|(_, m)| m.as_str());
                self.store
                    .record_terminal(run, RunStatus::Failed, reason)
                    .await
            }
            Ok(_) => {
                self.store
                    .record_terminal(run, RunStatus::Completed, None)
                    .await
            }
            Err(e) => {
                let reason = if matches!(e, OrchestratorError::VersionFenceMismatch { .. }) {
                    format!("stale: config changed ({e})")
                } else {
                    e.to_string()
                };
                self.store
                    .record_terminal(run, RunStatus::Failed, Some(&reason))
                    .await
            }
        }
    }

    /// The journal's high-water [`Seq`] for `run` right now — the boundary a drive's own events
    /// begin after. `0` for a run with nothing journaled yet (a fresh `submit`).
    ///
    /// Taken from `max`, not `last()`: the trait promises no ordering, and a boundary that is
    /// accidentally too LOW would silently re-admit older pauses into the window below.
    async fn watermark(&self, run: RunId) -> Result<Seq, OrchestratorError> {
        let events = self
            .journal
            .load(run)
            .await
            .map_err(OrchestratorError::Journal)?;
        Ok(events.iter().map(|(seq, _)| *seq).max().unwrap_or(0))
    }

    /// The EARLIEST non-`None` `RunPaused.resume_after` journaled by **this drive** (events with
    /// `Seq > since`) — the instant the scheduler must wake this run at.
    ///
    /// **Earliest, not last.** `drive` runs every ready node in a round even after one pauses, so a
    /// single drive can journal several `RunPaused` events; taking the last and `flatten()`ing it
    /// made `next_wake` depend on which pause happened to come last in graph declaration order. A
    /// deadline-less `AwaitSignal` declared after a timed one — two parallel human gates, one with
    /// an SLA and one without, which is a first-class HITL shape — nulled the timed gate's wake
    /// entirely: the run then sat `paused` with `next_wake` NULL and the deadline fired only if a
    /// human answered the OTHER gate. A budget pause or an in-doubt Mutation pause landing after a
    /// timed one does the same thing, which is why this is fixed here, for all pause classes, and
    /// not inside any one node kind. The earliest deadline is also the only safe choice: waking
    /// EARLY is free (a resume with nothing to do simply re-pauses, zero re-spend), where waking
    /// late means a deadline was missed.
    ///
    /// **This drive's, not the run's.** The `since` window is load-bearing in the other direction:
    /// a deadline from an EARLIER drive is, by definition, one this run has already been woken for,
    /// and it is almost always in the past. Re-adopting it would set a `next_wake` that every
    /// single `tick()` claims — a hot loop re-driving the run forever. `None` (no pause this drive
    /// carried a deadline) is SP-DATA-3's never-auto-woken class: correct, and the HOTL path.
    ///
    /// Every pause path journals its own `RunPaused` before returning (the gateway gate, the budget
    /// refusal, the in-doubt reconcile, and `AwaitSignal`), so a paused drive always has at least
    /// one event in this window.
    async fn earliest_resume_after(
        &self,
        run: RunId,
        since: Seq,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, OrchestratorError> {
        let events = self
            .journal
            .load_since(run, since)
            .await
            .map_err(OrchestratorError::Journal)?;
        Ok(events
            .iter()
            .filter_map(|(_, e)| match e {
                JournalEvent::RunPaused { resume_after, .. } => *resume_after,
                _ => None,
            })
            .min())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn run(n: u128) -> RunId {
        RunId(uuid::Uuid::from_u128(n))
    }

    fn no_jitter() -> WakeRetryPolicy {
        WakeRetryPolicy {
            jitter: 0.0,
            ..WakeRetryPolicy::default()
        }
    }

    #[test]
    fn the_default_policy_caps_at_five_attempts() {
        assert_eq!(WakeRetryPolicy::default().max_attempts, 5);
    }

    #[test]
    fn backoff_doubles_from_the_base_and_clamps_at_the_max() {
        let p = no_jitter();
        let r = run(1);
        assert_eq!(p.backoff(r, 1), Duration::seconds(30), "attempt 1 → base");
        assert_eq!(p.backoff(r, 2), Duration::seconds(60), "attempt 2 → 2×base");
        assert_eq!(
            p.backoff(r, 3),
            Duration::seconds(120),
            "attempt 3 → 4×base"
        );
        assert_eq!(p.backoff(r, 8), Duration::seconds(3600), "clamped to max");
        assert_eq!(
            p.backoff(r, u32::MAX),
            Duration::seconds(3600),
            "a huge attempt number clamps instead of overflowing"
        );
    }

    /// The jitter is a pure function of (seed, run, attempt): a test asserting a deadline gets
    /// the same one every time, and two runs that failed together do not retry together.
    #[test]
    fn jitter_is_deterministic_bounded_and_spreads_runs_apart() {
        let p = WakeRetryPolicy::default(); // jitter 0.2
        for attempt in 1..=5 {
            let full = no_jitter().backoff(run(0), attempt);
            let floor = full - full * 2 / 10;
            for n in 0..64 {
                let d = p.backoff(run(n), attempt);
                assert_eq!(d, p.backoff(run(n), attempt), "deterministic");
                assert!(
                    d <= full && d >= floor,
                    "attempt {attempt}: {d} outside [{floor}, {full}]"
                );
            }
        }
        let distinct: std::collections::HashSet<_> =
            (0..64).map(|n| p.backoff(run(n), 1)).collect();
        assert!(
            distinct.len() > 32,
            "jitter must spread runs apart, got {} distinct delays of 64",
            distinct.len()
        );
        let reseeded = WakeRetryPolicy {
            jitter_seed: 7,
            ..WakeRetryPolicy::default()
        };
        assert!(
            (0..64).any(|n| reseeded.backoff(run(n), 1) != p.backoff(run(n), 1)),
            "the seed keys the jitter"
        );
    }

    /// Jitter only ever SHORTENS a delay, by at most `jitter` (≤ ½), so the spacing between a
    /// run's attempts still strictly grows until the clamp.
    #[test]
    fn jittered_spacing_still_grows_attempt_over_attempt() {
        let p = WakeRetryPolicy::default();
        for n in 0..64 {
            let delays: Vec<_> = (1..=5).map(|a| p.backoff(run(n), a)).collect();
            assert!(
                delays.windows(2).all(|w| w[0] < w[1]),
                "run {n}: {delays:?} must strictly grow"
            );
        }
    }
}
