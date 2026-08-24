//! SP-DATA-3: the durable-scheduler driver. Drives an injected [`Executor`] and records each run's
//! pause/terminal into a [`SchedulerStore`]; [`tick`](Scheduler::tick) atomically claims due wakes and
//! re-drives `Executor::start`. Reads the pause deadline from the durable journal — the `Executor` is
//! unchanged. Observe (`status`/`list_paused`) + intervene (`cancel`/`force_wake`) delegate to the store.
//!
//! A double-drive is harmless (idempotent resume: fold + memo, zero re-spend), so the store's atomic
//! `claim_due` prevents a thundering herd while a crash between drive and record self-heals on the next
//! tick.

use crate::executor::{Executor, RunOutcome};
use orchestrator_core::{
    Clock, ExecutionJournal, Graph, JournalEvent, OrchestratorError, RunId, RunStatus,
    ScheduledRun, SchedulerStore, Seq, TokenBudget,
};
use std::sync::Arc;

const DEFAULT_LEASE_SECS: i64 = 60;
const CLAIM_BATCH: usize = 64;

/// Drives paused runs to their durable wakes over a [`SchedulerStore`].
pub struct Scheduler {
    store: Arc<dyn SchedulerStore>,
    executor: Executor,
    journal: Arc<dyn ExecutionJournal>,
    clock: Arc<dyn Clock>,
    lease: chrono::Duration,
}

impl Scheduler {
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
        }
    }

    pub fn with_lease(mut self, lease: chrono::Duration) -> Self {
        self.lease = lease;
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
        self.store.enqueue(run, &graph, self.clock.now()).await?;
        let since = self.watermark(run).await?;
        let outcome = self.executor.run_budgeted(run, &graph, budget).await;
        self.record(run, since, &outcome).await?;
        outcome
    }

    /// Claim due wakes and re-drive each via `Executor::start`; record each outcome; return the count
    /// woken. A STORE failure aborts loudly; a drive's own failure is recorded (terminal), not propagated.
    pub async fn tick(&self) -> Result<usize, OrchestratorError> {
        let due = self
            .store
            .claim_due(self.clock.now(), self.lease, CLAIM_BATCH)
            .await?;
        let n = due.len();
        for (run, graph) in due {
            let since = self.watermark(run).await?;
            let outcome = self.executor.start(run, &graph).await;
            self.record(run, since, &outcome).await?;
        }
        Ok(n)
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
