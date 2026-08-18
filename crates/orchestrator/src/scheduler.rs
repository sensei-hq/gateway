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
    ScheduledRun, SchedulerStore,
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
    pub async fn submit(&self, run: RunId, graph: Graph) -> Result<RunOutcome, OrchestratorError> {
        self.store.enqueue(run, &graph, self.clock.now()).await?;
        let outcome = self.executor.run(run, &graph).await;
        self.record(run, &outcome).await?;
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
            let outcome = self.executor.start(run, &graph).await;
            self.record(run, &outcome).await?;
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
    async fn record(
        &self,
        run: RunId,
        outcome: &Result<RunOutcome, OrchestratorError>,
    ) -> Result<(), OrchestratorError> {
        match outcome {
            Ok(o) if o.paused.is_some() => {
                let next_wake = self.last_resume_after(run).await?;
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

    /// The last journaled `RunPaused.resume_after` — the deadline the executor recorded on this pause.
    async fn last_resume_after(
        &self,
        run: RunId,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, OrchestratorError> {
        let events = self
            .journal
            .load(run)
            .await
            .map_err(OrchestratorError::Journal)?;
        Ok(events
            .iter()
            .rev()
            .find_map(|(_, e)| match e {
                JournalEvent::RunPaused { resume_after, .. } => Some(*resume_after),
                _ => None,
            })
            .flatten())
    }
}
