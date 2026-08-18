//! SP-DATA-3: the durable-scheduler seam. A [`SchedulerStore`] owns each run's original graph + its
//! wake-schedule; the `Scheduler` driver (in the `orchestrator` crate) drives it — `submit`ting runs,
//! recording their pauses, and on `tick()` atomically claiming due wakes to re-drive `Executor::start`.
//! Backend-agnostic (an `InMemory` + a `Postgres` impl), like [`ExecutionJournal`](crate::ExecutionJournal).

use crate::error::OrchestratorError;
use crate::graph::Graph;
use crate::ids::RunId;
use chrono::{DateTime, Duration, Utc};

/// A scheduled run's lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RunStatus {
    /// In-flight — either `submit`'s initial drive or a claimed wake; lease-protected.
    Waking,
    /// Awaiting a wake at `next_wake` (a NULL `next_wake` ⇒ needs `force_wake`).
    Paused,
    Completed,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunStatus::Waking => "waking",
            RunStatus::Paused => "paused",
            RunStatus::Completed => "completed",
            RunStatus::Failed => "failed",
            RunStatus::Cancelled => "cancelled",
        }
    }
    pub fn from_db_str(s: &str) -> Option<Self> {
        Some(match s {
            "waking" => RunStatus::Waking,
            "paused" => RunStatus::Paused,
            "completed" => RunStatus::Completed,
            "failed" => RunStatus::Failed,
            "cancelled" => RunStatus::Cancelled,
            _ => return None,
        })
    }
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled
        )
    }
}

/// The observe DTO (NOT the graph) — what `status`/`list_paused` return.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScheduledRun {
    pub run: RunId,
    pub status: RunStatus,
    pub next_wake: Option<DateTime<Utc>>,
    pub reason: Option<String>,
    pub updated_at: DateTime<Utc>,
}

/// A durable store of the scheduler's wake set — one row per submitted run holding its ORIGINAL graph
/// (so any process can re-drive `Executor::start(run, graph)` at the deadline) + its schedule/status.
///
/// The status transitions are: `enqueue` → `waking`; a paused drive → `record_paused` (`paused`); a
/// terminal drive → `record_terminal` (`completed`|`failed`); `cancel` → `cancelled`. `record_paused`/
/// `record_terminal` are CONDITIONAL on the row being `waking`, so a concurrent `cancel` wins and a
/// cancelled row is never resurrected.
#[async_trait::async_trait]
pub trait SchedulerStore: Send + Sync {
    /// Insert a NEW run as in-flight (`waking`), storing its graph + stamping `claimed_at=now`. A
    /// duplicate `run` id is a loud error (submit is once per run).
    async fn enqueue(
        &self,
        run: RunId,
        graph: &Graph,
        now: DateTime<Utc>,
    ) -> Result<(), OrchestratorError>;

    /// A drive PAUSED: `waking` → `paused` with `next_wake` (`None` ⇒ NULL, no timer). Conditional on
    /// the current status being `waking`; a no-op (not an error) otherwise.
    async fn record_paused(
        &self,
        run: RunId,
        next_wake: Option<DateTime<Utc>>,
        reason: &str,
    ) -> Result<(), OrchestratorError>;

    /// A drive ENDED: `waking` → `status` (`Completed`|`Failed`). Conditional on `waking`.
    async fn record_terminal(
        &self,
        run: RunId,
        status: RunStatus,
        reason: Option<&str>,
    ) -> Result<(), OrchestratorError>;

    /// Atomically claim up to `limit` due wakes — `(paused AND next_wake<=now)` OR a stale
    /// `(waking AND claimed_at < now-lease)` — flipping each to `waking`, stamping `claimed_at=now`,
    /// returning `(run, graph)`. A NULL `next_wake` is never claimed by the timer. This is both the
    /// exactly-once gate vs a fleet AND the crash-mid-wake reclaim.
    async fn claim_due(
        &self,
        now: DateTime<Utc>,
        lease: Duration,
        limit: usize,
    ) -> Result<Vec<(RunId, Graph)>, OrchestratorError>;

    /// Observe: the current record for `run`, if any.
    async fn status(&self, run: RunId) -> Result<Option<ScheduledRun>, OrchestratorError>;
    /// Observe: every currently-`paused` run (the operator's pending-wake view).
    async fn list_paused(&self) -> Result<Vec<ScheduledRun>, OrchestratorError>;

    /// Intervene: any NON-terminal status → `cancelled` (idempotent; a cancelled run is never woken).
    async fn cancel(&self, run: RunId) -> Result<(), OrchestratorError>;
    /// Intervene: a `paused` run → set `next_wake=now` so the next tick claims it regardless of the
    /// original deadline (the human-wake path for NULL-deadline pauses). Conditional on `paused`.
    async fn force_wake(&self, run: RunId, now: DateTime<Utc>) -> Result<(), OrchestratorError>;
}
