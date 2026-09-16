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
#[serde(rename_all = "lowercase")]
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

/// An exclusive hold on one run's drive (SP-OPS-1.4).
///
/// The lease alone could not provide this. `claimed_at` is stamped once at claim and never
/// renewed, and `tick` claims a batch — stamping every row at ONE instant — then drives them
/// serially, so the tail of a slow batch is past its 60s lease before it is even started and a
/// second worker reclaims and drives it CONCURRENTLY. The scheduler's own header called a
/// double-drive harmless, which is true of a *sequential* re-drive (the first drive's effects are
/// journaled, so the memo fences them) and false of a concurrent one: both drives load the journal
/// before either writes, so the memo has nothing to fence with and a plain `ModelCall` is
/// double-spent.
///
/// A lock makes that impossible rather than unlikely, and unlike a lease it needs no timing
/// assumption at all: the Postgres implementation is a SESSION-scoped advisory lock, so a worker
/// that is killed mid-drive has its lock released by Postgres when the connection dies — the
/// self-healing property the lease was approximating.
#[async_trait::async_trait]
pub trait RunLock: Send + Sync {
    /// Release the hold. Dropping without calling this MUST also release — a lock that leaks on a
    /// panic would strand the run until the process exits.
    async fn release(self: Box<Self>) -> Result<(), OrchestratorError>;
}

/// The lock a backend with exactly one driver hands out: nothing to exclude, nothing to release.
pub struct UncontendedRunLock;

#[async_trait::async_trait]
impl RunLock for UncontendedRunLock {
    async fn release(self: Box<Self>) -> Result<(), OrchestratorError> {
        Ok(())
    }
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
    /// Take the exclusive drive lock for `run` (SP-OPS-1.4). `Ok(None)` ⇒ another worker holds it
    /// and this one must NOT drive.
    ///
    /// Defaulted to "always granted" so a third-party backend keeps compiling and a
    /// single-process deployment needs no lock. Both shipped stores override it — the default is
    /// safe only where there is exactly one driver, which a backend author must decide, not
    /// inherit silently. Both overrides are exercised by tests.
    async fn try_lock_run(
        &self,
        _run: RunId,
    ) -> Result<Option<Box<dyn RunLock>>, OrchestratorError> {
        Ok(Some(Box::new(UncontendedRunLock)))
    }

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

    /// Observe: how many rows [`prune_terminal`](Self::prune_terminal) would delete at this exact
    /// `before`. The operator's PREVIEW — `torii run prune` shows this count and asks for
    /// confirmation before deleting anything.
    ///
    /// A separate method rather than a `dry_run: bool` on `prune_terminal` deliberately: a bool is
    /// easy to pass wrong, and a caller who inverts it DELETES when it meant to preview. Two
    /// methods cannot be confused for one another.
    async fn count_terminal_before(&self, before: DateTime<Utc>) -> Result<u64, OrchestratorError>;

    /// Delete TERMINAL rows (`completed`/`failed`/`cancelled`) whose `updated_at` is strictly older
    /// than `before`, returning the count actually deleted.
    ///
    /// **NEVER touches a non-terminal row**, at any age. A `paused` run has no age at which it
    /// becomes safe to forget — it is live work awaiting a wake, and the in-doubt-mutation class
    /// pauses with a NULL `next_wake` and waits INDEFINITELY for a human, so an old `paused` row is
    /// the NORM, not a leak. A `waking` row may be a live lease held by an in-flight drive in
    /// another process. Implementors must select terminal statuses by ALLOWLIST, never by excluding
    /// the known non-terminal ones, so an unrecognised status is kept rather than deleted.
    ///
    /// Required, NOT defaulted. A default would have to no-op, and a store that silently reports
    /// "0 deleted" while the table keeps growing is worse than one that fails to compile: the
    /// operator concludes retention is working. Every implementor must make this choice explicitly.
    async fn prune_terminal(&self, before: DateTime<Utc>) -> Result<u64, OrchestratorError>;
}
