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

/// What [`SchedulerStore::begin_wake_attempt`] reports about the wake it just started (AG-3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakeAttempt {
    /// 1-based count of CONSECUTIVE wake attempts, this one included: every attempt since the
    /// run's last successful drive (a drive that recorded a pause or a terminal outcome). `1` is
    /// the first attempt after a success; `n > 1` means the `n - 1` before it all failed.
    pub attempt: u32,
    /// The error the PREVIOUS attempt recorded via
    /// [`record_wake_failed`](SchedulerStore::record_wake_failed). `None` with `attempt > 1`
    /// means the previous attempt never recorded anything — its worker was lost mid-drive and
    /// its lease was reclaimed. Always `None` when `attempt == 1`.
    pub last_error: Option<String>,
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
    /// `(waking AND claimed_at < now-lease AND (next_wake IS NULL OR next_wake<=now))` — flipping
    /// each to `waking`, stamping `claimed_at=now`, returning `(run, graph)`. A NULL `next_wake` on a
    /// PAUSED row is never claimed by the timer. This is both the exactly-once gate vs a fleet AND
    /// the crash-mid-wake reclaim.
    ///
    /// AG-3: on a `waking` row `next_wake` is the retry deadline
    /// [`begin_wake_attempt`](Self::begin_wake_attempt) armed, so a drive whose worker was lost is
    /// reclaimed only once BOTH its lease and its backoff have passed — a crash-looping run is
    /// spaced out exactly like one whose drive returned an error.
    async fn claim_due(
        &self,
        now: DateTime<Utc>,
        lease: Duration,
        limit: usize,
    ) -> Result<Vec<(RunId, Graph)>, OrchestratorError>;

    /// AG-3: a claimed wake is about to be driven — count it. Called by the driver right after it
    /// takes the run's [`RunLock`], before any drive work. Conditional on `waking`; returns
    /// `Ok(None)` for a run that is not `waking` (or unknown).
    ///
    /// The store keeps a per-run count of consecutive attempts:
    /// - `enqueue` starts it at `1` — `submit`'s inline drive is the run's first attempt;
    /// - this method adds one, arms `next_wake = retry_at(new_attempt)` (the stale-`waking` reclaim
    ///   deadline, see [`claim_due`](Self::claim_due) — `retry_at` is the driver's backoff schedule,
    ///   called once with the NEW attempt number), and TAKES the previous attempt's recorded error
    ///   (returning it, and clearing it so the next call can tell a lost attempt from a failed one);
    /// - a successful drive — [`record_paused`](Self::record_paused) — resets it to `0`;
    /// - [`force_wake`](Self::force_wake) and [`cancel`](Self::cancel) leave it alone (an operator's
    ///   "wake now" skips the backoff, not the cap).
    ///
    /// Defaulted to `Ok(None)` — "this store does not count attempts" — which the driver treats as
    /// the pre-AG-3 behaviour (no cap, no backoff, every drive error terminal), so a third-party
    /// backend keeps compiling and behaves exactly as before. The in-memory store overrides it;
    /// torii's `PgSchedulerStore` must too (with the rest of the delta the testkit's `scheduler`
    /// suite checks) — until it does, production keeps the pre-AG-3 crash loop.
    async fn begin_wake_attempt(
        &self,
        _run: RunId,
        _retry_at: &(dyn Fn(u32) -> DateTime<Utc> + Send + Sync),
    ) -> Result<Option<WakeAttempt>, OrchestratorError> {
        Ok(None)
    }

    /// AG-3: a wake's drive FAILED with a retryable error: `waking` → `paused` with
    /// `next_wake = retry_at`, `reason = error`, and `error` kept as the attempt's recorded error
    /// (handed back by the next [`begin_wake_attempt`](Self::begin_wake_attempt)). The attempt
    /// count is NOT changed — `begin_wake_attempt` already counted this attempt. Conditional on
    /// `waking`, so a concurrent `cancel` wins.
    ///
    /// Only called for a run whose `begin_wake_attempt` returned `Some`. The default files the run
    /// terminal-`Failed` (the pre-AG-3 treatment of a drive error), so a store that overrides only
    /// `begin_wake_attempt` fails loud rather than looping; override both together.
    async fn record_wake_failed(
        &self,
        run: RunId,
        _retry_at: DateTime<Utc>,
        error: &str,
    ) -> Result<(), OrchestratorError> {
        self.record_terminal(run, RunStatus::Failed, Some(error))
            .await
    }

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
