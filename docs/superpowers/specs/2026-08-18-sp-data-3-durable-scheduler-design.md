---
title: SP-DATA-3 — durable scheduler (re-arm a paused run at resume_after)
doctype: design-spec
module: orchestrator
slice: SP-DATA-3
status: approved
date: 2026-08-18
---

# SP-DATA-3 — durable scheduler (re-arm a paused run at `resume_after`)

## 1. Summary

A durable control-plane driver that **wakes a paused run at its deadline** — in any
process, exactly-once, without re-spending tokens — turning a `RunPaused{resume_after}`
from a dead-end into a self-healing pause. A **`Scheduler`** (driver) sits over a durable
**`SchedulerStore`** (`scheduled_runs` table). The Scheduler owns the run's graph and
wake-schedule; the **`Executor` is unchanged** (the Scheduler calls `Executor::run`/`start`).

## 2. Motivation

Today a run can pause durably: `JournalEvent::RunPaused { reason, resume_after: Option<DateTime<Utc>> }`
(`crates/orchestrator-core/src/journal.rs:147`), and since SP-DATA-1 the journal is durable so
the pause survives a process crash. But **nothing durably wakes it** — a caller must manually call
`Executor::start(run, graph)` again at the deadline.

**The crux — graph recovery.** `Executor::start(run, graph)` takes the graph as a *caller parameter*,
and the graph is **not** in the journal. So a waker, given only a `run_id` at a deadline, has nothing
to call `start` with. The enabler: **`Graph` is `Serialize`/`Deserialize`** (`crates/orchestrator-core/src/graph.rs:245`),
so it can be persisted. For a runtime-`Expand` run the graph grows during execution, but only the
**original** submitted graph must be stored — `start` re-folds the journal's `PlanExpanded` events to
reconstruct expansions deterministically ("never re-planned").

**The correctness insight — waking is idempotent.** `Executor::start` already folds the journal + replays
the memo with **zero re-spend**, and an in-doubt Mutation goes through reconcile. So a *double*-wake (two
schedulers, or a crash mid-wake) is **safe** — re-driving re-runs nothing already recorded. Exactly-once
wake is therefore a *tidiness* concern (don't thundering-herd), not a *correctness* one — the executor's
determinism is the safety net beneath the claim mechanism.

## 3. Goals / Non-goals

**Goals**
- A durable `SchedulerStore` (`scheduled_runs` table) that owns each run's graph + wake-schedule.
- A `Scheduler` driver: `submit` a run, record its pause, and on `tick()` wake due runs (Clock-injected).
- Exactly-once wake under a fleet / crash via an atomic status-CAS claim + a lease (crash-reclaim).
- Basic HOTL control plane: observe (`status`, `list_paused`) + intervene (`cancel`, `force_wake`).
- Additive: default-off ⇒ byte-identical; the `Executor` unchanged (one tiny additive `PauseInfo` field).

**Non-goals (deferred, §9)**
- Rich scheduling policy (backoff/jitter/max-attempts/dead-letter).
- Pruning terminal rows; a production `run_forever` supervisor; LISTEN/NOTIFY low-latency wakes.
- The full management API/CLI (SP-DATA-4 builds on `list_paused`/`status`).
- Multi-tenant scheduling scope (the core is tenant-agnostic; tenancy is a wrapper).

## 4. Architecture & layering

Mirrors SP-DATA-1/2: the trait + DTOs are backend-agnostic in **`orchestrator-core`**; the
`InMemorySchedulerStore` + `PostgresSchedulerStore` live in **`orchestrator-store`**; the `Scheduler`
driver (which holds an injected `Executor` + `Clock`) lives in **`orchestrator`** (`src/scheduler.rs`).

```
   submit(run, graph)                         tick()  (poll, Clock-injected)
        │                                        │
        ▼                                        ▼
   enqueue graph ──► Executor::run ─┐     claim_due(now, lease) ──► Executor::start(run, stored_graph)
   (status 'waking')                │            (atomic CAS)                     │
        ┌───────────────────────────┘                                            │
        ▼   classify RunOutcome                                                   ▼  classify RunOutcome
   paused ─► record_paused(next_wake = resume_after)   completed/failed ─► record_terminal
        │
   scheduled_runs (durable): run_id, graph, status, next_wake, claimed_at, reason
        │
   observe: status(run) · list_paused()      intervene: cancel(run) · force_wake(run)
```

## 5. Schema (dbd, `orchestrator` schema)

```sql
-- ddl/table/orchestrator/scheduled_runs.sql
create table if not exists orchestrator.scheduled_runs (
    run_id     uuid        primary key,
    graph      jsonb       not null,   -- serde(Graph): the ORIGINAL submitted graph
    status     text        not null,   -- 'waking' | 'paused' | 'completed' | 'failed' | 'cancelled'
    next_wake  timestamptz,            -- auto-wake deadline; NULL = no timer (needs force_wake)
    claimed_at timestamptz,            -- lease stamp for 'waking' rows (crash-reclaim)
    reason     text,                   -- last pause/fail reason (observe surface)
    updated_at timestamptz not null default now()
);
create index if not exists scheduled_runs_due_idx
    on orchestrator.scheduled_runs (status, next_wake);
```
- Added to `_apply_all.sql` (inlined, idempotent, beside `ddl/` — SP-DATA-1 convention); `dbd-pattern-verifier` re-reviews.
- `status` stays `text` (not a Postgres enum) for parity with the jsonb-leaning config tables and to keep the state set editable without a migration; the adapter validates transitions in code.

## 6. `SchedulerStore` trait (core, backend-agnostic)

```rust
#[async_trait::async_trait]
pub trait SchedulerStore: Send + Sync {
    /// Insert a new run as in-flight ('waking'), storing its ORIGINAL graph + stamping the lease.
    async fn enqueue(&self, run: RunId, graph: &Graph, now: DateTime<Utc>) -> Result<(), OrchestratorError>;

    /// After a drive that PAUSED: 'waking' → 'paused' with `next_wake` (None ⇒ NULL, no timer).
    /// CONDITIONAL on the current status being 'waking' (so a concurrent `cancel` wins; the row is
    /// not resurrected if it was cancelled mid-drive).
    async fn record_paused(&self, run: RunId, next_wake: Option<DateTime<Utc>>, reason: &str)
        -> Result<(), OrchestratorError>;

    /// After a drive that ended: 'waking' → 'completed' | 'failed'. CONDITIONAL on 'waking'.
    async fn record_terminal(&self, run: RunId, status: RunStatus, reason: Option<&str>)
        -> Result<(), OrchestratorError>;

    /// Atomically claim up to `limit` due wakes — `(status='paused' AND next_wake<=now)` OR a stale
    /// `(status='waking' AND claimed_at < now-lease)` — flipping each to 'waking' + stamping
    /// `claimed_at=now`, `RETURNING (run_id, graph)`. This is the exactly-once gate vs a fleet AND
    /// the crash-reclaim. NULL `next_wake` is never claimed by the timer.
    async fn claim_due(&self, now: DateTime<Utc>, lease: chrono::Duration, limit: usize)
        -> Result<Vec<(RunId, Graph)>, OrchestratorError>;

    // observe
    async fn status(&self, run: RunId) -> Result<Option<ScheduledRun>, OrchestratorError>;
    async fn list_paused(&self) -> Result<Vec<ScheduledRun>, OrchestratorError>;

    // intervene
    /// Any non-terminal status → 'cancelled' (idempotent; a cancelled run is never woken).
    async fn cancel(&self, run: RunId) -> Result<(), OrchestratorError>;
    /// A 'paused' run → set `next_wake=now` so the next tick claims it regardless of the original
    /// deadline (the human-wake path for NULL-deadline pauses). CONDITIONAL on 'paused'.
    async fn force_wake(&self, run: RunId, now: DateTime<Utc>) -> Result<(), OrchestratorError>;
}

/// Terminal/observe status. `RunStatus` = { Waking, Paused, Completed, Failed, Cancelled }.
/// `ScheduledRun` = { run_id, status, next_wake, reason, updated_at } (the observe DTO — NOT the graph).
```

## 7. `Scheduler` driver (`orchestrator`)

```rust
pub struct Scheduler {
    store: Arc<dyn SchedulerStore>,
    executor: Executor,                 // cheaply cloned per drive (as run/start already do)
    journal: Arc<dyn ExecutionJournal>, // the SAME journal the executor holds — read the pause deadline
    clock: Arc<dyn Clock>,              // shared with the executor so 'now' is consistent + test-controllable
    lease: chrono::Duration,            // stale-'waking' reclaim window
}

impl Scheduler {
    pub fn new(store: Arc<dyn SchedulerStore>, executor: Executor,
               journal: Arc<dyn ExecutionJournal>, clock: Arc<dyn Clock>) -> Self; // default lease 60s
    pub fn with_lease(self, lease: chrono::Duration) -> Self;

    /// Enqueue the graph, drive `Executor::run`, classify the outcome into the store.
    pub async fn submit(&self, run: RunId, graph: Graph) -> Result<RunOutcome, OrchestratorError>;

    /// Claim due wakes and drive `Executor::start(run, stored_graph)` for each; classify each; return
    /// the number woken. A double-drive is harmless (idempotent resume), so a crash between drive and
    /// record self-heals on the next tick.
    pub async fn tick(&self) -> Result<usize, OrchestratorError>;

    /// Optional convenience: call `tick()` on an interval until cancelled (production supervisor is §9).
    pub async fn run_forever(&self, interval: std::time::Duration) -> Result<(), OrchestratorError>;

    // observe/intervene delegate to the store
    pub async fn status(&self, run: RunId) -> Result<Option<ScheduledRun>, OrchestratorError>;
    pub async fn list_paused(&self) -> Result<Vec<ScheduledRun>, OrchestratorError>;
    pub async fn cancel(&self, run: RunId) -> Result<(), OrchestratorError>;
    pub async fn force_wake(&self, run: RunId) -> Result<(), OrchestratorError>;
}
```

**Classify a `RunOutcome`:** `paused.is_some()` → read the deadline (below) → `record_paused(next_wake, reason)`;
`failed.is_some()` → `record_terminal(Failed, reason)`; else → `record_terminal(Completed)`. An
`Err(VersionFenceMismatch)` from `start` (config drifted — §8) → `record_terminal(Failed, "stale: config changed")`.

**Reading the pause deadline — the `Executor` stays UNCHANGED.** `PauseInfo { node, reason }` does not carry
`resume_after` (it's built from a `NodeExec::Paused { reason }` at `mod.rs:648`; the deadline is known only at
the inner quota→pause site, which journals it into `RunPaused { resume_after }`). Rather than thread it up
through every `NodeExec::Paused` producer, the Scheduler reads it from the **durable journal**: on a paused
drive it `journal.load(run)`s and takes the **last** `RunPaused { resume_after }` event. Hence the Scheduler
holds the same `Arc<dyn ExecutionJournal>` the Executor does (the embedding app already constructs both). Zero
executor/core change — consistent with SP-DATA-1/2.

## 8. The two wake classes + fence composition (design fallout)

- **`resume_after = Some(t)`** (quota→pause) → `next_wake = t` → **auto-woken** by `claim_due` at `t`.
- **`resume_after = None`** (in-doubt Mutation pause) → `next_wake = NULL` → **never auto-woken**; it needs
  **`force_wake`** — i.e. a human resolves the mutation, then force-wakes. The scheduler naturally separates
  timer-wakes (quota) from human-wakes (in-doubt) — the **HOTL** path, for free.
- **SP-DATA-2 fence composes:** the Scheduler's `Executor` carries a config handle (`from_source(pg)`). A wake
  whose config generation drifted since the pause hits `VersionFenceMismatch` on `start` → the Scheduler
  records the run **terminal-`Failed` ("stale: config changed")** — loud, never a silent resume under changed
  config. An operator decides next (HOTL intervene).

## 9. Error handling / determinism

- Waking is idempotent (fold + memo, zero re-spend; in-doubt→reconcile), so the CAS-claim prevents
  thundering-herd while a double-drive stays harmless. A crash between `claim_due` and the record leaves a
  stale `'waking'` row the lease reclaims.
- All store failures → `OrchestratorError::Store` (SP-DATA-1's durable-store variant), loud, never swallowed.
- The Scheduler adds **no determinism surface** to the executor: it only decides *when* to (re)invoke `start`;
  the wake time is the durable `resume_after`; the claim is a DB operation, never journaled.

## 10. Acceptance criteria

- **AC1 — schema:** `scheduled_runs` applies idempotently; `dbd inspect` zero-noise; dbd-pattern-verifier passes.
- **AC2 — store parity (in-mem + Postgres):** `enqueue`/`record_paused`/`record_terminal`/`status`/`list_paused`
  round-trip; the stored `Graph` round-trips via jsonb.
- **AC3 — claim atomicity:** a due paused run is claimed **exactly once** under two concurrent `claim_due`
  calls (one returns it, the other doesn't); a not-yet-due run (`next_wake > now`) is NOT claimed; a
  `NULL`-`next_wake` run is NOT claimed by the timer.
- **AC4 — lease reclaim:** a stale `'waking'` row (`claimed_at < now - lease`) IS reclaimed by `claim_due`
  (crash-mid-wake self-heals); a fresh `'waking'` row (within the lease) is NOT.
- **AC5 — driver wake (fake Clock):** `submit` a run whose gateway returns `AllGated{resume_after=t}` → the run
  is `paused` with `next_wake=t` and is NOT woken while `clock.now() < t`; advance the clock past `t` → `tick()`
  wakes it, the gateway now succeeds → `completed`; a second `tick()` is a no-op (terminal, count 0).
- **AC6 — cross-process durable wake (Docker):** process A `submit`s a run that pauses (persisted in PG
  `scheduled_runs` + journal); a **fresh** process-B `Scheduler` (new `PostgresSchedulerStore`/`PostgresJournal`/
  Executor on the same `DATABASE_URL`) advances its clock past the deadline and `tick()`s → the run wakes,
  `Executor::start` replays the durable journal with **zero re-spend** of the completed prefix, the gateway
  succeeds → `completed`.
- **AC7 — HOTL:** a `resume_after=None` pause → `next_wake=NULL` → NOT auto-woken; `force_wake` → the next
  `tick()` wakes it. `cancel` on a paused run → it is NEVER woken (a due-but-cancelled run is not claimed), and
  its `status` reads `cancelled`.
- **AC8 — fence composition:** a wake whose config generation was bumped (SP-DATA-2) since the pause →
  `VersionFenceMismatch` → the run is recorded terminal-`Failed` with a "stale config" reason (loud), not
  silently resumed.
- **AC9 — additivity:** default-off (no scheduler wired) ⇒ `cargo test --workspace` (feature-off) is
  byte-identical except the new (gated/in-mem) scheduler tests; the `Executor`/core are **unchanged** (the
  Scheduler reads the pause deadline from the existing journal — no `PauseInfo`/`NodeExec` change).
- **AC10 — Docker verification:** the store-crate scheduler suite (`--features postgres`) and the e2e
  (`--features postgres-tests`) run green against `postgres:16`, real (unpiped) exit codes.

## 11. Deferred / carry-forward

- Rich scheduling: exponential backoff + jitter on re-pause, `max_attempts` → dead-letter, per-run priority.
- Terminal-row pruning / retention policy; a production `run_forever` supervisor (graceful shutdown, metrics).
- `LISTEN`/`NOTIFY` low-latency wakes (vs poll); a batched multi-worker claim.
- The full management CLI/API (SP-DATA-4) built on `list_paused`/`status`/`cancel`/`force_wake`.
- Carrying the config generation forward so a re-pin can *offer* to resume a stale run under new config
  (today: fence-mismatch → terminal-`Failed`; the operator re-submits).

## 12. Files touched

- `database/ddl/table/orchestrator/scheduled_runs.sql` (new); `database/_apply_all.sql` (extend).
- `crates/orchestrator-core/src/scheduler.rs` (new): `SchedulerStore` trait + `RunStatus` + `ScheduledRun`
  DTO; `lib.rs` re-export. **No `executor/mod.rs` change** — the Scheduler reads the deadline from the journal.
- `crates/orchestrator-store/src/`: `InMemorySchedulerStore` (stores.rs or a sibling) + `PostgresSchedulerStore`
  (postgres.rs).
- `crates/orchestrator/src/scheduler.rs` (new): the `Scheduler` driver (holds the `Executor` + the
  `Arc<dyn ExecutionJournal>` + `Clock`); `lib.rs` re-export.
- `crates/orchestrator/src/executor/tests.rs` (or a scheduler test module): the driver + cross-process e2e
  (`#[cfg(feature = "postgres-tests")]`).
