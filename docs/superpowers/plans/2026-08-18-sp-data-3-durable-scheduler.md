# SP-DATA-3 Durable Scheduler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A durable scheduler that wakes a paused run at its `resume_after` deadline — in any process, exactly-once, with zero token re-spend — turning `RunPaused{resume_after}` from a dead-end into a self-healing pause.

**Architecture:** A `Scheduler` driver (holds an injected `Executor` + `Arc<dyn ExecutionJournal>` + `Clock`) over a durable `SchedulerStore` (a `scheduled_runs` table that owns each run's original graph + wake-schedule). The Scheduler `submit`s runs, records their pauses, and on `tick()` atomically claims due wakes and re-drives `Executor::start`. The **Executor is unchanged** (the Scheduler reads the pause deadline from the journal's last `RunPaused` event). Layering mirrors SP-DATA-1/2: trait + DTOs in `orchestrator-core`, `InMemory`/`Postgres` stores in `orchestrator-store`, driver in `orchestrator`. Verified on Docker Postgres; feature-off ⇒ byte-identical.

**Tech Stack:** Rust, sqlx 0.8 (postgres, **runtime** `sqlx::query`/`query_as`), dbd (schema), Docker Postgres, `chrono` (already a dep).

**Spec:** `docs/superpowers/specs/2026-08-18-sp-data-3-durable-scheduler-design.md`

**Baseline:** `develop` at `31c4c4c`; full workspace **1123 tests** green (macOS). `cargo fmt --all` before every commit (pre-commit = fmt-check + workspace `clippy -D warnings`, NO tests → always run tests yourself, real unpiped exit code). **Do NOT push** (the coordinator pushes after the whole-slice review) — this OVERRIDES any global "push after a develop commit" rule.

**Key existing seams (read before starting):**
- `crates/orchestrator-core/src/journal.rs:147` — `JournalEvent::RunPaused { reason: String, resume_after: Option<DateTime<Utc>> }`.
- `crates/orchestrator-core/src/graph.rs:245` — `Graph` is `Serialize`/`Deserialize`.
- `crates/orchestrator-core/src/clock.rs` — `Clock` trait + `SystemClock` (`fn now(&self) -> DateTime<Utc>`).
- `crates/orchestrator/src/executor/mod.rs` — `Executor::run`/`start(run, &Graph)`; `RunOutcome { paused: Option<PauseInfo>, failed: Option<..>, completed, outputs, .. }`; `PauseInfo { node, reason }`.
- **The pause↔resume pattern to mirror**, `crates/orchestrator/src/executor/tests.rs:6152` (`a_paused_gated_run_reattempts_and_completes_on_resume`): `timeout_gateway()` + a warm-up `gw.execute(&build_request("c", ..))` + `Executor::run` → `o.paused.is_some()` + a journaled `RunPaused{resume_after:Some}`; then a fresh `recording_gateway()` + `Executor::start` → completes. **Scheduler tests seed the pause with a gated executor and wake with an un-gated one** (the fake `Clock` drives `claim_due`, so no fight with the gateway's real-time cooldown).
- `crates/orchestrator/src/test_support.rs` — `timeout_gateway()` (`:548`), `recording_gateway()` (`:167`, returns `(Gateway, CallLog)`), `failing_after_gateway()`, `build_request` (via `support::build_request`).
- SP-DATA-1 `crates/orchestrator-store/src/postgres.rs` — `connect()`, the PG adapters, `store_err` (→ `OrchestratorError::Store`), `db_url()` test helper; `stores.rs` — `InMemory*` pattern (`Arc<Mutex<HashMap<..>>>`).
- SP-DATA-1 `crates/orchestrator/src/executor/tests.rs:11754` `mod postgres_e2e` (`#[cfg(feature="postgres-tests")]`) — the cross-process harness to extend.

**DOCKER POSTGRES HARNESS (Tasks 4–5):**
```bash
cd /Users/Jerry/Developer/gateway
docker rm -f spd3-pg >/dev/null 2>&1
docker run -d --name spd3-pg -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=orch -p 55436:5432 postgres:16 >/dev/null
until docker exec spd3-pg pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done
export DATABASE_URL="postgres://postgres:pw@localhost:55436/orch"
docker exec -i spd3-pg psql -U postgres -d orch -v ON_ERROR_STOP=1 < database/_apply_all.sql
cargo test -p sensei-orchestrator-store --features postgres -- --test-threads=1 ; echo "STORE_PG_EXIT=$?"
cargo test -p sensei-orchestrator --features postgres-tests -- --test-threads=1 scheduler_ ; echo "E2E_PG_EXIT=$?"
docker rm -f spd3-pg >/dev/null 2>&1
```
Read REAL exit codes. Every test uses a UNIQUE `run_id` so the shared `scheduled_runs` table never collides (`--test-threads=1` is the safety net). Tests `return` early when `DATABASE_URL` is unset.

---

## File Structure

- **Create** `database/ddl/table/orchestrator/scheduled_runs.sql`; **Modify** `database/_apply_all.sql`.
- **Create** `crates/orchestrator-core/src/scheduler.rs` (`SchedulerStore` trait + `RunStatus` + `ScheduledRun`); **Modify** `crates/orchestrator-core/src/lib.rs` (mod + re-export).
- **Create** `crates/orchestrator-store/src/scheduler_store.rs` (`InMemorySchedulerStore`); **Modify** `crates/orchestrator-store/src/postgres.rs` (`PostgresSchedulerStore`) + `lib.rs`.
- **Create** `crates/orchestrator/src/scheduler.rs` (the `Scheduler` driver); **Modify** `crates/orchestrator/src/lib.rs` (re-export).
- **Modify** `crates/orchestrator/src/executor/tests.rs` — the gated cross-process wake e2e in `mod postgres_e2e`.

**No `Cargo.toml` change** — the store's `postgres` feature + the orchestrator's `postgres-tests` feature already exist; `chrono` is already a dep of all three crates.

---

## Task 1: The dbd `scheduled_runs` schema

**Files:** Create `database/ddl/table/orchestrator/scheduled_runs.sql`; modify `database/_apply_all.sql`.

- [ ] **Step 1: Author the table DDL (idempotent, jsonb graph)**

`database/ddl/table/orchestrator/scheduled_runs.sql`:
```sql
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

- [ ] **Step 2: Extend `database/_apply_all.sql`**

READ the current file first (it INLINES table bodies — NOT `\i` — because it's piped into a containerized psql). Append the `scheduled_runs` body verbatim (table + index) after the SP-DATA-2 config tables, with a `-- ddl/table/orchestrator/scheduled_runs.sql` comment header matching the existing style. Keep everything `if not exists`.

- [ ] **Step 3: Apply to a Docker Postgres + verify idempotent (REAL exit codes)**

Start the Docker PG (harness top), then:
```bash
docker exec -i spd3-pg psql -U postgres -d orch -v ON_ERROR_STOP=1 < database/_apply_all.sql ; echo "APPLY1_EXIT=$?"
docker exec spd3-pg psql -U postgres -d orch -c "\d orchestrator.scheduled_runs"          # columns + the due index
docker exec -i spd3-pg psql -U postgres -d orch -v ON_ERROR_STOP=1 < database/_apply_all.sql ; echo "REAPPLY_EXIT=$?"
```
Expected: `APPLY1_EXIT=0`, the table + `scheduled_runs_due_idx` present, `REAPPLY_EXIT=0` (idempotent). Tear down. (The `dbd-pattern-verifier` re-reviews `database/` at whole-slice.)

- [ ] **Step 4: Commit**

```bash
cd /Users/Jerry/Developer/gateway
git add database/
git commit -m "feat(orchestrator): SP-DATA-3 (1/5) — dbd scheduled_runs schema (durable wake set)"
```

---

## Task 2: `SchedulerStore` trait + `InMemorySchedulerStore` + semantics tests

**Files:** Create `crates/orchestrator-core/src/scheduler.rs`; modify `crates/orchestrator-core/src/lib.rs`; create `crates/orchestrator-store/src/scheduler_store.rs`; modify `crates/orchestrator-store/src/lib.rs`.

- [ ] **Step 1: Define the trait + DTOs (core, no I/O)**

`crates/orchestrator-core/src/scheduler.rs`:
```rust
//! SP-DATA-3: the durable-scheduler seam. `SchedulerStore` owns each run's original graph + its
//! wake-schedule; the `Scheduler` driver (in the `orchestrator` crate) drives it. Backend-agnostic
//! (an `InMemory` + a `Postgres` impl), like `ExecutionJournal`/`ContentStore`.

use crate::error::OrchestratorError;
use crate::graph::Graph;
use crate::ids::RunId;
use chrono::{DateTime, Duration, Utc};

/// A scheduled run's lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RunStatus {
    Waking,    // in-flight (submit's initial drive OR a claimed wake) — lease-protected
    Paused,    // awaiting a wake at `next_wake` (NULL next_wake ⇒ needs force_wake)
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
    pub fn from_str(s: &str) -> Option<Self> {
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
        matches!(self, RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled)
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

#[async_trait::async_trait]
pub trait SchedulerStore: Send + Sync {
    /// Insert a NEW run as in-flight ('waking'), storing its ORIGINAL graph + stamping `claimed_at=now`.
    /// A duplicate `run` is a loud error (submit is once per run id).
    async fn enqueue(&self, run: RunId, graph: &Graph, now: DateTime<Utc>) -> Result<(), OrchestratorError>;

    /// A drive PAUSED: 'waking' → 'paused' with `next_wake` (None ⇒ NULL). CONDITIONAL on the current
    /// status being 'waking' (a concurrent `cancel` wins; a cancelled row is not resurrected). A no-op
    /// (not an error) if the row is not 'waking'.
    async fn record_paused(&self, run: RunId, next_wake: Option<DateTime<Utc>>, reason: &str)
        -> Result<(), OrchestratorError>;

    /// A drive ENDED: 'waking' → `status` (Completed|Failed). CONDITIONAL on 'waking' (no-op otherwise).
    async fn record_terminal(&self, run: RunId, status: RunStatus, reason: Option<&str>)
        -> Result<(), OrchestratorError>;

    /// Atomically claim up to `limit` due wakes: `(status='paused' AND next_wake<=now)` OR a stale
    /// `(status='waking' AND claimed_at < now-lease)`; flip each to 'waking', stamp `claimed_at=now`,
    /// return `(run, graph)`. NULL `next_wake` is never claimed by the timer.
    async fn claim_due(&self, now: DateTime<Utc>, lease: Duration, limit: usize)
        -> Result<Vec<(RunId, Graph)>, OrchestratorError>;

    async fn status(&self, run: RunId) -> Result<Option<ScheduledRun>, OrchestratorError>;
    async fn list_paused(&self) -> Result<Vec<ScheduledRun>, OrchestratorError>;

    /// Any NON-terminal status → 'cancelled' (idempotent; a cancelled run is never woken).
    async fn cancel(&self, run: RunId) -> Result<(), OrchestratorError>;
    /// A 'paused' run → set `next_wake=now` so the next tick claims it (the human-wake path).
    /// CONDITIONAL on 'paused'.
    async fn force_wake(&self, run: RunId, now: DateTime<Utc>) -> Result<(), OrchestratorError>;
}
```
Add to `crates/orchestrator-core/src/lib.rs`: `pub mod scheduler;` + `pub use scheduler::{RunStatus, ScheduledRun, SchedulerStore};` (place alphabetically near the other `pub use`s). If `chrono::Duration` re-export is needed, it's already available via the `chrono` dep.

- [ ] **Step 2: Write the failing `InMemorySchedulerStore` tests**

`crates/orchestrator-store/src/scheduler_store.rs` (declare `pub mod scheduler_store;` in `lib.rs`). Write these tests FIRST (they won't compile until Step 3 defines the struct). A tiny graph helper: `fn g() -> Graph { Graph { nodes: vec![] } }` (an empty graph round-trips fine; the store never executes it). Use `chrono::Utc::now()`-free time — build fixed instants from a base + `chrono::Duration` so tests are deterministic (do NOT call `Utc::now()`; construct e.g. `DateTime::<Utc>::from_timestamp(1_000_000, 0).unwrap()` as `t0`).
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{RunStatus, SchedulerStore};
    use orchestrator_core::graph::Graph;
    use orchestrator_core::ids::RunId;
    use chrono::{DateTime, Duration, Utc};

    fn t(secs: i64) -> DateTime<Utc> { DateTime::<Utc>::from_timestamp(1_000_000 + secs, 0).unwrap() }
    fn run() -> RunId { RunId(uuid::Uuid::new_v4()) }
    fn g() -> Graph { Graph { nodes: vec![] } }
    fn lease() -> Duration { Duration::seconds(60) }

    #[tokio::test]
    async fn claim_due_returns_a_due_paused_run_but_not_a_future_one() {
        let s = InMemorySchedulerStore::new();
        let (a, b) = (run(), run());
        s.enqueue(a, &g(), t(0)).await.unwrap();
        s.record_paused(a, Some(t(10)), "gated").await.unwrap();       // due at t=10
        s.enqueue(b, &g(), t(0)).await.unwrap();
        s.record_paused(b, Some(t(100)), "gated").await.unwrap();      // due at t=100
        let due = s.claim_due(t(20), lease(), 10).await.unwrap();      // now=20
        assert_eq!(due.len(), 1, "only `a` is due");
        assert_eq!(due[0].0, a);
        assert_eq!(s.status(a).await.unwrap().unwrap().status, RunStatus::Waking, "claim flips to waking");
    }

    #[tokio::test]
    async fn claim_due_never_claims_a_null_deadline_pause() {
        let s = InMemorySchedulerStore::new();
        let a = run();
        s.enqueue(a, &g(), t(0)).await.unwrap();
        s.record_paused(a, None, "in-doubt").await.unwrap();          // no timer
        assert!(s.claim_due(t(10_000), lease(), 10).await.unwrap().is_empty(), "NULL next_wake never auto-woken");
    }

    #[tokio::test]
    async fn claim_due_reclaims_a_stale_waking_but_not_a_fresh_one() {
        let s = InMemorySchedulerStore::new();
        let (stale, fresh) = (run(), run());
        s.enqueue(stale, &g(), t(0)).await.unwrap();   // 'waking' claimed_at=t(0)
        s.enqueue(fresh, &g(), t(100)).await.unwrap(); // 'waking' claimed_at=t(100)
        let due = s.claim_due(t(120), lease(), 10).await.unwrap();    // lease=60: stale (120-0>60) reclaimed; fresh (120-100<60) not
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].0, stale);
    }

    #[tokio::test]
    async fn cancel_makes_a_paused_run_unwakeable() {
        let s = InMemorySchedulerStore::new();
        let a = run();
        s.enqueue(a, &g(), t(0)).await.unwrap();
        s.record_paused(a, Some(t(10)), "gated").await.unwrap();
        s.cancel(a).await.unwrap();
        assert_eq!(s.status(a).await.unwrap().unwrap().status, RunStatus::Cancelled);
        assert!(s.claim_due(t(1000), lease(), 10).await.unwrap().is_empty(), "cancelled never claimed");
    }

    #[tokio::test]
    async fn force_wake_makes_a_null_deadline_pause_claimable() {
        let s = InMemorySchedulerStore::new();
        let a = run();
        s.enqueue(a, &g(), t(0)).await.unwrap();
        s.record_paused(a, None, "in-doubt").await.unwrap();
        s.force_wake(a, t(50)).await.unwrap();                        // next_wake := 50
        let due = s.claim_due(t(60), lease(), 10).await.unwrap();
        assert_eq!(due.len(), 1, "force_wake makes it due");
        assert_eq!(due[0].0, a);
    }

    #[tokio::test]
    async fn record_paused_is_a_noop_after_cancel_no_resurrection() {
        let s = InMemorySchedulerStore::new();
        let a = run();
        s.enqueue(a, &g(), t(0)).await.unwrap();       // 'waking'
        s.cancel(a).await.unwrap();                    // 'cancelled'
        s.record_paused(a, Some(t(10)), "gated").await.unwrap(); // conditional on 'waking' → no-op
        assert_eq!(s.status(a).await.unwrap().unwrap().status, RunStatus::Cancelled, "cancel wins");
    }
}
```

- [ ] **Step 3: Verify they fail, then implement `InMemorySchedulerStore`**

Run `cargo test -p sensei-orchestrator-store scheduler` → FAIL (`cannot find InMemorySchedulerStore`). Then implement (mirror `stores.rs`'s `Arc<Mutex<HashMap>>` pattern):
```rust
use chrono::{DateTime, Duration, Utc};
use orchestrator_core::graph::Graph;
use orchestrator_core::ids::RunId;
use orchestrator_core::{OrchestratorError, RunStatus, ScheduledRun, SchedulerStore};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Row {
    graph: Graph,
    status: RunStatus,
    next_wake: Option<DateTime<Utc>>,
    claimed_at: Option<DateTime<Utc>>,
    reason: Option<String>,
    updated_at: DateTime<Utc>,
}

#[derive(Clone, Default)]
pub struct InMemorySchedulerStore {
    rows: Arc<Mutex<HashMap<RunId, Row>>>,
}
impl InMemorySchedulerStore {
    pub fn new() -> Self { Self::default() }
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<RunId, Row>> {
        self.rows.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait::async_trait]
impl SchedulerStore for InMemorySchedulerStore {
    async fn enqueue(&self, run: RunId, graph: &Graph, now: DateTime<Utc>) -> Result<(), OrchestratorError> {
        let mut m = self.lock();
        if m.contains_key(&run) {
            return Err(OrchestratorError::Store(format!("duplicate submit for run {run:?}")));
        }
        m.insert(run, Row { graph: graph.clone(), status: RunStatus::Waking, next_wake: None,
                            claimed_at: Some(now), reason: None, updated_at: now });
        Ok(())
    }
    async fn record_paused(&self, run: RunId, next_wake: Option<DateTime<Utc>>, reason: &str) -> Result<(), OrchestratorError> {
        let mut m = self.lock();
        if let Some(r) = m.get_mut(&run) {
            if r.status == RunStatus::Waking {
                r.status = RunStatus::Paused; r.next_wake = next_wake; r.claimed_at = None;
                r.reason = Some(reason.to_string());
            }
        }
        Ok(())
    }
    async fn record_terminal(&self, run: RunId, status: RunStatus, reason: Option<&str>) -> Result<(), OrchestratorError> {
        let mut m = self.lock();
        if let Some(r) = m.get_mut(&run) {
            if r.status == RunStatus::Waking {
                r.status = status; r.next_wake = None; r.claimed_at = None;
                r.reason = reason.map(str::to_string);
            }
        }
        Ok(())
    }
    async fn claim_due(&self, now: DateTime<Utc>, lease: Duration, limit: usize) -> Result<Vec<(RunId, Graph)>, OrchestratorError> {
        let mut m = self.lock();
        let mut out = Vec::new();
        for (run, r) in m.iter_mut() {
            if out.len() >= limit { break; }
            let due_paused = r.status == RunStatus::Paused && r.next_wake.map(|w| w <= now).unwrap_or(false);
            let stale_waking = r.status == RunStatus::Waking && r.claimed_at.map(|c| now - c > lease).unwrap_or(false);
            if due_paused || stale_waking {
                r.status = RunStatus::Waking; r.claimed_at = Some(now); r.updated_at = now;
                out.push((*run, r.graph.clone()));
            }
        }
        Ok(out)
    }
    async fn status(&self, run: RunId) -> Result<Option<ScheduledRun>, OrchestratorError> {
        Ok(self.lock().get(&run).map(|r| ScheduledRun {
            run, status: r.status, next_wake: r.next_wake, reason: r.reason.clone(), updated_at: r.updated_at }))
    }
    async fn list_paused(&self) -> Result<Vec<ScheduledRun>, OrchestratorError> {
        Ok(self.lock().iter().filter(|(_, r)| r.status == RunStatus::Paused).map(|(run, r)| ScheduledRun {
            run: *run, status: r.status, next_wake: r.next_wake, reason: r.reason.clone(), updated_at: r.updated_at }).collect())
    }
    async fn cancel(&self, run: RunId) -> Result<(), OrchestratorError> {
        let mut m = self.lock();
        if let Some(r) = m.get_mut(&run) { if !r.status.is_terminal() { r.status = RunStatus::Cancelled; r.next_wake = None; } }
        Ok(())
    }
    async fn force_wake(&self, run: RunId, now: DateTime<Utc>) -> Result<(), OrchestratorError> {
        let mut m = self.lock();
        if let Some(r) = m.get_mut(&run) { if r.status == RunStatus::Paused { r.next_wake = Some(now); } }
        Ok(())
    }
}
```
Add `pub mod scheduler_store;` + `pub use scheduler_store::InMemorySchedulerStore;` to `crates/orchestrator-store/src/lib.rs`.

- [ ] **Step 4: Verify pass + clippy + default suite unchanged**

`cargo test -p sensei-orchestrator-store scheduler` → 6 pass. `cargo clippy -p sensei-orchestrator-store --all-targets -- -D warnings` clean. `cargo test -p sensei-orchestrator-core` unchanged.

- [ ] **Step 5: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator-core/src/scheduler.rs crates/orchestrator-core/src/lib.rs crates/orchestrator-store/src/scheduler_store.rs crates/orchestrator-store/src/lib.rs
git commit -m "feat(orchestrator): SP-DATA-3 (2/5) — SchedulerStore trait + InMemorySchedulerStore"
```

---

## Task 3: The `Scheduler` driver (submit / tick / observe / intervene)

**Files:** Create `crates/orchestrator/src/scheduler.rs`; modify `crates/orchestrator/src/lib.rs`.

- [ ] **Step 1: Write the failing driver tests**

`crates/orchestrator/src/scheduler.rs` `#[cfg(test)] mod tests`. Use a **`FakeClock`** (a settable clock) so `tick` fires deterministically, and the **gated-executor-submit / un-gated-executor-wake** split (mirroring `tests.rs:6152`). Grep `crate::test_support` for `timeout_gateway`/`recording_gateway`/`build_request` and `crate::executor::support::build_request`.
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::Executor;
    use crate::test_support::{recording_gateway, timeout_gateway};
    use orchestrator_core::{Clock, Graph, Node, NodeId, RunId, SchedulerStore};
    use orchestrator_store::{InMemoryJournal, InMemorySchedulerStore};
    use std::sync::{Arc, Mutex};
    use chrono::{DateTime, Duration, Utc};

    // A settable clock for deterministic wakes.
    struct FakeClock(Mutex<DateTime<Utc>>);
    impl FakeClock { fn new(t: DateTime<Utc>) -> Arc<Self> { Arc::new(Self(Mutex::new(t))) }
        fn set(&self, t: DateTime<Utc>) { *self.0.lock().unwrap() = t; } }
    impl Clock for FakeClock { fn now(&self) -> DateTime<Utc> { *self.0.lock().unwrap() } }

    fn one_node_graph() -> Graph { Graph { nodes: vec![Node {
        id: NodeId("n1".into()), kind: crate::executor::support::model_call("c", "go"), deps: vec![] }] } }

    /// AC5 (submit + wake, one store, two executors): submit with a GATED executor → the run is
    /// recorded 'paused' with next_wake from the journal; it is NOT woken before the deadline; after
    /// advancing the clock a tick on an UN-GATED scheduler wakes it → completed; a second tick no-ops.
    #[tokio::test]
    async fn a_paused_run_is_recorded_then_woken_by_a_tick_after_its_deadline() {
        let journal = InMemoryJournal::new();
        let store = Arc::new(InMemorySchedulerStore::new());
        let run = RunId(uuid::Uuid::new_v4());
        let graph = one_node_graph();
        let clock = FakeClock::new(DateTime::<Utc>::from_timestamp(1_000_000, 0).unwrap());

        // --- submit with a gated executor → the run pauses (resume_after journaled) ---
        let gw = timeout_gateway().await;
        let _ = gw.execute(&crate::executor::support::build_request("c", &serde_json::json!({"prompt":"warm"}))).await;
        let gated_exec = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1").with_clock(clock.clone());
        let sched_submit = Scheduler::new(store.clone(), gated_exec, Arc::new(journal.clone()), clock.clone());
        let o1 = sched_submit.submit(run, graph.clone()).await.expect("submit");
        assert!(o1.paused.is_some(), "the run pauses on the timed gate");
        let st = store.status(run).await.unwrap().unwrap();
        assert_eq!(st.status, orchestrator_core::RunStatus::Paused);
        let deadline = st.next_wake.expect("a timed pause has a next_wake");

        // --- before the deadline: a tick wakes nothing ---
        clock.set(deadline - Duration::seconds(1));
        // (use an un-gated scheduler for the wake half — a real quota reset)
        let (gw2, _c2) = recording_gateway().await;
        let un_gated = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1").with_clock(clock.clone());
        let sched = Scheduler::new(store.clone(), un_gated, Arc::new(journal.clone()), clock.clone());
        assert_eq!(sched.tick().await.unwrap(), 0, "not due yet");

        // --- past the deadline: the tick wakes it → completed; a second tick no-ops ---
        clock.set(deadline + Duration::seconds(1));
        assert_eq!(sched.tick().await.unwrap(), 1, "woken");
        assert_eq!(store.status(run).await.unwrap().unwrap().status, orchestrator_core::RunStatus::Completed);
        assert_eq!(sched.tick().await.unwrap(), 0, "terminal run is not re-woken");
    }

    /// AC7 (cancel): a cancelled paused run is never woken.
    #[tokio::test]
    async fn cancel_prevents_a_wake() {
        let journal = InMemoryJournal::new();
        let store = Arc::new(InMemorySchedulerStore::new());
        let run = RunId(uuid::Uuid::new_v4());
        let clock = FakeClock::new(DateTime::<Utc>::from_timestamp(1_000_000, 0).unwrap());
        // seed a paused run directly in the store (no gateway needed for this store-level behavior).
        store.enqueue(run, &one_node_graph(), clock.now()).await.unwrap();
        store.record_paused(run, Some(clock.now() + Duration::seconds(10)), "gated").await.unwrap();
        let (gw, _c) = recording_gateway().await;
        let sched = Scheduler::new(store.clone(),
            Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1").with_clock(clock.clone()),
            Arc::new(journal.clone()), clock.clone());
        sched.cancel(run).await.unwrap();
        clock.set(clock.now() + Duration::seconds(100));
        assert_eq!(sched.tick().await.unwrap(), 0, "cancelled run not woken");
        assert_eq!(store.status(run).await.unwrap().unwrap().status, orchestrator_core::RunStatus::Cancelled);
    }
}
```
(Grep `crate::executor::support` for the real `model_call`/`build_request` names — they're `pub(crate)` test helpers used by the existing quota→pause tests at `tests.rs:6047-6203`. If `model_call` lives elsewhere, use the same constructor the existing tests use.)

- [ ] **Step 2: Verify fail, then implement the driver**

```rust
//! SP-DATA-3: the durable-scheduler driver. Drives an injected `Executor` and records each run's
//! pause/terminal into a `SchedulerStore`; `tick()` atomically claims due wakes and re-drives
//! `Executor::start`. Reads the pause deadline from the durable journal (the Executor is unchanged).

use crate::executor::{Executor, RunOutcome};
use orchestrator_core::{
    Clock, ExecutionJournal, Graph, JournalEvent, OrchestratorError, RunId, RunStatus, ScheduledRun,
    SchedulerStore,
};
use std::sync::Arc;

const DEFAULT_LEASE_SECS: i64 = 60;
const CLAIM_BATCH: usize = 64;

pub struct Scheduler {
    store: Arc<dyn SchedulerStore>,
    executor: Executor,
    journal: Arc<dyn ExecutionJournal>,
    clock: Arc<dyn Clock>,
    lease: chrono::Duration,
}

impl Scheduler {
    pub fn new(store: Arc<dyn SchedulerStore>, executor: Executor,
               journal: Arc<dyn ExecutionJournal>, clock: Arc<dyn Clock>) -> Self {
        Self { store, executor, journal, clock, lease: chrono::Duration::seconds(DEFAULT_LEASE_SECS) }
    }
    pub fn with_lease(mut self, lease: chrono::Duration) -> Self { self.lease = lease; self }

    /// Enqueue the graph, drive a fresh run, and record the outcome. Returns the `RunOutcome`.
    pub async fn submit(&self, run: RunId, graph: Graph) -> Result<RunOutcome, OrchestratorError> {
        self.store.enqueue(run, &graph, self.clock.now()).await?;
        let outcome = self.executor.run(run, &graph).await;
        self.record(run, &outcome).await?;
        outcome
    }

    /// Claim due wakes and re-drive each via `Executor::start`; record each outcome; return the count.
    /// A double-drive is harmless (idempotent resume), so a crash between drive and record self-heals.
    pub async fn tick(&self) -> Result<usize, OrchestratorError> {
        let due = self.store.claim_due(self.clock.now(), self.lease, CLAIM_BATCH).await?;
        let n = due.len();
        for (run, graph) in due {
            let outcome = self.executor.start(run, &graph).await;
            self.record(run, &outcome).await?; // a STORE failure aborts loud; a drive failure is recorded
        }
        Ok(n)
    }

    pub async fn status(&self, run: RunId) -> Result<Option<ScheduledRun>, OrchestratorError> { self.store.status(run).await }
    pub async fn list_paused(&self) -> Result<Vec<ScheduledRun>, OrchestratorError> { self.store.list_paused().await }
    pub async fn cancel(&self, run: RunId) -> Result<(), OrchestratorError> { self.store.cancel(run).await }
    pub async fn force_wake(&self, run: RunId) -> Result<(), OrchestratorError> { self.store.force_wake(run, self.clock.now()).await }

    /// Classify a drive result into the store. A drive's own error (e.g. a config-fence mismatch) is
    /// recorded terminal-Failed (loud in the store, not propagated); only a STORE failure returns Err.
    async fn record(&self, run: RunId, outcome: &Result<RunOutcome, OrchestratorError>) -> Result<(), OrchestratorError> {
        match outcome {
            Ok(o) if o.paused.is_some() => {
                let next_wake = self.last_resume_after(run).await?;
                let reason = o.paused.as_ref().map(|p| p.reason.clone()).unwrap_or_default();
                self.store.record_paused(run, next_wake, &reason).await
            }
            Ok(o) if o.failed.is_some() => {
                self.store.record_terminal(run, RunStatus::Failed, o.failed.as_deref()).await
            }
            Ok(_) => self.store.record_terminal(run, RunStatus::Completed, None).await,
            Err(e) => {
                let reason = if matches!(e, OrchestratorError::VersionFenceMismatch { .. }) {
                    format!("stale: config changed ({e})")
                } else { e.to_string() };
                self.store.record_terminal(run, RunStatus::Failed, Some(&reason)).await
            }
        }
    }

    /// The last journaled `RunPaused.resume_after` — the deadline the executor recorded.
    async fn last_resume_after(&self, run: RunId) -> Result<Option<chrono::DateTime<chrono::Utc>>, OrchestratorError> {
        let events = self.journal.load(run).await.map_err(OrchestratorError::Journal)?;
        Ok(events.iter().rev().find_map(|(_, e)| match e {
            JournalEvent::RunPaused { resume_after, .. } => Some(*resume_after),
            _ => None,
        }).flatten())
    }
}
```
NOTES for the implementer:
- `o.failed` — check its ACTUAL type in `RunOutcome` (`mod.rs:106`): it may be `Option<String>` or `Option<PauseInfo>`-like. Adapt `o.failed.as_deref()`/`o.failed.is_some()` to the real type (grep `pub failed`).
- `RunOutcome` is returned by-value from `run`/`start`; `submit` returns it after `record(&outcome)` — since `record` borrows, either clone the outcome or restructure (drive → `record(run, &res)` → return `res`). The sketch borrows `&outcome` then returns `outcome` (the `Result` is moved after the borrow ends — fine).
- Add `pub mod scheduler;` + `pub use scheduler::Scheduler;` to `crates/orchestrator/src/lib.rs`.
- Confirm `Graph`, `Node`, `NodeId`, `JournalEvent`, `ExecutionJournal`, `Clock`, `RunStatus`, `ScheduledRun`, `SchedulerStore` are all re-exported from `orchestrator_core` (grep its `lib.rs`; some may need a path like `orchestrator_core::graph::Graph`).

- [ ] **Step 3: Verify pass + clippy**

Docker NOT needed (in-mem). `cargo test -p sensei-orchestrator scheduler` → the 2 driver tests pass. `cargo clippy -p sensei-orchestrator --all-targets -- -D warnings` clean.

- [ ] **Step 4: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/scheduler.rs crates/orchestrator/src/lib.rs
git commit -m "feat(orchestrator): SP-DATA-3 (3/5) — Scheduler driver (submit/tick/observe/intervene)"
```

---

## Task 4: `PostgresSchedulerStore` + Docker parity/atomicity tests

**Files:** Modify `crates/orchestrator-store/src/postgres.rs`.

- [ ] **Step 1: Write the failing Docker-PG tests (unique run_ids)**

Add to `postgres.rs` `#[cfg(test)] mod tests` (reuse `db_url()`). Mirror Task 2's semantics tests against `PostgresSchedulerStore`, PLUS a concurrency test proving `claim_due` is exactly-once. Use fixed instants via `DateTime::<Utc>::from_timestamp`.
```rust
    // (in the existing #[cfg(test)] mod tests, add:)
    use orchestrator_core::{RunStatus, SchedulerStore};
    use orchestrator_core::graph::Graph;

    fn sg() -> Graph { Graph { nodes: vec![] } }

    #[tokio::test]
    async fn pg_claim_due_is_exactly_once_under_concurrent_claims() {
        let Some(url) = db_url() else { return };
        let s = PostgresSchedulerStore::new(connect(&url).await.unwrap());
        let run = RunId(uuid::Uuid::new_v4());
        let now = DateTime::<Utc>::from_timestamp(2_000_000, 0).unwrap();
        s.enqueue(run, &sg(), now).await.unwrap();
        s.record_paused(run, Some(now), "gated").await.unwrap();     // due exactly at `now`
        // Two concurrent claims — exactly one gets the run.
        let (a, b) = tokio::join!(
            s.claim_due(now, chrono::Duration::seconds(60), 10),
            s.claim_due(now, chrono::Duration::seconds(60), 10),
        );
        let got: usize = a.unwrap().iter().chain(b.unwrap().iter()).filter(|(r, _)| *r == run).count();
        assert_eq!(got, 1, "a due run is claimed by exactly one of two concurrent claimers");
    }

    #[tokio::test]
    async fn pg_round_trip_and_transitions() {
        let Some(url) = db_url() else { return };
        let s = PostgresSchedulerStore::new(connect(&url).await.unwrap());
        let run = RunId(uuid::Uuid::new_v4());
        let t0 = DateTime::<Utc>::from_timestamp(2_100_000, 0).unwrap();
        s.enqueue(run, &sg(), t0).await.unwrap();
        s.record_paused(run, Some(t0 + chrono::Duration::seconds(10)), "gated").await.unwrap();
        let st = s.status(run).await.unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Paused);
        assert!(s.list_paused().await.unwrap().iter().any(|r| r.run == run));
        // not due yet
        assert!(s.claim_due(t0, chrono::Duration::seconds(60), 10).await.unwrap().iter().all(|(r,_)| *r != run));
        // due now → claimed → the stored graph round-trips
        let due = s.claim_due(t0 + chrono::Duration::seconds(20), chrono::Duration::seconds(60), 10).await.unwrap();
        assert!(due.iter().any(|(r, g)| *r == run && g.nodes.is_empty()), "claim returns the stored graph");
        s.record_terminal(run, RunStatus::Completed, None).await.unwrap();
        assert_eq!(s.status(run).await.unwrap().unwrap().status, RunStatus::Completed);
    }

    #[tokio::test]
    async fn pg_cancel_and_force_wake() {
        let Some(url) = db_url() else { return };
        let s = PostgresSchedulerStore::new(connect(&url).await.unwrap());
        let (c, f) = (RunId(uuid::Uuid::new_v4()), RunId(uuid::Uuid::new_v4()));
        let now = DateTime::<Utc>::from_timestamp(2_200_000, 0).unwrap();
        s.enqueue(c, &sg(), now).await.unwrap();
        s.record_paused(c, Some(now + chrono::Duration::seconds(10)), "g").await.unwrap();
        s.cancel(c).await.unwrap();
        assert_eq!(s.status(c).await.unwrap().unwrap().status, RunStatus::Cancelled);
        s.enqueue(f, &sg(), now).await.unwrap();
        s.record_paused(f, None, "in-doubt").await.unwrap();          // NULL deadline
        assert!(s.claim_due(now + chrono::Duration::seconds(1000), chrono::Duration::seconds(60), 10).await.unwrap().iter().all(|(r,_)| *r != f));
        s.force_wake(f, now).await.unwrap();
        assert!(s.claim_due(now + chrono::Duration::seconds(1), chrono::Duration::seconds(60), 10).await.unwrap().iter().any(|(r,_)| *r == f));
    }
```

- [ ] **Step 2: Verify fail (Docker-PG), then implement `PostgresSchedulerStore`**

Add to `postgres.rs`. Reuse `store_err` (→ `Store`). Graph ↔ jsonb via `serde_json`. Status ↔ text via `RunStatus::as_str`/`from_str`. The `claim_due` uses an atomic `UPDATE … WHERE run_id IN (SELECT … FOR UPDATE SKIP LOCKED) RETURNING` so concurrent claimers never overlap (the lock is held only for the brief UPDATE, NOT during the drive).
```rust
use orchestrator_core::graph::Graph;
use orchestrator_core::{RunStatus, ScheduledRun, SchedulerStore};

pub struct PostgresSchedulerStore { pool: PgPool }
impl PostgresSchedulerStore { pub fn new(pool: PgPool) -> Self { Self { pool } } }

#[async_trait::async_trait]
impl SchedulerStore for PostgresSchedulerStore {
    async fn enqueue(&self, run: RunId, graph: &Graph, now: DateTime<Utc>) -> Result<(), OrchestratorError> {
        let g = serde_json::to_value(graph).map_err(store_err_ser)?;
        // Fresh insert; a duplicate run id is a loud conflict.
        let res = sqlx::query(
            "insert into orchestrator.scheduled_runs (run_id, graph, status, claimed_at, updated_at)
             values ($1,$2,'waking',$3,$3) on conflict (run_id) do nothing")
            .bind(run.0).bind(g).bind(now).execute(&self.pool).await.map_err(store_err)?;
        if res.rows_affected() == 0 {
            return Err(OrchestratorError::Store(format!("duplicate submit for run {run:?}")));
        }
        Ok(())
    }
    async fn record_paused(&self, run: RunId, next_wake: Option<DateTime<Utc>>, reason: &str) -> Result<(), OrchestratorError> {
        sqlx::query(
            "update orchestrator.scheduled_runs set status='paused', next_wake=$2, claimed_at=null,
                    reason=$3, updated_at=now() where run_id=$1 and status='waking'")
            .bind(run.0).bind(next_wake).bind(reason).execute(&self.pool).await.map_err(store_err)?;
        Ok(())
    }
    async fn record_terminal(&self, run: RunId, status: RunStatus, reason: Option<&str>) -> Result<(), OrchestratorError> {
        sqlx::query(
            "update orchestrator.scheduled_runs set status=$2, next_wake=null, claimed_at=null,
                    reason=$3, updated_at=now() where run_id=$1 and status='waking'")
            .bind(run.0).bind(status.as_str()).bind(reason).execute(&self.pool).await.map_err(store_err)?;
        Ok(())
    }
    async fn claim_due(&self, now: DateTime<Utc>, lease: chrono::Duration, limit: usize) -> Result<Vec<(RunId, Graph)>, OrchestratorError> {
        let stale_before = now - lease;
        let rows: Vec<(uuid::Uuid, serde_json::Value)> = sqlx::query_as(
            "update orchestrator.scheduled_runs set status='waking', claimed_at=$1, updated_at=now()
             where run_id in (
                 select run_id from orchestrator.scheduled_runs
                 where (status='paused' and next_wake is not null and next_wake <= $1)
                    or (status='waking' and claimed_at < $2)
                 order by next_wake nulls last
                 limit $3
                 for update skip locked)
             returning run_id, graph")
            .bind(now).bind(stale_before).bind(limit as i64)
            .fetch_all(&self.pool).await.map_err(store_err)?;
        rows.into_iter().map(|(id, g)| Ok((RunId(id),
            serde_json::from_value(g).map_err(store_err_ser)?))).collect()
    }
    async fn status(&self, run: RunId) -> Result<Option<ScheduledRun>, OrchestratorError> {
        let row: Option<(String, Option<DateTime<Utc>>, Option<String>, DateTime<Utc>)> = sqlx::query_as(
            "select status, next_wake, reason, updated_at from orchestrator.scheduled_runs where run_id=$1")
            .bind(run.0).fetch_optional(&self.pool).await.map_err(store_err)?;
        Ok(row.map(|(s, nw, r, u)| ScheduledRun {
            run, status: RunStatus::from_str(&s).unwrap_or(RunStatus::Failed), next_wake: nw, reason: r, updated_at: u }))
    }
    async fn list_paused(&self) -> Result<Vec<ScheduledRun>, OrchestratorError> {
        let rows: Vec<(uuid::Uuid, Option<DateTime<Utc>>, Option<String>, DateTime<Utc>)> = sqlx::query_as(
            "select run_id, next_wake, reason, updated_at from orchestrator.scheduled_runs where status='paused'")
            .fetch_all(&self.pool).await.map_err(store_err)?;
        Ok(rows.into_iter().map(|(id, nw, r, u)| ScheduledRun {
            run: RunId(id), status: RunStatus::Paused, next_wake: nw, reason: r, updated_at: u }).collect())
    }
    async fn cancel(&self, run: RunId) -> Result<(), OrchestratorError> {
        sqlx::query(
            "update orchestrator.scheduled_runs set status='cancelled', next_wake=null, updated_at=now()
             where run_id=$1 and status not in ('completed','failed','cancelled')")
            .bind(run.0).execute(&self.pool).await.map_err(store_err)?;
        Ok(())
    }
    async fn force_wake(&self, run: RunId, now: DateTime<Utc>) -> Result<(), OrchestratorError> {
        sqlx::query(
            "update orchestrator.scheduled_runs set next_wake=$2, updated_at=now() where run_id=$1 and status='paused'")
            .bind(run.0).bind(now).execute(&self.pool).await.map_err(store_err)?;
        Ok(())
    }
}
```
Add `pub use postgres::PostgresSchedulerStore;` under the existing `#[cfg(feature="postgres")]` re-exports in `lib.rs` (grep how SP-DATA-1/2's PG types are re-exported). NOTE: `next_wake nulls last` + the `next_wake is not null` guard means NULL-deadline pauses are never claimed. `store_err_ser` already exists (SP-DATA-2).

- [ ] **Step 3: Verify (Docker-PG) pass + clippy + feature-off unchanged**

Docker-PG harness (store line) → the 3 new PG scheduler tests + all prior store tests pass (`STORE_PG_EXIT=0`). `cargo clippy -p sensei-orchestrator-store --features postgres --all-targets -- -D warnings` clean. `cargo test -p sensei-orchestrator-store` (feature-off) unchanged.

- [ ] **Step 4: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator-store/src/postgres.rs crates/orchestrator-store/src/lib.rs
git commit -m "feat(orchestrator): SP-DATA-3 (4/5) — PostgresSchedulerStore (atomic claim + lease)"
```

---

## Task 5: Cross-process durable-wake e2e + fence composition + additivity gate

**Files:** Modify `crates/orchestrator/src/executor/tests.rs` (a new `#[cfg(feature="postgres-tests")]` scheduler e2e module, or extend `mod postgres_e2e`).

- [ ] **Step 1: Write the cross-process wake e2e**

Mirror the SP-DATA-1 `postgres_e2e` harness + the `tests.rs:6152` pause pattern. Process A submits with a GATED executor (pauses, persists to PG); a FRESH process-B Scheduler (new `PostgresSchedulerStore`/`PostgresJournal` + an UN-GATED executor on the same `DATABASE_URL`) advances a fake clock past the deadline and `tick()`s → the run wakes, `Executor::start` replays with **zero re-spend**, completes.
```rust
    use orchestrator_store::PostgresSchedulerStore;
    use orchestrator_store::postgres::{PostgresJournal, connect};
    use orchestrator::Scheduler; // adjust to the real path
    // + a FakeClock (copy the one from scheduler.rs tests or a shared test helper)

    /// AC6: a run paused by process A wakes durably in a FRESH process B at its deadline, zero re-spend.
    #[tokio::test]
    async fn scheduler_wakes_a_paused_run_cross_process_with_zero_respend() {
        let Some(url) = db_url() else { return };
        let run = RunId(uuid::Uuid::new_v4());
        // A one-ModelCall("c","go") graph — the exact shape used at tests.rs:6156.
        let graph = Graph { nodes: vec![Node {
            id: NodeId("n1".into()), kind: support::model_call("c", "go"), deps: vec![] }] };
        let clock = FakeClock::new(DateTime::<Utc>::from_timestamp(3_000_000, 0).unwrap());

        // Process A: submit with a gated executor → pauses + persists (scheduled_runs + journal) in PG.
        let store_a = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
        let journal_a = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
        let gw = timeout_gateway().await;
        let _ = gw.execute(&support::build_request("c", &serde_json::json!({"prompt":"warm"}))).await;
        let exec_a = Executor::new(Arc::new(gw), journal_a.clone(), "v1").with_clock(clock.clone());
        let sched_a = Scheduler::new(store_a.clone(), exec_a, journal_a.clone(), clock.clone());
        let o1 = sched_a.submit(run, graph.clone()).await.unwrap();
        assert!(o1.paused.is_some());
        let deadline = store_a.status(run).await.unwrap().unwrap().next_wake.expect("timed pause");

        // Process B: FRESH store/journal/executor on the SAME DB; un-gated gateway with a call counter.
        let store_b = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
        let journal_b = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
        let (gw_b, calls_b) = recording_gateway().await;
        let clock_b = FakeClock::new(deadline + Duration::seconds(1));
        let exec_b = Executor::new(Arc::new(gw_b), journal_b.clone(), "v1")
            .with_content_store(Arc::new(PostgresContentStore::new(connect(&url).await.unwrap())))
            .with_clock(clock_b.clone());
        let sched_b = Scheduler::new(store_b.clone(), exec_b, journal_b.clone(), clock_b.clone());
        assert_eq!(sched_b.tick().await.unwrap(), 1, "process B wakes the run at its deadline");
        assert_eq!(store_b.status(run).await.unwrap().unwrap().status, RunStatus::Completed);
        assert_eq!(calls_b.lock().unwrap().len(), 1, "only the gated node re-attempted (prefix replayed → 0 re-spend)");
    }
```
Adapt: the one-`ModelCall` graph constructor (reuse the existing `tests.rs:6156` shape); the `FakeClock` (lift the one from `scheduler.rs` into a shared `#[cfg(test)]` spot or redefine locally); `PostgresContentStore` import (SP-DATA-1). Confirm `Scheduler`/`PostgresSchedulerStore` re-export paths.

- [ ] **Step 2: (optional, if cheap) fence-composition check (AC8)**

If a `RegistryHandle` + `PostgresConfigSource` wake-under-bumped-config test is straightforward, add `scheduler_wake_under_changed_config_is_recorded_stale_failed`: process A submits+pauses at config gen v (executor built `.with_registry_handle(from_source(pg_cfg))`); bump the config (`store+bump → v2`); process B's scheduler (handle at v2) ticks → `Executor::start` → `VersionFenceMismatch` → the Scheduler records `status=Failed` with a "stale: config changed" reason. If this balloons scope, DEFER it (note it) — AC8 is a composition property already implied by SP-DATA-2's fence + the driver's `record` arm.

- [ ] **Step 3: Verify (Docker-PG) the e2e passes**

```bash
cargo test -p sensei-orchestrator --features postgres-tests -- --test-threads=1 scheduler_ ; echo "E2E_EXIT=$?"
```
Expected `E2E_EXIT=0`; the wake e2e (+ fence test if added) pass. REAL exit code.

- [ ] **Step 4: Additivity + full-suite gate (feature OFF, macOS host, no PG)**

```bash
cd /Users/Jerry/Developer/gateway
cargo test --workspace > /tmp/spd3_fulltest.log 2>&1; echo "EXIT=$?"
grep -oE "[0-9]+ passed" /tmp/spd3_fulltest.log | awk '{s+=$1} END{print "TOTAL_PASSED="s}'
grep -oE "[1-9][0-9]* failed" /tmp/spd3_fulltest.log | head
cargo fmt --all --check; echo "FMT=$?"
```
Confirm `EXIT=0`, 0 failed, `TOTAL_PASSED = 1123 + (Task 2: 6 store tests) + (Task 3: 2 driver tests) = 1131` (the postgres/postgres-tests tests are gated OFF; the e2e `return`s without `DATABASE_URL`). If the count differs, reconcile before committing (the only default-suite additions are Task 2's 6 in-mem-store tests + Task 3's 2 driver tests). `FMT=0`; also `cargo clippy -p sensei-orchestrator-store --features postgres --all-targets -- -D warnings` clean.

- [ ] **Step 5: Commit** (do NOT push)

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/tests.rs
git commit -m "test(orchestrator): SP-DATA-3 (5/5) — cross-process durable-wake e2e + additivity gate"
```

---

## Self-Review notes (author)

- **Spec coverage:** §5 schema → T1. §6 trait → T2 (trait+in-mem+semantics). §7 driver → T3. §7 Postgres store → T4. AC1→T1; AC2→T2/T4; AC3 (claim atomicity)→T2 (due-vs-future/null) + T4 (concurrent exactly-once); AC4 (lease)→T2 (`claim_due_reclaims_a_stale_waking...`); AC5 (driver wake)→T3; AC6 (cross-process)→T5; AC7 (HOTL cancel/force_wake)→T2 + T3; AC8 (fence)→T5 Step 2 (optional/deferrable); AC9 (additive 1131)→T5 Step 4; AC10 (Docker)→T4/T5.
- **Type-consistency:** `SchedulerStore` methods (T2 def) are used identically in `InMemorySchedulerStore` (T2), `PostgresSchedulerStore` (T4), and the `Scheduler` driver (T3/T5). `RunStatus`/`ScheduledRun` shared. `claim_due -> Vec<(RunId, Graph)>` feeds `Executor::start(run, &graph)`.
- **Adapt-at-build items (verify against real code):** the exact `RunOutcome.failed` type (`Option<String>`? adapt `.as_deref()`/`.is_some()`); the `pub(crate)` test-helper names in `crate::executor::support` (`model_call`/`build_request`) + `crate::test_support` (`timeout_gateway`/`recording_gateway`); the `orchestrator_core` re-export paths for `Graph`/`Node`/`JournalEvent`/`ExecutionJournal`/`Clock`; how `#[cfg(feature="postgres")]` re-exports are declared in `orchestrator-store/src/lib.rs`; whether a shared `FakeClock` test helper should live in `test_support.rs` (if T5 reuses T3's). The DDL, trait, driver logic, and the atomic `claim_due` SQL are exact.
- **Executor unchanged:** the Scheduler reads the deadline from the journal (`last_resume_after`); no `PauseInfo`/`NodeExec`/core change → the 1123 baseline is untouched except the additive in-mem/driver tests.
- **Do NOT push** — the coordinator runs the whole-slice review (incl. `dbd-pattern-verifier`) then pushes.
