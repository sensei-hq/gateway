# SP-DATA-1 Durable Run State Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist a run's journal + CAS + context in Postgres so a run journaled in one process resumes in a fresh process against the same DB — cross-process crash durability, no token re-spend, durable in-doubt reconcile.

**Architecture:** Postgres implementations of the existing `ExecutionJournal`/`ContentStore`/`ContextStore` traits in `orchestrator-store` behind a `postgres` feature (schema-agnostic sqlx adapters, like `vault`), against a **dbd-managed schema** in `gateway/database/`, with a durable `format_version` fence. The `Executor` is unchanged (injects the trait objects). Verified on a Docker Postgres; feature-off ⇒ byte-identical.

**Tech Stack:** Rust, sqlx 0.8 (postgres, **runtime** queries — `sqlx::query`, NOT the compile-time `query!` macros, so the crate builds with no DB), dbd 0.10.5 (schema), Docker Postgres + CI `postgres` service.

**Spec:** `docs/superpowers/specs/2026-08-17-sp-data-1-durable-run-state-design.md`

**Baseline:** `develop` at `a0c2171`; full workspace **1120 tests** green (macOS). `cargo fmt --all` before every commit (pre-commit = fmt-check + workspace `clippy -D warnings`, NO tests → always run tests yourself, real unpiped exit code). Do NOT push (coordinator pushes after the whole-slice review).

**Parity reference:** `crates/orchestrator-store/src/lib.rs` (`InMemoryJournal` — `next_seq: AtomicU64` global counter) + `src/stores.rs` (`InMemoryContentStore`/`InMemoryContextStore`). The Postgres impls must match their observable semantics (a shared parity test-suite where practical). Trait sigs: `crates/orchestrator-core/src/{journal.rs,content.rs,context.rs}`.

**dbd reference:** `~/Developer/torii/database/{design.yaml, ddl/<type>/<schema>/*.sql}` — the family convention. A FRESH dbd project is **pre-release** ⇒ use the **`dbd reconcile`** workflow (edit `ddl/`, reconcile to the DB); do NOT hand-write migrations. The `dbd-pattern-verifier` agent reviews the schema in the whole-slice review.

**DOCKER POSTGRES HARNESS (reused in Tasks 2–5):**
```bash
# throwaway postgres; apply the schema; run the feature-gated tests; tear down.
docker rm -f spdata-pg >/dev/null 2>&1
docker run -d --name spdata-pg -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=orch -p 55432:5432 postgres:16 >/dev/null
# wait for readiness:
until docker exec spdata-pg pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done
export DATABASE_URL="postgres://postgres:pw@localhost:55432/orch"
# apply the schema (reliable path: psql the idempotent ddl; dbd-native path: `cd database && dbd reconcile`):
docker exec -i spdata-pg psql -U postgres -d orch -v ON_ERROR_STOP=1 < database/ddl/_apply_all.sql
cargo test -p sensei-orchestrator-store --features postgres -- --test-threads=1  ; echo "PG_TEST_EXIT=$?"
docker rm -f spdata-pg >/dev/null 2>&1
```
- `--test-threads=1` so parity tests don't collide on shared tables (or each test uses a unique `run_id` — prefer unique ids, keep the flag as a safety net).
- Read the REAL `PG_TEST_EXIT`. `postgres:16` pulls on first run.
- `database/ddl/_apply_all.sql` is a convenience include (Task 1) that `\i`'s the schema + tables in order — so the harness applies the dbd-authored ddl without needing dbd's target configured; `dbd reconcile` remains the authoring workflow.

---

## File Structure

- **Create** `gateway/database/design.yaml` + `database/ddl/schema/orchestrator.sql` + `database/ddl/table/orchestrator/{journal_events,cas_blobs,context_refs,run_snapshots,runs}.sql` + `database/ddl/_apply_all.sql` — the dbd schema.
- **Modify** `crates/orchestrator-store/Cargo.toml` — `[features] postgres = ["dep:sqlx", "dep:tokio"]` + optional `sqlx`.
- **Create** `crates/orchestrator-store/src/postgres.rs` (`#[cfg(feature="postgres")]`) — `connect()` + `PostgresJournal`/`PostgresContentStore`/`PostgresContextStore`.
- **Modify** `crates/orchestrator-store/src/lib.rs` — `#[cfg(feature="postgres")] pub mod postgres;`.
- **Modify** `crates/orchestrator-core/src/journal.rs` — add `JournalError::IncompatibleFormat` + a `FORMAT_VERSION` const.
- **Modify** `crates/orchestrator/Cargo.toml` + a gated e2e test — the headline cross-process resume (Task 5).

---

## Task 1: The dbd schema (`gateway/database/`)

**Files:** Create `gateway/database/design.yaml`, `database/ddl/schema/orchestrator.sql`, `database/ddl/table/orchestrator/*.sql`, `database/ddl/_apply_all.sql`.

- [ ] **Step 1: Study the convention + author the design.yaml**

Read `~/Developer/torii/database/design.yaml` for the exact dbd shape. Create `gateway/database/design.yaml`:

```yaml
project:
  name: sensei-orchestrator
source:
  dialect: postgresql
target:
  postgres:
    url: $DATABASE_URL
schemas:
  - orchestrator
```
(If dbd 0.10.5 requires the torii-style `target.supabase`, mirror torii's target block pointed at `$DATABASE_URL` — run `dbd doctor` to confirm the target parses.)

- [ ] **Step 2: Author the DDL (idempotent, dbd `ddl/<type>/<schema>/` layout)**

`database/ddl/schema/orchestrator.sql`:
```sql
create schema if not exists orchestrator;
```

`database/ddl/table/orchestrator/journal_events.sql`:
```sql
create table if not exists orchestrator.journal_events (
    seq        bigserial primary key,
    run_id     uuid        not null,
    event      jsonb       not null,
    created_at timestamptz not null default now()
);
create index if not exists journal_events_run_seq_idx
    on orchestrator.journal_events (run_id, seq);
```

`database/ddl/table/orchestrator/cas_blobs.sql`:
```sql
create table if not exists orchestrator.cas_blobs (
    digest     text        primary key,
    bytes      bytea       not null,
    created_at timestamptz not null default now()
);
```

`database/ddl/table/orchestrator/context_refs.sql`:
```sql
create table if not exists orchestrator.context_refs (
    scope_kind text        not null,   -- 'run' | 'node'
    scope_id   text        not null,   -- run id or node path
    ctx_key    text        not null,
    ctx_ref    jsonb       not null,   -- serialized ContextRef (references a cas digest)
    created_at timestamptz not null default now(),
    primary key (scope_kind, scope_id, ctx_key)
);
```

`database/ddl/table/orchestrator/run_snapshots.sql`:
```sql
create table if not exists orchestrator.run_snapshots (
    run_id     uuid        primary key,
    seq        bigint      not null,
    snapshot   jsonb       not null,
    updated_at timestamptz not null default now()
);
```

`database/ddl/table/orchestrator/runs.sql`:
```sql
create table if not exists orchestrator.runs (
    run_id         uuid        primary key,
    format_version integer     not null,
    created_at     timestamptz not null default now()
);
```

`database/ddl/_apply_all.sql` (harness convenience — applies schema then tables in dependency order):
```sql
\i database/ddl/schema/orchestrator.sql
\i database/ddl/table/orchestrator/journal_events.sql
\i database/ddl/table/orchestrator/cas_blobs.sql
\i database/ddl/table/orchestrator/context_refs.sql
\i database/ddl/table/orchestrator/run_snapshots.sql
\i database/ddl/table/orchestrator/runs.sql
```
(Note: `psql \i` paths are relative to the psql CWD; the harness runs psql with the repo root piped in, so the paths resolve. If `\i` pathing is awkward under `docker exec … < file`, inline the six files' contents into `_apply_all.sql` instead — the point is one idempotent apply.)

- [ ] **Step 3: Apply to a Docker Postgres + verify the schema is sound**

Start the Docker PG (harness top), apply `_apply_all.sql`, then:
```bash
docker exec spdata-pg psql -U postgres -d orch -c "\dt orchestrator.*"   # 5 tables present
docker exec spdata-pg psql -U postgres -d orch -c "select seq from orchestrator.journal_events limit 0"  # bigserial ok
( cd database && DATABASE_URL="$DATABASE_URL" dbd doctor ) 2>&1 | tail -5   # dbd sanity (if target parses)
```
Expected: the 5 `orchestrator.*` tables exist; re-applying `_apply_all.sql` is idempotent (no error). Tear down.

- [ ] **Step 4: Commit**

```bash
cd /Users/Jerry/Developer/gateway
git add database/
git commit -m "feat(orchestrator): SP-DATA-1 (1/5) — dbd schema for durable run state (journal/cas/context/snapshots/runs)"
```

---

## Task 2: The `postgres` feature + `connect()` + the Docker-PG harness smoke test

**Files:** Modify `crates/orchestrator-store/Cargo.toml`; create `crates/orchestrator-store/src/postgres.rs`; modify `crates/orchestrator-store/src/lib.rs`.

- [ ] **Step 1: Add the optional sqlx dep + `postgres` feature (mirror `vault`)**

In `crates/orchestrator-store/Cargo.toml`:
```toml
[features]
# Postgres adapters (PostgresJournal/ContentStore/ContextStore). Off by default so the crate
# stays dependency-light + the InMemory path is byte-identical; a deployment enables it.
postgres = ["dep:sqlx", "dep:tokio"]

[dependencies]
orchestrator-core = { package = "sensei-orchestrator-core", path = "../orchestrator-core" }
async-trait = "0.1"
serde_json = "1"
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres", "uuid"], optional = true }
tokio = { version = "1", optional = true }
```

- [ ] **Step 2: Declare the gated module + write the failing smoke test**

In `crates/orchestrator-store/src/lib.rs` add:
```rust
#[cfg(feature = "postgres")]
pub mod postgres;
```

Create `crates/orchestrator-store/src/postgres.rs` with the smoke test FIRST (the tests are `#[cfg(feature = "postgres")]` implicitly — the whole module is; they read `DATABASE_URL`):
```rust
//! SP-DATA-1: Postgres adapters for the run-state seams (`ExecutionJournal`/`ContentStore`/
//! `ContextStore`). Schema-agnostic — runs against the dbd-managed `orchestrator.*` schema.
//! Uses sqlx RUNTIME queries (not the compile-time `query!` macros) so the crate builds with
//! no database. Feature-gated: default builds don't pull sqlx.

use sqlx::postgres::{PgPool, PgPoolOptions};

/// Connect a pool to `database_url` (the dbd-applied `orchestrator.*` schema must exist).
pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new().max_connections(8).connect(database_url).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests require a live PG at $DATABASE_URL with the dbd schema applied (the Docker harness).
    /// Absent DATABASE_URL, they skip (so a bare `cargo test --features postgres` without a DB
    /// doesn't fail spuriously).
    fn db_url() -> Option<String> {
        std::env::var("DATABASE_URL").ok()
    }

    #[tokio::test]
    async fn connects_and_the_schema_exists() {
        let Some(url) = db_url() else { return };
        let pool = connect(&url).await.expect("connect");
        let (n,): (i64,) = sqlx::query_as(
            "select count(*) from information_schema.tables where table_schema='orchestrator'",
        )
        .fetch_one(&pool)
        .await
        .expect("query");
        assert!(n >= 5, "expected the 5 orchestrator.* tables, saw {n}");
    }
}
```

- [ ] **Step 3: Verify (Docker-PG harness) it fails then passes**

Default build unaffected: `cargo build -p sensei-orchestrator-store` (no feature) → clean, sqlx NOT pulled. `cargo build -p sensei-orchestrator-store --features postgres` → compiles. Then run the Docker-PG harness → `connects_and_the_schema_exists` PASSES (schema applied). Without the schema it would fail (n<5) — a genuine check. `cargo clippy -p sensei-orchestrator-store --features postgres --all-targets -- -D warnings` → clean.

- [ ] **Step 4: Confirm the whole default suite is byte-identical**

`cargo test -p sensei-orchestrator-store` (no feature, real `$?`) → the existing InMemory tests pass unchanged; `cargo build --workspace` clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator-store/Cargo.toml crates/orchestrator-store/src/lib.rs crates/orchestrator-store/src/postgres.rs
git commit -m "feat(orchestrator): SP-DATA-1 (2/5) — postgres feature + connect() + Docker-PG smoke test"
```

---

## Task 3: `PostgresJournal` + the durable `format_version` fence

**Files:** Modify `crates/orchestrator-core/src/journal.rs` (error variant + const); modify `crates/orchestrator-store/src/postgres.rs`.

- [ ] **Step 1: Add the fence primitives to core**

In `crates/orchestrator-core/src/journal.rs`: add a `FORMAT_VERSION` const (the effect-id/serialization scheme version) near the top of the module:
```rust
/// The durable journal format / effect-id scheme version. A persisted journal stamped with a
/// different value fences loudly on resume (never a silent mis-fold). Bump on any effect-id or
/// journal-serialization break.
pub const FORMAT_VERSION: i32 = 1;
```
And add a variant to `pub enum JournalError` (match the existing variant style):
```rust
    /// A persisted journal's `format_version` differs from this build's [`FORMAT_VERSION`] —
    /// the effect-id/serialization scheme is incompatible; resume must halt, not mis-fold.
    IncompatibleFormat { run: RunId, stored: i32, expected: i32 },
```
(Add its `Display`/`thiserror` message alongside the other variants.)

- [ ] **Step 2: Write the failing `PostgresJournal` tests**

Add to `postgres.rs` `#[cfg(test)] mod tests` (each uses a UNIQUE `run_id` so tests are isolated). Mirror `InMemoryJournal`'s tests in `lib.rs`:
```rust
    use orchestrator_core::{
        EffectClass, ExecutionJournal, JournalError, JournalEvent, NodeId, RunId, Seq,
    };

    fn run() -> RunId { RunId(uuid::Uuid::new_v4()) }
    fn started() -> JournalEvent { JournalEvent::RunStarted { version: "v1".into() } } // match the real variant shape
    fn node_started(id: &str) -> JournalEvent { JournalEvent::NodeStarted { node: NodeId(id.into()) } }

    #[tokio::test]
    async fn append_then_load_returns_events_in_ascending_seq() {
        let Some(url) = db_url() else { return };
        let j = PostgresJournal::new(connect(&url).await.unwrap());
        let r = run();
        let s1 = j.append(r, started()).await.unwrap();
        let s2 = j.append(r, node_started("n1")).await.unwrap();
        assert!(s2 > s1, "seq monotonic");
        let evs = j.load(r).await.unwrap();
        assert_eq!(evs.len(), 2);
        assert!(evs[0].0 < evs[1].0, "ascending seq order");
    }

    #[tokio::test]
    async fn load_since_returns_only_the_tail() {
        let Some(url) = db_url() else { return };
        let j = PostgresJournal::new(connect(&url).await.unwrap());
        let r = run();
        let s1 = j.append(r, started()).await.unwrap();
        let _ = j.append(r, node_started("n1")).await.unwrap();
        let tail = j.load_since(r, s1).await.unwrap();
        assert_eq!(tail.len(), 1, "only events with seq > s1");
    }

    #[tokio::test]
    async fn incompatible_format_version_fences_on_load() {
        let Some(url) = db_url() else { return };
        let pool = connect(&url).await.unwrap();
        let j = PostgresJournal::new(pool.clone());
        let r = run();
        j.append(r, started()).await.unwrap();
        // Simulate a journal written by an OLDER scheme: corrupt the runs.format_version.
        sqlx::query("update orchestrator.runs set format_version = -999 where run_id = $1")
            .bind(r.0).execute(&pool).await.unwrap();
        let err = j.load(r).await.unwrap_err();
        assert!(matches!(err, JournalError::IncompatibleFormat { .. }), "must fence, got {err:?}");
    }
```
(Also add a `snapshot`/`latest_snapshot` round-trip test + a `compact` test mirroring `InMemoryJournal`'s.)

- [ ] **Step 3: Verify they fail (Docker-PG)** — `cannot find PostgresJournal`. Report.

- [ ] **Step 4: Implement `PostgresJournal`**

Add to `postgres.rs`. NOTE: `Seq` wraps a `u64`; `bigserial` is `i64` — cast. `JournalEvent` is `Serialize`/`Deserialize` (jsonb via `serde_json::Value`). Use RUNTIME `sqlx::query`/`query_as`.
```rust
use orchestrator_core::{
    ExecutionJournal, JournalError, JournalEvent, RunId, Seq, Snapshot, FORMAT_VERSION,
};

pub struct PostgresJournal { pool: PgPool }

impl PostgresJournal {
    pub fn new(pool: PgPool) -> Self { Self { pool } }

    async fn check_format(&self, run: RunId) -> Result<(), JournalError> {
        let row: Option<(i32,)> = sqlx::query_as(
            "select format_version from orchestrator.runs where run_id = $1")
            .bind(run.0).fetch_optional(&self.pool).await.map_err(pg_err)?;
        if let Some((stored,)) = row {
            if stored != FORMAT_VERSION {
                return Err(JournalError::IncompatibleFormat { run, stored, expected: FORMAT_VERSION });
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ExecutionJournal for PostgresJournal {
    async fn append(&self, run: RunId, event: JournalEvent) -> Result<Seq, JournalError> {
        let ev = serde_json::to_value(&event).map_err(ser_err)?;
        // Stamp the format version once per run (on the first RunStarted); idempotent.
        if matches!(event, JournalEvent::RunStarted { .. }) {
            sqlx::query("insert into orchestrator.runs (run_id, format_version) values ($1,$2) on conflict (run_id) do nothing")
                .bind(run.0).bind(FORMAT_VERSION).execute(&self.pool).await.map_err(pg_err)?;
        }
        let (seq,): (i64,) = sqlx::query_as(
            "insert into orchestrator.journal_events (run_id, event) values ($1,$2) returning seq")
            .bind(run.0).bind(ev).fetch_one(&self.pool).await.map_err(pg_err)?;
        Ok(Seq(seq as u64))
    }

    async fn load(&self, run: RunId) -> Result<Vec<(Seq, JournalEvent)>, JournalError> {
        self.check_format(run).await?;
        let rows: Vec<(i64, serde_json::Value)> = sqlx::query_as(
            "select seq, event from orchestrator.journal_events where run_id=$1 order by seq")
            .bind(run.0).fetch_all(&self.pool).await.map_err(pg_err)?;
        rows.into_iter().map(|(s, v)| Ok((Seq(s as u64), serde_json::from_value(v).map_err(ser_err)?))).collect()
    }

    async fn load_since(&self, run: RunId, since: Seq) -> Result<Vec<(Seq, JournalEvent)>, JournalError> {
        self.check_format(run).await?;
        let rows: Vec<(i64, serde_json::Value)> = sqlx::query_as(
            "select seq, event from orchestrator.journal_events where run_id=$1 and seq > $2 order by seq")
            .bind(run.0).bind(since.0 as i64).fetch_all(&self.pool).await.map_err(pg_err)?;
        rows.into_iter().map(|(s, v)| Ok((Seq(s as u64), serde_json::from_value(v).map_err(ser_err)?))).collect()
    }

    async fn snapshot(&self, run: RunId, snap: Snapshot) -> Result<(), JournalError> {
        let v = serde_json::to_value(&snap).map_err(ser_err)?;
        sqlx::query("insert into orchestrator.run_snapshots (run_id, seq, snapshot) values ($1,$2,$3)
                     on conflict (run_id) do update set seq=excluded.seq, snapshot=excluded.snapshot, updated_at=now()")
            .bind(run.0).bind(snap.seq.0 as i64).bind(v).execute(&self.pool).await.map_err(pg_err)?;
        Ok(())
    }

    async fn latest_snapshot(&self, run: RunId) -> Result<Option<Snapshot>, JournalError> {
        let row: Option<(serde_json::Value,)> = sqlx::query_as(
            "select snapshot from orchestrator.run_snapshots where run_id=$1")
            .bind(run.0).fetch_optional(&self.pool).await.map_err(pg_err)?;
        row.map(|(v,)| serde_json::from_value(v).map_err(ser_err)).transpose()
    }

    async fn compact(&self, run: RunId, remove_seqs: &[Seq], add: JournalEvent) -> Result<(), JournalError> {
        let ev = serde_json::to_value(&add).map_err(ser_err)?;
        let removes: Vec<i64> = remove_seqs.iter().map(|s| s.0 as i64).collect();
        let mut tx = self.pool.begin().await.map_err(pg_err)?;
        sqlx::query("delete from orchestrator.journal_events where run_id=$1 and seq = any($2)")
            .bind(run.0).bind(&removes).execute(&mut *tx).await.map_err(pg_err)?;
        sqlx::query("insert into orchestrator.journal_events (run_id, event) values ($1,$2)")
            .bind(run.0).bind(ev).execute(&mut *tx).await.map_err(pg_err)?;
        tx.commit().await.map_err(pg_err)?;
        Ok(())
    }
}

// Error mappers — adapt the JournalError variant names to the real enum (a Postgres/serde error
// maps to the existing loud/fatal variant; grep JournalError for the right one).
fn pg_err(e: sqlx::Error) -> JournalError { /* JournalError::Backend(e.to_string()) or the existing variant */ }
fn ser_err(e: serde_json::Error) -> JournalError { /* same */ }
```
Adapt `pg_err`/`ser_err` to the actual `JournalError` variants (grep `enum JournalError`; add a `Backend(String)` variant if none fits — a small core addition). Match the real `JournalEvent` variant shapes in the test helpers.

- [ ] **Step 5: Verify (Docker-PG) the journal tests pass** — run the harness → append/load/load_since/snapshot/compact/fence tests green. clippy `--features postgres` clean.

- [ ] **Step 6: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator-core/src/journal.rs crates/orchestrator-store/src/postgres.rs
git commit -m "feat(orchestrator): SP-DATA-1 (3/5) — PostgresJournal + durable format_version fence"
```

---

## Task 4: `PostgresContentStore` + `PostgresContextStore` + parity tests

**Files:** Modify `crates/orchestrator-store/src/postgres.rs`.

- [ ] **Step 1: Write the failing CAS + context tests (mirror the InMemory tests)**

Study `crates/orchestrator-store/src/stores.rs` (`content_store_dedupes_identical_bytes_and_misses_loudly`, `context_store_collides_resolves_node_to_run_and_misses_to_none`, `insert_ref_rehydrates_an_entry_without_recomputing_the_cas`) and add Postgres equivalents to `postgres.rs` tests (unique keys per test): `put` of identical bytes → one row + same `Digest`; `get` miss → the same loud error; context `put` collision → `ContextKeyCollision`; `get` resolves Node→Run; `insert_ref` idempotent (re-insert same ref no error). Assert against `PostgresContentStore`/`PostgresContextStore`.

- [ ] **Step 2: Verify they fail (Docker-PG).**

- [ ] **Step 3: Implement the two stores**

Add to `postgres.rs`. Match `InMemory*`'s observable behavior (digests via `orchestrator_core::digest_of`; the same error variants).
```rust
use orchestrator_core::{ContentStore, ContextStore, ContextKey, ContextRef, Digest, OrchestratorError, Scope, digest_of};

pub struct PostgresContentStore { pool: PgPool }
impl PostgresContentStore { pub fn new(pool: PgPool) -> Self { Self { pool } } }

#[async_trait::async_trait]
impl ContentStore for PostgresContentStore {
    async fn put(&self, bytes: &[u8]) -> Result<Digest, OrchestratorError> {
        let d = digest_of(bytes);
        sqlx::query("insert into orchestrator.cas_blobs (digest, bytes) values ($1,$2) on conflict (digest) do nothing")
            .bind(&d.0).bind(bytes).execute(&self.pool).await.map_err(cas_err)?;
        Ok(d)
    }
    async fn get(&self, digest: &Digest) -> Result<Vec<u8>, OrchestratorError> {
        let row: Option<(Vec<u8>,)> = sqlx::query_as("select bytes from orchestrator.cas_blobs where digest=$1")
            .bind(&digest.0).fetch_optional(&self.pool).await.map_err(cas_err)?;
        row.map(|(b,)| b).ok_or_else(|| OrchestratorError::ContentDigestMiss(digest.clone())) // match the real miss variant
    }
}
```
For `PostgresContextStore`: `put(scope,key,value)` → serialize value, `PostgresContentStore::put` it (or write cas_blobs directly) → build a `ContextRef` → `insert into context_refs … on conflict → error` (collision must be LOUD, so NOT `do nothing` — detect the conflict → `ContextKeyCollision`); `get(scope,key)` resolves Node→Run (query node scope first, then run); `load(ref)` → get the bytes from cas_blobs by the ref's digest → deserialize; `insert_ref` → upsert (idempotent, `on conflict do nothing`). Decompose `Scope` into `(scope_kind, scope_id)` for the key columns. Match the exact `ContextRef`/`Scope` shapes from `context.rs`.

- [ ] **Step 4: Verify (Docker-PG) the CAS + context tests pass.** clippy clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator-store/src/postgres.rs
git commit -m "feat(orchestrator): SP-DATA-1 (4/5) — PostgresContentStore + PostgresContextStore (parity)"
```

---

## Task 5: The headline — cross-process resume e2e + in-doubt reconcile + additivity gate

**Files:** Modify `crates/orchestrator/Cargo.toml` (a gated test feature); add a gated e2e test to `crates/orchestrator/src/executor/tests.rs` (or a new `crates/orchestrator/tests/postgres_resume.rs`).

- [ ] **Step 1: Wire a gated test feature on the orchestrator crate**

In `crates/orchestrator/Cargo.toml` add a feature that enables the store's postgres adapters for tests:
```toml
[features]
postgres-tests = ["sensei-orchestrator-store/postgres", "dep:sqlx"]
```
(Add `sensei-orchestrator-store` as a normal/dev dependency if not already; add optional `sqlx` for the seed helper. Adapt to how the crate currently depends on orchestrator-store.)

- [ ] **Step 2: Write the headline cross-process resume test (`#[cfg(feature="postgres-tests")]`)**

Mirror the existing in-memory resume tests (grep `broker_not_reinvoked_for_a_memoized_tool_on_resume` / `fs_write_replays_from_memo_without_rewriting_on_resume`) but with `PostgresJournal` + `PostgresContentStore` on a Docker PG. Shape:
- Build a `PostgresJournal`/`PostgresContentStore` (`connect($DATABASE_URL)`), a fresh `RunId`.
- Run a partial agent/ModelCall graph to completion of an effect via a scripted gateway (journals to PG).
- Construct a **fresh** `Executor` + **fresh** `PostgresJournal`/`ContentStore` instances on the SAME `DATABASE_URL` (simulating process B) + a scripted gateway with a call-counter, and `.start(run, graph)`.
- Assert: the run COMPLETES; the completed effect replays from the durable journal with the fresh gateway's call-counter proving **zero re-spend**; a journaled `EffectOutput::Ref` (force one over the CAS threshold) materializes from the PG CAS.
Also add `postgres_in_doubt_reconcile_is_durable`: seed a standing `EffectIntent` (no `EffectRecorded`) in PG (via `PostgresJournal::append` of the Intent), resume via a fresh executor → the `in_doubt→reconcile` path runs (assert the reconciler is consulted / the run pauses or confirms, mirroring the in-memory in-doubt test).

- [ ] **Step 3: Verify (Docker-PG) the e2e passes**

Docker-PG harness, but for the orchestrator crate:
```bash
# (Docker PG up + schema applied, DATABASE_URL exported, as in the harness)
cargo test -p sensei-orchestrator --features postgres-tests -- --test-threads=1 postgres_ ; echo "E2E_EXIT=$?"
```
Expected: the cross-process resume test passes (0 re-spend, Ref materialized) + the in-doubt test passes. REAL exit code.

- [ ] **Step 4: Additivity + full-suite gate (feature OFF ⇒ byte-identical)**

On the macOS host (no PG), REAL unpiped exit codes, aggregate DIRECTLY:
```bash
cd /Users/Jerry/Developer/gateway
cargo test --workspace > /tmp/spd_fulltest.log 2>&1; echo "EXIT=$?"
grep -c "test result: ok" /tmp/spd_fulltest.log
grep -oE "[0-9]+ passed" /tmp/spd_fulltest.log | awk '{s+=$1} END{print s}'
grep -oE "[1-9][0-9]* failed" /tmp/spd_fulltest.log | head
cargo fmt --all --check; echo "FMT=$?"
cargo clippy --workspace --all-targets -- -D warnings > /tmp/spd_clippy.log 2>&1; echo "CLIPPY=$?"
```
Confirm `EXIT=0`, 0 failed, total = **1120** (byte-identical — the postgres feature is off by default; no non-postgres test added; the core `JournalError::IncompatibleFormat` variant + `FORMAT_VERSION` const are additive). `FMT=0`, `CLIPPY=0`. Also `cargo clippy -p sensei-orchestrator-store --features postgres --all-targets -- -D warnings` clean (the feature-on lint).

- [ ] **Step 5: Commit** (do NOT push)

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/Cargo.toml crates/orchestrator/src/executor/tests.rs
git commit -m "test(orchestrator): SP-DATA-1 (5/5) — cross-process resume e2e + durable in-doubt reconcile + additivity gate"
```

---

## Self-Review notes (author)

- **Spec coverage:** §4.1 schema → Task 1. §4.2 adapters → Tasks 2–4 (`connect` + Journal + CAS + Context). §4.3 cross-process resume + fence → Task 3 (fence) + Task 5 (resume e2e) + the durable in-doubt reconcile → Task 5. §4.5 Docker-PG verification → the harness (all tasks). §4.6 additive → Task 5 gate. AC1→T3, AC2→T3, AC3→T4, AC4→T5, AC5→T5, AC6→T3, AC7→T5.
- **Verification reality:** the Postgres code compiles on any host (feature-gated, runtime sqlx queries) but the behavior is verified on a **Docker Postgres** (analogous to the Linux slice's Docker) + a CI `postgres` service; default `cargo test` stays DB-free + byte-identical (feature off). Tests skip when `DATABASE_URL` is unset so a bare `--features postgres` run doesn't fail spuriously.
- **Adapt-in-DB items (like the sandbox's adapt-in-Docker):** the exact `JournalError`/`OrchestratorError` variant names (`pg_err`/`cas_err`/miss), the real `JournalEvent`/`Scope`/`ContextRef` variant shapes, and dbd's `target` config for a plain Postgres — resolved against the real types + `dbd doctor` when the Docker harness compiles/runs. The DDL + the trait semantics (parity with InMemory) are exact.
- **dbd:** a fresh project ⇒ pre-release ⇒ `dbd reconcile` authoring; the `dbd-pattern-verifier` agent reviews `database/` in the whole-slice review. The psql `_apply_all.sql` is only the test-harness applicator (idempotent), not a migration.
- **Additive:** `postgres` feature off by default + `#[cfg(feature)]` module + the two additive core items (const + error variant) ⇒ the 1120-test suite byte-identical.
```
