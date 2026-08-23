//! SP-DATA-1: Postgres adapters for the run-state seams (`ExecutionJournal`/`ContentStore`/
//! `ContextStore`). Schema-agnostic — runs against the dbd-managed `orchestrator.*` schema.
//! Uses sqlx RUNTIME queries (not the compile-time `query!` macros) so the crate builds with
//! no database. Feature-gated: default builds don't pull sqlx.

use chrono::{DateTime, Utc};
use orchestrator_core::{
    ChainBinding, ConfigSource, ContentRef, ContentStore, ContextKey, ContextRef, ContextStore,
    Digest, ExecutionJournal, FORMAT_VERSION, Graph, JournalError, JournalEvent, OrchestratorError,
    RegistryConfig, RunId, RunStatus, ScheduledRun, SchedulerStore, Scope, Seq, Snapshot,
    digest_of,
};
use sqlx::postgres::{PgPool, PgPoolOptions};

/// Connect a pool to `database_url` (the dbd-applied `orchestrator.*` schema must exist),
/// capped at 8 connections.
///
/// 8 was chosen (SP-DATA-1) for a world where each Postgres-backed adapter
/// (`PostgresJournal`/`PostgresContentStore`/`PostgresContextStore`/`PostgresSchedulerStore`/
/// `PostgresConfigSource`) opened its OWN pool — 8 connections per adapter, comfortably
/// inside a default Postgres's headroom. SP-DATA-4 then made every adapter in a worker
/// process share ONE pool (`boot::heavy`/`boot::light`) specifically to avoid four (or
/// five) separate 8-connection pools per worker, so that default is now the WORKER's
/// entire connection budget, not one adapter's. Most workers are still fine at 8 — the
/// journal/CAS/context acquires are short-lived — but a worker under sustained
/// concurrency, or a fleet of several workers against one Postgres, may want more. Use
/// [`connect_with_max`] for that; `connect` keeps this default for its many existing
/// callers, so behaviour is unchanged unless a caller opts in.
pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    connect_with_max(database_url, DEFAULT_MAX_CONNECTIONS).await
}

/// The default [`connect`] delegates to — see its doc comment.
const DEFAULT_MAX_CONNECTIONS: u32 = 8;

/// Connect a pool to `database_url`, capped at `max` connections. See [`connect`]'s doc
/// comment for why the default is 8 and when a caller might want more (e.g. torii's
/// `TORII_POOL_SIZE`, wired in `crates/torii/src/boot.rs`).
pub async fn connect_with_max(database_url: &str, max: u32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(max)
        .connect(database_url)
        .await
}

/// Map a Postgres/sqlx transport error onto the strict, surfaced journal error. Journal
/// writes never swallow a backend failure — it becomes a loud `Backend`.
fn pg_err(e: sqlx::Error) -> JournalError {
    JournalError::Backend(e.to_string())
}

/// Map a serde (de)serialization error onto the same strict backend error — a malformed
/// journal payload is a backend fault, never silently dropped.
fn ser_err(e: serde_json::Error) -> JournalError {
    JournalError::Backend(e.to_string())
}

/// A durable [`ExecutionJournal`] backed by the dbd-managed `orchestrator.*` schema.
///
/// Parity with [`InMemoryJournal`](crate::InMemoryJournal): `append` stamps a monotonic
/// `Seq` (the `bigserial` `journal_events.seq`), `load`/`load_since` return events in
/// ascending `Seq`, `snapshot`/`latest_snapshot` are latest-wins, and `compact` removes
/// the named seqs and appends the manifest in one transaction. Additionally, every load
/// checks the run's persisted [`FORMAT_VERSION`] and fences with
/// [`JournalError::IncompatibleFormat`] on a mismatch — a journal written by an
/// incompatible effect-id/serialization scheme halts resume loudly rather than mis-folding.
#[derive(Clone)]
pub struct PostgresJournal {
    pool: PgPool,
}

impl PostgresJournal {
    /// Wrap a connection pool (see [`connect`]).
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Fence: if this run's persisted `format_version` differs from this build's
    /// [`FORMAT_VERSION`], the durable journal was written by an incompatible scheme —
    /// halt loudly. A run with no `runs` row (no `RunStarted` yet) is not fenced.
    async fn check_format(&self, run: RunId) -> Result<(), JournalError> {
        let row: Option<(i32,)> =
            sqlx::query_as("select format_version from orchestrator.runs where run_id = $1")
                .bind(run.0)
                .fetch_optional(&self.pool)
                .await
                .map_err(pg_err)?;
        if let Some((stored,)) = row
            && stored != FORMAT_VERSION
        {
            return Err(JournalError::IncompatibleFormat {
                run,
                stored,
                expected: FORMAT_VERSION,
            });
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
            sqlx::query(
                "insert into orchestrator.runs (run_id, format_version) values ($1, $2) \
                 on conflict (run_id) do nothing",
            )
            .bind(run.0)
            .bind(FORMAT_VERSION)
            .execute(&self.pool)
            .await
            .map_err(pg_err)?;
        }
        let (seq,): (i64,) = sqlx::query_as(
            "insert into orchestrator.journal_events (run_id, event) values ($1, $2) returning seq",
        )
        .bind(run.0)
        .bind(ev)
        .fetch_one(&self.pool)
        .await
        .map_err(pg_err)?;
        Ok(seq as Seq)
    }

    async fn load(&self, run: RunId) -> Result<Vec<(Seq, JournalEvent)>, JournalError> {
        self.check_format(run).await?;
        let rows: Vec<(i64, serde_json::Value)> = sqlx::query_as(
            "select seq, event from orchestrator.journal_events where run_id = $1 order by seq",
        )
        .bind(run.0)
        .fetch_all(&self.pool)
        .await
        .map_err(pg_err)?;
        rows.into_iter()
            .map(|(s, v)| Ok((s as Seq, serde_json::from_value(v).map_err(ser_err)?)))
            .collect()
    }

    async fn load_since(
        &self,
        run: RunId,
        since: Seq,
    ) -> Result<Vec<(Seq, JournalEvent)>, JournalError> {
        self.check_format(run).await?;
        let rows: Vec<(i64, serde_json::Value)> = sqlx::query_as(
            "select seq, event from orchestrator.journal_events \
             where run_id = $1 and seq > $2 order by seq",
        )
        .bind(run.0)
        .bind(since as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(pg_err)?;
        rows.into_iter()
            .map(|(s, v)| Ok((s as Seq, serde_json::from_value(v).map_err(ser_err)?)))
            .collect()
    }

    async fn snapshot(&self, run: RunId, snap: Snapshot) -> Result<(), JournalError> {
        let v = serde_json::to_value(&snap).map_err(ser_err)?;
        sqlx::query(
            "insert into orchestrator.run_snapshots (run_id, seq, snapshot) values ($1, $2, $3) \
             on conflict (run_id) do update set \
             seq = excluded.seq, snapshot = excluded.snapshot, updated_at = now()",
        )
        .bind(run.0)
        .bind(snap.seq as i64)
        .bind(v)
        .execute(&self.pool)
        .await
        .map_err(pg_err)?;
        Ok(())
    }

    async fn latest_snapshot(&self, run: RunId) -> Result<Option<Snapshot>, JournalError> {
        let row: Option<(serde_json::Value,)> =
            sqlx::query_as("select snapshot from orchestrator.run_snapshots where run_id = $1")
                .bind(run.0)
                .fetch_optional(&self.pool)
                .await
                .map_err(pg_err)?;
        row.map(|(v,)| serde_json::from_value(v).map_err(ser_err))
            .transpose()
    }

    async fn compact(
        &self,
        run: RunId,
        remove_seqs: &[Seq],
        add: JournalEvent,
    ) -> Result<(), JournalError> {
        let ev = serde_json::to_value(&add).map_err(ser_err)?;
        let removes: Vec<i64> = remove_seqs.iter().map(|s| *s as i64).collect();
        // One transaction: drop the compacted events, then append the manifest (a fresh,
        // higher seq). The remaining events keep their original ascending seq order.
        let mut tx = self.pool.begin().await.map_err(pg_err)?;
        sqlx::query("delete from orchestrator.journal_events where run_id = $1 and seq = any($2)")
            .bind(run.0)
            .bind(&removes)
            .execute(&mut *tx)
            .await
            .map_err(pg_err)?;
        sqlx::query("insert into orchestrator.journal_events (run_id, event) values ($1, $2)")
            .bind(run.0)
            .bind(ev)
            .execute(&mut *tx)
            .await
            .map_err(pg_err)?;
        tx.commit().await.map_err(pg_err)?;
        Ok(())
    }
}

/// Map a Postgres/sqlx transport error onto a loud, surfaced orchestrator error. The CAS +
/// context stores never swallow a backend failure — it surfaces as `Store(..)`, the dedicated
/// store-backend channel (distinct from the journal's `Backend`, so a CAS/context fault isn't
/// mislabeled a journal fault). (The InMemory happy path never produces a transport error;
/// parity only concerns the domain variants `ContentDigestMiss` / `ContextKeyCollision` /
/// `Serialization`, which are matched exactly below.)
fn store_err(e: sqlx::Error) -> OrchestratorError {
    OrchestratorError::Store(e.to_string())
}

/// Map a serde (de)serialization error onto the same `Store` transport channel — used on the
/// [`PostgresConfigSource`] WRITE path (encoding a domain object to jsonb before the insert).
/// The load path uses [`cfg_load_err`] instead (→ `RegistryLoad`, the `ConfigSource` convention).
fn store_err_ser(e: serde_json::Error) -> OrchestratorError {
    OrchestratorError::Store(e.to_string())
}

/// True if `e` is a Postgres UNIQUE / primary-key violation (SQLSTATE 23505) — the loud signal
/// that a `(scope, key)` row already exists, which [`PostgresContextStore::put`] maps to a
/// [`ContextKeyCollision`](OrchestratorError::ContextKeyCollision).
fn is_unique_violation(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .map(|d| d.is_unique_violation())
        .unwrap_or(false)
}

/// The human scope label carried by a `ContextKeyCollision` — byte-for-byte parity with the
/// InMemory store (`"Run"` / `"Node(<id>)"`).
fn scope_label(scope: &Scope) -> String {
    match scope {
        Scope::Run => "Run".to_string(),
        Scope::Node(n) => format!("Node({})", n.0),
    }
}

/// A durable [`ContentStore`] backed by the `orchestrator.cas_blobs` table.
///
/// Parity with [`InMemoryContentStore`](crate::InMemoryContentStore): `put` is content-addressed
/// and idempotent (identical bytes → the same [`Digest`] + exactly one row, via
/// `on conflict do nothing`); `get` is strict — a digest miss is a loud
/// [`ContentDigestMiss`](OrchestratorError::ContentDigestMiss), never an empty value.
#[derive(Clone)]
pub struct PostgresContentStore {
    pool: PgPool,
}

impl PostgresContentStore {
    /// Wrap a connection pool (see [`connect`]).
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl ContentStore for PostgresContentStore {
    async fn put(&self, bytes: &[u8]) -> Result<Digest, OrchestratorError> {
        let digest = digest_of(bytes);
        // Idempotent dedupe: the digest is the PK, so identical content stores once.
        sqlx::query(
            "insert into orchestrator.cas_blobs (digest, bytes) values ($1, $2) \
             on conflict (digest) do nothing",
        )
        .bind(&digest.0)
        .bind(bytes)
        .execute(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(digest)
    }

    async fn get(&self, digest: &Digest) -> Result<Vec<u8>, OrchestratorError> {
        let row: Option<(Vec<u8>,)> =
            sqlx::query_as("select bytes from orchestrator.cas_blobs where digest = $1")
                .bind(&digest.0)
                .fetch_optional(&self.pool)
                .await
                .map_err(store_err)?;
        row.map(|(b,)| b)
            .ok_or_else(|| OrchestratorError::ContentDigestMiss(digest.0.clone()))
    }
}

/// A durable [`ContextStore`] backed by the `orchestrator.context_refs` table (keyed by
/// `(scope_kind, scope_id, ctx_key)`), storing each value's bytes once in the shared CAS.
///
/// Parity with [`InMemoryContextStore`](crate::InMemoryContextStore): `put` writes the value to
/// the CAS then inserts the ref, rejecting a re-write of an existing `(scope, key)` LOUDLY with
/// [`ContextKeyCollision`](OrchestratorError::ContextKeyCollision) (a mapped UNIQUE violation, no
/// silent last-write-wins); `get` resolves **up** the scope chain (`Node` → `Run`) and returns
/// `Ok(None)` on a miss; `load` fetches the referenced bytes lazily via the CAS; `insert_ref`
/// rehydrates a journaled write idempotently (`on conflict do nothing`, no CAS touch).
#[derive(Clone)]
pub struct PostgresContextStore {
    pool: PgPool,
}

impl PostgresContextStore {
    /// Wrap a connection pool (see [`connect`]).
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Decompose a [`Scope`] into the `(scope_kind, scope_id)` primary-key columns. `Run` carries
    /// an empty id; `Node(id)` carries the node path.
    fn scope_cols(scope: &Scope) -> (&'static str, String) {
        match scope {
            Scope::Run => ("run", String::new()),
            Scope::Node(n) => ("node", n.0.clone()),
        }
    }

    /// The shared CAS over the same pool (values are stored as content-addressed refs).
    fn cas(&self) -> PostgresContentStore {
        PostgresContentStore::new(self.pool.clone())
    }

    /// Fetch the single ref at the exact `(scope_kind, scope_id, ctx_key)` row, if present.
    async fn fetch(
        &self,
        kind: &str,
        id: &str,
        key: &str,
    ) -> Result<Option<ContextRef>, OrchestratorError> {
        let row: Option<(serde_json::Value,)> = sqlx::query_as(
            "select ctx_ref from orchestrator.context_refs \
             where scope_kind = $1 and scope_id = $2 and ctx_key = $3",
        )
        .bind(kind)
        .bind(id)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_err)?;
        row.map(|(v,)| serde_json::from_value(v).map_err(OrchestratorError::from))
            .transpose()
    }
}

#[async_trait::async_trait]
impl ContextStore for PostgresContextStore {
    async fn put(
        &self,
        scope: Scope,
        key: ContextKey,
        value: serde_json::Value,
    ) -> Result<ContextRef, OrchestratorError> {
        // Store the bytes first (idempotent CAS), then build + insert the ref.
        let bytes = serde_json::to_vec(&value)?;
        let digest = self.cas().put(&bytes).await?;
        let context_ref = ContextRef {
            key: key.clone(),
            scope: scope.clone(),
            content: ContentRef {
                digest,
                size: bytes.len(),
                summary: None,
            },
            summary: None,
        };
        let (kind, id) = Self::scope_cols(&scope);
        let ref_json = serde_json::to_value(&context_ref)?;
        // LOUD collision: a plain insert (never `on conflict do nothing`). A PK unique-violation
        // means this `(scope, key)` already exists → map it to `ContextKeyCollision`, not a
        // silent overwrite.
        let res = sqlx::query(
            "insert into orchestrator.context_refs (scope_kind, scope_id, ctx_key, ctx_ref) \
             values ($1, $2, $3, $4)",
        )
        .bind(kind)
        .bind(&id)
        .bind(&key.0)
        .bind(ref_json)
        .execute(&self.pool)
        .await;
        match res {
            Ok(_) => Ok(context_ref),
            Err(e) if is_unique_violation(&e) => Err(OrchestratorError::ContextKeyCollision {
                scope: scope_label(&scope),
                key: key.0,
            }),
            Err(e) => Err(store_err(e)),
        }
    }

    async fn get(
        &self,
        scope: Scope,
        key: ContextKey,
    ) -> Result<Option<ContextRef>, OrchestratorError> {
        let (kind, id) = Self::scope_cols(&scope);
        if let Some(found) = self.fetch(kind, &id, &key.0).await? {
            return Ok(Some(found));
        }
        // Resolve up the scope chain: a Node read falls back to the Run-scoped entry.
        if let Scope::Node(_) = scope {
            let (rk, rid) = Self::scope_cols(&Scope::Run);
            if let Some(found) = self.fetch(rk, &rid, &key.0).await? {
                return Ok(Some(found));
            }
        }
        Ok(None)
    }

    async fn load(&self, r: &ContextRef) -> Result<serde_json::Value, OrchestratorError> {
        let bytes = self.cas().get(&r.content.digest).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    async fn insert_ref(&self, r: ContextRef) -> Result<(), OrchestratorError> {
        // Rehydration from a journaled write: idempotent upsert (a fold replays every write), no
        // collision check — the journal is the source of truth. No CAS touch; the blob already
        // lives there. `do nothing` is first-write-wins (vs InMemory's last-write-wins overwrite);
        // immaterial, since `put` enforces collisions at journal-write time so a fold only ever
        // replays an identical ref for a given `(scope, key)`.
        let (kind, id) = Self::scope_cols(&r.scope);
        let ref_json = serde_json::to_value(&r)?;
        sqlx::query(
            "insert into orchestrator.context_refs (scope_kind, scope_id, ctx_key, ctx_ref) \
             values ($1, $2, $3, $4) on conflict (scope_kind, scope_id, ctx_key) do nothing",
        )
        .bind(kind)
        .bind(&id)
        .bind(&r.key.0)
        .bind(ref_json)
        .execute(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(())
    }
}

/// Read the whole registry over ONE connection. Callers decide the snapshot
/// semantics: `load` uses a pooled connection (per-statement snapshots — the
/// documented non-atomic path, retained for the unversioned contract), while
/// `load_versioned` passes a REPEATABLE READ transaction for one snapshot.
async fn read_all(conn: &mut sqlx::PgConnection) -> Result<RegistryConfig, OrchestratorError> {
    let agents: Vec<(String, serde_json::Value)> =
        sqlx::query_as("select name, def from orchestrator.config_agents order by name")
            .fetch_all(&mut *conn)
            .await
            .map_err(cfg_load_err)?;
    let skills: Vec<(String, serde_json::Value)> =
        sqlx::query_as("select name, def from orchestrator.config_skills order by name")
            .fetch_all(&mut *conn)
            .await
            .map_err(cfg_load_err)?;
    let tools: Vec<(String, serde_json::Value)> =
        sqlx::query_as("select name, spec from orchestrator.config_tools order by name")
            .fetch_all(&mut *conn)
            .await
            .map_err(cfg_load_err)?;
    let bindings: Vec<(String, String, String)> = sqlx::query_as(
        "select area, kind, chain from orchestrator.config_chain_bindings order by area, kind",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(cfg_load_err)?;
    Ok(RegistryConfig {
        agents: agents
            .into_iter()
            .map(|(n, v)| {
                serde_json::from_value(v)
                    .map_err(|e| OrchestratorError::RegistryLoad(format!("deser agent {n}: {e}")))
            })
            .collect::<Result<_, _>>()?,
        skills: skills
            .into_iter()
            .map(|(n, v)| {
                serde_json::from_value(v)
                    .map_err(|e| OrchestratorError::RegistryLoad(format!("deser skill {n}: {e}")))
            })
            .collect::<Result<_, _>>()?,
        tools: tools
            .into_iter()
            .map(|(n, v)| {
                serde_json::from_value(v)
                    .map_err(|e| OrchestratorError::RegistryLoad(format!("deser tool {n}: {e}")))
            })
            .collect::<Result<_, _>>()?,
        chain_bindings: bindings
            .into_iter()
            .map(|(area, kind, chain)| ChainBinding { area, kind, chain })
            .collect(),
    })
}

/// Replace-all write of every config table, on a caller-supplied connection so it
/// can be composed into a larger transaction (that composition is the whole point
/// of [`PostgresConfigSource::store_and_bump`]).
async fn write_all(
    conn: &mut sqlx::PgConnection,
    cfg: &RegistryConfig,
) -> Result<(), OrchestratorError> {
    for t in [
        "orchestrator.config_agents",
        "orchestrator.config_skills",
        "orchestrator.config_tools",
        "orchestrator.config_chain_bindings",
    ] {
        sqlx::query(&format!("delete from {t}"))
            .execute(&mut *conn)
            .await
            .map_err(store_err)?;
    }
    for a in &cfg.agents {
        let v = serde_json::to_value(a).map_err(store_err_ser)?;
        sqlx::query("insert into orchestrator.config_agents (name, def) values ($1, $2)")
            .bind(&a.name)
            .bind(v)
            .execute(&mut *conn)
            .await
            .map_err(store_err)?;
    }
    for s in &cfg.skills {
        let v = serde_json::to_value(s).map_err(store_err_ser)?;
        sqlx::query("insert into orchestrator.config_skills (name, def) values ($1, $2)")
            .bind(&s.name)
            .bind(v)
            .execute(&mut *conn)
            .await
            .map_err(store_err)?;
    }
    for t in &cfg.tools {
        let v = serde_json::to_value(t).map_err(store_err_ser)?;
        sqlx::query("insert into orchestrator.config_tools (name, spec) values ($1, $2)")
            .bind(&t.name)
            .bind(v)
            .execute(&mut *conn)
            .await
            .map_err(store_err)?;
    }
    for b in &cfg.chain_bindings {
        sqlx::query(
            "insert into orchestrator.config_chain_bindings (area, kind, chain) values ($1, $2, $3)",
        )
        .bind(&b.area)
        .bind(&b.kind)
        .bind(&b.chain)
        .execute(&mut *conn)
        .await
        .map_err(store_err)?;
    }
    Ok(())
}

/// The single-row atomic generation increment, on a caller-supplied connection.
async fn bump_on(conn: &mut sqlx::PgConnection) -> Result<u64, OrchestratorError> {
    let (v,): (i64,) = sqlx::query_as(
        "insert into orchestrator.config_versions (id, version) values (true, 1)
         on conflict (id) do update set version = orchestrator.config_versions.version + 1,
                                        updated_at = now()
         returning version",
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(store_err)?;
    Ok(v as u64)
}

/// A durable `ConfigSource` (SP-DATA-2): the registry config lives in the `orchestrator.config_*`
/// tables as jsonb rows, with a single-row `config_versions` global generation, so a run's
/// `#cfg{gen}` fence is cross-process meaningful.
///
/// ATOMICITY (SP-DATA-4 — closes the SP-DATA-2 carry-forwards). The (config, generation) pair is
/// only ever safe when both sides move together, so this type exposes exactly one atomic reader and
/// exactly one atomic writer:
/// - [`load_versioned`](ConfigSource::load_versioned) — the atomic READ: the four config tables AND
///   `config_versions` in ONE `REPEATABLE READ` transaction, so a concurrent writer can never hand
///   back a torn (stale config, fresh generation) pair.
/// - [`store_and_bump_if`](Self::store_and_bump_if) — the atomic write (SP-DATA-4.1), and the
///   one PRODUCTION CALLERS SHOULD USE: the replace-all content write AND the generation
///   increment in ONE transaction (so no crash window can leave new content durably parked
///   under an old generation), gated by a compare-and-swap against an expected generation (so
///   a writer that built its diff against a stale read cannot silently clobber a concurrent
///   one). `torii config push` uses this.
/// - [`store_and_bump`](Self::store_and_bump) — the unconditional atomic write this superseded.
///   Every production caller has migrated to `store_and_bump_if`; what remains is test seeding
///   (this crate's own fixtures, `sensei-orchestrator`'s executor tests, and — the reason it
///   cannot be gated behind `test-support`/`test` like its un-coupled siblings below —
///   `sensei-torii`'s own test suite, which depends on this crate with only the `postgres`
///   feature and so cannot see a `test-support`-gated item at all). Do not add a new
///   production caller; use `store_and_bump_if` instead.
///
/// [`load`](ConfigSource::load) and [`version`](ConfigSource::version) remain individually
/// non-atomic BY DESIGN: they are the unversioned `ConfigSource` contract (each is a single
/// self-consistent read of its own thing), and callers that need the pair use `load_versioned`
/// (`RegistryHandle::reload`/`from_source` already do). The un-coupled writers (`store`,
/// `bump_config_version`) are gated out of production builds behind `test-support`/`test` — see
/// [`store`](Self::store).
#[derive(Clone)]
pub struct PostgresConfigSource {
    pool: PgPool,
}

impl PostgresConfigSource {
    /// Wrap a connection pool (see [`connect`]).
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// The pool, for tests that need to drive an explicit transaction (the TOCTOU proof).
    #[cfg(test)]
    pub(crate) fn pool_for_test(&self) -> &PgPool {
        &self.pool
    }

    /// Replace the whole registry AND advance the generation in ONE transaction,
    /// unconditionally. Returns the new generation.
    ///
    /// SP-DATA-4.1: superseded as the production write path by
    /// [`store_and_bump_if`](Self::store_and_bump_if), which adds a compare-and-swap
    /// against an expected generation at zero extra round trips — do not add a new
    /// production caller here; use that instead. This method stays `pub` and
    /// ungated (unlike its un-coupled siblings `store`/`bump_config_version`) purely
    /// because it is still the seeding utility test code across this crate,
    /// `sensei-orchestrator`, and `sensei-torii` relies on, and `sensei-torii`
    /// depends on this crate with only the `postgres` feature — a `test-support`
    /// gate would be invisible to it. See the struct-level doc for the full
    /// reasoning.
    ///
    /// One transaction, not two calls: a `store()` followed by a `bump()` has a
    /// crash window between them, and dying in it leaves new content under an old
    /// generation DURABLY — a cross-process resume then matches the unchanged fence
    /// and silently runs the new config. Atomicity removes the window instead of
    /// asking callers to be careful.
    ///
    /// The bump comes FIRST, and the order is load-bearing — it is what makes
    /// concurrent writers serialize. Every writer's first act is to increment the
    /// single `config_versions` row, so a second writer blocks there until the first
    /// COMMITS; its replace-all `DELETE` then runs on a statement snapshot taken
    /// after that commit and actually removes the first writer's rows. True
    /// last-writer-wins.
    ///
    /// With the bump last, the loser's `DELETE` takes its snapshot BEFORE the winner
    /// commits (the transaction runs at READ COMMITTED), cannot see the winner's
    /// freshly-inserted rows, and they survive the "replace-all". What that produces
    /// depends on how the two configs overlap:
    /// - DISJOINT names → the durable content becomes the UNION of both configs while
    ///   both calls return `Ok`: a push that reports success and did not happen (an
    ///   operator revoking everything with `{}` concurrently with a `{x, z}` push
    ///   silently keeps `{x, z}`). This is the dangerous case — silent.
    /// - exactly ONE shared name → no lock cycle; the loser waits on the shared pkey
    ///   entry and fails loudly with `23505` duplicate key.
    /// - TWO shared names in OPPOSITE insert order → a lock cycle on the pkey index, and
    ///   Postgres aborts one writer with `40P01` deadlock detected.
    ///
    /// Bumping first removes all three: writers serialize before touching content.
    ///
    /// Deliberately NOT solved by raising the isolation level: REPEATABLE READ or
    /// SERIALIZABLE would turn the merge into a `40001` serialization failure, which
    /// only converts silent corruption into a retry obligation for every caller.
    pub async fn store_and_bump(&self, cfg: &RegistryConfig) -> Result<u64, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(store_err)?;
        let v = bump_on(&mut tx).await?;
        write_all(&mut tx, cfg).await?;
        tx.commit().await.map_err(store_err)?;
        Ok(v)
    }

    /// Replace the whole registry AND advance the generation, but ONLY if the current
    /// generation is still `expected` — a true compare-and-swap, at zero extra round
    /// trips over [`store_and_bump`](Self::store_and_bump). Returns `Ok(None)` if it
    /// moved (nothing is written); `Ok(Some(v))` is the new generation.
    ///
    /// SP-DATA-4.1 Task 1: closes the residual window a caller-side re-read-then-write
    /// cannot close. `torii config push` already re-reads the generation immediately
    /// before writing and refuses if it moved, which closes the *human-latency* window
    /// (a confirmation prompt can sit in front of an operator for minutes) — but that
    /// leaves the ~1 ms between the re-read returning and the write committing open.
    /// Folding the expectation into the bump's own `WHERE` clause removes that window
    /// entirely: the bump (which already runs FIRST in `store_and_bump`, precisely so
    /// concurrent writers serialize on the `config_versions` row) now only lands when
    /// it is still looking at the row the caller expected.
    ///
    /// FIRST PUSH — the subtlety a plain `UPDATE … WHERE version = $1` gets wrong.
    /// `config_versions` starts with NO ROW at all: absent means generation 0, by
    /// [`version`](ConfigSource::version)'s contract. A plain conditional `UPDATE` can
    /// never match a row that does not exist, so it would report a miss on a
    /// first-ever push even though nothing raced — a false refusal of the single most
    /// important write (bootstrapping an empty registry).
    ///
    /// The tempting fix is to handle that in the caller: on a miss with `expected ==
    /// 0`, re-check `version()`, and if it still reports `Some(0)`, fall back to
    /// unconditional `store_and_bump`. That works for the common case, but it
    /// reopens a — narrower — version of the exact race this method exists to close:
    /// if a SECOND concurrent first-push lands in the gap between that re-check and
    /// the fallback call, both fallback calls are plain `store_and_bump`s, which (by
    /// design, see that method's doc comment) serialize and both return `Ok` —
    /// true last-writer-wins, not a refusal. The caller has no way to tell its own
    /// write landed second.
    ///
    /// So this method folds both cases into ONE statement instead, in ONE
    /// transaction:
    /// - `expected == 0`: attempt `INSERT … ON CONFLICT (id) DO NOTHING`. If the row
    ///   is genuinely absent, this alone is the whole CAS — it lands, and the
    ///   sibling conditional `UPDATE` matches nothing (see below for why). If a
    ///   concurrent writer already created the row — including a second concurrent
    ///   first-push racing this exact call, which Postgres blocks on the unique index
    ///   until the first one commits, then re-evaluates the conflict — the `INSERT`
    ///   no-ops and the `UPDATE` takes over. Proven directly:
    ///   `concurrent_first_pushes_do_not_both_land`.
    /// - `expected != 0`: the `INSERT`'s `WHERE $1 = 0` guard is false, so it never
    ///   runs at all (and never emits conflict noise); only the conditional `UPDATE`
    ///   fires — the plain CAS case. Proven under concurrency too:
    ///   `concurrent_pushes_at_the_same_nonzero_generation_do_not_both_land`.
    ///
    /// WHY THE TWO SUB-STATEMENTS NEVER BOTH FIRE. It is tempting to credit the `WHERE
    /// NOT EXISTS (SELECT 1 FROM ins)` guard on `upd` with forcing an order — "the
    /// `INSERT` must fully resolve, conflict and all, before the `UPDATE` decides
    /// whether to run." That is NOT what Postgres guarantees, and citing it would be
    /// wrong: the documented behaviour for data-modifying CTEs is the opposite —
    /// sub-statements in one `WITH` "are executed concurrently with each other and
    /// with the main query… the order in which the specified updates actually happen
    /// is unpredictable." What actually holds, and what this method leans on, is the
    /// SHARED SNAPSHOT: those same sub-statements "cannot see one another's effects on
    /// the target tables." So when `ins` inserts the first-ever row, `upd`'s `WHERE
    /// version = $1` is evaluated against a snapshot in which that row does not exist
    /// at all — not a version mismatch, an absent row — and matches nothing regardless
    /// of the `NOT EXISTS` guard. Removing the guard entirely (tested against a
    /// scratch table seeded so `upd`'s predicate WOULD match the inserted value if it
    /// were visible) still produces no double-bump, which is the direct evidence: the
    /// guard is defensive belt-and-suspenders, not load-bearing. The snapshot is what
    /// makes this method correct.
    ///
    /// Both arms are one round trip and one transaction, so there is no window
    /// between "decide" and "write" for anything to land in, in either case.
    pub async fn store_and_bump_if(
        &self,
        cfg: &RegistryConfig,
        expected: u64,
    ) -> Result<Option<u64>, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(store_err)?;
        let applied: Option<(i64,)> = sqlx::query_as(
            "with ins as (
                 insert into orchestrator.config_versions (id, version)
                 select true, 1
                 where $1::bigint = 0
                 on conflict (id) do nothing
                 returning version
             ), upd as (
                 update orchestrator.config_versions
                    set version = version + 1, updated_at = now()
                  where id = true and version = $1::bigint
                    and not exists (select 1 from ins)
                 returning version
             )
             select version from ins
             union all
             select version from upd",
        )
        .bind(expected as i64)
        .fetch_optional(&mut *tx)
        .await
        .map_err(store_err)?;
        let Some((v,)) = applied else {
            // Neither the insert-if-absent nor the conditional update matched: the
            // generation genuinely moved. Roll back (drop `tx`) and report the miss.
            return Ok(None);
        };
        write_all(&mut tx, cfg).await?;
        tx.commit().await.map_err(store_err)?;
        Ok(Some(v as u64))
    }

    /// Replace-all write of the whole registry in one transaction, WITHOUT bumping the generation.
    ///
    /// The UN-COUPLED writer, gated behind `test-support` because a `store` whose caller forgets
    /// [`bump_config_version`](Self::bump_config_version) changes content without advancing the
    /// generation — a cross-process resume then matches the (unchanged) fence and silently runs the
    /// new config. Production code uses [`store_and_bump`](Self::store_and_bump); tests need the
    /// un-coupled pair to PROVE that the coupled path is what fixes it.
    ///
    /// The gate is `test-support` OR `test`, and needs both halves: `test` alone cannot reach the
    /// three callers in `sensei-orchestrator`'s executor tests (a DIFFERENT crate, where `cfg(test)`
    /// is not enabled for a dependency), and `test-support` alone cannot reach THIS crate's own unit
    /// tests under `cargo test --workspace` — `crates/torii` depends on us with
    /// `features = ["postgres"]` unconditionally, so workspace feature unification compiles this
    /// module's tests with `postgres` on but `test-support` off. Neither cfg is ever active in a
    /// production build, which is the property that matters: the footgun stays unreachable.
    #[cfg(any(feature = "test-support", test))]
    pub async fn store(&self, cfg: &RegistryConfig) -> Result<(), OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(store_err)?;
        write_all(&mut tx, cfg).await?;
        tx.commit().await.map_err(store_err)?;
        Ok(())
    }

    /// Atomic upsert-increment of the single-row global generation; returns the new version.
    /// The other half of the un-coupled pair — see [`store`](Self::store) for why it is gated.
    #[cfg(any(feature = "test-support", test))]
    pub async fn bump_config_version(&self) -> Result<u64, OrchestratorError> {
        let mut conn = self.pool.acquire().await.map_err(store_err)?;
        bump_on(&mut conn).await
    }
}

#[async_trait::async_trait]
impl ConfigSource for PostgresConfigSource {
    async fn load(&self) -> Result<RegistryConfig, OrchestratorError> {
        let mut conn = self.pool.acquire().await.map_err(store_err)?;
        read_all(&mut conn).await
    }

    async fn version(&self) -> Result<Option<u64>, OrchestratorError> {
        // A versioned source ALWAYS returns Some (absent row ⇒ Some(0)); None is reserved for
        // genuinely-unversioned sources (filesystem/in-memory).
        let row: Option<(i64,)> =
            sqlx::query_as("select version from orchestrator.config_versions where id = true")
                .fetch_optional(&self.pool)
                .await
                .map_err(store_err)?;
        Ok(Some(row.map(|(v,)| v as u64).unwrap_or(0)))
    }

    /// ONE `REPEATABLE READ` snapshot over the four config tables AND
    /// `config_versions` — closing the SP-DATA-2 TOCTOU. The snapshot is taken at
    /// the transaction's first read, so a concurrent `store_and_bump` lands wholly
    /// before or wholly after it: the pair can never be torn.
    async fn load_versioned(&self) -> Result<(RegistryConfig, Option<u64>), OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(store_err)?;
        sqlx::query("set transaction isolation level repeatable read")
            .execute(&mut *tx)
            .await
            .map_err(store_err)?;
        let cfg = read_all(&mut tx).await?;
        let row: Option<(i64,)> =
            sqlx::query_as("select version from orchestrator.config_versions where id = true")
                .fetch_optional(&mut *tx)
                .await
                .map_err(store_err)?;
        tx.commit().await.map_err(store_err)?;
        Ok((cfg, Some(row.map(|(v,)| v as u64).unwrap_or(0))))
    }
}

/// A config LOAD-path transport/parse failure → `RegistryLoad` (the `ConfigSource` convention;
/// parity with `FilesystemConfigSource`, which never surfaces a bare `Store`/`Serialization` on
/// a read).
fn cfg_load_err(e: sqlx::Error) -> OrchestratorError {
    OrchestratorError::RegistryLoad(format!("postgres config load: {e}"))
}

/// A durable [`SchedulerStore`] (SP-DATA-3): the wake set lives in `orchestrator.scheduled_runs`,
/// one row per submitted run holding its ORIGINAL graph (jsonb) + status/schedule. `claim_due` is an
/// atomic `UPDATE … WHERE run_id IN (SELECT … FOR UPDATE SKIP LOCKED) RETURNING`, so concurrent
/// claimers never overlap (the row lock is held only for the brief claim UPDATE, NOT during the drive).
pub struct PostgresSchedulerStore {
    pool: PgPool,
}

impl PostgresSchedulerStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl SchedulerStore for PostgresSchedulerStore {
    async fn enqueue(
        &self,
        run: RunId,
        graph: &Graph,
        now: DateTime<Utc>,
    ) -> Result<(), OrchestratorError> {
        let g = serde_json::to_value(graph).map_err(store_err_ser)?;
        let res = sqlx::query(
            "insert into orchestrator.scheduled_runs (run_id, graph, status, claimed_at, updated_at)
             values ($1,$2,'waking',$3,$3) on conflict (run_id) do nothing",
        )
        .bind(run.0)
        .bind(g)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(store_err)?;
        if res.rows_affected() == 0 {
            return Err(OrchestratorError::Store(format!(
                "duplicate submit for run {run:?}"
            )));
        }
        Ok(())
    }

    async fn record_paused(
        &self,
        run: RunId,
        next_wake: Option<DateTime<Utc>>,
        reason: &str,
    ) -> Result<(), OrchestratorError> {
        sqlx::query(
            "update orchestrator.scheduled_runs set status='paused', next_wake=$2, claimed_at=null,
                    reason=$3, updated_at=now() where run_id=$1 and status='waking'",
        )
        .bind(run.0)
        .bind(next_wake)
        .bind(reason)
        .execute(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(())
    }

    async fn record_terminal(
        &self,
        run: RunId,
        status: RunStatus,
        reason: Option<&str>,
    ) -> Result<(), OrchestratorError> {
        sqlx::query(
            "update orchestrator.scheduled_runs set status=$2, next_wake=null, claimed_at=null,
                    reason=$3, updated_at=now() where run_id=$1 and status='waking'",
        )
        .bind(run.0)
        .bind(status.as_str())
        .bind(reason)
        .execute(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(())
    }

    async fn claim_due(
        &self,
        now: DateTime<Utc>,
        lease: chrono::Duration,
        limit: usize,
    ) -> Result<Vec<(RunId, Graph)>, OrchestratorError> {
        let stale_before = now - lease;
        let rows: Vec<(sqlx::types::Uuid, serde_json::Value)> = sqlx::query_as(
            "update orchestrator.scheduled_runs set status='waking', claimed_at=$1, updated_at=now()
             where run_id in (
                 select run_id from orchestrator.scheduled_runs
                 where (status='paused' and next_wake is not null and next_wake <= $1)
                    or (status='waking' and claimed_at < $2)
                 order by next_wake nulls last
                 limit $3
                 for update skip locked)
             returning run_id, graph",
        )
        .bind(now)
        .bind(stale_before)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(store_err)?;
        rows.into_iter()
            .map(|(id, g)| Ok((RunId(id), serde_json::from_value(g).map_err(store_err_ser)?)))
            .collect()
    }

    async fn status(&self, run: RunId) -> Result<Option<ScheduledRun>, OrchestratorError> {
        let row: Option<(String, Option<DateTime<Utc>>, Option<String>, DateTime<Utc>)> =
            sqlx::query_as(
                "select status, next_wake, reason, updated_at
                 from orchestrator.scheduled_runs where run_id=$1",
            )
            .bind(run.0)
            .fetch_optional(&self.pool)
            .await
            .map_err(store_err)?;
        Ok(row.map(|(s, nw, r, u)| ScheduledRun {
            run,
            status: RunStatus::from_db_str(&s).unwrap_or(RunStatus::Failed),
            next_wake: nw,
            reason: r,
            updated_at: u,
        }))
    }

    async fn list_paused(&self) -> Result<Vec<ScheduledRun>, OrchestratorError> {
        let rows: Vec<(
            sqlx::types::Uuid,
            Option<DateTime<Utc>>,
            Option<String>,
            DateTime<Utc>,
        )> = sqlx::query_as(
            "select run_id, next_wake, reason, updated_at
                 from orchestrator.scheduled_runs where status='paused'",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(rows
            .into_iter()
            .map(|(id, nw, r, u)| ScheduledRun {
                run: RunId(id),
                status: RunStatus::Paused,
                next_wake: nw,
                reason: r,
                updated_at: u,
            })
            .collect())
    }

    async fn cancel(&self, run: RunId) -> Result<(), OrchestratorError> {
        sqlx::query(
            "update orchestrator.scheduled_runs set status='cancelled', next_wake=null, updated_at=now()
             where run_id=$1 and status not in ('completed','failed','cancelled')",
        )
        .bind(run.0)
        .execute(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(())
    }

    async fn force_wake(&self, run: RunId, now: DateTime<Utc>) -> Result<(), OrchestratorError> {
        sqlx::query(
            "update orchestrator.scheduled_runs set next_wake=$2, updated_at=now()
             where run_id=$1 and status='paused'",
        )
        .bind(run.0)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(())
    }

    async fn count_terminal_before(&self, before: DateTime<Utc>) -> Result<u64, OrchestratorError> {
        let (n,): (i64,) = sqlx::query_as(&format!(
            "select count(*) from orchestrator.scheduled_runs
              where status in ({TERMINAL_STATUS_LITERALS}) and updated_at < $1"
        ))
        .bind(before)
        .fetch_one(&self.pool)
        .await
        .map_err(store_err)?;
        // `count(*)` is never negative; the clamp exists so a hypothetical negative could
        // never wrap into an enormous u64 that an operator would read as a real backlog.
        Ok(n.max(0) as u64)
    }

    async fn prune_terminal(&self, before: DateTime<Utc>) -> Result<u64, OrchestratorError> {
        let res = sqlx::query(&format!(
            "delete from orchestrator.scheduled_runs
              where status in ({TERMINAL_STATUS_LITERALS}) and updated_at < $1"
        ))
        .bind(before)
        .execute(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(res.rows_affected())
    }
}

/// The terminal-status ALLOWLIST, written once and interpolated into both the count and the
/// delete so the operator's preview and the effect can never drift apart. Interpolation is
/// safe by construction — this is a compile-time `const` of SQL literals, never operator
/// input; the only bound parameter in either statement is the timestamp.
///
/// An allowlist, deliberately, not `status not in ('paused','waking')`: with an exclusion
/// list a row whose status this build does not recognise (a newer writer, a hand-edited
/// row, a future non-terminal state) would be DELETED by default. Here it is kept.
const TERMINAL_STATUS_LITERALS: &str = "'completed','failed','cancelled'";

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Duration, Utc};
    use orchestrator_core::{
        AgentDefinition, ChildStatus, CompactChild, ContentRef, ContentStore, ContextKey,
        ContextRef, ContextStore, Digest, EffectClass, ExecutionJournal, Graph, JournalError,
        JournalEvent, NetworkPolicy, NodeId, OrchestratorError, Permissions, RunId, RunStatus,
        SchedulerStore, Scope, SkillDef, Snapshot, ToolSpec,
    };
    // For `PgConnection::connect` on the advisory-lock guards' dedicated connection.
    use sqlx::Connection;
    use std::collections::HashMap;

    /// Tests require a live PG at $DATABASE_URL with the dbd schema applied (the Docker harness).
    /// Absent DATABASE_URL, they skip (so a bare `cargo test --features postgres` without a DB
    /// doesn't fail spuriously).
    ///
    /// A skip is ANNOUNCED, not silent (SP-DATA-4 whole-slice review): the same suite reports the
    /// same 53 passing tests whether it exercised Postgres or returned immediately, so a CI job
    /// that loses the variable reported a fully-green database suite that touched nothing.
    ///
    /// The notice goes to the process's REAL stderr rather than through `eprintln!`: libtest
    /// captures the print macros and replays them only for a FAILING test, so an `eprintln!`
    /// would be invisible in exactly the green run it exists to annotate. Under libtest the
    /// current thread's name is the test's path, which is what lets this one helper — the single
    /// choke point for every guard in the module — still name the test that skipped.
    fn db_url() -> Option<String> {
        let url = database_url_raw();
        if url.is_none() {
            use std::io::Write;
            let name = std::thread::current()
                .name()
                .unwrap_or("<unnamed test>")
                .to_string();
            // Formatted first, then ONE `write_all`: `Stderr` is unbuffered, so a
            // `writeln!` emits a separate syscall per format fragment and a parallel
            // test's output interleaves mid-line.
            let line = format!("SKIP {name}: DATABASE_URL not set\n");
            let _ = std::io::stderr().write_all(line.as_bytes());
        }
        url
    }

    /// The raw lookup with no side effects, shared by `db_url()` (the announcing choke
    /// point every test's own skip check goes through) and the advisory-lock guards below
    /// (which need the value too but must NOT print a second SKIP line — `db_url()` stays
    /// the single place that announces).
    fn database_url_raw() -> Option<String> {
        std::env::var("DATABASE_URL")
            .ok()
            .filter(|s| !s.trim().is_empty())
    }

    /// A fresh, unique run id — every test gets its own so the shared `orchestrator.*`
    /// tables never collide across tests (belt-and-suspenders with `--test-threads=1`).
    fn run() -> RunId {
        RunId(uuid::Uuid::new_v4())
    }

    // ---- Isolation for the tests that CANNOT scope themselves ------------------------------
    //
    // Most tests here isolate by a fresh `RunId`, so they are parallel-safe. Two groups
    // cannot, and they are serialized with process-wide guards rather than by relying on
    // `--test-threads=1` — correctness must not depend on how the suite is invoked, or a
    // plain `cargo test` goes red while `-j1` stays green (a masked failure).
    //
    // These are `tokio::sync::Mutex`, not `std::sync::Mutex`: the guard is held across the
    // test's awaits, which trips `clippy::await_holding_lock` (denied here) and would park a
    // worker thread of the multi-threaded test below. A bonus for this use: tokio's mutex has
    // NO poisoning, so a panicking test simply releases the guard instead of cascading one
    // real failure into a dozen confusing ones across its whole group.

    // ---- Cross-PROCESS isolation (SP-DATA-4.1 #3) ------------------------------------------
    //
    // The mutexes above serialize this crate's OWN tests, but `sensei-torii` runs its DB
    // tests against the same global `config_*` tables from a SEPARATE process, and an
    // in-process mutex cannot serialize across processes. Not reachable through `cargo test
    // --workspace` (cargo runs test binaries one at a time), but `cargo nextest` runs tests
    // in separate processes IN PARALLEL and would defeat these mutexes entirely — a trap
    // laid for whoever adopts it. Forcing two concurrent `cargo` invocations reproduced a
    // failure roughly one round in four before this fix.
    //
    // Postgres session-level advisory locks close that gap: held by a CONNECTION, released
    // on disconnect, visible across processes. Both crates' guards MUST use the same key.
    //
    /// Shared with `crates/torii/src/lib.rs`'s `test_guard::config_guard`. Both sites MUST
    /// use this exact key.
    const ADVISORY_CONFIG_TABLES: i64 = 0x5350_4441_5441_3401; // "SPDATA4" + 01
    /// `orchestrator-store`-only today (torii never touches `scheduled_runs`), but keyed and
    /// commented the same way in case a future torii DB test does.
    const ADVISORY_SCHEDULED_RUNS: i64 = 0x5350_4441_5441_3402;

    /// RAII: holds BOTH the in-process mutex guard and, when a database is configured, the
    /// dedicated connection holding the cross-process advisory lock. Dropping releases
    /// both — the connection's `Drop` closes the socket, which Postgres treats as a
    /// disconnect and releases the session-level lock immediately, so a PANICKING test
    /// still releases it without any explicit unlock (which wouldn't run on unwind anyway).
    struct DbGuard {
        _mutex: tokio::sync::MutexGuard<'static, ()>,
        _conn: Option<sqlx::PgConnection>,
    }

    /// Open a DEDICATED connection and hold `pg_advisory_lock(key)` on it. Deliberately NOT
    /// a connection borrowed from a pool: returning a pooled connection while the
    /// session-level lock is still held would leak the lock to whichever caller borrows
    /// that connection next. `None` when no database is configured — the test that follows
    /// immediately checks `db_url()` itself and skips, so nothing here needs to run.
    ///
    /// Blocking `pg_advisory_lock` (not `pg_try_advisory_lock`) is correct: a test should
    /// WAIT for its turn, not fail because another process got there first. An indefinite
    /// block only matters if a lock is somehow orphaned, and a session-level lock requires a
    /// LIVE connection to hold it — a crashed process's socket is reclaimed by the OS, which
    /// Postgres notices and releases the lock. As a defensive bound anyway (e.g. a wedged
    /// connection the OS hasn't yet reaped), the wait is capped by a `statement_timeout` on
    /// this dedicated connection rather than left truly infinite, so a genuinely stuck case
    /// fails loudly instead of hanging CI forever.
    async fn acquire_advisory(key: i64) -> Option<sqlx::PgConnection> {
        let url = database_url_raw()?;
        let mut conn = sqlx::PgConnection::connect(&url)
            .await
            .expect("advisory-lock connection: DATABASE_URL is set, so this must succeed");
        sqlx::query("set statement_timeout = '30s'")
            .execute(&mut conn)
            .await
            .expect("set statement_timeout");
        sqlx::query("select pg_advisory_lock($1)")
            .bind(key)
            .execute(&mut conn)
            .await
            .expect("pg_advisory_lock must succeed once connected");
        Some(conn)
    }

    /// Group 1 — the `config_*` tables and the single-row `config_versions`. There is no
    /// per-test key to scope by: `config_versions` is `id boolean PK check(id)`, i.e.
    /// deliberately ONE row, so any test asserting a specific generation conflicts with any
    /// concurrent config writer, and `store`/`store_and_bump` are replace-ALL.
    static CONFIG_TABLES: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// The in-process mutex is taken FIRST, before the advisory lock: same-process tests
    /// then queue on the cheap local mutex rather than on Postgres, and this never holds a
    /// database connection while merely waiting on that local mutex.
    ///
    /// Lock ORDER: no test in this crate takes both `config_guard` and `scheduler_guard`
    /// (verified — grep for a test calling both), and torii only ever takes the
    /// config-tables guard, so there is no path that could acquire these two advisory locks
    /// in different orders and deadlock. If that ever changes, fix a single global order
    /// (e.g. always config-tables before scheduled-runs) in BOTH crates.
    async fn config_guard() -> DbGuard {
        let mutex = CONFIG_TABLES.lock().await;
        let conn = acquire_advisory(ADVISORY_CONFIG_TABLES).await;
        DbGuard {
            _mutex: mutex,
            _conn: conn,
        }
    }

    /// Group 2 — `scheduled_runs` claim sweeps. A fresh `RunId` is NOT enough here:
    /// `claim_due` is an instance-wide sweep (`… limit N`), so a concurrent test's claim
    /// steals another test's due row and the victim's `any(|x| x == r)` assertion fails.
    /// Only tests that CLAIM need this; the enqueue/status-only tests scope fine by run id.
    static SCHEDULED_RUNS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// See `config_guard` for why the in-process mutex comes first and why lock ordering
    /// is a non-issue today.
    async fn scheduler_guard() -> DbGuard {
        let mutex = SCHEDULED_RUNS.lock().await;
        let conn = acquire_advisory(ADVISORY_SCHEDULED_RUNS).await;
        DbGuard {
            _mutex: mutex,
            _conn: conn,
        }
    }

    fn started() -> JournalEvent {
        JournalEvent::RunStarted {
            version: "v1".into(),
            // SP-DATA-5: this store test doesn't exercise a budget.
            budget: None,
        }
    }

    fn node_started(id: &str) -> JournalEvent {
        JournalEvent::NodeStarted {
            node: NodeId(id.into()),
        }
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

    #[tokio::test]
    async fn append_then_load_returns_events_in_ascending_seq() {
        let Some(url) = db_url() else { return };
        let j = PostgresJournal::new(connect(&url).await.unwrap());
        let r = run();

        let s1 = j.append(r, started()).await.unwrap();
        let s2 = j.append(r, node_started("n1")).await.unwrap();
        assert!(s2 > s1, "seq monotonic");

        let evs = j.load(r).await.unwrap();
        assert_eq!(evs.len(), 2, "both events present");
        assert_eq!(evs[0].0, s1);
        assert_eq!(evs[1].0, s2);
        assert!(evs[0].0 < evs[1].0, "load returns ascending seq order");
        assert!(matches!(evs[0].1, JournalEvent::RunStarted { .. }));
        assert!(matches!(evs[1].1, JournalEvent::NodeStarted { .. }));
    }

    #[tokio::test]
    async fn load_since_returns_only_the_tail() {
        let Some(url) = db_url() else { return };
        let j = PostgresJournal::new(connect(&url).await.unwrap());
        let r = run();

        let s1 = j.append(r, started()).await.unwrap();
        let s2 = j.append(r, node_started("n1")).await.unwrap();

        let tail = j.load_since(r, s1).await.unwrap();
        assert_eq!(tail.len(), 1, "only events with seq > s1");
        assert_eq!(tail[0].0, s2, "the tail is the second event");
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
            .bind(r.0)
            .execute(&pool)
            .await
            .unwrap();

        let err = j.load(r).await.unwrap_err();
        assert!(
            matches!(
                err,
                JournalError::IncompatibleFormat {
                    stored: -999,
                    expected: 1,
                    ..
                }
            ),
            "must fence loudly, got {err:?}"
        );
        // load_since fences on the same check.
        assert!(matches!(
            j.load_since(r, 0).await.unwrap_err(),
            JournalError::IncompatibleFormat { .. }
        ));
    }

    #[tokio::test]
    async fn snapshot_round_trips_latest_wins() {
        let Some(url) = db_url() else { return };
        let j = PostgresJournal::new(connect(&url).await.unwrap());
        let r = run();

        // No snapshot yet → None (never a silent empty struct).
        assert!(j.latest_snapshot(r).await.unwrap().is_none());

        let s1 = j.append(r, started()).await.unwrap();
        let s2 = j.append(r, node_started("n1")).await.unwrap();

        let snap = Snapshot {
            seq: s1,
            completed: vec![NodeId("n1".into())],
            skipped: vec![],
            outputs: vec![],
        };
        j.snapshot(r, snap).await.unwrap();
        let got = j
            .latest_snapshot(r)
            .await
            .unwrap()
            .expect("snapshot present");
        assert_eq!(got.seq, s1);
        assert_eq!(got.completed, vec![NodeId("n1".into())]);

        // Latest wins: a second snapshot overwrites the first.
        let snap2 = Snapshot {
            seq: s2,
            completed: vec![NodeId("n1".into()), NodeId("n2".into())],
            skipped: vec![],
            outputs: vec![],
        };
        j.snapshot(r, snap2).await.unwrap();
        assert_eq!(j.latest_snapshot(r).await.unwrap().unwrap().seq, s2);
    }

    #[tokio::test]
    async fn compact_removes_the_named_events_and_appends_the_manifest() {
        let Some(url) = db_url() else { return };
        let j = PostgresJournal::new(connect(&url).await.unwrap());
        let r = run();

        let s0 = j.append(r, started()).await.unwrap();
        let s1 = j.append(r, node_started("n1")).await.unwrap();
        let s2 = j.append(r, node_started("n2")).await.unwrap();

        let manifest = JournalEvent::MapCompacted {
            node: NodeId("m".into()),
            children: vec![CompactChild {
                index: 0,
                status: ChildStatus::Ok,
                digest: Some(Digest("abc".into())),
                input_hash: Some("h".into()),
            }],
        };
        j.compact(r, &[s1, s2], manifest).await.unwrap();

        let events = j.load(r).await.unwrap();
        let seqs: Vec<_> = events.iter().map(|(s, _)| *s).collect();
        assert!(seqs.contains(&s0), "the untouched event stays: {seqs:?}");
        assert!(
            !seqs.contains(&s1) && !seqs.contains(&s2),
            "the compacted events are removed: {seqs:?}"
        );
        assert!(
            events.iter().any(
                |(_, e)| matches!(e, JournalEvent::MapCompacted { node, .. } if node.0 == "m")
            ),
            "the manifest is appended"
        );
    }

    // ---- CAS + context parity (Task 4) ----------------------------------------------------
    // Mirror the InMemory parity tests in `stores.rs`, but against the Postgres adapters on a
    // live PG. Each test uses UNIQUE bytes/keys (a uuid tag) so the shared `orchestrator.*`
    // tables never collide across tests (belt-and-suspenders with `--test-threads=1`).

    /// Parity with `content_store_dedupes_identical_bytes_and_misses_loudly`:
    /// `put` of identical bytes yields ONE row + the same `Digest`; `get` of a missing digest
    /// is the SAME loud `ContentDigestMiss` the InMemory store raises.
    #[tokio::test]
    async fn pg_content_store_dedupes_identical_bytes_and_misses_loudly() {
        let Some(url) = db_url() else { return };
        let store = PostgresContentStore::new(connect(&url).await.unwrap());
        let payload = format!("hello world {}", uuid::Uuid::new_v4());

        let d1 = store.put(payload.as_bytes()).await.unwrap();
        let d2 = store.put(payload.as_bytes()).await.unwrap();
        assert_eq!(d1, d2, "identical content shares one digest");
        assert_eq!(store.get(&d1).await.unwrap(), payload.as_bytes());

        // Dedupe: identical bytes are stored exactly ONCE (the digest is unique to this test).
        let (n,): (i64,) =
            sqlx::query_as("select count(*) from orchestrator.cas_blobs where digest = $1")
                .bind(&d1.0)
                .fetch_one(&store.pool)
                .await
                .unwrap();
        assert_eq!(n, 1, "identical bytes deduplicate to one row");

        let d3 = store
            .put(format!("different {}", uuid::Uuid::new_v4()).as_bytes())
            .await
            .unwrap();
        assert_ne!(d1, d3, "different content, different digest");

        let miss = Digest(format!("deadbeef{}", uuid::Uuid::new_v4().simple()));
        let err = store.get(&miss).await.expect_err("a digest miss is loud");
        assert!(
            matches!(err, OrchestratorError::ContentDigestMiss(_)),
            "{err:?}"
        );
    }

    /// A content-addressed blob store must round-trip ARBITRARY bytes losslessly (not just UTF-8)
    /// — `bytea` over sqlx's binary protocol. Guards against a future serialization change quietly
    /// mangling non-UTF-8 content.
    #[tokio::test]
    async fn pg_content_store_round_trips_arbitrary_binary_bytes() {
        let Some(url) = db_url() else { return };
        let store = PostgresContentStore::new(connect(&url).await.unwrap());
        // Include a unique tail so the digest is per-run distinct, but keep the raw bytes binary.
        let mut raw: Vec<u8> = vec![0x00, 0xFF, 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x7F, 0x80, 0x01];
        raw.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
        let d = store.put(&raw).await.unwrap();
        assert_eq!(
            store.get(&d).await.unwrap(),
            raw,
            "bytea must round-trip arbitrary bytes losslessly"
        );
    }

    /// Parity with `context_store_collides_resolves_node_to_run_and_misses_to_none`:
    /// distinct-key writes round-trip via the CAS; a re-write of an existing `(scope,key)`
    /// collides LOUDLY (`ContextKeyCollision`, not a silent overwrite); a `Node` read resolves
    /// up to `Run`; a `Node`-scoped write is private to that node; a miss is `Ok(None)`.
    #[tokio::test]
    async fn pg_context_store_collides_resolves_node_to_run_and_misses_to_none() {
        let Some(url) = db_url() else { return };
        let store = PostgresContextStore::new(connect(&url).await.unwrap());
        let tag = uuid::Uuid::new_v4().simple().to_string();
        let k1 = || ContextKey(format!("k1-{tag}"));
        let node = || Scope::Node(NodeId(format!("n-{tag}")));

        // Run-scoped write round-trips through the CAS.
        let r1 = store
            .put(Scope::Run, k1(), serde_json::json!({ "v": 1 }))
            .await
            .unwrap();
        assert_eq!(
            store.load(&r1).await.unwrap(),
            serde_json::json!({ "v": 1 }),
            "load fetches the value back via the CAS"
        );

        // Re-writing the same (scope,key) is a loud collision (never last-write-wins).
        let err = store
            .put(Scope::Run, k1(), serde_json::json!({ "v": 2 }))
            .await
            .expect_err("same (scope,key) collides");
        assert!(
            matches!(err, OrchestratorError::ContextKeyCollision { .. }),
            "{err:?}"
        );

        // A Node-scoped read resolves up to the Run-scoped entry.
        assert!(
            store.get(node(), k1()).await.unwrap().is_some(),
            "Node read resolves up to Run"
        );

        // A read miss is an explicit Ok(None).
        assert!(
            store
                .get(Scope::Run, ContextKey(format!("absent-{tag}")))
                .await
                .unwrap()
                .is_none(),
            "a miss is Ok(None), never a silent empty value"
        );

        // A Node-scoped write is private to that node — not visible at Run.
        let k2 = || ContextKey(format!("k2-{tag}"));
        store
            .put(node(), k2(), serde_json::json!({ "n": true }))
            .await
            .unwrap();
        let node_entry = store.get(node(), k2()).await.unwrap();
        assert_eq!(
            node_entry.unwrap().scope,
            node(),
            "the Node entry is returned"
        );
        assert!(
            store.get(Scope::Run, k2()).await.unwrap().is_none(),
            "a Node-scoped write does not leak to Run"
        );
    }

    /// Parity with `insert_ref_rehydrates_an_entry_without_recomputing_the_cas`:
    /// `insert_ref` rehydrates from an already-journaled ref (no CAS recompute) and is
    /// idempotent — re-inserting the same ref does NOT collide (unlike `put`).
    #[tokio::test]
    async fn pg_insert_ref_rehydrates_an_entry_without_recomputing_the_cas() {
        let Some(url) = db_url() else { return };
        let pool = connect(&url).await.unwrap();
        let content = PostgresContentStore::new(pool.clone());
        let tag = uuid::Uuid::new_v4().simple().to_string();
        let bytes = serde_json::to_vec(&serde_json::json!({ "v": 1 })).unwrap();
        let digest = content.put(&bytes).await.unwrap();
        let r = ContextRef {
            key: ContextKey(format!("k-{tag}")),
            scope: Scope::Run,
            content: ContentRef {
                digest,
                size: bytes.len(),
                summary: None,
            },
            summary: None,
        };

        let store = PostgresContextStore::new(pool.clone());
        store.insert_ref(r.clone()).await.unwrap();
        let got = store
            .get(Scope::Run, ContextKey(format!("k-{tag}")))
            .await
            .unwrap()
            .expect("present after insert_ref");
        assert_eq!(
            store.load(&got).await.unwrap(),
            serde_json::json!({ "v": 1 })
        );

        // Idempotent — re-inserting the same (scope,key) does not collide (unlike `put`).
        store.insert_ref(r).await.unwrap();
    }

    // ---- PostgresConfigSource (SP-DATA-2 Task 3) ------------------------------------------
    // Isolated from the run-state tables above: these tests only touch `config_agents` /
    // `config_skills` / `config_tools` / `config_chain_bindings` / `config_versions`. Per-test
    // unique entity names so the shared tables never collide.

    fn uniq(p: &str) -> String {
        format!("{p}-{}", uuid::Uuid::new_v4())
    }

    fn skill(name: &str) -> SkillDef {
        // SkillDef { name, description: Option<String>, body: String, activation: #[serde(default)] }
        SkillDef {
            name: name.into(),
            description: Some("d".into()),
            body: "b".into(),
            activation: Default::default(),
        }
    }

    #[tokio::test]
    async fn store_then_load_round_trips_config() {
        let _guard = config_guard().await;
        let Some(url) = db_url() else { return };
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());
        let s = uniq("skill");
        let cfg = RegistryConfig {
            agents: vec![],
            skills: vec![skill(&s)],
            tools: vec![],
            chain_bindings: vec![],
        };
        src.store(&cfg).await.unwrap();
        let got = src.load().await.unwrap();
        assert!(
            got.skills.iter().any(|k| k.name == s),
            "the stored skill round-trips via jsonb"
        );
    }

    #[tokio::test]
    async fn version_is_zero_on_empty_then_monotonic_under_bump() {
        let _guard = config_guard().await;
        let Some(url) = db_url() else { return };
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());
        // config_versions is a single shared row; assert STRICT INCREASE (robust under a shared row).
        let v0 = src
            .version()
            .await
            .unwrap()
            .expect("a versioned source always returns Some");
        let v1 = src.bump_config_version().await.unwrap();
        assert!(v1 > v0, "bump strictly increases ({v0} -> {v1})");
        assert_eq!(
            src.version().await.unwrap(),
            Some(v1),
            "version() reflects the bump"
        );
    }

    #[tokio::test]
    async fn store_is_replace_all_removed_entities_do_not_linger() {
        let _guard = config_guard().await;
        let Some(url) = db_url() else { return };
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());
        let keep = uniq("keep");
        let drop = uniq("drop");
        src.store(&RegistryConfig {
            agents: vec![],
            skills: vec![skill(&keep), skill(&drop)],
            tools: vec![],
            chain_bindings: vec![],
        })
        .await
        .unwrap();
        src.store(&RegistryConfig {
            agents: vec![],
            skills: vec![skill(&keep)],
            tools: vec![],
            chain_bindings: vec![],
        })
        .await
        .unwrap();
        let got = src.load().await.unwrap();
        assert!(got.skills.iter().any(|k| k.name == keep));
        assert!(
            !got.skills.iter().any(|k| k.name == drop),
            "replace-all dropped the removed skill"
        );
    }

    #[tokio::test]
    async fn chain_bindings_round_trip_as_a_relational_row() {
        let _guard = config_guard().await;
        let Some(url) = db_url() else { return };
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());
        let area = uniq("area");
        src.store(&RegistryConfig {
            agents: vec![],
            skills: vec![],
            tools: vec![],
            chain_bindings: vec![ChainBinding {
                area: area.clone(),
                kind: "plan".into(),
                chain: "c".into(),
            }],
        })
        .await
        .unwrap();
        let got = src.load().await.unwrap();
        assert!(
            got.chain_bindings
                .iter()
                .any(|b| b.area == area && b.kind == "plan" && b.chain == "c")
        );
    }

    /// AC2's highest-risk serde path: `AgentDefinition` and `ToolSpec` carry NESTED structure
    /// (a `grants` map of `Permissions`, a nested `input_schema` object, a `credentials` vec) —
    /// unlike `SkillDef`/`ChainBinding`, which are flat. Proves the jsonb round-trip preserves
    /// that nested structure exactly, not just that a row with the right name exists.
    #[tokio::test]
    async fn agent_and_tool_round_trip_through_jsonb_including_nested_fields() {
        let _guard = config_guard().await;
        let Some(url) = db_url() else { return };
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());
        let agent_name = uniq("agent");
        let tool_name = uniq("tool");

        let mut grants = HashMap::new();
        grants.insert(
            tool_name.clone(),
            Permissions {
                paths: vec!["/w".into()],
                commands: vec![],
                network: NetworkPolicy::Hosts(vec!["x.example.com".into()]),
                caps: Default::default(),
            },
        );
        let agent = AgentDefinition {
            name: agent_name.clone(),
            area: "research".into(),
            kind: "reasoning".into(),
            chain: Some("c".into()),
            chains: HashMap::new(),
            grants,
            tools: vec![tool_name.clone()],
            skills: vec!["concise".into()],
            system_prompt: "be careful".into(),
        };

        let input_schema =
            serde_json::json!({"type":"object","properties":{"q":{"type":"string"}}});
        let tool = ToolSpec {
            name: tool_name.clone(),
            description: Some("does a thing".into()),
            input_schema: input_schema.clone(),
            effect_class: EffectClass::Observation,
            ttl_secs: Some(60),
            source: Some("web".into()),
            permissions: Permissions {
                paths: vec!["/w".into()],
                commands: vec!["ls".into()],
                network: NetworkPolicy::Deny,
                caps: Default::default(),
            },
            activation: Default::default(),
            credentials: vec!["api-key".into()],
        };

        src.store(&RegistryConfig {
            agents: vec![agent],
            skills: vec![],
            tools: vec![tool],
            chain_bindings: vec![],
        })
        .await
        .unwrap();

        let got = src.load().await.unwrap();

        let got_agent = got
            .agents
            .iter()
            .find(|a| a.name == agent_name)
            .expect("agent round-trips");
        assert_eq!(
            got_agent.tools,
            vec![tool_name.clone()],
            "tools list survives"
        );
        assert_eq!(
            got_agent.skills,
            vec!["concise".to_string()],
            "skills list survives"
        );
        let grant = got_agent
            .grants
            .get(&tool_name)
            .expect("grants map entry survives");
        assert_eq!(
            grant.paths,
            vec!["/w".to_string()],
            "nested Permissions.paths survives"
        );
        assert_eq!(
            grant.network,
            NetworkPolicy::Hosts(vec!["x.example.com".into()]),
            "nested Permissions.network survives"
        );

        let got_tool = got
            .tools
            .iter()
            .find(|t| t.name == tool_name)
            .expect("tool round-trips");
        assert_eq!(
            got_tool.input_schema, input_schema,
            "nested input_schema object survives"
        );
        assert_eq!(
            got_tool.credentials,
            vec!["api-key".to_string()],
            "credentials vec survives"
        );
        assert_eq!(
            got_tool.permissions.commands,
            vec!["ls".to_string()],
            "nested ToolSpec.permissions.commands survives"
        );
        assert_eq!(
            got_tool.effect_class,
            EffectClass::Observation,
            "effect_class survives"
        );
    }

    // ---- SP-DATA-4: the atomic read/write pair (the SP-DATA-2 TOCTOU carry-forwards) --------

    /// A whole config carrying exactly one named skill — the smallest content the
    /// atomicity tests can move and observe (mirrors `skill()` above; the name is
    /// verbatim, not uniquified, because these tests assert on it).
    fn cfg_with_skill(name: &str) -> RegistryConfig {
        RegistryConfig {
            agents: vec![],
            skills: vec![skill(name)],
            tools: vec![],
            chain_bindings: vec![],
        }
    }

    /// A minimal named `ToolSpec` — the atomic-read test needs content in a table read
    /// AFTER the first one, and `config_tools` is `read_all`'s third table.
    fn cfg_tool(name: &str) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            description: Some("d".into()),
            input_schema: serde_json::json!({"type": "object"}),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
            permissions: Default::default(),
            activation: Default::default(),
            credentials: Default::default(),
        }
    }

    /// The backend pid of a connection we control, so a wait predicate can name the
    /// specific blocker rather than any lock contention in the instance.
    async fn backend_pid(conn: &mut sqlx::PgConnection) -> i32 {
        let (pid,): (i32,) = sqlx::query_as("select pg_backend_pid()")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        pid
    }

    /// Block until some backend is waiting specifically on `holder_pid`, so the
    /// interleaving tests below are DETERMINISTIC rather than sleep-and-hope: a slow
    /// spawned task could otherwise still be pre-lock when we release the other side,
    /// and the test would pass for the wrong reason (a false green on broken code).
    ///
    /// The predicate is `pg_blocking_pids`, NOT `pg_locks where not granted`. The latter
    /// is instance-GLOBAL: any unrelated session contending anywhere in the cluster
    /// satisfies it, which returns early, skips the wait, and turns a true red into a
    /// false green (with the bump-last bug, a real wait yields `["a1","b1"]` and fails
    /// correctly, while no wait yields `["b1"]` and passes).
    async fn wait_until_blocked_by(pool: &PgPool, holder_pid: i32) {
        for _ in 0..200 {
            let (n,): (i64,) = sqlx::query_as(
                "select count(*) from pg_stat_activity where $1 = any(pg_blocking_pids(pid))",
            )
            .bind(holder_pid)
            .fetch_one(pool)
            .await
            .unwrap();
            if n > 0 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        panic!(
            "no backend ever blocked on connection {holder_pid} — the interleaving under test \
             never happened"
        );
    }

    /// The four config tables, in `write_all`'s replace-all order.
    const CFG_TABLES: [&str; 4] = [
        "orchestrator.config_agents",
        "orchestrator.config_skills",
        "orchestrator.config_tools",
        "orchestrator.config_chain_bindings",
    ];

    /// FIX 1 — two concurrent `store_and_bump`s must never MERGE their content.
    ///
    /// Both writers increment the single `config_versions` row FIRST, so they serialize
    /// there. The loser then runs its replace-all `DELETE` on a statement snapshot taken
    /// AFTER the winner committed, so it actually removes the winner's rows: true
    /// last-writer-wins.
    ///
    /// With the bump LAST (the original ordering) the loser's `DELETE` takes its snapshot
    /// before the winner commits, cannot see the winner's freshly-inserted rows, and they
    /// survive the "replace-all" — the durable content becomes the UNION of both configs
    /// while BOTH calls return `Ok`. That is a config push that reports success and did not
    /// happen: an operator revoking access with `{}` against a concurrent `{x, z}` push
    /// silently keeps `{x, z}`.
    #[tokio::test]
    async fn concurrent_store_and_bumps_do_not_merge_their_content() {
        let _guard = config_guard().await;
        let Some(url) = db_url() else { return };
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());

        // Seed EMPTY content: the loser's DELETE then has no rows of its own to block on,
        // so the ONLY contention is the version row — exactly the mechanism under test.
        let v0 = src
            .store_and_bump(&RegistryConfig::default())
            .await
            .unwrap();

        // Writer A: an explicit transaction shaped exactly like `store_and_bump` (bump,
        // then replace-all), held open so we control when it commits.
        let mut a = src.pool_for_test().begin().await.unwrap();
        let a_pid = backend_pid(&mut a).await;
        let (va,): (i64,) = sqlx::query_as(
            "insert into orchestrator.config_versions (id, version) values (true, 1)
             on conflict (id) do update set version = orchestrator.config_versions.version + 1,
                                            updated_at = now()
             returning version",
        )
        .fetch_one(&mut *a)
        .await
        .unwrap();
        for t in CFG_TABLES {
            sqlx::query(&format!("delete from {t}"))
                .execute(&mut *a)
                .await
                .unwrap();
        }
        sqlx::query("insert into orchestrator.config_skills (name, def) values ($1, $2)")
            .bind("a1")
            .bind(serde_json::to_value(skill("a1")).unwrap())
            .execute(&mut *a)
            .await
            .unwrap();

        // Writer B: a REAL `store_and_bump` on an independent connection. It must block
        // until A commits (on the version row when bumping first).
        let writer = PostgresConfigSource::new(connect(&url).await.unwrap());
        let b = tokio::spawn(async move { writer.store_and_bump(&cfg_with_skill("b1")).await });
        wait_until_blocked_by(src.pool_for_test(), a_pid).await;
        assert!(!b.is_finished(), "B must not commit before A does");

        a.commit().await.unwrap();
        let vb = b
            .await
            .unwrap()
            .expect("the losing writer still reports success");

        let (cfg, generation) = src.load_versioned().await.unwrap();
        let names: Vec<&str> = cfg.skills.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["b1"],
            "last-writer-wins: the durable content must be EXACTLY the last writer's config, \
             never the union of both"
        );
        assert_eq!(va as u64, v0 + 1, "A took the first generation step");
        assert_eq!(vb, v0 + 2, "B's bump serialized after A's");
        assert_eq!(generation, Some(v0 + 2), "both bumps are durable");
    }

    /// FIX 1 (corollary) — the same reorder also removes a DEADLOCK.
    ///
    /// The shape matters, and the three pre-fix failure modes are distinct:
    /// - DISJOINT names → the silent merge (both writers `Ok`, content is the union);
    ///   that is the test above.
    /// - exactly ONE shared name → no lock cycle, so the loser simply waits on the shared
    ///   pkey entry and then fails loudly with `23505` duplicate key.
    /// - TWO shared names in OPPOSITE insert order → a genuine cycle: each writer holds
    ///   the index entry the other needs next, and Postgres aborts one with `40P01`
    ///   deadlock detected. That is the shape used here, so this test covers the deadlock
    ///   it claims to.
    ///
    /// Bumping the single version row first serializes writers BEFORE either touches
    /// content, so none of the three interleavings can arise. The extra per-writer
    /// distinct name keeps the merge invariant meaningful alongside the deadlock shape:
    /// the durable content must equal EXACTLY one writer's config, never the union.
    /// A MULTI-THREADED runtime, unlike every other test here: the deadlock needs both
    /// writers to hold one index entry while wanting the other, and on the default
    /// current-thread runtime the two tasks interleave only at await points and in
    /// practice run to completion one after the other (which yields `23505`, not the
    /// cycle). Real parallelism is what makes the cycle reachable.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_writers_with_overlapping_names_neither_deadlock_nor_merge() {
        let _guard = config_guard().await;
        let Some(url) = db_url() else { return };
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());
        src.store_and_bump(&RegistryConfig::default())
            .await
            .unwrap();

        // Two SHARED names in opposite insert order (the `40P01` cycle), plus one distinct
        // name each so a merge would be visible as a fourth entity.
        let cfg_of = |own: &str, reversed: bool| RegistryConfig {
            agents: vec![],
            skills: if reversed {
                vec![skill("s_2"), skill("s_1"), skill(own)]
            } else {
                vec![skill("s_1"), skill("s_2"), skill(own)]
            },
            tools: vec![],
            chain_bindings: vec![],
        };
        let (one, two) = (cfg_of("a_x", false), cfg_of("b_y", true));
        let wa = PostgresConfigSource::new(connect(&url).await.unwrap());
        let wb = PostgresConfigSource::new(connect(&url).await.unwrap());

        // Repeat: a deadlock is a race, so one round could get lucky.
        for round in 0..10 {
            let (a, b) = (wa.clone(), wb.clone());
            let (o, t) = (one.clone(), two.clone());
            let ha = tokio::spawn(async move { a.store_and_bump(&o).await });
            let hb = tokio::spawn(async move { b.store_and_bump(&t).await });
            ha.await
                .unwrap()
                .unwrap_or_else(|e| panic!("round {round}: writer A failed: {e:?}"));
            hb.await
                .unwrap()
                .unwrap_or_else(|e| panic!("round {round}: writer B failed: {e:?}"));

            let (cfg, _) = src.load_versioned().await.unwrap();
            let mut names: Vec<&str> = cfg.skills.iter().map(|s| s.name.as_str()).collect();
            names.sort_unstable();
            assert!(
                names == ["a_x", "s_1", "s_2"] || names == ["b_y", "s_1", "s_2"],
                "round {round}: content must be exactly one writer's config, got {names:?} \
                 (both distinct names present = the two configs merged)"
            );
        }
    }

    /// AC5 — `load_versioned` ITSELF returns a consistent pair with a writer committing
    /// in the MIDDLE of its read. This is the real guard for the TOCTOU fix.
    ///
    /// Mechanism: `read_all` reads agents, then skills, then tools, then bindings. Holding
    /// `access exclusive` on `config_tools` lets the reader take its snapshot on the FIRST
    /// read and then block on the THIRD, giving the writer a window to commit a complete
    /// new config mid-read. Under the `repeatable read` snapshot the reader must still
    /// return the ENTIRELY OLD world; without it each statement re-snapshots and the
    /// returned pair is torn (`t_new` content under a fresh generation).
    ///
    /// Deliberately asserts on `load_versioned`'s OWN return value, not on a
    /// hand-rolled transaction: an earlier version of this test drove its own
    /// `repeatable read` transaction and asserted on values read inside it, which merely
    /// demonstrated Postgres semantics — it passed even with the `set transaction
    /// isolation level` line deleted from `load_versioned`. This version fails.
    #[tokio::test]
    async fn load_versioned_returns_a_consistent_pair_despite_a_writer_committing_mid_read() {
        let _guard = config_guard().await;
        let Some(url) = db_url() else { return };
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());

        // Seed a known, self-consistent world: skill "old" + tool "t_old" at v0.
        let seed = RegistryConfig {
            agents: vec![],
            skills: vec![skill("old")],
            tools: vec![cfg_tool("t_old")],
            chain_bindings: vec![],
        };
        let v0 = src.store_and_bump(&seed).await.unwrap();

        // Take the gate BEFORE the reader starts, so the reader is guaranteed to block
        // on its third read rather than racing past it.
        let gate = PostgresConfigSource::new(connect(&url).await.unwrap());
        let mut w = gate.pool_for_test().begin().await.unwrap();
        let w_pid = backend_pid(&mut w).await;
        sqlx::query("lock table orchestrator.config_tools in access exclusive mode")
            .execute(&mut *w)
            .await
            .unwrap();

        let reader = PostgresConfigSource::new(connect(&url).await.unwrap());
        let r = tokio::spawn(async move { reader.load_versioned().await });
        wait_until_blocked_by(src.pool_for_test(), w_pid).await;
        assert!(
            !r.is_finished(),
            "the reader must still be mid-read when the writer commits"
        );

        // A COMPLETE new config lands, generation and all, while the read is in flight.
        for t in CFG_TABLES {
            sqlx::query(&format!("delete from {t}"))
                .execute(&mut *w)
                .await
                .unwrap();
        }
        sqlx::query("insert into orchestrator.config_skills (name, def) values ($1, $2)")
            .bind("new")
            .bind(serde_json::to_value(skill("new")).unwrap())
            .execute(&mut *w)
            .await
            .unwrap();
        sqlx::query("insert into orchestrator.config_tools (name, spec) values ($1, $2)")
            .bind("t_new")
            .bind(serde_json::to_value(cfg_tool("t_new")).unwrap())
            .execute(&mut *w)
            .await
            .unwrap();
        let (vw,): (i64,) = sqlx::query_as(
            "insert into orchestrator.config_versions (id, version) values (true, 1)
             on conflict (id) do update set version = orchestrator.config_versions.version + 1,
                                            updated_at = now()
             returning version",
        )
        .fetch_one(&mut *w)
        .await
        .unwrap();
        w.commit().await.unwrap();

        let (cfg, ver) = r.await.unwrap().expect("the read itself must succeed");

        // The whole point: the pair is internally consistent and entirely pre-write.
        assert_eq!(
            ver,
            Some(v0),
            "the generation must be the one matching the content read, not the writer's"
        );
        assert_eq!(
            cfg.tools
                .iter()
                .map(|t| t.name.as_str())
                .collect::<Vec<_>>(),
            vec!["t_old"],
            "the tools read (AFTER the writer committed) must come from the original snapshot"
        );
        assert_eq!(
            cfg.skills
                .iter()
                .map(|s| s.name.as_str())
                .collect::<Vec<_>>(),
            vec!["old"],
            "the skills read (BEFORE the writer committed) must agree with the tools read"
        );
        assert_eq!(vw as u64, v0 + 1, "the writer really did advance the world");
    }

    /// A check of the Postgres mechanism `load_versioned` RELIES on, not of
    /// `load_versioned` itself: a `repeatable read` transaction's snapshot is fixed at its
    /// first read, so a concurrent single-transaction writer lands wholly before or wholly
    /// after it. Kept because that guarantee is the design's load-bearing assumption and a
    /// server-version change could invalidate it — but it drives its OWN transaction, so it
    /// canNOT detect a regression in `load_versioned`. See
    /// `load_versioned_returns_a_consistent_pair_despite_a_writer_committing_mid_read` for
    /// the test that guards the production code path.
    #[tokio::test]
    async fn repeatable_read_snapshots_are_fixed_at_the_first_read() {
        let _guard = config_guard().await;
        let Some(url) = db_url() else { return };
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());

        // Seed a known (content, generation) pair.
        src.store_and_bump(&cfg_with_skill("before")).await.unwrap();
        let (_, v0) = src.load_versioned().await.unwrap();
        let v0 = v0.expect("a versioned source always reports Some");

        // Open the snapshot and take its FIRST read inside the transaction.
        let mut tx = src.pool_for_test().begin().await.unwrap();
        sqlx::query("set transaction isolation level repeatable read")
            .execute(&mut *tx)
            .await
            .unwrap();
        let (first,): (i64,) = sqlx::query_as("select count(*) from orchestrator.config_skills")
            .fetch_one(&mut *tx)
            .await
            .unwrap();

        // A COMPLETE concurrent write from an independent connection.
        let writer = PostgresConfigSource::new(connect(&url).await.unwrap());
        writer
            .store_and_bump(&cfg_with_skill("after"))
            .await
            .unwrap();

        // Finish reading inside the original snapshot: it must still see the OLD world.
        let (again,): (i64,) = sqlx::query_as("select count(*) from orchestrator.config_skills")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        let (snap_v,): (i64,) =
            sqlx::query_as("select version from orchestrator.config_versions where id = true")
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        tx.commit().await.unwrap();

        assert_eq!(
            again, first,
            "the snapshot must not see the concurrent write"
        );
        assert_eq!(
            snap_v as u64, v0,
            "the snapshot's generation must match its content — never a torn pair"
        );
    }

    /// AC6 — content and generation move together, atomically.
    #[tokio::test]
    async fn store_and_bump_advances_content_and_generation_together() {
        let _guard = config_guard().await;
        let Some(url) = db_url() else { return };
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());
        let (_, before) = src.load_versioned().await.unwrap();
        let before = before.expect("Some");

        let v = src
            .store_and_bump(&cfg_with_skill("coupled"))
            .await
            .unwrap();

        let (cfg, after) = src.load_versioned().await.unwrap();
        assert_eq!(after, Some(v), "the returned version is the durable one");
        assert_eq!(v, before + 1, "exactly one generation step");
        assert!(
            cfg.skills.iter().any(|s| s.name == "coupled"),
            "the content landed with the bump"
        );
    }

    /// AC6 — a rolled-back write moves NEITHER content nor generation.
    #[tokio::test]
    async fn a_failed_store_and_bump_leaves_content_and_generation_untouched() {
        let _guard = config_guard().await;
        let Some(url) = db_url() else { return };
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());
        src.store_and_bump(&cfg_with_skill("stable")).await.unwrap();
        let (before_cfg, before_v) = src.load_versioned().await.unwrap();

        // A tool whose jsonb serialization is fine but whose NAME violates the
        // primary key twice in one transaction -> the txn aborts.
        let dup = ToolSpec {
            name: "dup".into(),
            description: Some("d".into()),
            input_schema: serde_json::json!({"type": "object"}),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
            permissions: Default::default(),
            activation: Default::default(),
            credentials: Default::default(),
        };
        let mut bad = cfg_with_skill("stable");
        bad.tools = vec![dup.clone(), dup];
        let err = src.store_and_bump(&bad).await;
        assert!(err.is_err(), "a duplicate primary key must abort the txn");

        let (after_cfg, after_v) = src.load_versioned().await.unwrap();
        assert_eq!(
            after_v, before_v,
            "generation must not advance on a failed write"
        );
        assert_eq!(
            after_cfg.skills.len(),
            before_cfg.skills.len(),
            "content must not change on a failed write"
        );
    }

    // ---- SP-DATA-4.1 Task 1: store_and_bump_if (an airtight CAS) ----------------------------

    /// A CAS write must apply ONLY at the expected generation. This is the residual
    /// window the SP-DATA-4 re-read could not close: between `version()` returning and
    /// the bump committing.
    #[tokio::test]
    async fn store_and_bump_if_refuses_at_an_unexpected_generation() {
        let Some(url) = db_url() else { return };
        let _guard = config_guard().await;
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());

        let v = src.store_and_bump(&cfg_with_skill("base")).await.unwrap();

        // Wrong expectation -> refuse, and change NOTHING.
        let refused = src
            .store_and_bump_if(&cfg_with_skill("should-not-land"), v - 1)
            .await
            .unwrap();
        assert!(refused.is_none(), "a stale expectation must not apply");
        let (cfg, now) = src.load_versioned().await.unwrap();
        assert_eq!(now, Some(v), "generation must not advance on a refusal");
        assert!(
            cfg.skills.iter().any(|s| s.name == "base"),
            "content must not change on a refusal"
        );

        // Correct expectation -> applies and advances by exactly one.
        let applied = src
            .store_and_bump_if(&cfg_with_skill("landed"), v)
            .await
            .unwrap()
            .expect("the matching expectation must apply");
        assert_eq!(applied, v + 1);
        let (cfg, now) = src.load_versioned().await.unwrap();
        assert_eq!(now, Some(v + 1));
        assert!(cfg.skills.iter().any(|s| s.name == "landed"));
    }

    /// The first-push subtlety: `config_versions` starts with NO ROW (absent ⇒
    /// generation 0 per `version()`'s contract). A naive `UPDATE … WHERE version = $1`
    /// can never match a genuinely absent row, so it would report a miss on a
    /// first-ever push even though nothing raced. `store_and_bump_if` must fold that
    /// case into the same atomic statement — not push it onto the caller as a
    /// re-check-then-fallback, which would reopen a (narrower) version of the very
    /// race this method exists to close (see the doc comment on the method for the
    /// full argument). Proven here directly against a table where the row is absent.
    #[tokio::test]
    async fn store_and_bump_if_lands_a_genuine_first_push_when_the_row_is_absent() {
        let Some(url) = db_url() else { return };
        let _guard = config_guard().await;
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());

        sqlx::query("delete from orchestrator.config_versions")
            .execute(src.pool_for_test())
            .await
            .unwrap();
        assert_eq!(
            src.version().await.unwrap(),
            Some(0),
            "an absent row reports generation 0"
        );

        let applied = src
            .store_and_bump_if(&cfg_with_skill("first-ever"), 0)
            .await
            .unwrap();
        assert_eq!(
            applied,
            Some(1),
            "a genuine first push against an absent row must land, not report a false miss"
        );
        let (cfg, now) = src.load_versioned().await.unwrap();
        assert_eq!(now, Some(1));
        assert!(cfg.skills.iter().any(|s| s.name == "first-ever"));
    }

    /// The concurrency proof for the same first-push case: two writers BOTH believing
    /// the generation is 0 (row absent) must never both land — exactly one succeeds,
    /// the loser gets a clean `None`, and the final generation is 1, not 2. This is
    /// what distinguishes the single-statement CAS from the plan's caller-side
    /// check-then-fallback, which — for this exact interleaving — would let both
    /// `store_and_bump` fallback calls succeed (true last-writer-wins, per
    /// `store_and_bump`'s own doc comment) rather than refusing the loser outright.
    #[tokio::test]
    async fn concurrent_first_pushes_do_not_both_land() {
        let Some(url) = db_url() else { return };
        let _guard = config_guard().await;
        let seed = PostgresConfigSource::new(connect(&url).await.unwrap());
        sqlx::query("delete from orchestrator.config_versions")
            .execute(seed.pool_for_test())
            .await
            .unwrap();

        let a = PostgresConfigSource::new(connect(&url).await.unwrap());
        let b = PostgresConfigSource::new(connect(&url).await.unwrap());
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let (ba, bb) = (barrier.clone(), barrier.clone());
        let ha = tokio::spawn(async move {
            ba.wait().await;
            a.store_and_bump_if(&cfg_with_skill("racer-a"), 0).await
        });
        let hb = tokio::spawn(async move {
            bb.wait().await;
            b.store_and_bump_if(&cfg_with_skill("racer-b"), 0).await
        });
        let (ra, rb) = (ha.await.unwrap().unwrap(), hb.await.unwrap().unwrap());

        let landed = [ra, rb].into_iter().filter(|r| r.is_some()).count();
        assert_eq!(
            landed, 1,
            "exactly one concurrent first push may land: a={ra:?} b={rb:?}"
        );
        let (cfg, now) = seed.load_versioned().await.unwrap();
        assert_eq!(
            now,
            Some(1),
            "the generation must advance exactly once, not merge two bumps"
        );
        let winner_name = if ra.is_some() { "racer-a" } else { "racer-b" };
        assert!(
            cfg.skills.iter().any(|s| s.name == winner_name),
            "the durable content must be the winner's alone, not a union: {:?}",
            cfg.skills
        );
    }

    /// The COMMON production path, under concurrency: two operators both pushing
    /// against the same non-zero generation. This rests on a DIFFERENT Postgres
    /// mechanism than the first-push race above — there the winner's row is simply
    /// invisible to the loser's snapshot; here a row ALREADY EXISTS at the expected
    /// version, so the loser's `UPDATE` blocks on the row lock, then Postgres's
    /// EvalPlanQual re-check re-evaluates `version = $1` (with the CTE subplan) against
    /// the winner's committed row and correctly finds no match. Nothing in the
    /// `expected == 0` tests exercises that path, so this is the one that would catch a
    /// regression here — notably, a future switch to `REPEATABLE READ` would turn this
    /// clean `None` into a `40001` serialization error instead.
    #[tokio::test]
    async fn concurrent_pushes_at_the_same_nonzero_generation_do_not_both_land() {
        let Some(url) = db_url() else { return };
        let _guard = config_guard().await;
        let seed = PostgresConfigSource::new(connect(&url).await.unwrap());
        let v0 = seed
            .store_and_bump(&cfg_with_skill("seed-nonzero"))
            .await
            .unwrap();

        let a = PostgresConfigSource::new(connect(&url).await.unwrap());
        let b = PostgresConfigSource::new(connect(&url).await.unwrap());
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let (ba, bb) = (barrier.clone(), barrier.clone());
        let ha = tokio::spawn(async move {
            ba.wait().await;
            a.store_and_bump_if(&cfg_with_skill("nz-racer-a"), v0).await
        });
        let hb = tokio::spawn(async move {
            bb.wait().await;
            b.store_and_bump_if(&cfg_with_skill("nz-racer-b"), v0).await
        });
        let (ra, rb) = (ha.await.unwrap().unwrap(), hb.await.unwrap().unwrap());

        let landed = [ra, rb].into_iter().filter(|r| r.is_some()).count();
        assert_eq!(
            landed, 1,
            "exactly one concurrent push at the same non-zero generation may land: \
             a={ra:?} b={rb:?}"
        );
        let (cfg, now) = seed.load_versioned().await.unwrap();
        assert_eq!(
            now,
            Some(v0 + 1),
            "the generation must advance exactly once past the seeded value, not twice"
        );
        let winner_name = if ra.is_some() {
            "nz-racer-a"
        } else {
            "nz-racer-b"
        };
        assert!(
            cfg.skills.iter().any(|s| s.name == winner_name),
            "the durable content must be the winner's alone, not a union: {:?}",
            cfg.skills
        );
    }

    /// The remaining untested cell of the matrix: `expected == 0` with the row
    /// PRESENT at generation 0 — the state where BOTH CTE arms are structurally live
    /// in the same statement (`ins`'s `WHERE $1 = 0` guard is true, so it attempts the
    /// insert; `upd`'s `WHERE version = $1` would also match if it ran). `bump_on`
    /// never actually produces a stored `version = 0` (its first write is `1`), so
    /// this state is reached here only by a direct row insert — but it is exactly the
    /// shape the `ON CONFLICT (id) DO NOTHING` exists to handle: `ins` attempts the
    /// insert, collides on the existing row, and no-ops (returning nothing); `upd`
    /// then sees its `NOT EXISTS (SELECT 1 FROM ins)` guard hold, and its own `WHERE
    /// version = $1` matches the row that is genuinely present — so the update, not
    /// the insert, is what lands.
    #[tokio::test]
    async fn store_and_bump_if_applies_when_the_row_is_present_at_generation_zero() {
        let Some(url) = db_url() else { return };
        let _guard = config_guard().await;
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());

        sqlx::query("delete from orchestrator.config_versions")
            .execute(src.pool_for_test())
            .await
            .unwrap();
        sqlx::query("insert into orchestrator.config_versions (id, version) values (true, 0)")
            .execute(src.pool_for_test())
            .await
            .unwrap();
        assert_eq!(
            src.version().await.unwrap(),
            Some(0),
            "the row is PRESENT, at version 0 — distinct from the absent-row case"
        );

        let applied = src
            .store_and_bump_if(&cfg_with_skill("present-at-zero"), 0)
            .await
            .unwrap();
        assert_eq!(
            applied,
            Some(1),
            "the conditional UPDATE must land: the insert collides and no-ops, \
             the update matches the present row"
        );
        let (cfg, now) = src.load_versioned().await.unwrap();
        assert_eq!(now, Some(1));
        assert!(cfg.skills.iter().any(|s| s.name == "present-at-zero"));
    }

    // ---- SP-DATA-3: PostgresSchedulerStore --------------------------------------------------

    fn sg() -> Graph {
        Graph { nodes: vec![] }
    }
    fn ts(secs: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(secs, 0).unwrap()
    }

    /// The exactly-once gate: two concurrent `claim_due` on one due run → exactly one wins.
    #[tokio::test]
    async fn pg_claim_due_is_exactly_once_under_concurrent_claims() {
        let _guard = scheduler_guard().await;
        let Some(url) = db_url() else { return };
        let s = PostgresSchedulerStore::new(connect(&url).await.unwrap());
        let r = run();
        let now = ts(2_000_000);
        s.enqueue(r, &sg(), now).await.unwrap();
        s.record_paused(r, Some(now), "gated").await.unwrap(); // due exactly at `now`
        let (a, b) = tokio::join!(
            s.claim_due(now, Duration::seconds(60), 10),
            s.claim_due(now, Duration::seconds(60), 10),
        );
        let got: usize = a
            .unwrap()
            .iter()
            .chain(b.unwrap().iter())
            .filter(|(x, _)| *x == r)
            .count();
        assert_eq!(
            got, 1,
            "a due run is claimed by exactly one of two concurrent claimers"
        );
    }

    /// Round-trip + transitions: not-due is skipped; due is claimed (stored graph returned); terminal.
    #[tokio::test]
    async fn pg_round_trip_and_transitions() {
        let _guard = scheduler_guard().await;
        let Some(url) = db_url() else { return };
        let s = PostgresSchedulerStore::new(connect(&url).await.unwrap());
        let r = run();
        let t0 = ts(2_100_000);
        s.enqueue(r, &sg(), t0).await.unwrap();
        s.record_paused(r, Some(t0 + Duration::seconds(10)), "gated")
            .await
            .unwrap();
        assert_eq!(
            s.status(r).await.unwrap().unwrap().status,
            RunStatus::Paused
        );
        assert!(s.list_paused().await.unwrap().iter().any(|x| x.run == r));
        // not due yet
        assert!(
            s.claim_due(t0, Duration::seconds(60), 10)
                .await
                .unwrap()
                .iter()
                .all(|(x, _)| *x != r)
        );
        // due → claimed, and the stored graph round-trips
        let due = s
            .claim_due(t0 + Duration::seconds(20), Duration::seconds(60), 10)
            .await
            .unwrap();
        assert!(
            due.iter().any(|(x, g)| *x == r && g.nodes.is_empty()),
            "claim returns the stored graph"
        );
        s.record_terminal(r, RunStatus::Completed, None)
            .await
            .unwrap();
        assert_eq!(
            s.status(r).await.unwrap().unwrap().status,
            RunStatus::Completed
        );
    }

    /// Intervene: cancel makes a paused run unwakeable; force_wake makes a NULL-deadline pause claimable.
    #[tokio::test]
    async fn pg_cancel_and_force_wake() {
        let _guard = scheduler_guard().await;
        let Some(url) = db_url() else { return };
        let s = PostgresSchedulerStore::new(connect(&url).await.unwrap());
        let (c, f) = (run(), run());
        let now = ts(2_200_000);
        s.enqueue(c, &sg(), now).await.unwrap();
        s.record_paused(c, Some(now + Duration::seconds(10)), "g")
            .await
            .unwrap();
        s.cancel(c).await.unwrap();
        assert_eq!(
            s.status(c).await.unwrap().unwrap().status,
            RunStatus::Cancelled
        );
        s.enqueue(f, &sg(), now).await.unwrap();
        s.record_paused(f, None, "in-doubt").await.unwrap(); // NULL deadline
        assert!(
            s.claim_due(now + Duration::seconds(1000), Duration::seconds(60), 10)
                .await
                .unwrap()
                .iter()
                .all(|(x, _)| *x != f),
            "a NULL-deadline pause is never auto-woken"
        );
        s.force_wake(f, now).await.unwrap();
        assert!(
            s.claim_due(now + Duration::seconds(1), Duration::seconds(60), 10)
                .await
                .unwrap()
                .iter()
                .any(|(x, _)| *x == f),
            "force_wake makes it claimable"
        );
    }

    // ---- SP-DATA-4.1 #7: retention pruning ---------------------------------------------

    /// The SQL allowlist is hand-written text; this pins it to `RunStatus::is_terminal()` so
    /// the two cannot drift into a `DELETE` that disagrees with the enum. A new `RunStatus`
    /// variant must be added to this array — the arity assertion is what makes forgetting
    /// visible, because the literal count and the array's terminal count stop matching.
    #[test]
    fn the_prune_allowlist_matches_run_status_is_terminal() {
        let all = [
            RunStatus::Waking,
            RunStatus::Paused,
            RunStatus::Completed,
            RunStatus::Failed,
            RunStatus::Cancelled,
        ];
        for st in all {
            let quoted = format!("'{}'", st.as_str());
            assert_eq!(
                TERMINAL_STATUS_LITERALS.contains(&quoted),
                st.is_terminal(),
                "{quoted} must appear in the prune allowlist iff it is terminal"
            );
        }
        assert_eq!(
            TERMINAL_STATUS_LITERALS.matches('\'').count() / 2,
            all.iter().filter(|s| s.is_terminal()).count(),
            "the allowlist holds exactly the terminal statuses and nothing else"
        );
    }

    /// Force a row's `updated_at`. Every transition stamps the server's `now()`, so writing
    /// the column directly is the only way to produce a row old enough to be prunable inside
    /// a test.
    async fn age_row(s: &PostgresSchedulerStore, r: RunId, at: DateTime<Utc>) {
        sqlx::query("update orchestrator.scheduled_runs set updated_at=$2 where run_id=$1")
            .bind(r.0)
            .bind(at)
            .execute(&s.pool)
            .await
            .expect("age a row");
    }

    /// Remove this test's OWN surviving rows. The non-terminal ones are deliberately
    /// unprunable and would otherwise accumulate forever in the dev database — and a
    /// leftover `paused` row with a 1971 deadline is permanently due, so every other test's
    /// `claim_due` sweep would keep picking it up.
    async fn forget_rows(s: &PostgresSchedulerStore, runs: &[RunId]) {
        for r in runs {
            sqlx::query("delete from orchestrator.scheduled_runs where run_id=$1")
                .bind(r.0)
                .execute(&s.pool)
                .await
                .expect("clean up this test's rows");
        }
    }

    /// Postgres parity for the in-memory suite, including THE safety property: an ancient
    /// `paused` row (BOTH classes — a timed deadline and the NULL-deadline in-doubt class)
    /// and an ancient `waking` row all survive a cutoff decades past them.
    ///
    /// Everything is aged into 1971 and the cutoff sits between the two 1971 stamps, so the
    /// blast radius cannot reach any other test's rows — every one of those carries the live
    /// server clock.
    #[tokio::test]
    async fn pg_prune_terminal_deletes_old_terminal_rows_and_never_a_live_one() {
        let _guard = scheduler_guard().await;
        let Some(url) = db_url() else { return };
        let s = PostgresSchedulerStore::new(connect(&url).await.unwrap());

        let old = ts(45_000_000); // 1971-06-06
        let cutoff = ts(45_100_000);
        let recent = ts(45_200_000); // still 1971, but INSIDE the retention window

        let (done, failed, cancelled, fresh) = (run(), run(), run(), run());
        let (timed, in_doubt, waking) = (run(), run(), run());

        for r in [done, failed, cancelled, fresh, timed, in_doubt, waking] {
            s.enqueue(r, &sg(), old).await.unwrap();
        }
        s.record_terminal(done, RunStatus::Completed, None)
            .await
            .unwrap();
        s.record_terminal(failed, RunStatus::Failed, Some("boom"))
            .await
            .unwrap();
        s.cancel(cancelled).await.unwrap();
        s.record_terminal(fresh, RunStatus::Completed, None)
            .await
            .unwrap();
        s.record_paused(timed, Some(old), "quota").await.unwrap();
        s.record_paused(in_doubt, None, "in-doubt mutation")
            .await
            .unwrap();
        // `waking` is left exactly as `enqueue` made it — an in-flight drive holding a lease.

        for r in [done, failed, cancelled, timed, in_doubt, waking] {
            age_row(&s, r, old).await;
        }
        age_row(&s, fresh, recent).await;

        // The preview and the effect must agree. `>=` rather than `==` on the count only
        // because a PREVIOUS run of this test that panicked mid-way could have left aged
        // terminal rows behind; the per-row assertions below are the exact ones.
        let counted = s.count_terminal_before(cutoff).await.unwrap();
        assert!(
            counted >= 3,
            "the three aged terminal rows must be previewed, saw {counted}"
        );
        let deleted = s.prune_terminal(cutoff).await.unwrap();
        assert_eq!(
            deleted, counted,
            "the operator's preview must match what is actually deleted"
        );

        for r in [done, failed, cancelled] {
            assert!(
                s.status(r).await.unwrap().is_none(),
                "an aged terminal row must be gone"
            );
        }
        assert_eq!(
            s.status(fresh).await.unwrap().unwrap().status,
            RunStatus::Completed,
            "a terminal row INSIDE the retention window must survive"
        );
        assert_eq!(
            s.status(timed).await.unwrap().unwrap().status,
            RunStatus::Paused,
            "a paused run awaiting a deadline is never prunable, at any age"
        );
        let survivor = s.status(in_doubt).await.unwrap().unwrap();
        assert_eq!(
            survivor.status,
            RunStatus::Paused,
            "a NULL-deadline pause waits indefinitely for a human — deleting it is data loss"
        );
        assert_eq!(
            survivor.next_wake, None,
            "and it is still the in-doubt class"
        );
        assert_eq!(
            s.status(waking).await.unwrap().unwrap().status,
            RunStatus::Waking,
            "a waking row may be a live lease held by another process"
        );
        assert_eq!(
            s.count_terminal_before(cutoff).await.unwrap(),
            0,
            "nothing terminal is left in scope after the prune"
        );

        forget_rows(&s, &[fresh, timed, in_doubt, waking]).await;
    }
}
