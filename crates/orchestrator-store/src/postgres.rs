//! SP-DATA-1: Postgres adapters for the run-state seams (`ExecutionJournal`/`ContentStore`/
//! `ContextStore`). Schema-agnostic — runs against the dbd-managed `orchestrator.*` schema.
//! Uses sqlx RUNTIME queries (not the compile-time `query!` macros) so the crate builds with
//! no database. Feature-gated: default builds don't pull sqlx.

use orchestrator_core::{
    ChainBinding, ConfigSource, ContentRef, ContentStore, ContextKey, ContextRef, ContextStore,
    Digest, ExecutionJournal, FORMAT_VERSION, JournalError, JournalEvent, OrchestratorError,
    RegistryConfig, RunId, Scope, Seq, Snapshot, digest_of,
};
use sqlx::postgres::{PgPool, PgPoolOptions};

/// Connect a pool to `database_url` (the dbd-applied `orchestrator.*` schema must exist).
pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(8)
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

/// A durable `ConfigSource` (SP-DATA-2): the registry config lives in the `orchestrator.config_*`
/// tables as jsonb rows, with a single-row `config_versions` global generation. `load()` reads the
/// whole registry; `version()` reports the durable generation so a run's `#cfg{gen}` fence is
/// cross-process meaningful. `store`/`bump_config_version` are the write path (this slice's seeder +
/// SP-DATA-4's CLI entry point).
///
/// KNOWN LIMITATION — cross-process fence correctness under a CONCURRENT config writer
/// (deferred: unreachable in this slice, MUST close before SP-DATA-4 ships a live writer):
/// `load()` and `version()` are separate reads, and `load()` itself reads the four config tables
/// across four independent pool snapshots. So a concurrent `store()`+`bump_config_version()` can
/// hand a reload a TORN pair — notably (STALE config, FRESH generation): a run then stamps a
/// fresh-gen fence while serving stale config, and a later resume that reads the now-consistent
/// (fresh, fresh) state MATCHES the fence and silently continues under different config. Re-reading
/// `version()` at resume does NOT neutralize this (it reads the consistent state and passes). It is
/// safe ONLY because this slice has no concurrent writer (`store` is test-only; reloads serialized).
/// Fix: read the four config tables AND `config_versions` in ONE `REPEATABLE READ` transaction
/// (a single `load_versioned()` snapshot); land it before SP-DATA-4's CLI introduces a live writer.
#[derive(Clone)]
pub struct PostgresConfigSource {
    pool: PgPool,
}

impl PostgresConfigSource {
    /// Wrap a connection pool (see [`connect`]).
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Replace-all write of the whole registry in one transaction: delete every config row, then
    /// insert `cfg`'s — so `load()` afterward reproduces `cfg` exactly. Does NOT bump the version
    /// (the caller bumps explicitly after committing a change).
    ///
    /// FOOTGUN (close in SP-DATA-4): the fence is generation-based, not content-based — a `store()`
    /// whose caller forgets `bump_config_version()` changes config content WITHOUT advancing the
    /// generation, so a cross-process resume matches the (unchanged) fence and silently runs the new
    /// config. A live config-mutation surface MUST couple store+bump (ideally one transaction / a
    /// `store_and_bump` helper). Safe in-slice: the only callers are tests that immediately bump.
    pub async fn store(&self, cfg: &RegistryConfig) -> Result<(), OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(store_err)?;
        for t in [
            "orchestrator.config_agents",
            "orchestrator.config_skills",
            "orchestrator.config_tools",
            "orchestrator.config_chain_bindings",
        ] {
            sqlx::query(&format!("delete from {t}"))
                .execute(&mut *tx)
                .await
                .map_err(store_err)?;
        }
        for a in &cfg.agents {
            let v = serde_json::to_value(a).map_err(store_err_ser)?;
            sqlx::query("insert into orchestrator.config_agents (name, def) values ($1, $2)")
                .bind(&a.name)
                .bind(v)
                .execute(&mut *tx)
                .await
                .map_err(store_err)?;
        }
        for s in &cfg.skills {
            let v = serde_json::to_value(s).map_err(store_err_ser)?;
            sqlx::query("insert into orchestrator.config_skills (name, def) values ($1, $2)")
                .bind(&s.name)
                .bind(v)
                .execute(&mut *tx)
                .await
                .map_err(store_err)?;
        }
        for t in &cfg.tools {
            let v = serde_json::to_value(t).map_err(store_err_ser)?;
            sqlx::query("insert into orchestrator.config_tools (name, spec) values ($1, $2)")
                .bind(&t.name)
                .bind(v)
                .execute(&mut *tx)
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
            .execute(&mut *tx)
            .await
            .map_err(store_err)?;
        }
        tx.commit().await.map_err(store_err)?;
        Ok(())
    }

    /// Atomic upsert-increment of the single-row global generation; returns the new version.
    pub async fn bump_config_version(&self) -> Result<u64, OrchestratorError> {
        let (v,): (i64,) = sqlx::query_as(
            "insert into orchestrator.config_versions (id, version) values (true, 1)
             on conflict (id) do update set version = orchestrator.config_versions.version + 1,
                                            updated_at = now()
             returning version",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(v as u64)
    }
}

#[async_trait::async_trait]
impl ConfigSource for PostgresConfigSource {
    async fn load(&self) -> Result<RegistryConfig, OrchestratorError> {
        let agents: Vec<(String, serde_json::Value)> =
            sqlx::query_as("select name, def from orchestrator.config_agents order by name")
                .fetch_all(&self.pool)
                .await
                .map_err(cfg_load_err)?;
        let skills: Vec<(String, serde_json::Value)> =
            sqlx::query_as("select name, def from orchestrator.config_skills order by name")
                .fetch_all(&self.pool)
                .await
                .map_err(cfg_load_err)?;
        let tools: Vec<(String, serde_json::Value)> =
            sqlx::query_as("select name, spec from orchestrator.config_tools order by name")
                .fetch_all(&self.pool)
                .await
                .map_err(cfg_load_err)?;
        let bindings: Vec<(String, String, String)> = sqlx::query_as(
            "select area, kind, chain from orchestrator.config_chain_bindings order by area, kind",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(cfg_load_err)?;
        Ok(RegistryConfig {
            agents: agents
                .into_iter()
                .map(|(n, v)| {
                    serde_json::from_value(v).map_err(|e| {
                        OrchestratorError::RegistryLoad(format!("deser agent {n}: {e}"))
                    })
                })
                .collect::<Result<_, _>>()?,
            skills: skills
                .into_iter()
                .map(|(n, v)| {
                    serde_json::from_value(v).map_err(|e| {
                        OrchestratorError::RegistryLoad(format!("deser skill {n}: {e}"))
                    })
                })
                .collect::<Result<_, _>>()?,
            tools: tools
                .into_iter()
                .map(|(n, v)| {
                    serde_json::from_value(v).map_err(|e| {
                        OrchestratorError::RegistryLoad(format!("deser tool {n}: {e}"))
                    })
                })
                .collect::<Result<_, _>>()?,
            chain_bindings: bindings
                .into_iter()
                .map(|(area, kind, chain)| ChainBinding { area, kind, chain })
                .collect(),
        })
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
}

/// A config LOAD-path transport/parse failure → `RegistryLoad` (the `ConfigSource` convention;
/// parity with `FilesystemConfigSource`, which never surfaces a bare `Store`/`Serialization` on
/// a read).
fn cfg_load_err(e: sqlx::Error) -> OrchestratorError {
    OrchestratorError::RegistryLoad(format!("postgres config load: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{
        AgentDefinition, ChildStatus, CompactChild, ContentRef, ContentStore, ContextKey,
        ContextRef, ContextStore, Digest, EffectClass, ExecutionJournal, JournalError,
        JournalEvent, NetworkPolicy, NodeId, OrchestratorError, Permissions, RunId, Scope,
        SkillDef, Snapshot, ToolSpec,
    };
    use std::collections::HashMap;

    /// Tests require a live PG at $DATABASE_URL with the dbd schema applied (the Docker harness).
    /// Absent DATABASE_URL, they skip (so a bare `cargo test --features postgres` without a DB
    /// doesn't fail spuriously).
    fn db_url() -> Option<String> {
        std::env::var("DATABASE_URL").ok()
    }

    /// A fresh, unique run id — every test gets its own so the shared `orchestrator.*`
    /// tables never collide across tests (belt-and-suspenders with `--test-threads=1`).
    fn run() -> RunId {
        RunId(uuid::Uuid::new_v4())
    }

    fn started() -> JournalEvent {
        JournalEvent::RunStarted {
            version: "v1".into(),
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
}
