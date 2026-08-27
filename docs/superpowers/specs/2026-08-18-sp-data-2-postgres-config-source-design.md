---
title: SP-DATA-2 — PostgresConfigSource + durable config-version fence
doctype: design-spec
module: orchestrator
slice: SP-DATA-2
status: approved
date: 2026-08-18
---

# SP-DATA-2 — PostgresConfigSource + durable config-version fence

## 1. Summary

Move the registry config (agents / skills / tools / chain-bindings) into Postgres,
and back the run's config-generation fence with a **durable, global** version so a
run that started in one process resumes correctly in another. This closes a fence
hole that only became reachable once SP-DATA-1 made runs resume **cross-process**.

The deliverable is the **second** `ConfigSource` backend (after `FilesystemConfigSource`)
plus a single-row `config_versions` counter that makes the `#cfg{gen}` suffix of a
run's fence version mean the same thing in every process on the same database.

## 2. Motivation — the hole SP-DATA-1 exposed

A run's determinism fence version is `"{base}#cfg{gen}"`:

- `.pinned(registry, generation)` sets `self.version = format!("{}#cfg{}", self.version, generation)`
  (`crates/orchestrator/src/executor/mod.rs:369-370`).
- `run`/`start` read `(registry, generation)` from `RegistryHandle::snapshot()` and pin them
  at entry (`mod.rs:392-395`, `435-438`).
- On resume, `start_inner` compares the journaled `RunStarted.version` against the
  reconstructed `this.version`; a difference is a loud `VersionFenceMismatch { recorded, current }`
  (`mod.rs:462-468`) — "never resume a run authored under a different config generation."

`{gen}` comes from `RegistryHandle`'s **in-process** counter: `Arc<RwLock<(Arc<Registry>, u64)>>`
starting at 0, `reload()` doing `w.1 += 1` (`crates/orchestrator-core/src/registry.rs:442-486`).
That counter is **process-local and content-blind**, so across two processes on the same DB it
is meaningless in both directions:

- **False negative (silent mis-resume):** process A at `#cfg3` and process B at `#cfg3` can hold
  *different* config content — the fence passes and the run resumes under changed config.
- **False positive (spurious refuse):** process A at `#cfg3` (after three reloads) and a fresh
  process B at `#cfg0` hold *identical* content — the fence wrongly refuses a legitimate resume.

SP-DATA-1 gave the journal/CAS/context durable cross-process resume; the config fence must get the
same treatment or the "resume in a fresh process" guarantee has a hole. SP-DATA-2 makes `{gen}` a
durable global generation read from `config_versions`, so two processes agree on the generation for
a given config state.

## 3. Goals / Non-goals

**Goals**

- A `PostgresConfigSource` implementing `ConfigSource` (`load()` reads config from Postgres).
- A durable, single-row `config_versions` global generation + an atomic `bump_config_version`.
- Wire the durable generation into `RegistryHandle` so the run fence (`#cfg{gen}`) is
  cross-process meaningful: an unchanged-config cross-process resume passes; a bumped-config
  cross-process resume refuses loudly.
- Additive: default-off (no Postgres source) ⇒ byte-identical to today (the full workspace suite
  stays at its current count).

**Non-goals (deferred, tracked in §9)**

- A background poller / `LISTEN`/`NOTIFY` (this slice is **on-demand reload only**).
- Per-component sub-versioning (torii's `components` jsonb) — one global counter suffices.
- Granular per-entity config editing + a SQL-callable `bump` function (SP-DATA-4 management surface).
- Multi-tenant config scoping (the core is tenant-agnostic; tenancy is a wrapper).

## 4. Schema (dbd, `gateway/database/`, `orchestrator` schema)

Follows the SP-DATA-1 dbd convention (torii-style layout under `ddl/table/orchestrator/`, idempotent
`CREATE … IF NOT EXISTS`, jsonb for structured payloads). Complex entities are stored as **jsonb rows**
because the registry is always loaded whole and validated in-process (`Registry::from_config`) — relational
decomposition of permissions/grants/activation/credentials would buy nothing and cost a large mapper.

```sql
-- ddl/table/orchestrator/config_agents.sql
create table if not exists orchestrator.config_agents (
    name        text        primary key,
    def         jsonb       not null,          -- serde(AgentDefinition)
    updated_at  timestamptz not null default now()
);

-- ddl/table/orchestrator/config_skills.sql
create table if not exists orchestrator.config_skills (
    name        text        primary key,
    def         jsonb       not null,          -- serde(SkillDef)
    updated_at  timestamptz not null default now()
);

-- ddl/table/orchestrator/config_tools.sql
create table if not exists orchestrator.config_tools (
    name        text        primary key,
    spec        jsonb       not null,          -- serde(ToolSpec)
    updated_at  timestamptz not null default now()
);

-- ddl/table/orchestrator/config_chain_bindings.sql
-- The one entity with no nested structure ⇒ a natural relational row (nothing to serialize).
create table if not exists orchestrator.config_chain_bindings (
    area        text        not null,
    kind        text        not null,
    chain       text        not null,
    updated_at  timestamptz not null default now(),
    primary key (area, kind)
);

-- ddl/table/orchestrator/config_versions.sql
-- Single-row global generation. The `id boolean primary key check(id)` idiom pins exactly one row.
create table if not exists orchestrator.config_versions (
    id          boolean     primary key default true check (id),
    version     bigint      not null default 1,
    updated_at  timestamptz not null default now()
);
```

- `_apply_all.sql` is extended to include the five new tables (kept out of `ddl/` so `dbd inspect`
  stays zero-noise, per SP-DATA-1).
- **`components` jsonb is deliberately dropped** vs torii: its per-component delta-sync serves a device
  fleet; we load-whole with one global counter (YAGNI).
- The `dbd-pattern-verifier` agent re-reviews `gateway/database/` in the whole-slice review (as SP-DATA-1).

## 5. `bump_config_version` — app-side atomic upsert (not a DB function)

The increment is a **single statement**, so it is atomic without a stored function:

```sql
insert into orchestrator.config_versions (id, version) values (true, 1)
on conflict (id) do update set version = orchestrator.config_versions.version + 1,
                               updated_at = now()
returning version;   -- the new global generation
```

torii uses a `CREATE FUNCTION` because its SQL-layer RPCs call `bump` directly; our only caller is Rust,
so a one-statement upsert is equally atomic and keeps the schema **tables-only** (matching SP-DATA-1). A
SQL-callable function is a SP-DATA-4 nicety if a non-Rust caller ever needs it.

## 6. Trait extension (additive, default-preserving)

`ConfigSource` gains one defaulted method so existing impls compile unchanged:

```rust
#[async_trait::async_trait]
pub trait ConfigSource: Send + Sync {
    async fn load(&self) -> Result<RegistryConfig, OrchestratorError>;

    /// The durable config generation, if this source is versioned.
    /// Default `None` ⇒ the handle keeps its local monotonic counter
    /// (filesystem / in-memory are unversioned).
    async fn version(&self) -> Result<Option<u64>, OrchestratorError> {
        Ok(None)
    }
}
```

`RegistryHandle` uses the durable version when the source provides one:

```rust
pub async fn reload(&self, source: &dyn ConfigSource) -> Result<u64, OrchestratorError> {
    let cfg = source.load().await?;
    let ver = source.version().await?;                 // NEW
    let next = Registry::from_config(cfg)?;
    let mut w = self.inner.write().unwrap_or_else(|e| e.into_inner());
    w.0 = Arc::new(next);
    w.1 = match ver { Some(v) => v, None => w.1 + 1 };  // durable version OR local increment
    Ok(w.1)
}

/// Boot a handle at a source's durable generation (filesystem ⇒ 0, unchanged).
pub async fn from_source(source: &dyn ConfigSource) -> Result<Self, OrchestratorError> {
    let cfg = source.load().await?;
    let ver = source.version().await?.unwrap_or(0);
    let registry = Registry::from_config(cfg)?;
    Ok(Self { inner: Arc::new(RwLock::new((Arc::new(registry), ver))) })
}
```

- **Filesystem / in-memory:** `version()` returns `None` ⇒ `reload` still does `w.1 + 1`, `from_source`
  boots at 0 — behavior identical to today.
- **Postgres:** `version()` **always returns `Some(n)`** (a versioned source; absent row ⇒ `Some(0)`),
  so the handle's generation equals the durable version. `None` is reserved for genuinely-unversioned
  sources.

## 7. PostgresConfigSource (`orchestrator-store`, `postgres` feature)

Lives beside SP-DATA-1's Postgres backends (same feature, `sqlx 0.8` runtime queries — no compile-time DB).

```rust
pub struct PostgresConfigSource { pool: sqlx::PgPool }

impl PostgresConfigSource {
    pub fn new(pool: sqlx::PgPool) -> Self;

    /// Replace-all write of the whole registry in one transaction: delete the four
    /// config tables' rows, then insert `cfg`'s — so `load()` afterward reproduces
    /// `cfg` exactly (removed entities do not linger). Does NOT bump the version
    /// (the caller bumps explicitly after committing a change). The entry point
    /// tests use to seed; SP-DATA-4's CLI grows granular edits on top.
    pub async fn store(&self, cfg: &RegistryConfig) -> Result<(), OrchestratorError>;

    /// Atomic upsert-increment; returns the new global generation.
    pub async fn bump_config_version(&self) -> Result<u64, OrchestratorError>;
}

#[async_trait::async_trait]
impl ConfigSource for PostgresConfigSource {
    async fn load(&self) -> Result<RegistryConfig, OrchestratorError>;      // read 4 tables → RegistryConfig
    async fn version(&self) -> Result<Option<u64>, OrchestratorError>;      // Some(config_versions.version), absent → Some(0)
}
```

- **`load()`** reads the four config tables and deserializes the jsonb payloads into
  `AgentDefinition`/`SkillDef`/`ToolSpec` plus the `(area,kind,chain)` rows into `ChainBinding`,
  assembling a `RegistryConfig`. A read/deser failure → `OrchestratorError::RegistryLoad` naming the
  entity (mirrors `FilesystemConfigSource`, which names the file). Ordering is stable (sorted by name)
  so `load()` is deterministic.
- **`store()`** upserts all rows inside a transaction; a partial write never leaves a half-applied config.
- **Write/version transport failures** (`store`, `bump_config_version`, `version`) → `OrchestratorError::Store`
  (SP-DATA-1's durable-store variant); load-path failures → `RegistryLoad` (ConfigSource-trait convention).
  Both loud, never swallowed.

## 8. Acceptance criteria

- **AC1 — schema:** the five tables apply cleanly and idempotently (re-apply is a no-op); `dbd inspect`
  stays zero-noise; the dbd-pattern-verifier passes.
- **AC2 — round-trip:** `store(cfg)` then `load()` reproduces `cfg` losslessly for all four entity kinds
  (jsonb round-trips `AgentDefinition`/`SkillDef`/`ToolSpec`, including nested permissions/grants/activation/
  credentials; the `(area,kind,chain)` triple round-trips).
- **AC3 — version:** `version()` on an empty DB returns `Some(0)`; `bump_config_version()` returns a strictly
  increasing sequence (1, 2, 3…) and is reflected by a subsequent `version()`.
- **AC4 — durable generation into the fence:** `RegistryHandle::from_source(&pg)` boots at the durable
  version; `reload(&pg)` sets the generation to the current durable version (not a blind `+1`).
- **AC5 — cross-process resume, unchanged config (PASS):** the config is seeded (`store(cfg)` + one
  `bump_config_version()` → v=1); process A boots a handle from a Postgres source (v=1), runs R end-to-end so
  the journal records `RunStarted.version = "{base}#cfg1"`; a **fresh** process B
  (new Executor + new `PostgresConfigSource`/handle on the same `DATABASE_URL`, config unchanged, v=1) resumes
  R → the fence matches → the completed prefix replays **from the memo with zero re-spend** (`calls_b == 1`,
  mirroring SP-DATA-1's e2e).
- **AC6 — cross-process resume, bumped config (LOUD REFUSE):** A runs R at v=1; then `store(new) + bump → v=2`;
  a fresh process B boots at v=2 and resumes R → `OrchestratorError::VersionFenceMismatch { recorded: "…#cfg1",
  current: "…#cfg2" }` — never a silent mis-resume under changed config.
- **AC7 — mutation-check (version() is load-bearing):** forcing Postgres `version()` to return `None` makes B
  fall back to the local counter, so AC6's mismatch no longer fires deterministically — proving the durable
  version is what carries the fence.
- **AC8 — additivity:** with no Postgres source wired, `version()` is `None`, the local counter is used, and
  `cargo test --workspace` (feature-off) is **byte-identical** to today (same count). The defaulted trait
  method leaves `FilesystemConfigSource`/`InMemoryConfigSource` unchanged.
- **AC9 — Docker verification:** the store-crate unit suite (`--features postgres`) and the e2e
  (`--features postgres-tests`) run green against `postgres:16` in Docker (dev box is macOS), with real
  (unpiped) exit codes.

## 9. Deferred / carry-forward

- **Live propagation:** a background poller (torii's `since`-compare) or `LISTEN`/`NOTIFY` push, so a running
  process auto-reloads when the durable version advances. On-demand `reload` + the fence already make a stale
  resume loud; live propagation is an operational nicety (SP-DATA-3 scheduler / SP-DATA-4 control-plane).
- **Per-component sub-versioning** (`components` jsonb) if delta-sync ever matters.
- **Granular per-entity edits** (`put_agent`/`delete_tool`/…) + a **SQL-callable `bump` function** — the
  SP-DATA-4 management CLI/API surface (`store` replace-all is this slice's only writer).
- **Multi-tenant config scoping** — a wrapper concern; the core stays tenant-agnostic.
- **Config-content redaction / secret handling in stored config** — config secrets remain a broker concern
  (SP-4); this slice stores config as authored.

## 10. Files touched

- `gateway/database/ddl/table/orchestrator/{config_agents,config_skills,config_tools,config_chain_bindings,config_versions}.sql` (new); `gateway/database/_apply_all.sql` (extend).
- `crates/orchestrator-core/src/registry.rs`: `ConfigSource::version` (defaulted), `RegistryHandle::reload` (use `version`), `RegistryHandle::from_source` (new).
- `crates/orchestrator-store/src/postgres.rs` (or a sibling `config_postgres.rs`): `PostgresConfigSource` + `store`/`bump_config_version`/`load`/`version`.
- `crates/orchestrator/src/executor/tests.rs`: the `#[cfg(feature = "postgres-tests")]` cross-process fence e2e (AC5/AC6/AC7).
- No executor/core control-flow change beyond the additive trait method + the `reload`/`from_source` wiring — the fence machinery (`pinned`, `VersionFenceMismatch`) is unchanged; SP-DATA-2 only makes `{gen}` durable.
