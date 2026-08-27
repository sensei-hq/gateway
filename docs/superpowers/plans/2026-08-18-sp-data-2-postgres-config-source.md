# SP-DATA-2 PostgresConfigSource + Durable Config-Version Fence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the registry config into Postgres and back the run's `#cfg{gen}` fence with a durable, global generation, so a run started in one process resumes correctly in another — an unchanged-config cross-process resume passes (zero re-spend), a bumped-config resume refuses loudly.

**Architecture:** A second `ConfigSource` backend (`PostgresConfigSource` in `orchestrator-store`, `postgres` feature, sqlx 0.8 **runtime** queries) reading config from jsonb-row tables in the SP-DATA-1 dbd schema, plus a single-row `config_versions` counter. `ConfigSource` gains a defaulted `version()`; `RegistryHandle::reload`/`from_source` pin the durable version as the generation when the source provides one (filesystem/in-memory unchanged). The fence machinery (`.pinned` → `"{base}#cfg{gen}"`, `VersionFenceMismatch`) is UNCHANGED — SP-DATA-2 only makes `{gen}` durable. Verified on Docker Postgres; feature-off ⇒ byte-identical.

**Tech Stack:** Rust, sqlx 0.8 (postgres, **runtime** `sqlx::query`/`query_as` — NOT `query!`), dbd (schema, `gateway/database/`), Docker Postgres.

**Spec:** `docs/superpowers/specs/2026-08-18-sp-data-2-postgres-config-source-design.md`

**Baseline:** `develop` at `990ca8a`; full workspace **1120 tests** green (macOS). `cargo fmt --all` before every commit (pre-commit = fmt-check + workspace `clippy -D warnings`, NO tests → always run tests yourself, real unpiped exit code). Do NOT push (the coordinator pushes after the whole-slice review).

**Key existing seams (read before starting):**
- `crates/orchestrator-core/src/registry.rs`: `RegistryConfig { agents, skills, tools, chain_bindings }` (`:240`), `ConfigSource` trait (`:253`, one `async fn load`), `RegistryHandle` (`:441`, `inner: Arc<RwLock<(Arc<Registry>, u64)>>`; `reload` at `:479` does `w.1 += 1`), `ChainBinding { area, kind, chain }` (`:230`). `AgentDefinition`/`SkillDef`/`ToolSpec` all derive `Serialize`/`Deserialize` ⇒ jsonb round-trips.
- `crates/orchestrator-store/src/config_source.rs`: `FilesystemConfigSource` + `InMemoryConfigSource(pub RegistryConfig)`; the `OrchestratorError::RegistryLoad(format!("parse agent {file}: {e}"))` naming convention (`:108`).
- `crates/orchestrator-store/src/postgres.rs` (SP-DATA-1): `connect()`, the PG adapters, and error mappers — reuse `store_err` (→ `OrchestratorError::Store`) for write/version transport.
- `crates/orchestrator/src/executor/mod.rs`: `Executor::with_registry_handle` (`:286`); `.pinned` folds `#cfg{gen}` (`:369`); the fence compares `RunStarted.version` on resume (`:462`).
- **The in-process fence test to mirror**, `crates/orchestrator/src/executor/tests.rs:6427` — runs at gen 0 (`RunStarted.version == "v1#cfg0"`), reloads to gen 1, `.start` → `VersionFenceMismatch { recorded: "v1#cfg0", current: "v1#cfg1" }`. SP-DATA-2's e2e is the **cross-process** form of this.
- **The SP-DATA-1 PG e2e to mirror**, `tests.rs:11770` (`postgres_cross_process_resume_replays_from_the_durable_journal`) — `two_node_graph("a","b")`, `failing_after_gateway(1)` (process A partial), `recording_gateway()` + `calls_b` (process B, zero-respend proof), `PostgresJournal`/`PostgresContentStore` on the same `DATABASE_URL`.

**DOCKER POSTGRES HARNESS (reused in Tasks 3–4):**
```bash
docker rm -f spdata-pg >/dev/null 2>&1
docker run -d --name spdata-pg -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=orch -p 55432:5432 postgres:16 >/dev/null
until docker exec spdata-pg pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done
export DATABASE_URL="postgres://postgres:pw@localhost:55432/orch"
docker exec -i spdata-pg psql -U postgres -d orch -v ON_ERROR_STOP=1 < database/_apply_all.sql
cargo test -p sensei-orchestrator-store --features postgres -- --test-threads=1 ; echo "STORE_PG_EXIT=$?"
cargo test -p sensei-orchestrator --features postgres-tests -- --test-threads=1 postgres_ ; echo "E2E_PG_EXIT=$?"
docker rm -f spdata-pg >/dev/null 2>&1
```
- Read the REAL exit codes. Every test uses a UNIQUE `run_id`/config so shared `orchestrator.*` tables never collide (`--test-threads=1` is the safety net). Tests `return` early when `DATABASE_URL` is unset (default suite is DB-free + byte-identical).

---

## File Structure

- **Create** `gateway/database/ddl/table/orchestrator/{config_agents,config_skills,config_tools,config_chain_bindings,config_versions}.sql` — the five config tables. **Modify** `gateway/database/_apply_all.sql` — include them.
- **Modify** `crates/orchestrator-core/src/registry.rs` — `ConfigSource::version` (defaulted); `RegistryHandle::reload` (use `version`); `RegistryHandle::from_source` (new); tests.
- **Modify** `crates/orchestrator-store/src/postgres.rs` — `PostgresConfigSource` (`new`/`load`/`version`/`store`/`bump_config_version`) + a `cfg_load_err` mapper; tests.
- **Modify** `crates/orchestrator/src/executor/tests.rs` — the gated cross-process fence e2e in the existing `postgres_e2e` module.

No `Cargo.toml` change is needed: the store crate's `postgres = ["dep:sqlx"]` (sqlx has `uuid`) and the orchestrator crate's `postgres-tests = ["orchestrator-store/postgres"]` already exist (SP-DATA-1).

---

## Task 1: The dbd config schema (`gateway/database/`)

**Files:** Create `gateway/database/ddl/table/orchestrator/{config_agents,config_skills,config_tools,config_chain_bindings,config_versions}.sql`; modify `gateway/database/_apply_all.sql`.

- [ ] **Step 1: Author the five table DDL files (idempotent, jsonb rows)**

`database/ddl/table/orchestrator/config_agents.sql`:
```sql
create table if not exists orchestrator.config_agents (
    name       text        primary key,
    def        jsonb       not null,
    updated_at timestamptz not null default now()
);
```

`database/ddl/table/orchestrator/config_skills.sql`:
```sql
create table if not exists orchestrator.config_skills (
    name       text        primary key,
    def        jsonb       not null,
    updated_at timestamptz not null default now()
);
```

`database/ddl/table/orchestrator/config_tools.sql`:
```sql
create table if not exists orchestrator.config_tools (
    name       text        primary key,
    spec       jsonb       not null,
    updated_at timestamptz not null default now()
);
```

`database/ddl/table/orchestrator/config_chain_bindings.sql` (the one entity with no nested structure ⇒ a natural relational row):
```sql
create table if not exists orchestrator.config_chain_bindings (
    area       text        not null,
    kind       text        not null,
    chain      text        not null,
    updated_at timestamptz not null default now(),
    primary key (area, kind)
);
```

`database/ddl/table/orchestrator/config_versions.sql` (single-row global generation; the `id boolean` idiom pins exactly one row):
```sql
create table if not exists orchestrator.config_versions (
    id         boolean     primary key default true check (id),
    version    bigint      not null default 1,
    updated_at timestamptz not null default now()
);
```

- [ ] **Step 2: Extend `_apply_all.sql`**

Append the five new tables to `gateway/database/_apply_all.sql` (after the SP-DATA-1 tables, before/after order among config tables is irrelevant — no FKs). If `_apply_all.sql` inlines table bodies (SP-DATA-1 relocated it out of `ddl/` and inlined the contents so `docker exec … <` piping works), inline these five bodies too; if it uses `\i` includes, add five `\i database/ddl/table/orchestrator/config_*.sql` lines. Match the existing file's style exactly (open it first).

- [ ] **Step 3: Apply to a Docker Postgres + verify idempotent**

Start the Docker PG (harness top), apply `_apply_all.sql`, then:
```bash
docker exec spdata-pg psql -U postgres -d orch -c "\dt orchestrator.config_*"   # 5 config tables present
docker exec spdata-pg psql -U postgres -d orch -c "select version from orchestrator.config_versions limit 0"  # bigint col ok
# Re-apply → idempotent (all 'already exists, skipping', exit 0):
docker exec -i spdata-pg psql -U postgres -d orch -v ON_ERROR_STOP=1 < database/_apply_all.sql ; echo "REAPPLY_EXIT=$?"
```
Expected: the five `orchestrator.config_*` tables exist; `REAPPLY_EXIT=0`. Tear down. (The `dbd-pattern-verifier` agent re-reviews `gateway/database/` in the whole-slice review.)

- [ ] **Step 4: Commit**

```bash
cd /Users/Jerry/Developer/gateway
git add gateway/database/
git commit -m "feat(orchestrator): SP-DATA-2 (1/4) — dbd config schema (agents/skills/tools/chain_bindings/config_versions)"
```

---

## Task 2: `ConfigSource::version()` + `RegistryHandle::reload`/`from_source` wiring (core, no DB)

**Files:** Modify `crates/orchestrator-core/src/registry.rs` (trait method, handle methods, tests).

- [ ] **Step 1: Write the failing tests**

Add to `registry.rs`'s `#[cfg(test)] mod tests`. There is already a `FixedSource(RegistryConfig)` helper (used by the existing reload tests) that impls `ConfigSource::load` and — because it does NOT override `version()` — must default to `None`. Add a versioned helper next to it and three tests:
```rust
    /// A ConfigSource that reports a durable generation (mirrors PostgresConfigSource).
    struct VersionedSource(RegistryConfig, u64);
    #[async_trait::async_trait]
    impl ConfigSource for VersionedSource {
        async fn load(&self) -> Result<RegistryConfig, OrchestratorError> {
            Ok(self.0.clone())
        }
        async fn version(&self) -> Result<Option<u64>, OrchestratorError> {
            Ok(Some(self.1))
        }
    }

    #[tokio::test]
    async fn reload_from_a_versioned_source_pins_the_durable_version_not_a_blind_increment() {
        let h = RegistryHandle::new(Registry::from_config(cfg_with_skill("s0")).unwrap());
        assert_eq!(h.generation(), 0);
        // A durable source at version 5 → the generation becomes 5 (NOT 0+1).
        let g = h.reload(&VersionedSource(cfg_with_skill("s1"), 5)).await.unwrap();
        assert_eq!(g, 5, "the durable version is pinned as the generation");
        assert_eq!(h.generation(), 5);
        assert!(h.current().skill("s1").is_some(), "new config is live");
    }

    #[tokio::test]
    async fn reload_from_an_unversioned_source_keeps_the_local_increment() {
        // FixedSource does not override version() → None → the existing +1 behavior (byte-identical).
        let h = RegistryHandle::new(Registry::from_config(cfg_with_skill("s0")).unwrap());
        assert_eq!(h.reload(&FixedSource(cfg_with_skill("s1"))).await.unwrap(), 1);
        assert_eq!(h.reload(&FixedSource(cfg_with_skill("s2"))).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn from_source_boots_at_the_durable_version_and_unversioned_at_zero() {
        // Versioned source → boot at its version.
        let h = RegistryHandle::from_source(&VersionedSource(cfg_with_skill("s0"), 7))
            .await
            .unwrap();
        assert_eq!(h.generation(), 7);
        assert!(h.current().skill("s0").is_some());
        // Unversioned source → boot at 0 (identical to `new`).
        let h0 = RegistryHandle::from_source(&FixedSource(cfg_with_skill("s0")))
            .await
            .unwrap();
        assert_eq!(h0.generation(), 0);
    }
```
(If `cfg_with_skill` / `FixedSource` are named differently in the test module, use the actual names — grep the `mod tests` in `registry.rs`.)

- [ ] **Step 2: Verify they fail**

Run: `cargo test -p sensei-orchestrator-core version` and `... from_source`
Expected: FAIL — `no method named version` on the trait / `no function from_source`. Report.

- [ ] **Step 3: Add the defaulted trait method**

In `registry.rs`, extend the `ConfigSource` trait (`:253`):
```rust
#[async_trait::async_trait]
pub trait ConfigSource: Send + Sync {
    /// Load the whole registry config (a one-shot snapshot; hot-reload re-calls it).
    async fn load(&self) -> Result<RegistryConfig, OrchestratorError>;

    /// The durable config generation, if this source is versioned. Default `None`
    /// ⇒ [`RegistryHandle`] keeps its local monotonic counter (filesystem / in-memory
    /// are unversioned). A versioned backend (`PostgresConfigSource`) returns
    /// `Some(n)` so the generation is globally meaningful across processes.
    async fn version(&self) -> Result<Option<u64>, OrchestratorError> {
        Ok(None)
    }
}
```

- [ ] **Step 4: Wire `reload` + add `from_source`**

Replace `RegistryHandle::reload`'s generation bump and add `from_source` (in the same `impl RegistryHandle`):
```rust
    pub async fn reload(&self, source: &dyn ConfigSource) -> Result<u64, OrchestratorError> {
        let cfg = source.load().await?;
        let ver = source.version().await?;
        let next = Registry::from_config(cfg)?;
        let mut w = self.inner.write().unwrap_or_else(|e| e.into_inner());
        w.0 = Arc::new(next);
        // A versioned source pins its durable generation; an unversioned one increments locally.
        w.1 = match ver {
            Some(v) => v,
            None => w.1 + 1,
        };
        Ok(w.1)
    }

    /// Boot a handle at a source's durable generation. A versioned source pins its
    /// version; an unversioned source (filesystem/in-memory) boots at 0 — identical
    /// to [`new`](Self::new). The `load`/`version`/`from_config` run before the handle
    /// exists, so a failed load/validate returns `Err` (no half-built handle).
    pub async fn from_source(source: &dyn ConfigSource) -> Result<Self, OrchestratorError> {
        let cfg = source.load().await?;
        let ver = source.version().await?.unwrap_or(0);
        let registry = Registry::from_config(cfg)?;
        Ok(Self {
            inner: Arc::new(RwLock::new((Arc::new(registry), ver))),
        })
    }
```

- [ ] **Step 5: Verify they pass + the existing reload tests still pass**

Run: `cargo test -p sensei-orchestrator-core`
Expected: the three new tests PASS; `registry_handle_reload_swaps_and_bumps_generation` and `registry_handle_reload_is_validated_and_last_good` still PASS (the `None` path is unchanged). `cargo clippy -p sensei-orchestrator-core --all-targets -- -D warnings` clean.

- [ ] **Step 6: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator-core/src/registry.rs
git commit -m "feat(orchestrator): SP-DATA-2 (2/4) — ConfigSource::version() + RegistryHandle durable-generation wiring"
```

---

## Task 3: `PostgresConfigSource` (load / version / store / bump)

**Files:** Modify `crates/orchestrator-store/src/postgres.rs`.

- [ ] **Step 1: Write the failing tests (Docker-PG; unique names per test)**

Add to `postgres.rs`'s `#[cfg(test)] mod tests` (reuse its `db_url()` helper). Build config with the real domain constructors (grep `AgentDefinition {` in `registry.rs` for the full field list; use minimal valid values). To keep tests isolated on shared tables, use per-test unique entity names (a uuid suffix) and read them back by name.
```rust
    use orchestrator_core::{ChainBinding, ConfigSource, RegistryConfig, SkillDef};

    fn uniq(p: &str) -> String { format!("{p}-{}", uuid::Uuid::new_v4()) }

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
        let cfg = RegistryConfig { agents: vec![], skills: vec![skill(&s)], tools: vec![], chain_bindings: vec![] };
        src.store(&cfg).await.unwrap();
        let got = src.load().await.unwrap();
        assert!(got.skills.iter().any(|k| k.name == s), "the stored skill round-trips via jsonb");
    }

    #[tokio::test]
    async fn version_is_zero_on_empty_then_monotonic_under_bump() {
        let Some(url) = db_url() else { return };
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());
        // NOTE: config_versions is a single shared row; do not assert an absolute start value across
        // a shared DB — assert STRICT INCREASE instead (robust under --test-threads=1 + shared row).
        let v0 = src.version().await.unwrap().expect("a versioned source always returns Some");
        let v1 = src.bump_config_version().await.unwrap();
        assert!(v1 > v0, "bump strictly increases ({v0} -> {v1})");
        assert_eq!(src.version().await.unwrap(), Some(v1), "version() reflects the bump");
    }

    #[tokio::test]
    async fn store_is_replace_all_removed_entities_do_not_linger() {
        let Some(url) = db_url() else { return };
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());
        let keep = uniq("keep");
        let drop = uniq("drop");
        src.store(&RegistryConfig { agents: vec![], skills: vec![skill(&keep), skill(&drop)], tools: vec![], chain_bindings: vec![] }).await.unwrap();
        src.store(&RegistryConfig { agents: vec![], skills: vec![skill(&keep)], tools: vec![], chain_bindings: vec![] }).await.unwrap();
        let got = src.load().await.unwrap();
        assert!(got.skills.iter().any(|k| k.name == keep));
        assert!(!got.skills.iter().any(|k| k.name == drop), "replace-all dropped the removed skill");
    }

    #[tokio::test]
    async fn chain_bindings_round_trip_as_a_relational_row() {
        let Some(url) = db_url() else { return };
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());
        let area = uniq("area");
        src.store(&RegistryConfig {
            agents: vec![], skills: vec![], tools: vec![],
            chain_bindings: vec![ChainBinding { area: area.clone(), kind: "plan".into(), chain: "c".into() }],
        }).await.unwrap();
        let got = src.load().await.unwrap();
        assert!(got.chain_bindings.iter().any(|b| b.area == area && b.kind == "plan" && b.chain == "c"));
    }
```
(Adjust `skill(...)`/the config constructors to the REAL field lists — grep `struct SkillDef`/`struct AgentDefinition`/`struct ToolSpec` in `registry.rs`. Keep every entity name unique so the shared tables never collide.)

- [ ] **Step 2: Verify they fail (Docker-PG)** — `cannot find PostgresConfigSource`. Report.

- [ ] **Step 3: Implement `PostgresConfigSource`**

Add to `postgres.rs`. Reuse the file's existing `store_err` (→ `OrchestratorError::Store`) for write/version transport; add a `cfg_load_err` (→ `RegistryLoad`) for the load path (trait convention, matching `FilesystemConfigSource`).
```rust
use orchestrator_core::{
    ChainBinding, ConfigSource, OrchestratorError, RegistryConfig,
};

/// A durable `ConfigSource` (SP-DATA-2): the registry config lives in the `orchestrator.config_*`
/// tables as jsonb rows, with a single-row `config_versions` global generation. `load()` reads the
/// whole registry; `version()` reports the durable generation so a run's `#cfg{gen}` fence is
/// cross-process meaningful. `store`/`bump_config_version` are the write path (this slice's seeder +
/// SP-DATA-4's CLI entry point).
pub struct PostgresConfigSource {
    pool: PgPool,
}

impl PostgresConfigSource {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Replace-all write of the whole registry in one transaction: delete every config row, then
    /// insert `cfg`'s — so `load()` afterward reproduces `cfg` exactly. Does NOT bump the version
    /// (the caller bumps explicitly after committing a change).
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
            sqlx::query("insert into orchestrator.config_agents (name, def) values ($1,$2)")
                .bind(&a.name)
                .bind(v)
                .execute(&mut *tx)
                .await
                .map_err(store_err)?;
        }
        for s in &cfg.skills {
            let v = serde_json::to_value(s).map_err(store_err_ser)?;
            sqlx::query("insert into orchestrator.config_skills (name, def) values ($1,$2)")
                .bind(&s.name)
                .bind(v)
                .execute(&mut *tx)
                .await
                .map_err(store_err)?;
        }
        for t in &cfg.tools {
            let v = serde_json::to_value(t).map_err(store_err_ser)?;
            sqlx::query("insert into orchestrator.config_tools (name, spec) values ($1,$2)")
                .bind(&t.name)
                .bind(v)
                .execute(&mut *tx)
                .await
                .map_err(store_err)?;
        }
        for b in &cfg.chain_bindings {
            sqlx::query(
                "insert into orchestrator.config_chain_bindings (area, kind, chain) values ($1,$2,$3)",
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

/// A config LOAD-path transport/parse failure → `RegistryLoad` (the ConfigSource convention,
/// matching `FilesystemConfigSource`).
fn cfg_load_err(e: sqlx::Error) -> OrchestratorError {
    OrchestratorError::RegistryLoad(format!("postgres config load: {e}"))
}
```
NOTE: if the existing `store_err` takes `sqlx::Error` only, add a tiny `store_err_ser(e: serde_json::Error) -> OrchestratorError { OrchestratorError::Store(e.to_string()) }` (or reuse whatever serde→Store mapper SP-DATA-1 added). If `PgPool` isn't already imported at module scope, it is (SP-DATA-1's adapters use it). If `postgres.rs` has grown unwieldy, splitting `PostgresConfigSource` into a sibling `config_postgres.rs` (`#[cfg(feature="postgres")] mod config_postgres;`) is acceptable — but same-file is fine and matches SP-DATA-1.

- [ ] **Step 4: Verify (Docker-PG) the tests pass + feature-off byte-identical**

Docker-PG harness → `store_then_load_round_trips_config`, `version_is_zero_on_empty_then_monotonic_under_bump`, `store_is_replace_all_…`, `chain_bindings_round_trip_…` all PASS. Then:
```bash
cargo clippy -p sensei-orchestrator-store --features postgres --all-targets -- -D warnings ; echo "CLIPPY_PG=$?"
cargo test -p sensei-orchestrator-store ; echo "STORE_DEFAULT=$?"   # feature-off, InMemory tests unchanged
```
Expected: `CLIPPY_PG=0`, `STORE_DEFAULT=0`.

- [ ] **Step 5: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator-store/src/postgres.rs
git commit -m "feat(orchestrator): SP-DATA-2 (3/4) — PostgresConfigSource (load/version/store/bump)"
```

---

## Task 4: The headline — cross-process config fence e2e + additivity gate

**Files:** Modify `crates/orchestrator/src/executor/tests.rs` (add to the existing `#[cfg(feature = "postgres-tests")] mod postgres_e2e`).

- [ ] **Step 1: Write the two failing e2e tests (`postgres_e2e` module)**

Add these to `postgres_e2e` (import `PostgresConfigSource` + `RegistryHandle`, `RegistryConfig`). They mirror the in-process fence test at `tests.rs:6427` and the SP-DATA-1 PG harness at `tests.rs:11770`. An **empty** `RegistryConfig` is valid (`Registry::from_config` yields an empty registry) and suffices for the linear `two_node_graph` (no agent refs).
```rust
    use orchestrator_store::postgres::PostgresConfigSource;
    use orchestrator_core::{RegistryConfig, RegistryHandle};

    /// AC5 — unchanged config, cross-process resume PASSES with zero re-spend. Process A runs a
    /// partial with a handle booted from the durable config version (v=1) → `RunStarted.version =
    /// "v1#cfg1"`. A FRESH process B boots a fresh handle from the SAME source (config unchanged, still
    /// v=1) → gen 1 → the fence matches → the completed prefix replays from the durable memo (the
    /// fresh gateway is called only for the tail). The cross-process form of `tests.rs:6427`.
    #[tokio::test]
    async fn postgres_unchanged_config_generation_permits_cross_process_resume() {
        let Some(url) = db_url() else { return };
        let run = RunId(uuid::Uuid::new_v4());
        let (graph, n1, n2) = two_node_graph("a", "b");

        // Seed durable config + move to a known generation (v=1).
        let cfg_src = PostgresConfigSource::new(connect(&url).await.unwrap());
        cfg_src.store(&RegistryConfig::default()).await.unwrap();
        let v = cfg_src.bump_config_version().await.unwrap(); // v >= 1

        // Process A: partial run (n1 ok, n2 crashes), handle pinned at the durable version.
        let handle_a = RegistryHandle::from_source(&cfg_src).await.unwrap();
        assert_eq!(handle_a.generation(), v, "handle boots at the durable version");
        let (gw_a, _ca) = failing_after_gateway(1).await;
        let out_a = Executor::new(Arc::new(gw_a), Arc::new(PostgresJournal::new(connect(&url).await.unwrap())), "v1")
            .with_content_store(Arc::new(PostgresContentStore::new(connect(&url).await.unwrap())))
            .with_cas_threshold(8)
            .with_registry_handle(handle_a)
            .run(run, &graph)
            .await
            .expect("seed run yields an outcome");
        assert!(out_a.failed.is_some(), "n2 crashes, leaving n1 durably journaled");

        // Process B: FRESH source/handle over the SAME DB, config unchanged (still v).
        let cfg_src_b = PostgresConfigSource::new(connect(&url).await.unwrap());
        let handle_b = RegistryHandle::from_source(&cfg_src_b).await.unwrap();
        assert_eq!(handle_b.generation(), v, "process B agrees on the durable generation");
        let (gw_b, calls_b) = recording_gateway().await;
        let out_b = Executor::new(Arc::new(gw_b), Arc::new(PostgresJournal::new(connect(&url).await.unwrap())), "v1")
            .with_content_store(Arc::new(PostgresContentStore::new(connect(&url).await.unwrap())))
            .with_cas_threshold(8)
            .with_registry_handle(handle_b)
            .start(run, &graph)
            .await
            .expect("fence matches across processes → resume proceeds");
        assert!(out_b.failed.is_none(), "{:?}", out_b.failed);
        assert_eq!(out_b.completed, vec![n1.clone(), n2.clone()]);
        assert_eq!(
            calls_b.lock().unwrap().len(),
            1,
            "n1 replayed from the durable memo → the fresh gateway ran only the tail (0 re-spend)"
        );
    }

    /// AC6 — a config change (a bump) between the original run and the resume is caught LOUDLY across
    /// the process boundary. Process A runs at v; then `store(new) + bump → v+1`; a fresh process B
    /// boots at v+1 → the fence refuses. AC7 (mutation-check): were `version()` to return None, both
    /// handles would boot at 0 and this mismatch would NOT fire — proving the durable version carries
    /// the fence.
    #[tokio::test]
    async fn postgres_bumped_config_generation_fences_a_cross_process_resume() {
        let Some(url) = db_url() else { return };
        let run = RunId(uuid::Uuid::new_v4());
        let (graph, _n1, _n2) = two_node_graph("a", "b");

        let cfg_src = PostgresConfigSource::new(connect(&url).await.unwrap());
        cfg_src.store(&RegistryConfig::default()).await.unwrap();
        let v = cfg_src.bump_config_version().await.unwrap();

        // Process A runs (fully) at generation v.
        let handle_a = RegistryHandle::from_source(&cfg_src).await.unwrap();
        let (gw_a, _ca) = recording_gateway().await;
        Executor::new(Arc::new(gw_a), Arc::new(PostgresJournal::new(connect(&url).await.unwrap())), "v1")
            .with_content_store(Arc::new(PostgresContentStore::new(connect(&url).await.unwrap())))
            .with_registry_handle(handle_a)
            .run(run, &graph)
            .await
            .expect("A completes at gen v");

        // Config changes: a new store + a bump → v+1.
        cfg_src.store(&RegistryConfig::default()).await.unwrap();
        let v2 = cfg_src.bump_config_version().await.unwrap();
        assert!(v2 > v, "the bump advanced the durable generation");

        // Process B boots at v+1 → resuming the v-authored run is fenced LOUDLY.
        let handle_b = RegistryHandle::from_source(&PostgresConfigSource::new(connect(&url).await.unwrap()))
            .await
            .unwrap();
        assert_eq!(handle_b.generation(), v2);
        let (gw_b, _cb) = recording_gateway().await;
        let err = Executor::new(Arc::new(gw_b), Arc::new(PostgresJournal::new(connect(&url).await.unwrap())), "v1")
            .with_content_store(Arc::new(PostgresContentStore::new(connect(&url).await.unwrap())))
            .with_registry_handle(handle_b)
            .start(run, &graph)
            .await
            .expect_err("a changed config generation must fence the cross-process resume");
        assert!(
            matches!(
                &err,
                OrchestratorError::VersionFenceMismatch { recorded, current }
                    if recorded == &format!("v1#cfg{v}") && current == &format!("v1#cfg{v2}")
            ),
            "expected a loud config-generation fence, got {err:?}"
        );
    }
```
(If `failing_after_gateway`/`recording_gateway`/`two_node_graph` have moved, grep them in `tests.rs`. `RegistryConfig::default()` is `#[derive(Default)]` on the struct.)

- [ ] **Step 2: Verify they fail then pass (Docker-PG)**

Run the Docker-PG harness e2e line:
```bash
cargo test -p sensei-orchestrator --features postgres-tests -- --test-threads=1 postgres_ ; echo "E2E_EXIT=$?"
```
Expected: the two new tests + the SP-DATA-1 ones PASS; `E2E_EXIT=0`. (Before Task 3/2 landed, `PostgresConfigSource`/`from_source` wouldn't resolve — this task depends on them.)

- [ ] **Step 3: Additivity + full-suite gate (feature OFF ⇒ byte-identical)**

On the macOS host (no PG), REAL unpiped exit codes:
```bash
cd /Users/Jerry/Developer/gateway
cargo test --workspace > /tmp/spd2_fulltest.log 2>&1; echo "EXIT=$?"
grep -oE "[0-9]+ passed" /tmp/spd2_fulltest.log | awk '{s+=$1} END{print "TOTAL_PASSED="s}'
grep -oE "[1-9][0-9]* failed" /tmp/spd2_fulltest.log | head
cargo fmt --all --check; echo "FMT=$?"
cargo clippy --workspace --all-targets -- -D warnings > /tmp/spd2_clippy.log 2>&1; echo "CLIPPY=$?"
```
Confirm `EXIT=0`, 0 failed, `TOTAL_PASSED=1123` (**1120 baseline + the 3 new core tests from Task 2**; the postgres/postgres-tests tests are feature-gated OFF so they don't count, and the e2e tests `return` early without `DATABASE_URL`). `FMT=0`, `CLIPPY=0`. If the count differs, reconcile before committing (the only additions to the default suite are Task 2's three `registry.rs` tests).

- [ ] **Step 4: Commit** (do NOT push)

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/tests.rs
git commit -m "test(orchestrator): SP-DATA-2 (4/4) — cross-process config fence e2e + additivity gate"
```

---

## Self-Review notes (author)

- **Spec coverage:** §4 schema → Task 1. §5 bump (app-side atomic upsert) → Task 3 `bump_config_version`. §6 trait `version()` + `reload`/`from_source` → Task 2. §7 `PostgresConfigSource` load/version/store → Task 3. AC1→T1; AC2/AC3→T3; AC4→T2; AC5→T4 (`postgres_unchanged_…`); AC6→T4 (`postgres_bumped_…`); AC7→T4 (the mutation-check is documented in the bumped test — forcing `version()→None` makes both handles boot at 0 so the mismatch can't fire); AC8→T4 Step 3 (byte-identical + the 3 additive core tests); AC9→the Docker harness (all PG tasks).
- **Type consistency:** `ConfigSource::version() -> Result<Option<u64>, OrchestratorError>` (T2 def) is used identically in T3 (`PostgresConfigSource::version`) and consumed by `reload`/`from_source` (T2). `RegistryHandle::from_source` (T2) is called in T4. `PostgresConfigSource::{new,store,bump_config_version,load}` (T3) are called in T4. `RegistryConfig { agents, skills, tools, chain_bindings }` + `ChainBinding { area, kind, chain }` match `registry.rs`.
- **Adapt-at-build items (verify against the real code, like SP-DATA-1's adapt-in-DB):** the exact field lists of `AgentDefinition`/`SkillDef`/`ToolSpec` for the T3 test constructors; the real names of the `registry.rs` test helpers (`cfg_with_skill`/`FixedSource`) and the `tests.rs` harness helpers (`two_node_graph`/`failing_after_gateway`/`recording_gateway`); whether a `store_err_ser` serde→`Store` mapper already exists (reuse it) or needs the 1-line addition. The DDL, the trait/handle wiring, the fence semantics, and the e2e shape are exact.
- **Additive:** the `postgres`/`postgres-tests` features are off by default; the only default-suite additions are Task 2's three `orchestrator-core` tests (1120 → 1123). The defaulted `version()` leaves `FilesystemConfigSource`/`InMemoryConfigSource` and every existing `reload` call byte-identical.
- **Verification reality:** the Postgres code compiles on any host (feature-gated, runtime sqlx); behavior is verified on a Docker Postgres. Tests skip when `DATABASE_URL` is unset. Do NOT push — the coordinator runs the whole-slice review (incl. `dbd-pattern-verifier`) then pushes.
