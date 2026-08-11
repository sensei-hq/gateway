---
title: SP-2 slice 1 — ConfigSource adapter + filesystem backend
doctype: design
module: orchestrator
spec: SP-2
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§9/§F registry, §16 SP-2, Q4 activation)
date: 2026-08-11
---

# SP-2 slice 1 — `ConfigSource` adapter + filesystem backend

## 1. Goal

Make the agent/skill/tool `Registry` **loadable from a pluggable config backend**,
not just the in-memory `.with_*` builders. The seam yields **already-parsed domain
objects** (`AgentDefinition`/`SkillDef`/`ToolSpec`), so a filesystem backend ships
now and a DB / HTTP backend (SP-DATA) drops in later with **no serialization format
baked into the contract**. This is SP-2's foundational slice; role→chain,
permissions, activation policy, and hot-reload are later slices.

## 2. SP-2 slicing (context)

1. **This slice** — `ConfigSource` adapter seam + `FilesystemConfigSource`.
2. role/kind → chain resolution (agents reference a role; registry binds role→chain, §122).
3. tool permission declarations (path/command/network allowlists, caps — §132/§287; declarations only, enforcement = SP-4).
4. activation policy (Q4: `when`/trigger, progressive disclosure vs today's always-on).
5. hot-reload (reload + swap `Arc<Registry>` + version bump).

## 3. Background & impact review

- **Current registry** (`orchestrator-core::registry`, slice 2): `AgentDefinition
  { name, area, kind, chain, tools, skills, system_prompt }`, `SkillDef { name,
  description, body }`, `ToolSpec { name, description, input_schema, effect_class,
  ttl_secs, source }` — all `Serialize + Deserialize`. Public parsers
  `AgentDefinition::from_frontmatter` / `SkillDef::from_frontmatter` (md+frontmatter
  subset). `Registry::{with_agent,with_skill,with_tool,agent,skill,tool,validate}`.
  `validate` checks **dangling** skill/tool refs only.
- **Impact: purely additive, zero executor change.** The executor's only registry
  seam is `Executor::with_registry(Arc<Registry>)`; the loader produces a `Registry`
  the caller wraps. No existing behavior/tests change.
- **`orchestrator-store` is I/O-free today** (deps: core, `async-trait`, `serde_json`;
  `tokio` is dev-only). The filesystem backend keeps it lean by using `std::fs`
  (see D2).
- `orchestrator-core` has `async-trait`; there is **no exhaustive `OrchestratorError`
  match** (adding a variant is safe); `ConfigSource`/`RegistryConfig`/`RegistryLoad`
  are new names (no clash).

## 4. Design

### 4.0 Extension model (what future backends implement)

**`ConfigSource` is the extension trait** — the seam future backends implement
(`FilesystemConfigSource`, `PostgresConfigSource`, `ConvexConfigSource`,
`HttpConfigSource`, …), mirroring the codebase's existing trait/impl seams
(`ExecutionJournal`/`ContentStore`/`ContextStore`/`Clock`/`ReconcileProvider`).
**`Registry` is NOT an extension point** — it is the concrete, backend-agnostic
*assembled result* that `Registry::from_config` builds + validates from whatever a
`ConfigSource` yields, and that the executor consumes via `with_registry`. Adapters
vary the **source**; the `Registry` type, `from_config` assembly, and `validate`
are uniform across every backend. Config loading is a one-shot startup read (a few
small artifacts), so it needs no task-queue/fan-out — the executor's `run_map`
fan-out is for gateway calls, not config I/O; a backend that ever loads *many*
artifacts can do bounded concurrency privately inside its own `load()`.

### 4.1 The seam (domain-typed, `orchestrator-core`)

```rust
/// The registry's config as domain objects — NO serialization format in the
/// contract. A backend produces these however it likes (files, DB rows, an API).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegistryConfig {
    pub agents: Vec<AgentDefinition>,
    pub skills: Vec<SkillDef>,
    pub tools:  Vec<ToolSpec>,
}

#[async_trait::async_trait]
pub trait ConfigSource: Send + Sync {
    /// Load the whole registry config. A whole-snapshot load is hot-reload-ready
    /// (a later slice re-calls this + swaps the `Arc<Registry>`).
    async fn load(&self) -> Result<RegistryConfig, OrchestratorError>;
}
```

### 4.2 The assembler (pure, shared, `orchestrator-core`)

```rust
impl Registry {
    /// Assemble + validate a Registry from already-parsed config. Rejects
    /// **duplicate** agent/skill/tool names loudly (over the Vecs, before the
    /// HashMap collapses them — with_* alone would silently last-wins), then
    /// runs the existing dangling-ref `validate`. The single validation point,
    /// reused by every backend.
    pub fn from_config(cfg: RegistryConfig) -> Result<Registry, OrchestratorError>;
}
```

- **Duplicate detection (D1):** while inserting each Vec, a repeated `name` →
  `OrchestratorError::RegistryLoad("duplicate agent/skill/tool: {name}")`.
- After assembly, call `self.validate()` (dangling skill/tool refs).
- Parsing (`from_frontmatter` / JSON) is **NOT** in this path — backends parse
  their own representation into the domain types.

### 4.3 Backends (`orchestrator-store`, beside the in-memory journal/CAS impls)

- **`FilesystemConfigSource { root: PathBuf }`.** `load()` reads
  `<root>/agents/*.md` (→ `AgentDefinition::from_frontmatter`),
  `<root>/skills/*.md` (→ `SkillDef::from_frontmatter`), `<root>/tools/*.json`
  (→ `serde_json::from_str::<ToolSpec>`), producing a `RegistryConfig`. **All
  filesystem/md/JSON knowledge is isolated here** — the only place that touches
  files. Entries read in **sorted filename order** (deterministic). A missing
  **root** → loud `RegistryLoad`; a missing `agents`/`skills`/`tools` **subdir** →
  treated as empty. An I/O error or a tool-JSON parse error → `RegistryLoad`
  naming the path; an md parse error → `FrontmatterParse` (from the core parser).
  `name` (frontmatter/JSON) is authoritative; the filename is a cosmetic
  container (not enforced to match).
- **`InMemoryConfigSource(RegistryConfig)`.** Returns it verbatim — for tests +
  programmatic config, and the vehicle for testing the `from_config` assembler
  without touching the filesystem.

### 4.4 Decisions (from the impact/depth review)

- **D1 — duplicate names fail loud.** `from_config` rejects repeats (a config with
  two agents named `x` is an error, never a silent last-wins).
- **D2 — `std::fs`, not `tokio::fs`.** `FilesystemConfigSource::load` does blocking
  `std::fs` reads inside the `async fn`. Config load is a one-shot startup read of a
  handful of small files, not a per-request hot path, so briefly blocking the async
  thread is acceptable — and it keeps `orchestrator-store` free of a runtime
  `tokio` dependency. A real-async DB backend uses proper async later.
- **D3 — one new error variant.** `OrchestratorError::RegistryLoad(String)` for I/O,
  tool-JSON parse, and duplicate-name; md parse errors stay `FrontmatterParse`.
- **D4 — tool-executor gap (known).** A disk `ToolSpec` with no code executor loads
  and `validate`s fine (the spec is resolvable for prompt assembly); executing it
  is a loud runtime `UnknownTool`. The Registry(specs)↔`ToolRegistry`(executors)
  split (slice 2) is unchanged; binding disk tools to executors (MCP bridge) is
  deferred.
- **D5 — fence untouched.** Config *content* changes are already fenced via
  `agent_input_hash` (editing a skill body → hash change → `DeterminismViolation`
  on resume, slice-2 test). Slice 1 loads at startup and does not touch the fence;
  a config-version + reload land with the hot-reload slice.

### 4.5 Load flow

```rust
let source = FilesystemConfigSource::new(root);          // or InMemoryConfigSource / (later) PostgresConfigSource
let registry = Registry::from_config(source.load().await?)?;
let exec = Executor::new(gw, journal, "v1").with_registry(Arc::new(registry));
```

## 5. Tool file format

`<root>/tools/<name>.json`:
```json
{ "name": "calc", "description": "…", "input_schema": { "type": "object", … },
  "effect_class": "Pure", "ttl_secs": null, "source": null }
```
`effect_class` is a serde unit-variant string (`"Pure"`/`"Observation"`/`"Mutation"`);
`ttl_secs`/`source` optional (null/omitted).

## 6. Deferred (stated)

- role→chain resolution, tool permission declarations, activation policy (Q4),
  hot-reload — later SP-2 slices.
- Disk-bound tool **executors** (MCP/external bridge) — a tool spec on disk still
  needs a code (or bridged) executor to run.
- `PostgresConfigSource` / `ConvexConfigSource` / `HttpConfigSource` (SP-DATA /
  later) — each impls `ConfigSource` (mapping rows / API responses → `RegistryConfig`,
  no md/JSON round-trip) and reuses `from_config` + `validate` unchanged.
- Async filesystem I/O (`tokio::fs` / `spawn_blocking`) if config load ever moves
  off the startup path.

## 7. Acceptance criteria (TDD)

1. **Assembler (in-memory).** `Registry::from_config` over an `InMemoryConfigSource`
   carrying agents+skills+tools → the registry resolves all three and `validate`
   passes; a dangling agent tool/skill ref → loud `UnknownToolRef`/`UnknownSkillRef`.
2. **Duplicate names fail (D1).** A `RegistryConfig` with two agents (or skills, or
   tools) of the same `name` → `RegistryLoad` naming the dup — never a silent
   last-wins.
3. **Filesystem backend.** `FilesystemConfigSource` over a temp dir with
   `agents/*.md` + `skills/*.md` + `tools/*.json` → the expected `RegistryConfig`
   (sorted order); a tool `.json` round-trips to the right `ToolSpec`
   (`effect_class`/`ttl_secs`); a missing subdir → that collection empty (no panic).
4. **Loud file errors.** A malformed tool `.json` → `RegistryLoad` naming the file;
   a malformed agent `.md` → `FrontmatterParse`; a non-existent **root** →
   `RegistryLoad`.
5. **End-to-end.** `Registry::from_config(FilesystemConfigSource::new(dir).load().await?)`
   → a validated registry an `Executor::with_registry` accepts (drive an agent node
   from disk-loaded config through a test gateway).
6. **Opt-in unaffected.** Existing `.with_*`/`from_frontmatter` paths and all current
   tests are byte-identical (additive only).
