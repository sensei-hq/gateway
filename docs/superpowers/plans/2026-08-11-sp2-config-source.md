# SP-2 slice 1 — ConfigSource adapter + filesystem backend — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Load the agent/skill/tool `Registry` from a pluggable, backend-agnostic `ConfigSource` (domain-typed seam), shipping a filesystem backend + an in-memory backend.

**Architecture:** `orchestrator-core` gains `RegistryConfig` (Vecs of the domain types), the `ConfigSource` async trait (`load() -> RegistryConfig`), and `Registry::from_config` (pure assemble + dup-reject + `validate`). `orchestrator-store` gains `FilesystemConfigSource` (std::fs; isolates all md/JSON parsing) + `InMemoryConfigSource`. Purely additive — the executor's `with_registry(Arc<Registry>)` is unchanged.

**Tech Stack:** Rust; `sensei-orchestrator-core` (types/trait/assembler), `sensei-orchestrator-store` (backends), `sensei-orchestrator` (e2e). `async-trait`, `serde_json`, `std::fs`; `uuid` (dev) for temp-dir tests.

**Design:** `docs/superpowers/specs/2026-08-11-sp2-config-source-design.md`. `ConfigSource` is the extension trait (fs / Postgres / Convex impl it); `Registry` is the uniform assembled result.

**Conventions (non-negotiable):** TDD (failing test → watch fail → minimal code). `cargo fmt --all` before every commit (pre-commit = fmt-check + workspace `clippy -D warnings`, NO tests — run `cargo test --workspace` yourself before committing). Verify REAL exit codes, never a piped `| tail`. Commit a fix BEFORE any `git checkout`-based mutation-verify.

---

## File structure

- `crates/orchestrator-core/src/registry.rs` — `RegistryConfig`, `ConfigSource`, `Registry::from_config`.
- `crates/orchestrator-core/src/error.rs` — `OrchestratorError::RegistryLoad(String)`.
- `crates/orchestrator-core/src/lib.rs` — export `RegistryConfig`, `ConfigSource`.
- `crates/orchestrator-store/src/config_source.rs` (new) — `FilesystemConfigSource`, `InMemoryConfigSource`.
- `crates/orchestrator-store/src/lib.rs` — `mod config_source; pub use …`.
- `crates/orchestrator/src/executor/tests.rs` — e2e (drive an agent from disk-loaded config).
- `docs/features/orchestrator/agents-skills-tools.md`, `README.md` — status.

---

## Task 1: core — `RegistryConfig` + `ConfigSource` + `Registry::from_config`

**Files:**
- Modify: `crates/orchestrator-core/src/{registry.rs, error.rs, lib.rs}`

- [ ] **Step 1: Add the error variant.** In `error.rs`, beside `ContextKeyCollision`:

```rust
    #[error("registry load error: {0}")]
    RegistryLoad(String),
```

- [ ] **Step 2: Write the failing test** in `registry.rs` `mod tests`:

```rust
    #[test]
    fn from_config_assembles_validates_and_rejects_duplicates() {
        let agent = AgentDefinition::from_frontmatter(AGENT_MD).unwrap(); // "researcher", tools:[calc], skills:[concise]
        let cfg = RegistryConfig {
            agents: vec![agent.clone()],
            skills: vec![SkillDef { name: "concise".into(), description: None, body: "b".into() }],
            tools: vec![tool_spec("calc")],
        };
        let reg = Registry::from_config(cfg).expect("assembles + validates");
        assert!(reg.agent("researcher").is_some() && reg.tool("calc").is_some());

        // Dangling ref → validate error.
        let dangling = RegistryConfig { agents: vec![agent.clone()], skills: vec![], tools: vec![] };
        assert!(matches!(
            Registry::from_config(dangling),
            Err(OrchestratorError::UnknownToolRef { .. }) | Err(OrchestratorError::UnknownSkillRef { .. })
        ));

        // Duplicate name → loud RegistryLoad (never silent last-wins).
        let dup = RegistryConfig {
            agents: vec![agent.clone(), agent],
            skills: vec![SkillDef { name: "concise".into(), description: None, body: "b".into() }],
            tools: vec![tool_spec("calc")],
        };
        assert!(matches!(
            Registry::from_config(dup),
            Err(OrchestratorError::RegistryLoad(m)) if m.contains("duplicate") && m.contains("researcher")
        ));
    }
```

- [ ] **Step 3: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator-core from_config_assembles 2>&1 | grep -E "cannot find|error\[|test result"`
Expected: FAIL (`RegistryConfig`/`from_config` do not exist).

- [ ] **Step 4: Implement** in `registry.rs`:

```rust
/// The registry's config as domain objects — the backend-agnostic payload a
/// [`ConfigSource`] yields (no serialization format in the contract).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegistryConfig {
    pub agents: Vec<AgentDefinition>,
    pub skills: Vec<SkillDef>,
    pub tools: Vec<ToolSpec>,
}

/// A pluggable source of registry config (§SP-2). The extension seam future
/// backends implement — `FilesystemConfigSource` now, `PostgresConfigSource` /
/// `ConvexConfigSource` later. `Registry` itself is the uniform assembled result,
/// NOT an extension point.
#[async_trait::async_trait]
pub trait ConfigSource: Send + Sync {
    async fn load(&self) -> Result<RegistryConfig, OrchestratorError>;
}

impl Registry {
    /// Assemble + validate a `Registry` from already-parsed config. Rejects a
    /// duplicate agent/skill/tool `name` loudly (the shared, single validation
    /// point every backend reuses), then runs the dangling-ref `validate`.
    pub fn from_config(cfg: RegistryConfig) -> Result<Registry, OrchestratorError> {
        let mut reg = Registry::default();
        for a in cfg.agents {
            if reg.agent(&a.name).is_some() {
                return Err(OrchestratorError::RegistryLoad(format!("duplicate agent: {}", a.name)));
            }
            reg = reg.with_agent(a);
        }
        for s in cfg.skills {
            if reg.skill(&s.name).is_some() {
                return Err(OrchestratorError::RegistryLoad(format!("duplicate skill: {}", s.name)));
            }
            reg = reg.with_skill(s);
        }
        for t in cfg.tools {
            if reg.tool(&t.name).is_some() {
                return Err(OrchestratorError::RegistryLoad(format!("duplicate tool: {}", t.name)));
            }
            reg = reg.with_tool(t);
        }
        reg.validate()?;
        Ok(reg)
    }
}
```

In `lib.rs`, extend the registry re-export: `pub use registry::{AgentDefinition, AgentRef, ConfigSource, Registry, RegistryConfig, SkillDef, ToolSpec};`

- [ ] **Step 5: Run to verify pass + core suite**

Run: `cargo test -p sensei-orchestrator-core > /tmp/t.log 2>&1; echo "EXIT=$?"; grep "test result" /tmp/t.log`
Expected: `EXIT=0`.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "feat(orchestrator-core): RegistryConfig + ConfigSource trait + Registry::from_config (config-source)"
```

---

## Task 2: `InMemoryConfigSource` (store)

**Files:**
- Create: `crates/orchestrator-store/src/config_source.rs`
- Modify: `crates/orchestrator-store/src/lib.rs`

- [ ] **Step 1: Write the failing test** — put the module + a test in `config_source.rs`:

```rust
//! Config-source backends (§SP-2): the filesystem loader + an in-memory source.
//! `FilesystemConfigSource` isolates ALL md/JSON parsing; the seam itself
//! (`ConfigSource`) is domain-typed, so a DB/HTTP backend drops in unchanged.

use orchestrator_core::{ConfigSource, OrchestratorError, RegistryConfig};

/// A `ConfigSource` returning a fixed `RegistryConfig` — for tests + programmatic
/// config, and the vehicle for exercising `Registry::from_config` off-disk.
#[derive(Clone, Default)]
pub struct InMemoryConfigSource(pub RegistryConfig);

#[async_trait::async_trait]
impl ConfigSource for InMemoryConfigSource {
    async fn load(&self) -> Result<RegistryConfig, OrchestratorError> {
        Ok(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{Registry, SkillDef};

    #[tokio::test]
    async fn in_memory_source_round_trips_through_from_config() {
        let cfg = RegistryConfig {
            agents: vec![],
            skills: vec![SkillDef { name: "s".into(), description: None, body: "b".into() }],
            tools: vec![],
        };
        let src = InMemoryConfigSource(cfg);
        let reg = Registry::from_config(src.load().await.unwrap()).unwrap();
        assert!(reg.skill("s").is_some());
    }
}
```

- [ ] **Step 2: Wire the module.** In `lib.rs`: `mod config_source;` + `pub use config_source::{FilesystemConfigSource, InMemoryConfigSource};` (Filesystem lands in Task 3 — add it to the `pub use` there, or temporarily export only `InMemoryConfigSource` now and extend in Task 3).

- [ ] **Step 3: Run to verify pass**

Run: `cargo test -p sensei-orchestrator-store in_memory_source 2>&1 | grep "test result"` → `ok`.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "feat(orchestrator): InMemoryConfigSource (config-source)"
```

---

## Task 3: `FilesystemConfigSource` (store, std::fs)

**Files:**
- Modify: `crates/orchestrator-store/src/config_source.rs`, `crates/orchestrator-store/src/lib.rs`

- [ ] **Step 1: Write the failing tests** in `config_source.rs` `mod tests` (temp dir via `std::env::temp_dir()` + `uuid`; no `tempfile` dep):

```rust
    use std::path::PathBuf;

    fn write(p: &std::path::Path, name: &str, content: &str) {
        std::fs::write(p.join(name), content).unwrap();
    }

    /// Build a temp config dir with agents/skills/tools; return its root.
    fn temp_config_root() -> PathBuf {
        let root = std::env::temp_dir().join(format!("sp2-cfg-{}", uuid::Uuid::new_v4()));
        for sub in ["agents", "skills", "tools"] {
            std::fs::create_dir_all(root.join(sub)).unwrap();
        }
        write(&root.join("agents"), "researcher.md",
            "---\nname: researcher\narea: research\nkind: reasoning\nchain: c\ntools: [calc]\nskills: [concise]\n---\nBe careful.\n");
        write(&root.join("skills"), "concise.md", "---\nname: concise\n---\nBe terse.\n");
        write(&root.join("tools"), "calc.json",
            r#"{"name":"calc","description":"adds","input_schema":{"type":"object"},"effect_class":"Pure","ttl_secs":null,"source":null}"#);
        root
    }

    #[tokio::test]
    async fn filesystem_source_loads_agents_skills_and_tools() {
        let root = temp_config_root();
        let cfg = FilesystemConfigSource::new(&root).load().await.expect("loads");
        let reg = Registry::from_config(cfg).expect("valid");
        assert!(reg.agent("researcher").is_some());
        assert!(reg.skill("concise").is_some());
        let calc = reg.tool("calc").expect("calc tool");
        assert_eq!(calc.effect_class, orchestrator_core::EffectClass::Pure);
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn missing_subdir_is_empty_not_an_error() {
        let root = std::env::temp_dir().join(format!("sp2-cfg-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("skills")).unwrap(); // no agents/ or tools/
        write(&root.join("skills"), "s.md", "---\nname: s\n---\nb\n");
        let cfg = FilesystemConfigSource::new(&root).load().await.expect("loads");
        assert_eq!(cfg.agents.len(), 0);
        assert_eq!(cfg.tools.len(), 0);
        assert_eq!(cfg.skills.len(), 1);
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn missing_root_is_loud() {
        let root = std::env::temp_dir().join(format!("sp2-nope-{}", uuid::Uuid::new_v4()));
        assert!(matches!(
            FilesystemConfigSource::new(&root).load().await,
            Err(OrchestratorError::RegistryLoad(_))
        ));
    }

    #[tokio::test]
    async fn malformed_tool_json_is_a_loud_registry_load_error() {
        let root = temp_config_root();
        write(&root.join("tools"), "bad.json", "{ not valid json");
        assert!(matches!(
            FilesystemConfigSource::new(&root).load().await,
            Err(OrchestratorError::RegistryLoad(m)) if m.contains("bad.json")
        ));
        std::fs::remove_dir_all(&root).ok();
    }
```

- [ ] **Step 2: Run to confirm failure** (`FilesystemConfigSource` does not exist).

- [ ] **Step 3: Implement** in `config_source.rs`:

```rust
use std::path::{Path, PathBuf};

use orchestrator_core::{AgentDefinition, SkillDef, ToolSpec};

/// A filesystem `ConfigSource`: reads `<root>/agents/*.md`, `<root>/skills/*.md`,
/// `<root>/tools/*.json` into a `RegistryConfig`. ALL md/JSON parsing is isolated
/// here (the seam is domain-typed). Uses blocking `std::fs` inside the async
/// method — a one-shot startup read, not a hot path (keeps the store tokio-free).
pub struct FilesystemConfigSource {
    root: PathBuf,
}

impl FilesystemConfigSource {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

/// Read `<root>/<sub>/*.<ext>` in sorted filename order → Vec<(filename, content)>.
/// A missing subdir yields an empty Vec; any other I/O error is loud.
fn read_dir_files(root: &Path, sub: &str, ext: &str) -> Result<Vec<(String, String)>, OrchestratorError> {
    let dir = root.join(sub);
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(OrchestratorError::RegistryLoad(format!("read {}: {e}", dir.display()))),
    };
    let mut paths: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some(ext))
        .collect();
    paths.sort();
    let mut out = Vec::with_capacity(paths.len());
    for p in paths {
        let content = std::fs::read_to_string(&p)
            .map_err(|e| OrchestratorError::RegistryLoad(format!("read {}: {e}", p.display())))?;
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or_default().to_string();
        out.push((name, content));
    }
    Ok(out)
}

#[async_trait::async_trait]
impl ConfigSource for FilesystemConfigSource {
    async fn load(&self) -> Result<RegistryConfig, OrchestratorError> {
        if !self.root.exists() {
            return Err(OrchestratorError::RegistryLoad(format!(
                "config root not found: {}",
                self.root.display()
            )));
        }
        let mut cfg = RegistryConfig::default();
        for (_, md) in read_dir_files(&self.root, "agents", "md")? {
            cfg.agents.push(AgentDefinition::from_frontmatter(&md)?); // md parse → FrontmatterParse
        }
        for (_, md) in read_dir_files(&self.root, "skills", "md")? {
            cfg.skills.push(SkillDef::from_frontmatter(&md)?);
        }
        for (file, json) in read_dir_files(&self.root, "tools", "json")? {
            cfg.tools.push(serde_json::from_str::<ToolSpec>(&json).map_err(|e| {
                OrchestratorError::RegistryLoad(format!("parse tool {file}: {e}"))
            })?);
        }
        Ok(cfg)
    }
}
```

Ensure `lib.rs` exports `FilesystemConfigSource` (extend the Task-2 `pub use`). Add `use orchestrator_core::{Registry, EffectClass};` etc. to the test mod as needed.

- [ ] **Step 4: Run + full store suite + clippy**

Run: `cargo test -p sensei-orchestrator-store > /tmp/t.log 2>&1; echo "EXIT=$?"; grep -c "test result: ok" /tmp/t.log`
Run: `cargo clippy --workspace --all-targets -- -D warnings > /tmp/c.log 2>&1; echo "CLIPPY=$?"`
Expected: `EXIT=0`, `CLIPPY=0`.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "feat(orchestrator): FilesystemConfigSource — load agents/skills/tools from disk (config-source)"
```

---

## Task 4: end-to-end + docs + memory

**Files:**
- Modify: `crates/orchestrator/src/executor/tests.rs`
- Modify: `docs/features/orchestrator/agents-skills-tools.md`, `docs/features/orchestrator/README.md`

- [ ] **Step 1: e2e test** in `tests.rs` — drive an `Agent` node whose registry was loaded **from disk** through a test gateway:

```rust
/// SP-2 e2e — a registry loaded from a filesystem ConfigSource drives an agent
/// node end-to-end (disk config → Registry::from_config → with_registry → run).
#[tokio::test]
async fn agent_runs_from_a_filesystem_loaded_registry() {
    use orchestrator_core::Registry;
    use orchestrator_store::FilesystemConfigSource;
    // Build a temp config dir with one no-tool agent "a" on chain "c".
    let root = std::env::temp_dir().join(format!("sp2-e2e-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(root.join("agents")).unwrap();
    std::fs::write(
        root.join("agents").join("a.md"),
        "---\nname: a\narea: research\nkind: reasoning\nchain: c\n---\nBe helpful.\n",
    ).unwrap();
    let registry = Registry::from_config(
        FilesystemConfigSource::new(&root).load().await.expect("load"),
    ).expect("validate");

    let (gw, _c) = recording_gateway().await;
    let graph = Graph { nodes: vec![agent_node("n1", "a", "hi")] };
    let out = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(Arc::new(registry))
        .with_tools(Arc::new(ToolRegistry::default()))
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{:?}", out.failed);
    assert_eq!(out.outputs[&NodeId("n1".into())]["text"], "canned-response");
    std::fs::remove_dir_all(&root).ok();
}
```

> Implementer: `recording_gateway`/`agent_node`/`ToolRegistry` are already imported in `tests.rs`. The agent has no tools/skills, so `from_config`'s `validate` passes with no tool/skill files.

- [ ] **Step 2: Full workspace gate**

Run: `cargo test --workspace > /tmp/ws.log 2>&1; echo "WS=$?"; grep -Eo "[0-9]+ passed" /tmp/ws.log | awk '{s+=$1} END{print s" passed"}'`
Run: `cargo clippy --workspace --all-targets -- -D warnings > /tmp/c.log 2>&1; echo "CLIPPY=$?"`
Expected: `WS=0`, `CLIPPY=0`.

- [ ] **Step 3: Docs.** In `agents-skills-tools.md`, note the registry can now load from a pluggable `ConfigSource` (filesystem `<root>/agents|skills/*.md` + `tools/*.json`; `Registry::from_config` assembles + validates + rejects dups); `ConfigSource` is the extension seam (Postgres/Convex later); tool executors still bind via `ToolRegistry`. Update the README row if present. Note deferred: role→chain, permissions, activation policy, hot-reload, disk-bound tool executors.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "feat(orchestrator): filesystem-loaded registry e2e + docs (SP-2 config-source COMPLETE)"
```

---

## Notes for the implementer

- **`ConfigSource` is the extension seam; `Registry` is the assembled result.** Backends (fs / Postgres / Convex) impl `ConfigSource`; they all reuse `Registry::from_config` + `validate`. Do NOT add backend-specific logic to `Registry`.
- **All md/JSON parsing stays inside `FilesystemConfigSource`** — the seam is domain-typed. A DB backend maps rows → `RegistryConfig` with no text round-trip.
- **Duplicate names fail loud** in `from_config` (checked before the HashMap collapses them). **Missing root** → loud `RegistryLoad`; **missing subdir** → empty.
- **`std::fs` inside the async `load`** is intentional (D2): a one-shot startup read keeps `orchestrator-store` free of a runtime `tokio` dependency.
- **Purely additive:** the executor's `with_registry(Arc<Registry>)` is unchanged; `.with_*`/`from_frontmatter` and all existing tests stay byte-identical.
- **Known gap (D4):** a disk `ToolSpec` with no code executor loads + validates fine but is a loud `UnknownTool` at execution (MCP bridge deferred).
- Pre-commit runs fmt + clippy (NO tests); run `cargo test --workspace` before committing. Branch: `feat/sp2-config-source` off `develop`.
