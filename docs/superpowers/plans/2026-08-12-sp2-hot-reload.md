# SP-2 slice 5 — registry hot-reload Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the registry live-reloadable: a `RegistryHandle` (swappable `Arc<Registry>` + config generation) with an atomic, validated, last-good `reload(source)`, which the executor reads once per run (pinning config+version), folding the generation into the existing version fence so a run uses exactly one config generation.

**Architecture:** `RegistryHandle` lives in `orchestrator-core` (std `RwLock<(Arc<Registry>, u64)>`, no I/O — the I/O is the injected `ConfigSource`). The executor gains an optional handle + a per-run pin at `run`/`start` (snapshot once, delegate to a pinned clone whose `version = "{base}#cfg{gen}"`). Reuses the existing `VersionFenceMismatch` for in-flight-run safety; no new determinism machinery.

**Tech Stack:** Rust workspace (`orchestrator-core`, `orchestrator`); std `Arc`/`RwLock`; `async-trait` (`ConfigSource`); `cargo test`/`clippy`. Spec: `docs/superpowers/specs/2026-08-12-sp2-hot-reload-design.md`.

**House rules (every task):**
- Pre-commit = `make lint` (fmt-check + workspace `clippy -D warnings`), NO tests → always `cargo fmt --all` then `cargo test --workspace` before committing.
- Verify the REAL exit code (never a piped `| tail`); run a single test with a SINGLE positional filter (cargo rejects multiple).
- Commit a fix BEFORE any `git checkout`-based mutation-verify.
- Branch `feat/sp2-hot-reload` (created; spec committed at `3fef108`). Crate `-p` names: `sensei-orchestrator-core`, `sensei-orchestrator`.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `crates/orchestrator-core/src/registry.rs` | registry types | add `RegistryHandle` (new/current/generation/snapshot/reload) + tests. |
| `crates/orchestrator-core/src/lib.rs` | exports | export `RegistryHandle`. |
| `crates/orchestrator/src/executor/mod.rs` | executor | `#[derive(Clone)]`; `handle` field + `with_registry_handle`; per-run pin (`run`/`start` → `run_inner`/`start_inner` + `pinned` helper). |
| `crates/orchestrator/src/executor/tests.rs` | tests | version-fence-on-reload, per-run pin, no-handle byte-identical, e2e agent-only-after-reload. |
| `docs/features/orchestrator/agents-skills-tools.md` | feature doc | slice-5 status note (closes SP-2). |

---

## Task 1: `RegistryHandle` (core, additive)

A new swappable-registry handle in `orchestrator-core`. Purely additive — no existing type changes, zero ripple.

**Files:**
- Modify: `crates/orchestrator-core/src/registry.rs` (add type + impl + tests)
- Modify: `crates/orchestrator-core/src/lib.rs` (export)

- [ ] **Step 1: Write the failing tests**

Add to the `#[cfg(test)] mod tests` block in `crates/orchestrator-core/src/registry.rs`. These use a local test `ConfigSource` (core can't depend on `orchestrator-store`'s `InMemoryConfigSource`):

```rust
    // A minimal in-core ConfigSource for handle tests (yields a fixed config).
    struct FixedSource(RegistryConfig);
    #[async_trait::async_trait]
    impl ConfigSource for FixedSource {
        async fn load(&self) -> Result<RegistryConfig, OrchestratorError> {
            Ok(self.0.clone())
        }
    }

    fn cfg_with_skill(name: &str) -> RegistryConfig {
        RegistryConfig {
            agents: vec![],
            skills: vec![SkillDef {
                name: name.into(),
                description: None,
                body: "B".into(),
                activation: Activation::default(),
            }],
            tools: vec![],
            chain_bindings: vec![],
        }
    }

    #[tokio::test]
    async fn registry_handle_new_current_and_generation() {
        let reg = Registry::from_config(cfg_with_skill("s0")).unwrap();
        let h = RegistryHandle::new(reg);
        assert_eq!(h.generation(), 0);
        assert!(h.current().skill("s0").is_some());
        let (snap_reg, snap_gen) = h.snapshot();
        assert_eq!(snap_gen, 0);
        assert!(snap_reg.skill("s0").is_some());
    }

    #[tokio::test]
    async fn registry_handle_reload_swaps_and_bumps_generation() {
        let h = RegistryHandle::new(Registry::from_config(cfg_with_skill("s0")).unwrap());
        let new_gen = h.reload(&FixedSource(cfg_with_skill("s1"))).await.unwrap();
        assert_eq!(new_gen, 1);
        assert_eq!(h.generation(), 1);
        assert!(h.current().skill("s1").is_some(), "new config is live");
        assert!(h.current().skill("s0").is_none(), "old config swapped out");
    }

    #[tokio::test]
    async fn registry_handle_reload_is_validated_and_last_good() {
        let h = RegistryHandle::new(Registry::from_config(cfg_with_skill("s0")).unwrap());
        // A config whose agent references a missing tool → from_config validate fails.
        let bad = RegistryConfig {
            agents: vec![AgentDefinition {
                name: "a".into(),
                area: "x".into(),
                kind: "y".into(),
                chain: Some("c".into()),
                chains: std::collections::HashMap::new(),
                grants: std::collections::HashMap::new(),
                tools: vec!["missing".into()],
                skills: vec![],
                system_prompt: "s".into(),
            }],
            skills: vec![],
            tools: vec![],
            chain_bindings: vec![],
        };
        let err = h.reload(&FixedSource(bad)).await;
        assert!(err.is_err(), "invalid config → reload errors");
        // Last-good preserved: old registry still live, generation unchanged.
        assert_eq!(h.generation(), 0);
        assert!(h.current().skill("s0").is_some());
    }
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator-core registry_handle_new_current_and_generation` (single filter).
Expected: FAIL to compile — `RegistryHandle` not found. (RED.)

- [ ] **Step 3: Add `RegistryHandle`**

In `crates/orchestrator-core/src/registry.rs`, add near the top imports `use std::sync::{Arc, RwLock};` (Arc may already be imported — check; add `RwLock`). Then add the type (e.g. after the `Registry` impl):

```rust
/// A cheaply-clonable handle to a live, swappable [`Registry`] + a monotonic
/// config generation. Clones share one `Arc<RwLock<…>>`, so an operator's clone
/// and the executor's clone observe the same swaps (SP-2 hot-reload).
#[derive(Clone)]
pub struct RegistryHandle {
    inner: Arc<RwLock<(Arc<Registry>, u64)>>,
}

impl RegistryHandle {
    /// A handle over `registry` at generation 0.
    pub fn new(registry: Registry) -> Self {
        Self {
            inner: Arc::new(RwLock::new((Arc::new(registry), 0))),
        }
    }

    /// The current registry (clones the `Arc`).
    pub fn current(&self) -> Arc<Registry> {
        self.inner.read().unwrap_or_else(|e| e.into_inner()).0.clone()
    }

    /// The current config generation.
    pub fn generation(&self) -> u64 {
        self.inner.read().unwrap_or_else(|e| e.into_inner()).1
    }

    /// The atomic `(registry, generation)` pair — read together so a run pins a
    /// consistent snapshot.
    pub fn snapshot(&self) -> (Arc<Registry>, u64) {
        let g = self.inner.read().unwrap_or_else(|e| e.into_inner());
        (g.0.clone(), g.1)
    }

    /// Reload from `source`, atomically swapping in the new registry and bumping
    /// the generation (returns the new generation). Validated + last-good: the
    /// load + `Registry::from_config` (which validates) run BEFORE the swap, so a
    /// failed load/validate returns `Err` with the old registry still live. The
    /// `.await` is OUTSIDE the lock.
    pub async fn reload(&self, source: &dyn ConfigSource) -> Result<u64, OrchestratorError> {
        let cfg = source.load().await?;
        let next = Registry::from_config(cfg)?;
        let mut w = self.inner.write().unwrap_or_else(|e| e.into_inner());
        w.0 = Arc::new(next);
        w.1 += 1;
        Ok(w.1)
    }
}
```
If `registry.rs` currently imports `use std::collections::HashMap;` only, add the `Arc`/`RwLock` import line separately (don't disturb existing imports).

- [ ] **Step 4: Export `RegistryHandle`**

`crates/orchestrator-core/src/lib.rs` — add `RegistryHandle` to the `pub use registry::{…}` list (keep alphabetical — after `Registry`, before `RegistryConfig`):

```rust
pub use registry::{
    Activation, AgentDefinition, AgentRef, ChainBinding, ConfigSource, NetworkPolicy, Permissions,
    Registry, RegistryConfig, RegistryHandle, ResourceCaps, SkillDef, ToolSpec,
};
```

- [ ] **Step 5: Run green + commit**

Run: each new test with a single filter (PASS), `cargo test --workspace` (all pass), `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings` (exit 0).

```bash
git add -A
git commit -m "feat(orchestrator): SP-2 slice 5 (1/3) — RegistryHandle (swappable registry + generation)

RegistryHandle over RwLock<(Arc<Registry>, u64)>: new/current/generation/snapshot +
async reload(source) — atomic, validated (from_config), last-good (swap only on
success; await outside the lock). Additive core type, no ripple."
```

---

## Task 2: Executor wiring — handle + per-run pin

Adds the optional handle, `#[derive(Clone)]`, and the per-run pin so `run`/`start` snapshot the handle once and fence the generation. No handle wired ⇒ byte-identical.

**Files:**
- Modify: `crates/orchestrator/src/executor/mod.rs`
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the failing tests**

Add to `crates/orchestrator/src/executor/tests.rs`. (`agent_registry(chain)` builds a `Registry` with agent `"a"` on chain `"c"`; `recording_gateway()` drives it; `agent_node("n1","a",..)` is the node helper; `orchestrator_core::RegistryHandle` + `orchestrator_store::InMemoryConfigSource` are available.)

```rust
#[tokio::test]
async fn reload_bumps_the_run_version_and_fences_in_flight_resume() {
    use orchestrator_core::{RegistryHandle, RegistryConfig};
    use orchestrator_store::InMemoryConfigSource;
    // Handle starts with agent "a" (chain "c"); executor wired via the handle.
    let handle = RegistryHandle::new(
        Registry::default().with_agent(agent_def("c")),
    );
    let journal = InMemoryJournal::new();
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry_handle(handle.clone());

    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph { nodes: vec![agent_node("n1", "a", "hi")] };
    exec.run(run, &graph).await.expect("run at gen 0");

    // The run recorded the pinned version "v1#cfg0".
    let recorded = journal.load(run).await.unwrap().into_iter().find_map(|(_, e)| match e {
        JournalEvent::RunStarted { version } => Some(version),
        _ => None,
    }).unwrap();
    assert_eq!(recorded, "v1#cfg0");

    // Reload → gen 1. Resuming the gen-0 run on the (now gen-1) executor is fenced.
    handle.reload(&InMemoryConfigSource(RegistryConfig {
        agents: vec![agent_def_cfg("c")], // same agent "a"; any valid config bumps the gen
        skills: vec![], tools: vec![], chain_bindings: vec![],
    })).await.unwrap();
    let err = exec.start(run, &graph).await.expect_err("reload fences the in-flight resume");
    assert!(matches!(
        err,
        OrchestratorError::VersionFenceMismatch { recorded, current }
            if recorded == "v1#cfg0" && current == "v1#cfg1"
    ), "got {err:?}");
}

#[tokio::test]
async fn each_run_pins_the_generation_live_at_its_start() {
    use orchestrator_core::{RegistryHandle, RegistryConfig};
    use orchestrator_store::InMemoryConfigSource;
    let handle = RegistryHandle::new(Registry::default().with_agent(agent_def("c")));
    let journal = InMemoryJournal::new();
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry_handle(handle.clone());
    let graph = Graph { nodes: vec![agent_node("n1", "a", "hi")] };

    let run_a = RunId(uuid::Uuid::new_v4());
    exec.run(run_a, &graph).await.expect("run A @ gen0");
    handle.reload(&InMemoryConfigSource(RegistryConfig {
        agents: vec![agent_def_cfg("c")], skills: vec![], tools: vec![], chain_bindings: vec![],
    })).await.unwrap();
    let run_b = RunId(uuid::Uuid::new_v4());
    exec.run(run_b, &graph).await.expect("run B @ gen1");

    let ver = |r: RunId| {
        let j = journal.clone();
        async move {
            j.load(r).await.unwrap().into_iter().find_map(|(_, e)| match e {
                JournalEvent::RunStarted { version } => Some(version), _ => None,
            }).unwrap()
        }
    };
    assert_eq!(ver(run_a).await, "v1#cfg0", "run A pinned gen 0");
    assert_eq!(ver(run_b).await, "v1#cfg1", "run B pinned gen 1 (live at its start)");
}
```

Add a small `agent_def_cfg` helper near `agent_def` (an owned `AgentDefinition` for a `RegistryConfig`, agent name `"a"`):
```rust
fn agent_def_cfg(chain: &str) -> AgentDefinition {
    agent_def(chain) // agent_def already returns an owned AgentDefinition named "a"
}
```
(If `agent_def` already returns an owned `AgentDefinition` you can use it directly in the `RegistryConfig` and skip `agent_def_cfg` — check its signature; the helper is only to make intent clear.)

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator reload_bumps_the_run_version_and_fences_in_flight_resume` (single filter).
Expected: FAIL to compile — `with_registry_handle` not found. (RED.)

- [ ] **Step 3: Derive Clone + add the handle field + builder**

In `crates/orchestrator/src/executor/mod.rs`:
- Add `#[derive(Clone)]` above `pub struct Executor {`.
- Add a field (after `hooks`): `    /// A hot-reload handle (SP-2 slice 5). When wired, each run pins the handle's\n    /// current registry + config generation at entry. `None` ⇒ the fixed `registry`.\n    handle: Option<RegistryHandle>,`
- In `Executor::new`, initialize `handle: None,` (in the struct literal).
- Add the builder near `with_registry`:
```rust
    /// Wire a hot-reloadable [`RegistryHandle`] (SP-2 slice 5). Each `run`/`start`
    /// pins the handle's current registry + generation; a reload bumps the
    /// generation, folded into the fence version so a run uses one generation.
    pub fn with_registry_handle(mut self, handle: RegistryHandle) -> Self {
        self.handle = Some(handle);
        self
    }
```
- Import `RegistryHandle`: extend the existing `use orchestrator_core::{…}` in mod.rs to include `RegistryHandle` (find the import that already brings in `Registry`).

- [ ] **Step 4: Add the per-run pin (`pinned` helper + `run`/`start` wrappers)**

Add the `pinned` helper to `impl Executor`:
```rust
    /// A per-run clone with the registry + fence version pinned from a
    /// `RegistryHandle` snapshot (handle cleared, so the pinned copy resolves the
    /// fixed registry directly — no double-pin).
    fn pinned(mut self, registry: Arc<Registry>, generation: u64) -> Self {
        self.version = format!("{}#cfg{}", self.version, generation);
        self.registry = registry;
        self.handle = None;
        self
    }
```

Rename the CURRENT `pub async fn run` body to a private `run_inner`, and add a thin public `run` that pins first:
```rust
    pub async fn run(&self, run: RunId, graph: &Graph) -> Result<RunOutcome, OrchestratorError> {
        if let Some(h) = &self.handle {
            let (registry, generation) = h.snapshot();
            return self.clone().pinned(registry, generation).run_inner(run, graph).await;
        }
        self.run_inner(run, graph).await
    }

    async fn run_inner(&self, run: RunId, graph: &Graph) -> Result<RunOutcome, OrchestratorError> {
        // ← the EXISTING body of `run` (validate_dag, append RunStarted{self.version}, drive) verbatim
    }
```

Do the same for `start`: rename its body to private `start_inner`, add a thin public `start` that pins first, and change the fresh-run delegation INSIDE `start_inner` from `self.run(...)` to `self.run_inner(...)` (it's already pinned, so avoid re-checking the handle):
```rust
    pub async fn start(&self, run: RunId, graph: &Graph) -> Result<RunOutcome, OrchestratorError> {
        if let Some(h) = &self.handle {
            let (registry, generation) = h.snapshot();
            return self.clone().pinned(registry, generation).start_inner(run, graph).await;
        }
        self.start_inner(run, graph).await
    }

    async fn start_inner(&self, run: RunId, graph: &Graph) -> Result<RunOutcome, OrchestratorError> {
        graph.validate_dag()?;
        let events = self.journal.load(run).await.map_err(OrchestratorError::Journal)?;
        if events.is_empty() {
            return self.run_inner(run, graph).await; // ← was self.run(...); already pinned
        }
        // ← the REST of the existing `start` body verbatim (version fence, fold, drive)
    }
```
(No other internal caller of `run`/`start` exists — `run_map`/`run_consolidate`/`run_loop` call `drive`/`drive_agent`, not these.)

- [ ] **Step 5: Run the new tests + workspace green**

Run: each new test (single filter) PASS; `cargo test --workspace` (all pass — no-handle path unchanged, `version` stays `"v1"` for existing tests); `cargo fmt --all`; `cargo clippy --workspace --all-targets -- -D warnings` (exit 0). If clippy flags the recursive-looking `run`/`start` (it won't — they call the distinct `_inner`), address per its suggestion.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(orchestrator): SP-2 slice 5 (2/3) — Executor.with_registry_handle + per-run pin

Executor derives Clone + gains an optional RegistryHandle; run/start snapshot the
handle once at entry and delegate to a pinned clone whose version is
\"{base}#cfg{gen}\" (run_inner/start_inner hold the existing bodies). A reload bumps
the generation → resuming an in-flight run is refused loud via the existing
VersionFenceMismatch (one config per run). No handle ⇒ byte-identical."
```

---

## Task 3: End-to-end (agent only after reload) + docs

Proves the live-config path: an executor wired with a handle whose initial registry lacks agent `"a"` fails a run referencing it; after `reload` with a config that has `"a"`, a NEW run drives it to completion. Then updates the feature doc (closes SP-2).

**Files:**
- Test: `crates/orchestrator/src/executor/tests.rs`
- Modify: `docs/features/orchestrator/agents-skills-tools.md`

- [ ] **Step 1: Write the e2e test**

```rust
#[tokio::test]
async fn a_reloaded_agent_becomes_runnable_end_to_end() {
    use orchestrator_core::{RegistryHandle, RegistryConfig};
    use orchestrator_store::InMemoryConfigSource;
    // Handle starts EMPTY — agent "a" does not exist yet.
    let handle = RegistryHandle::new(Registry::default());
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry_handle(handle.clone());
    let n1 = NodeId("n1".into());

    // Before reload: the run references unknown agent "a" → fails.
    let before = exec
        .run(RunId(uuid::Uuid::new_v4()), &Graph { nodes: vec![agent_node("n1", "a", "hi")] })
        .await
        .expect("run returns an outcome");
    assert!(before.failed.is_some(), "unknown agent fails before reload: {before:?}");

    // Reload a config that defines agent "a" on chain "c".
    handle.reload(&InMemoryConfigSource(RegistryConfig {
        agents: vec![agent_def("c")], skills: vec![], tools: vec![], chain_bindings: vec![],
    })).await.expect("reload");

    // After reload: a NEW run resolves and drives agent "a".
    let after = exec
        .run(RunId(uuid::Uuid::new_v4()), &Graph { nodes: vec![agent_node("n1", "a", "hi")] })
        .await
        .expect("run");
    assert!(after.failed.is_none(), "reloaded agent runs: {after:?}");
    assert!(after.outputs.contains_key(&n1));
}
```
(Verify `agent_def` returns an owned `AgentDefinition` named `"a"` usable in a `RegistryConfig`; if its name differs, align the `agent_node`/`RegistryConfig` accordingly. `recording_gateway` knows chain `"c"`.)

- [ ] **Step 2: Run — expect PASS** (Tasks 1-2 implemented the behavior).

Run: `cargo test -p sensei-orchestrator a_reloaded_agent_becomes_runnable_end_to_end` → PASS. If it FAILS, STOP and report BLOCKED with output (do not alter landed code).

- [ ] **Step 3: Mutation-verify the reload is load-bearing**

Hand-edit the test to DELETE the `handle.reload(...)` call (leave the registry empty). Re-run: the `after` run must now also fail (agent still unknown), so `assert!(after.failed.is_none())` FAILS. Then RESTORE the `reload` call by hand (do NOT `git checkout` — the test isn't committed). Re-run → PASS. Report both observations.

- [ ] **Step 4: Update the feature doc**

In `docs/features/orchestrator/agents-skills-tools.md`, add a slice-5 paragraph to the top `> **Status …**` blockquote and update the header status line to include "+ SP-2 slice 5 (SP-2 complete)":

```markdown
> **SP-2 slice 5 — registry hot-reload (closes SP-2):** a `RegistryHandle`
> (`orchestrator-core`) wraps a swappable `Arc<Registry>` + a config generation;
> `reload(source)` is atomic, validated (`Registry::from_config`), and last-good (a
> failed load/validate keeps the old config live). `Executor::with_registry_handle`
> pins the handle's `(registry, generation)` once per run, folding the generation
> into the fence version (`"{base}#cfg{gen}"`). A reload takes effect for NEW runs;
> resuming an in-flight run after a reload is refused loud via the existing
> `VersionFenceMismatch` (one config generation per run). No handle wired ⇒
> byte-identical. **Deferred:** version-pinned resume, an `on_config_reloaded` hook,
> file-watch/auto-reload, and a persistent cross-process config version (SP-DATA
> `config_versions`).
```

- [ ] **Step 5: Run green + commit**

Run: `cargo test --workspace` (all pass), `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings` (exit 0).

```bash
git add -A
git commit -m "feat(orchestrator): SP-2 slice 5 (3/3) — hot-reload e2e + docs (closes SP-2)

End-to-end: an executor wired with a RegistryHandle whose initial registry lacks
agent \"a\" fails the run; after handle.reload(...) a NEW run resolves and drives it
(mutation-verified: dropping the reload keeps it unknown). Feature doc updated —
SP-2 registry phase complete."
```

---

## Self-Review

**1. Spec coverage** (against `2026-08-12-sp2-hot-reload-design.md` §6):
- §6.1 handle basics → Task 1 `registry_handle_new_current_and_generation`.
- §6.2 reload swaps+bumps → Task 1 `registry_handle_reload_swaps_and_bumps_generation`.
- §6.3 validated last-good → Task 1 `registry_handle_reload_is_validated_and_last_good`.
- §6.4 executor uses live config (agent only after reload) → Task 3 `a_reloaded_agent_becomes_runnable_end_to_end`.
- §6.5 version fence on reload → Task 2 `reload_bumps_the_run_version_and_fences_in_flight_resume`.
- §6.6 per-run pin (each run stamps its gen) → Task 2 `each_run_pins_the_generation_live_at_its_start`.
- §6.7 no-handle byte-identical → Task 2 Step 5 (whole workspace green; existing tests keep `version == "v1"`, no `#cfg`).
All covered.

**2. Placeholder scan:** No TBD/TODO; every code step is complete; the `run`/`start` refactor names the exact bodies to move (`run_inner`/`start_inner`) and the one internal call site to repoint.

**3. Type consistency:** `RegistryHandle::{new, current, generation, snapshot, reload}` with `reload(&self, source: &dyn ConfigSource) -> Result<u64, OrchestratorError>`; `Executor::with_registry_handle(handle)`; `pinned(registry: Arc<Registry>, generation: u64)`; version format `"{base}#cfg{gen}"`; fence via the existing `OrchestratorError::VersionFenceMismatch { recorded, current }`. Used identically across Tasks 1-3.

**4. Green-per-commit:** Task 1 additive core type (no ripple). Task 2 adds the field + Clone + pin (no-handle path byte-identical). Task 3 additive test + docs.
