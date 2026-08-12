# SP-2 slice 2 — role/kind → chain resolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the orchestrator `Registry` a role→chain resolution layer — an agent declares `(area, kind)` (+ optional explicit `chain` + optional per-phase `chains`), and `Registry::resolve_chain(agent, phase)` yields the concrete gateway chain-id that `drive_agent` routes through.

**Architecture:** Resolution is a pure function in `orchestrator-core` with a strict fallback order (per-phase → explicit `chain` → `(area,kind)` binding → loud `UnknownChainRef`). The `chain` field flips `String → Option<String>` (the only non-additive ripple); explicit-chain agents keep the exact behavior via the override branch. `phase` becomes an optional attribute of an `Agent` node (not a mid-loop transition). The filesystem backend gains a `<root>/chains.json` policy table. Multi-tenancy stays out of the core (composition of per-tenant instances — no code here).

**Tech Stack:** Rust workspace (`orchestrator-core`, `orchestrator`, `orchestrator-store`); `cargo test`/`clippy`; `async-trait`; `serde`/`serde_json`. Spec: `docs/superpowers/specs/2026-08-11-sp2-role-chain-resolution-design.md`.

**House rules (apply to every task):**
- Pre-commit hook runs `make lint` (fmt-check + workspace `clippy -D warnings`) and NO tests → always run `cargo test --workspace` before committing, and `cargo fmt --all` before it.
- Verify the REAL exit code (never a piped `| tail`). "Green" = the actual `cargo test` result.
- Commit a fix BEFORE any `git checkout`-based mutation-verify (a checkout reverts uncommitted edits).
- This slice runs on branch `feat/sp2-role-chain-resolution` (already created; spec committed at `e2acf3f`).

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `crates/orchestrator-core/src/error.rs` | error taxonomy | Add `UnknownChainRef { agent }`. |
| `crates/orchestrator-core/src/registry.rs` | registry types, parser, resolution | `chain: Option<String>`; `chains: HashMap`; `ChainBinding`; `RegistryConfig.chain_bindings`; `Registry.chain_bindings` + `with_chain_binding`/`chain_binding`/`resolve_chain`; `from_config` dup-(area,kind); `validate` routability; `optional_scalar`/`optional_pairs` parse helpers. |
| `crates/orchestrator-core/src/lib.rs` | crate exports | Export `ChainBinding`. |
| `crates/orchestrator-core/src/graph.rs` | node kinds | `NodeKind::Agent` gains `phase: Option<String>`. |
| `crates/orchestrator/src/executor/agent.rs` | ReAct loop | `drive_agent` gains `phase: Option<&str>`; line 65 resolves via `resolve_chain`. |
| `crates/orchestrator/src/executor/mod.rs` | node dispatch | Destructure `phase` in the `Agent` arm; pass `phase.as_deref()` to `drive_agent`. |
| `crates/orchestrator/src/executor/fanout.rs` | Map/Consolidate/Loop | Pass `None` at the 3 `drive_agent` call sites. |
| `crates/orchestrator-store/src/config_source.rs` | filesystem/in-memory config | Load `<root>/chains.json` → `chain_bindings`; fix a stale fixture. |
| `crates/orchestrator/src/agent/prompt.rs` + `crates/orchestrator/src/executor/tests.rs` | test literals | Mechanical `chain: Some(..)` + `chains`/`phase`/`chain_bindings` field additions. |
| `docs/features/orchestrator/agents-skills-tools.md` | feature doc | Status note for slice 2. |

---

## Task 1: Core role→chain resolution (types + parser + resolver + ripple)

Delivers the whole `orchestrator-core` resolution layer in one green commit. Because flipping `chain: String → Option<String>` breaks `executor/agent.rs:65` at compile time, this task also rewires that one read site to `resolve_chain(agent, None)` (the `phase` argument becomes real in Task 2) and fixes every construction-site ripple so the **workspace** stays green.

**Files:**
- Modify: `crates/orchestrator-core/src/error.rs` (after line 30, the `UnknownToolRef` variant)
- Modify: `crates/orchestrator-core/src/registry.rs`
- Modify: `crates/orchestrator-core/src/lib.rs:29-31`
- Modify: `crates/orchestrator/src/executor/agent.rs:60-66`
- Modify (ripple): `crates/orchestrator/src/agent/prompt.rs:93`; `crates/orchestrator/src/executor/tests.rs` (lines 55, 88, 219, 906, 1796, 2375, 2491, 2646, 2910); `crates/orchestrator-core/src/registry.rs` `RegistryConfig` test literals (lines 378, 391, 403, 418, 440); `crates/orchestrator-store/src/config_source.rs:121`
- Test: unit tests inside `crates/orchestrator-core/src/registry.rs`

- [ ] **Step 1: Add the error variant**

In `crates/orchestrator-core/src/error.rs`, immediately after the `UnknownToolRef` variant (line 30):

```rust
    #[error("agent {agent:?} has no resolvable chain (no explicit chain, no (area,kind) binding)")]
    UnknownChainRef { agent: String },
```

- [ ] **Step 2: Write the failing resolution + parser tests**

Add to the `#[cfg(test)] mod tests` block in `crates/orchestrator-core/src/registry.rs` (they will FAIL TO COMPILE first — that is the RED for a type-driven change):

```rust
    fn role_agent(area: &str, kind: &str, chain: Option<&str>) -> AgentDefinition {
        AgentDefinition {
            name: "role".into(),
            area: area.into(),
            kind: kind.into(),
            chain: chain.map(|c| c.into()),
            chains: std::collections::HashMap::new(),
            tools: vec![],
            skills: vec![],
            system_prompt: "SYS".into(),
        }
    }

    #[test]
    fn resolve_chain_prefers_phase_then_explicit_then_binding_then_errors() {
        // Binding table: (research, reasoning) -> "bound".
        let reg = Registry::default().with_chain_binding(ChainBinding {
            area: "research".into(),
            kind: "reasoning".into(),
            chain: "bound".into(),
        });

        // 1. per-phase override wins.
        let mut phased = role_agent("research", "reasoning", Some("explicit"));
        phased.chains.insert("plan".into(), "phase-chain".into());
        assert_eq!(reg.resolve_chain(&phased, Some("plan")).unwrap(), "phase-chain");

        // 2. explicit `chain` wins when the phase key is absent.
        assert_eq!(reg.resolve_chain(&phased, Some("nope")).unwrap(), "explicit");
        assert_eq!(reg.resolve_chain(&phased, None).unwrap(), "explicit");

        // 3. (area,kind) binding when there is no explicit chain.
        let bound_only = role_agent("research", "reasoning", None);
        assert_eq!(reg.resolve_chain(&bound_only, None).unwrap(), "bound");

        // 4. nothing resolves -> loud UnknownChainRef naming the agent.
        let orphan = role_agent("misc", "misc", None);
        assert!(matches!(
            reg.resolve_chain(&orphan, None),
            Err(OrchestratorError::UnknownChainRef { agent }) if agent == "role"
        ));
    }

    #[test]
    fn from_frontmatter_parses_optional_chain_and_phase_chains() {
        // chain omitted -> None; chains inline pairs -> map.
        let md = "---\nname: n\narea: a\nkind: k\nchains: [plan=plan.frontier, execute=code.mid]\n---\nbody\n";
        let ag = AgentDefinition::from_frontmatter(md).unwrap();
        assert_eq!(ag.chain, None);
        assert_eq!(ag.chains.get("plan").map(String::as_str), Some("plan.frontier"));
        assert_eq!(ag.chains.get("execute").map(String::as_str), Some("code.mid"));

        // explicit chain still parses to Some.
        let md2 = "---\nname: n\narea: a\nkind: k\nchain: c\n---\nb\n";
        assert_eq!(AgentDefinition::from_frontmatter(md2).unwrap().chain.as_deref(), Some("c"));
    }

    #[test]
    fn from_frontmatter_malformed_phase_pair_errors() {
        // A `chains` element without '=' is a loud parse error.
        let md = "---\nname: n\narea: a\nkind: k\nchains: [bad]\n---\nb\n";
        assert!(matches!(
            AgentDefinition::from_frontmatter(md),
            Err(OrchestratorError::FrontmatterParse(_))
        ));
    }

    #[test]
    fn from_config_rejects_duplicate_area_kind_binding() {
        let cfg = RegistryConfig {
            agents: vec![],
            skills: vec![],
            tools: vec![],
            chain_bindings: vec![
                ChainBinding { area: "coding".into(), kind: "reasoning".into(), chain: "a".into() },
                ChainBinding { area: "coding".into(), kind: "reasoning".into(), chain: "b".into() },
            ],
        };
        assert!(matches!(
            Registry::from_config(cfg),
            Err(OrchestratorError::RegistryLoad(m)) if m.contains("duplicate chain binding") && m.contains("coding")
        ));
    }

    #[test]
    fn validate_rejects_an_agent_with_no_resolvable_chain() {
        // Agent omits chain and there is no (area,kind) binding -> unroutable.
        let reg = Registry::default().with_agent(role_agent("x", "y", None));
        assert!(matches!(
            reg.validate(),
            Err(OrchestratorError::UnknownChainRef { agent }) if agent == "role"
        ));
        // With a matching binding it validates.
        let ok = Registry::default()
            .with_agent(role_agent("x", "y", None))
            .with_chain_binding(ChainBinding { area: "x".into(), kind: "y".into(), chain: "c".into() });
        assert!(ok.validate().is_ok());
    }
```

- [ ] **Step 3: Run the tests to confirm they fail**

Run: `cargo test -p sensei-orchestrator-core registry 2>&1 | tail -20`
Expected: FAIL to compile — `ChainBinding` / `with_chain_binding` / `resolve_chain` / the `chains` field not found, and `chain` type mismatch. (This is the RED.)

- [ ] **Step 4: Add the types + parse helpers + resolver**

In `crates/orchestrator-core/src/registry.rs`:

(a) Change `AgentDefinition` (lines 16-25) — `chain` optional, add `chains`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentDefinition {
    pub name: String,
    pub area: String,
    pub kind: String,
    /// An explicit gateway chain-id override. `None` → resolve via the
    /// `(area,kind)` binding table. See [`Registry::resolve_chain`].
    pub chain: Option<String>,
    /// Per-phase chain overrides (phase → chain-id); empty when unused.
    pub chains: HashMap<String, String>,
    pub tools: Vec<String>,
    pub skills: Vec<String>,
    pub system_prompt: String,
}
```

(b) Add `ChainBinding` and the `RegistryConfig` field. After the `ToolSpec` struct (before `RegistryConfig`, ~line 49):

```rust
/// A registry role binding: `(area, kind)` → a gateway chain-id. The policy
/// table that lets one edit re-point every agent of that role (§122).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainBinding {
    pub area: String,
    pub kind: String,
    pub chain: String,
}
```

Add to `RegistryConfig` (after `tools`):

```rust
    pub chain_bindings: Vec<ChainBinding>,
```

(c) Add the storage field to `Registry` (after `tools: HashMap<...>`):

```rust
    chain_bindings: HashMap<(String, String), String>,
```

(d) Add builder + getter + resolver to `impl Registry` (beside `with_tool`/`tool`):

```rust
    pub fn with_chain_binding(mut self, b: ChainBinding) -> Self {
        self.chain_bindings.insert((b.area, b.kind), b.chain);
        self
    }
    pub fn chain_binding(&self, area: &str, kind: &str) -> Option<&str> {
        self.chain_bindings
            .get(&(area.to_string(), kind.to_string()))
            .map(String::as_str)
    }

    /// Resolve an agent's concrete gateway chain-id for an optional phase.
    /// Order: per-phase override → explicit `chain` → `(area,kind)` binding →
    /// loud `UnknownChainRef`. A phase key the agent does not define is NOT an
    /// error — it falls through.
    pub fn resolve_chain(
        &self,
        agent: &AgentDefinition,
        phase: Option<&str>,
    ) -> Result<&str, OrchestratorError> {
        if let Some(p) = phase
            && let Some(c) = agent.chains.get(p)
        {
            return Ok(c);
        }
        if let Some(c) = agent.chain.as_deref() {
            return Ok(c);
        }
        if let Some(c) = self.chain_binding(&agent.area, &agent.kind) {
            return Ok(c);
        }
        Err(OrchestratorError::UnknownChainRef {
            agent: agent.name.clone(),
        })
    }
```

(e) In `from_config`, after the `tools` loop and before `reg.validate()`, add the binding loop with dup detection:

```rust
        for b in cfg.chain_bindings {
            if reg.chain_binding(&b.area, &b.kind).is_some() {
                return Err(OrchestratorError::RegistryLoad(format!(
                    "duplicate chain binding: {}/{}",
                    b.area, b.kind
                )));
            }
            reg = reg.with_chain_binding(b);
        }
```

(f) In `validate`, inside the `for agent in self.agents.values()` loop (after the tool-ref check, before the closing brace), add the routability check:

```rust
            if agent.chain.is_none() && self.chain_binding(&agent.area, &agent.kind).is_none() {
                return Err(OrchestratorError::UnknownChainRef {
                    agent: agent.name.clone(),
                });
            }
```

(g) Add parse helpers near `optional_list` (~line 226):

```rust
fn optional_scalar(map: &HashMap<String, FmValue>, key: &str) -> Option<String> {
    match map.get(key) {
        Some(FmValue::Scalar(s)) if !s.is_empty() => Some(s.clone()),
        _ => None,
    }
}

/// Parse an inline `[k=v, k=v]` list into a map (the flat controlled subset —
/// no nesting). A member without '=', or with an empty key/value, is loud.
fn optional_pairs(
    map: &HashMap<String, FmValue>,
    key: &str,
) -> Result<HashMap<String, String>, OrchestratorError> {
    let mut out = HashMap::new();
    if let Some(FmValue::List(items)) = map.get(key) {
        for item in items {
            let (k, v) = item.split_once('=').ok_or_else(|| {
                OrchestratorError::FrontmatterParse(format!("{key} entry missing '=': {item}"))
            })?;
            let (k, v) = (k.trim(), v.trim());
            if k.is_empty() || v.is_empty() {
                return Err(OrchestratorError::FrontmatterParse(format!(
                    "{key} entry has empty key/value: {item}"
                )));
            }
            out.insert(k.to_string(), v.to_string());
        }
    }
    Ok(out)
}
```

(h) Update `AgentDefinition::from_frontmatter` (the constructor, ~line 238):

```rust
        Ok(AgentDefinition {
            name: required_scalar(&f, "name")?,
            area: required_scalar(&f, "area")?,
            kind: required_scalar(&f, "kind")?,
            chain: optional_scalar(&f, "chain"),
            chains: optional_pairs(&f, "chains")?,
            tools: optional_list(&f, "tools"),
            skills: optional_list(&f, "skills"),
            system_prompt: body.to_string(),
        })
```

- [ ] **Step 5: Export `ChainBinding`**

`crates/orchestrator-core/src/lib.rs:29-31` — add `ChainBinding` to the `registry` re-export (keep alphabetical):

```rust
pub use registry::{
    AgentDefinition, AgentRef, ChainBinding, ConfigSource, Registry, RegistryConfig, SkillDef,
    ToolSpec,
};
```

- [ ] **Step 6: Rewire the one executor read site**

`crates/orchestrator/src/executor/agent.rs`, line 65 — replace `let chain = agent.chain.clone();` with a resolution call (the `phase` argument is `None` until Task 2):

```rust
        let chain = self.registry.resolve_chain(agent, None)?.to_string();
```

- [ ] **Step 7: Fix every construction-site ripple (mechanical, keeps the workspace compiling)**

For **each** `AgentDefinition { … }` literal, two edits: change `chain: <x>.into()` → `chain: Some(<x>.into())`, and add `chains: std::collections::HashMap::new(),`. Sites (enumerate with `grep -rn "AgentDefinition {" crates/orchestrator*/src`):
- `crates/orchestrator/src/agent/prompt.rs:93`
- `crates/orchestrator/src/executor/tests.rs`: the `agent_def` helper (line 55: `chain: chain.into()` → `chain: Some(chain.into())`), and inline literals at lines 88, 219, 906, 1796, 2375, 2491, 2646, 2910.

Worked example — the `agent_def` helper (tests.rs:54-63) becomes:

```rust
fn agent_def(chain: &str) -> AgentDefinition {
    AgentDefinition {
        name: "a".into(),
        area: "research".into(),
        kind: "reasoning".into(),
        chain: Some(chain.into()),
        chains: std::collections::HashMap::new(),
        tools: vec![],
        skills: vec![],
        system_prompt: "SYS".into(),
    }
}
```

For **each** `RegistryConfig { … }` test literal, add `chain_bindings: vec![],`. Sites (`grep -rn "RegistryConfig {" crates/orchestrator*/src`, excluding the struct def at registry.rs:54): `registry.rs` lines 378, 391, 403, 418, 440; `config_source.rs:121`.

- [ ] **Step 8: Run the whole workspace green**

Run: `cargo test --workspace 2>&1 | tail -25`
Expected: all pass (the Step-2 tests now pass; every prior test still passes — explicit-chain agents route byte-identically). Then `cargo fmt --all` and `cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -5` (Expected: `Finished`, no warnings).

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "feat(orchestrator): SP-2 slice 2 (1/4) — role→chain resolver + chain: Option<String>

Registry::resolve_chain(agent, phase) with fallback phase → explicit chain →
(area,kind) binding → loud UnknownChainRef; ChainBinding + RegistryConfig.chain_bindings;
from_config rejects duplicate (area,kind); validate now checks routability. chain
flips String→Option (explicit-chain agents route byte-identically). from_frontmatter
gains optional chain + inline phase-pairs (chains: [plan=x, execute=y])."
```

---

## Task 2: Phase as an `Agent` node attribute (executor plumbing)

Adds `NodeKind::Agent.phase` and threads it into `drive_agent`, so an `Agent` node can select a per-phase chain. Fan-out bodies stay phase-less (`None`). New behavior: a node with `phase=Some("plan")` on an agent whose only route is `chains["plan"]` drives successfully; the same node with `phase=None` fails to resolve.

**Files:**
- Modify: `crates/orchestrator-core/src/graph.rs:15-18` (the `Agent` variant)
- Modify: `crates/orchestrator/src/executor/agent.rs` (`drive_agent` signature + the `None` from Task 1)
- Modify: `crates/orchestrator/src/executor/mod.rs:593-597` (destructure + pass `phase`)
- Modify: `crates/orchestrator/src/executor/fanout.rs:164, 250, 410` (pass `None`)
- Modify (ripple): `crates/orchestrator/src/executor/tests.rs` `agent_node` (~line 73) and `agent_node_with_deps` (~line 2888) helpers
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the failing phase-routing test**

Add to `crates/orchestrator/src/executor/tests.rs`:

```rust
#[tokio::test]
async fn agent_node_phase_selects_the_per_phase_chain() {
    // Agent has NO explicit chain and NO (area,kind) binding — only chains["plan"] = "c".
    // So it is routable ONLY when the node requests phase "plan".
    let (gateway, _calls) = recording_gateway().await; // knows chain "c"
    let mut agent = agent_def("c");
    agent.chain = None;
    agent.chains.insert("plan".into(), "c".into());
    let registry = Arc::new(Registry::default().with_agent(agent));
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(registry);

    // Node in phase "plan" resolves chains["plan"]="c" and completes.
    let node = Node {
        id: NodeId("n1".into()),
        kind: NodeKind::Agent {
            agent: AgentRef("a".into()),
            input: serde_json::json!("hi"),
            phase: Some("plan".into()),
        },
        deps: vec![],
    };
    let outcome = exec
        .run(RunId(uuid::Uuid::new_v4()), &Graph { nodes: vec![node] })
        .await
        .expect("run");
    assert!(outcome.failed.is_none(), "phase route completes: {:?}", outcome.failed);
}
```

- [ ] **Step 2: Run it to confirm failure**

Run: `cargo test -p sensei-orchestrator agent_node_phase_selects 2>&1 | tail -20`
Expected: FAIL to compile — `NodeKind::Agent` has no field `phase`. (RED.)

- [ ] **Step 3: Add the `phase` field to the node**

`crates/orchestrator-core/src/graph.rs`, the `Agent` variant (lines 15-18):

```rust
    Agent {
        agent: crate::registry::AgentRef,
        input: serde_json::Value,
        /// Optional phase selecting a per-phase chain (`AgentDefinition::chains`);
        /// `None` resolves the agent's default chain. A node attribute, fixed for
        /// the run — not a mid-loop transition.
        phase: Option<String>,
    },
```

- [ ] **Step 4: Thread `phase` through `drive_agent`**

`crates/orchestrator/src/executor/agent.rs` — add the parameter to `drive_agent` (after `fold`):

```rust
    pub(super) async fn drive_agent(
        &self,
        run: RunId,
        node_id: &NodeId,
        agent_ref: &AgentRef,
        input: &serde_json::Value,
        context: &[(ContextKey, serde_json::Value)],
        fold: &Fold,
        phase: Option<&str>,
    ) -> Result<AgentStep, OrchestratorError> {
```

And change the Task-1 line 65 resolution from `None` to `phase`:

```rust
        let chain = self.registry.resolve_chain(agent, phase)?.to_string();
```

- [ ] **Step 5: Update the call sites**

`crates/orchestrator/src/executor/mod.rs:593-597` — destructure `phase` and pass it:

```rust
            NodeKind::Agent { agent, input, phase } => {
                let context = self.resolve_context(node).await?;
                match self
                    .drive_agent(run, &node.id, agent, input, &context, fold, phase.as_deref())
                    .await?
```

`crates/orchestrator/src/executor/fanout.rs` — the 3 `drive_agent(...)` calls (lines 164, 250, 410) each get a trailing `None` argument (fan-out bodies are phase-less):

```rust
                    .drive_agent(run, &node.id, agent_ref, &input, &[], fold, None)
```
```rust
                            .drive_agent(run, &NodeId(path.clone()), agent_ref, item, &[], fold, None)
```
(and the multiline call at ~410 — add `None` as the final argument).

- [ ] **Step 6: Fix the node-construction ripple**

`crates/orchestrator/src/executor/tests.rs` — add `phase: None,` to the `NodeKind::Agent { … }` literals in the `agent_node` helper (~line 73) and `agent_node_with_deps` (~line 2891). Example (`agent_node`):

```rust
        kind: NodeKind::Agent {
            agent: AgentRef(agent.into()),
            input: serde_json::json!(input),
            phase: None,
        },
```

- [ ] **Step 7: Run the new test + the workspace green**

Run: `cargo test -p sensei-orchestrator agent_node_phase_selects 2>&1 | tail -5` (Expected: PASS)
Run: `cargo test --workspace 2>&1 | tail -25` (Expected: all pass)
Then `cargo fmt --all` + `cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -3`.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat(orchestrator): SP-2 slice 2 (2/4) — NodeKind::Agent.phase → per-phase chain

Agent node carries an optional phase (node attribute, not a mid-loop transition);
drive_agent takes phase: Option<&str> and resolves the per-phase chain. Fan-out
bodies stay phase-less (None). New test: phase=Some(plan) routes chains[plan]."
```

---

## Task 3: Filesystem `chains.json` policy table

Loads the `(area,kind)→chain` table from `<root>/chains.json` in `FilesystemConfigSource`; a missing file → empty table; a malformed file → loud `RegistryLoad`. Also fixes a fixture in the store tests that relied on `chain` being required (it is now optional).

**Files:**
- Modify: `crates/orchestrator-store/src/config_source.rs`
- Test: `crates/orchestrator-store/src/config_source.rs` (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing chains.json tests**

Add to the `tests` module in `crates/orchestrator-store/src/config_source.rs`:

```rust
    #[tokio::test]
    async fn filesystem_loads_chain_bindings_from_chains_json() {
        let root = temp_config_root();
        write(
            &root,
            "chains.json",
            r#"[{"area":"research","kind":"reasoning","chain":"research.bulk"}]"#,
        );
        let cfg = FilesystemConfigSource::new(&root).load().await.expect("loads");
        assert_eq!(cfg.chain_bindings.len(), 1);
        assert_eq!(cfg.chain_bindings[0].area, "research");
        assert_eq!(cfg.chain_bindings[0].chain, "research.bulk");
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn missing_chains_json_is_an_empty_table() {
        let root = temp_config_root(); // has no chains.json
        let cfg = FilesystemConfigSource::new(&root).load().await.expect("loads");
        assert!(cfg.chain_bindings.is_empty());
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn malformed_chains_json_is_a_loud_registry_load_error() {
        let root = temp_config_root();
        write(&root, "chains.json", "{ not an array");
        let err = FilesystemConfigSource::new(&root).load().await;
        assert!(
            matches!(&err, Err(OrchestratorError::RegistryLoad(m)) if m.contains("chains.json")),
            "got {err:?}"
        );
        std::fs::remove_dir_all(&root).ok();
    }
```

Note: `write(&root, "chains.json", …)` writes at the config-root level (the `write` helper joins `dir.join(name)`), not under a subdir — `chains.json` is a single top-level file.

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator-store chains_json 2>&1 | tail -20`
Expected: FAIL — `cfg.chain_bindings` populated as empty (assert on len==1 fails) / the malformed case returns `Ok` (no chains.json read yet). (RED.)

- [ ] **Step 3: Add the ChainBinding import + a single-file reader + the load step**

In `crates/orchestrator-store/src/config_source.rs`, extend the core import (line 7-9) to include `ChainBinding`:

```rust
use orchestrator_core::{
    AgentDefinition, ChainBinding, ConfigSource, OrchestratorError, RegistryConfig, SkillDef,
    ToolSpec,
};
```

Add a helper beside `read_dir_files`:

```rust
/// Read an optional top-level `<root>/<name>` file. Missing → `None`; any other
/// I/O error is loud.
fn read_optional_file(root: &Path, name: &str) -> Result<Option<String>, OrchestratorError> {
    let path = root.join(name);
    match std::fs::read_to_string(&path) {
        Ok(s) => Ok(Some(s)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(OrchestratorError::RegistryLoad(format!(
            "read {}: {e}",
            path.display()
        ))),
    }
}
```

In `load()`, after the `tools` loop and before `Ok(cfg)`:

```rust
        if let Some(json) = read_optional_file(&self.root, "chains.json")? {
            cfg.chain_bindings = serde_json::from_str::<Vec<ChainBinding>>(&json).map_err(|e| {
                OrchestratorError::RegistryLoad(format!("parse chains.json: {e}"))
            })?;
        }
```

- [ ] **Step 4: Fix the stale `malformed_agent_md_error_names_the_file` fixture**

That test (config_source.rs ~217) uses a `broken.md` that omits `chain:` — now valid (chain is optional). Change its fixture to omit the still-required `name:` so it stays a parse error:

```rust
        // Missing the required `name:` key → a parse error that must name the file.
        write(
            &root.join("agents"),
            "broken.md",
            "---\narea: a\nkind: k\n---\nb\n",
        );
```

- [ ] **Step 5: Run the store tests + workspace green**

Run: `cargo test -p sensei-orchestrator-store 2>&1 | tail -10` (Expected: all pass, incl. the 3 new + the fixed fixture test)
Run: `cargo test --workspace 2>&1 | tail -20` (Expected: all pass)
Then `cargo fmt --all` + `cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -3`.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(orchestrator): SP-2 slice 2 (3/4) — FilesystemConfigSource loads chains.json

<root>/chains.json ([{area,kind,chain}]) → RegistryConfig.chain_bindings; missing
file ⇒ empty table; malformed ⇒ loud RegistryLoad naming the file. Fix a store
fixture that relied on chain being required (now optional)."
```

---

## Task 4: End-to-end (table-routed agent) + docs

Proves the full stack: a `from_config`-assembled registry whose agent OMITS `chain` routes via the `(area,kind)` table through the real gateway harness, and drives a real turn. Then updates the feature doc.

**Files:**
- Test: `crates/orchestrator/src/executor/tests.rs`
- Modify: `docs/features/orchestrator/agents-skills-tools.md`

- [ ] **Step 1: Write the failing e2e test**

Add to `crates/orchestrator/src/executor/tests.rs`:

```rust
#[tokio::test]
async fn agent_routes_via_area_kind_binding_end_to_end() {
    use orchestrator_core::{ChainBinding, RegistryConfig};
    // Agent omits chain; the (research,reasoning) binding maps it to "c".
    let agent = AgentDefinition {
        name: "a".into(),
        area: "research".into(),
        kind: "reasoning".into(),
        chain: None,
        chains: std::collections::HashMap::new(),
        tools: vec![],
        skills: vec![],
        system_prompt: "SYS".into(),
    };
    let cfg = RegistryConfig {
        agents: vec![agent],
        skills: vec![],
        tools: vec![],
        chain_bindings: vec![ChainBinding {
            area: "research".into(),
            kind: "reasoning".into(),
            chain: "c".into(),
        }],
    };
    let registry = Arc::new(Registry::from_config(cfg).expect("assembles + validates"));

    let (gateway, _calls) = recording_gateway().await; // only knows chain "c"
    let n1 = NodeId("n1".into());
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(registry);
    let outcome = exec
        .run(
            RunId(uuid::Uuid::new_v4()),
            &Graph { nodes: vec![agent_node("n1", "a", "hi")] },
        )
        .await
        .expect("run");

    // Resolution reached the gateway's only chain "c" and completed. A wrong/absent
    // resolution would have failed at resolve time or at an unknown-chain gateway call.
    assert!(outcome.failed.is_none(), "table-routed agent completes: {:?}", outcome.failed);
    assert!(outcome.outputs.contains_key(&n1));
}
```

- [ ] **Step 2: Run to confirm it passes (resolution already implemented in Tasks 1-3)**

Run: `cargo test -p sensei-orchestrator agent_routes_via_area_kind_binding 2>&1 | tail -10`
Expected: PASS. (This is an integration guard over Tasks 1-2; it exercises `from_config` → table → executor as one path. If it fails, the resolution wiring is wrong — fix before proceeding.)

- [ ] **Step 3: Mutation-verify the e2e is load-bearing**

Confirm the test actually depends on the binding: temporarily delete the `chain_bindings` entry (make it `vec![]`) in the test — `Registry::from_config` must now fail (`UnknownChainRef` at validate), so the test panics on `.expect("assembles + validates")`. Restore it.

```bash
# after editing chain_bindings to vec![] in the test body:
cargo test -p sensei-orchestrator agent_routes_via_area_kind_binding 2>&1 | tail -5   # Expected: FAIL (validate rejects)
git checkout crates/orchestrator/src/executor/tests.rs                                 # restore (test is committed only in Step 5)
```
(If the test file has uncommitted edits from earlier steps you want to keep, revert only the `chain_bindings` line by hand instead of `git checkout`.)

- [ ] **Step 4: Update the feature doc**

In `docs/features/orchestrator/agents-skills-tools.md`, update the status blockquote to note slice 2. Replace the `SP-1 slice 2 + SP-2 slice 1` phrasing in the `> **Status …**` line and add a sentence:

```markdown
> **SP-2 slice 2 — role/kind → chain resolution:** an agent declares `(area, kind)`
> (plus an optional explicit `chain` and an optional per-phase `chains` map) and
> `Registry::resolve_chain(agent, phase)` yields the concrete gateway chain-id
> (order: per-phase → explicit → `(area,kind)` binding → loud `UnknownChainRef`).
> `chain` is now optional; the `(area,kind)` policy table loads from
> `<root>/chains.json`. Phase is an `Agent`-node attribute (not a mid-loop
> transition). **Deferred:** tiers (gateway-catalog), planner-driven phase
> transitions, tenant dimension (multi-tenancy is by composition — per-tenant
> `Executor` = per-tenant `Gateway` + tenant-scoped `ConfigSource`).
```

Also update the top-of-file `spec: SP-1, SP-2` line's status prose if it still says "slice 1" only.

- [ ] **Step 5: Run the workspace green + commit**

Run: `cargo test --workspace 2>&1 | tail -20` (Expected: all pass)
Then `cargo fmt --all` + `cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -3`.

```bash
git add -A
git commit -m "feat(orchestrator): SP-2 slice 2 (4/4) — table-routed agent e2e + docs

End-to-end: a from_config registry whose agent omits chain routes via the
(area,kind) binding through the gateway harness and drives a real turn
(mutation-verified: removing the binding fails validate). Feature doc updated."
```

---

## Self-Review

**1. Spec coverage** (against `2026-08-11-sp2-role-chain-resolution-design.md` §7 acceptance):
- §7.1 resolution order → Task 1 Step 2 `resolve_chain_prefers_phase_then_explicit_then_binding_then_errors`.
- §7.2 phase fall-through → same test (`resolve_chain(&phased, Some("nope"))` → explicit).
- §7.3 load-time guards → Task 1 `from_config_rejects_duplicate_area_kind_binding` + `validate_rejects_an_agent_with_no_resolvable_chain`.
- §7.4 frontmatter → Task 1 `from_frontmatter_parses_optional_chain_and_phase_chains` + `..._malformed_phase_pair_errors`.
- §7.5 filesystem → Task 3 three `chains_json` tests.
- §7.6 executor e2e → Task 2 (phase route) + Task 4 (table route through the gateway).
- §7.7 additive behavior → Task 1 Step 8 (whole workspace green; explicit-chain agents unchanged).
All covered.

**2. Placeholder scan:** No TBD/TODO; every code step shows complete code; every ripple site is enumerated by exact line + a `grep` to regenerate the list.

**3. Type consistency:** `resolve_chain(&self, agent: &AgentDefinition, phase: Option<&str>) -> Result<&str, _>` is used identically in Task 1 (unit), Task 1 Step 6 / Task 2 Step 4 (`resolve_chain(agent, None|phase)`), and via the executor in Task 4. `ChainBinding { area, kind, chain }`, `RegistryConfig.chain_bindings: Vec<ChainBinding>`, `Registry.chain_bindings: HashMap<(String,String),String>`, `with_chain_binding`/`chain_binding` names match across Tasks 1/3/4. `NodeKind::Agent { agent, input, phase }` matches between graph.rs (Task 2 Step 3), the mod.rs arm (Task 2 Step 5), and every node literal (Tasks 1/2/4). `chain: Option<String>` / `chains: HashMap<String,String>` consistent across all construction sites.

**4. Ordering / green-per-commit:** Task 1 bundles the `chain→Option` flip with `resolve_chain` and the full ripple (incl. `agent.rs:65`) so the workspace compiles at the commit boundary; Tasks 2-4 are each additive over a green tree.
