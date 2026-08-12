# SP-2 slice 3 — tool permission declarations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the two-sided tool permission *declaration* layer — a `Permissions` type on `ToolSpec` (tool needs) + `AgentDefinition.grants` (agent scope), with a static `Registry::validate` grant⊇need check — with NO runtime enforcement (that is SP-4).

**Architecture:** A single `Permissions` value type (paths/commands/network/caps, secure-default deny) with a pure `covers(grant, need)` predicate lives in `orchestrator-core`. Tools declare needs in `tools/*.json`; agents declare per-tool grants in a central, auditable `<root>/grants.json` merged by `FilesystemConfigSource`. `validate` rejects any agent whose grant doesn't cover a referenced tool's declared needs. Declarations are inert metadata — not in the prompt, not in `agent_input_hash`, executor tool-runtime untouched.

**Tech Stack:** Rust workspace (`orchestrator-core`, `orchestrator`, `orchestrator-store`); `serde`/`serde_json`; `cargo test`/`clippy`. Spec: `docs/superpowers/specs/2026-08-12-sp2-tool-permissions-design.md`.

**House rules (every task):**
- Pre-commit hook = `make lint` (fmt-check + workspace `clippy -D warnings`), NO tests → always `cargo fmt --all` then `cargo test --workspace` before committing.
- Verify the REAL exit code (never a piped `| tail`). "Green" = the actual `cargo test`/`cargo clippy` result.
- Commit a fix BEFORE any `git checkout`-based mutation-verify.
- Branch `feat/sp2-tool-permissions` (created; spec committed at `9051030`). Crate `-p` names: `sensei-orchestrator-core`, `sensei-orchestrator`, `sensei-orchestrator-store`.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `crates/orchestrator-core/src/registry.rs` | registry types + validation | `Permissions`/`NetworkPolicy`/`ResourceCaps` + `covers`; `ToolSpec.permissions`; `AgentDefinition.grants`; `validate` coverage check; `from_frontmatter` grants default. |
| `crates/orchestrator-core/src/error.rs` | error taxonomy | `PermissionNotGranted { agent, tool }`. |
| `crates/orchestrator-core/src/lib.rs` | exports | export `Permissions`, `NetworkPolicy`, `ResourceCaps`. |
| `crates/orchestrator-store/src/config_source.rs` | filesystem/in-memory config | load `<root>/grants.json`, merge into `AgentDefinition.grants`. |
| `crates/orchestrator/src/agent/tools.rs` + `agent/prompt.rs` + `executor/tests.rs` | ToolSpec/AgentDefinition literals | mechanical `permissions:`/`grants:` field additions. |
| `docs/features/orchestrator/agents-skills-tools.md` | feature doc | slice-3 status note. |

---

## Task 1: `Permissions` type + `covers` predicate (additive, no ripple)

Purely additive new types in `orchestrator-core` — no existing struct changes, so the workspace stays green with zero ripple. Delivers the value type and the coverage logic the later tasks consume.

**Files:**
- Modify: `crates/orchestrator-core/src/registry.rs` (add types + `impl`, and unit tests)
- Modify: `crates/orchestrator-core/src/lib.rs:29-32` (exports)

- [ ] **Step 1: Write the failing `covers` tests**

Add to the `#[cfg(test)] mod tests` block in `crates/orchestrator-core/src/registry.rs`:

```rust
    #[test]
    fn permissions_covers_each_dimension() {
        // paths: prefix covers; non-prefix fails.
        let grant = Permissions { paths: vec!["/workspace".into()], ..Default::default() };
        let need = Permissions { paths: vec!["/workspace/src/main.rs".into()], ..Default::default() };
        assert!(grant.covers(&need), "prefix grant covers deeper need");
        let bad = Permissions { paths: vec!["/etc/passwd".into()], ..Default::default() };
        assert!(!grant.covers(&bad), "non-prefix path not covered");

        // commands: subset covers; extra needed fails.
        let g = Permissions { commands: vec!["ls".into(), "cat".into()], ..Default::default() };
        assert!(g.covers(&Permissions { commands: vec!["ls".into()], ..Default::default() }));
        assert!(!g.covers(&Permissions { commands: vec!["rm".into()], ..Default::default() }));

        // network: Any ⊇ all; Hosts ⊇ subset & ⊇ Deny; Deny only ⊇ Deny.
        let any = Permissions { network: NetworkPolicy::Any, ..Default::default() };
        let hosts = Permissions { network: NetworkPolicy::Hosts(vec!["a.com".into(), "b.com".into()]), ..Default::default() };
        let deny = Permissions::default(); // network defaults to Deny
        assert!(any.covers(&hosts) && any.covers(&deny));
        assert!(hosts.covers(&Permissions { network: NetworkPolicy::Hosts(vec!["a.com".into()]), ..Default::default() }));
        assert!(hosts.covers(&deny));
        assert!(!hosts.covers(&any), "Hosts grant does not cover Any need");
        assert!(!deny.covers(&hosts), "Deny grant does not cover Hosts need");
        assert!(deny.covers(&Permissions::default()), "Deny covers Deny");

        // caps: need ≤ grant covers; need > grant fails; grant None = unlimited; need None trivially covered.
        let capped = Permissions { caps: ResourceCaps { mem_bytes: Some(1000), ..Default::default() }, ..Default::default() };
        assert!(capped.covers(&Permissions { caps: ResourceCaps { mem_bytes: Some(500), ..Default::default() }, ..Default::default() }));
        assert!(!capped.covers(&Permissions { caps: ResourceCaps { mem_bytes: Some(2000), ..Default::default() }, ..Default::default() }));
        let uncapped = Permissions::default(); // caps all None
        assert!(uncapped.covers(&Permissions { caps: ResourceCaps { mem_bytes: Some(9999), ..Default::default() }, ..Default::default() }),
            "grant None cap = unlimited, covers any need");
        assert!(capped.covers(&Permissions::default()), "need None cap trivially covered");

        // empty needs covered by anything (incl. an empty grant).
        assert!(Permissions::default().covers(&Permissions::default()));
    }
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator-core permissions_covers`
Expected: FAIL to compile — `Permissions`/`NetworkPolicy`/`ResourceCaps`/`covers` not found. (RED.)

- [ ] **Step 3: Add the types + `covers`**

In `crates/orchestrator-core/src/registry.rs`, add near the other public types (e.g. after `ToolSpec`):

```rust
/// A capability declaration — used BOTH as a tool's required needs
/// (`ToolSpec.permissions`) and an agent's per-tool grant
/// (`AgentDefinition.grants[tool]`). Secure default: deny/empty everything.
/// Declarations only this slice — runtime enforcement is SP-4.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Permissions {
    #[serde(default)]
    pub paths: Vec<String>,
    #[serde(default)]
    pub commands: Vec<String>,
    #[serde(default)]
    pub network: NetworkPolicy,
    #[serde(default)]
    pub caps: ResourceCaps,
}

/// Network egress policy. Default `Deny` (secure).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NetworkPolicy {
    Deny,
    Hosts(Vec<String>),
    Any,
}

impl Default for NetworkPolicy {
    fn default() -> Self {
        NetworkPolicy::Deny
    }
}

/// Resource ceilings; `None` = unbounded (on a grant) / no requirement (on a need).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourceCaps {
    #[serde(default)]
    pub cpu_ms: Option<u64>,
    #[serde(default)]
    pub mem_bytes: Option<u64>,
    #[serde(default)]
    pub wall_ms: Option<u64>,
}

impl Permissions {
    /// Does `self` (an agent's grant) cover `need` (a tool's declared needs)?
    /// paths: each need is prefixed by some grant path. commands: needed ⊆ granted.
    /// network/caps: see [`NetworkPolicy::covers`]/[`ResourceCaps::covers`].
    pub fn covers(&self, need: &Permissions) -> bool {
        need.paths
            .iter()
            .all(|p| self.paths.iter().any(|g| p.starts_with(g)))
            && need.commands.iter().all(|c| self.commands.contains(c))
            && self.network.covers(&need.network)
            && self.caps.covers(&need.caps)
    }
}

impl NetworkPolicy {
    /// `Any` covers all; `Hosts(G)` covers `Hosts(N)` iff N⊆G and covers `Deny`;
    /// `Deny` covers only `Deny`.
    fn covers(&self, need: &NetworkPolicy) -> bool {
        match (self, need) {
            (NetworkPolicy::Any, _) => true,
            (_, NetworkPolicy::Deny) => true,
            (NetworkPolicy::Hosts(g), NetworkPolicy::Hosts(n)) => n.iter().all(|h| g.contains(h)),
            _ => false,
        }
    }
}

impl ResourceCaps {
    fn covers(&self, need: &ResourceCaps) -> bool {
        cap_covers(self.cpu_ms, need.cpu_ms)
            && cap_covers(self.mem_bytes, need.mem_bytes)
            && cap_covers(self.wall_ms, need.wall_ms)
    }
}

/// A single cap dimension: no requirement → covered; grant `None` = unlimited →
/// covers any need; else the grant ceiling must be ≥ the need.
fn cap_covers(grant: Option<u64>, need: Option<u64>) -> bool {
    match (grant, need) {
        (_, None) => true,
        (None, Some(_)) => true,
        (Some(g), Some(n)) => g >= n,
    }
}
```

- [ ] **Step 4: Export the new types**

`crates/orchestrator-core/src/lib.rs` — add to the `pub use registry::{…}` list (keep alphabetical):

```rust
pub use registry::{
    AgentDefinition, AgentRef, ChainBinding, ConfigSource, NetworkPolicy, Permissions, Registry,
    RegistryConfig, ResourceCaps, SkillDef, ToolSpec,
};
```

- [ ] **Step 5: Run green + commit**

Run: `cargo test -p sensei-orchestrator-core permissions_covers` (PASS), then `cargo test --workspace` (all pass), `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings` (exit 0).

```bash
git add -A
git commit -m "feat(orchestrator): SP-2 slice 3 (1/4) — Permissions type + covers predicate

Permissions (paths/commands/network/caps, secure-default deny) + NetworkPolicy +
ResourceCaps + Permissions::covers (grant⊇need): path-prefix, command subset,
network Any/Hosts/Deny lattice, caps need≤grant with grant-None=unlimited.
Additive types only — no field changes, no ripple."
```

---

## Task 2: Attach permissions to `ToolSpec`/`AgentDefinition` + `validate` check + ripple

Adds the two fields, the coverage check in `validate`, the new error, and fixes every construction-site ripple so the whole workspace compiles in one green commit.

**Files:**
- Modify: `crates/orchestrator-core/src/error.rs` (after `UnknownChainRef`, ~line 34)
- Modify: `crates/orchestrator-core/src/registry.rs` (`ToolSpec`, `AgentDefinition`, `from_frontmatter`, `validate`)
- Modify (ripple): every `ToolSpec {` and `AgentDefinition {` literal (see Step 6)
- Test: `crates/orchestrator-core/src/registry.rs`

- [ ] **Step 1: Add the error variant**

`crates/orchestrator-core/src/error.rs`, after the `UnknownChainRef` variant:

```rust
    #[error("agent {agent:?} references tool {tool:?} without a grant covering its declared permissions")]
    PermissionNotGranted { agent: String, tool: String },
```

- [ ] **Step 2: Write the failing `validate` coverage tests**

Add to `crates/orchestrator-core/src/registry.rs` tests (uses the existing `role_agent` helper + a new tool-with-needs builder):

```rust
    fn tool_needing(name: &str, need: Permissions) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            description: None,
            input_schema: serde_json::json!({}),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
            permissions: need,
        }
    }

    #[test]
    fn validate_requires_a_grant_covering_a_tools_declared_needs() {
        let need = Permissions { paths: vec!["/workspace".into()], ..Default::default() };
        let tool = tool_needing("fs.write", need.clone());

        // Agent references the tool but grants nothing → PermissionNotGranted.
        let mut agent = role_agent("coding", "reasoning", Some("c"));
        agent.tools = vec!["fs.write".into()];
        let reg = Registry::default().with_agent(agent.clone()).with_tool(tool.clone());
        assert!(matches!(
            reg.validate(),
            Err(OrchestratorError::PermissionNotGranted { agent, tool })
                if agent == "role" && tool == "fs.write"
        ));

        // With a covering grant → ok.
        agent.grants.insert("fs.write".into(), Permissions { paths: vec!["/workspace".into()], ..Default::default() });
        let ok = Registry::default().with_agent(agent).with_tool(tool);
        assert!(ok.validate().is_ok());
    }

    #[test]
    fn validate_needs_no_grant_for_a_permissionless_tool() {
        // A tool with default (empty) permissions requires no grant.
        let mut agent = role_agent("coding", "reasoning", Some("c"));
        agent.tools = vec!["calc".into()];
        let reg = Registry::default()
            .with_agent(agent)
            .with_tool(tool_needing("calc", Permissions::default()));
        assert!(reg.validate().is_ok());
    }
```

- [ ] **Step 3: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator-core validate_requires_a_grant validate_needs_no_grant`
Expected: FAIL to compile — `ToolSpec` has no field `permissions`, `AgentDefinition` has no field `grants`, `PermissionNotGranted` may be unused. (RED.)

- [ ] **Step 4: Add the two fields**

In `crates/orchestrator-core/src/registry.rs`, add to `ToolSpec` (after `source`):

```rust
    /// The capabilities this tool declares it needs (§132). Enforcement = SP-4;
    /// this slice only validates an agent's grant covers it.
    #[serde(default)]
    pub permissions: Permissions,
```

Add to `AgentDefinition` (after `chains`):

```rust
    /// Per-tool permission grants (tool name → granted scope, §287). Checked
    /// against each tool's declared `permissions` at load; empty when unused.
    #[serde(default)]
    pub grants: HashMap<String, Permissions>,
```

- [ ] **Step 5: Extend `validate` with the coverage check**

In `crates/orchestrator-core/src/registry.rs`, replace the existing tool-ref loop inside `validate` with one that also checks coverage:

```rust
            for tool in &agent.tools {
                let spec = match self.tools.get(tool) {
                    Some(s) => s,
                    None => {
                        return Err(OrchestratorError::UnknownToolRef {
                            agent: agent.name.clone(),
                            tool: tool.clone(),
                        });
                    }
                };
                // grant⊇need: a missing grant is treated as the (deny/empty) default,
                // which covers a permissionless tool but not one that declares needs.
                let grant = agent.grants.get(tool).cloned().unwrap_or_default();
                if !grant.covers(&spec.permissions) {
                    return Err(OrchestratorError::PermissionNotGranted {
                        agent: agent.name.clone(),
                        tool: tool.clone(),
                    });
                }
            }
```

- [ ] **Step 6: Update `from_frontmatter` + fix every construction-site ripple**

In `from_frontmatter` (`crates/orchestrator-core/src/registry.rs`), add `grants` to the constructor (md agents get grants from `grants.json` via the loader, not frontmatter):

```rust
            chains: optional_pairs(&f, "chains")?,
            grants: HashMap::new(),
            tools: optional_list(&f, "tools"),
```

For **every** `ToolSpec { … }` literal, add `permissions: Permissions::default(),`. Enumerate with `grep -rn "ToolSpec {" crates/orchestrator*/src`. Sites: `registry.rs` (the `tool_spec` test helper ~line 463 — note the new `tool_needing` helper from Step 2 already sets `permissions`), `crates/orchestrator/src/agent/prompt.rs:115`, `crates/orchestrator/src/agent/tools.rs` (the `spec()` bodies for `Calc` ~80, `Search` ~132, and the other two at ~177 and ~298). Each needs `permissions: orchestrator_core::Permissions::default(),` (or `Permissions::default()` where already imported).

For **every** `AgentDefinition { … }` literal, add `grants: std::collections::HashMap::new(),`. Enumerate with `grep -rn "AgentDefinition {" crates/orchestrator*/src`. Sites: `registry.rs` `role_agent` helper (~580; note `from_frontmatter` was handled above), `crates/orchestrator/src/agent/prompt.rs:93`, `crates/orchestrator/src/executor/tests.rs` at the `agent_def` helper (~55) and inline literals at ~90, 254, 306, 354, 1041, 1931, 2511, 2628, 2784, 3050.

Worked example — the `agent_def` helper in `executor/tests.rs`:

```rust
fn agent_def(chain: &str) -> AgentDefinition {
    AgentDefinition {
        name: "a".into(),
        area: "research".into(),
        kind: "reasoning".into(),
        chain: Some(chain.into()),
        chains: std::collections::HashMap::new(),
        grants: std::collections::HashMap::new(),
        tools: vec![],
        skills: vec![],
        system_prompt: "SYS".into(),
    }
}
```

Import `Permissions` where a `ToolSpec` literal lives: `crates/orchestrator/src/agent/tools.rs` and `agent/prompt.rs` should `use orchestrator_core::Permissions;` (or reference it fully-qualified). In `registry.rs` the tests are in-module, so `Permissions` is in scope via `use super::*`.

- [ ] **Step 7: Run green + commit**

Run: `cargo test -p sensei-orchestrator-core validate_requires_a_grant validate_needs_no_grant` (PASS), then `cargo test --workspace` (all pass — existing tools/agents route byte-identically: permissionless tools need no grant), `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings` (exit 0).

```bash
git add -A
git commit -m "feat(orchestrator): SP-2 slice 3 (2/4) — ToolSpec.permissions + AgentDefinition.grants + validate

ToolSpec gains declared permissions (needs); AgentDefinition gains per-tool grants;
validate rejects an agent whose grant doesn't cover a referenced tool's needs
(PermissionNotGranted) — a missing grant = deny default, which still covers a
permissionless tool. Both fields #[serde(default)] (additive). Inert: no prompt/
hash/executor impact."
```

---

## Task 3: `FilesystemConfigSource` loads & merges `grants.json`

Loads the central `<root>/grants.json` policy file and merges each agent's grants into its `AgentDefinition`.

**Files:**
- Modify: `crates/orchestrator-store/src/config_source.rs`
- Test: `crates/orchestrator-store/src/config_source.rs` (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module in `crates/orchestrator-store/src/config_source.rs` (the `temp_config_root()` helper writes a valid `researcher` agent):

```rust
    #[tokio::test]
    async fn filesystem_merges_grants_json_into_agents() {
        use orchestrator_core::NetworkPolicy;
        let root = temp_config_root();
        write(
            &root,
            "grants.json",
            r#"{"researcher":{"calc":{"paths":["/workspace"],"network":"Any"}}}"#,
        );
        let cfg = FilesystemConfigSource::new(&root).load().await.expect("loads");
        let agent = cfg.agents.iter().find(|a| a.name == "researcher").expect("agent");
        let grant = agent.grants.get("calc").expect("grant for calc");
        assert_eq!(grant.paths, vec!["/workspace".to_string()]);
        assert_eq!(grant.network, NetworkPolicy::Any);
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn missing_grants_json_leaves_agents_ungranted() {
        let root = temp_config_root(); // no grants.json
        let cfg = FilesystemConfigSource::new(&root).load().await.expect("loads");
        assert!(cfg.agents.iter().all(|a| a.grants.is_empty()));
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn malformed_grants_json_is_a_loud_registry_load_error() {
        let root = temp_config_root();
        write(&root, "grants.json", "{ not json");
        let err = FilesystemConfigSource::new(&root).load().await;
        assert!(
            matches!(&err, Err(OrchestratorError::RegistryLoad(m)) if m.contains("grants.json")),
            "got {err:?}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn grants_for_an_unknown_agent_is_loud() {
        let root = temp_config_root();
        write(&root, "grants.json", r#"{"ghost":{"calc":{}}}"#);
        let err = FilesystemConfigSource::new(&root).load().await;
        assert!(
            matches!(&err, Err(OrchestratorError::RegistryLoad(m)) if m.contains("ghost")),
            "got {err:?}"
        );
        std::fs::remove_dir_all(&root).ok();
    }
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator-store grants`
Expected: FAIL — grants not populated (merge test), unknown-agent/malformed return `Ok` (no grants.json read yet). (RED.)

- [ ] **Step 3: Add the import + the merge step**

In `crates/orchestrator-store/src/config_source.rs`, extend the `orchestrator_core` import to add `Permissions`:

```rust
use orchestrator_core::{
    AgentDefinition, ChainBinding, ConfigSource, OrchestratorError, Permissions, RegistryConfig,
    SkillDef, ToolSpec,
};
```

In `load()`, after the `chains.json` step and before `Ok(cfg)` (uses the existing `read_optional_file` helper from slice 2):

```rust
        if let Some(json) = read_optional_file(&self.root, "grants.json")? {
            use std::collections::HashMap;
            let all: HashMap<String, HashMap<String, Permissions>> = serde_json::from_str(&json)
                .map_err(|e| OrchestratorError::RegistryLoad(format!("parse grants.json: {e}")))?;
            for (agent_name, grants) in all {
                let agent = cfg
                    .agents
                    .iter_mut()
                    .find(|a| a.name == agent_name)
                    .ok_or_else(|| {
                        OrchestratorError::RegistryLoad(format!(
                            "grants.json names unknown agent: {agent_name}"
                        ))
                    })?;
                agent.grants = grants;
            }
        }
```

- [ ] **Step 4: Run green + commit**

Run: `cargo test -p sensei-orchestrator-store grants` (PASS), `cargo test --workspace` (all pass), `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings` (exit 0).

```bash
git add -A
git commit -m "feat(orchestrator): SP-2 slice 3 (3/4) — FilesystemConfigSource loads grants.json

Central <root>/grants.json ({agent:{tool:Permissions}}) merged into
AgentDefinition.grants; missing ⇒ ungranted; malformed ⇒ loud RegistryLoad naming
the file; a grant for an unknown agent ⇒ loud RegistryLoad (fail-loud policy line)."
```

---

## Task 4: End-to-end (declared+granted, inert) + docs

Proves the full stack: a `from_config` registry with a tool that declares needs and an agent that grants a covering scope validates and drives a normal turn (declarations are inert — the tool runtime is unchanged); removing the grant fails `from_config`. Then updates the feature doc.

**Files:**
- Test: `crates/orchestrator/src/executor/tests.rs`
- Modify: `docs/features/orchestrator/agents-skills-tools.md`

- [ ] **Step 1: Write the e2e test**

Add to `crates/orchestrator/src/executor/tests.rs`:

```rust
#[tokio::test]
async fn granted_tool_permissions_are_inert_end_to_end() {
    use orchestrator_core::{NetworkPolicy, Permissions, RegistryConfig, ToolSpec};
    // Tool "calc" DECLARES a path+network need; agent GRANTS a covering scope.
    let tool = ToolSpec {
        name: "calc".into(),
        description: None,
        input_schema: serde_json::json!({}),
        effect_class: orchestrator_core::EffectClass::Pure,
        ttl_secs: None,
        source: None,
        permissions: Permissions { paths: vec!["/workspace".into()], network: NetworkPolicy::Any, ..Default::default() },
    };
    let mut agent = agent_def("c");
    agent.tools = vec!["calc".into()];
    agent.grants.insert(
        "calc".into(),
        Permissions { paths: vec!["/workspace".into()], network: NetworkPolicy::Any, ..Default::default() },
    );
    let cfg = RegistryConfig { agents: vec![agent], skills: vec![], tools: vec![tool], chain_bindings: vec![] };
    let registry = Arc::new(Registry::from_config(cfg).expect("assembles + validates (grant covers need)"));

    // The agent runs a normal turn; declarations don't gate anything (SP-4 does).
    let (gateway, _calls) = recording_gateway().await; // final response, no tool_calls
    let n1 = NodeId("n1".into());
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(registry);
    let outcome = exec
        .run(RunId(uuid::Uuid::new_v4()), &Graph { nodes: vec![agent_node("n1", "a", "hi")] })
        .await
        .expect("run");
    assert!(outcome.failed.is_none(), "granted tool runs (declarations inert): {:?}", outcome.failed);
    assert!(outcome.outputs.contains_key(&n1));
}
```

- [ ] **Step 2: Run — expect PASS** (Tasks 1-2 implemented resolution/validation).

Run: `cargo test -p sensei-orchestrator granted_tool_permissions_are_inert` → PASS. If it FAILS, STOP and report BLOCKED (do not alter landed code).

- [ ] **Step 3: Mutation-verify the grant is load-bearing**

Hand-edit the test to remove the `agent.grants.insert(...)` line (so the agent grants nothing). Re-run: `Registry::from_config` must now FAIL (`PermissionNotGranted`), so `.expect("assembles + validates …")` panics → the test FAILS. This proves the grant is what makes it valid. Then RESTORE the line by hand (do NOT `git checkout` — the test isn't committed yet).

Run (grant removed): `cargo test -p sensei-orchestrator granted_tool_permissions_are_inert` → expect FAIL. Restore → PASS.

- [ ] **Step 4: Update the feature doc**

In `docs/features/orchestrator/agents-skills-tools.md`, add a slice-3 paragraph to the top `> **Status …**` blockquote and update the header status line to include "+ SP-2 slice 3":

```markdown
> **SP-2 slice 3 — tool permission declarations:** a tool declares the capabilities
> it needs (`ToolSpec.permissions`: path/command/network allowlists + resource caps,
> secure-default deny) and an agent declares per-tool grants (`AgentDefinition.grants`,
> loaded from a central auditable `<root>/grants.json`). `Registry::validate` rejects
> any agent whose grant does not **cover** a referenced tool's declared needs
> (`PermissionNotGranted`); `Permissions::covers` is the shared predicate (path-prefix,
> command subset, network `Any`/`Hosts`/`Deny` lattice, caps `need ≤ grant` with
> grant-`None` = unlimited). Declarations are **inert** — not in the prompt/hash, tool
> runtime unchanged. **Deferred to SP-4:** runtime enforcement, sandbox/workspace
> isolation, command deny-lists, secret redaction.
```

- [ ] **Step 5: Run green + commit**

Run: `cargo test --workspace` (all pass), `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings` (exit 0).

```bash
git add -A
git commit -m "feat(orchestrator): SP-2 slice 3 (4/4) — declared+granted tool e2e (inert) + docs

End-to-end: a from_config registry with a tool declaring path/network needs and an
agent granting a covering scope validates and drives a normal turn (declarations
inert — tool runtime unchanged); removing the grant fails validate (mutation-verified).
Feature doc updated."
```

---

## Self-Review

**1. Spec coverage** (against `2026-08-12-sp2-tool-permissions-design.md` §7):
- §7.1 `covers` semantics → Task 1 `permissions_covers_each_dimension` (all four dimensions + empty).
- §7.2 serde defaults → covered structurally by `#[serde(default)]` on every field (Task 1/2); the fs merge test (Task 3) round-trips a partial `Permissions` JSON (omitted `commands`/`caps` default).
- §7.3 `validate` grant⊇need → Task 2 `validate_requires_a_grant…` + `validate_needs_no_grant…`.
- §7.4 filesystem `grants.json` → Task 3 (merge, missing, malformed, unknown-agent).
- §7.5 additive → Task 2 Step 7 (whole workspace green; permissionless tools need no grant).
- §7.6 end-to-end + mutation → Task 4.
All covered.

**2. Placeholder scan:** No TBD/TODO; every code step is complete; every ripple site enumerated by line + a `grep` to regenerate.

**3. Type consistency:** `Permissions { paths, commands, network, caps }`, `NetworkPolicy::{Deny,Hosts,Any}`, `ResourceCaps { cpu_ms, mem_bytes, wall_ms }`, `Permissions::covers(&self, need)`, `ToolSpec.permissions`, `AgentDefinition.grants: HashMap<String, Permissions>`, `PermissionNotGranted { agent, tool }` used identically across Tasks 1-4 and the fs merge (`HashMap<String, HashMap<String, Permissions>>`). `validate`'s `grants.get(tool).cloned().unwrap_or_default().covers(&spec.permissions)` matches the `covers` signature. Serde `#[serde(default)]` on all new fields keeps existing JSON/agents parsing.

**4. Green-per-commit:** Task 1 is additive types only (no ripple). Task 2 bundles the two field additions with their full ripple + `validate` in one commit. Tasks 3-4 are additive over green trees.
