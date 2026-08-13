# SP-3 slice 4A — Planner agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn slice-3's injected `Planner` seam into a real **journaled ReAct planner agent** that produces a validated, right-sized, self-describing plan — with a `NodePlan` metadata side-map, a pure `feasible()`/`validate_plan` validator, Pure discovery tools, and an `on_plan_expanded` hook.

**Architecture:** The planner runs as a `drive_agent` sub-run inside `run_expand` under `"{expand}/__plan__"` (turns are journaled Pure effects → mid-plan crash replays from the memo). It emits `PlannedGraph{graph, node_plans}` JSON; `run_expand` validates it, journals `PlanExpanded{node, subgraph, node_plans}`, and splices via the slice-3 `drive_nested`. Plan metadata is a **journaled side-map** (`PlanExpanded.node_plans`), so core `Node`/`Graph` are untouched.

**Tech Stack:** Rust, `orchestrator-core` (types + `feasible`/`parse_plan`) + `orchestrator` (executor + tools) + `orchestrator-store` (`InMemoryJournal`), `async_trait`, `tokio`, `serde_json`.

**Design spec:** `docs/superpowers/specs/2026-08-13-sp3-planner-agent-design.md` · **Overview:** `docs/superpowers/orchestrator-overview.md`

**Conventions (ops memory):** `cargo fmt --all` before every commit (pre-commit hook = fmt-check + `clippy -D warnings`, runs **no** tests). Always run `cargo test` yourself and read the **real** exit code (never a piped `| tail`/`grep`; multi-filter needs `-- name1 name2`). `crates/orchestrator/src/executor/` is a directory module. Commit on `develop`; don't `git add` `docs/`.

---

## File Structure

- `crates/orchestrator-core/src/plan.rs` **(create)** — `NodePlan`/`NodeNeeds`/`PlannedGraph`/`PlanError` + `parse_plan` + `feasible`.
- `crates/orchestrator-core/src/lib.rs` **(modify)** — `pub mod plan;` + re-exports.
- `crates/orchestrator-core/src/graph.rs` **(modify)** — `PlannerRef` enum; `NodeKind::Expand` gains `planner`.
- `crates/orchestrator-core/src/registry.rs` **(modify)** — `agents()`/`skills()`/`tools()`/`chain_names()` enumeration accessors (for the discovery tools + feasibility).
- `crates/orchestrator-core/src/journal.rs` **(modify)** — `PlanExpanded` gains `node_plans`.
- `crates/orchestrator-core/src/hooks.rs` **(modify)** — `on_plan_expanded`.
- `crates/orchestrator/src/executor/mod.rs` **(modify)** — fire `on_plan_expanded` in `append`; `Expand` dispatch already exists.
- `crates/orchestrator/src/executor/support.rs` **(modify)** — `fold_journal` ignores `node_plans` (`..`).
- `crates/orchestrator/src/executor/expand.rs` **(modify)** — dispatch on `PlannerRef`; the agent-backed branch.
- `crates/orchestrator/src/agent/tools.rs` **(modify)** — the 5 discovery tools.
- `crates/orchestrator/src/executor/tests.rs` **(modify)** — acceptance tests + a reference planner registration.

---

## Task 1: Core plan types + feasibility (pure, no LLM)

**Files:**
- Create: `crates/orchestrator-core/src/plan.rs`
- Modify: `crates/orchestrator-core/src/lib.rs`, `crates/orchestrator-core/src/registry.rs`

- [ ] **Step 1: Add Registry enumeration accessors**

In `crates/orchestrator-core/src/registry.rs`, add to `impl Registry` (near `agent`/`skill`/`tool`):

```rust
    /// Enumerate all agent definitions (for planner discovery + feasibility).
    pub fn agents(&self) -> impl Iterator<Item = &AgentDefinition> {
        self.agents.values()
    }
    /// Enumerate all skill definitions.
    pub fn skills(&self) -> impl Iterator<Item = &SkillDef> {
        self.skills.values()
    }
    /// Enumerate all tool specs.
    pub fn tools(&self) -> impl Iterator<Item = &ToolSpec> {
        self.tools.values()
    }
    /// Distinct chain ids the registry references (agent `chain`, per-phase
    /// `chains`, and `(area,kind)` bindings), sorted. Best-effort menu — the full
    /// gateway catalog is not registry-visible.
    pub fn chain_names(&self) -> Vec<String> {
        let mut set = std::collections::BTreeSet::new();
        for a in self.agents.values() {
            if let Some(c) = &a.chain {
                set.insert(c.clone());
            }
            for c in a.chains.values() {
                set.insert(c.clone());
            }
        }
        for c in self.chain_bindings.values() {
            set.insert(c.clone());
        }
        set.into_iter().collect()
    }
```

- [ ] **Step 2: Write the failing plan-types tests**

Create `crates/orchestrator-core/src/plan.rs` with ONLY this test module first (the types come next step), so the test drives the API:

```rust
#[cfg(test)]
mod tests {
    use super::*;                                   // NodeId comes via super::* (private in graph.rs)
    use crate::graph::{Dep, Graph, Node, NodeKind};
    use crate::registry::{AgentDefinition, Registry};
    use std::collections::HashMap;

    fn agent_reg() -> Registry {
        Registry::default().with_agent(AgentDefinition {
            name: "researcher".into(),
            area: "research".into(),
            kind: "reasoning".into(),
            chain: Some("research.bulk".into()),
            chains: HashMap::new(),
            grants: HashMap::new(),
            tools: vec![],
            skills: vec![],
            system_prompt: "r".into(),
        })
    }
    fn mc(id: &str, dep: Option<&str>) -> Node {
        Node {
            id: NodeId(id.into()),
            kind: NodeKind::ModelCall { chain: "research.bulk".into(), payload: serde_json::json!({}) },
            deps: dep.map(|d| vec![Dep::hard(d)]).unwrap_or_default(),
        }
    }

    #[test]
    fn parse_plan_roundtrips_a_planned_graph() {
        let text = r#"{"graph":{"nodes":[{"id":"n1","kind":{"ModelCall":{"chain":"research.bulk","payload":{}}},"deps":[]}]},"node_plans":{"n1":{"label":"do it"}}}"#;
        let plan = parse_plan(text).expect("parses");
        assert_eq!(plan.graph.nodes.len(), 1);
        assert_eq!(plan.node_plans[&NodeId("n1".into())].label, "do it");
    }

    #[test]
    fn parse_plan_without_node_plans_defaults_empty() {
        let text = r#"{"graph":{"nodes":[]}}"#;
        let plan = parse_plan(text).expect("parses");
        assert!(plan.node_plans.is_empty());
    }

    #[test]
    fn parse_plan_rejects_malformed_json() {
        assert!(matches!(parse_plan("not json"), Err(PlanError::Parse(_))));
    }

    #[test]
    fn feasible_accepts_a_clean_plan() {
        let plan = PlannedGraph {
            graph: Graph { nodes: vec![mc("n1", None)] },
            node_plans: HashMap::new(),
        };
        assert!(feasible(&plan, &agent_reg(), 512).is_ok());
    }

    #[test]
    fn feasible_reports_all_error_classes() {
        // dangling agent (via NodePlan.needs), reserved id, cycle, over-cap.
        let mut node_plans = HashMap::new();
        node_plans.insert(
            NodeId("n1".into()),
            NodePlan { label: "x".into(), description: None,
                needs: NodeNeeds { agents: vec!["ghost".into()], ..Default::default() } },
        );
        let plan = PlannedGraph {
            graph: Graph { nodes: vec![
                Node { id: NodeId("__plan__".into()), kind: mc("z", None).kind, deps: vec![] },
                mc("n1", None),
            ] },
            node_plans,
        };
        let errs = feasible(&plan, &agent_reg(), 1).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, PlanError::UnknownAgent(a) if a == "ghost")));
        assert!(errs.iter().any(|e| matches!(e, PlanError::ReservedNodeId(_))));
        assert!(errs.iter().any(|e| matches!(e, PlanError::TooManyNodes { .. })));
    }

    #[test]
    fn feasible_reports_a_structural_cycle() {
        let plan = PlannedGraph {
            graph: Graph { nodes: vec![mc("a", Some("b")), mc("b", Some("a"))] },
            node_plans: HashMap::new(),
        };
        let errs = feasible(&plan, &agent_reg(), 512).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, PlanError::Structural(_))));
    }
}
```

- [ ] **Step 3: Run to verify it fails (compile error — types missing)**

Run: `cargo test -p sensei-orchestrator-core --lib plan::` — Expected: FAIL to compile (`parse_plan`/`feasible`/`PlannedGraph`/… not found).

- [ ] **Step 4: Implement the plan types + parse + feasible**

Prepend to `crates/orchestrator-core/src/plan.rs` (above the test module):

```rust
//! Plan types + the pure validator (SP-3 slice 4A). A planner emits a
//! `PlannedGraph` (the executable `Graph` + a `NodePlan` metadata side-map);
//! `parse_plan` + `feasible` are the deterministic gate used both by the planner's
//! `validate_plan` tool and by `run_expand` before journaling `PlanExpanded`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::graph::{Graph, NodeKind};
use crate::ids::NodeId;                             // NodeId is private in graph.rs — import from ids
use crate::registry::Registry;

/// Path segment reserved for the planner sub-run (`"{expand}/__plan__"`); a plan
/// node may not use it as its id (would collide once namespaced).
pub const RESERVED_PLAN_ID: &str = "__plan__";

/// Human-meaningful plan metadata for one node (viz + tracking + declared needs).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct NodePlan {
    pub label: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub needs: NodeNeeds,
}

/// A node's declared requirements — doubles as its planner-selected activation set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct NodeNeeds {
    #[serde(default)]
    pub skills: Vec<String>,
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default)]
    pub agents: Vec<String>,
    #[serde(default)]
    pub self_discover: bool,
}

/// What a planner emits: the executable graph + its per-node metadata (local ids).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedGraph {
    pub graph: Graph,
    #[serde(default)]
    pub node_plans: HashMap<NodeId, NodePlan>,
}

/// A feasibility/parse error over a produced plan.
#[derive(Debug, Clone, PartialEq)]
pub enum PlanError {
    Parse(String),
    Structural(String),
    UnknownAgent(String),
    UnknownSkill(String),
    UnknownTool(String),
    ReservedNodeId(String),
    TooManyNodes { count: usize, limit: usize },
}

/// Parse planner output JSON into a `PlannedGraph`.
pub fn parse_plan(text: &str) -> Result<PlannedGraph, PlanError> {
    serde_json::from_str(text).map_err(|e| PlanError::Parse(e.to_string()))
}

/// Pure feasibility: structure (validate_dag) + registry-resolvable refs (Agent
/// nodes + each NodePlan.needs) + reserved-id + node-count. Returns ALL errors.
pub fn feasible(
    plan: &PlannedGraph,
    registry: &Registry,
    max_nodes: usize,
) -> Result<(), Vec<PlanError>> {
    let mut errs = Vec::new();

    if let Err(e) = plan.graph.validate_dag() {
        errs.push(PlanError::Structural(e.to_string()));
    }
    if plan.graph.nodes.len() > max_nodes {
        errs.push(PlanError::TooManyNodes { count: plan.graph.nodes.len(), limit: max_nodes });
    }
    for n in &plan.graph.nodes {
        if n.id.0 == RESERVED_PLAN_ID {
            errs.push(PlanError::ReservedNodeId(n.id.0.clone()));
        }
        if let NodeKind::Agent { agent, .. } = &n.kind
            && registry.agent(&agent.0).is_none()
        {
            errs.push(PlanError::UnknownAgent(agent.0.clone()));
        }
    }
    for np in plan.node_plans.values() {
        for a in &np.needs.agents {
            if registry.agent(a).is_none() {
                errs.push(PlanError::UnknownAgent(a.clone()));
            }
        }
        for s in &np.needs.skills {
            if registry.skill(s).is_none() {
                errs.push(PlanError::UnknownSkill(s.clone()));
            }
        }
        for t in &np.needs.tools {
            if registry.tool(t).is_none() {
                errs.push(PlanError::UnknownTool(t.clone()));
            }
        }
    }

    if errs.is_empty() { Ok(()) } else { Err(errs) }
}
```

- [ ] **Step 5: Export from the core crate**

In `crates/orchestrator-core/src/lib.rs`, add the module (after `pub mod journal;`):

```rust
pub mod plan;
```

and re-export (after the `pub use journal::{…};` block):

```rust
pub use plan::{NodeNeeds, NodePlan, PlanError, PlannedGraph, RESERVED_PLAN_ID, feasible, parse_plan};
```

- [ ] **Step 6: Run to verify all Task-1 tests pass**

Run: `cargo test -p sensei-orchestrator-core --lib plan::` — Expected: all 6 PASS. Verify real exit 0.

- [ ] **Step 7: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator-core/src/plan.rs crates/orchestrator-core/src/lib.rs crates/orchestrator-core/src/registry.rs
git commit -m "feat(orchestrator): SP-3 s4A (1/6) — plan types (NodePlan/PlannedGraph) + parse_plan + feasible + registry enumeration"
```

---

## Task 2: `PlanExpanded.node_plans` + `on_plan_expanded` hook

**Files:**
- Modify: `crates/orchestrator-core/src/journal.rs`, `crates/orchestrator-core/src/hooks.rs`
- Modify: `crates/orchestrator/src/executor/mod.rs`, `crates/orchestrator/src/executor/support.rs`, `crates/orchestrator/src/executor/expand.rs`

- [ ] **Step 1: Add `node_plans` to `PlanExpanded`**

In `crates/orchestrator-core/src/journal.rs`, update the variant (add the import `use crate::plan::NodePlan;` near the top `use crate::graph::Graph;`, and `use std::collections::HashMap;` if not present):

```rust
    PlanExpanded {
        node: NodeId,
        subgraph: Graph,
        /// Per-node plan metadata (local ids) — the self-describing side-map (§4.1).
        /// Serde-default so a pre-4A `PlanExpanded` (no field) still deserializes.
        #[serde(default)]
        node_plans: HashMap<NodeId, NodePlan>,
    },
```

- [ ] **Step 2: Fix the fold + the slice-3 construction sites**

In `crates/orchestrator/src/executor/support.rs`, in `fold_journal`, the existing arm becomes (ignore `node_plans` — the fold only needs the graph):

```rust
            JournalEvent::PlanExpanded { node, subgraph, .. } => {
                fold.expansions.insert(node.clone(), subgraph.clone());
            }
```

In `crates/orchestrator/src/executor/expand.rs`, the slice-3 injected-path `append(PlanExpanded{…})` (in `run_expand`) gains an empty map:

```rust
                self.append(
                    run,
                    JournalEvent::PlanExpanded {
                        node: node.id.clone(),
                        subgraph: produced.clone(),
                        node_plans: std::collections::HashMap::new(),
                    },
                )
                .await?;
```

(Update any other `PlanExpanded { … }` construction the compiler flags — e.g. the Task-1 journal roundtrip test in `journal.rs` if present — to include `node_plans`.)

- [ ] **Step 3: Write the failing hook test**

In `crates/orchestrator/src/executor/tests.rs`, append:

```rust
/// on_plan_expanded fires once with the graph + labels when a plan is journaled.
#[tokio::test]
async fn on_plan_expanded_fires_with_the_plan() {
    use std::sync::{Arc, Mutex};
    struct Spy(Arc<Mutex<Vec<(String, usize, usize)>>>);
    #[async_trait::async_trait]
    impl OrchestratorHooks for Spy {
        async fn on_plan_expanded(
            &self, _run: RunId, node: &NodeId, graph: &Graph,
            node_plans: &std::collections::HashMap<NodeId, orchestrator_core::NodePlan>,
        ) {
            self.0.lock().unwrap().push((node.0.clone(), graph.nodes.len(), node_plans.len()));
        }
    }
    let log = Arc::new(Mutex::new(Vec::new()));
    let (gateway, _c) = recording_gateway().await;
    let planner = Arc::new(FixedPlanner(Graph { nodes: vec![mc("n1", None)] }));
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_planner(planner)
        .with_hooks(Arc::new(Spy(log.clone())));
    let graph = Graph { nodes: vec![expand_node("e", vec![])] };
    exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    let seen = log.lock().unwrap().clone();
    assert_eq!(seen.len(), 1, "on_plan_expanded fired once: {seen:?}");
    assert_eq!(seen[0].0, "e");
    assert_eq!(seen[0].1, 1, "graph carried to the hook");
}
```

(`FixedPlanner`/`expand_node`/`mc` are the slice-3 test helpers; `FixedPlanner` uses the injected `Planner` trait, which journals `PlanExpanded` with an empty `node_plans` — enough to exercise the hook firing.)

- [ ] **Step 4: Run to verify it fails**

Run: `cargo test -p sensei-orchestrator on_plan_expanded_fires_with_the_plan` — Expected: FAIL to compile (`on_plan_expanded` not on the trait).

- [ ] **Step 5: Add the hook method + fire it**

In `crates/orchestrator-core/src/hooks.rs`, add the imports and the method:

```rust
use crate::graph::Graph;
use crate::plan::NodePlan;
use std::collections::HashMap;
```
```rust
    async fn on_plan_expanded(
        &self,
        _run: RunId,
        _node: &NodeId,
        _graph: &Graph,
        _node_plans: &HashMap<NodeId, NodePlan>,
    ) {
    }
```

In `crates/orchestrator/src/executor/mod.rs`, in `append`'s hook `match ev { … }` block, add an arm (after the `ContextWrite` arm):

```rust
                JournalEvent::PlanExpanded { node, subgraph, node_plans } => {
                    h.on_plan_expanded(run, node, subgraph, node_plans).await
                }
```

- [ ] **Step 6: Run to verify the hook test + the suite pass**

Run: `cargo test -p sensei-orchestrator on_plan_expanded_fires_with_the_plan` — Expected: PASS.
Run: `cargo test -p sensei-orchestrator -- expand_` — Expected: the slice-3 `expand_*` tests still PASS (node_plans is additive). Verify real exit 0.

- [ ] **Step 7: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator-core/src/journal.rs crates/orchestrator-core/src/hooks.rs \
        crates/orchestrator/src/executor/mod.rs crates/orchestrator/src/executor/support.rs \
        crates/orchestrator/src/executor/expand.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-3 s4A (2/6) — PlanExpanded.node_plans side-map + on_plan_expanded hook"
```

---

## Task 3: `PlannerRef` + `Expand.planner` field

**Files:**
- Modify: `crates/orchestrator-core/src/graph.rs`, `crates/orchestrator-core/src/lib.rs`
- Modify: `crates/orchestrator/src/executor/expand.rs`, `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Add `PlannerRef` + the `Expand.planner` field**

In `crates/orchestrator-core/src/graph.rs`, add the enum (near `NodeKind`):

```rust
/// How an `Expand` node's plan is produced (SP-3 slice 4A). `Injected` = the
/// slice-3 `Planner` trait (deterministic/test); `Agent` = a journaled ReAct
/// planner agent (this slice). Slice 4B adds `Select` (goal-based selection).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum PlannerRef {
    Agent(crate::registry::AgentRef),
    #[default]
    Injected,
}
```

Change the `Expand` variant to:

```rust
    Expand {
        input: serde_json::Value,
        #[serde(default)]
        planner: PlannerRef,
    },
```

Re-export in `lib.rs` (add `PlannerRef` to the `pub use graph::{…}` line).

- [ ] **Step 2: Fix the construction + destructure sites**

In `crates/orchestrator/src/executor/expand.rs`, `run_expand`'s let-else destructure becomes (it now needs `planner`):

```rust
        let NodeKind::Expand { input, planner } = &node.kind else {
            unreachable!("run_expand on non-Expand node");
        };
```

In `crates/orchestrator/src/executor/tests.rs`, the `expand_node` helper gains the field (default `Injected`, preserving slice-3 behavior):

```rust
fn expand_node(id: &str, deps: Vec<Dep>) -> Node {
    Node {
        id: NodeId(id.into()),
        kind: NodeKind::Expand { input: serde_json::json!({}), planner: orchestrator_core::PlannerRef::Injected },
        deps,
    }
}
```

(Fix any other `NodeKind::Expand { input }` literal the compiler flags with `, planner: PlannerRef::Injected`.)

- [ ] **Step 3: Write a round-trip + default test**

In `crates/orchestrator-core/src/graph.rs` `mod tests`:

```rust
    #[test]
    fn expand_deserializes_without_planner_as_injected() {
        let j = r#"{"Expand":{"input":{}}}"#;
        let k: NodeKind = serde_json::from_str(j).unwrap();
        assert!(matches!(k, NodeKind::Expand { planner: PlannerRef::Injected, .. }));
    }
    #[test]
    fn planner_ref_agent_roundtrips() {
        let r = PlannerRef::Agent(crate::registry::AgentRef("planner".into()));
        let s = serde_json::to_string(&r).unwrap();
        assert_eq!(serde_json::from_str::<PlannerRef>(&s).unwrap(), r);
    }
```

- [ ] **Step 4: Run to verify Task-3 tests + the slice-3 suite pass**

Run: `cargo test -p sensei-orchestrator-core --lib expand_deserializes_without_planner_as_injected planner_ref_agent_roundtrips` — PASS.
Run: `cargo test -p sensei-orchestrator -- expand_ subgraph branch` — Expected: all pass (the field defaults to `Injected` ⇒ slice-3 behavior byte-identical). Verify real exit 0.

- [ ] **Step 5: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator-core/src/graph.rs crates/orchestrator-core/src/lib.rs \
        crates/orchestrator/src/executor/expand.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-3 s4A (3/6) — PlannerRef + NodeKind::Expand.planner (default Injected)"
```

---

## Task 4: The journaled planner-agent branch in `run_expand`

**Files:**
- Modify: `crates/orchestrator/src/executor/expand.rs`
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the failing planner-agent tests**

In `crates/orchestrator/src/executor/tests.rs`, append. These need a planner **agent** that, through a scripted gateway, emits a plan JSON as its final answer. Add helpers + tests:

```rust
/// A registry with a `planner` agent on a plain chain (no tools for the minimal
/// path — the agent's single turn emits the plan JSON directly). The plan itself
/// comes from the scripted gateway, so this helper takes no plan argument.
fn planner_registry() -> Arc<Registry> {
    Arc::new(Registry::default().with_agent(AgentDefinition {
        name: "planner".into(), area: "planning".into(), kind: "reasoning".into(),
        chain: Some("c".into()), chains: std::collections::HashMap::new(),
        grants: std::collections::HashMap::new(), tools: vec![], skills: vec![],
        system_prompt: "Emit a plan as JSON.".into(),
    }))
}

fn expand_agent_node(id: &str, deps: Vec<Dep>) -> Node {
    Node {
        id: NodeId(id.into()),
        kind: NodeKind::Expand {
            input: serde_json::json!({ "goal": "do the thing" }),
            planner: orchestrator_core::PlannerRef::Agent(AgentRef("planner".into())),
        },
        deps,
    }
}

#[tokio::test]
async fn journaled_planner_agent_produces_and_splices_a_plan() {
    // The planner agent emits a 2-node plan (n1 -> n2); the executor splices + runs it.
    let plan_json = r#"{"graph":{"nodes":[
        {"id":"n1","kind":{"ModelCall":{"chain":"c","payload":{"prompt":"n1"}}},"deps":[]},
        {"id":"n2","kind":{"ModelCall":{"chain":"c","payload":{"prompt":"n2"}}},"deps":[{"on":"n1","kind":"Hard"}]}
    ]},"node_plans":{"n1":{"label":"first"},"n2":{"label":"second"}}}"#;
    let reg = planner_registry();
    let (gateway, _c) = scripted_gateway(vec![final_response(plan_json)]).await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1").with_registry(reg);
    let run = RunId(uuid::Uuid::new_v4());
    let e = NodeId("e".into());
    let graph = Graph { nodes: vec![expand_agent_node("e", vec![])] };
    let out = exec.run(run, &graph).await.expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    assert!(out.outputs[&e].get("n2").is_some(), "sink map has n2: {}", out.outputs[&e]);

    // The planner turns are journaled under "e/__plan__/…"; the plan nodes under "e/…".
    let labels: Vec<String> = journal.load(run).await.unwrap().iter()
        .filter_map(|(_, ev)| match ev {
            JournalEvent::NodeStarted { node } => Some(node.0.clone()),
            _ => None,
        }).collect();
    assert!(labels.iter().any(|l| l.starts_with("e/__plan__")), "planner turn journaled: {labels:?}");
    assert!(labels.iter().any(|l| l == "e/n1"), "plan node journaled: {labels:?}");
    assert!(!labels.iter().any(|l| l == "e/__plan__"), "__plan__ is not a plan node");
}

#[tokio::test]
async fn planner_agent_invalid_plan_fails_the_node() {
    let reg = planner_registry();
    let (gateway, _c) = scripted_gateway(vec![final_response("this is not json")]).await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1").with_registry(reg);
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph { nodes: vec![expand_agent_node("e", vec![]), mc_dep("d", Dep::hard("e"))] };
    let out = exec.run(run, &graph).await.expect("run");
    assert!(matches!(&out.failed, Some((n, _)) if n == &NodeId("e".into())), "{out:?}");
    assert!(out.skipped.contains(&NodeId("d".into())), "hard-dependent skipped");
    assert!(!journal.load(run).await.unwrap().iter()
        .any(|(_, ev)| matches!(ev, JournalEvent::PlanExpanded { .. })),
        "no PlanExpanded for an unparseable plan");
}

#[tokio::test]
async fn unresolvable_planner_agent_fails_the_node() {
    // PlannerRef::Agent names an agent NOT in the registry.
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(Arc::new(Registry::default()));
    let graph = Graph { nodes: vec![expand_agent_node("e", vec![])] };
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    assert!(matches!(&out.failed, Some((n, _)) if n == &NodeId("e".into())), "{out:?}");
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p sensei-orchestrator -- journaled_planner_agent_produces_and_splices_a_plan planner_agent_invalid_plan_fails_the_node unresolvable_planner_agent_fails_the_node` — Expected: FAIL (the `Agent` branch of `run_expand` isn't implemented; `PlannerRef::Agent` currently isn't handled, so the fresh path treats it as `Injected`/no-planner and behaves wrong).

- [ ] **Step 3: Implement the `PlannerRef::Agent` branch**

In `crates/orchestrator/src/executor/expand.rs`, in `run_expand`'s **fresh** (`None`) arm, replace the body that produces `produced` with a dispatch on `planner`. The full fresh arm becomes:

```rust
            None => {
                let produced = match planner {
                    orchestrator_core::PlannerRef::Injected => {
                        // slice-3 path: the injected Planner trait.
                        let Some(p) = &self.planner else {
                            return self.expand_failed(run, node, format!("expand {}: no planner wired", node.id.0)).await;
                        };
                        match p.plan(input).await {
                            Ok(g) => PlannedGraph { graph: g, node_plans: std::collections::HashMap::new() },
                            Err(e) => return self.expand_failed(run, node, format!("expand {} planner failed: {e}", node.id.0)).await,
                        }
                    }
                    orchestrator_core::PlannerRef::Agent(agent_ref) => {
                        let plan_node = NodeId(format!("{}/__plan__", node.id.0));
                        match self.drive_agent(run, &plan_node, agent_ref, input, &[], fold, None).await {
                            Ok(AgentStep::Completed(out)) => {
                                let text = out.get("text").and_then(|v| v.as_str()).unwrap_or_default();
                                match orchestrator_core::parse_plan(text) {
                                    Ok(p) => p,
                                    Err(e) => return self.expand_failed(run, node, format!("expand {} plan parse: {e:?}", node.id.0)).await,
                                }
                            }
                            Ok(AgentStep::Failed(msg)) => return self.expand_failed(run, node, format!("expand {} planner agent failed: {msg}", node.id.0)).await,
                            Ok(AgentStep::Paused(r)) => return Ok(NodeExec::Paused { reason: format!("planner {} paused: {r}", node.id.0) }),
                            // An unresolvable planner agent (unknown agent) is a config error → node Failed, not a hard halt.
                            Err(e) => return self.expand_failed(run, node, format!("expand {} planner unavailable: {e}", node.id.0)).await,
                        }
                    }
                };
                if let Err(errs) = orchestrator_core::feasible(&produced, &self.registry, self.max_nodes) {
                    return self.expand_failed(run, node, format!("expand {} infeasible plan: {errs:?}", node.id.0)).await;
                }
                self.check_expansion_budget(&produced.graph)?;
                self.append(run, JournalEvent::PlanExpanded {
                    node: node.id.clone(), subgraph: produced.graph.clone(), node_plans: produced.node_plans,
                }).await?;
                produced.graph
            }
```

Add imports at the top of `expand.rs` as needed: `use orchestrator_core::PlannedGraph;` and ensure `AgentStep` is in scope (`use super::AgentStep;`). The `Injected` sub-branch preserves the slice-3 `validate_dag`→feasibility path — note it now also runs `feasible` (a superset of `validate_dag`), so an injected planner returning a cyclic graph still fails; confirm the slice-3 `expand_invalid_plan_...` test still passes (it should — `feasible` includes the structural check).

- [ ] **Step 4: Run to verify Task-4 + regressions pass**

Run: `cargo test -p sensei-orchestrator -- journaled_planner_agent unresolvable_planner_agent planner_agent_invalid_plan expand_` — Expected: all PASS (new + slice-3 `expand_*`). Verify real exit 0.

- [ ] **Step 5: Write the resume tests (AC5/AC6)**

In `crates/orchestrator/src/executor/tests.rs`, append:

```rust
/// Resume post-PlanExpanded reuses the journaled plan; the planner agent is NOT
/// re-invoked and the plan node is NOT re-spent (mirrors the slice-3
/// `expand_completed_then_failing_tail_resumes_without_replan`, but the planner is
/// an agent). The load-bearing proof: run-2's gateway sees EXACTLY ONE call — `d`.
#[tokio::test]
async fn planner_agent_resume_reuses_journaled_plan() {
    // Single-node plan (ModelCall n1 with prompt "n1"). e (planner agent) -> d (tail).
    let plan_json = r#"{"graph":{"nodes":[{"id":"n1","kind":{"ModelCall":{"chain":"c","payload":{"prompt":"n1"}}},"deps":[]}]}}"#;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph { nodes: vec![expand_agent_node("e", vec![]), mc_dep("d", Dep::hard("e"))] };
    // Run 1: a scripted gateway supplies the planner's plan (call 1) and plan node n1
    // (call 2); it has NO 3rd response, so d's call errors → d fails, leaving PlanExpanded
    // + n1 journaled and NO RunCompleted.
    {
        let (gw_s, _c) = scripted_gateway(vec![
            final_response(plan_json),   // planner turn → the plan
            final_response("n1 out"),    // plan node n1
        ]).await;
        let exec = Executor::new(Arc::new(gw_s), Arc::new(journal.clone()), "v1").with_registry(planner_registry());
        let o1 = exec.run(run, &graph).await.expect("run1");
        assert!(o1.failed.is_some(), "tail d failed: {o1:?}");
    }
    // Run 2: a FRESH recording gateway (succeeds for everything) over the SAME journal.
    // Resume reuses the journaled plan (planner skipped, n1 replayed from memo) and
    // re-drives ONLY d.
    let (gw2, calls2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1").with_registry(planner_registry());
    let o2 = exec2.start(run, &graph).await.expect("resume");
    assert!(o2.failed.is_none(), "resume completes: {o2:?}");
    assert!(o2.outputs[&NodeId("e".into())].get("n1").is_some(), "journaled plan (n1) reused: {}", o2.outputs[&NodeId("e".into())]);
    let recorded2 = calls2.lock().unwrap().clone();
    assert_eq!(recorded2.len(), 1, "resume re-called the gateway only for d (no re-plan, no n1 re-spend): {recorded2:?}");
    assert_eq!(recorded2[0].1, "d", "the single resume call carried d's prompt");
}
```

- [ ] **Step 6: Run to verify the resume test passes**

Run: `cargo test -p sensei-orchestrator planner_agent_resume_reuses_journaled_plan` — Expected: PASS (the slice-3 `fold.expansions` short-circuit already skips re-planning; this confirms it holds for the agent planner). Verify real exit 0.

- [ ] **Step 7: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/expand.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-3 s4A (4/6) — journaled planner-agent branch in run_expand (parse+feasible+splice; resume reuses)"
```

---

## Task 5: Discovery tools (Pure)

**Files:**
- Modify: `crates/orchestrator/src/agent/tools.rs`
- Test: `crates/orchestrator/src/agent/tools.rs` (`mod tests`)

- [ ] **Step 1: Write the failing tool tests**

In `crates/orchestrator/src/agent/tools.rs` `mod tests` (create it if absent), add:

```rust
#[cfg(test)]
mod planner_tool_tests {
    use super::*;
    use orchestrator_core::{AgentDefinition, Registry};
    use std::collections::HashMap;

    fn reg() -> Arc<Registry> {
        Arc::new(Registry::default().with_agent(AgentDefinition {
            name: "researcher".into(), area: "research".into(), kind: "reasoning".into(),
            chain: Some("research.bulk".into()), chains: HashMap::new(), grants: HashMap::new(),
            tools: vec![], skills: vec![], system_prompt: "r".into(),
        }))
    }

    #[test]
    fn list_agents_returns_the_menu() {
        let t = ListAgents(reg());
        let out = t.call(serde_json::json!({})).unwrap();
        let arr = out["agents"].as_array().unwrap();
        assert!(arr.iter().any(|a| a["name"] == "researcher" && a["area"] == "research"));
    }

    #[test]
    fn validate_plan_tool_reports_errors_and_ok() {
        let t = ValidatePlan { registry: reg(), max_nodes: 512 };
        let bad = t.call(serde_json::json!({ "plan": "not json" })).unwrap();
        assert_eq!(bad["ok"], false);
        assert!(bad["errors"].as_array().unwrap().len() >= 1);
        let good = t.call(serde_json::json!({ "plan":
            r#"{"graph":{"nodes":[{"id":"n1","kind":{"ModelCall":{"chain":"research.bulk","payload":{}}},"deps":[]}]}}"# })).unwrap();
        assert_eq!(good["ok"], true);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p sensei-orchestrator planner_tool_tests` — Expected: FAIL to compile (`ListAgents`/`ValidatePlan` missing).

- [ ] **Step 3: Implement the discovery tools**

In `crates/orchestrator/src/agent/tools.rs`, add (they hold an `Arc<Registry>`; all `Pure`):

```rust
use orchestrator_core::Registry;

/// Pure discovery: list the registry's agents (name + role).
pub struct ListAgents(pub Arc<Registry>);
impl Tool for ListAgents {
    fn spec(&self) -> ToolSpec {
        ToolSpec { name: "list_agents".into(), description: Some("List available agents (name, area, kind)".into()),
            input_schema: serde_json::json!({"type":"object","properties":{}}),
            effect_class: EffectClass::Pure, ttl_secs: None, source: None,
            permissions: Permissions::default(), activation: Activation::default() }
    }
    fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        let mut agents: Vec<_> = self.0.agents()
            .map(|a| serde_json::json!({ "name": a.name, "area": a.area, "kind": a.kind }))
            .collect();
        agents.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        Ok(serde_json::json!({ "agents": agents }))
    }
}

/// Pure discovery: list skills (name + description).
pub struct ListSkills(pub Arc<Registry>);
impl Tool for ListSkills {
    fn spec(&self) -> ToolSpec {
        ToolSpec { name: "list_skills".into(), description: Some("List available skills".into()),
            input_schema: serde_json::json!({"type":"object","properties":{}}),
            effect_class: EffectClass::Pure, ttl_secs: None, source: None,
            permissions: Permissions::default(), activation: Activation::default() }
    }
    fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        let mut skills: Vec<_> = self.0.skills()
            .map(|s| serde_json::json!({ "name": s.name, "description": s.description }))
            .collect();
        skills.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        Ok(serde_json::json!({ "skills": skills }))
    }
}

/// Pure discovery: list tools (name + description + class).
pub struct ListTools(pub Arc<Registry>);
impl Tool for ListTools {
    fn spec(&self) -> ToolSpec {
        ToolSpec { name: "list_tools".into(), description: Some("List available tools".into()),
            input_schema: serde_json::json!({"type":"object","properties":{}}),
            effect_class: EffectClass::Pure, ttl_secs: None, source: None,
            permissions: Permissions::default(), activation: Activation::default() }
    }
    fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        let mut tools: Vec<_> = self.0.tools()
            .map(|t| serde_json::json!({ "name": t.name, "description": t.description, "effect_class": format!("{:?}", t.effect_class) }))
            .collect();
        tools.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        Ok(serde_json::json!({ "tools": tools }))
    }
}

/// Pure discovery: list registry-known chain ids (best-effort menu).
pub struct ListChains(pub Arc<Registry>);
impl Tool for ListChains {
    fn spec(&self) -> ToolSpec {
        ToolSpec { name: "list_chains".into(), description: Some("List registry-known chain ids".into()),
            input_schema: serde_json::json!({"type":"object","properties":{}}),
            effect_class: EffectClass::Pure, ttl_secs: None, source: None,
            permissions: Permissions::default(), activation: Activation::default() }
    }
    fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        Ok(serde_json::json!({ "chains": self.0.chain_names() }))
    }
}

/// Pure: validate a draft plan (`{plan: <json string>}`) → `{ok, errors}`.
pub struct ValidatePlan { pub registry: Arc<Registry>, pub max_nodes: usize }
impl Tool for ValidatePlan {
    fn spec(&self) -> ToolSpec {
        ToolSpec { name: "validate_plan".into(),
            description: Some("Validate a draft plan JSON; returns {ok, errors}".into()),
            input_schema: serde_json::json!({"type":"object","properties":{"plan":{"type":"string"}},"required":["plan"]}),
            effect_class: EffectClass::Pure, ttl_secs: None, source: None,
            permissions: Permissions::default(), activation: Activation::default() }
    }
    fn call(&self, args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        let text = args.get("plan").and_then(|v| v.as_str()).unwrap_or_default();
        match orchestrator_core::parse_plan(text) {
            Err(e) => Ok(serde_json::json!({ "ok": false, "errors": [format!("{e:?}")] })),
            Ok(plan) => match orchestrator_core::feasible(&plan, &self.registry, self.max_nodes) {
                Ok(()) => Ok(serde_json::json!({ "ok": true, "errors": [] })),
                Err(errs) => Ok(serde_json::json!({ "ok": false,
                    "errors": errs.iter().map(|e| format!("{e:?}")).collect::<Vec<_>>() })),
            },
        }
    }
}
```

- [ ] **Step 4: Run to verify the tool tests pass**

Run: `cargo test -p sensei-orchestrator planner_tool_tests` — Expected: PASS. Verify real exit 0.

- [ ] **Step 5: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/agent/tools.rs
git commit -m "feat(orchestrator): SP-3 s4A (5/6) — Pure planner discovery tools (list_agents/skills/tools/chains + validate_plan)"
```

---

## Task 6: End-to-end (planner uses the tools) + full-suite gate

**Files:**
- Test: `crates/orchestrator/src/executor/tests.rs`

**Note:** This wires a planner agent that is *granted* the discovery tools, drives it through a scripted gateway that first calls `validate_plan` then emits the final plan, and asserts the whole `goal → plan → result` path incl. `on_plan_expanded` and right-sizing. It reuses the slice-2 tool-call scripting (`tool_call_response`/`final_response`) and the two-registry split (core specs for prompt/validate + executable `ToolRegistry`).

- [ ] **Step 1: Write the e2e + right-sizing tests**

In `crates/orchestrator/src/executor/tests.rs`, append:

```rust
/// Full grounding e2e: the planner agent calls validate_plan (a real tool) on a draft,
/// then emits the final single-Agent plan (right-sizing: tier 1). Executed to completion.
#[tokio::test]
async fn planner_agent_uses_validate_plan_then_emits_a_single_agent_plan() {
    // Registry: a `planner` agent granted validate_plan + list_agents, and a `worker` agent.
    let worker = AgentDefinition {
        name: "worker".into(), area: "research".into(), kind: "reasoning".into(),
        chain: Some("c".into()), chains: std::collections::HashMap::new(),
        grants: std::collections::HashMap::new(), tools: vec![], skills: vec![],
        system_prompt: "work".into(),
    };
    let planner = AgentDefinition {
        name: "planner".into(), area: "planning".into(), kind: "reasoning".into(),
        chain: Some("c".into()), chains: std::collections::HashMap::new(),
        grants: std::collections::HashMap::new(),
        tools: vec!["validate_plan".into(), "list_agents".into()], skills: vec![],
        system_prompt: "Plan. Prefer the simplest structure.".into(),
    };
    let reg = Arc::new(Registry::default()
        .with_agent(planner).with_agent(worker)
        .with_tool(crate::agent::tools::ValidatePlan { registry: Arc::new(Registry::default()), max_nodes: 512 }.spec())
        .with_tool(crate::agent::tools::ListAgents(Arc::new(Registry::default())).spec()));

    // The single-Agent plan the planner ends up emitting (tier 1 — right-sizing).
    // The `Graph` serde shape is {"nodes":[{id, kind, deps}]}; an Agent node's kind is
    // {"Agent":{"agent":<name>, "input":<value>, "phase":null}}.
    let plan_json = r#"{"graph":{"nodes":[{"id":"n1","kind":{"Agent":{"agent":"worker","input":"go","phase":null}},"deps":[]}]},"node_plans":{"n1":{"label":"do it all"}}}"#;

    // Executable tools the planner actually calls: validate_plan (over the real reg) + list_agents.
    let tools = Arc::new(ToolRegistry::default()
        .with_tool(Arc::new(crate::agent::tools::ValidatePlan { registry: reg.clone(), max_nodes: 512 }))
        .with_tool(Arc::new(crate::agent::tools::ListAgents(reg.clone()))));

    // Scripted gateway: planner turn 1 → call validate_plan(draft); turn 2 → final plan JSON;
    // then the spliced worker agent's single turn → final answer.
    let validate_args = serde_json::json!({ "plan": plan_json }).to_string();
    let (gateway, _c) = scripted_gateway(vec![
        tool_call_response("t1", "validate_plan", &validate_args),
        final_response(plan_json),
        final_response("worker done"),
    ]).await;

    let journal = InMemoryJournal::new();
    let hooks_log = std::sync::Arc::new(std::sync::Mutex::new(0usize));
    struct Counter(std::sync::Arc<std::sync::Mutex<usize>>);
    #[async_trait::async_trait]
    impl OrchestratorHooks for Counter {
        async fn on_plan_expanded(&self, _r: RunId, _n: &NodeId, _g: &Graph,
            _p: &std::collections::HashMap<NodeId, orchestrator_core::NodePlan>) {
            *self.0.lock().unwrap() += 1;
        }
    }
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(reg).with_tools(tools).with_hooks(Arc::new(Counter(hooks_log.clone())));

    let e = NodeId("e".into());
    let graph = Graph { nodes: vec![expand_agent_node("e", vec![])] };
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    // Tier-1 right-sizing: the produced plan is a single node.
    assert!(out.outputs[&e].get("n1").is_some(), "single-agent plan executed: {}", out.outputs[&e]);
    assert_eq!(*hooks_log.lock().unwrap(), 1, "on_plan_expanded fired once for the produced plan");
}
```

- [ ] **Step 2: Run the e2e test**

Run: `cargo test -p sensei-orchestrator planner_agent_uses_validate_plan_then_emits_a_single_agent_plan` — Expected: PASS. Verify real exit 0.

- [ ] **Step 3: Full-workspace gate (AC13 additive + no regressions)**

Run: `cargo test --workspace` — read the REAL summary + exit code (do NOT pipe). Confirm 0 failures; report exact pass counts (prior baseline was 980).

- [ ] **Step 4: Lint gate**

Run: `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings` — both exit 0.

- [ ] **Step 5: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/tests.rs
git commit -m "test(orchestrator): SP-3 s4A (6/6) — planner grounding e2e (validate_plan + single-agent right-sizing); full-suite green"
```

- [ ] **Step 6: Push + verify**

```bash
cd /Users/Jerry/Developer/gateway
git push origin develop
git rev-parse HEAD origin/develop   # must be identical
```

---

## Acceptance Criteria → Task map (self-review)

| Spec AC | Task | Test |
|---|---|---|
| 1 schema round-trip + backward-compat | 1, 2, 3 | `parse_plan_*`, `parse_plan_without_node_plans_defaults_empty`, `expand_deserializes_without_planner_as_injected` |
| 2 feasible catches each class | 1 | `feasible_reports_all_error_classes`, `feasible_reports_a_structural_cycle` |
| 3 validate_plan primitive/tool | 1, 5 | `validate_plan_tool_reports_errors_and_ok` |
| 4 journaled planner produces + splices | 4 | `journaled_planner_agent_produces_and_splices_a_plan` |
| 5 resume mid-plan replays turns | 4 | (covered by the fold short-circuit; `planner_agent_resume_reuses_journaled_plan` proves no re-plan/no re-spend) |
| 6 resume post-PlanExpanded no re-plan | 4 | `planner_agent_resume_reuses_journaled_plan` |
| 7 planner failure → Failed → cascade | 4 | `planner_agent_invalid_plan_fails_the_node`, `unresolvable_planner_agent_fails_the_node` |
| 8 planner pause → run pauses | 4 | (Paused arm in run_expand; exercised via the shared pause machinery — add a quota-during-planning case if a fixture is cheap, else covered structurally) |
| 9 on_plan_expanded fires, replay-suppressed | 2, 6 | `on_plan_expanded_fires_with_the_plan`, e2e counter |
| 10 right-sizing single-Agent | 6 | `planner_agent_uses_validate_plan_then_emits_a_single_agent_plan` |
| 11 full-palette Map+Consolidate | 4/6 | the 2-node plan test (extend to Map+Consolidate if a scripted fixture is added) |
| 12 end-to-end | 6 | the grounding e2e |
| 13 additive | 3, 6 | slice-3 `expand_*`/`subgraph`/`branch` green; `cargo test --workspace` |

**Coverage gaps to close during implementation (flagged, not silently dropped):** AC8 (planner-pause) and AC11 (full-palette Map+Consolidate) have structural coverage but no dedicated fixture in the steps above — the implementer should add a `planner_agent_pause_pauses_the_run` (a quota/timeout gateway during the planner turn) and a `planner_emits_a_map_consolidate_plan` test if the scripted-gateway fixtures are cheap; if a fixture proves fragile, note it in the task report rather than skip silently.

---

## Post-implementation

- Update `docs/features/orchestrator/` (`execution-graph.md` / a new `planner.md`) + flip the overview index row `SP-3 s4A` to ✅ done.
- Update the memory topic file + `MEMORY.md`: s4A done; NEXT = s4B (`PlannerSelector` Rule+Llm + `PlannerRef::Select`).
- Carry-forward deferred (spec §6): activate-from-`needs`; first-class consensus node; HITL; gateway `chains()` for exact feasibility; needless-nesting lint.
