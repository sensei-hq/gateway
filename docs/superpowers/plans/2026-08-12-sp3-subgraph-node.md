# SP-3 slice 1 — Subgraph node Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `NodeKind::Subgraph { graph: Box<Graph> }` — a node whose work is a nested DAG, driven recursively under the node's path in the same run (namespaced ids → reuse `drive` → nested effects nest; resume replays inner nodes with no re-spend), with a `max_depth` self-DoS cap.

**Architecture:** `run_subgraph` (new `executor/subgraph.rs`) namespaces the nested graph's ids under `{node}/`, calls the existing `self.drive`, and maps the nested `RunOutcome` → the node's `NodeExec` (Completed = sink-outputs map · Failed · Paused). `validate_dag` recurses into nested graphs. The nested graph shares the run's journal/fold/memo — no new determinism machinery.

**Tech Stack:** Rust workspace (`orchestrator-core`, `orchestrator`); `serde`; `cargo test`/`clippy`. Spec: `docs/superpowers/specs/2026-08-12-sp3-subgraph-node-design.md`.

**House rules (every task):**
- Pre-commit = `make lint` (fmt-check + workspace `clippy -D warnings`), NO tests → always `cargo fmt --all` then `cargo test --workspace` before committing.
- Verify the REAL exit code (never a piped `| tail`); run a single test with a SINGLE positional filter.
- Commit a fix BEFORE any `git checkout`-based mutation-verify.
- Branch `feat/sp3-subgraph-node` (created; spec committed at `6c456c4`). Crate `-p` names: `sensei-orchestrator-core`, `sensei-orchestrator`.

**Key shapes (verified):** `Node { id: NodeId, kind: NodeKind, deps: Vec<Dep> }`; `Dep { on: NodeId, kind: EdgeKind }`; `Graph { nodes: Vec<Node> }`; `NodeKind::{ModelCall{chain,payload}, Agent{..}, Map{..}, Consolidate{over:NodeId,min_viable,body}, Loop{..}}`; `NodeExec::{ Completed(serde_json::Value), Failed{message,output}, Paused{reason} }`; `RunOutcome { completed, failed: Option<(NodeId,String)>, skipped, outputs: HashMap<NodeId, serde_json::Value>, paused: Option<PauseInfo> }`; `PauseInfo { node, reason }`. `run_node` dispatch ends `NodeKind::Loop { .. } => self.run_loop(run, node, fold).await,` (executor/mod.rs). `drive(&self, run, graph: &Graph, fold: &Fold) -> Result<RunOutcome, _>`. Executor submodules declared in `executor/mod.rs`: `mod agent; content; durability; fanout; support;` (+ `tests`).

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `crates/orchestrator-core/src/graph.rs` | node kinds + validation | `NodeKind::Subgraph { graph: Box<Graph> }`; recurse `validate_dag`. |
| `crates/orchestrator-core/src/error.rs` | errors | `GlobalCapExceeded { cap, limit }`. |
| `crates/orchestrator/src/executor/subgraph.rs` (new) | nested-graph execution | `run_subgraph` + `namespace_graph` + `sink_outputs`. |
| `crates/orchestrator/src/executor/mod.rs` | dispatch + config | `mod subgraph;`; `NodeKind::Subgraph` arm; `max_depth` field + `with_max_depth`. |
| `crates/orchestrator/src/executor/tests.rs` | tests | execution, resume, failure/pause propagation, cap, e2e. |
| `docs/features/orchestrator/execution-graph.md` | feature doc | Subgraph note. |

---

## Task 1: `NodeKind::Subgraph` + recursive `validate_dag` + `run_subgraph` (execution)

Adds the (boxed) variant, recursive validation, and `run_subgraph` (execute → sink map; the Failed/Paused mappings land here too but are tested in Task 3). Because a new `NodeKind` variant breaks `run_node`'s exhaustive match, the variant + the `run_node` arm land together (one green workspace commit).

**Files:**
- Modify: `crates/orchestrator-core/src/graph.rs` (the `NodeKind` enum; `validate_dag`)
- Create: `crates/orchestrator/src/executor/subgraph.rs`
- Modify: `crates/orchestrator/src/executor/mod.rs` (`mod subgraph;`; `run_node` arm)
- Test: `crates/orchestrator/src/executor/tests.rs` + `graph.rs` tests

- [ ] **Step 1: Write the failing core validate_dag recursion test**

Add to `crates/orchestrator-core/src/graph.rs` tests:

```rust
    #[test]
    fn validate_dag_recurses_into_subgraphs() {
        // A Subgraph whose nested graph has a cycle → loud InvalidGraph.
        let nested_cycle = Graph {
            nodes: vec![
                Node { id: NodeId("a".into()), kind: NodeKind::ModelCall { chain: "c".into(), payload: serde_json::json!(0) },
                       deps: vec![Dep { on: NodeId("b".into()), kind: EdgeKind::Hard }] },
                Node { id: NodeId("b".into()), kind: NodeKind::ModelCall { chain: "c".into(), payload: serde_json::json!(0) },
                       deps: vec![Dep { on: NodeId("a".into()), kind: EdgeKind::Hard }] },
            ],
        };
        let outer = Graph {
            nodes: vec![Node { id: NodeId("s".into()),
                kind: NodeKind::Subgraph { graph: Box::new(nested_cycle) }, deps: vec![] }],
        };
        assert!(matches!(outer.validate_dag(), Err(OrchestratorError::InvalidGraph(_))),
            "a nested cycle is rejected recursively");

        // A valid nested line passes.
        let nested_ok = Graph {
            nodes: vec![
                Node { id: NodeId("a".into()), kind: NodeKind::ModelCall { chain: "c".into(), payload: serde_json::json!(0) }, deps: vec![] },
                Node { id: NodeId("b".into()), kind: NodeKind::ModelCall { chain: "c".into(), payload: serde_json::json!(0) },
                       deps: vec![Dep { on: NodeId("a".into()), kind: EdgeKind::Hard }] },
            ],
        };
        let outer_ok = Graph { nodes: vec![Node { id: NodeId("s".into()),
            kind: NodeKind::Subgraph { graph: Box::new(nested_ok) }, deps: vec![] }] };
        assert!(outer_ok.validate_dag().is_ok());
    }
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator-core validate_dag_recurses_into_subgraphs`
Expected: FAIL to compile — `NodeKind::Subgraph` not found. (RED.)

- [ ] **Step 3: Add the (boxed) variant + recurse validate_dag**

In `crates/orchestrator-core/src/graph.rs`, add to the `NodeKind` enum (after `Loop`):
```rust
    /// A node whose work is a whole nested DAG, driven under this node's path in
    /// the SAME run (SP-3). `Box` breaks the recursive type (NodeKind → Graph →
    /// Node → NodeKind). Static this slice; slice 3 produces subgraphs at runtime.
    Subgraph { graph: Box<Graph> },
```
In `validate_dag`, after the per-node-kind sanity block (section "2b", the `Loop { max_iters: 0 }` check), add recursion:
```rust
        // 2c. A `Subgraph`'s nested graph must itself be a valid DAG (recursive).
        for node in &self.nodes {
            if let NodeKind::Subgraph { graph } = &node.kind {
                graph.validate_dag()?;
            }
        }
```

- [ ] **Step 4: Add the executor `run_subgraph` module (execution path)**

Create `crates/orchestrator/src/executor/subgraph.rs`:
```rust
//! The `Subgraph` node on the executor: drive a nested DAG under the node's path
//! in the SAME run (SP-3 slice 1). Namespacing the inner ids makes nested effects
//! nest via `effect_id`; `drive` (reused) + the fold give resume-without-re-spend.

use std::collections::HashSet;

use orchestrator_core::{Dep, Graph, Node, NodeId, NodeKind, OrchestratorError, RunId};

use super::{Executor, Fold, NodeExec};

/// Clone `graph` with every inner node id (and each `Dep.on`, and `Consolidate.over`)
/// rewritten to `"{prefix}/{id}"`. Deeper nesting is handled by recursion — a nested
/// `Subgraph`'s own inner graph is namespaced when its `run_subgraph` runs.
fn namespace_graph(prefix: &str, graph: &Graph) -> Graph {
    let ns = |id: &NodeId| NodeId(format!("{prefix}/{}", id.0));
    Graph {
        nodes: graph
            .nodes
            .iter()
            .map(|n| Node {
                id: ns(&n.id),
                kind: match &n.kind {
                    NodeKind::Consolidate { over, min_viable, body } => NodeKind::Consolidate {
                        over: ns(over),
                        min_viable: *min_viable,
                        body: body.clone(),
                    },
                    other => other.clone(),
                },
                deps: n.deps.iter().map(|d| Dep { on: ns(&d.on), kind: d.kind.clone() }).collect(),
            })
            .collect(),
    }
}

/// The subgraph's output: `{ sink_id: output }` for each sink (a node referenced by
/// no other node's `Dep`) that produced an output.
fn sink_outputs(
    graph: &Graph,
    prefix: &str,
    outputs: &std::collections::HashMap<NodeId, serde_json::Value>,
) -> serde_json::Value {
    let referenced: HashSet<&NodeId> =
        graph.nodes.iter().flat_map(|n| n.deps.iter().map(|d| &d.on)).collect();
    let mut map = serde_json::Map::new();
    for n in &graph.nodes {
        if !referenced.contains(&n.id) {
            let key = NodeId(format!("{prefix}/{}", n.id.0));
            if let Some(v) = outputs.get(&key) {
                map.insert(n.id.0.clone(), v.clone());
            }
        }
    }
    serde_json::Value::Object(map)
}

impl Executor {
    /// Drive a `Subgraph` node's nested DAG under `"{node}/…"` and fold the nested
    /// outcome into this node's `NodeExec`: paused ⇒ `Paused`, failed ⇒ `Failed`,
    /// else `Completed(sink map)`.
    pub(super) async fn run_subgraph(
        &self,
        run: RunId,
        node: &Node,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        let NodeKind::Subgraph { graph } = &node.kind else {
            unreachable!("run_subgraph on non-Subgraph node");
        };
        let inner = namespace_graph(&node.id.0, graph);
        let nested = self.drive(run, &inner, fold).await?;
        if let Some(p) = nested.paused {
            return Ok(NodeExec::Paused {
                reason: format!("subgraph {} paused: {}", node.id.0, p.reason),
            });
        }
        if let Some((n, msg)) = nested.failed {
            return Ok(NodeExec::Failed {
                message: format!("subgraph {} failed at {}: {}", node.id.0, n.0, msg),
                output: None,
            });
        }
        Ok(NodeExec::Completed(sink_outputs(graph, &node.id.0, &nested.outputs)))
    }
}
```
(`namespace_graph(&node.id.0, graph)` passes `&Box<Graph>` where `&Graph` is expected — Deref coercion applies. If the compiler objects, use `graph.as_ref()`.)

- [ ] **Step 5: Wire the module + the `run_node` arm**

In `crates/orchestrator/src/executor/mod.rs`: add `mod subgraph;` beside the other `mod` declarations. In `run_node`, after `NodeKind::Loop { .. } => self.run_loop(run, node, fold).await,`, add:
```rust
            NodeKind::Subgraph { .. } => self.run_subgraph(run, node, fold).await,
```

- [ ] **Step 6: Write the failing execution tests**

Add to `crates/orchestrator/src/executor/tests.rs`:
```rust
    // Build a nested ModelCall node on chain "c" (the recording gateway's chain).
    fn mc(id: &str, dep: Option<&str>) -> Node {
        Node {
            id: NodeId(id.into()),
            kind: NodeKind::ModelCall { chain: "c".into(), payload: serde_json::json!({ "prompt": id }) },
            deps: dep.map(|d| vec![Dep { on: NodeId(d.into()), kind: EdgeKind::Hard }]).unwrap_or_default(),
        }
    }
    fn subgraph_node(id: &str, inner: Vec<Node>) -> Node {
        Node { id: NodeId(id.into()), kind: NodeKind::Subgraph { graph: Box::new(Graph { nodes: inner }) }, deps: vec![] }
    }

#[tokio::test]
async fn subgraph_executes_a_nested_line_and_returns_the_sink_map() {
    let (gateway, _c) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let s = NodeId("s".into());
    // Nested line n1 → n2 (n2 is the sink).
    let graph = Graph { nodes: vec![subgraph_node("s", vec![mc("n1", None), mc("n2", Some("n1"))])] };
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    let sub_out = &out.outputs[&s];
    assert!(sub_out.get("n2").is_some(), "sink map has n2: {sub_out}");
    assert!(sub_out.get("n1").is_none(), "n1 is not a sink");
}

#[tokio::test]
async fn subgraph_diamond_returns_all_sink_outputs() {
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let s = NodeId("s".into());
    // a → {b, c}, both b and c are sinks.
    let inner = vec![mc("a", None), mc("b", Some("a")), mc("c", Some("a"))];
    let graph = Graph { nodes: vec![subgraph_node("s", inner)] };
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    let sub = &out.outputs[&s];
    assert!(sub.get("b").is_some() && sub.get("c").is_some(), "both sinks present: {sub}");
    assert!(sub.get("a").is_none(), "a is not a sink");
}
```
(Verify `recording_gateway` drives a `ModelCall` node on chain `"c"` to an output — grep existing `NodeKind::ModelCall` node tests for the exact gateway + output shape; if `recording_gateway` is agent-only, use the gateway the existing ModelCall-node tests use, e.g. `scripted_gateway`/`demo_reference_gateway`, and keep the sink-map assertions.)

- [ ] **Step 7: Run to confirm failure, then it passes after Steps 3-5**

Run: `cargo test -p sensei-orchestrator subgraph_executes_a_nested_line_and_returns_the_sink_map` (RED first if written before Steps 3-5; after implementing, PASS).

- [ ] **Step 8: Add a resume-no-respend test**

Model on the existing partial-run resume tests (grep `failing_after_gateway` + `.start(`): an outer graph `[ subgraph "s" (nested n1→n2), outer node "d" (Hard-dep on "s") ]` where run 1's gateway fails at "d" (after the subgraph completes), then resume on a `recording_gateway` and assert the subgraph's inner ModelCalls are NOT re-called (gateway call count unchanged) and the run completes. Name it `subgraph_inner_nodes_replay_from_memo_on_resume`. (Use the actual failing/recording gateway signatures from the existing resume tests.)

- [ ] **Step 9: Run green + commit**

Run: the new tests (single filter) PASS; `cargo test --workspace` (all pass); `cargo fmt --all`; `cargo clippy --workspace --all-targets -- -D warnings` (exit 0).

```bash
git add -A
git commit -m "feat(orchestrator): SP-3 slice 1 (1/3) — NodeKind::Subgraph + run_subgraph (nested-graph execution)

NodeKind::Subgraph{graph: Box<Graph>} driven recursively via namespace_graph +
self.drive in the same run; output = sink-outputs map. validate_dag recurses into
nested graphs (loud on nested cycle/dangling). Resume replays inner nodes from the
memo (no re-spend). Failed/Paused mappings included (tested in Task 3)."
```

---

## Task 2: `max_depth` global cap

Adds the nesting-depth self-DoS cap: `run_subgraph` halts loud when the path depth would exceed `max_depth`.

**Files:**
- Modify: `crates/orchestrator-core/src/error.rs` (after `MapChildPaused`)
- Modify: `crates/orchestrator/src/executor/mod.rs` (`max_depth` field + `with_max_depth`)
- Modify: `crates/orchestrator/src/executor/subgraph.rs` (depth check)
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the failing cap test**

```rust
#[tokio::test]
async fn subgraph_nesting_beyond_max_depth_halts_loud() {
    let (gateway, _c) = recording_gateway().await;
    // A subgraph containing a subgraph (2 levels of nesting).
    let inner = subgraph_node("inner", vec![mc("x", None)]);
    let graph = Graph { nodes: vec![subgraph_node("outer", vec![inner])] };
    // max_depth = 1 allows one subgraph level; the second is refused loud.
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_max_depth(1);
    let err = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await;
    // The nested subgraph's failure surfaces (GlobalCapExceeded propagates as the
    // inner node's failure → the outer subgraph fails, or as a top-level Err —
    // assert the cap message is present either way).
    let failed = match &err {
        Ok(o) => o.failed.as_ref().map(|(_, m)| m.clone()),
        Err(e) => Some(format!("{e:?}")),
    };
    assert!(failed.as_deref().unwrap_or("").contains("max_depth"), "cap halts loud: {err:?}");

    // With the default (8), the same graph runs fine.
    let (gateway2, _c2) = recording_gateway().await;
    let ok = Executor::new(Arc::new(gateway2), Arc::new(InMemoryJournal::new()), "v1")
        .run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("runs within default depth");
    assert!(ok.failed.is_none(), "{ok:?}");
}
```
(Note: the inner `GlobalCapExceeded` is returned by `run_subgraph` as an `Err`, which propagates through the nested `drive` as the inner node's failure → the outer subgraph reports `failed`. If instead it bubbles as a top-level `Err(GlobalCapExceeded)`, the `match` above still asserts the `max_depth` message. Confirm which path the harness produces and keep the assertion on the message.)

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator subgraph_nesting_beyond_max_depth_halts_loud`
Expected: FAIL — `with_max_depth` not found. (RED.)

- [ ] **Step 3: Add the error variant**

`crates/orchestrator-core/src/error.rs`, after the `MapChildPaused` variant:
```rust
    #[error("global cap {cap:?} exceeded (limit {limit})")]
    GlobalCapExceeded { cap: String, limit: usize },
```

- [ ] **Step 4: Add `max_depth` + `with_max_depth`**

In `crates/orchestrator/src/executor/mod.rs`: add a field (after `hooks` / `handle`):
```rust
    /// Max nesting depth (Subgraph levels; SP-3 self-DoS backstop). Default 8.
    max_depth: usize,
```
Initialize `max_depth: 8,` in `Executor::new`'s struct literal. Add the builder near `with_max_steps`:
```rust
    /// Set the max nesting depth (Subgraph self-DoS cap; default 8).
    pub fn with_max_depth(mut self, n: usize) -> Self {
        self.max_depth = n;
        self
    }
```

- [ ] **Step 5: Add the depth check to `run_subgraph`**

In `crates/orchestrator/src/executor/subgraph.rs`, prepend to `run_subgraph` (before the `let NodeKind::Subgraph …` destructure):
```rust
        // Depth cap (self-DoS backstop): the path segment count is the current
        // nesting level; a top-level subgraph node has 0 segments (level 1).
        let depth = node.id.0.matches('/').count();
        if depth + 1 > self.max_depth {
            return Err(OrchestratorError::GlobalCapExceeded {
                cap: "max_depth".into(),
                limit: self.max_depth,
            });
        }
```

- [ ] **Step 6: Run green + commit**

Run: the cap test (single filter) PASS; `cargo test --workspace` (all pass); `cargo fmt --all`; `cargo clippy --workspace --all-targets -- -D warnings` (exit 0).

```bash
git add -A
git commit -m "feat(orchestrator): SP-3 slice 1 (2/3) — max_depth cap (GlobalCapExceeded)

Executor.max_depth (default 8) + with_max_depth; run_subgraph halts loud with
GlobalCapExceeded when the path-derived nesting level would exceed it — a self-DoS
backstop. Node-count/expansion caps defer to slice 3 (PlanDelta)."
```

---

## Task 3: Failure/pause propagation tests + e2e + docs

`run_subgraph` already maps nested Failed/Paused (Task 1); this task adds their regression tests, a gateway e2e, and the feature doc.

**Files:**
- Test: `crates/orchestrator/src/executor/tests.rs`
- Modify: `docs/features/orchestrator/execution-graph.md`

- [ ] **Step 1: Failure-propagation test**

An outer graph `[ subgraph "s" (nested node fails), outer "d" (Hard-dep on "s"), outer "e" (Soft-dep on "s") ]`; the nested node fails (use `failing_after_gateway(0)` so the nested ModelCall fails), so `s` is `Failed` → `d` is cascade-skipped, `e` (soft) still runs.
```rust
#[tokio::test]
async fn a_failing_nested_node_fails_the_subgraph_and_cascades_hard_dependents() {
    let (gateway, _c) = failing_after_gateway(0).await; // fails immediately
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let graph = Graph { nodes: vec![
        subgraph_node("s", vec![mc("n1", None)]),
        Node { id: NodeId("d".into()), kind: NodeKind::ModelCall { chain: "c".into(), payload: serde_json::json!(0) },
               deps: vec![Dep { on: NodeId("s".into()), kind: EdgeKind::Hard }] },
        Node { id: NodeId("e".into()), kind: NodeKind::ModelCall { chain: "c".into(), payload: serde_json::json!(0) },
               deps: vec![Dep { on: NodeId("s".into()), kind: EdgeKind::Soft }] },
    ] };
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("outcome");
    assert!(out.failed.is_some(), "the subgraph failed: {out:?}");
    assert!(out.skipped.contains(&NodeId("d".into())), "hard dependent cascade-skipped");
}
```
(Adapt `failing_after_gateway`'s exact signature — grep it. If a 0-success gateway isn't available, use the smallest N that makes the nested node fail. The load-bearing asserts: `s` failed + `d` in `skipped`.)

- [ ] **Step 2: Pause-propagation test**

Adapt the existing in-doubt-Mutation map-child pause test (grep `in_doubt_mutation_in_a_map_child_pauses_the_whole_run`) to wrap the mutation-bearing agent in a **Subgraph** instead of a Map: the nested node pauses → `RunOutcome.paused` is set, no `RunCompleted`. Name it `an_in_doubt_mutation_in_a_subgraph_pauses_the_run`. Assert `out.paused.is_some()` and that no `RunCompleted` was journaled. (Reuse that test's `RecordNote`/`AlwaysIndeterminate`/reconcile harness verbatim; only the graph shape changes from a Map to a Subgraph wrapping the agent node.)

- [ ] **Step 3: End-to-end test (nested agent through the gateway)**

```rust
#[tokio::test]
async fn subgraph_drives_a_nested_agent_end_to_end() {
    let (gateway, _c) = recording_gateway().await;
    let registry = agent_registry("c"); // agent "a" on chain "c"
    let s = NodeId("s".into());
    let inner = vec![agent_node("n1", "a", "hi")]; // nested Agent node
    let graph = Graph { nodes: vec![subgraph_node("s", inner)] };
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(registry);
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    assert!(out.outputs[&s].get("n1").is_some(), "nested agent's output is the subgraph's sink: {}", out.outputs[&s]);
}
```
(Verify `agent_registry`/`agent_node` names; `n1` is the lone sink so the subgraph output is `{ "n1": <agent output> }`.)

- [ ] **Step 4: Run tests + update the feature doc**

Run each new test (single filter) PASS. In `docs/features/orchestrator/execution-graph.md`, add a Subgraph paragraph (match the doc's tone):
```markdown
- **`Subgraph { graph }`** (SP-3 slice 1) — a node whose work is a nested DAG,
  driven under the node's path (`{node}/…`) in the same run (namespaced ids ⇒ nested
  effects nest via `effect_id`; resume replays inner nodes with no re-spend). Its
  output is the **sink map** (`{sink_id: output}` for each terminal node). A nested
  failure/pause propagates to the node (`Failed`/`Paused`) and thus to the outer
  scheduler. `validate_dag` recurses into nested graphs. Nesting depth is capped by
  `Executor::with_max_depth` (default 8) → loud `GlobalCapExceeded`. **Deferred:**
  cross-boundary input/context (plan-scope blackboard), Loop-over-Subgraph (slice 5),
  runtime `PlanDelta` (slice 3), node-count/expansion caps (slice 3).
```

- [ ] **Step 5: Run green + commit**

Run: `cargo test --workspace` (all pass); `cargo fmt --all`; `cargo clippy --workspace --all-targets -- -D warnings` (exit 0).

```bash
git add -A
git commit -m "feat(orchestrator): SP-3 slice 1 (3/3) — subgraph failure/pause propagation + e2e + docs

Nested failure → subgraph Failed → outer cascade-skip (hard) with soft-deps running;
nested in-doubt Mutation → subgraph Paused → run pauses (no RunCompleted); e2e drives
a nested Agent through the gateway (sink map). execution-graph.md documents Subgraph."
```

---

## Self-Review

**1. Spec coverage** (against `2026-08-12-sp3-subgraph-node-design.md` §7):
- §7.1 nested line → sink map + effects nested → Task 1 `subgraph_executes_a_nested_line…`.
- §7.2 multiple sinks → Task 1 `subgraph_diamond_returns_all_sink_outputs`.
- §7.3 recursive validate_dag → Task 1 `validate_dag_recurses_into_subgraphs`.
- §7.4 max_depth cap → Task 2 `subgraph_nesting_beyond_max_depth_halts_loud`.
- §7.5 failure propagation → Task 3 `a_failing_nested_node_fails_the_subgraph_and_cascades_hard_dependents`.
- §7.6 pause propagation → Task 3 `an_in_doubt_mutation_in_a_subgraph_pauses_the_run`.
- §7.7 resume no-respend → Task 1 `subgraph_inner_nodes_replay_from_memo_on_resume`.
- §7.8 e2e nested agent → Task 3 `subgraph_drives_a_nested_agent_end_to_end`.
- §7.9 additive → Task 1 Step 9 (workspace green; a new arm only).
All covered.

**2. Placeholder scan:** No TBD/TODO; every code step complete; test steps reference the exact existing harness (`recording_gateway`/`failing_after_gateway`/the map-child-pause test) with instructions to match real signatures.

**3. Type consistency:** `NodeKind::Subgraph { graph: Box<Graph> }`, `run_subgraph(&self, run, node, fold) -> Result<NodeExec, _>`, `namespace_graph(prefix: &str, graph: &Graph) -> Graph`, `sink_outputs(graph, prefix, outputs) -> Value`, `GlobalCapExceeded { cap: String, limit: usize }`, `with_max_depth(n)`, `max_depth: usize` used identically across tasks. `Dep { on, kind }` / `Consolidate { over, min_viable, body }` remaps match the real shapes.

**4. Green-per-commit:** Task 1 lands the variant + `run_node` arm together (workspace compiles). Task 2 adds the cap over green. Task 3 is tests + docs over green.
