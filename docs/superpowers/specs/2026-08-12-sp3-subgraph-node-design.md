---
title: SP-3 slice 1 — Subgraph node (static nested graph)
doctype: design
module: orchestrator
spec: SP-3
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§10 execution graph, §220 node kinds, §229 loops-of-graphs, §7.2 effect_id nesting, global caps); SP-1 slice 3 (Map/Consolidate fan-out, namespaced child paths)
date: 2026-08-12
---

# SP-3 slice 1 — `Subgraph` node (static nested graph)

## 1. Goal

Add `NodeKind::Subgraph { graph }` — a node whose work is a whole **nested DAG**,
executed by recursively driving the nested graph under the node's path within the
**same run**. This is the structural foundation of the "hierarchical, runtime-
expandable graph" (D3): loops-of-graphs (slice 5) and runtime `PlanDelta` splicing
(slice 3) both produce/consume subgraphs. This slice ships **static** subgraphs
(author-provided) + the first **global cap** (nesting depth, a self-DoS backstop).

## 2. SP-3 slicing (context)

1. **This slice** — `Subgraph` node (static nested graph) + `max_depth` cap.
2. `Branch` node (deterministic conditional).
3. runtime `PlanDelta` / `PlanExpanded` (graph splicing) + node/expansion caps.
4. Planner agent (validated plan: JSON + cycle detection + feasibility).
5. Coordinator + loops-of-graphs (Loop over a Subgraph body) + caps/replan hardening.

## 3. Background & impact review

- **Current node kinds** (`orchestrator-core::graph`): `ModelCall`, `Agent`, `Map`,
  `Consolidate`, `Loop`. No `Subgraph`. `run_node` dispatches
  `NodeKind::X { .. } => self.run_x(...)`; `NodeExec::{ Completed(Value),
  Failed { message, output }, Paused { reason } }`.
- **The recursive-drive pattern already exists.** `drive(&self, run, graph, fold)`
  drives a `&Graph` and returns a `RunOutcome { completed, failed: Option<(NodeId,
  String)>, skipped, outputs: HashMap<NodeId, Value>, paused: Option<PauseInfo> }`;
  `run_inner` (not `drive`) owns `RunStarted`/`RunCompleted`. `run_map`/`run_loop`
  already namespace children at `format!("{node}/{i}")` and nest their effects there
  (`effect_id = hash(parent_path ‖ …)`, parent path = the node id). So a nested
  graph can reuse `drive` in the **same run** with namespaced ids — no new run.
- **Impact: additive.** New `NodeKind::Subgraph`, a `run_subgraph` arm in `run_node`,
  a `namespace_graph` helper, recursion in `validate_dag`, an `Executor.max_depth`
  field + `with_max_depth`, and one new error `GlobalCapExceeded`. Existing node
  kinds, `drive`, and all current tests are byte-identical (a new match arm).
- **Determinism:** the nested graph is author-static ⇒ reconstructs identically on
  resume; namespaced ids make nested effects nest deterministically, so the fold/memo
  replays completed inner nodes with **no re-spend** (identical to Map children). No
  new determinism machinery.

## 4. Design

### 4.1 The node kind (`orchestrator-core`)

```rust
/// A node whose work is a whole nested DAG, driven under this node's path in the
/// SAME run. Static (author-provided) this slice; slice 3 (`PlanDelta`) produces
/// subgraphs at runtime, slice 5 loops over one. Its output is the sink map (§4.3).
Subgraph { graph: Graph },
```

`Graph`/`Node`/`Dep` already derive `Serialize`/`Deserialize`, so a `Graph` nested
in a `NodeKind` serializes fine.

### 4.2 Execution — `run_subgraph`

A new arm `NodeKind::Subgraph { .. } => self.run_subgraph(run, node, fold).await` and:

```rust
async fn run_subgraph(&self, run, node, fold) -> Result<NodeExec, OrchestratorError> {
    // (1) depth cap — self-DoS backstop (§4.5), BEFORE recursing.
    let depth = node.id.0.matches('/').count();
    if depth + 1 > self.max_depth {
        return Err(OrchestratorError::GlobalCapExceeded { cap: "max_depth".into(), limit: self.max_depth });
    }
    // (2) namespace the nested graph under this node's path.
    let NodeKind::Subgraph { graph } = &node.kind else { unreachable!() };
    let inner = namespace_graph(&node.id.0, graph);
    // (3) drive the nested DAG in the SAME run (shares journal + fold + memo).
    let nested = self.drive(run, &inner, fold).await?;
    // (4) map the nested outcome → this node's NodeExec.
    if let Some(p) = nested.paused {
        return Ok(NodeExec::Paused { reason: format!("subgraph {} paused: {}", node.id.0, p.reason) });
    }
    if let Some((n, msg)) = nested.failed {
        return Ok(NodeExec::Failed {
            message: format!("subgraph {} failed at {}: {}", node.id.0, n.0, msg),
            output: None,
        });
    }
    Ok(NodeExec::Completed(sink_outputs(graph, &node.id.0, &nested.outputs)))
}
```

`namespace_graph(prefix, &graph) -> Graph`: clone the nested graph, rewriting each
node id → `"{prefix}/{id}"`, each `Dep`'s target id likewise, and
`Consolidate.over` likewise (the only `NodeKind` that references another node id; Map/
Loop bodies reference chains/agents, not ids). Deeper nesting is handled by
recursion — a nested `Subgraph`'s own inner graph is namespaced when *its*
`run_subgraph` runs.

### 4.3 Output — the sink map

The Subgraph node's output is a JSON object keyed by each **sink** (terminal —
referenced by no other node's `Dep`) node of the nested graph → that sink's output:
```json
{ "<sink_id>": <output>, … }
```
Always a map (predictable shape, even for a single sink). `sink_outputs(graph,
prefix, nested_outputs)` computes the sink ids from the *original* nested graph's dep
structure, then reads each at its namespaced key `"{prefix}/{sink_id}"` from
`nested.outputs`, including only sinks that produced an output (a soft-dep-skipped
sink is simply absent). The map flows to the Subgraph node's dependents like any
output.

### 4.4 Validation — recursive `validate_dag`

`Graph::validate_dag` recurses: after validating the outer DAG (cycle/dangling-dep),
for every `NodeKind::Subgraph { graph }` it validates the nested graph too. A nested
cycle or dangling dep is a **loud load-time error** (same as today's top-level check),
before any execution. (`run`/`start` call `validate_dag` at entry.)

### 4.5 Global cap — nesting depth (self-DoS groundwork)

Subgraph introduces **nesting depth**. `run_subgraph` derives the current depth from
the node-id path segment count (`node.id.0.matches('/').count()`) and halts loud with
`OrchestratorError::GlobalCapExceeded { cap, limit }` when `depth + 1 > max_depth`.
`Executor.max_depth` defaults to **8**, set via `with_max_depth(n)`. No signature
threading — the path is the depth. This is a **conservative backstop**: the path
segment count also counts Map/Loop child nesting (`{node}/{i}`), so it bounds *total*
structural nesting, not subgraph-nesting alone — acceptable and safe for a DoS guard
with a generous default. (A precise subgraph-only depth counter, and the node-count /
expansions-per-run caps, are slice 3, where runtime `PlanDelta` makes unbounded
expansion real.)

### 4.6 Failure / pause propagation

Reuses the existing recursive machinery: a nested node **failure** → the nested
`drive` returns `RunOutcome.failed` → the Subgraph node is `Failed` → the outer
scheduler cascade-skips its hard-dependents (soft-dependents still run). A nested
**pause** (in-doubt Mutation, quota→pause) → `RunOutcome.paused` → the Subgraph node
is `Paused` → the outer run pauses (no `RunCompleted`; resumable) — exactly the
`MapChildPaused` shape, one level up.

## 5. Decisions

- **D1 — sink-outputs map** (approved (a)): output = `{sink_id: output}`, always a
  map. Not a single designated node (would need a designation field) nor the full
  inner map (verbose; leaks internals).
- **D2 — `max_depth` only this slice** (approved (b)); node-count / expansions-per-run
  caps land with `PlanDelta` (slice 3).
- **D3 — namespace-by-id-rewrite**, not a threaded path prefix — localizes the change
  to `run_subgraph`/`namespace_graph`; `drive`/`run_node` signatures are untouched and
  effect nesting is automatic via the node id.
- **D4 — same-run recursion.** The nested graph shares the run's journal + fold +
  memo (no nested `RunStarted`/`RunCompleted`); resume replays completed inner nodes
  with no re-spend.
- **D5 — depth = path-segment count** — a conservative structural-nesting backstop
  (counts map/loop children too), generous default, no threading.

## 6. Deferred (stated)

- Cross-boundary input/context threading (blackboard **plan-scope**, §186) — a
  subgraph runs its nested graph as authored this slice.
- `Loop` over a `Subgraph` body (full loops-of-graphs) — slice 5.
- `Branch` (slice 2), runtime `PlanDelta` splicing (slice 3).
- Node-count / expansions-per-run caps + precise subgraph-only depth — slice 3.

## 7. Acceptance criteria (TDD)

1. **Executes a nested graph → sink map.** A Subgraph node wrapping a nested 2-node
   line `n1 → n2` (n2 the sink) completes with output `{ "n2": <n2 output> }`; the
   nested nodes' effects are journaled under `"{subgraph}/n1"`/`"{subgraph}/n2"`.
2. **Multiple sinks.** A nested diamond with two sinks → the output map has both sink
   keys.
3. **Recursive `validate_dag`.** A Subgraph whose nested graph has a cycle (or a
   dangling dep) → `validate_dag` (hence `run`/`start`) errors loud at load — before
   execution.
4. **`max_depth` cap.** A Subgraph nested beyond `max_depth` → `GlobalCapExceeded`
   (loud); within it → ok. `with_max_depth` changes the limit (a `with_max_depth(1)`
   run of a 2-deep nesting halts loud).
5. **Failure propagation.** A nested node that fails → the Subgraph node `Failed` →
   its hard-dependent in the outer graph is cascade-skipped; a soft-dependent runs.
6. **Pause propagation.** A nested in-doubt Mutation (or quota→pause) → the Subgraph
   node `Paused` → the run pauses (`RunOutcome.paused` set, no `RunCompleted`).
7. **Resume — no re-spend.** A run that completes a Subgraph's inner nodes then fails
   downstream, resumed, replays the inner nodes from the memo (gateway not re-called
   for them) and completes.
8. **End-to-end.** An outer graph with a Subgraph node driving a nested `Agent`/
   `ModelCall` through the test gateway completes; the Subgraph's sink output is
   present in `outcome.outputs`.
9. **Additive.** Existing node kinds + all current tests are byte-identical (Subgraph
   is a new arm; no behavior change elsewhere).
