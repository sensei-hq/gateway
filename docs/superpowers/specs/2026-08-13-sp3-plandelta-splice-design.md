---
title: SP-3 slice 3 — runtime PlanDelta / graph splicing (Expand node)
doctype: design
module: orchestrator
spec: SP-3
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§7.2 effect_id / runtime-spliced ids, §7.5 resume/replay, §7.6 PlanExpanded event, §9.5 subagent dispatch = PlanDelta, §10.3 runtime expansion, global caps); ./2026-08-12-sp3-subgraph-node-design.md (slice 1 — nested-graph execution, namespace/drive/sink helpers, max_depth); ./2026-08-12-sp3-branch-node-design.md (slice 2 — nested arms)
date: 2026-08-13
---

# SP-3 slice 3 — runtime `PlanDelta` / graph splicing (`Expand` node)

## 1. Goal

Add `NodeKind::Expand { input }` — a node that produces a **nested subgraph at
runtime** (from an injected `Planner`), journals it as `PlanExpanded`, and drives it
under the node's path in the same run. This is the first **impure** graph producer:
unlike slice 1's `Subgraph` (static author graph) and slice 2's `Branch` (pure
decision over a memoized output) — both of which reconstruct identically on resume
with **no journaling** — an `Expand`'s graph comes from an impure source (ultimately
the LLM planner, slice 4), so it **must be journaled and reconstructed from the
journal on resume**, never re-produced. This slice ships that journaled-splice
machinery + the deferred **node/expansion caps** (`max_expansions`, `max_nodes`) and
extracts the shared **`drive_nested`** helper (its third caller).

## 2. SP-3 slicing (context)

1. `Subgraph` node (slice 1 — done).
2. `Branch` node (slice 2 — done).
3. **This slice** — runtime `PlanDelta` / `PlanExpanded` (graph splicing) +
   node/expansion caps + `drive_nested` extraction.
4. Planner agent (validated plan: JSON + cycle detection + feasibility).
5. Coordinator + loops-of-graphs (Loop over a Subgraph/Expand body) + caps/replan
   hardening.

## 3. Background & impact review

- **The distinction that drives this slice.** `Subgraph`/`Branch` are **pure**: a
  static author graph, or a pure `BranchCond` over a completed-and-memoized output.
  On resume they recompute the identical graph/arm with **no structural journaling**
  (the `LoopGate` no-gate-journaling property). An `Expand`'s subgraph is produced by
  an **impure** `Planner` — re-running it on resume could yield a *different* graph,
  breaking the effect memo and determinism. So the essential new machinery is: journal
  the produced graph as `PlanExpanded{node, subgraph}` and, on resume, **reconstruct
  the graph from that event** rather than re-planning. It is the effect memo, but for
  **graph structure**.
- **Reuse-ready primitives.** `namespace_graph(prefix, &graph)` and
  `sink_outputs(graph, prefix, outputs)` (`executor/subgraph.rs`) already namespace a
  nested graph under a path and fold its sinks; `run_subgraph` / `run_branch` already
  do *depth-cap → namespace → `drive` → sink-map*; `drive(run, &graph, fold)` drives a
  `&Graph` in the same run sharing the journal + memo; `validate_dag()` validates a DAG
  (recursively). Injected collaborators (`gateway`, `reconcilers`, `content`,
  `context`, `hooks`, `handle`) show the seam pattern the `Planner` follows.
- **The slice-1/2 duplication to retire.** `run_subgraph` and `run_branch` are the same
  *depth-cap → namespace_graph → Box::pin(drive) → map paused/failed/Completed(
  sink_outputs)* dance; `run_expand` would be a third copy. This slice extracts
  `drive_nested` (the memory's deferred carryover) so all three call one helper.
- **Impact: additive.** New `NodeKind::Expand`, a `Planner` trait + `with_planner`, a
  `JournalEvent::PlanExpanded`, a `Fold.expansions` field + fold wiring, a
  `run_expand` arm, an `ExpansionBudget` (+ `with_max_expansions`/`with_max_nodes`),
  and the `drive_nested` extraction (behavior-preserving for `Subgraph`/`Branch`).
  `drive`/`run_node` signatures are unchanged for existing kinds. No planner wired ⇒
  an `Expand` node fails loudly and every existing test is byte-identical.

## 4. Design

### 4.1 Types (`orchestrator-core`)

```rust
// graph.rs — new node kind
/// A node that produces a nested subgraph AT RUNTIME (impure), drives it under
/// `"{expand}/…"`, and folds its sink map as output. Unlike `Subgraph` (static)
/// and `Branch` (pure decision), the produced graph is journaled as `PlanExpanded`
/// and reconstructed from the journal on resume — never re-planned. `input` is a
/// static `Value` this slice (author-provided on the node); slice 4/5 threads it
/// from a predecessor's output.
Expand { input: serde_json::Value },
```

```rust
// planner.rs (new) — the injected seam. Slice 3 ships test/stub impls; slice 4
// drops in the LLM-backed planner agent.
#[async_trait::async_trait]
pub trait Planner: Send + Sync {
    /// Produce a subgraph (LOCAL ids, not yet namespaced) from the node input.
    /// Returning `Err` (or an invalid graph) is a node-level failure, not a panic.
    async fn plan(&self, input: &serde_json::Value) -> Result<Graph, OrchestratorError>;
}
```

`Planner` is wired like the other collaborators: `Executor::with_planner(Arc<dyn
Planner>)`, stored `Option<Arc<dyn Planner>>`. No planner ⇒ `Expand` → `Failed`
(`PlannerUnavailable`), so behavior stays byte-identical for graphs without `Expand`.

### 4.2 Journal (`orchestrator-core::journal`)

```rust
/// A runtime graph expansion (§7.2/§7.6/§10.3): node `node` produced `subgraph`.
/// Journaled BEFORE the nested graph is driven, so a crash mid-expansion resumes
/// with the identical structure. The fold reconstructs the spliced graph from
/// this — the memo, but for graph structure. `subgraph` carries LOCAL ids
/// (namespaced under `node` at drive time), so the event is position-independent.
PlanExpanded { node: NodeId, subgraph: Graph },
```

`Graph` already derives `Serialize`/`Deserialize`, so the event serializes as-is.

### 4.3 Execution — `run_expand` + the extracted `drive_nested`

New dispatch arm: `NodeKind::Expand { .. } => self.run_expand(run, node, fold).await`.

```rust
async fn run_expand(&self, run, node, fold) -> Result<NodeExec, OrchestratorError> {
    let NodeKind::Expand { input } = &node.kind else { unreachable!() };

    // (1) RESUME: a node with a journaled PlanExpanded reuses that subgraph —
    //     the planner is NOT re-invoked (determinism, §4.4).
    let subgraph = match fold.expansions.get(&node.id) {
        Some(g) => g.clone(),
        None => {
            // (2) FRESH: produce → validate → cap-check → journal (in that order).
            let Some(planner) = &self.planner else {
                return Ok(NodeExec::Failed {
                    message: format!("expand {}: no planner wired", node.id.0),
                    output: None,
                });
            };
            let g = match planner.plan(input).await {
                Ok(g) => g,
                Err(e) => return self.expand_failed(run, node, format!("planner: {e}")).await,
            };
            if let Err(e) = g.validate_dag() {
                return self.expand_failed(run, node, format!("invalid plan: {e}")).await;
            }
            // caps are a self-DoS backstop → hard `Err` (halts the run), NOT a
            // node Failed — consistent with slice-1 `max_depth` (§4.5).
            self.budget.check(&g)?;
            self.append(run, JournalEvent::PlanExpanded {
                node: node.id.clone(), subgraph: g.clone(),
            }).await?;
            g
        }
    };

    // (3) Drive nested under "{expand}/…" via the shared helper.
    self.drive_nested(run, &node.id.0, &subgraph, fold).await
}
```

`expand_failed` appends `NodeFailed{node, error}` then returns `NodeExec::Failed` — so
an `Expand` failure is durable + surfaced (no silent failure) and cascade-skips its
hard-dependents, matching the `ModelCall` gateway-fail path.

**`drive_nested` (extracted; the third-caller refactor).** The common tail of
`run_subgraph` / `run_branch` / `run_expand`:

```rust
pub(super) async fn drive_nested(
    &self, run: RunId, prefix: &str, graph: &Graph, fold: &Fold,
) -> Result<NodeExec, OrchestratorError> {
    let depth = prefix.matches('/').count();
    if depth + 1 > self.max_depth {
        return Err(OrchestratorError::GlobalCapExceeded {
            cap: "max_depth".into(), limit: self.max_depth,
        });
    }
    let inner = namespace_graph(prefix, graph);
    let nested = Box::pin(self.drive(run, &inner, fold)).await?;
    if let Some(p) = nested.paused {
        return Ok(NodeExec::Paused { reason: format!("{prefix} paused: {}", p.reason) });
    }
    if let Some((n, msg)) = nested.failed {
        return Ok(NodeExec::Failed {
            message: format!("{prefix} failed at {}: {msg}", n.0), output: None,
        });
    }
    Ok(NodeExec::Completed(sink_outputs(graph, prefix, &nested.outputs)))
}
```

- `run_subgraph` → `self.drive_nested(run, &node.id.0, graph, fold)`.
- `run_branch` → compute `(label, selected)`, then
  `self.drive_nested(run, &format!("{}/{}", node.id.0, label), selected, fold)`.
- `run_expand` → `self.drive_nested(run, &node.id.0, &subgraph, fold)`.

The extraction is **behavior-preserving for `Subgraph`** (prefix = `node.id.0`, exactly
as before) and a **deliberate small tightening for `Branch`**:
- **Message wording:** pause/fail messages fold `prefix` in (was `node.id.0` for
  subgraph, `"{node} arm {label}"` for branch); the branch message now reads
  `"{branch}/{label} …"` — the slice-2 tests are updated for the wording.
- **`Branch` depth bound becomes exact:** old `run_branch` capped on `node.id.0`
  (excluding the arm label) while noting (slice-2 §4.2) that the arm's nodes actually
  live one segment deeper. `drive_nested` caps on the real namespacing prefix
  `"{node}/{label}"`, so a branch now hits `max_depth` **one level earlier** — the
  accurate bound the slice-2 comment called a "backstop, not an exact bound." No
  existing slice-2 AC pins a branch depth boundary, so this changes no passing test;
  any depth test added for branch uses the exact bound.

Only the selected/produced graph is ever namespaced/driven/journaled.

### 4.4 Determinism / resume (the crux)

- `Fold` gains `expansions: HashMap<NodeId, Graph>`, folded from `PlanExpanded`
  events by `fold_journal` (`support.rs`) — the structural analog of `memo`.
- On resume, `run_expand` finds the node in `fold.expansions` and **replays the
  journaled subgraph**; the planner is never re-invoked. The nested nodes then replay
  from the effect `memo` (no re-spend) exactly like a static `Subgraph`.
- **Ordering guarantee:** `PlanExpanded` is appended **before** the nested `drive`, so
  a crash between "planned" and "nested work done" still reconstructs the same
  structure and resumes the nested tail.
- **Determinism comes from journaling the OUTPUT graph** (not the input): a changed
  `input` on resume is irrelevant — the journaled subgraph wins. (An input-hash fence
  on `PlanExpanded` is deferred, §6: unneeded while `input` is static.)

### 4.5 Global caps — a run-scoped `ExpansionBudget`

```rust
struct ExpansionBudget {
    expansions: AtomicUsize, nodes: AtomicUsize,
    max_expansions: usize, max_nodes: usize,   // defaults 32 / 512
}
impl ExpansionBudget {
    fn check(&self, g: &Graph) -> Result<(), OrchestratorError> {
        if self.expansions.load(Relaxed) + 1 > self.max_expansions {
            return Err(OrchestratorError::GlobalCapExceeded {
                cap: "max_expansions".into(), limit: self.max_expansions });
        }
        if self.nodes.load(Relaxed) + g.nodes.len() > self.max_nodes {
            return Err(OrchestratorError::GlobalCapExceeded {
                cap: "max_nodes".into(), limit: self.max_nodes });
        }
        self.expansions.fetch_add(1, Relaxed);
        self.nodes.fetch_add(g.nodes.len(), Relaxed);
        Ok(())
    }
}
```

- Held as `Arc<ExpansionBudget>` on the executor; `run_inner`/`start_inner` install a
  **fresh** budget per run (the `pinned()` clone idiom — the same self-clone that
  pins the registry generation), so nested and deep `run_expand` calls all share one
  Arc and count against the **run** total.
- `check(&g)` runs **before** journaling `PlanExpanded`. Exceeding either cap →
  `GlobalCapExceeded { cap, limit }` — a **hard `Err` that halts** the run (self-DoS
  backstop, identical to slice 1's `max_depth`), *not* a node `Failed`. Nothing is
  journaled for the over-cap expansion.
- **Resume seeds the counters** from the loaded journal (count `PlanExpanded` events;
  sum their `subgraph.nodes.len()`) before driving, so a resume cannot exceed a cap by
  "forgetting" prior expansions.
- Setters: `with_max_expansions(n)` (default 32), `with_max_nodes(n)` (default 512);
  `max_depth` unchanged (default 8, from slice 1).

### 4.6 Output & propagation

Identical to `Subgraph` (via `drive_nested`): the `Expand` node's output is the
produced subgraph's **sink-outputs map**; a nested node **failure** → `Expand`
`Failed` → outer cascade-skip (hard); a nested **pause** (in-doubt Mutation, quota) →
`Expand` `Paused` → the run pauses (no `RunCompleted`, resumable).

### 4.7 Validation

`validate_dag` needs **no new recursion** for `Expand`: there is no static nested graph
at load time (`input` is a `Value`). The runtime-produced graph is validated inside
`run_expand` (`g.validate_dag()`); a malformed delta (nested cycle / dangling dep) is a
node `Failed` (§4.3), not a load-time error. Existing `validate_dag` tests are
byte-identical.

## 5. Decisions

- **D1 — injected `Planner` trait + `NodeKind::Expand { input }`** (approved): isolates
  the splice/journal/caps machinery from planner concerns; slice 4 drops in the LLM
  planner. Rejected: extending `Agent` to parse a `plan_delta` (couples slice 3 to
  ReAct output parsing, pulls planner concerns early); a *pure* `Expand`
  from a predecessor output (no journaling ⇒ doesn't exercise the machinery — a mere
  dynamic `Subgraph`).
- **D2 — nested under `"{expand}/…"`** (approved): reuses slice-1/2 topology; the
  `Expand` node stays un-`Completed` until its plan finishes, so its dependents wait
  for free; `drive()`'s immutable `&Graph` is untouched. Rejected: sibling splice into
  the outer graph (mutable `drive` node set, id-collision management, ready-set rebuild
  over a growing graph — a much larger change with no need slice 3 has, §6).
- **D3 — journal-reconstructed structure** (approved): `PlanExpanded{node, subgraph}`
  appended **before** the nested drive; resume replays the journaled graph, never
  re-plans. Determinism is anchored to the journaled **output** graph, so a changed
  `input` on resume is irrelevant.
- **D4 — extract `drive_nested`** (approved): one helper (depth-cap → `namespace_graph`
  → `drive` → `sink_outputs`) for `Subgraph`/`Branch`/`Expand`; retires the slice-1/2
  duplication (the memory's deferred carryover).
- **D5 — two-tier failure model** (approved): planner error / invalid produced graph /
  no planner → node `Failed` (journaled `NodeFailed`, cascade-skip, resumable);
  `max_expansions`/`max_nodes`/`max_depth` exceeded → hard `Err` halt (self-DoS).
- **D6 — caps: `max_expansions` (32) + `max_nodes` (512)** via a run-scoped
  `ExpansionBudget`, seeded from the journal on resume so the cap spans the crash seam.

## 6. Deferred (stated)

- The **real** planner agent — validated JSON (self-correcting), Kahn cycle detection,
  feasibility (slice 4). Slice 3 validates only `validate_dag()` on the delta.
- `Expand.input` threaded from a **predecessor's** output / blackboard (slice 4/5);
  a static `Value` this slice.
- **Sibling** splice into the outer graph (mutable `drive` node set) — nesting covers
  the slice-3 need; revisit only if a real cross-sibling-dependency case appears.
- Loops-of-graphs (`Loop` over a `Subgraph`/`Expand` body) + replan hardening (slice 5).
- **Input-hash fence** on `PlanExpanded` (halt if the recomputed input diverges from
  the recorded one on resume, mirroring the effect memo's input-hash fence) — unneeded
  while `input` is static; revisit in slice 4 when input becomes dynamic.
- Precise subgraph-only depth counter (the path-segment count still bounds *total*
  structural nesting, the slice-1 conservative backstop).

## 7. Acceptance criteria (TDD)

1. **Expand drives a produced plan → sink map.** A stub `Planner` returning a 2-node
   line `n1 → n2` (n2 the sink) → `Expand` output `{ "n2": <n2 output> }`; the inner
   effects are journaled under `"{expand}/n1"` / `"{expand}/n2"`.
2. **`PlanExpanded` journaled before nested work.** The event carries the produced
   subgraph and precedes the inner nodes' `NodeStarted`/`EffectRecorded`.
3. **Resume reconstructs from journal, never re-plans.** A run that crashes after
   `PlanExpanded`, resumed with a planner rigged to return a *different* graph, uses the
   **journaled** graph, replays the inner nodes from the memo (gateway not re-called),
   and completes.
4. **`Fold.expansions`.** A run that completes an `Expand`'s inner nodes then fails
   downstream, resumed, does not re-plan or re-spend (the expansion + inner effects fold
   from the journal).
5. **Planner error → `Failed`.** `plan()` returns `Err` → `Expand` `Failed` (journaled
   `NodeFailed`) → its hard-dependent cascade-skipped; a soft-dependent runs.
6. **Invalid produced graph → `Failed`.** A planner returning a graph with a nested
   cycle (or dangling dep) → `Expand` `Failed` (loud), run resumable; no `PlanExpanded`
   is journaled.
7. **No planner wired → `Failed`.** An `Expand` node with no `with_planner` → loud
   `Failed`; every existing (Expand-free) test byte-identical.
8. **`max_expansions` cap.** More `PlanDelta`s than `max_expansions` →
   `GlobalCapExceeded` (hard halt); within → ok; `with_max_expansions(1)` halts a
   2-expansion run.
9. **`max_nodes` cap.** Cumulative spliced-node count over `max_nodes` (across multiple
   and nested expansions) → `GlobalCapExceeded`; **spans resume** — a resume seeds the
   counter from the journal, so it cannot exceed the cap by forgetting prior nodes.
10. **Nested failure / pause propagation.** A failing node inside the produced plan →
    `Expand` `Failed` → outer hard-dependent cascade-skipped; a nested in-doubt Mutation
    → `Expand` `Paused` → run pauses (`RunOutcome.paused` set, no `RunCompleted`).
11. **`drive_nested` refactor is behavior-preserving.** All existing `Subgraph` tests
    pass unchanged; `Branch` tests pass with updated pause/fail message wording and the
    exact (`"{node}/{label}"`-derived) depth bound (§4.3). A `Subgraph`/`Branch` still
    enforces `max_depth`, folds the sink map, and propagates failure/pause identically.
12. **End-to-end.** An `Expand` whose produced plan contains an `Agent`/`ModelCall`
    drives it through the test gateway to completion; the plan's sink output is the
    `Expand`'s output and appears in `outcome.outputs`.
13. **Additive.** Existing node kinds + all current tests are byte-identical when no
    `Expand` node / no `Planner` is present.
