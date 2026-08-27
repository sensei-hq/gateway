---
title: SP-3 slice 5 — Coordinator + loops-of-graphs (Loop over Subgraph/Expand + gate-agent)
doctype: design
module: orchestrator
spec: SP-3
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§10.3 loops-of-graphs, §229 coordinator/finalize/budget-backstop); ./2026-08-10-sp1-loop-node-design.md (the leaf-body Loop: refine-thread, pure LoopGate, cap→best-effort); ./2026-08-13-sp3-plandelta-splice-design.md (Expand/drive_nested/caps); ./2026-08-13-sp3-planner-agent-design.md + ./2026-08-13-sp3-planner-selector-design.md (the planner path drive_expand_with extracts)
date: 2026-08-14
---

# SP-3 slice 5 — Coordinator + loops-of-graphs

## 1. Goal

Extend `NodeKind::Loop` from a **leaf** body (ModelCall/Agent) to a **graph** body — a
`Subgraph` (drive an authored graph per iteration) or an `Expand` (plan+execute per
iteration) — plus a **gate-agent** option alongside the pure `LoopGate`. This delivers the
two SP-3 marquee shapes:

- **loops-of-graphs** — iterate a nested graph until a gate says Stop (or `max_iters`);
- **the coordinator** — `Loop{ body: Expand, gate: Agent }`: each iteration *plans over the
  refined input → executes → a gate-agent evaluates → continue/stop*, i.e. the
  strategos plan→execute→evaluate→replan loop, native and resume-safe.

This is the **final SP-3 slice** (closes the hierarchical executor). It reuses the slice-1/3
`drive_nested`, the slice-4A/4B planner path (extracted into a shared `drive_expand_with`),
the slice-3 caps (which backstop a loop-of-expands for free), and the SP-1 Loop's
refine-thread + cap→best-effort finalize.

## 2. SP-3 slicing (context)

1. Subgraph ✅ · 2. Branch ✅ · 3. PlanDelta/Expand ✅ · 4A Planner ✅ · 4B Selector ✅.
5. **This slice** — Coordinator + loops-of-graphs. **SP-3 complete after this.**

## 3. Background & impact review

- **The SP-1 Loop** (`run_loop`, `fanout.rs`): `Loop{ body: MapBody, input, gate: LoopGate,
  max_iters }` iterates `body` at `"{loop}/{i}"`, threads each iteration's output *text*
  forward as the next input (refine), stops on the pure `gate` or `max_iters`; a body failure
  fails the Loop, an Agent-body pause pauses it, cap-without-Stop → best-effort
  `{iterations, converged:false, output}`. `NodeStarted`/`NodeCompleted` are fold-guarded.
- **Reuse-ready:** `drive_nested(run, kind_label, prefix, graph, fold)` (slice 1/3) drives a
  namespaced nested graph → `NodeExec`; `run_expand`'s fresh path (slice 4A/4B) does
  produce→`feasible`→`check_expansion_budget`→journal `PlanExpanded`→`drive_nested`, resume via
  `fold.expansions`/`fold.selections`, all **keyed by the node's path** — so it factors to an
  arbitrary path. The slice-3 run-scoped caps (`max_expansions`/`max_nodes`/`max_depth`) are
  charged per expansion and seeded across resume ⇒ a loop-of-expands is bounded automatically.
- **Impact:** the Loop's `body: MapBody` → `body: LoopBody` and `gate: LoopGate` → `gate:
  GateSpec` (a mechanical migration of ~12 `NodeKind::Loop{…}` construction sites, behavior-
  identical for the existing leaf/pure cases); a `drive_expand_with` extraction from
  `run_expand` (behavior-preserving); two new `run_loop` body arms (`Subgraph`/`Expand`) + the
  gate-agent branch. `MapBody` (Map/Consolidate) is **untouched** — the graph bodies are
  `LoopBody`-only. `Map`/`Consolidate`/`Branch`/`Subgraph`/`Expand` and their tests are
  byte-identical.

## 4. Design

### 4.1 Types (`orchestrator-core`, `graph.rs`)

```rust
NodeKind::Loop { body: LoopBody, input: serde_json::Value, gate: GateSpec, max_iters: usize }

/// What a `Loop` runs per iteration. The two leaf variants mirror `MapBody`; the two
/// graph variants (slice 5) drive a nested graph per iteration.
pub enum LoopBody {
    ModelCall { chain: String },
    Agent(crate::registry::AgentRef),
    Subgraph(Box<Graph>),                      // drive an authored graph each iteration
    Expand { planner: PlannerRef },            // plan+execute each iteration (coordinator)
}

/// A Loop's stop decision (slice 5). `Pure` is the SP-1 pure predicate (no journaling);
/// `Agent` runs a gate-agent over the iteration output, then a pure `stop_when` predicate
/// over the AGENT's answer decides stop (the agent turn is journaled ⇒ resume replays it).
pub enum GateSpec {
    Pure(LoopGate),
    Agent { agent: crate::registry::AgentRef, stop_when: LoopGate },
}
```
`Box<Graph>` breaks the recursive type. `PlannerRef`/`LoopGate` already exist. **Migration:**
existing `Loop{ body: MapBody::X, gate: g, … }` → `Loop{ body: LoopBody::X, gate:
GateSpec::Pure(g), … }` at the ~12 sites — mechanical, behavior-identical.

### 4.2 Per-iteration body dispatch (`run_loop`, `fanout.rs`)

Iteration `i` drives the body at path `"{loop}/{i}"`, yielding an output `Value` (or a
terminal Failed/Paused that the Loop propagates — the existing leaf handling):

| `LoopBody` | drive | refine → next `current_input` |
|---|---|---|
| `ModelCall{chain}` | `run_map_child_modelcall` (existing) | `{prompt: text}` |
| `Agent(a)` | `drive_agent("{loop}/{i}", a, current_input)` (existing) | `text` |
| `Subgraph(g)` | `drive_nested("loop", "{loop}/{i}", g, fold)` → sink map | *none* — fresh re-run; the gate decides stop |
| `Expand{planner}` | `drive_expand_with("{loop}/{i}", current_input, planner, fold)` → sink map | the iteration **output** (the planner refines over the prior result) |

`drive_nested`/`drive_expand_with` return `NodeExec`: `Completed(sink map)` → the iteration
output; `Failed` → the Loop fails (naming the iteration); `Paused` → the Loop pauses. This is
exactly how the SP-1 Agent-body Failed/Paused already flow.

### 4.3 The gate — pure or agent (`run_loop`)

The gate inspects the iteration **output**. For a **leaf** body that output is a flat
`{model, text}`, so a *pure* gate (`FieldTrue`/`FieldEquals`/`TextContains`) reads it
directly — this is the SP-1 convergence path and stays fully supported. For a **graph** body
the output is the **sink map** `{sink_id: {model, text}, …}`: a pure gate only sees top-level
fields, and every real sink value is nested one level under its id, so a pure gate **does not
converge over a graph body in practice** — it simply runs to `max_iters` and finalizes
best-effort (`converged:false`). Semantic convergence over a graph result is therefore the
**gate-agent's** job (§4.3 Agent arm below): it reads the whole nested output and answers
Continue|Stop. In short: *pure gate = leaf-body convergence; gate-agent = graph-body
convergence.* After computing the iteration `output`:
- `GateSpec::Pure(g)` → `g.should_stop(&output)` — recomputed each drive, **no journaling**
  (unchanged from SP-1).
- `GateSpec::Agent{ agent, stop_when }` → drive the gate-agent over `output` at the reserved
  path **`"{loop}/{i}/__gate__"`** (its ReAct turns are Pure effects, journaled + memoized);
  take its answer and apply the pure **`stop_when.should_stop(&gate_answer)`**. So the
  *semantic* Continue|Stop is an LLM decision (journaled ⇒ a resume replays the identical
  decision from the memo), while the *extraction* stays a pure predicate — **no gate-specific
  journal event**, mirroring the SP-1 no-gate-journaling property. A gate-agent Failed/Paused
  propagates (Loop Failed/Paused).

### 4.4 The `drive_expand_with` extraction (`expand.rs`)

Factor `run_expand`'s fresh+resume core into:
```rust
pub(super) async fn drive_expand_with(
    &self, run: RunId, path: &str, input: &serde_json::Value,
    planner: &PlannerRef, fold: &Fold,
) -> Result<NodeExec, OrchestratorError>
```
— the exact slice-4A/4B pipeline (resume via `fold.expansions.get(NodeId(path))`; else the
`match planner { Injected | Agent | Select }` dispatch → `PlannedGraph` → `feasible` →
`check_expansion_budget` → journal `PlanExpanded{ node: NodeId(path) }` (fires
`on_plan_expanded`) → `drive_nested("expand", path, graph, fold)`), keyed by `path` instead
of `node.id.0`. Then:
- `run_expand(node)` = `drive_expand_with(run, &node.id.0, input, planner, fold)`
  (behavior-preserving — all slice-4A/4B `expand_*`/`select_*` tests pass unchanged);
- Loop-Expand iteration `i` = `drive_expand_with(run, &format!("{}/{}", loop.id.0, i),
  &current_input, planner, fold)` — each iteration re-plans over the refined input, journals
  its own `PlanExpanded{ "{loop}/{i}" }` + `PlannerSelected`, and **charges the run-scoped
  caps** (so `max_iters × per-iteration expansion ≤ max_expansions/max_nodes` — the self-DoS
  backstop composes for free).

### 4.5 Determinism / resume · finalize · caps

- **Resume:** every iteration's effects nest under `"{loop}/{i}/…"` (body) and
  `"{loop}/{i}/__gate__"` (gate-agent); Expand iterations reuse their journaled
  `PlanExpanded`/`PlannerSelected` at `"{loop}/{i}"`. A resume replays completed iterations +
  gate decisions from the memo (no re-spend) and **stops at the same iteration**. No new
  Loop-level journal event; the fold-guarded `NodeStarted`/`NodeCompleted` are unchanged.
- **Finalize (best-effort):** gate-Stop before the cap ⇒ `{converged: true}`; cap-without-Stop
  ⇒ `{iterations, converged: false, output}` best-effort — never a bare fail (SP-1 §10.3
  preserved). *The synthesis pass (a reserved-budget "synthesize from what exists" finalize)
  is deferred, §6.*
- **Caps:** a loop-of-Expands is bounded by the slice-3 run-scoped caps (each iteration's
  expansion charges them; seeded across resume); `max_depth` bounds nesting; `max_iters`
  bounds the iteration count. A cap breach is a hard `Err` halt (self-DoS), as in slice 3.

### 4.6 The coordinator (composition, not a new node kind)

`Loop{ body: Expand{planner}, gate: Agent{agent, stop_when} }` **is** the coordinator: iterate
*plan(current_input) → execute → gate-agent evaluates the result → Stop when good, else refine
current_input = the result and replan*. No new machinery — it falls out of the Expand body +
the gate-agent + the refine-thread. This is the deliverable §229 calls "planner + coordinator
agents · loops-of-graphs."

## 5. Decisions

- **D1 — graph bodies are `LoopBody`-only** (approved "broad"): a dedicated `LoopBody`
  {ModelCall, Agent, Subgraph, Expand} keeps `MapBody` (Map/Consolidate) leaf-only and
  untouched. Rejected: extending `MapBody` (would force Subgraph/Expand handling — or loud
  rejection — into Map/Consolidate).
- **D2 — Subgraph body = fresh re-run, gate decides stop** (approved): no cross-iteration
  input threading for authored graphs (that needs the deferred plan-scope blackboard); each
  iteration drives the graph fresh at `"{loop}/{i}"` (an LLM subgraph re-samples). The
  **Expand body carries the refine** (its planning input is the threaded output).
- **D3 — gate-agent = drive-agent-then-pure-predicate** (approved): the gate-agent produces a
  journaled answer; a pure `stop_when: LoopGate` over that answer decides stop — reuses
  `LoopGate`, adds no gate-specific journal event, and stays resume-deterministic (the agent
  turn is memoized).
- **D4 — extract `drive_expand_with`** (behavior-preserving): `run_expand` and the Loop-Expand
  iteration share the whole slice-4A/4B planner path, keyed by an arbitrary path.
- **D5 — caps compose, no new budget axis** (approved scope): the slice-3 run-scoped caps +
  `max_iters` backstop a loop-of-expands; a **cost/token budget model** (the §229 "budget is
  the primary backstop") is deferred (the gateway budget axis is dormant).
- **D6 — reserved segments `__plan__` (existing) + `__gate__` (new)**: the gate-agent path
  `"{loop}/{i}/__gate__"` cannot collide with a body node (a Subgraph body node named
  `__gate__` would; `feasible` already rejects `__plan__` — the Loop does not run `feasible`
  on a `Subgraph` body, so this is a stated authoring constraint, not enforced this slice).

## 6. Deferred (stated)

- **Budget-primary-backstop + reserved synthesis budget + finalize-synthesize** (needs a
  cost/token budget model; §229) — this slice's finalize is the SP-1 best-effort
  `converged:false`, no synthesis pass.
- **Replan-on-failure** — a failed iteration caught by the Loop to replan (retry/continue)
  rather than failing the Loop; this slice keeps SP-1's "body failure fails the Loop."
- **Subgraph-body cross-iteration state** (plan-scope blackboard threading); **tier-downgrade-
  on-resume replan** (§202); a `feasible`/reserved-id guard on `Subgraph`/`Expand` **loop
  bodies** (authoring constraint only this slice).

## 7. Acceptance criteria (TDD)

1. **Migration is behavior-preserving.** All SP-1 Loop tests pass with `body: LoopBody::X` /
   `gate: GateSpec::Pure(g)` — leaf ModelCall/Agent bodies + pure gate + refine + cap→best-
   effort byte-identical.
2. **Subgraph body iterates (best-effort).** A `Loop{ body: Subgraph(2-node line), gate:
   Pure(...) }` drives the graph fresh at `"{loop}/{i}"` each iteration, yielding a sink-map
   output `{sink_id: {model, text}}`; each iteration's inner nodes are journaled under
   `"{loop}/{i}/…"`. Because a pure gate cannot match a nested sink map (§4.3), the Loop runs
   `max_iters` and finalizes best-effort `{iterations, converged:false, output: <sink map>}`.
   *Graph-body convergence (a gate deciding Stop over a nested result) is exercised by AC5
   (gate-agent) and AC10 (coordinator), not by a pure gate.*
3. **Expand body refines (the coordinator core).** A `Loop{ body: Expand{planner} }` — each
   iteration plans+executes; the refine-thread feeds iteration `i`'s output into iteration
   `i+1`'s planning input (assert the 2nd iteration's planner input carries the 1st's output);
   the gate stops.
4. **`drive_expand_with` behavior-preserving.** All slice-4A `expand_*` + slice-4B `select_*`
   tests pass unchanged (`run_expand` now delegates).
5. **Gate-agent decides stop.** A `Loop{ gate: Agent{agent, stop_when} }` — the gate-agent
   evaluates each iteration output at `"{loop}/{i}/__gate__"`, its answer drives `stop_when`
   → the Loop stops at the expected iteration; a gate-agent turn is journaled (no gate event).
6. **Determinism / resume.** A Loop that completes N iterations then fails downstream,
   resumed, replays the iterations + gate decisions from the memo (gateway not re-called) and
   stops at the same iteration. (Mutation-verified for the gate-agent path.)
7. **Failure / pause propagation.** A body-iteration failure → Loop `Failed` (names the
   iteration); an in-doubt Mutation inside a Subgraph/Expand iteration (or an Expand planner
   pause) → Loop `Paused` (run pauses, no `RunCompleted`).
8. **Caps compose.** A loop-of-Expands whose cumulative expansions exceed `max_expansions`
   (or nodes `max_nodes`) → `GlobalCapExceeded` (hard halt), bounded; within → ok.
9. **Cap-without-stop → best-effort.** A Loop whose gate never stops runs `max_iters` and
   completes `{converged:false}` (never a bare fail) — SP-1 behavior preserved.
10. **End-to-end coordinator.** `Loop{ body: Expand{planner}, gate: Agent{...} }` through the
    test gateway iterates plan→execute→gate to convergence; `on_plan_expanded` fires per
    iteration; the final output carries the converged result.
11. **Additive.** Existing Loop + all slice-1..4B tests byte-identical (aside from the
    mechanical `LoopBody`/`GateSpec` construction migration).
