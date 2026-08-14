---
title: Execution Graph
doctype: feature
module: orchestrator
status: partial
phase: 3
spec: SP-1, SP-3
source: crates/orchestrator*
---

# Execution Graph

> **Status: Partial (Phase 3 · SP-1/3).** Design §10. Implemented node kinds:
> `ModelCall`, `Agent`, `Map`, `Consolidate`, **`Loop`** (leaf + graph bodies +
> gate-agent), **`Subgraph`**, **`Branch`**, and **`Expand`** (runtime
> PlanDelta / planner-driven).
> Typed `hard`/`soft` edges + `validate_dag` + the round-based ready-node
> scheduler are live. **`Loop`** (SP-1 loop-node
> [`../../superpowers/specs/2026-08-10-sp1-loop-node-design.md`](../../superpowers/specs/2026-08-10-sp1-loop-node-design.md)
> + SP-3 slice 5 loops-of-graphs
> [`../../superpowers/specs/2026-08-14-sp3-coordinator-loops-of-graphs-design.md`](../../superpowers/specs/2026-08-14-sp3-coordinator-loops-of-graphs-design.md)):
> `NodeKind::Loop { body: LoopBody, input, gate: GateSpec, max_iters }` iterates
> `body` at path `"{loop}/{i}"` until the `gate` says Stop or `max_iters` is reached.
> **`LoopBody`** is a **leaf** (`ModelCall`/`Agent` — threads the iteration's output
> text into the next as input, the classic refine) or a **graph** body:
> **`Subgraph`** drives an authored DAG fresh each iteration (no thread — the gate
> decides stop) and **`Expand`** plans+executes each iteration, threading the whole
> iteration output into the next planning input (the **refine** that powers the
> coordinator). **`GateSpec`** is **`Pure(LoopGate)`** (a deterministic predicate over
> the iteration output — recomputed on resume, no journaling; the leaf-body convergence
> path) or **`Agent { agent, stop_when }`** — a **gate-agent** driven at the reserved
> `"{loop}/{i}/__gate__"` whose journaled answer feeds a pure `stop_when` predicate (the
> semantic Continue|Stop is an LLM decision, memoized ⇒ a resume replays it; graph bodies
> converge via the gate-agent, since a pure gate can't match a nested sink map).
> Cap-without-Stop completes best-effort (`converged: false`) — never a bare fail
> (§10.3); a body/gate failure fails the Loop (naming the iteration), a body/gate pause
> pauses it. Resume replays completed iterations + gate decisions from the memo (zero
> re-spend) and stops at the same iteration. A loop-of-`Expand`s is bounded by the
> run-scoped expansion/node/depth caps (charged per iteration, seeded across resume).
> Output: `{ iterations, converged, output }`.
> **The coordinator** = `Loop{ body: Expand{planner}, gate: Agent{…} }` —
> plan→execute→gate→replan, native and resume-safe.
> **Deferred:** a cost/timeout budget backstop (the budget axis is dormant, so
> `max_iters` + the node caps are the backstops), replan-on-failure, and Subgraph-body
> cross-iteration state (plan-scope blackboard).
>
> - **`Subgraph { graph }`** (SP-3 slice 1) — a node whose work is a nested DAG,
>   driven under the node's path (`{node}/…`) in the same run (namespaced ids ⇒ nested
>   effects nest via `effect_id`; resume replays inner nodes with no re-spend). Output
>   is the **sink map** (`{sink_id: output}` for each terminal node). A nested
>   failure/pause propagates to the node (`Failed`/`Paused`) and thus to the outer
>   scheduler. `validate_dag` recurses into nested graphs. Nesting depth is capped by
>   `Executor::with_max_depth` (default 8) → loud `GlobalCapExceeded`.
>   **Known limitation:** a fresh `run` returns the synthesized sink map under the
>   node's id, but a terminal re-`start` reconstructs `outputs` from the journal's
>   per-node `EffectRecorded` — the **namespaced inner nodes** (`{node}/…`), not the
>   sink map (which is never journaled). This fresh-vs-terminal asymmetry is shared
>   with `Map`/`Loop` synthesized outputs; captured, not fixed, in this slice.
>   **Deferred:** cross-boundary input/context (plan-scope blackboard),
>   Loop-over-Subgraph (slice 5), runtime `PlanDelta` (slice 3), node-count/expansion
>   caps (slice 3).
> - **`Branch { on, arms, default }`** (SP-3 slice 2) — a deterministic conditional:
>   tests predecessor `on`'s memoized output with a pure `BranchCond`
>   (`FieldEquals`/`FieldTrue`/`TextContains`, first match wins, required `default`) and
>   runs the selected arm as a nested graph under `{branch}/{label}/…` (reusing the
>   Subgraph machinery). The decision is recomputed on resume (no branch journaling);
>   only the selected arm runs. `on` must be a declared Hard dep (a failed `on`
>   cascade-skips the branch). Output = the selected arm's sink map; failure/pause
>   propagate like `Subgraph`. The output sink-map keys are the SELECTED arm's local
>   sink ids, so they vary by arm — a downstream consumer that needs a stable key
>   should give every arm a common sink node id (e.g. `result`).
> - **`Expand { input, planner }`** (SP-3 slices 3/4A/4B) — a node whose subgraph is
>   produced at **runtime** by a `Planner`, journaled as `PlanExpanded` and spliced under
>   the node's path (reconstructed on resume from the journal — the planner is **not**
>   re-invoked). `PlannerRef::{Injected, Agent, Select}` chooses the planner: an injected
>   trait, a journaled ReAct planner sub-run at `"{expand}/__plan__"`, or a
>   `PlannerSelector` that picks a `planning`-area agent for the goal (its pick journaled
>   as `PlannerSelected`). A pure `feasible` gate validates the plan (agent-refs, reserved
>   ids `__plan__`/`__gate__`, DAG, node count) before splicing; run-scoped
>   `max_expansions`/`max_nodes`/`max_depth` caps backstop self-DoS (a breach is a hard
>   `GlobalCapExceeded`). Output = the spliced subgraph's sink map; failure/pause propagate
>   like `Subgraph`.

A hierarchical, runtime-expandable graph. Node kinds: `Agent`, `Tool`, `Loop`,
`Subgraph`, `Branch`, `Map`, `Consolidate`, `HumanGate`. Edges are typed
**hard** (cascade-skip) vs **soft** (tolerate absence); `Map`/`Consolidate`
carry a completion policy (`fail_fast | best_effort | quorum`). A planner node
can emit a `PlanDelta` subgraph spliced in at runtime (journaled).

## Scenarios

```gherkin
Feature: Execution graph
  Scenario: Soft-edge partial failure still consolidates
    Given a Map of 5 searches over soft edges with quorum(min=3)
    And 2 searches fail
    Then Consolidate runs on the 3 successes and records a failure manifest

  Scenario: Hard-edge failure cascade-skips dependents
    Given node B depends on node A via a hard edge
    And A fails
    Then B is skipped

  Scenario: A loop of a subgraph repeats until its gate says stop
    Given a Loop over a Subgraph with a gate returning Continue then Stop
    Then the subgraph runs twice and the loop exits

  Scenario: Runtime PlanDelta is journaled and replays identically
    Given a planner node emits a subgraph
    Then PlanExpanded is journaled and resume reconstructs the same graph
```

## Notes

- Design intent: budget is the primary loop backstop, `max_iters` secondary. This
  slice ships **`max_iters` only** (the cost-budget axis is dormant); on exhaustion
  a Loop finalizes best-effort (`converged: false`) rather than failing bare
  (design §10.3). The budget/timeout backstop + reserved-synthesis-budget are
  deferred.
