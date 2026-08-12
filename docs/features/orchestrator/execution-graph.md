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
> `ModelCall`, `Agent`, `Map`, `Consolidate`, and **`Loop`** (walking skeleton).
> Typed `hard`/`soft` edges + `validate_dag` + the round-based ready-node
> scheduler are live. **`Loop`** (loop-node design
> [`../../superpowers/specs/2026-08-10-sp1-loop-node-design.md`](../../superpowers/specs/2026-08-10-sp1-loop-node-design.md)):
> `NodeKind::Loop { body: MapBody, input, gate: LoopGate, max_iters }` iterates
> `body` at path `"{loop}/{i}"`, threads each iteration's output into the next as
> input (refine), and stops when a **deterministic** `LoopGate`
> (`TextContains`/`FieldTrue`, pure over the body output) fires or `max_iters` is
> reached. Cap-without-Stop completes best-effort (`converged: false`) — never a
> bare fail (§10.3); a body failure fails the Loop (naming the iteration); an
> Agent-body pause pauses the Loop. Resume replays completed iterations (memo-hit,
> zero re-spend) and recomputes the pure gate, so it stops at the same iteration —
> no gate journaling. Output: `{ iterations, converged, output }`.
> **Deferred:** `Subgraph`/multi-node loop bodies, `Branch`, `PlanDelta` runtime
> expansion, an LLM/fuzzy gate agent, a cost/timeout budget backstop (the budget
> axis is dormant, so `max_iters` is the only backstop), and nested-loop/global
> node caps.

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
