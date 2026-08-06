---
title: Execution Graph
doctype: feature
module: orchestrator
status: planned
phase: 3
spec: SP-1, SP-3
source: orchestrator (new)
---

# Execution Graph

> **Status: Planned (Phase 3 · SP-1/3).** Design §10.

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

- Budget is the primary loop backstop; `max_iters` is secondary; on exhaustion a Loop finalizes best-effort rather than failing bare (design §10.3).
