---
title: Orchestrator — Module Reference
doctype: module
module: orchestrator
status: planned
---

# Orchestrator

The agentic execution framework that wraps the gateway: a hierarchical,
runtime-expandable graph of agents on a durable step-journal, resumable without
re-spending tokens and with no silent failures. Full design in
[`../../superpowers/specs/2026-08-06-sensei-orchestrator-design.md`](../../superpowers/specs/2026-08-06-sensei-orchestrator-design.md).

## Status

| Feature | Status | Source | Notes |
|---|---|---|---|
| [Execution graph](execution-graph.md) | Planned (Phase 3 · SP-1/3) | `orchestrator` (new crate) | nodes · typed edges · aggregation · runtime `PlanDelta` |
| [Durable journal](durable-journal.md) | Planned (Phase 3 · SP-1) | `orchestrator-core` | effect classes · two-phase + in-doubt · replay/resume |
| [Agents · skills · tools](agents-skills-tools.md) | Planned (Phase 3 · SP-1/2) | `orchestrator` | md+frontmatter registry · agent runtime · planner/coordinator |
| [Shared context](shared-context.md) | Planned (Phase 3 · SP-1) | `orchestrator` | scoped blackboard · refs-not-blobs · prompt budgeting |
| [Hooks](hooks.md) | Planned (Phase 3 · SP-1) | `orchestrator-core` | per-agent progress · attempts bubbling · replay suppression |

## Notes

- New crates in this workspace: `orchestrator-core` (types/traits), `orchestrator` (engine), `orchestrator-store` (adapters).
- Walking-skeleton first slice = a deep-research mini (design §17). First *implementation* slice overall is gateway [routing](../routing/README.md) SP-0.
