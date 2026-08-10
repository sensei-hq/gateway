---
title: Orchestrator — Module Reference
doctype: module
module: orchestrator
status: partial
---

# Orchestrator

The agentic execution framework that wraps the gateway: a hierarchical,
runtime-expandable graph of agents on a durable step-journal, resumable without
re-spending tokens and with no silent failures. Full design in
[`../../superpowers/specs/2026-08-06-sensei-orchestrator-design.md`](../../superpowers/specs/2026-08-06-sensei-orchestrator-design.md).

## Status

| Feature | Status | Source | Notes |
|---|---|---|---|
| [Durable executor (spine)](durable-executor.md) | Partial (SP-1 slice 1) | `crates/orchestrator*` | linear `ModelCall` (Pure) graph · effect-id + input-hash memoization · resume/fold · version fence · real reference-chain e2e |
| [Execution graph](execution-graph.md) | Planned (Phase 3 · SP-1/3) | `orchestrator` (new crate) | nodes · typed edges · aggregation · runtime `PlanDelta` |
| [Durable journal](durable-journal.md) | Planned (Phase 3 · SP-1) | `orchestrator-core` | effect classes · two-phase + in-doubt · replay/resume |
| [Agents · skills · tools](agents-skills-tools.md) | Partial (Phase 3 · SP-1 slice 2) | `orchestrator` | in-memory registry (frontmatter subset) · prompt assembly + per-turn window budget · Pure-only ReAct loop · resume-without-re-spend inside the loop |
| [Shared context](shared-context.md) | Partial (Phase 3 · SP-1) | `crates/orchestrator*` | scoped blackboard wired · node outputs publish to Run · agents read dependency context (dependency-scoped, deterministic) · refs-not-blobs · resume-rehydrated |
| [Hooks](hooks.md) | Planned (Phase 3 · SP-1) | `orchestrator-core` | per-agent progress · attempts bubbling · replay suppression |

## Notes

- New crates in this workspace: `orchestrator-core` (types/traits), `orchestrator` (engine), `orchestrator-store` (adapters).
- Walking-skeleton first slice = a deep-research mini (design §17). First *implementation* slice overall is gateway [routing](../routing/README.md) SP-0.
