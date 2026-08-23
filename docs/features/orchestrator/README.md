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
| [Execution graph](execution-graph.md) | Partial (Phase 3 · SP-1/3) | `crates/orchestrator*` | `ModelCall`/`Agent`/`Map`/`Consolidate`/`Loop` · typed edges + `validate_dag` · ready-node scheduler · aggregation · gated iteration (deterministic gate, `converged` finalize, resume-replay) · `Subgraph`/`Branch`/`PlanDelta` deferred |
| [Durable journal](durable-journal.md) | Partial (SP-1 · SP-DATA-1 · SP-DATA-5) | `orchestrator-core` · `orchestrator-store` | effect classes · two-phase + in-doubt · replay/resume · **durable Postgres backend** (`PostgresJournal`/`ContentStore`/`ContextStore`, `postgres` feature) — cross-process resume, zero re-spend, `format_version` fence; feature-off byte-identical · **durable spend ledger** (`EffectRecorded{usage}`/`RunStarted{budget}`/`BudgetRaised`, folded by effect id) |
| [Agents · skills · tools](agents-skills-tools.md) | Partial (Phase 3 · SP-1 slice 2) | `orchestrator` | in-memory registry (frontmatter subset) · prompt assembly + per-turn window budget · Pure-only ReAct loop · resume-without-re-spend inside the loop |
| [Shared context](shared-context.md) | Partial (Phase 3 · SP-1) | `crates/orchestrator*` | scoped blackboard wired · node outputs publish to Run · agents read dependency context (dependency-scoped, deterministic) · refs-not-blobs · resume-rehydrated |
| [Hooks](hooks.md) | Partial (Phase 3 · SP-1) | `crates/orchestrator*` | `OrchestratorHooks` (no-op defaults) · run/node/agent/context lifecycle · fired from `append` (can't-miss, replay-suppressed) · opt-in byte-identical · attempts-bubbling/stream/HookError-channel deferred |

## Notes

- New crates in this workspace: `orchestrator-core` (types/traits), `orchestrator` (engine), `orchestrator-store` (adapters — in-memory + the SP-DATA-1 Postgres backend behind a `postgres` feature; dbd schema in `gateway/database/`), and `torii` (SP-DATA-4 — the operator control plane, and the workspace's **first binary**; a lib+bin pair so its commands are callable from tests).
- Walking-skeleton first slice = a deep-research mini (design §17). First *implementation* slice overall is gateway [routing](../routing/README.md) SP-0.
- **Persistence (SP-DATA):** durable run state (journal + CAS + context in Postgres) landed in SP-DATA-1; **`PostgresConfigSource` + durable `config_versions` (the cross-process config-version fence)** in SP-DATA-2; the **durable scheduler** (`Scheduler` + `scheduled_runs` `SchedulerStore` — wakes a paused run at its `resume_after` cross-process, exactly-once, + observe/intervene) in SP-DATA-3; and **`torii`, the management CLI** (the first production `tick()` caller, two boot tiers, honest observe/intervene reporting, and the durable config write path — which closed SP-DATA-2's two carry-forwards with the atomic `load_versioned()` read and the bump-first `store_and_bump()` write) in SP-DATA-4; and the **per-run token budget** (the journal as a durable spend ledger — `EffectRecorded{usage}` + `RunStarted{budget}` + `BudgetRaised`, folded **by effect id** so a duplicate record cannot double-count on resume; one metered-dispatch chokepoint gating all four model-call producers; exhaustion → the HOTL pause class; `torii run submit|status|wake --budget-tokens`) in SP-DATA-5 — see the [overview decision log](../../superpowers/orchestrator-overview.md#3-decision-log-asked--confirmed). **SP-DATA is COMPLETE (all five slices).** Known edges left open: `LlmPlannerSelector::select` is a fifth, unbudgeted and unledgerable model-call site, and a gated `Map` fan-out journals one `RunPaused` per gated child.
