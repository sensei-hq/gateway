# Checkpoint

**Agentic completion — epic [torii#45](https://github.com/sensei-hq/torii/issues/45).** Agents run
end to end in the product: seiki (cloud) configures/publishes, torii (local app) submits/watches/
answers, one runtime = the gateway orchestrator. All 8 product decisions made 2026-10-06 (C1–C8,
torii#37–#44; see the epic). Follows epic #76 (done; gateway v0.10.0, torii pins it).

## Todo — Phase 1, this repo (then release + torii re-pin)

- [x] **AG-1** #85 — planner discovery tools (SP-REG-2) per run in `Executor::pinned`
- [ ] **AG-2** #86 — `OrchestratorHooks` for HITL events (feeds torii's SSE stream)
- [ ] **AG-3** #87 — scheduler wake backoff / jitter / `max_attempts` (+ testkit)
- [ ] **AG-12** #89 — money-denominated run budget (torii derives the cap; the engine enforces)
- [ ] **AG-15** #90 — per-tool call limit, confirm-before-run, escalation
- [ ] release; torii re-pins all 13 sensei-* deps (`one_gateway_ref`)

Phase 2 (torii): #34 boot seams · #33 run results · #35 config show/pull · #46 config init +
defaults · #47 gate authz · #49 budgets from caps · #36 slugs + authz.sql.
Phase 3 (torii): #51 API + retire X2 design · #50 seiki publishing · #48 per-tenant workers · #52 UI.

## Next

AG-2 (#86): `OrchestratorHooks` for HITL events — add no-op-default hooks for signal / gate /
agent-answer / loop-gate awaited+decided(+settled), fired at the journal write they mirror and never
on replay; red-first in `crates/orchestrator` (in-memory). AG-1 done (`7433d0d`, unreleased).

## Open questions

None — all product decisions are recorded on the epic.

## Known-broken / carry-forwards

- Lower-priority engine follow-ons (SP-7c, HITL/coordinator/sandbox/perf, per-entity engine
  versions, routing nits, flaky sandbox-straggler test, `seq: 0` fixture): #88.
- torii `authz.sql` declassify case fails on unmodified develop — torii#36.
