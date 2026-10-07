# Checkpoint

**Agentic completion — epic [torii#45](https://github.com/sensei-hq/torii/issues/45).** Agents run
end to end in the product: seiki (cloud) configures/publishes, torii (local app) submits/watches/
answers, one runtime = the gateway orchestrator. All 8 product decisions made 2026-10-06 (C1–C8,
torii#37–#44; see the epic). Follows epic #76 (done; gateway v0.10.0, torii pins it).

## Todo — Phase 1, this repo (then release + torii re-pin)

- [x] **AG-1** #85 — planner discovery tools (SP-REG-2) per run in `Executor::pinned`
- [x] **AG-2** #86 — `OrchestratorHooks` for HITL events, incl. AG-15's confirm/escalation hooks
- [x] **AG-3** #87 — scheduler wake backoff / jitter / `max_attempts` (+ testkit)
- [x] **AG-12** #89 — money-denominated run budget; unrecorded paid spend = `SpendUnrecorded`
- [x] **AG-15** #90 — per-tool call limit, confirm-before-run, escalation
- [ ] release v0.11.0 (needs the user's OK for develop→main); then torii#53 AG-18 re-pin + adopt

All on develop `38fa404` (integrated slice; whole-slice review + 3 fix rounds; 1815 passed).

Phase 2 (torii): #53 adopt v0.11.0 · #34 boot seams · #33 run results · #35 config show/pull ·
#46 config init + defaults · #47 gate authz · #49 budgets from caps · #36 slugs + authz.sql.
Phase 3 (torii): #51 API + retire X2 design · #50 seiki publishing · #48 per-tenant workers · #52 UI.

## Next

Ask the user to approve the v0.11.0 release (PR develop→main, CHANGELOG, docs sync, `make clean`),
then torii#53 in a torii worktree: re-pin all 13 deps to `v0.11.0` and implement the store/CLI/SSE
follow-ups listed there.

## Open questions

- Same gate option resubmitted by a different actor fires the decided hook again (attribution
  semantics) — documented; confirm with the user if it matters for the SSE UI.

## Known-broken / carry-forwards

- Process crash between a paid response and its `EffectRecorded` still re-buys one call per crash
  (pre-existing at-least-once window; documented in durable-journal.md).
- Until torii#53 lands, torii's Pg scheduler still crash-loops poison runs (trait defaults).
- Lower-priority engine follow-ons, incl. cancel-after-claim and hook timeouts: #88.
- torii `authz.sql` declassify case fails on unmodified develop — torii#36.
