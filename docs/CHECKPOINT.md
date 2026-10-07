# Checkpoint

**Agentic completion — epic [torii#45](https://github.com/sensei-hq/torii/issues/45).** seiki (cloud)
configures/publishes, torii (local app) submits/watches/answers, one runtime = the gateway
orchestrator. All 8 product decisions made 2026-10-06 (C1–C8, torii#37–#44; see the epic).

## Done

- Phase 1 (gateway): AG-1/2/3/12/15 → **v0.11.0** (PR #91, tag `0211bed`).
- torii#53 adopt v0.11.0 → torii #54. torii CI now runs clippy + DB-free tests (#56).
- Phase 2 wave A → torii #57 (`2724a1d`): #33 run results · #34 boot seams + registry reload ·
  #35 config show/pull · #36 non-UUID slugs + authz.sql.

## Todo

- [ ] Phase 2 wave B (torii): #46 config init + defaults · #49 budgets from caps · #55 follow-ups
- [ ] Phase 2 wave C: #47 gate authz — engine half in the gateway (approver lists + an authorizer
  port torii implements with core users/roles) → v0.12.0, then torii. Design first, show the user.
- [ ] Phase 3 (torii): #51 API + retire X2 design · #50 seiki publishing · #48 per-tenant workers · #52 UI.

## Next

Wave B as one workflow in torii worktrees. Each worktree gets its OWN `CARGO_TARGET_DIR` and its own
copy of the scratch DB (`tm6-supa` :55460; `create database x` + `pg_dump postgres | psql x`).
Integrate, whole-slice review, then do the final verify yourself on a fresh target dir.

## Open questions

- #55: may an operator correct a tool-confirmation decision before the run resumes? The engine
  folds last-wins; torii refuses a second decision today.

## Known-broken / carry-forwards

- Process crash between a paid response and its `EffectRecorded` re-buys one call (documented).
- torii DB-gated tests don't run in CI (#55). Gateway engine follow-ons: gateway#88.
- Every torii DB needs `dbd reconcile` (scheduled_runs AG-3 columns; tenants_slug_not_uuid CHECK).
