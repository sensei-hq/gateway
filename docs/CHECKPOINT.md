# Checkpoint

**Slice:** budget completeness — the two SP-DATA-5 carry-forwards that let a run spend
past its cap. Both CLOSED on `develop`. SP-6 is complete and **PR #48 is open** to `main`.

## Done

- `65dffb8` fix(orchestrator) — **the planner selector was the fifth, unmetered model
  call.** It held its own `Arc<Gateway>` and called `execute()` directly, so one call per
  `PlannerRef::Select` node spent past the cap with no ledger entry — and because
  `PlannerSelected` memoizes the CHOICE, a resumed run never re-invokes it, so the tokens
  were missing from the fold on every later resume too. Closed by INVERTING the
  capability: a new core trait `ModelDispatch` is lent to `select()`; the selector holds
  no gateway, so the bypass is unrepresentable. Text-in/text-out because
  `orchestrator-core` depends on no provider crate. Spend journals under a reserved
  `"{expand}/__select__"` path via the existing `EffectRecorded` (FORMAT_VERSION stays 1);
  that id is now reserved against plan node ids beside `__plan__`/`__gate__`.
- `e820d87` fix(orchestrator-core) — **a snapshot that forgets the budget un-caps the
  run.** `Snapshot` gains `spent` + `budget` (`#[serde(default)]`; `run_snapshots` is one
  `jsonb` column, so no schema change). Dormant, not live — but the trap was that a
  tail-only fold that compiles would have passed the whole suite.

**Mutation-verified, all four new guards:** producer 5/5 observed RED first (1 dispatch
against an exhausted cap, 0 allowed); un-reserve `__select__` → red; drop the
`EffectRecorded` append → red; hardcode `spent: 0, budget: None` → red (0 vs 50).

**Measured:** `cargo test --workspace` **1605 passed / 0 failed, exit 0**; `clippy
-D warnings` 0; `fmt --check` 0. Postgres on a throwaway container (port 55447, removed):
store suite **66 passed**, `e2e_pg` **7 passed, zero skips**.

## Remaining

The SP-6 whole-slice review's **7 LOW** only. Nothing blocking.

## Next command

Push `develop`. PR #48 (SP-6 → `main`) is CI-green and awaits the mandatory human review;
these two commits land on `develop` after it and are NOT in that PR.

## Known-broken

Nothing. `$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it; use a
throwaway container (`docker run -p 55447:5432 postgres:16`, then
`psql < database/_apply_all.sql`), and remove it afterwards.
