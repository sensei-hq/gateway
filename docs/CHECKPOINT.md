# Checkpoint

**Slice:** budget completeness — the two SP-DATA-5 carry-forwards, plus the adversarial
whole-slice review of them. All CLOSED on `develop`. SP-6 is complete and **PR #48 is
open** to `main`.

## Done

- `65dffb8` fix(orchestrator) — the planner selector was the fifth, unmetered model call.
  Closed by INVERTING the capability: a new core trait `ModelDispatch` is lent to
  `select()`, so the selector holds no gateway and the bypass is unrepresentable. Spend
  journals under a reserved `"{expand}/__select__"` path (FORMAT_VERSION stays 1).
- `e820d87` fix(orchestrator-core) — a snapshot that forgets the budget un-caps the run.
  `Snapshot` gains `spent` + `budget` (`#[serde(default)]`, no schema change). Dormant.
- `0a42175` fix(orchestrator) — **review CRITICAL.** `SelectorDispatch::complete` was the
  only producer with no `fold.memo` check, so a re-driven `Select` node re-dispatched a
  billed call whose `EffectRecorded` overwrote the last at one effect id. RED first: 5
  wakes = 5 calls / ledger 77. Now 1 call / ledger 77.
- `4148fdf` fix(orchestrator) — **review MEDIUM.** The effect id pinned `local_index = 0`,
  so a multi-call selector's calls collided on one key. RED first: 154 real, ledger 77.
  Fixed with a per-`select()` call counter; concurrent dispatch now halts loud.
- `c87b421` test(orchestrator) — **review MEDIUM.** The output-side redaction census was
  still 4 while the input-side was 5. Added `selector_model_text_is_redacted`; green on
  arrival, mutation-proven in a throwaway copy.

**Refuted with evidence:** the review's "the two ledgers disagree — `Snapshot.spent` 50 vs
`spend_of` 25" is a SYMPTOM of the CRITICAL, not a `write_snapshot` defect. Post-fix probe
across four drives: really == `spend_of` == `Snapshot.spent` == 25 every time.

**Measured:** `cargo test --workspace` **1608 passed / 0 failed, exit 0**; `clippy
-D warnings` 0; `fmt --check` 0. Postgres on a throwaway container (port 55437, removed):
store suite **66 passed**, `-p sensei-orchestrator --features postgres-tests` **355
passed** (5 `postgres_e2e`), `sensei-torii` **277 passed** — all 0 failed, zero skips.

## Remaining

The SP-6 whole-slice review's **7 LOW** only. Nothing blocking.

## Next command

Push `develop`. PR #48 (SP-6 → `main`) awaits the mandatory human review; these five
commits land on `develop` after it and are NOT in that PR.

## Known-broken

Nothing. `$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it; use a
throwaway container (`docker run -p 55437:5432 postgres:16`, then
`psql < database/_apply_all.sql`), and remove it afterwards.
