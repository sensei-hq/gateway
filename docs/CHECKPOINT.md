# Checkpoint

**SP-6 is COMPLETE and ON `main`** — PR #48 merged 2026-08-28 as `5ecf01e` (75 commits,
49 files, +15227/−3694). All open items are closed. `develop` is at `a530916`, 4 commits
ahead of `main`, CI green **against a real Postgres**.

## Done

- `e987865` fix — the 9 code LOWs the SP-6 s3 + budget reviews left open, red-first.
  None had been closed by earlier commits; nothing was skipped.
- `9e68537` docs — 5 stale claims. Item 12's false `load_since` claim lives in the
  SP-DATA-5 **spec**, not `scheduler.rs`; `orchestrator-overview.md:165` was already true.
- `171ccf5` test — a skipped Postgres test is now **`ignored`, not counted as passed**.
- `62f6cbd` docs — the 25th private-item doc link was ours; demoted to a code span rather
  than widening a private item's visibility.
- `a530916` ci — a digest-pinned `postgres:16` service container. **CI-verified:** the
  Postgres suites really ran (orchestrator 368/0, store 69/0, zero still-ignored).

**Two earlier records were WRONG and are corrected:** the "pre-existing PG flake" is fixed
(`dd9a3c1` + 3), and its recorded diagnosis was false on both counts — the advisory lock
already existed in two crates; `postgres_e2e` had **no guard at all**, and the race is
**intra-process**. And "pre-existing" is no longer accepted as a reason to defer.

**The CI blind spot was 48 tests, not 7** — every DB test early-returned, and libtest
counts that as a PASS. Workspace went 1623 passed/7 ignored → **1575/55**, arithmetic
closing exactly.

**Measured at `a530916`:** `cargo test --workspace` **1575 passed / 0 failed, exit 0**;
clippy `-D warnings` 0; fmt 0; `cargo doc` private-item links back to the **24** baseline;
live PG **9/9 exit 0** across 3 consecutive DEFAULT-parallel-threads rounds.

## Remaining

Nothing open.

## Next command

When wanted: open a `develop`→`main` PR for the 4 commits. `main`'s ruleset is strict —
`develop` must contain `main`'s merge commits; it does. Deferred, non-blocking: SP-7
prompt budgeting; `GateSpec::Agent`; the SP-DATA-5 carry-forwards.

## Known-broken

Nothing. `$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it; use a
throwaway container on an `lsof`-checked free port, and remove it afterwards.
