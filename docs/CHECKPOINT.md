# Checkpoint

**Slice:** DB-test isolation — the `config_versions` parallel failure, fixed at the root.
All CLOSED on `develop`. SP-6 is complete and **PR #48 is open** to `main`.

## Done

- `559a159` test(orchestrator-store) — **one** DB-test guard in
  `orchestrator_store::test_guard`, lent to all three crates that write the global
  `orchestrator.*` tables (the keys can no longer drift), plus a thread-local depth
  counter: taking a lock class twice used to hang forever on the non-reentrant
  `tokio::sync::Mutex` (measured exit=124 at a 60s cap). `DbGuard` is `!Send`, so the
  compiler enforces what makes the counter sound. Panic release is now pinned by a test.
- `dd9a3c1` fix(orchestrator) — **the reported defect.** `mod postgres_e2e` had NO guard
  at all, so its two `config_versions` tests raced each other inside one test binary
  (red 5/5 under default threads) and its unguarded advisory lock made the OTHER crates'
  worthless (red 10/10 against concurrent guarded suites). Also a per-run marker prompt
  and a bounded tick loop for the scheduler e2e.
- `c6df4e6` fix(torii) — takes the shared guard (its e2e's process-only mutex was
  justified by "cargo runs test binaries one at a time"; `nextest` defeats that), the
  fence test now also takes `config_guard`, and `serve_until_settled` survives a
  `scheduled_runs` backlog crowding a run out of the 64-row claim batch.
- `4eb8cef` fix(torii) — pre-existing, red 5/6 on untouched `473fcee`: the heavy-boot
  probe took a before/after delta of EVERY backend on the database. Now counts only
  backends carrying a unique `application_name`. Mutation-verified.

**Measured, DEFAULT parallel threads** (throwaway PG on port 56317, removed): 5
consecutive rounds, real exit codes, `sensei-orchestrator --features postgres-tests` /
`sensei-orchestrator-store --features postgres,test-support` / `sensei-torii` — **0/0/0
every round**. Cross-process contention 0/10 (was 10/10). `env -u DATABASE_URL cargo test
--workspace` **1618 passed / 0 failed, exit 0**; `clippy -D warnings` 0; `fmt --check` 0.

## Remaining

The SP-6 whole-slice review's **7 LOW** only. Nothing blocking.

## Known-broken

Nothing.

`$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it; use a throwaway
container (`docker run -p <free-port>:5432 postgres:16`, `psql < database/_apply_all.sql`)
on a port checked with `lsof`, and remove it afterwards.

## Next command

`git push origin develop`. PR #48 (SP-6 → `main`) awaits the mandatory human review;
these commits land on `develop` after it and are NOT in that PR.
