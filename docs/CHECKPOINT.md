# Checkpoint

**Slice:** budget completeness — the two SP-DATA-5 carry-forwards, their adversarial
review, and now the **re-review of the three unreviewed fix commits**. All CLOSED on
`develop`. SP-6 is complete and **PR #48 is open** to `main`.

## Done

- `65dffb8`/`e820d87`/`4cc3bd5` — the two carry-forwards (lent `ModelDispatch`;
  `Snapshot.spent`+`budget`) and their docs.
- `0a42175`/`4148fdf`/`c87b421` — the first review's CRITICAL + 2 MEDIUMs (selector memo
  check; per-`select()` `local_index`; the fifth redaction guard).
- `a2eafdf` fix(orchestrator) — **re-review HIGH.** The memo check `0a42175` added was the
  executor's ONLY determinism violation that did not halt: it escaped through
  `PlannerSelector::select` into the Expand arm's blanket `Err(e)` and became a soft
  `NodeFailed`, so the drive kept going and an independent sibling spent 77 real tokens.
  Same for `materialize`'s `ContentDigestMiss`. Fixed by stashing the executor's own error
  on the dispatch and re-raising it BEFORE `select()`'s result is read (so a swallowing
  selector cannot downgrade it). 3 guards, all mutation-verified.
- `444df47` test(orchestrator) — **re-review MEDIUM.** Nothing pinned that the goal, menu
  or system instruction reach the provider; gutting all three left 1611/0 green. New
  `prompt_recording_gateway` records `(system, user)`; 3 mutations red.
- `0c03596` test(orchestrator) — **re-review MEDIUM.** The memo KEY could be a constant
  (`json!({})` left 1612/0 green). A `DriftingSelector` pins each half separately.
- `0e2c633` docs — **re-review HIGH + 2 MEDIUM.** Four stale producer censuses said
  "four"; the overview §3 still published the closed selector hole as open (contradicting
  its own §5). Fixed + the dated design spec banner-marked SUPERSEDED.

**Measured:** `cargo test --workspace` **1614 passed / 0 failed, exit 0**; `clippy
-D warnings` 0; `fmt --check` 0. Throwaway Postgres (port 55439, removed): store **66/0**,
`-p sensei-orchestrator --features postgres-tests` **361/0**, `sensei-torii` **277/0** —
all `--test-threads=1`.

## Remaining

The SP-6 whole-slice review's **7 LOW** only. Nothing blocking.

## Known-broken

`postgres_unchanged_config_generation_permits_cross_process_resume` fails **in parallel**
(off-by-one on the singleton `config_versions` generation) and passes serially. **Not a
regression** — reproduced identically at `f6d2643`, before any of these commits. It is the
already-recorded carry-forward that the DB tests' isolation guards are process-wide only;
the real fix is a Postgres advisory lock shared by both crates' guards.

`$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it; use a throwaway
container (`docker run -p 55439:5432 postgres:16`, `psql < database/_apply_all.sql`), and
remove it afterwards.

## Next command

`git push origin develop`. PR #48 (SP-6 → `main`) awaits the mandatory human review;
these commits land on `develop` after it and are NOT in that PR.
