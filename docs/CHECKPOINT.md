# Checkpoint

**Slice:** SP-6 s3 `human-as-Agent` — the LAST slice of SP-6. On
`feat/sp-6-s3-human-as-agent` (branched from `develop` at `771af25`). **All 7 tasks done
(`f482d05`); the whole-slice review is the only thing left.**

## Done / remaining

- **s3 Tasks 1–7 ✅ all implemented AND each reviewed, findings fixed on top:**
  1 `AgentBacking::{Model,Human{timeout}}` + 4 `validate()` rules (`fa070dd`, +7 findings) ·
  2 `JournalEvent::{AgentAwaited,AgentAnswered}` + `MAX_HUMAN_TEXT_BYTES`, `FORMAT_VERSION`
  stays 1 (`864cb1f`, +3) · 3 the fold — answer last-wins, question first-wins (`608b316`, +2) ·
  4 `executor/human.rs::run_human_agent`, the branch between `assemble_prompt` and
  `resolve_chain` (`877bf96`, +2 High/7 Medium) · 5 `torii run agent answer` (`451be45`, +3) ·
  6 `list-paused` renders `agent: "<question>"` (`7fdb33e`, +2) · 7 the cross-process e2e
  (`f482d05`).
- **Task 7 ✅ AC13** — `n1 → review → n2` with `review` an ordinary top-level `NodeKind::Agent`;
  the substitution is registry-only. Green on arrival, so RED came from 3 scratch-copy mutations
  (delete the answer-read → stays `Paused`; drop the `## Task` half → the awaiting row loses the
  input; default-answer-on-any-drive → never pauses at all). Ran for real against
  `postgres:16` on 55434 with `database/_apply_all.sql`: **7 passed / 0 failed, exit 0, ZERO
  skips**, and `journal_events` holds `AgentAwaited`=1, `AgentAnswered`=1.
- **⬜ Remaining: the Final gate's last two rows** — every AC1–AC17 observed red before its fix
  (per-task evidence exists in each commit message; not re-collated), and `/review-slice` plus a
  RE-review of the fixes. SP-6 s2's re-review found three HIGH defects introduced while fixing.

## Next command

```
/review-slice
```

## Open questions

- The one seam this slice does NOT cover: a human-backed `AgentDefinition` through a live
  `PostgresConfigSource`. Adjacent tests exist (`agent_backing_is_serde_defaulted_and_round_trips`,
  `filesystem_source_carries_a_human_backing_through_to_the_registry`); the Postgres leg does not.
  Stated in `human_registry`'s doc in `crates/torii/tests/e2e_pg.rs`, not hidden.
- The e2e suite is `DATABASE_URL`-gated and **counts as PASSED while exercising nothing** when the
  var is absent. A raw-stderr `SKIP` line is the only signal. It cannot be `#[ignore]`d.

## Known-broken

None. **1583 passed / 0 failed / 7 ignored, exit 0** (`env -u DATABASE_URL cargo test
--workspace`); `clippy --workspace --all-targets -D warnings`, `fmt --all --check` and
`cargo doc --workspace --no-deps` all exit 0 with no new warnings.
`$DATABASE_URL` is a REMOTE Supabase instance — never run the DB suite against it; use a
throwaway container on a free port and remove only that one.
