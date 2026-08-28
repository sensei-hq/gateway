# Checkpoint

**Slice:** SP-6 s3 `human-as-Agent`, the LAST slice of SP-6, on
`feat/sp-6-s3-human-as-agent` (from `develop` at `771af25`). **All 7 tasks done
(`f482d05`); the whole-slice review is the only thing left.**

## Done / remaining

- **Tasks 1–7 ✅ implemented AND each reviewed, findings fixed on top:** 1 `AgentBacking` +
  4 `validate()` rules (`fa070dd`, +7) · 2 the two journal events + `MAX_HUMAN_TEXT_BYTES`,
  `FORMAT_VERSION` stays 1 (`864cb1f`, +3) · 3 the fold, answer last-wins / question
  first-wins (`608b316`, +2) · 4 `executor/human.rs::run_human_agent`, the branch between
  `assemble_prompt` and `resolve_chain` (`877bf96`, +2 High/7 Med) · 5 `torii run agent
  answer` (`451be45`, +3) · 6 `list-paused` renders `agent: "<question>"` (`7fdb33e`, +2) ·
  7 the cross-process e2e (`f482d05`).
- **Task 7 ✅ AC13** — `n1 → review → n2`, `review` an ordinary top-level `NodeKind::Agent`;
  the substitution is registry-only. Green on arrival, so RED came from 3 scratch mutations
  (delete the answer-read → stays `Paused`; drop the `## Task` half → the row loses the input;
  default-answer-on-any-drive → never pauses). Run for real on `postgres:16`/55434 with
  `database/_apply_all.sql`: **7 passed / 0 failed, exit 0, ZERO skips**; `journal_events`
  holds `AgentAwaited`=1, `AgentAnswered`=1.
- **⬜ Remaining — the Final gate's last two rows:** AC1–AC17 red-before-fix evidence exists
  per task in the commit messages but is not re-collated; and `/review-slice` plus a RE-review
  of its fixes (s2's re-review found three HIGH defects introduced while fixing).

## Next command — `/review-slice`
## Open questions

- Covered by nothing: a human-backed `AgentDefinition` through a live `PostgresConfigSource`
  (the adjacent legs — serde of a `config_agents` row, and md → `Registry` — both exist).
  Stated in `human_registry`'s doc in `crates/torii/tests/e2e_pg.rs`, not hidden.
- The e2e suite is `DATABASE_URL`-gated and **counts as PASSED while exercising nothing**
  without it; a raw-stderr `SKIP` is the only signal. It cannot be `#[ignore]`d.

## Known-broken

None. **1583 passed / 0 failed / 7 ignored, exit 0** (`env -u DATABASE_URL cargo test
--workspace`); clippy `-D warnings`, `fmt --check`, `cargo doc --no-deps` clean.
`$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it.
