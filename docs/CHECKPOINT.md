# Checkpoint

**SP-7 is ON MAIN (PR #54, merged 2026-09-08) and the SP-7b minors are CLOSED on `develop`.**
`main` = `24d1868`. Spec `2026-09-04-sp-7b-context-budgeting-design.md` (12 ACs).

## Done since the merge

Every minor the SP-7b whole-slice review left open, red-first and mutation-verified:

- `4901bef` **duplicate tool names dropped every copy of the schema.** `join_bounded` matched
  `dropped_tools` by NAME, so an agent listing a tool twice lost both copies while the note said
  "1 of 3 omitted" — a false disclosure, and silent, since dropping extra schemas only makes the
  prompt smaller. Dropped by POSITION now; both producers guarantee the tail.
- `3f88cb7` **`dropped_deps` guard.** It could be hard-wired to `0` with the whole workspace green.
  A real drop needs a HEADING wider than its share — equal keys and oversized bodies both cannot
  do it — so the fixture uses a 1200-character node id.
- `194da8c` **`torii run status` reports a budgeted turn.** The fourth channel stopped at a worker
  `warn`; an operator got a complete-looking answer that never said the answer came from a CUT
  prompt. Additive, byte-identity pinned both ways.
- `4a45f2c` **spec §5.2 corrected.** Named the human path's wrapper as the model path's renderer,
  and claimed a test gap that a 1.5 MB-vs-10-byte fixture had already closed. §5.3 was re-derived
  and HOLDS — left alone.

The fifth minor needs no action: a budgeted node being effectively SINGLE-TURN is already explicit
in spec §2's consequence note and deliberately pinned by
`a_budgeted_agent_that_calls_a_tool_busts_the_window_on_the_next_turn`.

## Verified

`cargo test --workspace` **1760 passed / 0 failed, real exit 0** across 35 suites · `clippy
--all-targets -D warnings` 0 · `fmt --check` 0. Every fix reddens its own test alone under
mutation; the `dropped_deps` and duplicate-name mutants were each run both ways.

## Next

`develop` is 4 commits ahead of `main` with no PR. Either open one, or start SP-7c (no spec yet —
begins at `/sensei:design`), or take up spec §9's deferred list (transcript compaction;
summarization; blackboard design D5 is FALSE in code — nothing populates or reads
`ContextWrite.summary`).

## Open

The review counted SEVEN minors; five were written down and are now closed. The other two were
never recorded durably and are lost — treat the list as closed. **Sensei daemon NOT running.**
