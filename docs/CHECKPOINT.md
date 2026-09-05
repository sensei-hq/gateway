# Checkpoint

**SP-7b context budgeting: IN BUILD, subagent-driven. Tasks 1-7 landed; 1-6 REVIEWED, task 7 NOT
yet. Task 8 (split the guard test + doc sweep) next.** Spec
`2026-09-04-sp-7b-context-budgeting-design.md` (12 ACs), plan
`2026-09-04-sp-7b-context-budgeting.md` (8 tasks). SP-7a is DONE and pushed (`864a8dd`).

## Done

T1-T4 (`fedb8ac`..`daeee45`) `max_context_window`, the pure planner + `CONTEXT_FLOOR_FRACTION`, the
measured renderer, and `ContextBudgeted` folded FIRST-wins — all reviewed, detail in the plan.
`cdea80d`+`16a344e` T5/T6 `drive_agent` wiring, plus two CRITICALs: an UN-budgeted turn was
unfenced, so a window shrink between drives cut a turn already on the wire and killed the run
terminally; and the replay arm re-ran `plan_budget` instead of reading its own journaled
`dropped_tools`. `5781e3e` T7 disclosure — the additive `context_budgeted` output key, the operator
`warn!`, `AgentRun.context_cut`, and the executor-level proof that the truncation MARKER reached
the provider (`ScriptedAdapter` gained a `SystemLog`).

**The one idea: journal the BUDGET, not the cut.** The window-derived integer was the only unfenced
input (`GatewayConfig` has NO version field); a `DeterminismViolation` on resume is unrevivable.

## Verified

`cargo test --workspace` **1750 passed / 0 failed / 56 ignored, real exit 0** · `clippy -D warnings`
0 · `fmt --check` 0. T7's three tests were RED first; AC11's passed on arrival, so all three of its
absence assertions were mutation-verified instead.

## Next

`/sensei:review` T7, then T8: judge whether the name
`oversized_dependency_context_halts_over_budget_never_truncates` still reads right (its resize is
MOOT — T5 established the fixture already halts at the floor), and sweep `PromptParts::join`'s and
`render_context_section_bounded`'s docs, which still argue the model path must NEVER truncate.

## Open

Nine plan errors and SEVEN false doc claims so far, three introduced by commits FIXING something
else. T7 fixed four: AC3's "once per turn" (§5.4 says per NODE), AC9's "both byte counts" (the
pre-render arm measured nothing and reported `0`), AC10 conflating the marker with the `N of M`
tail, and a `700030` that is `700027`. **Sensei daemon NOT running — this file is the only record.**
