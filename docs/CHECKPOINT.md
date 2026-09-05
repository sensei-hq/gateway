# Checkpoint

**SP-7b context budgeting: ALL EIGHT TASKS LANDED (`be89e7d`). Build phase COMPLETE; the
whole-slice review is what remains.** Spec `2026-09-04-sp-7b-context-budgeting-design.md` (12 ACs) +
its plan, whose Task 8 note records what shipped. SP-7a DONE (`864a8dd`).

## Done

T1-T4 (`fedb8ac`..`daeee45`) `max_context_window`, the pure planner + `CONTEXT_FLOOR_FRACTION`, the
measured renderer, `ContextBudgeted` folded FIRST-wins. `cdea80d`+`16a344e` T5/T6 `drive_agent`
wiring plus two CRITICALs (an unfenced UN-budgeted turn; the replay arm re-running `plan_budget`).
`5781e3e`+`03204bf` T7 disclosure on all four channels, including the read-back path
(`project_agent_outputs` re-attaches the key from the durable row — no new event).
`be89e7d` T8: T8's own steps 1-2 were ALREADY discharged by `16a344e`, so neither was re-done —
what shipped is the NAME judgment on the floor guard, a premise assertion on the SP-7a pause test
that the refusal is the GATE's (mutation: route `AuthoredOverBudget` to `pause_context_floor`),
the `join`/`render_context_section_bounded` rewrites (the word was SILENTLY; four channels), and
the doc sweep + a `ContextBudgeted` section `durable-journal.md` never had.

**The one idea: journal the BUDGET, not the cut.** The window-derived integer was the only unfenced
input (`GatewayConfig` has NO version field); a `DeterminismViolation` on resume is unrevivable.

## Verified

`cargo test --workspace` **1752 passed / 0 failed / 56 ignored, real exit 0** · `clippy --workspace
--all-targets -D warnings` 0 · `fmt --check` 0 · CLEAN `cargo doc --document-private-items` at the
**16** unresolved-link baseline, none new. 1752 is also HEAD's pre-change count (stash-verified) —
the 1747 in T8's brief was stale.

## Next / Open

Run the whole-slice review (`/sensei:review`) over SP-7b, then merge to `develop`'s upstream. Carry
into it: nine plan errors and seven false doc claims so far, three introduced by commits FIXING
something else — every quantified claim in this slice needs deriving, not repeating. Deliberately
untouched: the dated SP-7a spec/plan docs that call SP-7b a follow-on (true when written); the
`min_context_window` doc (correct — `min_context_window_is_the_smallest_model_in_the_chain`).
Known pre-existing flake, NOT this slice:
`both_clamp_signals_fire_when_the_clamp_bit_and_the_estimate_was_low` (did not fire this pass).
**Sensei daemon NOT running — this file is the only record.**
