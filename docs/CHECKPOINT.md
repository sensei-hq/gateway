# Checkpoint

**SP-7b context budgeting: IN BUILD, subagent-driven. Tasks 1-7 landed and REVIEWED; T7's three
review blockers FIXED (`03204bf`). Task 8 (split the guard test + doc sweep) next.** Spec
`2026-09-04-sp-7b-context-budgeting-design.md` (12 ACs), plan
`2026-09-04-sp-7b-context-budgeting.md` (8 tasks). SP-7a is DONE and pushed (`864a8dd`).

## Done

T1-T4 (`fedb8ac`..`daeee45`) `max_context_window`, the pure planner + `CONTEXT_FLOOR_FRACTION`, the
measured renderer, `ContextBudgeted` folded FIRST-wins. `cdea80d`+`16a344e` T5/T6 `drive_agent`
wiring plus two CRITICALs (an unfenced UN-budgeted turn; the replay arm re-running `plan_budget`).
`5781e3e` T7 disclosure — the additive `context_budgeted` key, the operator `warn!`,
`AgentRun.context_cut`, executor-level proof the truncation MARKER reached the provider.
`03204bf` T7 review fixes — channel 3 was shipped on the WRITING drive only: `start()` on a
terminal journal rebuilds outputs from `node_last_output` (raw model turn, no key), so the same run
reported a degraded node as un-degraded when read back. `project_agent_outputs` now re-attaches it
from the durable `ContextBudgeted` row (`effect_id(node, 0, 0)`) — no new event.

**The one idea: journal the BUDGET, not the cut.** The window-derived integer was the only unfenced
input (`GatewayConfig` has NO version field); a `DeterminismViolation` on resume is unrevivable.

## Verified

`cargo test --workspace` **1752 passed / 0 failed / 56 ignored, real exit 0** · `clippy -D warnings`
0 · `fmt --check` 0. The read-back test was RED first on the stated reason; its premise guard was
mutation-verified too (disabling the terminal short-circuit grows the journal by one
`RunCompleted`, AFTER the zero-call assertion passes — the call count was never the guard). Both
green-on-arrival guards were mutation-verified: `Some(c)` -> `None` on the replay arm, and a
run-global latch for the per-node `contains_key`.

## Next

T8: judge whether `oversized_dependency_context_halts_over_budget_never_truncates` still reads
right (its resize is MOOT — T5 established the fixture already halts at the floor), and sweep
`PromptParts::join`'s and `render_context_section_bounded`'s docs, which still argue the model path
must NEVER truncate.

## Open

Nine plan errors, SEVEN false doc claims. `03204bf` corrected two more from its own slice
(`finish_agent`'s "exactly ONE place the canonical output is built" — there are two, and the second
drifted; and the unasserted claim that the key rides every drive) and caught one in its OWN draft
before commit. **Sensei daemon NOT running — this file is the only record.**
