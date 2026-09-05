# Checkpoint

**SP-7b context budgeting: COMPLETE, whole-slice reviewed, pushed (`f489fbc`).** Eight tasks plus
the final review's three confirmed findings. Spec `2026-09-04-sp-7b-context-budgeting-design.md` (12
ACs) + its plan, whose Task 8 note records what shipped. SP-7a DONE (`864a8dd`).

## Done

T1-T4 (`fedb8ac`..`daeee45`) `max_context_window`, the pure planner + `CONTEXT_FLOOR_FRACTION`, the
measured renderer, `ContextBudgeted` folded FIRST-wins. `cdea80d`+`16a344e` T5/T6 wiring plus two
CRITICALs (an unfenced UN-budgeted turn — and restoring the config did NOT help, since the spurious
row is appended before the hash check; and a replay arm re-running `plan_budget`, which folds in the
one constant the spec says exists to be RE-TUNED). `5781e3e`+`03204bf` T7 four channels including
the read-back path. `be89e7d` T8 the name judgment, doc rewrites and sweep. `f489fbc` closes the
final review: a tool-only degradation was INVISIBLE to the model (schemas dropped silently, agent
never told a configured capability was gone); `dropped_tools` was documented as disclosure when it
is a folded REPLAY INPUT; and AC9's measured-floor arm had no test — now mutation-verified.

**The one idea: journal the BUDGET, not the cut.** The window-derived integer was the only unfenced
input (`GatewayConfig` has NO version field); a `DeterminismViolation` on resume is unrevivable.

## Verified

`cargo test --workspace` **1754 passed / 0 failed / 56 ignored, real exit 0** · `clippy
--all-targets -D warnings` 0 · `fmt --check` 0 · `cargo doc --document-private-items` at the **16**
unresolved-link baseline. Final review: 17 raw findings, **7 REFUTED** by adversarial verification
(including an overstated CRITICAL), 3 confirmed and fixed, 7 minors open.

## Next

A develop→main PR carrying SP-7a, the M1 reversal and SP-7b. Then SP-7c, or the minors.

## Open

7 MINORs, none blocking: duplicate tool NAMES drop the wrong schema; `dropped_deps` can be
hard-wired to 0 with the suite green; `torii` has NO `ContextBudgeted` arm, so that operator surface
is unbuilt; spec §5.2/§5.3 cite retracted rules. A budgeted node is effectively SINGLE-TURN, pinned
by a two-turn test. Pre-existing flake, NOT this slice:
`both_clamp_signals_fire_when_the_clamp_bit_and_the_estimate_was_low`. **Sensei daemon NOT running —
this file is the only record.**
