# Checkpoint

**SP-7b context budgeting: IN BUILD, subagent-driven. Tasks 1-4 landed and REVIEWED (both
rechecks PASS); task 5 next.** Spec `2026-09-04-sp-7b-context-budgeting-design.md` (12 ACs), plan
`2026-09-04-sp-7b-context-budgeting.md` (8 tasks). SP-7a is DONE and pushed (`864a8dd`).

## Done

`fedb8ac` T1 `Gateway::max_context_window` — the budget target, the only window accessor folding
`max` rather than `min`. `7c7d286` T2 `CONTEXT_FLOOR_FRACTION = 0.25` + the pure planner.
`6ada19d` T2 fix: `plan_budget` compared a SECTION-byte budget against a BODY-byte floor, so a plan
approved AT the floor rendered BELOW it and the schema-drop loop stopped early — **turning
degradable turns into refusals**. `ee13ddd` closed three FALSE CLAIMS, one introduced by that fix.
`0f618e1`+`2e844d6` T3 measured renderer; the fix put the bound IN PLAY — AC8 was unpinned and two
mutations of `join_bounded` left all 442 tests green. `42446e3`+`daeee45` T4 `ContextBudgeted`,
folded FIRST-wins via `entry().or_insert`, `label` arm added (`cargo build --workspace` cannot see
it), `FORMAT_VERSION` still 1.

**The design's one idea: journal the BUDGET, not the cut.** The truncator is pure and every other
input is replay-stable, so the window-derived integer was the only unfenced one (`GatewayConfig` has
NO version field). Mandatory, not defensive: every past turn's hash is recomputed on every partial
resume forever, and a `DeterminismViolation` leaves the run unrevivable.

## Verified

`cargo test --workspace` **1739 passed / 0 failed / 56 ignored, exit 0** · `--all-targets` 0 · `clippy -D warnings` 0 ·
`fmt --check` 0. Every new arithmetic term mutation-pinned, including the zero-floor saturation the
fix had left unguarded while claiming otherwise.

## Next

T5 ALONE next (the `drive_agent` wiring — the risky integration: the `resolve_chain` reorder, the
stale-fold trap, the floor's pause path), then T6-8. Passes are split so review lands inside one.

## Open

**Do NOT "fix" `min_context_window`'s doc.** A T1 agent called its "its own test" claim false; both
reviewers refuted that (the test is at `engine/mod.rs:835`; the grep was read at file granularity).
Plan amended: `plan_budget` takes entries, not a byte total; T5 passes `&parts.context`.
**Sensei daemon NOT running — this file is the only record.**
