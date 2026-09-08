# Checkpoint

**SP-7b context budgeting: COMPLETE, whole-slice reviewed. The one known-broken thing is now
FIXED — no known-broken state remains.** Spec `2026-09-04-sp-7b-context-budgeting-design.md` (12
ACs) + its plan, whose Task 8 note records what shipped. SP-7a DONE (`864a8dd`).

## Done

T1-T4 (`fedb8ac`..`daeee45`) `max_context_window`, the pure planner + `CONTEXT_FLOOR_FRACTION`, the
measured renderer, `ContextBudgeted` folded FIRST-wins. `cdea80d`+`16a344e` T5/T6 wiring plus two
CRITICALs (an unfenced UN-budgeted turn; a replay arm re-running `plan_budget`). `5781e3e`+`03204bf`
T7 four channels including the read-back path. `be89e7d` T8 the name judgment, doc rewrites and
sweep. `f489fbc` closed the final review's three confirmed findings.

`c177a72` **the clamp-signal flake, root-caused not retried.** `tracing` caches a callsite's
`Interest` on the static at first execution, and while only ONE `Dispatch` is registered — a lone
capture test — it resolves that from the EMITTING thread's subscriber. Any subscriber-less test
reaching `dispatch.rs`'s `warn!` first cached `Interest::never()`, permanently blinding the capture
to that line. `CapturingSubscriber::install()` keeps one extra `Dispatch` registered so the fast
path is disarmed. The previously recorded diagnosis was marked DISPROVEN wrongly: its probes
poisoned BEFORE `set_default`, which self-repairs.

**The one idea: journal the BUDGET, not the cut.** The window-derived integer was the only unfenced
input (`GatewayConfig` has NO version field); a `DeterminismViolation` on resume is unrevivable.

## Verified

`cargo test --workspace` **1755 passed / 0 failed, real exit 0** · `clippy --all-targets -D
warnings` 0 · `fmt --check` 0 · 40 consecutive runs of the orchestrator binary, 0 failures. The
flake fix is mutation-proven both ways (keepalive removed → red; restored → green).

## Next

`gh pr create --base main --head develop` — 37 commits carrying SP-7a, the M1 reversal, SP-7b and
the flake fix. Then SP-7c (no spec yet) or the minors.

## Open

7 MINORs, none blocking: duplicate tool NAMES drop the wrong schema; `dropped_deps` can be
hard-wired to 0 with the suite green; `torii` has NO `ContextBudgeted` arm, so that operator surface
is unbuilt; spec §5.2/§5.3 cite retracted rules. A budgeted node is effectively SINGLE-TURN, pinned
by a two-turn test. **Sensei daemon NOT running — this file is the only record.**
