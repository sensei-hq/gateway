# Checkpoint

**SP-7a COMPLETE and on `main`** — PR #52, `cbb8854`, 23 commits. `develop` == `main`. Nothing
in flight.

## Done

Window-aware selection: a sixth `AdmissionGate` asks per CANDIDATE what the orchestrator asked
once against the chain's SMALLEST window; the pre-check and `PromptOverBudget` are deleted.

Review corrected my spec's headline claim — the over-everything case is NOT a durable pause
(`resume_after` comes from TIMED skips only, and risk M1 resolved terminal exhaustion as
"fail-fast, never pause"). The slice delivers the DIAGNOSIS, not recovery. It also found
`AllGated` rendered neither `skipped` nor `human_action`, an estimator pricing `tool_calls` at
zero, and six tests that could not fail. The **sandbox flake is genuinely fixed**: two SEQUENTIAL
capture graces made the real bound 4s against a 5s assertion; one shared deadline, 4.02s → 2.01s.

## Verified at `cbb8854`

`cargo test --workspace` **1714/0/56 exit 0** · clippy 0 · fmt 0 · doc links **16** · 8 consecutive
full-suite runs green. CI PG: orchestrator 433/0, store 69/0.

## Next

**SP-7a's benefit does not apply on a BUDGETED run** — my own defect. The clamp bounds by
`min_context_window(chain) − est` and refuses `BelowFloor`, so AC1's example is rejected in the
orchestrator before `Gateway::execute` runs and the new gate never fires. Fix: move the clamp's
window term to the SELECTED candidate. Needs a design pass — the clamp runs before selection.

Then: SP-7b (context budgeting; truncation changes `agent_input_hash`, needs a determinism
argument), SP-7c (semantic activation), the M1 reversal.

## Known-broken

One OPEN flake: `both_clamp_signals_fire_…` failed once. Its recorded diagnosis is **disproven**
(three probes), so no fix shipped; the panic now dumps every captured record so the next
occurrence is actionable. **Sensei daemon is not running — this file is the only durable record.**
