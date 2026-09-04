# Checkpoint

**SP-7a is COMPLETE and ON `main`** — PR #52 merged as `cbb8854`, 23 commits. `develop` and
`main` are identical (0/0). No slice in flight.

## Done

**SP-7a — window-aware selection.** A sixth `AdmissionGate` asks per CANDIDATE what the
orchestrator used to ask once against the chain's SMALLEST window before dispatch. The
`min_context_window` pre-check and `PromptOverBudget` are deleted.

**My spec's headline claim was wrong and review caught it:** the over-everything case is NOT a
durable pause. `all_gated_error` takes `resume_after` from TIMED skips only, so all-terminal is
`AllGated { resume_after: None }` → `Fail`. And that is deliberate — risk M1 in
`docs/design/selection-policy-pipeline.md` resolved terminal-only exhaustion as "fail-fast, never
pause". What the slice delivers is the DIAGNOSIS (per-candidate windows), not the recovery.

Also fixed: `AllGated` rendered neither `skipped` nor `human_action`, so every number the slice
adds was dropped at the orchestrator boundary; the estimator priced an assistant turn's
`tool_calls` at zero (the ReAct loop's own shape — a 100 KB tool-call argument estimated 0); six
tests could not fail. Plus the **sandbox flake, genuinely fixed**: `spawn_capped` drained stdout
and stderr with two SEQUENTIAL graces, so the real bound was `2 × CAPTURE_GRACE` = 4s against a
5s assertion. One shared deadline — measured 4.02s → 2.01s.

## Verified at `cbb8854` — real exit codes

`cargo test --workspace` **1714 passed / 0 failed / 56 ignored, exit 0** · clippy **0** · fmt
**0** · `cargo doc` unresolved links **16** (baseline) · 8 consecutive full-suite runs, 0 failures.
CI ran the PG suites: orchestrator **433/0**, store **69/0**.

## Next — a defect SP-7a's own review found, and it is mine

**SP-7a's benefit does NOT apply on a budgeted run.** The SP-DATA-5 clamp bounds `max_tokens` by
`min_context_window(chain) − est` and refuses `BelowFloor` when that is under `MIN_OUTPUT_TOKENS`.
For AC1's own example (`[big 128k, small 8k]`, 20k prompt) that is `8192.saturating_sub(20000) = 0
< 256`, so the run is refused **in the orchestrator before `Gateway::execute` is called** and the
new gate never runs. The clamp's chain-minimum bound and the window gate disagree, and the clamp
wins. Fix = move the clamp's window term to the SELECTED candidate (the clamp spec's own §8 item,
reachable for the first time now that selection is window-aware). Needs a design pass: the clamp
runs BEFORE selection today.

Also open: SP-7b (context budgeting, needs the determinism argument — truncation changes
`agent_input_hash`), SP-7c (semantic activation), the M1 reversal.

## Known-broken

**One OPEN flake, honestly labelled:** `both_clamp_signals_fire_when_the_clamp_bit_and_the_estimate_was_low`
failed once. Its recorded diagnosis (thread-local `set_default` vs the global callsite `Interest`
cache) is **disproven** by three probes, so no fix was shipped — it would have been a remedy for a
mechanism that does not occur. The panic now dumps every captured record so the next occurrence
distinguishes "never emitted" from "capture missed it". **The sensei daemon is not running, so this
file is the only durable record.**
