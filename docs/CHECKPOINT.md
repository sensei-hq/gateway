# Checkpoint

**SP-7a is COMPLETE and ON `main`** — PR #52 merged as `cbb8854`, 23 commits. `develop` and
`main` are identical. No slice in flight.

## Done

**SP-7a — window-aware selection.** A sixth `AdmissionGate` asks per CANDIDATE what the
orchestrator used to ask once against the chain's SMALLEST window before dispatch; the
`min_context_window` pre-check and `PromptOverBudget` are deleted.

**My spec's headline claim was wrong and review caught it:** the over-everything case is NOT a
durable pause — `all_gated_error` takes `resume_after` from TIMED skips only, and risk M1 in
`docs/design/selection-policy-pipeline.md` deliberately resolved terminal-only exhaustion as
"fail-fast, never pause". The slice delivers the DIAGNOSIS (per-candidate windows), not recovery.

Also: `AllGated` rendered neither `skipped` nor `human_action` (every number the slice adds was
dropped at the orchestrator boundary); the estimator priced an assistant turn's `tool_calls` at
zero — a 100 KB tool-call argument estimated 0; six tests could not fail. Plus the **sandbox flake,
genuinely fixed** — `spawn_capped` used two SEQUENTIAL capture graces, so the real bound was
`2 × CAPTURE_GRACE` = 4s against a 5s assertion; one shared deadline, measured 4.02s → 2.01s.

## Verified at `cbb8854` — real exit codes

`cargo test --workspace` **1714/0/56 exit 0** · clippy **0** · fmt **0** · doc links **16**
(baseline) · 8 consecutive full-suite runs, 0 failures. CI PG suites: orchestrator 433/0, store 69/0.

## Next — a defect SP-7a's review found, and it is mine

**SP-7a's benefit does NOT apply on a budgeted run.** The clamp bounds `max_tokens` by
`min_context_window(chain) − est` and refuses `BelowFloor` under `MIN_OUTPUT_TOKENS`. For AC1's own
example (`[big 128k, small 8k]`, 20k prompt) that is `8192.saturating_sub(20000) = 0 < 256` — so the
run is refused in the orchestrator **before `Gateway::execute` is called** and the new gate never
runs. Fix = move the clamp's window term to the SELECTED candidate (the clamp spec's §8 item, now
reachable). Needs a design pass: the clamp runs BEFORE selection today.

Also open: SP-7b (context budgeting — truncation changes `agent_input_hash`, so it needs a
determinism argument), SP-7c (semantic activation), the M1 reversal.

## Known-broken

**One OPEN flake, honestly labelled:** `both_clamp_signals_fire_…` failed once; its recorded
diagnosis (thread-local `set_default` vs the global callsite `Interest` cache) is **disproven** by
three probes, so no fix shipped. The panic now dumps every captured record so the next occurrence
distinguishes "never emitted" from "capture missed it". **The sensei daemon is not running, so this
file is the only durable record.**
