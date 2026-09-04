# Checkpoint

**SP-7a serving-window bound: COMPLETE on `develop`, release gate GREEN, not pushed.**
Spec `docs/superpowers/specs/2026-09-04-sp-7a-serving-window-bound-design.md` (10 ACs).

## Done

The clamp's window term is `min_serving_context_window(chain, est) − est` — the minimum over
exactly the set `ContextWindowGate` admits — so a budgeted `[big 128k, small 8k]` run now serves a
20k prompt on `big`, where the chain minimum gave `8192 − 20000 → 0` and refused before the gate
ran. `None` (nothing can hold it) ⇒ NO window term; the GATE refuses, naming every candidate. One
`est`: the clamp calls the gateway's `estimate_input_tokens_pessimistic` on the payload it
dispatches, and both orchestrator estimators are deleted.

Release gate = verification + the doc sweep, which found the SP-DATA-5 clamp spec described **no
window term at all** (it arrived in that slice's own review, after the spec) and its §8 lacked the
"bound by the SELECTED candidate" residual FIVE other files cite as living there. Both fixed; §5.2
states both ceiling terms, §5.3 that its estimator is deleted. Overview: the `max_tokens` formula,
the `chars/3` bias-flip (gone — bytes now), line 230. Two code comments named `min_context_window`
as the live window half; the output accessor now says WHY it cannot narrow to a serving subset.

## Verified

`cargo test --workspace` **1718 passed / 0 failed / 56 ignored, exit 0** · `clippy --all-targets -D
warnings` 0 · `fmt --check` 0 · `cargo doc` unresolved links **16 = baseline**, no new breakage.

## Next

`git push origin develop`, then re-review, then SP-7b (context budgeting — truncation rewrites
`agent_input_hash`, needing a resume story 7a did not), SP-7c, the M1 reversal.

## Known-broken / open

One OPEN flake: `both_clamp_signals_fire_when_the_clamp_bit_and_the_estimate_was_low` failed ONCE.
Its diagnosis — thread-local `set_default` racing the global callsite `Interest` cache — is
**DISPROVEN by three probes**, so no fix shipped rather than a cargo-culted one; the panic now
dumps every captured record, so the next occurrence distinguishes "never emitted" from "the
capture missed it". Deferred: a sub-floor `min_max_output_tokens` renders as the BUDGET arm (§7).
**Sensei daemon is NOT running — this file is the only durable record.**
