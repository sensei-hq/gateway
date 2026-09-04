# Checkpoint

**SP-7a serving-window bound: review findings FIXED on `develop`, awaiting re-review.**
Spec `docs/superpowers/specs/2026-09-04-sp-7a-serving-window-bound-design.md` (now 10 ACs).

## Done

Slice: the clamp's window term is `min_serving_context_window(chain, est) − est`, the minimum over
exactly the set `ContextWindowGate` admits, so a budgeted `[big 128k, small 8k]` run now serves a
20k prompt on `big`. `None` (nothing fits) ⇒ NO term; the GATE refuses, per candidate.

Review, 16 findings, all addressed. **The Critical (4 reviewers, one defect): the clamp and the
gate computed `est` with two different functions**, and the clamp's was the LARGER on ASCII
(`Σceil` vs `ceilΣ`), so its serving set was a strict SUBSET of the set selection drew from — an
over-window `max_tokens` on the wire, a terminal `NodeFailed` where the PARENT commit gave a
recoverable pause. Fixed structurally: `estimate_input_tokens_pessimistic` is `pub`, the clamp
calls it on the payload it dispatches, and BOTH orchestrator estimators
(`dispatch::est_input_tokens`, `agent::prompt::est_tokens_pessimistic`) are deleted.
Also: AC4's assertion was vacuous (now `est+emitted == 4096`, exact, on a heterogeneous chain);
AC8's remedy string and the tie rule were unguarded (now pinned); §7's deferral claiming the two
estimators "agree in the safe DIRECTION" was false in every clause; 09-03 spec corrected in place.

## Verified

`cargo test --workspace` **1718/0/56 exit 0** · clippy `-D warnings` 0 · fmt 0 · `cargo doc` 0 new
warnings. Red first: both new cross-crate tests failed with `3800 + 1024 = 4824` against a 4096
window. Five mutations reddened: drop `− est`; `.min()`→`.max()`; tie→`None`; old remedy restored;
parent bound + per-string over-count (proving the parent did NOT dispatch).

## Next

Re-review this fix commit, then SP-7b (context budgeting; truncation changes
`agent_input_hash`), SP-7c (semantic activation), the M1 reversal.

## Known-broken

One OPEN flake: `both_clamp_signals_fire_…` failed once; its recorded diagnosis is **disproven**
(three probes), so no fix shipped. Deferred: a sub-floor `min_max_output_tokens` still renders as
the BUDGET arm (spec §7). **Sensei daemon is not running — this file is the only durable record.**
