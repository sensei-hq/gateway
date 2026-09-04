# Checkpoint

**SP-7a serving-window bound: implemented on `develop`, awaiting whole-slice review.**
Spec `docs/superpowers/specs/2026-09-04-sp-7a-serving-window-bound-design.md` (9 ACs).

## Done

The clamp set `max_tokens` before selection and bounded by `min_context_window(chain) − est` — the
chain MINIMUM — so `[big 128k, small 8k]` with a 20k prompt gave `8192 − 20000` → 0, under
`MIN_OUTPUT_TOKENS`, refused inside the orchestrator before `Gateway::execute`. SP-7a's gate never
ran on a budgeted run.

New `Gateway::min_serving_context_window(chain, est)` — the smallest window at or above `est`, i.e.
exactly the set `ContextWindowGate` admits — is now the clamp's window term. `None` (nothing fits)
contributes NO term: the call proceeds and the GATE refuses it with per-candidate diagnostics.
`BelowFloor`'s window doc and operator message rewritten.

Spec §5/AC8 CORRECTED during the build: "no cap raise clears this" is still TRUE (the term is
cap-blind); what became false was the chain-minimum arithmetic, the "smallest model" remedy, and
the implicature that a `Some` means the prompt does not fit. The message keeps the cap sentence.

## Verified

`cargo test --workspace` **1718/0/56 exit 0** · clippy `-D warnings` 0 · fmt 0. Red-first: AC3
failed with the real `BelowFloor` pause; both mutations reddened (`None`→zero window term;
`>=`→`>` in the filter).

## Next

Whole-slice review, then SP-7b (context budgeting; truncation changes `agent_input_hash`),
SP-7c (semantic activation), the M1 reversal.

## Known-broken

One OPEN flake: `both_clamp_signals_fire_…` failed once. Its recorded diagnosis is **disproven**
(three probes), so no fix shipped; the panic dumps every captured record. **Sensei daemon is not
running — this file is the only durable record.**
