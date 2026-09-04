# Checkpoint

**SP-7a serving-window bound: shipped, then REVIEWED. 5 of 6 findings fixed on `develop`
(`9bda14d` code, `becbf17` docs). ONE open decision for Jerry — the M1 reversal.** Review
range `cbb8854..b91403b`, five lenses; security and test-quality clean.

## Done — the review fixes

The refusal's remedy was false the same way twice. "Put a model with a larger window in
this chain" cannot clear it: the term is `min { w : w >= est }` and adding to a set cannot
RAISE its minimum — one prompt down `{4096}` and `{4096, 200 000}` gives byte-identical
refusals. It now names the guaranteed remedy (remove/replace that entry), and a TIE names
`max_output_tokens` as co-cause. Cap-independence went unpinned when the two-cap drive
moved to the gate path — two mutations passed all 427 tests. Docs: the overview's
"bias-flip is GONE" was a non-sequitur (bytes ≥ chars ⇏ ≥ true tokens) contradicting
`dispatch.rs`; the module README kept the chain-minimum formula; SP-7a's spec+plan cited a
renamed test — and that dead citation was what hid the unpinned property.

## Verified

`cargo test --workspace` **1720 passed / 0 failed / 56 ignored, exit 0** (1718 + 2) ·
`clippy --all-targets -D warnings` 0 · `fmt --check` 0 · `cargo doc` private-item
unresolved links **16 = baseline**, none in the changed items.

## Next — Jerry's call, then push

**The M1 question.** A budgeted over-window run is now TERMINAL and unrecoverable:
`min_serving_context_window` → `None` → gate skips all → `AllGated{resume_after: None}` →
`classify_gateway_error` = `Fail` → `record_terminal(Failed)`. `force_wake` is
`where status='paused'`, `torii run wake` says "not queued" — recovery needs hand-written
SQL. `cbb8854` kept a force-wakeable `RunPaused{resume_after: None}` here deliberately and
I traded it away. The fix (pause when `AllGated` carries a `human_action`) REVERSES risk M1
in `docs/design/selection-policy-pipeline.md` and changes every all-gated terminal case.

## Open

`both_clamp_signals_fire_when_the_clamp_bit_and_the_estimate_was_low` failed ONCE; its
`Interest`-cache diagnosis is **DISPROVEN by three probes**, so no fix shipped; the panic
now dumps every record. Deferred: a sub-floor `min_max_output_tokens` alone still renders
as the BUDGET arm. **Sensei daemon NOT running — this file is the only record.**
