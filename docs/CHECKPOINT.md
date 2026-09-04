# Checkpoint

**SP-7a serving-window bound: REVIEWED, all 6 findings CLOSED and pushed (`d68ba8a`).** Range
`cbb8854..b91403b`, five lenses; security and test-quality clean. Nothing open from it.

## Done — the review fixes

`9bda14d` The refusal's remedy was false the same way twice. "Put a model with a larger
window in this chain" cannot clear it: the term is `min { w : w >= est }` and adding to a set
cannot RAISE its minimum — one prompt down `{4096}` and `{4096, 200 000}` gives byte-identical
refusals. It now names the guaranteed remedy; a TIE names `max_output_tokens` as co-cause; and
cap-independence, unpinned when the two-cap drive moved to the gate path, is pinned (two
mutations had passed all 427 tests). `becbf17` Docs: "the bias-flip is GONE" was a non-sequitur
(bytes ≥ chars ⇏ ≥ true tokens) contradicting `dispatch.rs`; the module README kept the
chain-minimum formula; a renamed test's dead citation hid the unpinned property.

`64325f1` **M1 REVERSED — Jerry's call.** An `AllGated { resume_after: None }` carrying a
`human_action` is now the indefinite HOTL pause, not a `NodeFailed`; `None` with no action
still fails. The serving-window slice had made a budgeted over-window run terminal, and
nothing revives one (`force_wake` matches `status='paused'`; `wake` says "not queued") — memos
and spend were reachable only by hand SQL. Not window-specific: an auth lockout produced the
same state. `dfe33d0` records it on all ten doc surfaces that stated the old rule.

## Verified

`cargo test --workspace` **1721 passed / 0 failed / 56 ignored, exit 0** · `clippy -D warnings`
0 · `fmt --check` 0 · `cargo doc` private-item unresolved links **16 = baseline**. Every new
test mutation-verified; the M1 arm reddens both when it pauses with a deadline instead of NULL.

## Next

SP-7b (context budgeting — truncation rewrites `agent_input_hash`, needing a resume story 7a
did not), then SP-7c. A develop→main PR when wanted.

## Open

`both_clamp_signals_fire_when_the_clamp_bit_and_the_estimate_was_low` failed ONCE; its
`Interest`-cache diagnosis is **DISPROVEN by three probes**, so no fix shipped; the panic now
dumps every record. Deferred: a sub-floor `min_max_output_tokens` alone renders as the BUDGET
arm. **Sensei daemon NOT running — this file is the only durable record.**
