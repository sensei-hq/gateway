# Checkpoint

**SP-7b context budgeting: SPEC APPROVED and committed (`cef53ba`), awaiting Jerry's review of the
spec file before any plan or code.** SP-7a serving-window bound is DONE — all 6 review findings
closed and pushed (`864a8dd`), nothing open from it.

## Done

SP-7a review (`cbb8854..b91403b`, five lenses): the refusal remedy was false the same way twice —
"put a model with a larger window in this chain" cannot clear a `min { w : w >= est }` bound, proven
by byte-identical refusals down `{4096}` and `{4096, 200 000}`. A TIE now names `max_output_tokens`;
cap-independence is pinned. **M1 REVERSED** (`64325f1`): an `AllGated` carrying a `human_action` is
the indefinite HOTL pause, not a `NodeFailed`, because nothing revives a terminal run. Recorded on
ten doc surfaces (`dfe33d0`).

SP-7b spec: 12 ACs in `docs/superpowers/specs/2026-09-04-sp-7b-context-budgeting-design.md`, built on
a 12-agent research workflow (89 findings, **3 of 5 central claims refuted**). Jerry's decisions:
availability · a fixed floor · all four disclosure channels · scope = system + tools. **The one idea:
journal the BUDGET, not the cut** — the truncator is already pure and every other input replay-stable,
so the window-derived integer was the only unfenced one (`GatewayConfig` has NO version field, so the
`#cfg{gen}` fence has no catalog term). Mandatory, not defensive: every past turn's hash is recomputed
on every partial resume forever, and a `DeterminismViolation` leaves the run unrevivable.

## Verified

`cargo test --workspace` **1721 passed / 0 failed / 56 ignored, exit 0** · `clippy -D warnings` 0 ·
`fmt --check` 0 · `cargo doc` private-item links **16 = baseline**. Spec facts spot-checked at HEAD
myself: `eid` (agent.rs:383) is one line before `ih` (:384); `join` (:271) runs BEFORE `resolve_chain`
(:273), so §5.1 needs a reorder; no `max_context_window` accessor exists yet.

## Next

Jerry reviews the spec → writing-plans → subagent-driven execution. No SP-7b code exists yet.

## Open

`both_clamp_signals_fire_when_the_clamp_bit_and_the_estimate_was_low` failed ONCE; its
`Interest`-cache diagnosis is **DISPROVEN by three probes**, so no fix shipped. Spec §7 flags the real
cost: the guard test asserting "half a document never becomes work product" inverts under SP-7b and
gets SPLIT, not relaxed. **Sensei daemon NOT running — this file is the only record.**
