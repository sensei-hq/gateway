# Checkpoint

**Slice: SP-DATA-5 follow-on — the budget clamp.** On `develop`, unpushed. Tasks 1–8 done;
Task 9 (docs + release gate) is what remains.

## Done

Tasks 1–4 (floor constant, clamp fixture, pessimistic estimate, the clamp) shipped at `c301901`.
A three-reviewer whole-slice review then raised 20 findings at Minor or above; all are fixed in
nine commits on top, and Tasks 5, 6 and most of 8 landed as part of that — their findings were
"this property is untested", and the remedy is the test.

Both Criticals were real. (1) The clamp had no upper bound, so at a cap of 10240 it sent
`Some(10239)` and the provider answered a 400 — a budgeted run hard-failing where the unbudgeted
one succeeds. Fixed with `Gateway::min_max_output_tokens(chain)` and `min(allowance, ceiling)`.
(2) The Postgres AC6 e2e was left at `CAP = 100`, under the floor; the local suite skips it and
CI would have gone red. Rescaled ×10 and verified with an in-process replica.

Also: the floor has its own reason text (it reported "0 of 300 tokens spent" on a run that spent
nothing); `est_input_tokens` counts an assistant turn's `tool_calls`; `cap - spent` is a
`checked_sub` behind a `debug_assert!`, because the overflow panic it relied on was debug-only.

## Verified at the gate — real exit codes

`cargo test --workspace` **1676 passed / 0 failed / 56 ignored, exit 0** ·
`cargo clippy --workspace --all-targets -- -D warnings` exit 0 · `cargo fmt --all --check` exit 0.
Every mutation quoted in this round's commit messages was run against a clean tree and reverted.

## Remaining

- **Task 9**: the overview entry, the SP-DATA-5 spec's §8/§2, the doc-link baseline (expect 16),
  the release gate. The four prose surfaces Step 4 used to own are already swept.
- **Task 8's two `tracing` signals (AC10/AC11)** — the only ACs with no code. No finding touched
  them; they need no behaviour change.
- **Task 7** (AC13, the memo fence under a moving clamp) still has no test.

## Next command

`cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`, then Task 9.

## Known-broken

Nothing. `$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it. The **sensei
daemon is not running**, so this file is the only durable record; `sensei start` restores it.
