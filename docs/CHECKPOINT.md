# Checkpoint

**Slice: SP-DATA-5 follow-on — the budget clamp.** On `develop`, unpushed. Tasks 1–8 done;
Task 9 (docs + release gate) is all that remains.

## Done

Tasks 1–4 shipped at `c301901`; a three-reviewer whole-slice review raised 20 findings at Minor or
above, all fixed in nine commits, and Tasks 5, 6 and most of 8 landed with them. Two Criticals were
real: the clamp had no upper bound (a cap of 10240 sent `Some(10239)` and the provider answered a
400 — a budgeted run hard-failing where the unbudgeted one succeeds; fixed with
`min(allowance, Gateway::min_max_output_tokens(chain))`), and the Postgres AC6 e2e was left under
the floor at `CAP = 100`, invisible to the local suite and red in CI.

Since then — **Task 7** (`cd992df`): AC13, a clamped call replays from its memo when the budget
moved between drives. The first drive is made to stop half-way so the second must replay `n1` AND
dispatch `n2`; the plan's own fixture sketch could not have shown the clamp differing at all.
**Task 8** (`f4f97b0`): the two `tracing` signals, plus the two tests the plan never asked for. The
clamp-bit condition is `output_tokens >= the max_tokens SENT`, not `== allowance` as the spec said
— keying on the allowance is silent on every chain whose model limit sits below it.

## Verified at the gate — real exit codes

`cargo test --workspace` **1679 passed / 0 failed / 56 ignored, exit 0** ·
`cargo clippy --workspace --all-targets -- -D warnings` exit 0 · `cargo fmt --all --check` exit 0.
All seven mutations are tabulated with their failures in the plan's "mutation ledger"; each was run
against a clean tree and reverted.

## Remaining, and the next command

**Task 9** only: the overview entry, the older SP-DATA-5 spec's §8/§2, the doc-link baseline
(expect 16), the release gate. The four prose surfaces Step 4 used to own are already swept.
Run `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`, then Task 9.

## Known-broken

Nothing. `$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it. The **sensei
daemon is not running**, so this file is the only durable record; `sensei start` restores it.
