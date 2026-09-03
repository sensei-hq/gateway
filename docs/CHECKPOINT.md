# Checkpoint

**SP-DATA-5 budget clamp is COMPLETE and ON `main`** (PR #51, `41411f8`). `develop` is 2 commits
ahead with the sandbox-flake fix; no slice in flight.

## Done

A budgeted `Chat` carries `max_tokens = min(remaining − est_input, the chain's smallest
max_output_tokens, the chain's context window − est_input, the caller's own)`, set at the single
`dispatch_metered` chokepoint, so the PROVIDER enforces the cap. Below a `MIN_OUTPUT_TOKENS` (256)
allowance the run refuses through the existing durable pause and makes **no call**. Unbudgeted runs
byte-identical. **The claim it does NOT make:** the overshoot is bounded by the input-estimate error
and biased toward refusing early — **not eliminated**.

`/sensei:review` found **4 HIGH + 6 MEDIUM**, all fixed (`b22cce2` clamp bounded output but not the
CONTEXT WINDOW, so a long-prompt call that succeeds unbudgeted hard-failed once budgeted, plus the
clamp being unobserved on the agent/selector paths · `210d098` the floor's raise went stale ·
`d19f2bf` six doc surfaces).

## The sandbox flake is FIXED (`5ea6ebe`) — it was never a timing flake

`spawn_capped` drained stdout and stderr with two SEQUENTIAL `recv_timeout(CAPTURE_GRACE)` calls, so
the real bound was `2 × CAPTURE_GRACE` = 4s against a test asserting < 5s. Full-suite load ate that
margin; isolation did not, which is why it read as noise. The streams now share ONE deadline —
measured **4.02s → 2.01s**. Widening the test number would have left a 4s stall in production.
`CAPTURE_GRACE` is module-scope and **all four** wall assertions derive from it; two had the same
sub-second margin and had simply not failed yet. **6 consecutive full-suite runs, 0 failures.**

## Verified at `fa685dd` — real exit codes

`cargo test --workspace` **1686 passed / 0 failed / 56 ignored, exit 0** · `clippy --workspace
--all-targets -- -D warnings` **0** · `fmt --all --check` **0** · `cargo doc` unresolved links
**16** (baseline). No database started; `$DATABASE_URL` never read.

## Next command

Open the batched `develop` → `main` PR for the 2 commits when wanted:
`gh pr create --base main --head develop`.

## Known-broken

Nothing, and no known flakes. One gap written into the clamp's code rather than hidden: when the
WINDOW is the binding term the refusal still reads as a budget problem and names a raise that cannot
help. **The sensei daemon is not running, so this file is the only durable record.**
