# Checkpoint

**Slice: SP-DATA-5 follow-on — the budget clamp. COMPLETE, reviewed, pushed.** PR #51 is open
against `main` and awaiting the mandatory human review. Next action: merge it.

## Done

A budgeted `Chat` carries `max_tokens = min(remaining − est_input, the chain's smallest
max_output_tokens, the chain's context window − est_input, the caller's own)`, set at the single
`dispatch_metered` chokepoint, so the PROVIDER enforces the cap rather than our arithmetic. Below a
`MIN_OUTPUT_TOKENS` (256) allowance the run refuses through the existing durable pause and makes
**no call at all**. Unbudgeted runs byte-identical. **The claim this slice does NOT make:** the
overshoot is **bounded by the input-estimate error, biased toward refusing early — NOT eliminated**.

## The whole-slice review (`/sensei:review`) — 4 HIGH, 6 MEDIUM, all fixed

Five parallel adversarial lenses. Security returned NO FINDINGS; the sensei daemon was down so every
MCP check was NOT RUN and fell back to git/grep.

- `b22cce2` **A** — the ceiling bounded OUTPUT but not the CONTEXT WINDOW, so a long-prompt call that
  succeeds unbudgeted hard-FAILED once a budget was set, as `NodeFailed` (unrecoverable by `wake`).
  Plus two coverage holes: the clamp was unobserved on the AGENT and SELECTOR paths, and the
  estimate's `system`/`tools` terms were unwired at the call site — both mutations reddened nothing.
- `210d098` **B** — the floor's recommended raise went stale (a sibling spends after the pause, since
  `drive` does not stop at one), and the follow-up `Spent` message named no figure at all. Now stated
  as headroom above FINAL spend.
- `d19f2bf` **C** — six doc surfaces still describing the pre-clamp contract, incl. a config
  reference claiming to list "all" validation rules while listing 4 of 5, and the fence census
  saying 3 `input_hash` call sites where there are 5.

## Verified at `d19f2bf` — real exit codes

`cargo test --workspace` **1686 passed / 0 failed / 56 ignored, exit 0** · `clippy --workspace
--all-targets -- -D warnings` **exit 0** · `fmt --all --check` **exit 0** · `cargo doc` unresolved
links **16**, the baseline. No database started; `$DATABASE_URL` never read.

## Next command

`gh pr merge 51 --merge`, then `git merge origin/main && git push origin develop`.

## Known-broken / open

Nothing broken. **One PRE-EXISTING flake, NOT a regression:**
`a_backgrounded_straggler_is_reaped_on_clean_exit` (SP-4 sandbox) failed 1 of 6 runs on its 5s wall
bound, 0 of 10 in isolation, 0 in CI; wants a wider bound or an injected clock, not a retry wrapper.
Known gap written into the new code rather than hidden: when the WINDOW is the binding term, the
refusal still reads as a budget problem and names a raise that cannot help. **The sensei daemon is
not running, so this file is the only durable record.**
