# Checkpoint

**Slice: SP-DATA-5 follow-on — the budget clamp. COMPLETE on `develop`, unpushed.** Tasks 1–9 all
done, nothing outstanding. Next action is the batched `develop` → `main` PR.

## Done

A budgeted `Chat` request now carries `max_tokens = min(remaining − est_input, the chain's smallest
max_output_tokens, the caller's own)`, set at the single `dispatch_metered` chokepoint, so the
PROVIDER enforces the cap instead of our arithmetic. Below a `MIN_OUTPUT_TOKENS` (256) allowance the
run refuses through the existing durable pause and makes **no call at all**, naming the cap that
unblocks it (`spent + est_input + floor`). Two `tracing` records measure the estimator. Unbudgeted
runs are byte-identical. **The claim this slice does NOT make:** the overshoot is **bounded by the
input-estimate error, biased toward refusing early — NOT eliminated** (spec §4 has the arithmetic).
Task 9 also fixed a stale WHY: fan-out serialises to avoid STARVING siblings (see `ef44318`).

## Verified at the tip — real exit codes, re-run after the doc edits

`cargo test --workspace` **1682 passed / 0 failed / 56 ignored, exit 0** (35 suites), reproduced in
**5 of 6** runs — see the flake below · `clippy --workspace --all-targets -- -D warnings` **exit 0**
· `fmt --all --check` **exit 0** · `cargo doc` unresolved links **16**, the baseline exactly. No
database touched.

## Next command

```
git fetch origin && git merge origin/main   # main's ruleset is STRICT: merge FIRST or the PR sits BEHIND
git push origin develop && gh pr create --base main --head develop
```

## Known-broken / open

Nothing broken in this slice. **One PRE-EXISTING flake the gate surfaced, NOT a regression:**
`a_backgrounded_straggler_is_reaped_on_clean_exit` (SP-4 sandbox, untouched since) failed 1 of 6
full-suite runs on its 5s wall bound and 0 of 10 in isolation; this slice's diff is comment-only.
Suspect its two sequential 2s `recv_timeout` fallbacks under load; wants a wider bound, not a retry.
`$DATABASE_URL` is **REMOTE Supabase — never run a suite against it**; the Postgres AC6 budget e2e
is `ignore`d locally, rescaled ×10 above the clamp's floor and verified in-process. The **sensei
daemon is not running**, so this file is the only durable record.
