# Checkpoint

**Slice: SP-DATA-5 follow-on — the budget clamp. COMPLETE on `develop`, unpushed.** Tasks 1–9 done,
release gate re-run and green. Next action is the batched `develop` → `main` PR.

## Done

A budgeted `Chat` carries `max_tokens = min(remaining − est_input, the chain's smallest max_output_tokens,
the caller's own)`, set at the single `dispatch_metered` chokepoint, so the PROVIDER enforces the cap rather
than our arithmetic. Below a `MIN_OUTPUT_TOKENS` (256) allowance the run refuses through the existing durable
pause and makes **no call at all**, naming the cap that unblocks it (`spent + est_input + floor`); two
`tracing` records measure the estimator. Unbudgeted runs byte-identical. **The claim this slice does NOT
make:** the overshoot is **bounded by the input-estimate error, biased toward refusing early — NOT eliminated**.

The gate re-run found a **sixth** stale surface, now fixed: the s5 spec's §6.5a rejected a token reservation
because "§8 deliberately does not have" an output estimate, while §8 of that same file now marks that
deferral ADDRESSED — it contradicted itself. The decision stands on starvation, not on ignorance.

## Verified at `2fe0332` — real exit codes, re-run after this task's doc edits

`cargo test --workspace` **1682 passed / 0 failed / 56 ignored, exit 0** (35 suites) · `clippy
--workspace --all-targets -- -D warnings` **exit 0** · `fmt --all --check` **exit 0** · `cargo doc`
unresolved links **16**, the baseline exactly. No database started, `$DATABASE_URL` never read.

## Next command

```
git fetch origin && git merge origin/main   # main is STRICT: merge FIRST or the PR sits BEHIND
git push origin develop && gh pr create --base main --head develop
```

## Known-broken / open

Nothing broken; this slice's only source edits since the last gate are comments. **One PRE-EXISTING flake,
NOT a regression:** `a_backgrounded_straggler_is_reaped_on_clean_exit` (SP-4 sandbox) failed 1 of 6 earlier
runs on its 5s wall bound, 0 of 10 in isolation, and did not reproduce here — consistent with 1-in-6, not
evidence it is gone; wants a wider bound, not a retry wrapper. The Postgres AC6 budget e2e is `ignore`d
locally (rescaled ×10 above the floor) and runs for real in CI. **The sensei daemon is not running, so
this file is the only durable record.**
