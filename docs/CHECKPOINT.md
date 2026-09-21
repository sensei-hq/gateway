# Checkpoint

**SP-ROUTE-1.2 — 4/4 TASKS DONE, on `develop`.** The streaming path now has the
metering and explanation the unary path already had. Suite **1922 passed / 0
failed / 60 ignored**, real exit 0, zero `panicked at`. Clippy (Homebrew 0.1.97 +
rustup stable 0.1.98), `fmt --all --check`, `cargo test -p sensei-gateway
--features local --locked`, `cargo doc --workspace --no-deps` — all clean, all
real exit 0, zero unresolved doc links.
Plan: `docs/superpowers/plans/2026-09-21-sp-route-1-2-streaming-parity.md`
(Progress table — **read it first**).

## Done

T1 metering above the `yield` (`2fb6b86`, AC1–AC2/AC6) · T2 `attempt_start` on
the persisted row (`ab4e6ee`, AC3) · T3 `StreamEvent::Done.routing` (`7e89dc3`,
AC4–AC5) · T4 docs + verification (this commit, AC7).

Three fixes in one function, `Gateway::execute_stream`. **The billing one is the
one to remember:** `insert_inference_call` sat *after* `yield Done`, and in an
`async_stream` generator code after a `yield` runs only on the next poll — so a
consumer that stops at the terminal event (the normal SSE shape) was **never
metered**. The persisted `duration_ms` now means total attempt span on both
paths — a **deliberate one-time discontinuity** in `inference_calls`, documented
in `upgrading.md`, not back-filled. `Done` carries the decision the selection
*produced*, not a re-derivation.

AC2/AC6 re-measured in Task 4, not inherited: under the reverted mutation exactly
**1 of 442** gateway tests fails (AC1's), while every `collect_stream` test stays
green — including one that asserts a metering row and still finds it.

## Next

**Whole-slice review over the three-commit diff** (`2fb6b86..7e89dc3` + docs) —
this plan has no review step, unlike SP-ROUTE-1's Task 12. Then merge
`origin/main` into `develop` and open the develop→main PR (`main`'s ruleset is
strict: without main's merge commits the PR sits BEHIND and cannot land).

## Carry-forwards

Canonical ledger now in the SP-ROUTE-1 plan (§"Carry-forward ledger"), added this
slice because three surfaces had each invented their own numbering. **1** and
**3** closed here; **4** by SP-ROUTE-1.1. Still open: **2** `ExecutionTrace::routing`
is forward provision (nothing builds one in production) and **5** consensus legs
drop the caller's `routing` while a panel inherits it (consistent with
`budget`/`auth`; documented, not changed).

Open questions: none. Known-broken: nothing.
