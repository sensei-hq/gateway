# Checkpoint

**SP-ROUTE-1.2 — 4/4 TASKS + WHOLE-SLICE REVIEW DONE, on `develop`.** Streaming
now has the metering and explanation the unary path already had. Suite **1927
passed / 0 failed / 60 ignored**, real exit 0, zero `panicked at`. Clippy
(Homebrew 0.1.97 + rustup stable 0.1.98), `fmt --check`, `--features local
--locked` (451), `cargo doc` — all clean, real exit 0, zero unresolved links.
Plan: `docs/superpowers/plans/2026-09-21-sp-route-1-2-streaming-parity.md`
(Progress table + review section — **read it first**).

## Done

T1 metering above the `yield` (`2fb6b86`) · T2 `attempt_start` on the persisted
row (`ab4e6ee`) · T3 `Done.routing` (`7e89dc3`) · T4 docs (`4aee074`) ·
**review (this commit): 2 Critical, 1 Important, 1 Minor — 5 mutations, 5 real
panics.** It found T1 had fixed one third of the defect while T4's docs claimed
all of it.

- **C1** — neither streaming FAILURE path metered at all. A stream dying after
  generating real tokens recorded nothing, even fully drained, while `execute`
  writes a `Failed` row: unary 1, streamed 0. Both sites now write one.
- **C2** — the write ahead of the `yield` made the terminal event hostage to an
  unbounded consumer-supplied store: a 100ms-per-event consumer got the full
  content and NO `Done` — "delivered but unbilled" became "not delivered and
  unbilled". Bounded at 2s. My T1 comment calling it "the same price the
  dispatch charges" was false (that dispatch is synchronous); corrected.
- **I1** pins AC3 on the fallback branch; **M1** pins `routing: None`.

T4's docs are now true — the code was finished so the prose became true.

## Next

**Merge `origin/main` into `develop`, then open the develop→main PR** (`main`'s
ruleset is strict: without main's merge commits the PR sits BEHIND).

## Carry-forwards

Ledger in the SP-ROUTE-1 plan. **1**/**3** closed here, **4** by SP-ROUTE-1.1.
Open: **2** `ExecutionTrace::routing` forward provision; **5** consensus legs
drop the caller's `routing`. Open questions: none. Known-broken: nothing.
