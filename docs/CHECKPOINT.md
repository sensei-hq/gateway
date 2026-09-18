# Checkpoint

**SP-ROUTE-1 — 12/12 TASKS + THE WHOLE-SLICE REVIEW DONE, on `develop`.**
Suite **1909 passed / 0 failed / 60 ignored**, real exit 0, zero `panicked at`.
Clippy (Homebrew 1.97.1 + rustup stable 1.98.1), `fmt --check`,
`cargo test -p sensei-gateway --features local --locked` (439/0), and
`cargo doc --workspace --no-deps` all clean, all real exit 0.
Spec: `docs/superpowers/specs/2026-09-17-sp-route-1-provider-routing-preferences-design.md`
Plan: `docs/superpowers/plans/2026-09-17-sp-route-1-provider-routing-preferences.md`
(Progress table + "Whole-slice adversarial review" — **read those first**).

## Done

T1–T12 (request types → `RoutingDecision` on `InferenceResponse` → docs), each
reviewed. **Whole-slice review complete in two rounds:** `0c26c8c` behavioural
(a failed attempt no longer casts a latency observation; throughput is now
output tokens ÷ TOTAL attempt wall time on both paths; `NoCandidates` gained
`skipped`; the `--features local` CI step runs `cargo test`, not `cargo check`)
and this commit, documentation (3 Critical / 6 Important / 10 Minor).

The two Criticals worth carrying: `#[non_exhaustive]` forbids
`..Default::default()` too, so the documented way to set `min_samples` did not
compile for any consumer — now guarded from OUTSIDE the crate by a
`reexport_paths.rs` test and a `resilience.rs` doctest; and the `min_samples: 0`
hazard named a mechanism the code lacks (`stats()` returns `None` first, so a
cold process is fine — the real hazard is a live ring with a zeroed counter).

## Next

**Merge `origin/main` into `develop`, then open the develop→main PR.** `main`'s
ruleset is strict — without main's merge commits the PR sits BEHIND and cannot
land.

## Carry-forwards — deliberate, not forgotten

1. **Streaming carries no `RoutingDecision`** — `execute_stream` applies every
   preference but returns `StreamEvent`s, so there is nowhere to put it.
2. **`ExecutionTrace::routing` is forward provision** — nothing builds an
   `ExecutionTrace` in production; the response is the delivered surface.
3. `InferenceCall` store write in `stream.rs` sits **after** `yield Done`, so an
   SSE consumer that breaks on `Done` is never metered. Metering, not routing.
4. Nothing validates `ModelPricing` for finiteness or sign. **Contained** (both
   strategies fence non-finite keys), but a NEGATIVE price is accepted and sorts
   FIRST under `sort: price`. The loud load-time rejection belongs at the config
   layer — now documented as a hazard rather than left implicit.
5. **Consensus legs drop the caller's `routing`** (`routing: None` hardcoded),
   while a panel inherits it. Consistent with `budget`/`auth`; documented, not
   changed.

Open questions: none. Known-broken: nothing.
