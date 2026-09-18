# Checkpoint

**SP-ROUTE-1 — ALL 12 TASKS DONE, each reviewed, on `develop`.**
Suite **1904 passed / 0 failed / 60 ignored**, real exit 0, zero `panicked at`.
Clippy (Homebrew + rustup stable), `fmt --check`, `--features local`, and
`cargo doc` all clean.
Spec: `docs/superpowers/specs/2026-09-17-sp-route-1-provider-routing-preferences-design.md`
Plan: `docs/superpowers/plans/2026-09-17-sp-route-1-provider-routing-preferences.md`
(Progress table + "review lesson" + "three traps" — **read those first**).

## Done

T1 request types · T2 `ExcludedByPolicy` · T3 `RoutingPolicyGate` (`only`/`ignore`) ·
T4 performance store + `AttemptPhase` · T5 mid-stream failure recording ·
T6 `RandomSource` + `StrategyCtx` · T7 `GroupedWeightedStrategy` (the default) ·
T8 `PriceStrategy` · T9 `MetricStrategy` · T10 per-request resolution + engine wiring ·
T11 `RoutingDecision` on `InferenceResponse` · **T12 docs + final verification.**

All 11 ACs have a named test, each re-run individually and green (table in the
plan, Task 12 Step 4). Two of the plan's draft test names were wrong and are
corrected there.

## Next — the ONLY remaining step

**Whole-slice adversarial review** (`/sensei:review` over the full slice diff),
then the develop→main PR. Per the SP-6 lesson, review is deliberately *not* the
tail of a long session — run it fresh.

Note `main`'s ruleset is strict: merge `origin/main` into `develop` first or the
PR sits BEHIND and cannot land.

## Carry-forwards — deliberate, not forgotten

1. **Streaming carries no `RoutingDecision`.** `execute_stream` applies every
   preference but returns `StreamEvent`s, so there is nowhere to put the
   explanation. Documented as a known gap, not papered over.
2. **`ExecutionTrace::routing` is forward provision.** Nothing in the workspace
   builds an `ExecutionTrace` in production; the response is the delivered
   surface. Every doc sentence about it now carries that qualifier.
3. `InferenceCall` store write in `stream.rs` sits **after** `yield Done`, so an
   SSE consumer that breaks on `Done` is never metered. Metering, not routing.
4. Nothing validates `ModelPricing` for finiteness or sign. **Contained** (both
   strategies fence non-finite keys); the loud load-time rejection belongs at
   the config layer.

Open questions: none. Known-broken: nothing.
