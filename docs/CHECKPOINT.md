# Checkpoint

**SP-ROUTE-1 — Tasks 1–10 of 12 done, each reviewed, pushed** (`d6576b8` on `develop`).
Suite **1888 passed / 0 failed / 60 ignored**, real exit 0.
Spec: `docs/superpowers/specs/2026-09-17-sp-route-1-provider-routing-preferences-design.md`
Plan: `docs/superpowers/plans/2026-09-17-sp-route-1-provider-routing-preferences.md`
(carries a Progress table, a "review lesson" and a "three traps" section — **read those first**).

## Done

T1 request types · T2 `ExcludedByPolicy` · T3 `RoutingPolicyGate` (`only`/`ignore`) ·
T4 performance store + `AttemptPhase` · T5 stream completion/failure recording ·
T6 `RandomSource` + `StrategyCtx` · T7 `GroupedWeightedStrategy` (the default) ·
T8 `PriceStrategy` · T9 `MetricStrategy` · **T10 per-request resolution + engine wiring.**

**The feature is live as of T10.** Both planted tripwires were confirmed RED before it and green
after — `request.routing` had never reached selection, and production ran on the fixed-seed
`DEFAULT_RNG`.

**Next:** Task 11 (observability — `RoutingDecision` on the trace, AC10), then Task 12
(docs + final verification + whole-slice review).

## Three production defects found by review, all fixed

1. **T9:** `MetricStrategy` read the live `PerformanceStore` from *inside* `sort_by`'s comparator.
   The key could change between comparisons → intransitive → **`sort_by` panics inside model
   selection.** 89 panics in 719 selections at 40 candidates, zero at 12 (the check only runs above
   insertion-sort size). No fixture could see it — all three returned a constant. Fixed by
   snapshotting; also 144 → 12 `stats()` calls per selection, 7.5× throughput.
2. **T5:** the circuit breaker was structurally **unable** to trip on mid-stream failure — the
   acquisition dispatch's `success: true` reset `failure_count` right before the failure
   re-incremented it. Fixed by `AttemptPhase::is_verdict()`, now honoured by *every* recorder.
3. **T4:** `mean_latency_ms` pooled full-request wall time with stream-acquisition time.

## Rules earned, now in the plan

- Never call a port from inside a comparator — read once into a snapshot, then sort.
- A fixture returning a constant cannot test a live source.
- Never read a mean without its count (`samples` / `throughput_samples` / `verdict_samples`).
- A test asserting two things are EQUAL is green when neither works.
- A test proving two strategies *agree* cannot catch a swap between them.

## Carry-forwards — deliberate, not forgotten

1. `InferenceCall` store write in `stream.rs` sits **after** `yield Done`, so a real SSE consumer
   that breaks on `Done` is never metered. Metering, not routing — own slice.
2. Nothing validates `ModelPricing` for finiteness or sign. Now **contained** (both strategies
   fence non-finite keys) but the loud load-time rejection belongs at the config layer.
3. New kernel types not re-exported from `kernel/src/lib.rs`; `tests/reexport_paths.rs` not
   extended. Decide in T11/T12.

## Off-slice, landed

`5208952` — revived `facade.rs`'s test module (uncompilable since 2026-07-23) and added
`cargo check -p sensei-gateway --features local --all-targets` to `ci.yml`. **No CI job had ever
enabled a non-default feature.**

Open questions: none. Known-broken: nothing.
