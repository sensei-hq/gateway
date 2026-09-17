# Checkpoint

**SP-ROUTE-1 — Tasks 1–3 of 12 done, reviewed, pushed** (`51e73b9` on `develop`).
Spec: `docs/superpowers/specs/2026-09-17-sp-route-1-provider-routing-preferences-design.md`
Plan: `docs/superpowers/plans/2026-09-17-sp-route-1-provider-routing-preferences.md`
(carries a Progress table and a "review lesson" section).

## Done

**T1** request types (`83a5371`, `ba5e93f`, `cd1fcc7`) — `RoutingPreferences` on `InferenceRequest`.
**T2** `SkipReason::ExcludedByPolicy`, Structural (`cf19179`).
**T3** `RoutingPolicyGate` for `only`/`ignore`, registered **first** (`da0dfb5`, `6e5cfa5`).

Suite: 334 gateway / 1803 workspace passed, 0 failed.

**Next:** Task 4 — `gates/performance.rs` (`EndpointPerformanceRead` + bounded-ring
`PerformanceStore` + `PerformanceRecorder`; extend `AttemptOutcome` with `duration_ms` +
`output_tokens`; wire the store into `Gateway` before `recorders`). **Blocks T5 and T7.**

## The review lesson — now a plan requirement for T4–12

Every Critical/Important finding so far was a test that cannot fail on the thing its name claims,
usually the negative half asserted without the positive half. T3's gate could have excluded
**every** candidate and passed 332/332. So: assert the positive half, name the mutation up front
and actually run it, and test the mirror case of any two-axis rule.

## Corrected spec claim

§4.2 said `only` → `ignore` as if observable. **False** — both are pure predicates, so admission is
the commutative conjunction `only_ok && !ignore_match`; swapping the blocks left the suite green.
Fixed. `order`/`sort` *are* genuinely sequence-dependent (T10), which is why it mattered.

## Off-slice, landed

`5208952` — revived `crates/gateway/src/facade.rs`'s test module (uncompilable since 2026-07-23)
and added `cargo check -p sensei-gateway --features local --all-targets` to `ci.yml`. **No CI job
had ever enabled a non-default feature.** The revived test passes.

## Carry-forwards

1. `CandidateSet` derives `Eq`/`Hash`, but they are order/duplicate-sensitive over `Vec<String>`
   while its doc calls the axes sets — do NOT key a `HashMap` on it without normalizing. T3 uses
   linear scans and is immune.
2. `engine::exhaustion` + `all_gated_error` + `GateContribution` widened `pub(super)`→`pub(crate)`
   for one test; proven minimal (reverting `GateContribution` gives 2 hard errors).
3. New kernel types are not re-exported from `kernel/src/lib.rs`; `tests/reexport_paths.rs` not
   extended. Decide at T10.
4. `#[ignore]`d `a_requests_routing_preferences_reach_selection` in `engine/tests.rs` — T10 Step 4
   un-ignores it. **It is the only thing that would catch the whole feature being inert.**

Open questions: none. Known-broken: nothing.
