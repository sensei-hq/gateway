# Checkpoint

**SP-ROUTE-1.1 — 5/5 TASKS DONE, on `develop`.** Pricing that cannot be compared
is now rejected at the boundary. Suite **1918 passed / 0 failed / 60 ignored**,
real exit 0, zero `panicked at`. Clippy (Homebrew 1.97.1 + rustup stable 1.98.1),
`fmt --check`, `cargo test -p sensei-gateway --features local --locked`, and
`cargo doc --workspace --no-deps` all clean, all real exit 0.
Spec: `docs/superpowers/specs/2026-09-18-sp-route-1-1-pricing-validation-design.md`
Plan: `docs/superpowers/plans/2026-09-18-sp-route-1-1-pricing-validation.md`
(Progress table — **read it first**).

## Done

T1 `ModelPricing::validate` (`4d52044`) · T2 the `#[serde(try_from)]` boundary
(`1170aab`, AC1–AC3/AC7) · T3 `collect_validation_errors` Rule 7 (`3a091a6`,
AC6) · T4 `Facade::build` drops-and-warns (`cb5fbbf`, AC4–AC5) · T5 docs +
verification (this commit, AC8).

One rule, three call sites. **The breaking change:** a config **file** carrying
a non-finite or negative price now fails to load, with an error naming the field
and the value. Two non-rejections are deliberate and pinned: an explicit `0.0`
(a real price, ties with `None`) and a large finite `1e300`. `Facade::build`
**drops** such a model rather than nulling its price — `None` means free and
free sorts first, so the tempting repair hands a broken price the cheapest slot.
AC5 is the test that fails when the feature is implemented the *wrong* way.

This **closes SP-ROUTE-1 carry-forward 4** ("nothing validates `ModelPricing`"),
which was contained at the routing layer and is now fixed at the config layer.
The `PriceStrategy` / `MetricStrategy` non-finite fences stay as the last line.

## Next

**Merge `origin/main` into `develop`, then open the develop→main PR.** `main`'s
ruleset is strict — without main's merge commits the PR sits BEHIND and cannot
land.

## Carry-forwards — deliberate, not forgotten

1. **Streaming carries no `RoutingDecision`** (`execute_stream` returns
   `StreamEvent`s, nowhere to put it).
2. **`ExecutionTrace::routing` is forward provision** — nothing builds an
   `ExecutionTrace` in production.
3. `InferenceCall` store write in `stream.rs` sits **after** `yield Done`, so an
   SSE consumer that breaks on `Done` is never metered. Metering, not routing.
4. **Consensus legs drop the caller's `routing`** while a panel inherits it.
   Consistent with `budget`/`auth`; documented, not changed.
5. **No magnitude cap on pricing, by design** (SP-ROUTE-1.1 §2/§6): `1e300` can
   still overflow inside `estimate_cost`, and that non-finite *result* stays
   fenced by the strategies. Any cap would be an invented threshold.

Open questions: none. Known-broken: nothing.
