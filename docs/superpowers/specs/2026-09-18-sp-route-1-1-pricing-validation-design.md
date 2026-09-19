---
title: SP-ROUTE-1.1 — reject unusable model pricing at the boundary
doctype: design-spec
module: kernel + gateway
slice: SP-ROUTE-1.1
status: draft
date: 2026-09-18
closes: SP-ROUTE-1 carry-forward 2
---

# SP-ROUTE-1.1 — reject unusable model pricing at the boundary

## 1. Why this exists

SP-ROUTE-1's whole-slice review found that **nothing validates `ModelPricing` for finiteness or
sign**, on any path. `ModelPricing` is three bare `f64`s with a derived `Deserialize`:

```rust
pub struct ModelPricing {
    pub input_per_1k: f64,
    pub output_per_1k: f64,
    pub per_request: Option<f64>,
}
```

Two reachable consequences, both measured during that review:

- **A `NaN` estimate makes the routing comparator intransitive**, and Rust's `sort_by` *panics* on
  that — measured at n=25/30/60, inside model selection. `serde_json` rejects a literal `NaN` and
  `1e400`, but a finite-but-absurd `1e300` overflows during `estimate_cost`
  (`input_cost + output_cost`, where `inf + -inf` is `NaN`).
- **A negative price is accepted outright and sorts FIRST** under `sort: price` — the cheapest
  slot won by a number that is not a price.

SP-ROUTE-1 **contained** both at the routing layer: `PriceStrategy` maps a non-finite key to
`+inf` (sorts last) and `MetricStrategy` maps one to `None` (unmeasured). That stopped the panic.
It did not stop a **negative** price winning the cheapest slot, and containment at the point of use
is the wrong layer for a configuration error — an operator can act on a loud rejection at load
time and cannot act on a silently-deprioritised model.

## 2. What is invalid, and what deliberately is not

**Invalid:** `NaN`, `+inf`, `-inf`, and any negative value — on `input_per_1k`, `output_per_1k`,
and `per_request` when `Some`.

**Valid, and this matters:** **zero is a legitimate price.** `Some(0.0)` is an *explicit* zero,
distinct from `pricing: None`, and SP-ROUTE-1 Task 8 pinned that the two tie
(`price_sort_puts_an_unpriced_candidate_first_in_both_input_orders` and
`unpriced_ties_with_an_explicit_zero_price`). Rejecting zero would break a shipped, tested
behaviour.

**Deliberately NOT rejected: a large finite magnitude.** Rejecting `1e300` requires inventing a
threshold, and any threshold is arbitrary. The overflow it can cause is already contained by the
routing layer's non-finite fences, and a loud rejection of a number that is merely *implausible*
would refuse configurations that are legal and might be intentional (a deliberately prohibitive
price used to park a model at the back of a chain). Stated here so it reads as a decision rather
than an oversight.

## 3. Where the check goes — and why not the obvious place

The obvious place is `collect_validation_errors`, the existing rule set behind `GatewayBuilder` /
`try_new` / `try_update_config`. **On its own that would never fire in production.** The only
production construction site in the workspace is `facade.rs`, which calls the deliberately
unchecked `Gateway::new`; `Facade::build` returns `Facade`, not `Result`, and its doc states
*"construction never fails on a single bad router"*. The checked/unchecked split is a design stance,
not an oversight, and production sits on the unchecked side.

So the rule lands in **three** places, expressed **once**:

```rust
impl ModelPricing {
    /// `Err(reason)` when this pricing cannot be used to compare candidates.
    pub fn validate(&self) -> Result<(), String>;
}
```

| site | behaviour | why |
|---|---|---|
| **`Deserialize` for `ModelPricing`** | hard error, naming the field and value | Catches the realistic vector — a config file — on **every** path, checked or not, with no signature change. Loud at the earliest possible point. |
| **`Facade::build`** | **drop the model**, log at `warn` | The programmatic path. Matches the facade's existing, documented treatment of a bad cloud router: logged and skipped, construction still succeeds. |
| **`collect_validation_errors`** | adds an error string | Keeps the checked paths consistent, and costs one call now that the rule exists. |

One rule, three call sites, no drift — the shape SP-ROUTE-1 arrived at the hard way when a second
derivation of the same value survived its suite.

### 3.1 The facade must DROP, not neutralise

The tempting repair is to set `pricing: None` and carry on. **That is the hazard, not the fix.**
`None` means *free*, and free sorts **first** — so a broken price would win the cheapest slot,
which is exactly the reasoning SP-ROUTE-1 used when it chose to map a non-finite `PriceStrategy`
key to `+inf` rather than to zero.

Dropping the model is the honest outcome: a model whose price cannot be compared is not safely
routable. A chain referencing it then reports `SkipReason::ModelNotFound`, which is `Structural`
and surfaces in the selection diagnostics — traceable, and it cannot silently win anything.

## 4. Blast radius

- A config **file** carrying a negative or non-finite price now fails to load, where it previously
  loaded and mis-routed. That is the intended change and the only breaking one.
- `Facade::build`'s signature is **unchanged**; it gains a `warn` and may build with fewer models.
- No routing behaviour changes. The SP-ROUTE-1 fences stay exactly as they are — they remain the
  last line for anything that reaches the strategies by a path this slice does not cover.

## 5. Acceptance criteria

| # | Criterion |
|---|---|
| AC1 | A config with a negative `input_per_1k` / `output_per_1k` / `per_request` fails to deserialize, and the error names the field and the value |
| AC2 | The same for `NaN` and `±inf` where a format can express them (and the `1e400`-style out-of-range literal `serde_json` already rejects stays rejected) |
| AC3 | `Some(0.0)` still deserializes, and still ties with `pricing: None` under `sort: price` — the existing tests stay green |
| AC4 | A programmatically-constructed bad `ModelPricing` reaching `Facade::build` drops that model and logs at `warn`; the facade still builds |
| AC5 | The dropped model is **not** silently free — a chain referencing it reports `ModelNotFound`, and it never appears ahead of a priced candidate |
| AC6 | `collect_validation_errors` reports the same problem for `try_new` / `try_update_config` / `GatewayBuilder` |
| AC7 | A large finite price (`1e300`) is **accepted** — the non-rejection is deliberate and pinned so it cannot drift into a silent threshold |
| AC8 | Suite green; clippy and fmt clean under both toolchains |

## 6. Out of scope

- A magnitude cap (§2).
- Validating anything else on `ModelConfig` (`context_window`, `max_output_tokens`). Same layer,
  different rule set, no evidence of a defect.
- Making `Gateway::new` / `update_config` checked. That is a deliberate, documented split and
  changing it is a breaking API decision of its own.
