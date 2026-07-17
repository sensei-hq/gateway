# Feature: Budget & Cost Metering

- **Status:** Reference (reflects code as of 2026-07-17)
- **Crate:** `gateway`
- **Primary sources:** `src/budget.rs`, `src/types/cost.rs`, `src/types/config.rs`, `src/types/request.rs`
- **Live wiring:** `src/selection.rs`, `src/types/error.rs`, `src/engine.rs`

---

## 1. How cost metering works today

Cost metering in the gateway is **per-token dollar metering**. Every priced model
carries a `ModelPricing` giving a USD price per 1,000 input tokens and per 1,000
output tokens. From those two rates the gateway computes two kinds of figures:

- **A pre-flight estimate** (`CostEstimate`) — computed *before* a request is sent,
  from the input-token count and the model's *maximum* output-token allowance. This
  is what the request-scoped budget cap is checked against.
- **An actual cost** (`Cost`) — computed *after* a response returns, from the real
  `TokenUsage` (`input_tokens` / `output_tokens`) reported by the provider.

Both use the same linear formula: `tokens / 1000 * price_per_1k`, summed over input
and output. There is no tiering, no per-account quota, and no running daily/monthly
ledger applied at request time — metering is purely per-request, per-token, in USD.
(A `Budget` struct with daily/monthly limits exists in `cost.rs` but is not consulted
by the request path; see §7.)

The currency string is hard-coded to `"USD"` everywhere a cost or estimate is
constructed.

---

## 2. `ModelPricing`

Defined in `src/types/config.rs`. Attached to a model via `ModelConfig.pricing:
Option<ModelPricing>`.

```rust
pub struct ModelPricing {
    pub input_per_1k: f64,          // USD per 1,000 input tokens
    pub output_per_1k: f64,         // USD per 1,000 output tokens
    pub per_request: Option<f64>,   // USD flat fee per request (optional)
}
```

- `input_per_1k` / `output_per_1k` are the per-1,000-token rates used by every cost
  calculation.
- `per_request` is an optional flat per-request fee. **Caveat:** no code in the crate
  currently reads `per_request` — neither `estimate_cost` nor `Cost::from_usage` add
  it in. It is a defined-but-unused field today (see §7).
- A model with `pricing: None` is treated as **free**: cost estimation yields no
  estimate and the budget check is skipped for it (see §5).

---

## 3. `estimate_cost`, `filter_by_budget`, `AffordableModel` / `BudgetFilterResult`

These live in `src/budget.rs` as the module's public cost-metering primitives.

### `estimate_cost`

```rust
pub fn estimate_cost(
    pricing: &ModelPricing,
    model_id: &str,
    input_tokens: u32,
    max_output_tokens: u32,
) -> CostEstimate
```

The formula (verbatim from the source):

```text
input_cost  = input_tokens      as f64 * pricing.input_per_1k  / 1000.0
output_cost = max_output_tokens  as f64 * pricing.output_per_1k / 1000.0
estimated   = input_cost + output_cost
```

The returned `CostEstimate` is:

| field       | value                        |
| ----------- | ---------------------------- |
| `estimated` | `input_cost + output_cost`   |
| `minimum`   | `input_cost` (zero output)   |
| `maximum`   | `estimated` (full output)    |
| `currency`  | `"USD"`                      |
| `model`     | `model_id`                   |

Notes:
- The estimate is a **worst-case** figure: it assumes the model emits its full
  `max_output_tokens`. `minimum` is the input-only floor (no output generated);
  `maximum` equals `estimated` because the estimate already assumes maximum output.
- `pricing.per_request` is **not** included.
- Worked example (from the module tests, Haiku at `input_per_1k = 0.0008`,
  `output_per_1k = 0.004`, `1000` input / `500` max output tokens):
  `0.0008 + 0.002 = 0.0028` USD.

### `filter_by_budget`

```rust
pub fn filter_by_budget(
    models: &[(String, CostEstimate)],
    budget: f64,
) -> BudgetFilterResult
```

Partitions each `(model, estimate)` pair by comparing `estimate.estimated` against
`budget`:

- **affordable** when `estimate.estimated <= budget` (inclusive — a model priced
  exactly at the budget is affordable).
- **over_budget** otherwise.

Every entry is wrapped in an `AffordableModel` with `within_budget` set accordingly,
then pushed into the matching bucket. An empty input yields two empty buckets.

### `AffordableModel` / `BudgetFilterResult`

```rust
pub struct AffordableModel {
    pub model: String,
    pub cost_estimate: CostEstimate,
    pub within_budget: bool,
}

pub struct BudgetFilterResult {
    pub affordable: Vec<AffordableModel>,
    pub over_budget: Vec<AffordableModel>,
}
```

`within_budget` is `true` for every entry in `affordable` and `false` for every
entry in `over_budget`.

> **Caveat — this module is currently un-wired.** `estimate_cost`, `filter_by_budget`,
> `AffordableModel`, and `BudgetFilterResult` are `pub` (the crate exposes `pub mod
> budget`) but have **no callers inside the crate** outside `budget.rs`'s own tests.
> The live request path enforces the budget with a near-identical private copy in
> `selection.rs` (see §5). Both copies share the exact same formula.

---

## 4. `CostEstimate`, `Cost`, `TokenUsage`

All three are defined in `src/types/cost.rs` and are `Serialize`/`Deserialize`.

### `TokenUsage`

```rust
pub struct TokenUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub total_tokens: u32,
}
```

The raw token counts reported by a provider. Derives `Default` (all zero). Carried on
`InferenceResponse.usage` and `StreamChunk.usage`.

### `Cost`

```rust
pub struct Cost {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub total_tokens: u32,
    pub input_cost: f64,
    pub output_cost: f64,
    pub total_cost: f64,
    pub currency: String,
}
```

The **actual** post-response cost. Two constructors:

- `Cost::zero()` — all counts and costs `0`, `currency = "USD"`.
- `Cost::from_usage(usage: &TokenUsage, input_per_1k, output_per_1k)`:

  ```text
  input_cost  = (usage.input_tokens  as f64 / 1000.0) * input_per_1k
  output_cost = (usage.output_tokens as f64 / 1000.0) * output_per_1k
  total_cost  = input_cost + output_cost
  ```

  Unlike `estimate_cost`, this uses the **actual** `output_tokens`, not a maximum.
  `currency` is `"USD"`. (`per_request` is not applied here either.)

Carried on `InferenceResponse.actual_cost: Option<Cost>`.

### `CostEstimate`

```rust
pub struct CostEstimate {
    pub estimated: f64,
    pub minimum: f64,
    pub maximum: f64,
    pub currency: String,
    pub model: String,
}
```

The pre-flight estimate produced by `estimate_cost` (§3). Carried on
`InferenceResponse.estimated_cost: Option<CostEstimate>` and on
`SelectedModel.cost_estimate`.

---

## 5. Request-scoped `budget` cap and the `BudgetExceeded` fallback trigger

### The `budget` field

`InferenceRequest` (in `src/types/request.rs`) carries the per-request cap:

```rust
pub struct InferenceRequest {
    // ...
    pub budget: Option<f64>,   // USD ceiling for this request; skipped in JSON when None
}
```

Flow of the value (`src/engine.rs`): the engine estimates input tokens from the
payload (`estimate_input_tokens`) and builds a `SelectionCriteria` carrying
`budget: request.budget` and `input_tokens`. Model selection then does the check.

### Enforcement at selection time

In `src/selection.rs`, `ModelSelectionService` estimates each candidate's cost with
its own private `estimate_cost` method (identical formula to §3, using
`model_config.max_output_tokens` and `criteria.input_tokens.unwrap_or(0)`), then:

```rust
if let (Some(budget), Some(est)) = (criteria.budget, &cost_estimate)
    && est.estimated > budget
{
    // candidate is dropped as a SkippedCandidate:
    //   reason: "over budget (estimated {:.4}, budget {:.4})"
}
```

Consequences:
- A candidate is rejected only when `est.estimated > budget` (strictly over —
  exactly-at-budget passes, matching `filter_by_budget`'s inclusive `<=`).
- A candidate with no `pricing` produces no estimate (`None`), so the `if let` guard
  fails and the model **passes the budget check** (treated as free).
- When `request.budget` is `None`, no candidate is ever skipped for cost.
- Over-budget candidates surface as `SkippedCandidate { reason: "over budget …" }` in
  the `SelectionResult.skipped` diagnostics — they are **filtered out**, not surfaced
  as an error.

### The `BudgetExceeded` fallback trigger

Two related definitions exist:

- `FallbackTrigger::BudgetExceeded` (in `src/types/config.rs`, serialized as
  `"budget_exceeded"`) — a variant a `FallbackChainConfig.fallback_triggers` list may
  include.
- `GatewayError::BudgetExceeded { estimated: f64, remaining: f64 }` (in
  `src/types/error.rs`) — an error variant. `GatewayError::should_trigger_fallback`
  maps it to a fallback **only** when the chain's triggers contain
  `FallbackTrigger::BudgetExceeded`. It is **not** `is_retryable`.

> **Surprise — the `BudgetExceeded` error is defined and plumbed but never raised.**
> Across the whole repository, `GatewayError::BudgetExceeded { .. }` is constructed
> only inside unit tests (`error.rs`). No production code path emits it: the live
> selection path handles an over-budget candidate by **skipping** it (§ above), not by
> returning `BudgetExceeded`. So the "budget exceeded ⇒ fallback" routing exists and is
> unit-tested, but is presently dormant — the actual budget cap is enforced purely by
> candidate filtering during selection.

---

## 6. End-to-end summary

1. Caller sets `InferenceRequest.budget = Some(x)` (USD).
2. Engine estimates input tokens and passes `budget` + `input_tokens` into
   `SelectionCriteria`.
3. For each candidate, selection estimates `estimated = input_cost + output_cost`
   (worst-case, full max output). Priced candidates with `estimated > budget` are
   skipped; unpriced candidates pass as free.
4. On response, the actual `Cost` is computed from real `TokenUsage` via
   `Cost::from_usage` and returned on `InferenceResponse.actual_cost`; the pre-flight
   `CostEstimate` is returned on `estimated_cost`.

---

## 7. Implementation notes & caveats

- **Duplicated `estimate_cost`.** `budget.rs::estimate_cost` (free function, §3) and
  `selection.rs::estimate_cost` (private method, §5) implement the identical formula.
  The engine uses the `selection.rs` copy; the `budget.rs` module (and its
  `filter_by_budget` / `AffordableModel` / `BudgetFilterResult`) currently has no
  in-crate callers.
- **`ModelPricing.per_request` is unused.** No cost calculation adds the flat
  per-request fee; it round-trips through serde but never affects a figure.
- **`maximum` carries no extra information.** In every `CostEstimate` produced today,
  `maximum == estimated`, because the estimate already assumes full output.
- **`Budget` struct is not request-scoped state.** `cost.rs` defines
  `Budget { daily_limit: f64, monthly_limit: f64, alert_threshold: f32 }`
  (defaults `5.0` / `50.0` / `0.8`), but it is not read by the request path, the
  selection path, or the `budget.rs` functions. It is a standalone config-shaped type,
  distinct from the request-scoped `budget: Option<f64>` cap.
- **`BudgetExceeded` is dormant.** See §5 — defined and wired for fallback routing, but
  never constructed outside tests.

---

## 8. Future work (NOT YET IMPLEMENTED)

> The following describes intended direction only. None of it is implemented in the
> code today; do not rely on it.

Subscription/quota-based auth is expected to add **tiered and quota-based metering
alongside** the per-token dollar metering described above — not replacing it. A
request would still be metered per token in USD, but additionally counted against a
plan tier and/or a per-account quota (e.g. daily/monthly allowances of the kind the
existing `Budget` struct hints at). That work is tracked separately in the gateway
roadmap under the **AUTH** track and is out of scope for the current per-request budget
cap.
