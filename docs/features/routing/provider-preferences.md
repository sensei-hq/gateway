---
title: Provider Routing Preferences
doctype: feature
module: routing
status: implemented
spec: SP-ROUTE-1
source: crates/kernel/src/types/request.rs, crates/gateway/src/strategy.rs, crates/gateway/src/gates/routing_policy.rs, crates/gateway/src/selection.rs
---

# Feature: Per-Request Provider Routing Preferences

> **Status: Implemented (SP-ROUTE-1).** Design in
> [`../../superpowers/specs/2026-09-17-sp-route-1-provider-routing-preferences-design.md`](../../superpowers/specs/2026-09-17-sp-route-1-provider-routing-preferences-design.md).

Before this slice, candidate order came from exactly one place —
`ChainEntry.priority`, authored by an operator — and a caller had no say in it.
A caller could pin a model, pin a chain, or switch fallback off, but could not
say "cheapest first for this call", "not this provider, it is having an
incident", or "try these two in this order".

`InferenceRequest.routing` adds that, modelled on OpenRouter's provider-routing
surface and adapted where the domain differs (see
[§7 Why grouping](#7-why-grouping-rather-than-a-flat-weighting)).

- **Crate:** `gateway` (types in `kernel`)
- **Primary source:**
  - `crates/kernel/src/types/request.rs` — `RoutingPreferences`, `SortKey`, `CandidateSet`, `CandidateRef`
  - `crates/gateway/src/gates/routing_policy.rs` — `RoutingPolicyGate` (`only` / `ignore`)
  - `crates/gateway/src/strategy.rs` — `GroupedWeightedStrategy`, `PriceStrategy`, `MetricStrategy`
  - `crates/gateway/src/selection.rs` — `strategy_for`, the `order` re-rank, the `RoutingDecision`
  - `crates/kernel/src/types/trace.rs` — `RoutingDecision`, `RoutedCandidate`

---

## 1. The request surface

```rust
pub struct RoutingPreferences {
    pub sort:   Option<SortKey>,                 // price | latency | throughput
    pub only:   Option<CandidateSet>,            // allowlist
    pub ignore: Option<CandidateSet>,            // denylist
    pub order:  Option<Vec<CandidateRef>>,       // explicit try-order
}

pub enum SortKey { Price, Latency, Throughput }

/// An EMPTY list is "don't care" on that axis.
pub struct CandidateSet { pub routers: Vec<String>, pub models: Vec<String> }

/// An ABSENT field is a wildcard.
pub struct CandidateRef { pub router: Option<String>, pub model: Option<String> }
```

Every knob is optional and `#[serde(default, skip_serializing_if = …)]`, so a
request that carries no `routing` field serializes byte-identically to before
this slice. It also **routes** byte-identically — on any chain whose admitted
candidates have distinct priorities, which is every chain in this repo. A chain
that ties two entries at one priority is the exception, and [§8](#8-determinism)
is the precise statement; do not read this paragraph as the unconditional claim.

**Routers and models are separate axes.** The endpoint key is
`format!("{router}:{model}")` and **cannot be parsed back** — model ids contain
colons (`"ollama:gemma3:27b"`). There is therefore no flat `provider:model`
selector namespace; name the two parts separately.

```jsonc
{
  "capability": "text_chat",
  "chain": "chat_chain",
  "routing": {
    "sort": "price",
    "ignore": { "routers": ["ollama"] },
    "order":  [{ "model": "claude-haiku" }]
  }
}
```

---

## 2. Order of operations

Filtering happens first, then ordering:

```text
only / ignore  →  sort (or the weighted default)  →  order
```

1. **`only` / `ignore`** run as the `RoutingPolicyGate`, registered **first** in
   the admission-gate vector, so an excluded candidate never reaches a strategy.
2. **A strategy orders the survivors.** An explicit `sort` picks one; absent
   `sort` the default `GroupedWeightedStrategy` runs.
3. **`order` re-ranks** whatever the strategy produced.

**`only` and `ignore` have no precedence relative to *each other*.** Both are
pure predicates over a single candidate, so admission is the commutative
conjunction `only_ok && !ignore_match`. What *is* true — and what the tests pin —
is that satisfying `only` does not exempt a candidate from `ignore`.

`order` and `sort` genuinely **are** sequence-dependent: `order` wins for the
candidates it names, and `sort` (or the default weighting) orders the unnamed
tail that follows them. Combining the two is legal and composable, not an error.

### What runs *before* all of this: chain resolution

**`only`/`ignore` filter WITHIN the chain that was already resolved — they
cannot steer which chain a capability request picks.** Tier 3 (`capability`,
no `chain`) chooses one chain by **lowest chain id** among those serving the
capability, and that choice happens *before* any gate runs. An `only:
{ routers: ["anthropic"] }` on a capability request does not search the other
chains for an anthropic candidate; it filters the lowest-id chain and, if
nothing there survives, the request is a terminal `NoCandidates`.

To combine "this chain" with "these providers", pin the chain by name (tier 2)
and let the preferences filter inside it.

**Tier 1 (a direct `router` + `model` request) IS filtered.** The full gate
vector runs in `admit` on every resolution path, so an `ignore` that names that
router or model excludes the one candidate and the request fails with
`NoCandidates`. What a direct request does *not* get is an ordering — there is
one candidate, no strategy runs, and `response.routing` is `None` (§10). Absent
`routing` is "nothing was ordered", not "preferences did not apply".

---

## 3. `only` / `ignore` — filtering

**`only` is AND across non-empty axes.** A candidate is admitted iff it
satisfies every non-empty list.

**`ignore` is OR across non-empty axes.** A candidate is excluded iff it is
named on *any* axis.

**An empty list is "don't care", not "match nothing".**
`only: { routers: ["anthropic"] }` admits every anthropic candidate whatever its
model, because the `models` axis is empty and constrains nothing.

The asymmetry is deliberate — it is how an operator says these aloud. "Only
these routers and only these models" is a conjunction; "ignore this router and
that model" is a disjunction. An AND-ed `ignore` would exclude only the single
named *pair*, which nobody means.

| Preference | Candidate | Admitted? |
|---|---|---|
| `only: {routers: [anthropic], models: [claude-haiku]}` | `anthropic:claude-haiku` | yes |
| same | `anthropic:claude-opus` | no — the model axis binds |
| same | `bedrock:claude-haiku` | no — the router axis binds |
| `only: {routers: [anthropic]}` | `anthropic:anything` | yes — empty axis is don't-care |
| `ignore: {routers: [ollama], models: [claude-opus]}` | `ollama:gemma3:27b` | no — router match excludes |
| same | `anthropic:claude-opus` | no — model match excludes |
| same | `anthropic:claude-haiku` | yes — neither axis matched |

### What an exclusion looks like

An excluded candidate is recorded in `SelectionResult.skipped` with
`SkipReason::ExcludedByPolicy` ("excluded by request routing preferences"),
carrying no further detail — the caller already knows what it asked for.

Two consequences follow from the gate's **first** position and its `Structural`
classification:

- A candidate that is both excluded *and* circuit-open reports
  `ExcludedByPolicy`, not `CircuitOpen`. The caller's own instruction is the
  most specific explanation available.
- **Excluding every candidate is terminal, not a pause.** `Structural` skips
  contribute nothing to `AllGated.resume_after`, so an over-narrow `only` yields
  a terminal `NoCandidates` rather than parking an orchestrator run on a
  breaker deadline. No amount of elapsed time makes an excluded candidate
  eligible; it is a caller bug, not an outage.

`order` never filters. Use `only`/`ignore` to restrict.

---

## 4. `sort` — deterministic ordering

### `sort: price`

Ascending estimated cost across the **whole chain**, free first, ties broken by
authored priority (and equal price + equal priority keeps chain order).

This **deliberately overrides `priority` entirely** — that is what an explicit
`sort` means: load balancing switches off and the router tries candidates
strictly in the named order. Unlike the default, which only ever reorders
*within* a priority group.

The cost is `CostEstimate.estimated`, so it folds in this request's input tokens
and the model's `max_output_tokens` — "cheapest *for this call*", not cheapest
per token. A model with no `pricing` is treated as free (matching the budget
gate's reading). A non-finite estimate sorts **last**, not first: an unusable
price is not a price, and treating it as free would let a broken figure win the
cheapest slot.

> **"Free" includes a NEGATIVE estimate, and `sort: price` would put one
> first.** The default's free test is `cost <= 0.0`, not `cost == 0.0`, and
> `price_key` passes a negative estimate straight through as a finite number, so
> it would sort ahead of every genuinely free candidate. Routing is fenced
> against a *panic* from a non-finite price, not against a wrong one — so the
> fix belongs one layer up, and **SP-ROUTE-1.1 put it there**: `ModelPricing` is
> now rejected at the boundary for a non-finite or negative value, so a config
> file carrying one fails to load and `Facade::build` drops a model carrying one
> assembled in code. See
> [Pricing is validated before it can be sorted](#pricing-is-validated-before-it-can-be-sorted)
> just below. The `+inf` fence above stays as the last line for anything
> reaching the strategies by a path validation does not cover.

### Pricing is validated before it can be sorted

Since **SP-ROUTE-1.1** a `ModelPricing` whose values cannot be **compared** is
rejected rather than sorted. The rule is one function,
`ModelPricing::validate()` in `crates/kernel/src/types/config.rs`, called from
three places:

| Site | Behaviour |
|---|---|
| `Deserialize for ModelPricing` | **Hard error.** A config **file** with a bad price fails to load, and the message names the field and the value. Covers every path, checked or unchecked. |
| `Facade::build` | **Drops the model**, logs at `warn`. The signature is unchanged — `build` still returns `Facade`, not `Result`, and simply builds with fewer models. |
| `collect_validation_errors` | Adds `model '<id>' has unusable pricing: <reason>` for the checked paths (`try_new` / `try_update_config` / `GatewayBuilder`). |

**Invalid:** `NaN`, `+inf`, `-inf`, and any **negative** value, on
`input_per_1k`, `output_per_1k`, or `per_request` when `Some`.

**Two values are deliberately valid, and this is a decision rather than an
oversight:**

- **Zero.** `Some(0.0)` is an *explicit* price, distinct from `pricing: None`,
  and the two deliberately tie under `sort: price` — pinned by
  `price_sort_puts_an_unpriced_candidate_first_in_both_input_orders` and
  `unpriced_ties_with_an_explicit_zero_price` in `strategy.rs`. Rejecting zero
  would break shipped, tested behaviour.
- **A large finite magnitude.** `1e300` loads. Any cap would be an invented
  threshold; the overflow it can cause is already fenced by the non-finite
  guards described above; and a deliberately prohibitive price is a legitimate
  way to park a model at the back of a chain.

**Why `Facade::build` drops the model rather than nulling its price.** The
tempting repair is `pricing = None` — carry on without the bad number. That is
the hazard, not the fix: `None` means **free**, `price_key` maps it to `0.0`,
and free sorts **first**, so "cleaning" a broken price hands it the cheapest
slot. It is the same reasoning that made a non-finite `PriceStrategy` key map to
`+inf` rather than to `0.0`. A chain entry still naming a dropped model skips as
`SkipReason::ModelNotFound`, which is `Structural` and appears in the selection
diagnostics — traceable, and unable to silently win anything.

### `sort: latency` / `sort: throughput`

These sort on the **live rolling window** of observations
(`PerformanceStore`, per-process, in-memory, cold after restart), and they
**reorder only what they know about**:

1. Collect the indices of candidates with at least `min_samples` live
   observations of *that* metric.
2. Sort that subset (latency ascending, throughput descending).
3. Write it back into those same indices.

**Unmeasured candidates never move.** A fresh process therefore returns exactly
priority order, and a partially-observed chain is a monotone interpolation
between priority order and metric order — no imputed values, no magic
constants.

When a metric sort had two or more candidates but fewer than two *measured*
ones, it could express no preference at all, and the returned
`RoutingDecision.degraded` says so. That flag exists because a `sort: latency`
that silently returns priority order looks, from the outside, exactly like a
`sort: latency` that was ignored.

---

## 5. `order` — an explicit sequence

`order` is a stable re-rank by matched-ref index, layered on top of whichever
strategy ran. A `CandidateRef` matches iff every **present** field equals the
candidate's; an absent field is a wildcard.

**First matching ref wins.** A candidate takes the rank of the **first** ref it
matches, not the last:

```jsonc
"order": [ { "model": "charlie" }, { "router": "north" } ]
```

reads *"charlie, then the rest of north"*. `charlie` matches both refs and takes
rank 0, keeping its lead over the other north candidates at rank 1. Candidates
matching no ref rank last and follow as **fallbacks** — `order` never drops
anything.

**A router-only ref lifts every model on that router across priority tiers.**
`order: [{ "router": "anthropic" }]` puts *all* anthropic candidates ahead of
everything else regardless of their authored `priority`. That is correct and
caller-explicit, but it is precisely the cross-group reordering the default
strategy refuses to do on its own — so reach for it deliberately.

### Inert forms

Both of these are **no-ops**, and neither is an error:

| Form | Why it does nothing |
|---|---|
| `order: []` | No ref to match, so every candidate ranks last and the strategy's order survives intact. |
| `order: [ {} ]` | An all-wildcard ref matches **every** candidate, so all of them tie at rank 0 and the strategy's relative order is preserved. |

The second has a trap: **a default (all-wildcard) ref placed first shadows every
later ref**, because first-match-wins gives every candidate rank 0 before any
subsequent ref is consulted. `order: [{}, {"model": "charlie"}]` does not
promote charlie.

---

## 6. The default — `GroupedWeightedStrategy`

With no `sort`, candidates are:

1. **Partitioned by `priority`**, groups ordered ascending.
2. **Within each group**, drawn as a full permutation (not just a head pick, so
   the fallback order is weighted too) with weight
   `w = (1 / cost²) × reliability`.

Within a group:

- **Free candidates lead**, in chain order. A candidate is free iff it has no
  cost estimate, its estimate is `0.0`, or the estimate is small enough that
  `1/cost²` overflows. There is no finite multiple of "more likely" that
  expresses "costs nothing" — the limit of `1/c²` as `c → 0` *is* "always
  first".
- **Priced candidates** follow, drawn without replacement by weight. The ratio
  is the point: a \$1/M model still leads a \$3/M model about nine times in ten.
- **Zero-weight candidates go last**, in chain order, and are **never dropped**.
  A `reliability` of `0.0` (every observed attempt failed) drives `w` to zero,
  and a zero-weight member can never be drawn — but the breaker, not the router,
  is what removes a candidate from consideration.

`reliability` is the windowed success rate, read **only** when the endpoint
carries at least `min_samples` **verdicts**; below that it is treated as `1.0`
(unmeasured is healthy). The threshold is `>= min_samples` rather than `> 0`
deliberately: at `> 0`, one unlucky attempt drives `success_rate` to `0.0`,
which is weight zero, which is last in the group until the whole window rolls.

`verdict_samples` is a counter of its own because `success_rate` reads `0.0`
both when every verdict failed and when none has been cast. An endpoint that has
obtained a stream but not completed one carries `samples: 1, verdict_samples: 0,
success_rate: 0.0` — indistinguishable from "everything failed" on the rate
alone, and weighing it zero would bury a perfectly healthy endpoint for having
nothing to report yet.

---

## 7. Why grouping rather than a flat weighting

OpenRouter weights across **providers** of one model, which are
interchangeable — you are buying the same thing cheaper. A gateway chain holds
different **models**, which are not: they differ in quality, context window and
capability. Weighting across a whole chain by price would make a cheap model
nine times more likely to be tried first than the one an operator ranked first
*for quality*, silently inverting authored intent.

Equal `priority` is the only signal an operator has for "these are
interchangeable", so that is the only scope the weighting is applied at. With
distinct priorities every group is a singleton and the default *is*
`PriorityStrategy`.

---

## 8. Determinism

**With distinct chain priorities — which is every chain in this repo today —
routing is deterministic and identical to prior releases.** Every group is a
singleton, so the weighted default produces exactly the priority order
`PriorityStrategy` produced.

**If you author a chain that gives two entries the SAME priority**, those
entries become a load-balanced pool: two *fresh* runs may pick different models.
Orchestrator **resume is unaffected** — a completed model call replays from its
journal memo and never re-enters selection — but two independent runs of the
same graph may now diverge. That is the feature; ties are the opt-in.

### Two ways a tie can arise unintentionally

1. **A chain longer than 255 entries.** `catalog::assemble` reassigns ascending
   1-based priority by final position with
   `u8::try_from(pos + 1).unwrap_or(u8::MAX)`, which **saturates**. Measured
   both ways: a 255-entry chain maps to `1..=255` — 255 distinct priorities and
   **no** tie — and the first tie appears at 256. A 300-entry chain therefore
   has 255 distinct priorities with 46 entries all at 255 — a genuine tie group.
   Reachable through a `derive` tier predicate over a large catalog.
2. **Hand-authored chains.** `GatewayBuilder::add_chain` and `GatewayConfig`'s
   `Deserialize` both pass `ChainEntry.priority` through verbatim, and config
   validation has **no priority rule at all**. Nothing prevents an operator from
   authoring a tie, deliberately or otherwise.

So the precise claim is: **byte-identical for every chain whose admitted
candidates have distinct priorities**, which covers every chain in this repo and
every `assemble()` output of length ≤ 255. Anything else is opting into load
balancing — which is the intended way in, it is just not the only way.

---

## 9. The operator knob — `ResilienceConfig::min_samples`

```rust
use gateway::resilience::ResilienceConfig;

let mut resilience = ResilienceConfig::default();
resilience.min_samples = 5;

let gateway = Gateway::new(config, adapters, cb).with_resilience(resilience);
```

**`ResilienceConfig` is `#[non_exhaustive]`, and that forbids every struct
expression outside the gateway crate — `..Default::default()` included.**
`ResilienceConfig { min_samples: 5, ..Default::default() }` is
`error[E0639]: cannot create non-exhaustive struct using struct expression` for
any consumer. Build the value with `default()` and assign the fields you want,
as above. (This page printed the broken form until the whole-slice review
compiled it from a real downstream crate;
`crates/gateway/tests/reexport_paths.rs` now pins the working one.)

`min_samples` (default **3**) is the minimum count of live observations before a
candidate counts as *measured*. Below it the candidate is treated as unmeasured:
it holds its index under a metric sort, and weighs `1.0` reliability under the
default.

Three is the smallest count at which a mean is not simply the last observation,
and the cost of getting it wrong is bounded in both directions — too low and the
sort reacts to noise, too high and it degrades to priority order, which is the
documented fallback anyway.

Each metric has its **own** counter on `EndpointStats` — `samples` (latency),
`throughput_samples`, `verdict_samples` (reliability) — and they are
independent. `min_samples` is compared against the counter belonging to the
metric being sorted on.

Two neighbouring knobs decide what stays in the window rather than how much of
it is enough to act on: `perf_samples` (retention capacity, default 64) and
`perf_window` (retention age, default 300s). Changing either requires
`with_resilience` to **rebuild** the `PerformanceStore`, discarding samples
recorded before the rebuild; `min_samples` is read per request and needs no
rebuild.

### Hazard: do not set it to `0`

The advice is right; an earlier version of this section gave the wrong reason
for it, so here is the mechanism the code actually has.

**A never-observed endpoint is unmeasured at every threshold, zero included.**
`PerformanceStore::stats` returns `None` before any `>=` comparison can run —
`m.get(endpoint)?` when the endpoint has no ring at all, then
`if live.is_empty()` when every sample has aged out — and `None` takes the
unmeasured path regardless. So a cold process at `min_samples: 0` routes
normally. It does **not** weigh every candidate at zero, and it does not route
as though every provider were dead.

What zero reaches is narrower and still worth avoiding: an endpoint whose ring
holds live samples while **the counter being read** is `0` beside a mean of
`0.0`. The three counters are independent, so that is an ordinary state:

| At `min_samples: 0` | Stats | Effect |
|---|---|---|
| One `StreamAcquired`, no completion yet | `samples: 1`, `verdict_samples: 0`, `success_rate: 0.0` | the default reads `reliability: Some(0.0)` → `weight: Some(0.0)` → **last in its group**, for an endpoint that has simply not finished a request |
| One **failed** `Complete` (casts a verdict, contributes no latency) | `samples: 0`, `mean_latency_ms: 0.0` | `sort: latency` treats `0.0` as measured → the endpoint **leads a race it has never run** |

Any value `>= 1` makes both unreachable; the default of 3 keeps a margin past
that.

---

## 10. Observability — `InferenceResponse::routing`

A weighted router is otherwise unfalsifiable in production: two identical
requests may legitimately route differently, so there is nothing to re-run and
compare against. Every ordered selection therefore records a `RoutingDecision`
on the response:

```rust
pub struct RoutingDecision {
    pub strategy: String,               // "grouped_weighted" | "price" | "latency" | "throughput" | "priority"
    pub degraded: bool,                 // a metric sort found too few samples to reorder anything
    pub order: Vec<RoutedCandidate>,    // AFTER the `order` re-rank
}

pub struct RoutedCandidate {
    pub endpoint: String,               // "{router}:{model}"
    pub priority: u8,
    pub cost: Option<f64>,              // None ⇒ unpriced (free)
    pub reliability: Option<f64>,       // None ⇒ UNMEASURED, which is not Some(0.0)
    pub weight: Option<f64>,            // None ⇒ never entered a draw; Some(0.0) ⇒ a real zero
}
```

- `strategy` is read from the strategy object that **actually ran**, never
  re-derived from the request.
- `order` is the ranking selection handed the walk — "would try", not "did try".
  The walk may stop early (`allow_fallback: false`, or a terminal error);
  `response.attempts` is the record of what was attempted.
- `reliability: None` is **not** `Some(0.0)`. Flattening them is how a healthy
  fleet gets reported as though every provider were dead.
- **`reliability: None` means two different things, and the reader has to know
  which.** It is "this endpoint carries fewer than `min_samples` verdicts"
  *and* "the strategy that ran does not consult reliability at all". Only
  `GroupedWeightedStrategy` reads it, so under `sort: price`, `sort: latency`,
  `sort: throughput` or `priority` **every** candidate reports
  `reliability: null, weight: null` whatever the performance store holds. Read
  `strategy` first: a whole fleet reporting `null` under `sort: price` says
  nothing about its health. `weights` is likewise empty for every strategy but
  the default.
- `routing` is `None` when no strategy ran — a direct router+model request
  orders nothing. That is not "preferences were ignored": `only`/`ignore` still
  filtered it (§2), it just had one candidate and nothing to order.

### Two known gaps

- **The streaming path carries no routing decision.** `execute_stream` selects
  with the full preferences — filtering and ordering both apply — but it returns
  a `Stream` of `StreamEvent`s rather than an `InferenceResponse`, so there is
  nowhere to put the explanation. A streamed request routes correctly and cannot
  currently report *why*.
- **`ExecutionTrace::routing` is forward provision only.** The field exists and
  round-trips, but **nothing in this workspace builds an `ExecutionTrace` in
  production** — the type is constructed in one test helper, and
  `GatewayStore::insert_execution_trace` has no production caller. Every field
  of that struct is equally unfilled. `InferenceResponse::routing` is the
  delivered surface; treat the trace field as reserved for the day something
  produces one.

---

## 11. What this slice deliberately does NOT do

- **`require_parameters`** (OpenRouter's per-parameter filter) is excluded: the
  capability gate matches a coarse `Capability` enum, and there is no per-field
  capability model (tools, JSON mode, temperature, streaming) to filter against.
  Building one is a modelling job, not a routing knob.
- **Preferences do not reach orchestrator nodes.** `ModelCall` / `Agent` nodes
  carry no preferences, and `input_hash` is untouched — the moment a node
  carried them they would have to join the hash, or changing them and resuming
  would silently replay a memo produced under the old policy.
- **Consensus legs do not inherit the caller's preferences.**
  `consensus.rs` builds each debate / synthesis / judge / quorum leg from a
  fresh `InferenceRequest` with `routing: None` hardcoded, so a `sort` or an
  `ignore` on the outer request reaches none of them. A `panel` request behaves
  differently — `execute_panel` clones the caller's request, so preferences do
  carry. The asymmetry is worth stating plainly: **an `ignore` used as an
  incident switch is honoured for a panel and silently dropped for a
  consensus.** This is consistent with `budget` and `auth`, which consensus legs
  already drop for the same reason (a leg is a new request the workflow
  authored, not a copy of the caller's), and it is documented rather than
  changed here — threading them through is its own decision about how a
  workflow's own routing policy composes with a caller's.
- **Performance stats are per-process and in-memory.** Each worker learns
  independently, exactly as the circuit breaker already does. The
  `EndpointPerformanceRead` port is the seam a durable backend can be swapped
  behind later.
- **Per-chain config-side routing policy** stays with the catalog's
  `IntraTierStrategy`; this slice is scoped to the request.

---

## 12. Related behaviour change: mid-stream failures now count

The reliability weighting is only as good as its input, and its input was wrong.
On the streaming path the "success" outcome fired the instant a stream was
*obtained*, and the mid-stream error path returned without dispatching anything
— so a stream that died halfway was recorded to every health recorder as a
success.

One attempt now casts exactly **one** verdict, to every recorder. Stream
acquisition contributes a latency observation and **no** verdict; completion (or
mid-stream failure) contributes the verdict.

### Two more corrections to what an attempt observes

- **A failed attempt no longer contributes a latency observation** — it still
  casts its verdict. `MetricStrategy` reads `mean_latency_ms` with no
  reliability filter of its own (unlike the default, which multiplies by
  `reliability` precisely to avoid this), so counting a rejection as a latency
  sample made `sort: latency` route to the endpoint that **fails fastest** — a
  provider rejecting every request in 5 ms outranking one answering every
  request in 500 ms. The breaker does not save that case: `record_success`
  resets the consecutive-failure count, so an intermittently-failing endpoint
  never reaches the threshold and stays admitted.
- **Throughput is output tokens ÷ total attempt wall time, on both paths.** The
  streaming duration used to start after the stream was obtained, so a streamed
  and a non-streamed request generating at the same real rate reported different
  numbers into one mean. `AttemptOutcome::duration_ms` for `StreamCompleted`
  now means the **total attempt span**, and it feeds throughput only — it is
  never a latency observation.

**The user-visible consequence:** mid-stream failures now genuinely count toward
the circuit breaker, connection cooldown and model lockout. A single mid-stream
429 locks the endpoint for the rate-limit base duration, so on a
single-candidate chain the *next* request returns
`AllGated { resume_after: Some(t) }` rather than a stream — which the
orchestrator turns into a durable pause. Previously it retried immediately.

---

## Scenarios

```gherkin
Feature: Provider routing preferences
  Scenario: Absent preferences change nothing
    Given a chain whose entries have distinct priorities
    And a request with no routing field
    Then the candidate order is identical to strict priority order, on every seed

  Scenario: A tied group is load balanced by price
    Given two candidates at the same priority priced 1 and 3
    Then the cheaper one leads in about nine of ten seeded draws
    And a free candidate leads the group on every seed
    And a zero-reliability candidate goes last but is never dropped

  Scenario: only is AND, ignore is OR
    Given only = {routers: [anthropic], models: [claude-haiku]}
    Then anthropic:claude-opus is excluded and anthropic:claude-haiku admitted
    Given ignore = {routers: [ollama], models: [claude-opus]}
    Then a match on EITHER axis excludes

  Scenario: An excluded candidate reports the caller's own instruction
    Given a candidate that is both ignored and circuit-open
    Then it is skipped as ExcludedByPolicy
    And excluding every candidate is terminal, not a pause

  Scenario: order sequences named candidates
    Given order = [{model: charlie}, {router: north}]
    Then charlie leads, the rest of north follows, and unmatched candidates trail as fallbacks

  Scenario: A metric sort moves only what it has measured
    Given sort = latency and zero observations
    Then the order is exactly priority order and degraded is true
    Given partial observations
    Then unmeasured candidates hold their index

  Scenario: price overrides priority
    Given sort = price on a chain whose price order reverses its priority order
    Then the cheapest candidate leads the whole chain
```
