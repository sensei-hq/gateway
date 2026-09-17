---
title: SP-ROUTE-1 — per-request provider routing preferences
doctype: design-spec
module: gateway
slice: SP-ROUTE-1
status: draft
date: 2026-09-17
---

# SP-ROUTE-1 — per-request provider routing preferences

## 1. Why this exists

The gateway resolves a request to an ordered list of candidates and walks it. The order comes
from one place — `ChainEntry.priority`, authored by an operator — and a caller has no say in it.
A caller today can pin a model (`request.model`), pin a chain (`request.chain`), or switch
fallback off (`request.allow_fallback`), but cannot express "cheapest first for this call",
"not this provider, it's having an incident", or "try these two in this order".

This slice adds that, modelled on OpenRouter's provider-routing surface: `sort`, `only`,
`ignore`, `order`, and price-weighted uptime-aware selection as the default.

**The seam already exists and was reserved for this.** `RoutingStrategy` (`strategy.rs:5`) is
the single ordering point, and its doc comment reads *"SP-0 ships PriorityStrategy (the single
ordering seam, replacing the hardcoded sort); tier/headroom strategies arrive in SP-CAT/SP-DATA."*
`IntraTierStrategy` (`catalog/tiers.rs:13`) already names `Cost`, `Headroom` and `LeastUsed`,
with the latter two stubbing to `Priority` because they need live usage that does not exist yet.
This slice supplies that live usage and makes the seam per-request.

## 2. What is NOT a direct transplant, and why

OpenRouter routes one model across many **providers**. Its providers are interchangeable — they
serve the same model — so weighting by inverse-square price is sound: you are buying the same
thing cheaper.

A gateway chain is a list of **different models**. `chat_chain` is `[gemma3:27b, claude-haiku]`.
Those are not interchangeable; they differ in quality, context window, and capability. Weighting
across them by price would make a cheap model nine times more likely to be tried first than the
one an operator ranked first *for quality*, silently inverting authored intent.

Two further facts make the literal formula undefined here:

- **Free models.** `estimate_cost` returns `None` when `ModelConfig.pricing` is absent
  (`selection.rs:169`), and this repo is full of unpriced local models (`gemma3:27b`,
  `all-minilm`). `1 / 0²` has no value. OpenRouter never hits this; it has no free providers.
- **Unparseable endpoint keys.** The candidate key is `format!("{router}:{model}")`
  (`selection.rs:314`), and model ids contain colons (`gemma3:27b` → `"ollama:gemma3:27b"`).
  The string is a sound opaque key but **cannot be split back** into its two halves, so a flat
  `provider:model` selector namespace is not available.

§3–§5 resolve all three.

## 3. Decisions

Recorded so they can be re-opened deliberately rather than by accident.

| # | Decision | Rationale |
|---|---|---|
| D1 | **Per-request**, not per-chain config | The driver is caller control; config-side tiering already exists in SP-CAT |
| D2 | Ship **all four** knobs: `sort`, `only`/`ignore`, `order`, weighted load balancing | Parity with the described surface |
| D3 | `only`/`ignore`/`order` address **separate `routers` and `models` lists** | Unambiguous; the endpoint key cannot be parsed (§2) |
| D4 | Weighted random is **the default** | Chosen deliberately over deterministic-by-default |
| D5 | …but scoped to **equal-priority groups** | Ties are where an operator has declared interchangeability; makes D4 coherent and byte-identical today (§5.1) |
| D6 | Latency/throughput from an **in-memory rolling window** | Selection is synchronous; matches the existing health-port pattern (§6) |
| D7 | Fix the **mid-stream failure mis-attribution** in this slice | The reliability weighting is only as good as its input (§7) |
| D8 | Preferences do **not** reach orchestrator nodes | Keeps `input_hash` untouched; see §8 |

`require_parameters` is **excluded**. The `CapabilityGate` matches a coarse `Capability` enum;
there is no per-field capability model (tools, JSON mode, temperature, streaming) to filter
against. Building one is a modelling job, not a routing knob.

## 4. The request surface

`InferenceRequest` (`kernel/src/types/request.rs:375`) gains one optional, serde-defaulted field.
Absent ⇒ today's wire format, byte-identical.

```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
pub routing: Option<RoutingPreferences>,

pub struct RoutingPreferences {
    pub sort:   Option<SortKey>,
    pub only:   Option<CandidateSet>,
    pub ignore: Option<CandidateSet>,
    pub order:  Option<Vec<CandidateRef>>,
}

pub enum SortKey { Price, Latency, Throughput }

/// Separate axes (D3). An EMPTY list is "don't care" on that axis.
pub struct CandidateSet { pub routers: Vec<String>, pub models: Vec<String> }

/// One position in an explicit sequence. An ABSENT field is a wildcard.
pub struct CandidateRef { pub router: Option<String>, pub model: Option<String> }
```

`CandidateRef` is the one place a pair is unavoidable: a sequence needs elements, and the address
space is two-dimensional. `{router: "anthropic"}` means "all anthropic candidates, here".

`SelectionCriteria` (`selection.rs:20`) gains `preferences: Option<RoutingPreferences>`, filled at
the two production construction sites: `engine/execute.rs:51` and `engine/stream.rs:72`.
`ModelSelectionService` resolves a strategy per request rather than holding one in `self.strategy`.

### 4.1 Matching semantics

**`only` is AND across non-empty axes.** A candidate is admitted iff it satisfies every non-empty
list: `only.routers` non-empty ⇒ `candidate.router ∈ only.routers`; likewise for models. An empty
list constrains nothing. This follows `TierPredicate`'s existing convention — *"a model matches
iff every PRESENT field matches (logical AND); an absent field is don't-care"* (`catalog/tiers.rs`).

**`ignore` is OR across non-empty axes.** A candidate is excluded iff it is named on *any* axis.

The asymmetry is deliberate: it is how an operator says these aloud. "Only these routers, and only
these models" is a conjunction; "ignore this router, and ignore that model" is a disjunction. An
AND-ed `ignore` would exclude only the single named pair, which nobody means.

**`order` matches first-ref-wins.** A `CandidateRef` matches iff every *present* field equals the
candidate's. A candidate takes the index of the first ref it matches. Candidates matching no ref
sort after all matched ones — they become fallbacks rather than being dropped; `only` remains the
way to actually restrict.

### 4.2 Precedence

`only` → `ignore` → `order` → `sort` → default weighting.

`only` and `ignore` are filters (§5.0). `order` and `sort` are both orderings: `order` wins for
the candidates it names, and `sort` (or the default) orders the unnamed tail, which follows them.
Combining them is legal and composable rather than an error.

## 5. Selection

### 5.0 Filtering is a gate, registered FIRST

`only`/`ignore` become a `RoutingPolicyGate` at the head of the vector in
`ModelSelectionService::new` (`selection.rs:99`), emitting a new
`SkipReason::ExcludedByPolicy` whose `gate_status()` is `GateStatus::Structural`.

Two reasons for head position, both load-bearing — mirroring the reasoning already recorded for
`ContextWindowGate`'s last position:

1. **`admit` returns the FIRST skip**, so position decides which reason a multiply-gated candidate
   reports. The caller's own instruction is the most specific explanation available. Reporting
   "circuit breaker open" for a candidate the caller explicitly excluded is misleading.
2. **`Structural` must stick.** A candidate that is both excluded *and* circuit-open must not
   contribute its breaker deadline to `resume_after` — waiting will never make an excluded
   candidate eligible. `GateStatus::Structural` contributes nothing to `any_gate`
   (`exhaustion.rs:73, 90`), so "the caller excluded everything" returns `None` from
   `all_gated_error` and lands as a terminal error rather than a pause. That is correct: it is a
   caller bug, not an outage, and no elapsed time or human remedy changes it.

Because the gate vector runs on every resolution path, tier-1 direct requests get filtering for
free.

### 5.1 The default — `GroupedWeightedStrategy`

Partition admitted candidates by `priority`. Order groups ascending. Within each group emit a full
permutation — not just a head pick, so the fallback order is weighted too.

Within a group:

1. **Free candidates first**, in input order (stable). Every member of a group shares a priority
   by construction, so priority cannot break ties here. A candidate is free iff its
   `cost_estimate` is `None`, its `estimated` is `0.0`, or its `estimated` is small enough that
   `1 / cost²` is not finite.
2. **Priced candidates** follow, drawn without replacement with weight
   `w = (1 / cost²) × reliability`.
3. **Zero-weight candidates last**, in input order. A `reliability` of `0.0` (every observed
   attempt failed) drives `w` to zero, and a zero-weight member can never be drawn. It stays in
   the permutation as a last resort rather than being dropped: the breaker, not the router, is
   what removes a candidate from consideration.

`cost` is `CostEstimate.estimated`, already computed per candidate in `admit` (`selection.rs:214`).
Using the estimate rather than unit price folds in the request's actual input tokens and the
model's `max_output_tokens`, so the weighting answers "cheaper *for this call*". The ratio survives:
a $1/M model still leads a $3/M model nine times in ten.

`reliability` is the windowed success rate from §6, or `1.0` when unmeasured — this is the
uptime-awareness half (§6.3).

**Why free-first rather than an infinite weight.** There is no finite multiple of "more likely"
that expresses "costs nothing"; the limit of `1/c²` as `c → 0` *is* "always first". Folding the
non-finite case into the same rule means there is one rule, not a rule plus an overflow guard.

**Byte-identity, stated precisely.** When no two admitted candidates share a priority, every group
is a singleton and the output is identical to `PriorityStrategy`. `dedup_and_prioritize` reassigns
ascending 1-based priority by final position (`catalog/assemble.rs:145-154`), so **every
catalog-assembled chain has strictly distinct priorities by construction** and is unaffected. A
chain that *does* tie changes from "stable authoring order" to price-weighted random — that is the
feature, and no chain in the repo ties today.

### 5.2 `sort: price`

Flat ascending by estimated cost across all candidates, free first, ties by authored priority.
This deliberately overrides `priority` entirely — that is what "load balancing switches off and
the router tries providers strictly in that order" means.

### 5.3 `sort: latency` / `sort: throughput`, and cold start

A fresh process has no observations, and comparing "observed 400 ms" against "never measured" is
comparing incomparable units. Rather than impute a value, **the strategy reorders only what it
knows about**:

1. Collect the indices of candidates with at least `MIN_SAMPLES` observations in the window.
2. Sort that subset by the metric (latency ascending, throughput descending).
3. Write it back into those same indices. Unmeasured candidates never move.

`MIN_SAMPLES = 3`, a `ResilienceConfig`-style tunable rather than a bare literal. Three is the
smallest count at which a mean is not simply the last observation, and the cost of getting it
wrong is bounded in both directions: too low and the sort reacts to noise, too high and it
degrades to priority order — which is the documented fallback anyway.

Three properties, no magic constants: zero observations ⇒ pure priority order (matching the
existing `IntraTierStrategy::is_dynamic` convention of degrading to `Priority`); full observations
⇒ a full metric sort; partial ⇒ a monotone interpolation between the two.

### 5.4 The trait

```rust
pub trait RoutingStrategy: Send + Sync {
    fn order(&self, admitted: &mut Vec<SelectedModel>, ctx: &StrategyCtx<'_>);
}

pub struct StrategyCtx<'a> {
    pub perf: &'a dyn EndpointPerformanceRead,
    pub rng:  &'a dyn RandomSource,
}

/// Interior mutability + `&self`, matching `CircuitBreakerManager`'s `Mutex` style,
/// so a seeded source is trivial to inject in tests (the `FakeClock` precedent, SP-DATA-3).
pub trait RandomSource: Send + Sync { fn next_u64(&self) -> u64; }
```

Weighted draw: `u = (next_u64() / u64::MAX) × Σw`, walk the group accumulating until the running
sum exceeds `u`, take that member, remove it, repeat. Deterministic given a seed within a process.

## 6. The performance recorder

### 6.1 Why in-memory

Selection is **synchronous** — `ModelSelectionService::select` (`selection.rs:150`) and
`RoutingStrategy::order` are both sync, while `GatewayStore` is `#[async_trait]`. An inline store
query is not available without restructuring selection.

The existing pattern fits exactly: `HealthRecorder::on_outcome(&AttemptOutcome)` is a write-side
reducer fed after every attempt (`gates/mod.rs:79`), paired with synchronous in-memory read ports
(`EndpointHealthRead`, `RouterHealthRead`, `ModelLockoutRead`). Performance is the same shape.

### 6.2 The port

```rust
pub trait EndpointPerformanceRead: Send + Sync {
    fn stats(&self, endpoint: &str) -> Option<EndpointStats>;
}

pub struct EndpointStats {
    pub samples: u32,
    pub mean_latency_ms: f64,
    pub mean_tokens_per_sec: f64,
    pub success_rate: f64,
}
```

`PerformanceRecorder` implements the existing `HealthRecorder` and returns `None` from
`on_outcome` (it never gates, so it never writes a deadline). Storage is a bounded ring per
endpoint, evicted by count and age — the same bounded-memory discipline as the cooldown store's
eviction cap.

Process-local and cold after restart. Each torii worker learns independently, exactly as the
circuit breaker already does. The port is the seam a durable backend can be swapped behind later,
following `ConfigSource`/`SchedulerStore`.

### 6.3 Feeding it: latency and throughput have different wiring costs

**Latency** = time until the endpoint starts producing. Non-streaming that is the whole call
(`engine/execute.rs:332`); streaming it is the moment the stream is obtained — which is *exactly
where* `dispatch_outcome` already fires (`engine/stream.rs:230`). Same quantity, so latency needs
no new dispatch point, only an elapsed value passed into the existing call.

**Throughput** = output tokens ÷ generation time, which exists only at completion. On the
streaming path the duration (`stream.rs:289`) and usage (`stream.rs:263`) are computed at stream
end and today go only into the `InferenceCall` store record. This needs a **second, end-of-stream
dispatch**.

The streaming `duration_ms` starts *after* the stream is obtained, so it is not the same quantity
as the non-streaming one. They are never pooled into a single mean.

### 6.4 The plumbing change

`AttemptOutcome` (`gates/mod.rs:66`) gains `duration_ms: u64` and optional usage.
`dispatch_outcome` (`engine/mod.rs:700`) is the single fan-out, so this is one signature change
plus its call sites (`engine/mod.rs:656`, `engine/stream.rs:230`, `engine/stream.rs:320`, and the
new end-of-stream dispatch). Every existing recorder ignores the new fields.

## 7. The mid-stream failure correction (D7)

**The defect.** On the streaming path `dispatch_outcome(success = true)` fires at
`engine/stream.rs:230`, the instant a stream is *obtained*, before a single chunk arrives. The
mid-stream error path (`stream.rs:250-259`) yields `StreamEvent::Error` and returns **without
dispatching anything**. A stream that dies halfway is therefore recorded to every health recorder
as a success.

**The fix.** Dispatch a failure outcome on the mid-stream error path before returning.

**The accepted behavior change.** Mid-stream failures now count toward the circuit breaker,
connection cooldown, and model lockout, which they never have. This is beyond the ordinary remit
of a routing slice and is taken deliberately: §5.1 weights on `reliability`, and weighting on a
signal known to be wrong is worse than not weighting at all. It gets its own acceptance criterion
(AC9) so it is reviewed on its merits rather than arriving as a side effect.

## 8. The orchestrator boundary (D8)

This slice does **not** plumb preferences into `ModelCall` or `Agent` nodes. `build_request` and
`input_hash(chain, payload)` (`executor/mod.rs:1499`) stay untouched, so there is no fence change.

That is a deliberate line, not an omission. The moment a node carries preferences they must join
`input_hash`, or changing them and resuming silently replays a memo produced under the old policy
— SP-DATA-2's TOCTOU hole in a new costume. It deserves its own slice and its own fence test.

**Replay is unaffected regardless.** Selection happens inside a live effect; on a memo hit the
gateway is never called (`executor/mod.rs:1508-1511`). A nondeterministic router cannot produce a
`DeterminismViolation`.

**Orchestrator behavior is unchanged today**, because assembled chains have distinct priorities
(§5.1) and the default is a no-op on them.

**The documented consequence.** Once a tied chain is authored, two *fresh* runs may select
different models. Resume remains exact. This must be stated loudly in the operator docs, because
"the orchestrator is reproducible" is a property people rely on.

## 9. Observability

Weighted random is unexplainable in a bug report unless the decision is recorded. `SelectionResult`
carries, and `ExecutionTrace` stores:

- the strategy applied (and, for `Latency`/`Throughput`, whether it degraded for want of samples),
- the resulting candidate order,
- for the weighted default, the weight each candidate received and its `cost` / `reliability` inputs.

Without this, "why did it pick the expensive one" has no answer.

## 10. Acceptance criteria

| # | Criterion |
|---|---|
| AC1 | A request with no `routing` field selects byte-identically to today on every distinct-priority chain, across many `RandomSource` seeds |
| AC2 | In a tied group priced [1, 3], the cheaper candidate leads in ≈9 of 10 seeded draws |
| AC3 | A free (unpriced) candidate precedes every priced candidate in its group, and a zero-reliability candidate follows every drawable one — both on every seed |
| AC4 | `only` admits on AND across non-empty axes; `ignore` excludes on OR (§4.1) |
| AC5 | A candidate that is both `ignore`d and circuit-open reports `ExcludedByPolicy`, and an all-excluded selection is terminal, not pausable |
| AC6 | `order` sequences the candidates it names; unmatched candidates follow as fallbacks |
| AC7 | `sort: latency` with zero observations equals priority order; with partial observations, unmeasured candidates hold their index |
| AC8 | `sort: price` orders by estimated cost across the whole chain, overriding `priority` |
| AC9 | A mid-stream failure dispatches a failure outcome to every recorder (§7) |
| AC10 | `ExecutionTrace` records the applied strategy and the weights behind a weighted selection |
| AC11 | `cargo test --workspace` green; `cargo clippy --all-targets -- -D warnings` and `cargo fmt --check` clean |

## 11. Testing

Red-first per task. The tests that actually pin the design:

- **AC1 is the safety test.** It is what makes shipping a nondeterministic default defensible;
  it must run over many seeds, not one.
- **AC2 pins the formula**, not merely "it is random". A test asserting only that order varies
  would pass against any weighting, including a uniform one.
- **AC5 mirrors `a_health_skip_is_reported_ahead_of_the_window_for_the_same_candidate`**, which
  pins the same kind of gate-ordering claim and is the precedent for how to write it.
- **Mutation-test every "guarded by X" claim.** Per the SP-6 lesson: moving `RoutingPolicyGate`
  from first to last must redden AC5, and removing the free-first branch must redden AC3. If no
  single-line source mutation breaks a test, that test is not pinning anything.
- Verify real exit codes; `cargo test` exits 101 on a compile error too, so grep the log for
  `panicked at`.

## 12. Out of scope

- **`require_parameters`** — needs a per-field capability model (§3).
- **Durable / cross-worker performance stats** — the port is the seam; a Postgres backend is a
  later slice (D6).
- **Preferences on orchestrator nodes** — needs an `input_hash` change and a fence test (§8).
- **Populating `resume_after` on the mid-stream `StreamEvent::Error`.** §7's fix makes a deadline
  available where none was before, and surfacing it would be strictly more information for the
  caller. It is a change to the stream error payload, so it is recorded here rather than folded in
  silently; the dispatch result is discarded with `let _`, matching `stream.rs:229`.
- **Per-chain config-side routing policy** — SP-CAT's `IntraTierStrategy` already owns that axis,
  and D1 scoped this slice to the request.

## 13. Risks

| Risk | Mitigation |
|---|---|
| A nondeterministic default surprises someone | AC1 proves it is inert on every chain that exists; §8 documents the tied-chain consequence |
| Reliability weighting on a thin sample swings routing | `reliability` is `1.0` when unmeasured; `MIN_SAMPLES` gates the metric sorts; §9 makes the inputs visible |
| The §7 breaker change trips breakers that used not to trip | Own acceptance criterion (AC9), reviewed on its merits |
| `IntraTierStrategy::Weighted` looks like the obvious follow-on | Deliberately not in this slice; D1 keeps the request and config axes separate until the request one is proven |
