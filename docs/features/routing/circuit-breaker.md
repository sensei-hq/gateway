# Circuit Breaker

Per-endpoint circuit breaking for the inference routing engine. The breaker
tracks the health of each `router:model` endpoint and temporarily removes
failing endpoints from selection so the engine stops hammering a provider that
is down, rate-limiting, or erroring.

Source: `crates/gateway/src/circuit_breaker.rs`,
`crates/gateway/src/selection.rs`, `crates/gateway/src/engine.rs`.

## Purpose

When a provider endpoint starts failing, continuing to send it traffic wastes
latency budget on requests that are likely to fail and can deepen an outage.
The circuit breaker records the outcome of every attempt per endpoint. After a
configurable number of consecutive failures it "opens" that endpoint, and model
selection skips it entirely — routing traffic to healthy fallback candidates
instead — until a timeout elapses and a limited probe is allowed through.

State is held per endpoint in an in-memory `HashMap` and is **ephemeral**: it is
not persisted and does not survive a process restart (see the doc comment on
`CircuitBreakerManager`). The manager is `Clone` and internally
`Arc<Mutex<…>>`, so a single instance is shared by the `Gateway` and borrowed by
the `ModelSelectionService`.

## States and transitions

The breaker for an endpoint is one of three `BreakerState` variants:

| Variant | Data | Meaning |
| --- | --- | --- |
| `Closed { failure_count }` | consecutive failures since the last success | Healthy. Requests flow normally. |
| `Open { next_retry }` | `Instant` at which a probe becomes allowed | Tripped. Requests are blocked until `next_retry`. |
| `HalfOpen { success_count }` | consecutive successes while probing | Trial period. Probe requests are allowed to test recovery. |

`BreakerState::name()` maps these to the strings `"closed"`, `"open"`, and
`"half_open"`.

Unknown endpoints are lazily initialised to `Closed { failure_count: 0 }` on
their first `can_execute` call (`states.entry(...).or_insert(...)`).

### Transition rules

Transitions are driven by three methods: `can_execute` (admission check, called
during selection), `record_success`, and `record_failure` (outcome recording,
called by the engine after an attempt).

**From `Closed`:**

- `record_failure` increments `failure_count`. When it reaches
  `config.threshold` (`failure_count >= threshold`), the state becomes
  `Open { next_retry: Instant::now() + config.timeout }`.
- `record_success` resets `failure_count` to `0`. Because a success zeroes the
  counter, `threshold` counts *consecutive* failures.
- `can_execute` returns `true` (never blocks).

**From `Open`:**

- `can_execute` compares the clock to `next_retry`. If
  `Instant::now() >= next_retry` it transitions the endpoint to
  `HalfOpen { success_count: 0 }` and returns `true` (this one probe is
  admitted). Otherwise it returns `false` and the endpoint stays `Open`. The
  timeout is evaluated lazily on the next `can_execute`; there is no background
  timer.
- `record_success` and `record_failure` are both no-ops here (the code comments
  note this "shouldn't happen in normal flow", since selection does not admit
  an `Open` endpoint).

**From `HalfOpen`:**

- `can_execute` returns `true` (probe requests are allowed).
- `record_success` increments `success_count`. When it reaches
  `config.half_open_max_requests` (`success_count >= half_open_max_requests`),
  the state becomes `Closed { failure_count: 0 }` — the endpoint is considered
  recovered.
- `record_failure` immediately returns the endpoint to
  `Open { next_retry: Instant::now() + config.timeout }` — a single failure
  during the trial re-trips the breaker.

```
                 record_failure x threshold
      ┌────────┐ ───────────────────────────▶ ┌──────┐
      │ Closed │                               │ Open │
      └────────┘ ◀───────────────────────────  └──────┘
          ▲       record_success x                │  ▲
          │       half_open_max_requests          │  │ can_execute
          │                                        │  │ before next_retry
          │                          can_execute   │  │ (blocked, stays Open)
          │                          at/after      ▼  │
          │       ┌──────────┐       next_retry ───────
          └────── │ HalfOpen │ ◀────────────────
     record_       └──────────┘
     success          │
     (count++,        │ record_failure
      not yet met)    ▼
                   (back to Open)
```

## Configuration

`CircuitBreakerConfig` has three fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `threshold` | `usize` | Consecutive failures in `Closed` before the circuit opens. |
| `timeout` | `Duration` | How long the circuit stays `Open` before `can_execute` will promote it to `HalfOpen`. |
| `half_open_max_requests` | `usize` | Consecutive successes in `HalfOpen` required to close the circuit. |

`Default` values are `threshold: 5`, `timeout: Duration::from_secs(300)` (5
minutes), and `half_open_max_requests: 3`.

A manager is constructed with `CircuitBreakerManager::new(config)`. The
`Gateway` receives a fully-constructed manager via `Gateway::new(config,
adapters, circuit_breaker)`; the config is applied at construction and shared for
the process lifetime.

### Naming caveat

Despite its name, `half_open_max_requests` does **not** cap the number of probe
requests admitted while `HalfOpen`. `can_execute` returns `true` for every call
in `HalfOpen`, so any number of concurrent probes can be admitted. The field is
strictly the count of *successful* probes needed to close the circuit; the code
comment describes it accurately as "Number of consecutive successes in HalfOpen
before closing the circuit." Treat the name as a historical artifact.

## How the engine records outcomes

`Gateway::execute` (`engine.rs`) walks the ordered candidate list and, for each
attempt, builds the endpoint key and records the outcome against the shared
breaker:

```rust
let endpoint = format!("{}:{}", candidate.router, candidate.model);
// ... adapter.execute(...) ...
Ok(mut response) => {
    self.circuit_breaker.record_success(&endpoint);   // engine.rs
    // ... returns the response
}
Err(err) => {
    self.circuit_breaker.record_failure(&endpoint);   // engine.rs
    // ... fallback or break based on trigger classification
}
```

Notable behaviours traced from `engine.rs`:

- **Outcome is keyed on the adapter `Result`, not the response body.**
  `record_success` fires whenever `adapter.execute` returns `Ok(_)`, even if the
  returned `InferenceResponse.success` flag is `false`. `record_failure` fires
  only when the adapter returns `Err`.
- **Failures are recorded regardless of fallback classification.** A
  `record_failure` happens on every adapter `Err`, before the
  `should_trigger_fallback` decision that governs whether the loop continues to
  the next candidate or breaks. So even a non-fallback error (e.g. an
  authentication error that stops the chain) still counts toward opening the
  breaker.
- **A missing adapter does not count as a failure.** When no adapter is
  registered for `candidate.router`, the engine records a failed `Attempt` and
  `continue`s *without* calling `record_failure`, so an unregistered router does
  not trip the breaker.
- **Recovery requires multiple requests.** `execute` returns immediately on the
  first `Ok`, so a single request records at most one success. Closing a
  `HalfOpen` circuit therefore takes `half_open_max_requests` (default 3)
  separate successful requests.

Because selection has already run `can_execute` for every admitted candidate
(see below), the endpoint entry always exists in the map by the time the engine
records an outcome. This matters: `record_success` / `record_failure` use
`get_mut` and **early-return for endpoints that were never seen** — recording an
outcome for an endpoint that never went through `can_execute` is a silent no-op.

## How the breaker influences model selection

`ModelSelectionService` (`selection.rs`) holds a `&CircuitBreakerManager` and
consults it while resolving candidates. Both the direct path (`resolve_direct`,
tier 1) and the chain path (`resolve_chain`, tiers 2/3) apply the same check.

For each candidate, after validating that the router exists and is enabled and
that the model supports the requested capability — and *before* the cost/budget
check — selection runs:

```rust
let endpoint = format!("{}:{}", router_name, model_name);
if !self.circuit_breaker.can_execute(&endpoint) {
    skipped.push(SkippedCandidate {
        model:  model_name.clone(),
        router: router_name,
        reason: "circuit breaker open".to_string(),
    });
    // direct path: returns immediately; chain path: `continue`s to next entry
}
```

Consequences:

- A candidate whose breaker is `Open` (and whose `timeout` has not yet elapsed)
  is dropped from `all_candidates` and appended to `SelectionResult.skipped`
  with `reason == "circuit breaker open"`. In a fallback chain the walk simply
  continues to the next entry, so traffic naturally shifts to healthy models.
- Calling `can_execute` is what admits a `Closed` or `HalfOpen` candidate — and,
  for a timed-out `Open` endpoint, it is also the point at which the endpoint is
  promoted to `HalfOpen` and the probe is allowed through. So the first request
  after `timeout` is what triggers the half-open probe, and it flows through
  normal selection as an ordinary candidate.
- The `skipped` list is diagnostic only; it is surfaced on `SelectionResult`
  alongside the chosen candidates for tracing/observability.

## Endpoint keys

An "endpoint" is a single `router:model` pair, formatted as
`format!("{}:{}", router, model)`. The router and model components are resolved
consistently across the codebase:

- **Selection**, `resolve_direct`: `router` is the caller-supplied
  `criteria.router`; `model` is `criteria.model`.
- **Selection**, `resolve_chain`: `router` is the chain entry's `router`, or —
  when the entry leaves it unset — the model's own `provider`; `model` is the
  chain entry's `model`.
- **Engine**, `execute`: `format!("{}:{}", candidate.router, candidate.model)`,
  where `candidate.router` / `candidate.model` are the values selection resolved.

Because selection and the engine derive the key from the same resolved
router/model, the admission check and the outcome recording always address the
same breaker entry.

Two consequences worth noting:

- The key is a plain string concatenation with a single `:` separator. Model ids
  may themselves contain colons (the tests use `ollama:gemma3:27b`, i.e. router
  `ollama` + model `gemma3:27b`), so an endpoint key is not safe to split back
  into `(router, model)` on the first colon. It is only ever used as an opaque
  map key.
- Endpoints are fully independent: opening `ollama:gemma3:27b` has no effect on
  `anthropic:claude-haiku` or any other endpoint.

## Manager API summary

Beyond the three methods above, `CircuitBreakerManager` exposes:

- `get_state(endpoint) -> BreakerState` — returns the current state; returns
  `Closed { failure_count: 0 }` for an unknown endpoint **without** inserting it.
- `reset(endpoint)` — removes the endpoint's entry, resetting it to the default
  `Closed`.
- `reset_all()` — clears every endpoint's state.

The internal mutex is acquired with `.lock().unwrap_or_else(|e| e.into_inner())`,
so a poisoned lock is recovered rather than propagated as a panic.
