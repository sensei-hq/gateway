# Feature: Fallback Chains

- **Crate:** `gateway`
- **Primary source:**
  - `crates/gateway/src/types/config.rs` — `FallbackChainConfig`, `ChainEntry`, `FallbackTrigger`
  - `crates/gateway/src/types/error.rs` — `GatewayError::should_trigger_fallback`
  - `crates/gateway/src/engine.rs` — `Gateway::execute` (the candidate walk)
  - `crates/gateway/src/selection.rs` — candidate resolution / ordering
  - `crates/gateway/src/types/trace.rs` — `Attempt`

---

## 1. What a fallback chain is

A **fallback chain** is an ordered list of `(model, router)` candidates for a
single capability, plus a set of error conditions that are allowed to advance
from one candidate to the next. When a request runs, the engine walks the
candidates in priority order; if a candidate fails with an error that the
chain's `fallback_triggers` name, the engine *falls through* to the next
candidate. If it fails with any other error, the walk stops.

Use a chain when you want a request to degrade gracefully across providers or
models — e.g. try a cheap local model first, fall back to a hosted model on
timeout or provider outage — without the caller having to retry or re-route by
hand. A caller opts into a chain either by pinning it by name
(`InferenceRequest.chain`) or by leaving model/router/chain unset and letting
capability-based resolution pick one (see §4).

> A chain is **not** a retry policy. Each candidate is attempted **once**; a
> trigger moves to the *next* candidate, it does not re-run the same one. Retry
> semantics (`GatewayError::is_retryable`) are a separate concept — see §6.

---

## 2. Configuration types

### `FallbackChainConfig`

The chain itself. Keyed by its `id` inside `GatewayConfig.chains`.

```rust
pub struct FallbackChainConfig {
    pub id: String,
    pub capability: Capability,
    pub models: Vec<ChainEntry>,
    pub fallback_triggers: Vec<FallbackTrigger>,
}
```

| Field               | Type                    | Meaning |
| ------------------- | ----------------------- | ------- |
| `id`                | `String`                | Chain identifier. Also the tie-breaker for capability-based selection (lowest id wins — see §4). |
| `capability`        | `Capability`            | The capability this chain serves. Capability-based resolution matches on this. |
| `models`            | `Vec<ChainEntry>`       | The candidate entries. Walked in ascending `priority` order, **not** slice order. |
| `fallback_triggers` | `Vec<FallbackTrigger>`  | Which failure conditions are allowed to advance to the next candidate. An **empty** vec means *no* error triggers fallback (the walk stops on the first failing candidate). |

### `ChainEntry`

One candidate within a chain.

```rust
pub struct ChainEntry {
    pub model: String,
    pub router: Option<String>,
    pub api_model_id: Option<String>,
    pub priority: u8,
}
```

| Field          | Type              | Meaning |
| -------------- | ----------------- | ------- |
| `model`        | `String`          | Registry id of the model (key into `GatewayConfig.models`). Note: singular `model`, not `models`. |
| `router`       | `Option<String>`  | Router to run this model on. When `None`, resolution falls back to the model's own `provider` (`selection.rs::resolve_chain`). |
| `api_model_id` | `Option<String>`  | Override for the provider-facing model id. When `None`, resolution uses `ModelConfig.api_model_id`, and if that is also `None`, the registry `model` id itself. |
| `priority`     | `u8`              | Sort key. Entries are sorted ascending by `priority` before the walk; lower numbers are tried first. |

> **Note (field ownership).** `id`, `capability`, `models`, and
> `fallback_triggers` live on `FallbackChainConfig`; `router`, `api_model_id`,
> and `priority` live on `ChainEntry`. `ChainEntry` has a singular `model`
> field, not a `models` collection, and has no `capability`/`fallback_triggers`
> of its own — those are chain-wide.

### `FallbackTrigger`

Serialized `snake_case` (e.g. `"rate_limit"`).

```rust
pub enum FallbackTrigger {
    RateLimit,
    Timeout,
    ProviderError,
    ModelUnavailable,
    BudgetExceeded,
}
```

| Variant            | Falls through when the candidate returns… |
| ------------------ | ----------------------------------------- |
| `RateLimit`        | `GatewayError::RateLimit` — the provider rate-limited the request (optionally with `retry_after_ms`). |
| `Timeout`          | `GatewayError::Timeout` — the request exceeded the adapter's time budget. |
| `ProviderError`    | `GatewayError::ProviderError` — a provider-side failure (message + optional HTTP `status`). |
| `ModelUnavailable` | `GatewayError::ModelUnavailable` — the model is not available on that adapter. |
| `BudgetExceeded`   | `GatewayError::BudgetExceeded` — the estimate exceeded the remaining budget. |

Each variant enables fallback for exactly one `GatewayError` variant. A trigger
only has effect if the matching error is actually returned *and* the trigger is
present in the chain's `fallback_triggers`.

---

## 3. The fallback decision

The whole trigger/no-trigger decision lives in one method
(`types/error.rs`):

```rust
pub fn should_trigger_fallback(&self, triggers: &[FallbackTrigger]) -> bool {
    if triggers.is_empty() {
        return false;
    }
    match self {
        GatewayError::RateLimit { .. }        => triggers.contains(&FallbackTrigger::RateLimit),
        GatewayError::Timeout { .. }          => triggers.contains(&FallbackTrigger::Timeout),
        GatewayError::ProviderError { .. }    => triggers.contains(&FallbackTrigger::ProviderError),
        GatewayError::ModelUnavailable { .. } => triggers.contains(&FallbackTrigger::ModelUnavailable),
        GatewayError::BudgetExceeded { .. }   => triggers.contains(&FallbackTrigger::BudgetExceeded),
        // Auth and AllAttemptsFailed never trigger fallback
        GatewayError::Authentication { .. }
        | GatewayError::AllAttemptsFailed { .. }
        | GatewayError::NoCandidates { .. }
        | GatewayError::NotConfigured
        | GatewayError::Network(_)
        | GatewayError::Serialization(_) => false,
    }
}
```

Which errors can vs. cannot advance the walk:

| `GatewayError` variant  | Fallback? | Rationale |
| ----------------------- | --------- | --------- |
| `RateLimit`             | if listed | Transient provider condition; another provider may succeed. |
| `Timeout`               | if listed | Transient; another candidate may be faster/healthier. |
| `ProviderError`         | if listed | Provider-side failure; another candidate may work. |
| `ModelUnavailable`      | if listed | This model/adapter can't serve it; try the next. |
| `BudgetExceeded`        | if listed | A cheaper candidate later in the chain may fit the budget. |
| `Authentication`        | **never** | A bad/missing credential will not be fixed by another candidate; failing over would mask the misconfiguration. |
| `Network(_)`            | **never** | Local/transport failure, not attributable to the candidate. |
| `Serialization(_)`      | **never** | Bug in the request/response shape; retrying elsewhere won't help. |
| `NoCandidates`          | **never** | Raised before the walk; not an in-walk failure. |
| `NotConfigured`         | **never** | Raised before the walk. |
| `AllAttemptsFailed`     | **never** | This *is* the terminal error the walk produces. |

Two guards to keep in mind:

- **Empty `triggers` ⇒ always `false`.** A chain with no `fallback_triggers`
  (or a direct/capability path that resolved no chain — see §4) treats *every*
  failure as fatal and stops on the first failing candidate.
- **`Authentication` is the canonical stop.** Even with all five triggers
  listed, an auth error breaks the loop immediately. This is asserted directly
  by `auth_error_never_triggers_fallback` in `error.rs` and
  `execute_stops_on_auth_error` in `engine.rs`.

---

## 4. How the engine walks candidates

`Gateway::execute` (`engine.rs`) is the orchestrator. In order:

1. **Guard.** Clone the config; if `routers`, `models`, and `chains` are all
   empty, return `GatewayError::NotConfigured`.
2. **Resolve candidates.** Build `SelectionCriteria` from the request and call
   `ModelSelectionService::select_all`. Resolution is 3-tier
   (`selection.rs::resolve_candidates`):
   - **Tier 1 — direct:** `router` set, or `model` set with no `chain`. Yields
     at most one candidate and **no chain** (`chain: None`).
   - **Tier 2 — named chain:** `chain` set → look up that chain.
   - **Tier 3 — capability:** nothing pinned → pick the chain whose
     `capability` matches, breaking ties by **lowest `id`** (deterministic;
     `HashMap` order is not stable — see the `#80` note in `resolve_by_capability`).
   Within a chain, entries are `sort_by_key(|e| e.priority)` and each is
   validated (router exists + enabled, model supports the capability, circuit
   breaker closed, within budget). Failing candidates go to `skipped`, not to
   `all_candidates`. So the engine only ever walks pre-validated candidates.
3. **No candidates ⇒** `GatewayError::NoCandidates { capability }`.
4. **Triggers.** `fallback_triggers` are read from the resolved chain, or an
   **empty slice** when there is no chain (tier 1 direct, or an unresolved
   chain). Direct requests therefore never fall through.
5. **Walk.** For each candidate, in order, with a 1-based `sequence`:

   ```text
   ┌─ get adapter for candidate.router
   │   └─ not registered  → record Failed attempt (fallback_triggered = false),
   │                        then ALWAYS continue to next candidate
   ├─ execute(candidate.router_config, request)
   │   ├─ Ok(resp)  → circuit_breaker.record_success
   │   │              record Success attempt, attach all attempts to resp,
   │   │              set resp.model = candidate.model, return Ok
   │   └─ Err(e)    → circuit_breaker.record_failure
   │                  should_fallback = e.should_trigger_fallback(triggers)
   │                  record Failed attempt (fallback_triggered = should_fallback)
   │                  if should_fallback { continue } else { break }
   └─
   ```

6. **Exhausted / broken.** After the loop, join every attempt's error into one
   string and return
   `GatewayError::AllAttemptsFailed { attempts: <count>, errors }`.

### Model injection during the walk

If the caller did **not** pin `request.model`, the engine clones the request and
sets `model = Some(candidate.api_model_id)` before handing it to the adapter, so
chain/registry selection actually drives the provider model. A caller-pinned
`request.model` takes precedence and is passed through unchanged
(`chain_selection_injects_resolved_api_model_id` covers this).

### Fall-through vs. break — summary

- **Fall through (continue):** adapter execution returned an error *and*
  `should_trigger_fallback(triggers)` was `true`.
- **Break (stop):** adapter execution returned an error and
  `should_trigger_fallback` was `false` (non-trigger error, or empty triggers).
  The remaining candidates are **not** tried; the walk falls out to
  `AllAttemptsFailed`.
- **Special case — unregistered adapter:** if no adapter is registered for
  `candidate.router`, the engine records a Failed attempt and **always
  `continue`s** to the next candidate. This path does *not* consult
  `should_trigger_fallback`, and the recorded attempt has
  `fallback_triggered = false` even though the walk advanced. See §6.

---

## 5. Interaction with attempt tracing

Every step of the walk pushes an `Attempt` (`types/trace.rs`) onto a local
`Vec<Attempt>`:

```rust
pub struct Attempt {
    pub sequence: u8,            // 1-based position in the walk
    pub adapter: String,        // candidate.router
    pub model: String,          // candidate.model (registry id)
    pub api_model_id: String,   // resolved provider-facing id
    pub status: AttemptStatus,  // Success | Failed
    pub duration_ms: u64,       // measured with Instant per candidate
    pub tokens: Option<TokenUsage>,
    pub cost: Option<f64>,
    pub error: Option<String>,          // err.to_string() on failure
    pub fallback_triggered: bool,       // = should_trigger_fallback result
}
```

Behavior worth knowing:

- **One `Attempt` per candidate touched**, including candidates whose adapter was
  missing (status `Failed`, error `"no adapter registered for router '…'"`).
- **`fallback_triggered`** records whether *that* failure was a trigger. On a
  successful attempt it is `false`; on the missing-adapter path it is `false`
  even though the walk continued (see the caveat in §6).
- **On success**, the accumulated attempts are attached to
  `response.attempts` and returned to the caller (`execute_records_attempts`),
  so a winning-after-fallback response carries the full failed-then-succeeded
  history. `response.model` is set to the *winning* candidate's registry
  `model` id.
- **On total failure**, the structured `Vec<Attempt>` is **not** returned. Only
  its error strings survive, flattened into `AllAttemptsFailed.errors` as
  `"[adapter:model] <error>; …"`. Callers that need the structured trace on the
  failure path do not get it from `execute` today.
- The engine also feeds the circuit breaker per attempt
  (`record_success` / `record_failure` keyed on `"router:model"`), which
  influences whether a candidate is even offered on the *next* request (open
  breakers are filtered out during selection as `skipped`).

---

## 6. Notes, caveats, and surprises

- **`is_retryable` ≠ `should_trigger_fallback`.** They diverge on two variants:
  - `Network(_)` **is** retryable but **never** triggers fallback.
  - `BudgetExceeded` is **not** retryable but **can** trigger fallback (if
    `BudgetExceeded` is in the trigger set).
  So "retryable" and "fails over to the next candidate" are genuinely different
  predicates; do not assume one implies the other.
- **Unregistered adapters bypass the trigger check.** A missing adapter always
  advances the walk regardless of `fallback_triggers`, and its `Attempt` is
  recorded with `fallback_triggered = false`. This is an intentional skip, but
  it means `fallback_triggered = false` does **not** guarantee the walk stopped
  at that step — read `status` and position together.
- **Direct (tier-1) requests never fall over.** Tier-1 resolution returns
  `chain: None`, so the engine uses an empty trigger slice and one candidate;
  any failure yields `AllAttemptsFailed` with a single attempt.
- **Terminal error is always `AllAttemptsFailed`,** even when the underlying
  cause was a single non-trigger error (e.g. auth). The *reason* is preserved
  only inside the `errors` string, not as a typed variant.
- **`FallbackTrigger` serializes `snake_case`;** config authors write
  `"rate_limit"`, `"provider_error"`, `"model_unavailable"`,
  `"budget_exceeded"`, `"timeout"`.
