# Routing and Model Selection

This document describes how the `gateway` crate turns an inbound
`InferenceRequest` into a concrete provider call: how routers are configured,
how the `ModelSelectionService` picks candidate models, how the resolved
provider-facing model id is injected into the request, and the three ways a
request can be routed.

Sources:

- `crates/gateway/src/engine.rs` — the `Gateway` orchestrator (`execute`).
- `crates/gateway/src/selection.rs` — `ModelSelectionService`, `SelectionCriteria`.
- `crates/gateway/src/types/config.rs` — `RouterConfig`, `ModelConfig`, `ChainEntry`, `FallbackChainConfig`, `GatewayConfig`.

---

## Overview: from request to provider

`Gateway::execute` drives the whole flow:

1. **Snapshot config.** The `GatewayConfig` is cloned out of an
   `Arc<RwLock<...>>`. If `routers`, `models`, **and** `chains` are all empty,
   it returns `GatewayError::NotConfigured`.
2. **Build `SelectionCriteria`.** The request's `capability`, `model`, `router`,
   `chain`, and `budget` are copied across. `input_tokens` is filled from a
   rough `estimate_input_tokens` heuristic (≈ 1 token per 4 characters of
   payload text; STT is always `0`).
3. **Resolve candidates.** A `ModelSelectionService` is built over the config
   and the shared `CircuitBreakerManager`, and `select_all` produces an ordered
   `Vec<SelectedModel>`. If it is empty, `execute` returns
   `GatewayError::NoCandidates { capability }`.
4. **Read fallback triggers.** If a chain was resolved, its
   `fallback_triggers` slice is used; otherwise the trigger set is empty (so a
   direct, chain-less request never falls back).
5. **Walk candidates in order.** For each candidate the engine looks up an
   adapter by `candidate.router`, injects the resolved model (see
   [api_model_id resolution](#api_model_id-vs-registry-model-id)), and calls
   `adapter.execute(&candidate.router_config, req)`. Every attempt is recorded
   as an `Attempt` in `response.attempts`.
   - **Success** records `record_success` on the circuit breaker, sets
     `response.model = Some(candidate.model)` (the **registry** id), and returns.
   - **Failure** records `record_failure`; the loop continues to the next
     candidate only if the error `should_trigger_fallback(fallback_triggers)`,
     otherwise it breaks immediately.
   - **No adapter registered** for `candidate.router` records a failed attempt
     and always continues to the next candidate, regardless of the trigger set.
6. **Exhaustion.** If no candidate succeeds, `execute` returns
   `GatewayError::AllAttemptsFailed { attempts, errors }`.

Selection decides *which* provider+model to call; the adapter decides *how* to
call it. The circuit-breaker endpoint key used throughout is
`"{router}:{model}"` built from the **registry** model id, not the provider id.

---

## Router config (`RouterConfig`)

A router is a named provider endpoint. Routers live in
`GatewayConfig.routers` keyed by router id.

```rust
pub struct RouterConfig {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key_env: Option<String>,
    /// Literal API key — populated by the caller (e.g. daemon resolves it
    /// from Keychain). Takes precedence over `api_key_env` when both are set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}
```

| Field         | Type                      | Notes |
|---------------|---------------------------|-------|
| `url`         | `String`                  | Base endpoint. Required. |
| `api_key_env` | `Option<String>`          | Name of an env var holding the key. Omitted from JSON when `None`. |
| `api_key`     | `Option<String>`          | Literal key, injected by the caller (Keychain). **Takes precedence over `api_key_env`.** Omitted from JSON when `None`. |
| `enabled`     | `bool`                    | Defaults to `true` (via `default_true`) when absent from JSON. |
| `timeout_ms`  | `Option<u64>`             | Optional per-router timeout. Omitted when `None`. |
| `headers`     | `HashMap<String, String>` | Extra request headers. Defaults to empty. |

Only `enabled` is consulted by the selection service. `url`, `api_key`,
`api_key_env`, `timeout_ms`, and `headers` are carried through unread on
`SelectedModel.router_config` and consumed by the adapter at execute time.
`Gateway::refresh_router_keys` lets a caller re-resolve every router's `api_key`
in place (e.g. after a key is set or cleared) without a restart.

---

## Model selection flow

`ModelSelectionService::new(config, circuit_breaker)` borrows the config and
the breaker. Selection is driven by `SelectionCriteria`:

```rust
pub struct SelectionCriteria {
    pub capability: Capability,
    pub model: Option<String>,
    pub router: Option<String>,
    pub chain: Option<String>,
    pub budget: Option<f64>,
    pub input_tokens: Option<u32>,
}
```

The public entry points are `select` and `select_all`. Both call
`resolve_candidates`, then set `selected = all_candidates.first().cloned()`.
**The two methods are currently identical** (see
[Notes](#notes-and-surprises)); the engine calls `select_all`.

The result is a `SelectionResult`:

```rust
pub struct SelectionResult {
    pub selected: Option<SelectedModel>,
    pub all_candidates: Vec<SelectedModel>,
    pub skipped: Vec<SkippedCandidate>,
    pub chain: Option<FallbackChainConfig>,
}
```

`all_candidates` holds every model that passed validation (in try order);
`skipped` holds every rejected candidate with a human-readable `reason`;
`chain` is `Some` only when a chain drove resolution (tiers 2/3).

### Three-tier resolution

`resolve_candidates` dispatches on which criteria were supplied:

```rust
// Tier 1: Direct — router given, OR a model given with no chain.
if criteria.router.is_some() || (criteria.model.is_some() && criteria.chain.is_none()) {
    return self.resolve_direct(criteria);
}
// Tier 2: Named chain.
if let Some(chain_name) = &criteria.chain {
    if let Some(chain) = self.config.chains.get(chain_name) {
        return self.resolve_chain(chain, criteria);
    }
    return /* empty result */;
}
// Tier 3: By capability.
self.resolve_by_capability(criteria)
```

Note the precedence: **`router` (or a chain-less `model`) wins over `chain`.**
A request that sets *both* `router` and `chain` is resolved as a direct pair,
not as a chain.

### Per-candidate validation pipeline

Each candidate passes the same ordered gauntlet; the first failing check
appends a `SkippedCandidate` and drops the candidate:

1. **Router exists** in `config.routers` — else `"router not found"`.
2. **Router enabled** — else `"router disabled"`.
3. **Model exists** in `config.models` — else `"model not found"`.
4. **Model supports the capability** (`model_config.capabilities.contains(&criteria.capability)`) — else `"does not support {capability:?}"`.
5. **Circuit breaker closed** for `"{router}:{model}"` — else `"circuit breaker open"`.
6. **Within budget** — if both `criteria.budget` and a `CostEstimate` exist and
   `estimated > budget`, else `"over budget (estimated .., budget ..)"`.

In `resolve_direct` (tier 1) the pipeline runs once and any failure returns an
empty result (with the skip recorded). In `resolve_chain` a failure `continue`s
to the next entry, so later entries can still be selected.

### Cost estimation

`estimate_cost` returns `None` for any model without `pricing`, which makes it
pass the budget check unconditionally (treated as free). When pricing exists:

```text
input_cost  = input_tokens        * pricing.input_per_1k  / 1000
output_cost = max_output_tokens   * pricing.output_per_1k / 1000
estimated   = input_cost + output_cost   // also the CostEstimate.maximum
minimum     = input_cost                 // output assumed unused
```

`input_tokens` comes from the criteria (the engine's heuristic estimate) and
`max_output_tokens` from the model config, i.e. the estimate assumes the full
output budget is spent.

### Chain resolution (`resolve_chain`)

Entries are cloned and sorted by `ChainEntry.priority` (ascending), then walked:

- The model is looked up first; a missing model is skipped as
  `"model not found"` (its router is reported as the entry's `router` or
  `"unknown"`).
- The router is resolved as `entry.router` **else `model_config.provider`**
  (this is the single-provider fallback — see below).
- Validation steps 2–6 above then apply, each `continue`-ing on failure.
- Surviving entries become `SelectedModel`s carrying `priority = entry.priority`.

The returned `SelectionResult.chain` is `Some(chain.clone())`, so the engine can
read that chain's `fallback_triggers`.

### Capability resolution (`resolve_by_capability`, tier 3)

When neither a model, a router, nor a chain is pinned, tier 3 picks a chain by
capability:

```rust
self.config.chains.values()
    .filter(|c| c.capability == criteria.capability)
    .min_by(|a, b| a.id.cmp(&b.id))
```

Because `config.chains` is a `HashMap` with unstable iteration order, several
chains can share a capability; selecting the **lowest chain id** makes the
default deterministic (issue #80). Callers who need a specific chain should pin
it by name (tier 2). If no chain matches the capability, the result is empty and
the engine surfaces `NoCandidates`.

---

## `api_model_id` vs registry model id

There are two distinct model identifiers:

- **Registry id** — the key in `GatewayConfig.models` (equal to
  `ModelConfig.id`). Used internally for lookups, circuit-breaker endpoints,
  trace `Attempt.model`, and the returned `response.model`.
- **`api_model_id`** — the string the *provider* expects (e.g. a registry id of
  `claude-haiku` mapping to `claude-haiku-4-5-20251001`).

`SelectedModel.api_model_id` is resolved during selection:

- **Direct (tier 1):** `model_config.api_model_id` else the registry id.
  ```rust
  let api_model_id = model_config.api_model_id.clone()
      .unwrap_or_else(|| model_name.clone());
  ```
- **Chain (tiers 2/3):** the chain entry may override it:
  ```rust
  let api_model_id = entry.api_model_id.clone()
      .or_else(|| model_config.api_model_id.clone())
      .unwrap_or_else(|| model_name.clone());
  ```

### Injection at execute time

The engine only rewrites the outbound request's `model` when the **caller did
not pin one**:

```rust
let req_for_adapter: &InferenceRequest = if request.model.is_some() {
    request                                   // caller-pinned: passed verbatim
} else {
    owned_request = InferenceRequest {
        model: Some(candidate.api_model_id.clone()),  // resolved model injected
        ..request.clone()
    };
    &owned_request
};
```

So for chain/capability requests (no pinned `model`), the adapter receives the
resolved `api_model_id`; without this the adapter would fall back to its own
built-in default. This is exercised by the `chain_selection_injects_resolved_api_model_id`
test, where a chain entry with `api_model_id: None` resolves `noop` → `noop-v2`
from the model config and the adapter observes `noop-v2`.

**Caveat:** a caller-pinned `request.model` is passed through *verbatim* and is
**not** translated to its `api_model_id`. Because the direct tier requires that
pinned string to be a registry key (step 3 above), pinning a model whose
registry id differs from its `api_model_id` sends the *registry* id to the
provider. Pin the provider-facing id directly if that matters. Either way,
`response.model` on success always reports the registry id (`candidate.model`).

---

## The three routing modes

### 1. Direct model + router

The caller sets both `request.router` and `request.model`. Resolution goes
through `resolve_direct`, validating exactly that one pair. This is the most
explicit mode and never falls back (no chain ⇒ empty `fallback_triggers`).

```jsonc
{ "capability": "text_chat", "model": "gemma3:27b", "router": "ollama" }
```

### 2. Named chain

The caller sets `request.chain` (and no `router`, and no chain-less `model`).
The named `FallbackChainConfig` is walked in priority order, yielding an ordered
candidate list; the engine tries them in turn, honouring the chain's
`fallback_triggers` on provider errors. Tier 3 is the same machinery with the
chain chosen automatically by capability.

```jsonc
{ "capability": "text_chat", "chain": "chat_chain" }
```

### 3. Single-provider (`model.provider == router id`)

A model's `ModelConfig.provider` doubles as a router id. Inside a chain, an
entry that omits `router` resolves it to `model_config.provider`:

```rust
let router_name = entry.router.clone()
    .unwrap_or_else(|| model_config.provider.clone());
```

So a chain can list bare models and let each route to the router named by its
own `provider`. `Gateway::list_models_for_router` uses the same rule to
enumerate a router's reachable models: any model whose `provider == router_id`,
plus any chain entry pinned to that router.

> This provider fallback lives **only** in `resolve_chain`. `resolve_direct`
> does *not* fall back to `model.provider`: a request with a `model` but no
> `router` still enters tier 1, where `router_name` defaults to `""`, fails the
> "router not found" check, and yields no candidates. To route by provider,
> reach it through a chain (tier 2/3), not a bare model.

---

## Notes and surprises

Behaviours worth flagging when reading the source:

- **`select` and `select_all` are identical.** Both compute all candidates and
  set `selected` to the first. The doc comment on `select` ("Select the first
  valid candidate") describes intended, not actual, behaviour.
- **`SelectionResult.selected` is populated by the service**, but the engine
  ignores it and iterates `all_candidates` directly. Within `resolve_direct` /
  `resolve_chain` the field is left `None` with a `// filled by caller` comment;
  `select`/`select_all` fill it afterwards.
- **`ModelPricing.per_request` is never read** by `estimate_cost`; only
  `input_per_1k` and `output_per_1k` affect the estimate.
- **Direct requests never fall back.** A chain-less request has an empty
  trigger set, so a single provider error goes straight to `AllAttemptsFailed`.
- **Adapter-not-found always continues**, unlike a provider error which only
  continues when a fallback trigger matches.
- **Pinned models bypass `api_model_id` translation** (see the caveat above).
