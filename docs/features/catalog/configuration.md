---
title: Configuration
doctype: feature
module: catalog
status: implemented
source: crates/kernel/src/types/config.rs, crates/gateway/src/config.rs
---

# Configuration

The gateway is driven by a single in-memory config value, `GatewayConfig`. It
describes the **routers** (provider endpoints), **models**, and **fallback
chains** the engine can use. Config is plain data — `Serialize`/`Deserialize`
structs — so it can be authored as JSON, built programmatically, or resolved by
the daemon and handed to the engine.

Sources: `crates/gateway/src/types/config.rs`,
`crates/gateway/src/adapters/base.rs`, `crates/gateway/src/config.rs`,
`crates/gateway/src/engine.rs`.

## `GatewayConfig`

```rust
pub struct GatewayConfig {
    #[serde(default)] pub routers: HashMap<String, RouterConfig>,
    #[serde(default)] pub models:  HashMap<String, ModelConfig>,
    #[serde(default)] pub chains:  HashMap<String, FallbackChainConfig>,
}
```

Three maps, each keyed by string id:

- `routers` — keyed by router id (e.g. `"anthropic"`, `"openai"`, `"ollama"`).
  A model's `provider` and a chain entry's `router` refer to these keys.
- `models` — keyed by model id (the `ModelConfig.id`).
- `chains` — keyed by chain id (the `FallbackChainConfig.id`).

`GatewayConfig` derives `Default` (all maps empty). Every field is
`#[serde(default)]`, so an empty `{}` deserialises to an empty config. The engine
treats an all-empty config as unconfigured: `execute` returns
`GatewayError::NotConfigured`, and `is_configured()` returns `false`.

## `RouterConfig`

A provider endpoint plus how to authenticate and call it.

| Field | Type | Notes |
| --- | --- | --- |
| `url` | `String` | Base URL of the provider endpoint (required; must be non-empty per builder validation). |
| `api_key_env` | `Option<String>` | Name of an env var holding the API key. Omitted from JSON when `None`. |
| `api_key` | `Option<String>` | **Literal** API key. Populated by the caller (the daemon resolves it from the Keychain and inserts it before passing config to an adapter). Takes precedence over `api_key_env`. Omitted from JSON when `None`. |
| `enabled` | `bool` | `#[serde(default = "default_true")]` — defaults to `true` when absent. |
| `timeout_ms` | `Option<u64>` | Request timeout in milliseconds. Omitted from JSON when `None`. |
| `headers` | `HashMap<String, String>` | `#[serde(default)]` — extra HTTP headers, empty by default. |

### API key resolution (`resolve_api_key`)

From `crates/gateway/src/adapters/base.rs`, the adapter resolves the key for a
request with this precedence:

1. `config.api_key` — the literal key, if set. Returned directly.
2. `config.api_key_env` — otherwise, the value of the named env var
   (`std::env::var(env_var).ok()`), if the var exists.
3. Otherwise `None` — the request proceeds without auth.

```rust
pub fn resolve_api_key(config: &RouterConfig) -> Option<String> {
    if let Some(literal) = config.api_key.as_ref() {
        return Some(literal.clone());
    }
    config.api_key_env.as_ref()
        .and_then(|env_var| std::env::var(env_var).ok())
}
```

Note the env-var branch is best-effort: a set-but-missing env var yields `None`
(no error). The resolved key, when present, is applied as `bearer_auth` on the
outgoing request.

### `headers` and `timeout_ms`

Both are consumed in `adapters/base.rs`:

- `timeout_ms` — `build_client` sets `reqwest`'s client timeout to
  `Duration::from_millis(timeout_ms)` when present; otherwise the client has no
  explicit timeout.
- `headers` — every `(key, value)` in the map is attached to the outgoing
  request via `req.header(k, v)`, in addition to the bearer auth header. These
  are per-router extra headers (e.g. provider-specific version headers).

## `ModelConfig`

Describes one model and what it can do.

| Field | Type | Notes |
| --- | --- | --- |
| `id` | `String` | Internal model id; also the map key in `GatewayConfig.models`. |
| `api_model_id` | `Option<String>` | Provider-facing model id to send, when different from `id`. Omitted from JSON when `None`. |
| `provider` | `String` | Router id that serves this model. Must match a key in `routers` (builder validation). |
| `capabilities` | `Vec<Capability>` | Capabilities this model supports (e.g. `TextChat`, `TextEmbed`). |
| `context_window` | `u32` | Max context window in tokens. |
| `max_output_tokens` | `u32` | Max output tokens. Must be non-zero (rule 5 below). |
| `pricing` | `Option<ModelPricing>` | Cost model; `None` for free/local models. Omitted from JSON when `None`. |

### `ModelPricing`

| Field | Type | Notes |
| --- | --- | --- |
| `input_per_1k` | `f64` | USD per 1K input tokens. Must be **finite and non-negative**. |
| `output_per_1k` | `f64` | USD per 1K output tokens. Must be **finite and non-negative**. |
| `per_request` | `Option<f64>` | Flat per-request surcharge, if any. Must be **finite and non-negative** when `Some`. Omitted from JSON when `None`. |

#### Validation (SP-ROUTE-1.1)

One rule — `ModelPricing::validate()` in `crates/kernel/src/types/config.rs` —
called from three sites. It rejects `NaN`, `±inf` and any negative value,
because such a number cannot be used to **compare** candidates: a `NaN` makes
the routing comparator intransitive (`sort_by` panics on that) and a negative
value sorts *first* under `sort: price`, taking the cheapest slot with a figure
that is not a cost.

| Site | Behaviour |
| --- | --- |
| `Deserialize for ModelPricing` (`#[serde(try_from)]`) | Hard error naming the field and the value, e.g. `input_per_1k must not be negative, got -0.001`. A config **file** carrying a bad price does not load, on any path. Only `Deserialize` is affected — `Serialize` still derives, so a valid pricing serializes exactly as before and round-trips unchanged. |
| `Facade::build` | Drops the model, logs at `warn`. `build`'s signature is unchanged (still `-> Facade`); it may simply build with fewer models, and a chain entry naming a dropped model skips as `ModelNotFound`. |
| `collect_validation_errors` | Rule 7 above, for `GatewayBuilder::build` / `Gateway::try_new` / `try_update_config`. |

**Deliberately accepted, not oversights.** An explicit `0.0` is a real price,
distinct from `pricing: None`, and the two tie under `sort: price`; a large
finite magnitude such as `1e300` also loads, because any cap would be an
invented threshold and a prohibitive price is a legitimate way to park a model
at the back of a chain.

**Why the facade drops rather than nulls.** `pricing: None` means *free* and
free sorts **first**, so "repairing" a bad price by nulling it would hand it the
cheapest slot — the opposite of the intent.

Note the knock-on for the catalog: `cost_band` is derived from `pricing` when
`catalog.cost_band` is absent, so a model dropped by the facade contributes no
derived band either — it is not in the config at all.

## Fallback chains

### `ChainEntry`

One candidate in a chain.

| Field | Type | Notes |
| --- | --- | --- |
| `model` | `String` | Model id to use. Must reference a known model (builder validation). |
| `router` | `Option<String>` | Pin the entry to a specific router; when `None`, the model's default `provider` is used. Omitted from JSON when `None`. |
| `api_model_id` | `Option<String>` | Per-entry override of the provider model id. Omitted from JSON when `None`. |
| `priority` | `u8` | Ordering within the chain (lower = tried first). |

### `FallbackChainConfig`

| Field | Type | Notes |
| --- | --- | --- |
| `id` | `String` | Chain id; also the map key in `GatewayConfig.chains`. |
| `capability` | `Capability` | Capability this chain serves. |
| `models` | `Vec<ChainEntry>` | Ordered candidates. |
| `fallback_triggers` | `Vec<FallbackTrigger>` | Which failure classes cause the engine to advance to the next entry. |

### `FallbackTrigger`

`#[serde(rename_all = "snake_case")]` enum — the conditions that trigger a
fallback: `RateLimit` (`"rate_limit"`), `Timeout` (`"timeout"`), `ProviderError`
(`"provider_error"`), `ModelUnavailable` (`"model_unavailable"`),
`BudgetExceeded` (`"budget_exceeded"`).

## Building and validating config

`crates/gateway/src/config.rs` provides `GatewayBuilder`, a fluent builder that
validates before producing a `GatewayConfig`:

- `add_router(id, RouterConfig)`, `add_model(ModelConfig)` (keyed by
  `config.id`), `add_chain(FallbackChainConfig)` (keyed by `config.id`).
- `validate() -> Vec<String>` collects **all** errors:
  1. at least one router must be configured;
  2. every router `url` must be non-empty;
  3. every chain entry's `model` must reference a known model;
  4. every model's `provider` must have a corresponding router;
  5. every model's `max_output_tokens` must be non-zero — a model that can emit no
     output cannot serve a chat call, and the SP-DATA-5 budget clamp would otherwise
     send it `max_tokens: Some(0)` on every budgeted request;
  6. every model's `context_window` must be non-zero — since SP-7a the
     `ContextWindowGate` skips such a model for every request carrying any input;
  7. every model's `pricing`, when present, must be **comparable** — see
     [`ModelPricing`](#modelpricing). The error reads
     `model '<id>' has unusable pricing: <reason>`.
- `build() -> Result<GatewayConfig, Vec<String>>` returns `Err` with the full
  error list if validation fails.
- `from_config(GatewayConfig)` reconstitutes a builder from an existing config
  (round-trip / edit-then-revalidate).

## Loading and updating at runtime

The engine holds config behind an `Arc<RwLock<GatewayConfig>>` (see
`Gateway` in `crates/gateway/src/engine.rs`), so it can be swapped live without
rebuilding the `Gateway`.

### `Gateway::update_config`

```rust
pub async fn update_config(&self, config: GatewayConfig) {
    let mut guard = self.config.write().await;
    *guard = config;
}
```

Replaces the entire config atomically under the write lock. In-flight requests
that already cloned the config (`execute` clones under a read lock at the top of
each call) are unaffected; the next request picks up the new config. This is how
the daemon applies a freshly loaded/edited config.

### `Gateway::refresh_router_keys`

```rust
pub async fn refresh_router_keys<F>(&self, resolver: F)
where F: Fn(&str) -> Option<String>
{
    let mut config = self.config.write().await;
    for (id, router) in config.routers.iter_mut() {
        router.api_key = resolver(id);
    }
}
```

Re-resolves the **literal** `api_key` for every router by calling the
caller-supplied `resolver(router_id)`. Used after a key is set or cleared (e.g.
in the Keychain) so the next request picks up the change **without a daemon
restart**. Because it overwrites `api_key` for every router with the resolver's
return value, a resolver returning `None` for a router clears that router's
literal key (after which `resolve_api_key` would fall back to `api_key_env`).

## Scenarios

```gherkin
Feature: Configuration
  Scenario: Literal api_key wins over api_key_env
    Given a RouterConfig with both api_key and api_key_env set
    Then resolve_api_key returns the literal api_key
  Scenario: Invalid config is rejected
    Given a chain referencing a model with no matching router
    Then try_new returns an InvalidConfig error
  Scenario: Runtime update hot-swaps chains
    Given a running gateway
    When try_update_config installs a new chain set
    Then subsequent requests route by the new chains
```
