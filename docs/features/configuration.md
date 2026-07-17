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
| `max_output_tokens` | `u32` | Max output tokens. |
| `pricing` | `Option<ModelPricing>` | Cost model; `None` for free/local models. Omitted from JSON when `None`. |

### `ModelPricing`

| Field | Type | Notes |
| --- | --- | --- |
| `input_per_1k` | `f64` | USD per 1K input tokens. |
| `output_per_1k` | `f64` | USD per 1K output tokens. |
| `per_request` | `Option<f64>` | Flat per-request surcharge, if any. Omitted from JSON when `None`. |

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
  4. every model's `provider` must have a corresponding router.
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
