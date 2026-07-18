# Configuration

`GatewayConfig` is the whole picture: **routers**, **models**, **chains**, and
(optional) **constraints**. Build it with `GatewayBuilder` or deserialize it from
JSON — both produce the same struct.

## The four sections

```rust
pub struct GatewayConfig {
    routers: HashMap<String, RouterConfig>,        // provider endpoints + creds
    models:  HashMap<String, ModelConfig>,         // callable models
    chains:  HashMap<String, FallbackChainConfig>, // ordered fallback lists
    constraints: ConstraintsConfig,                // quotas (see recipes.md)
}
```

## RouterConfig — a provider endpoint

```rust
RouterConfig {
    url: "https://api.openai.com/v1".into(),
    api_key_env: Some("OPENAI_API_KEY".into()), // env var name to read at call time
    api_key: None,                              // OR a literal key (takes precedence)
    enabled: true,
    timeout_ms: Some(30_000),                   // per-request timeout
    headers: Default::default(),                // extra headers (don't put secrets here)
}
```

**Key resolution:** `api_key` (literal) wins over `api_key_env` (env lookup). The
daemon pattern is to resolve a secret (e.g. from the OS keychain) and inject it into
`api_key` before handing the config to the gateway — the library never reaches for a
secret itself. `Debug` on `RouterConfig` redacts the key.

## ModelConfig — a callable model

```rust
ModelConfig {
    id: "gpt-4o".into(),                         // registry id you reference in requests
    api_model_id: Some("gpt-4o".into()),         // id sent to the provider (defaults to id)
    provider: "openai".into(),                   // MUST match a router key
    capabilities: vec![Capability::TextChat, Capability::TextEmbed],
    context_window: 128_000,
    max_output_tokens: 4096,
    pricing: Some(ModelPricing { input_per_1k: 0.005, output_per_1k: 0.015, per_request: None }),
}
```

Add `pricing` to get real cost figures (`estimated_cost` / `actual_cost` on the
response, and dollar burn-rate via the store). Without it, costs are `0.0`.

## FallbackChainConfig — ordered fallback

```rust
FallbackChainConfig {
    id: "chat".into(),
    capability: Capability::TextChat,
    models: vec![
        ChainEntry { model: "gpt-4o".into(),        router: Some("openai".into()),    api_model_id: None, priority: 1 },
        ChainEntry { model: "claude-sonnet".into(), router: Some("anthropic".into()), api_model_id: None, priority: 2 },
    ],
    fallback_triggers: vec![
        FallbackTrigger::RateLimit,
        FallbackTrigger::Timeout,
        FallbackTrigger::ProviderError,
    ],
}
```

Candidates are tried in `priority` order. A failure only advances to the next
candidate if its error kind is in `fallback_triggers` (else the chain stops).
Triggers: `RateLimit`, `Timeout`, `ProviderError`, `ModelUnavailable`, `BudgetExceeded`.

## Build + validate

```rust
let config = GatewayBuilder::new()
    .add_router("openai", router_cfg)
    .add_model(model_cfg)
    .add_chain(chain_cfg)
    .constraints(constraints_cfg)   // optional (AUTH quotas)
    .build()                        // Result<GatewayConfig, Vec<String>> — ALL errors at once
    .map_err(|errs| errs.join("; "))?;
```

`build()` (and `Gateway::try_new`) reject: no routers, empty router URLs, a chain
referencing an unknown model, and a model whose `provider` has no router.

## Load from JSON instead

Every config type is `Serialize`/`Deserialize`. New 0.3 fields (`constraints`,
`auth`) are `#[serde(default)]`, so 0.2-era JSON still loads.

```rust
let config: GatewayConfig = serde_json::from_str(&json)?;
let gateway = Gateway::try_new(config, adapters, cb)?; // validates
```

## Update config at runtime

`gateway.update_config(new_config).await` (or `try_update_config` to validate first)
swaps the whole picture atomically — the next request uses it. Use
`refresh_router_keys(|id| …)` to re-inject keys without a restart.
