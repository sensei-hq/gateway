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

> **Every `pricing` value must be finite and non-negative.** `NaN`, `±inf` and
> any negative number on `input_per_1k`, `output_per_1k` or `per_request` are
> rejected: a config **file** carrying one fails to load, with an error naming
> the field and the value. A price that cannot be compared is not a price — a
> `NaN` makes the routing comparator intransitive, and a negative number sorts
> *first* under `sort: price`, winning the cheapest slot.
>
> Two values are deliberately **allowed**: an explicit `0.0` (a real free price,
> distinct from `pricing: None`, and the two tie under `sort: price`), and a
> large finite magnitude such as `1e300` (any cap would be an invented
> threshold, and a prohibitive price is a legitimate way to park a model at the
> back of a chain).
>
> A `ModelPricing` you assemble **in code** skips deserialization, so it is
> caught later instead: `Gateway::try_new` / `try_update_config` /
> `GatewayBuilder::build` report it as an `InvalidConfig` error, and
> `Facade::build` **drops** that model and logs at `warn` (the facade still
> builds — its signature is unchanged — and a chain entry naming the dropped
> model then skips as `ModelNotFound`). It drops rather than nulling the price
> because `None` means *free*, and free sorts first.

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

> **Give each entry a distinct `priority` unless you mean to load balance.**
> Entries that share a `priority` become a pool: their order is drawn per
> request, weighted by `(1 / cost²) × reliability`, so two identical requests
> may pick different models. Distinct priorities route deterministically, exactly
> as before. Nothing validates `priority`, so a tie is easy to author by
> accident — see [recipes](recipes.md#shape-routing-per-request).

## Tune how much evidence a metric sort needs

```rust
use gateway::resilience::ResilienceConfig;

let mut resilience = ResilienceConfig::default();
resilience.min_samples = 5;

let gateway = Gateway::new(config, adapters, cb).with_resilience(resilience);
```

`min_samples` (default `3`) is how many live observations an endpoint needs
before `sort: latency` / `sort: throughput` trusts its mean, and before the
default strategy trusts its success rate. Below it the candidate counts as
unmeasured: it holds its position, and weighs as healthy.

**`ResilienceConfig` is `#[non_exhaustive]`, which forbids *every* struct
expression outside the gateway crate — functional-update syntax included.**
`ResilienceConfig { min_samples: 5, ..Default::default() }` does not compile for
you: it is `error[E0639]: cannot create non-exhaustive struct using struct
expression`. Call `default()` and assign, as above. The same applies to
`perf_samples` and `perf_window`, its two neighbouring knobs (retention capacity
and retention age — changing either rebuilds the performance store and discards
the samples in it, while `min_samples` is read per request).

**Do not set `min_samples` to `0`.** Not because a cold process breaks — it does
not: an endpoint with no live samples reads as `None` and is unmeasured at every
threshold, zero included. The real hazard is an endpoint carrying live samples
of one *kind* while the counter being read sits at zero next to a mean of `0.0`,
which at zero is trusted as a measurement. A failed non-streaming attempt
contributes a verdict and no latency (`samples: 0`, `mean_latency_ms: 0.0`), so
under `sort: latency` it would lead a race it never ran; an obtained-but-unfinished
stream contributes latency and no verdict (`verdict_samples: 0`,
`success_rate: 0.0`), so under the default it would be weighed at zero and go
last. Any value `>= 1` makes both unreachable.

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
referencing an unknown model, a model whose `provider` has no router, a model whose
`max_output_tokens` is 0, and a model whose `pricing` carries a non-finite or negative
value (`model '<id>' has unusable pricing: <reason>`).

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
