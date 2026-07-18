# gateway — LLM usage guides

Task-oriented, copy-pasteable guides for using the `gateway` crate (and its
companion `gateway-embedded`). For deep reference see `docs/features/`; this
folder is the fast path for an agent that needs to *use* the crate.

## What it is

A **provider-agnostic LLM inference routing engine**. You configure providers +
models + fallback chains once, then send requests **by capability** (chat, embed,
image, …). The caller never picks a provider SDK — the gateway routes, retries
down a fallback chain, trips a per-endpoint circuit breaker, meters cost, and
(optionally) enforces subscription quotas.

## The mental model (4 pieces)

1. **Router** — a provider endpoint + credentials (`RouterConfig`, keyed by id, e.g. `"openai"`).
2. **Model** — something callable, tied to a router via `provider` (`ModelConfig`, e.g. `"gpt-4o"`).
3. **Chain** *(optional)* — an ordered list of models to try, with fallback triggers (`FallbackChainConfig`).
4. **Adapter** — the code that speaks a provider's wire format, registered under the router's id.

Then: `gateway.execute(&InferenceRequest { capability, payload, … }).await`.

## Setup in 4 steps

```rust
let config   = GatewayBuilder::new().add_router(..).add_model(..).build()?; // 1
let adapters = AdapterRegistry::new();                                       // 2
adapters.register(Arc::new(OpenAIAdapter::new()?)).await;                    //   (id must match the router key)
let cb       = CircuitBreakerManager::new(CircuitBreakerConfig { .. });      // 3
let gateway  = Gateway::new(config, adapters, cb);                           // 4
let resp     = gateway.execute(&request).await?;
```

## Guides

| Guide | Use it when you need to |
|---|---|
| [quickstart](quickstart.md) | Get a working chat/embed call end-to-end |
| [configuration](configuration.md) | Define routers, models, chains, pricing, keys (builder or JSON) |
| [recipes](recipes.md) | Routing modes, fallback, streaming, cost, budget, persistence, quotas, tools |
| [embedded-and-hf](embedded-and-hf.md) | Run local models (llama.cpp/ONNX) + download from the HF Hub |
| [custom-adapters](custom-adapters.md) | Add a provider the crate doesn't ship |
| [upgrade-0.2-to-0.3](upgrade-0.2-to-0.3.md) | Migrate a pinned dep from 0.2.x |

## Key invariants (don't fight these)

- **Input is uniform, output is uniform.** You send a `Payload` for a capability;
  you get an `InferenceResponse` with a known field set — regardless of provider.
- **`execute()` never changes shape per provider.** Provider differences live in
  adapters, behind the capability traits.
- **An adapter's `id()` must equal the router key** it's configured under.
- **Config is validated** (`build()` / `try_new`) — dangling model→router refs and
  empty URLs are rejected up front.
