# gateway

Shared **LLM inference routing engine** — fallback chains, circuit breaker, budget management — plus optional in-process (local) inference adapters. Consumed by both [`sensei`](https://github.com/sensei-hq/sensei) and [`strategos`](https://github.com/sensei-hq/strategos).

## Crates

| Crate | What it is |
|---|---|
| [`gateway`](crates/gateway) | Provider-agnostic routing engine. Trait-based adapters (~15 cloud providers), named fallback chains, per-endpoint circuit breaker, budget filtering, request tracing, and a `GatewayStore` trait for persistence. No DB of its own; HTTP via `reqwest`/`rustls`, async via `tokio`. |
| [`gateway-embedded`](crates/gateway-embedded) | In-process inference adapters (`llama.cpp`, ONNX Runtime, FastEmbed) and an on-disk model registry. Same `InferenceAdapter` trait as the cloud adapters, so local and cloud models compose in one routing config. Engines are feature-gated. |

`gateway-embedded` features (all off by default — each pulls heavyweight native deps):

```
llama-cpp   # GGUF generation/embedding via llama.cpp
fastembed   # lightweight embeddings
ort         # ONNX Runtime (CPU)
```

## Consuming it

Pin a tagged release via a git dependency:

```toml
gateway          = { git = "https://github.com/sensei-hq/gateway", tag = "v0.2.24" }
gateway-embedded = { git = "https://github.com/sensei-hq/gateway", tag = "v0.2.24", features = ["fastembed"] }
```

`Cargo.lock` in the consuming binary pins the exact commit, so there's no silent drift.

### Developing in-place from a consumer

Clone this repo next to the consumer and add a `[patch]` (keep it dev-only) at the consumer workspace root:

```toml
[patch."https://github.com/sensei-hq/gateway"]
gateway          = { path = "../gateway/crates/gateway" }
gateway-embedded = { path = "../gateway/crates/gateway-embedded" }
```

Edit locally, build the consumer against your changes, then push here, cut a new tag, and bump the pinned tag in each consumer.

## Versioning

This repo versions **independently** of its consumers. Tag releases with semver (`vMAJOR.MINOR.PATCH`); both crates currently share version `0.2.24`.

## License

MIT
