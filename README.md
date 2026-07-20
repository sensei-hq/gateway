# gateway

Shared **LLM inference routing engine** — fallback chains, circuit breaker, budget management — plus optional in-process (local) inference adapters. Consumed by both [`sensei`](https://github.com/sensei-hq/sensei) and [`strategos`](https://github.com/sensei-hq/strategos).

## Crates

| Crate | What it is |
|---|---|
| [`kernel`](crates/kernel) (`sensei-kernel`) | Shared types, capability traits, the `AdapterRegistry`, and the model-registry vocabulary underpinning the crates below. No I/O of its own — the foundation the cloud and local adapters build against. |
| [`cloud-providers`](crates/cloud-providers) (`sensei-cloud-providers`) | Cloud provider adapters (~15 providers incl. Anthropic, Bedrock, OpenAI). Gated behind `gateway`'s default `cloud` feature and re-exported at `gateway::adapters::<provider>`; build `gateway` with `--no-default-features` for a lean routing core with no AWS SDK. |
| [`gateway`](crates/gateway) (`sensei-gateway`) | Provider-agnostic routing engine. Trait-based adapters (~15 cloud providers), named fallback chains, per-endpoint circuit breaker, budget filtering, request tracing, and a `GatewayStore` trait for persistence. No DB of its own; HTTP via `reqwest`/`rustls`, async via `tokio`. |
| [`local-providers`](crates/local-providers) (`sensei-local-providers`) | In-process inference adapters (`llama.cpp`, ONNX Runtime, FastEmbed). Implement the same `kernel` capability traits as the cloud adapters, so local and cloud models compose in one routing config. Engines are feature-gated. |
| [`local-engine`](crates/local-engine) (`sensei-local-engine`) | The local model engine: resolvers that map a stable model id to on-disk bytes (managed / Ollama / external, composed via `ChainedResolver`), plus optional Hugging Face pull (`hf-download`). |

`local-providers` features (all off by default — each pulls heavyweight native deps):

```
llama-cpp   # GGUF generation/embedding via llama.cpp
fastembed   # lightweight embeddings
ort         # ONNX Runtime (CPU)
```

`local-engine`'s `hf-download` feature (off by default) adds Hugging Face model pull.

## Consuming it

Pin a tagged release via a git dependency:

```toml
gateway         = { package = "sensei-gateway", git = "https://github.com/sensei-hq/gateway", tag = "v0.2.24" }
local-providers = { package = "sensei-local-providers", git = "https://github.com/sensei-hq/gateway", tag = "v0.2.24", features = ["fastembed"] }
local-engine    = { package = "sensei-local-engine", git = "https://github.com/sensei-hq/gateway", tag = "v0.2.24" }
```

`Cargo.lock` in the consuming binary pins the exact commit, so there's no silent drift.

### Developing in-place from a consumer

Clone this repo next to the consumer and add a `[patch]` (keep it dev-only) at the consumer workspace root:

```toml
[patch."https://github.com/sensei-hq/gateway"]
sensei-gateway         = { path = "../gateway/crates/gateway" }
sensei-local-providers = { path = "../gateway/crates/local-providers" }
sensei-local-engine    = { path = "../gateway/crates/local-engine" }
```

Edit locally, build the consumer against your changes, then push here, cut a new tag, and bump the pinned tag in each consumer.

## Versioning

This repo versions **independently** of its consumers. Tag releases with semver (`vMAJOR.MINOR.PATCH`); all five crates currently share version `0.3.1`.

## License

MIT
