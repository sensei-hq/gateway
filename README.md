# gateway

Shared **LLM inference routing engine** — fallback chains, circuit breaker, budget management — plus optional in-process (local) inference adapters. Consumed by both [`sensei`](https://github.com/sensei-hq/sensei) and [`strategos`](https://github.com/sensei-hq/strategos).

## Crates

| Crate | What it is |
|---|---|
| [`kernel`](crates/kernel) (`sensei-kernel`) | Shared types, capability traits, the `AdapterRegistry`, and the model-registry vocabulary underpinning the crates below. No I/O of its own — the foundation the cloud and local adapters build against. |
| [`cloud-providers`](crates/cloud-providers) (`sensei-cloud-providers`) | Cloud provider adapters (~15 providers incl. Anthropic, Bedrock, OpenAI). Gated behind `gateway`'s default `cloud` feature and re-exported at `gateway::adapters::<provider>`; build `gateway` with `--no-default-features` for a lean routing core with no AWS SDK. |
| [`gateway`](crates/gateway) (`sensei-gateway`) | Provider-agnostic routing engine. Trait-based adapters (~15 cloud providers), named fallback chains, per-request routing preferences (`sort`/`only`/`ignore`/`order`), per-endpoint circuit breaker, budget filtering, request tracing, and a `GatewayStore` trait for persistence. No DB of its own; HTTP via `reqwest`/`rustls`, async via `tokio`. |
| [`local-providers`](crates/local-providers) (`sensei-local-providers`) | In-process inference adapters (`llama.cpp`, ONNX Runtime, FastEmbed). Implement the same `kernel` capability traits as the cloud adapters, so local and cloud models compose in one routing config. Engines are feature-gated. |
| [`local-engine`](crates/local-engine) (`sensei-local-engine`) | The local model engine: resolvers that map a stable model id to on-disk bytes (managed / Ollama / external, composed via `ChainedResolver`), plus optional Hugging Face pull (`hf-download`). |

The orchestrator stack builds **on top of** the routing engine above. `gateway` knows nothing about
it — no dependency, and no notion of agents, skills or tools — so the five crates above are usable
entirely on their own.

> **Persistence lives in torii.** The gateway is a library; persistence belongs to the product that
> runs it (torii `docs/DECISIONS.md` §11). The tenant-scoped Postgres stores, their schema and the
> `torii` operator CLI are in [`sensei-hq/torii`](https://github.com/sensei-hq/torii)
> (`crates/orchestrator-store`, `crates/cli`, `database/`). This repo ships the traits, the
> executor, the in-memory stores and the conformance suite every backend is held to.

| Crate | What it is |
|---|---|
| [`orchestrator-core`](crates/orchestrator-core) (`sensei-orchestrator-core`) | The domain types and the seams: `Graph`/`NodeKind`, the registry vocabulary (`AgentDefinition`, `SkillDef`, `ToolSpec`, `Activation`), and the traits a backend implements — `ExecutionJournal`, `ContentStore`, `ContextStore`, `ConfigSource` + its write side `ConfigStore`, `SchedulerStore`. No I/O. |
| [`orchestrator`](crates/orchestrator) (`sensei-orchestrator`) | The durable, resumable executor: journal-and-fold replay, effect classes (Pure / Observation / Mutation with two-phase in-doubt reconcile), hierarchical nodes (`Subgraph`, `Branch`, `Expand`, `Loop`), permission enforcement, secret redaction, workspace and subprocess isolation, human-in-the-loop gates and tool confirmation (with observer hooks), per-tool call ceilings and escalation, token and money budgets, a durable scheduler with wake backoff, and context budgeting. |
| [`orchestrator-store`](crates/orchestrator-store) (`sensei-orchestrator-store`) | The in-memory implementations of those seams (including a writable, versioned `InMemoryConfigStore`) and `FilesystemConfigSource`, the registry-directory reader. Durable stores implement the same traits elsewhere — torii's are Postgres, tenant-scoped. |
| [`orchestrator-testkit`](crates/orchestrator-testkit) (`sensei-orchestrator-testkit`) | The store conformance suite: one function per persistence trait, each its documented contract. Every backend runs it — the in-memory stores here, torii's tenant-scoped Postgres stores there. |

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

## Testing

```bash
cargo test --workspace
```

That is the whole default suite and it needs no database, no Docker and no network.

The orchestrator's durable half — Postgres stores, cross-process resume against a real database,
the `torii` operator CLI's end-to-end suites — moved to
[`sensei-hq/torii`](https://github.com/sensei-hq/torii) with the stores (see its
`crates/cli/README.md`, "Tests"). What stays here is checked against the in-memory stores and the
shared conformance suite (`sensei-orchestrator-testkit`), which torii's stores run too.

## Versioning

This repo versions **independently** of its consumers. Tag releases with semver (`vMAJOR.MINOR.PATCH`); every crate in the workspace shares one version (`Cargo.toml`).

## License

MIT
