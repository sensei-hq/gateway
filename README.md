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

## Testing

```bash
cargo test --workspace
```

That is the whole default suite and it needs no database, no Docker and no network.

### Postgres-backed tests

The orchestrator's durable half — the Postgres journal, CAS, context store, config source
and scheduler, plus torii's cross-process operator e2e — can only be exercised against a
real database. Those tests are **conditionally ignored**: with no `DATABASE_URL` they are
reported as `ignored`, not as passed, so the number `cargo test` prints is true either way.

Bring up a throwaway database, apply the schema, and run them:

```bash
docker run -d --name gw-pg -e POSTGRES_PASSWORD=postgres -p 55432:5432 postgres:16
sleep 12
docker exec -i gw-pg psql -U postgres -v ON_ERROR_STOP=1 < database/_apply_all.sql

export DATABASE_URL=postgres://postgres:postgres@127.0.0.1:55432/postgres

cargo test --workspace                                    # the 48 conditional tests now RUN
cargo test -p sensei-torii --test e2e_pg                  # the cross-process operator loop
cargo test -p sensei-orchestrator --features postgres-tests postgres_e2e

docker rm -f gw-pg
```

Pick a port that is actually free (`lsof -i :55432`) — the suite talks to whatever
`DATABASE_URL` names, so pointing it at a database you care about will write to it.

**How the gate works.** Each package with database tests has a five-line `build.rs` that
emits `cargo::rustc-cfg=have_database_url` when the variable is set, and every such test
carries `#[cfg_attr(not(have_database_url), ignore = "...")]`. It is a build-time cfg rather
than a plain `#[ignore]` or a cargo feature because both of those are static: a plain
`#[ignore]` would need `-- --ignored` even when a database IS configured, and a
`required-features` test target would make `cargo test -p sensei-torii --test e2e_pg` fail
outright. `cargo::rerun-if-env-changed=DATABASE_URL` is what makes exporting the variable
take effect on the next build. The runtime `db_url()` guard remains as a second layer, for
the case where the variable is set at build time and absent at run time.

**This is not wired into CI.** `.github/workflows/ci.yml`'s `build · test` job sets no
`DATABASE_URL`, so it now reports these tests as `ignored` — honestly — rather than as
passing. Adding a `services: postgres:16` container to that job plus a step applying
`database/_apply_all.sql` would close the gap; that is an outward-facing CI change and has
not been made here.

## Versioning

This repo versions **independently** of its consumers. Tag releases with semver (`vMAJOR.MINOR.PATCH`); all five crates currently share version `0.3.1`.

## License

MIT
