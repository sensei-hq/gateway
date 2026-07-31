---
name: using-gateway
description: >-
  Use when adding, integrating, or using the sensei gateway crates
  (sensei-gateway, sensei-local-providers, sensei-local-engine, sensei-vault) in a
  Rust project — a provider-agnostic multimodal inference routing engine (chat,
  embeddings, image, video, speech) with fallback chains, circuit breaker, budget
  metering, multi-model consensus/panels, in-process local models, and BYOK
  credentials. Covers how to add the dependency, pick feature flags, call it by
  capability, run local models, supply credentials, and report issues upstream.
---

# Using the gateway crates

`gateway` (`sensei-gateway`) is a **provider-agnostic multimodal inference routing
engine for Rust**. You configure routers + models + fallback chains once, then send
requests **by capability** — chat, embeddings, image, video, speech — and the engine
picks a healthy endpoint, retries down a fallback chain, trips a per-endpoint circuit
breaker, meters cost, and (optionally) runs consensus/panels or enforces quotas. The
caller never touches a provider SDK.

- **Repo:** https://github.com/sensei-hq/gateway
- **Docs:** https://gateway.sensei-hq.com/docs · machine-readable index
  https://gateway.sensei-hq.com/llms.txt · full corpus
  https://gateway.sensei-hq.com/llms-full.txt
- **In-repo docs:** `docs/llms/` (usage guides) and `docs/features/` (reference).

Always confirm the current release tag before pinning — check the repo's
[releases](https://github.com/sensei-hq/gateway/releases). Examples below use
`v0.4.8`.

## 1. Add the dependency

The gateway repo **gitignores `Cargo.lock`**, so consumers pin a **git tag** and their
own `Cargo.lock` pins the exact commit (no silent drift between builds).

**Recommended — one dependency.** Cloud adapters are on by default; opt into local
engines with features:

```toml
[dependencies]
gateway = { package = "sensei-gateway", git = "https://github.com/sensei-hq/gateway", tag = "v0.4.8", features = ["local", "local-fastembed"] }
```

`gateway` re-exports the local surface under `gateway::local`, so a single dep can use
cloud + local. Cloud-only? `default-features` already gives you that. Want to shed the
AWS SDK entirely? build with `--no-default-features` (drops the `cloud` feature).

**Multi-crate** — depend on the split crates directly if you need their types:

```toml
gateway         = { package = "sensei-gateway",         git = "https://github.com/sensei-hq/gateway", tag = "v0.4.8" }
local-providers = { package = "sensei-local-providers", git = "https://github.com/sensei-hq/gateway", tag = "v0.4.8", features = ["llama-cpp"] }
local-engine    = { package = "sensei-local-engine",    git = "https://github.com/sensei-hq/gateway", tag = "v0.4.8", features = ["hf-download"] }
# Optional BYOK credential vault (consumed out-of-band; no dep edge to gateway):
vault           = { package = "sensei-vault",           git = "https://github.com/sensei-hq/gateway", tag = "v0.4.8" }
```

After adding or bumping, apply transitive security updates locally (the repo can't
ship a lockfile): `cargo update` (or targeted `cargo update -p <crate>`).

## 2. Feature flags (`sensei-gateway`)

| Feature | Effect |
|---|---|
| `cloud` *(default)* | ~16 cloud provider adapters (re-exported at `gateway::adapters::<provider>`) |
| `local` | local engine: resolvers + provisioning supervisor |
| `local-hf-download` | pull GGUF/ONNX models from the Hugging Face Hub |
| `local-llama-cpp` | GGUF generation/embedding via llama.cpp |
| `local-fastembed` | lightweight ONNX embeddings *(currently deferred — use `local-ort`)* |
| `local-ort` | ONNX Runtime (CPU) embeddings |
| `local-kokoro` | in-process Kokoro text-to-speech |

Each `local-*` implies `local`. Default build compiles no local engines.

## 3. Mental model (4 pieces)

1. **Router** — a provider endpoint + credentials (`RouterConfig`, keyed by id, e.g. `"openai"`).
2. **Model** — something callable, tied to a router via `provider` (`ModelConfig`, e.g. `"gpt-4o"`).
3. **Chain** *(optional)* — an ordered list of models to try, with fallback triggers.
4. **Adapter** — the code that speaks a provider's wire format, registered under the router's id.

Then: `gateway.execute(&InferenceRequest { capability, payload, … }).await`. Input is
uniform and output is uniform (an `InferenceResponse` with a known field set),
regardless of provider. An adapter's `id()` must equal the router key it's registered
under.

## 4. Minimal usage

The exact builder API is in the docs — **read `quickstart` and `configuration`**
(https://gateway.sensei-hq.com/docs) before writing config. The shape:

```rust
// Quick path (v0.4+): FacadeBuilder registers cloud providers + wires the engine.
let gateway = gateway::FacadeBuilder::new(config).plans(plans).build().await?;

// Or manual: build config → register adapters (id == router key) → construct gateway.
let gateway = Gateway::new(config, adapters, circuit_breaker);

let resp = gateway.execute(&request).await?;   // routes, falls back, meters cost
```

Every response carries an **attempt-by-attempt trace** (`attempts`): which adapter /
model / duration / tokens / cost / error, and whether each triggered a fallback — use
it for debugging and observability.

## 5. Modalities (capability traits)

Send requests by capability; six are live (via `kernel` capability traits):

- **chat / text** (`ChatModel`) — incl. streaming (`execute_stream`), tool calling, vision input
- **embeddings** (`EmbedModel`)
- **image generation** (`ImageModel`) — e.g. openai, flux, stability, recraft, fal, replicate, together
- **video generation** (`VideoModel`) — e.g. fal, replicate, kling, luma, runway
- **speech-to-text** (`SttModel`) — openai, grok
- **text-to-speech** (`TtsModel`) — openai, grok (cloud), kokoro (local)

Add your own provider by implementing the capability trait(s) + `RegisterInto` — see
`docs/llms/custom-adapters.md`.

## 6. Local models

Full guide: https://gateway.sensei-hq.com/docs/local. Four ways to run local:

- **Ollama (server)** — point an `ollama` router at a running Ollama
  (`http://localhost:11434`, OpenAI-compatible); **no** `local-*` features needed.
- **Embedded Ollama models** — reuse Ollama's downloaded blobs **in-process** via an
  `OllamaResolver` + llama.cpp (no server running).
- **Embedded llama.cpp / ONNX** — resolve a `ModelEntry` (managed store) and load it
  into `LlamaCppAdapter` (chat/embed) or `OrtAdapter` (embed); Kokoro for TTS.
- **Hugging Face download** (`local-hf-download`) — pull GGUF/ONNX into the managed
  store; a fit-guard refuses a model that won't fit RAM/disk *before* downloading.

## 7. Credentials (BYOK)

The engine reads per-request credentials from `InferenceRequest.credentials`
(`HashMap<router_name, String>`). A plain value is an **API key**; a value prefixed
`oauth:` is an **OAuth/bearer token** (adapters branch on it — e.g. Anthropic sends
`Authorization: Bearer …` instead of `x-api-key`). For managed BYOK, the optional
`sensei-vault` crate seals API keys + OAuth bundles with AES-256-GCM envelope
encryption (per-tenant DEK under a KEK, Postgres/Supabase backing) and offers a
per-request `TenantKeyCache`; the gateway itself stays credential-store-agnostic.

## 8. Advanced routing (all opt-in)

- **Fallback chains** — named, priority-ordered model lists with per-error triggers.
- **Circuit breaker** — per `router:model` open/half-open/closed with cooldown probes.
- **Budget** — skip candidates over a cost cap pre-flight; record real per-call spend.
- **Consensus / panels** — `execute_panel` fans a prompt across family-distinct models
  in parallel; `execute_consensus` runs debate → synthesize → judge (independent judge
  enforced). See `docs/features/`.
- **Purpose workflows** — `execute_purpose` runs a declarative multi-step pipeline,
  threading each step's output into the next and picking a model by tier.
- **Subscription / quota** — operator tier limits + `AuthContext` on the request,
  enforced pre-flight as a hard stop.
- **Persistence** — implement the `GatewayStore` trait and attach with
  `gateway.with_store(...)`; the gateway ships no DB.

## Reporting issues

File issues on the GitHub tracker: **https://github.com/sensei-hq/gateway/issues**
(`gh issue create --repo sensei-hq/gateway`). Include:

- **Version/tag** you pinned (e.g. `v0.4.8`) and the resolved commit if you have it.
- **Feature flags** enabled, and cloud vs local path.
- **What you called** — the capability + a minimal `InferenceRequest` shape (redact
  keys) and which provider/model.
- **What happened vs expected** — the `GatewayError` variant and, crucially, the
  response **`attempts`** trace (it shows exactly which endpoints were tried and why).
- A **minimal reproduction** (smallest config + request that triggers it).
- For security-sensitive reports (credential/vault handling), note it in the issue so
  maintainers can triage privately rather than in a public thread.

Before filing, check the docs (https://gateway.sensei-hq.com/docs, especially
`configuration` and the relevant feature guide) and existing issues.
