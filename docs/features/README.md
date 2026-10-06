---
title: Feature Reference — Module Index
doctype: index
module: index
status: partial
---

# Gateway & Orchestrator — Feature Reference

The `gateway` crate is a provider-agnostic **LLM inference routing engine**:
connect provider credentials once, then route requests through named fallback
chains with health gates, budget filtering, and request tracing. The
`local-providers` crate adds in-process inference behind the same adapter
abstraction. The **agentic execution framework** (orchestrator) builds on it. Persistence —
catalog/config/usage data and the orchestrator's Postgres store — belongs to the
Torii product repo, not here ([data-tier](data-tier/README.md), torii DECISIONS §11). See the
program design in
[`../superpowers/specs/2026-08-06-sensei-orchestrator-design.md`](../superpowers/specs/2026-08-06-sensei-orchestrator-design.md)
and the full feature catalog in
[`../superpowers/specs/2026-08-06-sensei-orchestrator-features-and-approach.md`](../superpowers/specs/2026-08-06-sensei-orchestrator-features-and-approach.md).

Docs are organized **by module**. Each module has a `README.md` with a status
table (Implemented · Partial · Planned). Every page leads with frontmatter and
traces its claims to source, with a **Notes** section for quirks.

## Modules

### Gateway (existing core + Phase-1 enhancements)
| Module | Status | Covers |
|---|---|---|
| [routing](routing/README.md) | Partial | selection · fallback chains · circuit breaker · **+ connection cooldown · model lockout · quota demote-to-tier** (SP-0) · **+ per-request provider preferences** (SP-ROUTE-1) |
| [catalog](catalog/README.md) | Partial | model registry · configuration · **+ free-tier catalog · tiers & chains · catalog refresh · config versioning** (SP-CAT) |
| [inference](inference/README.md) | Implemented | providers · capabilities & adapters · streaming · tool-calling |
| [governance](governance/README.md) | Partial | budget & cost · subscription quota · **+ usage metering · expiration tracking · predicted lockout** |
| [local](local/README.md) | Implemented | embedded inference (llama.cpp / ONNX / FastEmbed) |
| [observability](observability/README.md) | Implemented | tracing & attempts · persistence store (`GatewayStore`) |
| [vault](vault/README.md) | Implemented | BYOK envelope-encrypted credential vault (`crates/vault`) |

### Orchestrator (Phase 3 — planned)
| Module | Status | Covers |
|---|---|---|
| [orchestrator](orchestrator/README.md) | Planned | execution graph · durable journal · agents/skills/tools · shared context · hooks |

### Data-tier (superseded — torii owns persistence, DECISIONS §11)
| Module | Status | Covers |
|---|---|---|
| [data-tier](data-tier/README.md) | Superseded — torii owns persistence ([§11](data-tier/README.md)) | catalog control-plane · management API · metering store — in `sensei-hq/torii` |

> **Layout:** every feature page leads with frontmatter (`doctype: feature`,
> `status`, `phase`/`spec`, `source`) and carries a `## Scenarios` (Gherkin)
> block; each module has a `README.md` with a status table.

## Capability × provider matrix

Rows are adapters; columns are the seven payload-backed capabilities. Derived from
each adapter's `RegisterInto` impl (cloud and embedded alike).

> Column key: **Chat** = `TextChat` (+ `TextComplete`), **Embed** = `TextEmbed`,
> **STT** = `AudioTranscribe`, **TTS** = `AudioGenerate`, **Image** =
> `ImageGenerate`, **Video** = `VideoGenerate`, **Decision** = `Decision`
> (System One — probabilities, not text).

| Adapter | Chat | Embed | STT | TTS | Image | Video | Decision |
|---------|:----:|:-----:|:---:|:---:|:-----:|:-----:|:--------:|
| **Cloud** | | | | | | | |
| `anthropic` | ✓ | | | | | | |
| `openai` | ✓ | ✓ | ✓ | ✓ | ✓ | | |
| `openrouter` | ✓ | ✓ | ✓ | ✓ | ✓ | | ✓ |
| `typesafe` | | | | | | | ✓ |
| `cloudflare` | | | | | | | ✓ |
| `llamacpp` | | | | | | | ✓ |
| `sglang` | | | | | | | ✓ |
| `gemini` | ✓ | ✓ | | | | | |
| `huggingface` | ✓ | ✓ | | | | | |
| `bedrock` | ✓ | ✓ | | | | | |
| `ollama` | ✓ | ✓ | | | | | ✓ |
| `together` | ✓ | | | | ✓ | | |
| `grok` | ✓ | | ✓ | ✓ | | | |
| `flux` | | | | | ✓ | | |
| `recraft` | | | | | ✓ | | |
| `stability` | | | | | ✓ | | |
| `fal` | | | | | ✓ | ✓ | |
| `replicate` | | | | | ✓ | ✓ | |
| `kling` | | | | | | ✓ | |
| `luma` | | | | | | ✓ | |
| `runway` | | | | | | ✓ | |
| `noop` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Embedded** | | | | | | | |
| `llama_cpp` | ✓ | ✓ | | | | | |
| `embedded_llama` | ✓ | ✓ | | | | | |
| `fastembed` | | ✓ | | | | | |
| `ort` | | ✓ | | | | | |
| `kokoro` | | | | ✓ | | | |

Notes:
- `noop` is the catch-all test/dev adapter — accepts every capability, returns a canned "no provider" response.
- `openai` also registers under other ids (`openrouter`, `vercel`, `nvidia`, …) via `with_id`, sharing one implementation across OpenAI-compatible endpoints. Its capability set is fixed regardless of id, so the `openrouter` row's chat…image ticks are OpenAI's set, not a claim about what OpenRouter serves.
- `openrouter` and `typesafe` are auto-registered by the facade when their router id is in config: `openrouter` = `OpenAIAdapter` (id `openrouter`) + a decision-only `SystemOneAdapter`; `typesafe` = `SystemOneAdapter` only. See [capabilities & adapters § Decision](inference/capabilities-and-adapters.md#decision-system-one).
- `cloudflare`, `llamacpp` and `sglang` are likewise auto-registered by the facade, decision-only: `cloudflare` = `CloudflareAdapter` (Workers AI `clef` / `clef-flash`; `url` = `https://api.cloudflare.com/client/v4/accounts/<account_id>/ai`), `llamacpp` / `sglang` = `SystemOneAdapter` pointed at a self-hosted `llama-server` / SGLang server. A chat adapter the caller registers under one of these ids lives in the chat map and is left untouched. `llamacpp` (a server over HTTP) is not the embedded `llama_cpp` engine. There is no `vllm` id — vLLM has no upstream `/v1/systemone`.
- `base` and `async_job` are shared helpers, not adapters — `async_job` drives the submit-then-poll pattern for async media adapters.
- Update this matrix when an adapter gains or loses a capability.
