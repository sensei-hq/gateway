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
abstraction. Planned work extends this into an **agentic execution framework**
(orchestrator) and a **decoupled data-tier** for catalog/config/usage. See the
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
| [routing](routing/README.md) | Partial | selection · fallback chains · circuit breaker · **+ connection cooldown · model lockout · quota demote-to-tier** (SP-0) |
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

### Data-tier (Phase 4 — planned, extracted from torii)
| Module | Status | Covers |
|---|---|---|
| [data-tier](data-tier/README.md) | Planned | catalog control-plane · management API · metering store |

> **Layout:** every feature page leads with frontmatter (`doctype: feature`,
> `status`, `phase`/`spec`, `source`) and carries a `## Scenarios` (Gherkin)
> block; each module has a `README.md` with a status table.

## Capability × provider matrix

Rows are adapters; columns are the six payload-backed capabilities. Derived from
each adapter's `supports()` (cloud) / `supports_capability()` (embedded).

> Column key: **Chat** = `TextChat` (+ `TextComplete`), **Embed** = `TextEmbed`,
> **STT** = `AudioTranscribe`, **TTS** = `AudioGenerate`, **Image** =
> `ImageGenerate`, **Video** = `VideoGenerate`.

| Adapter | Chat | Embed | STT | TTS | Image | Video |
|---------|:----:|:-----:|:---:|:---:|:-----:|:-----:|
| **Cloud** | | | | | | |
| `anthropic` | ✓ | | | | | |
| `openai` | ✓ | ✓ | ✓ | ✓ | ✓ | |
| `gemini` | ✓ | ✓ | | | | |
| `bedrock` | ✓ | ✓ | | | | |
| `ollama` | ✓ | ✓ | | | | |
| `together` | ✓ | | | | ✓ | |
| `grok` | ✓ | | ✓ | ✓ | | |
| `flux` | | | | | ✓ | |
| `recraft` | | | | | ✓ | |
| `stability` | | | | | ✓ | |
| `fal` | | | | | ✓ | ✓ |
| `replicate` | | | | | ✓ | ✓ |
| `kling` | | | | | | ✓ |
| `luma` | | | | | | ✓ |
| `runway` | | | | | | ✓ |
| `noop` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Embedded** | | | | | | |
| `llama_cpp` | ✓ | ✓ | | | | |
| `embedded_llama` | ✓ | ✓ | | | | |
| `fastembed` | | ✓ | | | | |
| `ort` | | ✓ | | | | |

Notes:
- `noop` is the catch-all test/dev adapter — accepts every capability, returns a canned "no provider" response.
- `openai` also registers under other ids (`openrouter`, `vercel`, `nvidia`, …) via `with_id`, sharing one implementation across OpenAI-compatible endpoints.
- `base` and `async_job` are shared helpers, not adapters — `async_job` drives the submit-then-poll pattern for async media adapters.
- Update this matrix when an adapter gains or loses a capability.
