# Gateway — Feature Reference

The `gateway` crate is a provider-agnostic **LLM inference routing engine**:
connect provider credentials once, then route requests through named fallback
chains with a per-endpoint circuit breaker, budget filtering, and request
tracing. The companion `gateway-embedded` crate adds in-process (local)
inference — llama.cpp, ONNX Runtime, FastEmbed — behind the same adapter
abstraction, so local and cloud models compose in one routing config.

This folder documents the library feature-by-feature. Every page traces its
claims to source and carries a **Notes** section flagging existing quirks.

## Contents

| Page | What it covers |
|------|----------------|
| [routing-and-selection](routing-and-selection.md) | How a request becomes a provider call: routers, `ModelSelectionService`, `api_model_id` resolution, the three routing modes |
| [fallback-chains](fallback-chains.md) | `FallbackChainConfig`/`ChainEntry`, `FallbackTrigger` variants, the candidate walk (continue vs. break) |
| [circuit-breaker](circuit-breaker.md) | Per-endpoint breaker states, config, and how it skips candidates |
| [budget-and-cost](budget-and-cost.md) | Token metering today (`ModelPricing`, `estimate_cost`, budget filtering); future quota/tiered metering |
| [capabilities-and-adapters](capabilities-and-adapters.md) | **Target** capability-trait model (`ChatModel`/`EmbedModel`/…), per-capability registry, how to add an adapter |
| [providers](providers.md) | Reference for the 16 cloud adapters: id, base URL, auth style, capabilities, quirks |
| [embedded-inference](embedded-inference.md) | `gateway-embedded`: llama.cpp / ONNX / FastEmbed engines, cargo features |
| [model-registry](model-registry.md) | `ModelResolver`, `ChainedResolver`, Managed / Ollama-read-through / External sources |
| [streaming](streaming.md) | `stream()`, `StreamChunk`, `StreamingToolCall` accumulation, SSE parsing |
| [tool-calling](tool-calling.md) | `ToolDefinition`/`ToolCall`, per-provider wire differences, streamed assembly |
| [tracing-and-attempts](tracing-and-attempts.md) | `Attempt`/`AttemptStatus`, what callers see on success vs. failure |
| [persistence-store](persistence-store.md) | The `GatewayStore` trait (consumer-implemented persistence) |
| [configuration](configuration.md) | `GatewayConfig`/`RouterConfig`, `resolve_api_key` precedence, runtime config updates |

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
- `noop` is the catch-all test/dev adapter — it accepts every capability and
  returns a canned "no provider" response.
- `openai` also registers under other ids (`openrouter`, `vercel`, `nvidia`, …)
  via `with_id`, sharing one implementation across OpenAI-compatible endpoints.
- `base` and `async_job` are shared helpers, not adapters — `async_job` drives
  the submit-then-poll pattern used by the async media adapters.
- This matrix must be updated when an adapter gains or loses a capability.
