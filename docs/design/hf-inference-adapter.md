# Design: OpenAI-compat consolidation + Hugging Face Inference adapter (HF-B)

- **Status:** Draft (decisions carried from the HF/auth brainstorm; recorded here against post-refactor state)
- **Date:** 2026-07-18
- **Crate:** `gateway`
- **Depends on:** the capability-trait refactor (landed) — adapters now implement `ChatModel`/`EmbedModel`.
- **Next in sequence:** HF-A (GGUF download), then AUTH.

## 1. Problem

Two things, in order:

1. **Duplication.** The four OpenAI-compatible cloud adapters re-declare the same wire
   types and helpers. Verified in current source:
   - `openai`, `ollama`, `grok`, `together` each redeclare `ChatCompletionRequest`,
     `ChatMessage`, `ChatChoice`, `ChatCompletionResponse`, `ChatResponseMessage`,
     `UsageResponse` (+ `EmbedRequest`/`EmbedResponse`/`EmbedData` in openai/ollama,
     `StreamChatResponse`/`StreamChoice`/`StreamDelta` in ollama/grok/together), and each
     has its own `build_chat_messages`/`usage_from_response`/`role_to_string`.
   - `openai` is the **full-featured** variant (tools via `ChatTool`/`ChatToolFunction`,
     multimodal polymorphic `ChatMessage.content`, streaming-with-tools); the other three
     are **simplified** copies (role+string content, no tools).
2. **No Hugging Face access.** HF hosts thousands of models behind an OpenAI-compatible
   router (`https://router.huggingface.co/v1`, bearer HF token, metered) — reachable as a
   thin adapter once the shared core exists.

## 2. Decisions (from brainstorm)

- **Approach A:** one canonical `openai_compat` module (wire types + `chat`/`chat_stream`/
  `embed` helper fns); every OpenAI-compatible adapter *delegates* to it. No generic
  registry adapter (keeps the id-per-type registration model).
- **HF capability scope:** chat + streaming + tools + embeddings (all four).
- **Metering:** HF router is ordinary pay-as-you-go → rides the existing dollar-metering
  cost path (per-model `ModelPricing`). No quota work here (that's AUTH).
- **Auth:** `Authorization: Bearer <HF_TOKEN>` via `resolve_api_key` — no new auth work.
- **One adapter, two surfaces:** default base_url targets the serverless router; a dedicated
  HF **Inference Endpoint** is the same wire format reached by overriding `RouterConfig.url`.

### New consequence to flag (decided: accept)
Consolidating onto the **full-featured** core means `ollama`/`grok`/`together` gain
tools + multimodal pass-through (today they silently drop tools). They are genuinely
OpenAI-compatible and support tools, so this is a **correctness improvement**, but it is a
behaviour change (tools now forwarded). Accepted.

## 3. Design

### 3.1 `adapters/openai_compat.rs` (the shared core)
A `pub(crate)` module owning the canonical OpenAI-compatible surface:
- **Wire types:** `ChatCompletionRequest` (with `tools`), `ChatMessage` (polymorphic
  content: string | multimodal parts), `ChatTool`/`ChatToolFunction`, tool-call shapes,
  `ChatCompletionResponse`/`ChatChoice`/`ChatResponseMessage`, `UsageResponse`,
  `EmbedRequest`/`EmbedResponse`/`EmbedData`, `StreamChatResponse`/`StreamChoice`/`StreamDelta`.
  (Lift openai's, since it's the superset.)
- **Helpers:** `build_chat_messages`, `usage_from_response`, `role_to_string`, tool
  (de)serialization, the SSE stream parser.
- **Entry points** the adapters call:
  ```rust
  pub(crate) async fn chat(client, base_url, default_model, cfg: &RouterConfig, req: &ChatRequest) -> Result<ChatResponse, GatewayError>;
  pub(crate) async fn chat_stream(client, base_url, default_model, cfg, req: &ChatRequest) -> Result<ChunkStream, GatewayError>;
  pub(crate) async fn embed(client, base_url, default_model, cfg, req: &EmbedRequest) -> Result<EmbedResponse, GatewayError>;
  ```
  These take the gateway's typed `io::ChatRequest`/`EmbedRequest` and return typed
  `io::ChatResponse`/`EmbedResponse` (so adapters are near-trivial). Auth = `resolve_api_key`
  + `config.headers`; model = `req.model` else `default_model`.

### 3.2 Migrate the four adapters
`openai`/`ollama`/`grok`/`together` keep their `struct XAdapter`, `Model::id`, their
non-OpenAI-compat capabilities (openai STT/TTS/Image; grok STT/TTS; together Image), and
their base_url/default-model consts. Their `ChatModel`/`EmbedModel` methods become thin
delegations to `openai_compat::{chat,chat_stream,embed}`. Delete each file's now-duplicate
wire types + helpers. Behaviour preserved (modulo the accepted tools pass-through).

### 3.3 `adapters/huggingface.rs` (the new adapter)
- `struct HuggingFaceAdapter { client }`, `Model::id -> "huggingface"`, `from_config`.
- `ChatModel` (chat + chat_stream) delegating to `openai_compat` with default base_url
  `https://router.huggingface.co/v1`.
- `EmbedModel` — **verify HF's embedding surface first.** HF's router is cleanly
  OpenAI-compatible for chat; embeddings historically go through the `feature-extraction`
  task, not always `/v1/embeddings`. If the router exposes `/v1/embeddings`, reuse
  `openai_compat::embed`; otherwise implement a small HF-specific embed path. Ship chat
  first; embed second.
- `RegisterInto`: `register_chat` (+ `register_embed` once embed lands).
- Register in `adapters/mod.rs`.

## 4. Build sequence
1. Create `openai_compat.rs` from openai's wire types + helpers + the three entry fns; unit-test it.
2. Migrate `openai` to delegate (source of truth) — tests green.
3. Migrate `ollama`, `grok`, `together` to delegate — tests green (adjust the tool-drop tests to expect pass-through).
4. Add `huggingface.rs` (chat + streaming + tools) — tests green.
5. Verify HF embeddings surface → add `EmbedModel` (shared or HF-specific).
6. Gate: `make check` + adapter tests green.

## 5. Non-goals
- No image/audio consolidation (separate concern). No quota/subscription metering (AUTH).
- Not migrating non-OpenAI-compatible adapters (anthropic/gemini/bedrock have their own shapes).
