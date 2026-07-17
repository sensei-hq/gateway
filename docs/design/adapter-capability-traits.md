# Design: Capability-Segregated Adapter Traits

- **Status:** Draft (awaiting review)
- **Date:** 2026-07-17
- **Crates affected:** `gateway`, `gateway-embedded`
- **Supersedes:** the single `InferenceAdapter` trait
- **Enables (downstream):** OpenAI-compatible adapter consolidation → Hugging Face Inference adapter (HF-B); later HF model download (HF-A) and subscription/quota auth (AUTH)

---

## 1. Context & problem

Today every provider implements one fat trait, `adapters::InferenceAdapter`:

```rust
trait InferenceAdapter: Send + Sync {
    fn id(&self) -> &str;
    fn supports(&self, capability: &Capability) -> bool;      // runtime declaration
    async fn execute(&self, cfg, req: &InferenceRequest) -> Result<InferenceResponse, _>;
    async fn stream (&self, cfg, req: &InferenceRequest) -> Result<Stream<StreamChunk>, _>;
}
```

`execute` matches on `request.payload` and returns a runtime error for payload
kinds the provider does not handle. This has four concrete problems:

1. **Runtime, not compile-time, capability safety.** Routing a chat request to an
   embed-only provider is a runtime `ProviderError`, not a type error.
2. **`supports()` drifts from reality.** It is a hand-maintained `match` that can
   fall out of sync with the actual `execute` arms. This has already happened —
   e.g. `together`'s `supports()` and its handled `Payload` arms disagree.
3. **The `Capability` enum has drifted from `Payload`.** `Capability` lists **11**
   variants (`TextChat`, `TextComplete`, `TextEmbed`, `TextRerank`, `TextModerate`,
   `ImageGenerate`, `ImageEdit`, `ImageAnalyze`, `AudioTranscribe`, `AudioGenerate`,
   `VideoGenerate`) but `Payload` models only **6** (`Chat`, `Embed`, `Stt`, `Tts`,
   `ImageGenerate`, `VideoGenerate`). Nothing enforces the relationship.
4. **`InferenceResponse` is a fat product type.** One struct carries `content`,
   `embeddings`, `transcription`, `audio`, `images`, `videos`, `tool_calls`, and
   `usage` as parallel `Option`s. A chat adapter fills `content`; an embed adapter
   fills `embeddings`; both must construct the whole struct with the rest `None`.

A downstream motivation: the OpenAI-compatible adapters (`openai`, `ollama`,
`grok`, `together`) each **re-declare** the same wire types. Consolidating that
duplication is cleaner on top of segregated capability traits, so this refactor
lands **first** (foundation-first).

## 2. Goals / Non-goals

**Goals**
- One trait per real capability; an adapter implements only what it supports.
- Capability mismatches become compile-time impossible on the dispatch path.
- Retire the hand-maintained `supports()` and the fat `InferenceResponse`
  **internally**.
- Preserve the public `Gateway::execute(InferenceRequest) -> InferenceResponse`
  facade so the two consumers (`sensei`, `strategos`) do not break.
- Keep fallback-chain, circuit-breaker, budget, and tracing behaviour identical.

**Non-goals**
- Changing the public API to capability-typed entry points (`gateway.chat(...)`).
  That is a future step (see §9); this design is a stepping stone toward it.
- Implementing the 5 drifted capabilities (`rerank`, `moderate`, `image_edit`,
  `image_analyze`). They are documented as **reserved** future traits, not built.
- The OpenAI-compat consolidation and the HF adapter themselves (separate specs
  that build on this one).

## 3. Design

### 3.1 Trait hierarchy

A shared supertrait provides identity; one trait per payload-backed capability:

```rust
/// Common identity for every adapter, regardless of capability.
trait Model: Send + Sync {
    fn id(&self) -> &str;
}

#[async_trait]
trait ChatModel: Model {
    async fn chat(&self, cfg: &RouterConfig, req: &ChatRequest)
        -> Result<ChatResponse, GatewayError>;

    /// Streaming is opt-in. Providers that stream override this; the rest
    /// inherit an `Unsupported` error rather than being forced to write a
    /// stub. Keeps a single chat trait while still expressing the capability.
    async fn chat_stream(&self, _cfg: &RouterConfig, _req: &ChatRequest)
        -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError> {
        Err(GatewayError::Unsupported { adapter: self.id().into(), what: "streaming".into() })
    }
}

#[async_trait] trait EmbedModel: Model { async fn embed(&self, cfg, req: &EmbedRequest) -> Result<EmbedResponse, GatewayError>; }
#[async_trait] trait SttModel:   Model { async fn transcribe(&self, cfg, req: &SttRequest)   -> Result<SttResponse,   GatewayError>; }
#[async_trait] trait TtsModel:   Model { async fn speak(&self, cfg, req: &TtsRequest)        -> Result<TtsResponse,   GatewayError>; }
#[async_trait] trait ImageModel: Model { async fn generate_image(&self, cfg, req: &ImageRequest) -> Result<ImageResponse, GatewayError>; }
#[async_trait] trait VideoModel: Model { async fn generate_video(&self, cfg, req: &VideoRequest) -> Result<VideoResponse, GatewayError>; }
```

An adapter implements only what it supports:

```rust
impl ChatModel  for OpenAiAdapter { … }
impl EmbedModel for OpenAiAdapter { … }
impl ImageModel for OpenAiAdapter { … }   // openai also does STT/TTS → SttModel + TtsModel
// grok:        ChatModel + SttModel + TtsModel
// together:    ChatModel + ImageModel
// ollama:      ChatModel + EmbedModel
// huggingface: ChatModel + EmbedModel   (added in the HF-B spec)
```

**Reserved future traits** (documented, not implemented): `RerankModel`,
`ModerateModel`, `ImageEditModel`, `ImageAnalyzeModel`, `CompleteModel`
(`TextComplete` is folded into `ChatModel` unless a provider needs a distinct
completion path).

> Naming note: `*Model` chosen over `*Adapter` for readability as capability
> interfaces. The registry keeps the `Adapter` name. Adjustable on review.

### 3.2 Capability ↔ Payload ↔ typed I/O

| `Capability`      | `Payload` variant | Trait        | Request       | Response       | Fills (unified response) |
|-------------------|-------------------|--------------|---------------|----------------|--------------------------|
| `TextChat`        | `Chat`            | `ChatModel`  | `ChatRequest` | `ChatResponse` | `content`, `tool_calls`, `usage` |
| `TextEmbed`       | `Embed`           | `EmbedModel` | `EmbedRequest`| `EmbedResponse`| `embeddings`, `usage` |
| `AudioTranscribe` | `Stt`             | `SttModel`   | `SttRequest`  | `SttResponse`  | `transcription`, `usage` |
| `AudioGenerate`   | `Tts`             | `TtsModel`   | `TtsRequest`  | `TtsResponse`  | `audio` |
| `ImageGenerate`   | `ImageGenerate`   | `ImageModel` | `ImageRequest`| `ImageResponse`| `images` |
| `VideoGenerate`   | `VideoGenerate`   | `VideoModel` | `VideoRequest`| `VideoResponse`| `videos` |

### 3.3 Typed request/response (decision 4b)

Each capability gets a focused request/response pair — no more parallel-`Option`
product type inside adapters:

```rust
struct ChatRequest  { model: Option<String>, messages: Vec<Message>, system: Option<String>,
                      max_tokens: Option<u32>, temperature: Option<f32>, tools: Vec<ToolDefinition> }
struct ChatResponse { content: Option<String>, tool_calls: Vec<ToolCall>, usage: Option<TokenUsage>, model: Option<String> }

struct EmbedRequest  { model: Option<String>, texts: Vec<String> }
struct EmbedResponse { embeddings: Vec<Vec<f32>>, usage: Option<TokenUsage> }

struct SttRequest  { model: Option<String>, audio: Vec<u8>, language: Option<String>, format: String }
struct SttResponse { transcription: String, usage: Option<TokenUsage> }

struct TtsRequest  { model: Option<String>, text: String, voice: Option<String>, speed: Option<f32>, output_format: AudioFormat }
struct TtsResponse { audio: Vec<u8> }

struct ImageRequest  { model: Option<String>, prompt: String, size: Option<String>, quality: Option<String>, style: Option<String>, n: u8 }
struct ImageResponse { images: Vec<ImageResult> }

struct VideoRequest  { model: Option<String>, prompt: String, duration_secs: Option<u32>, resolution: Option<String> }
struct VideoResponse { videos: Vec<VideoResult> }
```

Cross-cutting fields (`estimated_cost`, `actual_cost`, `attempts`, `success`)
stay **engine-owned** on the unified `InferenceResponse` — adapters never touch
them. Typed responses carry only provider-native results + `usage`.

**Boundary translation lives in the engine** (not in adapters or consumers):
- On the way in: `InferenceRequest` → extract `Payload` → build the typed request,
  injecting the chain-resolved `model` exactly as `engine.rs` does today.
- On the way out: typed response → assemble the unified `InferenceResponse`,
  filling only the relevant fields and attaching engine-owned cost/attempts.

The public `InferenceRequest` / `InferenceResponse` / `Payload` types are **kept
unchanged** for wire compatibility with consumers.

### 3.4 Registry

A single `dyn` object cannot be several traits at once, so storage becomes one
map per capability. The *same* `Arc<ConcreteAdapter>` is registered into each map
it qualifies for (a concrete `Arc` coerces to each `dyn *Model` independently):

```rust
struct AdapterRegistry {
    chat:  HashMap<String, Arc<dyn ChatModel>>,
    embed: HashMap<String, Arc<dyn EmbedModel>>,
    stt:   HashMap<String, Arc<dyn SttModel>>,
    tts:   HashMap<String, Arc<dyn TtsModel>>,
    image: HashMap<String, Arc<dyn ImageModel>>,
    video: HashMap<String, Arc<dyn VideoModel>>,
}

impl AdapterRegistry {
    fn register_chat (&mut self, a: Arc<dyn ChatModel>)  { self.chat.insert(a.id().into(), a); }
    fn register_embed(&mut self, a: Arc<dyn EmbedModel>) { self.embed.insert(a.id().into(), a); }
    // … one per capability. A provider that does chat+embed calls both.
    fn chat (&self, id: &str) -> Option<Arc<dyn ChatModel>>  { self.chat.get(id).cloned() }
}
```

`supports(cap)` becomes **structural**: membership in the capability's map.

### 3.5 Engine dispatch

`Gateway::execute` resolves the request's capability, then for each fallback
candidate looks up the capability-specific map and calls the typed method:

```rust
match request.capability {
    Capability::TextChat => {
        let Some(model) = self.adapters.chat(&candidate.router) else { /* no adapter → next candidate */ };
        let chat_req = to_chat_request(request, &candidate);
        model.chat(&candidate.router_config, &chat_req).await.map(from_chat_response)
    }
    Capability::TextEmbed => { … embed … }
    // …
}
```

Fallback-chain walking, circuit-breaker record-success/failure, attempt tracing,
and budget filtering are **unchanged** — they already key off `Capability`.

## 4. Migration plan (phased, low-risk)

1. **Add the new surface** — `Model` + 6 capability traits, the typed
   request/response structs, `GatewayError::Unsupported`, and the new
   `AdapterRegistry`, behind the scenes. Add a temporary bridge so the old path
   still compiles.
2. **Migrate cloud adapters one at a time**, `cargo test` green after each:
   `noop`, `anthropic`, `openai`, `gemini`, `bedrock`, `grok`, `together`,
   `ollama`, `fal`, `flux`, `kling`, `luma`, `recraft`, `replicate`, `runway`,
   `stability`. (`base` and `async_job` are shared helpers, not adapters — they
   need no trait impl, but `async_job`'s callers may shift to typed responses.)
3. **Migrate `gateway-embedded` adapters**: `llama_cpp`, `embedded_llama`,
   `fastembed`, `ort`.
4. **Switch the engine** to capability-map dispatch; **delete** the fat
   `InferenceAdapter` trait, `supports()`, and the bridge.
5. **Zero-errors gate**: `cargo test` + `cargo clippy` green across both crates.

Each phase is independently reviewable; the tree builds and tests pass at every
step.

## 5. Impact on downstream work

- **OpenAI-compat consolidation (next spec):** the shared `openai_compat` module
  provides `chat`/`embed`/stream helpers that each provider's `ChatModel` /
  `EmbedModel` impl calls — no duplicated wire types.
- **HF-B (Hugging Face adapter):** a thin `ChatModel + EmbedModel` on the shared
  core.
- **AUTH / quota (later):** capability-typed responses make per-capability usage
  and tiered/quota metering cleaner to attribute.

## 6. Testing & acceptance

- **Registry:** the same `Arc` appears in multiple capability maps; a
  wrong-capability lookup returns `None`.
- **Adapters:** each migrated adapter keeps its existing unit tests green against
  the typed methods.
- **Engine:** dispatch + fallback + circuit-breaker tests updated to the typed
  path; a test asserting an embed-only adapter is unreachable from chat dispatch.
- **Facade:** `Gateway::execute(InferenceRequest) -> InferenceResponse`
  round-trips unchanged for chat, embed, stt, tts, image, video.
- **Gate:** `cargo test` + `cargo clippy -- -D warnings` green in both crates.

**Done when:** the fat `InferenceAdapter` trait and `supports()` are deleted, all
adapters implement capability traits, the engine dispatches via capability maps,
the public facade is unchanged, and both crates are green.

## 7. Open questions

- Trait naming `*Model` vs `*Adapter` (default: `*Model`).
- Streaming as a default `ChatModel` method vs a separate `ChatStream` trait
  (default: method).
- Whether `TextComplete` warrants its own `CompleteModel` or stays folded into
  `ChatModel` (default: folded).

## 8. Future (not in scope)

- Capability-typed public entry points (`gateway.chat()/embed()/…`) — a
  coordinated consumer migration.
- Implementing the reserved capabilities (`rerank`, `moderate`, `image_edit`,
  `image_analyze`).
