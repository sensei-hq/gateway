# Capabilities & Adapters

> This page describes the capability-trait model shipped by the
> **adapter-capability-traits** refactor (now landed — the old single
> `InferenceAdapter` trait has been removed). See the design doc
> ([`docs/design/adapter-capability-traits.md`](../design/adapter-capability-traits.md)).

The gateway routes an `InferenceRequest` to whichever provider can serve its
`Capability`. The architecture replaces the former fat `InferenceAdapter`
trait — where `execute` matched on the payload and returned a runtime error for
unsupported kinds — with **one trait per capability**. An adapter implements only
the traits it supports, so a capability mismatch on the dispatch path is a
compile-time impossibility rather than a runtime `ProviderError`.

---

## The `Capability` enum

`crates/gateway/src/types/capability.rs` declares **11** capabilities across four
modalities:

| Modality | Variant | Meaning |
|----------|---------|---------|
| Text  | `TextChat`        | multi-turn messages, tools, system prompts → text |
| Text  | `TextComplete`    | single prompt → text (legacy / Ollama) |
| Text  | `TextEmbed`       | text → dense vectors |
| Text  | `TextRerank`      | candidates + query → ranked list |
| Text  | `TextModerate`    | text → safety labels + scores |
| Image | `ImageGenerate`   | text → image(s) |
| Image | `ImageEdit`       | image + instructions → image |
| Image | `ImageAnalyze`    | image → text (vision, OCR) |
| Audio | `AudioTranscribe` | audio → text (STT) |
| Audio | `AudioGenerate`   | text → audio (TTS) |
| Video | `VideoGenerate`   | text/image → video |

Only **6** of these have a corresponding `Payload` variant today
(`crates/gateway/src/types/request.rs`): `Chat`, `Embed`, `Stt`, `Tts`,
`ImageGenerate`, `VideoGenerate`. The other **5** are **reserved / future**:

- `TextComplete` **folds into `ChatModel`** — a single prompt is a one-message
  chat, so no distinct completion path is built unless a provider ever needs one.
- `TextRerank`, `TextModerate`, `ImageEdit`, `ImageAnalyze` have no `Payload`, no
  typed request/response, and no capability trait yet. They are documented as
  reserved future traits (`RerankModel`, `ModerateModel`, `ImageEditModel`,
  `ImageAnalyzeModel`), not implemented.

So the target model builds exactly **6 capability traits** — the ones backed by a
real `Payload`.

---

## The capability traits

A shared supertrait carries identity; each payload-backed capability gets its own
trait. An adapter is a plain struct that implements `Model` plus whichever
capability traits it supports.

```rust
/// Common identity for every adapter, regardless of capability.
trait Model: Send + Sync {
    fn id(&self) -> &str;
}

#[async_trait]
trait ChatModel: Model {
    async fn chat(&self, cfg: &RouterConfig, req: &ChatRequest)
        -> Result<ChatResponse, GatewayError>;

    /// Streaming is opt-in. Providers that stream override this default;
    /// the rest inherit an `Unsupported` error rather than writing a stub.
    /// One chat trait still expresses the streaming capability.
    async fn chat_stream(&self, _cfg: &RouterConfig, _req: &ChatRequest)
        -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError> {
        Err(GatewayError::Unsupported { adapter: self.id().into(), what: "streaming".into() })
    }
}

#[async_trait] trait EmbedModel: Model { async fn embed(&self, cfg: &RouterConfig, req: &EmbedRequest) -> Result<EmbedResponse, GatewayError>; }
#[async_trait] trait SttModel:   Model { async fn transcribe(&self, cfg: &RouterConfig, req: &SttRequest) -> Result<SttResponse, GatewayError>; }
#[async_trait] trait TtsModel:   Model { async fn speak(&self, cfg: &RouterConfig, req: &TtsRequest) -> Result<TtsResponse, GatewayError>; }
#[async_trait] trait ImageModel: Model { async fn generate_image(&self, cfg: &RouterConfig, req: &ImageRequest) -> Result<ImageResponse, GatewayError>; }
#[async_trait] trait VideoModel: Model { async fn generate_video(&self, cfg: &RouterConfig, req: &VideoRequest) -> Result<VideoResponse, GatewayError>; }
```

Notes:

- `chat_stream` is the only default method — a provider without streaming inherits
  a `GatewayError::Unsupported` instead of being forced to write a stub. (`Unsupported`
  is a new variant added by this refactor to the existing `GatewayError` in
  `crates/gateway/src/types/error.rs`.)
- The naming convention is `*Model` (chat capability = `ChatModel`), chosen over
  `*Adapter` for readability as capability interfaces. The registry keeps the
  `Adapter` name.

---

## Capability ↔ Payload ↔ trait ↔ typed I/O

Every dispatchable capability lines up one-to-one across the enum, the wire
`Payload`, the capability trait, and a focused typed request/response pair. No
more parallel-`Option` product type inside adapters.

| `Capability`      | `Payload` variant | Trait        | Request        | Response        | Fills (unified response) |
|-------------------|-------------------|--------------|----------------|-----------------|--------------------------|
| `TextChat`        | `Chat`            | `ChatModel`  | `ChatRequest`  | `ChatResponse`  | `content`, `tool_calls`, `usage` |
| `TextEmbed`       | `Embed`           | `EmbedModel` | `EmbedRequest` | `EmbedResponse` | `embeddings`, `usage` |
| `AudioTranscribe` | `Stt`             | `SttModel`   | `SttRequest`   | `SttResponse`   | `transcription`, `usage` |
| `AudioGenerate`   | `Tts`             | `TtsModel`   | `TtsRequest`   | `TtsResponse`   | `audio` |
| `ImageGenerate`   | `ImageGenerate`   | `ImageModel` | `ImageRequest` | `ImageResponse` | `images` |
| `VideoGenerate`   | `VideoGenerate`   | `VideoModel` | `VideoRequest` | `VideoResponse` | `videos` |

The typed request/response structs mirror each `Payload` variant — e.g.
`ChatRequest { model, messages, system, max_tokens, temperature, tools }` /
`ChatResponse { content, tool_calls, usage, model }`; `EmbedRequest { model, texts }`
/ `EmbedResponse { embeddings, usage }`; and so on for STT, TTS, image, and video.

**Boundary translation lives in the engine**, not in adapters or consumers:

- **In:** `InferenceRequest` → extract `Payload` → build the typed request,
  injecting the chain-resolved `model` exactly as `engine.rs` does today.
- **Out:** typed response → assemble the unified `InferenceResponse`, filling only
  the relevant fields and attaching engine-owned cost/attempts.

Cross-cutting fields (`estimated_cost`, `actual_cost`, `attempts`, `success`) stay
**engine-owned** on `InferenceResponse` — adapters never touch them. The public
`InferenceRequest` / `InferenceResponse` / `Payload` types are **kept unchanged**
for wire compatibility with consumers (`sensei`, `strategos`), and
`Gateway::execute(InferenceRequest) -> InferenceResponse` remains the facade.

---

## The per-capability registry

A single `dyn` object cannot be several traits at once, so storage becomes **one
map per capability**. The *same* `Arc<ConcreteAdapter>` is registered into each map
it qualifies for — a concrete `Arc` coerces to each `dyn *Model` independently.

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
    // … one register_* per capability. A provider that does chat+embed calls both.
    fn chat (&self, id: &str) -> Option<Arc<dyn ChatModel>> { self.chat.get(id).cloned() }
}
```

`supports(cap)` is no longer a hand-maintained `match` — it becomes **structural**:
membership in the capability's map. A wrong-capability lookup (e.g. `chat(id)` for
an embed-only adapter) returns `None`, and the engine moves to the next fallback
candidate.

### `RegisterInto`

Registering a multi-capability adapter by hand means one `register_*` call per
trait it implements. `RegisterInto` is the ergonomic wrapper: a concrete adapter
knows every capability it supports, so it inserts the same `Arc` into each map in
one place.

```rust
/// Implemented per concrete adapter. Inserts `self` (as an `Arc`) into every
/// capability map the adapter qualifies for, instead of scattering
/// `register_chat` / `register_embed` / … at each call site.
trait RegisterInto {
    fn register_into(self: Arc<Self>, registry: &mut AdapterRegistry);
}

// e.g. OpenAI does chat + embed + stt + tts + image:
impl RegisterInto for OpenAiAdapter {
    fn register_into(self: Arc<Self>, r: &mut AdapterRegistry) {
        r.register_chat(self.clone());
        r.register_embed(self.clone());
        r.register_stt(self.clone());
        r.register_tts(self.clone());
        r.register_image(self);
    }
}
```

Registration then reads `adapter.register_into(&mut registry)` per provider,
keeping the "which maps does this adapter belong in" decision next to the adapter.

---

## Engine dispatch

`Gateway::execute` resolves the request's capability, then for each fallback
candidate looks up the capability-specific map and calls the typed method:

```rust
match request.capability {
    Capability::TextChat => {
        let Some(model) = self.adapters.chat(&candidate.router) else { /* no adapter → next candidate */ };
        let chat_req = to_chat_request(request, &candidate);
        model.chat(&candidate.router_config, &chat_req).await.map(from_chat_response)
    }
    Capability::TextEmbed => { /* … embed … */ }
    // … one arm per capability
}
```

Fallback-chain walking, circuit-breaker record-success/failure, attempt tracing,
and budget filtering are **unchanged** — they already key off `Capability`.

---

## Capability × provider matrix

Rows are the 16 cloud adapters (`crates/gateway/src/adapters/`) plus the 4 embedded
adapters (`crates/gateway-embedded/src/adapters/`). A ✓ means the adapter will
implement that capability trait and be registered into that map.

| Adapter | Chat | Embed | STT | TTS | Image | Video |
|---------|:----:|:-----:|:---:|:---:|:-----:|:-----:|
| **Cloud** | | | | | | |
| `anthropic`  | ✓ |   |   |   |   |   |
| `openai`     | ✓ | ✓ | ✓ | ✓ | ✓ |   |
| `gemini`     | ✓ | ✓ |   |   |   |   |
| `bedrock`    | ✓ | ✓ |   |   |   |   |
| `ollama`     | ✓ | ✓ |   |   |   |   |
| `together`   | ✓ |   |   |   | ✓ |   |
| `grok`       | ✓ |   | ✓ | ✓ |   |   |
| `flux`       |   |   |   |   | ✓ |   |
| `recraft`    |   |   |   |   | ✓ |   |
| `stability`  |   |   |   |   | ✓ |   |
| `fal`        |   |   |   |   | ✓ | ✓ |
| `replicate`  |   |   |   |   | ✓ | ✓ |
| `kling`      |   |   |   |   |   | ✓ |
| `luma`       |   |   |   |   |   | ✓ |
| `runway`     |   |   |   |   |   | ✓ |
| `noop`       | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Embedded** | | | | | | |
| `llama_cpp`       | ✓ | ✓ |   |   |   |   |
| `embedded_llama`  | ✓ | ✓ |   |   |   |   |
| `fastembed`       |   | ✓ |   |   |   |   |
| `ort`             |   | ✓ |   |   |   |   |

`noop` is the catch-all test/dev adapter — it claims all six capabilities.
`base` and `async_job` under `crates/gateway/src/adapters/` are shared helpers, not
providers, and implement no capability trait.

---

## Adding a new adapter

1. **Write the struct** and implement `Model` for identity:

   ```rust
   struct AcmeAdapter { id: String, /* client, config, … */ }
   impl Model for AcmeAdapter { fn id(&self) -> &str { &self.id } }
   ```

2. **Implement one capability trait per capability the provider serves** — and
   *only* those. If Acme does chat and embedding:

   ```rust
   #[async_trait]
   impl ChatModel for AcmeAdapter {
       async fn chat(&self, cfg: &RouterConfig, req: &ChatRequest) -> Result<ChatResponse, GatewayError> { /* … */ }
       // override chat_stream only if the provider streams
   }

   #[async_trait]
   impl EmbedModel for AcmeAdapter {
       async fn embed(&self, cfg: &RouterConfig, req: &EmbedRequest) -> Result<EmbedResponse, GatewayError> { /* … */ }
   }
   ```

   Adapters translate only provider-native results plus `usage`; they never fill
   engine-owned cost/attempts.

3. **Register it** via `RegisterInto`, listing exactly the maps it belongs in:

   ```rust
   impl RegisterInto for AcmeAdapter {
       fn register_into(self: Arc<Self>, r: &mut AdapterRegistry) {
           r.register_chat(self.clone());
           r.register_embed(self);
       }
   }
   ```

That is the whole contract. There is no `supports()` to hand-maintain — the set of
capability-trait impls (and thus the maps the adapter registers into) *is* the
declaration, checked by the compiler. Routing a chat request to Acme works iff
`AcmeAdapter: ChatModel`; there is no way for the declaration to drift from the
implementation.
