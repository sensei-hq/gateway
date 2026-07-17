# Capability-Segregated Adapter Traits — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single fat `InferenceAdapter` trait with capability-segregated traits (`ChatModel`, `EmbedModel`, `SttModel`, `TtsModel`, `ImageModel`, `VideoModel`) so each adapter implements only what it supports, mismatches are compile-time impossible, and the fat `InferenceResponse`/`supports()` are retired internally — while the public `Gateway::execute` facade is unchanged.

**Architecture:** New capability traits (supertrait `Model`) + per-capability request/response types (decision 4b) live behind the scenes. A per-capability `AdapterRegistry` stores the same `Arc` in each map it qualifies for. The engine translates the unified `InferenceRequest` into a typed request, dispatches to the capability map, and translates the typed response back into the unified `InferenceResponse`. Adapters are migrated one at a time with old and new paths coexisting until a final switch-and-delete, so the tree builds and every test passes at every step. This is a **behaviour-preserving refactor**: the existing test suite is the safety net; new surface (types, traits, registry, conversions) gets new tests.

**Tech Stack:** Rust (edition 2024), `async-trait`, `tokio`, `futures`, `reqwest`, `serde`. Two crates: `gateway`, `gateway-embedded`. Build/test via `cargo` (workspace root `/Users/Jerry/Developer/strategos/gateway`).

**Reference spec:** `docs/design/adapter-capability-traits.md`

---

## Conventions for every task

- All paths are relative to the gateway repo root: `/Users/Jerry/Developer/strategos/gateway`.
- Run tests from the repo root. `gateway-embedded` engine features are off by default and pull heavy native deps — **do not** enable them for unit tests unless a task says so. Test the pure-Rust surface with `cargo test -p gateway` and `cargo test -p gateway-embedded` (feature-gated adapter bodies compile only when their feature is on; their migration steps note this).
- After any code step, the definition of "green" is: `cargo test -p gateway` passes and `cargo clippy -p gateway -- -D warnings` is clean (swap `-p gateway-embedded` for embedded tasks).
- Commit after each task with the message shown. Do not push (the maintainer pushes/ tags).

---

## Phase 0 — Features documentation (write first)

These document the **existing** library so the refactor updates living docs rather than a moving target. `capabilities-and-adapters.md` is written to describe the **target** capability-trait model (it is the one doc the refactor keeps in sync). Docs are prose; "green" for Phase 0 is: the file exists, covers every listed section, and every code/behaviour claim is verified against the cited source.

### Task 0.1: Features index + capability×provider matrix

**Files:**
- Create: `docs/features/README.md`

- [ ] **Step 1: Write the index.** Include: one-paragraph overview (lift from repo `README.md`), a table of contents linking every page in Task 0.2–0.13, and a **capability × provider matrix** (rows = the 16 cloud + 4 embedded adapters, columns = Chat / Embed / STT / TTS / Image / Video), populated from the matrix in the reference spec §3.1 and the adapter `supports()` methods. Add a legend noting the matrix is generated from adapter capabilities and must be updated when an adapter gains/loses one.
- [ ] **Step 2: Verify** every adapter row matches its source `supports()` (`crates/gateway/src/adapters/*.rs`, `crates/gateway-embedded/src/adapters/*.rs`).
- [ ] **Step 3: Commit.**
```bash
git add docs/features/README.md
git commit -m "docs(features): add features index and capability matrix"
```

### Tasks 0.2–0.13: One page per feature area

For each row below: create the file, write the listed sections, verify each claim against the cited sources, then commit with `docs(features): document <area>`.

| Task | File | Required sections | Source files to read |
|---|---|---|---|
| 0.2 | `docs/features/routing-and-selection.md` | Router config; model selection flow; `api_model_id` vs registry id resolution; direct-model vs chain vs single-provider routing | `src/selection.rs`, `src/engine.rs`, `src/types/config.rs` |
| 0.3 | `docs/features/fallback-chains.md` | `FallbackChainConfig`, `ChainEntry`, `FallbackTrigger` semantics, chain-walking order, which errors trigger fallback vs break | `src/types/config.rs`, `src/engine.rs`, `src/types/error.rs` |
| 0.4 | `docs/features/circuit-breaker.md` | Per-endpoint breaker states, threshold/timeout/half-open config, interaction with selection | `src/circuit_breaker.rs`, `src/selection.rs` |
| 0.5 | `docs/features/budget-and-cost.md` | Token metering today (`ModelPricing`, `estimate_cost`, `filter_by_budget`, request `budget`), `CostEstimate`/`Cost`/`TokenUsage`; a "Future: subscription/quota & tiered metering (AUTH track)" note | `src/budget.rs`, `src/types/cost.rs`, `src/types/config.rs` |
| 0.6 | `docs/features/capabilities-and-adapters.md` | **Target** capability-trait model (link the design doc), the capability↔payload↔trait table, how to add a new adapter/capability. **This is updated by Phase 4.** | `docs/design/adapter-capability-traits.md`, `src/adapters/mod.rs` |
| 0.7 | `docs/features/providers.md` | Per-provider one-liner: id, base URL default, auth style, capabilities, default model, notable quirks — for all 16 cloud adapters | every `src/adapters/*.rs` (read the `id()`, `supports()`, `DEFAULT_MODEL`, auth calls) |
| 0.8 | `docs/features/embedded-inference.md` | The `gateway-embedded` crate: llama.cpp / ONNX Runtime / fastembed engines, the three cargo features, when to use local vs cloud | `crates/gateway-embedded/src/lib.rs`, `crates/gateway-embedded/src/adapters/*.rs`, repo `README.md` |
| 0.9 | `docs/features/model-registry.md` | `ModelResolver` trait, `ChainedResolver` precedence, Managed / Ollama-read-through / External sources, `ModelFormat`, `ModelEntry` | `crates/gateway-embedded/src/registry/*.rs` |
| 0.10 | `docs/features/streaming.md` | `StreamChunk`, `StreamingToolCall` accumulation, SSE parsing pattern, which adapters stream | `src/types/request.rs`, `src/adapters/openai.rs`, `src/adapters/anthropic.rs` |
| 0.11 | `docs/features/tool-calling.md` | `ToolDefinition`/`ToolCall`, JSON-schema pass-through, per-provider wrapping differences, streamed tool-call assembly | `src/types/request.rs`, `src/adapters/openai.rs`, `src/adapters/anthropic.rs` |
| 0.12 | `docs/features/tracing-and-attempts.md` | `Attempt`/`AttemptStatus`, how the engine records attempts, what the response exposes | `src/types/trace.rs`, `src/engine.rs` |
| 0.13 | `docs/features/persistence-store.md` + `docs/features/configuration.md` | Store page: `GatewayStore` trait purpose + methods. Config page: `GatewayConfig`/`RouterConfig`, `resolve_api_key` precedence (literal → env), `headers` | `src/store.rs`, `src/types/config.rs`, `src/adapters/base.rs` |

- [ ] Complete Tasks 0.2–0.13 per the table (create file, write sections, verify against sources, commit).

---

## Phase 1 — New type + trait surface (no behaviour change)

### Task 1.1: Add `GatewayError::Unsupported`

**Files:**
- Modify: `src/types/error.rs`
- Test: `src/types/error.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Write the failing test.** Add to the tests module in `src/types/error.rs`:
```rust
#[test]
fn unsupported_error_displays_adapter_and_capability() {
    let e = GatewayError::Unsupported { adapter: "grok".into(), what: "streaming".into() };
    let s = e.to_string();
    assert!(s.contains("grok"));
    assert!(s.contains("streaming"));
}
```
- [ ] **Step 2: Run it, verify it fails.** Run: `cargo test -p gateway unsupported_error_displays -- --nocapture`. Expected: FAIL (variant does not exist).
- [ ] **Step 3: Add the variant.** In the `GatewayError` enum in `src/types/error.rs`, add (match the existing `thiserror` `#[error(...)]` style used by neighbouring variants):
```rust
    /// The adapter exists for this capability but cannot perform the
    /// requested sub-operation (e.g. a chat adapter that has no streaming).
    #[error("adapter '{adapter}' does not support {what}")]
    Unsupported { adapter: String, what: String },
```
- [ ] **Step 4: Check fallback classification.** If `error.rs` has a method like `should_trigger_fallback`, ensure `Unsupported` returns `false` (it must break the chain, not silently fall back — same class as `Authentication`). Add/extend a test asserting `Unsupported` does not trigger fallback if that method exists.
- [ ] **Step 5: Run tests, verify pass.** Run: `cargo test -p gateway`. Expected: PASS.
- [ ] **Step 6: Commit.**
```bash
git add src/types/error.rs
git commit -m "feat(gateway): add GatewayError::Unsupported for capability sub-op gaps"
```

### Task 1.2: Add typed request/response I/O structs

**Files:**
- Create: `src/types/io.rs`
- Modify: `src/types/mod.rs` (add `pub mod io;`)
- Test: `src/types/io.rs` (inline tests)

- [ ] **Step 1: Write the module** with the six request/response pairs. These reuse existing shared types (`Message`, `ToolDefinition`, `ToolCall`, `TokenUsage`, `ImageResult`, `VideoResult`, `AudioFormat`). Full content:
```rust
//! Capability-typed request/response structs used by the segregated
//! adapter traits. Internal to the gateway crate — the public API still
//! speaks `InferenceRequest`/`InferenceResponse`; the engine translates
//! at the boundary (see `crate::dispatch`).

use super::cost::TokenUsage;
use super::request::{AudioFormat, ImageResult, Message, ToolCall, ToolDefinition, VideoResult};

#[derive(Debug, Clone)]
pub struct ChatRequest {
    pub model: Option<String>,
    pub messages: Vec<Message>,
    pub system: Option<String>,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
    pub tools: Vec<ToolDefinition>,
}

#[derive(Debug, Clone, Default)]
pub struct ChatResponse {
    pub content: Option<String>,
    pub tool_calls: Vec<ToolCall>,
    pub usage: Option<TokenUsage>,
    pub model: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EmbedRequest {
    pub model: Option<String>,
    pub texts: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct EmbedResponse {
    pub embeddings: Vec<Vec<f32>>,
    pub usage: Option<TokenUsage>,
}

#[derive(Debug, Clone)]
pub struct SttRequest {
    pub model: Option<String>,
    pub audio: Vec<u8>,
    pub language: Option<String>,
    pub format: String,
}

#[derive(Debug, Clone, Default)]
pub struct SttResponse {
    pub transcription: String,
    pub usage: Option<TokenUsage>,
}

#[derive(Debug, Clone)]
pub struct TtsRequest {
    pub model: Option<String>,
    pub text: String,
    pub voice: Option<String>,
    pub speed: Option<f32>,
    pub output_format: AudioFormat,
}

#[derive(Debug, Clone, Default)]
pub struct TtsResponse {
    pub audio: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ImageRequest {
    pub model: Option<String>,
    pub prompt: String,
    pub size: Option<String>,
    pub quality: Option<String>,
    pub style: Option<String>,
    pub n: u8,
}

#[derive(Debug, Clone, Default)]
pub struct ImageResponse {
    pub images: Vec<ImageResult>,
}

#[derive(Debug, Clone)]
pub struct VideoRequest {
    pub model: Option<String>,
    pub prompt: String,
    pub duration_secs: Option<u32>,
    pub resolution: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct VideoResponse {
    pub videos: Vec<VideoResult>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chat_response_default_is_empty() {
        let r = ChatResponse::default();
        assert!(r.content.is_none());
        assert!(r.tool_calls.is_empty());
    }

    #[test]
    fn embed_request_holds_texts() {
        let r = EmbedRequest { model: None, texts: vec!["a".into(), "b".into()] };
        assert_eq!(r.texts.len(), 2);
    }
}
```
- [ ] **Step 2: Register the module.** Add `pub mod io;` to `src/types/mod.rs`.
- [ ] **Step 3: Run tests, verify pass.** Run: `cargo test -p gateway io::`. Expected: PASS.
- [ ] **Step 4: Commit.**
```bash
git add src/types/io.rs src/types/mod.rs
git commit -m "feat(gateway): add capability-typed request/response structs"
```

### Task 1.3: Define the capability traits

**Files:**
- Create: `src/adapters/capability.rs`
- Modify: `src/adapters/mod.rs` (add `pub mod capability;` and re-exports)

- [ ] **Step 1: Write the traits.** Full content of `src/adapters/capability.rs`:
```rust
//! Capability-segregated adapter traits. Each provider implements only
//! the traits for capabilities it supports; the registry stores one map
//! per capability. See `docs/design/adapter-capability-traits.md`.

use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;

use crate::types::config::RouterConfig;
use crate::types::error::GatewayError;
use crate::types::io::{
    ChatRequest, ChatResponse, EmbedRequest, EmbedResponse, ImageRequest, ImageResponse,
    SttRequest, SttResponse, TtsRequest, TtsResponse, VideoRequest, VideoResponse,
};
use crate::types::request::StreamChunk;

/// Identity shared by every adapter regardless of capability.
pub trait Model: Send + Sync {
    fn id(&self) -> &str;
}

type ChunkStream = Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>;

#[async_trait]
pub trait ChatModel: Model {
    async fn chat(&self, cfg: &RouterConfig, req: &ChatRequest)
        -> Result<ChatResponse, GatewayError>;

    /// Opt-in streaming. Providers that stream override this; the rest
    /// inherit `Unsupported`.
    async fn chat_stream(&self, _cfg: &RouterConfig, _req: &ChatRequest)
        -> Result<ChunkStream, GatewayError> {
        Err(GatewayError::Unsupported { adapter: self.id().to_string(), what: "streaming".into() })
    }
}

#[async_trait]
pub trait EmbedModel: Model {
    async fn embed(&self, cfg: &RouterConfig, req: &EmbedRequest)
        -> Result<EmbedResponse, GatewayError>;
}

#[async_trait]
pub trait SttModel: Model {
    async fn transcribe(&self, cfg: &RouterConfig, req: &SttRequest)
        -> Result<SttResponse, GatewayError>;
}

#[async_trait]
pub trait TtsModel: Model {
    async fn speak(&self, cfg: &RouterConfig, req: &TtsRequest)
        -> Result<TtsResponse, GatewayError>;
}

#[async_trait]
pub trait ImageModel: Model {
    async fn generate_image(&self, cfg: &RouterConfig, req: &ImageRequest)
        -> Result<ImageResponse, GatewayError>;
}

#[async_trait]
pub trait VideoModel: Model {
    async fn generate_video(&self, cfg: &RouterConfig, req: &VideoRequest)
        -> Result<VideoResponse, GatewayError>;
}
```
- [ ] **Step 2: Register + re-export.** In `src/adapters/mod.rs` add near the top module list `pub mod capability;` and after it:
```rust
pub use capability::{ChatModel, EmbedModel, ImageModel, Model, SttModel, TtsModel, VideoModel};
```
- [ ] **Step 3: Verify it compiles.** Run: `cargo build -p gateway`. Expected: builds (the old `InferenceAdapter` still present and untouched).
- [ ] **Step 4: Commit.**
```bash
git add src/adapters/capability.rs src/adapters/mod.rs
git commit -m "feat(gateway): define capability-segregated adapter traits"
```

### Task 1.4: Boundary conversions (unified ↔ typed)

**Files:**
- Create: `src/dispatch.rs`
- Modify: `src/lib.rs` (add `mod dispatch;`)
- Test: `src/dispatch.rs` (inline tests)

- [ ] **Step 1: Write the failing test.** Put in `src/dispatch.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::capability::Capability;
    use crate::types::io::ChatResponse;
    use crate::types::request::{InferenceRequest, Message, MessageRole, Payload};

    fn chat_req(model: Option<&str>) -> InferenceRequest {
        InferenceRequest {
            capability: Capability::TextChat,
            model: model.map(Into::into),
            router: None,
            chain: None,
            payload: Payload::Chat {
                messages: vec![Message::text(MessageRole::User, "hi")],
                system: Some("sys".into()),
                max_tokens: Some(10),
                temperature: Some(0.5),
                tools: Vec::new(),
            },
            budget: None,
        }
    }

    #[test]
    fn to_chat_request_extracts_payload_and_model() {
        let ir = chat_req(None);
        let cr = to_chat_request(&ir, Some("m1".into())).unwrap();
        assert_eq!(cr.model.as_deref(), Some("m1"));
        assert_eq!(cr.messages.len(), 1);
        assert_eq!(cr.system.as_deref(), Some("sys"));
        assert_eq!(cr.max_tokens, Some(10));
    }

    #[test]
    fn to_chat_request_rejects_non_chat_payload() {
        let ir = InferenceRequest {
            capability: Capability::TextEmbed,
            model: None, router: None, chain: None,
            payload: Payload::Embed { texts: vec!["x".into()] },
            budget: None,
        };
        assert!(to_chat_request(&ir, None).is_err());
    }

    #[test]
    fn from_chat_response_fills_only_chat_fields() {
        let resp = ChatResponse { content: Some("hello".into()), model: Some("m1".into()), ..Default::default() };
        let ir = from_chat_response(resp);
        assert_eq!(ir.content.as_deref(), Some("hello"));
        assert!(ir.embeddings.is_none());
        assert!(ir.success);
    }
}
```
- [ ] **Step 2: Run it, verify it fails.** Run: `cargo test -p gateway dispatch::`. Expected: FAIL (functions/module missing).
- [ ] **Step 3: Implement the conversions.** At the top of `src/dispatch.rs` (above the test module):
```rust
//! Boundary translation between the public unified request/response types
//! and the capability-typed structs the segregated traits use. Keeps the
//! `Gateway::execute(InferenceRequest) -> InferenceResponse` facade stable
//! while adapters speak focused types.

use crate::types::error::GatewayError;
use crate::types::io::{
    ChatRequest, ChatResponse, EmbedRequest, EmbedResponse, ImageRequest, ImageResponse,
    SttRequest, SttResponse, TtsRequest, TtsResponse, VideoRequest, VideoResponse,
};
use crate::types::request::{InferenceRequest, InferenceResponse, Payload};

fn wrong_payload(expected: &str) -> GatewayError {
    GatewayError::ProviderError {
        adapter: "dispatch".into(),
        message: format!("expected {expected} payload for this capability"),
        status: None,
    }
}

/// Base response with all optional result fields cleared — engine fills
/// `attempts`/cost afterwards.
fn empty_response() -> InferenceResponse {
    InferenceResponse {
        success: true,
        content: None, embeddings: None, transcription: None, audio: None,
        images: None, videos: None, model: None, usage: None,
        tool_calls: Vec::new(), estimated_cost: None, actual_cost: None,
        attempts: Vec::new(),
    }
}

pub fn to_chat_request(req: &InferenceRequest, model: Option<String>) -> Result<ChatRequest, GatewayError> {
    let Payload::Chat { messages, system, max_tokens, temperature, tools } = &req.payload else {
        return Err(wrong_payload("chat"));
    };
    Ok(ChatRequest {
        model: model.or_else(|| req.model.clone()),
        messages: messages.clone(),
        system: system.clone(),
        max_tokens: *max_tokens,
        temperature: *temperature,
        tools: tools.clone(),
    })
}

pub fn from_chat_response(r: ChatResponse) -> InferenceResponse {
    InferenceResponse { content: r.content, tool_calls: r.tool_calls, usage: r.usage, model: r.model, ..empty_response() }
}

pub fn to_embed_request(req: &InferenceRequest, model: Option<String>) -> Result<EmbedRequest, GatewayError> {
    let Payload::Embed { texts } = &req.payload else { return Err(wrong_payload("embed")); };
    Ok(EmbedRequest { model: model.or_else(|| req.model.clone()), texts: texts.clone() })
}

pub fn from_embed_response(r: EmbedResponse) -> InferenceResponse {
    InferenceResponse { embeddings: Some(r.embeddings), usage: r.usage, ..empty_response() }
}

pub fn to_stt_request(req: &InferenceRequest, model: Option<String>) -> Result<SttRequest, GatewayError> {
    let Payload::Stt { audio, language, format } = &req.payload else { return Err(wrong_payload("stt")); };
    Ok(SttRequest { model: model.or_else(|| req.model.clone()), audio: audio.clone(), language: language.clone(), format: format.clone() })
}

pub fn from_stt_response(r: SttResponse) -> InferenceResponse {
    InferenceResponse { transcription: Some(r.transcription), usage: r.usage, ..empty_response() }
}

pub fn to_tts_request(req: &InferenceRequest, model: Option<String>) -> Result<TtsRequest, GatewayError> {
    let Payload::Tts { text, voice, speed, output_format } = &req.payload else { return Err(wrong_payload("tts")); };
    Ok(TtsRequest { model: model.or_else(|| req.model.clone()), text: text.clone(), voice: voice.clone(), speed: *speed, output_format: output_format.clone() })
}

pub fn from_tts_response(r: TtsResponse) -> InferenceResponse {
    InferenceResponse { audio: Some(r.audio), ..empty_response() }
}

pub fn to_image_request(req: &InferenceRequest, model: Option<String>) -> Result<ImageRequest, GatewayError> {
    let Payload::ImageGenerate { prompt, size, quality, style, n } = &req.payload else { return Err(wrong_payload("image_generate")); };
    Ok(ImageRequest { model: model.or_else(|| req.model.clone()), prompt: prompt.clone(), size: size.clone(), quality: quality.clone(), style: style.clone(), n: *n })
}

pub fn from_image_response(r: ImageResponse) -> InferenceResponse {
    InferenceResponse { images: Some(r.images), ..empty_response() }
}

pub fn to_video_request(req: &InferenceRequest, model: Option<String>) -> Result<VideoRequest, GatewayError> {
    let Payload::VideoGenerate { prompt, duration_secs, resolution } = &req.payload else { return Err(wrong_payload("video_generate")); };
    Ok(VideoRequest { model: model.or_else(|| req.model.clone()), prompt: prompt.clone(), duration_secs: *duration_secs, resolution: resolution.clone() })
}

pub fn from_video_response(r: VideoResponse) -> InferenceResponse {
    InferenceResponse { videos: Some(r.videos), ..empty_response() }
}
```
- [ ] **Step 4: Register the module.** Add `mod dispatch;` to `src/lib.rs`.
- [ ] **Step 5: Run tests, verify pass.** Run: `cargo test -p gateway dispatch::`. Expected: PASS.
- [ ] **Step 6: Commit.**
```bash
git add src/dispatch.rs src/lib.rs
git commit -m "feat(gateway): add unified<->typed boundary conversions"
```

---

## Phase 2 — Per-capability registry

### Task 2.1: New `AdapterRegistry` with per-capability maps

**Files:**
- Modify: `src/adapters/mod.rs` (replace the `AdapterRegistry` struct + impl; keep the old `InferenceAdapter` trait in place for now)
- Test: `src/adapters/mod.rs` (extend inline tests)

- [ ] **Step 1: Write the failing test.** Add to the tests module in `src/adapters/mod.rs`:
```rust
#[tokio::test]
async fn same_adapter_registers_into_multiple_capability_maps() {
    use crate::adapters::capability::{ChatModel, EmbedModel, Model};
    use crate::types::config::RouterConfig;
    use crate::types::io::{ChatRequest, ChatResponse, EmbedRequest, EmbedResponse};

    struct Dual;
    impl Model for Dual { fn id(&self) -> &str { "dual" } }
    #[async_trait::async_trait]
    impl ChatModel for Dual {
        async fn chat(&self, _c: &RouterConfig, _r: &ChatRequest) -> Result<ChatResponse, crate::types::error::GatewayError> { Ok(ChatResponse::default()) }
    }
    #[async_trait::async_trait]
    impl EmbedModel for Dual {
        async fn embed(&self, _c: &RouterConfig, _r: &EmbedRequest) -> Result<EmbedResponse, crate::types::error::GatewayError> { Ok(EmbedResponse::default()) }
    }

    let reg = AdapterRegistry::new();
    let dual = std::sync::Arc::new(Dual);
    reg.register_chat(dual.clone()).await;
    reg.register_embed(dual.clone()).await;

    assert!(reg.chat("dual").await.is_some());
    assert!(reg.embed("dual").await.is_some());
    assert!(reg.image("dual").await.is_none());
}
```
- [ ] **Step 2: Run it, verify it fails.** Run: `cargo test -p gateway same_adapter_registers`. Expected: FAIL (new methods missing).
- [ ] **Step 3: Replace the registry.** In `src/adapters/mod.rs`, replace the existing `AdapterRegistry` struct and its impl block with:
```rust
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use capability::{ChatModel, EmbedModel, ImageModel, SttModel, TtsModel, VideoModel};

/// Thread-safe registry of adapters, one map per capability. The same
/// concrete `Arc` is registered into each map it qualifies for.
#[derive(Clone, Default)]
pub struct AdapterRegistry {
    chat: Arc<RwLock<HashMap<String, Arc<dyn ChatModel>>>>,
    embed: Arc<RwLock<HashMap<String, Arc<dyn EmbedModel>>>>,
    stt: Arc<RwLock<HashMap<String, Arc<dyn SttModel>>>>,
    tts: Arc<RwLock<HashMap<String, Arc<dyn TtsModel>>>>,
    image: Arc<RwLock<HashMap<String, Arc<dyn ImageModel>>>>,
    video: Arc<RwLock<HashMap<String, Arc<dyn VideoModel>>>>,
}

macro_rules! capability_map_accessors {
    ($field:ident, $reg:ident, $get:ident, $trait:ident) => {
        pub async fn $reg(&self, a: Arc<dyn $trait>) {
            self.$field.write().await.insert(a.id().to_string(), a);
        }
        pub async fn $get(&self, id: &str) -> Option<Arc<dyn $trait>> {
            self.$field.read().await.get(id).cloned()
        }
    };
}

impl AdapterRegistry {
    pub fn new() -> Self { Self::default() }

    capability_map_accessors!(chat, register_chat, chat, ChatModel);
    capability_map_accessors!(embed, register_embed, embed, EmbedModel);
    capability_map_accessors!(stt, register_stt, stt, SttModel);
    capability_map_accessors!(tts, register_tts, tts, TtsModel);
    capability_map_accessors!(image, register_image, image, ImageModel);
    capability_map_accessors!(video, register_video, video, VideoModel);
}
```
> Note: the old `InferenceAdapter` trait and its `register/get/list/unregister` helpers are removed with the struct only if they lived on `AdapterRegistry`. Keep the `InferenceAdapter` **trait definition** for now (adapters still impl it until Phase 3/4). If existing code calls `registry.register(...)`/`registry.get(...)`, those call sites are the engine (updated in Phase 4) and tests (updated per-adapter in Phase 3) — leave a temporary `#[allow(dead_code)]` shim only if the crate won't compile; prefer updating call sites as you reach them.
- [ ] **Step 4: Make the crate compile.** The engine currently calls `self.adapters.get(...)`. To keep Phase 2 green without touching the engine yet, temporarily retain a minimal legacy path: keep the previous `HashMap<String, Arc<dyn InferenceAdapter>>`-based registry under a new name `LegacyAdapterRegistry` in the same file and have the engine keep using it until Phase 4. (If simpler, defer the engine's switch by leaving `Gateway.adapters` typed as `LegacyAdapterRegistry` until Task 4.1.)
- [ ] **Step 5: Run tests, verify pass.** Run: `cargo test -p gateway`. Expected: PASS (including the new registry test).
- [ ] **Step 6: Commit.**
```bash
git add src/adapters/mod.rs
git commit -m "feat(gateway): per-capability AdapterRegistry"
```

### Task 2.2: Ergonomic self-registration (`RegisterInto`)

Registration is public API the consumers (`sensei`, `strategos`) call. Moving from
one `register()` to six `register_<cap>()` methods would force verbose, error-prone
consumer setup. Give each adapter a single self-registration entry point so consumer
wiring stays one call per adapter.

**Files:**
- Modify: `src/adapters/mod.rs` (define the trait)
- Test: `src/adapters/mod.rs` (extend the Task 2.1 test)

- [ ] **Step 1: Define the trait** in `src/adapters/mod.rs`:
```rust
/// One-call registration: an adapter inserts itself into every capability
/// map it implements. Consumers call `Arc::new(MyAdapter::new()?).register_into(&reg).await`.
#[async_trait::async_trait]
pub trait RegisterInto: Send + Sync {
    async fn register_into(self: Arc<Self>, reg: &AdapterRegistry);
}
```
- [ ] **Step 2: Extend the test** — impl `RegisterInto` for the `Dual` test type from Task 2.1 (register into chat + embed), assert both maps resolve it after a single `register_into` call.
- [ ] **Step 3: Run tests, verify pass.** Run: `cargo test -p gateway`. Expected: PASS.
- [ ] **Step 4: Commit.**
```bash
git add src/adapters/mod.rs
git commit -m "feat(gateway): RegisterInto for one-call adapter registration"
```

> **Consumer impact (out of this repo):** `sensei` and `strategos` construct the
> `AdapterRegistry` and register adapters. Their setup code changes from
> `reg.register(adapter)` to `adapter.register_into(&reg).await` (one mechanical
> line per adapter). `Gateway::execute(InferenceRequest) -> InferenceResponse` is
> unchanged, so nothing else in the consumers moves. Track this as a follow-up in
> each consumer when the new gateway tag is cut.

---

## Phase 3 — Migrate adapters to capability traits

Migrate each adapter by **moving the logic out of its `InferenceAdapter::execute`/`stream` match arms into capability-trait methods**, using the boundary types. During this phase the old `InferenceAdapter` impl may remain and delegate to the new methods (so the legacy engine path still works) — or be left untouched until Phase 4 deletes it; choose per adapter based on what keeps the crate compiling.

Each adapter task also implements `Model`, its capability traits, and `RegisterInto` (inserting itself into exactly the maps for the traits it implements). The uniform mechanical transformation per adapter is: (1) `impl Model` returning the adapter id; (2) for each `Payload` arm in `execute`, add the matching capability-trait method with that arm's body, reading from the typed request; (3) if the adapter streams, move `stream`'s body into `chat_stream`; (4) rename any adapter-local wire struct whose name collides with an `io::*` type; (5) `impl RegisterInto`; (6) port each existing `execute`/`stream` test to call the typed method (keep the old test until Phase 4).

**Exemplar (do this one fully first): `ollama` — `ChatModel` + `EmbedModel`.**

### Task 3.1: Migrate `ollama` (exemplar)

**Files:**
- Modify: `src/adapters/ollama.rs`
- Test: `src/adapters/ollama.rs` (inline tests)

- [ ] **Step 1: Add the trait impls** to `src/adapters/ollama.rs` (keep the existing wire types + helpers; move the request-building/response-parsing bodies from `execute`'s `Payload::Chat`/`Payload::Embed` arms into these methods). Implement `Model`, then:
```rust
use crate::adapters::capability::{ChatModel, EmbedModel, Model};
use crate::types::io::{ChatRequest, ChatResponse, EmbedRequest, EmbedResponse};

impl Model for OllamaAdapter { fn id(&self) -> &str { "ollama" } }

#[async_trait]
impl ChatModel for OllamaAdapter {
    async fn chat(&self, config: &RouterConfig, req: &ChatRequest) -> Result<ChatResponse, GatewayError> {
        let api_key = resolve_api_key(config);
        let model = req.model.clone().unwrap_or_else(|| DEFAULT_MODEL.to_string());
        let body = ChatCompletionRequest {
            model: model.clone(),
            messages: build_chat_messages(&req.messages, &req.system),
            max_tokens: req.max_tokens,
            temperature: req.temperature,
            stream: false,
        };
        let resp: ChatCompletionResponse = http_json(&self.client, &config.url, "/v1/chat/completions", &body, api_key.as_deref(), &config.headers).await?;
        Ok(ChatResponse {
            content: resp.choices.first().and_then(|c| c.message.content.clone()),
            tool_calls: Vec::new(),
            usage: usage_from_response(&resp.usage),
            model: Some(model),
        })
    }

    async fn chat_stream(&self, config: &RouterConfig, req: &ChatRequest)
        -> Result<std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError> {
        // PORT (Step 2): move the current `OllamaAdapter::stream` body here
        // verbatim, sourcing fields from `req` (`req.messages`, `req.system`,
        // `req.max_tokens`, `req.temperature`, `req.model`) instead of
        // destructuring `request.payload`. No logic change.
    }
}

#[async_trait]
impl EmbedModel for OllamaAdapter {
    async fn embed(&self, config: &RouterConfig, req: &EmbedRequest) -> Result<EmbedResponse, GatewayError> {
        let api_key = resolve_api_key(config);
        let model = req.model.clone().unwrap_or_else(|| DEFAULT_MODEL.to_string());
        let body = EmbedRequest_ { model: model.clone(), input: req.texts.clone() };
        let resp: EmbedResponse_ = http_json(&self.client, &config.url, "/v1/embeddings", &body, api_key.as_deref(), &config.headers).await?;
        Ok(EmbedResponse { embeddings: resp.data.into_iter().map(|d| d.embedding).collect(), usage: usage_from_response(&resp.usage) })
    }
}
```
> The adapter's own wire structs `EmbedRequest`/`EmbedResponse` collide by name with the new `io::EmbedRequest`/`io::EmbedResponse`. Rename the adapter-local wire structs (e.g. suffix `_` as shown, or `OllamaEmbedRequest`) to disambiguate. Do the same for any other adapter whose local wire type name collides with an `io::*` name.
- [ ] **Step 2: Port the streaming body.** Replace the `unimplemented!` with the exact body of the current `OllamaAdapter::stream`, reading `req.messages`/`req.system`/`req.max_tokens`/`req.temperature`/`req.model` instead of destructuring `request.payload`.
- [ ] **Step 3: Implement `RegisterInto`.** Add:
```rust
use crate::adapters::{AdapterRegistry, RegisterInto};

#[async_trait]
impl RegisterInto for OllamaAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &AdapterRegistry) {
        reg.register_chat(self.clone()).await;
        reg.register_embed(self).await;
    }
}
```
- [ ] **Step 4: Add trait-level tests.** Add:
```rust
#[tokio::test]
async fn ollama_chatmodel_times_out_against_silent_server() {
    // mirror the existing `execute_times_out_against_a_silent_server` test
    // but call `ChatModel::chat` / `EmbedModel::embed` instead of `execute`.
    // (reuse the same TcpListener setup; assert Err returned promptly)
}
```
Port the existing `execute_times_out_against_a_silent_server` to call `embed(...)`; keep the old test too until Phase 4.
- [ ] **Step 5: Run tests, verify pass.** Run: `cargo test -p gateway ollama`. Expected: PASS.
- [ ] **Step 6: Commit.**
```bash
git add src/adapters/ollama.rs
git commit -m "refactor(gateway): migrate ollama to ChatModel + EmbedModel"
```

### Tasks 3.2–3.16: Migrate the remaining cloud adapters

For each adapter below, repeat the Task 3.1 pattern: implement `Model` + the listed traits, moving each `execute` payload-arm body into the matching typed method (reading from the typed request), moving `stream` into `chat_stream` where the adapter streams, renaming any local wire struct that collides with an `io::*` name, and porting its tests to call the typed methods. Commit per adapter with `refactor(gateway): migrate <name> to <traits>`.

| Task | Adapter file | Traits | Streams? | Notes |
|---|---|---|---|---|
| 3.2 | `anthropic.rs` | `ChatModel` | yes | port `stream()` → `chat_stream`; tool-calling stays in `chat` |
| 3.3 | `openai.rs` | `ChatModel`, `EmbedModel`, `SttModel`, `TtsModel`, `ImageModel` | yes (chat) | largest; each `Payload` arm → its trait method; multimodal + tools stay in `chat` |
| 3.4 | `gemini.rs` | `ChatModel`, `EmbedModel` | check current `stream()` | |
| 3.5 | `bedrock.rs` | `ChatModel`, `EmbedModel` | check | |
| 3.6 | `grok.rs` | `ChatModel`, `SttModel`, `TtsModel` | yes (chat) | audio arms → `transcribe`/`speak` |
| 3.7 | `together.rs` | `ChatModel`, `ImageModel` | yes (chat) | |
| 3.8 | `flux.rs` | `ImageModel` | no | |
| 3.9 | `recraft.rs` | `ImageModel` | no | |
| 3.10 | `stability.rs` | `ImageModel` | no | |
| 3.11 | `fal.rs` | `ImageModel`, `VideoModel` | no | uses `async_job` polling — unchanged, called from typed methods |
| 3.12 | `replicate.rs` | `ImageModel`, `VideoModel` | no | uses `async_job` |
| 3.13 | `kling.rs` | `VideoModel` | no | uses `async_job` |
| 3.14 | `luma.rs` | `VideoModel` | no | uses `async_job` |
| 3.15 | `runway.rs` | `VideoModel` | no | uses `async_job` |
| 3.16 | `noop.rs` | `ChatModel`, `EmbedModel`, `SttModel`, `TtsModel`, `ImageModel`, `VideoModel` | no | catch-all; each method returns the same "no provider" style response the current `execute` returns |

- [ ] Complete Tasks 3.2–3.16, tests green + commit after each.

### Tasks 3.17–3.20: Migrate `gateway-embedded` adapters

Same pattern, in `crates/gateway-embedded/src/adapters/`. These bodies are behind cargo features — verify with the feature enabled: `cargo test -p gateway-embedded --features <feat>` (`llama-cpp` for `llama_cpp`/`embedded_llama`, `fastembed` for `fastembed`, `ort` for `ort`). Also run `cargo build -p gateway-embedded` (no features) to confirm the non-gated code compiles.

| Task | Adapter file | Traits | Feature |
|---|---|---|---|
| 3.17 | `llama_cpp.rs` | `ChatModel`, `EmbedModel` | `llama-cpp` |
| 3.18 | `embedded_llama.rs` | `ChatModel`, `EmbedModel` | `llama-cpp` |
| 3.19 | `fastembed.rs` | `EmbedModel` | `fastembed` |
| 3.20 | `ort.rs` | `EmbedModel` | `ort` |

- [ ] Complete Tasks 3.17–3.20, tests green (with the feature) + commit after each.

---

## Phase 4 — Switch engine, delete the old trait

### Task 4.1: Engine dispatches via capability maps

**Files:**
- Modify: `src/engine.rs`
- Modify: `src/lib.rs` and any constructor wiring that builds `AdapterRegistry`
- Test: `src/engine.rs` (update inline tests to register via `register_chat` etc.)

- [ ] **Step 1: Update the candidate loop.** In `Gateway::execute`, replace the `self.adapters.get(&candidate.router)` + `adapter.execute(...)` block with capability dispatch. The model-injection logic (inject `candidate.api_model_id` when the caller didn't pin `request.model`) moves into the `to_*_request(request, model)` call:
```rust
use crate::dispatch::*;

let model = if request.model.is_some() { request.model.clone() } else { Some(candidate.api_model_id.clone()) };
let cfg = &candidate.router_config;
let outcome: Result<InferenceResponse, GatewayError> = match request.capability {
    Capability::TextChat | Capability::TextComplete => match self.adapters.chat(&candidate.router).await {
        Some(m) => match to_chat_request(request, model) { Ok(r) => m.chat(cfg, &r).await.map(from_chat_response), Err(e) => Err(e) },
        None => Err(GatewayError::ProviderError { adapter: candidate.router.clone(), message: format!("no chat adapter for router '{}'", candidate.router), status: None }),
    },
    Capability::TextEmbed => match self.adapters.embed(&candidate.router).await {
        Some(m) => match to_embed_request(request, model) { Ok(r) => m.embed(cfg, &r).await.map(from_embed_response), Err(e) => Err(e) },
        None => Err(GatewayError::ProviderError { adapter: candidate.router.clone(), message: format!("no embed adapter for router '{}'", candidate.router), status: None }),
    },
    Capability::AudioTranscribe => match self.adapters.stt(&candidate.router).await {
        Some(m) => match to_stt_request(request, model) { Ok(r) => m.transcribe(cfg, &r).await.map(from_stt_response), Err(e) => Err(e) },
        None => Err(GatewayError::ProviderError { adapter: candidate.router.clone(), message: format!("no stt adapter for router '{}'", candidate.router), status: None }),
    },
    Capability::AudioGenerate => match self.adapters.tts(&candidate.router).await {
        Some(m) => match to_tts_request(request, model) { Ok(r) => m.speak(cfg, &r).await.map(from_tts_response), Err(e) => Err(e) },
        None => Err(GatewayError::ProviderError { adapter: candidate.router.clone(), message: format!("no tts adapter for router '{}'", candidate.router), status: None }),
    },
    Capability::ImageGenerate => match self.adapters.image(&candidate.router).await {
        Some(m) => match to_image_request(request, model) { Ok(r) => m.generate_image(cfg, &r).await.map(from_image_response), Err(e) => Err(e) },
        None => Err(GatewayError::ProviderError { adapter: candidate.router.clone(), message: format!("no image adapter for router '{}'", candidate.router), status: None }),
    },
    Capability::VideoGenerate => match self.adapters.video(&candidate.router).await {
        Some(m) => match to_video_request(request, model) { Ok(r) => m.generate_video(cfg, &r).await.map(from_video_response), Err(e) => Err(e) },
        None => Err(GatewayError::ProviderError { adapter: candidate.router.clone(), message: format!("no video adapter for router '{}'", candidate.router), status: None }),
    },
    other => Err(GatewayError::NoCandidates { capability: other.clone() }),
};
```
Keep the surrounding attempt-recording, circuit-breaker calls, `response.model`/`response.attempts` assignment, and fallback/break logic exactly as they are — `outcome` replaces the old `adapter.execute(...)` result.
- [ ] **Step 2: Update engine tests.** The inline test adapters (`RecordingAdapter`, `FailingAdapter`, `NoopAdapter` usage) currently impl `InferenceAdapter`. Reimplement them as `ChatModel` (+`Model`) and register with `register_chat`. Keep every assertion (fallback on provider error, stop on auth error, adapter-not-found, chain injects `api_model_id`, records attempts) — they must still pass against the typed path.
- [ ] **Step 3: Run tests, verify pass.** Run: `cargo test -p gateway`. Expected: PASS.
- [ ] **Step 4: Commit.**
```bash
git add src/engine.rs src/lib.rs
git commit -m "refactor(gateway): engine dispatches via capability maps"
```

### Task 4.2: Delete `InferenceAdapter`, `supports()`, and legacy registry

**Files:**
- Modify: `src/adapters/mod.rs` (delete the `InferenceAdapter` trait + `LegacyAdapterRegistry`)
- Modify: every `src/adapters/*.rs` and `crates/gateway-embedded/src/adapters/*.rs` (delete the old `impl InferenceAdapter`, `execute`, `stream`, `supports`, `from_config`-only-for-old-path if unused)
- Modify: any consumer wiring in this repo that referenced `InferenceAdapter`

- [ ] **Step 1: Delete the trait + legacy registry** from `src/adapters/mod.rs`.
- [ ] **Step 2: Remove old impls.** In each adapter file, delete the `impl InferenceAdapter for X` block and any now-unused imports. The wire types + helpers + typed-trait impls remain.
- [ ] **Step 3: Fix fallout.** Run `cargo build -p gateway && cargo build -p gateway-embedded` and resolve every reference to the removed symbols.
- [ ] **Step 4: Grep clean.** Run: `grep -rn "InferenceAdapter\|fn supports(" crates/ ; echo "exit: $?"`. Expected: no matches (exit 1 from grep).
- [ ] **Step 5: Full gate.** Run:
  - `cargo test -p gateway`
  - `cargo test -p gateway-embedded`
  - `cargo test -p gateway-embedded --features fastembed` (and `--features ort`, `--features llama-cpp` if the toolchain has the native deps; otherwise note skipped)
  - `cargo clippy --workspace -- -D warnings`
  Expected: all PASS / clean.
- [ ] **Step 6: Commit.**
```bash
git add -A
git commit -m "refactor(gateway): remove fat InferenceAdapter trait and supports()"
```

### Task 4.3: Sync the capabilities feature doc

**Files:**
- Modify: `docs/features/capabilities-and-adapters.md`, `docs/features/README.md` (matrix)

- [ ] **Step 1:** Update the capabilities doc + matrix to describe the shipped capability-trait model (traits, per-capability registry, how to add an adapter), matching the final code.
- [ ] **Step 2: Commit.**
```bash
git add docs/features
git commit -m "docs(features): sync capabilities page with shipped trait model"
```

---

## Self-review notes (for the executor)

- **Spec coverage:** every design-doc section maps to a task — traits (1.3), typed I/O 4b (1.2/1.4), registry (2.1), engine dispatch (4.1), migration across both crates (3.x), deletion of the fat trait (4.2), docs incl. features folder (Phase 0, 4.3).
- **Type consistency:** method names are fixed — `chat`/`chat_stream`, `embed`, `transcribe`, `speak`, `generate_image`, `generate_video`; registry `register_<cap>`/`<cap>`; conversions `to_<cap>_request`/`from_<cap>_response`. Use these exact names throughout.
- **Known pitfalls:** (1) adapter-local wire structs named `EmbedRequest`/`EmbedResponse`/`ImageResult` collide with `io::*` — rename the local ones. (2) `gateway-embedded` bodies are feature-gated; build with the feature to actually compile them. (3) keep engine cost/attempt/circuit-breaker logic byte-for-byte; only the adapter-call site changes.
```
