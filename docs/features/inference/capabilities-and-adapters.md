---
title: Capabilities & Adapters
doctype: feature
module: inference
status: implemented
source: crates/kernel/src/adapters/capability.rs
---

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

`crates/kernel/src/types/capability.rs` declares **12** capabilities across five
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
| Decision | `Decision`     | state + typed questions (+ images) → probabilities, not text (System One, gh#72) |

Only **7** of these have a corresponding `Payload` variant today
(`crates/kernel/src/types/request.rs`): `Chat`, `Embed`, `Stt`, `Tts`,
`ImageGenerate`, `VideoGenerate`, `Decision`. The other **5** are **reserved / future**:

- `TextComplete` **folds into `ChatModel`** — a single prompt is a one-message
  chat, so no distinct completion path is built unless a provider ever needs one.
- `TextRerank`, `TextModerate`, `ImageEdit`, `ImageAnalyze` have no `Payload`, no
  typed request/response, and no capability trait yet. They are documented as
  reserved future traits (`RerankModel`, `ModerateModel`, `ImageEditModel`,
  `ImageAnalyzeModel`), not implemented.

So the model builds exactly **7 capability traits** — the ones backed by a
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
#[async_trait] trait DecisionModel: Model { async fn decide(&self, cfg: &RouterConfig, req: &DecisionRequest) -> Result<DecisionResponse, GatewayError>; }
```

Notes:

- `DecisionModel` is non-streaming by contract — there is no `decide_stream`, and
  `execute_stream` refuses every non-chat capability with `Unsupported`.
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
| `Decision`        | `Decision`        | `DecisionModel` | `DecisionRequest` | `DecisionResponse` | `decisions`, `usage` |

The typed request/response structs mirror each `Payload` variant — e.g.
`ChatRequest { model, messages, system, max_tokens, temperature, tools }` /
`ChatResponse { content, tool_calls, usage, model }`; `EmbedRequest { model, texts }`
/ `EmbedResponse { embeddings, usage }`; `DecisionRequest { model, state, questions,
images, keep_alive }` / `DecisionResponse { answers, usage, model, degraded }`; and so
on for STT, TTS, image, and video.

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
    decision: HashMap<String, Arc<dyn DecisionModel>>,
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
    Capability::Decision  => { /* self.adapters.decision(router) → to_decision_request → decide → from_decision_response */ }
    // … one arm per capability; `TextComplete` shares the `TextChat` arm
}
```

The real match lives in `crates/gateway/src/engine/dispatch.rs` and is exhaustive
(no `_` arm), so a new `Capability` variant is a compile error until it is routed.
`TextComplete` dispatches through the same arm as `TextChat` (to `ChatModel`); the four
reserved capabilities return `GatewayError::Unsupported`.

Fallback-chain walking, circuit-breaker record-success/failure, attempt tracing,
and budget filtering are **unchanged** — they already key off `Capability`.

Decision-specific engine behaviour:

- **Validation before selection.** `execute` runs `validate_decision` on a
  `Payload::Decision` before any candidate is selected. A structural violation — no
  questions or more than `MAX_DECISION_QUESTIONS` (64), a `choice`/`score` question
  with fewer than `MIN_DECISION_CRITERIA` (2) criteria, a blank `state`, question
  name, `instructions` or choice option key — returns
  `GatewayError::InvalidRequest { message }`. It is never a fallback trigger and not
  retryable: every candidate would reject it identically. Per-provider *upper* bounds
  (e.g. Ollama's 26 options) are left to the provider's own 400 — a `ProviderError`,
  which falls back when the chain lists that trigger.
- **Context-window sizing.** Both input-token estimators count the shared `state`
  plus **every** question (name, instructions, criteria), so the `ContextWindowGate`
  skips a decision model whose `context_window` is too short for the whole question
  set. Each decision image is priced like a chat image attachment
  (`MAX_TOKENS_PER_ATTACHMENT`).
- **No streaming.** `execute_stream` returns `Unsupported` for `Decision`.
  Panels fan out through `execute`, so they get the same validation and gating.

---

## Capability × provider matrix

Rows are the cloud adapters (`crates/cloud-providers/src/`) plus the 5 embedded
adapters (`crates/local-providers/src/adapters/`). A ✓ means the adapter
implements that capability trait and is registered into that map.

| Adapter | Chat | Embed | STT | TTS | Image | Video | Decision |
|---------|:----:|:-----:|:---:|:---:|:-----:|:-----:|:--------:|
| **Cloud** | | | | | | | |
| `anthropic`  | ✓ |   |   |   |   |   |   |
| `openai`     | ✓ | ✓ | ✓ | ✓ | ✓ |   |   |
| `openrouter` | ✓ | ✓ | ✓ | ✓ | ✓ |   | ✓ |
| `typesafe`   |   |   |   |   |   |   | ✓ |
| `gemini`     | ✓ | ✓ |   |   |   |   |   |
| `huggingface` | ✓ | ✓ |   |   |   |   |   |
| `bedrock`    | ✓ | ✓ |   |   |   |   |   |
| `ollama`     | ✓ | ✓ |   |   |   |   | ✓ |
| `together`   | ✓ |   |   |   | ✓ |   |   |
| `grok`       | ✓ |   | ✓ | ✓ |   |   |   |
| `flux`       |   |   |   |   | ✓ |   |   |
| `recraft`    |   |   |   |   | ✓ |   |   |
| `stability`  |   |   |   |   | ✓ |   |   |
| `fal`        |   |   |   |   | ✓ | ✓ |   |
| `replicate`  |   |   |   |   | ✓ | ✓ |   |
| `kling`      |   |   |   |   |   | ✓ |   |
| `luma`       |   |   |   |   |   | ✓ |   |
| `runway`     |   |   |   |   |   | ✓ |   |
| `noop`       | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Embedded** | | | | | | | |
| `llama_cpp`       | ✓ | ✓ |   |   |   |   |   |
| `embedded_llama`  | ✓ | ✓ |   |   |   |   |   |
| `fastembed`       |   | ✓ |   |   |   |   |   |
| `ort`             |   | ✓ |   |   |   |   |   |
| `kokoro`          |   |   |   | ✓ |   |   |   |

`openrouter` is two adapters under one router id: `OpenAIAdapter` (id `openrouter`,
so it registers OpenAI's fixed chat/embed/STT/TTS/image set — whether OpenRouter
actually serves each of those is up to OpenRouter) plus a decision-only
`SystemOneAdapter`. `typesafe` is a decision-only `SystemOneAdapter`. Both are
auto-registered by the facade when their router id is in config.

`noop` is the catch-all test/dev adapter — it claims all seven capabilities (its
decision reply is an empty, `degraded` answer set).
`base` and `async_job` under `crates/gateway/src/adapters/` are shared helpers, not
providers, and implement no capability trait.

---

## Decision (System One)

A decision call sends one shared `state` plus 1–64 named, typed questions and gets
back **probabilities**, not text. The wire format is TypeSafe's System One API
(`POST {base}/v1/systemone`), served by Ollama (≥ 0.35), OpenRouter and TypeSafe.
Types live in `crates/kernel/src/types/decision.rs` (`gateway::types::decision`);
`DecisionQuestions` / `DecisionAnswers` are `IndexMap`s because order is semantic —
choice ties follow option order, score levels are lowest-first, and answers come back
in request order.

| Question | Criteria | Answer |
|---|---|---|
| `Choice { instructions, criteria }` | `IndexMap<option key, Option<description>>`, ≥ 2 | `Choice { choice, probabilities, confidence }` |
| `Noul { instructions, criteria }` | optional `NoulCriteria { false, true }` descriptions | `Noul { noul }` |
| `Score { instructions, criteria }` | `Vec<level description>`, lowest first, ≥ 2 | `Score { score, legend, probabilities, confidence }` |

```rust
use gateway::Capability;
use gateway::types::decision::{DecisionAnswer, DecisionQuestion, DecisionQuestions};
use gateway::types::request::{InferenceRequest, Payload};
use serde_json::json;

let questions = DecisionQuestions::from([
    ("intent".to_string(), DecisionQuestion::Choice {
        instructions: json!("What does the customer want?"),
        criteria: [("refund".to_string(), None),
                   ("exchange".to_string(), Some("Swap for another item".to_string()))]
            .into_iter().collect(),
    }),
    ("angry".to_string(), DecisionQuestion::Noul {
        instructions: json!("Is the customer angry?"),
        criteria: None,
    }),
    ("urgency".to_string(), DecisionQuestion::Score {
        instructions: json!("How urgent is this?"),
        criteria: vec!["low".into(), "medium".into(), "high".into()],
    }),
]);

let req = InferenceRequest {
    capability: Capability::Decision,
    model: None, router: None,
    chain: Some("decide".into()),            // a chain of Decision-capable models
    payload: Payload::Decision {
        state: json!("I was charged twice and want my money back today."),
        questions,
        images: vec![],                       // bare base64; vision decision models only
        keep_alive: None,                     // Ollama only, e.g. json!("5m")
    },
    budget: None, auth: None, panel: None, consensus: None,
    allow_fallback: true,
    credentials: Default::default(),
    routing: None,
};

let resp = gateway.execute(&req).await?;     // InvalidRequest if structurally malformed
for (name, answer) in resp.decisions.unwrap_or_default() {
    match answer {
        DecisionAnswer::Choice { choice, probabilities, confidence } => { /* … */ }
        DecisionAnswer::Noul { noul } => { /* P(true), 0–1 */ }
        DecisionAnswer::Score { score, legend, .. } => { /* 0..=levels-1 */ }
    }
}
```

Read the numbers for what they are:

- **`confidence` measures how concentrated the probability mass is on one answer,
  NOT whether that answer is correct.** It is uncalibrated: a confident answer can be
  wrong. Never present it to a user as accuracy, and never gate on it as if it were.
- **`noul` is a probability that the condition is true** (0–1), not a bool.
- **`score` is the probability-weighted mean of the zero-based level indices**, from
  `0` to `levels − 1` — not rounded and not normalized to 0–1. `legend` maps each
  index (as a string) to its description.

Routers:

- **`ollama`** — local, keyless; default decision model `nimble` (used only by direct
  adapter callers — through the engine the candidate's `api_model_id` is sent). Local
  models only: Ollama rejects cloud models on this endpoint with a 400 (a
  `ProviderError`, so it falls back when the chain lists that trigger). Its two
  404s are reworded: a JSON not-found means the model is not pulled
  (``run `ollama pull <model>` ``); a bare `404 page not found` means the server
  predates System One (needs Ollama ≥ 0.35). `OllamaAdapter::probe_decision_model(cfg,
  model)` reports `DecisionModelStatus::{Ready, ServerTooOld { version }, NotPulled,
  NotADecisionModel}` from `/api/version` and `/api/show`'s `capabilities`.
  (`DecisionModelStatus` and `SystemOneAdapter` live in `cloud_providers::systemone`,
  re-exported as `gateway::adapters::systemone` under the `cloud` feature.)
- **`openrouter`**, **`typesafe`** — `SystemOneAdapter` (bearer auth, decision-only).
  Their base URL defaults to `https://openrouter.ai/api` / `https://api.typesafe.ai`
  when `RouterConfig.url` is empty; any other id
  (`SystemOneAdapter::with_id` / `from_config_with_id`) needs a `url`
  (`InvalidConfig` otherwise). Config validation (`GatewayBuilder::build`,
  `Gateway::try_new`) rejects an empty router `url` anyway, so in practice set it —
  `https://openrouter.ai/api` serves both OpenRouter's chat and `/v1/systemone`.
  There is no default model: a call with no model is `InvalidRequest`.

Cloudflare Workers AI is **not** supported (different image shape; it truncates input).

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

## Scenarios

```gherkin
Feature: Capabilities & adapters
  Scenario: An adapter registers only into supported capability maps
    Given an adapter implementing only ChatModel
    Then registry.supports(Chat) is true and supports(Embed) is false
  Scenario: An unsupported capability returns Unsupported
    Given a chat-only adapter receives an embed request
    Then it returns GatewayError::Unsupported
```
