# Writing a custom adapter

Add a provider the crate doesn't ship. An adapter implements `Model` (its id) + the
capability trait(s) it supports + `RegisterInto` (how it lands in the registry).
It receives **typed** capability requests/responses; the gateway translates at the
boundary, so the public `execute()` facade never changes.

## Minimal chat adapter

```rust
use std::pin::Pin;
use std::sync::Arc;
use async_trait::async_trait;
use futures::Stream;
use gateway::adapters::{AdapterRegistry, RegisterInto, Model, ChatModel};
use gateway::types::config::RouterConfig;
use gateway::types::error::GatewayError;
use gateway::types::io::{ChatRequest, ChatResponse};
use gateway::types::request::StreamChunk;

pub struct MyAdapter { client: reqwest::Client }

impl Model for MyAdapter {
    fn id(&self) -> &str { "myprovider" }   // MUST equal the router key it's configured under
}

#[async_trait]
impl ChatModel for MyAdapter {
    async fn chat(&self, config: &RouterConfig, req: &ChatRequest)
        -> Result<ChatResponse, GatewayError>
    {
        // 1. Resolve creds/endpoint from `config` (config.url, and the key via
        //    gateway::adapters::base::resolve_api_key(config)).
        // 2. Translate `req` (model, messages, system, max_tokens, temperature, tools)
        //    into your provider's wire format and POST it.
        // 3. Map the provider response back into ChatResponse.
        Ok(ChatResponse {
            content: Some("…".into()),
            tool_calls: Vec::new(),
            usage: None,                    // Some(TokenUsage {..}) drives cost + quota
            model: req.model.clone(),
            degraded: false,                // true ⇒ response.success = false
        })
    }

    async fn chat_stream(&self, config: &RouterConfig, req: &ChatRequest)
        -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>
    {
        // Return a stream of StreamChunk { content, usage }. If you can't stream,
        // return GatewayError::Unsupported.
        todo!()
    }
}

#[async_trait]
impl RegisterInto for MyAdapter {
    async fn register_into(self: Arc<Self>, reg: &AdapterRegistry) {
        reg.register_chat(self).await;      // + register_embed(self.clone()) if you impl EmbedModel
    }
}
```

Register + use it exactly like a built-in:

```rust
adapters.register(Arc::new(MyAdapter { client: reqwest::Client::new() })).await;
// config must have a router keyed "myprovider" and a model whose provider = "myprovider"
```

## The capability traits

Implement only what your provider does; register into each:

| Trait | Method(s) | Request / Response |
|---|---|---|
| `ChatModel` | `chat`, `chat_stream` | `ChatRequest` / `ChatResponse` |
| `EmbedModel` | `embed` | `EmbedRequest` / `EmbedResponse` |
| `SttModel` | `transcribe` | `SttRequest` / `SttResponse` |
| `TtsModel` | `speak` | `TtsRequest` / `TtsResponse` |
| `ImageModel` | `generate_image` | `ImageRequest` / `ImageResponse` |
| `VideoModel` | `generate_video` | `VideoRequest` / `VideoResponse` |

A chat+embed adapter implements both and registers into both maps from its single
`register_into` — one `Arc`, two capabilities.

## Reuse the OpenAI-compatible core

If your provider speaks the OpenAI wire format (many do), don't hand-roll it —
delegate to the shared `adapters::openai_compat` core, exactly like the built-in
`ollama` / `grok` / `together` / `huggingface` adapters:

```rust
async fn chat(&self, config: &RouterConfig, req: &ChatRequest) -> Result<ChatResponse, GatewayError> {
    openai_compat::chat(&self.client, base_url(config), DEFAULT_MODEL, config, req).await
}
```

That gives you chat + tools + multimodal + streaming + embeddings for free. Read
`crates/gateway/src/adapters/huggingface.rs` as a ~100-line template.

## Rules

- `id()` is fixed and must match the router key — that's how the engine finds you.
- Don't bake in models/endpoints/versions; read them from `RouterConfig` (`url`,
  `headers`) and the request. Constants are fallback-only.
- Fail fast: return `GatewayError::Authentication` before any network I/O when a
  required key is missing.
