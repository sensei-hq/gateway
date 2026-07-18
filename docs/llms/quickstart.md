# Quickstart

A minimal end-to-end chat call, then embeddings. Assumes `OPENAI_API_KEY` is set
in the environment.

## Dependencies

```toml
gateway = { git = "https://github.com/sensei-hq/gateway", tag = "v0.3.0" }
tokio   = { version = "1", features = ["full"] }
```

## Chat

```rust
use std::sync::Arc;
use std::time::Duration;
use gateway::{Gateway, GatewayBuilder, Capability};
use gateway::adapters::AdapterRegistry;
use gateway::adapters::openai::OpenAIAdapter;
use gateway::circuit_breaker::{CircuitBreakerManager, CircuitBreakerConfig};
use gateway::types::config::{RouterConfig, ModelConfig};
use gateway::types::request::{InferenceRequest, Payload, Message, MessageRole};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Config — one router (endpoint + credentials), one model.
    let config = GatewayBuilder::new()
        .add_router("openai", RouterConfig {
            url: "https://api.openai.com/v1".into(),
            api_key_env: Some("OPENAI_API_KEY".into()), // resolved from env at call time
            api_key: None,                              // or inject a literal key here
            enabled: true,
            timeout_ms: Some(30_000),
            headers: Default::default(),
        })
        .add_model(ModelConfig {
            id: "gpt-4o".into(),
            api_model_id: Some("gpt-4o".into()),        // the id sent on the wire
            provider: "openai".into(),                  // must match a router key
            capabilities: vec![Capability::TextChat],
            context_window: 128_000,
            max_output_tokens: 4096,
            pricing: None,                              // add ModelPricing to get cost figures
        })
        .build()
        .map_err(|errs| errs.join("; "))?;

    // 2. Adapters — register OpenAI under id "openai" (matches the router key).
    let adapters = AdapterRegistry::new();
    adapters.register(Arc::new(OpenAIAdapter::new()?)).await;

    // 3. Circuit breaker + 4. gateway.
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig {
        threshold: 5,
        timeout: Duration::from_secs(300),
        half_open_max_requests: 3,
    });
    let gateway = Gateway::new(config, adapters, cb);

    // Send a chat request. `model: Some(..)` pins the model directly.
    let req = InferenceRequest {
        capability: Capability::TextChat,
        model: Some("gpt-4o".into()),
        router: None,
        chain: None,
        payload: Payload::Chat {
            messages: vec![Message::text(MessageRole::User, "Say hello in one sentence.")],
            system: None,
            max_tokens: Some(64),
            temperature: Some(0.7),
            tools: Vec::new(),
        },
        budget: None,
        auth: None,
    };

    let resp = gateway.execute(&req).await?;
    println!("{}", resp.content.unwrap_or_default());
    Ok(())
}
```

## Embeddings

Register an embed-capable adapter (OpenAI covers both), add a model with
`Capability::TextEmbed`, then:

```rust
let req = InferenceRequest {
    capability: Capability::TextEmbed,
    model: Some("text-embedding-3-small".into()),
    router: None, chain: None,
    payload: Payload::Embed { texts: vec!["hello".into(), "world".into()] },
    budget: None, auth: None,
};
let resp = gateway.execute(&req).await?;
let vectors: Vec<Vec<f32>> = resp.embeddings.unwrap_or_default();
```

## Reading the response

`InferenceResponse` has one known shape for every capability — read the field for
what you asked for:

| Capability | Field |
|---|---|
| `TextChat` / `TextComplete` | `content: Option<String>`, `tool_calls: Vec<ToolCall>` |
| `TextEmbed` | `embeddings: Option<Vec<Vec<f32>>>` |
| `AudioTranscribe` | `transcription: Option<String>` |
| `AudioGenerate` | `audio: Option<Vec<u8>>` |
| `ImageGenerate` | `images: Option<Vec<ImageResult>>` |
| `VideoGenerate` | `videos: Option<Vec<VideoResult>>` |

Always present: `success: bool`, `model: Option<String>`, `usage: Option<TokenUsage>`,
`estimated_cost` / `actual_cost`, `attempts: Vec<Attempt>` (the fallback trail).

## Errors

`execute` returns `Result<InferenceResponse, GatewayError>`. Common variants:
`NotConfigured`, `NoCandidates`, `Authentication`, `RateLimit`, `Timeout`,
`ProviderError`, `BudgetExceeded`, `QuotaExceeded`, `AllAttemptsFailed { attempts_detail }`
(carries every attempt's error). Prefer `Gateway::try_new(..)` over `new` to validate
config up front.

Next: [configuration](configuration.md) for chains + pricing, [recipes](recipes.md)
for streaming/cost/quotas.
