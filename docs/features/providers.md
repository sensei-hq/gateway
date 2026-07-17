# Provider adapters

The gateway routes every inference request through an **adapter**: a type that
implements the [`InferenceAdapter`](../../crates/gateway/src/adapters/mod.rs)
trait (`crates/gateway/src/adapters/mod.rs`). An adapter's job is to translate
the gateway's *unified* request types (`InferenceRequest` / `Payload::*`) into a
specific provider's wire format, issue the HTTP (or SDK) call, and translate the
response back into a unified `InferenceResponse` / `StreamChunk`.

The trait surface is small:

```rust
pub trait InferenceAdapter: Send + Sync {
    fn id(&self) -> &str;                       // registry key
    fn supports(&self, capability: &Capability) -> bool;
    async fn execute(&self, config, request) -> Result<InferenceResponse, _>;
    async fn stream(&self, config, request)  -> Result<…Stream…, _>;
}
```

Adapters are registered in an `AdapterRegistry` keyed by `id()`. The gateway
engine dispatches by **router id**, so an adapter's `id()` must match the
`RouterConfig` key that carries its URL, key, and headers.

## Shared helpers (not adapters)

- **`base.rs`** — `build_client` (reqwest client honouring `timeout_ms`),
  `resolve_api_key` (see below), and `http_json` (POST JSON, map 429 →
  `RateLimit`, 401/403 → `Authentication`, extract provider error messages).
- **`async_job.rs`** — `poll_until_complete(JobConfig, check_status)`: the
  polling loop used by every asynchronous media adapter. Default `JobConfig` is
  a 3 s poll interval and a 300 s (5 min) max wait, after which it returns
  `GatewayError::Timeout`.

### Auth resolution (`resolve_api_key`)

Every HTTP adapter that needs a key calls `base::resolve_api_key(config)`, whose
precedence is: (1) `config.api_key` literal (the daemon populates this after
reading the Keychain), then (2) `config.api_key_env` (env-var name), else
`None`. The **auth style** column below describes only how that resolved key is
placed on the wire.

## Adapter reference

Base URLs marked *from `RouterConfig.url`* have **no hardcoded default** in the
adapter — the daemon supplies the URL; the "canonical host" shown is the value
used in the module docs / tests. Base URLs marked *const, `config.url`
fallback* use the listed constant only when `config.url` is empty.

| id | Base URL / where configured | Auth style | Capabilities (`supports`) | Default model(s) | Notes / quirks |
|----|-----------------------------|------------|---------------------------|------------------|----------------|
| `anthropic` | `RouterConfig.url` (canonical `https://api.anthropic.com`) + `/v1/messages` | `x-api-key` header + `anthropic-version` header | `TextChat` | `claude-haiku-4-5-20250414` (`DEFAULT_MAX_TOKENS` = 1024) | Native Messages API, not OpenAI-shaped. Chat only. |
| `bedrock` | AWS SDK endpoint — `RouterConfig.url` **ignored** | AWS **SigV4** via `aws-sdk-bedrockruntime` credential-provider chain (env → shared creds → IAM role → IMDS) | `TextChat`, `TextEmbed` | chat `anthropic.claude-3-5-sonnet-20241022-v2:0`; embed `amazon.titan-embed-text-v2:0` (max-tokens 1024) | Uses the unified Converse API; `api_key`/`api_key_env`/`url` unused (only `headers` honoured). `stream()` is Chat-only. See discrepancy note. |
| `fal` | const `https://queue.fal.run`, `config.url` fallback | `Authorization: Key {key}` (not Bearer) | `VideoGenerate`, `ImageGenerate` | `fal-ai/veo3` | Async queue: submit then poll via `async_job`. |
| `flux` | const `https://api.bfl.ai/v1`, `config.url` fallback | `x-key` header | `ImageGenerate` | `flux-pro-1.1` | Black Forest Labs. Async submit + poll. |
| `gemini` | `RouterConfig.url` (canonical `https://generativelanguage.googleapis.com/v1beta`) | `x-goog-api-key` header | `TextChat`, `TextEmbed` | chat `gemini-2.0-flash`; embed `text-embedding-004` (max-tokens 1024) | Google-native `:generateContent` shape, not OpenAI-compatible. |
| `grok` | `RouterConfig.url` (canonical `https://api.x.ai`) + `/v1/...` | `bearer_auth` | `TextChat`, `AudioTranscribe`, `AudioGenerate` | chat `grok-4-fast`; audio `grok-2-audio`; voice `Ara` | xAI. Chat endpoint is OpenAI-compatible; STT is multipart, TTS is `/v1/audio/speech`. |
| `kling` | const `https://api.klingai.com/v1`, `config.url` fallback | `bearer_auth` | `VideoGenerate` | `kling-v2` | Async submit + poll. |
| `luma` | const `https://api.lumalabs.ai/dream-machine/v1`, `config.url` fallback | `bearer_auth` | `VideoGenerate` | `ray-2` | Async submit + poll. |
| `noop` | none | none | **all** (`supports` → `true`) | none (reports model `"none"`) | Last-resort fallback: never errors, returns `success: false` with an "install Ollama / configure a key" message. Not a real provider. |
| `ollama` | `RouterConfig.url` (canonical `http://localhost:11434`) + `/v1/chat/completions` | `bearer_auth` **only if a key is present** (optional) | `TextChat`, `TextComplete`, `TextEmbed` | `gemma3:27b` | Local OpenAI-compatible server; `DEFAULT_TIMEOUT_SECS` = 120. |
| `openai` | `RouterConfig.url` (canonical `https://api.openai.com`) + `/v1/...` | `bearer_auth` | `TextChat`, `TextEmbed`, `AudioTranscribe`, `AudioGenerate`, `ImageGenerate` | `gpt-4o-mini` | `id` is a field, not a constant — reusable for OpenAI-compatible clones (see below). |
| `recraft` | const `https://external.api.recraft.ai/v1`, `config.url` fallback | `bearer_auth` | `ImageGenerate` | `recraftv3` | Synchronous `POST /images/generations` (no polling). |
| `replicate` | const `https://api.replicate.com/v1`, `config.url` fallback | `bearer_auth` | `VideoGenerate`, `ImageGenerate` | `tencent/hunyuan-video` | Async `predictions` submit + poll. |
| `runway` | const `https://api.runwayml.com/v1`, `config.url` fallback | `bearer_auth` | `VideoGenerate` | `gen-4` | Async submit + poll. |
| `stability` | const `https://api.stability.ai/v2beta`, `config.url` fallback | `bearer_auth` + multipart form | `ImageGenerate` | `sd3.5-large` | Synchronous multipart upload (no polling). |
| `together` | const `https://api.together.xyz/v1`, `config.url` fallback | `bearer_auth` | `TextChat`, `ImageGenerate` | chat `meta-llama/Llama-3.3-70B-Instruct-Turbo`; image `black-forest-labs/FLUX.1-schnell-Free` | OpenAI-compatible chat + synchronous `/images/generations`. |

## Notes on notable behaviour

### OpenAI-compatible adapters and clones

`OpenAIAdapter` stores its `id` as a **field** rather than returning a literal.
`OpenAIAdapter::new()` / `from_config()` register it as `"openai"`, while
`with_id(id)` / `from_config_with_id(id, config)` let the *same* wire
implementation register under any other name (`"openrouter"`, `"vercel"`,
`"nvidia"`, …). Each such registration is driven entirely by its
`RouterConfig` (custom `url` + key), so any OpenAI-compatible endpoint can be
added without a new adapter type. Its capability set is fixed regardless of id.

`grok`, `together`, and `ollama` are separate adapter types but all speak the
OpenAI chat-completions shape (`POST …/v1/chat/completions`); they exist as
their own types mainly to add non-chat capabilities (Grok audio, Together
images) or provider-specific handling.

### Asynchronous media adapters (submit → poll)

`fal`, `flux`, `kling`, `luma`, `replicate`, and `runway` all generate
image/video via a **submit-then-poll** flow built on `async_job::poll_until_complete`:
they POST a job, receive a job/prediction id, then poll a status endpoint every
3 s until the result is ready or the 5-minute cap trips a `Timeout`. By
contrast the image adapters `recraft`, `stability`, and `together`(image) are
**synchronous** — a single request returns the image inline.

### `noop`

The `noop` adapter is the graceful-degradation last resort. `supports()`
returns `true` for every capability so it can always be selected, but `execute`
returns `Ok` with `success: false` and a single failed `Attempt` (adapter
`"noop"`, model `"none"`) rather than surfacing an error, guaranteeing the
gateway always yields a response.

## Discrepancies found

- **`bedrock` embeddings** — the module doc header states embeddings and
  streaming are "scoped as follow-ups", but `supports()` returns `true` for
  `TextEmbed` and `execute` fully implements `Payload::Embed` (Titan and Cohere
  embedding families, with `DEFAULT_EMBED_MODEL`). The comment is stale;
  embeddings are implemented. Streaming, however, genuinely is Chat-only —
  `stream()` returns an error for any non-`Chat` payload.
- **`bedrock` config fields** — `api_key`, `api_key_env`, and `url` on
  `RouterConfig` are silently ignored (auth is SigV4 via the AWS SDK). Only
  `headers` and the per-request model/params are used, which can surprise
  callers who set a URL or key expecting them to take effect.
