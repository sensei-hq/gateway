---
title: Providers
doctype: feature
module: inference
status: implemented
source: crates/cloud-providers/src
---

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
fallback* use the listed constant only when `config.url` is empty. The
decision rows (`openrouter`, `typesafe`) spell this out as *`RouterConfig.url`,
else const …*: the router's `url` is used whenever it is set.

| id | Base URL / where configured | Auth style | Capabilities (`supports`) | Default model(s) | Notes / quirks |
|----|-----------------------------|------------|---------------------------|------------------|----------------|
| `anthropic` | `RouterConfig.url` (canonical `https://api.anthropic.com`) + `/v1/messages` | `x-api-key` header + `anthropic-version` header | `TextChat` | `claude-haiku-4-5-20250414` (`DEFAULT_MAX_TOKENS` = 1024) | Native Messages API, not OpenAI-shaped. Chat only. |
| `bedrock` | AWS SDK endpoint — `RouterConfig.url` **ignored** | AWS **SigV4** via `aws-sdk-bedrockruntime` credential-provider chain (env → shared creds → IAM role → IMDS) | `TextChat`, `TextEmbed` | chat `anthropic.claude-3-5-sonnet-20241022-v2:0`; embed `amazon.titan-embed-text-v2:0` (max-tokens 1024) | Uses the unified Converse API; `api_key`/`api_key_env`/`url` unused (only `headers` honoured). `stream()` is Chat-only. See discrepancy note. |
| `cloudflare` | `RouterConfig.url` = `https://api.cloudflare.com/client/v4/accounts/<account_id>/ai` (no default — empty is `InvalidConfig`) + `/run/@cf/cloudflare/{model}` | `bearer_auth` (a Workers AI API token, Read + Edit) | `Decision` | none (a call without a model is `InvalidRequest`) | `CloudflareAdapter`, decision-only, auto-registered by the facade. System One models `clef` / `clef-flash` over Workers AI's own REST route (there is no `/v1/systemone`): the model is `clef`, `clef-flash` or a full `@cf/cloudflare/clef…` id — the full id goes in the path, the short name in the body; `keep_alive` is not sent; the `{result, success, errors, messages}` envelope is unwrapped, and a 2xx with `success: false` or no `result` is a `ProviderError`. Images go as data URLs. Workers AI truncates long state **silently** — see below. |
| `fal` | const `https://queue.fal.run`, `config.url` fallback | `Authorization: Key {key}` (not Bearer) | `VideoGenerate`, `ImageGenerate` | `fal-ai/veo3` | Async queue: submit then poll via `async_job`. |
| `flux` | const `https://api.bfl.ai/v1`, `config.url` fallback | `x-key` header | `ImageGenerate` | `flux-pro-1.1` | Black Forest Labs. Async submit + poll. |
| `gemini` | `RouterConfig.url` (canonical `https://generativelanguage.googleapis.com/v1beta`) | `x-goog-api-key` header | `TextChat`, `TextEmbed` | chat `gemini-2.0-flash`; embed `text-embedding-004` (max-tokens 1024) | Google-native `:generateContent` shape, not OpenAI-compatible. |
| `grok` | `RouterConfig.url` (canonical `https://api.x.ai`) + `/v1/...` | `bearer_auth` | `TextChat`, `AudioTranscribe`, `AudioGenerate` | chat `grok-4-fast`; audio `grok-2-audio`; voice `Ara` | xAI. Chat endpoint is OpenAI-compatible; STT is multipart, TTS is `/v1/audio/speech`. |
| `kling` | const `https://api.klingai.com/v1`, `config.url` fallback | `bearer_auth` | `VideoGenerate` | `kling-v2` | Async submit + poll. |
| `llamacpp` | `RouterConfig.url` (no default — empty is `InvalidConfig`; e.g. `http://localhost:8091`) + `/v1/systemone` | `bearer_auth` **only if a key is present** | `Decision` | none (a call without a model is `InvalidRequest`) | `SystemOneAdapter::from_config_with_id("llamacpp", …)`, decision-only, auto-registered by the facade — a self-hosted `llama-server`. Distinct from the embedded `llama_cpp` engine (`local-providers`); a chat adapter you register under `llamacpp` is untouched. Server requirements below. |
| `luma` | const `https://api.lumalabs.ai/dream-machine/v1`, `config.url` fallback | `bearer_auth` | `VideoGenerate` | `ray-2` | Async submit + poll. |
| `noop` | none | none | **all** (`supports` → `true`) | none (reports model `"none"`) | Last-resort fallback: never errors, returns `success: false` with an "install Ollama / configure a key" message. Not a real provider. |
| `ollama` | `RouterConfig.url` (canonical `http://localhost:11434`) + `/v1/chat/completions`, `/v1/embeddings`, `/v1/systemone` | `bearer_auth` **only if a key is present** (optional) | `TextChat`, `TextEmbed`, `Decision` (`TextComplete` requests are dispatched by the engine to the chat path — the adapter has no completion-specific code) | chat/embed `gemma3:27b`; decision `nimble` (only when the request carries no model) | Local OpenAI-compatible server; `DEFAULT_TIMEOUT_SECS` = 120. Decisions need Ollama ≥ 0.35 and a **local** decision model (cloud models are rejected with a 400 `ProviderError`, which falls back when the chain lists that trigger). A JSON 404 is reworded to ``run `ollama pull <model>` ``; a bare `404 page not found` to "needs Ollama >= 0.35". `probe_decision_model(cfg, model)` → `Ready` / `ServerTooOld { version }` / `NotPulled` / `NotADecisionModel`. |
| `openai` | `RouterConfig.url` (canonical `https://api.openai.com`) + `/v1/...` | `bearer_auth` | `TextChat`, `TextEmbed`, `AudioTranscribe`, `AudioGenerate`, `ImageGenerate` | `gpt-4o-mini` | `id` is a field, not a constant — reusable for OpenAI-compatible clones (see below). |
| `openrouter` | chat etc.: `RouterConfig.url` (canonical `https://openrouter.ai/api`) + `/v1/...`; decision: `RouterConfig.url`, else const `https://openrouter.ai/api`, + `/v1/systemone` | `bearer_auth` | `OpenAIAdapter` set (`TextChat`, `TextEmbed`, `AudioTranscribe`, `AudioGenerate`, `ImageGenerate`) + `Decision` | OpenAI adapter defaults for chat etc.; **no default decision model** (a call without one is `InvalidRequest`) | Two adapters under one id, auto-registered by the facade: `OpenAIAdapter::from_config_with_id("openrouter", …)` + `SystemOneAdapter::from_config_with_id("openrouter", …)`. Decision models include `typesafe/jev-1.13`. |
| `recraft` | const `https://external.api.recraft.ai/v1`, `config.url` fallback | `bearer_auth` | `ImageGenerate` | `recraftv3` | Synchronous `POST /images/generations` (no polling). |
| `replicate` | const `https://api.replicate.com/v1`, `config.url` fallback | `bearer_auth` | `VideoGenerate`, `ImageGenerate` | `tencent/hunyuan-video` | Async `predictions` submit + poll. |
| `runway` | const `https://api.runwayml.com/v1`, `config.url` fallback | `bearer_auth` | `VideoGenerate` | `gen-4` | Async submit + poll. |
| `sglang` | `RouterConfig.url` (no default — empty is `InvalidConfig`) + `/v1/systemone` | `bearer_auth` **only if a key is present** (the server's optional `--api-key`) | `Decision` | none (a call without a model is `InvalidRequest`) | `SystemOneAdapter::from_config_with_id("sglang", …)`, decision-only, auto-registered by the facade — a self-hosted SGLang server. Server caveats below. |
| `laya` | `RouterConfig.url` (no default — empty is `InvalidConfig`; e.g. `http://127.0.0.1:8000`) + `/v1/systemone` | `bearer_auth` **only if a key is present** (the server's `LAYA_API_KEY`) | `Decision` | none (a call without a model is `InvalidRequest`; pin a checkpoint such as `english`) | `SystemOneAdapter::from_config_with_id("laya", …)`, decision-only, auto-registered by the facade — a local `laya-coreml-serve` (CoreML). Refuses images and over-budget questions; a `usage.truncated: true` or collapsed-options answer is an `InputRejected`; a re-routed checkpoint is a `ProviderError`. Errors are FastAPI `{detail}`. Server caveats below. |
| `stability` | const `https://api.stability.ai/v2beta`, `config.url` fallback | `bearer_auth` + multipart form | `ImageGenerate` | `sd3.5-large` | Synchronous multipart upload (no polling). |
| `typesafe` | `RouterConfig.url`, else const `https://api.typesafe.ai`, + `/v1/systemone` | `bearer_auth` | `Decision` | none (a call without a model is `InvalidRequest`) | `SystemOneAdapter`, decision-only, auto-registered by the facade. TypeSafe's System One API is the origin of the decision wire format. |
| `together` | const `https://api.together.xyz/v1`, `config.url` fallback | `bearer_auth` | `TextChat`, `ImageGenerate` | chat `meta-llama/Llama-3.3-70B-Instruct-Turbo`; image `black-forest-labs/FLUX.1-schnell-Free` | OpenAI-compatible chat + synchronous `/images/generations`. |

## Notes on notable behaviour

### System One decision routers (`systemone.rs`)

`SystemOneAdapter` (`crates/cloud-providers/src/systemone.rs`) is a generic,
id-configurable, decision-only adapter for any bearer-auth host of
`POST {base}/v1/systemone`. Built with `SystemOneAdapter::with_id(id)` or
`from_config_with_id(id, config)`. The ids `openrouter` and `typesafe` default their
base URL when `RouterConfig.url` is empty; any other id (including `llamacpp`,
`sglang` and `laya`) needs a `url` (else `InvalidConfig`). It never picks a model for you. The
Ollama adapter and `CloudflareAdapter` (`cloudflare.rs`) delegate to the same POST
core: Ollama adds its own reading of Ollama's two 404s; Cloudflare speaks the
Workers AI *dialect* of it (different path, short body `model`, no `keep_alive`, an
unwrapped result envelope). Transport, auth, timeout, header forwarding, error
mapping and the missing-answer check are shared. See
[capabilities & adapters § Decision](capabilities-and-adapters.md#decision-system-one)
for the request/answer types and how to read `confidence`, `noul` and `score`.

**Images are encoded per host.** The caller passes bare base64 in
`DecisionRequest.images`. Ollama receives it verbatim (it rejects data URLs).
`SystemOneAdapter` (every id: `openrouter`, `typesafe`, `llamacpp`, `sglang`, custom)
and `CloudflareAdapter` send `data:<mime>;base64,…`, with the mime sniffed from the
decoded magic bytes (PNG / JPEG / WebP / GIF); a value already starting `data:` passes
through. An image that is none of those four is an `InputRejected` **before any
request is sent** — never a guessed type. The exception is `laya`, which refuses any
image before sending (also `InputRejected`).

**Error messages** from these hosts are the extracted message, not the raw body:
`base::extract_error_message` reads `{error:{message}}`, `{error:"…"}`,
`{detail:"…"}`, a FastAPI `{detail:[{loc,msg}]}` list (joined as `loc: msg; …`), a
Cloudflare v4 `{errors:[{message}]}` array (joined with `; `) and a flat top-level
`{message}`; anything else falls back to the raw body.

**Cloudflare Workers AI (`cloudflare`).** `url =
"https://api.cloudflare.com/client/v4/accounts/<account_id>/ai"` (the account id is
part of every path) and a Workers AI API token (Read + Edit) in `api_key` /
`api_key_env`. Host limits: images ≤ 4, PNG / JPEG / WebP only; `choice` 2–255
options, `score` 2–10 levels, 1–64 questions. Long state is truncated **silently** —
seed the models' `context_window` with the real 65,536-token window so the engine's
`ContextWindowGate` (state + every question + prompt framing + images) never routes a
request here that Cloudflare would truncate. Pricing is input-only ($0.24/M input
tokens `clef`, $0.09/M `clef-flash`).

**llama.cpp server (`llamacpp`).** Needs release b11364+ (nimble) / b11371+ (clef);
Homebrew's formula is too old. `POST /v1/systemone` is always registered but answers
501 `This model is not a decision model` unless the GGUF carries
`<arch>.decision.type` + a `systemone` template — Ollama's `nimble` blob does **not**;
use `ggml-org/Bespoke-Nimble-9B-v3-GGUF`. Images must be data URLs (bare → 400), ≤ 8,
and need a vision model + `--mmproj` (else 501). The response `model` is the server's
alias; `usage.output_tokens` is always 0. Launch e.g. `llama-server -m
Bespoke-Nimble-9B-v3-Q4_K_M.gguf --alias nimble-v3 --port 8091 -c 8192`, router `url =
"http://localhost:8091"`. Live-verified with b11381.

**SGLang (`sglang`).** `/v1/systemone` ships in v0.5.21; images and decision
checkpoints are only on main (#42183) — **v0.5.21 silently drops images** (200,
text-only answers). A model name containing `:` is parsed as a LoRA adapter → 400, so
serve it under a colon-free name. Optional `--api-key` bearer. `usage.output_tokens` is
0.

**Laya (`laya`).** An open-weight System One model on Apple CoreML; weights are public
Hub downloads (offline after the first), so no cloud key. `pip install
"laya-coreml[serve]"` (verified with 0.5.1), then `LAYA_HOST=127.0.0.1 LAYA_PORT=8000
LAYA_MODELS=english laya-coreml-serve`; router `url = "http://127.0.0.1:8000"` and pin
`api_model_id`. Checkpoints: `english` (512-token window), `multilingual` (1024),
`typed-decisions` (1024), or your own via `LAYA_EXTRA_MODELS` (e.g. the ANE checkpoint
`{"ane":"aac6fef/laya-multilingual-coreml-ane"}`, a 96-token window). Bearer
(`Authorization: Bearer <key>`) only if the server sets `LAYA_API_KEY`. The response
`model` is always `laya-rl-agent`, not the checkpoint, and `usage.output_tokens` is 0.
Errors are FastAPI `{"detail": "…"}`: 401 → `Authentication`; 400 / 413 / 422 / 500 /
503 → `ProviderError` carrying the detail (so Laya's 413 "too many questions/options" and
422 "exceeds the exported max_options" — CoreML caps at 32 options — count toward the
breaker; the gateway does not yet turn them into `InputRejected`). Out of scope: `/v1/systemone/batch` and
CLM-8B (needs vLLM / CUDA).
- **Security:** `laya-coreml-serve` defaults to `LAYA_HOST=0.0.0.0` with no auth unless
  `LAYA_API_KEY` is set. Keep `LAYA_HOST=127.0.0.1`, or set a key and the router's
  `api_key_env`.
- **Images:** Laya ignores them, so a `laya` decision with `images` is refused before
  sending, as `GatewayError::InputRejected` ("this candidate cannot serve this input; the
  endpoint is healthy"). The guard keys on the exact router id `laya`; a Laya server
  registered under another id would get images it silently ignores.
- **`InputRejected` semantics (D5):** it advances the chain **whatever
  `fallback_triggers` holds, even empty**, is never retryable, and is never recorded by
  the circuit breaker, lockout, cooldown or performance recorders. It is neutral to
  `AllGated` (a chain whose other candidates are gated pauses on those gates); a chain
  with only rejections is `AllAttemptsFailed { retryable: false }`; beside a hard failure
  the exhaustion stays retryable. Stream error code `"input_rejected"`. So chain a second
  decision router for refused requests; retries (`with_max_transient_attempts`) never
  apply to it.
- **Per-question head budget (D6, `laya` only):** Laya silently cuts each question's head
  (`<type> question: <instructions>` plus its options) to the checkpoint's `head_max_len`
  and reports nothing, and also trims every option when the options leave fewer than 16
  tokens. The gateway therefore estimates each question before sending and refuses
  (`InputRejected`) if the head exceeds the budget or its options exceed `budget - 16`.
  Budgets: `english` 192; `multilingual` / `typed-decisions` 256; any other name 192. The
  model name is canonicalised with Laya's aliases first (`en` / `laya` / `default` ->
  english; `multi` / `ml` / `laya-multilingual` -> multilingual; `typed` /
  `typed_decisions` / `laya-typed-decisions` / `decisions` -> typed-decisions;
  `convaiinnovations/laya-multilingual` and `convaiinnovations/laya-typed-decisions` map to
  their checkpoints); the bundle id `convaiinnovations/laya` means "let Laya choose" and
  gets 192. The estimate is a conservative character-class token count (non-ASCII counts
  its UTF-8 bytes, symbols and digits 1 each; options are counted as Laya renders them,
  `" " + text` capped at 48 tokens, +1 per option). Measured against both checkpoints'
  real tokenizers it was never under across 52 option and 30 head samples (fixture
  `crates/cloud-providers/tests/fixtures/laya_head_tokens.py`). It adds safety margins — 2 tokens per option and 1 on the head: the fixture found 0 under-counts per option across 900 000 random keys at margin 2, against 15 at margin 1 and 833+ at 0. **Cost:** it errs high, about 1.7x on English
  (about 300 instruction chars fit on `english`; a 288-character instruction estimates 185 of 192, against a real limit of about 560) and
  2-7x on non-Latin scripts (Chinese on `multilingual` allows about 70 chars).
  **Limitation:** a `LAYA_EXTRA_MODELS` checkpoint with `head_max_len` below 192 is unknown
  to the gateway and can still be cut silently.
- **Truncation:** over-window input is also truncated **silently** — still a 200, with
  only `usage.truncated: true` to show it. The adapter reads `usage.truncated` on
  every decision host (`SystemOneAdapter` ids, `OllamaAdapter`, `CloudflareAdapter`; only Laya sends it today); `true` is an
  `InputRejected`, so the chain falls back. A usage `options` entry with `distinct < total`
  (options that collapsed to identical tokens; only Laya sends it, every host reads it) is
  an `InputRejected` too. Set the model's
  `context_window` to its real token window (english 512, multilingual /
  typed-decisions 1024) so the `ContextWindowGate` refuses oversize requests before
  sending; the runtime message says "set the model's context_window to its real
  token window (or lower it if it already is)". Keep Laya's default payload:
  `LAYA_JEV_STRICT` strips `truncated`, which disables the check. A truncated call on a
  hosted provider was dispatched, but `InputRejected` carries no usage, so it is not
  metered.
- **Re-routed checkpoint (D7):** Laya auto-routes an unknown model name and still returns
  200. If the response's `routing.model` differs from the requested model (both
  canonicalised as above) the gateway fails the call with a `ProviderError` (status 200)
  naming both and the valid names, so a misspelled `api_model_id` is refused. Valid names
  are `english`, `multilingual`, `typed-decisions`, their aliases, the two
  `convaiinnovations/laya-*` ids, or a key of the server's `LAYA_EXTRA_MODELS`; `/health`
  `loaded` shows only the checkpoints resident right now (`LAYA_MAX_LOADED`, default 2),
  not the servable names, and Laya has no endpoint that lists them. The check runs first,
  before truncation, collapsed options and missing answers: a response that is both
  re-routed and truncated is this `ProviderError`, since a wrong checkpoint is an endpoint
  fault. The bundle id skips this check, and a `routing` field
  that is not an object or has no string `model` is ignored.
- **Overhead:** measured on an M4 Max, HTTP adds about 0.35 ms per decision — about
  8% on the ANE checkpoint (4.1 ms), negligible on CPU+GPU (~50 ms).

**vLLM** has no upstream `/v1/systemone` (PR #59299 is open; only an example proxy
exists), so it is not supported and there is no `vllm` facade id.

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

## Scenarios

```gherkin
Feature: Providers
  Scenario: Anthropic adapter maps chat to the native API
    Given an Anthropic router and a chat request
    Then the adapter calls the Anthropic messages API and returns a ChatResponse
  Scenario: OpenAI-compatible ids share one implementation
    Given ids openrouter and vercel registered via with_id
    Then both dispatch through the OpenAI adapter
  Scenario: A 429 maps to a RateLimit error
    Given a provider returns HTTP 429 with Retry-After
    Then the adapter returns GatewayError::RateLimit { retry_after_ms }
```
