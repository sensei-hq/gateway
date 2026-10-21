# Local

Run models **in-process** (no network) or reuse a local Ollama install. Local engines
implement the *same* capability traits as the cloud adapters — so a local model
registers and executes exactly like a cloud one. Engines are behind cargo features
(each pulls heavy native deps), so you compile only what you use. Model resolution and
Hugging Face pull live in the companion `local-engine` crate; the in-process engines
live in `local-providers`.

```toml
local-providers = { package = "sensei-local-providers", git = "https://github.com/sensei-hq/gateway",
                    tag = "v0.12.0", features = ["llama-cpp"] }   # or: ort  (fastembed deferred, see note)
local-engine    = { package = "sensei-local-engine", git = "https://github.com/sensei-hq/gateway",
                    tag = "v0.12.0", features = ["hf-download"] } # resolvers + HF pull
```

| Feature | Engine | Capabilities | Format |
|---|---|---|---|
| `llama-cpp` | `LlamaCppAdapter` | chat + embed | GGUF |
| `fastembed` _(deferred, gh#7)_ | `FastembedAdapter` | embed | ONNX (+ tokenizer files) |
| `ort` | `OrtAdapter` | embed | ONNX |
| `kokoro` | `KokoroAdapter` | tts | ONNX (model + voices + lexicon) |
| `hf-download` | — (registry) | pull models from the HF Hub | GGUF / ONNX |

Default build compiles none of them. The `llama-cpp` / `fastembed` / `ort` / `kokoro`
engine features live on `local-providers`; `hf-download` lives on `local-engine`. From
a single `sensei-gateway` dep, use the pass-through features (`local-llama-cpp`,
`local-ort`, `local-kokoro`, `local-hf-download`).

## Ollama (server)

The simplest local path needs **no `local-providers` / `local-engine` at all**: run
Ollama yourself and point an `ollama` router at it. The `ollama` adapter speaks the
OpenAI-compatible API, so it behaves like any cloud router — just over
`http://localhost:11434`. Configure it like any other router (see `configuration.md`);
models you've `ollama pull`ed are available for chat + embed, and decision models
(e.g. `nimble`) for System One decisions on Ollama ≥ 0.35 — see
[recipes § decision](recipes.md#ask-a-decision-system-one). Use this when Ollama is
already running on the box and you don't want to link native engines.

## Self-hosted decision servers (llama.cpp, SGLang, Laya)

For System One **decisions** you can also point a router at your own `llama-server`
SGLang server or Laya server — again no `local-providers` / `local-engine` needed. The facade
auto-registers the router ids `llamacpp`, `sglang` and `laya` as decision-only
`SystemOneAdapter`s (POST `{url}/v1/systemone`; bearer only if a key is configured).
This `llamacpp` router is an HTTP server, unrelated to the embedded `llama-cpp`
feature below, and a chat adapter you register under the same id is left untouched.

- **llama.cpp** — needs release b11364+ (nimble) / b11371+ (clef); Homebrew's formula
  is too old. The GGUF must carry `<arch>.decision.type` + a `systemone` template, else
  every call is 501 `This model is not a decision model`; Ollama's `nimble` blob does
  not, so use `ggml-org/Bespoke-Nimble-9B-v3-GGUF`:
  `llama-server -m Bespoke-Nimble-9B-v3-Q4_K_M.gguf --alias nimble-v3 --port 8091 -c 8192`,
  router `url = "http://localhost:8091"`, model `api_model_id = "nimble-v3"`. Images
  (≤ 8) need a vision model + `--mmproj` (else 501). The response `model` is the alias;
  `usage.output_tokens` is always 0. Live-verified with b11381.
- **SGLang** — `/v1/systemone` ships in v0.5.21, but images and decision checkpoints
  are only on main (#42183): **v0.5.21 silently drops images** (200, text-only
  answers). A model name containing `:` is parsed as a LoRA adapter → 400; serve it
  under a colon-free name. Optional `--api-key` bearer. `usage.output_tokens` is 0.
- **Laya** — open-weight, CoreML on Apple Silicon — no cloud key; weights are public
  Hub downloads (offline after the first). `pip install "laya-coreml[serve]"` (verified
  with 0.5.1), then `LAYA_HOST=127.0.0.1 LAYA_PORT=8000 LAYA_MODELS=english
  laya-coreml-serve`; router `url = "http://127.0.0.1:8000"`, pin `api_model_id` to a
  checkpoint (`english` 512-token window, `multilingual` / `typed-decisions` 1024, or
  your own via `LAYA_EXTRA_MODELS`). Bearer only if the server sets `LAYA_API_KEY`.
  **Keep `LAYA_HOST=127.0.0.1`**: Laya's default is `0.0.0.0` with no auth unless
  `LAYA_API_KEY` is set, so otherwise anyone on the network can call it — set a key and
  the router's `api_key_env` if it must listen wider.
  Laya ignores images and cuts over-long questions **silently**, so the gateway refuses
  both itself, before sending, as `GatewayError::InputRejected` ("this candidate cannot
  serve this input; the endpoint is healthy"): the chain falls back **whatever
  `fallback_triggers` holds** (even empty), it is never retried and never counts toward
  the breaker. Concretely, a `laya` decision is refused if it carries images, or if a
  question's head (`<type> question: <instructions>` plus its options) is estimated over the checkpoint's
  `head_max_len` (`english` 192 tokens; `multilingual` / `typed-decisions` 256; any other
  name, 192), or its options over that budget minus 16 (Laya also trims every option
  below 16 tokens of room). The estimate is a conservative character-class count measured
  against both checkpoints' real tokenizers (never under in the committed fixture), so it
  errs high (it adds a margin of 2 tokens per option and 1 on the head) — about 1.7x on English (roughly 300 instruction chars fit on `english`, a 288-character
  instruction estimating 185 of 192, against a real limit near 560), 2-7x on non-Latin scripts (Chinese on `multilingual`
  allows about 70 chars). The model name is canonicalised with Laya's aliases first
  (`en` / `laya` / `default`, `multi` / `ml`, `typed` / `decisions`, the
  `convaiinnovations/laya-*` ids). A `LAYA_EXTRA_MODELS` checkpoint with `head_max_len`
  below 192 is unknown to the gateway and can still be cut silently. Over-window input
  is also caught by the model's `context_window` (set it to the real window) and by
  `usage.truncated: true` on a 200 (an `InputRejected` too); keep Laya's default payload
  (`LAYA_JEV_STRICT` strips the flag). A 200 whose `usage.options` has an entry with
  `distinct < total` (options that collapsed to the same tokens) is an `InputRejected`
  as well. Laya **auto-routes an unknown model name and still answers 200**, so if the
  response's `routing.model` differs from the one you asked for (both canonicalised), the
  gateway fails the call with a `ProviderError` naming both: a misspelled `api_model_id`
  is refused (the message lists the valid names: `english`, `multilingual`, `typed-decisions`, their aliases, the two `convaiinnovations/laya-*` ids, or a key of the server's `LAYA_EXTRA_MODELS`; `/health` `loaded` shows only the checkpoints resident right now — `LAYA_MAX_LOADED`, default 2 — not the servable names, and Laya has no endpoint that lists them). The check runs first: a re-routed response is this `ProviderError` even if it is also truncated. Chain a second
  decision router for refused requests: with only rejections in the chain the engine
  returns `AllAttemptsFailed { retryable: false }`. Laya's own 413 (too many
  questions/options) and 422 (e.g. over the exported `max_options`; CoreML caps at 32)
  are still `ProviderError`s and do count toward the breaker. The response `model` is
  always `laya-rl-agent`; `usage.output_tokens` is 0. No `/v1/systemone/batch`.
- **vLLM** — no upstream `/v1/systemone` (PR #59299 open; only an example proxy); not
  supported, no `vllm` id.

Neither has a default model — pin one via `api_model_id`. Router + model config:
[recipes § decision](recipes.md#ask-a-decision-system-one).

## Embedded Ollama models

To run Ollama's downloaded models **in-process** (no server process), reuse Ollama's
blob cache with an `OllamaResolver` and load them through the embedded llama.cpp engine:

```
ollama pull hf.co/<user>/<repo>:<quant>
```

then point an `OllamaResolver` at Ollama's cache — no `hf-download` feature needed. The
resolved `ModelEntry` loads into `LlamaCppAdapter` exactly like a managed model (next
section). Use this to reuse models you already pulled with Ollama, but execute them in
your own process.

## Embedded llama.cpp (managed models)

In-process GGUF generation + embedding. Local engines load from a **`ModelEntry`**
resolved by the registry, not from a URL. Three steps: resolve → load → register.

```rust
use std::sync::Arc;
use local_engine::registry::{ModelResolver, ManagedResolver};
use local_providers::adapters::llama_cpp::{LlamaCppAdapter, LlamaCppConfig};
use local_providers::adapters::llama_cpp::LlamaBackend; // process-wide backend

// 1. Resolve a model from the managed store (an index of on-disk models).
let registry = ManagedResolver::new("/path/to/models");
let entry = registry.resolve("qwen2.5-0.5b").await?.expect("model present");

// 2. Load the engine adapter around that entry.
let backend = Arc::new(LlamaBackend::init()?);
let adapter = LlamaCppAdapter::load(backend, &entry, LlamaCppConfig::default())?;

// 3. Register it like any adapter, then execute by capability as usual.
adapters.register(Arc::new(adapter)).await;   // id comes from the adapter/config
```

Engine `*Config` types tune context size, threads, pooling, etc. — see
`docs/features/embedded-inference.md`.

## ONNX embeddings (ort / fastembed)

`ort` (ONNX Runtime) and `fastembed` are embedding-only engines that load an ONNX
`ModelEntry` the same way as llama.cpp: `OrtAdapter::load(&entry, cfg)` /
`FastembedAdapter::load(&entry, cfg)`. `ort` gives lower-level control (configurable
pooling, threads); `fastembed` ships curated BERT-class defaults.

> **`fastembed` is deferred (gh#7).** Its 5.x line pins `hf-hub 0.5`, which would
> duplicate the `hf-hub 1.0` used for HF download, so the dependency is disabled and
> the feature is an inert placeholder — it won't build until fastembed ships on
> `hf-hub 1.0`. For ONNX embeddings use `ort`; Ollama embeddings remain the primary
> path and are unaffected.

## Text-to-speech (Kokoro)

`kokoro` is the first *local* TTS engine — a `TtsModel` backed by the Apache-2.0
Kokoro-82M ONNX model via the `sensei-kokoro` engine. Enable `local-kokoro` on the
gateway (or `kokoro` on `local-providers`). Provision with an `HfKokoro` plan: it pulls
the model + a voice from `onnx-community/Kokoro-82M-v1.0-ONNX`. The misaki
`us_gold.json` lexicon is GitHub-only (not on the model repo), so supply it as a
sibling — `KokoroConfig::hf_layout("af_heart")` sets the right relative paths for the
pulled layout. English (US/UK) today; emits 24 kHz WAV.

## Hugging Face download (`hf-download`)

Pull a GGUF/ONNX model straight into the managed store. The **fit guard runs inside
`pull`**: it checks RAM + disk from the file size *before downloading* and refuses a
model that can't run on the machine (`PullError::WontFit`) — no 30 GB download on an
8 GB box.

```rust
use local_engine::registry::{ManagedResolver, ModelFormat};
use local_engine::registry::pull::{HfHubPuller, ModelPuller, PullSpec};

let managed = ManagedResolver::new("/path/to/models");
let puller  = HfHubPuller::new(managed, std::env::var("HF_TOKEN").ok()); // token: gated/private repos

let spec = PullSpec {
    repo: "bartowski/Qwen2.5-0.5B-Instruct-GGUF".into(),
    revision: None,                                   // defaults to "main"
    id: "qwen2.5-0.5b".into(),                        // registry id to register under
    name: Some("Qwen2.5 0.5B Instruct".into()),
    format: ModelFormat::Gguf,
    files: vec!["Qwen2.5-0.5B-Instruct-Q4_K_M.gguf".into()], // files[0] = the model; rest = siblings
};

// Pre-check without downloading (e.g. to show a UI):
let report = puller.check_fit(&spec).await?;
if report.fits {
    let entry = puller.pull(&spec).await?;            // downloads, stages, registers → ModelEntry
    // now load an engine adapter around `entry` as above
}
```

- ONNX: list `["model.onnx", "tokenizer.json", …]` — `files[0]` is the source, the rest
  are siblings placed alongside it.
- **Config pull-on-missing:** wrap the store in a `PullingResolver` seeded with a
  `HashMap<id, PullSpec>` — a configured-but-absent model is fetched (and fit-checked)
  the first time an engine resolves it.
- `HF_ENDPOINT` is honoured (size probe + download) for self-hosted mirrors.

## Model resolvers & download options

Every local engine loads a `ModelEntry`; where that entry comes from is a
`ModelResolver`, composable via `ChainedResolver`:

- **`ManagedResolver`** — an `index.json`-backed store you populate (HF downloads land here).
- **`OllamaResolver`** — read-through Ollama's local blob cache (`ollama pull …` then resolve).
- **`ExternalResolver`** — a model at an explicit path you point to.
- **`ChainedResolver`** — try several in order (e.g. managed → ollama).
- **`PullingResolver`** — a managed store that fetches a configured-but-absent model on
  first resolve (see HF download above).

A `ModelEntry` carries `{ id, name, format (Gguf|Onnx|Safetensors), source (Managed|Ollama|External), size_bytes, … }`.

## Benchmarking System One models

To measure which local decision model (Laya, Bespoke-Nimble) is best for which scenario, use the
`decision-bench` crate. It sends seeded cases through the gateway to each model and ranks them per
scenario by accuracy with a confidence interval, calibration and confident-wrong rate. It does not
route by score. The roster, how to start each server, dataset licences and how to read the report
are in `crates/decision-bench/README.md`.

```sh
cargo run --release -p decision-bench -- check                      # probe each model; unreachable or unfittable ones are NOT RUN
cargo run --release -p decision-bench -- fetch --per-scenario 40    # download public datasets, build seeded cases
cargo run --release -p decision-bench -- run                        # run every reachable model, write results.json
cargo run --release -p decision-bench -- report                     # render report.md from results.json
```
