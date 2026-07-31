# Local

Run models **in-process** (no network) or reuse a local Ollama install. Local engines
implement the *same* capability traits as the cloud adapters — so a local model
registers and executes exactly like a cloud one. Engines are behind cargo features
(each pulls heavy native deps), so you compile only what you use. Model resolution and
Hugging Face pull live in the companion `local-engine` crate; the in-process engines
live in `local-providers`.

```toml
local-providers = { package = "sensei-local-providers", git = "https://github.com/sensei-hq/gateway",
                    tag = "v0.4.8", features = ["llama-cpp"] }   # or: ort  (fastembed deferred, see note)
local-engine    = { package = "sensei-local-engine", git = "https://github.com/sensei-hq/gateway",
                    tag = "v0.4.8", features = ["hf-download"] } # resolvers + HF pull
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
models you've `ollama pull`ed are available for chat + embed. Use this when Ollama is
already running on the box and you don't want to link native engines.

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
