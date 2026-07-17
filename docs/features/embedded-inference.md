# Embedded Inference

In-process (local) inference adapters for the gateway. `gateway-embedded` loads
models into the host process and runs generation/embedding directly — no HTTP,
no daemon, no separate provider account — while presenting the exact same
adapter surface the cloud providers use.

Source: `crates/gateway-embedded/src/lib.rs`,
`crates/gateway-embedded/src/adapters/*.rs`,
`crates/gateway-embedded/Cargo.toml`.

## What `gateway-embedded` is

Every embedded engine is wrapped in an adapter that implements the **same
`gateway::adapters::InferenceAdapter` trait** as the ~15 cloud adapters in the
`gateway` crate. The crate deliberately introduces no new abstraction in the
gateway core (see the module doc in `lib.rs`): an embedded adapter is just
another `InferenceAdapter`, so local and cloud models compose in one routing
config, share the same fallback chains, circuit breaker, and budget filtering,
and are selected the same way.

The crate has three modules:

- `adapters` — the engine adapters (all feature-gated).
- `registry` — resolves a stable model id to an on-disk path from three
  sources (`ModelSource::Managed` → `Ollama` → `External`), composed via a
  `ModelResolver`. Managed files live under `~/.sensei/models/`; the Ollama
  resolver is a read-only walk over `~/.ollama/models/`; External is an
  arbitrary user-pointed path.
- `math` — shared helpers (e.g. `l2_normalize_in_place`).

## The engines

| Engine | Adapter(s) | Cargo feature | Native dependency | Capabilities |
| --- | --- | --- | --- | --- |
| llama.cpp (GGUF) | `LlamaCppAdapter`, `EmbeddedLlamaAdapter` | `llama-cpp` | `llama-cpp-2` (C++) | generation + embedding |
| ONNX Runtime | `OrtAdapter` | `ort` | `ort` (C++), `tokenizers`, `ndarray` | embedding only |
| FastEmbed | `FastembedAdapter` | `fastembed` | `fastembed` (wraps ORT) | embedding only |

### llama.cpp — `llama_cpp` and `embedded_llama`

Built on `llama-cpp-2`. Two adapters, both behind the `llama-cpp` feature.

**`LlamaCppAdapter`** (`adapters/llama_cpp.rs`) holds exactly one loaded GGUF
model in one mode, chosen at load time via `LlamaCppMode`:

- `LlamaCppMode::Embedding { pooling }` — single-shot `encode()` + per-sequence
  pooled vector read for BERT-class embedding models. Built by
  `LlamaCppConfig::embed(model_id)` (defaults: `n_ctx` 512, `n_threads` 1,
  `n_seq_max` 64, mean pooling). Output vectors are L2-normalised.
- `LlamaCppMode::Generation { default_max_tokens, default_temperature, seed }`
  — autoregressive `decode()` loop with token sampling, formatting messages via
  the model's bundled chat template. Built by `LlamaCppConfig::chat(model_id)`
  (defaults: `n_ctx` 4096, `n_seq_max` 1, `default_max_tokens` 512,
  `default_temperature` 0.0 = greedy, `seed` 42).

Requests whose `model` field disagrees with the adapter's `model_id` return
`GatewayError::ModelUnavailable`. The adapter accepts any `ModelEntry`
regardless of `ModelSource` variant — it only needs the on-disk GGUF path.

Two process-wide singletons back it: `shared_backend()` returns the one-per-
process `LlamaBackend` (init is only allowed once), and `cached_model()` keeps a
`Weak`-referenced cache of loaded `LlamaModel` weights keyed by path, so a
second adapter for the same GGUF clones an `Arc` instead of re-reading a
multi-GB file. The per-adapter `LlamaContext` (KV cache) is behind a `Mutex`, so
concurrent `execute()` calls serialise.

**`EmbeddedLlamaAdapter`** (`adapters/embedded_llama.rs`, #79) presents the
llama.cpp runtime as a **single router** (`embedded-llama`) that serves many
models across capabilities — the same shape as the `ollama` router. It owns a
lazily-built map of per-`(model_id, mode)` `LlamaCppAdapter` workers:

- Which model comes from `request.model` (the engine injects the resolved chain
  model id here); a missing `request.model` is an error.
- Which mode is derived from the payload: `Payload::Embed` → an embedding
  worker, `Payload::Chat` → a generation worker. Non-text payloads (image,
  audio, …) are rejected.
- Model bytes are resolved through a `ModelResolver`, so a model already pulled
  by Ollama is reused in place and nothing ships with the binary. Weights are
  still shared process-wide through `cached_model`, so a second mode of the same
  GGUF is a context build, not a re-read.

### ONNX Runtime — `ort`

`OrtAdapter` (`adapters/ort.rs`) calls ONNX Runtime directly via the `ort`
crate — the lowest-level of the three engines. It exists alongside FastEmbed to
give: custom pooling levers, arbitrary BERT-class ONNX exports (any ONNX whose
tokenizer sits alongside it), and execution-provider/thread tuning.

- `OrtConfig` defaults: `adapter_id` `"ort"`, `model_id` `"default"`,
  `max_length` 256, `pooling` `OrtPoolingStrategy::Mean`, `threads` 1. Builders:
  `OrtConfig::bert(model_id)` (mean pooling) and `OrtConfig::bert_cls(model_id)`
  (CLS-token pooling).
- `OrtPoolingStrategy` has two variants today: `Mean` (attention-masked mean of
  the last hidden state, the sentence-transformers default) and `Cls` (first
  token). A Last-token strategy is noted as addable.
- `load()` reads the ONNX file at `entry.source.path()` plus a sibling
  `tokenizer.json` (only that one file is required). The session is built at
  `GraphOptimizationLevel::Level3` with `config.threads` intra-op threads.
- Runs the model with `input_ids` / `attention_mask` / `token_type_ids`,
  expects a rank-3 `[batch, seq, hidden]` output, pools in Rust, then
  L2-normalises. `Session::run` needs `&mut self`, so calls serialise on a
  `Mutex`.
- The `ort` feature enables the crate's `download-binaries` (ORT is fetched at
  build) plus `ndarray`; the README describes this engine as "ONNX Runtime
  (CPU)".

### FastEmbed — `fastembed`

`FastembedAdapter` (`adapters/fastembed.rs`) wraps `fastembed::TextEmbedding`,
which itself runs on ONNX Runtime but ships hand-tuned, pre-quantised ONNX
exports and the correct pooling/tokenizer defaults for the popular BERT-class
embedding models (MiniLM, BGE, nomic-embed). The value is "one library that
loads any of the standard embedding models correctly."

- `FastembedConfig` defaults: `adapter_id` `"fastembed"`, `model_id`
  `"default"`, `max_length` 256, `pooling` `Pooling::Mean`, `quantization`
  `QuantizationMode::None`. Builder: `FastembedConfig::bert(model_id)`.
- `load()` reads the ONNX file at `entry.source.path()` **plus four sibling
  tokenizer files** in the same directory: `tokenizer.json`, `config.json`,
  `special_tokens_map.json`, `tokenizer_config.json` (the layout produced by
  Qdrant's exports and `optimum-cli export onnx`).
- `TextEmbedding::embed` needs `&mut self`, so calls serialise on a `Mutex`.
  fastembed L2-normalises BERT-class output by default.
- Note: an `Ollama`-sourced entry resolves to a GGUF blob, not ONNX, so loading
  one through this adapter fails at fastembed's ONNX parse step.

## Cargo features

All three engine features are **off by default** (`default = []` in
`Cargo.toml`). Each pulls in heavyweight native dependencies, so they are
strictly opt-in:

| Feature | Enables | Pulls in |
| --- | --- | --- |
| `llama-cpp` | `LlamaCppAdapter` + `EmbeddedLlamaAdapter` | `dep:llama-cpp-2` (C++ toolchain) |
| `fastembed` | `FastembedAdapter` | `dep:fastembed` (C++ / ORT runtime) |
| `ort` | `OrtAdapter` | `dep:ort` + `dep:tokenizers` + `dep:ndarray` (C++, ORT binaries downloaded) |

Because the modules in `adapters/mod.rs` are `#[cfg(feature = ...)]`-gated, a
consumer that enables no feature compiles the crate with the registry and math
helpers but no engine. Enable exactly the engines you need — e.g.
`features = ["fastembed"]` for embeddings only.

## Per-adapter capabilities

Straight from each adapter's `supports()`:

| Adapter | `id()` | `TextEmbed` | `TextChat` | `TextComplete` | Streaming |
| --- | --- | --- | --- | --- | --- |
| `LlamaCppAdapter` (Embedding mode) | `"llama-cpp"` | yes | no | no | no |
| `LlamaCppAdapter` (Generation mode) | `"llama-cpp"` | no | yes | yes | yes (`Payload::Chat`) |
| `EmbeddedLlamaAdapter` | configurable (e.g. `"embedded-llama"`) | yes | yes | yes | yes (generation) |
| `OrtAdapter` | `"ort"` | yes | no | no | no |
| `FastembedAdapter` | `"fastembed"` | yes | no | no | no |

The three embedding-only adapters (`Ort`, `Fastembed`, and `LlamaCpp` in
Embedding mode) reject any non-`Payload::Embed` request and return an error from
`stream()`. `LlamaCppAdapter` in Generation mode implements true token
streaming: a `spawn_blocking` worker emits a `StreamChunk` for each valid UTF-8
prefix increment, ending with a final empty chunk whose `finish_reason` is
`"stop"` (EOS) or `"length"` (hit the token cap).

## When to use local vs cloud

Local (embedded) adapters shine when:

- **Latency on small work matters.** For short embedding queries, calling a
  local Ollama daemon over HTTP spends 5–9x more time in protocol overhead than
  in inference (measured in `rust-embedding-bench`); loading the GGUF/ONNX
  in-process recovers that. The `minilm-bench` harness measured the `ort` (fp32)
  path at ~1.10 ms p50 for single short-text queries on an Apple M4 Max
  (fastembed ~1.71 ms at batch=1).
- **Data must not leave the host**, or you want zero per-request cost and no
  provider account.
- **The model is already on disk** — a GGUF pulled by Ollama, a managed model,
  or a user-pointed ONNX/GGUF — and can be reused in place.

Reach for cloud adapters when you need frontier-scale models, capabilities the
embedded engines don't serve (image, audio, video — the embedded runtime only
covers `TextEmbed` / `TextChat` / `TextComplete`), or you don't want to pay the
native build cost and local compute/memory of loading weights in-process.
Because both are the same `InferenceAdapter`, the common pattern is to mix them
in one fallback chain — e.g. a fast local embedding model first, a cloud
embedder as the fallback.

## Discrepancies / surprises

- **`embedded_llama` has no feature of its own.** `EmbeddedLlamaAdapter` ships
  with the `llama-cpp` feature alongside `LlamaCppAdapter`; there are only three
  features (`llama-cpp`, `fastembed`, `ort`), not four.
- **Stale doc comment on streaming.** The module doc at the top of
  `adapters/llama_cpp.rs` says "Streaming is not yet implemented — `stream()`
  returns an error," but `stream()` is in fact fully implemented for
  `Payload::Chat` (Generation mode). The comment is out of date.
- **Tokenizer-file requirements differ.** `OrtAdapter` needs only a sibling
  `tokenizer.json`; `FastembedAdapter` needs four sibling files
  (`tokenizer.json`, `config.json`, `special_tokens_map.json`,
  `tokenizer_config.json`). A directory that loads under `ort` may not load
  under `fastembed`.
- **Version drift in the README.** The root `README.md` says both crates
  "currently share version `0.2.18`," but `gateway-embedded/Cargo.toml` is at
  `0.2.24`. The README examples also pin tag `v0.2.18`.
