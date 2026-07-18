# Sketch: safetensors "HF as a separate engine" (Metal / candle)

- **Status:** Sketch — future track, not scheduled
- **Crate:** `gateway-embedded`
- **Builds on:** the model registry (`ModelFormat::Safetensors` already exists) and
  the HF-A puller (`docs/design/hf-model-download.md`). Deferred from HF-A, which
  shipped GGUF + ONNX.

## 1. Why a separate engine

The embedded engines are format-specific: **GGUF → llama.cpp**, **ONNX → ort**.
Native HF weights ship as **safetensors** (fp16/bf16 raw tensors + `config.json` +
`tokenizer.json`) — a different runtime again. The draw is running these directly
on **Apple Silicon GPU via Metal**, without a GGUF conversion/quantization step, so
a model published only as safetensors is usable on a Mac.

## 2. Runtime choice

| Option | Fit | Notes |
|---|---|---|
| **candle** (HF's Rust ML framework) | **Best** | Native Rust, Metal backend (`Device::new_metal`), loads safetensors directly, ships per-architecture model code (llama/mistral/qwen/gemma/…), and BERT-family for embeddings. Cleanest fit for a Rust `gateway-embedded` engine. |
| **mistral.rs** | Good | Built on candle; more batteries (paged attention, quant, many models, an OpenAI-ish server) but heavier and more opinionated. Consider if we want broad model coverage without hand-wiring architectures. |
| **MLX** (Apple) | Poor (for Rust) | Fast on Apple Silicon, but Python/C++/Swift-first; Rust bindings immature. Not a fit for a Rust crate today. |

**Recommendation:** build the engine on **candle** (native Rust + Metal +
safetensors-native), and evaluate **mistral.rs** later if per-architecture coverage
becomes the bottleneck.

## 3. Integration shape

- New engine module in `gateway-embedded` behind a **`candle`** (or `safetensors`)
  cargo feature — opt-in like `llama-cpp`/`ort`/`fastembed`, since it drags in a GPU
  stack.
- Implements the same capability traits — `ChatModel` (generate) and `EmbedModel`
  (BERT/e5/bge encoders) — so it composes in the routing config exactly like the
  cloud adapters and other embedded engines. No engine-specific surface leaks to
  callers (holds the "consumer doesn't know the provider" premise).
- Resolves a **Managed** model whose `ModelFormat::Safetensors`. The HF-A puller
  already supports this: a `PullSpec { format: Safetensors, files: ["model.safetensors",
  "config.json", "tokenizer.json"] }` (sharded weights → list each
  `model-0000X-of-0000Y.safetensors` + `model.safetensors.index.json`).
- Loading: candle reads the safetensors weights + `config.json` (architecture +
  hyperparams) + `tokenizer.json`. **Model coverage = architectures candle
  implements** — unlike GGUF's near-universal loader, each architecture needs code.
  That's the main scope constraint.

## 4. Metal + the resource guard

- Device: `candle_core::Device::new_metal(0)`; fp16/bf16 compute on Metal.
- **Memory:** safetensors are **unquantized**, so ≈2 bytes/param resident (fp16) —
  much larger than a GGUF Q4. HF-A's `check_fit` RAM heuristic is GGUF-tuned
  (`need_ram ≈ size × 1.2`); for safetensors it's closer to `file size × 1.1` +
  KV/activation headroom, and it competes with the **unified memory** the GPU shares.
  → `check_fit` should branch its heuristic on `ModelFormat` (a small extension to
  the HF-A guard). A 13B fp16 model (~26 GB) won't fit a 16 GB Mac — exactly the
  case the guard must catch.

## 5. Scope / risks vs HF-A

Bigger lift than HF-A: per-architecture model code (not a universal loader), larger
memory footprint, candle maturity varies by architecture, and sampling/streaming
parity must be built (KV cache, temperature/top-p, token streaming into
`StreamChunk`). Treat as its own track, not a follow-on commit.

## 6. Build order (when scheduled)

1. candle engine skeleton behind the feature: load one safetensors arch
   (llama/mistral) on Metal, greedy/temperature generation, `ChatModel`.
2. Wire `ModelFormat::Safetensors` resolution + the sharded-file `PullSpec`.
3. Branch `check_fit`'s RAM heuristic per format (fp16 footprint + unified memory).
4. Streaming: token-by-token into `StreamChunk` (mirror the llama.cpp engine).
5. `EmbedModel` via candle BERT (bge/e5) for local embeddings.

## 7. Non-goals

Training/fine-tuning; quantization beyond what candle offers; non-Apple GPUs (CUDA
is a later, separate backend); auto-selecting an architecture (the `config.json`
`architectures` field drives it, else error).
