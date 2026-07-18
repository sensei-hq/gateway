# Design: Hugging Face model download (HF-A)

- **Status:** Approved (2026-07-18)
- **Crate:** `gateway-embedded`
- **Depends on:** the model registry (`ModelResolver`/`ManagedResolver`).
- **Next in sequence:** AUTH.

## 1. Goal

Fetch a model file from the HF Hub → place it in the **Managed** store → register a
`ModelEntry` → the embedded engines run it (`llama.cpp` for GGUF, `ort` for ONNX). No
Ollama required. Download is a **local/embedded-only** capability — the cloud crate can't
pull; expressed as a registry-layer trait, not bolted onto the inference-adapter structs
(which don't own storage).

## 2. Decisions (confirmed)

- **`hf-hub` crate** (async/tokio) for the download — handles LFS, revisions, gated-repo
  tokens, and caching — behind a new **opt-in `hf-download` feature** (like the engines).
- **Explicit file selection.** The caller/UI picks the repo + file(s); the puller does not
  guess a quant.
- **Formats now:** GGUF (one quant file) + ONNX (`model.onnx` + sibling `tokenizer.json`).
  Safetensors deferred (future candle/Metal "HF as an engine" track).
- **Managed store:** downloaded files land in the managed root and are registered via
  `ManagedResolver::add` as `ModelSource::Managed`.
- **Ollama path is no-code:** `ollama pull hf.co/<user>/<repo>:<quant>` + the existing
  `OllamaResolver` read-through already serves operators who prefer Ollama's cache — just
  documented.

## 3. Design (`gateway-embedded/src/registry/pull.rs`, feature `hf-download`)

```rust
/// A model file (+ any siblings) to fetch and register.
pub struct PullSpec {
    pub repo: String,             // e.g. "bartowski/Llama-3.2-3B-Instruct-GGUF"
    pub revision: Option<String>, // default "main"
    pub id: String,               // stable registry id to register under
    pub name: Option<String>,     // display name (defaults to id)
    pub format: ModelFormat,      // Gguf | Onnx
    /// Files to download. `files[0]` is the model file registered as the source
    /// path; the rest are siblings (e.g. tokenizer.json for ONNX).
    pub files: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum PullError { Hub(String), Io(std::io::Error), Registry(ResolveError), EmptySpec }

#[async_trait]
pub trait ModelPuller: Send + Sync {
    /// Download the spec's files into managed storage and register the entry.
    /// Returns the registered `ModelEntry`.
    async fn pull(&self, spec: &PullSpec) -> Result<ModelEntry, PullError>;
}

pub struct HfHubPuller {
    managed: ManagedResolver, // owns the managed root + index
    token: Option<String>,    // HF token for gated/private repos
}
```

`HfHubPuller::pull`:
1. Reject an empty `files` list (`EmptySpec`).
2. Build an `hf_hub::api::tokio::Api` with the optional token; select the repo at `revision`
   (default `main`).
3. For each file: `api.get(file).await` → the hf-cache path; copy it into
   `<managed_root>/<id>/<file>` (create the dir; hard-link with copy fallback to save disk).
4. Build a `ModelEntry { id, name, format, source: Managed { path: <managed>/<id>/<files[0]> },
   sha256: None, size_bytes: fs len of the model file }` and `managed.add(entry.clone())`.
5. Return the entry. The engine adapters then resolve it via the normal `ManagedResolver`.

Notes:
- GGUF: `files = ["<quant>.gguf"]`, one file. ONNX: `files = ["model.onnx", "tokenizer.json"]`
  — the `ort` adapter resolves the sibling `tokenizer.json` next to the `.onnx`.
- Idempotence: hf-hub caches, and the copy is skipped if the managed target already exists
  with the right size.

## 4. Build sequence
1. Add `hf-hub` dep + `hf-download` feature to `gateway-embedded/Cargo.toml`.
2. `registry/pull.rs`: `PullSpec`/`PullError`/`ModelPuller`/`HfHubPuller` (all under
   `#[cfg(feature = "hf-download")]`). Re-export from `registry/mod.rs` under the same gate.
3. Tests: unit-test the managed-path + `ModelEntry` construction + `EmptySpec` with fixtures
   (no network); a real end-to-end pull of a tiny public GGUF is `#[ignore]`.
4. Gate: default build unaffected; `cargo build/test -p gateway-embedded --features hf-download`
   green; clippy `-D warnings` clean.

## 5. Non-goals
- No safetensors, no auto-quant-selection, no GGUF→ONNX conversion. No UI (consumer drives
  `PullSpec`). No quota/subscription (AUTH).
