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

## 4a. Phase 2 — resource pre-flight + pull-on-missing (requested 2026-07-18)

Two additions on top of the base puller:

### Resource pre-flight (`check_fit`) — fail before a doomed download
A model must not be downloaded/loaded if it can't run on the machine (e.g. a ~30 GB
model on 8 GB RAM, or insufficient disk).
- **Model size** is obtained from HF **without downloading** — hf-hub repo info /
  siblings sizes, or a `HEAD` on the resolve URL (`Content-Length`). Sum across `files`.
- **Machine resources** via the `sysinfo` crate (behind `hf-download`): total + available
  RAM, and free disk on the filesystem holding the managed root.
- **Fit rule (heuristic, documented as approximate):**
  - `need_disk = size * 1.05`; error `WontFit` if `> available_disk`.
  - `need_ram ≈ size * 1.2` (GGUF loads ≈ file size resident + KV/context margin); error
    `WontFit` if `> total_ram`. RAM need truly depends on quant + context length, so this
    is a *gross* guard that catches the "won't remotely fit" cases, not a precise budget.
- API: `pub struct FitReport { model_bytes, disk_available, ram_total, ram_available, fits, reason: Option<String> }`
  and `async fn check_fit(&self, spec: &PullSpec) -> Result<FitReport, PullError>`.
  **The guard lives INSIDE `pull` (mandatory, not an optional caller step):** `pull` calls
  `check_fit` FIRST and returns `PullError::WontFit(String)` **before any network I/O / any
  download** (no point fetching a 30 GB file that can't run), with a clear message, e.g. *"model 'X' is ~18.0 GB and needs ~21.6 GB RAM; this
  machine has 8.0 GB — not usable"* or *"insufficient disk for 'X': need 18.9 GB, 5.2 GB free
  at <managed root>"*. `check_fit` is public so a UI can pre-check before offering a pull.
- New `PullError::WontFit(String)` variant.

### Pull-on-missing (`PullingResolver`) — config-driven fetch on first use
A `ModelResolver` that wraps `(inner: ManagedResolver, puller: HfHubPuller, specs:
HashMap<String /*id*/, PullSpec>)`:
- `resolve(id)`: if `inner` already has it → return it; else if a `PullSpec` is registered
  for `id` → run `check_fit` then `pull` (which registers it into the managed store) and
  return the entry; else `Ok(None)`.
- `WontFit`/download errors surface as `ResolveError` (add a `ResolveError` variant, or map
  to the existing error type) so the caller gets the actionable "won't run on this machine"
  message rather than a silent miss.
- The `specs` map IS the "config": the consumer/daemon populates it from the operator's
  model config (each local model that has an HF source), so a configured-but-absent model
  is fetched — and resource-checked — the first time an embedded engine asks for it.

Build order: land the base puller (Phase 1) → add `sysinfo` + `check_fit` + `WontFit` →
add `PullingResolver`. All under `#[cfg(feature = "hf-download")]`.

## 5. Non-goals
- No safetensors, no auto-quant-selection, no GGUF→ONNX conversion. No UI (consumer drives
  `PullSpec`). No quota/subscription (AUTH).
