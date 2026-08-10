//! The process-wide llama.cpp backend singleton (`shared_backend`) and the
//! dedup-by-path model cache (`model_cache` / `cached_model`) so repeated loads
//! of the same GGUF share one `Arc<LlamaModel>`. Split out of `super` for
//! readability.

use std::sync::Arc;

use kernel::types::error::GatewayError;

use super::*;

/// Process-wide singleton [`LlamaBackend`].
///
/// `LlamaBackend::init()` is allowed to be called only once per
/// process; a second call returns `BackendAlreadyInitialized`. Callers
/// that need a backend (the test suite, sensei's `register_llama_cpp_*`
/// helpers, anything else that loads multiple LlamaCpp adapters) should
/// go through this function instead of calling `init()` directly so
/// they get the same `Arc<LlamaBackend>` back every time.
///
/// The first-call error is cached in the `OnceLock`, so subsequent
/// callers see the same error rather than blowing up on a misleading
/// "already initialized" message.
pub fn shared_backend() -> Result<Arc<LlamaBackend>, GatewayError> {
    use std::sync::OnceLock;
    static BACKEND: OnceLock<Result<Arc<LlamaBackend>, String>> = OnceLock::new();
    let cached = BACKEND.get_or_init(|| {
        LlamaBackend::init()
            .map(Arc::new)
            .map_err(|e| format!("LlamaBackend::init: {e}"))
    });
    cached.clone().map_err(|e| GatewayError::ProviderError {
        adapter: "llama-cpp".into(),
        message: e,
        status: None,
    })
}

/// Process-wide cache of loaded [`LlamaModel`] weights, keyed by the
/// canonicalised on-disk path. Entries are `Weak<LlamaModel>` so a
/// model that no [`LlamaCppAdapter`] is holding gets dropped and the
/// next [`cached_model`] call re-reads the file. Held in a
/// `RwLock` so the common path (cache hit) only takes a read lock.
fn model_cache() -> &'static std::sync::RwLock<
    std::collections::HashMap<std::path::PathBuf, std::sync::Weak<LlamaModel>>,
> {
    use std::sync::OnceLock;
    static CACHE: OnceLock<
        std::sync::RwLock<
            std::collections::HashMap<std::path::PathBuf, std::sync::Weak<LlamaModel>>,
        >,
    > = OnceLock::new();
    CACHE.get_or_init(|| std::sync::RwLock::new(std::collections::HashMap::new()))
}

/// Get an `Arc<LlamaModel>` for the GGUF at `path`, loading from disk
/// only on a cache miss.
///
/// The key is the path as given. Two callers that pass the same
/// `&Path` reuse the same model; symlinks or different relative paths
/// that point at the same file currently get separate cache entries.
/// `canonicalize` would be safer, but it requires the file to exist
/// at lookup time and would add an `std::fs` round-trip per call —
/// not worth it for the expected usage pattern (a handful of adapters
/// per process).
///
/// Loading is protected with the write-lock held so a thundering
/// herd doesn't race to read the same multi-GB file. A second caller
/// that arrives while the first is loading blocks on the write lock,
/// then sees the freshly-inserted entry.
pub(super) fn cached_model(
    backend: &Arc<LlamaBackend>,
    path: &std::path::Path,
) -> Result<Arc<LlamaModel>, String> {
    // Cheap-path: a read lock + an upgrade. Returns immediately when
    // a live model is already cached.
    {
        let cache = model_cache()
            .read()
            .map_err(|e| format!("model cache poisoned: {e}"))?;
        if let Some(weak) = cache.get(path)
            && let Some(arc) = weak.upgrade()
        {
            return Ok(arc);
        }
    }
    // Slow path: take the write lock. Re-check inside the lock —
    // another thread may have populated the entry while we were
    // upgrading the lock.
    let mut cache = model_cache()
        .write()
        .map_err(|e| format!("model cache poisoned: {e}"))?;
    if let Some(weak) = cache.get(path)
        && let Some(arc) = weak.upgrade()
    {
        return Ok(arc);
    }
    let params = LlamaModelParams::default();
    let model = LlamaModel::load_from_file(backend.as_ref(), path, &params)
        .map_err(|e| format!("model load: {e}"))?;
    let arc = Arc::new(model);
    cache.insert(path.to_path_buf(), Arc::downgrade(&arc));
    Ok(arc)
}
