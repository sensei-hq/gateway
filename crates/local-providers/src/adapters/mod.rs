//! In-process inference adapters that implement the gateway's capability
//! traits (`Model` + `ChatModel`/`EmbedModel`).
//!
//! Each engine sits behind its own cargo feature so that callers only pay
//! the build cost (C++ toolchain for `llama-cpp-2`, ORT runtime download
//! for `ort`, etc.) for the engines they actually use.

#[cfg(feature = "llama-cpp")]
pub mod llama_cpp;

#[cfg(feature = "llama-cpp")]
pub use llama_cpp::{LlamaCppAdapter, LlamaCppConfig, LlamaCppMode, shared_backend};

#[cfg(feature = "llama-cpp")]
pub mod embedded_llama;

#[cfg(feature = "llama-cpp")]
pub use embedded_llama::EmbeddedLlamaAdapter;

#[cfg(feature = "fastembed")]
pub mod fastembed;

#[cfg(feature = "fastembed")]
pub use fastembed::{FastembedAdapter, FastembedConfig};

#[cfg(feature = "ort")]
pub mod ort;

#[cfg(feature = "ort")]
pub use self::ort::{OrtAdapter, OrtConfig, OrtPoolingStrategy};

#[cfg(feature = "kokoro")]
pub mod kokoro;

#[cfg(feature = "kokoro")]
pub use kokoro::{KokoroAdapter, KokoroConfig, KokoroLang};

/// Rejects a request pinned to a model id this adapter doesn't serve. Every
/// embedded adapter enforces the same contract: `req.model` is either unset
/// (the request accepts whatever this adapter is configured for) or it must
/// match `model_id` exactly.
///
/// Not gated to a single feature (unlike the adapter modules above) since
/// every one of them calls it; gated on "any engine feature" so a
/// no-features build doesn't carry a dead `pub(crate)` fn.
#[cfg(any(
    feature = "llama-cpp",
    feature = "ort",
    feature = "kokoro",
    feature = "fastembed"
))]
pub(crate) fn reject_model_mismatch(
    adapter_id: &str,
    model_id: &str,
    requested: Option<&str>,
) -> Result<(), kernel::types::error::GatewayError> {
    if let Some(requested) = requested
        && requested != model_id
    {
        return Err(kernel::types::error::GatewayError::ModelUnavailable {
            adapter: adapter_id.to_string(),
            model: requested.to_string(),
        });
    }
    Ok(())
}

/// Runs `embed`, wrapping its dense vectors in an [`EmbedResponse`] — after
/// rejecting a request pinned to a model this adapter doesn't serve. Shared
/// by every embed-capable adapter's `EmbedModel::embed`: each supplies only
/// its own inherent `embed(&[String])` as the closure, since the model-check
/// and response-wrapping around it is otherwise identical (and was flagged
/// as near-duplicate code between `fastembed.rs` and `ort.rs`).
///
/// [`EmbedResponse`]: kernel::types::io::EmbedResponse
#[cfg(any(feature = "llama-cpp", feature = "ort", feature = "fastembed"))]
pub(crate) fn embed_response(
    adapter_id: &str,
    model_id: &str,
    requested: Option<&str>,
    embed: impl FnOnce() -> Result<Vec<Vec<f32>>, kernel::types::error::GatewayError>,
) -> Result<kernel::types::io::EmbedResponse, kernel::types::error::GatewayError> {
    reject_model_mismatch(adapter_id, model_id, requested)?;
    let embeddings = embed()?;
    Ok(kernel::types::io::EmbedResponse {
        embeddings,
        usage: None,
        degraded: false,
    })
}
