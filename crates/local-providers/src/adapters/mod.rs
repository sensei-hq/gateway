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
