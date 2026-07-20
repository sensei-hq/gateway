//! Model-registry vocabulary — the shared types a resolver produces and an
//! adapter consumes: a stable model id mapped to on-disk bytes, plus the
//! [`ModelResolver`] trait that backends implement. The concrete resolvers
//! (managed / Ollama / external / pulling) live in the `local-engine` crate.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// On-disk encoding of a model file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelFormat {
    /// GGUF container (llama.cpp ecosystem).
    Gguf,
    /// ONNX graph.
    Onnx,
    /// HuggingFace safetensors.
    Safetensors,
}

/// Where the bytes of a registered model live and who owns them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ModelSource {
    /// Owned by sensei under `~/.sensei/models/`. Sensei may delete, replace,
    /// or GC unreferenced files.
    Managed { path: PathBuf },

    /// Read-through view of a model already pulled into a local Ollama cache.
    /// The Ollama daemon owns the bytes; we only read them. The
    /// [`Self::Ollama::manifest`] field is the manifest file we walked to
    /// discover the blob, kept for diagnostics and re-resolution.
    Ollama {
        manifest: PathBuf,
        blob_digest: String,
        blob_path: PathBuf,
    },

    /// Arbitrary path supplied by the user (e.g. a hand-downloaded GGUF).
    /// Linked in place — never moved without an explicit "Move to library"
    /// action that promotes the entry to [`Self::Managed`].
    External { path: PathBuf },
}

impl ModelSource {
    /// The on-disk path to the model bytes, regardless of which source kind
    /// this is. Suitable for mmap or open() calls.
    pub fn path(&self) -> &Path {
        match self {
            Self::Managed { path } => path,
            Self::Ollama { blob_path, .. } => blob_path,
            Self::External { path } => path,
        }
    }
}

/// A registered model. The `id` is stable across sensei versions and is what
/// adapters and workflows refer to; the [`Self::source`] field tells the
/// registry how to find the bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelEntry {
    /// Stable sensei id (e.g. `"all-minilm-l6-v2-f16"`).
    pub id: String,
    /// Display name for UI.
    pub name: String,
    /// On-disk format.
    pub format: ModelFormat,
    /// Where the bytes are.
    pub source: ModelSource,
    /// Optional content hash for integrity verification on load.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    /// File size in bytes, if known at registration time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}

/// Errors surfaced by a [`ModelResolver`].
#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    /// An I/O failure walking the resolver's storage.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// A manifest or registry index file was unreadable or malformed.
    #[error("invalid manifest at {path}: {message}")]
    InvalidManifest { path: PathBuf, message: String },

    /// A JSON parse failure (e.g. while reading an Ollama manifest).
    #[error("json: {0}")]
    Serde(#[from] serde_json::Error),

    /// A model pull (download or resource pre-flight) failed while resolving
    /// through a pulling resolver. Carries the actionable message (e.g. "won't
    /// fit on this machine") so the caller sees why the model is absent rather
    /// than a silent miss.
    #[error("pull: {0}")]
    Pull(String),
}

/// Looks up models by stable id from one storage backend.
///
/// Multiple resolvers compose to form the registry (via the `local-engine`
/// crate's `ChainedResolver`); the first one to return `Some` wins.
#[async_trait::async_trait]
pub trait ModelResolver: Send + Sync {
    /// Resolve a model id to its [`ModelEntry`], or `Ok(None)` if this
    /// resolver doesn't know about it. `Err` is reserved for backend failures
    /// (broken manifest, I/O error) — "not found" is `Ok(None)`.
    async fn resolve(&self, id: &str) -> Result<Option<ModelEntry>, ResolveError>;

    /// Enumerate every model this resolver currently knows about.
    async fn list(&self) -> Result<Vec<ModelEntry>, ResolveError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_source_path_returns_the_bytes_path_for_each_variant() {
        let managed = ModelSource::Managed {
            path: PathBuf::from("/home/u/.sensei/models/m.gguf"),
        };
        assert_eq!(managed.path(), Path::new("/home/u/.sensei/models/m.gguf"));

        let external = ModelSource::External {
            path: PathBuf::from("/downloads/custom.onnx"),
        };
        assert_eq!(external.path(), Path::new("/downloads/custom.onnx"));

        let ollama = ModelSource::Ollama {
            manifest: PathBuf::from("/home/u/.ollama/models/manifests/all-minilm/latest"),
            blob_digest: "797b70c4edf85907fe0a49eb85811256f65fa0f7bf52166b147fd16be2be4662".into(),
            blob_path: PathBuf::from(
                "/home/u/.ollama/models/blobs/sha256-797b70c4edf85907fe0a49eb85811256f65fa0f7bf52166b147fd16be2be4662",
            ),
        };
        assert!(
            ollama
                .path()
                .to_string_lossy()
                .ends_with("797b70c4edf85907fe0a49eb85811256f65fa0f7bf52166b147fd16be2be4662")
        );
    }

    #[test]
    fn model_entry_roundtrips_through_json_preserving_source_kind() {
        let entry = ModelEntry {
            id: "all-minilm-l6-v2-f16".into(),
            name: "MiniLM L6 v2 (F16 GGUF)".into(),
            format: ModelFormat::Gguf,
            source: ModelSource::Managed {
                path: PathBuf::from("/home/u/.sensei/models/all-minilm-l6-v2-f16.gguf"),
            },
            sha256: Some("deadbeef".into()),
            size_bytes: Some(45_949_216),
        };
        let json = serde_json::to_string(&entry).expect("serialize");
        let parsed: ModelEntry = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed, entry);
    }

    #[test]
    fn model_source_serializes_kind_as_external_tag() {
        let src = ModelSource::External {
            path: PathBuf::from("/x.gguf"),
        };
        let v: serde_json::Value = serde_json::to_value(&src).unwrap();
        assert_eq!(v["kind"], "external");
    }

    #[test]
    fn ollama_source_carries_manifest_digest_and_blob_path() {
        let src = ModelSource::Ollama {
            manifest: PathBuf::from("/m"),
            blob_digest: "abc123".into(),
            blob_path: PathBuf::from("/blobs/sha256-abc123"),
        };
        let json = serde_json::to_string(&src).unwrap();
        let parsed: ModelSource = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, src);
    }
}
