//! Model registry — resolves a stable model id to an on-disk path,
//! drawing from any of three sources looked up in order:
//!
//! 1. [`ModelSource::Managed`] — files sensei owns under `~/.sensei/models/`.
//! 2. [`ModelSource::Ollama`]  — read-through into a local `~/.ollama/models/`
//!    blob store. Never written to (the Ollama daemon owns its store).
//! 3. [`ModelSource::External`] — arbitrary user-pointed paths, linked in
//!    place; only moved into managed storage on explicit user action.
//!
//! Resolvers compose via [`ChainedResolver`]; the first one to return
//! `Some` wins.

pub mod external;
pub mod managed;
pub mod ollama;

pub use external::ExternalResolver;
pub use managed::ManagedResolver;
pub use ollama::OllamaResolver;

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
}

/// Looks up models by stable id from one storage backend.
///
/// Multiple resolvers compose to form the registry; the first one to return
/// `Some` wins. Each implementation lives in a child module of `registry`
/// (e.g. `registry::managed`, `registry::ollama`, `registry::external`).
#[async_trait::async_trait]
pub trait ModelResolver: Send + Sync {
    /// Resolve a model id to its [`ModelEntry`], or `Ok(None)` if this
    /// resolver doesn't know about it. `Err` is reserved for backend failures
    /// (broken manifest, I/O error) — "not found" is `Ok(None)`.
    async fn resolve(&self, id: &str) -> Result<Option<ModelEntry>, ResolveError>;

    /// Enumerate every model this resolver currently knows about.
    async fn list(&self) -> Result<Vec<ModelEntry>, ResolveError>;
}

/// Composes multiple [`ModelResolver`]s and dispatches lookups in the order
/// they were added: the first resolver that returns `Some` wins, satisfying
/// the registry's Managed → Ollama → External precedence.
///
/// `list()` returns the union, deduplicated by id (earlier resolvers shadow
/// later ones).
#[derive(Default, Clone)]
pub struct ChainedResolver {
    resolvers: Vec<Arc<dyn ModelResolver>>,
}

impl ChainedResolver {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a resolver to the lookup chain.
    pub fn push(mut self, resolver: Arc<dyn ModelResolver>) -> Self {
        self.resolvers.push(resolver);
        self
    }
}

#[async_trait::async_trait]
impl ModelResolver for ChainedResolver {
    async fn resolve(&self, id: &str) -> Result<Option<ModelEntry>, ResolveError> {
        for resolver in &self.resolvers {
            if let Some(entry) = resolver.resolve(id).await? {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    async fn list(&self) -> Result<Vec<ModelEntry>, ResolveError> {
        let mut all = Vec::new();
        for resolver in &self.resolvers {
            all.extend(resolver.list().await?);
        }
        let mut seen = HashSet::new();
        all.retain(|entry| seen.insert(entry.id.clone()));
        Ok(all)
    }
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

    fn ext(id: &str, path: &str) -> ModelEntry {
        ModelEntry {
            id: id.into(),
            name: id.into(),
            format: ModelFormat::Gguf,
            source: ModelSource::External {
                path: PathBuf::from(path),
            },
            sha256: None,
            size_bytes: None,
        }
    }

    #[tokio::test]
    async fn chained_resolver_returns_first_match_in_order() {
        let earlier = ExternalResolver::new();
        earlier.register(ext("shared", "/earlier.gguf")).await;
        let later = ExternalResolver::new();
        later.register(ext("shared", "/later.gguf")).await;

        let chain = ChainedResolver::new()
            .push(Arc::new(earlier))
            .push(Arc::new(later));

        let got = chain.resolve("shared").await.unwrap().unwrap();
        assert_eq!(got.source.path(), Path::new("/earlier.gguf"));
    }

    #[tokio::test]
    async fn chained_resolver_falls_through_when_earlier_returns_none() {
        let earlier = ExternalResolver::new();
        // earlier has no entries
        let later = ExternalResolver::new();
        later.register(ext("only-in-later", "/x.gguf")).await;

        let chain = ChainedResolver::new()
            .push(Arc::new(earlier))
            .push(Arc::new(later));

        let got = chain.resolve("only-in-later").await.unwrap().unwrap();
        assert_eq!(got.source.path(), Path::new("/x.gguf"));
    }

    #[tokio::test]
    async fn chained_resolver_list_dedupes_by_id_keeping_earlier() {
        let earlier = ExternalResolver::new();
        earlier.register(ext("dup", "/earlier.gguf")).await;
        earlier.register(ext("only-earlier", "/e.gguf")).await;
        let later = ExternalResolver::new();
        later.register(ext("dup", "/later.gguf")).await;
        later.register(ext("only-later", "/l.gguf")).await;

        let chain = ChainedResolver::new()
            .push(Arc::new(earlier))
            .push(Arc::new(later));

        let entries = chain.list().await.unwrap();
        assert_eq!(entries.len(), 3, "dup should appear only once");
        let dup = entries.iter().find(|e| e.id == "dup").unwrap();
        assert_eq!(dup.source.path(), Path::new("/earlier.gguf"));
    }

    #[tokio::test]
    async fn empty_chain_returns_none_and_empty_list() {
        let chain = ChainedResolver::new();
        assert!(chain.resolve("anything").await.unwrap().is_none());
        assert!(chain.list().await.unwrap().is_empty());
    }
}
