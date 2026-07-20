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
#[cfg(feature = "hf-download")]
pub mod pull;

pub use external::ExternalResolver;
pub use managed::ManagedResolver;
pub use ollama::OllamaResolver;
#[cfg(feature = "hf-download")]
pub use pull::{FitReport, HfHubPuller, ModelPuller, PullError, PullSpec, PullingResolver};

// Vocabulary lives in the kernel; re-export so `super::ModelEntry` etc. in the
// resolver submodules keep resolving, and downstream keeps its paths.
pub use kernel::registry::{ModelEntry, ModelFormat, ModelResolver, ModelSource, ResolveError};

use std::collections::HashSet;
use std::sync::Arc;

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
    use std::path::{Path, PathBuf};

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
