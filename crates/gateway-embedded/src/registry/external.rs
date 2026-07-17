//! In-memory resolver for user-pointed model paths (no filesystem index).
//!
//! Callers register external model paths at runtime — e.g. from app settings
//! when the user picks an arbitrary GGUF. The file itself stays where the
//! user put it; the registry only records the pointer. To take ownership of
//! the file ("Move to library"), promote the entry to a Managed source via
//! the [`super::ManagedResolver`] — that is a higher-level operation outside
//! this resolver.

use crate::registry::{ModelEntry, ModelResolver, ModelSource, ResolveError};
use std::collections::HashMap;
use tokio::sync::RwLock;

/// Resolver backed by an in-memory map of `id → ModelEntry`.
///
/// Every entry registered here must have `ModelSource::External { .. }`;
/// the resolver panics in debug builds if a foreign source kind is added,
/// since that would let an external entry masquerade as managed/Ollama.
#[derive(Default)]
pub struct ExternalResolver {
    entries: RwLock<HashMap<String, ModelEntry>>,
}

impl ExternalResolver {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an external model. Replaces any prior entry with the same id.
    pub async fn register(&self, entry: ModelEntry) {
        debug_assert!(
            matches!(entry.source, ModelSource::External { .. }),
            "ExternalResolver only accepts External source entries; got {:?}",
            entry.source
        );
        self.entries.write().await.insert(entry.id.clone(), entry);
    }

    /// Remove and return an external entry, or `None` if it wasn't registered.
    pub async fn unregister(&self, id: &str) -> Option<ModelEntry> {
        self.entries.write().await.remove(id)
    }
}

#[async_trait::async_trait]
impl ModelResolver for ExternalResolver {
    async fn resolve(&self, id: &str) -> Result<Option<ModelEntry>, ResolveError> {
        Ok(self.entries.read().await.get(id).cloned())
    }

    async fn list(&self) -> Result<Vec<ModelEntry>, ResolveError> {
        Ok(self.entries.read().await.values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::ModelFormat;
    use std::path::PathBuf;

    fn ext_entry(id: &str, path: &str) -> ModelEntry {
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
    async fn resolve_returns_none_when_unregistered() {
        let r = ExternalResolver::new();
        assert!(r.resolve("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn register_then_resolve_returns_the_entry() {
        let r = ExternalResolver::new();
        r.register(ext_entry("foo", "/x/foo.gguf")).await;
        let got = r.resolve("foo").await.unwrap().expect("registered");
        assert_eq!(got.id, "foo");
        assert_eq!(got.source.path(), std::path::Path::new("/x/foo.gguf"));
    }

    #[tokio::test]
    async fn register_replaces_existing_id() {
        let r = ExternalResolver::new();
        r.register(ext_entry("foo", "/v1/foo.gguf")).await;
        r.register(ext_entry("foo", "/v2/foo.gguf")).await;
        let got = r.resolve("foo").await.unwrap().unwrap();
        assert_eq!(got.source.path(), std::path::Path::new("/v2/foo.gguf"));
    }

    #[tokio::test]
    async fn unregister_removes_the_entry_and_returns_it() {
        let r = ExternalResolver::new();
        r.register(ext_entry("foo", "/x/foo.gguf")).await;
        let removed = r.unregister("foo").await.expect("present");
        assert_eq!(removed.id, "foo");
        assert!(r.resolve("foo").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_returns_all_registered_entries() {
        let r = ExternalResolver::new();
        r.register(ext_entry("a", "/a.gguf")).await;
        r.register(ext_entry("b", "/b.gguf")).await;
        let mut ids: Vec<_> = r.list().await.unwrap().into_iter().map(|e| e.id).collect();
        ids.sort();
        assert_eq!(ids, vec!["a", "b"]);
    }
}
