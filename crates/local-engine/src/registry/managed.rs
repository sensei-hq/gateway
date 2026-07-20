//! Resolver for sensei-owned models under a managed root directory.
//!
//! State lives in an `index.json` at the root, structured as
//! `{ "version": 1, "models": [ModelEntry, ...] }`. Reads load the file
//! fresh on each call (no in-memory cache — the file is tiny, and avoiding
//! cache state simplifies reasoning across processes). Writes are
//! serialised by a single-process mutex and use write-tmp-then-rename for
//! atomicity, so partial writes can't corrupt the index.
//!
//! This resolver is read-write: `add` / `remove` mutate the index. The
//! [`ModelResolver`] trait methods are the read side.

use crate::registry::{ModelEntry, ModelResolver, ModelSource, ResolveError};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;

const INDEX_FILE: &str = "index.json";
const INDEX_TMP: &str = "index.json.tmp";
const CURRENT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Index {
    version: u32,
    models: Vec<ModelEntry>,
}

impl Default for Index {
    fn default() -> Self {
        Self {
            version: CURRENT_VERSION,
            models: Vec::new(),
        }
    }
}

/// Resolver backed by a managed directory + JSON index file.
pub struct ManagedResolver {
    root: PathBuf,
    write_lock: Mutex<()>,
}

impl ManagedResolver {
    /// Create a resolver rooted at the given directory. The directory and
    /// `index.json` are created on first write; reads against a non-existent
    /// root return an empty model list (treated as "no managed models yet").
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            write_lock: Mutex::new(()),
        }
    }

    /// Path to the JSON index file.
    pub fn index_path(&self) -> PathBuf {
        self.root.join(INDEX_FILE)
    }

    /// Path to the managed root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    async fn load(&self) -> Result<Index, ResolveError> {
        match tokio::fs::read_to_string(self.index_path()).await {
            Ok(contents) => {
                serde_json::from_str(&contents).map_err(|e| ResolveError::InvalidManifest {
                    path: self.index_path(),
                    message: e.to_string(),
                })
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Index::default()),
            Err(err) => Err(err.into()),
        }
    }

    async fn save_atomic(&self, index: &Index) -> Result<(), ResolveError> {
        tokio::fs::create_dir_all(&self.root).await?;
        let tmp = self.root.join(INDEX_TMP);
        let json = serde_json::to_string_pretty(index)?;
        tokio::fs::write(&tmp, json).await?;
        tokio::fs::rename(&tmp, self.index_path()).await?;
        Ok(())
    }

    /// Register a managed model. Replaces any prior entry with the same id.
    /// The caller is responsible for placing the model file under the
    /// managed root before calling this; the resolver only records metadata.
    pub async fn add(&self, entry: ModelEntry) -> Result<(), ResolveError> {
        debug_assert!(
            matches!(entry.source, ModelSource::Managed { .. }),
            "ManagedResolver only accepts Managed source entries; got {:?}",
            entry.source
        );
        let _guard = self.write_lock.lock().await;
        let mut index = self.load().await?;
        index.models.retain(|m| m.id != entry.id);
        index.models.push(entry);
        self.save_atomic(&index).await
    }

    /// Remove a model by id. Returns `true` if it existed. Does not delete
    /// the underlying file — the caller decides what to do with the bytes.
    pub async fn remove(&self, id: &str) -> Result<bool, ResolveError> {
        let _guard = self.write_lock.lock().await;
        let mut index = self.load().await?;
        let before = index.models.len();
        index.models.retain(|m| m.id != id);
        let removed = index.models.len() < before;
        if removed {
            self.save_atomic(&index).await?;
        }
        Ok(removed)
    }
}

#[async_trait::async_trait]
impl ModelResolver for ManagedResolver {
    async fn resolve(&self, id: &str) -> Result<Option<ModelEntry>, ResolveError> {
        Ok(self.load().await?.models.into_iter().find(|m| m.id == id))
    }

    async fn list(&self) -> Result<Vec<ModelEntry>, ResolveError> {
        Ok(self.load().await?.models)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::ModelFormat;
    use tempfile::TempDir;

    fn managed_entry(root: &Path, id: &str) -> ModelEntry {
        ModelEntry {
            id: id.into(),
            name: id.into(),
            format: ModelFormat::Gguf,
            source: ModelSource::Managed {
                path: root.join(format!("{id}.gguf")),
            },
            sha256: None,
            size_bytes: None,
        }
    }

    #[tokio::test]
    async fn resolve_returns_none_when_index_file_missing() {
        let dir = TempDir::new().unwrap();
        let r = ManagedResolver::new(dir.path());
        assert!(r.resolve("anything").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_returns_empty_when_index_file_missing() {
        let dir = TempDir::new().unwrap();
        let r = ManagedResolver::new(dir.path());
        assert!(r.list().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn add_then_resolve_returns_the_entry() {
        let dir = TempDir::new().unwrap();
        let r = ManagedResolver::new(dir.path());
        r.add(managed_entry(dir.path(), "foo")).await.unwrap();
        let got = r.resolve("foo").await.unwrap().expect("present");
        assert_eq!(got.id, "foo");
    }

    #[tokio::test]
    async fn add_with_existing_id_replaces_the_entry() {
        let dir = TempDir::new().unwrap();
        let r = ManagedResolver::new(dir.path());
        let mut first = managed_entry(dir.path(), "foo");
        first.name = "v1".into();
        let mut second = managed_entry(dir.path(), "foo");
        second.name = "v2".into();

        r.add(first).await.unwrap();
        r.add(second).await.unwrap();

        let got = r.resolve("foo").await.unwrap().unwrap();
        assert_eq!(got.name, "v2");
        assert_eq!(r.list().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn remove_returns_true_for_present_id_and_false_for_missing() {
        let dir = TempDir::new().unwrap();
        let r = ManagedResolver::new(dir.path());
        r.add(managed_entry(dir.path(), "foo")).await.unwrap();
        assert!(r.remove("foo").await.unwrap());
        assert!(!r.remove("foo").await.unwrap());
        assert!(r.resolve("foo").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn index_persists_across_resolver_instances() {
        let dir = TempDir::new().unwrap();
        {
            let r = ManagedResolver::new(dir.path());
            r.add(managed_entry(dir.path(), "a")).await.unwrap();
            r.add(managed_entry(dir.path(), "b")).await.unwrap();
        }
        let r2 = ManagedResolver::new(dir.path());
        let mut ids: Vec<_> = r2.list().await.unwrap().into_iter().map(|e| e.id).collect();
        ids.sort();
        assert_eq!(ids, vec!["a", "b"]);
    }

    #[tokio::test]
    async fn atomic_write_leaves_no_tmp_file_after_success() {
        let dir = TempDir::new().unwrap();
        let r = ManagedResolver::new(dir.path());
        r.add(managed_entry(dir.path(), "a")).await.unwrap();
        let tmp = dir.path().join(INDEX_TMP);
        assert!(!tmp.exists(), "tmp file should be renamed away");
        assert!(r.index_path().exists(), "index.json must exist");
    }

    #[tokio::test]
    async fn invalid_index_json_surfaces_as_invalid_manifest_error() {
        let dir = TempDir::new().unwrap();
        tokio::fs::write(dir.path().join(INDEX_FILE), b"{ not valid json")
            .await
            .unwrap();
        let r = ManagedResolver::new(dir.path());
        match r.resolve("foo").await.unwrap_err() {
            ResolveError::InvalidManifest { path, .. } => {
                assert_eq!(path, dir.path().join(INDEX_FILE));
            }
            other => panic!("expected InvalidManifest, got {other:?}"),
        }
    }
}
