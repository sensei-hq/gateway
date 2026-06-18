//! Read-through resolver for models pulled into a local Ollama cache.
//!
//! Sensei never writes into Ollama's store — the daemon owns it, manages its
//! own integrity, and may GC blobs that aren't referenced by a manifest.
//! All operations here are read-only walks over
//! `<root>/manifests/<registry>/<namespace>/<name>/<tag>` files; each manifest
//! is JSON with a `layers` array, and the layer whose `mediaType` is
//! `application/vnd.ollama.image.model` carries the model bytes (referenced
//! by digest into `<root>/blobs/sha256-<digest>`).
//!
//! IDs are constructed from manifest paths using Ollama's display
//! conventions: `library/<name>` and `registry.ollama.ai` are implicit and
//! stripped, so the user-visible id of the all-minilm:latest manifest under
//! the default registry is just `all-minilm`. Non-latest tags are preserved
//! as `name:tag`.
//!
//! GGUF is assumed for every model layer. Ollama doesn't ship ONNX or
//! safetensors today; if that changes, the format will need to be derived
//! by sniffing the blob's magic bytes.

use crate::registry::{ModelEntry, ModelFormat, ModelResolver, ModelSource, ResolveError};
use serde::Deserialize;
use std::path::{Path, PathBuf};

const MODEL_MEDIA_TYPE: &str = "application/vnd.ollama.image.model";
const DEFAULT_REGISTRY: &str = "registry.ollama.ai";
const DEFAULT_NAMESPACE: &str = "library";
const DEFAULT_TAG: &str = "latest";

#[derive(Debug, Deserialize)]
struct OllamaManifest {
    layers: Vec<OllamaLayer>,
}

#[derive(Debug, Deserialize)]
struct OllamaLayer {
    #[serde(rename = "mediaType")]
    media_type: String,
    digest: String,
    #[serde(default)]
    size: Option<u64>,
}

/// Read-through resolver over a local Ollama cache root (typically
/// `~/.ollama/models`).
pub struct OllamaResolver {
    root: PathBuf,
}

impl OllamaResolver {
    /// Create a resolver rooted at the given Ollama models directory.
    /// The directory does not need to exist; absent / unreadable trees
    /// resolve as "no models" rather than as errors.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The configured Ollama root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn manifests_dir(&self) -> PathBuf {
        self.root.join("manifests")
    }

    fn blob_path(&self, digest: &str) -> PathBuf {
        self.root
            .join("blobs")
            .join(format!("sha256-{}", digest.trim_start_matches("sha256:")))
    }
}

#[async_trait::async_trait]
impl ModelResolver for OllamaResolver {
    async fn resolve(&self, id: &str) -> Result<Option<ModelEntry>, ResolveError> {
        let target = canonical_id(id);
        // Walk and short-circuit on first match — a fresh user cache has
        // ~10s of manifests max, so this is fine.
        for manifest_path in walk_files(&self.manifests_dir()).await? {
            if let Some(entry) = self.read_manifest_entry(&manifest_path).await?
                && entry.id == target
            {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    async fn list(&self) -> Result<Vec<ModelEntry>, ResolveError> {
        let mut entries = Vec::new();
        for manifest_path in walk_files(&self.manifests_dir()).await? {
            if let Some(entry) = self.read_manifest_entry(&manifest_path).await? {
                entries.push(entry);
            }
        }
        Ok(entries)
    }
}

impl OllamaResolver {
    /// Returns `Ok(None)` for manifests we should skip (no model layer,
    /// missing blob); `Err` only for genuine corruption (unreadable file,
    /// invalid JSON).
    async fn read_manifest_entry(
        &self,
        manifest_path: &Path,
    ) -> Result<Option<ModelEntry>, ResolveError> {
        let contents = tokio::fs::read_to_string(manifest_path).await?;
        let manifest: OllamaManifest =
            serde_json::from_str(&contents).map_err(|e| ResolveError::InvalidManifest {
                path: manifest_path.to_path_buf(),
                message: e.to_string(),
            })?;

        let Some(model_layer) = manifest
            .layers
            .iter()
            .find(|layer| layer.media_type == MODEL_MEDIA_TYPE)
        else {
            // Manifest doesn't describe a model layer (license-only, etc.) —
            // not a corruption, just nothing to surface.
            return Ok(None);
        };

        let digest = model_layer.digest.trim_start_matches("sha256:").to_string();
        let blob = self.blob_path(&digest);
        if !blob.exists() {
            tracing::warn!(
                manifest = %manifest_path.display(),
                blob = %blob.display(),
                "Ollama manifest references missing blob; skipping",
            );
            return Ok(None);
        }

        let Some(id) = id_from_manifest_path(&self.manifests_dir(), manifest_path) else {
            return Ok(None);
        };

        Ok(Some(ModelEntry {
            id: id.clone(),
            name: id,
            format: ModelFormat::Gguf,
            source: ModelSource::Ollama {
                manifest: manifest_path.to_path_buf(),
                blob_digest: digest,
                blob_path: blob,
            },
            sha256: Some(model_layer.digest.trim_start_matches("sha256:").to_string()),
            size_bytes: model_layer.size,
        }))
    }
}

/// Recursively collect every regular file under `root`. Returns an empty
/// vec if `root` doesn't exist.
async fn walk_files(root: &Path) -> Result<Vec<PathBuf>, ResolveError> {
    let mut result = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(e) => e,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let file_type = entry.file_type().await?;
            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file() {
                result.push(path);
            }
        }
    }
    Ok(result)
}

/// Build a sensei id from `<root>/manifests/<registry>/<namespace>/<name>/<tag>`.
/// Implicit defaults are stripped: the default registry `registry.ollama.ai`
/// and namespace `library` are omitted, and `:latest` is dropped.
fn id_from_manifest_path(manifests_root: &Path, manifest_path: &Path) -> Option<String> {
    let rel = manifest_path.strip_prefix(manifests_root).ok()?;
    let parts: Vec<String> = rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    if parts.len() != 4 {
        // Unexpected layout — don't try to be clever.
        return None;
    }
    let (registry, namespace, name, tag) = (&parts[0], &parts[1], &parts[2], &parts[3]);

    let base = if registry == DEFAULT_REGISTRY && namespace == DEFAULT_NAMESPACE {
        name.clone()
    } else if registry == DEFAULT_REGISTRY {
        format!("{namespace}/{name}")
    } else {
        format!("{registry}/{namespace}/{name}")
    };

    Some(if tag == DEFAULT_TAG {
        base
    } else {
        format!("{base}:{tag}")
    })
}

/// Strip an implicit `:latest` suffix so callers can resolve `all-minilm`
/// or `all-minilm:latest` interchangeably.
fn canonical_id(id: &str) -> String {
    id.strip_suffix(":latest").unwrap_or(id).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Build a minimal valid Ollama cache layout with one manifest +
    /// matching blob. Returns the tempdir handle (must outlive the test).
    async fn write_cache(
        registry: &str,
        namespace: &str,
        name: &str,
        tag: &str,
        digest: &str,
        size: u64,
    ) -> TempDir {
        let dir = TempDir::new().unwrap();
        let manifest_dir = dir
            .path()
            .join("manifests")
            .join(registry)
            .join(namespace)
            .join(name);
        tokio::fs::create_dir_all(&manifest_dir).await.unwrap();

        let manifest = serde_json::json!({
            "layers": [
                {
                    "mediaType": MODEL_MEDIA_TYPE,
                    "digest": format!("sha256:{digest}"),
                    "size": size,
                },
                {
                    "mediaType": "application/vnd.ollama.image.license",
                    "digest": "sha256:0000",
                    "size": 12,
                },
            ]
        });
        tokio::fs::write(
            manifest_dir.join(tag),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .await
        .unwrap();

        let blobs_dir = dir.path().join("blobs");
        tokio::fs::create_dir_all(&blobs_dir).await.unwrap();
        tokio::fs::write(blobs_dir.join(format!("sha256-{digest}")), b"GGUF_FAKE_BYTES")
            .await
            .unwrap();

        dir
    }

    #[tokio::test]
    async fn list_returns_empty_when_root_does_not_exist() {
        let r = OllamaResolver::new("/nonexistent/sensei/test/path/ollama");
        assert!(r.list().await.unwrap().is_empty());
        assert!(r.resolve("anything").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_returns_empty_for_empty_manifests_tree() {
        let dir = TempDir::new().unwrap();
        tokio::fs::create_dir_all(dir.path().join("manifests"))
            .await
            .unwrap();
        let r = OllamaResolver::new(dir.path());
        assert!(r.list().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn library_default_tag_resolves_with_short_id() {
        let dir = write_cache("registry.ollama.ai", "library", "all-minilm", "latest", "abc123", 100).await;
        let r = OllamaResolver::new(dir.path());

        let by_short = r.resolve("all-minilm").await.unwrap().expect("present");
        assert_eq!(by_short.id, "all-minilm");
        assert_eq!(by_short.format, ModelFormat::Gguf);

        let by_full = r.resolve("all-minilm:latest").await.unwrap().expect("alias");
        assert_eq!(by_full.id, "all-minilm");

        match by_short.source {
            ModelSource::Ollama {
                blob_digest,
                blob_path,
                ..
            } => {
                assert_eq!(blob_digest, "abc123");
                assert!(blob_path.to_string_lossy().ends_with("sha256-abc123"));
            }
            other => panic!("expected Ollama source, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn non_default_tag_preserves_tag_in_id() {
        let dir = write_cache("registry.ollama.ai", "library", "qwen", "7b", "deadbeef", 0).await;
        let r = OllamaResolver::new(dir.path());
        let got = r.resolve("qwen:7b").await.unwrap().expect("present");
        assert_eq!(got.id, "qwen:7b");
    }

    #[tokio::test]
    async fn non_default_namespace_keeps_namespace_in_id() {
        let dir = write_cache("registry.ollama.ai", "myorg", "private", "latest", "feed", 0).await;
        let r = OllamaResolver::new(dir.path());
        let got = r.resolve("myorg/private").await.unwrap().expect("present");
        assert_eq!(got.id, "myorg/private");
    }

    #[tokio::test]
    async fn missing_blob_makes_manifest_invisible_not_an_error() {
        let dir = write_cache("registry.ollama.ai", "library", "ghost", "latest", "deadbeef", 0).await;
        // Remove the blob to simulate post-GC state.
        tokio::fs::remove_file(dir.path().join("blobs").join("sha256-deadbeef"))
            .await
            .unwrap();
        let r = OllamaResolver::new(dir.path());
        assert!(r.resolve("ghost").await.unwrap().is_none());
        assert!(r.list().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn manifest_without_model_layer_is_silently_skipped() {
        let dir = TempDir::new().unwrap();
        let manifest_dir = dir
            .path()
            .join("manifests")
            .join("registry.ollama.ai")
            .join("library")
            .join("template-only");
        tokio::fs::create_dir_all(&manifest_dir).await.unwrap();
        let manifest = serde_json::json!({
            "layers": [
                { "mediaType": "application/vnd.ollama.image.template", "digest": "sha256:0", "size": 1 }
            ]
        });
        tokio::fs::write(manifest_dir.join("latest"), serde_json::to_vec(&manifest).unwrap())
            .await
            .unwrap();
        let r = OllamaResolver::new(dir.path());
        assert!(r.list().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn corrupt_manifest_surfaces_as_invalid_manifest_error() {
        let dir = TempDir::new().unwrap();
        let manifest_dir = dir
            .path()
            .join("manifests")
            .join("registry.ollama.ai")
            .join("library")
            .join("broken");
        tokio::fs::create_dir_all(&manifest_dir).await.unwrap();
        tokio::fs::write(manifest_dir.join("latest"), b"{ broken json")
            .await
            .unwrap();
        let r = OllamaResolver::new(dir.path());
        match r.list().await.unwrap_err() {
            ResolveError::InvalidManifest { path, .. } => {
                assert!(path.ends_with("latest"));
            }
            other => panic!("expected InvalidManifest, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn list_returns_all_models_across_namespaces() {
        let dir = TempDir::new().unwrap();
        // library/foo:latest
        let m1 = dir
            .path()
            .join("manifests/registry.ollama.ai/library/foo");
        tokio::fs::create_dir_all(&m1).await.unwrap();
        tokio::fs::write(
            m1.join("latest"),
            serde_json::to_vec(&serde_json::json!({
                "layers": [{ "mediaType": MODEL_MEDIA_TYPE, "digest": "sha256:aaa", "size": 1 }]
            }))
            .unwrap(),
        )
        .await
        .unwrap();
        // myorg/bar:v2
        let m2 = dir
            .path()
            .join("manifests/registry.ollama.ai/myorg/bar");
        tokio::fs::create_dir_all(&m2).await.unwrap();
        tokio::fs::write(
            m2.join("v2"),
            serde_json::to_vec(&serde_json::json!({
                "layers": [{ "mediaType": MODEL_MEDIA_TYPE, "digest": "sha256:bbb", "size": 2 }]
            }))
            .unwrap(),
        )
        .await
        .unwrap();
        // Both blobs exist
        let blobs = dir.path().join("blobs");
        tokio::fs::create_dir_all(&blobs).await.unwrap();
        tokio::fs::write(blobs.join("sha256-aaa"), b"x").await.unwrap();
        tokio::fs::write(blobs.join("sha256-bbb"), b"x").await.unwrap();

        let r = OllamaResolver::new(dir.path());
        let mut ids: Vec<_> = r.list().await.unwrap().into_iter().map(|e| e.id).collect();
        ids.sort();
        assert_eq!(ids, vec!["foo", "myorg/bar:v2"]);
    }
}
