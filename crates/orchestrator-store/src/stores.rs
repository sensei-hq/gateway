//! In-memory [`ContentStore`] and [`ContextStore`] — the CAS + blackboard seams
//! the executor writes to (§7.4, §8). Both are `Arc`-shared and clonable, so a
//! resumed run rebuilds them from the journal; persistent implementations of the
//! same traits are a held-off layer.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use orchestrator_core::{
    ContentRef, ContentStore, ContextKey, ContextRef, ContextStore, Digest, OrchestratorError,
    Scope, digest_of,
};

/// In-memory content-addressed store: a shared `Digest → bytes` map. `put`
/// deduplicates (identical content → one entry); `get` is loud on a miss.
#[derive(Clone, Default)]
pub struct InMemoryContentStore {
    blobs: Arc<Mutex<HashMap<Digest, Vec<u8>>>>,
}

impl InMemoryContentStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl ContentStore for InMemoryContentStore {
    async fn put(&self, bytes: &[u8]) -> Result<Digest, OrchestratorError> {
        let digest = digest_of(bytes);
        self.blobs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(digest.clone())
            .or_insert_with(|| bytes.to_vec());
        Ok(digest)
    }

    async fn get(&self, digest: &Digest) -> Result<Vec<u8>, OrchestratorError> {
        self.blobs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(digest)
            .cloned()
            .ok_or_else(|| OrchestratorError::ContentDigestMiss(digest.0.clone()))
    }
}

/// In-memory scoped blackboard, backed by a [`ContentStore`] (values are stored
/// as content-addressed refs, never inline). Writes to an existing `(scope, key)`
/// are rejected; reads resolve `Node` → `Run`.
#[derive(Clone)]
pub struct InMemoryContextStore {
    content: Arc<dyn ContentStore>,
    entries: Arc<Mutex<HashMap<(Scope, ContextKey), ContextRef>>>,
}

impl InMemoryContextStore {
    pub fn new(content: Arc<dyn ContentStore>) -> Self {
        Self {
            content,
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn scope_label(scope: &Scope) -> String {
        match scope {
            Scope::Run => "Run".to_string(),
            Scope::Node(n) => format!("Node({})", n.0),
        }
    }
}

#[async_trait::async_trait]
impl ContextStore for InMemoryContextStore {
    async fn put(
        &self,
        scope: Scope,
        key: ContextKey,
        value: serde_json::Value,
    ) -> Result<ContextRef, OrchestratorError> {
        // Store the bytes first (idempotent, no lock held across the await),
        // then take the lock only for the sync collision-check + insert.
        let bytes = serde_json::to_vec(&value)?;
        let digest = self.content.put(&bytes).await?;
        let content = ContentRef {
            digest,
            size: bytes.len(),
            summary: None,
        };
        let context_ref = ContextRef {
            key: key.clone(),
            scope: scope.clone(),
            content,
            summary: None,
        };

        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if entries.contains_key(&(scope.clone(), key.clone())) {
            return Err(OrchestratorError::ContextKeyCollision {
                scope: Self::scope_label(&scope),
                key: key.0,
            });
        }
        entries.insert((scope, key), context_ref.clone());
        Ok(context_ref)
    }

    async fn get(
        &self,
        scope: Scope,
        key: ContextKey,
    ) -> Result<Option<ContextRef>, OrchestratorError> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(found) = entries.get(&(scope.clone(), key.clone())) {
            return Ok(Some(found.clone()));
        }
        // Resolve up the scope chain: a Node read falls back to Run.
        if let Scope::Node(_) = scope
            && let Some(found) = entries.get(&(Scope::Run, key))
        {
            return Ok(Some(found.clone()));
        }
        Ok(None)
    }

    async fn load(&self, r: &ContextRef) -> Result<serde_json::Value, OrchestratorError> {
        let bytes = self.content.get(&r.content.digest).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{Digest, NodeId};

    /// Acceptance 7 (store level) — content-addressing dedupes identical bytes
    /// to one digest, and a digest miss is loud (never an empty value).
    #[tokio::test]
    async fn content_store_dedupes_identical_bytes_and_misses_loudly() {
        let store = InMemoryContentStore::new();
        let d1 = store.put(b"hello world").await.unwrap();
        let d2 = store.put(b"hello world").await.unwrap();
        assert_eq!(d1, d2, "identical content shares one digest");
        assert_eq!(store.get(&d1).await.unwrap(), b"hello world");

        let d3 = store.put(b"different").await.unwrap();
        assert_ne!(d1, d3, "different content, different digest");

        let err = store
            .get(&Digest("deadbeef".into()))
            .await
            .expect_err("a digest miss is loud");
        assert!(
            matches!(err, OrchestratorError::ContentDigestMiss(_)),
            "{err:?}"
        );
    }

    /// Acceptance 6 — distinct-key writes round-trip; a re-write of an existing
    /// `(scope, key)` collides loudly; a `Node` read resolves up to `Run`; a
    /// `Node`-scoped write is not visible at `Run`; a miss is `Ok(None)`.
    #[tokio::test]
    async fn context_store_collides_resolves_node_to_run_and_misses_to_none() {
        let store = InMemoryContextStore::new(Arc::new(InMemoryContentStore::new()));
        let k1 = || ContextKey("k1".into());
        let node = || Scope::Node(NodeId("n".into()));

        // Run-scoped write round-trips through the CAS.
        let r1 = store
            .put(Scope::Run, k1(), serde_json::json!({ "v": 1 }))
            .await
            .unwrap();
        assert_eq!(
            store.load(&r1).await.unwrap(),
            serde_json::json!({ "v": 1 }),
            "load fetches the value back via the CAS"
        );

        // Re-writing the same (scope,key) is a loud collision.
        let err = store
            .put(Scope::Run, k1(), serde_json::json!({ "v": 2 }))
            .await
            .expect_err("same (scope,key) collides");
        assert!(
            matches!(err, OrchestratorError::ContextKeyCollision { .. }),
            "{err:?}"
        );

        // A Node-scoped read resolves up to the Run-scoped entry.
        let got = store.get(node(), k1()).await.unwrap();
        assert!(got.is_some(), "Node read resolves up to Run");

        // A read miss is an explicit Ok(None).
        assert!(
            store
                .get(Scope::Run, ContextKey("absent".into()))
                .await
                .unwrap()
                .is_none(),
            "a miss is Ok(None), never a silent empty value"
        );

        // A Node-scoped write is private to that node — not visible at Run.
        store
            .put(
                node(),
                ContextKey("k2".into()),
                serde_json::json!({ "n": true }),
            )
            .await
            .unwrap();
        let node_entry = store.get(node(), ContextKey("k2".into())).await.unwrap();
        assert_eq!(
            node_entry.unwrap().scope,
            node(),
            "the Node entry is returned"
        );
        assert!(
            store
                .get(Scope::Run, ContextKey("k2".into()))
                .await
                .unwrap()
                .is_none(),
            "a Node-scoped write does not leak to Run"
        );
    }
}
