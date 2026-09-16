//! In-memory [`ContentStore`] and [`ContextStore`] — the CAS + blackboard seams
//! the executor writes to (§7.4, §8). Both are `Arc`-shared and clonable, so a
//! resumed run rebuilds them from the journal; persistent implementations of the
//! same traits are a held-off layer.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use orchestrator_core::{
    ContentRef, ContentStore, ContextKey, ContextRef, ContextStore, Digest, OrchestratorError,
    RunId, Scope, digest_of,
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
/// as content-addressed refs, never inline). Writes to an existing `(run, scope, key)`
/// are rejected; reads resolve `Node` → `Run` **within one run**.
///
/// The [`RunId`] in the key is load-bearing (SP-OPS-1.1): one `Scheduler` drives up to
/// `CLAIM_BATCH` runs through a single `Executor`, so this map is shared across runs in
/// process just as the Postgres table is across processes.
#[derive(Clone)]
pub struct InMemoryContextStore {
    content: Arc<dyn ContentStore>,
    entries: Arc<Mutex<HashMap<ContextEntryKey, ContextRef>>>,
}

/// The blackboard's composite key. Mirrors the `context_refs` primary key
/// `(run_id, scope_kind, scope_id, ctx_key)` — [`Scope`] supplies the two scope columns.
type ContextEntryKey = (RunId, Scope, ContextKey);

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
        run: RunId,
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
        if entries.contains_key(&(run, scope.clone(), key.clone())) {
            return Err(OrchestratorError::ContextKeyCollision {
                scope: Self::scope_label(&scope),
                key: key.0,
            });
        }
        entries.insert((run, scope, key), context_ref.clone());
        Ok(context_ref)
    }

    async fn get(
        &self,
        run: RunId,
        scope: Scope,
        key: ContextKey,
    ) -> Result<Option<ContextRef>, OrchestratorError> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(found) = entries.get(&(run, scope.clone(), key.clone())) {
            return Ok(Some(found.clone()));
        }
        // Resolve up the scope chain: a Node read falls back to Run — in the SAME run.
        if let Scope::Node(_) = scope
            && let Some(found) = entries.get(&(run, Scope::Run, key))
        {
            return Ok(Some(found.clone()));
        }
        Ok(None)
    }

    async fn load(&self, r: &ContextRef) -> Result<serde_json::Value, OrchestratorError> {
        let bytes = self.content.get(&r.content.digest).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    async fn insert_ref(&self, run: RunId, r: ContextRef) -> Result<(), OrchestratorError> {
        // Rehydration from a journaled write: plain insert (last wins on an
        // identical fold replay), no collision check — the journal is the source
        // of truth. No CAS touch; the blob already lives there.
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.insert((run, r.scope.clone(), r.key.clone()), r);
        Ok(())
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
        let run = RunId(uuid::Uuid::new_v4());
        let k1 = || ContextKey("k1".into());
        let node = || Scope::Node(NodeId("n".into()));

        // Run-scoped write round-trips through the CAS.
        let r1 = store
            .put(run, Scope::Run, k1(), serde_json::json!({ "v": 1 }))
            .await
            .unwrap();
        assert_eq!(
            store.load(&r1).await.unwrap(),
            serde_json::json!({ "v": 1 }),
            "load fetches the value back via the CAS"
        );

        // Re-writing the same (scope,key) is a loud collision.
        let err = store
            .put(run, Scope::Run, k1(), serde_json::json!({ "v": 2 }))
            .await
            .expect_err("same (scope,key) collides");
        assert!(
            matches!(err, OrchestratorError::ContextKeyCollision { .. }),
            "{err:?}"
        );

        // A Node-scoped read resolves up to the Run-scoped entry.
        let got = store.get(run, node(), k1()).await.unwrap();
        assert!(got.is_some(), "Node read resolves up to Run");

        // A read miss is an explicit Ok(None).
        assert!(
            store
                .get(run, Scope::Run, ContextKey("absent".into()))
                .await
                .unwrap()
                .is_none(),
            "a miss is Ok(None), never a silent empty value"
        );

        // A Node-scoped write is private to that node — not visible at Run.
        store
            .put(
                run,
                node(),
                ContextKey("k2".into()),
                serde_json::json!({ "n": true }),
            )
            .await
            .unwrap();
        let node_entry = store
            .get(run, node(), ContextKey("k2".into()))
            .await
            .unwrap();
        assert_eq!(
            node_entry.unwrap().scope,
            node(),
            "the Node entry is returned"
        );
        assert!(
            store
                .get(run, Scope::Run, ContextKey("k2".into()))
                .await
                .unwrap()
                .is_none(),
            "a Node-scoped write does not leak to Run"
        );
    }

    /// **SP-OPS-1.1 (analysis §2.3) — two runs may publish the same key.**
    ///
    /// The blackboard was keyed `(scope, key)` with no run dimension, so a `Scope::Run`
    /// entry was global to the deployment forever. Because node ids are author-chosen and
    /// stable, `publish_context` keyed on the bare node id meant **re-running the same
    /// graph collided on its first completed node** — and the collision propagates with
    /// `?` out of `apply_node_result`, aborting the whole drive after the model call was
    /// already paid for. It was also a permanent poison pill: the failed `put` journals no
    /// `ContextWrite`, so the fold guard never engages and every resume re-collides.
    ///
    /// Asserts both halves, because fixing only the first would be a silent regression of
    /// the second: distinct runs are INDEPENDENT, and a repeat within ONE run still
    /// collides loudly.
    #[tokio::test]
    async fn two_runs_publish_the_same_key_independently_but_one_run_still_collides() {
        let store = InMemoryContextStore::new(Arc::new(InMemoryContentStore::new()));
        let (a, b) = (RunId(uuid::Uuid::new_v4()), RunId(uuid::Uuid::new_v4()));
        let key = || ContextKey("n1".into());

        let ra = store
            .put(a, Scope::Run, key(), serde_json::json!({ "run": "a" }))
            .await
            .expect("run A publishes n1");
        let rb = store
            .put(b, Scope::Run, key(), serde_json::json!({ "run": "b" }))
            .await
            .expect("run B publishes the SAME node id — the whole point of the fix");

        // Not merely both-Ok: each run must read back its OWN value. A shared row that
        // happened not to error would pass an is_ok() check and still be the bug.
        assert_eq!(
            store.load(&ra).await.unwrap(),
            serde_json::json!({ "run": "a" })
        );
        assert_eq!(
            store.load(&rb).await.unwrap(),
            serde_json::json!({ "run": "b" })
        );
        assert_eq!(
            store
                .get(a, Scope::Run, key())
                .await
                .unwrap()
                .map(|r| store_digest(&r)),
            Some(store_digest(&ra)),
            "run A reads back A's entry, not B's"
        );
        assert_eq!(
            store
                .get(b, Scope::Run, key())
                .await
                .unwrap()
                .map(|r| store_digest(&r)),
            Some(store_digest(&rb)),
            "run B reads back B's entry, not A's"
        );

        // The within-run guard is NOT relaxed by the fix.
        let err = store
            .put(a, Scope::Run, key(), serde_json::json!({ "run": "a2" }))
            .await
            .expect_err("a repeat within ONE run still collides loudly");
        assert!(
            matches!(err, OrchestratorError::ContextKeyCollision { .. }),
            "{err:?}"
        );
    }

    fn store_digest(r: &ContextRef) -> String {
        r.content.digest.0.clone()
    }

    /// `insert_ref` rehydrates an entry from an already-journaled ref (resume
    /// fold) without recomputing the CAS, and is idempotent (a fold replays every
    /// write) — no collision on a repeat, unlike `put`.
    #[tokio::test]
    async fn insert_ref_rehydrates_an_entry_without_recomputing_the_cas() {
        use orchestrator_core::{ContentRef, ContextRef};
        let content = Arc::new(InMemoryContentStore::new());
        let bytes = serde_json::to_vec(&serde_json::json!({"v":1})).unwrap();
        let digest = content.put(&bytes).await.unwrap();
        let r = ContextRef {
            key: ContextKey("k".into()),
            scope: Scope::Run,
            content: ContentRef {
                digest,
                size: bytes.len(),
                summary: None,
            },
            summary: None,
        };
        let store = InMemoryContextStore::new(content);
        let run = RunId(uuid::Uuid::new_v4());
        store.insert_ref(run, r.clone()).await.unwrap();
        let got = store
            .get(run, Scope::Run, ContextKey("k".into()))
            .await
            .unwrap()
            .expect("present after insert_ref");
        assert_eq!(store.load(&got).await.unwrap(), serde_json::json!({"v":1}));
        // Idempotent — re-inserting the same (scope,key) does not collide.
        store.insert_ref(run, r).await.unwrap();
    }
}
