//! The scoped shared-context blackboard (§8). Nodes publish values under a
//! `(scope, key)` and read them back by key, resolving up the scope chain
//! (`Node` → `Run`). Values are stored **as refs, not blobs** — `put` writes the
//! bytes into a [`ContentStore`](crate::content::ContentStore) and keeps a
//! [`ContentRef`]; the fold rebuilds the store from journaled writes without
//! materializing payloads. Writes to an existing `(scope, key)` are rejected
//! loudly (no last-write-wins), so fan-out children must use distinct keys.

use serde::{Deserialize, Serialize};

use crate::content::ContentRef;
use crate::error::OrchestratorError;
use crate::ids::NodeId;

/// The visibility scope of a blackboard entry. `Run` is shared by the whole run;
/// `Node(id)` is private to one node but resolves up to `Run` on read. (`Plan`
/// and `Agent` scopes are deferred.)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Scope {
    Run,
    Node(NodeId),
}

/// A blackboard key (author-assigned, e.g. `"result.3"`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContextKey(pub String);

/// A blackboard entry: its key + scope, the content-addressed ref to its value,
/// and an optional summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextRef {
    pub key: ContextKey,
    pub scope: Scope,
    pub content: ContentRef,
    pub summary: Option<String>,
}

/// The scoped blackboard seam. Slice 3 ships an in-memory implementation.
///
/// - `put` rejects a write to an existing `(scope, key)` with
///   [`ContextKeyCollision`](OrchestratorError::ContextKeyCollision) — loud, no
///   silent overwrite.
/// - `get` resolves **up** the scope chain (`Node` → `Run`); a read miss is an
///   explicit `Ok(None)`, never a silent empty value.
/// - `load` fetches the referenced bytes lazily via the CAS and deserializes.
#[async_trait::async_trait]
pub trait ContextStore: Send + Sync {
    async fn put(
        &self,
        scope: Scope,
        key: ContextKey,
        value: serde_json::Value,
    ) -> Result<ContextRef, OrchestratorError>;

    async fn get(
        &self,
        scope: Scope,
        key: ContextKey,
    ) -> Result<Option<ContextRef>, OrchestratorError>;

    async fn load(&self, r: &ContextRef) -> Result<serde_json::Value, OrchestratorError>;

    /// Rehydrate an entry from an already-journaled ref (resume fold), WITHOUT
    /// touching the CAS. Idempotent: a fold replays every write, so re-inserting
    /// an identical `(scope, key)` must not error (unlike [`put`](Self::put)).
    async fn insert_ref(&self, r: ContextRef) -> Result<(), OrchestratorError>;
}
