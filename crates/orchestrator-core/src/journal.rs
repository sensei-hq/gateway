use serde::{Deserialize, Serialize};

use crate::content::{ContentRef, Digest, EffectOutput};
use crate::effect::{EffectClass, EffectId};
use crate::error::JournalError;
use crate::ids::{NodeId, RunId, Seq};

/// A compacted per-child record (§5.3): after a `Map`'s `Consolidate` completes,
/// each child's full `EffectRecorded` collapses to this small shape and leaves
/// the hot fold path. The child's content stays retrievable from the
/// `ContentStore` by [`digest`](CompactChild::digest) — never dropped. The
/// `input_hash` lets the resume fold rebuild the child's memo entry (as a
/// content ref) so a replay still re-spends nothing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompactChild {
    pub index: usize,
    pub status: ChildStatus,
    /// The content address of the child's output — `Some` for an `Ok` child,
    /// `None` for a `Failed` one (which never journaled an output).
    pub digest: Option<Digest>,
    /// The child effect's determinism key (`Ok` children only) — feeds memo
    /// reconstruction on resume.
    pub input_hash: Option<String>,
}

/// A compacted child's terminal status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChildStatus {
    Ok,
    Failed,
}

/// Provenance + freshness of an `Observation` effect (§7.1). `content_hash` (the
/// third provenance element) is derived — the recorded output's digest — not stored.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObservationMeta {
    pub fetched_at: chrono::DateTime<chrono::Utc>,
    pub ttl_secs: u64,
    pub source: String,
}

/// An append-only event in a run's durable journal. Folding a run's events
/// reconstructs its state for deterministic resume.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JournalEvent {
    RunStarted {
        version: String,
    },
    NodeStarted {
        node: NodeId,
    },
    EffectRecorded {
        node: NodeId,
        effect_id: EffectId,
        class: EffectClass,
        input_hash: String,
        seq: Seq,
        /// The effect's output, carried **inline** for small payloads or as a
        /// content-addressed [`ContentRef`](crate::content::ContentRef) for
        /// over-threshold ones (§7.4). The fold reads this without loading blobs.
        output: EffectOutput,
        /// Set only for `Observation` effects (§7.1): freshness + provenance so a
        /// resume can decide replay-vs-re-read. `None` for Pure/Mutation.
        observation: Option<ObservationMeta>,
    },
    /// The intent phase of a two-phase `Mutation` (§7.3), appended BEFORE the side
    /// effect. On resume an `EffectIntent` with no matching `EffectRecorded` is
    /// IN-DOUBT → reconcile, never blind re-run or blind memoize.
    EffectIntent {
        node: NodeId,
        effect_id: EffectId,
        idempotency_key: String,
        args_hash: String,
        seq: Seq,
    },
    NodeCompleted {
        node: NodeId,
    },
    NodeFailed {
        node: NodeId,
        error: String,
    },
    /// A node was skipped without running because a `Hard` dependency ended
    /// `Failed` or `Skipped` — cascade-skip (§3.3). Journaled so a skip is never
    /// silent; surfaced in `RunOutcome.skipped`.
    NodeSkipped {
        node: NodeId,
    },
    /// A `Map` node fanned out over `child_count` items (§3.4). The child
    /// manifest is fixed by the node's `over`, so this is deterministic and
    /// order-independent; each child's own effects follow under the structural
    /// path `"{node}/{i}"`.
    MapExpanded {
        node: NodeId,
        child_count: usize,
    },
    /// A completed `Map`'s per-child `EffectRecorded` records were compacted
    /// (§5.3) once its `Consolidate` finished: the children collapse to a small
    /// `{index, status, digest}` manifest (content stays addressable in the CAS).
    /// The fold rebuilds the children's memo (as content refs) from this, so the
    /// Map replays on resume without re-spending.
    MapCompacted {
        node: NodeId,
        children: Vec<CompactChild>,
    },
    /// A shared-scope blackboard publish (§8). Journaled so a resume rebuilds the
    /// `ContextStore` (as refs, no blob load) via
    /// [`ContextStore::insert_ref`](crate::context::ContextStore::insert_ref). The
    /// `content` is a CAS ref — never an inline blob.
    ContextWrite {
        scope: crate::context::Scope,
        key: crate::context::ContextKey,
        content: ContentRef,
        summary: Option<String>,
        seq: Seq,
    },
    RunCompleted,
    RunPaused {
        reason: String,
        resume_after: Option<chrono::DateTime<chrono::Utc>>,
    },
}

/// A round-boundary checkpoint of a run's state (§7.4). Written to the journal's
/// snapshot store (out-of-band — NOT an event in the log, so the control-flow
/// event order stays byte-identical) after each scheduling round; the latest
/// wins. A resume seeds from the latest snapshot and folds only the journal
/// **tail** (events with `Seq >` [`seq`](Snapshot::seq)), bounding fold cost for
/// wide/long runs.
///
/// Carries the completed/skipped node sets and each completed node's output (as
/// a ref-or-inline [`EffectOutput`], so large outputs stay lean). The per-effect
/// memo for a partially-completed tail node is rebuilt by folding the tail, so
/// it is not stored here; the blackboard's `context_refs` are deferred until the
/// executor writes to the `ContextStore`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Snapshot {
    /// The journal `Seq` this snapshot covers up to; a resume folds events with
    /// `Seq >` this.
    pub seq: Seq,
    pub completed: Vec<NodeId>,
    pub skipped: Vec<NodeId>,
    /// Each completed node's output, keyed by node id (ref-or-inline).
    pub outputs: Vec<(NodeId, EffectOutput)>,
}

/// The durable-journal seam. Slice 1 ships an in-memory implementation; a
/// `PostgresJournal` implements this same trait in a later slice.
///
/// `append` is strict: a write error is surfaced (fatal/pause), never swallowed.
#[async_trait::async_trait]
pub trait ExecutionJournal: Send + Sync {
    async fn append(&self, run: RunId, event: JournalEvent) -> Result<Seq, JournalError>;
    async fn load(&self, run: RunId) -> Result<Vec<(Seq, JournalEvent)>, JournalError>;

    /// Load only the journal **tail** — events with `Seq > since`. The default
    /// filters [`load`](Self::load); a persistent backend overrides this with an
    /// indexed range query. Powers snapshot-resume (fold the tail, not the whole
    /// log).
    async fn load_since(
        &self,
        run: RunId,
        since: Seq,
    ) -> Result<Vec<(Seq, JournalEvent)>, JournalError> {
        Ok(self
            .load(run)
            .await?
            .into_iter()
            .filter(|(seq, _)| *seq > since)
            .collect())
    }

    /// Persist the latest round-boundary [`Snapshot`] for `run` (latest wins).
    /// The default is a no-op — a backend without snapshot support simply folds
    /// from the start (the slice-1/2 path); [`InMemoryJournal`] overrides it.
    async fn snapshot(&self, _run: RunId, _snap: Snapshot) -> Result<(), JournalError> {
        Ok(())
    }

    /// The latest [`Snapshot`] for `run`, or `None` if none was written. The
    /// default returns `None` (fold-from-start).
    async fn latest_snapshot(&self, _run: RunId) -> Result<Option<Snapshot>, JournalError> {
        Ok(None)
    }

    /// Compaction primitive (§5.3): remove the events at `remove_seqs` and append
    /// `add` in one step. Generic — the executor picks the seqs (a completed
    /// Map's per-child `EffectRecorded`) and `add` (a `MapCompacted` manifest);
    /// the journal stays oblivious to Map semantics. The default is a graceful
    /// no-removal append (a backend without compaction keeps the child records but
    /// still records the manifest); [`InMemoryJournal`] overrides it to remove.
    async fn compact(
        &self,
        run: RunId,
        _remove_seqs: &[Seq],
        add: JournalEvent,
    ) -> Result<(), JournalError> {
        self.append(run, add).await.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::ObservationMeta;
    use crate::{EffectClass, EffectOutput, JournalEvent, NodeId, effect_id};

    #[test]
    fn journal_event_roundtrips() {
        let e = JournalEvent::EffectRecorded {
            node: NodeId("n1".into()),
            effect_id: effect_id("", 0, 0),
            class: EffectClass::Pure,
            input_hash: "abc".into(),
            seq: 1,
            output: EffectOutput::Inline(serde_json::json!({"text":"hi"})),
            observation: None,
        };
        let s = serde_json::to_string(&e).unwrap();
        let back: JournalEvent = serde_json::from_str(&s).unwrap();
        assert!(matches!(back, JournalEvent::EffectRecorded { .. }));
    }

    #[test]
    fn effect_intent_and_observation_meta_roundtrip() {
        let intent = JournalEvent::EffectIntent {
            node: NodeId("n1".into()),
            effect_id: effect_id("n1", 0, 1),
            idempotency_key: "k".into(),
            args_hash: "h".into(),
            seq: 0,
        };
        let s = serde_json::to_string(&intent).unwrap();
        assert!(matches!(
            serde_json::from_str::<JournalEvent>(&s).unwrap(),
            JournalEvent::EffectIntent { .. }
        ));

        let obs = ObservationMeta {
            fetched_at: chrono::Utc::now(),
            ttl_secs: 60,
            source: "search".into(),
        };
        let rec = JournalEvent::EffectRecorded {
            node: NodeId("n1".into()),
            effect_id: effect_id("n1", 0, 1),
            class: EffectClass::Observation,
            input_hash: "h".into(),
            seq: 0,
            output: EffectOutput::Inline(serde_json::json!({"x":1})),
            observation: Some(obs),
        };
        assert!(matches!(
            serde_json::from_str::<JournalEvent>(&serde_json::to_string(&rec).unwrap()).unwrap(),
            JournalEvent::EffectRecorded {
                observation: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn context_write_event_roundtrips() {
        use crate::content::{ContentRef, Digest};
        use crate::context::{ContextKey, Scope};
        let e = JournalEvent::ContextWrite {
            scope: Scope::Run,
            key: ContextKey("n1".into()),
            content: ContentRef {
                digest: Digest("d".into()),
                size: 3,
                summary: None,
            },
            summary: None,
            seq: 0,
        };
        let s = serde_json::to_string(&e).unwrap();
        assert!(matches!(
            serde_json::from_str::<JournalEvent>(&s).unwrap(),
            JournalEvent::ContextWrite { .. }
        ));
    }
}
