use serde::{Deserialize, Serialize};

use crate::content::EffectOutput;
use crate::effect::{EffectClass, EffectId};
use crate::error::JournalError;
use crate::ids::{NodeId, RunId, Seq};

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
}

#[cfg(test)]
mod tests {
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
        };
        let s = serde_json::to_string(&e).unwrap();
        let back: JournalEvent = serde_json::from_str(&s).unwrap();
        assert!(matches!(back, JournalEvent::EffectRecorded { .. }));
    }
}
