use serde::{Deserialize, Serialize};

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
        output: serde_json::Value,
    },
    NodeCompleted {
        node: NodeId,
    },
    NodeFailed {
        node: NodeId,
        error: String,
    },
    RunCompleted,
    RunPaused {
        reason: String,
        resume_after: Option<chrono::DateTime<chrono::Utc>>,
    },
}

/// The durable-journal seam. Slice 1 ships an in-memory implementation; a
/// `PostgresJournal` implements this same trait in a later slice.
///
/// `append` is strict: a write error is surfaced (fatal/pause), never swallowed.
#[async_trait::async_trait]
pub trait ExecutionJournal: Send + Sync {
    async fn append(&self, run: RunId, event: JournalEvent) -> Result<Seq, JournalError>;
    async fn load(&self, run: RunId) -> Result<Vec<(Seq, JournalEvent)>, JournalError>;
}

#[cfg(test)]
mod tests {
    use crate::{EffectClass, JournalEvent, NodeId, effect_id};

    #[test]
    fn journal_event_roundtrips() {
        let e = JournalEvent::EffectRecorded {
            node: NodeId("n1".into()),
            effect_id: effect_id("", 0, 0),
            class: EffectClass::Pure,
            input_hash: "abc".into(),
            seq: 1,
            output: serde_json::json!({"text":"hi"}),
        };
        let s = serde_json::to_string(&e).unwrap();
        let back: JournalEvent = serde_json::from_str(&s).unwrap();
        assert!(matches!(back, JournalEvent::EffectRecorded { .. }));
    }
}
