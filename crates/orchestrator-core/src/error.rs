use crate::effect::EffectId;
use crate::ids::NodeId;

/// An error from an [`ExecutionJournal`](crate::journal::ExecutionJournal)
/// backend. Journal writes are strict: this error is surfaced, never swallowed.
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    #[error("journal backend error: {0}")]
    Backend(String),
}

/// A top-level orchestrator error.
#[derive(Debug, thiserror::Error)]
pub enum OrchestratorError {
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error("version fence mismatch: recorded {recorded}, current {current}")]
    VersionFenceMismatch { recorded: String, current: String },
    #[error("determinism violation at node {node:?}")]
    DeterminismViolation { node: NodeId, effect_id: EffectId },
    #[error("invalid graph: {0}")]
    InvalidGraph(String),
    #[error("gateway error: {0}")]
    Gateway(String),
}
