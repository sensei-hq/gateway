use crate::effect::EffectId;
use crate::ids::{NodeId, RunId};

/// An error from an [`ExecutionJournal`](crate::journal::ExecutionJournal)
/// backend. Journal writes are strict: this error is surfaced, never swallowed.
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    #[error("journal backend error: {0}")]
    Backend(String),
    /// A persisted journal's `format_version` differs from this build's
    /// [`FORMAT_VERSION`](crate::journal::FORMAT_VERSION) — the effect-id/serialization
    /// scheme is incompatible; resume must halt loudly, not mis-fold.
    #[error("incompatible journal format for run {run:?}: stored {stored}, expected {expected}")]
    IncompatibleFormat {
        run: RunId,
        stored: i32,
        expected: i32,
    },
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
    #[error("frontmatter parse error: {0}")]
    FrontmatterParse(String),
    #[error("agent {agent:?} references unknown skill {skill:?}")]
    UnknownSkillRef { agent: String, skill: String },
    #[error("agent {agent:?} references unknown tool {tool:?}")]
    UnknownToolRef { agent: String, tool: String },
    #[error(
        "agent {agent:?} has no base chain route: add an explicit `chain` or an `(area,kind)` binding (per-phase `chains` are overrides layered on a base route, so they do not by themselves make an agent routable)"
    )]
    UnknownChainRef { agent: String },
    /// Reserved for a future strict/opt-in load-time grant check (SP-4). Not
    /// currently produced: runtime enforcement is per-call (ceiling model), and
    /// `validate` no longer verifies grants cover a tool's declared surface.
    #[error(
        "agent {agent:?} references tool {tool:?} without a grant covering its declared permissions"
    )]
    PermissionNotGranted { agent: String, tool: String },
    #[error("payload serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    /// A durable store (CAS/context) backend/transport failure — loud, never swallowed. Distinct
    /// from [`JournalError::Backend`] so a store fault isn't mislabeled a journal fault.
    #[error("store backend error: {0}")]
    Store(String),
    #[error(
        "tool {tool:?} has non-Pure effect class {class:?}; Observation/Mutation are deferred to SP-1 slice 4"
    )]
    ToolEffectDeferred {
        tool: String,
        class: crate::effect::EffectClass,
    },
    #[error("unknown tool {0:?}")]
    UnknownTool(String),
    #[error("tool {tool:?} failed: {message}")]
    Tool { tool: String, message: String },
    /// A tool requested a path that escapes its per-run workspace jail (SP-4 s3).
    /// The message names the requested (relative) path but NOT the absolute host root,
    /// so the journal/transcript never leaks the host filesystem layout.
    #[error("workspace escape: {0}")]
    WorkspaceEscape(String),
    #[error("unknown agent {0:?}")]
    UnknownAgent(String),
    // `PromptOverBudget { node, turn, est, min_win }` lived here until SP-7a. The
    // executor raised it before dispatching a live agent turn whose prompt exceeded
    // `Gateway::min_context_window(chain)` — the chain's SMALLEST window — which refused
    // requests a larger entry in the same chain could have served. Window fit is now the
    // gateway's `ContextWindowGate`, asked per candidate, and an over-every-window
    // request surfaces as `GatewayError::AllGated` carrying each candidate's own window
    // and a `UseLargerContextWindow` remedy.
    //
    // REMOVED rather than left unconstructed: a variant no code can produce is a claim
    // the type makes and the code does not honour, and callers matching on it would be
    // writing dead arms against a decision this crate no longer owns.
    #[error("agent node {node:?} exceeded max_steps")]
    AgentMaxStepsExceeded { node: NodeId },
    #[error(
        "consolidate {node:?} starved: {have} viable input(s), need {need} — refusing to synthesize over too few survivors"
    )]
    ConsolidateStarved {
        node: NodeId,
        have: usize,
        need: usize,
    },
    #[error(
        "map child {node:?} paused (in-doubt mutation): {reason} — carried out of the fan-out so the whole Map pauses loud"
    )]
    MapChildPaused { node: NodeId, reason: String },
    #[error("global cap {cap:?} exceeded (limit {limit})")]
    GlobalCapExceeded { cap: String, limit: usize },
    #[error("branch {branch:?} has no decision value — its `on` node {on:?} produced no output")]
    BranchInputMissing { branch: NodeId, on: NodeId },
    #[error("blackboard collision: scope {scope} already has key {key:?}")]
    ContextKeyCollision { scope: String, key: String },
    #[error("content-store digest miss: {0} — content is not addressable")]
    ContentDigestMiss(String),
    #[error("registry load error: {0}")]
    RegistryLoad(String),
}
