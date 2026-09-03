//! `sensei-orchestrator-core` — zero-I/O core types for the sensei orchestrator:
//! the graph, effect, and journal vocabulary shared by the executor and its
//! journal stores. Depends on nothing else in the workspace (not the gateway).

pub mod budget;
pub mod clock;
pub mod content;
pub mod context;
pub mod credential;
pub mod effect;
pub mod error;
pub mod graph;
pub mod hooks;
pub mod ids;
pub mod journal;
pub mod plan;
pub mod planner;
pub mod reconcile;
pub mod redact;
pub mod registry;
pub mod scheduler;

pub use budget::{TokenBudget, TokenUsage};
pub use clock::{Clock, SystemClock};
pub use content::{ContentRef, ContentStore, Digest, EffectOutput, digest_of};
pub use context::{ContextKey, ContextRef, ContextStore, Scope};
pub use credential::{CredentialBroker, Secret};
pub use effect::{EffectClass, EffectId, effect_id};
pub use error::{JournalError, OrchestratorError};
pub use graph::{
    Aggregation, BranchCond, Dep, EdgeKind, GateOption, GateOutcome, GateSpec, Graph, LoopBody,
    LoopGate, LoopGateOption, MapBody, Node, NodeKind, PlannerRef,
};
pub use hooks::OrchestratorHooks;
pub use ids::{NodeId, RunId, Seq};
pub use journal::{
    ChildStatus, CompactChild, ExecutionJournal, FORMAT_VERSION, JournalEvent,
    MAX_HUMAN_CONTEXT_BYTES, MAX_HUMAN_TEXT_BYTES, ObservationMeta, Snapshot,
};
pub use plan::{
    NodeNeeds, NodePlan, PlanError, PlannedGraph, RESERVED_PLAN_ID, feasible, parse_plan,
};
pub use planner::{
    ModelDispatch, PLANNER_AREA, Planner, PlannerSelector, RESERVED_SELECT_ID, RulePlannerSelector,
};
pub use reconcile::{ReconcileOutcome, ReconcileProvider, idempotency_key};
pub use redact::{PatternRedactor, Redactor};
pub use registry::{
    Activation, AgentBacking, AgentDefinition, AgentRef, ChainBinding, ConfigSource, NetworkPolicy,
    Permissions, Registry, RegistryConfig, RegistryHandle, ResourceCaps, SkillDef, ToolSpec,
};
pub use scheduler::{RunStatus, ScheduledRun, SchedulerStore};
