//! `sensei-orchestrator-core` — zero-I/O core types for the sensei orchestrator:
//! the graph, effect, and journal vocabulary shared by the executor and its
//! journal stores. Depends on nothing else in the workspace (not the gateway).

pub mod clock;
pub mod content;
pub mod context;
pub mod effect;
pub mod error;
pub mod graph;
pub mod hooks;
pub mod ids;
pub mod journal;
pub mod plan;
pub mod planner;
pub mod reconcile;
pub mod registry;

pub use clock::{Clock, SystemClock};
pub use content::{ContentRef, ContentStore, Digest, EffectOutput, digest_of};
pub use context::{ContextKey, ContextRef, ContextStore, Scope};
pub use effect::{EffectClass, EffectId, effect_id};
pub use error::{JournalError, OrchestratorError};
pub use graph::{
    Aggregation, BranchCond, Dep, EdgeKind, Graph, LoopGate, MapBody, Node, NodeKind, PlannerRef,
};
pub use hooks::OrchestratorHooks;
pub use ids::{NodeId, RunId, Seq};
pub use journal::{
    ChildStatus, CompactChild, ExecutionJournal, JournalEvent, ObservationMeta, Snapshot,
};
pub use plan::{
    NodeNeeds, NodePlan, PlanError, PlannedGraph, RESERVED_PLAN_ID, feasible, parse_plan,
};
pub use planner::{PLANNER_AREA, Planner, PlannerSelector, RulePlannerSelector};
pub use reconcile::{ReconcileOutcome, ReconcileProvider, idempotency_key};
pub use registry::{
    Activation, AgentDefinition, AgentRef, ChainBinding, ConfigSource, NetworkPolicy, Permissions,
    Registry, RegistryConfig, RegistryHandle, ResourceCaps, SkillDef, ToolSpec,
};
