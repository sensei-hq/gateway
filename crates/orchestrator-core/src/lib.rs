//! `sensei-orchestrator-core` — zero-I/O core types for the sensei orchestrator:
//! the graph, effect, and journal vocabulary shared by the executor and its
//! journal stores. Depends on nothing else in the workspace (not the gateway).

pub mod content;
pub mod context;
pub mod effect;
pub mod error;
pub mod graph;
pub mod ids;
pub mod journal;
pub mod registry;

pub use content::{ContentRef, ContentStore, Digest, EffectOutput, digest_of};
pub use context::{ContextKey, ContextRef, ContextStore, Scope};
pub use effect::{EffectClass, EffectId, effect_id};
pub use error::{JournalError, OrchestratorError};
pub use graph::{Aggregation, Dep, EdgeKind, Graph, MapBody, Node, NodeKind};
pub use ids::{NodeId, RunId, Seq};
pub use journal::{ChildStatus, CompactChild, ExecutionJournal, JournalEvent, Snapshot};
pub use registry::{AgentDefinition, AgentRef, Registry, SkillDef, ToolSpec};
