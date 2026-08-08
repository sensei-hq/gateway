//! `sensei-orchestrator-core` — zero-I/O core types for the sensei orchestrator:
//! the graph, effect, and journal vocabulary shared by the executor and its
//! journal stores. Depends on nothing else in the workspace (not the gateway).

pub mod effect;
pub mod error;
pub mod graph;
pub mod ids;
pub mod journal;

pub use effect::{EffectClass, EffectId, effect_id};
pub use error::{JournalError, OrchestratorError};
pub use graph::{Graph, Node, NodeKind};
pub use ids::{NodeId, RunId, Seq};
pub use journal::{ExecutionJournal, JournalEvent};
