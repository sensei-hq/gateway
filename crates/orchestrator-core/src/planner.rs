//! The injected planner seam (SP-3 slice 3): a node's runtime graph producer.
//! Slice 3 ships test/stub impls; slice 4 drops in the LLM-backed planner agent.

use async_trait::async_trait;

use crate::error::OrchestratorError;
use crate::graph::Graph;

/// Produces a nested subgraph at runtime for a [`NodeKind::Expand`](crate::graph::NodeKind::Expand)
/// node. The returned `Graph` carries LOCAL ids (namespaced under the node at drive
/// time). Returning `Err` — or a graph that fails `validate_dag` — is a node-level
/// failure the executor maps to `Failed`, never a panic.
#[async_trait]
pub trait Planner: Send + Sync {
    async fn plan(&self, input: &serde_json::Value) -> Result<Graph, OrchestratorError>;
}
