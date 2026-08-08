use serde::{Deserialize, Serialize};

use crate::error::OrchestratorError;
use crate::ids::NodeId;

/// The kind of work a node performs. Two variants: a raw `ModelCall` that
/// compiles directly into an `InferenceRequest` (slice 1), and an `Agent` node
/// that runs a durable ReAct loop over a named agent (slice 2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeKind {
    ModelCall {
        chain: String,
        payload: serde_json::Value,
    },
    Agent {
        agent: crate::registry::AgentRef,
        input: serde_json::Value,
    },
}

/// A single node in the execution graph, with its explicit dependencies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub kind: NodeKind,
    pub deps: Vec<NodeId>,
}

/// An execution graph. Slice 1 validates that graphs are strictly linear.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Graph {
    pub nodes: Vec<Node>,
}

impl Graph {
    /// Validate that the graph is strictly linear: node ids are distinct, the
    /// first node has no dependencies, and every subsequent node depends on
    /// exactly the immediately-prior node.
    pub fn validate_linear(&self) -> Result<(), OrchestratorError> {
        let mut seen = std::collections::HashSet::new();
        for node in &self.nodes {
            if !seen.insert(&node.id) {
                return Err(OrchestratorError::InvalidGraph(format!(
                    "duplicate node id: {:?}",
                    node.id
                )));
            }
        }
        for (i, node) in self.nodes.iter().enumerate() {
            if i == 0 {
                if !node.deps.is_empty() {
                    return Err(OrchestratorError::InvalidGraph(format!(
                        "first node {:?} must have no dependencies",
                        node.id
                    )));
                }
            } else {
                let prior = &self.nodes[i - 1].id;
                if node.deps.len() != 1 || &node.deps[0] != prior {
                    return Err(OrchestratorError::InvalidGraph(format!(
                        "node {:?} must depend on exactly the prior node {:?}",
                        node.id, prior
                    )));
                }
            }
        }
        Ok(())
    }
}
