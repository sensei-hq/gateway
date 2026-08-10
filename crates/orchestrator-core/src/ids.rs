use serde::{Deserialize, Serialize};

/// Identifies a single orchestrator run.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RunId(pub uuid::Uuid);

/// A stable, author-assigned node identifier (e.g. `"n1"`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        NodeId(s.to_string())
    }
}

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        NodeId(s)
    }
}

/// A global monotonic sequence number stamping every journal event.
pub type Seq = u64;
