use serde::{Deserialize, Serialize};

/// Identifies a single orchestrator run.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RunId(pub uuid::Uuid);

/// A stable, author-assigned node identifier (e.g. `"n1"`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

/// A global monotonic sequence number stamping every journal event.
pub type Seq = u64;
