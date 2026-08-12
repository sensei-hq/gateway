//! Best-effort observability hooks (§15). No-op defaults; a wired impl observes
//! run/node/agent/context lifecycle. Hooks never affect execution or determinism.

use crate::context::{ContextKey, Scope};
use crate::ids::{NodeId, RunId};

/// Observation callbacks fired by the executor at lifecycle points. Every method
/// defaults to a no-op, so an impl overrides only what it cares about. Best-effort
/// (§11.1.3): a hook must not depend on execution and should be fast (it is awaited
/// inline). Anything execution depends on is a durable effect, not a hook.
#[async_trait::async_trait]
pub trait OrchestratorHooks: Send + Sync {
    async fn on_run_started(&self, _run: RunId) {}
    async fn on_run_completed(&self, _run: RunId) {}
    async fn on_run_paused(&self, _run: RunId, _reason: &str) {}
    async fn on_node_started(&self, _run: RunId, _node: &NodeId) {}
    async fn on_node_completed(&self, _run: RunId, _node: &NodeId) {}
    async fn on_node_failed(&self, _run: RunId, _node: &NodeId, _error: &str) {}
    async fn on_node_skipped(&self, _run: RunId, _node: &NodeId) {}
    async fn on_agent_started(&self, _run: RunId, _node: &NodeId, _agent: &str, _chain: &str) {}
    async fn on_agent_turn(&self, _run: RunId, _node: &NodeId, _turn: usize) {}
    async fn on_agent_tool_call(&self, _run: RunId, _node: &NodeId, _tool: &str) {}
    async fn on_context_write(&self, _run: RunId, _scope: &Scope, _key: &ContextKey) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct Spy(Arc<Mutex<Vec<String>>>);
    #[async_trait::async_trait]
    impl OrchestratorHooks for Spy {
        async fn on_node_started(&self, _run: RunId, node: &NodeId) {
            self.0.lock().unwrap().push(format!("started({})", node.0));
        }
    }

    /// A default (no-op) impl and a partial override both compile and behave: the
    /// override records, defaulted methods do nothing.
    #[tokio::test]
    async fn default_is_noop_and_override_records() {
        struct NoOp;
        impl OrchestratorHooks for NoOp {}
        let run = RunId(uuid::Uuid::new_v4());
        NoOp.on_run_started(run).await; // no panic, returns ()

        let log = Arc::new(Mutex::new(Vec::new()));
        let spy = Spy(log.clone());
        spy.on_node_started(run, &NodeId("n1".into())).await;
        spy.on_run_started(run).await; // defaulted no-op → not recorded
        assert_eq!(*log.lock().unwrap(), vec!["started(n1)".to_string()]);
    }
}
