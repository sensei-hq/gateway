//! The deterministic executor: drives a linear `ModelCall` graph through the
//! gateway, journaling every step so a crashed run can resume (Task 4).

use std::collections::HashMap;
use std::sync::Arc;

use gateway::Gateway;
use kernel::types::capability::Capability;
use kernel::types::request::{InferenceRequest, Message, MessageRole, Payload};
use orchestrator_core::{
    EffectClass, EffectId, ExecutionJournal, Graph, JournalEvent, NodeId, NodeKind,
    OrchestratorError, RunId, Seq, effect_id,
};
use sha2::{Digest, Sha256};

/// The deterministic executor over a durable journal, wired to the gateway.
pub struct Executor {
    gateway: Arc<Gateway>,
    journal: Arc<dyn ExecutionJournal>,
    version: String,
}

/// The terminal outcome of a run: the nodes that completed, the first failure
/// (which halts the run), and each node's memoized output.
#[derive(Debug, Default)]
pub struct RunOutcome {
    pub completed: Vec<NodeId>,
    pub failed: Option<(NodeId, String)>,
    pub outputs: HashMap<NodeId, serde_json::Value>,
}

impl Executor {
    /// Build an executor over a gateway + journal, fencing every run it starts
    /// with `version` (recorded in `RunStarted`, checked on resume in Task 4).
    pub fn new(
        gateway: Arc<Gateway>,
        journal: Arc<dyn ExecutionJournal>,
        version: impl Into<String>,
    ) -> Self {
        Self {
            gateway,
            journal,
            version: version.into(),
        }
    }

    /// Execute a fresh linear graph end-to-end: journal `RunStarted`, then drive
    /// every node with an empty memo (nothing has run yet).
    pub async fn run(&self, run: RunId, graph: &Graph) -> Result<RunOutcome, OrchestratorError> {
        graph.validate_linear()?;
        self.append(
            run,
            JournalEvent::RunStarted {
                version: self.version.clone(),
            },
        )
        .await?;
        self.drive(run, graph, &HashMap::new()).await
    }

    /// Shared node loop for both `run` (empty memo) and `start` (memo folded
    /// from the journal, Task 4). `memo` maps each node's structural
    /// [`EffectId`] to its recorded `(input_hash, output)`:
    ///
    /// - a memo hit whose input-hash matches ⇒ replay the recorded output with
    ///   NO gateway call and NO new `EffectRecorded` (it is already journaled);
    /// - a memo hit whose input-hash differs ⇒ a determinism violation (the
    ///   graph changed under a resume) — halt;
    /// - a memo miss ⇒ execute the node against the gateway and journal it.
    ///
    /// For a fresh `run` the memo is always empty, so every node executes; the
    /// memo branches exist for Task 4's resume and are reachable code.
    async fn drive(
        &self,
        run: RunId,
        graph: &Graph,
        memo: &HashMap<EffectId, (String, serde_json::Value)>,
    ) -> Result<RunOutcome, OrchestratorError> {
        let mut outcome = RunOutcome::default();
        for (index, node) in graph.nodes.iter().enumerate() {
            let NodeKind::ModelCall { chain, payload } = &node.kind;
            let eid = effect_id("", 0, index);
            let ih = input_hash(chain, payload)?;

            if let Some((recorded_ih, output)) = memo.get(&eid) {
                if recorded_ih != &ih {
                    return Err(OrchestratorError::DeterminismViolation {
                        node: node.id.clone(),
                        effect_id: eid,
                    });
                }
                // Memoized: replay the recorded output — no gateway call, no new
                // `EffectRecorded` (it is already in the journal).
                outcome.outputs.insert(node.id.clone(), output.clone());
                outcome.completed.push(node.id.clone());
                continue;
            }

            self.append(
                run,
                JournalEvent::NodeStarted {
                    node: node.id.clone(),
                },
            )
            .await?;

            let request = build_request(chain, payload);
            match self.gateway.execute(&request).await {
                Ok(response) => {
                    let output = serde_json::json!({
                        "model": response.model,
                        "text": response.content.clone().unwrap_or_default(),
                    });
                    // `EffectRecorded.seq` is advisory: `append` assigns the
                    // authoritative outer `Seq`, and the Task 4 resume fold
                    // orders events by that outer `(Seq, event)` from `load` —
                    // never by this in-event field — so it is set to 0 rather
                    // than the (circular) value `append` would return.
                    self.append(
                        run,
                        JournalEvent::EffectRecorded {
                            node: node.id.clone(),
                            effect_id: eid,
                            class: EffectClass::Pure,
                            input_hash: ih,
                            seq: 0,
                            output: output.clone(),
                        },
                    )
                    .await?;
                    self.append(
                        run,
                        JournalEvent::NodeCompleted {
                            node: node.id.clone(),
                        },
                    )
                    .await?;
                    outcome.outputs.insert(node.id.clone(), output);
                    outcome.completed.push(node.id.clone());
                }
                Err(error) => {
                    let message = error.to_string();
                    self.append(
                        run,
                        JournalEvent::NodeFailed {
                            node: node.id.clone(),
                            error: message.clone(),
                        },
                    )
                    .await?;
                    // Surface the failure in the outcome and stop the run — the
                    // failure is reported, not swallowed.
                    outcome.failed = Some((node.id.clone(), message));
                    return Ok(outcome);
                }
            }
        }
        self.append(run, JournalEvent::RunCompleted).await?;
        Ok(outcome)
    }

    /// Append one event, mapping a journal-backend error to a fatal
    /// `OrchestratorError::Journal` (strict — a journal write failure aborts the
    /// run; it is never swallowed). Returns the authoritative `Seq`.
    async fn append(&self, run: RunId, event: JournalEvent) -> Result<Seq, OrchestratorError> {
        self.journal
            .append(run, event)
            .await
            .map_err(OrchestratorError::Journal)
    }
}

/// Structural content hash of a node's `(chain, payload)` — the determinism key
/// checked against the memo on resume: `sha256_hex("{chain}|{json(payload)}")`.
fn input_hash(chain: &str, payload: &serde_json::Value) -> Result<String, OrchestratorError> {
    let serialized = serde_json::to_string(payload)?;
    let mut hasher = Sha256::new();
    hasher.update(format!("{chain}|{serialized}").as_bytes());
    Ok(format!("{:x}", hasher.finalize()))
}

/// Compile a `ModelCall`'s `(chain, payload)` into a plain single-turn chat
/// [`InferenceRequest`]: `TextChat` over the named chain, fallback enabled, all
/// other addressing/identity fields defaulted. The payload's `"prompt"` string
/// (if present) becomes the sole user message.
fn build_request(chain: &str, payload: &serde_json::Value) -> InferenceRequest {
    let prompt = payload
        .get("prompt")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    InferenceRequest {
        capability: Capability::TextChat,
        model: None,
        router: None,
        chain: Some(chain.to_string()),
        payload: Payload::Chat {
            messages: vec![Message::text(MessageRole::User, prompt)],
            system: None,
            max_tokens: None,
            temperature: None,
            tools: Vec::new(),
        },
        budget: None,
        auth: None,
        panel: None,
        consensus: None,
        allow_fallback: true,
        credentials: Default::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::recording_gateway;
    use orchestrator_core::{Graph, Node, NodeId, NodeKind};
    use orchestrator_store::InMemoryJournal;

    fn model_call(chain: &str, prompt: &str) -> NodeKind {
        NodeKind::ModelCall {
            chain: chain.to_string(),
            payload: serde_json::json!({ "prompt": prompt }),
        }
    }

    /// Compact, order-preserving label for a journal event, so the test asserts
    /// the exact event sequence (kind + node) without matching payloads.
    fn label(event: &JournalEvent) -> String {
        match event {
            JournalEvent::RunStarted { .. } => "RunStarted".to_string(),
            JournalEvent::NodeStarted { node } => format!("NodeStarted({})", node.0),
            JournalEvent::EffectRecorded { node, .. } => format!("EffectRecorded({})", node.0),
            JournalEvent::NodeCompleted { node } => format!("NodeCompleted({})", node.0),
            JournalEvent::NodeFailed { node, .. } => format!("NodeFailed({})", node.0),
            JournalEvent::RunCompleted => "RunCompleted".to_string(),
            JournalEvent::RunPaused { .. } => "RunPaused".to_string(),
        }
    }

    #[tokio::test]
    async fn run_drives_linear_graph_through_gateway_and_journals_in_order() {
        let (gateway, calls) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        let executor = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");

        let n1 = NodeId("n1".into());
        let n2 = NodeId("n2".into());
        let graph = Graph {
            nodes: vec![
                Node {
                    id: n1.clone(),
                    kind: model_call("c", "a"),
                    deps: vec![],
                },
                Node {
                    id: n2.clone(),
                    kind: model_call("c", "b"),
                    deps: vec![n1.clone()],
                },
            ],
        };

        let run = RunId(uuid::Uuid::new_v4());
        let outcome = executor.run(run, &graph).await.expect("run succeeds");

        // Both nodes completed, in order, with no failure.
        assert!(
            outcome.failed.is_none(),
            "no node should fail: {:?}",
            outcome.failed
        );
        assert_eq!(outcome.completed, vec![n1.clone(), n2.clone()]);
        assert!(outcome.outputs.contains_key(&n1));
        assert!(outcome.outputs.contains_key(&n2));

        // Exactly two gateway calls reached the recording adapter, carrying the
        // two nodes' distinct prompts in order.
        let recorded = calls.lock().unwrap().clone();
        assert_eq!(recorded.len(), 2, "one gateway call per node: {recorded:?}");
        assert_eq!(recorded[0].1, "a");
        assert_eq!(recorded[1].1, "b");

        // The journal holds the exact event sequence, in order.
        let events = journal.load(run).await.expect("load");
        let kinds: Vec<String> = events.iter().map(|(_, e)| label(e)).collect();
        assert_eq!(
            kinds,
            vec![
                "RunStarted",
                "NodeStarted(n1)",
                "EffectRecorded(n1)",
                "NodeCompleted(n1)",
                "NodeStarted(n2)",
                "EffectRecorded(n2)",
                "NodeCompleted(n2)",
                "RunCompleted",
            ],
        );
    }
}
