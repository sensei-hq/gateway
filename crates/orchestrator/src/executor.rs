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

    /// Resume (or freshly start) a run from its durable journal — the headline
    /// crash/resume path that never re-spends tokens on already-recorded
    /// effects. Load the journal and:
    ///
    /// - **empty journal** ⇒ nothing to resume, delegate to [`run`](Self::run);
    /// - **version fence** ⇒ if the recorded `RunStarted.version` differs from
    ///   this executor's, refuse with [`VersionFenceMismatch`] (never resume a
    ///   run authored by a different executor version);
    /// - **already terminal** (a `RunCompleted` is present) ⇒ return the folded
    ///   outcome WITHOUT re-driving, so no second `RunCompleted` is appended;
    /// - **partial** ⇒ fold every `EffectRecorded` into the memo and hand off to
    ///   [`drive`](Self::drive), which replays the completed prefix (no gateway
    ///   call, no duplicate journal events), runs the tail, and appends
    ///   `RunCompleted` once.
    ///
    /// [`VersionFenceMismatch`]: OrchestratorError::VersionFenceMismatch
    pub async fn start(&self, run: RunId, graph: &Graph) -> Result<RunOutcome, OrchestratorError> {
        graph.validate_linear()?;
        let events = self
            .journal
            .load(run)
            .await
            .map_err(OrchestratorError::Journal)?;
        if events.is_empty() {
            // Nothing journaled → a fresh run (appends `RunStarted` itself).
            return self.run(run, graph).await;
        }

        // Version fence: the first recorded `RunStarted.version` must match ours.
        if let Some(recorded) = events.iter().find_map(|(_, e)| match e {
            JournalEvent::RunStarted { version } => Some(version.clone()),
            _ => None,
        }) && recorded != self.version
        {
            return Err(OrchestratorError::VersionFenceMismatch {
                recorded,
                current: self.version.clone(),
            });
        }

        // Fold the journal in `Seq` order: memoize every recorded effect and, for
        // an already-terminal run, reconstruct the completed outcome.
        let terminal = events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted));
        let mut memo: HashMap<EffectId, (String, serde_json::Value)> = HashMap::new();
        let mut outcome = RunOutcome::default();
        for (_, event) in &events {
            match event {
                JournalEvent::EffectRecorded {
                    node,
                    effect_id,
                    input_hash,
                    output,
                    ..
                } => {
                    memo.insert(effect_id.clone(), (input_hash.clone(), output.clone()));
                    outcome.outputs.insert(node.clone(), output.clone());
                }
                JournalEvent::NodeCompleted { node } => outcome.completed.push(node.clone()),
                _ => {}
            }
        }

        if terminal {
            // Already done: return the folded outcome; do NOT re-drive (which
            // would append a second `RunCompleted`).
            return Ok(outcome);
        }

        // Resume the tail: `drive`'s memo branch replays the completed prefix
        // (no gateway call, no new `EffectRecorded`) and finishes the run.
        self.drive(run, graph, &memo).await
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
    use crate::test_support::{demo_reference_gateway, failing_after_gateway, recording_gateway};
    use orchestrator_core::{Graph, JournalError, Node, NodeId, NodeKind};
    use orchestrator_store::InMemoryJournal;

    fn model_call(chain: &str, prompt: &str) -> NodeKind {
        NodeKind::ModelCall {
            chain: chain.to_string(),
            payload: serde_json::json!({ "prompt": prompt }),
        }
    }

    /// A canonical linear 2-node graph `[n1{prompt:p1} → n2{prompt:p2}]` on the
    /// recording chain `"c"`, returned with its node ids for assertions.
    fn two_node_graph(p1: &str, p2: &str) -> (Graph, NodeId, NodeId) {
        let n1 = NodeId("n1".into());
        let n2 = NodeId("n2".into());
        let graph = Graph {
            nodes: vec![
                Node {
                    id: n1.clone(),
                    kind: model_call("c", p1),
                    deps: vec![],
                },
                Node {
                    id: n2.clone(),
                    kind: model_call("c", p2),
                    deps: vec![n1.clone()],
                },
            ],
        };
        (graph, n1, n2)
    }

    /// A journal whose every `append` fails — proves a backend write error is
    /// surfaced as `OrchestratorError::Journal`, never swallowed.
    struct FailingJournal;

    #[async_trait::async_trait]
    impl ExecutionJournal for FailingJournal {
        async fn append(&self, _run: RunId, _event: JournalEvent) -> Result<Seq, JournalError> {
            Err(JournalError::Backend("injected backend failure".into()))
        }
        async fn load(&self, _run: RunId) -> Result<Vec<(Seq, JournalEvent)>, JournalError> {
            Ok(Vec::new())
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

    /// Headline / load-bearing: a run that dies after n1 resumes to completion
    /// WITHOUT re-spending tokens on n1 — the second gateway is called for n2
    /// only, because n1 is replayed from the journal.
    #[tokio::test]
    async fn start_resumes_without_respending_memoized_model_calls() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let (graph, n1, n2) = two_node_graph("a", "b");

        // Run 1: adapter succeeds on its 1st call (n1) and errors on its 2nd
        // (n2) — a provider dying mid-run. n1 is journaled+completed; n2 fails;
        // NO RunCompleted is written.
        let (gw1, calls1) = failing_after_gateway(1).await;
        let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1");
        let outcome1 = exec1
            .run(run, &graph)
            .await
            .expect("run 1 yields an outcome");
        assert_eq!(
            outcome1.completed,
            vec![n1.clone()],
            "only n1 completed in run 1"
        );
        match &outcome1.failed {
            Some((node, _)) => assert_eq!(node, &n2, "n2 is the failed node in run 1"),
            None => panic!("run 1 must fail at n2, got {:?}", outcome1.failed),
        }
        assert_eq!(
            calls1.lock().unwrap().len(),
            2,
            "run 1 hit the gateway for n1 and the failing n2"
        );

        // Run 2: a FRESH gateway + adapter that always succeeds, over the SAME
        // journal. `start` folds the journal, memoizes n1, and drives only the
        // tail.
        let (gw2, calls2) = recording_gateway().await;
        let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1");
        let outcome2 = exec2
            .start(run, &graph)
            .await
            .expect("start resumes the run");
        assert!(
            outcome2.failed.is_none(),
            "resume completes with no failure: {:?}",
            outcome2.failed
        );
        assert_eq!(
            outcome2.completed,
            vec![n1.clone(), n2.clone()],
            "both nodes completed after resume"
        );
        assert!(outcome2.outputs.contains_key(&n1));
        assert!(outcome2.outputs.contains_key(&n2));

        // The proof: run-2's gateway saw EXACTLY ONE call, carrying n2's prompt
        // "b". n1 was replayed from the journal — not re-spent.
        let recorded2 = calls2.lock().unwrap().clone();
        assert_eq!(
            recorded2.len(),
            1,
            "resume re-called the gateway only for the tail node n2: {recorded2:?}"
        );
        assert_eq!(
            recorded2[0].1, "b",
            "the single resume call carried n2's prompt"
        );

        // Exactly one RunCompleted across both runs (run 1 wrote none; the
        // resume wrote one), and the journal ends on it.
        let events = journal.load(run).await.expect("load");
        let completes = events
            .iter()
            .filter(|(_, e)| matches!(e, JournalEvent::RunCompleted))
            .count();
        assert_eq!(completes, 1, "exactly one RunCompleted across both runs");
        assert!(
            matches!(
                events.last().map(|(_, e)| e),
                Some(JournalEvent::RunCompleted)
            ),
            "the journal ends with RunCompleted"
        );
    }

    /// A resume whose graph changed under a completed node halts with a
    /// determinism violation — it never silently re-runs or re-memoizes, and
    /// never calls the gateway for the changed node.
    #[tokio::test]
    async fn start_halts_on_determinism_violation_without_calling_gateway() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let n1 = NodeId("n1".into());

        // Pre-seed a partial journal: n1 recorded for payload {prompt:"a"}, no
        // RunCompleted. Direct appends — independent of the gateway.
        journal
            .append(
                run,
                JournalEvent::RunStarted {
                    version: "v1".into(),
                },
            )
            .await
            .unwrap();
        let ih_a = input_hash("c", &serde_json::json!({ "prompt": "a" })).expect("hash");
        journal
            .append(
                run,
                JournalEvent::EffectRecorded {
                    node: n1.clone(),
                    effect_id: effect_id("", 0, 0),
                    class: EffectClass::Pure,
                    input_hash: ih_a,
                    seq: 0,
                    output: serde_json::json!({ "model": "m", "text": "canned-response" }),
                },
            )
            .await
            .unwrap();
        journal
            .append(run, JournalEvent::NodeCompleted { node: n1.clone() })
            .await
            .unwrap();

        // Resume with n1's payload CHANGED — its input hash no longer matches.
        let (gw, calls) = recording_gateway().await;
        let exec = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1");
        let (graph, _, _) = two_node_graph("CHANGED", "b");
        let err = exec
            .start(run, &graph)
            .await
            .expect_err("determinism violation halts the resume");
        match err {
            OrchestratorError::DeterminismViolation { node, .. } => assert_eq!(node, n1),
            other => panic!("expected DeterminismViolation, got {other:?}"),
        }
        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "a determinism violation never touches the gateway"
        );
    }

    /// A resume against a journal written by a different version is refused by
    /// the version fence — no gateway call, no silent re-run.
    #[tokio::test]
    async fn start_refuses_resume_on_version_fence_mismatch() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        journal
            .append(
                run,
                JournalEvent::RunStarted {
                    version: "v1".into(),
                },
            )
            .await
            .unwrap();

        let (gw, calls) = recording_gateway().await;
        let exec = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v2");
        let (graph, _, _) = two_node_graph("a", "b");
        let err = exec
            .start(run, &graph)
            .await
            .expect_err("version fence refuses the resume");
        match err {
            OrchestratorError::VersionFenceMismatch { recorded, current } => {
                assert_eq!(recorded, "v1");
                assert_eq!(current, "v2");
            }
            other => panic!("expected VersionFenceMismatch, got {other:?}"),
        }
        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "a fenced run never touches the gateway"
        );
    }

    /// A journal-backend write error is surfaced as `OrchestratorError::Journal`
    /// and aborts the run — it is never swallowed and the run does not silently
    /// continue to the gateway.
    #[tokio::test]
    async fn run_surfaces_a_journal_backend_error_instead_of_swallowing_it() {
        let (gw, calls) = recording_gateway().await;
        let exec = Executor::new(Arc::new(gw), Arc::new(FailingJournal), "v1");
        let (graph, _, _) = two_node_graph("a", "b");
        let run = RunId(uuid::Uuid::new_v4());

        let err = exec
            .run(run, &graph)
            .await
            .expect_err("a journal backend error surfaces");
        assert!(
            matches!(err, OrchestratorError::Journal(JournalError::Backend(_))),
            "expected OrchestratorError::Journal(Backend), got {err:?}"
        );
        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "the run aborts on the first failed append, before any gateway call"
        );
    }

    /// Resuming an already-completed run is a no-op re-append: it returns the
    /// folded outcome, does not re-drive (no gateway call), and appends no
    /// second RunCompleted.
    #[tokio::test]
    async fn start_on_a_completed_run_is_a_noop_reappend() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let (graph, n1, n2) = two_node_graph("a", "b");

        // Drive the run to full completion first.
        let (gw1, _calls1) = recording_gateway().await;
        let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1");
        let outcome1 = exec1.run(run, &graph).await.expect("first run completes");
        assert!(outcome1.failed.is_none());
        assert_eq!(outcome1.completed, vec![n1.clone(), n2.clone()]);

        let before = journal.load(run).await.unwrap();
        let completes_before = before
            .iter()
            .filter(|(_, e)| matches!(e, JournalEvent::RunCompleted))
            .count();
        assert_eq!(completes_before, 1, "one RunCompleted after the first run");

        // Resume the already-terminal run on a FRESH gateway: returns the folded
        // outcome without re-driving.
        let (gw2, calls2) = recording_gateway().await;
        let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1");
        let outcome2 = exec2
            .start(run, &graph)
            .await
            .expect("resume of a completed run");
        assert!(outcome2.failed.is_none());
        assert_eq!(
            outcome2.completed,
            vec![n1.clone(), n2.clone()],
            "folded outcome lists both completed nodes"
        );
        assert!(outcome2.outputs.contains_key(&n1));
        assert!(outcome2.outputs.contains_key(&n2));
        assert_eq!(
            calls2.lock().unwrap().len(),
            0,
            "a completed run is not re-driven — no gateway call"
        );

        let after = journal.load(run).await.unwrap();
        let completes_after = after
            .iter()
            .filter(|(_, e)| matches!(e, JournalEvent::RunCompleted))
            .count();
        assert_eq!(
            completes_after, 1,
            "resume of a completed run appends no second RunCompleted"
        );
        assert_eq!(
            after.len(),
            before.len(),
            "resume of a completed run appends nothing at all"
        );
    }

    /// Real end-to-end: the durable executor drives the REAL gateway assembled
    /// from the illustrative demo catalog (`gateway::catalog::assemble(
    /// demo_catalog())`) over a REFERENCE chain (`research.bulk`). The selector
    /// walks `groq-llama-free` (no adapter → fall over) → `deepseek-chat` (no
    /// adapter → fall over) → `llama3.1-local` (served by the local ollama
    /// adapter). The run completes and the orchestrator records the model the
    /// chain fell over to — proving the spine drives the real gateway + a real
    /// reference chain, not a bespoke test-only single-model chain.
    #[tokio::test]
    async fn run_drives_real_reference_chain_end_to_end_to_local_fallover() {
        let (gateway, calls) = demo_reference_gateway().await;
        let journal = InMemoryJournal::new();
        let executor = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");

        // A single `ModelCall` node on the reference chain `research.bulk`.
        let n1 = NodeId("n1".into());
        let graph = Graph {
            nodes: vec![Node {
                id: n1.clone(),
                kind: NodeKind::ModelCall {
                    chain: "research.bulk".into(),
                    payload: serde_json::json!({ "prompt": "hello" }),
                },
                deps: vec![],
            }],
        };

        let run = RunId(uuid::Uuid::new_v4());
        let outcome = executor.run(run, &graph).await.expect("run succeeds");

        // The reference chain ran to completion via genuine fallover.
        assert!(
            outcome.failed.is_none(),
            "the reference chain runs to completion via fallover: {:?}",
            outcome.failed
        );
        assert_eq!(outcome.completed, vec![n1.clone()], "n1 completed");
        // The load-bearing assertion: the orchestrator recorded that the chain
        // fell over the credential-gated cloud entries to the LOCAL model.
        assert_eq!(
            outcome.outputs[&n1]["model"], "llama3.1-local",
            "the reference chain fell over cloud entries to the local model, recorded by the orchestrator: {:?}",
            outcome.outputs[&n1],
        );

        // The chain genuinely reached the local adapter (the terminal candidate
        // was served, not short-circuited earlier).
        assert_eq!(
            calls.lock().unwrap().len(),
            1,
            "the served terminal candidate hit the local ollama adapter exactly once",
        );

        // And the journal is a clean single-node run ending on RunCompleted.
        let events = journal.load(run).await.expect("load");
        let kinds: Vec<String> = events.iter().map(|(_, e)| label(e)).collect();
        assert_eq!(
            kinds,
            vec![
                "RunStarted",
                "NodeStarted(n1)",
                "EffectRecorded(n1)",
                "NodeCompleted(n1)",
                "RunCompleted",
            ],
        );
    }
}
