//! The deterministic executor: drives a linear `ModelCall` graph through the
//! gateway, journaling every step so a crashed run can resume (Task 4).

use std::collections::HashMap;
use std::sync::Arc;

use gateway::Gateway;
use kernel::types::capability::Capability;
use kernel::types::request::{
    InferenceRequest, Message, MessageContent, MessageRole, Payload, ToolCall, ToolDefinition,
};
use orchestrator_core::{
    AgentDefinition, AgentRef, EffectClass, EffectId, ExecutionJournal, Graph, JournalEvent,
    NodeId, NodeKind, OrchestratorError, Registry, RunId, Seq, effect_id,
};
use sha2::{Digest, Sha256};

use crate::agent::prompt::{assemble_prompt, over_budget};
use crate::agent::tools::ToolRegistry;

/// The deterministic executor over a durable journal, wired to the gateway.
pub struct Executor {
    gateway: Arc<Gateway>,
    journal: Arc<dyn ExecutionJournal>,
    version: String,
    registry: Arc<Registry>,
    tools: Arc<ToolRegistry>,
    max_steps: usize,
}

/// The terminal outcome of a run: the nodes that completed, the first failure
/// (which halts the run), and each node's memoized output.
#[derive(Debug, Default)]
pub struct RunOutcome {
    pub completed: Vec<NodeId>,
    pub failed: Option<(NodeId, String)>,
    pub outputs: HashMap<NodeId, serde_json::Value>,
}

/// The state folded from a journal on resume: the effect memo plus which nodes
/// have already been started/completed (so an Agent node's `NodeStarted`/
/// `NodeCompleted` are appended at most once across resumes).
#[derive(Default)]
struct Fold {
    memo: HashMap<EffectId, (String, serde_json::Value)>,
    started: std::collections::HashSet<NodeId>,
    completed: std::collections::HashSet<NodeId>,
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
            registry: Arc::new(Registry::default()),
            tools: Arc::new(ToolRegistry::default()),
            max_steps: 8,
        }
    }

    /// Attach the agent registry an `Agent` node resolves its definition against.
    pub fn with_registry(mut self, registry: Arc<Registry>) -> Self {
        self.registry = registry;
        self
    }

    /// Attach the executable tool runtime an `Agent` node dispatches Pure calls to.
    pub fn with_tools(mut self, tools: Arc<ToolRegistry>) -> Self {
        self.tools = tools;
        self
    }

    /// Override the ReAct loop's max turns (default 8).
    pub fn with_max_steps(mut self, n: usize) -> Self {
        self.max_steps = n;
        self
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
        self.drive(run, graph, &Fold::default()).await
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
        let mut fold = Fold::default();
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
                    fold.memo
                        .insert(effect_id.clone(), (input_hash.clone(), output.clone()));
                    outcome.outputs.insert(node.clone(), output.clone());
                }
                JournalEvent::NodeStarted { node } => {
                    fold.started.insert(node.clone());
                }
                JournalEvent::NodeCompleted { node } => {
                    fold.completed.insert(node.clone());
                    outcome.completed.push(node.clone());
                }
                _ => {}
            }
        }

        if terminal {
            // Already done: return the folded outcome; do NOT re-drive (which
            // would append a second `RunCompleted`). But first project each Agent
            // node's folded output — the RAW final model-turn effect (`{model,
            // text, tool_calls}`) — down to the canonical `{model, text}` that a
            // fresh `run` and a non-terminal resume return from
            // `AgentStep::Completed`, so a completed Agent node yields an
            // identical JSON shape on every completion path (design §4). This is a
            // pure projection of the already-folded outputs — no re-drive, no
            // append. `ModelCall` nodes already store the canonical shape and are
            // left untouched.
            for node in &graph.nodes {
                if let NodeKind::Agent { .. } = &node.kind
                    && let Some(output) = outcome.outputs.get(&node.id).cloned()
                {
                    let model = output
                        .get("model")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    let text = output.get("text").cloned().unwrap_or_default();
                    outcome.outputs.insert(
                        node.id.clone(),
                        serde_json::json!({ "model": model, "text": text }),
                    );
                }
            }
            return Ok(outcome);
        }

        // Resume the tail: `drive`'s memo branch replays the completed prefix
        // (no gateway call, no new `EffectRecorded`) and finishes the run.
        self.drive(run, graph, &fold).await
    }

    /// Shared node loop for both `run` (an empty [`Fold`]) and `start` (a `Fold`
    /// folded from the journal, Task 4). The `fold: &Fold` carries three sets:
    ///
    /// - `fold.memo` maps each effect's structural [`EffectId`] to its recorded
    ///   `(input_hash, output)`. A hit whose input-hash matches replays the
    ///   recorded output with NO gateway call and NO new `EffectRecorded` (it is
    ///   already journaled); a hit whose input-hash differs is a determinism
    ///   violation (the graph changed under a resume) — halt; a miss executes the
    ///   node against the gateway and journals it.
    /// - `fold.started` / `fold.completed` name the nodes whose `NodeStarted` /
    ///   `NodeCompleted` are already journaled, so an `Agent` node's ReAct loop
    ///   appends each at most once across resumes (a `ModelCall` node runs
    ///   atomically per drive and does not consult them).
    ///
    /// For a fresh `run` the fold is empty, so every node executes; the memo
    /// branches exist for Task 4's resume and are reachable code.
    async fn drive(
        &self,
        run: RunId,
        graph: &Graph,
        fold: &Fold,
    ) -> Result<RunOutcome, OrchestratorError> {
        let mut outcome = RunOutcome::default();
        for (index, node) in graph.nodes.iter().enumerate() {
            match &node.kind {
                NodeKind::ModelCall { chain, payload } => {
                    let eid = effect_id("", 0, index);
                    let ih = input_hash(chain, payload)?;

                    if let Some((recorded_ih, output)) = fold.memo.get(&eid) {
                        if recorded_ih != &ih {
                            return Err(OrchestratorError::DeterminismViolation {
                                node: node.id.clone(),
                                effect_id: eid,
                            });
                        }
                        // Memoized: replay the recorded output — no gateway call, no
                        // new `EffectRecorded` (it is already in the journal).
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
                NodeKind::Agent { agent, input } => {
                    match self.drive_agent(run, node, agent, input, fold).await? {
                        AgentStep::Completed(output) => {
                            outcome.outputs.insert(node.id.clone(), output);
                            outcome.completed.push(node.id.clone());
                        }
                        AgentStep::Failed(message) => {
                            outcome.failed = Some((node.id.clone(), message));
                            return Ok(outcome);
                        }
                    }
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

/// The terminal result of one `Agent` node: a completed output, or a node-level
/// failure (budget/max-steps/gateway/tool) already journaled as `NodeFailed`.
enum AgentStep {
    Completed(serde_json::Value),
    Failed(String),
}

impl Executor {
    /// Run one `Agent` node's ReAct loop. Each turn is a Pure `ModelCall` effect
    /// (iteration-aware id `effect_id(node.id, turn, 0)`); each Pure tool call is a
    /// Pure effect (`effect_id(node.id, turn, k+1)`). Memoized turns/tools replay
    /// from the journal with no gateway call and no re-execution (resume without
    /// re-spend); an input-hash mismatch halts with `DeterminismViolation`.
    async fn drive_agent(
        &self,
        run: RunId,
        node: &orchestrator_core::Node,
        agent_ref: &AgentRef,
        input: &serde_json::Value,
        fold: &Fold,
    ) -> Result<AgentStep, OrchestratorError> {
        let agent: &AgentDefinition = self
            .registry
            .agent(&agent_ref.0)
            .ok_or_else(|| OrchestratorError::UnknownAgent(agent_ref.0.clone()))?;
        let (system, tools) = assemble_prompt(&self.registry, agent)?;
        let chain = agent.chain.clone();
        let min_win = self.gateway.min_context_window(&chain).await;

        let mut messages: Vec<Message> =
            vec![Message::text(MessageRole::User, render_input(input))];
        let mut node_started = fold.started.contains(&node.id);

        for turn in 0..self.max_steps {
            let eid = effect_id(&node.id.0, turn as u64, 0);
            let ih = agent_input_hash(&chain, &system, &messages, &tools)?;

            // Reuse a memoized turn (resume): no gateway call, no re-append.
            let turn_output = if let Some((recorded_ih, output)) = fold.memo.get(&eid) {
                if recorded_ih != &ih {
                    return Err(OrchestratorError::DeterminismViolation {
                        node: node.id.clone(),
                        effect_id: eid,
                    });
                }
                output.clone()
            } else {
                // Live turn: budget → NodeStarted (once) → gateway → EffectRecorded.
                if over_budget(min_win, &system, &messages, &tools) {
                    let est = est_prompt_tokens(&system, &messages, &tools);
                    let err = OrchestratorError::PromptOverBudget {
                        node: node.id.clone(),
                        turn,
                        est,
                        min_win: min_win.unwrap_or(0),
                    };
                    let message = err.to_string();
                    self.append(
                        run,
                        JournalEvent::NodeFailed {
                            node: node.id.clone(),
                            error: message.clone(),
                        },
                    )
                    .await?;
                    return Ok(AgentStep::Failed(message));
                }
                if !node_started {
                    self.append(
                        run,
                        JournalEvent::NodeStarted {
                            node: node.id.clone(),
                        },
                    )
                    .await?;
                    node_started = true;
                }
                let request = build_chat_request(&chain, &system, messages.clone(), tools.clone());
                match self.gateway.execute(&request).await {
                    Ok(response) => {
                        let output = serde_json::json!({
                            "model": response.model,
                            "text": response.content.clone().unwrap_or_default(),
                            "tool_calls": response.tool_calls,
                        });
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
                        output
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
                        return Ok(AgentStep::Failed(message));
                    }
                }
            };

            let tool_calls: Vec<ToolCall> = serde_json::from_value(
                turn_output
                    .get("tool_calls")
                    .cloned()
                    .unwrap_or(serde_json::json!([])),
            )?;
            if tool_calls.is_empty() {
                // Final answer.
                if !fold.completed.contains(&node.id) {
                    self.append(
                        run,
                        JournalEvent::NodeCompleted {
                            node: node.id.clone(),
                        },
                    )
                    .await?;
                }
                let text = turn_output.get("text").cloned().unwrap_or_default();
                let model = turn_output
                    .get("model")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                return Ok(AgentStep::Completed(
                    serde_json::json!({ "model": model, "text": text }),
                ));
            }

            // Execute (or replay) each tool call, then extend the transcript.
            let assistant_text = turn_output
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            messages.push(Message {
                role: MessageRole::Assistant,
                content: MessageContent::Text {
                    text: assistant_text,
                },
                tool_calls: tool_calls.clone(),
                attachments: Vec::new(),
            });
            for (k, call) in tool_calls.iter().enumerate() {
                let teid = effect_id(&node.id.0, turn as u64, k + 1);
                let args: serde_json::Value =
                    serde_json::from_str(&call.arguments).unwrap_or(serde_json::Value::Null);
                let tih = tool_input_hash(&call.name, &call.arguments);
                let result = if let Some((recorded_ih, output)) = fold.memo.get(&teid) {
                    if recorded_ih != &tih {
                        return Err(OrchestratorError::DeterminismViolation {
                            node: node.id.clone(),
                            effect_id: teid,
                        });
                    }
                    output.clone()
                } else {
                    match self.tools.execute(&call.name, args) {
                        Ok(result) => {
                            self.append(
                                run,
                                JournalEvent::EffectRecorded {
                                    node: node.id.clone(),
                                    effect_id: teid,
                                    class: EffectClass::Pure,
                                    input_hash: tih,
                                    seq: 0,
                                    output: result.clone(),
                                },
                            )
                            .await?;
                            result
                        }
                        Err(err) => {
                            let message = err.to_string();
                            self.append(
                                run,
                                JournalEvent::NodeFailed {
                                    node: node.id.clone(),
                                    error: message.clone(),
                                },
                            )
                            .await?;
                            return Ok(AgentStep::Failed(message));
                        }
                    }
                };
                messages.push(Message::tool_result(call.id.clone(), result.to_string()));
            }
        }

        // Ran out of steps without a final answer.
        let err = OrchestratorError::AgentMaxStepsExceeded {
            node: node.id.clone(),
        };
        let message = err.to_string();
        self.append(
            run,
            JournalEvent::NodeFailed {
                node: node.id.clone(),
                error: message.clone(),
            },
        )
        .await?;
        Ok(AgentStep::Failed(message))
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

/// Render an agent node's JSON `input` into user-message text: a JSON string
/// passes through; any other value is serialized (deterministic — feeds the hash).
fn render_input(input: &serde_json::Value) -> String {
    match input {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Compile one ReAct turn into a chat `InferenceRequest` (system + transcript +
/// tools) over the agent's chain. `budget: None` — cost budgeting is the gateway's
/// dormant axis in slice 2 (see the design); this request carries only window-fit.
fn build_chat_request(
    chain: &str,
    system: &str,
    messages: Vec<Message>,
    tools: Vec<ToolDefinition>,
) -> InferenceRequest {
    InferenceRequest {
        capability: Capability::TextChat,
        model: None,
        router: None,
        chain: Some(chain.to_string()),
        payload: Payload::Chat {
            messages,
            system: Some(system.to_string()),
            max_tokens: None,
            temperature: None,
            tools,
        },
        budget: None,
        auth: None,
        panel: None,
        consensus: None,
        allow_fallback: true,
        credentials: Default::default(),
    }
}

/// Determinism key for a ReAct turn: `sha256_hex(chain | system | messages | tools)`.
fn agent_input_hash(
    chain: &str,
    system: &str,
    messages: &[Message],
    tools: &[ToolDefinition],
) -> Result<String, OrchestratorError> {
    let messages = serde_json::to_string(messages)?;
    let tools = serde_json::to_string(tools)?;
    let mut hasher = Sha256::new();
    hasher.update(format!("{chain}|{system}|{messages}|{tools}").as_bytes());
    Ok(format!("{:x}", hasher.finalize()))
}

/// Determinism key for a Pure tool call: `sha256_hex(name | arguments)`.
fn tool_input_hash(name: &str, arguments: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(format!("{name}|{arguments}").as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Estimate a prompt's tokens (for the over-budget diagnostic's `est`).
fn est_prompt_tokens(system: &str, messages: &[Message], tools: &[ToolDefinition]) -> usize {
    use crate::agent::prompt::est_tokens;
    let mut est = est_tokens(system);
    for m in messages {
        est += est_tokens(m.content.as_text());
    }
    for t in tools {
        est += est_tokens(&t.name)
            + t.description.as_deref().map(est_tokens).unwrap_or(0)
            + est_tokens(&t.input_schema.to_string());
    }
    est
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        demo_reference_gateway, failing_after_gateway, final_response, recording_gateway,
        scripted_gateway, tool_call_response,
    };
    use orchestrator_core::{Graph, JournalError, Node, NodeId, NodeKind};
    use orchestrator_store::InMemoryJournal;

    use crate::agent::tools::{Calc, Tool, ToolRegistry};
    use orchestrator_core::{AgentDefinition, AgentRef, Registry};
    use std::sync::Arc;

    fn agent_def(chain: &str) -> AgentDefinition {
        AgentDefinition {
            name: "a".into(),
            area: "research".into(),
            kind: "reasoning".into(),
            chain: chain.into(),
            tools: vec![],
            skills: vec![],
            system_prompt: "SYS".into(),
        }
    }

    /// A demo registry/executor: one agent "a" on the recording chain "c".
    fn agent_registry(chain: &str) -> Arc<Registry> {
        Arc::new(Registry::default().with_agent(agent_def(chain)))
    }

    fn agent_node(id: &str, agent: &str, input: &str) -> Node {
        Node {
            id: NodeId(id.into()),
            kind: NodeKind::Agent {
                agent: AgentRef(agent.into()),
                input: serde_json::json!(input),
            },
            deps: vec![],
        }
    }

    fn tool_agent_registry() -> Arc<Registry> {
        // The core `Registry` needs the tool's *schema* (`ToolSpec`, via
        // `Tool::spec()`) to compile it into the prompt (`assemble_prompt`);
        // the *executable* side is the separate `ToolRegistry` (`calc_tools`).
        Arc::new(
            Registry::default()
                .with_agent(AgentDefinition {
                    tools: vec!["calc".into()],
                    ..agent_def("c")
                })
                .with_tool(Calc.spec()),
        )
    }
    fn calc_tools() -> Arc<ToolRegistry> {
        Arc::new(ToolRegistry::default().with_tool(Arc::new(Calc)))
    }

    #[tokio::test]
    async fn agent_react_loop_executes_a_pure_tool_and_feeds_the_result_back() {
        let (gateway, calls) = scripted_gateway(vec![
            tool_call_response("t1", "calc", "{\"op\":\"add\",\"a\":2,\"b\":3}"),
            final_response("the answer is 5"),
        ])
        .await;
        let journal = InMemoryJournal::new();
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
            .with_registry(tool_agent_registry())
            .with_tools(calc_tools());

        let n1 = NodeId("n1".into());
        let graph = Graph {
            nodes: vec![agent_node("n1", "a", "add 2 and 3")],
        };
        let run = RunId(uuid::Uuid::new_v4());
        let outcome = exec.run(run, &graph).await.expect("run");

        assert!(outcome.failed.is_none(), "{:?}", outcome.failed);
        assert_eq!(outcome.outputs[&n1]["text"], "the answer is 5");
        assert_eq!(calls.lock().unwrap().len(), 2, "two model turns");

        let kinds: Vec<String> = journal
            .load(run)
            .await
            .unwrap()
            .iter()
            .map(|(_, e)| label(e))
            .collect();
        assert_eq!(
            kinds,
            vec![
                "RunStarted",
                "NodeStarted(n1)",
                "EffectRecorded(n1)",
                "EffectRecorded(n1)", // turn-0 model + calc
                "EffectRecorded(n1)", // turn-1 model (final)
                "NodeCompleted(n1)",
                "RunCompleted",
            ]
        );
    }

    #[tokio::test]
    async fn agent_rejects_a_non_pure_tool_loudly() {
        let (gateway, _calls) =
            scripted_gateway(vec![tool_call_response("t1", "read", "{}")]).await;
        let journal = InMemoryJournal::new();
        struct Reader;
        impl crate::agent::tools::Tool for Reader {
            fn spec(&self) -> orchestrator_core::ToolSpec {
                orchestrator_core::ToolSpec {
                    name: "read".into(),
                    description: None,
                    input_schema: serde_json::json!({}),
                    effect_class: orchestrator_core::EffectClass::Observation,
                }
            }
            fn call(&self, _a: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
                Ok(serde_json::json!({}))
            }
        }
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
            .with_registry(Arc::new(
                Registry::default()
                    .with_agent(AgentDefinition {
                        tools: vec!["read".into()],
                        ..agent_def("c")
                    })
                    .with_tool(Reader.spec()),
            ))
            .with_tools(Arc::new(
                ToolRegistry::default().with_tool(Arc::new(Reader)),
            ));
        let graph = Graph {
            nodes: vec![agent_node("n1", "a", "read")],
        };
        let outcome = exec
            .run(RunId(uuid::Uuid::new_v4()), &graph)
            .await
            .expect("outcome");
        let (_, msg) = outcome.failed.expect("non-Pure tool fails the node");
        assert!(msg.contains("slice 4"), "deferral message: {msg}");
    }

    #[tokio::test]
    async fn agent_halts_at_max_steps_when_the_model_never_finalizes() {
        let (gateway, calls) = scripted_gateway(vec![
            tool_call_response("t1", "calc", "{\"op\":\"add\",\"a\":1,\"b\":1}"),
            tool_call_response("t2", "calc", "{\"op\":\"add\",\"a\":1,\"b\":1}"),
        ])
        .await;
        let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
            .with_registry(tool_agent_registry())
            .with_tools(calc_tools())
            .with_max_steps(2);
        let graph = Graph {
            nodes: vec![agent_node("n1", "a", "loop")],
        };
        let outcome = exec
            .run(RunId(uuid::Uuid::new_v4()), &graph)
            .await
            .expect("outcome");
        let (_, msg) = outcome.failed.expect("max_steps halts");
        assert!(msg.contains("max_steps"), "{msg}");
        assert_eq!(
            calls.lock().unwrap().len(),
            2,
            "exactly max_steps model turns"
        );
    }

    #[tokio::test]
    async fn agent_node_single_turn_runs_through_gateway_and_journals() {
        let (gateway, calls) = recording_gateway().await; // returns empty tool_calls → final on turn 0
        let journal = InMemoryJournal::new();
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
            .with_registry(agent_registry("c"))
            .with_tools(Arc::new(ToolRegistry::default().with_tool(Arc::new(Calc))));

        let n1 = NodeId("n1".into());
        let graph = Graph {
            nodes: vec![agent_node("n1", "a", "hello")],
        };
        let run = RunId(uuid::Uuid::new_v4());
        let outcome = exec.run(run, &graph).await.expect("run");

        assert!(outcome.failed.is_none(), "{:?}", outcome.failed);
        assert_eq!(outcome.completed, vec![n1.clone()]);
        assert_eq!(outcome.outputs[&n1]["text"], "canned-response");
        assert_eq!(
            calls.lock().unwrap().len(),
            1,
            "one model turn, one gateway call"
        );

        let kinds: Vec<String> = journal
            .load(run)
            .await
            .unwrap()
            .iter()
            .map(|(_, e)| label(e))
            .collect();
        assert_eq!(
            kinds,
            vec![
                "RunStarted",
                "NodeStarted(n1)",
                "EffectRecorded(n1)",
                "NodeCompleted(n1)",
                "RunCompleted"
            ]
        );
    }

    #[tokio::test]
    async fn agent_node_halts_over_budget_before_any_gateway_call() {
        let (gateway, calls) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        // max_context of chain "c" is 4096; force a tiny window via max_steps? No —
        // budget uses the chain window. Use a registry whose agent has a huge body.
        let big = AgentDefinition {
            system_prompt: "x".repeat(100_000),
            ..agent_def("c")
        };
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
            .with_registry(Arc::new(Registry::default().with_agent(big)))
            .with_tools(Arc::new(ToolRegistry::default()));

        let graph = Graph {
            nodes: vec![agent_node("n1", "a", "hi")],
        };
        let run = RunId(uuid::Uuid::new_v4());
        let outcome = exec.run(run, &graph).await.expect("run yields an outcome");
        match &outcome.failed {
            Some((node, msg)) => {
                assert_eq!(node.0, "n1");
                assert!(msg.contains("over budget"), "{msg}");
            }
            None => panic!("expected an over-budget failure"),
        }
        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "over-budget halts before spending"
        );
    }

    /// A terminal resume of a completed Agent node returns the SAME canonical
    /// `{model, text}` output as the original `run` — not the raw 3-key final
    /// model-turn effect (`{model, text, tool_calls}`). Proves the durable output
    /// shape is identical across every completion path, while preserving the
    /// no-op-reappend contract (the terminal resume appends nothing).
    #[tokio::test]
    async fn agent_node_terminal_resume_yields_canonical_output_shape() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let n1 = NodeId("n1".into());
        let graph = Graph {
            nodes: vec![agent_node("n1", "a", "hello")],
        };

        // Run 1: drive the single-turn agent node to full completion.
        let (gw1, _calls1) = recording_gateway().await;
        let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
            .with_registry(agent_registry("c"))
            .with_tools(Arc::new(ToolRegistry::default().with_tool(Arc::new(Calc))));
        let outcome1 = exec1.run(run, &graph).await.expect("first run completes");
        assert!(outcome1.failed.is_none());
        assert_eq!(outcome1.completed, vec![n1.clone()]);

        let before = journal.load(run).await.unwrap();

        // Terminal resume on a FRESH gateway: returns the folded outcome without
        // re-driving, projected to the canonical agent-node output shape.
        let (gw2, calls2) = recording_gateway().await;
        let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
            .with_registry(agent_registry("c"))
            .with_tools(Arc::new(ToolRegistry::default().with_tool(Arc::new(Calc))));
        let outcome2 = exec2
            .start(run, &graph)
            .await
            .expect("resume of a completed agent run");

        // Same canonical shape on terminal resume as on the original run.
        assert_eq!(
            outcome2.outputs[&n1], outcome1.outputs[&n1],
            "terminal resume yields the same output shape as the original run"
        );
        // Canonical `{model, text}` — NOT the 3-key raw model-turn effect.
        assert!(
            outcome2.outputs[&n1].get("tool_calls").is_none(),
            "terminal-resume agent output is canonical (no raw tool_calls key): {:?}",
            outcome2.outputs[&n1]
        );
        assert_eq!(
            calls2.lock().unwrap().len(),
            0,
            "a completed run is not re-driven — no gateway call"
        );

        // No-op reappend preserved: the terminal resume appended nothing.
        let after = journal.load(run).await.unwrap();
        assert_eq!(
            after.len(),
            before.len(),
            "terminal resume of a completed agent run appends nothing"
        );
    }

    /// Headline: a run that dies at turn 1 resumes and completes WITHOUT re-calling
    /// the gateway for turn 0 or re-executing turn 0's tool — memoized on resume.
    #[tokio::test]
    async fn agent_resume_does_not_respend_completed_turns() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let graph = Graph {
            nodes: vec![agent_node("n1", "a", "add 2 and 3")],
        };

        // Run 1: turn 0 (calc tool_call) succeeds, then turn 1 is scripted to ERROR
        // (script exhausted → ProviderError). Turn 0's model + calc effects are
        // journaled; the node fails at turn 1; NO RunCompleted.
        let (gw1, calls1) = scripted_gateway(vec![tool_call_response(
            "t1",
            "calc",
            "{\"op\":\"add\",\"a\":2,\"b\":3}",
        )])
        .await;
        let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
            .with_registry(tool_agent_registry())
            .with_tools(calc_tools());
        let outcome1 = exec1
            .run(run, &graph)
            .await
            .expect("run 1 yields an outcome");
        assert!(outcome1.failed.is_some(), "run 1 fails at turn 1");
        assert_eq!(
            calls1.lock().unwrap().len(),
            2,
            "run 1 called the gateway for turn 0 and the failing turn 1"
        );

        // Run 2: a FRESH scripted gateway that serves ONLY turn 1's final answer,
        // over the SAME journal. Resume memoizes turn 0 (model + calc) → the run-2
        // gateway is called exactly once (turn 1).
        let (gw2, calls2) = scripted_gateway(vec![final_response("the answer is 5")]).await;
        let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
            .with_registry(tool_agent_registry())
            .with_tools(calc_tools());
        let outcome2 = exec2.start(run, &graph).await.expect("resume completes");
        assert!(outcome2.failed.is_none(), "{:?}", outcome2.failed);
        assert_eq!(
            outcome2.outputs[&NodeId("n1".into())]["text"],
            "the answer is 5"
        );

        // The proof: run-2's gateway saw EXACTLY ONE call (turn 1). Turn 0 was
        // replayed from the journal — not re-spent — and calc was not re-executed.
        assert_eq!(
            calls2.lock().unwrap().len(),
            1,
            "resume re-spent nothing for turn 0: {:?}",
            calls2.lock().unwrap()
        );
        let events = journal.load(run).await.unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|(_, e)| matches!(e, JournalEvent::RunCompleted))
                .count(),
            1
        );

        // The non-vacuous proof: turn 0's model effect and its calc tool effect
        // each appear in EXACTLY ONE `EffectRecorded` across BOTH runs — recorded
        // once in run 1, and NOT re-appended in run 2. If the memo lookup were
        // broken (forcing turn 0 to re-run live on resume), these effects would
        // be re-recorded and this count would be 2, even though `calls2 == 1` and
        // the final-text assertion above would still spuriously pass (a lone
        // scripted final response finalizes a wrongly-re-run turn 0 in one call).
        let recorded_count = |eid: &EffectId| {
            events
                .iter()
                .filter(|(_, e)| {
                    matches!(e, JournalEvent::EffectRecorded { effect_id: rec, .. } if rec == eid)
                })
                .count()
        };
        assert_eq!(
            recorded_count(&effect_id("n1", 0, 0)),
            1,
            "turn 0's model call was replayed from the journal on resume (memoized), not re-recorded/re-spent"
        );
        assert_eq!(
            recorded_count(&effect_id("n1", 0, 1)),
            1,
            "turn 0's calc tool was memoized on resume, not re-executed/re-recorded"
        );
    }

    /// Editing a skill body changes the turn's system prompt → its input-hash no
    /// longer matches the memoized turn → resume halts with DeterminismViolation
    /// (never mixes new instructions into a memoized old turn). No gateway call.
    #[tokio::test]
    async fn agent_resume_halts_when_a_skill_changed_under_a_completed_turn() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());

        // A registry with agent "a" (skill "s") whose skill body is parameterized.
        let registry = |body: &str| {
            Arc::new(
                Registry::default()
                    .with_agent(AgentDefinition {
                        skills: vec!["s".into()],
                        ..agent_def("c")
                    })
                    .with_skill(orchestrator_core::SkillDef {
                        name: "s".into(),
                        description: None,
                        body: body.into(),
                    }),
            )
        };

        // Graph [agent n1, model n2]. Run 1 with skill body "V1": n1's single turn
        // succeeds (gateway call 1), then n2 fails (gateway call 2) → n1 is fully
        // journaled+completed, but there is NO RunCompleted (a partial run to resume).
        let graph = Graph {
            nodes: vec![
                agent_node("n1", "a", "hi"),
                Node {
                    id: NodeId("n2".into()),
                    kind: model_call("c", "b"),
                    deps: vec![NodeId("n1".into())],
                },
            ],
        };
        let (gw1, _c1) = failing_after_gateway(1).await;
        let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
            .with_registry(registry("V1"));
        let out1 = exec1
            .run(run, &graph)
            .await
            .expect("run 1 yields an outcome");
        assert!(
            out1.failed.is_some(),
            "n2 fails, leaving n1's turn journaled without RunCompleted"
        );

        // Run 2: resume with skill body CHANGED to "V2" → n1's turn system prompt
        // (and thus input-hash) differs from the memoized turn → determinism halt.
        let (gw2, calls2) = recording_gateway().await;
        let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
            .with_registry(registry("V2"));
        let err = exec2
            .start(run, &graph)
            .await
            .expect_err("determinism violation");
        assert!(
            matches!(err, OrchestratorError::DeterminismViolation { .. }),
            "got {err:?}"
        );
        assert_eq!(
            calls2.lock().unwrap().len(),
            0,
            "a determinism violation never touches the gateway"
        );
    }

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

    /// Real end-to-end: an `Agent` node whose role resolves to the reference chain
    /// `research.bulk` drives the REAL gateway (assembled from `demo_catalog`). The
    /// chain falls over the credential-gated cloud entries to the local ollama
    /// model; the agent's single (no-tool) turn is served by `llama3.1-local`.
    #[tokio::test]
    async fn agent_node_drives_real_reference_chain_to_local_fallover() {
        let (gateway, calls) = demo_reference_gateway().await;
        let journal = InMemoryJournal::new();
        let registry = Arc::new(Registry::default().with_agent(AgentDefinition {
            name: "researcher".into(),
            area: "research".into(),
            kind: "reasoning".into(),
            chain: "research.bulk".into(),
            tools: vec![],
            skills: vec![],
            system_prompt: "Research carefully.".into(),
        }));
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
            .with_registry(registry)
            .with_tools(Arc::new(ToolRegistry::default()));

        let n1 = NodeId("n1".into());
        let graph = Graph {
            nodes: vec![agent_node("n1", "researcher", "summarize the news")],
        };
        let outcome = exec
            .run(RunId(uuid::Uuid::new_v4()), &graph)
            .await
            .expect("run");

        assert!(outcome.failed.is_none(), "{:?}", outcome.failed);
        assert_eq!(
            outcome.outputs[&n1]["model"], "llama3.1-local",
            "fell over to the local model: {:?}",
            outcome.outputs[&n1]
        );
        assert_eq!(
            calls.lock().unwrap().len(),
            1,
            "the served terminal candidate hit the local adapter once"
        );
    }
}
