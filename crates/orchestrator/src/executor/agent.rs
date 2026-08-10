//! The agent runtime on the executor: the durable ReAct loop (`drive_agent`) and
//! its per-turn tool execution (`run_agent_tools`). Split out of `super` for
//! readability; both are `impl Executor` methods and share its private state.

use kernel::types::request::{InferenceRequest, Message, MessageContent, MessageRole, ToolCall};
use orchestrator_core::{
    AgentDefinition, AgentRef, EffectClass, EffectId, JournalEvent, NodeId, OrchestratorError,
    RunId, effect_id,
};

use super::support::{
    agent_input_hash, build_chat_request, est_prompt_tokens, render_input, tool_input_hash,
};
use super::{AgentStep, Executor, Fold};
use crate::agent::prompt::{assemble_prompt, over_budget};

impl Executor {
    /// Run one agent ReAct loop against `node_id` — a top-level `Agent` node's id,
    /// or a `Map`/`Consolidate` child path (`"{map}/{i}"`); the id is the only
    /// thing the loop needs, so the same driver serves both. Each turn is a Pure
    /// `ModelCall` effect (iteration-aware id `effect_id(node_id, turn, 0)`); each
    /// Pure tool call is a Pure effect (`effect_id(node_id, turn, k+1)`). Memoized
    /// turns/tools replay from the journal with no gateway call and no
    /// re-execution (resume without re-spend); an input-hash mismatch halts with
    /// `DeterminismViolation`.
    pub(super) async fn drive_agent(
        &self,
        run: RunId,
        node_id: &NodeId,
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
        let mut node_started = fold.started.contains(node_id);

        for turn in 0..self.max_steps {
            let eid = effect_id(&node_id.0, turn as u64, 0);
            let ih = agent_input_hash(&chain, &system, &messages, &tools)?;

            // Reuse a memoized turn (resume): no gateway call, no re-append.
            let turn_output = if let Some((recorded_ih, output)) = fold.memo.get(&eid) {
                if recorded_ih != &ih {
                    return Err(OrchestratorError::DeterminismViolation {
                        node: node_id.clone(),
                        effect_id: eid,
                    });
                }
                self.materialize(output).await?
            } else {
                // Live turn: budget → NodeStarted (once) → gateway → EffectRecorded.
                if over_budget(min_win, &system, &messages, &tools) {
                    let est = est_prompt_tokens(&system, &messages, &tools);
                    let err = OrchestratorError::PromptOverBudget {
                        node: node_id.clone(),
                        turn,
                        est,
                        min_win: min_win.unwrap_or(0),
                    };
                    let message = err.to_string();
                    self.append(
                        run,
                        JournalEvent::NodeFailed {
                            node: node_id.clone(),
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
                            node: node_id.clone(),
                        },
                    )
                    .await?;
                    node_started = true;
                }
                let request = build_chat_request(&chain, &system, messages.clone(), tools.clone());
                match self
                    .dispatch_model_turn(run, node_id, eid, ih, request)
                    .await?
                {
                    Ok(output) => output,
                    Err(failure) => return Ok(AgentStep::Failed(failure)),
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
                if !fold.completed.contains(node_id) {
                    self.append(
                        run,
                        JournalEvent::NodeCompleted {
                            node: node_id.clone(),
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

            // Not a final answer → execute this turn's tool calls and extend the
            // transcript. A tool failure ends the node (already journaled).
            match self
                .run_agent_tools(run, node_id, turn, &turn_output, fold)
                .await?
            {
                Ok(turn_messages) => messages.extend(turn_messages),
                Err(failure) => return Ok(AgentStep::Failed(failure)),
            }
        }

        // Ran out of steps without a final answer.
        let err = OrchestratorError::AgentMaxStepsExceeded {
            node: node_id.clone(),
        };
        let message = err.to_string();
        self.append(
            run,
            JournalEvent::NodeFailed {
                node: node_id.clone(),
                error: message.clone(),
            },
        )
        .await?;
        Ok(AgentStep::Failed(message))
    }

    /// Execute (or replay) one ReAct turn's tool calls and return the transcript
    /// messages to append (the assistant turn + each tool result). Each Pure tool
    /// call is a Pure effect `effect_id(node_id, turn, k+1)`: a memo hit replays it
    /// (no re-execution), a hash mismatch is a `DeterminismViolation` (fatal outer
    /// `Err`). The inner `Result` is the turn's messages, or a tool's failure
    /// message (already journaled `NodeFailed`) — the same shape as a Map child.
    async fn run_agent_tools(
        &self,
        run: RunId,
        node_id: &NodeId,
        turn: usize,
        turn_output: &serde_json::Value,
        fold: &Fold,
    ) -> Result<Result<Vec<Message>, String>, OrchestratorError> {
        let assistant_text = turn_output
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let tool_calls: Vec<ToolCall> = serde_json::from_value(
            turn_output
                .get("tool_calls")
                .cloned()
                .unwrap_or(serde_json::json!([])),
        )?;
        let mut out = vec![Message {
            role: MessageRole::Assistant,
            content: MessageContent::Text {
                text: assistant_text,
            },
            tool_calls: tool_calls.clone(),
            attachments: Vec::new(),
        }];
        for (k, call) in tool_calls.iter().enumerate() {
            let teid = effect_id(&node_id.0, turn as u64, k + 1);
            let args: serde_json::Value =
                serde_json::from_str(&call.arguments).unwrap_or(serde_json::Value::Null);
            let tih = tool_input_hash(&call.name, &call.arguments);
            let result = if let Some((recorded_ih, output)) = fold.memo.get(&teid) {
                if recorded_ih != &tih {
                    return Err(OrchestratorError::DeterminismViolation {
                        node: node_id.clone(),
                        effect_id: teid,
                    });
                }
                self.materialize(output).await?
            } else {
                match self.tools.execute(&call.name, args) {
                    Ok(result) => {
                        let recorded = self.split_output(&result).await?;
                        self.append(
                            run,
                            JournalEvent::EffectRecorded {
                                node: node_id.clone(),
                                effect_id: teid,
                                class: EffectClass::Pure,
                                input_hash: tih,
                                seq: 0,
                                output: recorded,
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
                                node: node_id.clone(),
                                error: message.clone(),
                            },
                        )
                        .await?;
                        return Ok(Err(message));
                    }
                }
            };
            out.push(Message::tool_result(call.id.clone(), result.to_string()));
        }
        Ok(Ok(out))
    }

    /// Dispatch one live model turn through the gateway and journal its result:
    /// on success, record the `{model, text, tool_calls}` output as a Pure effect
    /// (`eid`) and return it; on a gateway error, journal `NodeFailed` and return
    /// the failure message. The outer `Err` is a fatal journal/CAS error.
    async fn dispatch_model_turn(
        &self,
        run: RunId,
        node_id: &NodeId,
        eid: EffectId,
        ih: String,
        request: InferenceRequest,
    ) -> Result<Result<serde_json::Value, String>, OrchestratorError> {
        match self.gateway.execute(&request).await {
            Ok(response) => {
                let output = serde_json::json!({
                    "model": response.model,
                    "text": response.content.clone().unwrap_or_default(),
                    "tool_calls": response.tool_calls,
                });
                let recorded = self.split_output(&output).await?;
                self.append(
                    run,
                    JournalEvent::EffectRecorded {
                        node: node_id.clone(),
                        effect_id: eid,
                        class: EffectClass::Pure,
                        input_hash: ih,
                        seq: 0,
                        output: recorded,
                    },
                )
                .await?;
                Ok(Ok(output))
            }
            Err(error) => {
                let message = error.to_string();
                self.append(
                    run,
                    JournalEvent::NodeFailed {
                        node: node_id.clone(),
                        error: message.clone(),
                    },
                )
                .await?;
                Ok(Err(message))
            }
        }
    }
}
