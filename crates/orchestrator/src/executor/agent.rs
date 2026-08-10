//! The agent runtime on the executor: the durable ReAct loop (`drive_agent`) and
//! its per-turn tool execution (`run_agent_tools`). Split out of `super` for
//! readability; both are `impl Executor` methods and share its private state.

use kernel::types::request::{
    InferenceRequest, Message, MessageContent, MessageRole, ToolCall, ToolDefinition,
};
use orchestrator_core::{
    AgentDefinition, AgentRef, EffectClass, EffectId, JournalEvent, NodeId, ObservationMeta,
    OrchestratorError, RunId, effect_id, idempotency_key,
};

use super::support::{
    agent_input_hash, build_chat_request, est_prompt_tokens, render_input, tool_input_hash,
};
use super::{AgentStep, Executor, Fold};
use crate::agent::prompt::{assemble_prompt, over_budget};

/// The invariant-across-turns context of one agent invocation — assembled once
/// (agent lookup, prompt, chain, window budget) so the per-turn helpers share it
/// instead of each threading the same six values through their signatures.
struct AgentRun<'a> {
    run: RunId,
    node_id: &'a NodeId,
    chain: String,
    system: String,
    tools: Vec<ToolDefinition>,
    min_win: Option<u32>,
    fold: &'a Fold,
}

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
        let ar = AgentRun {
            run,
            node_id,
            chain,
            system,
            tools,
            min_win,
            fold,
        };

        let mut messages: Vec<Message> =
            vec![Message::text(MessageRole::User, render_input(input))];
        let mut node_started = fold.started.contains(node_id);

        for turn in 0..self.max_steps {
            // Produce this turn's model output — memoized replay or a live call.
            let turn_output = match self
                .agent_turn_output(&ar, turn, &messages, &mut node_started)
                .await?
            {
                Ok(output) => output,
                Err(failure) => return Ok(AgentStep::Failed(failure)),
            };

            let tool_calls: Vec<ToolCall> = serde_json::from_value(
                turn_output
                    .get("tool_calls")
                    .cloned()
                    .unwrap_or(serde_json::json!([])),
            )?;
            if tool_calls.is_empty() {
                return self.finish_agent(&ar, &turn_output).await;
            }

            // Not a final answer → execute this turn's tool calls and extend the
            // transcript. A tool failure ends the node (already journaled).
            match self.run_agent_tools(&ar, turn, &turn_output).await? {
                Ok(turn_messages) => messages.extend(turn_messages),
                Err(failure) => return Ok(AgentStep::Failed(failure)),
            }
        }

        // Ran out of steps without a final answer.
        let message = OrchestratorError::AgentMaxStepsExceeded {
            node: node_id.clone(),
        }
        .to_string();
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

    /// Produce one ReAct turn's model output: a memoized turn replays from the
    /// journal (no gateway call; a hash mismatch is a fatal `DeterminismViolation`);
    /// otherwise a live turn runs the budget gate → `NodeStarted` (once, tracked in
    /// `node_started`) → gateway → `EffectRecorded`. The inner `Result` is the
    /// turn's output value, or a node-level failure message (over-budget or a
    /// gateway error, already journaled `NodeFailed`).
    async fn agent_turn_output(
        &self,
        ar: &AgentRun<'_>,
        turn: usize,
        messages: &[Message],
        node_started: &mut bool,
    ) -> Result<Result<serde_json::Value, String>, OrchestratorError> {
        let eid = effect_id(&ar.node_id.0, turn as u64, 0);
        let ih = agent_input_hash(&ar.chain, &ar.system, messages, &ar.tools)?;

        if let Some((recorded_ih, output)) = ar.fold.memo.get(&eid) {
            if recorded_ih != &ih {
                return Err(OrchestratorError::DeterminismViolation {
                    node: ar.node_id.clone(),
                    effect_id: eid,
                });
            }
            return Ok(Ok(self.materialize(output).await?));
        }

        // Live turn. Budget-gate before spending; halt loud if over.
        if over_budget(ar.min_win, &ar.system, messages, &ar.tools) {
            let message = OrchestratorError::PromptOverBudget {
                node: ar.node_id.clone(),
                turn,
                est: est_prompt_tokens(&ar.system, messages, &ar.tools),
                min_win: ar.min_win.unwrap_or(0),
            }
            .to_string();
            self.append(
                ar.run,
                JournalEvent::NodeFailed {
                    node: ar.node_id.clone(),
                    error: message.clone(),
                },
            )
            .await?;
            return Ok(Err(message));
        }
        if !*node_started {
            self.append(
                ar.run,
                JournalEvent::NodeStarted {
                    node: ar.node_id.clone(),
                },
            )
            .await?;
            *node_started = true;
        }
        let request =
            build_chat_request(&ar.chain, &ar.system, messages.to_vec(), ar.tools.clone());
        self.dispatch_model_turn(ar.run, ar.node_id, eid, ih, request)
            .await
    }

    /// Finalize a completed agent node: journal `NodeCompleted` once (guarded on
    /// resume) and return the canonical `{model, text}` output.
    async fn finish_agent(
        &self,
        ar: &AgentRun<'_>,
        turn_output: &serde_json::Value,
    ) -> Result<AgentStep, OrchestratorError> {
        if !ar.fold.completed.contains(ar.node_id) {
            self.append(
                ar.run,
                JournalEvent::NodeCompleted {
                    node: ar.node_id.clone(),
                },
            )
            .await?;
        }
        let text = turn_output.get("text").cloned().unwrap_or_default();
        let model = turn_output
            .get("model")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        Ok(AgentStep::Completed(
            serde_json::json!({ "model": model, "text": text }),
        ))
    }

    /// Execute (or replay) one ReAct turn's tool calls and return the transcript
    /// messages to append (the assistant turn + each tool result). Each Pure tool
    /// call is a Pure effect `effect_id(node_id, turn, k+1)`: a memo hit replays it
    /// (no re-execution), a hash mismatch is a `DeterminismViolation` (fatal outer
    /// `Err`). The inner `Result` is the turn's messages, or a tool's failure
    /// message (already journaled `NodeFailed`) — the same shape as a Map child.
    async fn run_agent_tools(
        &self,
        ar: &AgentRun<'_>,
        turn: usize,
        turn_output: &serde_json::Value,
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
            let teid = effect_id(&ar.node_id.0, turn as u64, k + 1);
            let result = match self.execute_tool_effect(ar, &teid, call).await? {
                Ok(value) => value,
                Err(failure) => return Ok(Err(failure)),
            };
            out.push(Message::tool_result(call.id.clone(), result.to_string()));
        }
        Ok(Ok(out))
    }

    /// Execute (or replay) ONE tool call as a durable effect, dispatched by its
    /// [`EffectClass`] (§7.1). The outer `Result` is a fatal journal/CAS/
    /// determinism error; the inner `Result` is the tool's output value, or its
    /// failure message (already journaled `NodeFailed`, same shape as a turn).
    ///
    /// - **Pure** — a memo hit replays (never re-executes); a miss executes and
    ///   records `observation: None`.
    /// - **Observation** — a memo hit replays only while FRESH (`fetched_at + ttl`
    ///   has not lapsed per the injected `Clock`); a stale hit (or a miss) re-reads
    ///   and appends a superseding record with fresh [`ObservationMeta`].
    /// - **Mutation** — a memo hit (Intent+Recorded) replays; a miss executes and
    ///   records `class: Mutation`. The two-phase Intent and in-doubt reconcile
    ///   land in slice-4 Tasks 8–9.
    async fn execute_tool_effect(
        &self,
        ar: &AgentRun<'_>,
        teid: &EffectId,
        call: &ToolCall,
    ) -> Result<Result<serde_json::Value, String>, OrchestratorError> {
        let args: serde_json::Value =
            serde_json::from_str(&call.arguments).unwrap_or(serde_json::Value::Null);
        let tih = tool_input_hash(&call.name, &call.arguments);
        let spec = self.tools.spec_of(&call.name);
        let class = spec
            .as_ref()
            .map(|s| s.effect_class)
            .unwrap_or(EffectClass::Pure);

        // Memo hit: replay, unless it is a STALE Observation (fall through to a
        // live re-read). The determinism fence guards every replay.
        if let Some((recorded_ih, output)) = ar.fold.memo.get(teid) {
            if recorded_ih != &tih {
                return Err(OrchestratorError::DeterminismViolation {
                    node: ar.node_id.clone(),
                    effect_id: teid.clone(),
                });
            }
            let stale = class == EffectClass::Observation && !self.observation_fresh(ar, teid);
            if !stale {
                return Ok(Ok(self.materialize(output).await?));
            }
        }

        // Live path. A Mutation is two-phase (Intent → side effect → Recorded,
        // §7.3); Pure/Observation record directly (Observations carry
        // freshness/provenance so a later resume can decide replay-vs-re-read).
        match class {
            EffectClass::Mutation => self.mutation_tool_effect(ar, teid, call, args, &tih).await,
            _ => {
                let observation = (class == EffectClass::Observation).then(|| ObservationMeta {
                    fetched_at: self.clock.now(),
                    ttl_secs: spec.as_ref().and_then(|s| s.ttl_secs).unwrap_or(0),
                    source: spec
                        .as_ref()
                        .and_then(|s| s.source.clone())
                        .unwrap_or_else(|| call.name.clone()),
                });
                self.record_tool_effect(ar, teid, call, args, &tih, (class, observation))
                    .await
            }
        }
    }

    /// The live path for a Mutation effect (§7.3): journal an `EffectIntent`
    /// (idempotency key + args hash) BEFORE the side effect, then execute and
    /// record. On a crash between the two, resume finds the Intent without a
    /// Recorded — the in-doubt case reconciled in slice-4 Task 9.
    async fn mutation_tool_effect(
        &self,
        ar: &AgentRun<'_>,
        teid: &EffectId,
        call: &ToolCall,
        args: serde_json::Value,
        tih: &str,
    ) -> Result<Result<serde_json::Value, String>, OrchestratorError> {
        self.append(
            ar.run,
            JournalEvent::EffectIntent {
                node: ar.node_id.clone(),
                effect_id: teid.clone(),
                idempotency_key: idempotency_key(teid, tih),
                args_hash: tih.to_string(),
                seq: 0,
            },
        )
        .await?;
        self.record_tool_effect(ar, teid, call, args, tih, (EffectClass::Mutation, None))
            .await
    }

    /// Whether a memoized `Observation` is still fresh: its recorded
    /// `fetched_at + ttl_secs` has not lapsed per the injected `Clock`. A missing
    /// provenance record or `ttl_secs == 0` (a `None` TTL) is never fresh — always
    /// re-read (§7.1).
    fn observation_fresh(&self, ar: &AgentRun<'_>, teid: &EffectId) -> bool {
        match ar.fold.observations.get(teid) {
            Some(meta) => {
                meta.ttl_secs > 0
                    && self.clock.now()
                        <= meta.fetched_at + chrono::Duration::seconds(meta.ttl_secs as i64)
            }
            None => false,
        }
    }

    /// Execute a tool live and journal its `EffectRecorded` (shared by all effect
    /// classes). On a tool error, journal `NodeFailed` and return the failure
    /// message. `observation` is `Some` only for Observation effects.
    async fn record_tool_effect(
        &self,
        ar: &AgentRun<'_>,
        teid: &EffectId,
        call: &ToolCall,
        args: serde_json::Value,
        tih: &str,
        record: (EffectClass, Option<ObservationMeta>),
    ) -> Result<Result<serde_json::Value, String>, OrchestratorError> {
        let (class, observation) = record;
        match self.tools.execute(&call.name, args) {
            Ok(result) => {
                let recorded = self.split_output(&result).await?;
                self.append(
                    ar.run,
                    JournalEvent::EffectRecorded {
                        node: ar.node_id.clone(),
                        effect_id: teid.clone(),
                        class,
                        input_hash: tih.to_string(),
                        seq: 0,
                        output: recorded,
                        observation,
                    },
                )
                .await?;
                Ok(Ok(result))
            }
            Err(err) => {
                let message = err.to_string();
                self.append(
                    ar.run,
                    JournalEvent::NodeFailed {
                        node: ar.node_id.clone(),
                        error: message.clone(),
                    },
                )
                .await?;
                Ok(Err(message))
            }
        }
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
                        observation: None,
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
