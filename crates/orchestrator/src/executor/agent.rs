//! The agent runtime on the executor: the durable ReAct loop (`drive_agent`) and
//! its per-turn tool execution (`run_agent_tools`). Split out of `super` for
//! readability; both are `impl Executor` methods and share its private state.

use kernel::types::request::{
    InferenceRequest, Message, MessageContent, MessageRole, ToolCall, ToolDefinition,
};
use orchestrator_core::{
    AgentDefinition, AgentRef, ContextKey, EffectClass, EffectId, JournalEvent, NodeId,
    ObservationMeta, OrchestratorError, ReconcileOutcome, RunId, effect_id, idempotency_key,
};

use super::support::{
    GatewayDisposition, agent_input_hash, build_chat_request, classify_gateway_error,
    est_prompt_tokens, render_input, tool_input_hash,
};
use super::{AgentStep, Executor, Fold};
use crate::agent::prompt::{assemble_prompt, over_budget};

/// The 3-way outcome of one tool effect (or one ReAct turn's tools, §7.3): a
/// value/transcript, a node failure (already journaled `NodeFailed`), or a durable
/// pause (`RunPaused` journaled) — an in-doubt Mutation awaiting resolution.
enum ToolOutcome<T> {
    Ok(T),
    Failed(String),
    Paused(String),
}

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
    // The acting agent's authorization surface (owned clones, built once per
    // agent-node-run) — the SP-4 s1 gate in `execute_tool_effect` checks each tool
    // call against these: the tool must be LISTED and its grant must COVER the need.
    agent_tools: Vec<String>,
    agent_grants: std::collections::HashMap<String, orchestrator_core::Permissions>,
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
    ///
    /// `top_level` says whether this call is a `NodeKind::Agent` in the RUN's OWN graph, as
    /// opposed to a `Map`/`Consolidate`/`Loop` body, a Loop gate-agent, an `Expand` planner,
    /// or an `Agent` node nested inside a `Subgraph`/`Branch` arm/`Expand`. It exists ONLY
    /// for SP-6 s3's human-backed roles, which are legal at that one position and nowhere
    /// else; see the branch below.
    ///
    /// Only `run_node` can pass `true`, and it passes `!nested` — `drive`'s position flag,
    /// cleared by `drive_nested` — never a literal. Deciding this on the CALLER instead was
    /// a real, shipped bypass; the branch below records what it cost.
    // The per-invocation inputs (run/node/agent/input/context/fold/phase/top_level) are
    // each distinct and passed positionally at six call sites; bundling them behind a
    // struct would only relocate the plumbing, so the arity is allowed here.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn drive_agent(
        &self,
        run: RunId,
        node_id: &NodeId,
        agent_ref: &AgentRef,
        input: &serde_json::Value,
        context: &[(ContextKey, serde_json::Value)],
        fold: &Fold,
        phase: Option<&str>,
        top_level: bool,
    ) -> Result<AgentStep, OrchestratorError> {
        let agent: &AgentDefinition = self
            .registry
            .agent(&agent_ref.0)
            .ok_or_else(|| OrchestratorError::UnknownAgent(agent_ref.0.clone()))?;
        let query = render_input(input);
        let (system, tools) = assemble_prompt(&self.registry, agent, context, &query)?;

        // SP-6 s3: a human-backed role answers instead of a model. The branch sits HERE —
        // AFTER `assemble_prompt`, which needs no chain, so the human's question reuses the
        // model path's own prompt assembly rather than a second implementation that could
        // drift from it; and BEFORE `resolve_chain`, so no chain is resolved, no gateway is
        // touched and zero token spend is STRUCTURAL rather than measured. (`assemble_prompt`
        // is not the WHOLE question — see the composition below, which adds the node input.)
        //
        // `on_agent_started` does NOT fire on this path, deliberately: the hook's
        // signature requires `&ar.chain`, and a human-backed agent by construction never
        // has one. `resolve_chain` would in fact FAIL for the common shape of such a role
        // (`chain: None` with no `(area, kind)` binding), which is why the branch cannot
        // simply sit lower down and skip the call.
        if let orchestrator_core::AgentBacking::Human { timeout } = agent.backed_by {
            // `AgentStep` has no `From<NodeExec>` conversion, so the mapping is written
            // inline — the same three-arm mapping `run_node` applies to this function's
            // result, in the other direction. `NodeExec::Failed.output` is `None` on every
            // path `run_human_agent`/`fail_human_agent` can return, so nothing is dropped.
            let step = |exec: super::NodeExec| match exec {
                super::NodeExec::Completed(output) => AgentStep::Completed(output),
                super::NodeExec::Failed { message, .. } => AgentStep::Failed(message),
                super::NodeExec::Paused { reason } => AgentStep::Paused(reason),
            };

            if !top_level {
                // Design §5.5: legal ONLY as a top-level `NodeKind::Agent`. `drive_agent`
                // is the shared choke point for six callers, and the five non-top-level
                // ones each mean a DIFFERENT unbuilt feature: N concurrent human asks for a
                // `Map`, the same for a `Consolidate`, a human re-answering every `Loop`
                // iteration, a human deciding loop continuation, and — the sharpest — a
                // human-backed PLANNER, whose answer feeds `parse_plan(text)`, so the
                // person would have to hand-author a machine-parseable plan GRAPH.
                //
                // **`top_level` is a POSITION, not a caller.** The sixth caller,
                // `run_node`'s own `NodeKind::Agent` arm, passes `!nested` — the flag
                // `drive` carries and `drive_nested` clears — rather than a literal `true`.
                // It used to pass the literal, and the whole-slice review showed what that
                // cost: `run_loop` → `drive_nested` → `drive` → `run_node`, so wrapping the
                // very same agent in a one-node `Subgraph` body declared it top-level and
                // delivered "a human re-answers every `Loop` iteration" — one of the four
                // features listed above as unbuilt — with no refusal at all. The same
                // wrapper trick worked through a `Subgraph` node, a `Branch` arm and an
                // `Expand` planner's spliced graph, which is the one an UNTRUSTED planner
                // can author. `non_top_level_sites` now carries a row for each.
                //
                // Enforced HERE, at runtime, rather than in `Graph::validate_dag`, because
                // `validate_dag` cannot see the registry: a graph names an `AgentRef`, and
                // whether that ref is human-backed is a REGISTRY fact. That is a stated
                // limitation of §5.5, not an oversight — which is why the refusal is loud
                // and journaled rather than a silent fallback to the model path.
                //
                // The precheck FIRST, for the same reason `run_human_agent`'s step 0 does
                // it: this refusal is TERMINAL for the node, but the run it kills journals
                // no `RunCompleted`, so `start_inner` never takes its terminal branch and
                // every later wake re-drives the Map child / Loop iteration / planner. A
                // re-derived refusal appends a fresh `NodeFailed` for an already-dead node
                // on each of them — an unboundedly growing journal for a run that cannot
                // make progress. Read the verdict back instead. (Review found this arm
                // doing exactly that: three drives, three rows.)
                if let Some(failed) = self.gate_precheck_by_id(node_id, fold) {
                    return Ok(step(failed));
                }
                // Through `fail_human_agent` rather than a local `append`, so this arm
                // cannot skip the redaction chokepoint every other human-path failure goes
                // through — the `model_output` precedent, and the specific defect s2 shipped
                // when one gate failure arm scrubbed and another did not.
                return Ok(step(
                    self.fail_human_agent(
                        run,
                        node_id,
                        format!(
                            "human_agent: agent {:?} is human-backed and may only be used \
                             as a top-level Agent node — not as a Map or Consolidate body, \
                             a Loop body, a Loop gate, a planner, or an Agent node nested \
                             inside a Subgraph, a Branch arm or an Expand",
                            agent_ref.0
                        ),
                    )
                    .await?,
                ));
            }

            // The QUESTION is the model-EQUIVALENT one, which is `assemble_prompt`'s output
            // plus the node's input — not `assemble_prompt`'s output alone.
            //
            // `assemble_prompt` returns `(system, tools)`: `system_prompt` + activated skill
            // bodies + the rendered `## Context` section. It takes `query` only to evaluate
            // each skill's/tool's `activation.is_active(query)` and never puts it in the
            // string it returns. The model path supplies the input SEPARATELY, as the first
            // user message (`Message::text(MessageRole::User, query)`, below). So journaling
            // `system` alone showed the human the role's standing instructions and the
            // upstream context but NOT the thing being asked about — a reviewer role reading
            // "say whether the contract permits sub-processing" with no contract named.
            // Design §5.4's rule is "the human sees precisely what the model would have",
            // and its accepted cost is explicitly one-directional: never show the human
            // LESS than the model would have had.
            //
            // `## Task` mirrors `assemble_prompt`'s own `## Context` heading so the two
            // sections read as one document. The composed string — not `system` — is what
            // `run_human_agent` bounds against `MAX_HUMAN_TEXT_BYTES`, so the durable row is
            // bounded as a whole rather than in the part.
            let question = format!("{system}\n\n## Task\n{query}");
            return Ok(step(
                self.run_human_agent(run, node_id, &question, timeout, fold)
                    .await?,
            ));
        }

        let chain = self.registry.resolve_chain(agent, phase)?.to_string();
        let min_win = self.gateway.min_context_window(&chain).await;
        let ar = AgentRun {
            run,
            node_id,
            chain,
            system,
            tools,
            min_win,
            fold,
            agent_tools: agent.tools.clone(),
            agent_grants: agent.grants.clone(),
        };

        let mut messages: Vec<Message> = vec![Message::text(MessageRole::User, query)];
        let mut node_started = fold.started.contains(node_id);

        // Best-effort hook (§15): fire `on_agent_started` once, gated on the same
        // `!node_started` condition that guards the `NodeStarted` append — so a
        // resume replay of an already-started agent node does not re-fire it.
        if !node_started && let Some(h) = &self.hooks {
            h.on_agent_started(run, node_id, &agent_ref.0, &ar.chain)
                .await;
        }

        for turn in 0..self.max_steps {
            // Produce this turn's model output — memoized replay or a live call.
            let turn_output = match self
                .agent_turn_output(&ar, turn, &messages, &mut node_started)
                .await?
            {
                ToolOutcome::Ok(output) => output,
                ToolOutcome::Failed(failure) => return Ok(AgentStep::Failed(failure)),
                ToolOutcome::Paused(reason) => return Ok(AgentStep::Paused(reason)),
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
                ToolOutcome::Ok(turn_messages) => messages.extend(turn_messages),
                ToolOutcome::Failed(failure) => return Ok(AgentStep::Failed(failure)),
                ToolOutcome::Paused(reason) => return Ok(AgentStep::Paused(reason)),
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
    ) -> Result<ToolOutcome<serde_json::Value>, OrchestratorError> {
        let eid = effect_id(&ar.node_id.0, turn as u64, 0);
        let ih = agent_input_hash(&ar.chain, &ar.system, messages, &ar.tools)?;

        if let Some((recorded_ih, output)) = ar.fold.memo.get(&eid) {
            if recorded_ih != &ih {
                return Err(OrchestratorError::DeterminismViolation {
                    node: ar.node_id.clone(),
                    effect_id: eid,
                });
            }
            return Ok(ToolOutcome::Ok(self.materialize(output).await?));
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
            return Ok(ToolOutcome::Failed(message));
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
        // Best-effort hook (§15): a LIVE turn about to dispatch (a memoized replay
        // returned above, and an over-budget turn already halted) — so a resume
        // never re-fires a replayed turn.
        if let Some(h) = &self.hooks {
            h.on_agent_turn(ar.run, ar.node_id, turn).await;
        }
        let request =
            build_chat_request(&ar.chain, &ar.system, messages.to_vec(), ar.tools.clone());
        // SP-DATA-5: the token ledger rides as the chokepoint's own `Meter` view rather
        // than the (private, `mod.rs`-owned) `Fold`, so `dispatch_model_turn` stays
        // independent of the fold's shape — and matches the chokepoint's own signature.
        // It must be the LIVE view and not a pair of scalars: a ReAct node dispatches
        // once PER TURN inside a single drive, so a frozen `spent` would let an agent
        // burn all `max_steps` turns against the ledger as it stood before turn 0.
        self.dispatch_model_turn(ar.run, ar.node_id, eid, ih, request, &ar.fold.meter())
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
    ) -> Result<ToolOutcome<Vec<Message>>, OrchestratorError> {
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
            let value = match self.execute_tool_effect(ar, &teid, call).await? {
                ToolOutcome::Ok(value) => value,
                ToolOutcome::Failed(failure) => return Ok(ToolOutcome::Failed(failure)),
                ToolOutcome::Paused(reason) => return Ok(ToolOutcome::Paused(reason)),
            };
            out.push(Message::tool_result(call.id.clone(), value.to_string()));
        }
        Ok(ToolOutcome::Ok(out))
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
    ) -> Result<ToolOutcome<serde_json::Value>, OrchestratorError> {
        // Unparseable `arguments` degrade to `Null` (a deliberate, currently-safe
        // posture): the gate then derives an EMPTY `need` (so it passes), but every
        // tool's `call` fail-closes on missing/invalid args and a non-overriding tool
        // falls back to its static surface — so no unauthorized side effect escapes. A
        // future strict mode could instead deny outright on unparseable args.
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
                return Ok(ToolOutcome::Ok(self.materialize(output).await?));
            }
        }

        // SP-4 s1 authorization gate: the acting agent must LIST this tool AND hold a
        // grant covering the concrete permissions THIS call needs. Denials are fed
        // back to the agent (recorded as a Pure effect ⇒ replayed on resume). Runs on
        // the LIVE path only — a memo hit above already replayed a recorded allow/deny.
        //
        // Fail asymmetry: an *unauthorized* call is a SOFT, recoverable denial fed back
        // to the agent (policy violation — the agent can adapt), whereas an *unknown*
        // tool (listed+granted but absent from the ToolRegistry) HARD-fails the node via
        // `execute_ctx` → `UnknownTool` (a misconfiguration — loud, not recoverable).
        let need = self.tools.required_of(&call.name, &args);
        let no_grant = orchestrator_core::Permissions::default();
        let grant = ar.agent_grants.get(&call.name).unwrap_or(&no_grant);
        let listed = ar.agent_tools.iter().any(|t| t == &call.name);
        if !(listed && grant.covers(&need)) {
            // Model-facing reason stays TERSE: NEVER enumerate the grant (the
            // allowlist) into the transcript/journal — the denied party IS the model,
            // and handing it the full allowlist invites a redirect to another granted
            // resource (confused-deputy / injection surface). Operators get the full
            // need/grant via the debug log.
            let detail = if !listed {
                format!("tool '{}' is not available to this agent", call.name)
            } else {
                format!(
                    "the requested access for tool '{}' is not permitted by its grant",
                    call.name
                )
            };
            tracing::debug!(tool = %call.name, ?need, ?grant, listed, "tool permission denied");
            return self
                .record_denied_effect(ar, teid, call, &tih, detail)
                .await;
        }

        // SP-4 s3 workspace confinement: when a per-run jail is wired, every concrete path
        // THIS (s1-authorized) call declares must resolve WITHIN it. A declared escape is a
        // terse denial — recorded Pure (like the s1 denial), no side effect, replayed on
        // resume. The jail BINDS the abstract grant to a live per-run dir + canonicalizes
        // real paths (per-run isolation + symlink defense — what s1's lexical `covers`
        // cannot do). In-process ambient authority means this confines the DECLARED surface;
        // a tool bypassing the shared helper is the (deferred) subprocess sandbox's job.
        if !need.paths.is_empty()
            && let Some(root) = self.workspace_root_for(ar.run)?
        {
            for p in &need.paths {
                if crate::agent::workspace::confine(&root, p).is_err() {
                    tracing::debug!(tool = %call.name, path = %p, "workspace jail escape denied");
                    let detail = format!(
                        "the requested path for tool '{}' is outside its workspace",
                        call.name
                    );
                    return self
                        .record_denied_effect(ar, teid, call, &tih, detail)
                        .await;
                }
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
                self.record_tool_effect(
                    ar,
                    teid,
                    call,
                    args,
                    &tih,
                    &idempotency_key(teid, &tih),
                    (class, observation),
                )
                .await
            }
        }
    }

    /// The live path for a Mutation effect (§7.3). A resume that finds a standing
    /// `EffectIntent` with no `EffectRecorded` (`teid ∈ fold.intents`) is IN-DOUBT
    /// and reconciles — never blind re-runs. Otherwise (never ran) it is two-phase:
    /// journal an `EffectIntent` BEFORE the side effect, then execute and record.
    async fn mutation_tool_effect(
        &self,
        ar: &AgentRun<'_>,
        teid: &EffectId,
        call: &ToolCall,
        args: serde_json::Value,
        tih: &str,
    ) -> Result<ToolOutcome<serde_json::Value>, OrchestratorError> {
        if ar.fold.intents.contains_key(teid) {
            return self.reconcile_in_doubt(ar, teid, call, args, tih).await;
        }
        // The effective idempotency key: the tool's author key (a domain ref) if it
        // overrides `Tool::idempotency_key`, else the structural
        // `sha256(effect_id | args_hash)`. Journaled in the Intent AND threaded to the
        // tool via `call_ctx` (so the tool sends the SAME key to its external API); on
        // an in-doubt resume `reconcile_in_doubt` READS this journaled key back.
        let key = self
            .tools
            .idempotency_key_of(&call.name, &args)
            .unwrap_or_else(|| idempotency_key(teid, tih));
        self.append(
            ar.run,
            JournalEvent::EffectIntent {
                node: ar.node_id.clone(),
                effect_id: teid.clone(),
                idempotency_key: key.clone(),
                args_hash: tih.to_string(),
                seq: 0,
            },
        )
        .await?;
        self.record_tool_effect(
            ar,
            teid,
            call,
            args,
            tih,
            &key,
            (EffectClass::Mutation, None),
        )
        .await
    }

    /// Reconcile an in-doubt Mutation on resume (§7.3): ask the per-tool
    /// [`ReconcileProvider`] (absent ⇒ `Indeterminate`) whether the side effect
    /// already applied, and never guess.
    /// - `Confirmed(output)` → record it from the provider's output; do NOT re-run.
    /// - `NotApplied` → run the effect now (the standing Intent already covers it,
    ///   so no second Intent is journaled).
    /// - `Indeterminate` → journal `RunPaused` and pause loud.
    async fn reconcile_in_doubt(
        &self,
        ar: &AgentRun<'_>,
        teid: &EffectId,
        call: &ToolCall,
        args: serde_json::Value,
        tih: &str,
    ) -> Result<ToolOutcome<serde_json::Value>, OrchestratorError> {
        // SP-4 s5: READ the effective key the live run journaled in the Intent (an
        // author override or the structural key) rather than recompute it, so the
        // reconciler queries the provider under the SAME key the tool sent to its
        // external API. The fallback is unreachable in practice (this arm runs only
        // when `teid ∈ fold.intents`); it keeps the recompute as a defensive default.
        let key = ar
            .fold
            .intents
            .get(teid)
            .cloned()
            .unwrap_or_else(|| idempotency_key(teid, tih));
        let verdict = match self.reconcilers.get(&call.name) {
            Some(provider) => provider.reconcile(&key, &args).await?,
            None => ReconcileOutcome::Indeterminate,
        };
        match verdict {
            ReconcileOutcome::Confirmed(output) => {
                // SP-4 s2: scrub the reconciler's output BEFORE journal + return (mirrors
                // `record_tool_effect`). This is a recorded side-effect output — the same
                // secret-bearing class as a live tool result — and run 1 recorded no
                // `EffectRecorded` (in-doubt), so there is no memo to fence; redacting once
                // keeps the journaled `split_output` and the returned value identical.
                let output = self.redact(&output);
                let recorded = self.split_output(&output).await?;
                self.append(
                    ar.run,
                    JournalEvent::EffectRecorded {
                        node: ar.node_id.clone(),
                        effect_id: teid.clone(),
                        class: EffectClass::Mutation,
                        input_hash: tih.to_string(),
                        seq: 0,
                        output: recorded,
                        observation: None,
                        // SP-DATA-5: stays None — `output` comes from a reconciler
                        // querying the provider's SIDE (`ReconcileOutcome::Confirmed`),
                        // never from an `InferenceResponse`, so there is no usage to
                        // thread here.
                        usage: None,
                    },
                )
                .await?;
                Ok(ToolOutcome::Ok(output))
            }
            ReconcileOutcome::NotApplied => {
                self.record_tool_effect(
                    ar,
                    teid,
                    call,
                    args,
                    tih,
                    &key,
                    (EffectClass::Mutation, None),
                )
                .await
            }
            ReconcileOutcome::Indeterminate => {
                let reason = format!("mutation in-doubt: {key}");
                self.append(
                    ar.run,
                    JournalEvent::RunPaused {
                        reason: reason.clone(),
                        resume_after: None,
                    },
                )
                .await?;
                Ok(ToolOutcome::Paused(reason))
            }
        }
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

    /// Record a permission denial as the call's effect output — a Pure, memoize-
    /// forever `EffectRecorded` with NO tool execution (and, for a Mutation, NO
    /// `EffectIntent`) — and feed it back to the agent. The decision is a pure fn of
    /// (config grant, call args) ⇒ a resume replays it from the memo, tool never run.
    async fn record_denied_effect(
        &self,
        ar: &AgentRun<'_>,
        teid: &EffectId,
        call: &ToolCall,
        tih: &str,
        detail: String,
    ) -> Result<ToolOutcome<serde_json::Value>, OrchestratorError> {
        let denial = serde_json::json!({
            "error": "permission_denied",
            "tool": call.name,
            "detail": detail,
        });
        let recorded = self.split_output(&denial).await?;
        self.append(
            ar.run,
            JournalEvent::EffectRecorded {
                node: ar.node_id.clone(),
                effect_id: teid.clone(),
                class: EffectClass::Pure,
                input_hash: tih.to_string(),
                seq: 0,
                output: recorded,
                observation: None,
                // SP-DATA-5: stays None — a denial never dispatches, so there is no
                // provider response to report usage from.
                usage: None,
            },
        )
        .await?;
        Ok(ToolOutcome::Ok(denial))
    }

    /// Resolve (and lazily create) the CANONICAL per-run workspace root, or `None` if no
    /// base is wired. `create_dir_all` is idempotent (safe on resume); `canonicalize`
    /// resolves symlinks in the base (e.g. macOS `/var`→`/private/var`) so the jail compares
    /// canonical-to-canonical. Called on the LIVE path only (a memo hit replays without it).
    pub(super) fn workspace_root_for(
        &self,
        run: RunId,
    ) -> Result<Option<std::sync::Arc<std::path::PathBuf>>, OrchestratorError> {
        let Some(base) = &self.workspace_root_base else {
            return Ok(None);
        };
        let dir = base.join(run.0.to_string());
        std::fs::create_dir_all(&dir).map_err(|e| OrchestratorError::Tool {
            tool: "workspace".into(),
            message: format!("create workspace {}: {e}", dir.display()),
        })?;
        let canon = dir.canonicalize().map_err(|e| OrchestratorError::Tool {
            tool: "workspace".into(),
            message: format!("canonicalize workspace {}: {e}", dir.display()),
        })?;
        Ok(Some(std::sync::Arc::new(canon)))
    }

    /// SP-4 s4: build a per-call `BoundSandbox` for `tool`, or `None` (⇒ the `shell` tool
    /// refuses loud). Requires ALL THREE: a wired `Sandbox`, a resolved per-run workspace, AND a
    /// grant for the tool — the grant's caps/network are the enforced policy (the tool supplies
    /// only argv, so it can NEVER widen them). Any one missing ⇒ `None` ⇒ fail-closed.
    fn bound_sandbox_for(
        &self,
        ar: &AgentRun<'_>,
        tool: &str,
        workspace: &Option<std::sync::Arc<std::path::PathBuf>>,
    ) -> Option<std::sync::Arc<crate::agent::sandbox::BoundSandbox>> {
        let inner = self.sandbox.clone()?;
        let ws = workspace.clone()?;
        let grant = ar.agent_grants.get(tool)?;
        Some(std::sync::Arc::new(
            crate::agent::sandbox::BoundSandbox::new(
                inner,
                ws,
                grant.caps.clone(),
                grant.network.clone(),
            ),
        ))
    }

    /// Execute a tool live and journal its `EffectRecorded` (shared by all effect
    /// classes). On a tool error, journal `NodeFailed` and return the failure
    /// message. `observation` is `Some` only for Observation effects.
    // The effect coordinates (id/call/args/hashes/key/class) are each distinct and
    // passed positionally by the three call sites; bundling them behind a struct would
    // only relocate the plumbing, so the arity is allowed here.
    #[allow(clippy::too_many_arguments)]
    async fn record_tool_effect(
        &self,
        ar: &AgentRun<'_>,
        teid: &EffectId,
        call: &ToolCall,
        args: serde_json::Value,
        tih: &str,
        idempotency_key: &str,
        record: (EffectClass, Option<ObservationMeta>),
    ) -> Result<ToolOutcome<serde_json::Value>, OrchestratorError> {
        let (class, observation) = record;
        // Best-effort hook (§15): a LIVE tool execution (a memoized tool replays via
        // `materialize` and never reaches here), so a resume never re-fires it.
        if let Some(h) = &self.hooks {
            h.on_agent_tool_call(ar.run, ar.node_id, &call.name).await;
        }
        // SP-4 broker: resolve the tool's DECLARED credential refs + inject into the ctx
        // (ephemeral — never journaled/hashed). A declared ref that cannot be resolved (no
        // broker, or the broker returns None) fails loud — never a silent missing credential.
        let mut resolved = std::collections::HashMap::new();
        if let Some(spec) = self.tools.spec_of(&call.name) {
            for cred_ref in &spec.credentials {
                let secret = match &self.credential_broker {
                    // A broker that ERRORS (an infra failure, not a miss) fails LOUD as a node
                    // failure — journal `NodeFailed` + return `Failed`, mirroring the `None`
                    // arm below (journal-everything parity; never a raw run-level `Err`).
                    Some(broker) => match broker.resolve(cred_ref).await {
                        Ok(s) => s,
                        Err(e) => {
                            let msg = format!(
                                "tool '{}' requires credential '{}' but the broker errored: {}",
                                call.name, cred_ref, e
                            );
                            self.append(
                                ar.run,
                                JournalEvent::NodeFailed {
                                    node: ar.node_id.clone(),
                                    error: msg.clone(),
                                },
                            )
                            .await?;
                            return Ok(ToolOutcome::Failed(msg));
                        }
                    },
                    None => None,
                };
                match secret {
                    Some(s) => {
                        resolved.insert(cred_ref.clone(), s);
                    }
                    None => {
                        let msg = format!(
                            "tool '{}' requires credential '{}' but no broker resolved it",
                            call.name, cred_ref
                        );
                        self.append(
                            ar.run,
                            JournalEvent::NodeFailed {
                                node: ar.node_id.clone(),
                                error: msg.clone(),
                            },
                        )
                        .await?;
                        return Ok(ToolOutcome::Failed(msg));
                    }
                }
            }
        }
        // SP-4 s5: thread the effective idempotency key into the tool via `call_ctx`
        // (journaled in the Intent for a Mutation; the structural key for Pure/
        // Observation, which ignore it) so a real tool can send the SAME key to its
        // external API for provider-side dedup. SP-4 broker: the resolved credentials
        // ride alongside (ephemeral — never journaled/hashed, zeroized on drop).
        // SP-4 s3: resolve the canonical per-run workspace root ONCE (a fs tool resolves its
        // target within it via `confine`; `None` when no jail is wired ⇒ byte-identical) and
        // reuse it below to also derive the s4 sandbox — avoiding a redundant resolve.
        let workspace_root = self.workspace_root_for(ar.run)?;
        // SP-4 s4: the per-call sandbox handle, policy-fixed by the executor from the grant's
        // caps/network + this per-run workspace. `None` unless a `Sandbox` is wired AND a
        // workspace is resolved AND the agent holds a grant for this tool ⇒ `shell` refuses loud.
        let sandbox = self.bound_sandbox_for(ar, &call.name, &workspace_root);
        let ctx = crate::agent::tools::ToolContext {
            idempotency_key: idempotency_key.to_string(),
            effect_id: teid.clone(),
            credentials: std::sync::Arc::new(resolved),
            workspace_root,
            sandbox,
        };
        match self.tools.execute_ctx(&call.name, args, &ctx) {
            Ok(result) => {
                // SP-4 broker: scrub the EXACT injected credential VALUES first. A tool echoes
                // its own credential verbatim, so a whole-value match always succeeds here;
                // doing this BEFORE the s2 pattern redactor prevents a wrapped/composite secret
                // (whose high-entropy span the pattern would fragment) from partially surviving.
                // Per-call + pure ⇒ a tool holds only its own creds, so this stays
                // determinism-safe.
                let result =
                    super::content::scrub_secret_values(&result, &ctx.exposed_secret_values());
                // SP-4 s2: then pattern-redact the residual. Both passes are pure ⇒ the
                // journaled record and the value fed back to the agent are identical (live ==
                // journaled == replayed).
                let result = self.redact(&result);
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
                        // SP-DATA-5: stays None — this records a TOOL execution
                        // (`self.tools.execute_ctx`), not a model call, so there is no
                        // `InferenceResponse` in scope to report usage from.
                        usage: None,
                    },
                )
                .await?;
                Ok(ToolOutcome::Ok(result))
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
                Ok(ToolOutcome::Failed(message))
            }
        }
    }

    /// Dispatch one live model turn through the metered chokepoint and journal its
    /// result: on success, record the `{model, text, tool_calls}` output as a Pure
    /// effect (`eid`) and return it; on a refusal (SP-DATA-5 — an exhausted budget,
    /// or an unmetered call under a budget) return the journaled pause/failure; on a
    /// gateway error, journal `NodeFailed` and return the failure message. The outer
    /// `Err` is a fatal journal/CAS error.
    ///
    /// `meter` is the run's live token ledger, passed as the chokepoint's borrowed view
    /// so this helper does not depend on `Fold`'s shape.
    // Six positional inputs, each distinct and read straight through to either the
    // effect record or the chokepoint; bundling them would only relocate the plumbing.
    #[allow(clippy::too_many_arguments)]
    async fn dispatch_model_turn(
        &self,
        run: RunId,
        node_id: &NodeId,
        eid: EffectId,
        ih: String,
        request: InferenceRequest,
        meter: &super::dispatch::Meter<'_>,
    ) -> Result<ToolOutcome<serde_json::Value>, OrchestratorError> {
        match self.dispatch_metered(&request, meter).await {
            Ok(Ok(response)) => {
                // SP-4 s2: `model_output` builds `{model, text}` with `text` scrubbed
                // (the single redaction chokepoint) before journal + feed-back; the
                // ReAct path then appends `tool_calls` (the structured call args the
                // next turn dispatches on) UNREDACTED so a redacted turn still drives
                // its tools correctly. Same `{model, text, tool_calls}` shape as before.
                let mut output = self.model_output(&response);
                output["tool_calls"] = serde_json::json!(response.tool_calls);
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
                        // SP-DATA-5: the ReAct-turn producer — the real usage the
                        // provider reported on this turn, converted at the boundary.
                        usage: response.usage.map(super::content::convert_usage),
                    },
                )
                .await?;
                Ok(ToolOutcome::Ok(output))
            }
            // SP-DATA-5: the chokepoint refused before spending. `record_refusal`
            // journaled it (`RunPaused` for an exhausted budget — the HOTL class —
            // or `NodeFailed` for an unmetered call), so just carry it out.
            Ok(Err(refusal)) => match self.record_refusal(run, node_id, refusal).await? {
                super::dispatch::RefusalKind::Paused(reason) => Ok(ToolOutcome::Paused(reason)),
                super::dispatch::RefusalKind::Failed(message) => Ok(ToolOutcome::Failed(message)),
            },
            // A fully-gated chain with a timed re-eligibility (§11.2) is a durable
            // pause (resumable) — on resume the turn re-attempts (no `EffectRecorded`
            // was journaled). Every other gateway error fails the node.
            Err(error) => match classify_gateway_error(&error) {
                GatewayDisposition::Pause {
                    resume_after,
                    reason,
                } => {
                    self.append(
                        run,
                        JournalEvent::RunPaused {
                            reason: reason.clone(),
                            resume_after: Some(resume_after),
                        },
                    )
                    .await?;
                    Ok(ToolOutcome::Paused(reason))
                }
                GatewayDisposition::Fail(message) => {
                    self.append(
                        run,
                        JournalEvent::NodeFailed {
                            node: node_id.clone(),
                            error: message.clone(),
                        },
                    )
                    .await?;
                    Ok(ToolOutcome::Failed(message))
                }
            },
        }
    }
}
