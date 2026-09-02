//! Pure, state-free helpers shared by the executor's orchestration modules
//! (`super`/`agent`/`fanout`): journal folding, round scheduling, request
//! compilation, and determinism hashing. Kept off `Executor` so all three
//! modules reuse them without duplication.

use std::collections::{HashMap, HashSet};

use kernel::types::capability::Capability;
use kernel::types::request::{InferenceRequest, Message, MessageRole, Payload, ToolDefinition};
use orchestrator_core::{
    ChildStatus, ContentRef, ContextRef, EdgeKind, EffectOutput, Graph, JournalEvent, MapBody,
    Node, NodeId, NodeKind, OrchestratorError, Seq, effect_id,
};
use sha2::{Digest, Sha256};

use super::{AgentAnswer, Fold, GateDecision, LoopGateAsk, LoopGateDecision};

/// One scheduling round's **ready set** (§3.2): the not-yet-terminal nodes whose
/// `Hard` deps have all completed and `Soft` deps are all terminal, in graph
/// declaration order (deterministic). A linear graph yields exactly one.
pub(crate) fn ready_nodes<'g>(
    graph: &'g Graph,
    completed: &HashSet<NodeId>,
    terminal: &HashSet<NodeId>,
) -> Vec<&'g Node> {
    graph
        .nodes
        .iter()
        .filter(|node| !terminal.contains(&node.id))
        .filter(|node| {
            node.deps.iter().all(|dep| match dep.kind {
                EdgeKind::Hard => completed.contains(&dep.on),
                EdgeKind::Soft => terminal.contains(&dep.on),
            })
        })
        .collect()
}

/// If `node` is a `Consolidate` over a `ModelCall`-body `Map`, return that Map's
/// id — the Map whose per-child records become compactable once the Consolidate
/// completes (§5.3). `Agent`-body Maps are multi-effect and are not compacted.
pub(crate) fn consolidate_compaction_target<'g>(
    graph: &'g Graph,
    node: &'g Node,
) -> Option<&'g NodeId> {
    let NodeKind::Consolidate { over, .. } = &node.kind else {
        return None;
    };
    let over_is_modelcall_map = graph.nodes.iter().any(|n| {
        &n.id == over
            && matches!(
                &n.kind,
                NodeKind::Map {
                    body: MapBody::ModelCall { .. },
                    ..
                }
            )
    });
    over_is_modelcall_map.then_some(over)
}

/// Fold a run's journal into resume state: the effect memo, the started/
/// completed sets, each node's **last output as a ref** (no blob loaded — §7.4),
/// and the completed-node order (used only to reconstruct a terminal outcome). A
/// `MapCompacted` manifest rebuilds its children's memo entries as content refs
/// (§5.3), so a compacted Map replays without re-spending.
pub(crate) fn fold_journal(
    events: &[(Seq, JournalEvent)],
) -> (Fold, HashMap<NodeId, EffectOutput>, Vec<NodeId>) {
    let mut fold = Fold::default();
    let mut node_last_output: HashMap<NodeId, EffectOutput> = HashMap::new();
    let mut completed: Vec<NodeId> = Vec::new();
    for (_, event) in events {
        match event {
            JournalEvent::EffectRecorded {
                node,
                effect_id,
                input_hash,
                output,
                observation,
                usage,
                ..
            } => {
                fold.memo
                    .insert(effect_id.clone(), (input_hash.clone(), output.clone()));
                node_last_output.insert(node.clone(), output.clone());
                // §7.1: an Observation's latest record wins its freshness slot, so
                // a stale re-read (which appends a fresh record) supersedes.
                if let Some(meta) = observation {
                    fold.observations.insert(effect_id.clone(), meta.clone());
                }
                // SP-DATA-5: keyed by effect id, NOT summed over events — the
                // two-phase Mutation path can append a second `EffectRecorded` for
                // one effect_id (an in-doubt `Confirmed` reconcile); an `insert`
                // here overwrites that duplicate rather than double-counting it, so
                // folding stays idempotent across any number of resumes.
                if let Some(u) = usage {
                    fold.usage.insert(effect_id.clone(), *u);
                }
            }
            JournalEvent::NodeStarted { node } => {
                fold.started.insert(node.clone());
            }
            JournalEvent::NodeCompleted { node } => {
                fold.completed.insert(node.clone());
                completed.push(node.clone());
            }
            // SP-6 s1 (whole-slice review): the recorded failure of a node, folded so a
            // node kind that is TERMINAL on failure can read its own verdict back instead
            // of re-deriving it. Nothing else consults `fold.failed`, and that is the
            // point: a `NodeFailed` does NOT make a node terminal in general — a
            // `ModelCall`/`Agent` whose provider died re-attempts on resume, and there are
            // tests that require exactly that. FIRST wins, so the verdict a resume reads is
            // the one the run actually stopped on.
            JournalEvent::NodeFailed { node, error } => {
                fold.failed.entry(node.clone()).or_insert(error.clone());
            }
            // The cascade-skip record, folded so `cascade_skip_from` appends each node's
            // `NodeSkipped` at most once ACROSS drives. It was a `_` catch-all until the
            // SP-6 s4 review measured a terminally-failed-but-still-resumable run growing
            // by one row per hard dependent per wake — the same bounded-growth class as
            // the `NodeFailed` arm above, one edge further out. Read only by that guard;
            // it does not make a node terminal (`ready_nodes` works off the per-drive
            // `DriveState`, which is rebuilt from the graph every time).
            JournalEvent::NodeSkipped { node } => {
                fold.skipped.insert(node.clone());
            }
            // The intent phase of a two-phase Mutation (§7.3). An effect id in
            // `intents` with no matching `EffectRecorded` is in-doubt on resume.
            JournalEvent::EffectIntent {
                effect_id,
                idempotency_key,
                ..
            } => {
                fold.intents
                    .insert(effect_id.clone(), idempotency_key.clone());
            }
            // A blackboard publish (§8): fold it so a resume rehydrates the store
            // (as refs, no blob load) and the publish-guard skips re-publishing.
            JournalEvent::ContextWrite {
                scope,
                key,
                content,
                summary,
                ..
            } => {
                fold.context.insert(
                    (scope.clone(), key.clone()),
                    ContextRef {
                        key: key.clone(),
                        scope: scope.clone(),
                        content: content.clone(),
                        summary: summary.clone(),
                    },
                );
            }
            JournalEvent::MapCompacted { node, children } => {
                for c in children {
                    let child = NodeId(format!("{}/{}", node.0, c.index));
                    // SP-DATA-5: the compacted child's spend re-enters the ledger
                    // under the SAME key its deleted `EffectRecorded` used —
                    // `effect_id(child, 0, 0)`, exactly the id the memo below
                    // reconstructs. Keying (rather than accumulating) is what keeps
                    // folding idempotent: fold the same `MapCompacted` twice and the
                    // second `insert` overwrites the first, so it counts ONCE; and if
                    // a child's own record somehow survived alongside the manifest,
                    // the identical key still counts it once rather than twice. This
                    // is the same argument that makes `EffectRecorded`'s own
                    // `usage.insert` safe against a duplicate `Confirmed` reconcile.
                    if let Some(u) = c.usage {
                        fold.usage.insert(effect_id(&child.0, 0, 0), u);
                    }
                    if c.status == ChildStatus::Ok
                        && let (Some(digest), Some(input_hash)) = (&c.digest, &c.input_hash)
                    {
                        let output = EffectOutput::Ref(ContentRef {
                            digest: digest.clone(),
                            size: 0,
                            summary: None,
                        });
                        fold.memo.insert(
                            effect_id(&child.0, 0, 0),
                            (input_hash.clone(), output.clone()),
                        );
                        node_last_output.insert(child, output);
                    }
                }
            }
            JournalEvent::PlanExpanded { node, subgraph, .. } => {
                fold.expansions.insert(node.clone(), subgraph.clone());
            }
            JournalEvent::PlannerSelected { node, agent } => {
                fold.selections.insert(node.clone(), agent.clone());
            }
            // SP-6 s1: an EXPLICIT arm, never the `_` catch-all below — a
            // silently-swallowed `SignalReceived` would mean a node that can never be
            // signalled, and it would compile perfectly. LAST wins: `insert` overwrites,
            // so an operator can correct a mistaken decision before the run resumes.
            JournalEvent::SignalReceived { node, payload } => {
                fold.signals.insert(node.clone(), payload.clone());
            }
            // SP-6 s1: also EXPLICIT, for the same reason. FIRST wins —
            // `entry().or_insert()`, NOT `insert` — the opposite asymmetry from
            // `SignalReceived` above and deliberately so: overwriting here would let a
            // later `SignalAwaited` push the deadline forward on every resume, so a run
            // force-woken every ten minutes with a one-hour timeout would NEVER expire.
            JournalEvent::SignalAwaited {
                node,
                deadline: Some(d),
            } => {
                fold.deadlines.entry(node.clone()).or_insert(Some(*d));
                fold.signal_asks.insert(node.clone());
            }
            // The deadline-LESS gate. `None` is folded as a REAL value, not dropped: the
            // map's key answers "has this node begun waiting?", which is a different
            // question from "by when?". Whole-slice review I1 — the arm used to be an
            // empty `{}`, which made the node's first-execution branch fire on EVERY
            // drive and re-journal `SignalAwaited` each time. First-wins is unchanged;
            // only what is remembered got wider.
            JournalEvent::SignalAwaited {
                node,
                deadline: None,
            } => {
                fold.deadlines.entry(node.clone()).or_insert(None);
                fold.signal_asks.insert(node.clone());
            }
            // Both `SignalAwaited` arms above also record this node in `signal_asks`, the
            // per-kind counterpart of `menus`/`agent_prompts`. It is a SET rather than an
            // `or_insert` of a value because this event carries nothing beyond the deadline
            // (which belongs in the shared map), and set membership is idempotent, so
            // first-wins and last-wins coincide.
            // SP-6 s2: the ask. Deliberately EXPLICIT rather than folded with
            // `SignalAwaited` by a catch-all — the menu has no analogue there, and a
            // catch-all would silently absorb a future variant.
            //
            // FIRST wins for BOTH the deadline and the menu (`entry().or_insert`, never
            // `insert`). For the deadline that is s1's never-expires fix. For the menu it
            // is the §4 rule: a human was shown a menu, and a later ask must not change
            // what their answer meant.
            JournalEvent::GateAwaited {
                node,
                deadline,
                options,
            } => {
                fold.deadlines.entry(node.clone()).or_insert(*deadline);
                fold.menus.entry(node.clone()).or_insert(options.clone());
            }
            // SP-6 s2: the answer. LAST wins (`insert` overwrites) — an operator can
            // correct a mistaken decision while the run is still paused.
            JournalEvent::GateDecided {
                node,
                option,
                actor,
                note,
            } => {
                fold.gate_decisions.insert(
                    node.clone(),
                    GateDecision {
                        option: option.clone(),
                        actor: actor.clone(),
                        note: note.clone(),
                    },
                );
            }
            // SP-6 s3: the ask. EXPLICIT, never folded by a catch-all — a catch-all
            // silently absorbing a new variant is how this codebase has shipped fold bugs.
            //
            // FIRST wins for BOTH the deadline and the prompt (`entry().or_insert`).
            // The deadline goes into the SHARED map because `wait_or_expire` reads
            // `deadline_for` and knows nothing about which kind recorded it. That makes
            // this the THIRD writer of `Fold::deadlines` (after `SignalAwaited` and
            // `GateAwaited`; SP-6 s4's `LoopGateAwaited`, two arms below, is the FOURTH).
            // When a FIFTH is added, update the writer lists on `Fold::deadlines` and
            // `Fold::deadline_for`, and give it a kind-specific record of its own plus
            // the missing-ask arm that reads it — every reader of this map reasons from
            // an explicit enumeration of these writers: `run_await_signal`,
            // `run_human_gate`, `run_human_agent`, and s4's `run_human_loop_gate`.
            //
            // And the enumeration is restated OUTSIDE those arms, not only in them: FOUR
            // `..._fails_loudly` kind-swap tests in `tests.rs`, one per kind, spell out the
            // writer list in their rustdoc to explain why their arm is reachable at all;
            // `human.rs`'s module header counts the kinds; and `durable-journal.md` states
            // it for the feature docs. (There were only three of those tests until the s4
            // review added `a_loop_gate_that_recorded_a_wait_without_a_menu_fails_loudly` —
            // s3 shipped ITS copy missing and review found it, then s4 shipped the same
            // way, so this sentence overstated what the suite held for two slices running.)
            //
            // s4's Task 4 updated the three arms and left every one of those sites saying
            // THREE; its whole-slice review caught that, and the Tasks 6+7 review then
            // found four more that had only become stale once the fourth EXECUTING kind
            // landed. So the instruction is
            // `rg -in 'all (THREE|FOUR|FIVE)|(three|four|five) waiting kinds' crates docs`
            // and fix the WHOLE set — updating only the arms is how this went stale.
            //
            // `deadline` is folded THROUGH, `None` included — never `if
            // deadline.is_some()`. The key alone answers "has this node begun asking?",
            // and `AgentBacking::Human { timeout: None }` is a real configuration, so
            // dropping the `None` would make an indefinite human agent re-ask on every
            // drive (s1 shipped exactly that bug on the `SignalAwaited` arm above).
            JournalEvent::AgentAwaited {
                node,
                deadline,
                prompt,
            } => {
                fold.deadlines.entry(node.clone()).or_insert(*deadline);
                fold.agent_prompts
                    .entry(node.clone())
                    .or_insert(prompt.clone());
            }
            // SP-6 s3: the answer. LAST wins (`insert` overwrites).
            JournalEvent::AgentAnswered { node, text, actor } => {
                fold.agent_answers.insert(
                    node.clone(),
                    AgentAnswer {
                        text: text.clone(),
                        actor: actor.clone(),
                    },
                );
            }
            // SP-6 s4: the ask. EXPLICIT, never folded by a catch-all.
            //
            // FIRST wins for the deadline, the prompt AND the menu (`entry().or_insert`).
            // This is the FOURTH writer of the SHARED `Fold::deadlines` map, after
            // `SignalAwaited`, `GateAwaited` and `AgentAwaited` — the writer lists on
            // `Fold::deadlines` and `Fold::deadline_for` name it, and the arm that reads
            // its kind-specific record is `run_human_loop_gate`'s missing-MENU arm (this
            // kind's version of the missing-ask guard: it reads `Fold::loop_gate_menu_for`,
            // which answers "did the LOOP GATE kind begin here?" and hands back the menu
            // the decision must be validated against in the same call).
            //
            // `deadline` is folded THROUGH, `None` included. A role with
            // `backed_by: human { timeout: None }` gating a loop is a real configuration,
            // and dropping the `None` would make it re-journal `LoopGateAwaited` on every
            // drive — the bug s1 shipped on the `SignalAwaited` arm.
            JournalEvent::LoopGateAwaited {
                node,
                deadline,
                prompt,
                menu,
            } => {
                fold.deadlines.entry(node.clone()).or_insert(*deadline);
                fold.loop_gate_asks
                    .entry(node.clone())
                    .or_insert(LoopGateAsk {
                        prompt: prompt.clone(),
                        menu: menu.clone(),
                    });
            }
            // SP-6 s4: the decision. LAST wins (`insert` overwrites).
            JournalEvent::LoopGateDecided {
                node,
                option,
                actor,
            } => {
                fold.loop_gate_decisions.insert(
                    node.clone(),
                    LoopGateDecision {
                        option: option.clone(),
                        actor: actor.clone(),
                    },
                );
            }
            // SP-6 s4: the drive that HONOURED a decision, recorded so no later drive
            // re-derives that gate against a clock which has since passed its deadline.
            // FIRST wins — the executor writes at most one, so a second can only come from
            // a journal it did not write, and the first is the one that happened.
            //
            // This is the arm that makes `LoopGateDecided`'s LAST-wins rule bounded rather
            // than unbounded: a correction wins right up to the drive that acts on the
            // answer, and not after it.
            JournalEvent::LoopGateSettled { node, option } => {
                fold.loop_gate_settlements
                    .entry(node.clone())
                    .or_insert_with(|| option.clone());
            }
            // SP-DATA-5: the run's original cap, set once at submit. An EXPLICIT
            // arm — not the `_` catch-all below — because a budget that silently
            // never folds is a bug the compiler cannot catch for us (`budget` stays
            // `Option`-shaped either way), so this must be deliberate, not implicit.
            JournalEvent::RunStarted { budget, .. } => {
                fold.budget = budget.map(|b| b.total_tokens);
            }
            // SP-DATA-5: an operator-issued raise (or lower). Latest value wins, so
            // this OVERWRITES rather than accumulates — also an EXPLICIT arm for the
            // same reason as `RunStarted` above: falling through to `_ => {}` would
            // compile cleanly and silently make the budget un-raisable.
            JournalEvent::BudgetRaised { new_total_tokens } => {
                fold.budget = Some(*new_total_tokens);
            }
            _ => {}
        }
    }
    (fold, node_last_output, completed)
}

/// Project each Agent node's raw final model-turn output (`{model, text,
/// tool_calls}`) down to the canonical `{model, text}` a fresh `run` returns from
/// `AgentStep::Completed` (design §4) — so a completed Agent node yields an
/// identical shape on every completion path. Pure over already-materialized
/// outputs; `ModelCall` nodes already store the canonical shape and are untouched.
///
/// **A HUMAN-answered node (SP-6 s3) is passed through unchanged**, because its
/// canonical shape is a different one: `{text, actor}` (design §4 / AC2), and forcing
/// it through the model projection would drop the `actor` and invent `model: null`
/// for a node no model ever touched.
///
/// The discriminator is the presence of an `"actor"` key, and that is sound because
/// this function's inputs are never author- or model-supplied: EVERY Agent-node output
/// is built by the executor itself. The model path builds exactly `{model, text}`
/// (`finish_agent`) or `{model, text, tool_calls}` (`dispatch_model_turn`) — no
/// `actor`, ever — and `actor` reaches an output only by being folded from
/// `JournalEvent::AgentAnswered`. So `actor` present ⟺ a human answered.
///
/// Deciding this HERE rather than in the human drive path is deliberate: this function
/// runs on exactly ONE path (the `terminal` branch of `start_inner`), so getting it
/// wrong is invisible on the drive that completes a node and shows up only when the
/// finished run is read back — the same run reporting two different outputs depending
/// on when it is read. Review caught that as a forward-looking finding against the
/// event before the drive path existed; see
/// `the_projection_preserves_a_human_answer_and_leaves_model_outputs_canonical`.
///
/// **The `actor` exemption is still forward-looking as of SP-6 s3 Task 4, and nothing in
/// the shipped human path reaches it.** The terminal branch builds `outputs` from
/// [`fold_journal`]'s `node_last_output`, which is populated ONLY from `EffectRecorded`
/// and `MapCompacted`; `run_human_agent` journals neither, so a human-answered node is
/// absent from a terminal re-read altogether rather than present-and-mis-projected
/// (`a_finished_human_backed_run_reports_no_output_when_read_back`). The exemption is
/// kept, and its unit test with it, because the projection is the wrong place to
/// discover that: the first change that gives a waiting node kind a durable per-node
/// output would otherwise silently rewrite `{text, actor}` into `{model: null, text}`.
/// This note exists because the sibling comment in `human.rs` previously described that
/// mis-projection as something that happens TODAY, which it does not.
///
/// The alternative — resolving each node's `AgentRef` against the `Registry` to ask
/// whether its backing is `Human` — was rejected: it makes a pure, total projection
/// depend on registry resolution that can fail, to recover a fact the output already
/// carries.
pub(crate) fn project_agent_outputs(
    graph: &Graph,
    outputs: &mut HashMap<NodeId, serde_json::Value>,
) {
    for node in &graph.nodes {
        if let NodeKind::Agent { .. } = &node.kind
            && let Some(output) = outputs.get(&node.id).cloned()
        {
            if output.get("actor").is_some() {
                continue;
            }
            let model = output
                .get("model")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let text = output.get("text").cloned().unwrap_or_default();
            outputs.insert(
                node.id.clone(),
                serde_json::json!({ "model": model, "text": text }),
            );
        }
    }
}

/// Structural content hash of a node's `(chain, payload)` — the determinism key
/// checked against the memo on resume: `sha256_hex("{chain}|{json(payload)}")`.
pub(crate) fn input_hash(
    chain: &str,
    payload: &serde_json::Value,
) -> Result<String, OrchestratorError> {
    let serialized = serde_json::to_string(payload)?;
    let mut hasher = Sha256::new();
    hasher.update(format!("{chain}|{serialized}").as_bytes());
    Ok(format!("{:x}", hasher.finalize()))
}

/// Compile a `ModelCall`'s `(chain, payload)` into a plain single-turn chat
/// [`InferenceRequest`]: `TextChat` over the named chain, fallback enabled, all
/// other addressing/identity fields defaulted. The payload's `"prompt"` string
/// (if present) becomes the sole user message.
pub(crate) fn build_request(chain: &str, payload: &serde_json::Value) -> InferenceRequest {
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
pub(crate) fn render_input(input: &serde_json::Value) -> String {
    match input {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Compile one ReAct turn into a chat `InferenceRequest` (system + transcript +
/// tools) over the agent's chain. `budget: None` — cost budgeting is the gateway's
/// dormant axis in slice 2 (see the design); this request carries only window-fit.
pub(crate) fn build_chat_request(
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
pub(crate) fn agent_input_hash(
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
pub(crate) fn tool_input_hash(name: &str, arguments: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(format!("{name}|{arguments}").as_bytes());
    format!("{:x}", hasher.finalize())
}

/// How the executor should treat a gateway error (§11.2): a timed chain-gate is a
/// durable pause; everything else fails (a terminal gate carries its human-action
/// hint in the message).
#[derive(Debug)]
pub(crate) enum GatewayDisposition {
    Pause {
        resume_after: chrono::DateTime<chrono::Utc>,
        reason: String,
    },
    Fail(String),
}

/// Classify a gateway error: only `AllGated{resume_after: Some(t)}` (every
/// candidate gated, with a timed re-eligibility) pauses — to `t`. Every other
/// error, including `AllGated{None}` (all gates terminal), fails; its `Display`
/// carries the reason / human-action hint.
pub(crate) fn classify_gateway_error(
    err: &kernel::types::error::GatewayError,
) -> GatewayDisposition {
    match err {
        kernel::types::error::GatewayError::AllGated {
            resume_after: Some(t),
            ..
        } => GatewayDisposition::Pause {
            resume_after: *t,
            reason: format!("all candidates gated; resume after {t}"),
        },
        other => GatewayDisposition::Fail(other.to_string()),
    }
}

/// Estimate a prompt's tokens (for the over-budget diagnostic's `est`).
pub(crate) fn est_prompt_tokens(
    system: &str,
    messages: &[Message],
    tools: &[ToolDefinition],
) -> usize {
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

    #[test]
    fn fold_journal_captures_plan_expansions() {
        use orchestrator_core::{Graph, JournalEvent, Node, NodeId, NodeKind};
        let subgraph = Graph {
            nodes: vec![Node {
                id: NodeId("n1".into()),
                kind: NodeKind::ModelCall {
                    chain: "c".into(),
                    payload: serde_json::json!(0),
                },
                deps: vec![],
            }],
        };
        let events = vec![(
            0u64,
            JournalEvent::PlanExpanded {
                node: NodeId("e".into()),
                subgraph: subgraph.clone(),
                node_plans: std::collections::HashMap::new(),
            },
        )];
        let (fold, _last, _completed) = fold_journal(&events);
        assert_eq!(
            fold.expansions
                .get(&NodeId("e".into()))
                .map(|g| g.nodes.len()),
            Some(1),
            "PlanExpanded folds into fold.expansions"
        );
    }

    #[test]
    fn fold_captures_the_intent_idempotency_key() {
        use orchestrator_core::{JournalEvent, NodeId};
        let eid = effect_id("n1", 0, 1);
        let events = vec![(
            0u64,
            JournalEvent::EffectIntent {
                node: NodeId("n1".into()),
                effect_id: eid.clone(),
                idempotency_key: "the-key".into(),
                args_hash: "h".into(),
                seq: 0,
            },
        )];
        let (fold, _last, _completed) = fold_journal(&events);
        assert_eq!(fold.intents.get(&eid), Some(&"the-key".to_string()));
    }

    /// THE guard. Two `EffectRecorded` events for the SAME effect_id — reachable via
    /// the two-phase Mutation path's in-doubt `Confirmed` reconcile — must count ONCE.
    /// Summing the event stream instead of keying by effect id double-counts here, and
    /// the overcount compounds on every resume.
    #[test]
    fn duplicate_effect_records_count_their_usage_only_once() {
        use orchestrator_core::{EffectClass, EffectId, JournalEvent, NodeId, TokenUsage};
        let usage = TokenUsage {
            input_tokens: 100,
            output_tokens: 50,
            total_tokens: 150,
        };
        let ev = |seq: Seq| {
            (
                seq,
                JournalEvent::EffectRecorded {
                    node: NodeId("n1".into()),
                    effect_id: EffectId("same-id".into()),
                    class: EffectClass::Mutation,
                    input_hash: "h".into(),
                    seq,
                    output: EffectOutput::Inline(serde_json::Value::Null),
                    observation: None,
                    usage: Some(usage),
                },
            )
        };
        let (fold, _, _) = fold_journal(&[ev(0), ev(1)]);
        assert_eq!(
            fold.spent(),
            150,
            "one effect id must contribute its usage once, not once per event"
        );
    }

    #[test]
    fn distinct_effects_sum_their_usage() {
        use orchestrator_core::{EffectClass, EffectId, JournalEvent, NodeId, TokenUsage};
        let mk = |id: &str, total: u32, seq: Seq| {
            (
                seq,
                JournalEvent::EffectRecorded {
                    node: NodeId("n1".into()),
                    effect_id: EffectId(id.into()),
                    class: EffectClass::Pure,
                    input_hash: "h".into(),
                    seq,
                    output: EffectOutput::Inline(serde_json::Value::Null),
                    observation: None,
                    usage: Some(TokenUsage {
                        input_tokens: 0,
                        output_tokens: 0,
                        total_tokens: total,
                    }),
                },
            )
        };
        let (fold, _, _) = fold_journal(&[mk("a", 100, 0), mk("b", 250, 1)]);
        assert_eq!(fold.spent(), 350);
    }

    /// The compacted-spend analogue of `duplicate_effect_records_count_their_usage_only_once`.
    /// A `MapCompacted` folded TWICE (or folded alongside a surviving child record)
    /// must count each child once: the manifest re-enters the ledger under the child's
    /// ORIGINAL effect id, so the `HashMap` key does the deduping — not a running sum.
    #[test]
    fn a_map_compacted_manifest_counts_its_children_once_however_often_it_is_folded() {
        use orchestrator_core::{
            ChildStatus, CompactChild, Digest, EffectClass, JournalEvent, NodeId, TokenUsage,
        };
        let child = |index: usize, total: u32| CompactChild {
            index,
            status: ChildStatus::Ok,
            digest: Some(Digest("d".into())),
            input_hash: Some("h".into()),
            usage: Some(TokenUsage {
                input_tokens: 0,
                output_tokens: total,
                total_tokens: total,
            }),
        };
        let manifest = || JournalEvent::MapCompacted {
            node: NodeId("m".into()),
            children: vec![child(0, 100), child(1, 200)],
        };
        // A child record that outlived compaction: same effect id as the manifest's
        // entry for index 0, so it must not be counted a second time.
        let survivor = JournalEvent::EffectRecorded {
            node: NodeId("m/0".into()),
            effect_id: effect_id("m/0", 0, 0),
            class: EffectClass::Pure,
            input_hash: "h".into(),
            seq: 0,
            output: EffectOutput::Inline(serde_json::Value::Null),
            observation: None,
            usage: Some(TokenUsage {
                input_tokens: 0,
                output_tokens: 100,
                total_tokens: 100,
            }),
        };
        let (fold, _, _) = fold_journal(&[(0, manifest())]);
        assert_eq!(fold.spent(), 300, "each compacted child counts once");
        let (twice, _, _) = fold_journal(&[(0, manifest()), (1, manifest())]);
        assert_eq!(
            twice.spent(),
            300,
            "folding the manifest twice is idempotent"
        );
        let (mixed, _, _) = fold_journal(&[(0, survivor), (1, manifest())]);
        assert_eq!(
            mixed.spent(),
            300,
            "a surviving child record and its manifest entry share one effect id"
        );
    }

    /// Additivity for the new `CompactChild.usage` field: a `MapCompacted` serialized
    /// BEFORE it existed still deserializes and folds exactly as it always did —
    /// memo rebuilt, spend zero (those children's tokens are already gone; the fix
    /// cannot invent them, and pretending otherwise would be worse than reporting a
    /// short ledger for a pre-fix run).
    #[test]
    fn a_pre_fix_map_compacted_without_usage_still_deserializes_and_folds() {
        use orchestrator_core::JournalEvent;
        let json = r#"{"MapCompacted":{"node":"m","children":[
            {"index":0,"status":"Ok","digest":"abc","input_hash":"h"}]}}"#;
        let event: JournalEvent = serde_json::from_str(json).expect("old manifest deserializes");
        let (fold, last, _) = fold_journal(&[(0, event)]);
        assert_eq!(fold.spent(), 0, "no usage recorded ⇒ no usage folded");
        assert!(
            fold.memo.contains_key(&effect_id("m/0", 0, 0))
                && last.contains_key(&NodeId("m/0".into())),
            "the memo rebuild is untouched by the added field"
        );
    }

    #[test]
    fn a_budget_is_folded_from_run_started_and_the_latest_raise_wins() {
        use orchestrator_core::{JournalEvent, TokenBudget};
        let evs = vec![
            (
                0,
                JournalEvent::RunStarted {
                    version: "v1".into(),
                    budget: Some(TokenBudget {
                        total_tokens: 1_000,
                    }),
                },
            ),
            (
                1,
                JournalEvent::BudgetRaised {
                    new_total_tokens: 5_000,
                },
            ),
            (
                2,
                JournalEvent::BudgetRaised {
                    new_total_tokens: 2_000,
                },
            ),
        ];
        let (fold, _, _) = fold_journal(&evs);
        assert_eq!(
            fold.budget(),
            Some(2_000),
            "latest wins — lowering is a legitimate way to halt a run"
        );
    }

    #[test]
    fn an_unbudgeted_run_folds_no_budget_and_no_spend() {
        use orchestrator_core::JournalEvent;
        let evs = vec![(
            0,
            JournalEvent::RunStarted {
                version: "v1".into(),
                budget: None,
            },
        )];
        let (fold, _, _) = fold_journal(&evs);
        assert_eq!(fold.budget(), None);
        assert_eq!(fold.spent(), 0);
    }

    #[test]
    fn a_received_signal_is_folded_by_node_id() {
        let evs = vec![(
            0,
            JournalEvent::SignalReceived {
                node: NodeId("gate".into()),
                payload: serde_json::json!({"decision": "approved"}),
            },
        )];
        let (fold, _, _) = fold_journal(&evs);
        assert_eq!(
            fold.signal_for(&NodeId("gate".into())).unwrap()["decision"],
            "approved"
        );
    }

    /// Last delivery wins while the node is still paused — an operator must be able to
    /// correct a mistaken decision before the run resumes.
    #[test]
    fn a_later_signal_overwrites_an_earlier_one_for_the_same_node() {
        let sig = |seq: Seq, d: &str| {
            (
                seq,
                JournalEvent::SignalReceived {
                    node: NodeId("gate".into()),
                    payload: serde_json::json!({ "decision": d }),
                },
            )
        };
        let (fold, _, _) = fold_journal(&[sig(0, "rejected"), sig(1, "approved")]);
        assert_eq!(
            fold.signal_for(&NodeId("gate".into())).unwrap()["decision"],
            "approved"
        );
    }

    /// THE guard for this slice's trap. The deadline is recorded ONCE and folded
    /// thereafter; a second `SignalAwaited` must not move it. Recomputing `now + timeout`
    /// on each execution is the bug this pins.
    #[test]
    fn the_first_recorded_deadline_wins_and_is_never_moved() {
        let t0 = chrono::DateTime::<chrono::Utc>::from_timestamp(1_000_000, 0).unwrap();
        let t1 = chrono::DateTime::<chrono::Utc>::from_timestamp(9_000_000, 0).unwrap();
        let ev = |seq: Seq, d| {
            (
                seq,
                JournalEvent::SignalAwaited {
                    node: NodeId("gate".into()),
                    deadline: Some(d),
                },
            )
        };
        let (fold, _, _) = fold_journal(&[ev(0, t0), ev(1, t1)]);
        assert_eq!(
            fold.deadline_for(&NodeId("gate".into())),
            Some(Some(t0)),
            "the ORIGINAL deadline must survive; a later record must not extend it"
        );
    }

    /// Whole-slice review I1 — a deadline-LESS `SignalAwaited` folds as a real value, so
    /// the node reads back as "already waiting" and its first-execution branch (which
    /// journals the event) fires exactly once. Dropping the `None` here is what let the
    /// node re-record itself on every drive.
    #[test]
    fn a_deadline_less_await_still_records_that_the_node_is_waiting() {
        let ev = |seq: Seq| {
            (
                seq,
                JournalEvent::SignalAwaited {
                    node: NodeId("gate".into()),
                    deadline: None,
                },
            )
        };
        let (fold, _, _) = fold_journal(&[ev(0)]);
        assert_eq!(
            fold.deadline_for(&NodeId("gate".into())),
            Some(None),
            "`Some(None)` = began waiting, with no deadline — NOT `None` = never waited"
        );

        // And first-wins still holds across the two shapes: a later `Some` must not
        // retro-fit a deadline onto a gate that began waiting without one.
        let later = (
            1,
            JournalEvent::SignalAwaited {
                node: NodeId("gate".into()),
                deadline: Some(
                    chrono::DateTime::<chrono::Utc>::from_timestamp(9_000_000, 0).unwrap(),
                ),
            },
        );
        let (fold, _, _) = fold_journal(&[ev(0), later]);
        assert_eq!(fold.deadline_for(&NodeId("gate".into())), Some(None));
    }

    #[test]
    fn a_node_with_no_signal_and_no_deadline_folds_to_none() {
        let (fold, _, _) = fold_journal(&[]);
        assert_eq!(fold.signal_for(&NodeId("gate".into())), None);
        assert_eq!(fold.deadline_for(&NodeId("gate".into())), None);
        assert_eq!(fold.failure_for(&NodeId("gate".into())), None);
    }

    /// **Whole-slice review, Important.** A `NodeFailed` folds, keyed by node, FIRST wins —
    /// the verdict a resume reads must be the one the run actually stopped on, not a later
    /// re-derivation of it. Without this arm an expired `AwaitSignal` gate re-ran on every
    /// drive: it re-appended its own `NodeFailed`, and a late `SignalReceived` completed it
    /// as *approved*.
    ///
    /// The fold is deliberately inert for every other node kind — see `Fold::failed`.
    #[test]
    fn a_node_failure_is_folded_by_node_id_and_the_first_verdict_wins() {
        let fail = |seq: Seq, error: &str| {
            (
                seq,
                JournalEvent::NodeFailed {
                    node: NodeId("gate".into()),
                    error: error.into(),
                },
            )
        };
        let (fold, _, _) = fold_journal(&[fail(0, "deadline passed"), fail(1, "something else")]);
        assert_eq!(
            fold.failure_for(&NodeId("gate".into())),
            Some("deadline passed"),
            "the ORIGINAL verdict survives a later one"
        );
        assert_eq!(fold.failure_for(&NodeId("other".into())), None);
    }

    fn at(unix_secs: i64) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::<chrono::Utc>::from_timestamp(unix_secs, 0).expect("valid timestamp")
    }

    fn gopt(name: &str, outcome: orchestrator_core::GateOutcome) -> orchestrator_core::GateOption {
        orchestrator_core::GateOption {
            name: name.to_string(),
            outcome,
        }
    }

    /// The two fold asymmetries are OPPOSITE and both load-bearing, exactly as s1's are.
    #[test]
    fn gate_decisions_are_last_wins_and_the_menu_is_first_wins() {
        use orchestrator_core::GateOutcome;
        let events = vec![
            (
                1,
                JournalEvent::GateAwaited {
                    node: NodeId("release".into()),
                    deadline: Some(at(1_000)),
                    options: vec![
                        gopt("ship", GateOutcome::Complete),
                        gopt("hold", GateOutcome::Fail),
                    ],
                },
            ),
            (
                2,
                JournalEvent::GateDecided {
                    node: NodeId("release".into()),
                    option: "hold".into(),
                    actor: "alice".into(),
                    note: None,
                },
            ),
            // An operator corrects themselves before the run resumes: LAST wins.
            (
                3,
                JournalEvent::GateDecided {
                    node: NodeId("release".into()),
                    option: "ship".into(),
                    actor: "alice".into(),
                    note: Some("legal cleared it".into()),
                },
            ),
            // A second ask must NOT move the deadline or the menu: FIRST wins.
            // Overwriting the deadline IS the never-expires bug.
            (
                4,
                JournalEvent::GateAwaited {
                    node: NodeId("release".into()),
                    deadline: Some(at(9_999)),
                    options: vec![gopt("escalate", GateOutcome::Complete)],
                },
            ),
        ];
        let (fold, _, _) = fold_journal(&events);

        let d = fold
            .gate_decision_for(&NodeId("release".into()))
            .expect("decided");
        assert_eq!(d.option, "ship", "LAST decision wins");
        assert_eq!(d.actor, "alice");
        assert_eq!(d.note.as_deref(), Some("legal cleared it"));

        assert_eq!(
            fold.deadline_for(&NodeId("release".into())),
            Some(Some(at(1_000))),
            "FIRST ask wins — a later one must not push the deadline forward"
        );
        assert_eq!(
            fold.menu_for(&NodeId("release".into())).map(|m| m.len()),
            Some(2),
            "FIRST menu wins — the human was shown THIS menu, not the later one-option ask"
        );
        assert_eq!(
            fold.menu_for(&NodeId("release".into())).unwrap()[0].name,
            "ship"
        );
        // The OUTCOME survives the fold too, not just the name — §5 calls it "as much a
        // part of the offer as the name", because a menu whose options all read as
        // `Complete` would make every rejected gate resume as an approval.
        assert_eq!(
            fold.menu_for(&NodeId("release".into())).unwrap()[1].outcome,
            GateOutcome::Fail
        );
    }

    /// The indefinite gate: `None` is folded as a REAL value, so the node's "have I begun
    /// asking?" question is answered by the KEY, not by the value. Without this the node
    /// re-journals `GateAwaited` on every drive.
    #[test]
    fn a_deadline_less_gate_records_that_it_began_asking() {
        use orchestrator_core::GateOutcome;
        let events = vec![(
            1,
            JournalEvent::GateAwaited {
                node: NodeId("release".into()),
                deadline: None,
                options: vec![gopt("approve", GateOutcome::Complete)],
            },
        )];
        let (fold, _, _) = fold_journal(&events);
        assert_eq!(fold.deadline_for(&NodeId("release".into())), Some(None));
        assert!(fold.menu_for(&NodeId("release".into())).is_some());
    }

    /// The two asymmetries are OPPOSITE and both load-bearing, exactly as s1's and s2's
    /// are: the ANSWER is last-wins (an operator corrects themselves before the run
    /// resumes) and the QUESTION is first-wins (the human was asked THIS question).
    #[test]
    fn agent_answers_are_last_wins_and_the_prompt_is_first_wins() {
        let events = vec![
            (
                1,
                JournalEvent::AgentAwaited {
                    node: NodeId("review".into()),
                    deadline: Some(at(1_000)),
                    prompt: "Original question?".into(),
                },
            ),
            (
                2,
                JournalEvent::AgentAnswered {
                    node: NodeId("review".into()),
                    text: "first answer".into(),
                    actor: "alice".into(),
                },
            ),
            (
                3,
                JournalEvent::AgentAnswered {
                    node: NodeId("review".into()),
                    text: "corrected answer".into(),
                    actor: "alice".into(),
                },
            ),
            (
                4,
                JournalEvent::AgentAwaited {
                    node: NodeId("review".into()),
                    deadline: Some(at(9_999)),
                    prompt: "Rewritten question?".into(),
                },
            ),
        ];
        let (fold, _, _) = fold_journal(&events);

        let a = fold
            .agent_answer_for(&NodeId("review".into()))
            .expect("answered");
        assert_eq!(a.text, "corrected answer", "LAST answer wins");
        assert_eq!(a.actor, "alice");

        assert_eq!(
            fold.prompt_for(&NodeId("review".into())),
            Some("Original question?"),
            "FIRST question wins — the human was asked THIS one"
        );
        assert_eq!(
            fold.deadline_for(&NodeId("review".into())),
            Some(Some(at(1_000))),
            "AgentAwaited folds into the SHARED deadlines map, first-wins"
        );
    }

    /// The INDEFINITE human agent — `AgentBacking::Human { timeout: None }`, which is a
    /// real configuration and not a hypothetical (see `AgentBacking` in
    /// `orchestrator-core::registry`, whose `timeout` is an `Option`). Its `AgentAwaited`
    /// carries `deadline: None`, and that `None` must be folded as a REAL value: the
    /// map's KEY answers "has this node begun asking?", which is a different question
    /// from "by when?".
    ///
    /// Guarding the exact bug s1's whole-slice review already found once on the
    /// `SignalAwaited` arm (I1: the deadline-less arm was an empty `{}`, so the value was
    /// dropped and the key never appeared). `wait_or_expire` keys `NotYetAsking` off the
    /// OUTER `Option` of `deadline_for`, so dropping it would make a deadline-less human
    /// agent look like it had never asked on EVERY drive: it would re-journal
    /// `AgentAwaited` each time, and a re-ask is not human-bounded. s2 shipped the same
    /// guard for `GateAwaited` (`a_deadline_less_gate_records_that_it_began_asking`); this
    /// is its s3 twin, and it reddens under `if deadline.is_some() { … }` on the
    /// `AgentAwaited` fold arm — a mutation the rest of the workspace does not detect.
    #[test]
    fn a_deadline_less_human_agent_records_that_it_began_asking() {
        let events = vec![(
            1,
            JournalEvent::AgentAwaited {
                node: NodeId("review".into()),
                deadline: None,
                prompt: "Ship it?".into(),
            },
        )];
        let (fold, _, _) = fold_journal(&events);

        assert_eq!(
            fold.deadline_for(&NodeId("review".into())),
            Some(None),
            "key PRESENT with value None — began asking, with no deadline"
        );
        assert_eq!(
            fold.prompt_for(&NodeId("review".into())),
            Some("Ship it?"),
            "the question is durable even when the SLA is not"
        );
    }

    fn lopt(name: &str, stops: bool) -> orchestrator_core::LoopGateOption {
        orchestrator_core::LoopGateOption {
            name: name.to_string(),
            stops,
        }
    }

    /// The DEADLINE, the prompt and the menu are FIRST-wins: a second ask must not
    /// retroactively change what a human's answer meant, nor when it is due. The decision
    /// is LAST-wins: an operator may correct it before resume.
    ///
    /// The s4 twin of `gate_decisions_are_last_wins_and_the_menu_is_first_wins` (s2) and
    /// `agent_answers_are_last_wins_and_the_prompt_is_first_wins` (s3), and it asserts the
    /// same three things they do for the same reasons — including the deadline, which the
    /// first version of this test dropped. `Fold::deadlines` is SHARED, `wait_or_expire`
    /// reads it without knowing which kind wrote it, and a LAST-wins deadline is the
    /// never-expires bug: a run force-woken every ten minutes under a one-hour SLA
    /// re-arms its deadline on every drive and never fires it.
    #[test]
    fn the_loop_gate_fold_is_first_wins_for_the_menu_and_last_wins_for_the_decision() {
        let node = NodeId("lp/0/__gate__".into());
        let events = vec![
            (
                1,
                JournalEvent::LoopGateAwaited {
                    node: node.clone(),
                    deadline: Some(at(1_000)),
                    prompt: "first question".into(),
                    menu: vec![lopt("done", true), lopt("again", false)],
                },
            ),
            (
                2,
                JournalEvent::LoopGateAwaited {
                    node: node.clone(),
                    deadline: Some(at(9_999)),
                    prompt: "second question".into(),
                    menu: vec![lopt("done", false)],
                },
            ),
            (
                3,
                JournalEvent::LoopGateDecided {
                    node: node.clone(),
                    option: "again".into(),
                    actor: "a".into(),
                },
            ),
            (
                4,
                JournalEvent::LoopGateDecided {
                    node: node.clone(),
                    option: "done".into(),
                    actor: "b".into(),
                },
            ),
        ];
        let (fold, _, _) = fold_journal(&events);

        assert_eq!(
            fold.deadline_for(&node),
            Some(Some(at(1_000))),
            "FIRST ask wins — a later one must not push the deadline forward; \
             overwriting it IS the never-expires bug"
        );

        let menu = fold.loop_gate_menu_for(&node).expect("menu folded");
        assert_eq!(
            menu.len(),
            2,
            "FIRST menu wins — the human was shown THIS menu, not the later one-option ask"
        );
        assert_eq!(
            menu[0].name, "done",
            "the option NAME survives the fold, in order: it is the side of the match \
             Task 6 checks a decision against"
        );
        assert!(
            menu[0].stops,
            "FIRST menu wins: the second ask must not flip `stops`"
        );
        assert_eq!(
            menu[1].name, "again",
            "the whole menu is folded, not just its head"
        );
        // `stops` is as much a part of the offer as the name — s2's twin makes the same
        // point about `GateOutcome`. A menu whose options all read the same way would make
        // every "keep going" answer converge the loop, or every "we're done" answer spin it.
        assert!(!menu[1].stops, "per-option `stops`, not a single flag");

        assert_eq!(
            fold.loop_gate_prompt_for(&node).expect("prompt folded"),
            "first question",
            "FIRST prompt wins"
        );
        let decision = fold.loop_gate_decision_for(&node).expect("decision folded");
        assert_eq!(decision.actor, "b", "LAST decision wins");
        // The OPTION as well as the actor: it is what Task 6 matches against the journaled
        // menu, so a decision that folds without its name is a decision no arm can honour.
        // The two decisions name DIFFERENT options on purpose — with one option this
        // assertion would hold even if the fold dropped the name and kept the first.
        assert_eq!(
            decision.option, "done",
            "LAST decision's option, not the first"
        );
    }

    /// The fold copies `actor` VERBATIM and never launders a degenerate one.
    ///
    /// This replaces a test that pinned the `Option`-shaped distinction between "nobody
    /// said who" (`None`) and "somebody claimed to be the empty string" (`Some("")`).
    /// That premise is gone: `LoopGateDecided.actor` is now a required `String`, because
    /// a loop-gate decision is an approval — answering `continue` authorizes another
    /// iteration of spend — and an approval always records who claimed to give it. There
    /// is no unattributed state left to distinguish.
    ///
    /// What survives is worth keeping, because the narrowing did not make it automatic.
    /// `""` is still expressible, and a plausible-looking "helpful" fold — `if
    /// actor.is_empty() { "unknown".into() }` — would mirror what torii's
    /// `cmd::gate::actor_or` legitimately does at the WRITE side. Doing it HERE instead
    /// is a laundering bug: it makes a journal row that literally reads `""` display as
    /// `unknown`, which is precisely what a row written THROUGH `actor_or` (an operator
    /// whose `$USER` was unresolvable) also displays as. Two different failures — the CLI
    /// was bypassed, versus the CLI could not name the operator — would become one
    /// indistinguishable audit line, and the fold is where a run's history stops being
    /// re-derivable from the journal.
    #[test]
    fn a_loop_gate_decisions_actor_folds_verbatim_including_an_empty_one() {
        let claimed_empty = NodeId("lp/0/__gate__".into());
        let named = NodeId("lp/1/__gate__".into());
        let (fold, _, _) = fold_journal(&[
            (
                1,
                JournalEvent::LoopGateDecided {
                    node: claimed_empty.clone(),
                    option: "done".into(),
                    actor: String::new(),
                },
            ),
            (
                2,
                JournalEvent::LoopGateDecided {
                    node: named.clone(),
                    option: "done".into(),
                    actor: "unknown".into(),
                },
            ),
        ]);

        assert_eq!(
            fold.loop_gate_decision_for(&claimed_empty)
                .expect("decided")
                .actor,
            "",
            "an empty actor folds as the empty string — never re-labelled `unknown`, \
             which is what a WRITER that routed through `actor_or` would have stored"
        );
        // The sibling catches a DIFFERENT mutation, NOT the laundering one: the assertion
        // above already catches that unaided — `if actor.is_empty() { "unknown".into() }`
        // in the fold arm reddens it on ITS line, mutation-proven, and execution never
        // reaches this assertion at all.
        //
        // What the assertion above cannot see is a fold that BLANKS the actor regardless
        // of what was appended, because its expected value IS `""`, so such a fold agrees
        // with it; only a node whose actor is non-empty notices. Mutation: `actor:
        // String::new()` in the fold arm reddens HERE and leaves the one above green.
        //
        // The non-empty value is `"unknown"` rather than an arbitrary name because it is
        // also the exact string the laundering bug would invent, so the pair doubles as
        // the audit distinction — bypassed CLI versus unnameable operator — held apart.
        assert_eq!(
            fold.loop_gate_decision_for(&named).expect("decided").actor,
            "unknown",
            "…and a genuinely-`unknown` actor is still stored as written, so the two \
             stay distinguishable from each other"
        );
    }

    /// `LoopGateAwaited` is the FOURTH writer of the SHARED `deadlines` map, so
    /// "has this node begun asking?" still has one answer for every waiting kind. The
    /// `None` is folded THROUGH — dropping it is the re-ask-every-drive bug s1 shipped.
    ///
    /// It also writes ONLY its own kind-specific record. That is the other half of the
    /// four-writer bookkeeping and it is load-bearing in both directions: the shared map
    /// must see this ask, and no other kind's per-kind record may. If a loop gate leaked
    /// into `agent_prompts`, `run_human_agent`'s missing-question arm — which exists
    /// precisely to fail loud when a node bears ANOTHER kind's awaited record — would
    /// instead resume a human-backed `Agent` with a loop gate's question and let
    /// `AgentAnswered` complete it.
    #[test]
    fn a_deadline_less_loop_gate_records_that_it_began_asking() {
        let node = NodeId("lp/0/__gate__".into());
        let (fold, _, _) = fold_journal(&[(
            1,
            JournalEvent::LoopGateAwaited {
                node: node.clone(),
                deadline: None,
                prompt: "q".into(),
                menu: vec![lopt("done", true)],
            },
        )]);
        assert_eq!(
            fold.deadline_for(&node),
            Some(None),
            "the key must be PRESENT with a None value: present = began asking, \
             None = no deadline"
        );
        assert_eq!(
            fold.loop_gate_prompt_for(&node),
            Some("q"),
            "the question is durable even when the SLA is not"
        );
        assert!(
            fold.loop_gate_menu_for(&node).is_some(),
            "and so is the menu it was asked with"
        );

        // The SHARED map, and nothing else. One negative assertion per sibling kind.
        assert!(
            fold.prompt_for(&node).is_none(),
            "a loop gate is not answerable by `AgentAnswered`, so its prompt must not \
             land in `agent_prompts`"
        );
        assert!(
            fold.menu_for(&node).is_none(),
            "nor its menu in the `HumanGate` menu map — the two vocabularies differ \
             (`stops` vs `GateOutcome`)"
        );
        assert!(
            !fold.has_signal_ask(&node),
            "nor may it claim to be an `AwaitSignal` ask"
        );
    }

    #[test]
    fn classify_gateway_error_pauses_only_on_timed_allgated() {
        use kernel::types::error::{GatewayError, HumanAction};
        let t = chrono::DateTime::from_timestamp(1_000_000_000, 0).unwrap();
        // Timed AllGated → Pause (reason names the instant).
        match classify_gateway_error(&GatewayError::AllGated {
            resume_after: Some(t),
            skipped: vec![],
            human_action: None,
        }) {
            GatewayDisposition::Pause {
                resume_after,
                reason,
            } => {
                assert_eq!(resume_after, t);
                assert!(reason.contains(&t.to_string()), "reason names t: {reason}");
            }
            d => panic!("expected Pause, got {d:?}"),
        }
        // Terminal AllGated → Fail (message carries the human-action hint).
        let none = GatewayError::AllGated {
            resume_after: None,
            skipped: vec![],
            human_action: Some(HumanAction::TopUpCredits),
        };
        let none_msg = none.to_string();
        assert!(
            matches!(classify_gateway_error(&none), GatewayDisposition::Fail(m) if m == none_msg)
        );
        // Other errors → Fail.
        let budget = GatewayError::BudgetExceeded {
            estimated: 1.0,
            remaining: 0.0,
        };
        assert!(matches!(
            classify_gateway_error(&budget),
            GatewayDisposition::Fail(_)
        ));
    }
}
