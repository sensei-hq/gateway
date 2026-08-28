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

use super::{Fold, GateDecision};

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
            }
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
