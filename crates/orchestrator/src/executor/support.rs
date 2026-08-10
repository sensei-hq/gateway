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

use super::Fold;

/// One scheduling round's **ready set** (§3.2): the not-yet-terminal nodes whose
/// `Hard` deps have all completed and `Soft` deps are all terminal, in graph
/// declaration order (deterministic). A linear graph yields exactly one.
pub(crate) fn ready_nodes<'g>(
    graph: &'g Graph,
    completed: &HashSet<NodeId>,
    terminal: &HashSet<NodeId>,
) -> Vec<(usize, &'g Node)> {
    graph
        .nodes
        .iter()
        .enumerate()
        .filter(|(_, node)| !terminal.contains(&node.id))
        .filter(|(_, node)| {
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
            }
            JournalEvent::NodeStarted { node } => {
                fold.started.insert(node.clone());
            }
            JournalEvent::NodeCompleted { node } => {
                fold.completed.insert(node.clone());
                completed.push(node.clone());
            }
            // The intent phase of a two-phase Mutation (§7.3). An effect id in
            // `intents` with no matching `EffectRecorded` is in-doubt on resume.
            JournalEvent::EffectIntent { effect_id, .. } => {
                fold.intents.insert(effect_id.clone());
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
                    if c.status == ChildStatus::Ok
                        && let (Some(digest), Some(input_hash)) = (&c.digest, &c.input_hash)
                    {
                        let child = NodeId(format!("{}/{}", node.0, c.index));
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
pub(crate) fn project_agent_outputs(
    graph: &Graph,
    outputs: &mut HashMap<NodeId, serde_json::Value>,
) {
    for node in &graph.nodes {
        if let NodeKind::Agent { .. } = &node.kind
            && let Some(output) = outputs.get(&node.id).cloned()
        {
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
