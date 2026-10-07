//! AG-15 (#90): ESCALATION of a human-backed `Agent` node whose SLA expires unanswered.
//!
//! Semantics, stated once (the registry doc on `AgentDefinition::escalate_to` and the
//! `AgentEscalated` journal doc point here):
//!
//! - **Same question.** The escalation target is asked the question the node's
//!   `AgentAwaited` journaled — not one recomposed from the target's own `system_prompt`.
//!   The person escalated to sees exactly what the first person saw, and the journal holds
//!   ONE question per node, which is what `torii` renders.
//! - **Its own SLA**, from the escalation instant: the target's `AgentBacking::Human
//!   { timeout }`, recorded ABSOLUTE on `AgentEscalated` and never recomputed (the
//!   never-expires rule). `None` waits on the target indefinitely.
//! - **Chainable.** When the target's SLA passes, its own `escalate_to` (if any) is
//!   followed; load rejects a cycle, and this file refuses one at runtime too (a registry
//!   built with `with_agent` is not validated), so a chain always ends.
//! - **Answered the usual way.** The answer is the node-keyed `AgentAnswered` the node was
//!   always waiting for, read BEFORE any expiry (the human-agent ordering), so escalation
//!   never pre-empts an answer that landed; `actor` records who gave it.
//! - **Loud at the end.** Only the LAST agent in the chain expiring fails the node, and the
//!   failure names the chain it walked. With no `escalate_to` anywhere the failure is the
//!   pre-AG-15 message, byte for byte.
//! - **Top-level `Agent` nodes only.** A `GateSpec::Human` loop gate does not escalate; its
//!   expiry failure says so when its role declares `escalate_to`.
//!
//! Each drive escalates at most ONE hop and then pauses on the new holder's deadline:
//! `now + the target's timeout`, measured from the ESCALATION INSTANT (the drive that
//! appends `AgentEscalated`), not from when the previous holder's SLA passed. A run left
//! undriven past an SLA therefore does not catch up: every hop waits its target's full SLA
//! from whenever it is actually escalated, so walking an n-hop chain costs n full SLAs after
//! the first expiry is noticed (each hop's pause carries that FUTURE `resume_after`, which
//! is what the scheduler wakes it on). One hop per drive keeps every hop its own durable
//! journal row, decided against a fresh clock, rather than a burst decided from one stale
//! fold.

use orchestrator_core::{AgentBacking, AgentRef, JournalEvent, NodeId, OrchestratorError, RunId};

use super::{Executor, Fold, NodeExec};

impl Executor {
    /// The asked agent's SLA (`original` — the node's recorded `AgentAwaited` deadline) has
    /// passed unanswered. Wait on the current escalation target, escalate one more hop, or
    /// fail the node at the end of the chain.
    pub(super) async fn escalate_or_expire(
        &self,
        run: RunId,
        node_id: &NodeId,
        agent_ref: &AgentRef,
        original: chrono::DateTime<chrono::Utc>,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        let hops = fold.escalations_for(node_id);
        // Who holds the question now, and until when.
        let (holder, holder_deadline) = match hops.last() {
            Some(hop) => (hop.to.as_str(), hop.deadline),
            None => (agent_ref.0.as_str(), Some(original)),
        };

        // Still inside the holder's SLA (or the holder waits indefinitely) ⇒ keep waiting on
        // the RECORDED deadline.
        let expired_at = match holder_deadline {
            Some(d) if self.clock.now() >= d => d,
            current => {
                let reason = format!(
                    "human_agent: node {}'s question was escalated to agent {holder}; \
                     waiting for a human answer{}",
                    node_id.0,
                    current
                        .map(|d| format!(" (deadline {d})"))
                        .unwrap_or_default()
                );
                return self.pause_awaiting(run, reason, current).await;
            }
        };

        let walked = || {
            hops.iter()
                .map(|h| h.to.as_str())
                .collect::<Vec<_>>()
                .join(" -> ")
        };

        let next = self
            .registry
            .agent(holder)
            .and_then(|a| a.escalate_to.clone());
        let Some(to) = next else {
            // The end of the chain. With no escalation at all this is the pre-AG-15 message
            // verbatim — behaviour is byte-identical when the feature is unused.
            let message = if hops.is_empty() {
                format!(
                    "human_agent: node {} passed its deadline {expired_at} with no answer",
                    node_id.0
                )
            } else {
                format!(
                    "human_agent: node {} passed its deadline {expired_at} with no answer, \
                     after escalating from {} to {}; agent {holder} declares no further \
                     escalate_to",
                    node_id.0,
                    agent_ref.0,
                    walked()
                )
            };
            return self.fail_human_agent(run, node_id, message).await;
        };

        // A cycle never ends (and each hop is a durable row) — load rejects one, and this
        // refuses one a hand-built registry slipped past `validate`.
        if to == agent_ref.0 || hops.iter().any(|h| h.to == to) {
            return self
                .fail_human_agent(
                    run,
                    node_id,
                    format!(
                        "human_agent: node {} cannot escalate from {holder} to {to}: {to} \
                         already held this question (escalation cycle)",
                        node_id.0
                    ),
                )
                .await;
        }
        let timeout = match self.registry.agent(&to).map(|a| &a.backed_by) {
            Some(AgentBacking::Human { timeout }) => *timeout,
            _ => {
                return self
                    .fail_human_agent(
                        run,
                        node_id,
                        format!(
                            "human_agent: node {} cannot escalate from {holder} to {to}: \
                             {to} is missing or not human-backed",
                            node_id.0
                        ),
                    )
                    .await;
            }
        };
        // Bounded at load (`MAX_AWAIT_SIGNAL_TIMEOUT`); an overflow still fails loudly rather
        // than panicking inside a node, as `wait_or_expire` does.
        let deadline = match timeout {
            None => None,
            Some(t) => match self.clock.now().checked_add_signed(t) {
                Some(d) => Some(d),
                None => {
                    return self
                        .fail_human_agent(
                            run,
                            node_id,
                            format!(
                                "human_agent: node {} cannot escalate to {to}: its timeout \
                                 ({t}) overflows the representable instant range",
                                node_id.0
                            ),
                        )
                        .await;
                }
            },
        };
        self.append(
            run,
            JournalEvent::AgentEscalated {
                node: node_id.clone(),
                from: holder.to_string(),
                to: to.clone(),
                deadline,
            },
        )
        .await?;
        let reason = format!(
            "human_agent: node {}'s question was escalated from agent {holder} (deadline \
             {expired_at} passed) to agent {to}; waiting for a human answer{}",
            node_id.0,
            deadline
                .map(|d| format!(" (deadline {d})"))
                .unwrap_or_default()
        );
        self.pause_awaiting(run, reason, deadline).await
    }
}
