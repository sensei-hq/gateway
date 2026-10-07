//! AG-15 (#90): the engine-enforced half of an agent's tool policy — CONFIRM-BEFORE-RUN.
//!
//! A tool named in [`AgentDefinition::confirm_tools`](orchestrator_core::AgentDefinition)
//! does not run on the model's say-so: each call pauses the run on a human decision first,
//! and runs only if approved. The per-tool call CEILING lives in `agent.rs` beside the s1
//! permission gate, because it is a pure function of the transcript and needs no journal
//! events of its own.
//!
//! **The waiting is the HITL machinery's, re-keyed.** The node-keyed helpers in `signal.rs`
//! (`wait_or_expire`, `pause_awaiting`) answer "has this NODE begun asking?" — but an agent
//! node can ask about many calls over its life, so a confirmation is keyed by the CALL's
//! effect id instead (`ToolConfirmAwaited`/`ToolConfirmDecided`). The rules are the gates'
//! verbatim:
//!
//! - **the ask precedes the answer** — `ToolConfirmAwaited` is journaled before any decision
//!   is read, so an early decision is honoured in the same drive and there is never a
//!   decision with no recorded question;
//! - **the deadline is ABSOLUTE and FIRST-wins** — recorded once, never `now + timeout`
//!   again on a resume (the never-expires bug);
//! - **the deadline is checked BEFORE the decision is read** — `run_human_gate`'s ordering,
//!   so an approval landing after the SLA never runs the tool;
//! - **the pause is durable** — `RunPaused { resume_after: deadline }`, so the scheduler
//!   wakes the run to expire it, and a resume re-derives everything from the journal.
//!
//! What DIFFERS from a gate, deliberately: a rejection or an expiry does not fail the node.
//! It is fed back to the model as a terse `not_confirmed` refusal — the shape an ungranted
//! tool already gets — recorded as a Pure effect so a resume replays it. A gate is a node of
//! its own whose whole output is the decision; a confirmation is one step of a model's
//! work, and refusing that step is the fail-closed outcome that keeps the rest of it.

use kernel::types::request::ToolCall;
use orchestrator_core::{
    EffectId, JournalEvent, MAX_HUMAN_TEXT_BYTES, NodeId, OrchestratorError, RunId,
};

use super::{Executor, Fold};

/// The outcome of putting one tool call to a human.
pub(super) enum Confirmation {
    /// Approved inside its deadline — run the tool.
    Approved,
    /// Rejected, expired, or not askable — refuse it to the model with this terse detail.
    Refused(String),
    /// Asked and not yet answered — the run is paused (`RunPaused` already journaled).
    Paused(String),
}

impl Executor {
    /// Put one confirm-before-run tool call to a human, or read back the answer.
    ///
    /// Runs on the LIVE path only, after the s1 permission gate and the workspace jail: a
    /// memo hit has already replayed the call's recorded outcome before this is reached,
    /// and a person is never asked to approve a call the agent is not permitted to make.
    ///
    /// The detail strings handed back for a refusal are model-facing and TERSE: they name
    /// the tool and nothing else — not the operator's note, not who decided, not the
    /// deadline — for the same confused-deputy reason the s1 denial never echoes a grant.
    // The per-call inputs are each distinct; bundling them would only relocate the plumbing.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn confirm_tool_call(
        &self,
        run: RunId,
        node: &NodeId,
        fold: &Fold,
        teid: &EffectId,
        call: &ToolCall,
        tih: &str,
        timeout: Option<chrono::Duration>,
    ) -> Result<Confirmation, OrchestratorError> {
        let refused = || {
            Confirmation::Refused(format!(
                "the call to tool '{}' was not confirmed by a human",
                call.name
            ))
        };

        // 1. The ask, before any answer is read. Only the FIRST drive journals it; every
        //    later one reads the deadline back, so it never moves.
        let deadline = match fold.tool_confirm_deadline(teid) {
            Some(recorded) => recorded,
            None => {
                // What the human approves, redacted BEFORE it reaches the durable row and
                // measured AFTER redaction (`[REDACTED]` can be longer than what it
                // replaces). Over the bound it is refused, never truncated: approving a call
                // whose arguments were cut is approving something nobody saw.
                let arguments = self.redact_text(call.arguments.clone());
                if arguments.len() > MAX_HUMAN_TEXT_BYTES {
                    tracing::warn!(
                        node = %node.0,
                        tool = %call.name,
                        bytes = arguments.len(),
                        "AG-15: tool call arguments too large to confirm; refused"
                    );
                    return Ok(refused());
                }
                // `confirm_timeout` is bounded at load (`MAX_AWAIT_SIGNAL_TIMEOUT`), so an
                // overflow here needs a clock near the end of time; it still fails CLOSED
                // (refused, never run) rather than panicking inside a node.
                let fresh = match timeout {
                    None => None,
                    Some(t) => match self.clock.now().checked_add_signed(t) {
                        Some(d) => Some(d),
                        None => return Ok(refused()),
                    },
                };
                self.append(
                    run,
                    JournalEvent::ToolConfirmAwaited {
                        node: node.clone(),
                        effect_id: teid.clone(),
                        tool: call.name.clone(),
                        arguments,
                        args_hash: tih.to_string(),
                        deadline: fresh,
                    },
                )
                .await?;
                fresh
            }
        };

        // 2. The deadline BEFORE the decision — a late approval runs nothing.
        if let Some(d) = deadline
            && self.clock.now() >= d
        {
            return Ok(refused());
        }

        // 3. The decision, against the ask just published or read back.
        match fold.tool_confirm_decision(teid) {
            Some(true) => Ok(Confirmation::Approved),
            Some(false) => Ok(refused()),
            None => {
                let reason = format!(
                    "tool_confirm: waiting for a human to confirm tool '{}' on node {} \
                     (call {}){}",
                    call.name,
                    node.0,
                    teid.0,
                    deadline
                        .map(|d| format!(" (deadline {d})"))
                        .unwrap_or_default()
                );
                self.append(
                    run,
                    JournalEvent::RunPaused {
                        reason: reason.clone(),
                        resume_after: deadline,
                    },
                )
                .await?;
                Ok(Confirmation::Paused(reason))
            }
        }
    }
}
