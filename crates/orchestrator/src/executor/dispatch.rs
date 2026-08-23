//! SP-DATA-5: the single metered-dispatch chokepoint.
//!
//! Every model call in the executor routes through [`Executor::dispatch_metered`] so
//! the budget gate cannot be bypassed by a new producer. This mirrors the
//! `model_output` chokepoint on the OUTPUT side and exists for the same reason:
//! SP-4 s2's review found the secret redactor wired into only 1 of the 4 producers.
//! Here the failure would be worse — an ungated path spends real tokens past the
//! operator's cap, silently.
//!
//! The four producers are the ReAct turn (`agent.rs`), the `ModelCall` node
//! (`mod.rs`), a Map item and the `Consolidate` synthesis (`fanout.rs`). Their
//! GATEWAY-ERROR handling deliberately differs (two classify the error into
//! pause-vs-fail, two just stringify it), so the chokepoint returns
//! `Result<Result<_, Refusal>, GatewayError>`: every site's existing `Err(error)` arm
//! is untouched and only a new `Ok(Err(refusal))` arm is added. Their REFUSAL
//! handling, by contrast, must be identical — so [`Executor::record_refusal`] owns
//! the journaling and the wording, and each site only maps the returned
//! [`RefusalKind`] onto its own local return idiom (`NodeExec`, `ToolOutcome`, or a
//! Map child's `MapChildPaused`).

use gateway::GatewayError;
use kernel::types::request::{InferenceRequest, InferenceResponse};
use orchestrator_core::{JournalEvent, NodeId, OrchestratorError, RunId};

use super::Executor;

/// Why a metered dispatch refused to run. Constructed ONLY by
/// [`Executor::dispatch_metered`] and consumed ONLY by
/// [`Executor::record_refusal`] — a producer never invents or interprets one.
pub(super) enum Refusal {
    /// The run has already spent its budget. Carries `(spent, budget)` for the
    /// operator-facing message.
    BudgetExhausted { spent: u64, budget: u64 },
    /// A budget is set but the provider reported no usage, so this call's spend would
    /// be invisible to the ledger. Fail closed: a budget you cannot measure is not a
    /// budget. (SP-DATA-5 Task 4 owns capturing usage and the tests for this arm;
    /// today it is unreachable in practice because nothing sets a budget until
    /// Task 5 wires `--budget-tokens`.)
    Unmetered { model: String },
}

/// What a journaled refusal means for the producer that hit it: a durable pause the
/// run stays resumable from, or a node failure. The distinction lives here rather
/// than at each site so all four producers cannot drift apart on it.
pub(super) enum RefusalKind {
    Paused(String),
    Failed(String),
}

impl Executor {
    /// Gate on the folded spend, then dispatch. `spent`/`budget` come from the run's
    /// journal fold, so they are correct across any number of resumes and across a
    /// process boundary.
    ///
    /// The check is `spent >= cap` BEFORE the call, which makes the budget a
    /// FLOOR-TRIGGER rather than a ceiling: output tokens are unknowable until the
    /// call returns, so a cap can be overshot by at most one call. `budget: None`
    /// (every pre-SP-DATA-5 run) never gates — the additivity guarantee.
    pub(super) async fn dispatch_metered(
        &self,
        request: &InferenceRequest,
        spent: u64,
        budget: Option<u64>,
    ) -> Result<Result<InferenceResponse, Refusal>, GatewayError> {
        if let Some(cap) = budget
            && spent >= cap
        {
            return Ok(Err(Refusal::BudgetExhausted { spent, budget: cap }));
        }
        let response = self.gateway.execute(request).await?;
        if budget.is_some() && response.usage.is_none() {
            return Ok(Err(Refusal::Unmetered {
                model: response
                    .model
                    .clone()
                    .unwrap_or_else(|| "<unknown>".to_string()),
            }));
        }
        Ok(Ok(response))
    }

    /// Journal a [`Refusal`] and report what it means for the caller. One place owns
    /// both the durable record and the operator-facing wording, so the four producers
    /// pause/fail identically on the same cause.
    pub(super) async fn record_refusal(
        &self,
        run: RunId,
        node: &NodeId,
        refusal: Refusal,
    ) -> Result<RefusalKind, OrchestratorError> {
        match refusal {
            Refusal::BudgetExhausted { spent, budget } => {
                let reason = format!(
                    "budget: {spent} of {budget} tokens spent; raise it with `torii run wake --budget-tokens N`"
                );
                // `resume_after: None` is the HOTL pause class (SP-DATA-3): the
                // scheduler records a NULL `next_wake` and never auto-wakes the run.
                // Correct here — no amount of waiting refills a budget, only an
                // operator raising the cap (`BudgetRaised` then `force_wake`) does.
                self.append(
                    run,
                    JournalEvent::RunPaused {
                        reason: reason.clone(),
                        resume_after: None,
                    },
                )
                .await?;
                Ok(RefusalKind::Paused(reason))
            }
            Refusal::Unmetered { model } => {
                // A node failure, not a pause: retrying the same unmetered provider
                // would refuse again, so there is nothing for an operator to unblock.
                let error = format!(
                    "unmetered model call: '{model}' reported no token usage while a budget is set; refusing to spend unmeasured"
                );
                self.append(
                    run,
                    JournalEvent::NodeFailed {
                        node: node.clone(),
                        error: error.clone(),
                    },
                )
                .await?;
                Ok(RefusalKind::Failed(error))
            }
        }
    }
}
