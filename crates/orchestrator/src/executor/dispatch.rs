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
use std::sync::atomic::{AtomicU64, Ordering};

use super::Executor;

/// One drive's token ledger, as the chokepoint sees it: the spend already **journaled**
/// (folded by effect id, so idempotent across resumes) plus the spend **this drive** has
/// added since, and the run's cap.
///
/// A borrowed live view rather than a pair of `u64`/`Option<u64>` scalars, and that is
/// load-bearing. `Fold` is built ONCE per drive and handed to every node as `&Fold`, so a
/// `spent: u64` snapshot freezes at the value the drive STARTED with: node 2 of a graph
/// would gate against node 1's pre-call ledger, node 3 against the same, and a freshly
/// submitted run — whose journaled spend is 0 by definition — would never gate at all,
/// spending its entire reachable graph no matter how small the cap. Threading the live
/// counter instead is what makes the design's "overshoot is bounded by ONE call" (spec
/// §6.5) actually true rather than "bounded by everything one drive can reach".
///
/// The counter is `Relaxed`-atomic because a `Map` fan-out dispatches its children
/// concurrently over this same shared view. Relaxed is sufficient: the gate is a spend
/// backstop, not a synchronization primitive, and the only cost of a racing read is that
/// several concurrent children can pass the gate together — which §6.3a already accepts
/// as inherent to fan-out.
pub(super) struct Meter<'a> {
    /// Folded from the journal by effect id — see `Fold::spent`.
    journaled: u64,
    budget: Option<u64>,
    /// Tokens dispatched by this drive and not yet re-folded from the journal. Reset to
    /// zero by construction on the next drive, whose `journaled` base then includes them.
    live: &'a AtomicU64,
}

impl<'a> Meter<'a> {
    pub(super) fn new(journaled: u64, budget: Option<u64>, live: &'a AtomicU64) -> Self {
        Self {
            journaled,
            budget,
            live,
        }
    }

    /// Everything this run has spent: journaled + in-flight.
    pub(super) fn spent(&self) -> u64 {
        self.journaled
            .saturating_add(self.live.load(Ordering::Relaxed))
    }

    pub(super) fn budget(&self) -> Option<u64> {
        self.budget
    }

    /// Add a completed call's tokens to the in-drive tally.
    ///
    /// Saturating, for the same reason `Fold::spent` saturates: overflowing `u64` from
    /// summed `u32` token counts is unreachable, but saturating HIGH pauses the run while
    /// a wrapping `fetch_add` would reset the ledger near zero and let it spend unbounded.
    fn record(&self, tokens: u64) {
        let _ = self
            .live
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |t| {
                Some(t.saturating_add(tokens))
            });
    }
}

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
    /// Gate on the ledger, dispatch, then charge the call back to the ledger.
    ///
    /// `meter` is the run's live spend view: its journaled base is folded by effect id
    /// (correct across any number of resumes and across a process boundary) and its
    /// in-drive tally is updated HERE, at the same single chokepoint that reads it — so
    /// a producer can neither bypass the gate nor forget to account for what it spent.
    ///
    /// The check is `spent >= cap` BEFORE the call, which makes the budget a
    /// FLOOR-TRIGGER rather than a ceiling: output tokens are unknowable until the
    /// call returns, so a cap can be overshot by at most one call. `budget: None`
    /// (every pre-SP-DATA-5 run) never gates — the additivity guarantee.
    ///
    /// Tokens are charged on a successful RESPONSE rather than after the caller
    /// journals its `EffectRecorded`: the provider has been paid either way, so a
    /// journal append that fails afterwards must not also lose the accounting.
    pub(super) async fn dispatch_metered(
        &self,
        request: &InferenceRequest,
        meter: &Meter<'_>,
    ) -> Result<Result<InferenceResponse, Refusal>, GatewayError> {
        let spent = meter.spent();
        if let Some(cap) = meter.budget()
            && spent >= cap
        {
            return Ok(Err(Refusal::BudgetExhausted { spent, budget: cap }));
        }
        let response = self.gateway.execute(request).await?;
        let Some(usage) = &response.usage else {
            if meter.budget().is_some() {
                return Ok(Err(Refusal::Unmetered {
                    model: response
                        .model
                        .clone()
                        .unwrap_or_else(|| "<unknown>".to_string()),
                }));
            }
            // Unbudgeted and unmetered: nothing to charge, nothing to gate.
            return Ok(Ok(response));
        };
        meter.record(u64::from(usage.total_tokens));
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
