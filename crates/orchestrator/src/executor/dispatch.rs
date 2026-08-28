//! SP-DATA-5: the single metered-dispatch chokepoint.
//!
//! Every model call in the executor routes through [`Executor::dispatch_metered`] so
//! the budget gate cannot be bypassed by a new producer. This mirrors the
//! `model_output` chokepoint on the OUTPUT side and exists for the same reason:
//! SP-4 s2's review found the secret redactor wired into only 1 of the 4 producers.
//! Here the failure would be worse — an ungated path spends real tokens past the
//! operator's cap, silently.
//!
//! The five producers are the ReAct turn (`agent.rs`), the `ModelCall` node
//! (`mod.rs`), a Map item and the `Consolidate` synthesis (`fanout.rs`), and the
//! planner selector (`selector.rs`, reaching this chokepoint through
//! [`SelectorDispatch`]). The selector was the miss this module's own doc warned
//! about: it shipped holding its own `Arc<Gateway>` and spent past the cap with no
//! ledger entry, so it is now handed a borrowed capability instead of owning one.
//!
//! Their GATEWAY-ERROR handling deliberately differs (two classify the error into
//! pause-vs-fail, the rest just stringify it), so the chokepoint returns
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
/// # A budgeted run serialises its model calls; an unbudgeted one does not
///
/// The counter alone is not enough, and the whole-slice review proved it: a `Map`
/// polls all its children under ONE `join_all`, so every child read the ledger before
/// any sibling's response returned and a 6-item Map under a 100-token cap spent 900
/// tokens and completed. That is a deterministic check-then-act, not a memory-ordering
/// race, so no atomic ordering fixes it. Re-checking inside the fan-out semaphore does
/// not either — those permits ARE the concurrency, so N holders still check together
/// against an unchanged ledger. A reservation would need an output-token estimate that
/// is unknowable before the call (§8).
///
/// So a run WITH a budget takes [`gate`](Meter::gate) — a 1-permit `tokio::sync::Mutex`
/// held across the whole check → dispatch → charge sequence — and therefore has at most
/// one model call in flight at a time. That is what makes §6.5's "overshoot bounded by
/// at most one call" true under fan-out, with no estimation.
///
/// **The trade, stated plainly: a budgeted run gives up fan-out throughput for an exact
/// cap.** A 6-wide `Map` under a budget dispatches its children one after another. That
/// is the price of a cap that holds; a run that does not want it simply does not set a
/// budget.
///
/// An UNBUDGETED run takes no lock at all and keeps full concurrency — the additivity
/// guarantee the pre-SP-DATA-5 suite depends on.
///
/// The counter stays `Relaxed`-atomic: under a budget the mutex already orders every
/// read and write of it, and without one the counter is never gated on.
pub(super) struct Meter<'a> {
    /// Folded from the journal by effect id — see `Fold::spent`.
    journaled: u64,
    budget: Option<u64>,
    /// Tokens dispatched by this drive and not yet re-folded from the journal. Reset to
    /// zero by construction on the next drive, whose `journaled` base then includes them.
    live: &'a AtomicU64,
    /// The per-run, per-drive serialisation gate. Acquired only when `budget` is `Some`.
    gate: &'a tokio::sync::Mutex<()>,
}

impl<'a> Meter<'a> {
    pub(super) fn new(
        journaled: u64,
        budget: Option<u64>,
        live: &'a AtomicU64,
        gate: &'a tokio::sync::Mutex<()>,
    ) -> Self {
        Self {
            journaled,
            budget,
            live,
            gate,
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

    /// The run's serialisation gate — held by [`Executor::dispatch_metered`] across
    /// check→dispatch→charge, and ONLY when a budget is set.
    fn gate(&self) -> &'a tokio::sync::Mutex<()> {
        self.gate
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
/// than at each site so all five producers cannot drift apart on it.
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
    ///
    /// A BUDGETED run holds the meter's 1-permit gate across this entire body, so the
    /// check and the charge are atomic with respect to every other model call in the
    /// run and a concurrent `Map` fan-out cannot walk the gate en masse (see
    /// [`Meter`]). An unbudgeted run takes no lock. The lock is held across the
    /// provider `.await` — deliberately, that is the whole point — so it must be the
    /// async `tokio::sync::Mutex`, and nothing inside the critical section may
    /// re-enter `dispatch_metered`: the only await here is the gateway call, which
    /// never drives executor nodes, so a nested `Subgraph`/`Loop`/`Map` child (each of
    /// which reaches this function only from its OWN task, never from inside another's
    /// critical section) blocks on the gate and is woken when the holder releases.
    pub(super) async fn dispatch_metered(
        &self,
        request: &InferenceRequest,
        meter: &Meter<'_>,
    ) -> Result<Result<InferenceResponse, Refusal>, GatewayError> {
        // Bound to a named local: it must live to the end of the function, and only a
        // budgeted run acquires it at all.
        let _serialised = match meter.budget() {
            Some(_) => Some(meter.gate().lock().await),
            None => None,
        };
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

/// The executor's [`ModelDispatch`]: the only provider access a
/// [`PlannerSelector`](orchestrator_core::PlannerSelector) gets.
///
/// Binds one selector call to one run + Expand node so it gates on the run's budget,
/// charges the live meter and journals its spend exactly like the other four producers.
/// The selector supplies only the prompts; it cannot widen the capability, pick a
/// different provider, or skip the gate.
///
/// The selector's spend is journaled under the reserved
/// [`RESERVED_SELECT_ID`](orchestrator_core::RESERVED_SELECT_ID) path rather than the
/// Expand node's own id, so it lands on the durable ledger without colliding with the
/// node's effects. That matters beyond tidiness: `PlannerSelected` memoizes the CHOICE,
/// so a resumed run never re-invokes the selector — without a journaled
/// `EffectRecorded` the tokens it really spent would vanish from the fold on every
/// subsequent resume and the run would drift permanently under its true spend.
pub(super) struct SelectorDispatch<'a> {
    exec: &'a Executor,
    run: RunId,
    /// The Expand node. The journaled effect hangs off `"{node}/__select__"`.
    node: NodeId,
    fold: &'a super::Fold,
    /// A refusal this dispatch already journaled. `ModelDispatch::complete` can only
    /// return `OrchestratorError`, which cannot carry the pause-vs-fail distinction, so
    /// the caller reads it back here rather than re-deriving it from a message.
    refusal: std::sync::Mutex<Option<RefusalKind>>,
}

impl<'a> SelectorDispatch<'a> {
    pub(super) fn new(exec: &'a Executor, run: RunId, node: NodeId, fold: &'a super::Fold) -> Self {
        Self {
            exec,
            run,
            node,
            fold,
            refusal: std::sync::Mutex::new(None),
        }
    }

    /// Take the refusal this dispatch journaled, if any. `None` means the selector's
    /// `Err` was its own (a bad pick, an empty response), not the budget gate's.
    pub(super) fn take_refusal(&self) -> Option<RefusalKind> {
        self.refusal.lock().expect("selector refusal lock").take()
    }

    /// `"{node}/__select__"` — the reserved path this call's spend is journaled under.
    fn select_path(&self) -> String {
        format!("{}/{}", self.node.0, orchestrator_core::RESERVED_SELECT_ID)
    }
}

#[async_trait::async_trait]
impl orchestrator_core::ModelDispatch for SelectorDispatch<'_> {
    async fn complete(
        &self,
        system: &str,
        user: &str,
        chain: Option<&str>,
    ) -> Result<String, OrchestratorError> {
        let chain = chain.unwrap_or_default();
        // Built here rather than via `build_request`, which hardcodes `system: None`;
        // the selector's instruction ("answer with ONLY the exact agent name") is a
        // system prompt and dropping it would change what the model returns.
        let request = InferenceRequest {
            capability: kernel::types::capability::Capability::TextChat,
            model: None,
            router: None,
            chain: Some(chain.to_string()),
            payload: kernel::types::request::Payload::Chat {
                messages: vec![kernel::types::request::Message::text(
                    kernel::types::request::MessageRole::User,
                    user,
                )],
                system: Some(system.to_string()),
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
        };

        let response = match self
            .exec
            .dispatch_metered(&request, &self.fold.meter())
            .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(refusal)) => {
                // Already journaled by `record_refusal`; stash the kind for the caller
                // and surface an `Err` so the selector cannot proceed to a fallback.
                let kind = self
                    .exec
                    .record_refusal(self.run, &self.node, refusal)
                    .await?;
                let message = match &kind {
                    RefusalKind::Paused(reason) => reason.clone(),
                    RefusalKind::Failed(error) => error.clone(),
                };
                *self.refusal.lock().expect("selector refusal lock") = Some(kind);
                return Err(OrchestratorError::Gateway(message));
            }
            Err(error) => return Err(OrchestratorError::Gateway(error.to_string())),
        };

        // SP-4 s2: the same redaction chokepoint every other producer's output crosses.
        let output = self.exec.model_output(&response);
        let text = output
            .get("text")
            .and_then(|t| t.as_str())
            .unwrap_or_default()
            .to_string();

        let path = self.select_path();
        let recorded = self.exec.split_output(&output).await?;
        self.exec
            .append(
                self.run,
                JournalEvent::EffectRecorded {
                    node: NodeId(path.clone()),
                    effect_id: orchestrator_core::effect_id(&path, 0, 0),
                    class: orchestrator_core::EffectClass::Pure,
                    input_hash: super::support::input_hash(
                        chain,
                        &serde_json::json!({ "system": system, "user": user }),
                    )
                    .map_err(|e| OrchestratorError::Gateway(e.to_string()))?,
                    seq: 0,
                    output: recorded,
                    observation: None,
                    usage: response.usage.map(super::content::convert_usage),
                },
            )
            .await?;
        Ok(text)
    }
}
