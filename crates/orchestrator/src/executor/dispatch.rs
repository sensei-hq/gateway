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
//! **A new producer owes THREE tests, not one.** The five are a census on both sides,
//! and each side has its own per-producer guard in `tests.rs`: the budget gate
//! (`budget_gate_stops_the_*_producer`), the redaction leaf (`*_text_is_redacted` —
//! the SP-4 s2 review's own remedy), and the memo replay, which is what keeps a
//! re-driven node from re-spending and what keeps the ledger honest across resumes.
//! The selector shipped with the first and neither of the other two, and the review
//! that caught it found exactly the defect the missing memo guard would have.
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
use kernel::types::request::{
    InferenceRequest, InferenceResponse, Message, Payload, ToolDefinition,
};
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

/// The pessimistic input estimate for the budget clamp: the system prompt, every
/// message body, every assistant turn's tool CALLS, and the tool schemas.
///
/// # What is counted, and what is not
///
/// Tool schemas are counted rather than waved off as small for two reasons. They are pure
/// JSON, which is the worst case for a chars-per-token heuristic and the reason
/// [`est_tokens_pessimistic`](crate::agent::prompt::est_tokens_pessimistic) exists at all;
/// and an agent's activated schemas routinely outweigh its prompt. `over_budget` already
/// counts them, for the same reason, and is the reference this mirrors — including its
/// treatment of `description` as optional.
///
/// A message's `tool_calls` are counted for a plainer reason: they are SENT. The ReAct
/// loop appends an assistant turn carrying them on every turn (`agent.rs`) and re-sends
/// the whole transcript on every turn after that; `anthropic/convert.rs` renders each as
/// a `ToolUse` block and `openai_compat/convert.rs` as a `tool_calls` array. Those turns
/// have an EMPTY `content`, so counting only `as_text()` priced a serialized plan or an
/// `fs_write` body — the largest payloads the loop produces — at zero, which under-counts
/// in the one direction this function must not: `allowance = remaining − est`, so an
/// estimate that is too low leaves an allowance that is too high.
///
/// `attachments` are NOT counted, and this is not an oversight to be silent about: the
/// orchestrator never populates them (`agent.rs:469` and this module's own builder both
/// pass `Vec::new()`), so counting them would be dead arithmetic. A producer that starts
/// attaching media owes this function a term, and there is no way for it to find out
/// except by reading this paragraph.
///
/// Deliberately NOT shared with `over_budget`/`est_prompt_tokens`: those answer "will this
/// fit the context window", which wants to avoid false alarms and so wants the opposite
/// bias. One function cannot serve both, and merging them would silently change a
/// window-fit behaviour this slice has no business touching. `over_budget` has the same
/// `tool_calls` gap and keeps it: an under-count there is the SAFE direction (it lets a
/// turn through that might not fit) where here it is the expensive one.
fn est_input_tokens(system: Option<&str>, messages: &[Message], tools: &[ToolDefinition]) -> u64 {
    let est = |s: &str| crate::agent::prompt::est_tokens_pessimistic(s) as u64;
    let mut total = system.map_or(0, est);
    for m in messages {
        total += est(m.content.as_text());
        for call in &m.tool_calls {
            total += est(&call.name) + est(&call.arguments);
        }
    }
    for t in tools {
        total += est(&t.name)
            + t.description.as_deref().map_or(0, est)
            + est(&t.input_schema.to_string());
    }
    total
}

/// Why a metered dispatch refused to run. Constructed ONLY by
/// [`Executor::dispatch_metered`] and consumed ONLY by
/// [`Executor::record_refusal`] — a producer never invents or interprets one.
pub(super) enum Refusal {
    /// A budgeted run may not make this call. Carries `(spent, budget)` for the
    /// operator-facing message and `cause` for WHICH of the two budget checks said so
    /// — see [`BudgetRefusal`], which is the whole reason this is not just a pair of
    /// numbers.
    BudgetExhausted {
        spent: u64,
        budget: u64,
        cause: BudgetRefusal,
    },
    /// A budget is set but the provider reported no usage, so this call's spend would
    /// be invisible to the ledger. Fail closed: a budget you cannot measure is not a
    /// budget. (SP-DATA-5 Task 4 owns capturing usage and the tests for this arm;
    /// today it is unreachable in practice because nothing sets a budget until
    /// Task 5 wires `--budget-tokens`.)
    Unmetered { model: String },
}

/// Which of the two budget checks refused, and therefore what the operator is being
/// told.
///
/// ONE refusal kind with a cause, not two kinds: the clamp design's §2 requires the
/// floor to travel the EXISTING `BudgetExhausted` durable-pause path — same
/// `RunPaused`, same `resume_after: None` HOTL class, same recovery verb — and a second
/// variant would invite a second pause class to grow next to it. What §2 does not ask
/// for is a single message for two different situations, and reusing one was a real
/// defect: a fresh run with a cap of 300 paused reporting "budget: 0 of 300 tokens
/// spent", which is not true (nothing was spent), and told the operator to raise the cap
/// without saying by how much.
///
/// The two situations have different remedies. `Spent` means the run is out of budget
/// and any raise buys more work. `BelowFloor` means the cap must clear
/// `MIN_OUTPUT_TOKENS` plus this call's input estimate before the run can move AT ALL —
/// a raise smaller than that leaves it stuck in exactly the same place.
pub(super) enum BudgetRefusal {
    /// `spent >= cap`: the ledger has already reached the cap.
    Spent,
    /// The output allowance left after the pessimistic input estimate is below
    /// [`orchestrator_core::MIN_OUTPUT_TOKENS`]. Carries that allowance, which is what
    /// makes the required raise computable in the message.
    BelowFloor { allowance: u64 },
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
    /// There are TWO budget checks here, not one, and a reader who knows only the first
    /// has the wrong model of when a budgeted run stops.
    ///
    /// The first is `spent >= cap` BEFORE the call — a floor-trigger, which on its own
    /// permits an overshoot of one whole call. The second is the SP-DATA-5 clamp below
    /// it, which bounds that call's OUTPUT half by setting `max_tokens` to what the
    /// remaining budget affords, and REFUSES when what is left cannot buy a reply worth
    /// paying input tokens for. So a budgeted run can pause with `spent < cap`, and the
    /// residual overshoot is the input estimate's error rather than a model's whole
    /// output limit. Bounded and biased safe, not eliminated — the clamp block's own
    /// comment writes out the arithmetic.
    ///
    /// `budget: None` (every pre-SP-DATA-5 run) never gates and never clamps — the
    /// additivity guarantee.
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
            return Ok(Err(Refusal::BudgetExhausted {
                spent,
                budget: cap,
                cause: BudgetRefusal::Spent,
            }));
        }
        // The SP-DATA-5 clamp. The gate immediately above is a FLOOR-TRIGGER — it
        // refuses only once `spent` has ALREADY passed the cap — so without this a
        // single call can overshoot by however much output the adapter allows when
        // `max_tokens` is `None`, which is not one number: `openai_compat` and the local
        // engine pass the `Option` through (so the model's own maximum applies), while
        // `anthropic`, `gemini` and `bedrock` each substitute their own 1024. Setting
        // `max_tokens` replaces all of that with one rule under our control: the call
        // CANNOT return more output than the remaining budget affords.
        //
        // On the three that default to 1024, that is a WIDENING as well as an
        // enforcement — a budgeted call may now return more than the same unbudgeted one
        // — and the design's §6 records it as an accepted cost with the alternative that
        // was rejected. The run's TOTAL is bounded either way, which is the property this
        // exists for; only the per-call shape moves.
        //
        // What this does NOT do is eliminate the overshoot, and the distinction is the
        // whole reason the design argues for a pessimistic estimate. With
        // `max_tokens = remaining − est_input`, the real total is
        // `actual_input + output ≤ actual_input + (remaining − est_input)`, which
        // exceeds `remaining` by exactly `actual_input − est_input`. So the residual is
        // the ESTIMATE's error, biased toward refusing early because
        // `est_tokens_pessimistic` over-counts on the JSON-heavy prompts this
        // orchestrator actually sends. Bounded and biased safe, not zero.
        //
        // Only for a budgeted run, and only for `Chat`: `Embed`/`Stt` have no
        // `max_tokens` to set, so they fall through to the pre-existing floor-trigger
        // behaviour unchanged. `budget: None` never even computes the estimate — the
        // additivity guarantee the whole pre-SP-DATA-5 suite rests on.
        //
        // The request is CLONED and the clone modified: `dispatch_metered` takes a
        // `&InferenceRequest` and the caller's copy must not change under it. That is
        // safe for the memo fence because every determinism key covers the SEMANTIC
        // inputs and none of them covers a transport parameter: `support::input_hash`
        // hashes `{chain}|{payload}`, `agent_input_hash` hashes
        // `{chain}|{system}|{messages}|{tools}`, and the selector hashes
        // `{chain}|{system,user}`. `max_tokens` appears in none of the three, so a call
        // whose clamp differs between drives still hashes identically and replays from
        // its memo rather than raising `DeterminismViolation`.
        let clamped;
        let request = match (meter.budget(), &request.payload) {
            (
                Some(cap),
                Payload::Chat {
                    system,
                    messages,
                    tools,
                    ..
                },
            ) => {
                let est = est_input_tokens(system.as_deref(), messages, tools);
                // `cap - spent` cannot underflow while the gate at the top of this
                // function stands: it returned when `spent >= cap`, reading the same two
                // values (`cap` from `meter.budget()`, a plain field read, and this same
                // `spent` local). The gate is the ONLY thing keeping that true, so this
                // subtraction is written to do two different jobs depending on the
                // build, and both are deliberate.
                //
                // In a DEBUG build the `debug_assert!` panics with a message naming the
                // cause, which is what makes removing or reordering the gate loud rather
                // than silently absorbed: delete the gate and eleven tests redden.
                //
                // In a RELEASE build there is no tripwire to rely on — the workspace
                // profile sets no `overflow-checks`, so a plain `cap - spent` would WRAP
                // to something near `u64::MAX` and sail past the floor into a dispatch
                // that should have been refused. `checked_sub` fails CLOSED instead: the
                // impossible case refuses the call, which is the same answer the gate
                // would have given. That is the whole point of not writing
                // `saturating_sub` here — a saturating subtraction would produce 0 and
                // refuse too, but SILENTLY, in debug as well, and the gate's removal
                // would then be invisible to the suite.
                debug_assert!(
                    spent <= cap,
                    "the `spent >= cap` gate was bypassed or reordered: spent {spent} > cap {cap}"
                );
                let Some(remaining) = cap.checked_sub(spent) else {
                    return Ok(Err(Refusal::BudgetExhausted {
                        spent,
                        budget: cap,
                        cause: BudgetRefusal::Spent,
                    }));
                };
                // `saturating_sub` on the ESTIMATE is a different matter and is
                // load-bearing: `est` genuinely can exceed what is left, for a long
                // prompt against a nearly spent budget, and a plain subtraction there
                // would wrap to an enormous allowance — a clamp WIDER than the cap,
                // which is worse than no clamp at all.
                let allowance = remaining.saturating_sub(est);
                if allowance < orchestrator_core::MIN_OUTPUT_TOKENS {
                    // Below the floor, refuse rather than clamp — and refuse BEFORE the
                    // call, so no input tokens are spent on a reply that would arrive
                    // truncated mid-sentence and flow downstream as work product. This
                    // is the EXISTING durable pause, not a new refusal kind: the
                    // operator's recovery (`torii run wake --budget-tokens N`) is
                    // already built and already documented.
                    return Ok(Err(Refusal::BudgetExhausted {
                        spent,
                        budget: cap,
                        cause: BudgetRefusal::BelowFloor { allowance },
                    }));
                }
                // The MODEL's own output limit, which the allowance knows nothing
                // about. `allowance` is a pure budget figure: for any realistic
                // whole-run cap it is far larger than any model's maximum output, and
                // the providers do not shrug that off — Anthropic answers a 400
                // `invalid_request_error`, and every adapter here forwards `max_tokens`
                // verbatim. Without this bound, setting a budget would hard-FAIL the
                // first call of a run that succeeds unbudgeted: the clamp causing the
                // failure it exists to prevent.
                //
                // `None` (an unknown chain, or one with no resolvable models) means "no
                // ceiling from here", matching how `over_budget` treats an unknown
                // context window — a missing catalog entry must not silently truncate
                // every reply, and the budget allowance still bounds the call.
                let ceiling = match request.chain.as_deref() {
                    Some(chain) => self.gateway.min_max_output_tokens(chain).await,
                    None => None,
                };
                let mut r = request.clone();
                if let Payload::Chat { max_tokens, .. } = &mut r.payload {
                    // NEVER widen: a caller's own limit wins whenever it is lower. A
                    // clamp that could RAISE a caller's ceiling is the same defect as a
                    // tool that supplies argv and thereby widens its own sandbox policy,
                    // which SP-4 s4 spent a slice ruling out. Every orchestrator
                    // producer passes `None` today, so this guards a future caller
                    // rather than a present one.
                    //
                    // `u32::MAX` on an allowance too large for the `u32` field: this is
                    // saturation of the u64→u32 conversion ONLY, not a claim that
                    // `u32::MAX` is ever emitted. The `min` with `ceiling` on the next
                    // line is what actually bounds it whenever the chain resolves, and a
                    // budget with more than 4 billion tokens left is not the case the
                    // budget half exists to constrain anyway.
                    let want = u32::try_from(allowance).unwrap_or(u32::MAX);
                    let want = ceiling.map_or(want, |limit| want.min(limit));
                    *max_tokens = Some(max_tokens.map_or(want, |caller| caller.min(want)));
                }
                clamped = r;
                &clamped
            }
            _ => request,
        };
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
    /// both the durable record and the operator-facing wording, so the five producers
    /// pause/fail identically on the same cause.
    ///
    /// FIVE, matching the census this module's header and `dispatch_metered` keep and the
    /// `model_output` chokepoint keeps on the OUTPUT side: the ReAct turn (`agent.rs`), the
    /// `ModelCall` node (`mod.rs`), the `Map`-item call and the `Consolidate` synthesis
    /// (`fanout.rs`), and the selector's lent dispatch (`dispatch.rs`). The
    /// budget-completeness pass moved that number from four to five in four places and
    /// missed this one. The two neighbours below saying "the OTHER four producers" are
    /// correct as written — they speak from inside `SelectorDispatch`, i.e. the fifth about
    /// the other four.
    pub(super) async fn record_refusal(
        &self,
        run: RunId,
        node: &NodeId,
        refusal: Refusal,
    ) -> Result<RefusalKind, OrchestratorError> {
        match refusal {
            Refusal::BudgetExhausted {
                spent,
                budget,
                cause,
            } => {
                // Both messages start `budget: ` — that prefix is the operator- and
                // test-visible marker for "this pause is about the cap", and torii's
                // status/list-paused surfaces match on it.
                let reason = match cause {
                    BudgetRefusal::Spent => format!(
                        "budget: {spent} of {budget} tokens spent; raise it with `torii run wake --budget-tokens N`"
                    ),
                    // The floor. Reusing the wording above here would report a spend
                    // that did not happen ("0 of 300 tokens spent" on a fresh run) and
                    // give no hint of how far the cap must move. The raise it names is
                    // the SMALLEST one that unblocks THIS call — a later, longer prompt
                    // can still land under the floor at that cap, which is why it says
                    // "at least".
                    BudgetRefusal::BelowFloor { allowance } => {
                        let floor = orchestrator_core::MIN_OUTPUT_TOKENS;
                        let needed = budget.saturating_add(floor.saturating_sub(allowance));
                        format!(
                            "budget: only {allowance} tokens left for output after the input \
                             estimate, below the {floor}-token floor ({spent} of {budget} \
                             spent); raise the cap to at least {needed} with \
                             `torii run wake --budget-tokens N`"
                        )
                    }
                };
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
/// Binds one `select()` to one run + Expand node so that EVERY call it makes gates on
/// the run's budget, charges the live meter and journals its spend exactly like the
/// other four producers. The selector supplies only the prompts; it cannot widen the
/// capability, pick a different provider, or skip the gate.
///
/// The selector's spend is journaled under the reserved
/// [`RESERVED_SELECT_ID`](orchestrator_core::RESERVED_SELECT_ID) path rather than the
/// Expand node's own id, so it lands on the durable ledger without colliding with the
/// node's effects. That matters beyond tidiness: `PlannerSelected` memoizes the CHOICE,
/// so a resumed run that got that far never re-invokes the selector — without a
/// journaled `EffectRecorded` the tokens it really spent would vanish from the fold on
/// every subsequent resume and the run would drift permanently under its true spend.
///
/// That record is also this dispatch's MEMO: `PlannerSelected` is journaled only after
/// `select()` returns, so a run that failed before it re-enters the `Select` arm on its
/// next drive, and [`complete`](orchestrator_core::ModelDispatch::complete) replays the
/// recorded text rather than paying for the call again.
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
    /// An error raised by the EXECUTOR itself inside this dispatch — a memo mismatch
    /// (`DeterminismViolation`) or an unreadable recorded output (`ContentDigestMiss`
    /// and friends, from `materialize`). It is stashed rather than merely returned
    /// because the return value is at the mercy of an arbitrary `PlannerSelector`: it
    /// travels back through `select()`, whose `Err` the `Select` arm otherwise reads as
    /// the SELECTOR's own failure (a bad pick, an empty response) and downgrades to a
    /// soft node `Failed`. Every other producer's identical check is a hard halt; this
    /// stash is how the `Select` arm re-raises it as one, even if the selector swallows
    /// or rewraps the `Err`.
    fatal: std::sync::Mutex<Option<OrchestratorError>>,
    /// How many calls this `select()` has made — the effect id's `local_index`.
    ///
    /// `ModelDispatch` is lent to an arbitrary `PlannerSelector`, so "one call per
    /// `select()`" is `LlmPlannerSelector`'s habit, not the port's contract: a
    /// shortlist-then-choose selector is the obvious second shape. Pinned to `0`, every
    /// call after the first collided on one journal key and `fold_journal`'s keyed
    /// `usage.insert` kept exactly one of them — the ledger under-counted a real spend,
    /// which is the one thing an exact cap cannot survive.
    ///
    /// The index is assignment-ORDERED, so it is the memo key only for a selector that
    /// dispatches SEQUENTIALLY (as the trait's "run one metered text completion"
    /// implies). One that dispatches concurrently can see its calls numbered
    /// differently across drives — and then a resume reads a mismatched `input_hash`
    /// and halts `DeterminismViolation`. Loud, and the intended outcome: a selector
    /// whose calls have no stable order has no stable replay either.
    calls: AtomicU64,
}

impl<'a> SelectorDispatch<'a> {
    pub(super) fn new(exec: &'a Executor, run: RunId, node: NodeId, fold: &'a super::Fold) -> Self {
        Self {
            exec,
            run,
            node,
            fold,
            refusal: std::sync::Mutex::new(None),
            fatal: std::sync::Mutex::new(None),
            calls: AtomicU64::new(0),
        }
    }

    /// Take the refusal this dispatch journaled, if any. `None` means the selector's
    /// `Err` was its own (a bad pick, an empty response), not the budget gate's.
    pub(super) fn take_refusal(&self) -> Option<RefusalKind> {
        self.refusal.lock().expect("selector refusal lock").take()
    }

    /// Take the executor's own fatal error, if this dispatch raised one. `Some` means
    /// the drive must abort with it — the journal is inconsistent, so continuing would
    /// write a `NodeFailed` into a record already proved unreliable and keep spending
    /// against the cap on the strength of it.
    pub(super) fn take_fatal(&self) -> Option<OrchestratorError> {
        self.fatal.lock().expect("selector fatal lock").take()
    }

    /// Record `e` as the executor's own failure and hand the selector a surrogate to
    /// abort on. The surrogate carries `e`'s message but not its type, because
    /// `OrchestratorError` is not `Clone` and the typed original is the one the
    /// `Select` arm re-raises — nothing downstream of the selector reads this copy.
    fn fatal(&self, e: OrchestratorError) -> OrchestratorError {
        let surrogate = OrchestratorError::Gateway(e.to_string());
        *self.fatal.lock().expect("selector fatal lock") = Some(e);
        surrogate
    }

    /// `"{node}/__select__"` — the reserved path the selector's spend is journaled
    /// under, one effect per call (`effect_id(path, 0, call_index)`).
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

        // The same memo check the other four producers make, and for the same reason.
        // `PlannerSelected` fences a re-invocation only AFTER the selector returns, so
        // every window that ends before it — the anti-hallucination reject, a gateway
        // error, a crash between this `EffectRecorded` and that `PlannerSelected` —
        // leaves a run that re-enters the `Select` arm on its next drive. Such a run is
        // re-driven in production: a failed node withholds `RunCompleted` so the run
        // stays resumable, and `Scheduler::record` ranks `paused` ahead of `failed`.
        // Without this lookup each wake was a fresh BILLED call whose `EffectRecorded`
        // overwrote the last at the same effect id (`fold_journal` keys `usage`, it does
        // not sum), so the durable ledger froze at one call's tokens while the run spent
        // without bound and the cap could never fire.
        let path = self.select_path();
        let eid = orchestrator_core::effect_id(
            &path,
            0,
            self.calls.fetch_add(1, Ordering::Relaxed) as usize,
        );
        let ih = super::support::input_hash(
            chain,
            &serde_json::json!({ "system": system, "user": user }),
        )?;
        if let Some((recorded_ih, output)) = self.fold.memo.get(&eid) {
            if recorded_ih != &ih {
                return Err(self.fatal(OrchestratorError::DeterminismViolation {
                    node: NodeId(path),
                    effect_id: eid,
                }));
            }
            // Replay the recorded (already redacted) text: no gateway call, no new
            // journal event, nothing charged to the meter. A recorded output that
            // cannot be read back (`ContentDigestMiss`) is the executor's failure too,
            // and just as fatal — the memo is unreliable either way.
            let replayed = match self.exec.materialize(output).await {
                Ok(v) => v,
                Err(e) => return Err(self.fatal(e)),
            };
            return Ok(replayed
                .get("text")
                .and_then(|t| t.as_str())
                .unwrap_or_default()
                .to_string());
        }

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

        let recorded = self.exec.split_output(&output).await?;
        self.exec
            .append(
                self.run,
                JournalEvent::EffectRecorded {
                    node: NodeId(path),
                    effect_id: eid,
                    class: orchestrator_core::EffectClass::Pure,
                    input_hash: ih,
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

#[cfg(test)]
mod tests {
    use super::*;
    use kernel::types::request::{MessageContent, MessageRole, ToolCall};

    fn user(text: &str) -> Message {
        Message::text(MessageRole::User, text)
    }

    fn tool(name: &str, description: Option<&str>, schema: serde_json::Value) -> ToolDefinition {
        ToolDefinition {
            name: name.to_string(),
            description: description.map(str::to_string),
            input_schema: schema,
        }
    }

    /// Every term of the estimate is COUNTED, proven one term at a time.
    ///
    /// The estimate is the whole clamp: `allowance = remaining − est`, so a term that
    /// silently contributes nothing leaves the allowance too high and the cap is
    /// overshot by exactly that much. Before this test, deleting the tool loop or
    /// replacing the system term with `0` left `cargo test --workspace` green, and the
    /// only guarded term was messages — guarded by one token of margin, because every
    /// clamp test used a two-character prompt.
    ///
    /// Strict `>` on each step, against the same call with that one term absent. A
    /// weaker `>=` would pass for a function that ignored the term entirely.
    #[test]
    fn every_part_of_the_input_is_counted_in_the_estimate() {
        let msgs = vec![user("the user's question, which is not short")];
        let bare = est_input_tokens(None, &msgs, &[]);
        assert!(bare > 0, "the message body itself costs something");

        assert!(
            est_input_tokens(Some("a system prompt the agent was given"), &msgs, &[]) > bare,
            "the system prompt is counted"
        );

        let named = tool("fs_write", None, serde_json::json!({}));
        let with_name = est_input_tokens(None, &msgs, std::slice::from_ref(&named));
        assert!(with_name > bare, "a tool's NAME is counted");

        let described = tool(
            "fs_write",
            Some("write a file, creating parent directories"),
            serde_json::json!({}),
        );
        assert!(
            est_input_tokens(None, &msgs, &[described]) > with_name,
            "a tool's DESCRIPTION is counted — it is optional, not free"
        );

        let schemad = tool(
            "fs_write",
            None,
            serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "contents": { "type": "string" }
                },
                "required": ["path", "contents"]
            }),
        );
        assert!(
            est_input_tokens(None, &msgs, &[schemad]) > with_name,
            "a tool's JSON SCHEMA is counted — the term the estimate exists for"
        );
    }

    /// An assistant turn's `tool_calls` are counted, because the provider is sent them.
    ///
    /// The ReAct loop appends exactly such a message every turn (`agent.rs`), and every
    /// subsequent turn re-sends it: `anthropic/convert.rs` emits a `ToolUse` block per
    /// entry, `openai_compat/convert.rs` emits `tool_calls`. `Message::content` is empty
    /// on those turns, so counting only `as_text()` made a serialized plan or an
    /// `fs_write` body — the biggest payloads the loop produces — cost zero.
    ///
    /// That under-counts in the one direction the design forbids: `allowance =
    /// remaining − est` comes out too high, so the residual overshoot exceeds the
    /// design's §4 bound and "biased toward refusing early" stops being true for the
    /// agent path, which is the path that generates the large arguments.
    #[test]
    fn an_assistant_turns_tool_call_arguments_are_counted() {
        let plain = Message {
            role: MessageRole::Assistant,
            content: MessageContent::Text {
                text: String::new(),
            },
            tool_calls: Vec::new(),
            attachments: Vec::new(),
        };
        let calling = Message {
            tool_calls: vec![ToolCall {
                id: "t0".into(),
                name: "fs_write".into(),
                arguments: serde_json::json!({
                    "path": "notes.md",
                    "contents": "a body long enough to matter to a token budget"
                })
                .to_string(),
            }],
            ..plain.clone()
        };

        let without = est_input_tokens(None, std::slice::from_ref(&plain), &[]);
        let with = est_input_tokens(None, std::slice::from_ref(&calling), &[]);
        assert!(
            with > without,
            "a tool call's arguments are sent to the provider, so they are estimated: \
             {with} vs {without}"
        );
        // And by roughly what they cost: the arguments alone are ~90 characters, so a
        // token or two of difference would mean only the NAME was counted.
        assert!(
            with - without >= 20,
            "the ARGUMENTS are counted, not just the call's name: {with} vs {without}"
        );
    }
}
