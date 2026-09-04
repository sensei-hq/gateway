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
use kernel::types::request::{InferenceRequest, InferenceResponse, Payload};
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
/// against an unchanged ledger. A reservation was rejected because it would need an
/// output-token estimate unknowable before the call — and that reason no longer holds:
/// the SP-DATA-5 clamp below gives every budgeted `Chat` an explicit `max_tokens`, so a
/// reservation could simply hold `est_input + max_tokens` and be conservative without
/// predicting anything. What rules it out now is starvation rather than ignorance. That
/// reservation is essentially the WHOLE remaining allowance, so the first child to claim
/// it leaves every sibling under `MIN_OUTPUT_TOKENS` and refused — i.e. it would make a
/// concurrent fan-out safe without making it concurrent, and pause siblings that
/// serialising would have run. Recorded rather than deleted because "a reservation is
/// impossible" is the wrong reason to carry forward if anyone revisits this.
///
/// So a run WITH a budget takes [`gate`](Meter::gate) — a 1-permit `tokio::sync::Mutex`
/// held across the whole check → dispatch → charge sequence — and therefore has at most
/// one model call in flight at a time. That is what makes §6.5's "overshoot bounded by
/// at most one call" true under fan-out, and it does so with no estimation of its own —
/// the clamp's estimate bounds the size of that one call, not the number in flight.
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

// `est_input_tokens(system, messages, tools)` lived here until the serving-window
// review. It was the budget clamp's own pessimistic input estimate — the same parts the
// gateway's `estimate_input_tokens_pessimistic` counts (system prompt, every message
// body, every assistant turn's tool CALLS, every tool's name/description/schema), the
// same `/3` divisor, and `attachments` omitted from both because no orchestrator
// producer populates them.
//
// It is deleted rather than aligned, and the clamp now calls
// `gateway::estimate_input_tokens_pessimistic` on the very `Payload` it is about to
// dispatch. The reason is not tidiness — it is that the two functions were not the same
// number, and the serving-window bound's soundness is a statement about the SET
// `{ m : m.context_window >= est }` being the set selection draws from, which is only
// true if both halves say `est`.
//
// They did not. This one applied `ceil` PER STRING and summed; the gateway's sums the
// byte lengths of every string and applies `ceil` ONCE. On ASCII `Σ ceil(Lᵢ/3) >=
// ceil(Σ Lᵢ/3)`, so this figure was the LARGER one — 4200 against 3800 on the
// 600-message payload the tests below use, and 60 against 59 on this repo's own
// `tool_agent_registry` fixture as the review measured it — and the clamp's serving set
// was a strict SUBSET of the gate's admitted set.
// Two reachable consequences, both a provider 400 (`prompt + max_tokens >
// context_window`) arriving as a terminal `NodeFailed` on a budgeted run where the
// identical UNBUDGETED call succeeds, since `max_tokens: None` is omitted from the wire:
//
// - the subset is EMPTY while the gate's is not ⇒ no window term at all ⇒ `max_tokens`
//   bounded only by the output limit (`the_clamp_bounds_max_tokens_by_the_estimate_the_
//   gate_will_judge_by`);
// - the subset drops the SMALL candidate the gate keeps ⇒ the bound is a big model's
//   window and the request lands on the small one (`the_serving_window_bound_is_safe_
//   for_the_smallest_candidate_the_gate_admits`).
//
// Aligning the arithmetic by hand instead would have left an invariant between two
// crates that nothing enforces; on multi-byte text the drift ran the OTHER way (bytes
// there, chars here), so the two figures were not ordered in either direction and no
// slack constant could have covered it. Calling one function makes the equality
// structural. The cost is a `pub` on the gateway side, argued in that function's own doc.
//
// What the BUDGET half trades for that, in both directions, because the two differences
// do not point the same way and "more margin" would be a false summary:
//
// - UNIT, bytes instead of characters: strictly more margin, since bytes >= chars, and
//   much more where the old figure was weakest — CJK is 3 bytes per character and
//   tokenizes near 1 token per character, so `chars/3` under-counted it threefold and
//   `bytes/3` lands close to the truth.
// - ROUNDING, one `ceil` instead of one per string: strictly LESS margin on ASCII, by
//   `Σ ceil(Lᵢ/3) − ceil(Σ Lᵢ/3)`, which is at most ⅔ of a token per payload part (the
//   worst case being a length that is `1 mod 3`). A ReAct turn has a system prompt, a
//   handful of messages and its tool schemas, so that is tens of tokens; the tests' own
//   600-part fixture is built to maximise it and reaches 400.
//
// So on Latin text the estimate came DOWN by up to `parts` tokens, which widens the §4
// residual (`actual_input − est_input`) by the same amount and nothing else — the bound
// is still `remaining`, the overshoot is still the estimate's error, and tens of tokens
// against `MIN_OUTPUT_TOKENS = 256` and any real cap is noise. Worth the trade for a
// window bound that is sound. The AC11 `budget clamp under-estimated the input` warning
// below is what measures the residual in production, and it is the same warning either
// way.

/// What the clamp did on one call, kept so the two post-response diagnostics can be
/// emitted against it (design §5.4). `None` on an unbudgeted run and on a non-`Chat`
/// payload — the two paths the clamp does not touch — which is what keeps both signals
/// silent there.
///
/// One struct rather than three loose `Option`s because the three numbers are only ever
/// meaningful together: a reader of `emitted` alone cannot tell whether the budget or
/// the model's limit produced it, and a reader of `est_input` alone cannot tell whether
/// an estimate was made at all.
struct ClampRecord {
    /// `remaining − est_input`, the BUDGET's own term before any other bound.
    allowance: u64,
    /// The `max_tokens` actually sent, i.e. `min(allowance, model ceiling, caller's)`.
    /// Equal to `allowance` exactly when the budget was the binding term.
    emitted: u64,
    /// The pessimistic input estimate the allowance was computed from.
    est_input: u64,
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
    /// [`orchestrator_core::MIN_OUTPUT_TOKENS`].
    ///
    /// Carries BOTH terms the message needs, and the second is not redundant. The
    /// allowance is what the operator is told is left; the ESTIMATE is what the required
    /// raise must be computed from, because the allowance is
    /// `remaining.saturating_sub(est)` and that saturation is reachable — a long prompt
    /// against a nearly spent budget floors it at 0 and destroys `est − remaining`.
    /// Deriving the raise from the allowance instead (`budget + floor − allowance`)
    /// agrees everywhere else and understates it by exactly that difference here, which
    /// is the arm nothing else can observe.
    ///
    /// `window` is `Some(w)` when the SERVING CONTEXT WINDOW pushed the ceiling under the
    /// floor, and `w` is the raw window that did it: the smallest context window in the
    /// chain that **can actually hold this call's input**. It exists because that
    /// situation and an exhausted budget have different remedies and were once reported
    /// identically, in budget wording naming a cap raise.
    ///
    /// # It discriminates ONE of the two model bounds, and the other one still misdirects
    ///
    /// Stated narrowly on purpose: an earlier version of this sentence read "when a MODEL
    /// bound — not the budget — pushed the ceiling under the floor", and there are two
    /// model bounds. `ceiling = min(min_max_output_tokens, window_term)`, and when the
    /// OUTPUT LIMIT is what lands under [`orchestrator_core::MIN_OUTPUT_TOKENS`] the
    /// `term == c` test fails, `window` is `None`, and the operator gets the budget arm's
    /// message — "raise it with `torii run wake --budget-tokens N`" — for a term that does
    /// not read the cap either. **That is a known misdirection, not a claim this field
    /// makes.**
    ///
    /// It is reachable rather than theoretical: `collect_validation_errors`' Rule 5
    /// rejects only `max_output_tokens == 0`, so a chain entry declaring 200 is valid
    /// config and `Gateway::new` validates nothing at all. It is left rather than fixed
    /// because closing it means a THIRD message arm — the refusal would have to name
    /// "this chain's smallest model can only emit 200 tokens" and the remedy is a
    /// different one again (drop the entry, or raise its declared limit) — and inventing
    /// that wording is a change the serving-window review did not ask for and no test
    /// currently pins. The spec's deferred list carries it.
    ///
    /// A TIE goes to the window (`term == c` holds when both terms are equal), which is
    /// the least-misdirecting answer available: both terms are cap-blind, and only the
    /// window arm's wording says so. Pinned by
    /// `a_tie_between_the_output_limit_and_the_window_term_names_the_window`.
    ///
    /// # What a `Some` MEANS changed with the serving-window bound
    ///
    /// The term used to be `min_context_window(chain) − est`, so a `Some(w)` meant "your
    /// prompt is bigger than the chain's SMALLEST model" — a claim about a model the
    /// request might never have been routed to, and one that swept up the genuinely
    /// different "fits nothing at all" case with it. That case no longer arrives here.
    /// An empty serving set yields NO window term, the call is dispatched, and the
    /// gateway's `ContextWindowGate` refuses it as an `AllGated` naming each candidate's
    /// own window and a `UseLargerContextWindow` remedy
    /// (`an_over_every_window_prompt_is_refused_by_the_gate_budgeted_or_not`).
    ///
    /// So a `Some(w)` now certifies something narrower and strictly true: **the input
    /// FITS.** `w >= est_input` holds by construction of
    /// `Gateway::min_serving_context_window`, and the refusal is about the room left
    /// BESIDE the input. `w − est_input` is under [`orchestrator_core::MIN_OUTPUT_TOKENS`],
    /// so the reply would arrive truncated mid-sentence and travel downstream as work
    /// product — worse than not making the call.
    ///
    /// The remedies follow from that, and they are not the ones the old message named.
    /// "Route to a chain whose smallest model has a larger window" was advice about the
    /// wrong model: on a heterogeneous chain the smallest entry may already have been
    /// filtered out of the decision, and widening it changes nothing. What moves this
    /// refusal is sending less input, or putting a model with a larger window into the
    /// chain.
    ///
    /// What has NOT changed is that the cap is not one of them, and the message still
    /// says so. The window term reads no budget figure, so the identical refusal arrives
    /// at a cap of 1e6 and at `u64::MAX`; and every round trip on this pause is a manual
    /// `BudgetRaised` plus a `force_wake` by a human, so pointing at a cap that is
    /// already astronomical costs an operator one of those and ends where it started.
    /// (The slice design's §5 predicted this sentence would become false too. It did not
    /// — the arithmetic is unchanged — and the design has been corrected rather than the
    /// code made to match it; a comment that told an operator to try the cap here would
    /// be a new false claim, not a repaired one.)
    BelowFloor {
        allowance: u64,
        est_input: u64,
        window: Option<u32>,
    },
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
        // `max_tokens` is `None`. That is not one number; it is five different ones, and
        // the survey below was re-read against the adapters rather than assumed, because
        // an earlier version of this comment got two of the five wrong:
        //
        // - `openai_compat` (`openai_compat/mod.rs:73`, `:128`) is the only true
        //   pass-through: the wire field is `skip_serializing_if = "Option::is_none"`, so
        //   `None` OMITS it and the model's own maximum applies.
        // - `anthropic` (`anthropic/mod.rs:155`, `:218`) and `bedrock`
        //   (`bedrock/mod.rs:205`, `:271`) substitute their own `DEFAULT_MAX_TOKENS` of
        //   1024 unconditionally.
        // - `gemini` (`gemini.rs:636`, `:685`) builds `generationConfig` only when
        //   `max_tokens.is_some() || temperature.is_some()`, and every producer here
        //   sends `temperature: None` (`support.rs:500`, `support.rs:539`, and this
        //   module's own selector request). So with `max_tokens: None` NO
        //   `generationConfig` is sent at all and gemini's 1024 is never reached —
        //   Gemini's own server-side default applies.
        // - the local engine (`llama_cpp/mod.rs:398`, `:597`) does
        //   `max_tokens.unwrap_or(default_max_tokens)`, which the chat builder sets to
        //   512 (`:142`). `None` there means 512, not the model's maximum.
        //
        // Setting `max_tokens` replaces all five with one rule under our control: the
        // call CANNOT return more output than the remaining budget affords.
        //
        // The DIRECTION of the change therefore differs by family, and it is not the
        // split the design first recorded. Against a large cap the emitted value is the
        // chain's own `max_output_tokens` (2048 to 8192 across the shipped example
        // catalog and presets), so the
        // clamp WIDENS on `anthropic`, `bedrock` and the local engine — all three of
        // which substitute a small constant for `None` — and NARROWS on `openai_compat`
        // and `gemini`, where `None` meant the model's or the provider's own maximum. The
        // design's §6 records both directions as accepted costs, with the alternative
        // that was rejected. The run's TOTAL is bounded either way, which is the property
        // this exists for; only the per-call shape moves.
        //
        // What this does NOT do is eliminate the overshoot, and the distinction is the
        // whole reason the design argues for a pessimistic estimate. With
        // `max_tokens = remaining − est_input`, the real total is
        // `actual_input + output ≤ actual_input + (remaining − est_input)`, which
        // exceeds `remaining` by exactly `actual_input − est_input`. So the residual is
        // the ESTIMATE's error, biased toward refusing early because
        // `gateway::estimate_input_tokens_pessimistic` divides by 3 and counts every part
        // that goes on the wire, which over-counts on the JSON-heavy prompts this
        // orchestrator actually sends. Bounded and biased safe, not zero.
        //
        // "Biased toward refusing early" carries a SCRIPT assumption, and it is worth
        // naming because nothing else in the code does. The estimate is
        // `ceil(UTF-8 BYTES / 3)`, and three bytes per token is an over-count only where
        // a token is worth three or more bytes — Latin-script text, where a byte is a
        // character. It used to be `chars / 3`, which under-counted CJK by a factor of
        // three (3 bytes per character, tokenizing near 1 token PER character), and
        // counting bytes closes most of that: 3 bytes/3 lands at ~1 token per CJK
        // character, which is roughly the truth rather than a third of it. The margin is
        // thinner there than on Latin text and the sign can still flip on scripts that
        // tokenize worse than one token per character, so the §4 residual
        // (`actual_input − est_input`) is not guaranteed non-negative on non-Latin input.
        // The AC11 `tracing::warn!` below is the measurement of what is left, and a real
        // tokenizer (design §8) is the fix. (The byte count arrived with the estimator
        // unification — see the tombstone above `ClampRecord` — so this paragraph is the
        // one place that records the improvement it brought as a side effect.)
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
        let mut clamp: Option<ClampRecord> = None;
        let request = match (meter.budget(), &request.payload) {
            (Some(cap), Payload::Chat { .. }) => {
                // ONE estimate, computed by the GATEWAY's estimator over the very payload
                // that is about to be dispatched — not a second one of the orchestrator's
                // own. The tombstone above `ClampRecord` records the function this
                // replaced and the defect that made two numbers unacceptable; the short
                // version is that everything below reasons about the set
                // `{ m : m.context_window >= est }` being the set selection draws from,
                // and that is a claim about ONE `est`.
                //
                // `&request.payload` and not the destructured parts: `Gateway::execute`
                // estimates `&request.payload` too (`engine/execute.rs`), and the clone
                // this block goes on to make differs from it only in `max_tokens`, which
                // the `Chat` arm of the estimator does not read. So the figure here is
                // the figure the `ContextWindowGate` will judge this request by, by
                // construction rather than by agreement.
                //
                // `u32`, as the accessor wants it. The saturation the old `u64` sum
                // needed at the accessor boundary is now inside the estimator, which
                // saturates to `u32::MAX` on a payload too large to count — safe in the
                // only direction that matters, because it can only ask for a LARGER
                // window than the truth and so shrink the serving set.
                let est_u32 = gateway::estimate_input_tokens_pessimistic(&request.payload);
                let est = u64::from(est_u32);
                // `cap - spent` cannot underflow while the gate at the top of this
                // function stands: it returned when `spent >= cap`, reading the same two
                // values (`cap` from `meter.budget()`, a plain field read, and this same
                // `spent` local). The gate is the ONLY thing keeping that true, so this
                // subtraction is written to do two different jobs depending on the
                // build, and both are deliberate.
                //
                // In a DEBUG build the `debug_assert!` panics with a message naming the
                // cause, which is what makes removing or reordering the gate loud rather
                // than silently absorbed: delete the gate and every budget-producer test
                // that drives past the cap reddens on this `debug_assert!`'s own message,
                // plus two that do not touch it —
                // `a_non_chat_payload_is_gated_but_not_clamped` (the one payload the
                // clamp skips, so the gate is all that is left) and
                // `spending_exactly_the_cap_stops_the_run` (whose reason assertion tells
                // the gate's message from the floor's). Stated as a property rather than
                // a count: the count moved the last two times a budget test was added,
                // and a stale number in a load-bearing comment is worse than no number.
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
                        cause: BudgetRefusal::BelowFloor {
                            allowance,
                            est_input: est,
                            // The BUDGET allowance alone fell under the floor — no model
                            // bound was consulted to reach this point, so there is no
                            // window to blame and a cap raise genuinely is the remedy.
                            window: None,
                        },
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
                // ceiling from here" — a missing catalog entry must not silently truncate
                // every reply, and the budget allowance still bounds the call. The
                // deleted `over_budget` treated an unknown window the same way, and this
                // is now the LAST place that convention lives: the gateway's
                // `ContextWindowGate` has no unknown-window case at all, because it reads
                // a resolved candidate's non-optional `ModelConfig.context_window`.
                //
                // It is a `min` over the CHAIN, which has a cost §6 records: on a
                // HETEROGENEOUS fallback chain the weakest entry's limit binds from the
                // very first call, however much budget remains. A budgeted run on
                // [gpt-4o 16384, small-fallback 4096] gets 4096-token replies on the
                // primary throughout, where the same run unbudgeted would get 16384.
                // That is the price of setting `max_tokens` before selection, and it is
                // paid deliberately: a request that fails over lands on a different
                // entry, and a value the fallback would reject turns a survivable
                // failover into a hard 400.
                //
                // A ceiling of `Some(0)` would emit `max_tokens: Some(0)` — "generate
                // nothing" — and is NOT special-cased here. It is rejected one layer
                // down instead: `collect_validation_errors` refuses a model with
                // `max_output_tokens == 0`, because zero output is broken for every
                // reader of that field and not just this one. Deliberately not widened
                // to "ignore any ceiling below MIN_OUTPUT_TOKENS" — that would send an
                // over-large `max_tokens` to a model whose limit is genuinely small,
                // which is the case the floor-before-ceiling ordering exists to serve.
                // The residual is the documented unchecked `Gateway::new` /
                // `update_config` path, which validates nothing at all.
                //
                // **The output limit is not the only model bound, and on its own it is
                // not enough.** A provider enforces `prompt + max_tokens <=
                // context_window` as well, and the unbudgeted path never trips that
                // because `max_tokens: None` is OMITTED from the wire entirely
                // (`openai_compat/convert.rs`'s `skip_serializing_if`), so the sum is
                // never formed. Bounding only by `max_output_tokens` therefore
                // reintroduces the very regression the paragraph above describes, by the
                // other route: a long-prompt call that succeeds unbudgeted hard-FAILS
                // once a budget is set. Worse than the output-limit case, because it
                // arrives as a `NodeFailed` rather than a budget pause — `torii run wake
                // --budget-tokens N` cannot recover it and the operator's only remedy is
                // to drop the budget.
                //
                // Not hypothetical: the shipped presets are `8192 / 4096`
                // (`gateway/src/catalog/presets.rs`), so any prompt past ~4096 tokens is
                // in this territory, and SP-6 s4 bounds ONE `## Context` section at
                // 32 KiB — ~10.9k tokens by itself under `chars/3`. Found by the
                // whole-slice review, which reproduced the provider's 400 against a
                // double applying the documented sum rule.
                //
                // `est` is subtracted rather than the real prompt count for the reason
                // the whole clamp uses it: the real count is not known until the response
                // comes back, and `est` is biased HIGH, so this bound errs toward asking
                // for less. Same `None` handling as the output limit — an unknown chain
                // yields no bound from here rather than a silent truncation.
                //
                // **The window it subtracts from is the smallest one that can SERVE this
                // request, not the chain's smallest — and the difference is a whole
                // slice.** `min_context_window(chain)` was safe, because a value fitting
                // the weakest entry fits every entry, and far too strong: on a
                // `[128k, 8k]` chain a 20k prompt gave `8192 − 20000`, `saturating_sub`
                // floored it at 0, the floor check below refused — and it refused HERE,
                // inside the orchestrator, before `Gateway::execute` was called at all.
                // So SP-7a's `ContextWindowGate`, which admits the 128k entry and serves
                // the request, never ran on any budgeted run. Two components disagreed
                // about one request and the upstream one won.
                //
                // `min_serving_context_window(chain, est)` folds the min over exactly the
                // candidates the gate admits (`window >= est`). Selection can only return
                // a member of that set, and every member is at least the minimum over it,
                // so the bound is safe for whichever candidate wins WITHOUT knowing which
                // — the property the chain minimum bought by being needlessly pessimistic.
                // The accessor's own doc carries the full argument, including why
                // bounding by the chain's LARGEST window was rejected.
                //
                // **"Exactly the candidates the gate admits" is a claim about ONE `est`,
                // and it shipped false for a day.** The clamp had its own estimator and it
                // returned a bigger number than the gate's on ASCII text, so its serving
                // set was a strict SUBSET of the set selection drew from and both bullets
                // above failed — either the subset was empty and no window term was
                // contributed at all, or it excluded the small candidate that then won.
                // The tombstone above `ClampRecord` has the arithmetic. `est` is now the
                // gate's own figure from the gate's own function, which is why the two
                // sets are the same set rather than two sets that agree in testing.
                //
                // **`None` here means nothing in the chain can serve the request, and it
                // must contribute NO window term** — not a zero, which would trip the
                // floor below and re-create the early refusal this change exists to
                // remove. The `(a, b) => a.or(b)` arm is what delivers that: with the
                // output limit `Some` and the window `None` the ceiling is the output
                // limit alone, the call proceeds, and the GATE refuses it — naming each
                // candidate's own window, which is a diagnosis the clamp cannot produce.
                // That handover is the design's choice, not a fallback: the clamp's
                // `BelowFloor` is budget-flavoured and names a cap raise, and a cap raise
                // has never been able to make a prompt fit a window.
                //
                // `est_u32` is passed straight through — the estimator returns a `u32`, so
                // there is no conversion here to get wrong any more. Its own saturation to
                // `u32::MAX` on an uncountable payload is documented at the estimator and
                // errs the safe way: a larger `est` shrinks the serving set and shrinks
                // the bound. Reaching it takes a prompt of some 12.9 billion bytes, at
                // which point the set is empty for every `context_window` short of
                // `u32::MAX` itself and the refusal is the gate's. A config that literally
                // declared `u32::MAX` would keep that entry with a window term of 0 and
                // refuse on the floor below, which is also correct — nothing is dispatched
                // either way.
                //
                // `saturating_sub` on the next line is now UNREACHABLE saturation: every
                // window in the serving set satisfies `w >= est_u32` by construction, so
                // the subtraction is always exact. Kept rather than swapped for `-`
                // because a plain subtraction would panic in debug and WRAP in release
                // (the workspace profile sets no `overflow-checks`) if the accessor's
                // filter were ever loosened, and a wrapped window term is an enormous
                // `max_tokens` sent at a model that cannot take it.
                //
                // `binding_window` carries the RAW serving window when the window term is
                // what produced the ceiling, and it exists purely so the refusal below can
                // be truthful. Without it the two very different situations — "your cap is
                // too small" and "the smallest model that can hold this prompt has no room
                // left for a reply" — reach the operator in identical budget wording, and
                // only one of them is fixed by the `torii run wake --budget-tokens N` that
                // wording names. A tie counts as window-bound: when both terms land on the
                // same figure the window is a true cause of the refusal, and the window is
                // the half a cap raise cannot move.
                let (ceiling, binding_window) = match request.chain.as_deref() {
                    Some(chain) => {
                        let out = self.gateway.min_max_output_tokens(chain).await;
                        let min_win = self
                            .gateway
                            .min_serving_context_window(chain, est_u32)
                            .await;
                        let window = min_win.map(|w| w.saturating_sub(est_u32));
                        let ceiling = match (out, window) {
                            (Some(a), Some(b)) => Some(a.min(b)),
                            (a, b) => a.or(b),
                        };
                        let binding = match (ceiling, window, min_win) {
                            (Some(c), Some(term), Some(raw)) if term == c => Some(raw),
                            _ => None,
                        };
                        (ceiling, binding)
                    }
                    None => (None, None),
                };
                // The floor again, now against the bound that will ACTUALLY be emitted.
                // The check above tests the budget allowance alone, which is the right
                // question for "has this run got enough left"; this one catches the case
                // where the run has budget to spare but the MODEL cannot fit a useful
                // reply beside the prompt. Without it a window term of 0 (a prompt at or
                // past the whole context window) would emit `max_tokens: Some(0)` —
                // "generate nothing" — which the paragraph above rules out for the output
                // limit and which is no more acceptable here.
                //
                // Deliberately a refusal rather than "ignore a ceiling under the floor":
                // ignoring it sends an over-large `max_tokens` at a model that cannot
                // take it, which is the 400 this whole block exists to avoid.
                //
                // `window` is threaded through so the refusal can name the term that
                // actually bound it. This USED to be recorded here as a known gap — the
                // message read as a budget problem whatever the cause and named a raise
                // that could not help — which was survivable while SP-DATA-5 shipped
                // alone and stopped being so when SP-7a deleted the orchestrator's
                // pre-dispatch window halt: that halt used to fire FIRST on a budgeted
                // run and fail the node with a window message, so this arm is now the
                // only thing an over-window budgeted run ever sees.
                if ceiling.is_some_and(|c| u64::from(c) < orchestrator_core::MIN_OUTPUT_TOKENS) {
                    return Ok(Err(Refusal::BudgetExhausted {
                        spent,
                        budget: cap,
                        cause: BudgetRefusal::BelowFloor {
                            allowance: ceiling.map_or(allowance, u64::from),
                            est_input: est,
                            window: binding_window,
                        },
                    }));
                }
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
                    // Bound before it is stored, rather than read back out of
                    // `max_tokens` afterwards: the signals below need this number and
                    // reading it back would need an `expect` on an `Option` this line
                    // just filled.
                    let emitted = max_tokens.map_or(want, |caller| caller.min(want));
                    *max_tokens = Some(emitted);
                    clamp = Some(ClampRecord {
                        allowance,
                        emitted: u64::from(emitted),
                        est_input: est,
                    });
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
        // The clamp's two diagnostics, and they say DIFFERENT things: the first that our
        // budget cut a reply short, the second that our estimate of the input was low.
        //
        // Both are `tracing` records and neither is a journal event — a decision, not a
        // deferral (design §5.4). They describe our own ESTIMATOR, not run state:
        // nothing folds them, no resume depends on them, and no operator decision keys
        // on them. A journal event would make them durable FORMAT — a `FORMAT_VERSION`
        // concern, a fold arm, and a row on every clamped call of every budgeted run —
        // to carry what the ledger already implies, since `usage` is journaled and the
        // allowance is recomputable from the fold. They earn their place by being cheap.
        //
        // Both live inside this `if let` because both are statements ABOUT the clamp.
        // Hoisting either out would make every unbudgeted call — every pre-SP-DATA-5
        // run in the system — report against an estimate that was never made, which is
        // the additivity guarantee broken in the log rather than in the request.
        if let Some(clamp) = &clamp {
            if u64::from(usage.output_tokens) >= clamp.emitted {
                // The reply stopped AT the limit we imposed, so it was almost certainly
                // cut short rather than finished. Inferred from the token count because
                // `InferenceResponse` carries no finish reason — only a streaming chunk
                // does — so this is the available signal, not the ideal one.
                //
                // Compared against `emitted` and not `allowance`: `emitted` is what the
                // provider was actually told, and on any chain whose model limit is
                // below the allowance (the common case for a large cap) a reply can
                // never reach `allowance` at all, so keying on that would report nothing
                // on exactly the runs where replies get truncated. Both numbers are
                // logged so the reader can tell WHICH bound bit: equal means the budget
                // truncated this reply, `emitted < allowance` means the model's own
                // limit (or a caller's own `max_tokens`) did and the budget merely did
                // not prevent it.
                //
                // `>=` rather than `==` is fail-loud: a provider that reported one token
                // more than it was allowed should still surface here, not fall silently
                // between the two comparisons.
                tracing::info!(
                    max_tokens = clamp.emitted,
                    allowance = clamp.allowance,
                    output_tokens = u64::from(usage.output_tokens),
                    "budget clamp bit: the reply stopped at the clamped output limit"
                );
            }
            if u64::from(usage.input_tokens) > clamp.est_input {
                // The residual overshoot the design's §4 bounds, and the reason the
                // claim is "bounded and biased safe" rather than "eliminated": the total
                // exceeds the remaining budget by exactly `actual_input − est_input`.
                // Emitted so that term is MEASURABLE in production instead of assumed —
                // the provider reports the real input count at the same chokepoint that
                // made the estimate, which is the only feedback loop a `chars / 3`
                // heuristic can have.
                tracing::warn!(
                    estimated = clamp.est_input,
                    actual = u64::from(usage.input_tokens),
                    "budget clamp under-estimated the input; the cap may be exceeded by \
                     the difference"
                );
            }
        }
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
                // The two BUDGET messages start `budget: ` — that prefix is the operator-
                // and test-visible marker for "this pause is about the cap". The third,
                // `BelowFloor { window: Some(_) }`, deliberately does NOT: it travels the
                // same durable-pause path but the cap is not what refused it, and saying
                // "budget" there sends the operator to the one lever that cannot help.
                // Nothing in torii matches these prefixes in production code (checked);
                // the convention is for humans and for this file's own tests.
                let reason = match cause {
                    // Names a concrete lower bound, not just the arithmetic. It used to
                    // end at "raise it", which is a dead end precisely where an operator
                    // most needs a number: this is the arm a run lands on after applying
                    // a stale figure from the floor arm below, so it is the SECOND round
                    // trip of a sequence that is manual at every step. "More than
                    // {spent}" is the honest bound — it is what unblocks the gate — and
                    // the caveat is stated rather than implied, because clearing the gate
                    // only to refuse on the floor one line later is the same round trip
                    // wasted again.
                    BudgetRefusal::Spent => format!(
                        "budget: {spent} of {budget} tokens spent; raise the cap above \
                         {spent} — and far enough above it that the next call's input \
                         estimate still leaves {} tokens, or it will refuse on the floor \
                         instead; raise it with `torii run wake --budget-tokens N`",
                        orchestrator_core::MIN_OUTPUT_TOKENS
                    ),
                    // The floor. Reusing the wording above here would report a spend
                    // that did not happen ("0 of 300 tokens spent" on a fresh run) and
                    // give no hint of how far the cap must move. The raise it names is
                    // the SMALLEST one that unblocks THIS call — a later, longer prompt
                    // can still land under the floor at that cap, which is why it says
                    // "at least".
                    //
                    // Computed from the same three terms the refusal itself used, and
                    // deliberately NOT from the allowance. A dispatch needs
                    // `(cap − spent) − est >= floor`, so the smallest cap that clears it
                    // is `spent + est + floor`. The tempting one-liner
                    // `budget + (floor − allowance)` is algebraically identical WHENEVER
                    // `est <= remaining` — and silently wrong when it is not, because
                    // `allowance` is `remaining.saturating_sub(est)` and the saturation
                    // has already thrown away `est − remaining`. That is the arm AC5
                    // exists for, so it is exactly where the operator would be handed a
                    // number that re-pauses the run on the same node.
                    //
                    // `saturating_add` for the u64 sum: it cannot realistically overflow
                    // (a cap near `u64::MAX` is not a budget), and a wrap would name a
                    // raise SMALLER than the current cap.
                    //
                    // **And it is stated as HEADROOM ABOVE THE FINAL SPEND, not as an
                    // absolute cap.** `spent` here is the ledger as it stood at THIS
                    // call, and the drive does not stop when a node pauses: `drive`'s
                    // `for node in ready` loop marks the refusing node terminal and keeps
                    // going, so an independent sibling can dispatch afterwards and push
                    // the ledger past any absolute figure computed here. An operator who
                    // applied that figure verbatim would re-pause — on the `Spent` arm,
                    // whose message named no figure at all, so the second round trip had
                    // nothing to work from. Every round trip is manual: this pause is
                    // `resume_after: None`, so it is a `BudgetRaised` plus a `force_wake`
                    // by a human each time.
                    //
                    // A headroom cannot go stale, because it is relative to whatever the
                    // ledger ends at rather than to what it read mid-drive. The absolute
                    // is still shown — it is the right answer when nothing else spends,
                    // which is the common single-node case — but it is labelled as a
                    // floor under the real requirement rather than as the answer.
                    //
                    // **And when the WINDOW is what bound the ceiling, none of that
                    // arithmetic applies and the message says something else entirely.**
                    // The window term is `min_serving_context_window(chain, est) − est`,
                    // which never reads the cap, so every figure above — headroom,
                    // needed, "raise it with `torii run wake --budget-tokens N`" — names
                    // a lever that cannot move this refusal: the identical pause arrives
                    // at a cap of 1e6 and at one of `u64::MAX`. An operator who follows
                    // the budget wording raises the cap, wakes the run, and re-pauses on
                    // the same node having spent a manual round trip on nothing.
                    //
                    // **What this arm MEANS is not what it meant before the serving-window
                    // bound, and the wording is rewritten rather than rewired.** The term
                    // was the chain MINIMUM, so this message used to fire on a prompt too
                    // big for the chain's weakest entry — including one too big for every
                    // entry — and named "the chain's smallest context window", a model the
                    // request may never have been routed to. Now the window is the
                    // smallest that CAN HOLD the input, so reaching this arm proves the
                    // input fits and the shortfall is output room; and the fits-nothing
                    // case does not reach it at all (empty serving set ⇒ no window term ⇒
                    // the gate refuses, naming every candidate). The old remedy — "route
                    // to a chain whose smallest model has a larger window" — was therefore
                    // pointing at a model that may already have been filtered out of the
                    // decision, where widening it changes nothing.
                    //
                    // The prefix differs too (`context window: ` rather than `budget: `),
                    // which is deliberate: that prefix is the operator- and test-visible
                    // marker for what a pause is ABOUT, and this one is not about the
                    // cap. It is still a `BudgetExhausted` refusal on the same durable
                    // pause path — `resume_after: None`, SP-DATA-3's HOTL class — because
                    // the run is worth preserving for an operator who widens the chain,
                    // where a node failure would destroy it.
                    BudgetRefusal::BelowFloor {
                        allowance,
                        est_input,
                        window: Some(window),
                    } => {
                        let floor = orchestrator_core::MIN_OUTPUT_TOKENS;
                        format!(
                            "context window: this call's input is estimated at {est_input} \
                             tokens; the smallest model in this chain that can hold it has \
                             a {window}-token context window, leaving {allowance} for \
                             output — below the {floor}-token floor, so the reply would be \
                             cut off mid-sentence. The budget is not the binding term \
                             ({spent} of {budget} spent) and raising the cap does not move \
                             this: send less input, or put a model with a larger window in \
                             this chain."
                        )
                    }
                    BudgetRefusal::BelowFloor {
                        allowance,
                        est_input,
                        window: None,
                    } => {
                        let floor = orchestrator_core::MIN_OUTPUT_TOKENS;
                        let headroom = est_input.saturating_add(floor);
                        let needed = spent.saturating_add(headroom);
                        format!(
                            "budget: only {allowance} tokens left for output after the input \
                             estimate, below the {floor}-token floor ({spent} of {budget} \
                             spent); the cap must exceed this run's final spend by at least \
                             {headroom} (≥ {needed} if nothing else in this drive spends — \
                             independent nodes may still run after this pause and push it \
                             higher); raise it with `torii run wake --budget-tokens N`"
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

// ------------------------------------------------------------------------------------
// This module has no `mod tests`, and that is a deliberate deletion rather than a gap.
//
// It held `every_part_of_the_input_is_counted_in_the_estimate` and
// `an_assistant_turns_tool_call_arguments_are_counted` over `est_input_tokens`. Both
// went with that function (see its tombstone above `ClampRecord`); the coverage did NOT.
// The clamp's estimate is now `gateway::estimate_input_tokens_pessimistic`, whose own
// module holds the same claims over a whole `Payload`, and holds them as exact
// equalities rather than the strict `>` these used:
//
// - the system prompt: `the_system_prompt_is_counted_in_the_window_estimate`
// - a tool's name + description + schema: `a_tools_json_schema_is_counted_not_just_the_tools_name`
// - an assistant turn's `tool_calls`: `an_assistant_turns_tool_call_arguments_are_counted`
// - the divisor and the rounding: `the_estimate_is_ceil_of_utf8_bytes_over_three`
//
// Re-asserting them here would test the same function twice and, worse, imply that the
// clamp still has an estimate of its own to verify — which is the belief the deleted
// function encoded. What the clamp owes a test is that it bounds by the figure the GATE
// will judge the same request by, and that is
// `the_clamp_bounds_max_tokens_by_the_estimate_the_gate_will_judge_by` plus
// `the_serving_window_bound_is_safe_for_the_smallest_candidate_the_gate_admits` in
// `executor/tests.rs`. No unit test of an estimator can stand in for either.
// ------------------------------------------------------------------------------------
