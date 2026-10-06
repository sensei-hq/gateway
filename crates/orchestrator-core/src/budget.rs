//! SP-DATA-5: the per-run token budget. Deliberately tiny and pure — the ledger
//! lives in the journal (see `JournalEvent::EffectRecorded.usage`), not here.

use serde::{Deserialize, Serialize};

/// A per-run cap on total tokens, journaled on `RunStarted` and raisable via
/// `BudgetRaised`.
///
/// This caps CONSUMPTION, not spend: 50k tokens costs very different amounts across
/// models. Money denomination is deferred (spec §8) because it needs durable,
/// current per-model pricing, and a stale price would silently make the cap wrong.
///
/// It is not a hard ceiling, and the shape of the slack has TWO parts.
///
/// The gate tests already-accumulated spend before each call — a floor-trigger — so on
/// its own it permits an overshoot of one whole call. The SP-DATA-5 clamp then bounds
/// that call's OUTPUT half by setting `max_tokens` to what the remaining budget affords,
/// which leaves only the INPUT half unbounded: the residual is
/// `actual_input − est_input`, biased toward refusing early by a pessimistic estimate.
/// Bounded and biased safe, not eliminated.
///
/// The same clamp means a budgeted run can PAUSE WITH `spent < cap`, which is the wrong
/// mental model to be missing when debugging one. Once the allowance left after the
/// input estimate falls under [`MIN_OUTPUT_TOKENS`], the run refuses rather than paying
/// for a reply too short to be useful, and says by how much the cap must rise. So
/// `spent >= cap` is not the only refusal.
///
/// `Copy`: a single immutable `u64` cap value, not a handle to shared mutable state
/// — the one-source-of-truth concern that `Copy` would threaten belongs to the
/// spend LEDGER (the fold's `HashMap<EffectId, TokenUsage>` + accumulated
/// `Option<u64>`, see the executor's `Fold`), not to this cap. Copying the cap
/// around cheaply is exactly what a plain value type should do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenBudget {
    pub total_tokens: u64,
}

/// AG-12: a per-run cap on MONEY, in integer micro-dollars (1 USD = 1 000 000).
///
/// Journaled on `RunStarted.money_budget` beside (or instead of) the token cap, and
/// moved by `JournalEvent::MoneyBudgetRaised`. torii derives the figure from its
/// individual/group/org caps (torii#41); the engine only enforces it.
///
/// Integer, never `f64`: this value and the spend it is compared against live in the
/// journal and the fold, and a float there would make "spent >= cap" depend on the
/// summation ORDER of the ledger — a resume that folds the same effects in a different
/// order could land either side of the cap. Prices are `f64` in the gateway's config;
/// they are converted to micro-dollars once per call, rounding toward the safe side,
/// and only the integer is ever stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoneyBudget {
    pub total_micro_usd: u64,
}

/// Micro-dollars per US dollar — the unit [`MoneyBudget`] and
/// [`TokenUsage::cost_micro_usd`] are denominated in.
pub const MICRO_USD_PER_USD: u64 = 1_000_000;

/// AG-12: both run caps together, as a submitter hands them to
/// `Executor::run_with_budget` / `Scheduler::submit_with_budget`. Either, both or
/// neither may be set; `RunBudget::default()` is an unbudgeted run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunBudget {
    pub tokens: Option<TokenBudget>,
    pub money: Option<MoneyBudget>,
}

/// Mirrors `kernel::types::cost::TokenUsage`. Defined locally because
/// `orchestrator-core` deliberately depends on nothing else in the workspace; the
/// executor converts at the boundary. `Copy` for the same reason as `TokenBudget`:
/// a plain reported-usage value, not the accumulating ledger itself.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub total_tokens: u32,
    /// AG-12: what the gateway priced this call at (`InferenceResponse.actual_cost`),
    /// in micro-dollars rounded UP. Rides on the usage — and therefore on
    /// `EffectRecorded.usage` and `CompactChild.usage` — so the money ledger is keyed
    /// by effect id exactly like the token ledger and inherits every idempotency
    /// argument the token ledger already makes (duplicate `Confirmed` records, Map
    /// compaction, resume).
    ///
    /// `Some` only on a run with a money cap in force when the call was made: an
    /// unbudgeted or token-only run journals byte-identically to before.
    #[serde(default)]
    pub cost_micro_usd: Option<u64>,
}

/// The smallest output allowance worth spending input tokens on (SP-DATA-5 clamp).
///
/// Below this, the metered-dispatch chokepoint refuses rather than clamping: a reply
/// truncated to a handful of tokens still costs the full input, arrives mid-sentence,
/// and flows downstream as work product with no signal that it was cut short. The
/// existing `BudgetExhausted` pause is louder, recoverable
/// (`torii run wake --budget-tokens`), and already built.
///
/// One constant rather than a per-agent knob, deliberately: a gate agent answering one
/// word needs far less and a planner emitting a graph needs far more, so this WILL be
/// wrong for somebody — which is the argument for the deferred per-role setting (see
/// the clamp design's §8), not for tuning this number without data.
pub const MIN_OUTPUT_TOKENS: u64 = 256;

/// The fraction of an agent's requested dependency context that must SURVIVE budgeting for the
/// turn to be worth dispatching. Below it, SP-7b refuses instead of degrading.
///
/// **This number is a judgment call and there is no evidence behind it yet.** The argument for
/// having a floor at all is sound: unbounded degradation answers a 200 000-token question from 4%
/// of its context, confidently, and the model has no way to know it is unqualified — it will
/// answer anyway, and that answer flows downstream as work product. The argument for `0.25`
/// specifically is only that a quarter of what the graph decided the agent needed is the point
/// where "answering" starts to look like guessing.
///
/// The AC10 `tracing::warn!` exists to replace this guess with a measurement. When there is
/// fleet data on real degradation ratios, set it from that and delete this paragraph.
pub const CONTEXT_FLOOR_FRACTION: f64 = 0.25;

#[cfg(test)]
mod tests {
    use super::*;

    /// The worst case must land on the REFUSE side of the floor.
    ///
    /// The chokepoint's predicate is `allowance < MIN_OUTPUT_TOKENS`, and the worst
    /// allowance it can compute is `0` — the input estimate has eaten the entire
    /// remaining budget and `saturating_sub` bottomed out. A floor of `0` makes that
    /// comparison unreachable for a `u64`, so the refusal arm dies and the run
    /// dispatches with `max_tokens: Some(0)` instead of pausing: the input tokens are
    /// paid for, the reply is empty or a provider error, and the operator sees neither
    /// a pause nor an answer.
    ///
    /// Stated as the predicate rather than as `MIN_OUTPUT_TOKENS > 0` because the two
    /// are the same claim and only one of them says WHICH behaviour depends on it. The
    /// exact VALUE is a judgement call the design's §6 says plainly will be wrong for
    /// somebody, so it is deliberately not pinned here — asserting `== 256` would test
    /// the literal against itself and make a future re-tuning look like a regression.
    #[test]
    fn an_allowance_of_zero_is_below_the_floor() {
        let nothing_left: u64 = 0;
        assert!(
            nothing_left < MIN_OUTPUT_TOKENS,
            "a zero floor makes `allowance < MIN_OUTPUT_TOKENS` unreachable, so the \
             clamp would dispatch max_tokens: Some(0) rather than refuse"
        );
    }
}
