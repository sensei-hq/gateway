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
/// It is a FLOOR-TRIGGER, not a ceiling. The gate tests already-accumulated spend
/// before each call, and output tokens are unknowable before the call returns, so a
/// budget can be overshot by at most one call.
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

/// Mirrors `kernel::types::cost::TokenUsage`. Defined locally because
/// `orchestrator-core` deliberately depends on nothing else in the workspace; the
/// executor converts at the boundary. `Copy` for the same reason as `TokenBudget`:
/// a plain reported-usage value, not the accumulating ledger itself.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub total_tokens: u32,
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
