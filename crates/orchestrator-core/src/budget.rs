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
