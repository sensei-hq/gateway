//! Reconciliation of an in-doubt `Mutation` on resume (§7.3): decide whether the
//! side effect already applied, so the executor never blind-re-runs or blind-memoizes.

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::effect::EffectId;
use crate::error::OrchestratorError;

/// The verdict of reconciling an in-doubt Mutation against the world.
#[derive(Debug, Clone, PartialEq)]
pub enum ReconcileOutcome {
    /// It DID apply — here is the recorded output; memoize it, do not re-run.
    Confirmed(serde_json::Value),
    /// It did NOT apply — the world is unchanged; safe to run the effect now.
    NotApplied,
    /// Cannot determine — the executor must pause loud (never guess).
    Indeterminate,
}

/// A per-tool reconciler queried when a Mutation is in-doubt on resume.
#[async_trait]
pub trait ReconcileProvider: Send + Sync {
    async fn reconcile(
        &self,
        idempotency_key: &str,
        args: &serde_json::Value,
    ) -> Result<ReconcileOutcome, OrchestratorError>;
}

/// The default idempotency key for a Mutation effect: `sha256(effect_id | args_hash)`
/// — deterministic, so a resumed intent maps to the same key.
pub fn idempotency_key(effect_id: &EffectId, args_hash: &str) -> String {
    let mut h = Sha256::new();
    h.update(format!("{}|{}", effect_id.0, args_hash).as_bytes());
    format!("{:x}", h.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effect::effect_id;

    #[test]
    fn idempotency_key_is_stable_and_input_bound() {
        let e = effect_id("n", 0, 1);
        assert_eq!(idempotency_key(&e, "a"), idempotency_key(&e, "a"));
        assert_ne!(idempotency_key(&e, "a"), idempotency_key(&e, "b"));
    }
}
