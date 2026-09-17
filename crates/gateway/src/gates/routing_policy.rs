use super::{AdmissionGate, CandidateView, GateVerdict, SelectionCtx};
use crate::skip_reason::SkipReason;
use crate::types::request::{CandidateSet, RoutingPreferences};

/// Gate: the request's own `only`/`ignore` preferences must not exclude this
/// candidate.
///
/// **Registered FIRST** in `ModelSelectionService::new`, and that position is
/// load-bearing twice over. `admit` returns the FIRST skip, so position decides
/// which reason a multiply-gated candidate reports: the caller's own instruction
/// is the most specific explanation available, and reporting "circuit breaker
/// open" for a candidate the caller explicitly excluded is misleading. More
/// importantly `ExcludedByPolicy` is `Structural`, which contributes nothing to
/// `all_gated_error`'s `any_gate` — so an excluded candidate must NOT donate its
/// breaker deadline to `resume_after`. Waiting never makes an excluded candidate
/// eligible, so a run that pauses on one waits for nothing.
pub struct RoutingPolicyGate;

impl AdmissionGate for RoutingPolicyGate {
    fn name(&self) -> &'static str {
        "routing_policy"
    }

    fn evaluate(&self, c: &CandidateView<'_>, x: &SelectionCtx<'_>) -> GateVerdict {
        if admitted_by_policy(x.preferences, c.router, c.model) {
            GateVerdict::Admit
        } else {
            GateVerdict::Skip(SkipReason::ExcludedByPolicy)
        }
    }
}

/// `only` then `ignore`, in that order.
pub(crate) fn admitted_by_policy(
    prefs: Option<&RoutingPreferences>,
    router: &str,
    model: &str,
) -> bool {
    let Some(p) = prefs else { return true };
    if let Some(only) = &p.only
        && !matches_all_axes(only, router, model)
    {
        return false;
    }
    if let Some(ignore) = &p.ignore
        && matches_any_axis(ignore, router, model)
    {
        return false;
    }
    true
}

/// AND across non-empty axes — an empty list is don't-care.
fn matches_all_axes(s: &CandidateSet, router: &str, model: &str) -> bool {
    let router_ok = s.routers.is_empty() || s.routers.iter().any(|r| r == router);
    let model_ok = s.models.is_empty() || s.models.iter().any(|m| m == model);
    router_ok && model_ok
}

/// OR across non-empty axes.
fn matches_any_axis(s: &CandidateSet, router: &str, model: &str) -> bool {
    s.routers.iter().any(|r| r == router) || s.models.iter().any(|m| m == model)
}

#[cfg(test)]
mod tests {
    use crate::types::request::{CandidateSet, RoutingPreferences};

    fn set(routers: &[&str], models: &[&str]) -> CandidateSet {
        CandidateSet {
            routers: routers.iter().map(|s| s.to_string()).collect(),
            models: models.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn admits(p: &RoutingPreferences, router: &str, model: &str) -> bool {
        super::admitted_by_policy(Some(p), router, model)
    }

    /// `only` is AND across NON-EMPTY axes. An empty axis constrains nothing —
    /// the convention `TierPredicate` already uses ("every PRESENT field must
    /// match; an absent field is don't-care").
    #[test]
    fn only_is_and_across_non_empty_axes() {
        let p = RoutingPreferences {
            only: Some(set(&["anthropic"], &["claude-haiku"])),
            ..Default::default()
        };
        assert!(admits(&p, "anthropic", "claude-haiku"));
        assert!(
            !admits(&p, "anthropic", "claude-opus"),
            "model axis must bind"
        );
        assert!(
            !admits(&p, "bedrock", "claude-haiku"),
            "router axis must bind"
        );

        let routers_only = RoutingPreferences {
            only: Some(set(&["anthropic"], &[])),
            ..Default::default()
        };
        assert!(
            admits(&routers_only, "anthropic", "anything"),
            "an EMPTY axis is don't-care, not 'match nothing'"
        );
    }

    /// `ignore` is OR across non-empty axes. AND would exclude only the single
    /// named pair, which is not what "ignore this router and that model" means.
    #[test]
    fn ignore_is_or_across_non_empty_axes() {
        let p = RoutingPreferences {
            ignore: Some(set(&["ollama"], &["claude-opus"])),
            ..Default::default()
        };
        assert!(!admits(&p, "ollama", "gemma3:27b"), "router match excludes");
        assert!(
            !admits(&p, "anthropic", "claude-opus"),
            "model match excludes"
        );
        assert!(
            admits(&p, "anthropic", "claude-haiku"),
            "neither ⇒ admitted"
        );
    }

    /// `ignore` is applied AFTER `only`, so it can subtract from an allowlist.
    /// Reversing the order would let `only` re-admit an ignored candidate.
    #[test]
    fn ignore_subtracts_from_only() {
        let p = RoutingPreferences {
            only: Some(set(&["anthropic"], &[])),
            ignore: Some(set(&[], &["claude-opus"])),
            ..Default::default()
        };
        assert!(admits(&p, "anthropic", "claude-haiku"));
        assert!(!admits(&p, "anthropic", "claude-opus"));
    }

    /// No preferences at all ⇒ everything admitted. The additive guarantee.
    #[test]
    fn absent_preferences_admit_everything() {
        let p = RoutingPreferences::default();
        assert!(admits(&p, "any", "thing"));
        assert!(super::admitted_by_policy(None, "any", "thing"));
    }
}
