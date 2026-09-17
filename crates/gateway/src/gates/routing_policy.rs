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

/// A candidate is admitted iff it satisfies `only` AND is not named by `ignore`.
fn admitted_by_policy(prefs: Option<&RoutingPreferences>, router: &str, model: &str) -> bool {
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

        let models_only = RoutingPreferences {
            only: Some(set(&[], &["claude-haiku"])),
            ..Default::default()
        };
        assert!(
            admits(&models_only, "bedrock", "claude-haiku"),
            "an EMPTY ROUTERS axis is don't-care too"
        );
        assert!(
            !admits(&models_only, "bedrock", "claude-opus"),
            "and the non-empty models axis still binds"
        );
    }

    /// Degenerate case: BOTH axes of `only` empty admits everything, same as
    /// `only` being entirely absent. Reachable from the wire as
    /// `{"routing":{"only":{}}}`.
    #[test]
    fn only_with_both_axes_empty_admits_everything() {
        let p = RoutingPreferences {
            only: Some(CandidateSet::default()),
            ..Default::default()
        };
        assert!(admits(&p, "any", "thing"));
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

    /// A model id containing a colon (`"gemma3:27b"`) must still match via the
    /// MODELS axis — the two axes are separate fields precisely because the
    /// `"{router}:{model}"` endpoint key can't be parsed back apart, so this
    /// checks the model string is compared whole, not split on `:`.
    #[test]
    fn ignore_matches_a_colon_bearing_model_id_via_the_models_axis() {
        let p = RoutingPreferences {
            ignore: Some(set(&[], &["gemma3:27b"])),
            ..Default::default()
        };
        assert!(!admits(&p, "ollama", "gemma3:27b"));
        assert!(admits(&p, "ollama", "claude-haiku"));
    }

    /// `only` and `ignore` compose as a conjunction — passing the allowlist does
    /// not exempt a candidate from the denylist. The two are order-independent by
    /// construction (both are pure predicates with an early `return false`); the
    /// spec's `only → ignore` names pipeline stages, not an observable sequence.
    #[test]
    fn ignore_excludes_even_when_only_admits() {
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
