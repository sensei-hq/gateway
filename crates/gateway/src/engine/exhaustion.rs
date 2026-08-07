use crate::selection::SkippedCandidate;
use crate::skip_reason::GateStatus;
use crate::types::error::{GatewayError, HumanAction};
use chrono::{DateTime, Utc};
use std::time::Instant;

/// One attempted candidate's contribution to exhaustion aggregation.
pub(super) enum GateContribution {
    /// Attempted, failed with a recoverable limit that locked it until `Instant`.
    Timed(Instant),
    /// Attempted, failed with a terminal limit needing caller action.
    Terminal(HumanAction),
    /// Attempted, failed with a non-limit fault (500 / network / unclassified).
    HardFailure,
}

/// Anchor a monotonic `Instant` deadline to wall-clock. Past/elapsed ⇒ now.
pub(super) fn instant_to_utc(until: Instant) -> DateTime<Utc> {
    let remaining = until.saturating_duration_since(Instant::now());
    Utc::now() + chrono::Duration::from_std(remaining).unwrap_or_else(|_| chrono::Duration::zero())
}

/// A failed attempt's contribution, from its error + the `Instant` the recorder
/// pipeline just wrote (for a recoverable limit that locked it).
pub(super) fn contribution_for(
    err: &GatewayError,
    written_until: Option<Instant>,
) -> GateContribution {
    use crate::gates::lockout::{LockReason, classify};
    match classify(err) {
        Some(r) if r.is_recoverable() => match written_until {
            Some(u) => GateContribution::Timed(u),
            None => GateContribution::HardFailure,
        },
        Some(LockReason::CreditsExhausted) => GateContribution::Terminal(HumanAction::TopUpCredits),
        Some(_) => GateContribution::Terminal(HumanAction::RotateCredential), // Auth
        None => GateContribution::HardFailure,
    }
}

/// Build the terminal error at chain exhaustion. `Some(AllGated)` iff every
/// candidate was gated (health-skip or classified limit) and none hard-failed;
/// `None` ⇒ "not all-gated — use the existing terminal error".
pub(super) fn all_gated_error(
    skipped: &[SkippedCandidate],
    contributions: &[GateContribution],
) -> Option<GatewayError> {
    if contributions
        .iter()
        .any(|c| matches!(c, GateContribution::HardFailure))
    {
        return None;
    }
    let mut timed: Vec<Instant> = Vec::new();
    let mut human: Option<HumanAction> = None;
    let mut diagnostics: Vec<String> = Vec::new();
    let mut any_gate = false;

    for s in skipped {
        match s.reason.gate_status() {
            GateStatus::Timed(u) => {
                any_gate = true;
                timed.push(u);
            }
            GateStatus::Terminal(h) => {
                any_gate = true;
                let _ = human.get_or_insert(h);
            }
            GateStatus::Structural => {}
        }
        diagnostics.push(format!("{}:{} — {}", s.router, s.model, s.reason));
    }
    for c in contributions {
        match c {
            GateContribution::Timed(u) => {
                any_gate = true;
                timed.push(*u);
            }
            GateContribution::Terminal(h) => {
                any_gate = true;
                let _ = human.get_or_insert(*h);
            }
            GateContribution::HardFailure => {}
        }
    }
    if !any_gate {
        return None;
    }
    let resume_after = timed.into_iter().min().map(instant_to_utc);
    let human_action = if resume_after.is_some() { None } else { human };
    Some(GatewayError::AllGated {
        resume_after,
        skipped: diagnostics,
        human_action,
    })
}
