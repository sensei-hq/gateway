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
            // Defensive fallback, not a live path: a recoverable classification
            // always drives `ModelLockoutSink` to write a timed deadline (`Some`),
            // so `written_until == None` here shouldn't occur — treat it as a hard
            // failure (the safe default) rather than a timed lock with no deadline.
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
    // BOTH, when both are known. `resume_after` decides whether the caller pauses
    // (`classify_gateway_error` pauses on `Some(t)` and fails on `None`) and is taken
    // from the TIMED gates alone, which is unchanged; `human_action` is diagnosis, and it
    // used to be nulled out whenever a wake had been scheduled — "a timed retry wins over
    // the terminal remedy".
    //
    // That discarded information the caller cannot recover, and SP-7a is what made the
    // loss bite. `OverContextWindow` is the first terminal reason no elapsed time can
    // EVER clear: a chain whose one healthy model is too small and whose other model is
    // breaker-open now pauses for the breaker's five minutes, wakes, finds the request
    // still does not fit, and only then fails — never having said that the prompt was
    // always too large. Carrying both says "we will retry at t, AND here is the thing
    // that will still be true then".
    //
    // Not a change to the pause/fail split: the caller keys on `resume_after`, and a
    // remedy beside it is strictly more to read. The one thing it must not do is make an
    // all-TIMED exhaustion claim a remedy it does not have, and it cannot: `human` is
    // only ever set from a `Terminal` status.
    Some(GatewayError::AllGated {
        resume_after,
        skipped: diagnostics,
        human_action: human,
    })
}
