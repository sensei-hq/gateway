use crate::types::capability::Capability;
use crate::types::error::HumanAction;
use std::time::Instant;

/// How a skip participates in exhaustion aggregation (Task 4). `Timed` gates clear
/// on their own at `Instant`; `Terminal` gates need caller action; `Structural`
/// skips (misconfig / wrong capability) are not "gated" and don't make a run pausable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateStatus {
    Timed(std::time::Instant),
    Terminal(HumanAction),
    Structural,
}

/// Why a candidate was excluded during selection.
#[derive(Debug, Clone)]
pub enum SkipReason {
    ModelNotFound,
    RouterNotFound,
    RouterDisabled,
    UnsupportedCapability(Capability),
    OverBudget {
        estimated: f64,
        budget: f64,
    },
    CircuitOpen {
        until: Instant,
    },
    Cooling {
        until: Instant,
    },
    LockedOut {
        reason: crate::gates::lockout::LockReason,
        until: Option<Instant>,
    },
}

impl std::fmt::Display for SkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SkipReason::ModelNotFound => write!(f, "model not found"),
            SkipReason::RouterNotFound => write!(f, "router not found"),
            SkipReason::RouterDisabled => write!(f, "router disabled"),
            SkipReason::UnsupportedCapability(c) => write!(f, "does not support {c:?}"),
            SkipReason::OverBudget { estimated, budget } => {
                write!(
                    f,
                    "over budget (estimated {estimated:.4}, budget {budget:.4})"
                )
            }
            SkipReason::CircuitOpen { .. } => write!(f, "circuit breaker open"),
            SkipReason::Cooling { .. } => write!(f, "router cooling down"),
            SkipReason::LockedOut { reason, .. } => write!(f, "model locked out ({reason:?})"),
        }
    }
}

impl SkipReason {
    pub fn gate_status(&self) -> GateStatus {
        match self {
            SkipReason::Cooling { until } | SkipReason::CircuitOpen { until } => {
                GateStatus::Timed(*until)
            }
            SkipReason::LockedOut {
                until: Some(until), ..
            } => GateStatus::Timed(*until),
            SkipReason::LockedOut {
                reason,
                until: None,
            } => GateStatus::Terminal(match reason {
                crate::gates::lockout::LockReason::CreditsExhausted => HumanAction::TopUpCredits,
                _ => HumanAction::RotateCredential, // Auth (terminal); rate/quota are never terminal (until is Some)
            }),
            SkipReason::OverBudget { .. } => GateStatus::Terminal(HumanAction::RaiseBudget),
            SkipReason::ModelNotFound
            | SkipReason::RouterNotFound
            | SkipReason::RouterDisabled
            | SkipReason::UnsupportedCapability(_) => GateStatus::Structural,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn skip_reason_renders_and_classifies() {
        assert_eq!(SkipReason::RouterDisabled.to_string(), "router disabled");
        assert!(matches!(
            SkipReason::UnsupportedCapability(crate::types::capability::Capability::TextEmbed),
            SkipReason::UnsupportedCapability(_)
        ));
    }

    #[test]
    fn gate_status_classifies_each_reason() {
        use crate::gates::lockout::LockReason;
        use crate::types::error::HumanAction;
        let t = std::time::Instant::now() + std::time::Duration::from_secs(60);
        assert_eq!(
            SkipReason::Cooling { until: t }.gate_status(),
            GateStatus::Timed(t)
        );
        assert_eq!(
            SkipReason::CircuitOpen { until: t }.gate_status(),
            GateStatus::Timed(t)
        );
        assert_eq!(
            SkipReason::LockedOut {
                reason: LockReason::QuotaExhausted,
                until: Some(t)
            }
            .gate_status(),
            GateStatus::Timed(t)
        );
        assert_eq!(
            SkipReason::LockedOut {
                reason: LockReason::Auth,
                until: None
            }
            .gate_status(),
            GateStatus::Terminal(HumanAction::RotateCredential)
        );
        assert_eq!(
            SkipReason::LockedOut {
                reason: LockReason::CreditsExhausted,
                until: None
            }
            .gate_status(),
            GateStatus::Terminal(HumanAction::TopUpCredits)
        );
        assert_eq!(
            SkipReason::OverBudget {
                estimated: 1.0,
                budget: 0.5
            }
            .gate_status(),
            GateStatus::Terminal(HumanAction::RaiseBudget)
        );
        assert_eq!(
            SkipReason::ModelNotFound.gate_status(),
            GateStatus::Structural
        );
        assert_eq!(
            SkipReason::RouterNotFound.gate_status(),
            GateStatus::Structural
        );
        assert_eq!(
            SkipReason::RouterDisabled.gate_status(),
            GateStatus::Structural
        );
        assert_eq!(
            SkipReason::UnsupportedCapability(crate::types::capability::Capability::TextChat)
                .gate_status(),
            GateStatus::Structural
        );
    }
}
