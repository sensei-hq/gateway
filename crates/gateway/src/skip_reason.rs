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
    /// The candidate's context window cannot hold this request's estimated input.
    ///
    /// Carries both numbers because the remedy depends on the gap: a request slightly
    /// over a small model's window is a ROUTING problem (widen or reorder the chain),
    /// and one over every window is a PROMPT problem (send less). A single "too big"
    /// would not distinguish them, and `AllGated` renders these strings verbatim, so
    /// whatever is not here is not recoverable by the operator reading it.
    ///
    /// `estimated` is the pessimistic figure from
    /// `engine::util::estimate_input_tokens_pessimistic`, NOT the cost estimate — see
    /// that function for why the two deliberately differ.
    OverContextWindow {
        estimated: u32,
        window: u32,
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
            SkipReason::OverContextWindow { estimated, window } => {
                write!(
                    f,
                    "estimated {estimated} input tokens exceeds the model's \
                     {window}-token context window"
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
            // Terminal, not Timed: no deadline passes that makes a model's window
            // bigger. And not Structural either — the candidate is well configured, it
            // is this request that does not fit it, and a Structural skip would not make
            // the run pausable at all.
            SkipReason::OverContextWindow { .. } => {
                GateStatus::Terminal(HumanAction::UseLargerContextWindow)
            }
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

    /// An over-window skip is TERMINAL and points at the window, not at money.
    ///
    /// `Timed` would be wrong — no deadline passes that makes a model's window bigger —
    /// and `Structural` would be wrong too, because the candidate is perfectly well
    /// configured; it is this REQUEST that does not fit it. Structural skips do not make
    /// a run pausable at all, so classifying it there would turn an operator-fixable
    /// situation back into the hard failure this slice exists to remove.
    /// Terminal-with-a-remedy is the only classification that produces an actionable
    /// `AllGated` pause.
    #[test]
    fn over_context_window_is_terminal_and_names_the_window_remedy() {
        let r = SkipReason::OverContextWindow {
            estimated: 20_000,
            window: 8_192,
        };
        assert!(
            matches!(
                r.gate_status(),
                GateStatus::Terminal(HumanAction::UseLargerContextWindow)
            ),
            "over-window must be terminal with the window remedy, got {:?}",
            r.gate_status()
        );
        let shown = r.to_string();
        assert!(
            shown.contains("20000") && shown.contains("8192"),
            "the message must name BOTH the estimate and the window it exceeded, so an \
             operator can see how far over they are: {shown}"
        );
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
