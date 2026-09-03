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
    /// would not distinguish them. `AllGated` renders these strings verbatim — it did
    /// NOT when this variant landed, which made the sentence you are reading false for
    /// two commits; `GatewayError`'s `#[error]` dropped the whole `skipped` vector, and
    /// widening it is what made the numbers reach an operator.
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
            // is this REQUEST that does not fit it. Structural skips contribute nothing
            // to `all_gated_error`, so a selection skipped entirely on structural
            // grounds is not "gated" at all and surfaces as a bare `NoCandidates`
            // (`engine/execute.rs`), which names no cause and no remedy. Terminal
            // carries a `HumanAction` that names the lever.
            //
            // What Terminal does NOT do — despite what an earlier version of this
            // comment asserted — is make the run PAUSABLE. `all_gated_error` takes
            // `resume_after` from the TIMED skips alone, so an all-over-window selection
            // is `AllGated { resume_after: None }`, and `classify_gateway_error`
            // (orchestrator `executor/support.rs`) pauses only on `Some(t)`; everything
            // else fails the node. That is deliberate and predates this slice — risk M1
            // in `docs/design/selection-policy-pipeline.md` resolved it as "terminal-only
            // ⇒ fail-fast human-action, never pause", and `GatewayError::AllGated`'s own
            // doc says the caller must not pause forever. So what this variant buys is a
            // better-DIAGNOSED terminal failure, not a recoverable one: each candidate's
            // own window and the estimate that exceeded it, in place of the
            // orchestrator's single chain-minimum guess.
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
    /// configured; it is this REQUEST that does not fit it, and a structural skip
    /// contributes nothing to `all_gated_error`, so the caller would see a bare
    /// `NoCandidates` naming neither cause nor remedy. Terminal is the classification
    /// that carries a `HumanAction`.
    ///
    /// It does not make the run pausable — see `gate_status`'s comment for why a
    /// deadline-less `AllGated` is a terminal failure by design. What is bought here is
    /// the DIAGNOSIS, which is why the message assertion below is exact rather than
    /// substring-loose: `contains("20000") && contains("8192")` also passes when the two
    /// placeholders are swapped, and "estimated 8192 tokens exceeds the model's
    /// 20000-token window" tells an operator to shrink a prompt that is already small
    /// and to leave alone the chain that is actually wrong. The numbers only help if
    /// they are the right way round.
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
        assert_eq!(
            r.to_string(),
            "estimated 20000 input tokens exceeds the model's 8192-token context window",
            "the message must name the estimate and the window, each labelled by which \
             it is — this string is what reaches an operator through AllGated's Display"
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
