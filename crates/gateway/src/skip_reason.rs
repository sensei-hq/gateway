use crate::types::capability::Capability;
use std::time::Instant;

/// Why a candidate was excluded during selection.
#[derive(Debug, Clone)]
pub enum SkipReason {
    ModelNotFound,
    RouterNotFound,
    RouterDisabled,
    UnsupportedCapability(Capability),
    OverBudget { estimated: f64, budget: f64 },
    CircuitOpen { until: Instant },
}

impl SkipReason {
    pub fn until(&self) -> Option<Instant> {
        match self {
            SkipReason::CircuitOpen { until } => Some(*until),
            _ => None,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            SkipReason::ModelNotFound | SkipReason::UnsupportedCapability(_)
        )
    }
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
        assert!(SkipReason::ModelNotFound.until().is_none());
        assert!(!SkipReason::RouterNotFound.is_terminal());
    }
}
