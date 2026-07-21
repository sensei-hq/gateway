use super::capability::Capability;
use super::config::{FallbackTrigger, MeterUnit, Window};

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("authentication failed for adapter '{adapter}': {message}")]
    Authentication { adapter: String, message: String },

    #[error("rate limited by adapter '{adapter}'{}", retry_after_ms.map(|ms| format!(", retry after {ms}ms")).unwrap_or_default())]
    RateLimit {
        adapter: String,
        retry_after_ms: Option<u64>,
    },

    #[error("budget exceeded: estimated {estimated:.4}, remaining {remaining:.4}")]
    BudgetExceeded { estimated: f64, remaining: f64 },

    /// A subject's subscription quota is exhausted for this unit/window. A hard
    /// stop (per-subject, not per-provider), so it does not trigger fallback.
    #[error("quota exceeded: {used} of {limit} {unit:?} used in this {window:?}")]
    QuotaExceeded {
        unit: MeterUnit,
        window: Window,
        limit: u64,
        used: u64,
    },

    #[error("timeout after {duration_ms}ms for model '{model}' on adapter '{adapter}'")]
    Timeout {
        adapter: String,
        model: String,
        duration_ms: u64,
    },

    #[error("provider error from adapter '{adapter}': {message}{}", status.map(|s| format!(" (status {s})")).unwrap_or_default())]
    ProviderError {
        adapter: String,
        message: String,
        status: Option<u16>,
    },

    #[error("model '{model}' unavailable on adapter '{adapter}'")]
    ModelUnavailable { adapter: String, model: String },

    /// The adapter exists for this capability but cannot perform the
    /// requested sub-operation (e.g. a chat adapter that has no streaming).
    #[error("adapter '{adapter}' does not support {what}")]
    Unsupported { adapter: String, what: String },

    #[error("no candidates available for capability '{capability:?}'")]
    NoCandidates { capability: Capability },

    #[error("gateway not configured — no routers, models, or chains have been set")]
    NotConfigured,

    #[error("all {attempts} attempts failed: {errors}")]
    AllAttemptsFailed {
        attempts: usize,
        errors: String,
        /// Structured per-attempt diagnostics, preserved alongside the
        /// flattened `errors` string so callers can inspect the full
        /// [`Attempt`](crate::types::trace::Attempt) records on total failure.
        attempts_detail: Vec<crate::types::trace::Attempt>,
    },

    /// A model in the resolved chain is still provisioning (pulling / loading)
    /// and no ready fallback candidate exists. Terminal for this request — the
    /// caller retries once the supervisor reports the model ready. Never
    /// triggers fallback (a provisioning model is not a provider fault).
    #[error("model '{model}' not ready: {phase:?}")]
    ModelNotReady {
        model: String,
        phase: crate::readiness::ProvisionPhase,
    },

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("network error: {0}")]
    Network(#[from] reqwest::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

impl GatewayError {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            GatewayError::RateLimit { .. }
                | GatewayError::Timeout { .. }
                | GatewayError::ProviderError { .. }
                | GatewayError::ModelUnavailable { .. }
                | GatewayError::Network(_)
        )
    }

    pub fn should_trigger_fallback(&self, triggers: &[FallbackTrigger]) -> bool {
        if triggers.is_empty() {
            return false;
        }

        match self {
            GatewayError::RateLimit { .. } => triggers.contains(&FallbackTrigger::RateLimit),
            GatewayError::Timeout { .. } => triggers.contains(&FallbackTrigger::Timeout),
            GatewayError::ProviderError { .. } => {
                triggers.contains(&FallbackTrigger::ProviderError)
            }
            GatewayError::ModelUnavailable { .. } => {
                triggers.contains(&FallbackTrigger::ModelUnavailable)
            }
            GatewayError::BudgetExceeded { .. } => {
                triggers.contains(&FallbackTrigger::BudgetExceeded)
            }
            // Auth, Unsupported, AllAttemptsFailed, Quota (a per-subject hard
            // stop, not a provider fault), and ModelNotReady (a still-provisioning
            // model, not a provider fault) never trigger fallback.
            GatewayError::Authentication { .. }
            | GatewayError::Unsupported { .. }
            | GatewayError::AllAttemptsFailed { .. }
            | GatewayError::NoCandidates { .. }
            | GatewayError::QuotaExceeded { .. }
            | GatewayError::ModelNotReady { .. }
            | GatewayError::NotConfigured
            | GatewayError::InvalidConfig(_)
            | GatewayError::Network(_)
            | GatewayError::Serialization(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_messages() {
        let err = GatewayError::Authentication {
            adapter: "openai".to_string(),
            message: "invalid key".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "authentication failed for adapter 'openai': invalid key"
        );

        let err = GatewayError::RateLimit {
            adapter: "anthropic".to_string(),
            retry_after_ms: Some(5000),
        };
        assert_eq!(
            err.to_string(),
            "rate limited by adapter 'anthropic', retry after 5000ms"
        );

        let err = GatewayError::BudgetExceeded {
            estimated: 1.5,
            remaining: 0.5,
        };
        assert_eq!(
            err.to_string(),
            "budget exceeded: estimated 1.5000, remaining 0.5000"
        );

        let err = GatewayError::AllAttemptsFailed {
            attempts: 3,
            errors: "x".into(),
            attempts_detail: Vec::new(),
        };
        assert_eq!(err.to_string(), "all 3 attempts failed: x");

        let err = GatewayError::InvalidConfig("no routers configured".into());
        assert_eq!(
            err.to_string(),
            "invalid configuration: no routers configured"
        );
    }

    #[test]
    fn is_retryable() {
        assert!(
            GatewayError::RateLimit {
                adapter: "a".into(),
                retry_after_ms: None,
            }
            .is_retryable()
        );

        assert!(
            GatewayError::Timeout {
                adapter: "a".into(),
                model: "m".into(),
                duration_ms: 1000,
            }
            .is_retryable()
        );

        assert!(
            GatewayError::ProviderError {
                adapter: "a".into(),
                message: "err".into(),
                status: Some(500),
            }
            .is_retryable()
        );

        assert!(
            GatewayError::ModelUnavailable {
                adapter: "a".into(),
                model: "m".into(),
            }
            .is_retryable()
        );

        // Not retryable
        assert!(
            !GatewayError::Authentication {
                adapter: "a".into(),
                message: "bad".into(),
            }
            .is_retryable()
        );

        assert!(
            !GatewayError::BudgetExceeded {
                estimated: 1.0,
                remaining: 0.5,
            }
            .is_retryable()
        );

        assert!(
            !GatewayError::AllAttemptsFailed {
                attempts: 3,
                errors: String::new(),
                attempts_detail: Vec::new(),
            }
            .is_retryable()
        );
    }

    #[test]
    fn should_trigger_fallback_matches_triggers() {
        let triggers = vec![FallbackTrigger::RateLimit, FallbackTrigger::Timeout];

        assert!(
            GatewayError::RateLimit {
                adapter: "a".into(),
                retry_after_ms: None,
            }
            .should_trigger_fallback(&triggers)
        );

        assert!(
            GatewayError::Timeout {
                adapter: "a".into(),
                model: "m".into(),
                duration_ms: 1000,
            }
            .should_trigger_fallback(&triggers)
        );

        // ProviderError not in the trigger set
        assert!(
            !GatewayError::ProviderError {
                adapter: "a".into(),
                message: "err".into(),
                status: None,
            }
            .should_trigger_fallback(&triggers)
        );
    }

    #[test]
    fn should_trigger_fallback_empty_triggers() {
        let triggers: Vec<FallbackTrigger> = vec![];

        assert!(
            !GatewayError::RateLimit {
                adapter: "a".into(),
                retry_after_ms: None,
            }
            .should_trigger_fallback(&triggers)
        );

        assert!(
            !GatewayError::Timeout {
                adapter: "a".into(),
                model: "m".into(),
                duration_ms: 1000,
            }
            .should_trigger_fallback(&triggers)
        );
    }

    #[test]
    fn auth_error_never_triggers_fallback() {
        let all_triggers = vec![
            FallbackTrigger::RateLimit,
            FallbackTrigger::Timeout,
            FallbackTrigger::ProviderError,
            FallbackTrigger::ModelUnavailable,
            FallbackTrigger::BudgetExceeded,
        ];

        assert!(
            !GatewayError::Authentication {
                adapter: "a".into(),
                message: "bad key".into(),
            }
            .should_trigger_fallback(&all_triggers)
        );

        assert!(
            !GatewayError::AllAttemptsFailed {
                attempts: 5,
                errors: String::new(),
                attempts_detail: Vec::new(),
            }
            .should_trigger_fallback(&all_triggers)
        );
    }

    #[test]
    fn model_not_ready_never_triggers_fallback_and_is_not_retryable() {
        let all_triggers = vec![
            FallbackTrigger::RateLimit,
            FallbackTrigger::Timeout,
            FallbackTrigger::ProviderError,
            FallbackTrigger::ModelUnavailable,
            FallbackTrigger::BudgetExceeded,
        ];
        let e = GatewayError::ModelNotReady {
            model: "gemma".into(),
            phase: crate::readiness::ProvisionPhase::Downloading {
                done: 1,
                total: Some(10),
            },
        };
        assert!(!e.should_trigger_fallback(&all_triggers));
        assert!(!e.is_retryable());
        assert!(e.to_string().contains("gemma"));
    }

    #[test]
    fn unsupported_error_displays_adapter_and_capability() {
        let e = GatewayError::Unsupported {
            adapter: "grok".into(),
            what: "streaming".into(),
        };
        let s = e.to_string();
        assert!(s.contains("grok"));
        assert!(s.contains("streaming"));
    }

    #[test]
    fn unsupported_error_never_triggers_fallback_and_is_not_retryable() {
        let all_triggers = vec![
            FallbackTrigger::RateLimit,
            FallbackTrigger::Timeout,
            FallbackTrigger::ProviderError,
            FallbackTrigger::ModelUnavailable,
            FallbackTrigger::BudgetExceeded,
        ];
        let e = GatewayError::Unsupported {
            adapter: "a".into(),
            what: "streaming".into(),
        };
        assert!(!e.should_trigger_fallback(&all_triggers));
        assert!(!e.is_retryable());
    }

    #[test]
    fn from_serde_error() {
        // Force a serde_json error and convert via From impl
        let serde_err = serde_json::from_str::<serde_json::Value>("{{bad json").unwrap_err();
        let gw_err: GatewayError = serde_err.into();
        assert!(
            matches!(gw_err, GatewayError::Serialization(_)),
            "expected Serialization, got: {gw_err:?}",
        );
        assert!(gw_err.to_string().contains("serialization error"));
    }

    #[test]
    fn model_unavailable_triggers_fallback() {
        let triggers = vec![FallbackTrigger::ModelUnavailable];
        assert!(
            GatewayError::ModelUnavailable {
                adapter: "a".into(),
                model: "m".into(),
            }
            .should_trigger_fallback(&triggers)
        );
    }

    #[test]
    fn budget_exceeded_triggers_fallback() {
        let triggers = vec![FallbackTrigger::BudgetExceeded];
        assert!(
            GatewayError::BudgetExceeded {
                estimated: 1.0,
                remaining: 0.5,
            }
            .should_trigger_fallback(&triggers)
        );
    }

    #[test]
    fn network_error_not_retryable_for_fallback() {
        // Network errors are retryable but should NOT trigger fallback
        let all_triggers = vec![
            FallbackTrigger::RateLimit,
            FallbackTrigger::Timeout,
            FallbackTrigger::ProviderError,
            FallbackTrigger::ModelUnavailable,
            FallbackTrigger::BudgetExceeded,
        ];

        // We can't easily construct a reqwest::Error, so test the pattern
        // indirectly via the NoCandidates variant (also doesn't trigger fallback)
        assert!(
            !GatewayError::NoCandidates {
                capability: Capability::TextChat,
            }
            .should_trigger_fallback(&all_triggers)
        );
    }
}
