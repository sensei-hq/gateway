use super::capability::Capability;
use super::config::{FallbackTrigger, MeterUnit, Window};

/// A caller-actionable remedy attached to a terminal `AllGated` when no candidate
/// has a timed retry (every candidate is health-locked terminally, over budget, or
/// over its context window). Guides the caller — the gateway never acts on it
/// (tenant-agnostic; the caller owns credentials/budget/routing).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HumanAction {
    /// Provider credits/billing exhausted — top up.
    TopUpCredits,
    /// Auth failed / credential invalid — rotate the key.
    RotateCredential,
    /// Every candidate was over budget — raise the budget.
    RaiseBudget,
    /// Every candidate's context window is smaller than the request — no waiting helps,
    /// and no budget change helps either. The human must route to a model with a larger
    /// window (widen or reorder the chain) or make the request smaller.
    ///
    /// Distinct from [`HumanAction::RaiseBudget`], which is about MONEY: an over-budget
    /// skip is fixed by spending more at the same model, and this one cannot be fixed by
    /// spending at all — the window is a property of the model, not of the cap. Rendering
    /// them alike would send an operator to the wrong lever.
    UseLargerContextWindow,
}

/// The remedy as a sentence an operator can act on, because a `Debug`-rendered
/// variant name is not one.
///
/// This exists so [`GatewayError::AllGated`] can say WHICH action is required rather
/// than only that one is: the bare "human action required" it printed before named the
/// need and hid the lever. Kept as `Display` on the enum rather than a `match` at each
/// rendering site so the four remedies cannot drift apart per site.
impl std::fmt::Display for HumanAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            HumanAction::TopUpCredits => "top up the provider account's credits",
            HumanAction::RotateCredential => "rotate the provider credential",
            HumanAction::RaiseBudget => "raise the budget",
            HumanAction::UseLargerContextWindow => {
                "route to a model with a larger context window, or send less"
            }
        })
    }
}

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

    /// Every candidate was gated (health-locked / cooling / breaker-open / over
    /// budget / over its context window) — none was attemptable. `resume_after` is
    /// the **wall-clock** earliest eligibility (min over timed gates); `None` ⇒ no
    /// gate clears on its own and the caller must not pause forever. `skipped` is
    /// human-readable diagnostics. Distinct from `AllAttemptsFailed` (a candidate
    /// genuinely failed) and `NoCandidates` (nothing configured/eligible). Never
    /// triggers fallback.
    ///
    /// **The two fields are independent, and the caller must key on `resume_after`
    /// alone.** `human_action` is DIAGNOSIS, and it can be `Some` beside a `Some`
    /// `resume_after`: a chain with one breaker-open candidate and one whose window is
    /// too small for the request yields a wake (the breaker will close) AND a remedy
    /// (nothing about waiting makes the other model bigger). Reading `human_action` as
    /// "this is terminal" was safe only while `all_gated_error` nulled it whenever a
    /// wake existed, which threw the remedy away — see that function for why the
    /// suppression was removed.
    ///
    /// **`Display` renders all three fields**, and that is load-bearing rather than
    /// cosmetic: the orchestrator's `classify_gateway_error` turns this error into a
    /// `NodeFailed` whose reason is `err.to_string()`, so anything `Display` drops
    /// never reaches the operator who has to act. It rendered only the first field
    /// for several slices, which silently discarded every per-candidate reason and
    /// the remedy alike — the caller was told "human action required" and not which.
    #[error("all candidates gated{}{}{}",
        resume_after
            .map(|t| format!(", resume after {t}"))
            .unwrap_or_default(),
        match (human_action, resume_after) {
            (Some(h), _) => format!(", human action required: {h}"),
            // No remedy AND no deadline: `all_gated_error` cannot build this (a gate is
            // either Timed or Terminal, and Terminal always carries an action), so it
            // is a hand-constructed value. Say the bare thing rather than nothing.
            (None, None) => ", human action required".to_string(),
            (None, Some(_)) => String::new(),
        },
        if skipped.is_empty() {
            String::new()
        } else {
            format!(" (skipped: {})", skipped.join(" | "))
        })]
    AllGated {
        resume_after: Option<chrono::DateTime<chrono::Utc>>,
        skipped: Vec<String>,
        human_action: Option<HumanAction>,
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
            | GatewayError::AllGated { .. }
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

    /// `UseLargerContextWindow` RENDERS as its own remedy, not as a spelling of
    /// `RaiseBudget`.
    ///
    /// The two are easy to conflate — both are terminal, both are "your request was
    /// too big for this candidate" — but they point at opposite levers. An over-budget
    /// skip is about MONEY and clears the moment someone raises the cap at the same
    /// model. An over-window skip cannot be cleared by spending anything: the window is
    /// a property of the model, so the operator must route to a bigger one (widen or
    /// reorder the chain) or send less.
    ///
    /// Asserted on the rendered TEXT rather than on `assert_ne!` over two unit variants,
    /// because that comparison is a tautology over a derived `PartialEq` and no
    /// plausible source defect breaks it. The defect that IS plausible is a `Display`
    /// arm that points the operator at the wrong lever — writing "raise the budget" for
    /// a window problem — and this reddens on exactly that.
    #[test]
    fn use_larger_context_window_renders_a_different_remedy_from_raise_budget() {
        let window = HumanAction::UseLargerContextWindow.to_string();
        let budget = HumanAction::RaiseBudget.to_string();
        assert_ne!(
            window, budget,
            "the two remedies must not render alike: an operator handed 'raise the \
             budget' for an over-window skip can burn any amount of money and change \
             nothing"
        );
        assert!(
            window.contains("context window"),
            "the window remedy must name the window as the thing to change: {window}"
        );
        assert!(
            !window.contains("budget"),
            "and must not mention budget at all, which is the lever that cannot help \
             here: {window}"
        );
    }

    /// The `AllGated` message names every candidate's reason AND the remedy.
    ///
    /// This string is the only channel that survives to an operator on the orchestrator
    /// path: `classify_gateway_error` builds its `NodeFailed` reason from
    /// `err.to_string()`, so a field that `Display` drops is a field nobody reads. The
    /// variant carried `skipped` and `human_action` for a whole slice while rendering
    /// neither, which made every doc comment claiming "`AllGated` renders these strings
    /// verbatim" false.
    #[test]
    fn all_gated_renders_each_candidates_reason_and_the_remedy() {
        let e = GatewayError::AllGated {
            resume_after: None,
            skipped: vec![
                "anthropic:small — estimated 20000 input tokens exceeds the model's \
                 8192-token context window"
                    .to_string(),
                "openai:tiny — estimated 20000 input tokens exceeds the model's \
                 4096-token context window"
                    .to_string(),
            ],
            human_action: Some(HumanAction::UseLargerContextWindow),
        };
        let shown = e.to_string();
        assert!(
            shown.contains("estimated 20000") && shown.contains("8192-token"),
            "the per-candidate diagnostics must reach the operator — both the estimate \
             and the window it exceeded: {shown}"
        );
        assert!(
            shown.contains("4096-token"),
            "and EVERY candidate's reason, not just the first: {shown}"
        );
        assert!(
            shown.contains("larger context window"),
            "as must the remedy, or the message says 'human action required' without \
             saying which: {shown}"
        );
    }

    /// A wake and a remedy render TOGETHER — the two fields are independent.
    ///
    /// The old `#[error]` put them in an `unwrap_or_else`, so a `Some` `resume_after`
    /// swallowed the remedy at the rendering layer as well as at the aggregation one.
    /// A chain with one breaker-open candidate and one whose window is too small
    /// produces exactly this pair, and the operator needs both: retry at `t`, and the
    /// other model will still be too small then.
    #[test]
    fn all_gated_renders_a_wake_and_a_remedy_together() {
        let t = chrono::Utc::now() + chrono::Duration::minutes(5);
        let shown = GatewayError::AllGated {
            resume_after: Some(t),
            skipped: vec![
                "noop:small — circuit breaker open".to_string(),
                "noop:big — estimated 200000 input tokens exceeds the model's \
                 128000-token context window"
                    .to_string(),
            ],
            human_action: Some(HumanAction::UseLargerContextWindow),
        }
        .to_string();
        assert!(
            shown.contains("resume after"),
            "the wake decides whether the caller pauses: {shown}"
        );
        assert!(
            shown.contains("larger context window"),
            "and the remedy says what will still be wrong after it: {shown}"
        );
    }

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
    fn all_gated_is_terminal_and_displays() {
        let all_triggers = vec![
            FallbackTrigger::RateLimit,
            FallbackTrigger::Timeout,
            FallbackTrigger::ProviderError,
            FallbackTrigger::ModelUnavailable,
            FallbackTrigger::BudgetExceeded,
        ];
        let e = GatewayError::AllGated {
            resume_after: None,
            skipped: vec!["r:m — model locked out (Auth)".to_string()],
            human_action: Some(HumanAction::TopUpCredits),
        };
        assert!(!e.should_trigger_fallback(&all_triggers)); // terminal — never falls over
        assert!(!e.is_retryable()); // a durable pause, not an immediate retry
        assert!(e.to_string().contains("all candidates gated"));
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
