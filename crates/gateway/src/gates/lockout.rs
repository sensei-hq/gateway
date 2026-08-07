use crate::types::error::GatewayError;

/// Why a `router:model` is (or should be) locked out. **Provider-side only** —
/// distinct from the caller's subscription `GatewayError::QuotaExceeded`, which
/// is a subject hard-stop and never produces a lockout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockReason {
    /// 429 — recovers fast (short cooldown, honoring Retry-After if present).
    RateLimit,
    /// Provider quota for this model exhausted (403 + quota body) — recovers at a
    /// reset boundary; demotes to the next tier.
    QuotaExhausted,
    /// Out of credits / billing (403 credit body, or 402) — terminal until a
    /// human tops up.
    CreditsExhausted,
    /// 401, or a bare 403 forbidden — terminal until the credential changes.
    Auth,
}

impl LockReason {
    /// Recoverable reasons demote-and-retry (the model comes back). Terminal
    /// reasons need human action (top-up / rotate key) and carry `until: None`.
    pub fn is_recoverable(&self) -> bool {
        matches!(self, LockReason::RateLimit | LockReason::QuotaExhausted)
    }
    pub fn is_terminal(&self) -> bool {
        !self.is_recoverable()
    }
}

/// Classify a provider error into a lockout reason, or `None` when it is not a
/// provider limit signal. The single source of truth shared by the (future)
/// in-flight walk and the next-request `ModelLockoutSink` — they cannot disagree.
/// Reads the boundary-enriched error (Task 1): 403 preserves status+body; 429
/// carries retry-after.
pub fn classify(err: &GatewayError) -> Option<LockReason> {
    match err {
        GatewayError::RateLimit { .. } => Some(LockReason::RateLimit),
        GatewayError::Authentication { .. } => Some(LockReason::Auth), // 401
        GatewayError::ProviderError {
            status: Some(402), ..
        } => Some(LockReason::CreditsExhausted),
        GatewayError::ProviderError {
            status: Some(403),
            message,
            ..
        } => Some(classify_403_body(message)),
        _ => None,
    }
}

/// A 403 is ambiguous: quota vs credits vs a plain forbidden. Disambiguate by
/// body keywords (providers throttle via non-standard 403 bodies). Order
/// matters: credits/billing before quota, since "you have exceeded your credit
/// limit" contains both senses and the billing sense is terminal.
fn classify_403_body(message: &str) -> LockReason {
    let m = message.to_ascii_lowercase();
    if m.contains("credit")
        || m.contains("billing")
        || m.contains("payment")
        || m.contains("insufficient")
    {
        LockReason::CreditsExhausted
    } else if m.contains("quota")
        || m.contains("exceed")
        || m.contains("exhaust")
        || m.contains("rate limit")
    {
        LockReason::QuotaExhausted
    } else {
        LockReason::Auth // bare forbidden → terminal (preserves pre-(d) 403 semantics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::error::GatewayError;

    fn provider(status: u16, msg: &str) -> GatewayError {
        GatewayError::ProviderError {
            adapter: "a".into(),
            message: msg.into(),
            status: Some(status),
        }
    }

    #[test]
    fn classify_maps_provider_limits() {
        let rl = GatewayError::RateLimit {
            adapter: "a".into(),
            retry_after_ms: Some(1000),
        };
        assert_eq!(classify(&rl), Some(LockReason::RateLimit));
        assert!(LockReason::RateLimit.is_recoverable());

        assert_eq!(
            classify(&provider(403, "You exceeded your quota")),
            Some(LockReason::QuotaExhausted)
        );
        assert!(LockReason::QuotaExhausted.is_recoverable());

        assert_eq!(
            classify(&provider(403, "insufficient credits, please add billing")),
            Some(LockReason::CreditsExhausted)
        );
        assert!(LockReason::CreditsExhausted.is_terminal());

        assert_eq!(
            classify(&provider(402, "payment required")),
            Some(LockReason::CreditsExhausted)
        );

        assert_eq!(
            classify(&provider(403, "forbidden")),
            Some(LockReason::Auth)
        );

        let auth = GatewayError::Authentication {
            adapter: "a".into(),
            message: "bad key".into(),
        };
        assert_eq!(classify(&auth), Some(LockReason::Auth));
        assert!(LockReason::Auth.is_terminal());

        assert_eq!(classify(&provider(500, "boom")), None);
        assert_eq!(
            classify(&GatewayError::Timeout {
                adapter: "a".into(),
                model: "m".into(),
                duration_ms: 1
            }),
            None
        );
        assert_eq!(
            classify(&GatewayError::ModelUnavailable {
                adapter: "a".into(),
                model: "m".into()
            }),
            None
        );
    }
}
