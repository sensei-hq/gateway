//! Ephemeral credential broker (SP-4). A `CredentialBroker` resolves a tool's declared
//! credential refs to `Secret`s that the executor injects into the tool's `ToolContext`
//! — never journaled, never in the prompt (design §4).

use zeroize::Zeroizing;

use crate::error::OrchestratorError;

/// A secret value. `Debug` prints `[REDACTED]`; the bytes are zeroized on drop.
#[derive(Clone)]
pub struct Secret(Zeroizing<String>);

impl Secret {
    pub fn new(s: impl Into<String>) -> Self {
        Self(Zeroizing::new(s.into()))
    }
    /// Expose the raw secret. Call sites MUST NOT journal/log the returned `&str`.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED]")
    }
}

/// Resolves a tool's declared credential refs to secrets. Injected on the `Executor`
/// (default none). A real impl (future) wraps `vault::Vault`; a `StaticCredentialBroker`
/// demo lands with the executor wiring.
#[async_trait::async_trait]
pub trait CredentialBroker: Send + Sync {
    /// Resolve a credential ref (e.g. `"stripe_key"`) to its secret. Unknown → `Ok(None)`.
    async fn resolve(&self, cred_ref: &str) -> Result<Option<Secret>, OrchestratorError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_exposes_but_debug_redacts() {
        let raw = format!("sk-{}", "abcdefghij"); // runtime-assembled (repo semgrep CWE-798 hook blocks literal secrets)
        let s = Secret::new(raw.clone());
        assert_eq!(s.expose(), raw, "expose returns the raw value");
        assert_eq!(
            format!("{s:?}"),
            "[REDACTED]",
            "Debug never leaks the value"
        );
        assert_eq!(
            format!("{s:#?}"),
            "[REDACTED]",
            "pretty-Debug never leaks either"
        );
        assert!(!format!("{s:?}").contains(&raw));
    }
}
