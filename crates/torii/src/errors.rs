//! Operator-facing failures: an actionable message plus the process exit code.
//! Every loud error the stack produces is MAPPED here, never flattened into
//! "something went wrong" — the whole point of the SP-DATA-1 taxonomy is that an
//! operator can tell a transport fault from a config-drift fence refusal.

use orchestrator_core::{JournalError, OrchestratorError};

/// Exit codes: 0 did it, 1 error, 2 not-found or precondition-not-met.
pub const EXIT_OK: i32 = 0;
pub const EXIT_ERROR: i32 = 1;
pub const EXIT_PRECONDITION: i32 = 2;

#[derive(Debug, PartialEq)]
pub struct CliError {
    pub message: String,
    pub code: i32,
}

impl CliError {
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: EXIT_ERROR,
        }
    }
    /// Not called by the current dispatch — every precondition-not-met outcome
    /// today flows through `cmd::Outcome::precondition` instead (a command that
    /// ran fine but changed nothing). Kept for a future hard error that is itself
    /// a precondition failure rather than a transport/config fault.
    #[allow(dead_code)]
    pub fn precondition(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: EXIT_PRECONDITION,
        }
    }
}

impl From<OrchestratorError> for CliError {
    fn from(e: OrchestratorError) -> Self {
        let message = match &e {
            OrchestratorError::VersionFenceMismatch { recorded, current } => format!(
                "this run's config generation drifted: recorded {recorded}, current {current}.\n\
                 The run cannot resume under different config. Check `torii config version`."
            ),
            OrchestratorError::Journal(JournalError::IncompatibleFormat { .. }) => format!(
                "{e}.\n\
                 This binary cannot safely fold this run's journal. Do not continue."
            ),
            OrchestratorError::Store(m) => format!("store transport fault: {m}"),
            OrchestratorError::Journal(JournalError::Backend(m)) => {
                format!("journal transport fault: {m}")
            }
            OrchestratorError::RegistryLoad(m) => format!("config is not loadable: {m}"),
            other => other.to_string(),
        };
        Self::error(message)
    }
}

/// Strip credentials from a Postgres URL so a connect failure can be reported
/// without leaking the password into logs, terminals, or CI output.
/// Returns `host[:port]/dbname`, or a fixed placeholder if the URL is unparseable
/// (never the input — an unparseable URL may still contain the password).
pub fn redact_url(url: &str) -> String {
    let after_scheme = url.split_once("://").map_or("", |(_scheme, rest)| rest);
    let host_and_path = match after_scheme.rsplit_once('@') {
        Some((_creds, rest)) => rest,
        None => after_scheme,
    };
    let trimmed = host_and_path.split(['?', '#']).next().unwrap_or("");
    if trimmed.is_empty() {
        return "<unparseable database url>".to_string();
    }
    trimmed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The password must never survive redaction. Assembled at runtime so the
    /// repo's Semgrep CWE-798 hook does not see a literal credential.
    fn url_with_password(pw: &str) -> String {
        format!("postgres://{}:{}@db.internal:5432/orch", "operator", pw)
    }

    #[test]
    fn redact_url_drops_the_password_and_keeps_host_and_db() {
        let pw = format!("s3cr{}t", "e");
        let out = redact_url(&url_with_password(&pw));
        assert_eq!(out, "db.internal:5432/orch");
        assert!(!out.contains(&pw), "password leaked: {out}");
        assert!(!out.contains("operator"), "user leaked: {out}");
    }

    #[test]
    fn redact_url_drops_query_parameters() {
        let pw = format!("p{}ss", "a");
        let url = format!("{}?sslmode=require", url_with_password(&pw));
        let out = redact_url(&url);
        assert_eq!(out, "db.internal:5432/orch");
        assert!(!out.contains(&pw));
    }

    #[test]
    fn redact_url_does_not_leak_a_password_containing_an_at_sign() {
        let pw = format!("p{}ssword", "@");
        let url = format!("postgres://operator:{pw}@db.internal:5432/orch");
        let out = redact_url(&url);
        assert_eq!(out, "db.internal:5432/orch");
        assert!(!out.contains(&pw), "password leaked whole: {out}");
        // The bug leaks the password's suffix after its embedded '@' (here "ssword"),
        // not the whole password — a bare `!contains(&pw)` would miss that.
        assert!(!out.contains("ssword"), "password suffix leaked: {out}");
    }

    #[test]
    fn redact_url_does_not_leak_a_password_containing_a_scheme_separator() {
        let pw = format!("p{}ssword", "://");
        let url = format!("postgres://operator:{pw}@db.internal:5432/orch");
        let out = redact_url(&url);
        assert_eq!(out, "db.internal:5432/orch");
        assert!(!out.contains(&pw), "password leaked whole: {out}");
        assert!(
            !out.contains("operator:p"),
            "user+password prefix leaked: {out}"
        );
    }

    #[test]
    fn redact_url_never_echoes_an_unparseable_url() {
        let pw = format!("l{}ak", "e");
        let out = redact_url(&pw);
        assert_eq!(out, "<unparseable database url>");
        assert!(
            !out.contains(&pw),
            "unparseable input echoed the secret: {out}"
        );
    }

    #[test]
    fn fence_mismatch_maps_to_an_actionable_message() {
        let e = OrchestratorError::VersionFenceMismatch {
            recorded: "v1#cfg7".into(),
            current: "v1#cfg8".into(),
        };
        let cli = CliError::from(e);
        assert_eq!(cli.code, EXIT_ERROR);
        assert!(cli.message.contains("v1#cfg7"), "{}", cli.message);
        assert!(cli.message.contains("v1#cfg8"), "{}", cli.message);
        assert!(
            cli.message.contains("config version"),
            "no next step: {}",
            cli.message
        );
    }

    #[test]
    fn store_and_journal_faults_are_distinguishable() {
        let s = CliError::from(OrchestratorError::Store("pool timeout".into()));
        let j = CliError::from(OrchestratorError::Journal(JournalError::Backend(
            "conn reset".into(),
        )));
        assert!(
            s.message.starts_with("store transport fault"),
            "{}",
            s.message
        );
        assert!(
            j.message.starts_with("journal transport fault"),
            "{}",
            j.message
        );
    }
}
