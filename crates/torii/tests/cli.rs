//! Binary-level smoke tests. `CARGO_BIN_EXE_torii` is set by cargo for integration
//! tests, so no `assert_cmd` dependency is needed.

use std::process::Command;

fn torii() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_torii"));
    // Never inherit a developer's real database.
    c.env_remove("DATABASE_URL");
    c.env_remove("TORII_FENCE_VERSION");
    c
}

#[test]
fn help_lists_all_three_command_groups() {
    let out = torii().arg("--help").output().expect("runs");
    assert!(out.status.success(), "exit: {:?}", out.status);
    let text = String::from_utf8_lossy(&out.stdout);
    for group in ["run", "worker", "config"] {
        assert!(text.contains(group), "missing {group} in help:\n{text}");
    }
}

#[test]
fn a_missing_database_url_fails_with_a_named_variable() {
    let out = torii().args(["run", "list-paused"]).output().expect("runs");
    assert_eq!(out.status.code(), Some(1));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("DATABASE_URL"), "{err}");
}

#[test]
fn an_invalid_run_id_is_rejected_before_any_connection() {
    let out = torii()
        .env("DATABASE_URL", "postgres://nobody@127.0.0.1:1/none")
        .args(["run", "status", "not-a-uuid"])
        .output()
        .expect("runs");
    assert_eq!(out.status.code(), Some(1));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("invalid run id"), "{err}");
}

#[test]
fn an_unparseable_interval_is_rejected_by_the_parser() {
    let out = torii()
        .args([
            "worker",
            "serve",
            "--interval",
            "soon",
            "--gateway-config",
            "/nonexistent",
        ])
        .output()
        .expect("runs");
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("invalid interval"), "{err}");
}

/// A connect failure must never echo the password. Deliberately uses a port
/// number out of the valid u16 range (not a refused-connection address like
/// `127.0.0.1:1`): sqlx's pool treats `ECONNREFUSED` as transient and retries
/// with backoff for the whole default 30s `acquire_timeout`, so a refused-port
/// target makes this test take ~30s. An out-of-range port fails to parse into
/// `PgConnectOptions` before any I/O happens at all, so this is a millisecond
/// test — while still routing the raw URL through the real `boot::light` ->
/// `redact_url` error path, so the assertion below exercises genuine
/// redaction rather than short-circuiting before it ever runs. (An
/// unresolvable hostname was also measured fast, but rejected: it depends on
/// the CI/sandbox DNS stack resolving-and-failing quickly, which a parse
/// failure does not need at all.)
#[test]
fn a_connect_failure_does_not_leak_the_password() {
    let pw = format!("s3cr{}t", "e");
    let url = format!("postgres://operator:{pw}@127.0.0.1:999999/none");
    let out = torii()
        .env("DATABASE_URL", &url)
        .args(["config", "version"])
        .output()
        .expect("runs");
    assert!(!out.status.success());
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!combined.contains(&pw), "password leaked:\n{combined}");
}
