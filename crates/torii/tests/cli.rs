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

/// WHOLE-SLICE FIX 6: `Some(url)` to run, `None` plus a VISIBLE skip notice naming the
/// test to skip. Written to the real stderr rather than through `eprintln!` because
/// libtest captures the print macros for a passing test and replays them only on failure —
/// an `eprintln!` notice would be invisible in exactly the green run it exists to annotate.
/// Under libtest the current thread's name is the test's path, which is what lets one
/// helper name the test that skipped. (Duplicated from torii's own `test_guard`: an
/// integration test is a separate crate and cannot see a `#[cfg(test)]` item.)
fn db_url() -> Option<String> {
    let url = std::env::var("DATABASE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty());
    if url.is_none() {
        use std::io::Write;
        let name = std::thread::current()
            .name()
            .unwrap_or("<unnamed test>")
            .to_string();
        // Formatted first, then ONE `write_all`: `Stderr` is unbuffered, so a
        // `writeln!` emits a separate syscall per format fragment and a parallel
        // test's output interleaves mid-line.
        let line = format!("SKIP {name}: DATABASE_URL not set\n");
        let _ = std::io::stderr().write_all(line.as_bytes());
    }
    url
}

/// The subcommand names listed under clap's `Commands:` heading — i.e. what the binary
/// actually dispatches, not what its prose happens to mention.
///
/// WHOLE-SLICE FIX 7: the version of this test that asserted `text.contains("run")` could
/// not fail. All three words appear in the `long_about` paragraph alone, so deleting every
/// subcommand from the enum left it green (proven). clap renders the section as a blank
/// line, `Commands:`, then two-space-indented `  <name>  <about>` rows until the next
/// blank line — so the names are the first token of each row in that block.
fn help_command_names(help: &str) -> Vec<String> {
    help.lines()
        .skip_while(|l| l.trim() != "Commands:")
        .skip(1)
        .take_while(|l| !l.trim().is_empty())
        .filter(|l| l.starts_with("  ") && !l.starts_with("   "))
        .filter_map(|l| l.split_whitespace().next().map(str::to_string))
        .collect()
}

#[test]
fn help_lists_all_three_command_groups() {
    let out = torii().arg("--help").output().expect("runs");
    assert!(out.status.success(), "exit: {:?}", out.status);
    let text = String::from_utf8_lossy(&out.stdout);
    let listed = help_command_names(&text);
    for group in ["run", "worker", "config"] {
        assert!(
            listed.iter().any(|c| c == group),
            "`{group}` is not a dispatchable subcommand under `Commands:` \
             (found {listed:?}):\n{text}"
        );
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

/// AC1, which nothing covered: every other light-tier test here exercises a FAILURE path,
/// so "a light-tier command runs with only `DATABASE_URL`" — the whole point of the
/// two-tier boot — was asserted nowhere. `TORII_FENCE_VERSION` is explicitly removed (by
/// `torii()`) and no `--gateway-config` is passed, so a light-tier command that had crept
/// into needing either would fail here rather than at an operator's terminal.
#[test]
fn a_light_tier_command_runs_with_only_a_database_url() {
    let Some(url) = db_url() else { return };
    let out = torii()
        .env("DATABASE_URL", &url)
        .args(["run", "list-paused"])
        .output()
        .expect("runs");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        out.status.code(),
        Some(0),
        "the light tier must need nothing but DATABASE_URL:\n{stderr}"
    );
}
