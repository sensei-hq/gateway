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

/// **The SECOND layer, since the conditional-ignore gate.** The first is
/// `#[cfg_attr(not(have_database_url), ignore = "...")]` on every test below, driven by
/// this package's `build.rs` — that is what makes a database-less run report these as
/// `ignored` rather than as PASSED, which is what the runtime early-return made them.
/// This helper still runs, and still announces, for the case the cfg cannot see: the
/// variable present at BUILD time and gone at run time.
///
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

/// SP-DATA-4.1 #7: the prune subcommand is actually wired into `run`, not just implemented
/// in the library. Reuses `help_command_names` for the same reason it exists — asserting
/// `text.contains("prune")` would also pass on the prose.
#[test]
fn run_help_lists_prune() {
    let out = torii().args(["run", "--help"]).output().expect("runs");
    assert!(out.status.success(), "exit: {:?}", out.status);
    let text = String::from_utf8_lossy(&out.stdout);
    let listed = help_command_names(&text);
    assert!(
        listed.iter().any(|c| c == "prune"),
        "`prune` is not a dispatchable `run` subcommand (found {listed:?}):\n{text}"
    );
}

/// The window is rejected by clap's own value parser, i.e. before any connection — and
/// before anything could be deleted. No `DATABASE_URL` is set here, which is the point:
/// reaching the "not set" error would mean the parse had already been accepted.
#[test]
fn an_unparseable_retention_window_is_rejected_by_the_parser() {
    let out = torii()
        .args(["run", "prune", "--older-than", "eventually", "--yes"])
        .output()
        .expect("runs");
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("invalid retention window"), "{err}");
    assert!(
        !err.contains("DATABASE_URL"),
        "the parse must fail before the environment is even read: {err}"
    );
}

/// `prune` is LIGHT tier — the scheduler store is all it needs, so an operator can run it
/// on a box with no gateway config and no fence version (both removed by `torii()`).
///
/// A century-wide window makes this non-destructive by construction: no row in any
/// developer's database was last changed before 1926, so the count is zero and `--yes`
/// deletes nothing. It still exercises the whole real path — boot, connect,
/// `count_terminal_before` — against a live database.
#[cfg_attr(
    not(have_database_url),
    ignore = "needs a Postgres at $DATABASE_URL; see README, Postgres-backed tests"
)]
#[test]
fn prune_is_a_light_tier_command_and_a_century_window_deletes_nothing() {
    let Some(url) = db_url() else { return };
    let out = torii()
        .env("DATABASE_URL", &url)
        .args(["run", "prune", "--older-than", "36500d", "--yes"])
        .output()
        .expect("runs");
    let stderr = String::from_utf8_lossy(&out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        out.status.code(),
        Some(0),
        "prune must need nothing but DATABASE_URL:\n{stderr}"
    );
    assert!(
        stdout.contains("nothing to prune"),
        "a 100-year window can have nothing in scope: {stdout}"
    );
}

// ---- SP-6 s1: `torii run signal` ------------------------------------------------------

/// The subcommand is actually WIRED, not just implemented in the library. Reuses
/// `help_command_names` for the same reason it exists: `text.contains("signal")` would
/// also pass on the prose.
#[test]
fn run_help_lists_signal() {
    let out = torii().args(["run", "--help"]).output().expect("runs");
    assert!(out.status.success(), "exit: {:?}", out.status);
    let text = String::from_utf8_lossy(&out.stdout);
    let listed = help_command_names(&text);
    assert!(
        listed.iter().any(|c| c == "signal"),
        "`signal` is not a dispatchable `run` subcommand (found {listed:?}):\n{text}"
    );
}

/// §6.4 requires this IN THE HELP, because the failure mode is a human pasting a token
/// that lands in durable storage *and* in a model prompt. Redaction is a best-effort
/// scrub by shape; the operator being told not to do it in the first place is the real
/// control.
#[test]
fn signal_help_says_a_signal_is_not_a_credential_channel() {
    let out = torii()
        .args(["run", "signal", "--help"])
        .output()
        .expect("runs");
    assert!(out.status.success(), "exit: {:?}", out.status);
    let text = String::from_utf8_lossy(&out.stdout).to_lowercase();
    assert!(
        text.contains("not a credential channel"),
        "the help must warn that a signal is not a credential channel:\n{text}"
    );
    assert!(
        text.contains("credential broker"),
        "and must name what IS the credential channel:\n{text}"
    );
}

/// Run `torii run signal` with a bogus (never-connectable) `DATABASE_URL`, exactly as
/// `an_invalid_run_id_is_rejected_before_any_connection` does. The out-of-u16-range port
/// fails to parse into `PgConnectOptions` before any I/O, so reaching a CONNECT error
/// instead of the payload error would mean the payload had already been accepted.
fn signal_with_payload(payload: &str) -> std::process::Output {
    torii()
        .env("DATABASE_URL", "postgres://nobody@127.0.0.1:999999/none")
        .args([
            "run",
            "signal",
            "00000000-0000-0000-0000-000000000000",
            "--node",
            "gate",
            "--payload",
            payload,
        ])
        .output()
        .expect("runs")
}

#[test]
fn an_unparseable_signal_payload_is_rejected_before_any_connection() {
    let out = signal_with_payload("approved");
    assert_eq!(out.status.code(), Some(1));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("invalid --payload"), "{err}");
    assert!(
        !err.contains("cannot connect"),
        "the parse must fail before the connection is attempted: {err}"
    );
}

/// THE leak this flag's whole error-message design exists for. The likeliest way an
/// operator pastes a credential here is to type the token BARE, which is not valid JSON —
/// so the invalid-payload path is exactly what would print it to stderr, and thus into
/// journald and CI logs.
///
/// This is asserted at the BINARY level, not just on `parse_payload`, because clap wraps
/// any `value_parser` failure as `invalid value '<THE VALUE>' for '--payload …'` and
/// echoes the value itself — a library-level assertion alone would have passed while the
/// real binary leaked. (Verified: it did, before `--payload` was moved off the value
/// parser.)
#[test]
fn an_invalid_signal_payload_never_echoes_a_pasted_credential() {
    let secret = format!("sk-{}", "A".repeat(24));
    let out = signal_with_payload(&secret);
    assert!(!out.status.success());
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !combined.contains(&secret),
        "a pasted credential reached the terminal:\n{combined}"
    );
    assert!(
        !combined.contains(&"A".repeat(8)),
        "a fragment of it leaked:\n{combined}"
    );
}

/// §6.5's cap, at the binary boundary: an over-limit payload never reaches a connection,
/// let alone a journal row.
#[test]
fn an_oversized_signal_payload_is_rejected_before_any_connection() {
    let out = signal_with_payload(&format!("{{\"n\":\"{}\"}}", "x".repeat(5_000)));
    assert_eq!(out.status.code(), Some(1));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("4096"), "must name the limit: {err}");
    assert!(
        !err.contains("cannot connect"),
        "the cap must apply before the connection is attempted: {err}"
    );
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
#[cfg_attr(
    not(have_database_url),
    ignore = "needs a Postgres at $DATABASE_URL; see README, Postgres-backed tests"
)]
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

/// The one sink `--payload`'s redaction cannot reach: the process's own argv.
///
/// This flag is, by this codebase's own reckoning, the place an operator is most likely to
/// paste a credential — `main.rs`'s `Signal` doc says so, and `parse_payload` goes to
/// deliberate lengths never to echo the value. But a flag value is argv, and argv is read
/// by `ps auxww`, by `/proc/<pid>/cmdline`, by the shell's history file and by the echo of
/// any CI job that shells out — all of them BEFORE any redaction runs. This file's sibling
/// `a_connect_failure_does_not_leak_the_password` guards the same property for
/// `DATABASE_URL`, which is env-only for exactly this reason (`main.rs`: "a flag would leak
/// the password into `ps`"). The rule was not applied to the flag more likely to carry a
/// secret.
///
/// `--payload-file` is the non-argv path. The assertion is on the CHILD's argv, read while
/// it is alive, so it fails if the file's contents ever get re-expanded onto a command line.
#[test]
fn a_payload_file_keeps_the_decision_out_of_argv() {
    let sentinel = format!("SENTINEL{}", "9f2b7c1e4a8d3b5f");
    let dir = std::env::temp_dir().join(format!("torii-payload-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let path = dir.join("decision.json");
    std::fs::write(&path, format!(r#"{{"decision":"{sentinel}"}}"#)).expect("write payload");

    let child = torii()
        // Never-connectable, so the child lives long enough to be observed but performs
        // no I/O — the same out-of-u16-range trick `signal_with_payload` uses.
        .env("DATABASE_URL", "postgres://nobody@127.0.0.1:999999/none")
        .args([
            "run",
            "signal",
            "00000000-0000-0000-0000-000000000000",
            "--node",
            "gate",
            "--payload-file",
            path.to_str().expect("utf-8 path"),
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawns");

    let argv = Command::new("ps")
        .args(["-o", "args=", "-p", &child.id().to_string()])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
        .unwrap_or_default();
    let out = child.wait_with_output().expect("runs");

    assert!(
        !argv.contains(&sentinel),
        "the decision reached the child's command line: {argv}"
    );
    // And the flag really did deliver it — otherwise the assertion above is vacuous.
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !combined.contains("unexpected argument"),
        "--payload-file must be a real flag: {combined}"
    );
    assert!(
        !combined.contains("invalid --payload"),
        "the file's contents must parse as the payload: {combined}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Exactly one source, or the operator does not know which one won.
#[test]
fn payload_and_payload_file_are_mutually_exclusive_and_one_is_required() {
    let both = torii()
        .env("DATABASE_URL", "postgres://nobody@127.0.0.1:999999/none")
        .args([
            "run",
            "signal",
            "00000000-0000-0000-0000-000000000000",
            "--node",
            "gate",
            "--payload",
            r#"{"decision":"approved"}"#,
            "--payload-file",
            "/nonexistent",
        ])
        .output()
        .expect("runs");
    let both_err = String::from_utf8_lossy(&both.stderr);
    assert!(!both.status.success(), "two sources must be refused");
    assert!(
        both_err.contains("cannot be used with"),
        "must be refused AS A CONFLICT, not incidentally: {both_err}"
    );

    let neither = torii()
        .env("DATABASE_URL", "postgres://nobody@127.0.0.1:999999/none")
        .args([
            "run",
            "signal",
            "00000000-0000-0000-0000-000000000000",
            "--node",
            "gate",
        ])
        .output()
        .expect("runs");
    let neither_err = String::from_utf8_lossy(&neither.stderr);
    assert!(!neither.status.success(), "no source must be refused");
    assert!(
        neither_err.contains("required") && neither_err.contains("payload"),
        "must be refused for the MISSING PAYLOAD specifically: {neither_err}"
    );
}

// ---- SP-6 s2: `torii run gate` --------------------------------------------------------

/// The subcommand is actually WIRED, not just implemented in the library. Reuses
/// `help_command_names` for the same reason it exists: `text.contains("gate")` would also
/// pass on the prose.
#[test]
fn run_help_lists_gate() {
    let out = torii().args(["run", "--help"]).output().expect("runs");
    assert!(out.status.success(), "exit: {:?}", out.status);
    let text = String::from_utf8_lossy(&out.stdout);
    let listed = help_command_names(&text);
    assert!(
        listed.iter().any(|c| c == "gate"),
        "`gate` is not a dispatchable `run` subcommand (found {listed:?}):\n{text}"
    );
}

/// All three verbs dispatch, not just the general one — `approve`/`reject` are the forms
/// an operator actually types, and wiring only `decide` would leave them as documentation.
#[test]
fn gate_help_lists_all_three_verbs() {
    let out = torii()
        .args(["run", "gate", "--help"])
        .output()
        .expect("runs");
    assert!(out.status.success(), "exit: {:?}", out.status);
    let text = String::from_utf8_lossy(&out.stdout);
    let listed = help_command_names(&text);
    for verb in ["approve", "reject", "decide"] {
        assert!(
            listed.iter().any(|c| c == verb),
            "`{verb}` is not a dispatchable `run gate` subcommand (found {listed:?}):\n{text}"
        );
    }
}

/// AC10 at the binary level: clap itself must refuse a reject with no reason, before any
/// connection is opened.
#[test]
fn gate_reject_requires_a_reason() {
    let out = torii()
        .env("DATABASE_URL", "postgres://nobody@127.0.0.1:999999/none")
        .args([
            "run",
            "gate",
            "reject",
            "00000000-0000-0000-0000-000000000000",
            "--node",
            "release",
        ])
        .output()
        .expect("runs");
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("--reason"),
        "must name the missing flag: {err}"
    );
    assert!(
        !err.contains("cannot connect"),
        "must fail before the connection is attempted: {err}"
    );
}

/// The help must state the trust boundary, because an operator reading `--as` will
/// otherwise reasonably assume it is authenticated. Anyone who can reach the database can
/// write any actor string, so the flag answers "who CLAIMED to decide" and nothing more.
///
/// Asserted on BOTH surfaces, and with `&&` rather than `||`: the group help is what an
/// operator browses, `run gate decide --help` is what they read when they type the flag,
/// and half the sentence ("attribution" alone) does not warn anybody.
#[test]
fn gate_help_says_attribution_is_not_authentication() {
    for args in [
        vec!["run", "gate", "--help"],
        vec!["run", "gate", "decide", "--help"],
    ] {
        let out = torii().args(&args).output().expect("runs");
        assert!(out.status.success(), "exit: {:?}", out.status);
        let text = String::from_utf8_lossy(&out.stdout);
        let lower = text.to_lowercase();
        assert!(
            lower.contains("attribution") && lower.contains("not authentication"),
            "`{}` must not let --as read as authenticated:\n{text}",
            args.join(" ")
        );
        // clap's own `[default: ""]` rendering contradicts the sentence above on the very
        // surface that states the trust boundary — the effective default is $USER, which
        // `cmd::gate::actor_or_user` resolves, not the empty string clap holds.
        assert!(
            !text.contains(r#"[default: ""]"#),
            "`{}` advertises an empty default that is not what actually happens:\n{text}",
            args.join(" ")
        );
    }
}

// ---- SP-6 s3: `torii run agent answer` ------------------------------------------------

/// The subcommand is actually WIRED, not just implemented in the library. Reuses
/// `help_command_names` for the same reason it exists: `text.contains("agent")` would also
/// pass on the prose.
#[test]
fn run_help_lists_agent() {
    let out = torii().args(["run", "--help"]).output().expect("runs");
    assert!(out.status.success(), "exit: {:?}", out.status);
    let text = String::from_utf8_lossy(&out.stdout);
    let listed = help_command_names(&text);
    assert!(
        listed.iter().any(|c| c == "agent"),
        "`agent` is not a dispatchable `run` subcommand (found {listed:?}):\n{text}"
    );
}

/// The help must describe the surface that SHIPS, not the one that is planned.
///
/// **This test is the POSITIVE form of a negative one, flipped by Task 6 exactly as its
/// predecessor said it should be.** Before the question was rendered, both
/// `run agent --help` and `run agent answer --help` told an operator that
/// `torii run list-paused` "shows the question the human was actually asked" — and it did
/// not: `render::AwaitingNode` was `{node, deadline, options}` and `cmd::run::awaiting_nodes`
/// never read `AgentAwaited.prompt`. So the assertion was inverted (the help must NOT say
/// "question") until the behaviour caught up. It now has: `AwaitingNode` carries a
/// `question`, `awaiting_nodes` folds `AgentAwaited` into it, and `render::awaiting_section`
/// renders it in the node's own `agent:` row.
///
/// **This test pins the HELP side, and only that side.** It inspects `--help` stdout and
/// has no path to the renderer, so it cannot see whether `list-paused` still shows a
/// question. An earlier draft of this comment claimed it could ("delete the question from
/// the listing and this reddens"), which was false and was caught in review — the eighth
/// instance of the same class of defect this test exists to prevent, now in the doc of the
/// guard itself. Measured rather than argued: with
/// `cmd::run::awaiting_nodes`'s `let question = questions.get(&node).cloned();` replaced by
/// `None` — the question gone from both the table cell and `--json` — the whole `--test cli`
/// target still passes 26/26.
///
/// The renderer side is pinned by the LIB tests in another target, which the same mutation
/// reddens six of: `cmd::run::tests::list_paused_shows_a_human_agents_question`,
/// `list_paused_tells_a_human_backed_agent_from_an_await_signal`,
/// `an_overlong_question_is_capped_so_it_cannot_wreck_the_block`,
/// `a_secret_shaped_question_is_redacted_in_the_listing`,
/// `an_ordinary_prose_question_that_wraps_after_bearer_survives` and
/// `a_control_bisected_secret_in_a_question_is_still_withheld`. Nothing MECHANICALLY couples
/// the two halves — this file cannot reach `list_paused` (it drives the built binary, and
/// the listing needs a store and a journal), so the coupling is this comment plus the pair
/// of test names. If the question is ever dropped from the listing, those six go red and
/// this one does not; a reviewer following that trail must then delete the help sentence
/// here too.
///
/// It stays a test in both directions all the same, because a CLI help sentence describing
/// behaviour the command does not have has been the recurring defect of this feature: the
/// help may not promise the question until the renderer shows one, and — the direction this
/// assertion covers — may not stop promising it while the renderer does.
///
/// The pointer at `list-paused` survives the flip: it is the only way an operator discovers
/// a node id without reading the graph.
#[test]
fn agent_help_names_the_question_list_paused_now_shows() {
    for args in [
        vec!["run", "agent", "--help"],
        vec!["run", "agent", "answer", "--help"],
    ] {
        let out = torii().args(&args).output().expect("runs");
        assert!(out.status.success(), "exit: {:?}", out.status);
        let text = String::from_utf8_lossy(&out.stdout);
        assert!(
            text.contains("list-paused"),
            "`{}` must still point at the command that names the waiting nodes — it is the \
             only way to discover a node id without reading the graph:\n{text}",
            args.join(" ")
        );
        assert!(
            text.to_lowercase().contains("question"),
            "`{}` must say that `list-paused` shows the question, because it now does — an \
             operator who does not know that has no way to learn what they are answering \
             short of reading the graph and the registry:\n{text}",
            args.join(" ")
        );
    }
}

/// The trust boundary must be on the surface an operator reads when they type the flag.
/// It matters MORE here than on `run gate`: a gate's actor is an audit trail, whereas this
/// one is folded into the node's OUTPUT (`{"text","actor"}`) and flows into every
/// downstream model prompt for the life of the run — so an operator who reads `--as` as
/// authenticated would be branching real work on an unverified string.
///
/// Asserted on BOTH surfaces and with `&&` rather than `||`, exactly as `run gate`'s
/// equivalent: the group help is what an operator browses, `answer --help` is what they
/// read when they type the flag, and half the sentence warns nobody.
#[test]
fn agent_answer_help_says_attribution_is_not_authentication() {
    for args in [
        vec!["run", "agent", "--help"],
        vec!["run", "agent", "answer", "--help"],
    ] {
        let out = torii().args(&args).output().expect("runs");
        assert!(out.status.success(), "exit: {:?}", out.status);
        let text = String::from_utf8_lossy(&out.stdout);
        let lower = text.to_lowercase();
        assert!(
            lower.contains("attribution") && lower.contains("not authentication"),
            "`{}` must not let --as read as authenticated:\n{text}",
            args.join(" ")
        );
        // clap's own `[default: ""]` rendering contradicts the sentence above on the very
        // surface that states the trust boundary — the effective default is $USER, which
        // `cmd::gate::actor_or_user` resolves, not the empty string clap holds.
        assert!(
            !text.contains(r#"[default: ""]"#),
            "`{}` advertises an empty default that is not what actually happens:\n{text}",
            args.join(" ")
        );
    }
}

/// AC11, and the one sink redaction cannot reach: the process's own argv.
///
/// A flag value is read by `ps auxww`, by `/proc/<pid>/cmdline`, by the shell's history
/// file and by the echo of any CI job that shells out — all of them BEFORE any redaction
/// runs. s1 shipped `--payload` argv-only and a review caught this; s2 repeated it with
/// `--note`. An agent's answer is the longest free text of the three and the most likely to
/// be pasted from elsewhere, so `--text-file` ships with the command rather than after it.
///
/// The assertion is on the CHILD's argv, read while it is alive, so it fails if the file's
/// contents ever get re-expanded onto a command line.
#[test]
fn an_answer_file_keeps_the_text_out_of_argv() {
    let sentinel = format!("SENTINEL{}", "4c8e1a97b3d20f6e");
    let dir = std::env::temp_dir().join(format!("torii-answer-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let path = dir.join("answer.txt");
    std::fs::write(&path, &sentinel).expect("write answer");

    let child = torii()
        // Never-connectable, so the child lives long enough to be observed but performs no
        // I/O — the same out-of-u16-range trick `signal_with_payload` uses.
        .env("DATABASE_URL", "postgres://nobody@127.0.0.1:999999/none")
        .args([
            "run",
            "agent",
            "answer",
            "00000000-0000-0000-0000-000000000000",
            "--node",
            "reviewer",
            "--text-file",
            path.to_str().expect("utf-8 path"),
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawns");

    let argv = Command::new("ps")
        .args(["-o", "args=", "-p", &child.id().to_string()])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
        .unwrap_or_default();
    let out = child.wait_with_output().expect("runs");

    assert!(
        !argv.contains(&sentinel),
        "the answer reached the child's command line: {argv}"
    );
    // And the flag really did deliver it — otherwise the assertion above is vacuous, which
    // it demonstrably is on a binary that has no `run agent` at all: clap's "unrecognized
    // subcommand" also contains no sentinel. So the invocation must be proven to have got
    // PAST clap and past the file read, all the way to the connection this URL cannot
    // satisfy. Anything earlier means the argv assertion proved nothing.
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        combined.contains("cannot connect"),
        "the whole invocation must be accepted and the file read — only then does the argv \
         assertion above mean anything: {combined}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Exactly one source, or the operator does not know which one won.
#[test]
fn text_and_text_file_are_mutually_exclusive_and_one_is_required() {
    let both = torii()
        .env("DATABASE_URL", "postgres://nobody@127.0.0.1:999999/none")
        .args([
            "run",
            "agent",
            "answer",
            "00000000-0000-0000-0000-000000000000",
            "--node",
            "reviewer",
            "--text",
            "ship it",
            "--text-file",
            "/nonexistent",
        ])
        .output()
        .expect("runs");
    let both_err = String::from_utf8_lossy(&both.stderr);
    assert!(!both.status.success(), "two sources must be refused");
    assert!(
        both_err.contains("cannot be used with"),
        "must be refused AS A CONFLICT, not incidentally: {both_err}"
    );

    let neither = torii()
        .env("DATABASE_URL", "postgres://nobody@127.0.0.1:999999/none")
        .args([
            "run",
            "agent",
            "answer",
            "00000000-0000-0000-0000-000000000000",
            "--node",
            "reviewer",
        ])
        .output()
        .expect("runs");
    let neither_err = String::from_utf8_lossy(&neither.stderr);
    assert!(!neither.status.success(), "no source must be refused");
    assert!(
        neither_err.contains("required") && neither_err.contains("text"),
        "must be refused for the MISSING ANSWER specifically: {neither_err}"
    );
}

/// An unreadable file is an operator typo, and must be reported before any connection —
/// and without echoing whatever was read.
#[test]
fn an_unreadable_payload_file_is_rejected_before_any_connection() {
    let out = torii()
        .env("DATABASE_URL", "postgres://nobody@127.0.0.1:999999/none")
        .args([
            "run",
            "signal",
            "00000000-0000-0000-0000-000000000000",
            "--node",
            "gate",
            "--payload-file",
            "/nonexistent/decision.json",
        ])
        .output()
        .expect("runs");
    assert_eq!(out.status.code(), Some(1));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("--payload-file"), "must name the flag: {err}");
    assert!(
        !err.contains("cannot connect"),
        "must fail before the connection is attempted: {err}"
    );
}
