//! The durable config write path. Validate before writing, diff before
//! overwriting, and never write content without advancing the generation.

use crate::cmd::Outcome;
use crate::diff::{ConfigDiff, diff};
use crate::errors::CliError;
use crate::render::one_line;
use orchestrator_core::{ConfigSource, Registry, RegistryConfig};
use orchestrator_store::FilesystemConfigSource;
use orchestrator_store::postgres::PostgresConfigSource;
use std::io::{BufRead, Read, Write};
use std::path::Path;

/// What `plan_push` decided, before any write happens.
// Consumed by Task 10 (main.rs clap dispatch), `torii config push <dir>`.
#[allow(dead_code)]
#[derive(Debug)]
pub enum PushDecision {
    /// Nothing to do — the incoming config matches the durable one.
    NoOp(ConfigDiff),
    /// Safe to write.
    Apply(ConfigDiff),
    /// Refused: removals need confirmation that was not given.
    NeedsConfirmation(ConfigDiff),
}

/// The pure decision: is this push safe to apply? `confirmed` is true when the
/// operator passed `--yes` or answered the prompt.
// Consumed by Task 10 (main.rs clap dispatch), `torii config push <dir>`.
#[allow(dead_code)]
pub fn plan_push(
    current: &RegistryConfig,
    incoming: &RegistryConfig,
    confirmed: bool,
) -> PushDecision {
    let d = diff(current, incoming);
    if d.is_noop() {
        return PushDecision::NoOp(d);
    }
    if d.requires_confirmation() && !confirmed {
        return PushDecision::NeedsConfirmation(d);
    }
    PushDecision::Apply(d)
}

/// Render a diff for the operator. This text IS the destruction consent an operator
/// reads before answering the confirmation prompt, so an entity name — free text from
/// a config file, never validated for shape — must never be interpolated raw: a name
/// containing a newline plus a forged line (e.g. a fake "no changes" message, or a
/// fake removal for a DIFFERENT entity) can hide what is actually being destroyed, and
/// a name containing ANSI cursor-control escapes can blank real removal lines from the
/// rendered terminal output entirely. `one_line` (shared with `render::table`, which
/// guards the same class of attack from a run's pause reason) collapses every control
/// character — including ESC — to a space.
// Consumed by Task 10 (main.rs clap dispatch), `torii config push <dir>`.
#[allow(dead_code)]
pub fn describe_diff(d: &ConfigDiff, current_version: u64, source: &str) -> String {
    let mut s = format!("config diff (durable v{current_version} -> {source}):\n");
    for e in &d.added {
        s.push_str(&format!(
            "  + {:<6} {}\n",
            e.kind.label(),
            one_line(&e.name)
        ));
    }
    for e in &d.changed {
        s.push_str(&format!(
            "  ~ {:<6} {}\n",
            e.kind.label(),
            one_line(&e.name)
        ));
    }
    for e in &d.removed {
        s.push_str(&format!(
            "  - {:<6} {}\n",
            e.kind.label(),
            one_line(&e.name)
        ));
    }
    s.push_str(&format!("  = {} unchanged\n", d.unchanged));
    if d.requires_confirmation() {
        let n = d.removed.len();
        let noun = if n == 1 { "entity" } else { "entities" };
        s.push_str(&format!(
            "\nThis REMOVES {n} {noun}. A push is replace-all: removed entities cannot be recovered.\n"
        ));
    }
    s
}

// Consumed by Task 10 (main.rs clap dispatch), `torii config version`.
#[allow(dead_code)]
pub async fn version(src: &PostgresConfigSource, json: bool) -> Result<Outcome, CliError> {
    let v = src.version().await?.unwrap_or(0);
    Ok(Outcome::ok(if json {
        // Pretty-printed, matching every other `--json` path (`render::json`) — one
        // convention across the CLI, not a compact one-off.
        serde_json::to_string_pretty(&serde_json::json!({ "version": v }))
            .map_err(|e| CliError::error(e.to_string()))?
    } else if v == 0 {
        // v0 is ambiguous: it is both "nothing has ever been pushed" and "the durable
        // generation zero". Say which one it is rather than making the operator guess.
        "config version: 0 (no config has been pushed)".to_string()
    } else {
        format!("config version: {v}")
    }))
}

/// The shared tail of both writable `plan_push` outcomes (`Apply` and a confirmed
/// `NeedsConfirmation`): re-check the generation immediately before writing, then
/// `store_and_bump` and report the new version.
///
/// The re-check is load-bearing, not decorative: `current_v` was computed from the
/// snapshot `push` took in step 2, and on the confirmation path a human can sit on the
/// prompt for minutes. A concurrent writer landing in that window means the approved
/// diff no longer describes what this replace-all would do — writing anyway would
/// silently destroy entities the operator never saw (and on the `Apply` path, which
/// never prompts at all, with zero operator visibility). Sharing this one function
/// between both call sites is what makes the guard cover both of them.
///
/// `text` is `Some(diff)` ONLY on the `Apply` path, which never calls `confirm` and so
/// is the only place the operator will ever see the diff at all — it must appear in
/// EITHER outcome (success or a stale-generation refusal). On the confirmed
/// `NeedsConfirmation` path `confirm` already displayed the diff once; passing `None`
/// there keeps this function from repeating it (an operator would otherwise see the
/// full diff — and the "REMOVES N" warning — twice on every confirmed push, which
/// trains skimming on the one line that must not be skimmed, and for a large diff
/// scrolls the copy they actually approved out of view).
// Only called from `push`, which itself isn't consumed outside tests until Task 10
// (main.rs clap dispatch) — see the allow there.
#[allow(dead_code)]
async fn write_and_report(
    src: &PostgresConfigSource,
    incoming: &RegistryConfig,
    current_v: u64,
    text: Option<&str>,
) -> Result<Outcome, CliError> {
    let prefix = text.map(|t| format!("{t}\n")).unwrap_or_default();
    let now_v = src.version().await?.unwrap_or(0);
    if now_v != current_v {
        return Ok(Outcome::precondition(format!(
            "{prefix}refused: durable config moved v{current_v} -> v{now_v} while this diff was \
             being reviewed; nothing written. Re-run `torii config push` to see the current diff."
        )));
    }
    let v = src.store_and_bump(incoming).await?;
    Ok(Outcome::ok(format!("{prefix}pushed: config now at v{v}")))
}

/// `confirm` is called ONLY when the diff removes something and `--yes` was absent.
/// Task 10 wires it to [`interactive_confirm`] against real stdin/stderr; a scripted
/// push (`< /dev/null`, a cron job) hits EOF there, which `interactive_confirm`
/// specifies as a refusal — so a scripted push that would delete config refuses
/// instead of proceeding. `push` itself has no way to enforce that through the `&mut
/// dyn FnMut` shape; it is `interactive_confirm`'s contract, verified by its own tests.
// Consumed by Task 10 (main.rs clap dispatch), `torii config push <dir>`.
#[allow(dead_code)]
pub async fn push(
    src: &PostgresConfigSource,
    dir: &Path,
    yes: bool,
    confirm: &mut dyn FnMut(&str) -> bool,
) -> Result<Outcome, CliError> {
    // 1. Load AND VALIDATE the incoming config before touching a single row.
    let incoming = FilesystemConfigSource::new(dir).load().await.map_err(|e| {
        CliError::error(format!(
            "refusing to push: cannot load {}: {e}",
            dir.display()
        ))
    })?;
    Registry::from_config(incoming.clone()).map_err(|e| {
        CliError::error(format!(
            "refusing to push: {} does not assemble into a valid registry: {e}",
            dir.display()
        ))
    })?;

    // 2. One atomic read of the durable (content, generation) pair.
    let (current, current_v) = src.load_versioned().await?;
    let current_v = current_v.unwrap_or(0);

    // 3. Decide.
    let source = sanitized_source(dir);
    match plan_push(&current, &incoming, yes) {
        PushDecision::NoOp(_) => Ok(Outcome::ok(format!(
            "no changes: {source} already matches durable config v{current_v}"
        ))),
        PushDecision::NeedsConfirmation(d) => {
            let text = describe_diff(&d, current_v, &source);
            if !confirm(&text) {
                // `confirm` already showed `text` to the operator — do not repeat it.
                return Ok(Outcome::precondition(format!(
                    "refused: nothing written, config still at v{current_v}"
                )));
            }
            // `confirm` already showed `text` — see `write_and_report`'s doc.
            write_and_report(src, &incoming, current_v, None).await
        }
        PushDecision::Apply(d) => {
            let text = describe_diff(&d, current_v, &source);
            // This path never calls `confirm` — `text` must appear in the outcome, see
            // `write_and_report`'s doc.
            write_and_report(src, &incoming, current_v, Some(&text)).await
        }
    }
}

/// The push source label rendered in both `describe_diff`'s header and the `NoOp`
/// message. `dir.display()` is free text — the incoming path is not necessarily typed
/// by the consenting human (a wrapper script, a Makefile variable, a CI job
/// interpolating a repo-supplied value) — so it gets the same [`one_line`] guard as an
/// entity name, removing the need to reason about provenance at all.
// Only called from `push` — see the allow there.
#[allow(dead_code)]
fn sanitized_source(dir: &Path) -> String {
    one_line(&dir.display().to_string())
}

/// The real confirmer: Task 10 calls this with `stdin().lock()` and `stderr()`. Writes
/// `prompt` plus a `Continue? [y/N] ` cue to `w`, reads one line from `r`, and returns
/// true ONLY for an explicit affirmative (`y`/`yes`, case-insensitive). EOF (nothing to
/// read — a scripted push with stdin redirected from `/dev/null`), an empty line, and a
/// failure to DISPLAY the prompt all return false: the safe default for a destructive,
/// unrecoverable write is refusal, not an ambiguous "empty means yes" — and if the
/// operator never saw the diff, there is nothing for them to have consented to. (A
/// silently-discarded write error, e.g. `2>/dev/null`, cannot be distinguished from a
/// successfully rendered prompt and is out of scope — `IsTerminal` would be the fuller
/// answer, and a deliberately piped `y` is arguably equivalent to `--yes` anyway.)
///
/// The read is bounded to 64 bytes — far more than any legitimate answer — so a
/// pathological input (`torii config push ./cfg < /dev/zero`) cannot buffer an
/// unbounded line into memory.
// Consumed by Task 10 (main.rs clap dispatch) as the real, non-test `confirm` callback.
#[allow(dead_code)]
pub fn interactive_confirm(prompt: &str, r: &mut impl BufRead, w: &mut impl Write) -> bool {
    if writeln!(w, "{prompt}").is_err()
        || write!(w, "Continue? [y/N] ").is_err()
        || w.flush().is_err()
    {
        // Could not show the operator the diff, so there is nothing to consent to.
        return false;
    }
    let mut line = String::new();
    let read = (&mut *r).take(64).read_line(&mut line).unwrap_or(0);
    if read == 0 {
        return false;
    }
    matches!(line.trim().to_ascii_lowercase().as_str(), "y" | "yes")
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{Activation, SkillDef};

    fn skill(name: &str, body: &str) -> SkillDef {
        SkillDef {
            name: name.into(),
            description: None,
            body: body.into(),
            activation: Activation::default(),
        }
    }

    fn cfg(skills: Vec<SkillDef>) -> RegistryConfig {
        RegistryConfig {
            agents: vec![],
            skills,
            tools: vec![],
            chain_bindings: vec![],
        }
    }

    #[test]
    fn a_pure_addition_applies_without_confirmation() {
        let d = plan_push(&cfg(vec![]), &cfg(vec![skill("s", "b")]), false);
        assert!(matches!(d, PushDecision::Apply(_)), "{d:?}");
    }

    #[test]
    fn an_identical_config_is_a_noop() {
        let c = cfg(vec![skill("s", "b")]);
        let d = plan_push(&c, &c, false);
        assert!(matches!(d, PushDecision::NoOp(_)), "{d:?}");
    }

    /// AC4: a removal without confirmation must refuse, so nothing is written.
    #[test]
    fn a_removal_without_confirmation_is_refused() {
        let d = plan_push(&cfg(vec![skill("s", "b")]), &cfg(vec![]), false);
        match d {
            PushDecision::NeedsConfirmation(diff) => {
                assert_eq!(diff.removed.len(), 1);
            }
            other => panic!("a removal must refuse without --yes: {other:?}"),
        }
    }

    #[test]
    fn a_removal_with_confirmation_applies() {
        let d = plan_push(&cfg(vec![skill("s", "b")]), &cfg(vec![]), true);
        assert!(matches!(d, PushDecision::Apply(_)), "{d:?}");
    }

    #[test]
    fn describe_diff_names_the_removals_and_the_current_version() {
        let d = diff(
            &cfg(vec![skill("gone", "b")]),
            &cfg(vec![skill("new", "b")]),
        );
        let text = describe_diff(&d, 7, "./config");
        assert!(text.contains("v7"), "{text}");
        assert!(text.contains("./config"), "{text}");
        assert!(text.contains("gone"), "removals must be named: {text}");
        assert!(text.contains("new"), "additions must be named: {text}");
        assert!(
            text.contains("REMOVES"),
            "the destructive fact must be loud: {text}"
        );
    }

    /// FIX 2 reachability: a config entity name is free text from a JSON/md file that
    /// passes `Registry::from_config` untouched (no shape validation on names) — so an
    /// operator-hostile name can embed a fake status line. Without sanitization this
    /// name would forge torii's own "no changes" message AND a fake unchanged-count
    /// after the real removal line, hiding what is actually being destroyed.
    #[test]
    fn describe_diff_sanitizes_a_multiline_entity_name_so_it_cannot_forge_a_status_line() {
        use crate::diff::{DiffEntry, EntityKind};
        let d = ConfigDiff {
            added: vec![],
            changed: vec![],
            removed: vec![DiffEntry {
                kind: EntityKind::Skill,
                name: "zzz\n  = 9 unchanged\n\nno changes: ./cfg already matches durable config v7"
                    .into(),
            }],
            unchanged: 0,
        };
        let text = describe_diff(&d, 7, "./cfg");
        let removal_lines: Vec<&str> = text
            .lines()
            .filter(|l| l.trim_start().starts_with("- skill"))
            .collect();
        assert_eq!(
            removal_lines.len(),
            1,
            "the malicious name's embedded newlines must not forge extra removal lines \
             or a fake standalone status line — the whole payload must stay glued to its \
             own '- skill' line: {text}"
        );
    }

    /// FIX 2 reachability: a chain binding's `kind` containing a cursor-up-and-erase
    /// ANSI sequence (`\x1b[4A\x1b[2K...`) could blank the REAL removal lines printed
    /// before it on a real terminal, replacing them with a fabricated line. Proving no
    /// raw escape byte survives is what closes that: without it, no cursor movement or
    /// erasure can occur when the text is printed.
    #[test]
    fn describe_diff_collapses_an_escape_sequence_in_an_entity_name() {
        use crate::diff::{DiffEntry, EntityKind};
        let d = ConfigDiff {
            added: vec![],
            changed: vec![],
            removed: vec![DiffEntry {
                kind: EntityKind::ChainBinding,
                name: "k\u{1b}[4A\u{1b}[2K  - skill  routine-cleanup\u{1b}[K".into(),
            }],
            unchanged: 0,
        };
        let text = describe_diff(&d, 1, "./cfg");
        assert!(
            !text.contains('\u{1b}'),
            "no raw escape byte may survive: {text:?}"
        );
        assert_eq!(
            text.lines()
                .filter(|l| l.contains("routine-cleanup"))
                .count(),
            1,
            "the payload renders as inert text on its own line, not a cursor-control escape: {text}"
        );
        assert!(
            text.contains("REMOVES 1 entity"),
            "the destructive fact must still render intact: {text}"
        );
    }

    #[test]
    fn describe_diff_pluralizes_a_single_removal_correctly() {
        let d = diff(&cfg(vec![skill("only", "b")]), &cfg(vec![]));
        let text = describe_diff(&d, 1, "./cfg");
        assert!(
            text.contains("This REMOVES 1 entity."),
            "singular must not read '1 entities': {text}"
        );
    }

    // ---- interactive_confirm (FIX 5) --------------------------------------------------

    #[test]
    fn interactive_confirm_refuses_on_eof() {
        let mut r = std::io::Cursor::new(b"" as &[u8]);
        let mut w: Vec<u8> = Vec::new();
        assert!(!interactive_confirm("prompt", &mut r, &mut w));
    }

    #[test]
    fn interactive_confirm_refuses_on_an_empty_line() {
        let mut r = std::io::Cursor::new(b"\n" as &[u8]);
        let mut w: Vec<u8> = Vec::new();
        assert!(!interactive_confirm("prompt", &mut r, &mut w));
    }

    #[test]
    fn interactive_confirm_accepts_an_explicit_yes() {
        let mut r = std::io::Cursor::new(b"y\n" as &[u8]);
        let mut w: Vec<u8> = Vec::new();
        assert!(interactive_confirm("prompt", &mut r, &mut w));
    }

    /// A `Write` whose every call errors — simulating a closed stderr (`2>&-`, EBADF)
    /// or a broken pipe into a dead pager.
    struct FailingWriter;
    impl Write for FailingWriter {
        fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
    }

    /// MINOR 1: if the prompt cannot be displayed at all, there is nothing for the
    /// operator to have consented to — a `y` sitting on stdin (e.g. a pre-typed
    /// keystroke, or a piped answer meant for a DIFFERENT, earlier prompt) must not be
    /// accepted as consent to a diff the operator never saw.
    #[test]
    fn interactive_confirm_fails_closed_when_the_prompt_cannot_be_displayed() {
        let mut r = std::io::Cursor::new(b"y\n" as &[u8]);
        let mut w = FailingWriter;
        assert!(
            !interactive_confirm("prompt", &mut r, &mut w),
            "a write/flush failure must refuse, not fall through to reading stdin"
        );
    }

    /// MINOR 4: unbounded `read_line` would buffer an arbitrarily large line into
    /// memory (`torii config push ./cfg < /dev/zero`). `Cursor::position()` reports
    /// exactly how many bytes were consumed from the input, so this proves the read is
    /// bounded rather than merely proving the function returns eventually.
    #[test]
    fn interactive_confirm_bounds_the_read_so_a_huge_line_cannot_allocate_unboundedly() {
        let huge = vec![b'a'; 10_000_000];
        let mut r = std::io::Cursor::new(huge.as_slice());
        let mut w: Vec<u8> = Vec::new();
        let accepted = interactive_confirm("prompt", &mut r, &mut w);
        assert!(!accepted, "10MB of 'a' is not an explicit yes");
        assert!(
            r.position() <= 64,
            "read_line must be bounded to a few bytes, not the whole 10MB input: consumed {}",
            r.position()
        );
    }

    /// MINOR 2: `source` is free text (`dir.display()`) never validated for shape — the
    /// incoming path is not necessarily typed by the consenting human (a wrapper
    /// script, a CI job interpolating a repo-supplied value). An unsanitized escape
    /// here (e.g. SGR conceal, `\x1b[8m`) could render the entire removal list and the
    /// "REMOVES N" warning invisible on a terminal that implements it.
    #[test]
    fn describe_diffs_output_never_contains_a_raw_escape_from_the_source_path() {
        let dir = Path::new("./cfg\u{1b}[8m");
        let d = diff(&cfg(vec![]), &cfg(vec![skill("s", "b")]));
        let text = describe_diff(&d, 1, &sanitized_source(dir));
        assert!(
            !text.contains('\u{1b}'),
            "the source path must be sanitized before rendering: {text:?}"
        );
    }

    // ---- DB: the diff-goes-stale race (FIX 1) ---------------------------------------
    // Requires a live PG at $DATABASE_URL with the dbd schema applied; skips otherwise,
    // matching the convention in orchestrator-store's postgres.rs tests. `store_and_bump`
    // is replace-all, so seeding via it establishes exactly our state regardless of
    // leftover rows from other probing.

    fn db_url() -> Option<String> {
        std::env::var("DATABASE_URL").ok()
    }

    /// An existing, empty directory: `FilesystemConfigSource::load` treats a missing
    /// SUBdir as empty, but a missing ROOT is loud — so the root itself must exist.
    fn empty_config_dir() -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!("torii-cfg-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    /// Reproduces the reviewer's live race: durable `{a, b}` is read for the prompt;
    /// while the (simulated) operator is still looking at it, a concurrent writer lands
    /// `{survivor}` and bumps the generation; only THEN does the operator answer `y`.
    /// Before FIX 1, `push` wrote unconditionally and the concurrent writer's entity
    /// was silently destroyed. After FIX 1, the stale generation must be caught and the
    /// write refused, leaving the concurrent writer's content intact.
    #[tokio::test]
    async fn a_confirmed_push_refuses_when_the_diff_went_stale_mid_prompt() {
        let Some(url) = db_url() else { return };
        let src =
            PostgresConfigSource::new(orchestrator_store::postgres::connect(&url).await.unwrap());

        let a = format!("a-{}", uuid::Uuid::new_v4());
        let b = format!("b-{}", uuid::Uuid::new_v4());
        src.store_and_bump(&cfg(vec![skill(&a, "x"), skill(&b, "y")]))
            .await
            .unwrap();

        // The incoming push removes everything — requires confirmation.
        let dir = empty_config_dir();
        let survivor = format!("survivor-{}", uuid::Uuid::new_v4());

        // The confirm callback is the only hook available to inject code between the
        // snapshot read and the eventual write — exactly where a human reading the
        // prompt for minutes leaves a window open. It spins up its own thread + runtime
        // so the concurrent write runs to completion independently of this test's
        // runtime, then answers "yes".
        let mut confirm = |_text: &str| {
            let url = url.clone();
            let entry = skill(&survivor, "z");
            let handle = std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    let concurrent = PostgresConfigSource::new(
                        orchestrator_store::postgres::connect(&url).await.unwrap(),
                    );
                    concurrent
                        .store_and_bump(&RegistryConfig {
                            agents: vec![],
                            skills: vec![entry],
                            tools: vec![],
                            chain_bindings: vec![],
                        })
                        .await
                        .unwrap();
                });
            });
            handle.join().unwrap();
            true
        };

        let out = push(&src, &dir, false, &mut confirm)
            .await
            .expect("no hard error");
        std::fs::remove_dir_all(&dir).ok();

        assert_eq!(
            out.code,
            crate::errors::EXIT_PRECONDITION,
            "a push racing a concurrent writer must refuse, not silently destroy: {}",
            out.text
        );
        assert!(out.text.contains("moved v"), "{}", out.text);
        assert!(out.text.contains("nothing written"), "{}", out.text);
        // MINOR 3: `confirm` already displayed the diff once (the closure above IS the
        // display, standing in for `interactive_confirm` writing it to stderr) — the
        // stale-generation refusal must not repeat it.
        assert!(
            !out.text.contains("config diff (durable v"),
            "confirm already showed the diff; the stale-refusal must not repeat it: {}",
            out.text
        );

        let after = src.load().await.unwrap();
        assert!(
            after.skills.iter().any(|s| s.name == survivor),
            "the concurrent writer's entity must survive an aborted push: {:?}",
            after.skills
        );
    }

    /// MINOR 3, the decline path: `confirm` returns false without ever seeing a
    /// concurrent write. The refusal must still say "refused" but must NOT repeat the
    /// diff `confirm` already displayed.
    #[tokio::test]
    async fn a_declined_push_refuses_without_repeating_the_diff_text() {
        let Some(url) = db_url() else { return };
        let src =
            PostgresConfigSource::new(orchestrator_store::postgres::connect(&url).await.unwrap());
        let a = format!("decline-{}", uuid::Uuid::new_v4());
        src.store_and_bump(&cfg(vec![skill(&a, "x")]))
            .await
            .unwrap();

        let dir = empty_config_dir(); // removes everything -> requires confirmation
        let mut confirm = |_text: &str| false; // the operator declines
        let out = push(&src, &dir, false, &mut confirm)
            .await
            .expect("no hard error");
        std::fs::remove_dir_all(&dir).ok();

        assert_eq!(out.code, crate::errors::EXIT_PRECONDITION, "{}", out.text);
        assert!(out.text.contains("refused"), "{}", out.text);
        assert!(
            !out.text.contains("config diff (durable v"),
            "confirm already showed the diff; the decline outcome must not repeat it: {}",
            out.text
        );
    }

    /// MINOR 3, the `Apply` path (via `write_and_report` directly): `Apply` never calls
    /// `confirm`, so `text` must survive into the outcome — including on a
    /// stale-generation refusal, which is deliberately forced here (rather than raced)
    /// with an impossible `current_v`, so the assertion holds regardless of any
    /// concurrent activity from other tests sharing the same durable generation.
    #[tokio::test]
    async fn write_and_report_keeps_the_diff_text_when_confirm_was_never_called() {
        let Some(url) = db_url() else { return };
        let src =
            PostgresConfigSource::new(orchestrator_store::postgres::connect(&url).await.unwrap());
        let text = "config diff (durable vX -> ./cfg):\n  - skill  gone\n".to_string();
        let out = write_and_report(&src, &cfg(vec![]), u64::MAX, Some(&text))
            .await
            .unwrap();
        assert_eq!(out.code, crate::errors::EXIT_PRECONDITION, "{}", out.text);
        assert!(out.text.contains("moved v"), "{}", out.text);
        assert!(
            out.text.contains("config diff (durable vX"),
            "the Apply path never calls confirm, so a refusal must still show the diff: {}",
            out.text
        );
    }

    /// MINOR 3, the confirmed-write path (via `write_and_report` directly): the
    /// counterpart of the test above — `None` means `confirm` already displayed the
    /// diff, so neither a success NOR a refusal may repeat it. Forced with an
    /// impossible `current_v`, same rationale as above.
    #[tokio::test]
    async fn write_and_report_omits_the_diff_text_when_confirm_already_showed_it() {
        let Some(url) = db_url() else { return };
        let src =
            PostgresConfigSource::new(orchestrator_store::postgres::connect(&url).await.unwrap());
        let out = write_and_report(&src, &cfg(vec![]), u64::MAX, None)
            .await
            .unwrap();
        assert_eq!(out.code, crate::errors::EXIT_PRECONDITION, "{}", out.text);
        assert!(out.text.contains("moved v"), "{}", out.text);
        assert!(out.text.contains("nothing written"), "{}", out.text);
        assert!(
            !out.text.contains("config diff (durable v"),
            "confirm already showed the diff; the outcome must not repeat it: {}",
            out.text
        );
    }
}
