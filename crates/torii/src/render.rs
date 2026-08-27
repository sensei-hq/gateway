//! Operator output: a human table by default, `--json` for scripting.

use std::sync::LazyLock;

use chrono::{DateTime, Utc};
use orchestrator_core::{PatternRedactor, Redactor, ScheduledRun};

/// A NULL `next_wake` means "never auto-woken; needs `torii run wake`" (the s3
/// in-doubt class). It renders as an em dash in the table and `null` in JSON.
fn fmt_wake(w: Option<DateTime<Utc>>) -> String {
    match w {
        Some(t) => t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        None => "—".to_string(),
    }
}

/// A pause reason is free text from a provider or a pause site, so it can contain
/// newlines and tabs. The table is line-oriented, so a raw newline would split one
/// run's row into fragments with no id prefix — and a UUID inside such a fragment
/// reads as a separate row, which is how an operator ends up cancelling the wrong
/// run. Collapse control characters for DISPLAY only; JSON keeps the raw value.
///
/// `char::is_control` covers Unicode category Cc, which includes ESC (`\u{1b}`) — so
/// this also collapses ANSI escape sequences, not just newlines/tabs. `pub(crate)`
/// because `cmd::config::describe_diff` shares it: an entity name is equally free
/// text, and its diff text is the destruction consent an operator reads before
/// approving a replace-all write, so it needs the identical guard.
pub(crate) fn one_line(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

/// Built once (SP-DATA-4.1 task 2) — `PatternRedactor::default()` compiles a regex
/// set, which is not free to redo per row. Shared by every redaction path in this
/// crate: pause reasons ([`redact_reason`]) and SP-6 s1 signal payloads
/// ([`redact_payload`]), so there is exactly ONE redactor here, not two.
static REDACTOR: LazyLock<PatternRedactor> = LazyLock::new(PatternRedactor::default);

/// SP-6 s1 §6.4: scrub a `torii run signal --payload` value with the SP-4 s2 redactor
/// **before it is journaled**.
///
/// Task 3 redacts on the fold-READ side, which covers the node's return and the
/// blackboard write it derives from it. It does NOT cover the journal ROW, and
/// [`crate::cmd::run::signal`] is that row's only writer — so without this, a human who
/// pastes a token has put it into durable storage permanently, where a later fold hands
/// it to a model prompt. Redacting on both sides is intentional and harmless: the
/// redactor is idempotent, because `[REDACTED]` matches no credential shape.
///
/// Deliberately the PLAIN [`Redactor`] pass, NOT [`redact_reason`]'s stricter
/// withhold-on-evasion transform. A pause reason is only ever displayed, so discarding it
/// wholesale costs nothing; a payload BECOMES the node's output, and the executor applies
/// this same plain pass to whatever it folds. Applying a different transform here would
/// make the value torii writes disagree with the value the executor would produce from
/// it — the exact live/journaled/replayed divergence s2's determinism rule exists to
/// prevent.
pub(crate) fn redact_payload(v: &serde_json::Value) -> serde_json::Value {
    REDACTOR.redact(v)
}

/// The literal placeholder `PatternRedactor` substitutes on a match
/// (`crates/orchestrator-core/src/redact.rs`, `PLACEHOLDER`). Not exported — it is a
/// private const there — so it is duplicated here. Only `safe_reason`'s
/// mid-placeholder cap guard depends on this literal staying in sync; the evasion
/// check in `redact_reason` below does not (it compares whole redacted strings, not
/// this substring), so a drift here would silently reopen only the MINOR
/// straddled-truncation cosmetic issue, not the CRITICAL leak.
const PLACEHOLDER_TEXT: &str = "[REDACTED]";

/// What a WITHHELD reason renders as: the whole reason discarded, not a partial
/// redaction. See `redact_reason` for when this fires.
const WITHHELD_REASON: &str = "[REDACTED: reason withheld]";

/// A pause reason is `ScheduledRun.reason`: free text lifted from `PauseInfo.reason`
/// and provider messages (SP-DATA-3), stored UNREDACTED — the SP-4 s2 `Redactor`
/// covers effect outputs and model output, not pause reasons, and torii is the first
/// thing to DISPLAY them. Scrub here, at display time, not at write time in the
/// scheduler: write-time would mean injecting a `Redactor` into `Scheduler` and
/// changing what lands in durable storage, which is a larger question about the
/// redactor's coverage and touches the determinism reasoning s2 was careful about.
/// Display-time closes the exposure torii itself introduces, costs nothing, and
/// leaves the durable row truthful. **The durable `scheduled_runs.reason` column
/// still holds the raw, unredacted text** — anyone querying Postgres directly is
/// still exposed; that residue is a recorded carry-forward, not fixed by this.
///
/// `Redactor::redact` operates on `serde_json::Value`, not `&str`, so a plain string
/// is wrapped and unwrapped around the call.
///
/// **A control-character evasion, and why this withholds rather than half-redacts.**
/// `PatternRedactor`'s whole-match patterns (`sk-[A-Za-z0-9_-]{20,}` and siblings)
/// are contiguous character classes that exclude control characters. A secret with
/// one control byte spliced into the middle —
/// `"sk-AAAAAAAAAAAA\u{1}AAAAAAAAAAAA"` — therefore fails to match as a whole: the
/// pattern only ever sees two 12-char runs, neither long enough to fire, and
/// `one_line`'s later newline→space collapse does not create this leak, it just
/// fails to hide it (the pattern already missed on the raw text). This is unmodified
/// SP-4 s2 behavior — `PatternRedactor` is documented there as best-effort-by-shape
/// — and changing it is out of scope here: its blast radius is effect outputs and
/// model output, a separate design question. The defense on THIS display path is to
/// DETECT the evasion rather than out-pattern it: redact once as given, redact again
/// on a control-character-STRIPPED copy (characters removed outright, not collapsed
/// to spaces, so a split secret reassembles into one contiguous run), then check
/// whether stripping control characters OUT of the already-redacted original agrees
/// with stripping-then-redacting. If a secret was fully caught on the first pass (or
/// never present at all), both orderings reduce to the same string — stripping a
/// control character from an inert placeholder or from ordinary prose does not
/// depend on when it happens. A disagreement means the ordering mattered, which only
/// happens when stripping-first exposed a contiguous run the original pass missed.
///
/// A simpler placeholder-COUNT comparison (redacted-original vs redacted-stripped)
/// was tried first and rejected: verified against `"Bearer abc123defghi\u{1}abc123defghi"`,
/// the RAW text partially matches — `bearer\s+[A-Za-z0-9._-]{8,}` consumes "Bearer "
/// plus the first fragment before the control byte stops the class — producing ONE
/// placeholder, the SAME count as the fully-reassembled stripped pass (also one
/// placeholder, now covering both fragments). The counts tie while the CONTENT
/// differs: the original pass leaves the second fragment as raw trailing text
/// (`"[REDACTED] abc123defghi"`). Comparing full strings after normalizing control
/// characters catches this; comparing counts does not.
///
/// On a disagreement, withhold the ENTIRE reason rather than guessing which part is
/// safe to keep: this is a display path, and a legitimate provider message does not
/// contain a credential bisected by a control byte, so there is no honest partial
/// rendering to fall back to.
///
/// `pub(crate)` because `run list-paused`'s `--json` path shares it: a per-run journal
/// fault is rendered into `awaiting_error`, and free text bound for a script gets exactly
/// the treatment [`json`] already gives `reason` — redaction only, since the
/// control-character collapse and the length cap are display-only concerns.
pub(crate) fn redact_reason(s: &str) -> String {
    let redact_once = |text: &str| -> String {
        match REDACTOR.redact(&serde_json::Value::String(text.to_string())) {
            serde_json::Value::String(out) => out,
            other => {
                unreachable!("redacting a Value::String must yield a Value::String, got {other:?}")
            }
        }
    };
    let strip_control =
        |text: &str| -> String { text.chars().filter(|c| !c.is_control()).collect() };

    let redacted_first = redact_once(s);
    let redacted_first_then_stripped = strip_control(&redacted_first);
    let stripped_first_then_redacted = redact_once(&strip_control(s));

    if redacted_first_then_stripped != stripped_first_then_redacted {
        return WITHHELD_REASON.to_string();
    }
    redacted_first
}

/// Rendered reasons are capped so one unbounded provider message can't wreck the
/// table's column alignment or scroll an operator's terminal off-screen.
const REASON_MAX: usize = 300;

/// The table-display transform for a reason: redact, THEN collapse control
/// characters (`one_line`), THEN cap length.
///
/// Order: redact-before-collapse looks like the obviously safer order on its face —
/// collapse-first could in principle glue two halves of a newline-split secret into
/// a form that no longer matches a pattern that only fires on the concatenated text.
/// Checked directly against `PatternRedactor`'s patterns
/// (`crates/orchestrator-core/src/redact.rs`): every whole-match pattern is a
/// contiguous character class that excludes BOTH raw control characters and the
/// space `one_line` replaces them with, so — at the level of `PatternRedactor`
/// alone — a secret split across an embedded control character fails to match
/// EITHER before or after collapsing; the two orders are equivalent there. That
/// finding is NOT what makes a split secret safe, though — equivalently-failing is
/// still failing, since `PatternRedactor` alone leaves the un-caught fragments as
/// plain text either way. The actual defense against a split secret is
/// `redact_reason`'s own evasion check (see its doc comment), which runs regardless
/// of order and withholds the whole reason when it detects one. Redact-first is kept
/// here anyway as the safer general default — it costs nothing — but this function
/// does not need to (and does not) carry the split-secret defense itself.
fn safe_reason(s: &str) -> String {
    let redacted = redact_reason(s);
    let collapsed = one_line(&redacted);
    cap_chars(&collapsed, REASON_MAX)
}

/// Cap `s` to at most `max` CHARACTERS (not bytes — a byte-offset slice through a
/// multi-byte character, anything outside ASCII, would panic at the split point),
/// appending `…` when truncated.
///
/// MINOR fix: if the naive cut point at `max` would land strictly inside an
/// occurrence of the redaction placeholder, back the cut up to just before that
/// occurrence starts instead. No data is disclosed either way — the secret behind
/// the placeholder was already replaced before this function ever sees it — but a
/// straddled cut renders a confusing shard like `[REDA…` in scrollback, which reads
/// as a truncated SECRET rather than a truncated placeholder.
pub(crate) fn cap_chars(s: &str, max: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max {
        return s.to_string();
    }
    let placeholder: Vec<char> = PLACEHOLDER_TEXT.chars().collect();
    let plen = placeholder.len();
    let mut cut = max;
    if plen <= chars.len() {
        for start in 0..=(chars.len() - plen) {
            if start < max && max < start + plen && chars[start..start + plen] == placeholder[..] {
                cut = start;
                break;
            }
        }
    }
    let mut truncated: String = chars[..cut].iter().collect();
    truncated.push('…');
    truncated
}

pub fn table(rows: &[ScheduledRun]) -> String {
    let mut s = String::from(
        "RUN                                   STATUS     NEXT WAKE             REASON\n",
    );
    for r in rows {
        s.push_str(&format!(
            "{}  {:<9}  {:<20}  {}\n",
            r.run.0,
            r.status.as_str(),
            fmt_wake(r.next_wake),
            r.reason.as_deref().map(safe_reason).unwrap_or_default()
        ));
    }
    s
}

/// SP-6 s1: one node currently waiting for a human, folded out of a run's journal
/// (`SignalAwaited`/`GateAwaited`, minus anything that has since terminated the node).
/// `RunPaused` is not node-keyed, so this is the only way an operator can learn WHAT to
/// answer without reading the graph.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct AwaitingNode {
    pub node: orchestrator_core::NodeId,
    /// `None` is the INDEFINITE class: `resume_after: None`, so the durable scheduler
    /// never auto-wakes it and it waits for a human forever. That is the case an
    /// operator is most likely to lose track of, so it renders explicitly rather than
    /// blank.
    pub deadline: Option<DateTime<Utc>>,
    /// SP-6 s2: the menu, for a `HumanGate`. `None` = an `AwaitSignal`, which takes
    /// arbitrary JSON and so has no menu to show.
    ///
    /// Read from the journaled `GateAwaited`, so `list-paused` needs no graph load —
    /// which matters because `list-paused` folds one journal per paused run and has no
    /// graph in hand.
    ///
    /// **Skipped when absent rather than serialized as `null`**, so a run with no gate
    /// produces byte-identical `--json` to the pre-s2 output and a script written against
    /// s1 is unaffected. Key PRESENCE is the discriminator on that path — the same
    /// technique `list_paused` uses for `awaiting_error`, and for the same reason: a
    /// script must be able to tell the two waiting kinds apart, and it needs the menu to
    /// build a `gate decide` without loading the graph either.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<String>>,
}

/// A node id is author- (or planner-) supplied free text, so it gets the same
/// control-character collapse and length cap a pause reason does — for the same reason:
/// a raw newline would fragment this block into lines that read as separate rows.
/// It is NOT redacted: an id is structural, not a value, exactly as `PatternRedactor`
/// leaves object KEYS alone.
const NODE_MAX: usize = 80;

/// The same cap for a rendered MENU, which is author free text on the same line and
/// bounded by nothing upstream: `Graph::validate_dag` checks a `HumanGate`'s options for
/// non-emptiness, uniqueness and a reachable outcome — never for LENGTH, and never for
/// how many there are. One 5,000-character option name, or a fifty-option menu, would
/// otherwise wreck the alignment of every other row in this block. Wider than a node id
/// because it holds several names joined together, and still well under the 300 a pause
/// reason is allowed.
///
/// `pub(crate)` because [`crate::cmd::gate::decide`] recites the SAME journaled option
/// names when it refuses an undeclared one, and had no cap at all — one bound for one
/// class of value, not two that can drift.
pub(crate) const MENU_MAX: usize = 160;

/// One run's awaiting set — or, when that run's journal could not be folded, the reason.
///
/// **Whole-slice review, Important.** The awaiting set is per-RUN and so is the fault that
/// hides it: `list-paused` loads one journal per paused run, and a single unreadable
/// journal (a `format_version` fence during a rolling deploy is the realistic case) used
/// to abort the whole command with an empty stdout, hiding every OTHER paused run —
/// including the ones an operator could still signal, wake or cancel. The error is still
/// never swallowed; it is reported in the row it belongs to. See [`crate::cmd::run::list_paused`].
pub type Awaiting = Result<Vec<AwaitingNode>, String>;

/// The `AWAITING A SIGNAL` block appended below `run list-paused`'s table.
///
/// Returns the EMPTY string when nothing is awaiting **and nothing failed**, which is what
/// keeps a run with no `AwaitSignal` node byte-identical to the pre-SP-6 output. A separate
/// block rather than a fifth column, deliberately: one paused run can have several awaiting
/// children (a Map fan-out — the accepted shape from SP-DATA-5 §6.3a), which a
/// single-line-per-run table cannot represent, and widening the shared `table()` would also
/// move `run status`'s columns.
///
/// **SP-6 s2: the block now holds BOTH waiting kinds, so each row says which it is** — a
/// gate renders its menu (`gate: ship|hold`), an `AwaitSignal` renders `signal`. The two
/// take different commands and refuse each other's, so listing them identically would
/// send an operator to a refusal for a node they had correctly identified. The extra
/// `gate decide` line in the header appears only when a gate is present, so the s1 output
/// is unchanged for a fleet that has none.
///
/// An [`Err`] row renders as `unknown: <error>` — never as an absent or empty awaiting set,
/// which is the one answer that would tell an operator there is nothing to signal on a run
/// that may be blocked on a human. The message goes through the same [`safe_reason`]
/// transform a pause reason does (redact, then collapse control characters, then cap): a
/// journal-backend fault is free text from the driver and can carry a connection string, a
/// newline that would forge a row, or an ANSI escape.
pub fn awaiting_section(rows: &[(orchestrator_core::RunId, Awaiting)]) -> String {
    let any = rows.iter().any(|(_, a)| match a {
        Ok(nodes) => !nodes.is_empty(),
        Err(_) => true,
    });
    if !any {
        return String::new();
    }
    let mut s = String::from(
        "\nAWAITING A SIGNAL — deliver with \
         `torii run signal <run> --node <node> --payload <json>`\n",
    );
    let any_gate = rows
        .iter()
        .any(|(_, a)| matches!(a, Ok(nodes) if nodes.iter().any(|n| n.options.is_some())));
    if any_gate {
        s.push_str(
            "                   a `gate:` row takes a named option instead — \
             `torii run gate decide <run> --node <node> --option <name>`\n",
        );
    }
    for (run, a) in rows {
        match a {
            Ok(nodes) => {
                for a in nodes {
                    // Option names are author free text reaching a line-oriented table, so
                    // they get the same control-character collapse and cap a node id does:
                    // a raw newline would forge an extra row, and an ESC could rewrite what
                    // is already on screen.
                    let cell = match &a.options {
                        Some(opts) => cap_chars(
                            &format!(
                                "gate: {}",
                                opts.iter()
                                    .map(|o| one_line(o))
                                    .collect::<Vec<_>>()
                                    .join("|")
                            ),
                            MENU_MAX,
                        ),
                        None => "signal".to_string(),
                    };
                    s.push_str(&format!(
                        "{}  {}  {}  {}\n",
                        run.0,
                        cap_chars(&one_line(&a.node.0), NODE_MAX),
                        cell,
                        match a.deadline {
                            Some(d) => format!("deadline {}", fmt_wake(Some(d))),
                            // Says what it MEANS, not just that the field is empty: this
                            // run is never auto-woken and will wait until a human acts.
                            None => "no deadline — waits until signalled".to_string(),
                        }
                    ));
                }
            }
            Err(e) => s.push_str(&format!("{}  unknown: {}\n", run.0, safe_reason(e))),
        }
    }
    s
}

/// JSON keeps the exact stored text otherwise — that is the existing `one_line`
/// precedent, deliberately NOT applied here, because a script consuming `--json`
/// wants the raw value and a newline is a display-only concern. A secret is
/// different: a script should not receive a credential either, so redaction (only —
/// no control-character collapse, no length cap, both of which stay display-only
/// concerns) applies on this path too. Rows are mapped through a redacted COPY
/// before serializing rather than post-processing the serialized JSON string, which
/// would be fragile and could corrupt escaping around the value it just rewrote.
pub fn json(rows: &[ScheduledRun]) -> Result<String, serde_json::Error> {
    let redacted: Vec<ScheduledRun> = rows
        .iter()
        .cloned()
        .map(|mut r| {
            r.reason = r.reason.map(|reason| redact_reason(&reason));
            r
        })
        .collect();
    serde_json::to_string_pretty(&redacted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{RunId, RunStatus};

    fn row(next_wake: Option<DateTime<Utc>>, reason: Option<&str>) -> ScheduledRun {
        ScheduledRun {
            run: RunId(uuid::Uuid::from_u128(
                0x1234_5678_9abc_def0_1234_5678_9abc_def0,
            )),
            status: RunStatus::Paused,
            next_wake,
            reason: reason.map(|s| s.to_string()),
            updated_at: DateTime::<Utc>::from_timestamp(3_000_000, 0).unwrap(),
        }
    }

    #[test]
    fn table_prints_the_full_run_id_so_it_can_be_pasted_into_cancel() {
        let r = row(None, None);
        let out = table(std::slice::from_ref(&r));
        assert!(
            out.contains(&r.run.0.to_string()),
            "the full uuid must appear verbatim: {out}"
        );
    }

    #[test]
    fn a_null_next_wake_renders_as_an_em_dash_in_the_table() {
        let out = table(&[row(None, Some("in-doubt mutation"))]);
        assert!(
            out.contains("—"),
            "NULL next_wake must be visibly distinct: {out}"
        );
        assert!(out.contains("in-doubt mutation"), "{out}");
    }

    #[test]
    fn a_timed_next_wake_renders_as_rfc3339() {
        let t = DateTime::<Utc>::from_timestamp(3_000_000, 0).unwrap();
        let out = table(&[row(Some(t), None)]);
        assert!(out.contains("1970-02-04T17:20:00Z"), "{out}");
    }

    #[test]
    fn json_renders_a_null_next_wake_as_json_null() {
        let out = json(&[row(None, None)]).expect("serializes");
        assert!(out.contains("\"next_wake\": null"), "{out}");
    }

    #[test]
    fn json_round_trips_back_into_scheduled_runs() {
        let t = DateTime::<Utc>::from_timestamp(3_000_000, 0).unwrap();
        let out = json(&[row(Some(t), Some("quota"))]).expect("serializes");
        let back: Vec<ScheduledRun> = serde_json::from_str(&out).expect("round-trips");
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].next_wake, Some(t));
        assert_eq!(back[0].reason.as_deref(), Some("quota"));
    }

    #[test]
    fn a_multiline_reason_cannot_forge_a_second_table_row() {
        let forged = uuid::Uuid::from_u128(0xdead_beef_dead_beef_dead_beef_dead_beef);
        let mut r = row(None, None);
        r.reason = Some(format!("provider conflict\n{forged} is stuck"));
        let out = table(&[r]);
        let data_lines = out.lines().filter(|l| !l.starts_with("RUN ")).count();
        assert_eq!(
            data_lines, 1,
            "one run must render as exactly one line:\n{out}"
        );
        assert!(
            out.contains("provider conflict"),
            "the reason text is kept: {out}"
        );
    }

    /// `char::is_control` is documented as covering Unicode category Cc — verify that
    /// actually includes ESC (`\u{1b}`) rather than assume it, since a `describe_diff`
    /// consent prompt depends on `one_line` collapsing ANSI cursor-control escapes, not
    /// just newlines/tabs.
    #[test]
    fn one_line_collapses_the_escape_control_character() {
        let out = one_line("k\u{1b}[4A\u{1b}[2Kerased\u{1b}[K");
        assert!(
            !out.contains('\u{1b}'),
            "no raw escape byte may survive: {out:?}"
        );
        assert_eq!(out.lines().count(), 1, "still a single line: {out:?}");
        assert!(out.contains("erased"), "the real text is kept: {out:?}");
    }

    #[test]
    fn json_status_is_lowercase_matching_as_str_and_the_db() {
        let out = json(&[row(None, None)]).expect("serializes");
        assert!(out.contains("\"status\": \"paused\""), "{out}");
        assert!(
            !out.contains("\"Paused\""),
            "PascalCase would break scripts: {out}"
        );
    }

    #[test]
    fn a_pause_reason_is_redacted_before_display() {
        let secret = format!("sk-{}", "A".repeat(24));
        let mut r = row(None, Some(&format!("quota exceeded for {secret}")));
        let out = table(&[r.clone()]);
        assert!(
            !out.contains(&secret),
            "a secret-shaped reason leaked: {out}"
        );
        assert!(out.contains("[REDACTED]"), "{out}");

        r.reason = Some(format!("quota exceeded for {secret}"));
        let j = json(&[r]).expect("serializes");
        assert!(!j.contains(&secret), "the JSON path leaked: {j}");
    }

    #[test]
    fn an_overlong_pause_reason_is_capped() {
        let long = "x".repeat(5_000);
        let out = table(&[row(None, Some(&long))]);
        let line = out.lines().nth(1).expect("a data row");
        assert!(
            line.len() < 400,
            "an unbounded reason wrecks the table: {} chars",
            line.len()
        );
        assert!(out.contains('…'), "truncation must be visible: {out}");
    }

    /// The cap must count CHARACTERS, not bytes — a naive byte-boundary slice through
    /// a multi-byte character panics. Every char here is 3 bytes (`€`), so a byte cap
    /// at 300 would land mid-character.
    #[test]
    fn the_cap_counts_characters_not_bytes_so_multibyte_reasons_do_not_panic() {
        let long = "€".repeat(500);
        let out = table(&[row(None, Some(&long))]);
        assert!(out.contains('…'), "truncation must be visible: {out}");
    }

    // -- Review follow-up: a secret split by an embedded control character evades
    // `PatternRedactor`'s contiguous-character-class patterns entirely (they exclude
    // control characters, so a control byte spliced into the middle of e.g.
    // `sk-AAAA...AAAA` breaks the match into two runs, neither long enough to fire).
    // `one_line`'s later newline→space collapse does not create the leak; the pattern
    // already failed to match on the raw text. These tests must fail before the fix.

    #[test]
    fn a_control_split_secret_is_withheld_entirely_not_partially_redacted() {
        let half = "A".repeat(12);
        let secret = format!("sk-{half}\u{1}{half}"); // 24 alnum chars total, split by one SOH byte
        let reason = format!("quota exceeded for {secret}");

        let out = table(&[row(None, Some(&reason))]);
        assert!(
            !out.contains(&half),
            "a 12-char fragment of the split key leaked verbatim: {out}"
        );
        assert!(
            out.contains("[REDACTED"),
            "must render a redaction marker, not raw text: {out}"
        );

        let j = json(&[row(None, Some(&reason))]).expect("serializes");
        assert!(
            !j.contains(&half),
            "the JSON path leaked a fragment of the split key: {j}"
        );
    }

    #[test]
    fn a_control_split_bearer_token_is_withheld_not_half_leaked() {
        // A partial match on the raw text ("Bearer " + the first fragment) can catch
        // ONE placeholder while leaving the second fragment as untouched trailing
        // text — this is the case a naive placeholder-count comparison misses.
        let half = "abc123defghi";
        let reason = format!("Bearer {half}\u{1}{half}");

        let out = table(&[row(None, Some(&reason))]);
        assert!(
            !out.contains(half),
            "the trailing half of a split bearer token leaked verbatim: {out}"
        );
    }

    #[test]
    fn a_newline_split_secret_is_withheld_not_glued_and_leaked() {
        // The shape a real garbled provider message is most likely to take.
        let half = "A".repeat(12);
        let secret = format!("sk-{half}\n{half}");
        let reason = format!("quota exceeded for {secret}");

        let out = table(&[row(None, Some(&reason))]);
        assert!(
            !out.contains(&half),
            "a 12-char fragment of the newline-split key leaked verbatim: {out}"
        );
    }

    /// Guards against the withholding being over-eager: a reason with control
    /// characters but genuinely no secret must still render normally (control chars
    /// collapsed via `one_line`, exactly as before), not get blanked.
    #[test]
    fn a_reason_with_control_characters_but_no_secret_still_renders_normally() {
        let reason = "provider timed out\u{1}please retry";
        let out = table(&[row(None, Some(reason))]);
        assert!(
            out.contains("provider timed out") && out.contains("please retry"),
            "an ordinary control-bearing reason must not be withheld: {out}"
        );
        assert!(
            !out.to_lowercase().contains("withheld"),
            "must not be over-eager: {out}"
        );
    }

    /// MINOR: the cap must not land mid-placeholder. `[REDACTED]` straddling the cut
    /// would render as e.g. `[REDA…`, disclosing nothing but confusing in scrollback.
    /// 295 'x's then a real (non-split) secret means the placeholder occupies chars
    /// [295, 305) — squarely straddling the 300-char cap.
    #[test]
    fn a_capped_reason_never_splits_the_redaction_placeholder() {
        let secret = format!("sk-{}", "B".repeat(30));
        let reason = format!("{}{}", "x".repeat(295), secret);

        let out = table(&[row(None, Some(&reason))]);
        let line = out.lines().nth(1).expect("a data row");
        assert!(out.contains('…'), "truncation must still be visible: {out}");
        assert!(
            line.contains("[REDACTED]") || !line.contains("[RED"),
            "a truncation must not leave a mangled placeholder fragment: {line}"
        );
    }

    #[test]
    fn header_labels_align_with_data_columns_for_the_longest_status() {
        // "cancelled" is 9 chars — the longest `RunStatus::as_str()` — so this pins
        // the hand-counted header spacing against the `{:<9}` field with zero margin.
        let r = ScheduledRun {
            run: RunId(uuid::Uuid::from_u128(
                0x1234_5678_9abc_def0_1234_5678_9abc_def0,
            )),
            status: RunStatus::Cancelled,
            next_wake: None,
            reason: None,
            updated_at: DateTime::<Utc>::from_timestamp(3_000_000, 0).unwrap(),
        };
        let out = table(&[r]);
        let mut lines = out.lines();
        let header = lines.next().expect("header line");
        let data = lines.next().expect("data line");
        let status_col = header.find("STATUS").expect("STATUS header present");
        let status_data = data.find("cancelled").expect("status text present");
        assert_eq!(
            status_col, status_data,
            "STATUS header must align with the status column:\n{out}"
        );
        let wake_col = header.find("NEXT WAKE").expect("NEXT WAKE header present");
        let wake_data = data.find('—').expect("em dash present");
        assert_eq!(
            wake_col, wake_data,
            "NEXT WAKE header must align with the wake column:\n{out}"
        );
    }
}
