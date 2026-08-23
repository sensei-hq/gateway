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
/// set, which is not free to redo per row.
static REASON_REDACTOR: LazyLock<PatternRedactor> = LazyLock::new(PatternRedactor::default);

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
fn redact_reason(s: &str) -> String {
    match REASON_REDACTOR.redact(&serde_json::Value::String(s.to_string())) {
        serde_json::Value::String(out) => out,
        other => {
            unreachable!("redacting a Value::String must yield a Value::String, got {other:?}")
        }
    }
}

/// Rendered reasons are capped so one unbounded provider message can't wreck the
/// table's column alignment or scroll an operator's terminal off-screen.
const REASON_MAX: usize = 300;

/// The table-display transform for a reason: redact, THEN collapse control
/// characters (`one_line`), THEN cap length.
///
/// Order, and what was checked rather than assumed: redact-before-collapse looks
/// like the obviously safer order on its face — collapse-first could in principle
/// glue two halves of a newline-split secret into a form that no longer matches a
/// pattern that only fires on the concatenated text (or, less intuitively, the
/// reverse: collapsing could join two halves the pattern DOES then match). Checked
/// against `PatternRedactor`'s actual patterns (`crates/orchestrator-core/src/redact.rs`):
/// every whole-match pattern is a contiguous character class — `[A-Za-z0-9_-]` and
/// siblings — that excludes BOTH raw control characters and the space `one_line`
/// replaces them with, so a secret split across an embedded newline fails to match
/// EITHER before or after collapsing; the assignment-form pattern's value group
/// (`[^\s"',&;]{6,}`) excludes whitespace outright for the same reason. The one
/// pattern that spans newlines on purpose — the PEM private-key block, `[\s\S]*?` —
/// matches a run of spaces exactly as it matches a run of newlines, so it too is
/// order-independent. So for this redactor's current pattern set, the two orders are
/// provably equivalent on the "split secret" scenario. Redact-first is still the
/// order used below: it is the safer default in general, it costs nothing here, and
/// it does not depend on today's patterns staying exactly this narrow if more are
/// added later.
fn safe_reason(s: &str) -> String {
    let redacted = redact_reason(s);
    let collapsed = one_line(&redacted);
    if collapsed.chars().count() <= REASON_MAX {
        collapsed
    } else {
        // Cap in CHARACTERS, not bytes: a byte-offset slice through a multi-byte
        // character (anything outside ASCII) would panic at the split point.
        let mut truncated: String = collapsed.chars().take(REASON_MAX).collect();
        truncated.push('…');
        truncated
    }
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
