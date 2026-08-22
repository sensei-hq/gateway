//! Operator output: a human table by default, `--json` for scripting.

use chrono::{DateTime, Utc};
use orchestrator_core::ScheduledRun;

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
fn one_line(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

// Consumed by Task 6 (cmd/run.rs `status`/`list-paused`), the default (non-`--json`) output.
#[allow(dead_code)]
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
            r.reason.as_deref().map(one_line).unwrap_or_default()
        ));
    }
    s
}

// Consumed by Task 6 (cmd/run.rs `status`/`list-paused`), the `--json` output.
#[allow(dead_code)]
pub fn json(rows: &[ScheduledRun]) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(rows)
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
