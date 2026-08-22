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
            r.reason.as_deref().unwrap_or("")
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
}
