//! Observe and intervene on runs. Every command reports the EFFECT it achieved,
//! never the fact that the store call returned Ok — `cancel` on a terminal run and
//! `wake` on a non-paused run are both silent no-ops at the store level.

use crate::cmd::Outcome;
use crate::errors::CliError;
use crate::render;
use chrono::{DateTime, Utc};
use orchestrator_core::{
    ExecutionJournal, JournalEvent, OrchestratorError, RunId, RunStatus, SchedulerStore,
    TokenBudget,
};

pub async fn status(
    store: &dyn SchedulerStore,
    journal: &dyn ExecutionJournal,
    run: RunId,
    json: bool,
) -> Result<Outcome, CliError> {
    match store.status(run).await? {
        // WHOLE-SLICE FIX 5: `--json` promises machine-parseable STDOUT, and the not-found
        // path was emitting prose there — so `torii run status X --json | jq` failed on
        // exactly the case a script is most likely to hit. `null` is the honest JSON answer
        // (the run does not exist); the exit code, not the payload, still carries "2".
        None => Ok(Outcome::precondition(if json {
            "null".to_string()
        } else {
            format!("no such run: {}", run.0)
        })),
        Some(r) => {
            // SP-DATA-5 Task 5: spend lives in the JOURNAL (`EffectRecorded.usage`), not
            // the `scheduled_runs` row, so it has to be folded here. Goes through
            // `orchestrator::spend_of` — the SAME fold the metered-dispatch gate itself
            // uses — rather than summing `EffectRecorded.usage` locally: a second,
            // torii-side sum would inevitably drift from the real one (e.g. missing the
            // effect-id keying that makes a duplicate record from the two-phase
            // Mutation in-doubt-`Confirmed` path count once, not twice) and stay wrong
            // silently, compounding on every resume.
            let events = journal
                .load(run)
                .await
                .map_err(OrchestratorError::Journal)?;
            let (spent, budget) = orchestrator::spend_of(&events);

            if json {
                let base = render::json(&[r]).map_err(|e| CliError::error(e.to_string()))?;
                match budget {
                    // No budget ⇒ return `render::json`'s own string UNTOUCHED — no
                    // parse/re-serialize round trip at all. That round trip is not
                    // idempotent: `serde_json::Value`'s object map does not preserve
                    // insertion order the way `ScheduledRun`'s derived `Serialize`
                    // does, so re-serializing would silently reorder every key. Only
                    // taking that detour when there is something to splice in is what
                    // keeps the unbudgeted case byte-identical.
                    None => Ok(Outcome::ok(base)),
                    Some(cap) => {
                        // Reuse `render::json` for the row shape + redaction, then
                        // splice spent/budget in — rather than hand-building the
                        // object here, which would duplicate `render::json`'s
                        // redaction of `reason`.
                        let mut rows: serde_json::Value = serde_json::from_str(&base)
                            .map_err(|e| CliError::error(e.to_string()))?;
                        rows[0]["spent"] = serde_json::json!(spent);
                        rows[0]["budget"] = serde_json::json!(cap);
                        Ok(Outcome::ok(
                            serde_json::to_string_pretty(&rows)
                                .map_err(|e| CliError::error(e.to_string()))?,
                        ))
                    }
                }
            } else {
                let mut text = render::table(&[r]);
                // Same additivity: nothing appended when unbudgeted, so the table
                // stays byte-identical to the pre-SP-DATA-5 output.
                if let Some(cap) = budget {
                    text.push_str(&format!("spent: {spent} / budget: {cap} tokens\n"));
                }
                Ok(Outcome::ok(text))
            }
        }
    }
}

pub async fn list_paused(store: &dyn SchedulerStore, json: bool) -> Result<Outcome, CliError> {
    let rows = store.list_paused().await?;
    Ok(Outcome::ok(if json {
        render::json(&rows).map_err(|e| CliError::error(e.to_string()))?
    } else {
        render::table(&rows)
    }))
}

pub async fn cancel(store: &dyn SchedulerStore, run: RunId) -> Result<Outcome, CliError> {
    if store.status(run).await?.is_none() {
        return Ok(Outcome::precondition(format!("no such run: {}", run.0)));
    }
    store.cancel(run).await?;
    // Re-read: `cancel` is a conditional no-op on a terminal row, so only the
    // observed state proves what happened.
    // This row is never deleted by any shipped store, so `None` here would mean a
    // hypothetical future retention/purge raced us, not a reachable path today.
    let after = store
        .status(run)
        .await?
        .ok_or_else(|| CliError::error(format!("run {} vanished mid-cancel", run.0)))?;
    if after.status == RunStatus::Cancelled {
        Ok(Outcome::ok(format!("cancelled: {}", run.0)))
    } else {
        Ok(Outcome::precondition(format!(
            "not cancelled: {} is already {}",
            run.0,
            after.status.as_str()
        )))
    }
}

pub async fn wake(
    store: &dyn SchedulerStore,
    journal: &dyn ExecutionJournal,
    run: RunId,
    now: DateTime<Utc>,
    budget: Option<TokenBudget>,
) -> Result<Outcome, CliError> {
    let Some(before) = store.status(run).await? else {
        return Ok(Outcome::precondition(format!("no such run: {}", run.0)));
    };
    if before.status != RunStatus::Paused {
        return Ok(Outcome::precondition(format!(
            "not queued: {} is {}, and only a paused run can be woken",
            run.0,
            before.status.as_str()
        )));
    }
    // SP-DATA-5 Task 5: if the operator is raising the cap, this MUST append to the
    // journal BEFORE calling `force_wake` below — never the other way round. This is
    // a real race, not a style preference: `force_wake` only flips the store's
    // `next_wake` to `now`; it does not itself drive the run. But a worker's
    // `tick()` polling loop can claim the SAME due wake the instant that deadline
    // lands, in another process, before this function returns. If the wake landed
    // first, that worker could win the race, drive the run under the OLD
    // (already-exhausted) cap, and re-pause it immediately — the operator's command
    // would then appear to "succeed" while the run is right back where it started,
    // one moment later, under the very cap they just tried to raise. Appending
    // first closes that window: any worker that can observe the wake can only ever
    // fold a journal that already includes the raise.
    if let Some(b) = budget {
        journal
            .append(
                run,
                JournalEvent::BudgetRaised {
                    new_total_tokens: b.total_tokens,
                },
            )
            .await
            .map_err(OrchestratorError::Journal)?;
    }
    store.force_wake(run, now).await?;
    // This row is never deleted by any shipped store, so `None` here would mean a
    // hypothetical future retention/purge raced us, not a reachable path today.
    let after = store
        .status(run)
        .await?
        .ok_or_else(|| CliError::error(format!("run {} vanished mid-wake", run.0)))?;
    // The primary signal is STATUS, not next_wake's mere presence: `claim_due` flips
    // `paused -> waking` and leaves a stale `next_wake` untouched, and `cancel` clears
    // it to NULL — neither on its own tells us whether OUR force_wake actually applied
    // (both shipped stores make force_wake conditional on the row still being
    // `paused`). A real force_wake success leaves the row `paused` with `next_wake`
    // pinned to (within clock precision of) `now`; a lost race to a concurrent claim
    // or cancel moves the status away from `paused` instead, which the timestamp alone
    // cannot distinguish from a stale pre-existing deadline.
    //
    // The timestamp tolerance is not compensating for multi-process clock skew — `now`
    // here is the exact value this call sent to the store — it only absorbs the
    // sub-microsecond rounding a `timestamptz` column performs on write. Measured
    // empirically against a live Postgres: encoding rounds a nanosecond-precision
    // value to the nearest microsecond (round-half-to-even), a drift of at most
    // ±500ns, in EITHER direction — so a one-sided `t <= now` is not safe. 2µs is a
    // 4x margin over that measured bound, and still five orders of magnitude tighter
    // than any real re-pause deadline (seconds-to-minutes out), so it cannot be
    // satisfied by an unrelated pause that happens to land in the race window.
    let applied = after.status == RunStatus::Paused
        && after.next_wake.is_some_and(|t| {
            let drift = if t >= now { t - now } else { now - t };
            drift <= chrono::Duration::microseconds(2)
        });
    if applied {
        Ok(Outcome::ok(format!(
            "queued for wake: {} (a worker tick will drive it)",
            run.0
        )))
    } else {
        Ok(Outcome::precondition(format!(
            "not queued: {} is {} — force_wake did not apply",
            run.0,
            after.status.as_str()
        )))
    }
}

/// Parse `--budget-tokens N` (on both `run submit` and `run wake`): a plain positive
/// integer, rejecting 0 loudly and with the offending value echoed back —
/// consistent with `--interval 0s` (`cmd::worker::parse_interval`) and
/// `TORII_POOL_SIZE=0` (`boot::parse_pool_size`). A zero-token budget can never
/// dispatch a single model call: the gate checks already-spent-vs-cap BEFORE every
/// dispatch (§6.2 of the design), so `spent (0) >= cap (0)` is true from the very
/// first check and the run would pause immediately, before doing any work at all —
/// almost certainly not what an operator setting a budget intends, so it is
/// refused as a precondition rather than accepted and silently useless.
pub fn parse_budget_tokens(s: &str) -> Result<u64, String> {
    let t = s.trim();
    let v: u64 = t
        .parse()
        .map_err(|_| format!("invalid --budget-tokens {t:?}: {t:?} is not a whole number"))?;
    if v == 0 {
        return Err(format!(
            "invalid --budget-tokens {t:?}: a zero-token budget can never dispatch a single \
             model call — the run would pause immediately, before doing any work"
        ));
    }
    Ok(v)
}

/// Parse a retention window: `30d`, `12h`, `90m`, `45s`.
///
/// A SIBLING of [`crate::cmd::worker::parse_interval`], deliberately, rather than an
/// extension of it. Extending that one to accept `d` would make `worker serve --interval
/// 30d` legal — a poll loop that sleeps for a month — and the units a flag accepts are part
/// of its contract, so widening the shared parser to serve `prune` would quietly loosen
/// `serve`. The two also have opposite natural ranges (milliseconds-to-minutes for a poll,
/// hours-to-days for retention), which is why `ms` is rejected here and `d` is rejected
/// there. `parse_interval` is untouched by this task.
///
/// Returns `chrono::Duration` because the only thing the caller does with it is subtract it
/// from a `DateTime<Utc>`; converting from `std::time::Duration` at the call site would add
/// a second fallible step for nothing.
pub fn parse_retention(s: &str) -> Result<chrono::Duration, String> {
    const UNITS: &str = "expected a number then one of s, m, h, d — e.g. 30d, 12h, 90m";
    let t = s.trim();
    // Split on the first non-digit: the number can only be digits, so the remainder is
    // exactly the unit. This is what turns `500ms` into a named "unknown unit" complaint
    // instead of a confusing "500m is not a number".
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    if num.is_empty() {
        return Err(format!("invalid retention window {t:?}: {UNITS}"));
    }
    // Digits only, so this can fail only by exceeding i64 — which is an overflow, not a
    // typo, and is reported as such.
    let v: i64 = num
        .parse()
        .map_err(|_| format!("invalid retention window {t:?}: {num:?} is out of range"))?;
    let window = match unit {
        "s" => chrono::Duration::try_seconds(v),
        "m" => chrono::Duration::try_minutes(v),
        "h" => chrono::Duration::try_hours(v),
        "d" => chrono::Duration::try_days(v),
        "" => return Err(format!("invalid retention window {t:?}: no unit — {UNITS}")),
        other => {
            return Err(format!(
                "invalid retention window {t:?}: unknown unit {other:?} — {UNITS}"
            ));
        }
    }
    .ok_or_else(|| format!("invalid retention window {t:?}: overflow"))?;
    if window.is_zero() {
        return Err(format!(
            "invalid retention window {t:?}: a zero window would delete a run that went \
             terminal a moment ago — use 1s if that is genuinely what you want"
        ));
    }
    Ok(window)
}

fn fmt_cutoff(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Delete terminal run records older than `older_than`, after showing the operator how many
/// that is and (unless `yes`) asking.
///
/// **Only `completed`/`failed`/`cancelled` rows are ever eligible** — the store enforces
/// that, at any age. A `paused` run is live work awaiting a wake (the in-doubt-mutation
/// class waits INDEFINITELY, with no deadline at all), and a `waking` row may be a lease
/// held by an in-flight drive in another process; neither has an age at which forgetting it
/// is correct.
///
/// This is an explicit operator command rather than something `tick()` does on its own:
/// deleting durable rows as a silent side effect of a poll loop is exactly the kind of
/// surprise that should not exist, and it would make `tick()`'s contract much harder to
/// reason about.
///
/// The count is taken FIRST and shown before the prompt, using the same cutoff the delete
/// then uses — but the number reported at the end is the number actually DELETED, which can
/// differ if a run went terminal, or was pruned by another operator, in between. Reporting
/// the intent instead of the effect is precisely what the rest of this module refuses to do.
pub async fn prune(
    store: &dyn SchedulerStore,
    older_than: chrono::Duration,
    now: DateTime<Utc>,
    yes: bool,
    confirm: &mut dyn FnMut(&str) -> bool,
) -> Result<Outcome, CliError> {
    // `parse_retention` already bounds the window, so this cannot realistically fire — but
    // `now - window` is still fallible arithmetic, and a panic in a delete command is not an
    // acceptable way to find that out.
    let cutoff = now.checked_sub_signed(older_than).ok_or_else(|| {
        CliError::error(format!(
            "retention window {older_than} cannot be subtracted from {}",
            fmt_cutoff(now)
        ))
    })?;

    let counted = store.count_terminal_before(cutoff).await?;
    if counted == 0 {
        // Nothing to consent to, so nothing is asked. Exit 0: the state the operator asked
        // for already holds.
        return Ok(Outcome::ok(format!(
            "nothing to prune: no completed, failed or cancelled run last changed before {}",
            fmt_cutoff(cutoff)
        )));
    }

    if !yes {
        let text = format!(
            "prune will DELETE {counted} terminal run record(s) — completed, failed or \
             cancelled — last changed before {}.\nPaused and waking runs are never \
             eligible and are not counted here.\nThis deletes durable rows and cannot be \
             undone.",
            fmt_cutoff(cutoff)
        );
        if !confirm(&text) {
            // `confirm` already showed `text` to the operator — do not repeat it.
            return Ok(Outcome::precondition(format!(
                "refused: nothing deleted, {counted} terminal run record(s) still stored"
            )));
        }
    }

    let deleted = store.prune_terminal(cutoff).await?;
    let mut text = format!(
        "pruned: {deleted} terminal run record(s) deleted (last changed before {})",
        fmt_cutoff(cutoff)
    );
    if deleted != counted {
        text.push_str(&format!(
            " — the preview counted {counted}; the eligible set changed in between"
        ));
    }
    Ok(Outcome::ok(text))
}

/// Submit a fresh run and drive it inline. Blocks until the run pauses or ends —
/// there is no `--detach` yet, because `enqueue` stamps the row `waking`, so a
/// detached run would only be picked up once the lease expired and the crash-reclaim
/// path grabbed it. Abusing crash recovery as a scheduling primitive was rejected;
/// a real `pending` status is the fix.
///
/// `announce` is called EXACTLY when the submit is known to be going ahead: after the
/// duplicate pre-check, before the (potentially very long) drive. `main` passes the
/// `submitted: <id>` print — WHOLE-SLICE FIX 4. It used to run before this function was
/// even entered, which announced an effect that a rejected `enqueue` never performed:
/// precisely the "report the effect, not the Ok" discipline the rest of this module
/// enforces. It must still precede the drive, so an operator who loses the terminal can
/// find the run.
pub async fn submit(
    scheduler: &orchestrator::Scheduler,
    run: RunId,
    graph: orchestrator_core::Graph,
    budget: Option<TokenBudget>,
    announce: impl FnOnce(),
) -> Result<Outcome, CliError> {
    // A run id that already has a schedule record cannot be submitted again. Left to
    // `enqueue`, this surfaces as `OrchestratorError::Store` — which §7.4 defines as a
    // RETRYABLE TRANSPORT FAULT — at exit 1, with `RunId(..)` `Debug` formatting in the
    // text. It is neither retryable nor a transport problem: the caller passed a
    // `--run-id` that is already taken, which is a precondition failure (exit 2).
    //
    // Read through the scheduler rather than a separately-passed store, deliberately:
    // `Scheduler::status` delegates to the very store its own `enqueue` will hit, so the
    // pre-check cannot be aimed at a different database than the write.
    //
    // A pre-check is a better MESSAGE, not a lock: two concurrent submits of the same id
    // can both pass it. `enqueue` remains the real guard (`on conflict do nothing` +
    // `rows_affected == 0`), so that race degrades to the old loud error rather than
    // double-enqueueing — which is why the check belongs here and not in place of it.
    if let Some(existing) = scheduler.status(run).await? {
        return Ok(Outcome::precondition(format!(
            "already submitted: {} is {} — a run id can only be submitted once. Use \
             `torii run status {}` to inspect it.",
            run.0,
            existing.status.as_str(),
            run.0
        )));
    }
    announce();
    // SP-DATA-5 Task 5: `submit_budgeted` with `None` is exactly `submit` — the
    // operator-specified cap (if any) rides on `RunStarted` from here.
    let outcome = scheduler.submit_budgeted(run, graph, budget).await?;
    if let Some(p) = &outcome.paused {
        return Ok(Outcome::ok(format!(
            "paused: {} at node {} ({})",
            run.0, p.node.0, p.reason
        )));
    }
    if let Some((node, msg)) = &outcome.failed {
        // A run that actually executed and failed is an EXECUTION error (exit 1),
        // not a precondition-not-met no-op (exit 2, this taxonomy's code for "ran
        // fine, nothing to do"): automation needs to tell "the workload failed,
        // page someone" apart from an idempotent no-op. `announce` has already
        // reached stdout by the time this returns — that is fine and intentional
        // (an operator who loses the terminal must still be able to find the run),
        // and by then the enqueue really did happen.
        return Err(CliError::error(format!(
            "failed: {} at node {} ({msg})",
            run.0, node.0
        )));
    }
    Ok(Outcome::ok(format!(
        "completed: {} ({} node(s))",
        run.0,
        outcome.completed.len()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::{EXIT_OK, EXIT_PRECONDITION};
    use orchestrator_core::{EffectClass, EffectId, EffectOutput, Graph, NodeId, TokenUsage};
    use orchestrator_store::{InMemoryJournal, InMemorySchedulerStore};
    use std::sync::Arc;

    fn now() -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(3_000_000, 0).unwrap()
    }

    fn empty_graph() -> Graph {
        Graph { nodes: vec![] }
    }

    /// A journal with nothing on it. `status`/`wake` only touch the journal when a
    /// run needs its spend folded (`status`, in the `Some(r)` branch) or a raise is
    /// requested (`wake`, when `budget` is `Some`) — most tests here exercise
    /// neither, so an empty journal is a safe, inert stand-in.
    fn empty_journal() -> InMemoryJournal {
        InMemoryJournal::new()
    }

    /// A run enqueued then recorded paused with a deadline.
    async fn paused_store(run: RunId, next_wake: Option<DateTime<Utc>>) -> InMemorySchedulerStore {
        let s = InMemorySchedulerStore::default();
        s.enqueue(run, &empty_graph(), now()).await.unwrap();
        s.record_paused(run, next_wake, "quota: rate limited")
            .await
            .unwrap();
        s
    }

    #[tokio::test]
    async fn status_of_an_unknown_run_is_a_precondition_failure_not_an_error() {
        let s = InMemorySchedulerStore::default();
        let out = status(&s, &empty_journal(), RunId(uuid::Uuid::new_v4()), false)
            .await
            .expect("no hard error");
        assert_eq!(out.code, EXIT_PRECONDITION);
        assert!(out.text.contains("no such run"), "{}", out.text);
    }

    /// WHOLE-SLICE FIX 5: `--json` must never put prose on stdout — `torii run status X
    /// --json | jq` has to survive the not-found case, which is the one a script polling
    /// for a run is most likely to hit first.
    #[tokio::test]
    async fn status_of_an_unknown_run_is_still_valid_json_under_json() {
        let s = InMemorySchedulerStore::default();
        let out = status(&s, &empty_journal(), RunId(uuid::Uuid::new_v4()), true)
            .await
            .expect("no hard error");
        assert_eq!(
            out.code, EXIT_PRECONDITION,
            "the exit code still says not-found: {}",
            out.text
        );
        let v: serde_json::Value = serde_json::from_str(&out.text)
            .unwrap_or_else(|e| panic!("--json emitted non-JSON {:?}: {e}", out.text));
        assert!(
            v.is_null(),
            "a run that does not exist is JSON null, not a fabricated record: {v}"
        );
    }

    #[tokio::test]
    async fn list_paused_renders_the_pending_wake_set() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        let out = list_paused(&s, false).await.expect("lists");
        assert_eq!(out.code, EXIT_OK);
        assert!(out.text.contains(&run.0.to_string()), "{}", out.text);
        assert!(out.text.contains("quota: rate limited"), "{}", out.text);
    }

    #[tokio::test]
    async fn list_paused_json_is_machine_readable() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let out = list_paused(&s, true).await.expect("lists");
        let rows: Vec<orchestrator_core::ScheduledRun> =
            serde_json::from_str(&out.text).expect("valid json");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].next_wake, None);
    }

    #[tokio::test]
    async fn cancel_reports_the_transition_it_actually_made() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        let out = cancel(&s, run).await.expect("cancels");
        assert_eq!(out.code, EXIT_OK);
        assert!(out.text.starts_with("cancelled:"), "{}", out.text);
        assert_eq!(
            s.status(run).await.unwrap().unwrap().status,
            RunStatus::Cancelled
        );
    }

    /// THE honest-reporting case: the store call SUCCEEDS on a terminal run but
    /// changes nothing. Reporting "cancelled" here would be a lie.
    #[tokio::test]
    async fn cancel_on_a_terminal_run_reports_not_cancelled() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = InMemorySchedulerStore::default();
        s.enqueue(run, &empty_graph(), now()).await.unwrap();
        s.record_terminal(run, RunStatus::Completed, None)
            .await
            .unwrap();

        let out = cancel(&s, run).await.expect("no hard error");
        assert_eq!(out.code, EXIT_PRECONDITION);
        assert!(out.text.contains("not cancelled"), "{}", out.text);
        assert!(
            out.text.contains("completed"),
            "must name the actual state: {}",
            out.text
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().status,
            RunStatus::Completed,
            "and the run really is untouched"
        );
    }

    #[tokio::test]
    async fn wake_says_queued_never_resumed() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let out = wake(&s, &empty_journal(), run, now(), None)
            .await
            .expect("wakes");
        assert_eq!(out.code, EXIT_OK);
        assert!(out.text.contains("queued"), "{}", out.text);
        assert!(
            !out.text.contains("resumed") && !out.text.contains("woken"),
            "force_wake only sets next_wake; a worker tick does the driving: {}",
            out.text
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            Some(now()),
            "the NULL deadline is now set to now, so the next tick claims it"
        );
    }

    #[tokio::test]
    async fn wake_on_a_non_paused_run_reports_not_queued() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = InMemorySchedulerStore::default();
        s.enqueue(run, &empty_graph(), now()).await.unwrap(); // status = waking, not paused
        let out = wake(&s, &empty_journal(), run, now(), None)
            .await
            .expect("no hard error");
        assert_eq!(out.code, EXIT_PRECONDITION);
        assert!(out.text.contains("not queued"), "{}", out.text);
        assert!(
            out.text.contains("waking"),
            "must name the actual state: {}",
            out.text
        );
    }

    // ---- SP-DATA-5 Task 5: `--budget-tokens` on submit/wake, spent/budget in status ---

    #[test]
    fn parse_budget_tokens_rejects_zero_with_an_actionable_message() {
        let e = parse_budget_tokens("0").expect_err("a zero budget can never dispatch anything");
        assert!(e.contains("--budget-tokens"), "{e}");
        assert!(e.contains('0'), "{e}");
    }

    #[test]
    fn parse_budget_tokens_accepts_a_positive_value() {
        assert_eq!(parse_budget_tokens("50000"), Ok(50_000));
        assert_eq!(parse_budget_tokens(" 12 "), Ok(12), "whitespace is trimmed");
    }

    #[test]
    fn parse_budget_tokens_rejects_garbage_loudly() {
        let e = parse_budget_tokens("many").expect_err("not a number");
        assert!(e.contains("--budget-tokens"), "{e}");
        assert!(e.contains("many"), "must echo the offending value: {e}");
        assert!(
            parse_budget_tokens("-5").is_err(),
            "negative is not a token count"
        );
        assert!(parse_budget_tokens("").is_err());
    }

    /// A journal seeded with `RunStarted{budget}` plus two `EffectRecorded{usage}` — the
    /// DB-free harness the task calls for, proving `status` displays spend without a live
    /// Postgres.
    async fn journal_with_budget_and_spend(run: RunId, cap: u64) -> InMemoryJournal {
        let journal = InMemoryJournal::new();
        journal
            .append(
                run,
                JournalEvent::RunStarted {
                    version: "v1".into(),
                    budget: Some(TokenBudget { total_tokens: cap }),
                },
            )
            .await
            .unwrap();
        journal
            .append(
                run,
                JournalEvent::EffectRecorded {
                    node: NodeId("n1".into()),
                    effect_id: EffectId("e1".into()),
                    class: EffectClass::Pure,
                    input_hash: "h".into(),
                    seq: 0,
                    output: EffectOutput::Inline(serde_json::Value::Null),
                    observation: None,
                    usage: Some(TokenUsage {
                        input_tokens: 100,
                        output_tokens: 50,
                        total_tokens: 150,
                    }),
                },
            )
            .await
            .unwrap();
        journal
            .append(
                run,
                JournalEvent::EffectRecorded {
                    node: NodeId("n2".into()),
                    effect_id: EffectId("e2".into()),
                    class: EffectClass::Pure,
                    input_hash: "h".into(),
                    seq: 1,
                    output: EffectOutput::Inline(serde_json::Value::Null),
                    observation: None,
                    usage: Some(TokenUsage {
                        input_tokens: 20,
                        output_tokens: 30,
                        total_tokens: 50,
                    }),
                },
            )
            .await
            .unwrap();
        journal
    }

    #[tokio::test]
    async fn status_shows_spent_and_budget_when_a_budget_is_set() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        let journal = journal_with_budget_and_spend(run, 50_000).await;

        let out = status(&s, &journal, run, false).await.expect("status");
        assert_eq!(out.code, EXIT_OK);
        assert!(
            out.text.contains("200") && out.text.contains("50000"),
            "spent (150+50 = 200, summed over two distinct effects' total_tokens) and the \
             50000 cap must both be visible: {}",
            out.text
        );
    }

    #[tokio::test]
    async fn status_json_includes_spent_and_budget_when_a_budget_is_set() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        let journal = journal_with_budget_and_spend(run, 50_000).await;

        let out = status(&s, &journal, run, true).await.expect("status");
        assert_eq!(out.code, EXIT_OK);
        let v: serde_json::Value = serde_json::from_str(&out.text).expect("valid json");
        assert_eq!(v[0]["spent"], serde_json::json!(200));
        assert_eq!(v[0]["budget"], serde_json::json!(50_000));
    }

    /// Additivity, at the command level: an UNBUDGETED run's `status` table must be
    /// BYTE-IDENTICAL to `render::table` alone — nothing appended, nothing reordered.
    #[tokio::test]
    async fn status_table_is_byte_identical_for_an_unbudgeted_run() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        // No `RunStarted` at all ⇒ folds to `budget: None` — the same as a run whose
        // `RunStarted.budget` is explicitly `None` (Task 1's additivity case).
        let journal = empty_journal();

        let out = status(&s, &journal, run, false).await.expect("status");
        let row = s.status(run).await.unwrap().unwrap();
        assert_eq!(
            out.text,
            render::table(&[row]),
            "an unbudgeted run must render EXACTLY what the pre-SP-DATA-5 table did"
        );
    }

    /// The JSON counterpart of the byte-identical guarantee above.
    #[tokio::test]
    async fn status_json_is_byte_identical_for_an_unbudgeted_run() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        let journal = empty_journal();

        let out = status(&s, &journal, run, true).await.expect("status");
        let row = s.status(run).await.unwrap().unwrap();
        assert_eq!(
            out.text,
            render::json(&[row]).unwrap(),
            "an unbudgeted run's --json must be EXACTLY what the pre-SP-DATA-5 render did"
        );
    }

    #[tokio::test]
    async fn wake_with_budget_tokens_appends_budget_raised_to_the_journal() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        let journal = empty_journal();

        let out = wake(
            &s,
            &journal,
            run,
            now(),
            Some(TokenBudget {
                total_tokens: 5_000,
            }),
        )
        .await
        .expect("wakes");
        assert_eq!(out.code, EXIT_OK, "{}", out.text);

        let events = journal.load(run).await.unwrap();
        assert!(
            events.iter().any(|(_, e)| matches!(
                e,
                JournalEvent::BudgetRaised {
                    new_total_tokens: 5_000
                }
            )),
            "the raise must be journaled: {events:?}"
        );
    }

    #[tokio::test]
    async fn wake_without_budget_tokens_appends_nothing_to_the_journal() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        let journal = empty_journal();

        let out = wake(&s, &journal, run, now(), None).await.expect("wakes");
        assert_eq!(out.code, EXIT_OK, "{}", out.text);
        assert!(
            journal.load(run).await.unwrap().is_empty(),
            "no --budget-tokens ⇒ no journal write at all — unbudgeted wake stays exactly \
             the pre-SP-DATA-5 behavior"
        );
    }

    /// A `SchedulerStore` that delegates everything to a real `InMemorySchedulerStore`
    /// EXCEPT `force_wake`, which always fails. This is the discriminator for the
    /// append-then-wake ORDER, not just its presence: if `wake()` called `force_wake`
    /// BEFORE appending `BudgetRaised` (the swapped, wrong order), the injected failure
    /// here would short-circuit via `?` and the append would never run — the journal
    /// would end up with NO `BudgetRaised` event at all. Because the real order appends
    /// first, the event lands durably even though the subsequent `force_wake` fails.
    struct FailingForceWakeStore(InMemorySchedulerStore);

    #[async_trait::async_trait]
    impl SchedulerStore for FailingForceWakeStore {
        async fn enqueue(
            &self,
            run: RunId,
            graph: &Graph,
            now: DateTime<Utc>,
        ) -> Result<(), OrchestratorError> {
            self.0.enqueue(run, graph, now).await
        }
        async fn record_paused(
            &self,
            run: RunId,
            next_wake: Option<DateTime<Utc>>,
            reason: &str,
        ) -> Result<(), OrchestratorError> {
            self.0.record_paused(run, next_wake, reason).await
        }
        async fn record_terminal(
            &self,
            run: RunId,
            status: RunStatus,
            reason: Option<&str>,
        ) -> Result<(), OrchestratorError> {
            self.0.record_terminal(run, status, reason).await
        }
        async fn claim_due(
            &self,
            now: DateTime<Utc>,
            lease: chrono::Duration,
            limit: usize,
        ) -> Result<Vec<(RunId, Graph)>, OrchestratorError> {
            self.0.claim_due(now, lease, limit).await
        }
        async fn status(
            &self,
            run: RunId,
        ) -> Result<Option<orchestrator_core::ScheduledRun>, OrchestratorError> {
            self.0.status(run).await
        }
        async fn list_paused(
            &self,
        ) -> Result<Vec<orchestrator_core::ScheduledRun>, OrchestratorError> {
            self.0.list_paused().await
        }
        async fn cancel(&self, run: RunId) -> Result<(), OrchestratorError> {
            self.0.cancel(run).await
        }
        async fn count_terminal_before(
            &self,
            before: DateTime<Utc>,
        ) -> Result<u64, OrchestratorError> {
            self.0.count_terminal_before(before).await
        }
        async fn prune_terminal(&self, before: DateTime<Utc>) -> Result<u64, OrchestratorError> {
            self.0.prune_terminal(before).await
        }
        async fn force_wake(
            &self,
            _run: RunId,
            _now: DateTime<Utc>,
        ) -> Result<(), OrchestratorError> {
            Err(OrchestratorError::Store(
                "injected: proves append-then-wake ordering".into(),
            ))
        }
    }

    #[tokio::test]
    async fn wake_appends_budget_raised_before_calling_force_wake() {
        let run = RunId(uuid::Uuid::new_v4());
        let inner = paused_store(run, Some(now())).await;
        let store = FailingForceWakeStore(inner);
        let journal = empty_journal();

        let result = wake(
            &store,
            &journal,
            run,
            now(),
            Some(TokenBudget {
                total_tokens: 5_000,
            }),
        )
        .await;

        assert!(
            result.is_err(),
            "the injected force_wake failure must surface, not be swallowed"
        );
        let events = journal.load(run).await.unwrap();
        assert!(
            events.iter().any(|(_, e)| matches!(
                e,
                JournalEvent::BudgetRaised {
                    new_total_tokens: 5_000
                }
            )),
            "BudgetRaised must already be durable even though force_wake failed — proving \
             the append happens BEFORE force_wake is called, not after: {events:?}"
        );
    }

    // ---- SP-DATA-4.1 #7: `torii run prune` -------------------------------------------

    /// A confirmer that must never be reached. Used on the paths where asking would itself
    /// be the bug (`--yes`, and "nothing to prune").
    fn never_asked() -> impl FnMut(&str) -> bool {
        |text: &str| panic!("the operator must not be prompted here, but saw: {text}")
    }

    /// A store holding `n` terminal runs whose `updated_at` is `at` (this store stamps it at
    /// `enqueue`), plus optionally a paused and a waking run at the same instant.
    async fn prunable_store(
        terminal: usize,
        at: DateTime<Utc>,
        with_live_runs: bool,
    ) -> (InMemorySchedulerStore, Vec<RunId>, Vec<RunId>) {
        let s = InMemorySchedulerStore::default();
        let mut dead = Vec::new();
        for _ in 0..terminal {
            let r = RunId(uuid::Uuid::new_v4());
            s.enqueue(r, &empty_graph(), at).await.unwrap();
            s.record_terminal(r, RunStatus::Completed, None)
                .await
                .unwrap();
            dead.push(r);
        }
        let mut live = Vec::new();
        if with_live_runs {
            let paused = RunId(uuid::Uuid::new_v4());
            s.enqueue(paused, &empty_graph(), at).await.unwrap();
            // The in-doubt class: NULL deadline, waits indefinitely for a human.
            s.record_paused(paused, None, "in-doubt mutation")
                .await
                .unwrap();
            let waking = RunId(uuid::Uuid::new_v4());
            s.enqueue(waking, &empty_graph(), at).await.unwrap();
            live.push(paused);
            live.push(waking);
        }
        (s, dead, live)
    }

    /// A day before `now()`, i.e. comfortably outside any window the tests below use.
    fn long_ago() -> DateTime<Utc> {
        now() - chrono::Duration::days(30)
    }

    #[tokio::test]
    async fn prune_reports_the_number_actually_deleted() {
        let (s, dead, _) = prunable_store(3, long_ago(), false).await;
        let out = prune(
            &s,
            chrono::Duration::days(7),
            now(),
            true,
            &mut never_asked(),
        )
        .await
        .expect("prunes");
        assert_eq!(out.code, EXIT_OK);
        assert!(out.text.contains('3'), "must name the effect: {}", out.text);
        assert!(out.text.starts_with("pruned:"), "{}", out.text);
        for r in dead {
            assert!(s.status(r).await.unwrap().is_none(), "the row is gone");
        }
    }

    /// THE safety property, at the command level: `prune` must not be able to delete live
    /// work even when the operator passes `--yes` and a window that every row is older than.
    #[tokio::test]
    async fn prune_never_deletes_a_paused_or_waking_run() {
        let (s, dead, live) = prunable_store(1, long_ago(), true).await;
        let out = prune(
            &s,
            chrono::Duration::seconds(1),
            now(),
            true,
            &mut never_asked(),
        )
        .await
        .expect("prunes");
        assert_eq!(out.code, EXIT_OK);
        assert!(
            out.text.contains('1'),
            "exactly the one terminal row: {}",
            out.text
        );
        assert!(s.status(dead[0]).await.unwrap().is_none());
        assert_eq!(
            s.status(live[0]).await.unwrap().unwrap().status,
            RunStatus::Paused,
            "a NULL-deadline pause is live work awaiting a human, at any age"
        );
        assert_eq!(
            s.status(live[1]).await.unwrap().unwrap().status,
            RunStatus::Waking,
            "a waking row may be a live lease in another process"
        );
    }

    /// Nothing to delete must not put a destructive prompt in front of an operator, and it
    /// is not a failure — the state they asked for already holds.
    #[tokio::test]
    async fn prune_with_nothing_eligible_exits_zero_without_prompting() {
        let (s, _, live) = prunable_store(0, long_ago(), true).await;
        let out = prune(
            &s,
            chrono::Duration::days(7),
            now(),
            false, // NOT --yes: the prompt is only skipped because the count is zero
            &mut never_asked(),
        )
        .await
        .expect("no hard error");
        assert_eq!(out.code, EXIT_OK);
        assert!(out.text.contains("nothing to prune"), "{}", out.text);
        assert_eq!(
            s.status(live[0]).await.unwrap().unwrap().status,
            RunStatus::Paused
        );
    }

    /// A terminal row INSIDE the retention window is not eligible — the window is the whole
    /// point, so a prune that ignored it would still pass every "deletes terminal rows" test.
    #[tokio::test]
    async fn prune_keeps_a_terminal_row_inside_the_retention_window() {
        let (s, dead, _) = prunable_store(1, now() - chrono::Duration::hours(1), false).await;
        let out = prune(
            &s,
            chrono::Duration::days(7),
            now(),
            true,
            &mut never_asked(),
        )
        .await
        .expect("no hard error");
        assert_eq!(out.code, EXIT_OK);
        assert!(out.text.contains("nothing to prune"), "{}", out.text);
        assert!(
            s.status(dead[0]).await.unwrap().is_some(),
            "an hour-old terminal row is inside a 7-day window"
        );
    }

    /// The operator sees the count BEFORE consenting — a confirmation prompt that does not
    /// disclose the size of the delete is not informed consent.
    #[tokio::test]
    async fn prune_shows_the_count_in_the_prompt_before_asking() {
        let (s, _, _) = prunable_store(4, long_ago(), false).await;
        let mut prompt = String::new();
        {
            let mut confirm = |text: &str| {
                prompt = text.to_string();
                true
            };
            let out = prune(&s, chrono::Duration::days(7), now(), false, &mut confirm)
                .await
                .expect("prunes");
            assert_eq!(out.code, EXIT_OK);
        }
        assert!(prompt.contains('4'), "the count must be shown: {prompt}");
        assert!(
            prompt.to_lowercase().contains("delete"),
            "the prompt must say what it does: {prompt}"
        );
        assert!(
            prompt.contains("cannot be undone"),
            "a durable delete must say so: {prompt}"
        );
    }

    /// Declining must delete nothing. `interactive_confirm` returns false on EOF, so this is
    /// also the shape a non-interactive `torii run prune` (cron, `< /dev/null`) takes.
    #[tokio::test]
    async fn prune_refused_at_the_prompt_deletes_nothing() {
        let (s, dead, _) = prunable_store(2, long_ago(), false).await;
        let mut confirm = |_: &str| false;
        let out = prune(&s, chrono::Duration::days(7), now(), false, &mut confirm)
            .await
            .expect("no hard error");
        assert_eq!(out.code, EXIT_PRECONDITION);
        assert!(out.text.contains("refused"), "{}", out.text);
        assert!(out.text.contains("nothing deleted"), "{}", out.text);
        for r in dead {
            assert!(
                s.status(r).await.unwrap().is_some(),
                "a declined prune must leave every row in place"
            );
        }
    }

    /// The number reported must be the EFFECT, never the preview. `confirm` is the only hook
    /// that lands between the count and the delete — exactly the window a human spends
    /// reading the prompt — so another operator's prune runs there, on its own thread and
    /// runtime (the same technique `cmd::config`'s push-race test uses, `confirm` being
    /// sync). By the time this prune fires there is nothing left, and reporting the
    /// previewed 2 would be a lie. Without this, an implementation that simply echoed
    /// `counted` would pass every other test here.
    #[tokio::test]
    async fn prune_reports_what_it_deleted_not_what_it_previewed() {
        let (s, dead, _) = prunable_store(2, long_ago(), false).await;
        let window = chrono::Duration::days(7);
        let cutoff = now() - window;
        let mut confirm = {
            let s = s.clone(); // `Clone` shares the one Arc-backed map
            move |_: &str| {
                let s = s.clone();
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    rt.block_on(async move { s.prune_terminal(cutoff).await.unwrap() })
                })
                .join()
                .expect("the concurrent prune must not panic");
                true
            }
        };

        let out = prune(&s, window, now(), false, &mut confirm)
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_OK);
        assert!(
            out.text.contains("pruned: 0"),
            "must report what it actually deleted, not the 2 it previewed: {}",
            out.text
        );
        assert!(
            out.text.contains("preview counted 2"),
            "and must say why the two numbers differ: {}",
            out.text
        );
        for r in dead {
            assert!(
                s.status(r).await.unwrap().is_none(),
                "the other operator's prune really did take them"
            );
        }
    }

    #[test]
    fn parse_retention_accepts_days_hours_minutes_and_seconds() {
        assert_eq!(parse_retention("30d"), Ok(chrono::Duration::days(30)));
        assert_eq!(parse_retention("12h"), Ok(chrono::Duration::hours(12)));
        assert_eq!(parse_retention("90m"), Ok(chrono::Duration::minutes(90)));
        assert_eq!(parse_retention("45s"), Ok(chrono::Duration::seconds(45)));
        assert_eq!(parse_retention(" 30d "), Ok(chrono::Duration::days(30)));
    }

    #[test]
    fn parse_retention_rejects_garbage_loudly() {
        assert!(parse_retention("soon").is_err());
        assert!(parse_retention("30").is_err(), "a bare number has no unit");
        assert!(parse_retention("").is_err());
        assert!(parse_retention("d").is_err(), "a bare unit has no number");
        // Milliseconds are a poll-interval unit, not a retention unit — and the complaint
        // must name the unit rather than claiming "500m is not a number".
        let e = parse_retention("500ms").expect_err("ms is not a retention unit");
        assert!(e.contains("\"ms\""), "{e}");
    }

    #[test]
    fn parse_retention_rejects_a_zero_window() {
        assert!(parse_retention("0d").is_err());
        assert!(parse_retention("0s").is_err());
    }

    #[test]
    fn parse_retention_rejects_an_overflow_instead_of_panicking() {
        // `chrono::Duration::try_days` returns None rather than panicking on these.
        assert!(parse_retention("999999999999999999d").is_err());
        assert!(
            parse_retention("99999999999999999999999d").is_err(),
            "past i64 entirely"
        );
    }

    /// The reason `parse_retention` is a sibling rather than an extension: `--interval 30d`
    /// must stay illegal. If someone later "unifies" the two parsers, this fails.
    #[test]
    fn the_poll_interval_parser_still_refuses_a_retention_unit() {
        assert!(
            crate::cmd::worker::parse_interval("30d").is_err(),
            "a month-long poll interval must not become legal"
        );
    }

    // ---- WHOLE-SLICE FIX 4: a duplicate submit ---------------------------------------

    /// A real `Scheduler` over in-memory backends. No database, no live provider: the
    /// recording gateway is never reached on the duplicate path (the pre-check returns
    /// first), and the empty graph spends nothing on the fresh path.
    async fn scheduler_over(store: Arc<InMemorySchedulerStore>) -> orchestrator::Scheduler {
        let journal = Arc::new(orchestrator_store::InMemoryJournal::new());
        let (gw, _calls) = orchestrator::test_support::recording_gateway().await;
        let clock = orchestrator::test_support::FakeClock::new(now());
        let exec = orchestrator::Executor::new(Arc::new(gw), journal.clone(), "v1");
        orchestrator::Scheduler::new(store, exec, journal, clock)
    }

    /// Re-submitting a taken run id must be a PRECONDITION failure, and must not have
    /// announced `submitted:` first.
    ///
    /// Both halves were wrong before: `enqueue`'s refusal surfaced as
    /// `OrchestratorError::Store` — which the CLI's taxonomy (§7.4) reserves for a
    /// RETRYABLE TRANSPORT FAULT — at exit 1, carrying `RunId(..)` `Debug` formatting; and
    /// `main` had already printed `submitted: <id>` for a run that was never enqueued.
    #[tokio::test]
    async fn a_duplicate_submit_is_a_precondition_failure_and_announces_nothing() {
        let run = RunId(uuid::Uuid::new_v4());
        let store = Arc::new(InMemorySchedulerStore::default());
        store.enqueue(run, &empty_graph(), now()).await.unwrap();
        let sched = scheduler_over(store.clone()).await;

        let mut announced = false;
        let out = submit(&sched, run, empty_graph(), None, || announced = true)
            .await
            .expect("a duplicate submit is not a transport fault");

        assert_eq!(
            out.code, EXIT_PRECONDITION,
            "a taken run id is a precondition failure, not a retryable transport fault: {}",
            out.text
        );
        assert!(out.text.contains("already submitted"), "{}", out.text);
        assert!(
            out.text.contains("waking"),
            "must name the run's actual state: {}",
            out.text
        );
        assert!(
            out.text.contains(&run.0.to_string()) && !out.text.contains("RunId("),
            "the id must be the plain uuid, not `RunId(..)` Debug formatting: {}",
            out.text
        );
        assert!(
            !announced,
            "an effect that never happened must never be announced"
        );
    }

    /// The counterpart, so the fix cannot be satisfied by never announcing at all:
    /// a FIRST submit announces (and structurally does so before the drive — `announce()`
    /// is the statement immediately preceding `scheduler.submit`).
    #[tokio::test]
    async fn a_fresh_submit_announces_and_then_drives() {
        let run = RunId(uuid::Uuid::new_v4());
        let store = Arc::new(InMemorySchedulerStore::default());
        let sched = scheduler_over(store.clone()).await;

        let mut announced = false;
        let out = submit(&sched, run, empty_graph(), None, || announced = true)
            .await
            .expect("a fresh submit runs");

        assert!(announced, "the operator must get the id");
        assert_eq!(out.code, EXIT_OK, "{}", out.text);
        assert!(out.text.starts_with("completed:"), "{}", out.text);
        assert!(
            store.status(run).await.unwrap().is_some(),
            "and the enqueue really happened"
        );
    }

    /// Which concurrent actor lands in the gap between `wake`'s pre-check (`status`)
    /// and its own `force_wake` call.
    #[derive(Clone, Copy)]
    enum ConcurrentActor {
        /// A worker's tick claims the same due pause first (`paused -> waking`).
        ClaimsFirst,
        /// Another operator cancels the run first (`paused -> cancelled`).
        CancelsFirst,
        /// A worker's tick claims it, wake()'s OWN `force_wake` lands while it is
        /// `waking` (a no-op), and THEN the executor's drive finishes and re-pauses
        /// it with a fresh, UNRELATED deadline — landing status back on `paused`
        /// before `wake`'s post-check re-reads it. Proves the status check alone is
        /// not enough: only the timestamp half catches this.
        ReclaimsThenRepausesWithUnrelatedDeadline,
    }

    /// Delegates to a real `InMemorySchedulerStore` for everything, EXCEPT that its
    /// `force_wake` runs `actor` against `run` first. `wake()` calls `store.status`
    /// (the pre-check) and only then `store.force_wake` — so running the concurrent
    /// actor at the top of THIS `force_wake` lands it exactly in that gap, reproducing
    /// a real multi-process race deterministically, single-threaded, no database.
    struct RacingStore {
        inner: InMemorySchedulerStore,
        run: RunId,
        actor: ConcurrentActor,
    }

    #[async_trait::async_trait]
    impl SchedulerStore for RacingStore {
        async fn enqueue(
            &self,
            run: RunId,
            graph: &Graph,
            now: DateTime<Utc>,
        ) -> Result<(), orchestrator_core::OrchestratorError> {
            self.inner.enqueue(run, graph, now).await
        }
        async fn record_paused(
            &self,
            run: RunId,
            next_wake: Option<DateTime<Utc>>,
            reason: &str,
        ) -> Result<(), orchestrator_core::OrchestratorError> {
            self.inner.record_paused(run, next_wake, reason).await
        }
        async fn record_terminal(
            &self,
            run: RunId,
            status: RunStatus,
            reason: Option<&str>,
        ) -> Result<(), orchestrator_core::OrchestratorError> {
            self.inner.record_terminal(run, status, reason).await
        }
        async fn claim_due(
            &self,
            now: DateTime<Utc>,
            lease: chrono::Duration,
            limit: usize,
        ) -> Result<Vec<(RunId, Graph)>, orchestrator_core::OrchestratorError> {
            self.inner.claim_due(now, lease, limit).await
        }
        async fn status(
            &self,
            run: RunId,
        ) -> Result<Option<orchestrator_core::ScheduledRun>, orchestrator_core::OrchestratorError>
        {
            self.inner.status(run).await
        }
        async fn list_paused(
            &self,
        ) -> Result<Vec<orchestrator_core::ScheduledRun>, orchestrator_core::OrchestratorError>
        {
            self.inner.list_paused().await
        }
        async fn cancel(&self, run: RunId) -> Result<(), orchestrator_core::OrchestratorError> {
            self.inner.cancel(run).await
        }
        async fn count_terminal_before(
            &self,
            before: DateTime<Utc>,
        ) -> Result<u64, orchestrator_core::OrchestratorError> {
            self.inner.count_terminal_before(before).await
        }
        async fn prune_terminal(
            &self,
            before: DateTime<Utc>,
        ) -> Result<u64, orchestrator_core::OrchestratorError> {
            self.inner.prune_terminal(before).await
        }
        async fn force_wake(
            &self,
            run: RunId,
            now: DateTime<Utc>,
        ) -> Result<(), orchestrator_core::OrchestratorError> {
            if run != self.run {
                return self.inner.force_wake(run, now).await;
            }
            match self.actor {
                ConcurrentActor::ClaimsFirst => {
                    self.inner
                        .claim_due(now, chrono::Duration::seconds(60), 10)
                        .await?;
                    self.inner.force_wake(run, now).await
                }
                ConcurrentActor::CancelsFirst => {
                    self.inner.cancel(run).await?;
                    self.inner.force_wake(run, now).await
                }
                ConcurrentActor::ReclaimsThenRepausesWithUnrelatedDeadline => {
                    self.inner
                        .claim_due(now, chrono::Duration::seconds(60), 10)
                        .await?;
                    // This IS wake()'s own force_wake call — it lands while the row
                    // is `waking` (conditional on `paused`), so it is a no-op.
                    self.inner.force_wake(run, now).await?;
                    // The executor's drive finishes and re-pauses with a fresh,
                    // UNRELATED deadline (mirrors a journaled `RunPaused.resume_after`
                    // backoff — realistically seconds-to-minutes out, from a
                    // different process than the CLI's own `now`).
                    self.inner
                        .record_paused(
                            run,
                            Some(now + chrono::Duration::minutes(5)),
                            "unrelated re-pause",
                        )
                        .await
                }
            }
        }
    }

    /// FALSE POSITIVE reproduction: a worker's `claim_due` claims the same overdue
    /// pause in the window between `wake`'s pre-check and its `force_wake`. The old
    /// `is_some()` check reported success (`next_wake` survives the claim untouched);
    /// the run was ALREADY being driven and torii's own call changed nothing.
    #[tokio::test]
    async fn wake_reports_not_queued_when_a_concurrent_claim_wins_the_race() {
        let run = RunId(uuid::Uuid::new_v4());
        // Overdue: next_wake <= now, so the injected claim_due actually claims it.
        let inner = paused_store(run, Some(now())).await;
        let racing = RacingStore {
            inner: inner.clone(),
            run,
            actor: ConcurrentActor::ClaimsFirst,
        };

        let out = wake(&racing, &empty_journal(), run, now(), None)
            .await
            .expect("no hard error");

        assert_eq!(
            out.code, EXIT_PRECONDITION,
            "a claimed run must NOT be reported as a successful wake: {}",
            out.text
        );
        assert!(out.text.contains("not queued"), "{}", out.text);
        assert!(
            out.text.contains("waking"),
            "must name the real state, not a proxy: {}",
            out.text
        );
        assert_eq!(
            inner.status(run).await.unwrap().unwrap().status,
            RunStatus::Waking,
            "the claim, not our force_wake, owns this run now"
        );
    }

    /// MISLEADING FAILURE reproduction: another operator's `cancel` wins the race.
    /// The old code read the resulting NULL `next_wake` and reported the generic
    /// "still has no wake deadline" (the retryable HOTL phrasing) — hiding that the
    /// run was actually CANCELLED and retrying will no-op forever.
    #[tokio::test]
    async fn wake_reports_not_queued_when_a_concurrent_cancel_wins_the_race() {
        let run = RunId(uuid::Uuid::new_v4());
        let inner = paused_store(run, Some(now())).await;
        let racing = RacingStore {
            inner: inner.clone(),
            run,
            actor: ConcurrentActor::CancelsFirst,
        };

        let out = wake(&racing, &empty_journal(), run, now(), None)
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION);
        assert!(out.text.contains("not queued"), "{}", out.text);
        assert!(
            out.text.contains("cancelled"),
            "must name the true reason (cancelled), not a generic NULL-deadline phrase: {}",
            out.text
        );
        assert_eq!(
            inner.status(run).await.unwrap().unwrap().status,
            RunStatus::Cancelled
        );
    }

    /// Guards the TIMESTAMP half of the check specifically — `status == Paused` alone
    /// is NOT enough. A `paused -> waking -> paused` round trip (a claim, then the
    /// executor's own re-pause) lands the row back on `Paused` with a fresh,
    /// UNRELATED deadline. Our own `force_wake` landed while the row was `waking` and
    /// never applied, but by the time `wake` re-reads it, status is `Paused` again —
    /// purely because of someone else's unrelated pause, not our call. Without the
    /// timestamp condition this would report a false success.
    #[tokio::test]
    async fn wake_reports_not_queued_when_a_re_pause_restores_paused_with_an_unrelated_deadline() {
        let run = RunId(uuid::Uuid::new_v4());
        let inner = paused_store(run, Some(now())).await;
        let racing = RacingStore {
            inner: inner.clone(),
            run,
            actor: ConcurrentActor::ReclaimsThenRepausesWithUnrelatedDeadline,
        };

        let out = wake(&racing, &empty_journal(), run, now(), None)
            .await
            .expect("no hard error");

        assert_eq!(
            out.code, EXIT_PRECONDITION,
            "our force_wake never applied; an unrelated re-pause landing inside the \
             race window must not be reported as success: {}",
            out.text
        );
        assert!(out.text.contains("not queued"), "{}", out.text);
        assert!(
            out.text.contains("paused"),
            "must name the real state: {}",
            out.text
        );
        assert_eq!(
            inner.status(run).await.unwrap().unwrap().status,
            RunStatus::Paused,
            "the row IS paused again — just not because of our force_wake"
        );
    }
}
