//! Observe and intervene on runs. Every command reports the EFFECT it achieved,
//! never the fact that the store call returned Ok — `cancel` on a terminal run and
//! `wake` on a non-paused run are both silent no-ops at the store level.

use crate::cmd::Outcome;
use crate::errors::CliError;
use crate::render;
use chrono::{DateTime, Utc};
use orchestrator_core::{RunId, RunStatus, SchedulerStore};

// Consumed by Task 10 (main.rs clap dispatch), `torii run status <id>`.
#[allow(dead_code)]
pub async fn status(
    store: &dyn SchedulerStore,
    run: RunId,
    json: bool,
) -> Result<Outcome, CliError> {
    match store.status(run).await? {
        None => Ok(Outcome::precondition(format!("no such run: {}", run.0))),
        Some(r) => Ok(Outcome::ok(if json {
            render::json(&[r]).map_err(|e| CliError::error(e.to_string()))?
        } else {
            render::table(&[r])
        })),
    }
}

// Consumed by Task 10 (main.rs clap dispatch), `torii run list-paused`.
#[allow(dead_code)]
pub async fn list_paused(store: &dyn SchedulerStore, json: bool) -> Result<Outcome, CliError> {
    let rows = store.list_paused().await?;
    Ok(Outcome::ok(if json {
        render::json(&rows).map_err(|e| CliError::error(e.to_string()))?
    } else {
        render::table(&rows)
    }))
}

// Consumed by Task 10 (main.rs clap dispatch), `torii run cancel <id>`.
#[allow(dead_code)]
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

// Consumed by Task 10 (main.rs clap dispatch), `torii run wake <id>`.
#[allow(dead_code)]
pub async fn wake(
    store: &dyn SchedulerStore,
    run: RunId,
    now: DateTime<Utc>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::{EXIT_OK, EXIT_PRECONDITION};
    use orchestrator_core::Graph;
    use orchestrator_store::InMemorySchedulerStore;

    fn now() -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(3_000_000, 0).unwrap()
    }

    fn empty_graph() -> Graph {
        Graph { nodes: vec![] }
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
        let out = status(&s, RunId(uuid::Uuid::new_v4()), false)
            .await
            .expect("no hard error");
        assert_eq!(out.code, EXIT_PRECONDITION);
        assert!(out.text.contains("no such run"), "{}", out.text);
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
        let out = wake(&s, run, now()).await.expect("wakes");
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
        let out = wake(&s, run, now()).await.expect("no hard error");
        assert_eq!(out.code, EXIT_PRECONDITION);
        assert!(out.text.contains("not queued"), "{}", out.text);
        assert!(
            out.text.contains("waking"),
            "must name the actual state: {}",
            out.text
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

        let out = wake(&racing, run, now()).await.expect("no hard error");

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

        let out = wake(&racing, run, now()).await.expect("no hard error");

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

        let out = wake(&racing, run, now()).await.expect("no hard error");

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
