//! SP-DATA-3: the in-memory [`SchedulerStore`] — the reference impl + parity target for
//! `PostgresSchedulerStore`. `Clone` shares one `Arc`-backed map (the crash/resume seam, like
//! [`InMemoryJournal`](crate::InMemoryJournal)).

use chrono::{DateTime, Duration, Utc};
use orchestrator_core::graph::Graph;
use orchestrator_core::ids::RunId;
use orchestrator_core::{OrchestratorError, RunStatus, ScheduledRun, SchedulerStore};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Row {
    graph: Graph,
    status: RunStatus,
    next_wake: Option<DateTime<Utc>>,
    claimed_at: Option<DateTime<Utc>>,
    reason: Option<String>,
    updated_at: DateTime<Utc>,
}

/// In-memory durable-scheduler store. `Clone` shares one Arc-backed map.
#[derive(Clone, Default)]
pub struct InMemorySchedulerStore {
    rows: Arc<Mutex<HashMap<RunId, Row>>>,
}

impl InMemorySchedulerStore {
    pub fn new() -> Self {
        Self::default()
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<RunId, Row>> {
        self.rows.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn observe(run: RunId, r: &Row) -> ScheduledRun {
        ScheduledRun {
            run,
            status: r.status,
            next_wake: r.next_wake,
            reason: r.reason.clone(),
            updated_at: r.updated_at,
        }
    }
}

#[async_trait::async_trait]
impl SchedulerStore for InMemorySchedulerStore {
    async fn enqueue(
        &self,
        run: RunId,
        graph: &Graph,
        now: DateTime<Utc>,
    ) -> Result<(), OrchestratorError> {
        let mut m = self.lock();
        if m.contains_key(&run) {
            return Err(OrchestratorError::Store(format!(
                "duplicate submit for run {run:?}"
            )));
        }
        m.insert(
            run,
            Row {
                graph: graph.clone(),
                status: RunStatus::Waking,
                next_wake: None,
                claimed_at: Some(now),
                reason: None,
                updated_at: now,
            },
        );
        Ok(())
    }

    async fn record_paused(
        &self,
        run: RunId,
        next_wake: Option<DateTime<Utc>>,
        reason: &str,
    ) -> Result<(), OrchestratorError> {
        let mut m = self.lock();
        if let Some(r) = m.get_mut(&run)
            && r.status == RunStatus::Waking
        {
            r.status = RunStatus::Paused;
            r.next_wake = next_wake;
            r.claimed_at = None;
            r.reason = Some(reason.to_string());
        }
        Ok(())
    }

    async fn record_terminal(
        &self,
        run: RunId,
        status: RunStatus,
        reason: Option<&str>,
    ) -> Result<(), OrchestratorError> {
        let mut m = self.lock();
        if let Some(r) = m.get_mut(&run)
            && r.status == RunStatus::Waking
        {
            r.status = status;
            r.next_wake = None;
            r.claimed_at = None;
            r.reason = reason.map(str::to_string);
        }
        Ok(())
    }

    async fn claim_due(
        &self,
        now: DateTime<Utc>,
        lease: Duration,
        limit: usize,
    ) -> Result<Vec<(RunId, Graph)>, OrchestratorError> {
        let mut m = self.lock();
        let mut out = Vec::new();
        for (run, r) in m.iter_mut() {
            if out.len() >= limit {
                break;
            }
            let due_paused =
                r.status == RunStatus::Paused && r.next_wake.map(|w| w <= now).unwrap_or(false);
            let stale_waking = r.status == RunStatus::Waking
                && r.claimed_at.map(|c| now - c > lease).unwrap_or(false);
            if due_paused || stale_waking {
                r.status = RunStatus::Waking;
                r.claimed_at = Some(now);
                r.updated_at = now;
                out.push((*run, r.graph.clone()));
            }
        }
        Ok(out)
    }

    async fn status(&self, run: RunId) -> Result<Option<ScheduledRun>, OrchestratorError> {
        Ok(self.lock().get(&run).map(|r| Self::observe(run, r)))
    }

    async fn list_paused(&self) -> Result<Vec<ScheduledRun>, OrchestratorError> {
        Ok(self
            .lock()
            .iter()
            .filter(|(_, r)| r.status == RunStatus::Paused)
            .map(|(run, r)| Self::observe(*run, r))
            .collect())
    }

    async fn cancel(&self, run: RunId) -> Result<(), OrchestratorError> {
        let mut m = self.lock();
        if let Some(r) = m.get_mut(&run)
            && !r.status.is_terminal()
        {
            r.status = RunStatus::Cancelled;
            r.next_wake = None;
        }
        Ok(())
    }

    async fn force_wake(&self, run: RunId, now: DateTime<Utc>) -> Result<(), OrchestratorError> {
        let mut m = self.lock();
        if let Some(r) = m.get_mut(&run)
            && r.status == RunStatus::Paused
        {
            r.next_wake = Some(now);
        }
        Ok(())
    }

    async fn count_terminal_before(&self, before: DateTime<Utc>) -> Result<u64, OrchestratorError> {
        Ok(self.lock().values().filter(|r| prunable(r, before)).count() as u64)
    }

    async fn prune_terminal(&self, before: DateTime<Utc>) -> Result<u64, OrchestratorError> {
        let mut m = self.lock();
        let before_len = m.len();
        m.retain(|_, r| !prunable(r, before));
        Ok((before_len - m.len()) as u64)
    }
}

/// The single eligibility predicate, shared by the count and the delete so the operator's
/// preview cannot disagree with the effect.
///
/// `RunStatus::is_terminal` is an ALLOWLIST (`Completed`|`Failed`|`Cancelled`), which is the
/// safety property: `paused` and `waking` are not merely excluded by name, they are outside
/// the set that is ever considered — so no age, and no future status added to the enum,
/// makes a live run eligible.
///
/// PARITY NOTE: this store stamps `updated_at` only at `enqueue`/`claim_due` — its transition
/// methods take no `now` and cannot read a clock — whereas `PostgresSchedulerStore` sets
/// `updated_at = now()` on every transition. So an in-memory row's age is measured from its
/// last enqueue/claim rather than from the moment it went terminal, which can make it
/// eligible slightly sooner. That gap is confined to the non-durable reference store (torii
/// only ever prunes Postgres) and cannot affect the safety property, which is keyed on status
/// alone.
fn prunable(r: &Row, before: DateTime<Utc>) -> bool {
    r.status.is_terminal() && r.updated_at < before
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::graph::Graph;
    use orchestrator_core::ids::RunId;
    use orchestrator_core::{RunStatus, SchedulerStore};

    fn t(secs: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(1_000_000 + secs, 0).unwrap()
    }
    fn run() -> RunId {
        RunId(uuid::Uuid::new_v4())
    }
    fn g() -> Graph {
        Graph { nodes: vec![] }
    }
    fn lease() -> Duration {
        Duration::seconds(60)
    }

    #[tokio::test]
    async fn claim_due_returns_a_due_paused_run_but_not_a_future_one() {
        let s = InMemorySchedulerStore::new();
        let (a, b) = (run(), run());
        s.enqueue(a, &g(), t(0)).await.unwrap();
        s.record_paused(a, Some(t(10)), "gated").await.unwrap();
        s.enqueue(b, &g(), t(0)).await.unwrap();
        s.record_paused(b, Some(t(100)), "gated").await.unwrap();
        let due = s.claim_due(t(20), lease(), 10).await.unwrap();
        assert_eq!(due.len(), 1, "only `a` is due");
        assert_eq!(due[0].0, a);
        assert_eq!(
            s.status(a).await.unwrap().unwrap().status,
            RunStatus::Waking,
            "claim flips to waking"
        );
    }

    #[tokio::test]
    async fn claim_due_never_claims_a_null_deadline_pause() {
        let s = InMemorySchedulerStore::new();
        let a = run();
        s.enqueue(a, &g(), t(0)).await.unwrap();
        s.record_paused(a, None, "in-doubt").await.unwrap();
        assert!(
            s.claim_due(t(10_000), lease(), 10)
                .await
                .unwrap()
                .is_empty(),
            "NULL next_wake is never auto-woken"
        );
    }

    #[tokio::test]
    async fn claim_due_reclaims_a_stale_waking_but_not_a_fresh_one() {
        let s = InMemorySchedulerStore::new();
        let (stale, fresh) = (run(), run());
        s.enqueue(stale, &g(), t(0)).await.unwrap(); // claimed_at = t(0)
        s.enqueue(fresh, &g(), t(100)).await.unwrap(); // claimed_at = t(100)
        // now = t(120), lease = 60: stale (120-0 > 60) reclaimed; fresh (120-100 < 60) not.
        let due = s.claim_due(t(120), lease(), 10).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].0, stale);
    }

    #[tokio::test]
    async fn cancel_makes_a_paused_run_unwakeable() {
        let s = InMemorySchedulerStore::new();
        let a = run();
        s.enqueue(a, &g(), t(0)).await.unwrap();
        s.record_paused(a, Some(t(10)), "gated").await.unwrap();
        s.cancel(a).await.unwrap();
        assert_eq!(
            s.status(a).await.unwrap().unwrap().status,
            RunStatus::Cancelled
        );
        assert!(
            s.claim_due(t(1000), lease(), 10).await.unwrap().is_empty(),
            "a cancelled run is never claimed"
        );
    }

    #[tokio::test]
    async fn force_wake_makes_a_null_deadline_pause_claimable() {
        let s = InMemorySchedulerStore::new();
        let a = run();
        s.enqueue(a, &g(), t(0)).await.unwrap();
        s.record_paused(a, None, "in-doubt").await.unwrap();
        s.force_wake(a, t(50)).await.unwrap();
        let due = s.claim_due(t(60), lease(), 10).await.unwrap();
        assert_eq!(due.len(), 1, "force_wake makes it due");
        assert_eq!(due[0].0, a);
    }

    // ---- SP-DATA-4.1 #7: retention pruning -------------------------------------------
    //
    // `updated_at` in THIS store is stamped at `enqueue`/`claim_due` only (its transition
    // methods take no `now`), so these tests age a row by enqueuing it at `t(0)` and then
    // pruning with a cutoff decades later — see `prune_terminal`'s impl comment.

    /// A cutoff so far past every row's `updated_at` that age can never be the reason a
    /// row survives — ~31 years after `t(0)`. The safety tests below rely on this: if a
    /// `paused` row survives THIS, it survived because of its status, not its age.
    fn far_future() -> DateTime<Utc> {
        t(1_000_000_000)
    }

    #[tokio::test]
    async fn prune_terminal_deletes_old_terminal_rows_of_every_terminal_status() {
        let s = InMemorySchedulerStore::new();
        let (c, f, x) = (run(), run(), run());
        for r in [c, f, x] {
            s.enqueue(r, &g(), t(0)).await.unwrap();
        }
        s.record_terminal(c, RunStatus::Completed, None)
            .await
            .unwrap();
        s.record_terminal(f, RunStatus::Failed, Some("boom"))
            .await
            .unwrap();
        s.cancel(x).await.unwrap();

        assert_eq!(s.count_terminal_before(far_future()).await.unwrap(), 3);
        assert_eq!(s.prune_terminal(far_future()).await.unwrap(), 3);
        for r in [c, f, x] {
            assert!(
                s.status(r).await.unwrap().is_none(),
                "every terminal status must be eligible"
            );
        }
        assert_eq!(
            s.prune_terminal(far_future()).await.unwrap(),
            0,
            "a second prune finds nothing left"
        );
    }

    #[tokio::test]
    async fn prune_terminal_keeps_a_terminal_row_newer_than_the_cutoff() {
        let s = InMemorySchedulerStore::new();
        let (old, new) = (run(), run());
        s.enqueue(old, &g(), t(0)).await.unwrap();
        s.enqueue(new, &g(), t(500)).await.unwrap();
        for r in [old, new] {
            s.record_terminal(r, RunStatus::Completed, None)
                .await
                .unwrap();
        }
        // Cutoff between the two: strictly-older-than, so `new` (at t(500)) is out of scope.
        assert_eq!(s.count_terminal_before(t(100)).await.unwrap(), 1);
        assert_eq!(s.prune_terminal(t(100)).await.unwrap(), 1);
        assert!(s.status(old).await.unwrap().is_none(), "the old one goes");
        assert!(
            s.status(new).await.unwrap().is_some(),
            "a terminal row inside the retention window stays"
        );
    }

    /// THE safety property. A `paused` run has no age at which it becomes safe to forget:
    /// it is live work awaiting a wake, and the in-doubt-mutation class pauses with a NULL
    /// `next_wake` and waits INDEFINITELY for a human. Deleting one destroys a run that was
    /// working correctly. The cutoff here is decades past both rows, so nothing but the
    /// status guard can be saving them.
    #[tokio::test]
    async fn prune_terminal_never_deletes_a_paused_run_however_old() {
        let s = InMemorySchedulerStore::new();
        let (timed, in_doubt) = (run(), run());
        s.enqueue(timed, &g(), t(0)).await.unwrap();
        s.record_paused(timed, Some(t(10)), "quota").await.unwrap();
        s.enqueue(in_doubt, &g(), t(0)).await.unwrap();
        s.record_paused(in_doubt, None, "in-doubt mutation")
            .await
            .unwrap();

        assert_eq!(
            s.count_terminal_before(far_future()).await.unwrap(),
            0,
            "a paused row must not even be COUNTED as prunable"
        );
        assert_eq!(s.prune_terminal(far_future()).await.unwrap(), 0);
        assert_eq!(
            s.status(timed).await.unwrap().unwrap().status,
            RunStatus::Paused,
            "a paused run awaiting a deadline is never prunable"
        );
        assert_eq!(
            s.status(in_doubt).await.unwrap().unwrap().status,
            RunStatus::Paused,
            "a NULL-deadline pause waits indefinitely for a human — deleting it is data loss"
        );
    }

    /// THE other safety property: a `waking` row may be a LIVE LEASE — an in-flight drive
    /// in another process. Its `claimed_at` being ancient means the lease is reclaimable,
    /// not that the run is disposable.
    #[tokio::test]
    async fn prune_terminal_never_deletes_a_waking_run_however_old() {
        let s = InMemorySchedulerStore::new();
        let a = run();
        s.enqueue(a, &g(), t(0)).await.unwrap(); // enqueue leaves it `waking`

        assert_eq!(s.count_terminal_before(far_future()).await.unwrap(), 0);
        assert_eq!(s.prune_terminal(far_future()).await.unwrap(), 0);
        assert_eq!(
            s.status(a).await.unwrap().unwrap().status,
            RunStatus::Waking,
            "a waking row may be an in-flight drive holding a lease"
        );
    }

    /// The count is the CLI's preview of the delete, so the two must agree on the same
    /// cutoff — a preview that over-counts asks an operator to consent to more than
    /// happens, and one that under-counts deletes more than they agreed to.
    #[tokio::test]
    async fn count_terminal_before_agrees_with_what_prune_deletes() {
        let s = InMemorySchedulerStore::new();
        let (done, paused, waking) = (run(), run(), run());
        s.enqueue(done, &g(), t(0)).await.unwrap();
        s.record_terminal(done, RunStatus::Completed, None)
            .await
            .unwrap();
        s.enqueue(paused, &g(), t(0)).await.unwrap();
        s.record_paused(paused, None, "in-doubt").await.unwrap();
        s.enqueue(waking, &g(), t(0)).await.unwrap();

        let counted = s.count_terminal_before(far_future()).await.unwrap();
        let deleted = s.prune_terminal(far_future()).await.unwrap();
        assert_eq!(counted, 1, "only the terminal row is in scope");
        assert_eq!(deleted, counted, "the preview must match the effect");
    }

    #[tokio::test]
    async fn record_paused_is_a_noop_after_cancel_no_resurrection() {
        let s = InMemorySchedulerStore::new();
        let a = run();
        s.enqueue(a, &g(), t(0)).await.unwrap(); // waking
        s.cancel(a).await.unwrap(); // cancelled
        s.record_paused(a, Some(t(10)), "gated").await.unwrap(); // conditional on waking → no-op
        assert_eq!(
            s.status(a).await.unwrap().unwrap().status,
            RunStatus::Cancelled,
            "cancel wins; the row is not resurrected"
        );
    }
}
