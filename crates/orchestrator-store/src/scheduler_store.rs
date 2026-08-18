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
