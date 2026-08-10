//! `sensei-orchestrator-store` — journal stores for the sensei orchestrator.
//!
//! Slice 1 ships [`InMemoryJournal`], the `impl ExecutionJournal` the executor
//! writes to and folds on resume. A `PostgresJournal` implements the same trait
//! in a later slice; persistence is held off until then.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use orchestrator_core::{ExecutionJournal, JournalError, JournalEvent, RunId, Seq};

mod stores;
pub use stores::{InMemoryContentStore, InMemoryContextStore};

/// The shared, `Seq`-stamped event log keyed by run, guarded for concurrent
/// appends and clonable across executors.
type SharedLog = Arc<Mutex<HashMap<RunId, Vec<(Seq, JournalEvent)>>>>;

/// In-memory [`ExecutionJournal`]. `Clone` shares one Arc-backed log, so a fresh
/// executor built from a clone sees a prior run's events — the crash/resume
/// seam. Every append stamps a globally monotonic [`Seq`], and appends land in
/// per-run insertion order, which is therefore already `Seq`-ascending.
#[derive(Clone, Default)]
pub struct InMemoryJournal {
    runs: SharedLog,
    next_seq: Arc<AtomicU64>,
}

impl InMemoryJournal {
    /// Creates an empty journal.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl ExecutionJournal for InMemoryJournal {
    async fn append(&self, run: RunId, event: JournalEvent) -> Result<Seq, JournalError> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        self.runs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(run)
            .or_default()
            .push((seq, event));
        Ok(seq)
    }

    async fn load(&self, run: RunId) -> Result<Vec<(Seq, JournalEvent)>, JournalError> {
        Ok(self
            .runs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&run)
            .cloned()
            .unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{ExecutionJournal, JournalEvent, NodeId, RunId};

    fn run_started() -> JournalEvent {
        JournalEvent::RunStarted {
            version: "v".into(),
        }
    }

    fn node_started() -> JournalEvent {
        JournalEvent::NodeStarted {
            node: NodeId("n".into()),
        }
    }

    #[tokio::test]
    async fn append_then_load_returns_events_in_ascending_seq() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());

        let s0 = journal.append(run, run_started()).await.unwrap();
        let s1 = journal.append(run, node_started()).await.unwrap();

        assert!(s1 > s0, "second append seq must exceed the first");

        let loaded = journal.load(run).await.unwrap();
        assert_eq!(loaded.len(), 2, "both events present");
        assert_eq!(loaded[0].0, s0);
        assert_eq!(loaded[1].0, s1);
        assert!(
            loaded[0].0 < loaded[1].0,
            "load returns events in strictly ascending seq"
        );
        assert!(matches!(loaded[0].1, JournalEvent::RunStarted { .. }));
        assert!(matches!(loaded[1].1, JournalEvent::NodeStarted { .. }));
    }

    #[tokio::test]
    async fn seq_is_globally_monotonic_across_runs() {
        let journal = InMemoryJournal::new();
        let run_a = RunId(uuid::Uuid::new_v4());
        let run_b = RunId(uuid::Uuid::new_v4());

        // Interleave: A, B, A — seqs must strictly increase across the whole log.
        let s0 = journal.append(run_a, run_started()).await.unwrap();
        let s1 = journal.append(run_b, run_started()).await.unwrap();
        let s2 = journal.append(run_a, node_started()).await.unwrap();

        assert!(s0 < s1, "seq strictly increases across runs");
        assert!(s1 < s2, "seq strictly increases across runs");

        // Within run A the two seqs it saw are still ascending in load order.
        let a = journal.load(run_a).await.unwrap();
        assert_eq!(a.len(), 2);
        assert_eq!(a[0].0, s0);
        assert_eq!(a[1].0, s2);
        assert!(a[0].0 < a[1].0);
    }

    #[tokio::test]
    async fn clones_share_one_arc_backed_log() {
        // Two clones are the crash/resume seam: a fresh executor built on a
        // clone must see a prior run's appends.
        let clone1 = InMemoryJournal::new();
        let clone2 = clone1.clone();
        let run = RunId(uuid::Uuid::new_v4());

        let seq = clone1.append(run, run_started()).await.unwrap();

        let loaded = clone2.load(run).await.unwrap();
        assert_eq!(loaded.len(), 1, "clone2 sees clone1's append");
        assert_eq!(loaded[0].0, seq);
        assert!(matches!(loaded[0].1, JournalEvent::RunStarted { .. }));
    }

    #[tokio::test]
    async fn distinct_runs_are_isolated() {
        let journal = InMemoryJournal::new();
        let run_a = RunId(uuid::Uuid::new_v4());
        let run_b = RunId(uuid::Uuid::new_v4());

        journal.append(run_a, run_started()).await.unwrap();

        let b = journal.load(run_b).await.unwrap();
        assert!(b.is_empty(), "run B does not see run A's events");

        let a = journal.load(run_a).await.unwrap();
        assert_eq!(a.len(), 1, "run A keeps its own event");
    }
}
