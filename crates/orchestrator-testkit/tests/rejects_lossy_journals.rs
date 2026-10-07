//! The conformance suite must FAIL a backend that loses AG-2's `DecisionHookFired` marker
//! (AG phase-1 review, gateway#86). The executor folds that row back to keep the HITL hooks
//! firing once: a backend that rejects the kind (a CHECK list that predates it) makes every
//! later drive re-fire `on_gate_decided`/`on_signal_received`/`on_agent_answered`, and one
//! that drops its `effect_id` (a jsonb mapping that keeps only known keys) folds a
//! confirm-call marker as a node marker, so `on_tool_confirm_decided` re-fires on resume.
//! Every in-crate executor test uses `InMemoryJournal`, so only the testkit can catch this.

use async_trait::async_trait;
use orchestrator_core::{ExecutionJournal, JournalError, JournalEvent, RunId, Seq, Snapshot};
use orchestrator_store::InMemoryJournal;

/// Which way the wrapped journal mishandles `DecisionHookFired`.
#[derive(Clone, Copy)]
enum Lossy {
    /// Refuses to store the kind at all.
    Rejects,
    /// Stores it with `effect_id` stripped.
    DropsEffectId,
}

/// A conforming in-memory journal except for its handling of `DecisionHookFired`.
struct LossyJournal {
    inner: InMemoryJournal,
    mode: Lossy,
}

#[async_trait]
impl ExecutionJournal for LossyJournal {
    async fn append(&self, run: RunId, event: JournalEvent) -> Result<Seq, JournalError> {
        let event = match (self.mode, event) {
            (Lossy::Rejects, JournalEvent::DecisionHookFired { .. }) => {
                return Err(JournalError::Backend(
                    "unknown event kind DecisionHookFired".into(),
                ));
            }
            (Lossy::DropsEffectId, JournalEvent::DecisionHookFired { node, decision, .. }) => {
                JournalEvent::DecisionHookFired {
                    node,
                    decision,
                    effect_id: None,
                }
            }
            (_, other) => other,
        };
        self.inner.append(run, event).await
    }

    async fn load(&self, run: RunId) -> Result<Vec<(Seq, JournalEvent)>, JournalError> {
        self.inner.load(run).await
    }

    async fn snapshot(&self, run: RunId, snap: Snapshot) -> Result<(), JournalError> {
        self.inner.snapshot(run, snap).await
    }

    async fn latest_snapshot(&self, run: RunId) -> Result<Option<Snapshot>, JournalError> {
        self.inner.latest_snapshot(run).await
    }

    async fn compact(
        &self,
        run: RunId,
        remove_seqs: &[Seq],
        add: JournalEvent,
    ) -> Result<(), JournalError> {
        self.inner.compact(run, remove_seqs, add).await
    }
}

#[tokio::test]
#[should_panic(expected = "append a hooks marker")]
async fn the_testkit_fails_a_journal_that_rejects_decision_hook_fired() {
    let j = LossyJournal {
        inner: InMemoryJournal::default(),
        mode: Lossy::Rejects,
    };
    orchestrator_testkit::journal(&j).await;
}

#[tokio::test]
#[should_panic(expected = "DecisionHookFired (node- and call-keyed) round-trips exactly")]
async fn the_testkit_fails_a_journal_that_drops_the_marker_effect_id() {
    let j = LossyJournal {
        inner: InMemoryJournal::default(),
        mode: Lossy::DropsEffectId,
    };
    orchestrator_testkit::journal(&j).await;
}
