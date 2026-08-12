//! Durability hooks on the executor: round-boundary snapshots (`write_snapshot`,
//! §5.2) and post-Consolidate compaction (`compact_map`, §5.3). Split out of
//! `super` for readability; both are `impl Executor` methods sharing its state.

use orchestrator_core::{
    ChildStatus, CompactChild, EffectOutput, JournalEvent, NodeId, OrchestratorError, RunId,
    Snapshot,
};

use super::{Executor, RunOutcome};

impl Executor {
    /// Compact a completed `Map`'s per-child journal records (§5.3): collect its
    /// children's `EffectRecorded` (structural path `"{map}/{i}"`), ensure each
    /// output is addressable in the CAS (an inline one is `put` to obtain its
    /// digest; a ref already has one), then `compact` the journal — drop those
    /// child records and append a `MapCompacted` manifest of `{index, status,
    /// digest, input_hash}`. Failed children (which journaled no output) are
    /// recorded from the Map's result manifest as `Failed`. A no-CAS executor
    /// skips compaction (nowhere to keep the content addressable).
    pub(super) async fn compact_map(
        &self,
        run: RunId,
        map: &NodeId,
        outcome: &RunOutcome,
    ) -> Result<(), OrchestratorError> {
        let Some(content) = &self.content else {
            return Ok(());
        };
        let events = self
            .journal
            .load(run)
            .await
            .map_err(OrchestratorError::Journal)?;
        let prefix = format!("{}/", map.0);

        let mut remove_seqs = Vec::new();
        let mut children = Vec::new();
        for (seq, event) in &events {
            let JournalEvent::EffectRecorded {
                node,
                input_hash,
                output,
                ..
            } = event
            else {
                continue;
            };
            let Some(index) = node
                .0
                .strip_prefix(&prefix)
                .and_then(|i| i.parse::<usize>().ok())
            else {
                continue;
            };
            remove_seqs.push(*seq);
            // Materialize the child's content address: a ref already has one; an
            // inline value is put into the CAS (same bytes `split_output` would
            // have written, so the digest is stable).
            let digest = match output {
                EffectOutput::Ref(r) => r.digest.clone(),
                EffectOutput::Inline(value) => content.put(&serde_json::to_vec(value)?).await?,
            };
            children.push(CompactChild {
                index,
                status: ChildStatus::Ok,
                digest: Some(digest),
                input_hash: Some(input_hash.clone()),
            });
        }

        if remove_seqs.is_empty() {
            return Ok(()); // already compacted, or a body kind with no child records
        }

        // Record the failed children (no journal record to drop) from the Map's
        // result manifest, so the compacted manifest describes the whole fan-out.
        if let Some(results) = outcome
            .outputs
            .get(map)
            .and_then(|o| o.get("results"))
            .and_then(|r| r.as_array())
        {
            for r in results {
                if r.get("error").is_some()
                    && let Some(index) = r.get("index").and_then(|i| i.as_u64())
                {
                    children.push(CompactChild {
                        index: index as usize,
                        status: ChildStatus::Failed,
                        digest: None,
                        input_hash: None,
                    });
                }
            }
        }
        children.sort_by_key(|c| c.index);

        self.journal
            .compact(
                run,
                &remove_seqs,
                JournalEvent::MapCompacted {
                    node: map.clone(),
                    children,
                },
            )
            .await
            .map_err(OrchestratorError::Journal)
    }

    /// Write a round-boundary [`Snapshot`] of the current outcome to the journal's
    /// snapshot store (§5.2). Its `seq` is the current max journal `Seq` — the
    /// boundary a resume folds past; each completed node's output is carried as a
    /// ref-or-inline [`EffectOutput`] (large ones split into the CAS, keeping the
    /// snapshot lean). A backend without snapshot support no-ops (trait default).
    pub(super) async fn write_snapshot(
        &self,
        run: RunId,
        outcome: &RunOutcome,
    ) -> Result<(), OrchestratorError> {
        let seq = self
            .journal
            .load(run)
            .await
            .map_err(OrchestratorError::Journal)?
            .iter()
            .map(|(seq, _)| *seq)
            .max()
            .unwrap_or(0);
        let mut outputs = Vec::with_capacity(outcome.outputs.len());
        for (node, value) in &outcome.outputs {
            outputs.push((node.clone(), self.split_output(value).await?));
        }
        let snap = Snapshot {
            seq,
            completed: outcome.completed.clone(),
            skipped: outcome.skipped.clone(),
            outputs,
        };
        self.journal
            .snapshot(run, snap)
            .await
            .map_err(OrchestratorError::Journal)
    }
}
