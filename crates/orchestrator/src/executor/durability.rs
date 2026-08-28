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
        // Keyed by child index, not pushed, so a child with TWO `EffectRecorded`
        // events (the two-phase Mutation path's in-doubt `Confirmed` reconcile can
        // append a second one for the same effect id) collapses to ONE manifest
        // entry, last-wins — matching what `fold_journal`'s keyed `insert` would have
        // done with those records. Two entries for one index would have been harmless
        // for the memo but would DOUBLE-COUNT the child's tokens once the manifest
        // carries `usage`.
        let mut children: std::collections::BTreeMap<usize, CompactChild> =
            std::collections::BTreeMap::new();
        for (seq, event) in &events {
            let JournalEvent::EffectRecorded {
                node,
                input_hash,
                output,
                usage,
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
            children.insert(
                index,
                CompactChild {
                    index,
                    status: ChildStatus::Ok,
                    digest: Some(digest),
                    input_hash: Some(input_hash.clone()),
                    // SP-DATA-5: carry the child's spend onto the manifest — the
                    // record holding it is about to be deleted.
                    usage: *usage,
                },
            );
        }
        let mut children: Vec<CompactChild> = children.into_values().collect();

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
                        // A failed child journaled no `EffectRecorded`, so it has no
                        // spend to preserve.
                        usage: None,
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
    ///
    /// `fold` supplies the SP-DATA-5 ledger scalars. They are not derivable from
    /// `outcome`, and a snapshot without them is only safe for as long as nothing
    /// folds tail-only — see [`Snapshot::spent`].
    pub(super) async fn write_snapshot(
        &self,
        run: RunId,
        outcome: &RunOutcome,
        fold: &super::Fold,
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
            // Journaled + this drive's live tally: the same total the gate reads, so a
            // seeded resume starts from where the run actually is, not where its
            // journal prefix left off.
            spent: fold.spent(),
            budget: fold.budget(),
        };
        self.journal
            .snapshot(run, snap)
            .await
            .map_err(OrchestratorError::Journal)
    }
}
