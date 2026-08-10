//! Fan-out on the executor: the `Map` bounded-concurrency fan-out
//! (`run_map` + `run_map_child_modelcall`) and the `Consolidate` synthesis
//! (`run_consolidate`). Split out of `super` for readability; all are
//! `impl Executor` methods sharing its private state.

use std::collections::HashMap;
use std::sync::Arc;

use orchestrator_core::{
    Aggregation, EffectClass, JournalEvent, MapBody, NodeId, NodeKind, OrchestratorError, RunId,
    effect_id,
};

use super::support::{build_request, input_hash};
use super::{AgentStep, Executor, Fold, NodeExec};

impl Executor {
    /// Run a `Consolidate` node (§3.5): read the successful results of its `over`
    /// Map from `prior_outputs`, gate on `min_viable` (fewer survivors ⇒
    /// `ConsolidateStarved`, a loud halt — never a silent empty synthesis), then
    /// run `body` **once** over the collected survivors and return its output. A
    /// determinism violation / journal-write error aborts as `Err`.
    pub(super) async fn run_consolidate(
        &self,
        run: RunId,
        node: &orchestrator_core::Node,
        prior_outputs: &HashMap<NodeId, serde_json::Value>,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        let NodeKind::Consolidate {
            over,
            min_viable,
            body,
        } = &node.kind
        else {
            unreachable!("run_consolidate is only dispatched for a Consolidate node");
        };
        let min_viable = *min_viable;
        // Collect the Map's successful results (the `ok` value of each child) in
        // item order. A missing/absent Map output yields zero survivors, which
        // the min-viable gate turns into a loud starvation rather than a silent
        // empty synthesis.
        let survivors: Vec<serde_json::Value> = prior_outputs
            .get(over)
            .and_then(|map_out| map_out.get("results"))
            .and_then(|results| results.as_array())
            .map(|results| {
                results
                    .iter()
                    .filter_map(|r| r.get("ok").cloned())
                    .collect()
            })
            .unwrap_or_default();

        if survivors.len() < min_viable {
            let err = OrchestratorError::ConsolidateStarved {
                node: node.id.clone(),
                have: survivors.len(),
                need: min_viable,
            };
            let message = err.to_string();
            self.append(
                run,
                JournalEvent::NodeFailed {
                    node: node.id.clone(),
                    error: message.clone(),
                },
            )
            .await?;
            return Ok(NodeExec::Failed {
                message,
                output: None,
            });
        }

        // Run `body` once over the survivors. A `ModelCall` body journals its own
        // `NodeStarted`/synthesis effect/`NodeCompleted` (all fold-guarded,
        // resume-safe); an `Agent` body delegates to `drive_agent`, which owns
        // the node's `NodeStarted`/turns/`NodeCompleted` — so `run_consolidate`
        // must not double-journal them.
        let input = serde_json::json!({ "results": survivors });
        match body {
            MapBody::ModelCall { chain } => {
                if !fold.started.contains(&node.id) {
                    self.append(
                        run,
                        JournalEvent::NodeStarted {
                            node: node.id.clone(),
                        },
                    )
                    .await?;
                }
                // The structural effect id nests under this node's own path
                // (`effect_id(node, 0, 0)`), so a resume memoizes the synthesis
                // without re-spending.
                let eid = effect_id(&node.id.0, 0, 0);
                let payload = serde_json::json!({ "prompt": input.to_string() });
                let ih = input_hash(chain, &payload)?;

                // Memoized on resume: replay the recorded synthesis — no gateway
                // call, no re-append. A hash mismatch is a determinism violation.
                let output = if let Some((recorded_ih, recorded)) = fold.memo.get(&eid) {
                    if recorded_ih != &ih {
                        return Err(OrchestratorError::DeterminismViolation {
                            node: node.id.clone(),
                            effect_id: eid,
                        });
                    }
                    self.materialize(recorded).await?
                } else {
                    let request = build_request(chain, &payload);
                    match self.gateway.execute(&request).await {
                        Ok(response) => {
                            let output = serde_json::json!({
                                "model": response.model,
                                "text": response.content.clone().unwrap_or_default(),
                            });
                            let recorded = self.split_output(&output).await?;
                            self.append(
                                run,
                                JournalEvent::EffectRecorded {
                                    node: node.id.clone(),
                                    effect_id: eid,
                                    class: EffectClass::Pure,
                                    input_hash: ih,
                                    seq: 0,
                                    output: recorded,
                                    observation: None,
                                },
                            )
                            .await?;
                            output
                        }
                        Err(error) => {
                            let message = error.to_string();
                            self.append(
                                run,
                                JournalEvent::NodeFailed {
                                    node: node.id.clone(),
                                    error: message.clone(),
                                },
                            )
                            .await?;
                            return Ok(NodeExec::Failed {
                                message,
                                output: None,
                            });
                        }
                    }
                };
                if !fold.completed.contains(&node.id) {
                    self.append(
                        run,
                        JournalEvent::NodeCompleted {
                            node: node.id.clone(),
                        },
                    )
                    .await?;
                }
                Ok(NodeExec::Completed(output))
            }
            MapBody::Agent(agent_ref) => {
                match self
                    .drive_agent(run, &node.id, agent_ref, &input, fold)
                    .await?
                {
                    AgentStep::Completed(output) => Ok(NodeExec::Completed(output)),
                    AgentStep::Failed(message) => Ok(NodeExec::Failed {
                        message,
                        output: None,
                    }),
                    AgentStep::Paused(reason) => Ok(NodeExec::Paused { reason }),
                }
            }
        }
    }

    /// Run a `Map` node's internal bounded fan-out (§3.4). Journals
    /// `NodeStarted → MapExpanded`, runs `body` once per item in `over`
    /// concurrently (capped by `min(map.concurrency, executor.concurrency)`,
    /// each item at the structural path `"{map}/{i}"`), then folds the children
    /// into `{ results, manifest }` — results **indexed by item order**
    /// regardless of completion order — and decides the Map's own status by
    /// `aggregation`. A completed Map journals `NodeCompleted`; a failed one
    /// journals `NodeFailed` and still carries the manifest out via
    /// [`NodeExec::Failed`]`.output`. A fatal error (journal write / determinism)
    /// aborts as `Err`.
    pub(super) async fn run_map(
        &self,
        run: RunId,
        map_node: &orchestrator_core::Node,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        let NodeKind::Map {
            body,
            over,
            concurrency,
            aggregation,
        } = &map_node.kind
        else {
            unreachable!("run_map is only dispatched for a Map node");
        };
        let concurrency = *concurrency;
        // Resume-safety: a Map replayed on resume (its children memoized) must NOT
        // re-append its `NodeStarted`/`MapExpanded`/`NodeCompleted` — those are
        // already journaled. Guarded via the fold, exactly like `drive_agent`.
        // (Slice 3 runs a Map atomically per round, so its start and completion
        // are journaled together; the guards make a resumed replay idempotent.)
        let already_started = fold.started.contains(&map_node.id);
        if !already_started {
            self.append(
                run,
                JournalEvent::NodeStarted {
                    node: map_node.id.clone(),
                },
            )
            .await?;
            self.append(
                run,
                JournalEvent::MapExpanded {
                    node: map_node.id.clone(),
                    child_count: over.len(),
                },
            )
            .await?;
        }

        // Bounded concurrent fan-out. The semaphore caps how many children hold
        // a permit (i.e. are dispatching a gateway call) at once; `join_all`
        // polls them cooperatively on this task, so concurrency is realized at
        // the children's `.await` points (the gateway I/O).
        let cap = concurrency.min(self.concurrency).max(1);
        let sem = Arc::new(tokio::sync::Semaphore::new(cap));
        let child_futures = over.iter().enumerate().map(|(i, item)| {
            let sem = sem.clone();
            let map_id = map_node.id.0.clone();
            async move {
                let _permit = sem.acquire().await.expect("semaphore is never closed");
                let path = format!("{map_id}/{i}");
                let result = match body {
                    MapBody::ModelCall { chain } => {
                        self.run_map_child_modelcall(run, &path, chain, item, fold)
                            .await
                    }
                    // An Agent child is a per-item ReAct sub-run at the child
                    // path; its outer error is fatal, its `Failed` becomes the
                    // child's manifest error, its `Completed` the child's value.
                    MapBody::Agent(agent_ref) => {
                        match self
                            .drive_agent(run, &NodeId(path.clone()), agent_ref, item, fold)
                            .await
                        {
                            Ok(AgentStep::Completed(output)) => Ok(Ok(output)),
                            Ok(AgentStep::Failed(message)) => Ok(Err(message)),
                            // A Mutation pause inside a fanned-out Map child is out
                            // of slice-4 scope (the demo places Mutations at the
                            // top level / Consolidate, never in Map children):
                            // surface it as the child's manifest error rather than
                            // threading a pause out of `join_all`. Whole-Map pause
                            // propagation lands with agent-child Mutations later.
                            Ok(AgentStep::Paused(reason)) => {
                                Ok(Err(format!("paused (unsupported in map child): {reason}")))
                            }
                            Err(fatal) => Err(fatal),
                        }
                    }
                };
                (i, result)
            }
        });
        let mut collected = futures::future::join_all(child_futures).await;
        // `join_all` preserves input order, but sort by index to make the
        // deterministic-ordering guarantee explicit and completion-order-proof.
        collected.sort_by_key(|(i, _)| *i);

        // Fold children into the manifest, propagating any fatal error.
        let mut results = Vec::with_capacity(over.len());
        let mut ok = 0usize;
        let mut failed = 0usize;
        for (i, child) in collected {
            match child? {
                Ok(value) => {
                    ok += 1;
                    results.push(serde_json::json!({ "index": i, "ok": value }));
                }
                Err(message) => {
                    failed += 1;
                    results.push(serde_json::json!({ "index": i, "error": message }));
                }
            }
        }
        let total = over.len();
        let output = serde_json::json!({
            "results": results,
            "manifest": { "ok": ok, "failed": failed },
        });

        let satisfied = match aggregation {
            Aggregation::BestEffort => true,
            Aggregation::FailFast => failed == 0,
            Aggregation::Quorum {
                min_count,
                min_fraction,
            } => {
                let count_ok = min_count.is_none_or(|m| ok >= m);
                let frac_ok =
                    min_fraction.is_none_or(|f| total > 0 && (ok as f64 / total as f64) >= f);
                count_ok && frac_ok
            }
        };

        if satisfied {
            // Guard the completion append too — a replayed completed Map must not
            // re-journal `NodeCompleted` (it is already recorded).
            if !fold.completed.contains(&map_node.id) {
                self.append(
                    run,
                    JournalEvent::NodeCompleted {
                        node: map_node.id.clone(),
                    },
                )
                .await?;
            }
            Ok(NodeExec::Completed(output))
        } else {
            let message = format!(
                "map {:?} aggregation not satisfied: {ok}/{total} succeeded, {failed} failed",
                map_node.id
            );
            self.append(
                run,
                JournalEvent::NodeFailed {
                    node: map_node.id.clone(),
                    error: message.clone(),
                },
            )
            .await?;
            Ok(NodeExec::Failed {
                message,
                output: Some(output),
            })
        }
    }

    /// Run one `MapBody::ModelCall` child at structural path `path` — a single
    /// Pure effect `effect_id(path, 0, 0)` with `item` as the request payload.
    /// The outer `Result` is fatal (journal write / determinism) and aborts the
    /// run; the inner `Result` is the child's own success value or failure
    /// message, which lands in the Map's manifest. A memoized child replays with
    /// no gateway call (resume); a live one journals its `EffectRecorded`. A
    /// failed child records nothing durable, so a resume re-dispatches it.
    async fn run_map_child_modelcall(
        &self,
        run: RunId,
        path: &str,
        chain: &str,
        item: &serde_json::Value,
        fold: &Fold,
    ) -> Result<Result<serde_json::Value, String>, OrchestratorError> {
        let eid = effect_id(path, 0, 0);
        let ih = input_hash(chain, item)?;

        if let Some((recorded_ih, output)) = fold.memo.get(&eid) {
            if recorded_ih != &ih {
                return Err(OrchestratorError::DeterminismViolation {
                    node: NodeId(path.to_string()),
                    effect_id: eid,
                });
            }
            return Ok(Ok(self.materialize(output).await?));
        }

        let request = build_request(chain, item);
        match self.gateway.execute(&request).await {
            Ok(response) => {
                let output = serde_json::json!({
                    "model": response.model,
                    "text": response.content.clone().unwrap_or_default(),
                });
                let recorded = self.split_output(&output).await?;
                self.append(
                    run,
                    JournalEvent::EffectRecorded {
                        node: NodeId(path.to_string()),
                        effect_id: eid,
                        class: EffectClass::Pure,
                        input_hash: ih,
                        seq: 0,
                        output: recorded,
                        observation: None,
                    },
                )
                .await?;
                Ok(Ok(output))
            }
            Err(error) => Ok(Err(error.to_string())),
        }
    }
}
