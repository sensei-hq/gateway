//! Fan-out on the executor: the `Map` bounded-concurrency fan-out
//! (`run_map` + `run_map_child_modelcall`) and the `Consolidate` synthesis
//! (`run_consolidate`). Split out of `super` for readability; all are
//! `impl Executor` methods sharing its private state.

use std::collections::HashMap;
use std::sync::Arc;

use orchestrator_core::{
    Aggregation, EffectClass, GateSpec, JournalEvent, LoopBody, MapBody, NodeId, NodeKind,
    OrchestratorError, RunId, effect_id,
};

use super::human::LoopGateStep;
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
                    // SP-DATA-5: the Consolidate producer routes through the single
                    // metered chokepoint.
                    match self.dispatch_metered(&request, &fold.meter()).await {
                        Ok(Ok(response)) => {
                            // SP-4 s2: scrub the synthesis text via the shared chokepoint.
                            let output = self.model_output(&response);
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
                                    // SP-DATA-5: the Consolidate producer — the real usage
                                    // the provider reported, converted at the boundary.
                                    usage: response.usage.map(super::content::convert_usage),
                                },
                            )
                            .await?;
                            output
                        }
                        // SP-DATA-5: refused before spending — already journaled by
                        // `record_refusal`. A budget pause halts the Consolidate
                        // resumably; an unmetered call fails the node.
                        Ok(Err(refusal)) => {
                            return match self.record_refusal(run, &node.id, refusal).await? {
                                super::dispatch::RefusalKind::Paused(reason) => {
                                    Ok(NodeExec::Paused { reason })
                                }
                                super::dispatch::RefusalKind::Failed(message) => {
                                    Ok(NodeExec::Failed {
                                        message,
                                        output: None,
                                    })
                                }
                            };
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
                    .drive_agent(run, &node.id, agent_ref, &input, &[], fold, None, false)
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
                            .drive_agent(
                                run,
                                &NodeId(path.clone()),
                                agent_ref,
                                item,
                                &[],
                                fold,
                                None,
                                false,
                            )
                            .await
                        {
                            Ok(AgentStep::Completed(output)) => Ok(Ok(output)),
                            Ok(AgentStep::Failed(message)) => Ok(Err(message)),
                            // An in-doubt Mutation in a fanned-out Map child paused
                            // (its `RunPaused` is already journaled). Slice 4 cannot
                            // partially-complete a Map around an unresolved Intent,
                            // so carry the pause OUT of `join_all` as a distinct
                            // error and let `run_map` pause the WHOLE Map — never
                            // swallow it into the manifest and let the Map complete
                            // (that would journal `RunCompleted` over an unresolved
                            // Intent — a silent failure). Per-child partial-pause
                            // resumption lands with agent-child Mutations later.
                            Ok(AgentStep::Paused(reason)) => {
                                Err(OrchestratorError::MapChildPaused {
                                    node: NodeId(path.clone()),
                                    reason,
                                })
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

        // Fold children into the manifest, propagating any fatal error. A child
        // that PAUSED (in-doubt Mutation) pauses the whole Map — loud + resumable,
        // never a `RunCompleted` over an unresolved Intent (§7.3 no-silent-failure).
        let mut results = Vec::with_capacity(over.len());
        let mut ok = 0usize;
        let mut failed = 0usize;
        let mut paused: Option<String> = None;
        for (i, child) in collected {
            match child {
                Ok(Ok(value)) => {
                    ok += 1;
                    results.push(serde_json::json!({ "index": i, "ok": value }));
                }
                Ok(Err(message)) => {
                    failed += 1;
                    results.push(serde_json::json!({ "index": i, "error": message }));
                }
                Err(OrchestratorError::MapChildPaused { reason, .. }) => {
                    paused.get_or_insert(reason);
                }
                Err(fatal) => return Err(fatal),
            }
        }
        // A paused child means the Map cannot complete this round: return `Paused`
        // (marks the Map terminal-for-now + sets `RunOutcome.paused`, suppressing
        // `RunCompleted`). Completed siblings stay memoized; a resume re-drives the
        // Map, replays them, and re-reconciles the in-doubt child.
        if let Some(reason) = paused {
            return Ok(NodeExec::Paused { reason });
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

    /// Fail a `Loop` node: journal its `NodeFailed` — unless this exact failure is already
    /// recorded — and return the matching [`NodeExec::Failed`]`{ output: None }`.
    ///
    /// Colocating the append-then-return pairing keeps the invariant in ONE place, and
    /// the invariant is: **when `run_loop` returns `Failed`, this `Loop` has a
    /// `NodeFailed` in the journal, written exactly once.** All THREE of `run_loop`'s
    /// failure paths go through here — a body-iteration failure, a gate-AGENT failure and
    /// (SP-6 s4) a human-gate failure. (A `Map` aggregation failure carries its manifest
    /// out via `output: Some(..)`, so it does NOT use this helper.)
    ///
    /// **The guard is what makes "exactly once" true, and it is not cosmetic.** A human
    /// loop gate's verdict is TERMINAL — `run_human_loop_gate`'s step 0 reads it back
    /// forever — while the run it kills journals no `RunCompleted` and so stays resumable,
    /// so every later wake re-drives the iteration and re-reaches this helper. Appending
    /// unconditionally grew the journal by a row per wake for a run that can never
    /// progress: exactly the defect `gate_precheck` exists to prevent, one level further
    /// out (measured: three drives, three `NodeFailed(lp)` rows).
    ///
    /// It is guarded HERE, on the `Loop`'s own recorded failure, rather than by a flag
    /// carried out of the gate. The first shipped shape did the latter and the two claims
    /// came apart: the flag meant "this drive wrote the GATE's row" while the caller
    /// needed "the LOOP's row is missing", so a transient journal error between the two
    /// appends left the durable record showing the gate dead and the Loop untouched
    /// FOREVER — in-process `RunOutcome.failed` named the `Loop` while `torii run status`,
    /// anything folding `NodeFailed`, and the `on_node_failed` hook never heard of it.
    /// Reading the fold is self-healing instead: the next drive writes the missing row.
    ///
    /// **It asks whether THIS MESSAGE is already on the journal, not whether the node has
    /// SOME failure row, and the difference is an SP-6 s4 review finding.** The guard
    /// covers all three paths, but only the human-gate one is TERMINAL; the other two are
    /// explicitly not (a `ModelCall` or `Agent` whose provider died re-attempts on resume).
    /// `DriveState` is per-drive and `ready_nodes` consults no failure map, so a `Loop`
    /// that failed once is re-driven, can succeed, and can later fail for a wholly
    /// DIFFERENT reason. Presence-keyed, the guard suppressed that new row, leaving the
    /// durable record attributing the run's death to a transient gateway error it had
    /// recovered from — the real cause surviving only in the gate's own row and in the
    /// in-process `RunOutcome`. "Self-healing: the next drive writes the missing row"
    /// covers a MISSING row, not a CHANGED one.
    ///
    /// **And it reads [`Fold::failure_messages`], not [`Fold::failed`], which is the second
    /// half of the same finding — found by breaking it.** The obvious repair was equality
    /// against `failure_for`, and that trades the defect for its mirror: `failed` is
    /// FIRST-wins by design, so once a stale cause sits there the new message never matches
    /// and the append fires on EVERY wake, unbounded. Measured, not reasoned about. The set
    /// answers the only question an idempotent append may ask.
    ///
    /// Guarded from both sides by
    /// `a_loop_that_dies_of_a_new_cause_does_not_keep_reporting_the_old_one` (the new cause
    /// lands, AND two further wakes append nothing) beside
    /// `a_dead_loop_gate_stops_appending_node_failed_rows_on_every_wake`.
    ///
    /// Neither read makes a terminality claim: the `Loop` fails on this drive's own verdict
    /// whatever either map says, and they decide only whether a row is written. That is why
    /// this is not a loosening of [`Fold::failed`]'s fence, which is about which kinds may
    /// treat a recorded failure as FINAL.
    pub(super) async fn fail_loop(
        &self,
        run: RunId,
        node: &NodeId,
        message: String,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        if !fold.has_failure_message(node, &message) {
            self.append(
                run,
                JournalEvent::NodeFailed {
                    node: node.clone(),
                    error: message.clone(),
                },
            )
            .await?;
        }
        Ok(NodeExec::Failed {
            message,
            output: None,
        })
    }

    /// Run a `Loop` node (§10.3): drive `body` at `"{loop}/{i}"` each iteration until
    /// `gate` says Stop or `max_iters` is reached. A leaf `ModelCall`/`Agent` body
    /// threads its answer TEXT forward as the next iteration's input (refine); a
    /// `Subgraph` body drives an authored graph fresh per iteration (no thread —
    /// each iteration re-runs over the same `input`); an `Expand` body plans+executes
    /// per iteration and threads its whole output (the sink map) as the next planner
    /// input. A graph-body
    /// Failed/Paused fails/pauses the Loop exactly like a leaf body. The `gate` is
    /// `Pure` (an SP-1 pure predicate over the iteration output, no journaling),
    /// `Agent` — a gate-agent driven over the output at `"{loop}/{i}/__gate__"`,
    /// whose journaled answer feeds the pure `stop_when` (a gate-agent Failed fails the
    /// Loop, a gate-agent Paused pauses it) — or `Human` — a PERSON picks from an
    /// enumerated menu at that same reserved path, once per iteration, and the chosen
    /// option's `stops` is the decision (SP-6 s4, [`Executor::run_human_loop_gate`]: a
    /// gate failure fails the Loop and a pending decision pauses it, exactly as the
    /// gate-agent's do; no chain is resolved, so the gate itself spends nothing).
    /// Cap-without-Stop
    /// completes best-effort (`converged: false`), never a bare fail; a body failure
    /// fails the Loop; a body pause pauses the Loop. Resume replays completed
    /// iterations (memo-hit, no re-spend) and recomputes the gate — a pure gate from
    /// the memoized output, a gate-agent from its memoized turns, a human gate from its
    /// journaled **`LoopGateSettled`** — so it stops at the same iteration.
    ///
    /// **From the SETTLEMENT, never from `LoopGateDecided`**, and this clause named the
    /// wrong event until the s4 whole-slice review. A decision folds LAST-wins so an
    /// operator can correct it *before the run resumes*; a settlement is the line after
    /// which "before" has passed, because the loop has already spent an iteration on the
    /// strength of the answer. Re-deriving the gate from the decision is not a rewording —
    /// it has to pass the CLOCK on the way, which is precisely how an already-honoured gate
    /// came to re-expire and retroactively kill converged runs (the Critical §4 records).
    /// See `run_human_loop_gate`'s step 1.
    ///
    /// The Loop's own
    /// `NodeStarted`/`NodeCompleted` are
    /// fold-guarded (like `run_map`) so a replayed completed Loop does not re-journal
    /// them.
    pub(super) async fn run_loop(
        &self,
        run: RunId,
        loop_node: &orchestrator_core::Node,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        let NodeKind::Loop {
            body,
            input,
            gate,
            max_iters,
        } = &loop_node.kind
        else {
            unreachable!("run_loop is only dispatched for a Loop node");
        };
        if !fold.started.contains(&loop_node.id) {
            self.append(
                run,
                JournalEvent::NodeStarted {
                    node: loop_node.id.clone(),
                },
            )
            .await?;
        }

        let mut current_input = input.clone();
        let mut last_output = serde_json::Value::Null;
        let mut converged = false;
        let mut ran = 0usize;
        for i in 0..*max_iters {
            let path = format!("{}/{}", loop_node.id.0, i);
            let output_res: Result<serde_json::Value, String> = match body {
                LoopBody::ModelCall { chain } => {
                    match self
                        .run_map_child_modelcall(run, &path, chain, &current_input, fold)
                        .await
                    {
                        Ok(inner) => inner,
                        // SP-DATA-5: a `ModelCall` body shares the Map child's pause
                        // channel (`MapChildPaused`). Pause the Loop — exactly like an
                        // `Agent` body's `AgentStep::Paused` below — rather than
                        // letting it escape `?` as a FATAL error, which would abort the
                        // whole run instead of leaving it resumable.
                        Err(OrchestratorError::MapChildPaused { reason, .. }) => {
                            return Ok(NodeExec::Paused { reason });
                        }
                        Err(fatal) => return Err(fatal),
                    }
                }
                LoopBody::Agent(agent_ref) => match self
                    .drive_agent(
                        run,
                        &NodeId(path.clone()),
                        agent_ref,
                        &current_input,
                        &[],
                        fold,
                        None,
                        false,
                    )
                    .await?
                {
                    AgentStep::Completed(o) => Ok(o),
                    AgentStep::Failed(m) => Err(m),
                    AgentStep::Paused(reason) => return Ok(NodeExec::Paused { reason }),
                },
                // A graph body: drive the authored graph fresh under `"{loop}/{i}"`
                // (its inner nodes journal there); the `Completed` sink map is this
                // iteration's output, a failure fails the Loop, a pause pauses it.
                LoopBody::Subgraph(g) => {
                    match self.drive_nested(run, "loop", &path, g, fold).await? {
                        NodeExec::Completed(o) => Ok(o),
                        NodeExec::Failed { message, .. } => Err(message),
                        NodeExec::Paused { reason } => return Ok(NodeExec::Paused { reason }),
                    }
                }
                // An Expand body: plan+execute a fresh graph under `"{loop}/{i}"` each
                // iteration (shared `drive_expand_with` — resume/cap/journal), the
                // `Completed` sink map is this iteration's output; a failure fails the
                // Loop, a pause pauses it.
                LoopBody::Expand { planner } => {
                    match self
                        .drive_expand_with(
                            run,
                            &NodeId(path.clone()),
                            &current_input,
                            planner,
                            fold,
                        )
                        .await?
                    {
                        NodeExec::Completed(o) => Ok(o),
                        NodeExec::Failed { message, .. } => Err(message),
                        NodeExec::Paused { reason } => return Ok(NodeExec::Paused { reason }),
                    }
                }
            };
            let output = match output_res {
                Ok(o) => o,
                Err(message) => {
                    let msg = format!("loop {:?} failed at iteration {i}: {message}", loop_node.id);
                    return self.fail_loop(run, &loop_node.id, msg, fold).await;
                }
            };
            ran = i + 1;
            last_output = output.clone();
            let stop = match gate {
                GateSpec::Pure(g) => g.should_stop(&output),
                // A gate-agent (§4.3/D3): drive it over this iteration's `output` at the
                // reserved path `"{loop}/{i}/__gate__"`, then apply the pure `stop_when`
                // to ITS answer (not the body output). The agent's own ReAct turns are
                // journaled/memoized — no separate gate journaling — so a resume replays
                // the identical Stop/Continue decision from the memo (no gateway re-call).
                GateSpec::Agent { agent, stop_when } => {
                    let gate_path = NodeId(format!("{path}/__gate__"));
                    match self
                        .drive_agent(run, &gate_path, agent, &output, &[], fold, None, false)
                        .await?
                    {
                        AgentStep::Completed(ans) => stop_when.should_stop(&ans),
                        // A gate-agent failure fails the Loop (like a body-iteration failure).
                        AgentStep::Failed(m) => {
                            let msg = format!(
                                "loop {:?} gate agent failed at iteration {i}: {m}",
                                loop_node.id
                            );
                            return self.fail_loop(run, &loop_node.id, msg, fold).await;
                        }
                        // A gate-agent pause (in-doubt Mutation / quota) pauses the Loop.
                        AgentStep::Paused(reason) => return Ok(NodeExec::Paused { reason }),
                    }
                }
                // SP-6 s4: a PERSON decides. Same reserved `"{loop}/{i}/__gate__"` path as
                // the gate-agent above, but no agent is driven at all —
                // `run_human_loop_gate` resolves no chain and never reaches the gateway, so
                // zero token spend is STRUCTURAL. That matters more here than anywhere else
                // in SP-6: the decision being made IS whether to spend more, so a gate that
                // itself cost tokens would be self-undermining.
                //
                // The iteration `output` is passed as the thing being JUDGED (it becomes
                // the question's `## Context`); the graph's `menu` is passed for the first
                // ask only — every later drive resolves the decision against the JOURNALED
                // copy. See `run_human_loop_gate`.
                GateSpec::Human { agent, menu } => {
                    let gate_path = NodeId(format!("{path}/__gate__"));
                    match self
                        .run_human_loop_gate(run, &gate_path, agent, menu, &output, fold)
                        .await?
                    {
                        LoopGateStep::Decided { stop } => stop,
                        // A gate failure fails the Loop, exactly as the gate-AGENT arm
                        // above does — no new outcome shape (AC10). An expired gate lands
                        // here, and so does a decision naming an option the published menu
                        // does not contain.
                        //
                        // A human gate's failure is TERMINAL, unlike the gate-agent's
                        // above: `run_human_loop_gate`'s step 0 reads it back forever,
                        // while the run it kills journals no `RunCompleted` and so stays
                        // resumable, so every later wake re-drives this iteration and
                        // re-reaches this arm. `fail_loop` is idempotent for exactly that
                        // reason — see its doc; this arm needs no special case, and the
                        // flag it used to carry to get one was a second claim that could
                        // (and did) come apart from the first.
                        LoopGateStep::Failed(message) => {
                            let msg = format!(
                                "loop {:?} human gate failed at iteration {i}: {message}",
                                loop_node.id
                            );
                            return self.fail_loop(run, &loop_node.id, msg, fold).await;
                        }
                        // Waiting on a person pauses the whole Loop, like every other body
                        // or gate pause. The `RunPaused` is already journaled, on the
                        // deadline the GATE recorded, so the SP-DATA-3 scheduler wakes this
                        // run at the SLA rather than immediately.
                        LoopGateStep::Paused(reason) => {
                            return Ok(NodeExec::Paused { reason });
                        }
                    }
                }
            };
            if stop {
                converged = true;
                break;
            }
            // Refine: feed this iteration's answer TEXT forward, in the shape the
            // body reads it — a `ModelCall` body's request is `payload["prompt"]`
            // (so wrap as `{prompt: text}`), an `Agent` body renders its input
            // directly (so pass the text string). Threading the raw `{model, text}`
            // object instead would leave a ModelCall body with no `prompt` (an
            // empty request every iteration) — a silent no-op refine. A `Subgraph`
            // body does not thread (each iteration is a fresh re-run over `input`);
            // an `Expand` body threads the whole output (its sink map) as the next
            // planner input.
            let text = output
                .get("text")
                .cloned()
                .unwrap_or_else(|| output.clone());
            current_input = match body {
                LoopBody::ModelCall { .. } => serde_json::json!({ "prompt": text }),
                LoopBody::Agent(_) => text,
                LoopBody::Subgraph(_) => current_input,
                LoopBody::Expand { .. } => output.clone(),
            };
        }

        let out = serde_json::json!({
            "iterations": ran,
            "converged": converged,
            "output": last_output,
        });
        if !fold.completed.contains(&loop_node.id) {
            self.append(
                run,
                JournalEvent::NodeCompleted {
                    node: loop_node.id.clone(),
                },
            )
            .await?;
        }
        Ok(NodeExec::Completed(out))
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
        // SP-DATA-5: the Map-item producer routes through the single metered chokepoint.
        match self.dispatch_metered(&request, &fold.meter()).await {
            Ok(Ok(response)) => {
                // SP-4 s2: scrub the Map-item model text via the shared chokepoint.
                let output = self.model_output(&response);
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
                        // SP-DATA-5: the Map-item producer — the real usage the
                        // provider reported, converted at the boundary.
                        usage: response.usage.map(super::content::convert_usage),
                    },
                )
                .await?;
                Ok(Ok(output))
            }
            // SP-DATA-5: refused before spending. A child has no pause channel in its
            // inner `Result` (that carries only a manifest error message), so a budget
            // pause rides OUT on `MapChildPaused` — the SAME signal an in-doubt
            // Agent-child Mutation uses, which `run_map` folds into a whole-Map pause
            // and `run_loop` into a Loop pause. Never swallowed into the manifest:
            // that would let the Map complete over an un-run child. An unmetered call
            // is an ordinary child failure and does land in the manifest.
            Ok(Err(refusal)) => {
                match self
                    .record_refusal(run, &NodeId(path.to_string()), refusal)
                    .await?
                {
                    super::dispatch::RefusalKind::Paused(reason) => {
                        Err(OrchestratorError::MapChildPaused {
                            node: NodeId(path.to_string()),
                            reason,
                        })
                    }
                    super::dispatch::RefusalKind::Failed(message) => Ok(Err(message)),
                }
            }
            Err(error) => Ok(Err(error.to_string())),
        }
    }
}
