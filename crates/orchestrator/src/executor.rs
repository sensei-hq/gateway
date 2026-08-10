//! The deterministic executor: drives a linear `ModelCall` graph through the
//! gateway, journaling every step so a crashed run can resume (Task 4).

use std::collections::HashMap;
use std::sync::Arc;

use gateway::Gateway;
use kernel::types::capability::Capability;
use kernel::types::request::{
    InferenceRequest, Message, MessageContent, MessageRole, Payload, ToolCall, ToolDefinition,
};
use orchestrator_core::{
    AgentDefinition, AgentRef, Aggregation, ContentRef, ContentStore, EffectClass, EffectId,
    EffectOutput, ExecutionJournal, Graph, JournalEvent, MapBody, NodeId, NodeKind,
    OrchestratorError, Registry, RunId, Seq, Snapshot, effect_id,
};
use sha2::{Digest, Sha256};

use crate::agent::prompt::{assemble_prompt, over_budget};
use crate::agent::tools::ToolRegistry;

/// The deterministic executor over a durable journal, wired to the gateway.
pub struct Executor {
    gateway: Arc<Gateway>,
    journal: Arc<dyn ExecutionJournal>,
    version: String,
    registry: Arc<Registry>,
    tools: Arc<ToolRegistry>,
    max_steps: usize,
    concurrency: usize,
    /// The content-addressed store (§7.4) an over-threshold effect output is
    /// split into. `None` (the default) means no CAS is wired, so every output
    /// stays inline in the journal (the slice-1/2 behavior); wire a shared store
    /// via [`with_content_store`](Self::with_content_store) to enable the split —
    /// shared across the crash/resume boundary so a resume reads blobs back.
    content: Option<Arc<dyn ContentStore>>,
    /// The serialized-byte size **above which** an effect output is stored in the
    /// `ContentStore` (as a [`ContentRef`]) instead of inline. Only consulted
    /// when a `content` store is wired.
    cas_threshold: usize,
}

/// The terminal outcome of a run: the nodes that completed, the first failure,
/// the nodes cascade-skipped by a failure (across hard edges), and each node's
/// memoized output. A run with a failure is not marked `RunCompleted` (it stays
/// resumable), but soft-dependents of the failure still run and appear in
/// `completed`.
#[derive(Debug, Default)]
pub struct RunOutcome {
    pub completed: Vec<NodeId>,
    pub failed: Option<(NodeId, String)>,
    pub skipped: Vec<NodeId>,
    pub outputs: HashMap<NodeId, serde_json::Value>,
}

/// The state folded from a journal on resume: the effect memo plus which nodes
/// have already been started/completed (so an Agent node's `NodeStarted`/
/// `NodeCompleted` are appended at most once across resumes).
#[derive(Default)]
struct Fold {
    /// Each effect's structural id → its recorded `(input_hash, output)`. The
    /// output is a ref-or-inline [`EffectOutput`]: folding stores it verbatim
    /// (no blob load); a node materializes lazily via [`Executor::materialize`]
    /// only when it replays the effect.
    memo: HashMap<EffectId, (String, EffectOutput)>,
    started: std::collections::HashSet<NodeId>,
    completed: std::collections::HashSet<NodeId>,
}

impl Executor {
    /// Build an executor over a gateway + journal, fencing every run it starts
    /// with `version` (recorded in `RunStarted`, checked on resume in Task 4).
    pub fn new(
        gateway: Arc<Gateway>,
        journal: Arc<dyn ExecutionJournal>,
        version: impl Into<String>,
    ) -> Self {
        Self {
            gateway,
            journal,
            version: version.into(),
            registry: Arc::new(Registry::default()),
            tools: Arc::new(ToolRegistry::default()),
            max_steps: 8,
            concurrency: 8,
            content: None,
            cas_threshold: 4096,
        }
    }

    /// Wire the content-addressed store (§7.4) that over-threshold effect outputs
    /// split into. Injected (not defaulted to a concrete impl) so the executor
    /// stays decoupled from any store crate, and so a resume can share the SAME
    /// store as the original run — the crash/resume seam the CAS blobs live in.
    pub fn with_content_store(mut self, content: Arc<dyn ContentStore>) -> Self {
        self.content = Some(content);
        self
    }

    /// Override the CAS split threshold (default 4 KiB): an effect output whose
    /// serialized size exceeds this is stored in the `ContentStore` and the
    /// journal carries a [`ContentRef`]; smaller outputs stay inline.
    pub fn with_cas_threshold(mut self, bytes: usize) -> Self {
        self.cas_threshold = bytes;
        self
    }

    /// Override the global fan-out concurrency cap (default 8) — the ceiling on
    /// how many `Map` children run at once (bounded by `min(map.concurrency,
    /// executor.concurrency)`).
    pub fn with_concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }

    /// Attach the agent registry an `Agent` node resolves its definition against.
    pub fn with_registry(mut self, registry: Arc<Registry>) -> Self {
        self.registry = registry;
        self
    }

    /// Attach the executable tool runtime an `Agent` node dispatches Pure calls to.
    pub fn with_tools(mut self, tools: Arc<ToolRegistry>) -> Self {
        self.tools = tools;
        self
    }

    /// Override the ReAct loop's max turns (default 8).
    pub fn with_max_steps(mut self, n: usize) -> Self {
        self.max_steps = n;
        self
    }

    /// Execute a fresh linear graph end-to-end: journal `RunStarted`, then drive
    /// every node with an empty memo (nothing has run yet).
    pub async fn run(&self, run: RunId, graph: &Graph) -> Result<RunOutcome, OrchestratorError> {
        graph.validate_dag()?;
        self.append(
            run,
            JournalEvent::RunStarted {
                version: self.version.clone(),
            },
        )
        .await?;
        self.drive(run, graph, &Fold::default()).await
    }

    /// Resume (or freshly start) a run from its durable journal — the headline
    /// crash/resume path that never re-spends tokens on already-recorded
    /// effects. Load the journal and:
    ///
    /// - **empty journal** ⇒ nothing to resume, delegate to [`run`](Self::run);
    /// - **version fence** ⇒ if the recorded `RunStarted.version` differs from
    ///   this executor's, refuse with [`VersionFenceMismatch`] (never resume a
    ///   run authored by a different executor version);
    /// - **already terminal** (a `RunCompleted` is present) ⇒ return the folded
    ///   outcome WITHOUT re-driving, so no second `RunCompleted` is appended;
    /// - **partial** ⇒ fold every `EffectRecorded` into the memo and hand off to
    ///   [`drive`](Self::drive), which replays the completed prefix (no gateway
    ///   call, no duplicate journal events), runs the tail, and appends
    ///   `RunCompleted` once.
    ///
    /// [`VersionFenceMismatch`]: OrchestratorError::VersionFenceMismatch
    pub async fn start(&self, run: RunId, graph: &Graph) -> Result<RunOutcome, OrchestratorError> {
        graph.validate_dag()?;
        let events = self
            .journal
            .load(run)
            .await
            .map_err(OrchestratorError::Journal)?;
        if events.is_empty() {
            // Nothing journaled → a fresh run (appends `RunStarted` itself).
            return self.run(run, graph).await;
        }

        // Version fence: the first recorded `RunStarted.version` must match ours.
        if let Some(recorded) = events.iter().find_map(|(_, e)| match e {
            JournalEvent::RunStarted { version } => Some(version.clone()),
            _ => None,
        }) && recorded != self.version
        {
            return Err(OrchestratorError::VersionFenceMismatch {
                recorded,
                current: self.version.clone(),
            });
        }

        // Fold the journal in `Seq` order: memoize every recorded effect and, for
        // an already-terminal run, reconstruct the completed outcome.
        let terminal = events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted));
        let mut fold = Fold::default();
        let mut outcome = RunOutcome::default();
        // The last recorded output per node, kept as a **ref** (never loaded) —
        // the fold reads refs without deserializing blobs (§7.4). Only a terminal
        // resume materializes these, lazily and bounded (one per node); the
        // non-terminal path re-materializes on demand while `drive` replays.
        let mut node_last_output: HashMap<NodeId, EffectOutput> = HashMap::new();
        for (_, event) in &events {
            match event {
                JournalEvent::EffectRecorded {
                    node,
                    effect_id,
                    input_hash,
                    output,
                    ..
                } => {
                    fold.memo
                        .insert(effect_id.clone(), (input_hash.clone(), output.clone()));
                    node_last_output.insert(node.clone(), output.clone());
                }
                JournalEvent::NodeStarted { node } => {
                    fold.started.insert(node.clone());
                }
                JournalEvent::NodeCompleted { node } => {
                    fold.completed.insert(node.clone());
                    outcome.completed.push(node.clone());
                }
                _ => {}
            }
        }

        if terminal {
            // Already done: return the folded outcome; do NOT re-drive (which
            // would append a second `RunCompleted`). Materialize each node's final
            // output lazily from its folded ref (inline value, or a CAS blob) —
            // the only place the terminal fold touches content, bounded to one
            // read per node.
            for (node, output) in &node_last_output {
                let value = self.materialize(output).await?;
                outcome.outputs.insert(node.clone(), value);
            }
            // Then project each Agent node's folded output — the RAW final
            // model-turn effect (`{model, text, tool_calls}`) — down to the
            // canonical `{model, text}` that a fresh `run` and a non-terminal
            // resume return from `AgentStep::Completed`, so a completed Agent node
            // yields an identical JSON shape on every completion path (design §4).
            // This is a pure projection of the already-materialized outputs — no
            // re-drive, no append. `ModelCall` nodes already store the canonical
            // shape and are left untouched.
            for node in &graph.nodes {
                if let NodeKind::Agent { .. } = &node.kind
                    && let Some(output) = outcome.outputs.get(&node.id).cloned()
                {
                    let model = output
                        .get("model")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    let text = output.get("text").cloned().unwrap_or_default();
                    outcome.outputs.insert(
                        node.id.clone(),
                        serde_json::json!({ "model": model, "text": text }),
                    );
                }
            }
            return Ok(outcome);
        }

        // Resume the tail: `drive`'s memo branch replays the completed prefix
        // (no gateway call, no new `EffectRecorded`) and finishes the run.
        self.drive(run, graph, &fold).await
    }

    /// Shared node loop for both `run` (an empty [`Fold`]) and `start` (a `Fold`
    /// folded from the journal, Task 4). The `fold: &Fold` carries three sets:
    ///
    /// - `fold.memo` maps each effect's structural [`EffectId`] to its recorded
    ///   `(input_hash, output)`. A hit whose input-hash matches replays the
    ///   recorded output with NO gateway call and NO new `EffectRecorded` (it is
    ///   already journaled); a hit whose input-hash differs is a determinism
    ///   violation (the graph changed under a resume) — halt; a miss executes the
    ///   node against the gateway and journals it.
    /// - `fold.started` / `fold.completed` name the nodes whose `NodeStarted` /
    ///   `NodeCompleted` are already journaled, so an `Agent` node's ReAct loop
    ///   appends each at most once across resumes (a `ModelCall` node runs
    ///   atomically per drive and does not consult them).
    ///
    /// For a fresh `run` the fold is empty, so every node executes; the memo
    /// branches exist for Task 4's resume and are reachable code.
    ///
    /// **Scheduling (slice 3):** instead of iterating nodes in declaration
    /// order, the executor advances the graph in **rounds** of *ready* nodes. A
    /// node is ready when every `Hard` dep has `Completed` and every `Soft` dep
    /// is `terminal` (§3.2). Ready nodes in a round are dispatched in
    /// declaration order (deterministic); after the round the ready set is
    /// recomputed. A **linear** graph has exactly one ready node per round, so
    /// this reproduces the slice-1/2 sequential order byte-for-byte. A `Failed`
    /// node cascade-skips its hard-dependents (§3.3) but does NOT halt the run —
    /// soft-dependent branches still run; the failure suppresses `RunCompleted`,
    /// so the run stays resumable (the slice-1/2 contract on a linear graph,
    /// where the failure has no downstream to skip).
    async fn drive(
        &self,
        run: RunId,
        graph: &Graph,
        fold: &Fold,
    ) -> Result<RunOutcome, OrchestratorError> {
        use std::collections::HashSet;

        let mut outcome = RunOutcome::default();
        let mut completed: HashSet<NodeId> = HashSet::new();
        let mut terminal: HashSet<NodeId> = HashSet::new();

        loop {
            // Ready = not-yet-terminal nodes whose Hard deps have completed and
            // Soft deps are terminal, in declaration order (deterministic).
            let ready: Vec<(usize, &orchestrator_core::Node)> = graph
                .nodes
                .iter()
                .enumerate()
                .filter(|(_, node)| !terminal.contains(&node.id))
                .filter(|(_, node)| {
                    node.deps.iter().all(|dep| match dep.kind {
                        orchestrator_core::EdgeKind::Hard => completed.contains(&dep.on),
                        orchestrator_core::EdgeKind::Soft => terminal.contains(&dep.on),
                    })
                })
                .collect();
            if ready.is_empty() {
                break;
            }

            for (index, node) in ready {
                // The immutable borrow of `outcome.outputs` (a Consolidate reads
                // its Map's result from it) ends when the future resolves, before
                // the match body mutates `outcome`.
                let exec = self
                    .run_node(run, index, node, fold, &outcome.outputs)
                    .await?;
                match exec {
                    NodeExec::Completed(output) => {
                        outcome.outputs.insert(node.id.clone(), output);
                        outcome.completed.push(node.id.clone());
                        completed.insert(node.id.clone());
                        terminal.insert(node.id.clone());
                    }
                    NodeExec::Failed { message, output } => {
                        // Carry a node's failure output (a Map's manifest) into
                        // the outcome — never dropped (§3.4).
                        if let Some(output) = output {
                            outcome.outputs.insert(node.id.clone(), output);
                        }
                        // Record the first failure; mark the node terminal, then
                        // cascade-skip its hard-dependents. The run does NOT halt:
                        // soft-dependents of the failure still become ready and
                        // run (§3.3). The failure suppresses `RunCompleted` below,
                        // so the run stays resumable.
                        if outcome.failed.is_none() {
                            outcome.failed = Some((node.id.clone(), message));
                        }
                        terminal.insert(node.id.clone());
                        self.cascade_skip_from(run, graph, &node.id, &mut terminal, &mut outcome)
                            .await?;
                    }
                }
            }

            // Round boundary (§5.2): checkpoint the run's progress to the snapshot
            // store, OUT-OF-BAND (no journal event, so the control-flow log stays
            // byte-identical). A resume seeds from the latest snapshot and folds
            // only the tail. Written even on a fresh `run` — harmlessly unused
            // unless the run later crashes and resumes.
            self.write_snapshot(run, &outcome).await?;
        }
        // A run with any failure is not marked complete — it stays resumable
        // (the slice-1/2 contract), even though soft-dependent branches ran.
        if outcome.failed.is_none() {
            self.append(run, JournalEvent::RunCompleted).await?;
        }
        Ok(outcome)
    }

    /// Write a round-boundary [`Snapshot`] of the current outcome to the journal's
    /// snapshot store (§5.2). Its `seq` is the current max journal `Seq` — the
    /// boundary a resume folds past; each completed node's output is carried as a
    /// ref-or-inline [`EffectOutput`] (large ones split into the CAS, keeping the
    /// snapshot lean). A backend without snapshot support no-ops (trait default).
    async fn write_snapshot(
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

    /// Cascade-skip: mark every not-yet-terminal node that `Hard`-depends on
    /// `origin` as `Skipped` — journaling `NodeSkipped`, adding it to the
    /// terminal set and to `RunOutcome.skipped` — and recurse into ITS
    /// hard-dependents (§3.3). `Soft` edges never cascade, so a soft-dependent
    /// of a failed/skipped node is left runnable. Deterministic in graph
    /// declaration order; each node is skipped at most once (guarded by the
    /// terminal set).
    async fn cascade_skip_from(
        &self,
        run: RunId,
        graph: &Graph,
        origin: &NodeId,
        terminal: &mut std::collections::HashSet<NodeId>,
        outcome: &mut RunOutcome,
    ) -> Result<(), OrchestratorError> {
        let mut frontier = vec![origin.clone()];
        while let Some(current) = frontier.pop() {
            for node in &graph.nodes {
                if terminal.contains(&node.id) {
                    continue;
                }
                let hard_on_current = node
                    .deps
                    .iter()
                    .any(|dep| dep.kind == orchestrator_core::EdgeKind::Hard && dep.on == current);
                if hard_on_current {
                    self.append(
                        run,
                        JournalEvent::NodeSkipped {
                            node: node.id.clone(),
                        },
                    )
                    .await?;
                    terminal.insert(node.id.clone());
                    outcome.skipped.push(node.id.clone());
                    frontier.push(node.id.clone());
                }
            }
        }
        Ok(())
    }

    /// Execute one node to a terminal result. `index` is the node's declaration
    /// position, which keys a `ModelCall`'s structural effect id
    /// (`effect_id("", 0, index)` — the slice-1 scheme, preserved). A memoized
    /// `ModelCall` replays with no gateway call and no new journal event; a
    /// live one journals `NodeStarted → EffectRecorded → NodeCompleted`. An
    /// `Agent` node delegates to [`drive_agent`](Self::drive_agent), which owns
    /// its own per-turn journaling. A determinism violation propagates as `Err`
    /// (halting the run before any gateway call). `prior_outputs` carries the
    /// outputs of already-completed nodes this round advances past — a
    /// `Consolidate` reads its Map's result from it.
    async fn run_node(
        &self,
        run: RunId,
        index: usize,
        node: &orchestrator_core::Node,
        fold: &Fold,
        prior_outputs: &HashMap<NodeId, serde_json::Value>,
    ) -> Result<NodeExec, OrchestratorError> {
        match &node.kind {
            NodeKind::ModelCall { chain, payload } => {
                let eid = effect_id("", 0, index);
                let ih = input_hash(chain, payload)?;

                if let Some((recorded_ih, output)) = fold.memo.get(&eid) {
                    if recorded_ih != &ih {
                        return Err(OrchestratorError::DeterminismViolation {
                            node: node.id.clone(),
                            effect_id: eid,
                        });
                    }
                    // Memoized: replay the recorded output — no gateway call, no
                    // new `EffectRecorded` (it is already in the journal). The
                    // output is materialized lazily (inline value, or a CAS blob).
                    return Ok(NodeExec::Completed(self.materialize(output).await?));
                }

                self.append(
                    run,
                    JournalEvent::NodeStarted {
                        node: node.id.clone(),
                    },
                )
                .await?;

                let request = build_request(chain, payload);
                match self.gateway.execute(&request).await {
                    Ok(response) => {
                        let output = serde_json::json!({
                            "model": response.model,
                            "text": response.content.clone().unwrap_or_default(),
                        });
                        // `EffectRecorded.seq` is advisory: `append` assigns the
                        // authoritative outer `Seq`, and the resume fold orders
                        // events by that outer `(Seq, event)` from `load` — never by
                        // this in-event field — so it is set to 0 rather than the
                        // (circular) value `append` would return.
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
                            },
                        )
                        .await?;
                        self.append(
                            run,
                            JournalEvent::NodeCompleted {
                                node: node.id.clone(),
                            },
                        )
                        .await?;
                        Ok(NodeExec::Completed(output))
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
                        Ok(NodeExec::Failed {
                            message,
                            output: None,
                        })
                    }
                }
            }
            NodeKind::Agent { agent, input } => {
                match self.drive_agent(run, node, agent, input, fold).await? {
                    AgentStep::Completed(output) => Ok(NodeExec::Completed(output)),
                    AgentStep::Failed(message) => Ok(NodeExec::Failed {
                        message,
                        output: None,
                    }),
                }
            }
            NodeKind::Map {
                body,
                over,
                concurrency,
                aggregation,
            } => {
                self.run_map(run, node, body, over, *concurrency, aggregation, fold)
                    .await
            }
            NodeKind::Consolidate {
                over,
                min_viable,
                body,
            } => {
                self.run_consolidate(run, node, over, *min_viable, body, prior_outputs, fold)
                    .await
            }
        }
    }

    /// Run a `Consolidate` node (§3.5): read the successful results of its `over`
    /// Map from `prior_outputs`, gate on `min_viable` (fewer survivors ⇒
    /// `ConsolidateStarved`, a loud halt — never a silent empty synthesis), then
    /// run `body` **once** over the collected survivors and return its output. A
    /// determinism violation / journal-write error aborts as `Err`.
    #[allow(clippy::too_many_arguments)]
    async fn run_consolidate(
        &self,
        run: RunId,
        node: &orchestrator_core::Node,
        over: &NodeId,
        min_viable: usize,
        body: &MapBody,
        prior_outputs: &HashMap<NodeId, serde_json::Value>,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
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

        // Run `body` once over the survivors. The structural effect id nests
        // under this node's own path (`effect_id(node, 0, 0)`), so a resume
        // memoizes the synthesis without re-spending. `NodeStarted`/`NodeCompleted`
        // are guarded via the fold (resume-safe, like `run_map`/`drive_agent`).
        if !fold.started.contains(&node.id) {
            self.append(
                run,
                JournalEvent::NodeStarted {
                    node: node.id.clone(),
                },
            )
            .await?;
        }
        let input = serde_json::json!({ "results": survivors });
        let output = match body {
            MapBody::ModelCall { chain } => {
                let eid = effect_id(&node.id.0, 0, 0);
                let payload = serde_json::json!({ "prompt": input.to_string() });
                let ih = input_hash(chain, &payload)?;

                // Memoized on resume: replay the recorded synthesis — no gateway
                // call, no re-append. A hash mismatch is a determinism violation.
                if let Some((recorded_ih, recorded)) = fold.memo.get(&eid) {
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
    #[allow(clippy::too_many_arguments)]
    async fn run_map(
        &self,
        run: RunId,
        map_node: &orchestrator_core::Node,
        body: &MapBody,
        over: &[serde_json::Value],
        concurrency: usize,
        aggregation: &Aggregation,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
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
                    },
                )
                .await?;
                Ok(Ok(output))
            }
            Err(error) => Ok(Err(error.to_string())),
        }
    }

    /// Split an effect output for the journal (§7.4): if a `ContentStore` is
    /// wired and the serialized output exceeds `cas_threshold`, `put` the bytes
    /// into the CAS and return a [`ContentRef`] (identical content dedupes to one
    /// digest); otherwise carry the value inline. Keeps the durable journal a
    /// lean control-flow log while large payloads live once in the CAS.
    async fn split_output(
        &self,
        output: &serde_json::Value,
    ) -> Result<EffectOutput, OrchestratorError> {
        // No CAS wired ⇒ everything stays inline (the slice-1/2 behavior).
        let Some(content) = &self.content else {
            return Ok(EffectOutput::Inline(output.clone()));
        };
        let bytes = serde_json::to_vec(output)?;
        if bytes.len() <= self.cas_threshold {
            return Ok(EffectOutput::Inline(output.clone()));
        }
        // Over threshold: store the bytes in the CAS (identical content dedupes
        // to one digest) and carry a lightweight ref in the journal.
        let digest = content.put(&bytes).await?;
        Ok(EffectOutput::Ref(ContentRef {
            digest,
            size: bytes.len(),
            summary: None,
        }))
    }

    /// Materialize a recorded [`EffectOutput`] into its value: an inline value is
    /// cloned; a [`ContentRef`] is fetched lazily from the `ContentStore` and
    /// deserialized. A ref with no store wired, or a digest miss, is loud
    /// ([`ContentDigestMiss`](OrchestratorError::ContentDigestMiss)) — never a
    /// silent empty value.
    async fn materialize(
        &self,
        out: &EffectOutput,
    ) -> Result<serde_json::Value, OrchestratorError> {
        match out {
            EffectOutput::Inline(value) => Ok(value.clone()),
            EffectOutput::Ref(r) => {
                let store = self
                    .content
                    .as_ref()
                    .ok_or_else(|| OrchestratorError::ContentDigestMiss(r.digest.0.clone()))?;
                let bytes = store.get(&r.digest).await?;
                Ok(serde_json::from_slice(&bytes)?)
            }
        }
    }

    /// Append one event, mapping a journal-backend error to a fatal
    /// `OrchestratorError::Journal` (strict — a journal write failure aborts the
    /// run; it is never swallowed). Returns the authoritative `Seq`.
    async fn append(&self, run: RunId, event: JournalEvent) -> Result<Seq, OrchestratorError> {
        self.journal
            .append(run, event)
            .await
            .map_err(OrchestratorError::Journal)
    }
}

/// The terminal result of one `Agent` node: a completed output, or a node-level
/// failure (budget/max-steps/gateway/tool) already journaled as `NodeFailed`.
enum AgentStep {
    Completed(serde_json::Value),
    Failed(String),
}

/// The terminal result of one scheduled node (any kind): its completed output,
/// or a node-level failure already journaled as `NodeFailed`. A determinism
/// violation is not a `NodeExec` — it propagates as `Err` and halts the run.
///
/// `Failed.output` carries a node's result even on failure — a `Map` that fails
/// its aggregation still attaches its failure manifest so it reaches
/// `RunOutcome`, never dropped (§3.4). `ModelCall`/`Agent` failures carry `None`.
enum NodeExec {
    Completed(serde_json::Value),
    Failed {
        message: String,
        output: Option<serde_json::Value>,
    },
}

impl Executor {
    /// Run one `Agent` node's ReAct loop. Each turn is a Pure `ModelCall` effect
    /// (iteration-aware id `effect_id(node.id, turn, 0)`); each Pure tool call is a
    /// Pure effect (`effect_id(node.id, turn, k+1)`). Memoized turns/tools replay
    /// from the journal with no gateway call and no re-execution (resume without
    /// re-spend); an input-hash mismatch halts with `DeterminismViolation`.
    async fn drive_agent(
        &self,
        run: RunId,
        node: &orchestrator_core::Node,
        agent_ref: &AgentRef,
        input: &serde_json::Value,
        fold: &Fold,
    ) -> Result<AgentStep, OrchestratorError> {
        let agent: &AgentDefinition = self
            .registry
            .agent(&agent_ref.0)
            .ok_or_else(|| OrchestratorError::UnknownAgent(agent_ref.0.clone()))?;
        let (system, tools) = assemble_prompt(&self.registry, agent)?;
        let chain = agent.chain.clone();
        let min_win = self.gateway.min_context_window(&chain).await;

        let mut messages: Vec<Message> =
            vec![Message::text(MessageRole::User, render_input(input))];
        let mut node_started = fold.started.contains(&node.id);

        for turn in 0..self.max_steps {
            let eid = effect_id(&node.id.0, turn as u64, 0);
            let ih = agent_input_hash(&chain, &system, &messages, &tools)?;

            // Reuse a memoized turn (resume): no gateway call, no re-append.
            let turn_output = if let Some((recorded_ih, output)) = fold.memo.get(&eid) {
                if recorded_ih != &ih {
                    return Err(OrchestratorError::DeterminismViolation {
                        node: node.id.clone(),
                        effect_id: eid,
                    });
                }
                self.materialize(output).await?
            } else {
                // Live turn: budget → NodeStarted (once) → gateway → EffectRecorded.
                if over_budget(min_win, &system, &messages, &tools) {
                    let est = est_prompt_tokens(&system, &messages, &tools);
                    let err = OrchestratorError::PromptOverBudget {
                        node: node.id.clone(),
                        turn,
                        est,
                        min_win: min_win.unwrap_or(0),
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
                    return Ok(AgentStep::Failed(message));
                }
                if !node_started {
                    self.append(
                        run,
                        JournalEvent::NodeStarted {
                            node: node.id.clone(),
                        },
                    )
                    .await?;
                    node_started = true;
                }
                let request = build_chat_request(&chain, &system, messages.clone(), tools.clone());
                match self.gateway.execute(&request).await {
                    Ok(response) => {
                        let output = serde_json::json!({
                            "model": response.model,
                            "text": response.content.clone().unwrap_or_default(),
                            "tool_calls": response.tool_calls,
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
                        return Ok(AgentStep::Failed(message));
                    }
                }
            };

            let tool_calls: Vec<ToolCall> = serde_json::from_value(
                turn_output
                    .get("tool_calls")
                    .cloned()
                    .unwrap_or(serde_json::json!([])),
            )?;
            if tool_calls.is_empty() {
                // Final answer.
                if !fold.completed.contains(&node.id) {
                    self.append(
                        run,
                        JournalEvent::NodeCompleted {
                            node: node.id.clone(),
                        },
                    )
                    .await?;
                }
                let text = turn_output.get("text").cloned().unwrap_or_default();
                let model = turn_output
                    .get("model")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                return Ok(AgentStep::Completed(
                    serde_json::json!({ "model": model, "text": text }),
                ));
            }

            // Execute (or replay) each tool call, then extend the transcript.
            let assistant_text = turn_output
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            messages.push(Message {
                role: MessageRole::Assistant,
                content: MessageContent::Text {
                    text: assistant_text,
                },
                tool_calls: tool_calls.clone(),
                attachments: Vec::new(),
            });
            for (k, call) in tool_calls.iter().enumerate() {
                let teid = effect_id(&node.id.0, turn as u64, k + 1);
                let args: serde_json::Value =
                    serde_json::from_str(&call.arguments).unwrap_or(serde_json::Value::Null);
                let tih = tool_input_hash(&call.name, &call.arguments);
                let result = if let Some((recorded_ih, output)) = fold.memo.get(&teid) {
                    if recorded_ih != &tih {
                        return Err(OrchestratorError::DeterminismViolation {
                            node: node.id.clone(),
                            effect_id: teid,
                        });
                    }
                    self.materialize(output).await?
                } else {
                    match self.tools.execute(&call.name, args) {
                        Ok(result) => {
                            let recorded = self.split_output(&result).await?;
                            self.append(
                                run,
                                JournalEvent::EffectRecorded {
                                    node: node.id.clone(),
                                    effect_id: teid,
                                    class: EffectClass::Pure,
                                    input_hash: tih,
                                    seq: 0,
                                    output: recorded,
                                },
                            )
                            .await?;
                            result
                        }
                        Err(err) => {
                            let message = err.to_string();
                            self.append(
                                run,
                                JournalEvent::NodeFailed {
                                    node: node.id.clone(),
                                    error: message.clone(),
                                },
                            )
                            .await?;
                            return Ok(AgentStep::Failed(message));
                        }
                    }
                };
                messages.push(Message::tool_result(call.id.clone(), result.to_string()));
            }
        }

        // Ran out of steps without a final answer.
        let err = OrchestratorError::AgentMaxStepsExceeded {
            node: node.id.clone(),
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
        Ok(AgentStep::Failed(message))
    }
}

/// Structural content hash of a node's `(chain, payload)` — the determinism key
/// checked against the memo on resume: `sha256_hex("{chain}|{json(payload)}")`.
fn input_hash(chain: &str, payload: &serde_json::Value) -> Result<String, OrchestratorError> {
    let serialized = serde_json::to_string(payload)?;
    let mut hasher = Sha256::new();
    hasher.update(format!("{chain}|{serialized}").as_bytes());
    Ok(format!("{:x}", hasher.finalize()))
}

/// Compile a `ModelCall`'s `(chain, payload)` into a plain single-turn chat
/// [`InferenceRequest`]: `TextChat` over the named chain, fallback enabled, all
/// other addressing/identity fields defaulted. The payload's `"prompt"` string
/// (if present) becomes the sole user message.
fn build_request(chain: &str, payload: &serde_json::Value) -> InferenceRequest {
    let prompt = payload
        .get("prompt")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    InferenceRequest {
        capability: Capability::TextChat,
        model: None,
        router: None,
        chain: Some(chain.to_string()),
        payload: Payload::Chat {
            messages: vec![Message::text(MessageRole::User, prompt)],
            system: None,
            max_tokens: None,
            temperature: None,
            tools: Vec::new(),
        },
        budget: None,
        auth: None,
        panel: None,
        consensus: None,
        allow_fallback: true,
        credentials: Default::default(),
    }
}

/// Render an agent node's JSON `input` into user-message text: a JSON string
/// passes through; any other value is serialized (deterministic — feeds the hash).
fn render_input(input: &serde_json::Value) -> String {
    match input {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Compile one ReAct turn into a chat `InferenceRequest` (system + transcript +
/// tools) over the agent's chain. `budget: None` — cost budgeting is the gateway's
/// dormant axis in slice 2 (see the design); this request carries only window-fit.
fn build_chat_request(
    chain: &str,
    system: &str,
    messages: Vec<Message>,
    tools: Vec<ToolDefinition>,
) -> InferenceRequest {
    InferenceRequest {
        capability: Capability::TextChat,
        model: None,
        router: None,
        chain: Some(chain.to_string()),
        payload: Payload::Chat {
            messages,
            system: Some(system.to_string()),
            max_tokens: None,
            temperature: None,
            tools,
        },
        budget: None,
        auth: None,
        panel: None,
        consensus: None,
        allow_fallback: true,
        credentials: Default::default(),
    }
}

/// Determinism key for a ReAct turn: `sha256_hex(chain | system | messages | tools)`.
fn agent_input_hash(
    chain: &str,
    system: &str,
    messages: &[Message],
    tools: &[ToolDefinition],
) -> Result<String, OrchestratorError> {
    let messages = serde_json::to_string(messages)?;
    let tools = serde_json::to_string(tools)?;
    let mut hasher = Sha256::new();
    hasher.update(format!("{chain}|{system}|{messages}|{tools}").as_bytes());
    Ok(format!("{:x}", hasher.finalize()))
}

/// Determinism key for a Pure tool call: `sha256_hex(name | arguments)`.
fn tool_input_hash(name: &str, arguments: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(format!("{name}|{arguments}").as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Estimate a prompt's tokens (for the over-budget diagnostic's `est`).
fn est_prompt_tokens(system: &str, messages: &[Message], tools: &[ToolDefinition]) -> usize {
    use crate::agent::prompt::est_tokens;
    let mut est = est_tokens(system);
    for m in messages {
        est += est_tokens(m.content.as_text());
    }
    for t in tools {
        est += est_tokens(&t.name)
            + t.description.as_deref().map(est_tokens).unwrap_or(0)
            + est_tokens(&t.input_schema.to_string());
    }
    est
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        content_gated_gateway, demo_reference_gateway, failing_after_gateway, final_response,
        recording_gateway, scripted_gateway, tool_call_response,
    };
    use orchestrator_core::{Dep, Graph, JournalError, Node, NodeId, NodeKind};
    use orchestrator_store::InMemoryJournal;

    use crate::agent::tools::{Calc, Tool, ToolRegistry};
    use orchestrator_core::{AgentDefinition, AgentRef, Registry};
    use std::sync::Arc;

    fn agent_def(chain: &str) -> AgentDefinition {
        AgentDefinition {
            name: "a".into(),
            area: "research".into(),
            kind: "reasoning".into(),
            chain: chain.into(),
            tools: vec![],
            skills: vec![],
            system_prompt: "SYS".into(),
        }
    }

    /// A demo registry/executor: one agent "a" on the recording chain "c".
    fn agent_registry(chain: &str) -> Arc<Registry> {
        Arc::new(Registry::default().with_agent(agent_def(chain)))
    }

    fn agent_node(id: &str, agent: &str, input: &str) -> Node {
        Node {
            id: NodeId(id.into()),
            kind: NodeKind::Agent {
                agent: AgentRef(agent.into()),
                input: serde_json::json!(input),
            },
            deps: vec![],
        }
    }

    fn tool_agent_registry() -> Arc<Registry> {
        // The core `Registry` needs the tool's *schema* (`ToolSpec`, via
        // `Tool::spec()`) to compile it into the prompt (`assemble_prompt`);
        // the *executable* side is the separate `ToolRegistry` (`calc_tools`).
        Arc::new(
            Registry::default()
                .with_agent(AgentDefinition {
                    tools: vec!["calc".into()],
                    ..agent_def("c")
                })
                .with_tool(Calc.spec()),
        )
    }
    fn calc_tools() -> Arc<ToolRegistry> {
        Arc::new(ToolRegistry::default().with_tool(Arc::new(Calc)))
    }

    #[tokio::test]
    async fn agent_react_loop_executes_a_pure_tool_and_feeds_the_result_back() {
        let (gateway, calls) = scripted_gateway(vec![
            tool_call_response("t1", "calc", "{\"op\":\"add\",\"a\":2,\"b\":3}"),
            final_response("the answer is 5"),
        ])
        .await;
        let journal = InMemoryJournal::new();
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
            .with_registry(tool_agent_registry())
            .with_tools(calc_tools());

        let n1 = NodeId("n1".into());
        let graph = Graph {
            nodes: vec![agent_node("n1", "a", "add 2 and 3")],
        };
        let run = RunId(uuid::Uuid::new_v4());
        let outcome = exec.run(run, &graph).await.expect("run");

        assert!(outcome.failed.is_none(), "{:?}", outcome.failed);
        assert_eq!(outcome.outputs[&n1]["text"], "the answer is 5");
        assert_eq!(calls.lock().unwrap().len(), 2, "two model turns");

        let kinds: Vec<String> = journal
            .load(run)
            .await
            .unwrap()
            .iter()
            .map(|(_, e)| label(e))
            .collect();
        assert_eq!(
            kinds,
            vec![
                "RunStarted",
                "NodeStarted(n1)",
                "EffectRecorded(n1)",
                "EffectRecorded(n1)", // turn-0 model + calc
                "EffectRecorded(n1)", // turn-1 model (final)
                "NodeCompleted(n1)",
                "RunCompleted",
            ]
        );
    }

    #[tokio::test]
    async fn agent_rejects_a_non_pure_tool_loudly() {
        let (gateway, _calls) =
            scripted_gateway(vec![tool_call_response("t1", "read", "{}")]).await;
        let journal = InMemoryJournal::new();
        struct Reader;
        impl crate::agent::tools::Tool for Reader {
            fn spec(&self) -> orchestrator_core::ToolSpec {
                orchestrator_core::ToolSpec {
                    name: "read".into(),
                    description: None,
                    input_schema: serde_json::json!({}),
                    effect_class: orchestrator_core::EffectClass::Observation,
                }
            }
            fn call(&self, _a: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
                Ok(serde_json::json!({}))
            }
        }
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
            .with_registry(Arc::new(
                Registry::default()
                    .with_agent(AgentDefinition {
                        tools: vec!["read".into()],
                        ..agent_def("c")
                    })
                    .with_tool(Reader.spec()),
            ))
            .with_tools(Arc::new(
                ToolRegistry::default().with_tool(Arc::new(Reader)),
            ));
        let graph = Graph {
            nodes: vec![agent_node("n1", "a", "read")],
        };
        let outcome = exec
            .run(RunId(uuid::Uuid::new_v4()), &graph)
            .await
            .expect("outcome");
        let (_, msg) = outcome.failed.expect("non-Pure tool fails the node");
        assert!(msg.contains("slice 4"), "deferral message: {msg}");
    }

    #[tokio::test]
    async fn agent_halts_at_max_steps_when_the_model_never_finalizes() {
        let (gateway, calls) = scripted_gateway(vec![
            tool_call_response("t1", "calc", "{\"op\":\"add\",\"a\":1,\"b\":1}"),
            tool_call_response("t2", "calc", "{\"op\":\"add\",\"a\":1,\"b\":1}"),
        ])
        .await;
        let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
            .with_registry(tool_agent_registry())
            .with_tools(calc_tools())
            .with_max_steps(2);
        let graph = Graph {
            nodes: vec![agent_node("n1", "a", "loop")],
        };
        let outcome = exec
            .run(RunId(uuid::Uuid::new_v4()), &graph)
            .await
            .expect("outcome");
        let (_, msg) = outcome.failed.expect("max_steps halts");
        assert!(msg.contains("max_steps"), "{msg}");
        assert_eq!(
            calls.lock().unwrap().len(),
            2,
            "exactly max_steps model turns"
        );
    }

    #[tokio::test]
    async fn agent_node_single_turn_runs_through_gateway_and_journals() {
        let (gateway, calls) = recording_gateway().await; // returns empty tool_calls → final on turn 0
        let journal = InMemoryJournal::new();
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
            .with_registry(agent_registry("c"))
            .with_tools(Arc::new(ToolRegistry::default().with_tool(Arc::new(Calc))));

        let n1 = NodeId("n1".into());
        let graph = Graph {
            nodes: vec![agent_node("n1", "a", "hello")],
        };
        let run = RunId(uuid::Uuid::new_v4());
        let outcome = exec.run(run, &graph).await.expect("run");

        assert!(outcome.failed.is_none(), "{:?}", outcome.failed);
        assert_eq!(outcome.completed, vec![n1.clone()]);
        assert_eq!(outcome.outputs[&n1]["text"], "canned-response");
        assert_eq!(
            calls.lock().unwrap().len(),
            1,
            "one model turn, one gateway call"
        );

        let kinds: Vec<String> = journal
            .load(run)
            .await
            .unwrap()
            .iter()
            .map(|(_, e)| label(e))
            .collect();
        assert_eq!(
            kinds,
            vec![
                "RunStarted",
                "NodeStarted(n1)",
                "EffectRecorded(n1)",
                "NodeCompleted(n1)",
                "RunCompleted"
            ]
        );
    }

    #[tokio::test]
    async fn agent_node_halts_over_budget_before_any_gateway_call() {
        let (gateway, calls) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        // max_context of chain "c" is 4096; force a tiny window via max_steps? No —
        // budget uses the chain window. Use a registry whose agent has a huge body.
        let big = AgentDefinition {
            system_prompt: "x".repeat(100_000),
            ..agent_def("c")
        };
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
            .with_registry(Arc::new(Registry::default().with_agent(big)))
            .with_tools(Arc::new(ToolRegistry::default()));

        let graph = Graph {
            nodes: vec![agent_node("n1", "a", "hi")],
        };
        let run = RunId(uuid::Uuid::new_v4());
        let outcome = exec.run(run, &graph).await.expect("run yields an outcome");
        match &outcome.failed {
            Some((node, msg)) => {
                assert_eq!(node.0, "n1");
                assert!(msg.contains("over budget"), "{msg}");
            }
            None => panic!("expected an over-budget failure"),
        }
        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "over-budget halts before spending"
        );
    }

    /// A terminal resume of a completed Agent node returns the SAME canonical
    /// `{model, text}` output as the original `run` — not the raw 3-key final
    /// model-turn effect (`{model, text, tool_calls}`). Proves the durable output
    /// shape is identical across every completion path, while preserving the
    /// no-op-reappend contract (the terminal resume appends nothing).
    #[tokio::test]
    async fn agent_node_terminal_resume_yields_canonical_output_shape() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let n1 = NodeId("n1".into());
        let graph = Graph {
            nodes: vec![agent_node("n1", "a", "hello")],
        };

        // Run 1: drive the single-turn agent node to full completion.
        let (gw1, _calls1) = recording_gateway().await;
        let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
            .with_registry(agent_registry("c"))
            .with_tools(Arc::new(ToolRegistry::default().with_tool(Arc::new(Calc))));
        let outcome1 = exec1.run(run, &graph).await.expect("first run completes");
        assert!(outcome1.failed.is_none());
        assert_eq!(outcome1.completed, vec![n1.clone()]);

        let before = journal.load(run).await.unwrap();

        // Terminal resume on a FRESH gateway: returns the folded outcome without
        // re-driving, projected to the canonical agent-node output shape.
        let (gw2, calls2) = recording_gateway().await;
        let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
            .with_registry(agent_registry("c"))
            .with_tools(Arc::new(ToolRegistry::default().with_tool(Arc::new(Calc))));
        let outcome2 = exec2
            .start(run, &graph)
            .await
            .expect("resume of a completed agent run");

        // Same canonical shape on terminal resume as on the original run.
        assert_eq!(
            outcome2.outputs[&n1], outcome1.outputs[&n1],
            "terminal resume yields the same output shape as the original run"
        );
        // Canonical `{model, text}` — NOT the 3-key raw model-turn effect.
        assert!(
            outcome2.outputs[&n1].get("tool_calls").is_none(),
            "terminal-resume agent output is canonical (no raw tool_calls key): {:?}",
            outcome2.outputs[&n1]
        );
        assert_eq!(
            calls2.lock().unwrap().len(),
            0,
            "a completed run is not re-driven — no gateway call"
        );

        // No-op reappend preserved: the terminal resume appended nothing.
        let after = journal.load(run).await.unwrap();
        assert_eq!(
            after.len(),
            before.len(),
            "terminal resume of a completed agent run appends nothing"
        );
    }

    /// Headline: a run that dies at turn 1 resumes and completes WITHOUT re-calling
    /// the gateway for turn 0 or re-executing turn 0's tool — memoized on resume.
    #[tokio::test]
    async fn agent_resume_does_not_respend_completed_turns() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let graph = Graph {
            nodes: vec![agent_node("n1", "a", "add 2 and 3")],
        };

        // Run 1: turn 0 (calc tool_call) succeeds, then turn 1 is scripted to ERROR
        // (script exhausted → ProviderError). Turn 0's model + calc effects are
        // journaled; the node fails at turn 1; NO RunCompleted.
        let (gw1, calls1) = scripted_gateway(vec![tool_call_response(
            "t1",
            "calc",
            "{\"op\":\"add\",\"a\":2,\"b\":3}",
        )])
        .await;
        let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
            .with_registry(tool_agent_registry())
            .with_tools(calc_tools());
        let outcome1 = exec1
            .run(run, &graph)
            .await
            .expect("run 1 yields an outcome");
        assert!(outcome1.failed.is_some(), "run 1 fails at turn 1");
        assert_eq!(
            calls1.lock().unwrap().len(),
            2,
            "run 1 called the gateway for turn 0 and the failing turn 1"
        );

        // Run 2: a FRESH scripted gateway that serves ONLY turn 1's final answer,
        // over the SAME journal. Resume memoizes turn 0 (model + calc) → the run-2
        // gateway is called exactly once (turn 1).
        let (gw2, calls2) = scripted_gateway(vec![final_response("the answer is 5")]).await;
        let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
            .with_registry(tool_agent_registry())
            .with_tools(calc_tools());
        let outcome2 = exec2.start(run, &graph).await.expect("resume completes");
        assert!(outcome2.failed.is_none(), "{:?}", outcome2.failed);
        assert_eq!(
            outcome2.outputs[&NodeId("n1".into())]["text"],
            "the answer is 5"
        );

        // The proof: run-2's gateway saw EXACTLY ONE call (turn 1). Turn 0 was
        // replayed from the journal — not re-spent — and calc was not re-executed.
        assert_eq!(
            calls2.lock().unwrap().len(),
            1,
            "resume re-spent nothing for turn 0: {:?}",
            calls2.lock().unwrap()
        );
        let events = journal.load(run).await.unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|(_, e)| matches!(e, JournalEvent::RunCompleted))
                .count(),
            1
        );

        // The non-vacuous proof: turn 0's model effect and its calc tool effect
        // each appear in EXACTLY ONE `EffectRecorded` across BOTH runs — recorded
        // once in run 1, and NOT re-appended in run 2. If the memo lookup were
        // broken (forcing turn 0 to re-run live on resume), these effects would
        // be re-recorded and this count would be 2, even though `calls2 == 1` and
        // the final-text assertion above would still spuriously pass (a lone
        // scripted final response finalizes a wrongly-re-run turn 0 in one call).
        let recorded_count = |eid: &EffectId| {
            events
                .iter()
                .filter(|(_, e)| {
                    matches!(e, JournalEvent::EffectRecorded { effect_id: rec, .. } if rec == eid)
                })
                .count()
        };
        assert_eq!(
            recorded_count(&effect_id("n1", 0, 0)),
            1,
            "turn 0's model call was replayed from the journal on resume (memoized), not re-recorded/re-spent"
        );
        assert_eq!(
            recorded_count(&effect_id("n1", 0, 1)),
            1,
            "turn 0's calc tool was memoized on resume, not re-executed/re-recorded"
        );
    }

    /// Editing a skill body changes the turn's system prompt → its input-hash no
    /// longer matches the memoized turn → resume halts with DeterminismViolation
    /// (never mixes new instructions into a memoized old turn). No gateway call.
    #[tokio::test]
    async fn agent_resume_halts_when_a_skill_changed_under_a_completed_turn() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());

        // A registry with agent "a" (skill "s") whose skill body is parameterized.
        let registry = |body: &str| {
            Arc::new(
                Registry::default()
                    .with_agent(AgentDefinition {
                        skills: vec!["s".into()],
                        ..agent_def("c")
                    })
                    .with_skill(orchestrator_core::SkillDef {
                        name: "s".into(),
                        description: None,
                        body: body.into(),
                    }),
            )
        };

        // Graph [agent n1, model n2]. Run 1 with skill body "V1": n1's single turn
        // succeeds (gateway call 1), then n2 fails (gateway call 2) → n1 is fully
        // journaled+completed, but there is NO RunCompleted (a partial run to resume).
        let graph = Graph {
            nodes: vec![
                agent_node("n1", "a", "hi"),
                Node {
                    id: NodeId("n2".into()),
                    kind: model_call("c", "b"),
                    deps: vec![Dep::hard("n1")],
                },
            ],
        };
        let (gw1, _c1) = failing_after_gateway(1).await;
        let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
            .with_registry(registry("V1"));
        let out1 = exec1
            .run(run, &graph)
            .await
            .expect("run 1 yields an outcome");
        assert!(
            out1.failed.is_some(),
            "n2 fails, leaving n1's turn journaled without RunCompleted"
        );

        // Run 2: resume with skill body CHANGED to "V2" → n1's turn system prompt
        // (and thus input-hash) differs from the memoized turn → determinism halt.
        let (gw2, calls2) = recording_gateway().await;
        let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
            .with_registry(registry("V2"));
        let err = exec2
            .start(run, &graph)
            .await
            .expect_err("determinism violation");
        assert!(
            matches!(err, OrchestratorError::DeterminismViolation { .. }),
            "got {err:?}"
        );
        assert_eq!(
            calls2.lock().unwrap().len(),
            0,
            "a determinism violation never touches the gateway"
        );
    }

    fn model_call(chain: &str, prompt: &str) -> NodeKind {
        NodeKind::ModelCall {
            chain: chain.to_string(),
            payload: serde_json::json!({ "prompt": prompt }),
        }
    }

    /// A canonical linear 2-node graph `[n1{prompt:p1} → n2{prompt:p2}]` on the
    /// recording chain `"c"`, returned with its node ids for assertions.
    fn two_node_graph(p1: &str, p2: &str) -> (Graph, NodeId, NodeId) {
        let n1 = NodeId("n1".into());
        let n2 = NodeId("n2".into());
        let graph = Graph {
            nodes: vec![
                Node {
                    id: n1.clone(),
                    kind: model_call("c", p1),
                    deps: vec![],
                },
                Node {
                    id: n2.clone(),
                    kind: model_call("c", p2),
                    deps: vec![Dep::hard(n1.clone())],
                },
            ],
        };
        (graph, n1, n2)
    }

    /// A journal whose every `append` fails — proves a backend write error is
    /// surfaced as `OrchestratorError::Journal`, never swallowed.
    struct FailingJournal;

    #[async_trait::async_trait]
    impl ExecutionJournal for FailingJournal {
        async fn append(&self, _run: RunId, _event: JournalEvent) -> Result<Seq, JournalError> {
            Err(JournalError::Backend("injected backend failure".into()))
        }
        async fn load(&self, _run: RunId) -> Result<Vec<(Seq, JournalEvent)>, JournalError> {
            Ok(Vec::new())
        }
    }

    /// Compact, order-preserving label for a journal event, so the test asserts
    /// the exact event sequence (kind + node) without matching payloads.
    fn label(event: &JournalEvent) -> String {
        match event {
            JournalEvent::RunStarted { .. } => "RunStarted".to_string(),
            JournalEvent::NodeStarted { node } => format!("NodeStarted({})", node.0),
            JournalEvent::EffectRecorded { node, .. } => format!("EffectRecorded({})", node.0),
            JournalEvent::NodeCompleted { node } => format!("NodeCompleted({})", node.0),
            JournalEvent::NodeFailed { node, .. } => format!("NodeFailed({})", node.0),
            JournalEvent::NodeSkipped { node } => format!("NodeSkipped({})", node.0),
            JournalEvent::MapExpanded { node, child_count } => {
                format!("MapExpanded({}x{})", node.0, child_count)
            }
            JournalEvent::RunCompleted => "RunCompleted".to_string(),
            JournalEvent::RunPaused { .. } => "RunPaused".to_string(),
        }
    }

    /// The DAG scheduler runs a diamond (`a → {b, c} → d`) declared OUT of
    /// topological order, scheduling each node only once its dependencies have
    /// completed. The old linear drive rejected this graph outright
    /// (`validate_linear`); the scheduler runs it and completes in a valid
    /// topological order (`a` first, `d` last, `b`/`c` before `d`).
    #[tokio::test]
    async fn scheduler_runs_a_diamond_dag_in_topological_order() {
        let (gateway, _calls) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");

        // Declared out of order: [d, b, c, a]. d hard-deps b & c; b, c hard-dep a.
        let graph = Graph {
            nodes: vec![
                Node {
                    id: NodeId("d".into()),
                    kind: model_call("c", "pd"),
                    deps: vec![Dep::hard("b"), Dep::hard("c")],
                },
                Node {
                    id: NodeId("b".into()),
                    kind: model_call("c", "pb"),
                    deps: vec![Dep::hard("a")],
                },
                Node {
                    id: NodeId("c".into()),
                    kind: model_call("c", "pc"),
                    deps: vec![Dep::hard("a")],
                },
                Node {
                    id: NodeId("a".into()),
                    kind: model_call("c", "pa"),
                    deps: vec![],
                },
            ],
        };

        let run = RunId(uuid::Uuid::new_v4());
        let outcome = exec.run(run, &graph).await.expect("diamond DAG runs");

        assert!(outcome.failed.is_none(), "{:?}", outcome.failed);
        assert_eq!(outcome.completed.len(), 4, "all four nodes completed");
        let pos = |id: &str| {
            outcome
                .completed
                .iter()
                .position(|n| n.0 == id)
                .unwrap_or_else(|| panic!("{id} completed"))
        };
        assert_eq!(outcome.completed.first().unwrap().0, "a", "root first");
        assert_eq!(outcome.completed.last().unwrap().0, "d", "sink last");
        assert!(pos("a") < pos("b") && pos("a") < pos("c"), "a before b,c");
        assert!(pos("b") < pos("d") && pos("c") < pos("d"), "b,c before d");
    }

    /// Map items as `{prompt}` payloads for `MapBody::ModelCall`; a prompt
    /// containing `"FAIL"` fails under the content-gated gateway.
    fn map_items<const N: usize>(prompts: [&str; N]) -> Vec<serde_json::Value> {
        prompts
            .iter()
            .map(|p| serde_json::json!({ "prompt": p }))
            .collect()
    }

    /// A single-node graph holding one `Map` over `over` with the given aggregation.
    fn map_graph(id: &str, over: Vec<serde_json::Value>, aggregation: Aggregation) -> Graph {
        Graph {
            nodes: vec![Node {
                id: NodeId(id.into()),
                kind: NodeKind::Map {
                    body: MapBody::ModelCall { chain: "c".into() },
                    over,
                    concurrency: 4,
                    aggregation,
                },
                deps: vec![],
            }],
        }
    }

    /// Acceptance 2 — a `BestEffort` Map with two failing children completes,
    /// carrying a `{ok:3, failed:2}` manifest and results indexed by item order.
    #[tokio::test]
    async fn map_best_effort_completes_with_a_failure_manifest() {
        let (gateway, _calls) = content_gated_gateway().await;
        let journal = InMemoryJournal::new();
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");

        let over = map_items(["a-0", "b-1", "c-2", "FAIL-3", "FAIL-4"]);
        let graph = map_graph("m", over, Aggregation::BestEffort);
        let m = NodeId("m".into());

        let outcome = exec
            .run(RunId(uuid::Uuid::new_v4()), &graph)
            .await
            .expect("map runs");

        assert!(
            outcome.failed.is_none(),
            "BestEffort never fails the run: {:?}",
            outcome.failed
        );
        assert_eq!(outcome.completed, vec![m.clone()], "the Map node completed");
        let out = &outcome.outputs[&m];
        assert_eq!(out["manifest"]["ok"], 3, "manifest: {out}");
        assert_eq!(out["manifest"]["failed"], 2, "manifest: {out}");

        let results = out["results"].as_array().expect("results array");
        assert_eq!(results.len(), 5, "one result per item");
        // Deterministic index order regardless of concurrent completion order.
        for (i, r) in results.iter().enumerate() {
            assert_eq!(r["index"], i as i64, "result {i} carries its index");
        }
        assert!(results[0].get("ok").is_some(), "child 0 succeeded");
        assert!(results[2].get("ok").is_some(), "child 2 succeeded");
        assert!(results[3].get("error").is_some(), "child 3 failed");
        assert!(results[4].get("error").is_some(), "child 4 failed");
    }

    /// Acceptance 3 — `Quorum{min_fraction:0.6}` over 5: with 2 failures (3/5 =
    /// 0.6) the Map completes; with 3 failures (2/5 = 0.4) it fails loudly, with
    /// the manifest still attached to the outcome.
    #[tokio::test]
    async fn map_quorum_completes_at_threshold_and_fails_below_it() {
        let quorum = || Aggregation::Quorum {
            min_count: None,
            min_fraction: Some(0.6),
        };
        let m = NodeId("m".into());

        // 3 ok / 5 == 0.6 → meets quorum → Completed.
        let (g1, _c1) = content_gated_gateway().await;
        let exec1 = Executor::new(Arc::new(g1), Arc::new(InMemoryJournal::new()), "v1");
        let graph1 = map_graph("m", map_items(["a", "b", "c", "FAIL", "FAIL"]), quorum());
        let out1 = exec1
            .run(RunId(uuid::Uuid::new_v4()), &graph1)
            .await
            .expect("runs");
        assert!(
            out1.failed.is_none(),
            "3/5 == 0.6 meets quorum: {:?}",
            out1.failed
        );
        assert_eq!(out1.outputs[&m]["manifest"]["ok"], 3);

        // 2 ok / 5 == 0.4 → below quorum → Failed, manifest attached.
        let (g2, _c2) = content_gated_gateway().await;
        let exec2 = Executor::new(Arc::new(g2), Arc::new(InMemoryJournal::new()), "v1");
        let graph2 = map_graph("m", map_items(["a", "b", "FAIL", "FAIL", "FAIL"]), quorum());
        let out2 = exec2
            .run(RunId(uuid::Uuid::new_v4()), &graph2)
            .await
            .expect("runs");
        let (fnode, _msg) = out2.failed.as_ref().expect("2/5 < 0.6 fails quorum");
        assert_eq!(fnode.0, "m", "the Map node is the failure");
        assert_eq!(
            out2.outputs[&m]["manifest"]["failed"], 3,
            "the failure manifest is carried into the outcome, never dropped"
        );
    }

    /// Acceptance 4 — a failed node cascade-skips its hard-dependents
    /// (transitively across hard edges), journaling `NodeSkipped` and surfacing
    /// them in `RunOutcome.skipped`; a node that only *soft*-depends on the
    /// failure still runs.
    #[tokio::test]
    async fn cascade_skip_hard_dependents_but_run_soft_dependents() {
        let (gateway, _calls) = content_gated_gateway().await;
        let journal = InMemoryJournal::new();
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");

        let mc = |p: &str| NodeKind::ModelCall {
            chain: "c".into(),
            payload: serde_json::json!({ "prompt": p }),
        };
        // f fails; h hard-deps f → skip; h2 hard-deps h → cascade-skip;
        // s soft-deps f → still runs.
        let graph = Graph {
            nodes: vec![
                Node {
                    id: NodeId("f".into()),
                    kind: mc("FAIL"),
                    deps: vec![],
                },
                Node {
                    id: NodeId("h".into()),
                    kind: mc("h-ok"),
                    deps: vec![Dep::hard("f")],
                },
                Node {
                    id: NodeId("h2".into()),
                    kind: mc("h2-ok"),
                    deps: vec![Dep::hard("h")],
                },
                Node {
                    id: NodeId("s".into()),
                    kind: mc("s-ok"),
                    deps: vec![Dep::soft("f")],
                },
            ],
        };

        let run = RunId(uuid::Uuid::new_v4());
        let outcome = exec.run(run, &graph).await.expect("run yields an outcome");

        let (fnode, _) = outcome.failed.as_ref().expect("f failed");
        assert_eq!(fnode.0, "f");

        let skipped: Vec<&str> = outcome.skipped.iter().map(|n| n.0.as_str()).collect();
        assert!(
            skipped.contains(&"h"),
            "h hard-depends on failed f → skipped: {skipped:?}"
        );
        assert!(
            skipped.contains(&"h2"),
            "h2 hard-depends on skipped h → cascade-skipped: {skipped:?}"
        );
        assert!(
            outcome.completed.iter().any(|n| n.0 == "s"),
            "s soft-depends on f → still runs"
        );
        assert!(!skipped.contains(&"s"), "s is not skipped");

        // NodeSkipped is journaled for both h and h2 (no silent skip).
        let skips: Vec<String> = journal
            .load(run)
            .await
            .unwrap()
            .iter()
            .filter_map(|(_, e)| match e {
                JournalEvent::NodeSkipped { node } => Some(node.0.clone()),
                _ => None,
            })
            .collect();
        assert!(skips.contains(&"h".to_string()) && skips.contains(&"h2".to_string()));

        // A failed run never writes RunCompleted (stays resumable).
        assert!(
            !journal
                .load(run)
                .await
                .unwrap()
                .iter()
                .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
            "a run with a failure is not marked complete"
        );
    }

    /// A `Map`("m", BestEffort over 5 with 2 failing) → `Consolidate`("cons")
    /// soft-depending on it, with the given `min_viable`.
    fn consolidate_graph(min_viable: usize) -> Graph {
        Graph {
            nodes: vec![
                Node {
                    id: NodeId("m".into()),
                    kind: NodeKind::Map {
                        body: MapBody::ModelCall { chain: "c".into() },
                        over: map_items(["a", "b", "c", "FAIL", "FAIL"]),
                        concurrency: 4,
                        aggregation: Aggregation::BestEffort,
                    },
                    deps: vec![],
                },
                Node {
                    id: NodeId("cons".into()),
                    kind: NodeKind::Consolidate {
                        over: NodeId("m".into()),
                        min_viable,
                        body: MapBody::ModelCall { chain: "c".into() },
                    },
                    deps: vec![Dep::soft("m")],
                },
            ],
        }
    }

    /// Acceptance 5 — `Consolidate` synthesizes over the Map's survivors when
    /// they meet `min_viable`, and halts loudly (`ConsolidateStarved`) when they
    /// don't — never a silent empty synthesis.
    #[tokio::test]
    async fn consolidate_synthesizes_survivors_and_starves_below_min_viable() {
        let cons = NodeId("cons".into());

        // 3 survivors ≥ min_viable 3 → Consolidate runs and produces output.
        let (g1, _c1) = content_gated_gateway().await;
        let exec1 = Executor::new(Arc::new(g1), Arc::new(InMemoryJournal::new()), "v1");
        let out1 = exec1
            .run(RunId(uuid::Uuid::new_v4()), &consolidate_graph(3))
            .await
            .expect("runs");
        assert!(
            out1.failed.is_none(),
            "3 survivors ≥ min_viable 3: {:?}",
            out1.failed
        );
        assert!(
            out1.completed.iter().any(|n| n.0 == "cons"),
            "consolidate completed"
        );
        assert!(
            out1.outputs.contains_key(&cons),
            "consolidate produced a synthesis output"
        );

        // Only 3 survivors < min_viable 4 → ConsolidateStarved (loud halt).
        let (g2, _c2) = content_gated_gateway().await;
        let exec2 = Executor::new(Arc::new(g2), Arc::new(InMemoryJournal::new()), "v1");
        let out2 = exec2
            .run(RunId(uuid::Uuid::new_v4()), &consolidate_graph(4))
            .await
            .expect("runs");
        let (fnode, msg) = out2.failed.as_ref().expect("starved below min_viable");
        assert_eq!(fnode.0, "cons");
        assert!(
            msg.contains("starved") || msg.contains("viable"),
            "loud starvation message: {msg}"
        );
        assert!(
            !out2.completed.iter().any(|n| n.0 == "cons"),
            "a starved consolidate does not complete"
        );
        // The Map's manifest is still carried through, never dropped.
        assert_eq!(out2.outputs[&NodeId("m".into())]["manifest"]["ok"], 3);
    }

    #[tokio::test]
    async fn run_drives_linear_graph_through_gateway_and_journals_in_order() {
        let (gateway, calls) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        let executor = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");

        let n1 = NodeId("n1".into());
        let n2 = NodeId("n2".into());
        let graph = Graph {
            nodes: vec![
                Node {
                    id: n1.clone(),
                    kind: model_call("c", "a"),
                    deps: vec![],
                },
                Node {
                    id: n2.clone(),
                    kind: model_call("c", "b"),
                    deps: vec![Dep::hard(n1.clone())],
                },
            ],
        };

        let run = RunId(uuid::Uuid::new_v4());
        let outcome = executor.run(run, &graph).await.expect("run succeeds");

        // Both nodes completed, in order, with no failure.
        assert!(
            outcome.failed.is_none(),
            "no node should fail: {:?}",
            outcome.failed
        );
        assert_eq!(outcome.completed, vec![n1.clone(), n2.clone()]);
        assert!(outcome.outputs.contains_key(&n1));
        assert!(outcome.outputs.contains_key(&n2));

        // Exactly two gateway calls reached the recording adapter, carrying the
        // two nodes' distinct prompts in order.
        let recorded = calls.lock().unwrap().clone();
        assert_eq!(recorded.len(), 2, "one gateway call per node: {recorded:?}");
        assert_eq!(recorded[0].1, "a");
        assert_eq!(recorded[1].1, "b");

        // The journal holds the exact event sequence, in order.
        let events = journal.load(run).await.expect("load");
        let kinds: Vec<String> = events.iter().map(|(_, e)| label(e)).collect();
        assert_eq!(
            kinds,
            vec![
                "RunStarted",
                "NodeStarted(n1)",
                "EffectRecorded(n1)",
                "NodeCompleted(n1)",
                "NodeStarted(n2)",
                "EffectRecorded(n2)",
                "NodeCompleted(n2)",
                "RunCompleted",
            ],
        );
    }

    /// Headline / load-bearing: a run that dies after n1 resumes to completion
    /// WITHOUT re-spending tokens on n1 — the second gateway is called for n2
    /// only, because n1 is replayed from the journal.
    #[tokio::test]
    async fn start_resumes_without_respending_memoized_model_calls() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let (graph, n1, n2) = two_node_graph("a", "b");

        // Run 1: adapter succeeds on its 1st call (n1) and errors on its 2nd
        // (n2) — a provider dying mid-run. n1 is journaled+completed; n2 fails;
        // NO RunCompleted is written.
        let (gw1, calls1) = failing_after_gateway(1).await;
        let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1");
        let outcome1 = exec1
            .run(run, &graph)
            .await
            .expect("run 1 yields an outcome");
        assert_eq!(
            outcome1.completed,
            vec![n1.clone()],
            "only n1 completed in run 1"
        );
        match &outcome1.failed {
            Some((node, _)) => assert_eq!(node, &n2, "n2 is the failed node in run 1"),
            None => panic!("run 1 must fail at n2, got {:?}", outcome1.failed),
        }
        assert_eq!(
            calls1.lock().unwrap().len(),
            2,
            "run 1 hit the gateway for n1 and the failing n2"
        );

        // Run 2: a FRESH gateway + adapter that always succeeds, over the SAME
        // journal. `start` folds the journal, memoizes n1, and drives only the
        // tail.
        let (gw2, calls2) = recording_gateway().await;
        let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1");
        let outcome2 = exec2
            .start(run, &graph)
            .await
            .expect("start resumes the run");
        assert!(
            outcome2.failed.is_none(),
            "resume completes with no failure: {:?}",
            outcome2.failed
        );
        assert_eq!(
            outcome2.completed,
            vec![n1.clone(), n2.clone()],
            "both nodes completed after resume"
        );
        assert!(outcome2.outputs.contains_key(&n1));
        assert!(outcome2.outputs.contains_key(&n2));

        // The proof: run-2's gateway saw EXACTLY ONE call, carrying n2's prompt
        // "b". n1 was replayed from the journal — not re-spent.
        let recorded2 = calls2.lock().unwrap().clone();
        assert_eq!(
            recorded2.len(),
            1,
            "resume re-called the gateway only for the tail node n2: {recorded2:?}"
        );
        assert_eq!(
            recorded2[0].1, "b",
            "the single resume call carried n2's prompt"
        );

        // Exactly one RunCompleted across both runs (run 1 wrote none; the
        // resume wrote one), and the journal ends on it.
        let events = journal.load(run).await.expect("load");
        let completes = events
            .iter()
            .filter(|(_, e)| matches!(e, JournalEvent::RunCompleted))
            .count();
        assert_eq!(completes, 1, "exactly one RunCompleted across both runs");
        assert!(
            matches!(
                events.last().map(|(_, e)| e),
                Some(JournalEvent::RunCompleted)
            ),
            "the journal ends with RunCompleted"
        );
    }

    /// A resume whose graph changed under a completed node halts with a
    /// determinism violation — it never silently re-runs or re-memoizes, and
    /// never calls the gateway for the changed node.
    #[tokio::test]
    async fn start_halts_on_determinism_violation_without_calling_gateway() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let n1 = NodeId("n1".into());

        // Pre-seed a partial journal: n1 recorded for payload {prompt:"a"}, no
        // RunCompleted. Direct appends — independent of the gateway.
        journal
            .append(
                run,
                JournalEvent::RunStarted {
                    version: "v1".into(),
                },
            )
            .await
            .unwrap();
        let ih_a = input_hash("c", &serde_json::json!({ "prompt": "a" })).expect("hash");
        journal
            .append(
                run,
                JournalEvent::EffectRecorded {
                    node: n1.clone(),
                    effect_id: effect_id("", 0, 0),
                    class: EffectClass::Pure,
                    input_hash: ih_a,
                    seq: 0,
                    output: EffectOutput::Inline(
                        serde_json::json!({ "model": "m", "text": "canned-response" }),
                    ),
                },
            )
            .await
            .unwrap();
        journal
            .append(run, JournalEvent::NodeCompleted { node: n1.clone() })
            .await
            .unwrap();

        // Resume with n1's payload CHANGED — its input hash no longer matches.
        let (gw, calls) = recording_gateway().await;
        let exec = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1");
        let (graph, _, _) = two_node_graph("CHANGED", "b");
        let err = exec
            .start(run, &graph)
            .await
            .expect_err("determinism violation halts the resume");
        match err {
            OrchestratorError::DeterminismViolation { node, .. } => assert_eq!(node, n1),
            other => panic!("expected DeterminismViolation, got {other:?}"),
        }
        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "a determinism violation never touches the gateway"
        );
    }

    /// A resume against a journal written by a different version is refused by
    /// the version fence — no gateway call, no silent re-run.
    #[tokio::test]
    async fn start_refuses_resume_on_version_fence_mismatch() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        journal
            .append(
                run,
                JournalEvent::RunStarted {
                    version: "v1".into(),
                },
            )
            .await
            .unwrap();

        let (gw, calls) = recording_gateway().await;
        let exec = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v2");
        let (graph, _, _) = two_node_graph("a", "b");
        let err = exec
            .start(run, &graph)
            .await
            .expect_err("version fence refuses the resume");
        match err {
            OrchestratorError::VersionFenceMismatch { recorded, current } => {
                assert_eq!(recorded, "v1");
                assert_eq!(current, "v2");
            }
            other => panic!("expected VersionFenceMismatch, got {other:?}"),
        }
        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "a fenced run never touches the gateway"
        );
    }

    /// A journal-backend write error is surfaced as `OrchestratorError::Journal`
    /// and aborts the run — it is never swallowed and the run does not silently
    /// continue to the gateway.
    #[tokio::test]
    async fn run_surfaces_a_journal_backend_error_instead_of_swallowing_it() {
        let (gw, calls) = recording_gateway().await;
        let exec = Executor::new(Arc::new(gw), Arc::new(FailingJournal), "v1");
        let (graph, _, _) = two_node_graph("a", "b");
        let run = RunId(uuid::Uuid::new_v4());

        let err = exec
            .run(run, &graph)
            .await
            .expect_err("a journal backend error surfaces");
        assert!(
            matches!(err, OrchestratorError::Journal(JournalError::Backend(_))),
            "expected OrchestratorError::Journal(Backend), got {err:?}"
        );
        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "the run aborts on the first failed append, before any gateway call"
        );
    }

    /// Resuming an already-completed run is a no-op re-append: it returns the
    /// folded outcome, does not re-drive (no gateway call), and appends no
    /// second RunCompleted.
    #[tokio::test]
    async fn start_on_a_completed_run_is_a_noop_reappend() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let (graph, n1, n2) = two_node_graph("a", "b");

        // Drive the run to full completion first.
        let (gw1, _calls1) = recording_gateway().await;
        let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1");
        let outcome1 = exec1.run(run, &graph).await.expect("first run completes");
        assert!(outcome1.failed.is_none());
        assert_eq!(outcome1.completed, vec![n1.clone(), n2.clone()]);

        let before = journal.load(run).await.unwrap();
        let completes_before = before
            .iter()
            .filter(|(_, e)| matches!(e, JournalEvent::RunCompleted))
            .count();
        assert_eq!(completes_before, 1, "one RunCompleted after the first run");

        // Resume the already-terminal run on a FRESH gateway: returns the folded
        // outcome without re-driving.
        let (gw2, calls2) = recording_gateway().await;
        let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1");
        let outcome2 = exec2
            .start(run, &graph)
            .await
            .expect("resume of a completed run");
        assert!(outcome2.failed.is_none());
        assert_eq!(
            outcome2.completed,
            vec![n1.clone(), n2.clone()],
            "folded outcome lists both completed nodes"
        );
        assert!(outcome2.outputs.contains_key(&n1));
        assert!(outcome2.outputs.contains_key(&n2));
        assert_eq!(
            calls2.lock().unwrap().len(),
            0,
            "a completed run is not re-driven — no gateway call"
        );

        let after = journal.load(run).await.unwrap();
        let completes_after = after
            .iter()
            .filter(|(_, e)| matches!(e, JournalEvent::RunCompleted))
            .count();
        assert_eq!(
            completes_after, 1,
            "resume of a completed run appends no second RunCompleted"
        );
        assert_eq!(
            after.len(),
            before.len(),
            "resume of a completed run appends nothing at all"
        );
    }

    /// Real end-to-end: the durable executor drives the REAL gateway assembled
    /// from the illustrative demo catalog (`gateway::catalog::assemble(
    /// demo_catalog())`) over a REFERENCE chain (`research.bulk`). The selector
    /// walks `groq-llama-free` (no adapter → fall over) → `deepseek-chat` (no
    /// adapter → fall over) → `llama3.1-local` (served by the local ollama
    /// adapter). The run completes and the orchestrator records the model the
    /// chain fell over to — proving the spine drives the real gateway + a real
    /// reference chain, not a bespoke test-only single-model chain.
    #[tokio::test]
    async fn run_drives_real_reference_chain_end_to_end_to_local_fallover() {
        let (gateway, calls) = demo_reference_gateway().await;
        let journal = InMemoryJournal::new();
        let executor = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");

        // A single `ModelCall` node on the reference chain `research.bulk`.
        let n1 = NodeId("n1".into());
        let graph = Graph {
            nodes: vec![Node {
                id: n1.clone(),
                kind: NodeKind::ModelCall {
                    chain: "research.bulk".into(),
                    payload: serde_json::json!({ "prompt": "hello" }),
                },
                deps: vec![],
            }],
        };

        let run = RunId(uuid::Uuid::new_v4());
        let outcome = executor.run(run, &graph).await.expect("run succeeds");

        // The reference chain ran to completion via genuine fallover.
        assert!(
            outcome.failed.is_none(),
            "the reference chain runs to completion via fallover: {:?}",
            outcome.failed
        );
        assert_eq!(outcome.completed, vec![n1.clone()], "n1 completed");
        // The load-bearing assertion: the orchestrator recorded that the chain
        // fell over the credential-gated cloud entries to the LOCAL model.
        assert_eq!(
            outcome.outputs[&n1]["model"], "llama3.1-local",
            "the reference chain fell over cloud entries to the local model, recorded by the orchestrator: {:?}",
            outcome.outputs[&n1],
        );

        // The chain genuinely reached the local adapter (the terminal candidate
        // was served, not short-circuited earlier).
        assert_eq!(
            calls.lock().unwrap().len(),
            1,
            "the served terminal candidate hit the local ollama adapter exactly once",
        );

        // And the journal is a clean single-node run ending on RunCompleted.
        let events = journal.load(run).await.expect("load");
        let kinds: Vec<String> = events.iter().map(|(_, e)| label(e)).collect();
        assert_eq!(
            kinds,
            vec![
                "RunStarted",
                "NodeStarted(n1)",
                "EffectRecorded(n1)",
                "NodeCompleted(n1)",
                "RunCompleted",
            ],
        );
    }

    /// Real end-to-end: an `Agent` node whose role resolves to the reference chain
    /// `research.bulk` drives the REAL gateway (assembled from `demo_catalog`). The
    /// chain falls over the credential-gated cloud entries to the local ollama
    /// model; the agent's single (no-tool) turn is served by `llama3.1-local`.
    #[tokio::test]
    async fn agent_node_drives_real_reference_chain_to_local_fallover() {
        let (gateway, calls) = demo_reference_gateway().await;
        let journal = InMemoryJournal::new();
        let registry = Arc::new(Registry::default().with_agent(AgentDefinition {
            name: "researcher".into(),
            area: "research".into(),
            kind: "reasoning".into(),
            chain: "research.bulk".into(),
            tools: vec![],
            skills: vec![],
            system_prompt: "Research carefully.".into(),
        }));
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
            .with_registry(registry)
            .with_tools(Arc::new(ToolRegistry::default()));

        let n1 = NodeId("n1".into());
        let graph = Graph {
            nodes: vec![agent_node("n1", "researcher", "summarize the news")],
        };
        let outcome = exec
            .run(RunId(uuid::Uuid::new_v4()), &graph)
            .await
            .expect("run");

        assert!(outcome.failed.is_none(), "{:?}", outcome.failed);
        assert_eq!(
            outcome.outputs[&n1]["model"], "llama3.1-local",
            "fell over to the local model: {:?}",
            outcome.outputs[&n1]
        );
        assert_eq!(
            calls.lock().unwrap().len(),
            1,
            "the served terminal candidate hit the local adapter once"
        );
    }

    /// Acceptance 7 (split + dedupe) — with a `ContentStore` wired, an effect
    /// output whose serialized size exceeds `cas_threshold` is stored in the CAS
    /// and the journal carries a `ContentRef` (never the inline value); two
    /// identical outputs share one digest (dedupe); a below-threshold output
    /// stays inline (the gate cuts both ways). The blob round-trips via the CAS.
    #[tokio::test]
    async fn cas_threshold_splits_large_outputs_to_deduped_refs_and_keeps_small_ones_inline() {
        use orchestrator_store::InMemoryContentStore;

        // Two ModelCall nodes; the recording gateway returns the SAME canned
        // output for both (~38 bytes), so with a low threshold both split to ONE
        // shared digest, and with the default high threshold both stay inline.
        let (graph, n1, n2) = two_node_graph("a", "b");

        // Low threshold (8 < ~38 bytes) → both outputs split to refs.
        let (gw_lo, _c_lo) = recording_gateway().await;
        let journal_lo = InMemoryJournal::new();
        let content = Arc::new(InMemoryContentStore::new());
        let exec_lo = Executor::new(Arc::new(gw_lo), Arc::new(journal_lo.clone()), "v1")
            .with_content_store(content.clone())
            .with_cas_threshold(8);
        let run_lo = RunId(uuid::Uuid::new_v4());
        let out_lo = exec_lo
            .run(run_lo, &graph)
            .await
            .expect("low-threshold run");
        assert!(out_lo.failed.is_none(), "{:?}", out_lo.failed);

        // Every EffectRecorded carries a Ref; collect their digests.
        let digests: Vec<String> = journal_lo
            .load(run_lo)
            .await
            .unwrap()
            .iter()
            .filter_map(|(_, e)| match e {
                JournalEvent::EffectRecorded {
                    output: EffectOutput::Ref(r),
                    ..
                } => Some(r.digest.0.clone()),
                JournalEvent::EffectRecorded {
                    output: EffectOutput::Inline(v),
                    ..
                } => panic!("over-threshold output must split to a Ref, got inline {v}"),
                _ => None,
            })
            .collect();
        assert_eq!(digests.len(), 2, "both nodes recorded a ref");
        assert_eq!(
            digests[0], digests[1],
            "identical outputs dedupe to one digest"
        );

        // The blob is addressable in the CAS and round-trips to the recorded value.
        let bytes = content
            .get(&orchestrator_core::Digest(digests[0].clone()))
            .await
            .expect("blob present in the CAS");
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["text"], "canned-response");
        // The outcome still exposes the full materialized value.
        assert_eq!(out_lo.outputs[&n1]["text"], "canned-response");
        assert_eq!(out_lo.outputs[&n2]["text"], "canned-response");

        // Default (4 KiB) threshold with a store wired → the same small output
        // stays INLINE (behavior-preserving).
        let (gw_hi, _c_hi) = recording_gateway().await;
        let journal_hi = InMemoryJournal::new();
        let exec_hi = Executor::new(Arc::new(gw_hi), Arc::new(journal_hi.clone()), "v1")
            .with_content_store(Arc::new(InMemoryContentStore::new()));
        let run_hi = RunId(uuid::Uuid::new_v4());
        exec_hi
            .run(run_hi, &graph)
            .await
            .expect("high-threshold run");
        for (_, e) in journal_hi.load(run_hi).await.unwrap() {
            if let JournalEvent::EffectRecorded { output, .. } = e {
                assert!(
                    matches!(output, EffectOutput::Inline(_)),
                    "below-threshold output stays inline: {output:?}"
                );
            }
        }
    }

    /// Acceptance 7 (lazy fold + resume) — a large memoized output is recorded as
    /// a ref; on resume the fold reads that ref WITHOUT loading its blob, and the
    /// node re-materializes it from the SHARED CAS exactly once (the memoized
    /// replay) — re-spending no tokens. If the fold eagerly loaded blobs, the CAS
    /// `get` count on resume would be 2 (fold + replay) instead of 1.
    #[tokio::test]
    async fn resume_folds_a_ref_lazily_and_rematerializes_it_from_the_cas_without_respending() {
        use orchestrator_core::{ContentStore, Digest};
        use orchestrator_store::InMemoryContentStore;
        use std::sync::atomic::{AtomicUsize, Ordering};

        // A get-counting CAS wrapper — proves the fold does not load blobs.
        struct CountingCas {
            inner: InMemoryContentStore,
            gets: Arc<AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl ContentStore for CountingCas {
            async fn put(&self, bytes: &[u8]) -> Result<Digest, OrchestratorError> {
                self.inner.put(bytes).await
            }
            async fn get(&self, d: &Digest) -> Result<Vec<u8>, OrchestratorError> {
                self.gets.fetch_add(1, Ordering::SeqCst);
                self.inner.get(d).await
            }
        }

        let gets = Arc::new(AtomicUsize::new(0));
        let content: Arc<dyn ContentStore> = Arc::new(CountingCas {
            inner: InMemoryContentStore::new(),
            gets: gets.clone(),
        });

        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let (graph, n1, n2) = two_node_graph("a", "b");

        // Run 1: n1 succeeds (recorded as a ref via the low threshold), n2 fails
        // → no RunCompleted. The live path never reads back from the CAS.
        let (gw1, _c1) = failing_after_gateway(1).await;
        let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
            .with_content_store(content.clone())
            .with_cas_threshold(8);
        let out1 = exec1
            .run(run, &graph)
            .await
            .expect("run 1 yields an outcome");
        assert!(
            out1.failed.is_some(),
            "n2 fails, leaving n1 journaled without RunCompleted"
        );
        assert_eq!(
            gets.load(Ordering::SeqCst),
            0,
            "the live run never reads back from the CAS"
        );

        // n1's effect was recorded as a REF (not inline).
        let n1_is_ref = journal.load(run).await.unwrap().iter().any(|(_, e)| {
            matches!(
                e,
                JournalEvent::EffectRecorded { node, output: EffectOutput::Ref(_), .. }
                    if node == &n1
            )
        });
        assert!(n1_is_ref, "n1's over-threshold output was split to a ref");

        // Run 2: resume on a FRESH gateway over the SAME journal + SAME CAS. The
        // fold reads n1's ref without loading it; the replay materializes it once.
        let gets_before_run2 = gets.load(Ordering::SeqCst);
        let (gw2, calls2) = recording_gateway().await;
        let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
            .with_content_store(content.clone())
            .with_cas_threshold(8);
        let out2 = exec2.start(run, &graph).await.expect("resume completes");
        assert!(out2.failed.is_none(), "{:?}", out2.failed);
        assert_eq!(out2.completed, vec![n1.clone(), n2.clone()]);
        assert_eq!(
            out2.outputs[&n1]["text"], "canned-response",
            "n1 re-materialized from the CAS"
        );

        // The proof of lazy fold: resume loaded n1's blob EXACTLY ONCE (the
        // memoized replay), not twice (which an eager fold would cause).
        assert_eq!(
            gets.load(Ordering::SeqCst) - gets_before_run2,
            1,
            "resume loaded n1's blob once (lazy replay), never during the fold"
        );
        // And n1 was not re-spent: the run-2 gateway was called only for n2.
        assert_eq!(
            calls2.lock().unwrap().len(),
            1,
            "resume re-called the gateway only for the tail n2"
        );
    }

    /// Increment 8a — the executor writes a round-boundary snapshot after each
    /// scheduling round (out-of-band, so the journal event order is unchanged):
    /// the latest snapshot captures every completed node and its output.
    #[tokio::test]
    async fn drive_writes_a_round_boundary_snapshot_capturing_completed_nodes() {
        let (gateway, _calls) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
        let (graph, n1, n2) = two_node_graph("a", "b"); // linear → two rounds
        let run = RunId(uuid::Uuid::new_v4());
        let outcome = exec.run(run, &graph).await.expect("run");
        assert!(outcome.failed.is_none(), "{:?}", outcome.failed);

        // The latest snapshot reflects BOTH completed nodes and carries each
        // node's output.
        let snap = journal
            .latest_snapshot(run)
            .await
            .unwrap()
            .expect("a snapshot was written");
        assert!(
            snap.completed.contains(&n1) && snap.completed.contains(&n2),
            "snapshot lists completed nodes: {:?}",
            snap.completed
        );
        let keyed: Vec<&NodeId> = snap.outputs.iter().map(|(k, _)| k).collect();
        assert!(
            keyed.contains(&&n1) && keyed.contains(&&n2),
            "snapshot carries per-node outputs: {keyed:?}"
        );
        assert!(snap.seq > 0, "snapshot records a journal boundary seq");

        // The journal event order is byte-identical (snapshots are out-of-band).
        let kinds: Vec<String> = journal
            .load(run)
            .await
            .unwrap()
            .iter()
            .map(|(_, e)| label(e))
            .collect();
        assert_eq!(
            kinds,
            vec![
                "RunStarted",
                "NodeStarted(n1)",
                "EffectRecorded(n1)",
                "NodeCompleted(n1)",
                "NodeStarted(n2)",
                "EffectRecorded(n2)",
                "NodeCompleted(n2)",
                "RunCompleted",
            ],
        );
    }

    /// Acceptance 8 (headline) — a run that dies after a `Map` completed but
    /// before its dependent finished resumes and **re-spends nothing** for the
    /// Map's children: the completed Map is replayed from the journal memo (no
    /// gateway calls, its aggregated output reconstructed) and is NOT re-journaled
    /// (no duplicate `NodeStarted`/`MapExpanded`/`NodeCompleted`), so each child's
    /// effect stays exactly-once. Only the unfinished tail node runs live.
    #[tokio::test]
    async fn resume_replays_a_completed_map_with_no_respend_and_no_reappend() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let m = NodeId("m".into());
        let n2 = NodeId("n2".into());
        let graph = Graph {
            nodes: vec![
                Node {
                    id: m.clone(),
                    kind: NodeKind::Map {
                        body: MapBody::ModelCall { chain: "c".into() },
                        over: map_items(["i0", "i1", "i2"]),
                        concurrency: 4,
                        aggregation: Aggregation::BestEffort,
                    },
                    deps: vec![],
                },
                Node {
                    id: n2.clone(),
                    kind: model_call("c", "tail"),
                    deps: vec![Dep::hard("m")],
                },
            ],
        };

        // Run 1: the 3 Map children succeed (gateway calls 1–3), then n2 fails
        // (call 4) → no RunCompleted. The Map is fully journaled + completed.
        let (gw1, calls1) = failing_after_gateway(3).await;
        let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1");
        let out1 = exec1
            .run(run, &graph)
            .await
            .expect("run 1 yields an outcome");
        assert!(out1.failed.is_some(), "n2 fails in run 1");
        assert_eq!(
            calls1.lock().unwrap().len(),
            4,
            "run 1: 3 Map children + the failing n2"
        );
        let before = journal.load(run).await.unwrap().len();

        // Run 2: resume on a FRESH gateway. n2 succeeds; the Map replays.
        let (gw2, calls2) = recording_gateway().await;
        let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1");
        let out2 = exec2.start(run, &graph).await.expect("resume completes");
        assert!(out2.failed.is_none(), "{:?}", out2.failed);
        assert!(
            out2.completed.contains(&m) && out2.completed.contains(&n2),
            "both nodes completed after resume: {:?}",
            out2.completed
        );
        assert_eq!(
            out2.outputs[&m]["manifest"]["ok"], 3,
            "the Map's aggregated output is reconstructed on resume"
        );

        // Re-spend nothing for the children: run-2 gateway called ONLY for n2.
        let recorded2 = calls2.lock().unwrap().clone();
        assert_eq!(
            recorded2.len(),
            1,
            "resume re-called the gateway only for the tail n2: {recorded2:?}"
        );
        assert_eq!(recorded2[0].1, "tail");

        // The completed Map is NOT re-journaled on resume.
        let all = journal.load(run).await.unwrap();
        let run2_labels: Vec<String> = all[before..].iter().map(|(_, e)| label(e)).collect();
        assert!(
            !run2_labels.iter().any(|l| l == "NodeStarted(m)"
                || l == "NodeCompleted(m)"
                || l.starts_with("MapExpanded(m")),
            "the completed Map is not re-journaled on resume: {run2_labels:?}"
        );
        // Each child's effect appears in exactly ONE EffectRecorded across BOTH runs.
        for i in 0..3 {
            let eid = effect_id(&format!("m/{i}"), 0, 0);
            let count = all
                .iter()
                .filter(|(_, e)| {
                    matches!(e, JournalEvent::EffectRecorded { effect_id, .. } if effect_id == &eid)
                })
                .count();
            assert_eq!(count, 1, "child {i}'s effect recorded exactly once");
        }
    }

    /// Acceptance 8 (Consolidate) — a completed `Consolidate` replays on resume
    /// WITHOUT re-spending its synthesis body: its body effect is memoized (no
    /// gateway call) and it is not re-journaled. Only the unfinished tail runs.
    #[tokio::test]
    async fn resume_replays_a_completed_consolidate_without_respending_its_body() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let cons = NodeId("cons".into());
        let n3 = NodeId("n3".into());
        // Map m (3 ok) → Consolidate cons (soft m) → ModelCall n3 (hard cons).
        let graph = Graph {
            nodes: vec![
                Node {
                    id: NodeId("m".into()),
                    kind: NodeKind::Map {
                        body: MapBody::ModelCall { chain: "c".into() },
                        over: map_items(["i0", "i1", "i2"]),
                        concurrency: 4,
                        aggregation: Aggregation::BestEffort,
                    },
                    deps: vec![],
                },
                Node {
                    id: cons.clone(),
                    kind: NodeKind::Consolidate {
                        over: NodeId("m".into()),
                        min_viable: 1,
                        body: MapBody::ModelCall { chain: "c".into() },
                    },
                    deps: vec![Dep::soft("m")],
                },
                Node {
                    id: n3.clone(),
                    kind: model_call("c", "tail"),
                    deps: vec![Dep::hard("cons")],
                },
            ],
        };

        // Run 1: 3 children (calls 1–3) + cons body (call 4) succeed, n3 fails
        // (call 5) → no RunCompleted.
        let (gw1, calls1) = failing_after_gateway(4).await;
        let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1");
        let out1 = exec1
            .run(run, &graph)
            .await
            .expect("run 1 yields an outcome");
        assert!(out1.failed.is_some(), "n3 fails in run 1");
        assert_eq!(
            calls1.lock().unwrap().len(),
            5,
            "run 1: 3 children + cons body + failing n3"
        );
        let before = journal.load(run).await.unwrap().len();

        // Run 2: resume on a fresh gateway → only n3 runs live.
        let (gw2, calls2) = recording_gateway().await;
        let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1");
        let out2 = exec2.start(run, &graph).await.expect("resume completes");
        assert!(out2.failed.is_none(), "{:?}", out2.failed);
        assert!(out2.completed.contains(&cons) && out2.completed.contains(&n3));

        // Re-spend nothing: the run-2 gateway is called only for the tail n3 —
        // NOT for the Map's children and NOT for the Consolidate's body.
        let recorded2 = calls2.lock().unwrap().clone();
        assert_eq!(
            recorded2.len(),
            1,
            "resume re-called the gateway only for n3: {recorded2:?}"
        );

        // The Consolidate is not re-journaled, and its body effect stays exactly-once.
        let all = journal.load(run).await.unwrap();
        let run2_labels: Vec<String> = all[before..].iter().map(|(_, e)| label(e)).collect();
        assert!(
            !run2_labels
                .iter()
                .any(|l| l == "NodeStarted(cons)" || l == "NodeCompleted(cons)"),
            "the completed Consolidate is not re-journaled on resume: {run2_labels:?}"
        );
        let cons_eid = effect_id("cons", 0, 0);
        let body_count = all
            .iter()
            .filter(
                |(_, e)| matches!(e, JournalEvent::EffectRecorded { effect_id, .. } if effect_id == &cons_eid),
            )
            .count();
        assert_eq!(
            body_count, 1,
            "the Consolidate body effect recorded exactly once"
        );
    }
}
