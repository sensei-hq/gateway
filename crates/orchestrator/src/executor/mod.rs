//! The deterministic executor: drives a linear `ModelCall` graph through the
//! gateway, journaling every step so a crashed run can resume (Task 4).

use std::collections::HashMap;
use std::sync::Arc;

use gateway::Gateway;
use orchestrator_core::{
    Clock, ContentStore, ContextStore, EffectClass, EffectId, EffectOutput, ExecutionJournal,
    Graph, JournalEvent, NodeId, NodeKind, ObservationMeta, OrchestratorError, Registry, RunId,
    Seq, SystemClock, effect_id,
};

use crate::agent::tools::{ReconcileRegistry, ToolRegistry};

mod agent;
mod content;
mod durability;
mod fanout;
mod support;
use support::{
    build_request, consolidate_compaction_target, fold_journal, input_hash, project_agent_outputs,
    ready_nodes,
};

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
    /// The wall-clock an Observation's TTL is checked against (default
    /// `SystemClock`) — injected so tests can control time deterministically.
    clock: Arc<dyn Clock>,
    /// Reconcile providers, queried when a Mutation is in-doubt on resume.
    reconcilers: Arc<ReconcileRegistry>,
    /// The scoped blackboard (§8) node outputs publish to and agent prompts read
    /// dependency context from. Optional/injected — no store wired ⇒ every
    /// blackboard step is a no-op (slice-4 behavior byte-identical).
    context: Option<Arc<dyn ContextStore>>,
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
    /// Set when the run halted on a durable pause (§7.3) — e.g. an in-doubt
    /// Mutation whose reconcile was `Indeterminate`. Like `failed`, a pause
    /// suppresses `RunCompleted`; the run stays resumable.
    pub paused: Option<PauseInfo>,
}

/// A durable pause: the run stopped resumable (no `RunCompleted`) at `node`,
/// never blindly applying/memoizing the effect in question (§7.3).
#[derive(Debug, Clone)]
pub struct PauseInfo {
    pub node: NodeId,
    pub reason: String,
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
    /// Effect ids that journaled an `EffectIntent` (§7.3). An id in `intents` but
    /// NOT in `memo` (no `EffectRecorded`) is an **in-doubt** Mutation on resume.
    intents: std::collections::HashSet<EffectId>,
    /// Each `Observation` effect's recorded freshness + provenance (§7.1). A memo
    /// hit whose `fetched_at + ttl` has lapsed (per the injected `Clock`) is
    /// re-read instead of replayed.
    observations: HashMap<EffectId, ObservationMeta>,
}

/// The mutable scheduling state threaded through a `drive` loop: the accumulating
/// [`RunOutcome`] plus the `completed`/`terminal` node sets the ready-set
/// computation reads to decide the next round.
#[derive(Default)]
struct DriveState {
    outcome: RunOutcome,
    completed: std::collections::HashSet<NodeId>,
    terminal: std::collections::HashSet<NodeId>,
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
            clock: Arc::new(SystemClock),
            reconcilers: Arc::new(ReconcileRegistry::default()),
            context: None,
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

    /// Inject the wall-clock (default `SystemClock`) — Observation TTL reads it.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Attach reconcile providers, queried when a Mutation is in-doubt on resume.
    pub fn with_reconcilers(mut self, reconcilers: Arc<ReconcileRegistry>) -> Self {
        self.reconcilers = reconcilers;
        self
    }

    /// Wire the scoped blackboard (§8): completed node outputs publish to it, and
    /// an `Agent` node's prompt is assembled with its dependencies' outputs read
    /// from it. Injected (shared across the crash/resume seam). Requires a
    /// `ContentStore` (entries are CAS refs). No store ⇒ every blackboard step is
    /// a no-op, so behavior stays byte-identical.
    pub fn with_context_store(mut self, context: Arc<dyn ContextStore>) -> Self {
        self.context = Some(context);
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

        // Fold the journal into resume state (memo + started/completed sets +
        // each node's last output as a ref, no blob loaded).
        let terminal = events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted));
        let (fold, node_last_output, completed) = fold_journal(&events);

        if terminal {
            // Already done: return the folded outcome WITHOUT re-driving (which
            // would append a second `RunCompleted`). Materialize each node's final
            // output lazily from its folded ref (inline value, or a CAS blob) —
            // the only place the terminal fold touches content, bounded to one
            // read per node — then project each Agent node's raw output down to its
            // canonical `{model, text}` shape.
            let mut outcome = RunOutcome {
                completed,
                ..RunOutcome::default()
            };
            for (node, output) in &node_last_output {
                outcome
                    .outputs
                    .insert(node.clone(), self.materialize(output).await?);
            }
            project_agent_outputs(graph, &mut outcome.outputs);
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
        let mut state = DriveState::default();
        loop {
            let ready = ready_nodes(graph, &state.completed, &state.terminal);
            if ready.is_empty() {
                break;
            }
            for (index, node) in ready {
                // The immutable borrow of `state.outcome.outputs` (a Consolidate
                // reads its Map's result from it) ends when the future resolves,
                // before `apply_node_result` mutates `state`.
                let result = self
                    .run_node(run, index, node, fold, &state.outcome.outputs)
                    .await?;
                self.apply_node_result(run, graph, node, result, &mut state)
                    .await?;
            }
            // Round boundary (§5.2): checkpoint progress to the snapshot store,
            // OUT-OF-BAND (no journal event, so the control-flow log stays
            // byte-identical). Written even on a fresh `run` — harmlessly unused
            // unless the run later crashes and resumes.
            self.write_snapshot(run, &state.outcome).await?;
        }
        // A run with any failure OR a durable pause is not marked complete — it
        // stays resumable (the slice-1/2 contract), even though soft-dependent
        // branches ran.
        if state.outcome.failed.is_none() && state.outcome.paused.is_none() {
            self.append(run, JournalEvent::RunCompleted).await?;
        }
        Ok(state.outcome)
    }

    /// Fold one scheduled node's run result into the drive `state`. A
    /// **completed** node is recorded terminal (and, after a `Consolidate` over a
    /// `ModelCall` Map, its Map is compacted, §5.3). A **failed** node carries its
    /// output (a Map's manifest, never dropped — §3.4), records the run's first
    /// failure, marks itself terminal, and cascade-skips its hard-dependents; the
    /// run does NOT halt — soft-dependents still run (§3.3).
    async fn apply_node_result(
        &self,
        run: RunId,
        graph: &Graph,
        node: &orchestrator_core::Node,
        result: NodeExec,
        state: &mut DriveState,
    ) -> Result<(), OrchestratorError> {
        match result {
            NodeExec::Completed(output) => {
                state.outcome.outputs.insert(node.id.clone(), output);
                state.outcome.completed.push(node.id.clone());
                state.completed.insert(node.id.clone());
                state.terminal.insert(node.id.clone());
                if let Some(over) = consolidate_compaction_target(graph, node) {
                    self.compact_map(run, over, &state.outcome).await?;
                }
            }
            NodeExec::Failed { message, output } => {
                if let Some(output) = output {
                    state.outcome.outputs.insert(node.id.clone(), output);
                }
                if state.outcome.failed.is_none() {
                    state.outcome.failed = Some((node.id.clone(), message));
                }
                state.terminal.insert(node.id.clone());
                self.cascade_skip_from(
                    run,
                    graph,
                    &node.id,
                    &mut state.terminal,
                    &mut state.outcome,
                )
                .await?;
            }
            NodeExec::Paused { reason } => {
                // Durable pause (§7.3): record it (first pause wins), mark terminal,
                // and do NOT cascade. The run stays resumable — `drive` suppresses
                // `RunCompleted` while `paused` is set.
                if state.outcome.paused.is_none() {
                    state.outcome.paused = Some(PauseInfo {
                        node: node.id.clone(),
                        reason,
                    });
                }
                state.terminal.insert(node.id.clone());
            }
        }
        Ok(())
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
                                observation: None,
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
                match self.drive_agent(run, &node.id, agent, input, fold).await? {
                    AgentStep::Completed(output) => Ok(NodeExec::Completed(output)),
                    AgentStep::Failed(message) => Ok(NodeExec::Failed {
                        message,
                        output: None,
                    }),
                    AgentStep::Paused(reason) => Ok(NodeExec::Paused { reason }),
                }
            }
            NodeKind::Map { .. } => self.run_map(run, node, fold).await,
            NodeKind::Consolidate { .. } => {
                self.run_consolidate(run, node, prior_outputs, fold).await
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

/// The terminal result of one `Agent` node: a completed output, a node-level
/// failure (budget/max-steps/gateway/tool) already journaled as `NodeFailed`, or
/// a durable **pause** — an in-doubt Mutation whose reconcile was `Indeterminate`
/// (§7.3), journaled as `RunPaused`, never blindly applied.
enum AgentStep {
    Completed(serde_json::Value),
    Failed(String),
    Paused(String),
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
    /// The node halted on a durable pause (§7.3) — the run stops resumable (no
    /// `RunCompleted`), never blindly applying the in-doubt effect.
    Paused {
        reason: String,
    },
}

#[cfg(test)]
mod tests;
