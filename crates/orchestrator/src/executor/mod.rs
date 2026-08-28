//! The deterministic executor: drives a linear `ModelCall` graph through the
//! gateway, journaling every step so a crashed run can resume (Task 4).

use std::collections::HashMap;
use std::sync::Arc;

use gateway::Gateway;
use orchestrator_core::{
    AgentRef, Clock, ContentStore, ContextKey, ContextRef, ContextStore, EffectClass, EffectId,
    EffectOutput, ExecutionJournal, Graph, JournalEvent, NodeId, NodeKind, ObservationMeta,
    OrchestratorError, OrchestratorHooks, PLANNER_AREA, Planner, PlannerSelector, Registry,
    RegistryHandle, RunId, Scope, Seq, SystemClock, TokenBudget, effect_id,
};

use crate::agent::tools::{ReconcileRegistry, ToolRegistry};

mod agent;
mod branch;
mod content;
mod dispatch;
mod durability;
mod expand;
mod fanout;
mod gate;
mod human;
pub(crate) mod selector;
mod signal;
mod subgraph;
mod support;
use support::{
    GatewayDisposition, build_request, classify_gateway_error, consolidate_compaction_target,
    fold_journal, input_hash, project_agent_outputs, ready_nodes,
};

/// The deterministic executor over a durable journal, wired to the gateway.
#[derive(Clone)]
pub struct Executor {
    gateway: Arc<Gateway>,
    journal: Arc<dyn ExecutionJournal>,
    version: String,
    registry: Arc<Registry>,
    tools: Arc<ToolRegistry>,
    max_steps: usize,
    /// Max nesting depth (Subgraph levels; SP-3 self-DoS backstop). Default 8.
    max_depth: usize,
    concurrency: usize,
    /// The content-addressed store (§7.4) an over-threshold effect output is
    /// split into. `None` (the default) means no CAS is wired, so every output
    /// stays inline in the journal (the slice-1/2 behavior); wire a shared store
    /// via [`with_content_store`](Self::with_content_store) to enable the split —
    /// shared across the crash/resume boundary so a resume reads blobs back.
    content: Option<Arc<dyn ContentStore>>,
    /// An optional secret [`Redactor`](orchestrator_core::Redactor) (SP-4 s2) applied
    /// to every effect output at the two LEAF sites BEFORE it is journaled or fed back
    /// to the agent. `None` (the default) ⇒ outputs pass through verbatim (the slice-1
    /// behavior, byte-identical). Pure ⇒ live == journaled == replayed, so a resume
    /// reproduces the scrub exactly (no determinism drift).
    redactor: Option<Arc<dyn orchestrator_core::Redactor>>,
    /// An optional [`CredentialBroker`](orchestrator_core::CredentialBroker) (SP-4) the
    /// executor resolves a tool's declared credential refs against, injecting the secrets
    /// into the per-call `ToolContext` (Task 3). `None` (the default) ⇒ no credentials are
    /// resolved — inert until wired.
    credential_broker: Option<Arc<dyn orchestrator_core::CredentialBroker>>,
    /// SP-4 s3: base dir for the per-run workspace jail (`base/<run_id>/`). `None` ⇒ no fs
    /// tools / byte-identical. Set via [`with_workspace_root`](Self::with_workspace_root).
    workspace_root_base: Option<std::path::PathBuf>,
    /// SP-4 s4: the injected OS-confinement backend for the `shell` tool (default `None` ⇒
    /// the tool refuses loud). Set via [`with_sandbox`](Self::with_sandbox).
    sandbox: Option<Arc<dyn crate::agent::sandbox::Sandbox>>,
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
    /// Best-effort observability hooks (§15). `None` ⇒ no firing (byte-identical).
    hooks: Option<Arc<dyn OrchestratorHooks>>,
    /// A hot-reload handle (SP-2 slice 5). When wired, each run pins the handle's
    /// current registry + config generation at entry. `None` ⇒ the fixed `registry`.
    handle: Option<RegistryHandle>,
    /// The injected planner an `Expand` node produces its subgraph from (SP-3
    /// slice 3). `None` ⇒ an `Expand` node fails loudly (byte-identical for graphs
    /// without `Expand`).
    planner: Option<Arc<dyn Planner>>,
    /// The injected selector a `PlannerRef::Select` node uses to pick a planner agent
    /// (slice 4B). `None` ⇒ a `Select` node fails loudly.
    selector: Option<Arc<dyn PlannerSelector>>,
    /// Max runtime expansions (`PlanDelta`s) per run — a self-DoS cap. Default 32.
    max_expansions: usize,
    /// Max cumulative spliced-node count per run — a self-DoS cap. Default 512.
    max_nodes: usize,
    /// Run-scoped expansion counters (seeded from the journal on resume) the caps
    /// are checked against. Reset per run by `run_inner`/`start_inner`.
    expansion_counters: Arc<ExpansionCounters>,
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
    /// Effect ids that journaled an `EffectIntent` → the journaled idempotency key
    /// (§7.3, SP-4 s5). An id here with no matching `EffectRecorded` is in-doubt on
    /// resume; reconcile queries the provider by THIS key.
    intents: std::collections::HashMap<EffectId, String>,
    /// Each `Observation` effect's recorded freshness + provenance (§7.1). A memo
    /// hit whose `fetched_at + ttl` has lapsed (per the injected `Clock`) is
    /// re-read instead of replayed.
    observations: HashMap<EffectId, ObservationMeta>,
    /// Blackboard entries folded from `ContextWrite` events (§8). On resume the
    /// store is rehydrated from these (refs, no blob load), and a completed node
    /// whose key is already here is NOT re-published — the guard against a
    /// memoized replay re-`put`ting (which would collide) or re-journaling.
    context: HashMap<(Scope, ContextKey), ContextRef>,
    /// Runtime graph expansions folded from `PlanExpanded` events (§4.4). The
    /// structural analog of `memo`: on resume, `run_expand` replays the journaled
    /// subgraph for a node found here — never re-invoking the planner.
    expansions: HashMap<NodeId, Graph>,
    /// Planner selections folded from `PlannerSelected` (§4.5). On resume the `Select`
    /// arm reuses the recorded agent — the selector is NOT re-invoked.
    selections: std::collections::HashMap<NodeId, orchestrator_core::AgentRef>,
    /// SP-6 s1: signals delivered per `AwaitSignal` node, folded from `SignalReceived`.
    /// LAST delivery wins (`insert` overwrites) — an operator must be able to correct a
    /// mistaken decision before the run resumes, so a later signal supersedes an earlier
    /// one for the same node.
    signals: HashMap<NodeId, serde_json::Value>,
    /// SP-6 s1: what each WAITING node recorded when it began waiting, folded from
    /// `SignalAwaited` and — since SP-6 s2 and s3 — from `GateAwaited` and `AgentAwaited`
    /// too, so that "has this node begun asking?" has ONE answer for all THREE waiting
    /// kinds. Keep this list of writers current: every one of them is an
    /// `entry().or_insert` in `fold_journal`, and
    /// [`wait_or_expire`](Executor::wait_or_expire) reads the map without knowing which
    /// kind wrote it — so a reader that reasons about which kinds can be present (see
    /// `run_human_gate`'s missing-menu arm) is reasoning off THIS sentence.
    ///
    /// FIRST record wins — the opposite of `signals`, and deliberately
    /// so: if a later `SignalAwaited` could overwrite it, every resume would push the
    /// deadline forward, and a run force-woken every ten minutes with a one-hour timeout
    /// would NEVER expire.
    ///
    /// The VALUE is itself an `Option`, so the two layers mean different things:
    /// *key absent* = this node has never begun waiting; `Some(None)` = it began waiting
    /// with **no deadline** — the indefinite HITL gate, and since s3 also the indefinite
    /// human agent (`AgentBacking::Human { timeout: None }`). Folding that `None` as a
    /// real value — rather than dropping it — is what makes the deadline-less arm of
    /// [`run_await_signal`](Executor::run_await_signal) node-keyed idempotent: without it
    /// the node re-journals `SignalAwaited` on every drive, and a re-drive is NOT
    /// human-bounded (a dep-free sibling that pauses with a deadline in the same round
    /// keeps the whole run auto-wakeable). The same holds for the s2 and s3 kinds, which
    /// re-journal `GateAwaited`/`AgentAwaited` respectively — see
    /// `a_deadline_less_gate_records_that_it_began_asking` and
    /// `a_deadline_less_human_agent_records_that_it_began_asking`.
    deadlines: HashMap<NodeId, Option<chrono::DateTime<chrono::Utc>>>,
    /// SP-6 s2: each `HumanGate`'s decision, folded from `GateDecided`. LAST wins, like
    /// `signals` and for the same reason: an operator must be able to correct a mistaken
    /// decision before the run resumes.
    gate_decisions: HashMap<NodeId, GateDecision>,
    /// SP-6 s2: the MENU each `HumanGate` published when it began asking, folded from
    /// `GateAwaited`. FIRST wins — the human was shown THIS menu, and a later ask must
    /// not retroactively change what their answer meant.
    ///
    /// `deadlines` is folded from `GateAwaited` too, so the "has this node begun asking?"
    /// question stays in one place — as of s3 for all three waiting kinds, not two.
    menus: HashMap<NodeId, Vec<orchestrator_core::GateOption>>,
    /// SP-6 s3: each human-backed agent node's answer, from `AgentAnswered`. LAST wins,
    /// like `signals`/`gate_decisions` and for the same reason: an operator must be able
    /// to correct an answer before the run resumes.
    agent_answers: HashMap<NodeId, AgentAnswer>,
    /// SP-6 s3: the QUESTION each human-backed agent node published when it began
    /// asking, from `AgentAwaited`. FIRST wins — the human was asked THIS question, and
    /// a later ask must not retroactively change what their answer was to.
    ///
    /// `deadlines` is folded from `AgentAwaited` too, for the same reason it is folded
    /// from `GateAwaited`: "has this node begun asking?" stays one question, and
    /// `wait_or_expire` reads only `deadline_for`.
    agent_prompts: HashMap<NodeId, String>,
    /// SP-6 s1 (whole-slice review): each node's journaled `NodeFailed` message, FIRST
    /// wins. Read through exactly ONE consumer — [`gate_precheck`](Executor::gate_precheck),
    /// the shared arm 0 of the two WAITING node kinds, for which a failure is TERMINAL (an
    /// expired gate stays expired). SP-6 s2 moved that read out of `run_await_signal` and
    /// into the shared helper, so it now has two CALLERS —
    /// [`run_await_signal`](Executor::run_await_signal) and
    /// [`run_human_gate`](Executor::run_human_gate) — but still one reader.
    ///
    /// It is deliberately not consulted anywhere else, and the fence is on the READER, not
    /// on the caller count: a third node kind may read this map only by being a waiting
    /// kind that calls `gate_precheck` first. A `NodeFailed` does not make a node terminal
    /// in general: a `ModelCall` or `Agent` node whose provider died journals one and
    /// RE-ATTEMPTS on the next drive, which is the documented resume contract (see
    /// `a_paused_gated_run_reattempts_and_completes_on_resume`, and `resolve_context`'s note
    /// that a failed node "carries no memo and re-runs on resume"). Making this map
    /// authoritative for every kind would silently delete retry-on-resume, so the
    /// generalization is refused: only a node kind whose failure is by definition
    /// irreversible — a deadline that has passed — may read it.
    failed: HashMap<NodeId, String>,
    /// SP-DATA-5 spend ledger, keyed by effect id — NOT a running total over events.
    /// The two-phase Mutation path can append a second `EffectRecorded` for one id (an
    /// in-doubt `Confirmed` reconcile); keying absorbs that, a sum would double-count
    /// it on every resume.
    usage: HashMap<EffectId, orchestrator_core::TokenUsage>,
    /// The effective cap: `RunStarted.budget`, then the latest `BudgetRaised` (latest
    /// wins). `None` for an unbudgeted run — the gate never fires.
    budget: Option<u64>,
    /// SP-DATA-5: tokens dispatched by THIS drive, not yet visible in `usage`.
    ///
    /// A `Fold` is built once per drive (from the journal on resume, or empty-but-for-
    /// the-budget on a fresh run) and shared as `&Fold` by every node, so `usage` alone
    /// is a snapshot of the ledger as it stood when the drive STARTED. Without this
    /// counter the gate re-reads that same frozen number before every call — and a
    /// freshly submitted run, whose journaled spend is 0 by definition, would never gate
    /// at all. Interior-mutable (and shared with a `Map`'s concurrent children) because
    /// the fold is handed out immutably; see [`dispatch::Meter`] for the ordering
    /// rationale.
    live_spend: Arc<std::sync::atomic::AtomicU64>,
    /// SP-DATA-5 (whole-slice review, Critical 1): the 1-permit gate a BUDGETED run
    /// holds across its whole check→dispatch→charge sequence, so at most one model
    /// call per run is ever in flight and `live_spend` is current before the next
    /// gate read. One `Fold` per drive ⇒ one gate per run-drive, shared by every
    /// node including a `Map`'s concurrent children and any nested Subgraph/Loop
    /// (which are handed this same `&Fold`).
    ///
    /// Taken ONLY when `budget.is_some()` — see [`dispatch::Meter`] for the trade
    /// this makes and why an unbudgeted run must never touch it.
    serial_gate: Arc<tokio::sync::Mutex<()>>,
}

/// SP-6 s3: a folded `AgentAnswered`.
#[derive(Debug, Clone, PartialEq)]
struct AgentAnswer {
    text: String,
    /// ATTRIBUTION, NOT AUTHENTICATION — see `JournalEvent::AgentAnswered`.
    actor: String,
}

/// SP-6 s2: a folded `GateDecided`.
#[derive(Debug, Clone, PartialEq)]
struct GateDecision {
    option: String,
    /// ATTRIBUTION, NOT AUTHENTICATION — see `JournalEvent::GateDecided`.
    actor: String,
    note: Option<String>,
}

impl Fold {
    /// Tokens this run had spent as of the journal this fold was built from.
    ///
    /// Idempotent across any number of resumes because it sums over effect ids
    /// (`usage`'s keys), never over raw events.
    fn journaled_spend(&self) -> u64 {
        // `saturating_add` is deliberate and is the ONE place saturation is right
        // here: overflowing `u64` from summed `u32` token counts needs ~4 billion
        // maximal effects, and saturating HIGH makes the gate MORE conservative (it
        // pauses the run), where a wrapping add could reset the ledger near zero and
        // let a run spend unbounded past its cap.
        self.usage
            .values()
            .map(|u| u64::from(u.total_tokens))
            .fold(0u64, |acc, t| acc.saturating_add(t))
    }

    /// Total tokens this run has spent: journaled + in-flight this drive.
    ///
    /// The in-flight half is zero at the start of every drive and is subsumed into the
    /// journaled half by the next fold, so the two can never double-count one call.
    fn spent(&self) -> u64 {
        self.journaled_spend()
            .saturating_add(self.live_spend.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// The run's effective token cap, or `None` if unbudgeted.
    fn budget(&self) -> Option<u64> {
        self.budget
    }

    /// This fold's ledger as the metered-dispatch chokepoint consumes it. Borrowing the
    /// live counter (rather than copying two scalars out) is what lets spend accumulate
    /// WITHIN a drive — see [`dispatch::Meter`].
    fn meter(&self) -> dispatch::Meter<'_> {
        dispatch::Meter::new(
            self.journaled_spend(),
            self.budget,
            &self.live_spend,
            &self.serial_gate,
        )
    }

    /// SP-6 s1: the folded signal for an `AwaitSignal` node, if one has been delivered
    /// (§6.2's three-way read, arm 1). `None` for a node that has never been signalled.
    fn signal_for(&self, node: &NodeId) -> Option<&serde_json::Value> {
        self.signals.get(node)
    }

    /// SP-6 s1: what a waiting node recorded when it began waiting.
    ///
    /// Two layers, and they are not the same question:
    /// - `None` — this node has NEVER begun waiting (no `SignalAwaited`/`GateAwaited`/
    ///   `AgentAwaited` — the three writers of [`Fold::deadlines`], all of them).
    /// - `Some(None)` — it began waiting with **no deadline** (the indefinite gate, or
    ///   since s3 the indefinite human agent).
    /// - `Some(Some(t))` — it began waiting with the absolute deadline `t`.
    ///
    /// Read through [`wait_or_expire`](Executor::wait_or_expire) — SP-6 s2's shared arm,
    /// called on EVERY execution by BOTH `run_await_signal` and `run_human_gate`. (s3's
    /// `AgentAwaited` is already a WRITER as of this task; its reader, `run_human_agent`,
    /// lands in the next one — the fold deliberately goes in first so the durable record
    /// exists before anything reads it.) It is the durable half of the never-recompute
    /// rule; the caller must not fall back to `now + timeout` when this returns `Some`, in
    /// EITHER of its two inner shapes.
    fn deadline_for(&self, node: &NodeId) -> Option<Option<chrono::DateTime<chrono::Utc>>> {
        self.deadlines.get(node).copied()
    }

    /// SP-6 s1: the failure this node already journaled, if any — see [`Fold::failed`] for
    /// why only [`gate_precheck`](Executor::gate_precheck), on behalf of the two waiting
    /// node kinds, may act on it.
    fn failure_for(&self, node: &NodeId) -> Option<&str> {
        self.failed.get(node).map(String::as_str)
    }

    /// SP-6 s2: the decision folded for this `HumanGate`, if a human has answered.
    ///
    /// Read by [`run_human_gate`](Executor::run_human_gate) only AFTER the ask has been
    /// journaled and only AFTER `gate_precheck`/`wait_or_expire` have had their say — an
    /// answer counts only while the node was still asking.
    fn gate_decision_for(&self, node: &NodeId) -> Option<&GateDecision> {
        self.gate_decisions.get(node)
    }

    /// SP-6 s2: the menu this gate published when it began asking.
    ///
    /// `None` = it has not asked yet — the trigger for `run_human_gate` to journal
    /// `GateAwaited` FIRST, before it reads any decision (§6.2). That ordering is why a
    /// decision-without-a-menu never arises, rather than something to detect: validating
    /// against the GRAPH in that one path would reintroduce exactly the non-durable menu
    /// §4 rejects, so the ask is unconditional and the answer is read against the menu it
    /// just published.
    ///
    /// It is therefore read by [`run_human_gate`](Executor::run_human_gate) only on the
    /// `Waiting` arm — the drive that FIRST asks resolves against the menu it just
    /// journaled, which this snapshot of the journal cannot yet see.
    fn menu_for(&self, node: &NodeId) -> Option<&[orchestrator_core::GateOption]> {
        self.menus.get(node).map(Vec::as_slice)
    }

    /// SP-6 s3: the answer folded for this human-backed agent node.
    ///
    /// Task 3 carried an `expect(dead_code)` here because the fold landed one task ahead
    /// of its only non-test consumer. Task 4 shipped that consumer — `run_human_agent`
    /// (`executor/human.rs`) reads this to complete the node — so the attribute is gone,
    /// exactly as its own doc said it would be: an `expect` that is no longer needed is
    /// itself a `-D warnings` failure, which is what made it delete itself rather than
    /// silently outlive its reason the way a stale `allow` would.
    fn agent_answer_for(&self, node: &NodeId) -> Option<&AgentAnswer> {
        self.agent_answers.get(node)
    }

    /// SP-6 s3: the question this node published when it began asking. `None` = it has
    /// not asked yet — the trigger for `run_human_agent` to journal `AgentAwaited`
    /// FIRST, before reading any answer, so an answer without a question never arises.
    ///
    /// See [`Fold::agent_answer_for`] for why this is `expect(dead_code)` and not `allow`.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "SP-6 s3 Task 4's run_human_agent is the consumer; the fold lands first"
        )
    )]
    fn prompt_for(&self, node: &NodeId) -> Option<&str> {
        self.agent_prompts.get(node).map(String::as_str)
    }
}

/// SP-DATA-5 Task 5: a run's folded `(spent, budget)`, exposed so `torii run status`
/// can display spend without re-deriving it.
///
/// Deliberately routes through the SAME `fold_journal`/`Fold` the metered-dispatch
/// gate (Task 3) itself uses, rather than handing the caller raw events to sum
/// independently. `fold_journal` keys `EffectRecorded.usage` by effect id
/// specifically so a duplicate record — reachable via the two-phase Mutation path's
/// in-doubt `Confirmed` reconcile — counts once, not once per event (see
/// `Fold::spent`'s doc comment, and the Task 2 test that mutation-verifies it). A
/// second, independently-written sum over the raw event stream would inevitably
/// diverge from this one — the exact drift the s2 secret-redactor review warned
/// about when it found a chokepoint bypassed by one of several call sites — and the
/// diverged copy would stay silently wrong on every resume, growing with each one.
pub fn spend_of(events: &[(Seq, JournalEvent)]) -> (u64, Option<u64>) {
    let (fold, _, _) = fold_journal(events);
    (fold.spent(), fold.budget())
}

/// Run-scoped tallies for the expansion caps (§4.5). Only ever mutated from the
/// sequential top-level drive loop (a `Map`'s concurrency wraps `ModelCall`/`Agent`
/// bodies, never an `Expand`), so `Relaxed` ordering is sufficient — the check is a
/// self-DoS backstop, not a synchronization primitive.
#[derive(Default)]
struct ExpansionCounters {
    expansions: std::sync::atomic::AtomicUsize,
    nodes: std::sync::atomic::AtomicUsize,
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
            max_depth: 8,
            concurrency: 8,
            content: None,
            redactor: None,
            credential_broker: None,
            workspace_root_base: None,
            sandbox: None,
            cas_threshold: 4096,
            clock: Arc::new(SystemClock),
            reconcilers: Arc::new(ReconcileRegistry::default()),
            context: None,
            hooks: None,
            handle: None,
            planner: None,
            selector: None,
            max_expansions: 32,
            max_nodes: 512,
            expansion_counters: Arc::new(ExpansionCounters::default()),
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

    /// Wire a secret [`Redactor`](orchestrator_core::Redactor) (SP-4 s2). Default
    /// none ⇒ effect outputs are journaled/fed-back verbatim (byte-identical).
    /// Recommended for production: `.with_redactor(Arc::new(PatternRedactor::default()))`.
    pub fn with_redactor(mut self, redactor: Arc<dyn orchestrator_core::Redactor>) -> Self {
        self.redactor = Some(redactor);
        self
    }

    /// Wire a [`CredentialBroker`](orchestrator_core::CredentialBroker) (SP-4). Default none.
    /// (Task 3 wires the resolve+inject: a tool that declares a credential ref the broker
    /// can't resolve — or with no broker wired — will fail loud, never a silent missing
    /// credential. In THIS commit the field is inert.)
    pub fn with_credential_broker(
        mut self,
        broker: Arc<dyn orchestrator_core::CredentialBroker>,
    ) -> Self {
        self.credential_broker = Some(broker);
        self
    }

    /// SP-4 s3: root a durable per-run workspace jail at `base/<run_id>/`. Default none ⇒
    /// byte-identical, no fs tools. Confined `fs_write`/`fs_read` tools resolve their targets
    /// within the canonical per-run dir; the executor pre-checks each declared path.
    pub fn with_workspace_root(mut self, base: impl Into<std::path::PathBuf>) -> Self {
        self.workspace_root_base = Some(base.into());
        self
    }

    /// SP-4 s4: wire the subprocess sandbox backend (e.g. `MacosSandbox`) used by the `shell`
    /// tool. Default `None` ⇒ `shell` refuses loud (fail-closed — never an unconfined run).
    pub fn with_sandbox(mut self, sandbox: Arc<dyn crate::agent::sandbox::Sandbox>) -> Self {
        self.sandbox = Some(sandbox);
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

    /// Wire a hot-reloadable [`RegistryHandle`] (SP-2 slice 5). Each `run`/`start`
    /// pins the handle's current registry + generation; a reload bumps the
    /// generation, folded into the fence version so a run uses one generation.
    /// Supersedes [`with_registry`](Self::with_registry) when both are set — the
    /// handle's current registry is pinned per run, overwriting the fixed one.
    pub fn with_registry_handle(mut self, handle: RegistryHandle) -> Self {
        self.handle = Some(handle);
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

    /// Set the max nesting depth (Subgraph self-DoS cap; default 8).
    pub fn with_max_depth(mut self, n: usize) -> Self {
        self.max_depth = n;
        self
    }

    /// Attach the planner an `Expand` node produces its subgraph from (SP-3 slice 3).
    pub fn with_planner(mut self, planner: Arc<dyn Planner>) -> Self {
        self.planner = Some(planner);
        self
    }

    /// Attach the planner selector a `PlannerRef::Select` node uses (slice 4B).
    pub fn with_planner_selector(mut self, selector: Arc<dyn PlannerSelector>) -> Self {
        self.selector = Some(selector);
        self
    }

    /// Set the max runtime expansions (`PlanDelta`s) per run (self-DoS cap; default 32).
    pub fn with_max_expansions(mut self, n: usize) -> Self {
        self.max_expansions = n;
        self
    }

    /// Set the max cumulative spliced-node count per run (self-DoS cap; default 512).
    pub fn with_max_nodes(mut self, n: usize) -> Self {
        self.max_nodes = n;
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
    /// an `Agent` node's prompt is assembled with its `Hard` dependencies' outputs
    /// read from it. The store carries its own CAS (entries are content refs); on
    /// resume it is **rebuilt fresh** from the journaled `ContextWrite`s (via
    /// `insert_ref`), so only its backing CAS must persist across the crash seam —
    /// pass a fresh `ContextStore` over the same CAS on resume, not the original
    /// in-memory instance. No store ⇒ every blackboard step is a no-op, so behavior
    /// stays byte-identical.
    pub fn with_context_store(mut self, context: Arc<dyn ContextStore>) -> Self {
        self.context = Some(context);
        self
    }

    /// Attach best-effort observability hooks (§15): fired at run/node/agent/
    /// context lifecycle points, they never affect execution or determinism, and
    /// do not double-count on resume. No hooks wired ⇒ zero firing (byte-identical).
    pub fn with_hooks(mut self, hooks: Arc<dyn OrchestratorHooks>) -> Self {
        self.hooks = Some(hooks);
        self
    }

    /// A per-run clone with the registry + fence version pinned from a
    /// `RegistryHandle` snapshot (handle cleared, so the pinned copy resolves the
    /// fixed registry directly — no double-pin).
    fn pinned(mut self, registry: Arc<Registry>, generation: u64) -> Self {
        self.version = format!("{}#cfg{}", self.version, generation);
        self.registry = registry;
        self.handle = None;
        self
    }

    /// A per-run clone with FRESH expansion counters seeded to `(expansions, nodes)`
    /// — 0/0 for a fresh `run`, or the journal's expansion tally for a resume — so the
    /// caps span the crash seam and every nested `run_expand` shares one counter.
    fn with_expansion_seed(mut self, expansions: usize, nodes: usize) -> Self {
        use std::sync::atomic::AtomicUsize;
        self.expansion_counters = Arc::new(ExpansionCounters {
            expansions: AtomicUsize::new(expansions),
            nodes: AtomicUsize::new(nodes),
        });
        self
    }

    /// Execute a fresh linear graph end-to-end: journal `RunStarted`, then drive
    /// every node with an empty memo (nothing has run yet). Unbudgeted — delegates
    /// to [`run_budgeted`](Self::run_budgeted) with `None`, so every pre-SP-DATA-5
    /// caller (all of them, until Task 5 wired a CLI flag to reach the budgeted
    /// twin) stays byte-identical.
    pub async fn run(&self, run: RunId, graph: &Graph) -> Result<RunOutcome, OrchestratorError> {
        self.run_budgeted(run, graph, None).await
    }

    /// SP-DATA-5 Task 5: like [`run`](Self::run), but journals `budget` on
    /// `RunStarted` so the metered-dispatch gate (Task 3) can pause the run once
    /// its folded spend meets the cap.
    ///
    /// A NEW method rather than a third parameter on `run` itself, deliberately:
    /// `run(run, &graph)` has on the order of a hundred existing call sites across
    /// this crate's tests plus `Scheduler::submit`'s own production caller, every
    /// one of them unbudgeted. Widening `run`'s signature would force each of those
    /// to thread a `None` through for no behavior change — pure churn — where a
    /// same-behavior delegating twin costs nothing and touches nothing.
    pub async fn run_budgeted(
        &self,
        run: RunId,
        graph: &Graph,
        budget: Option<TokenBudget>,
    ) -> Result<RunOutcome, OrchestratorError> {
        if let Some(h) = &self.handle {
            let (registry, generation) = h.snapshot();
            return self
                .clone()
                .pinned(registry, generation)
                .run_inner(run, graph, budget)
                .await;
        }
        self.run_inner(run, graph, budget).await
    }

    async fn run_inner(
        &self,
        run: RunId,
        graph: &Graph,
        budget: Option<TokenBudget>,
    ) -> Result<RunOutcome, OrchestratorError> {
        graph.validate_dag()?;
        let this = self.clone().with_expansion_seed(0, 0);
        this.append(
            run,
            JournalEvent::RunStarted {
                version: this.version.clone(),
                budget,
            },
        )
        .await?;
        // SP-DATA-5: a FRESH run has nothing journaled to fold, so the cap has to be
        // seeded into the fold by hand — `Fold::default()` alone would hand the drive
        // `budget: None` and the gate could never fire on a run's first drive at all,
        // however small the cap. (A resume gets the same value from `fold_journal`
        // reading the `RunStarted` this call just appended, plus any `BudgetRaised`.)
        let fold = Fold {
            budget: budget.map(|b| b.total_tokens),
            ..Default::default()
        };
        // The RUN's own graph: a human-backed `Agent` node here is at the one
        // position §5.5 permits.
        let outcome = this.drive(run, graph, &fold, false).await?;
        this.finalize_run(run, &outcome).await?;
        Ok(outcome)
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
        if let Some(h) = &self.handle {
            let (registry, generation) = h.snapshot();
            return self
                .clone()
                .pinned(registry, generation)
                .start_inner(run, graph)
                .await;
        }
        self.start_inner(run, graph).await
    }

    async fn start_inner(
        &self,
        run: RunId,
        graph: &Graph,
    ) -> Result<RunOutcome, OrchestratorError> {
        graph.validate_dag()?;
        let events = self
            .journal
            .load(run)
            .await
            .map_err(OrchestratorError::Journal)?;
        if events.is_empty() {
            // Nothing journaled → a fresh run (appends `RunStarted` itself). Already
            // pinned, so call `run_inner` directly (avoid a redundant handle re-check).
            // Unbudgeted: `start()` resumes an EXISTING submission — a real budget, if
            // any, is already journaled by whatever `submit` call created it. This
            // branch only fires for a run id that was never submitted at all, which is
            // not a product path `Scheduler::tick` reaches (it only re-drives runs its
            // own `submit` already journaled `RunStarted` for).
            return self.run_inner(run, graph, None).await;
        }

        // Version fence: the first recorded `RunStarted.version` must match ours.
        // Explicit `budget: _` (not `..`) so a FUTURE field added to `RunStarted`
        // forces a compile error here — a conscious decision, not silent absorption.
        // The fence compares the executor version string only; `budget` is
        // deliberately not fenced (a config-only change, not a code-version change).
        if let Some(recorded) = events.iter().find_map(|(_, e)| match e {
            JournalEvent::RunStarted { version, budget: _ } => Some(version.clone()),
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

        // Rehydrate the blackboard from folded `ContextWrite`s (§8) so a resumed
        // Agent node reads its dependencies' context identically to the original
        // run (deterministic prompt → memoized turns replay).
        //
        // Seed the expansion counters from the journaled expansions so the caps span
        // the crash seam, then rehydrate + resume off that per-run clone.
        let seed_nodes: usize = fold.expansions.values().map(|g| g.nodes.len()).sum();
        let this = self
            .clone()
            .with_expansion_seed(fold.expansions.len(), seed_nodes);
        this.rehydrate_context(&fold).await?;
        // The RUN's own graph: a human-backed `Agent` node here is at the one
        // position §5.5 permits.
        let outcome = this.drive(run, graph, &fold, false).await?;
        this.finalize_run(run, &outcome).await?;
        Ok(outcome)
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
    ///
    /// **`nested` says whether this drive is the RUN's own graph or one nested inside a
    /// node of it**, and it exists for exactly one consumer: SP-6 s3's rule that a
    /// human-backed role is legal only as a top-level `NodeKind::Agent`. `false` at the two
    /// run-level callers (`run_inner`/`start_inner`); `true` at [`drive_nested`], which is
    /// the single tail every `Subgraph`, `Branch` arm, `Loop`-`Subgraph` body and
    /// planner-spliced `Expand` graph goes through.
    ///
    /// It is threaded rather than derived from the node path because the property is about
    /// POSITION and nothing else carries it: `run_node` sees only a `&Node`, whose id is
    /// already namespaced (`"{loop}/0/review"`) but whose SHAPE — a `/`-bearing id — is
    /// also legal for a top-level node an author simply named that way. Deciding legality
    /// on the caller instead was the shipped defect: `run_node`'s `Agent` arm passed a
    /// hardcoded `true`, so wrapping the agent in a one-node `Subgraph` bypassed the rule
    /// entirely and delivered "a human re-answers every `Loop` iteration" — one of the four
    /// unbuilt features the refusal exists to prevent — through a trivial wrapper, and
    /// through any untrusted `Expand` planner that splices such a node.
    async fn drive(
        &self,
        run: RunId,
        graph: &Graph,
        fold: &Fold,
        nested: bool,
    ) -> Result<RunOutcome, OrchestratorError> {
        let mut state = DriveState::default();
        loop {
            let ready = ready_nodes(graph, &state.completed, &state.terminal);
            if ready.is_empty() {
                break;
            }
            for node in ready {
                // The immutable borrow of `state.outcome.outputs` (a Consolidate
                // reads its Map's result from it) ends when the future resolves,
                // before `apply_node_result` mutates `state`.
                let result = self
                    .run_node(run, node, fold, &state.outcome.outputs, nested)
                    .await?;
                self.apply_node_result(run, graph, node, result, fold, &mut state)
                    .await?;
            }
            // Round boundary (§5.2): checkpoint progress to the snapshot store,
            // OUT-OF-BAND (no journal event, so the control-flow log stays
            // byte-identical). Written even on a fresh `run` — harmlessly unused
            // unless the run later crashes and resumes.
            self.write_snapshot(run, &state.outcome).await?;
        }
        // NOTE: `drive` does NOT append `RunCompleted` — that is a RUN-level event,
        // appended once by the run-level callers (`run_inner`/`start_inner`) via
        // [`finalize_run`]. This matters for a `Subgraph` node, which drives its
        // nested DAG through `drive` in the SAME run (SP-3): a completing nested
        // drive must not emit a premature/duplicate `RunCompleted` for the whole
        // run. `drive` just returns the outcome; the finalizer decides completion.
        Ok(state.outcome)
    }

    /// Append `RunCompleted` iff the run's outcome is clean (no failure, no durable
    /// pause) — a RUN-level finalization done once by the top-level `run_inner`/
    /// `start_inner`, NOT inside [`drive`] (so a nested `Subgraph` drive can't emit
    /// a premature/duplicate one). A failed or paused run is left unmarked so it
    /// stays resumable (the slice-1/2 contract).
    async fn finalize_run(
        &self,
        run: RunId,
        outcome: &RunOutcome,
    ) -> Result<(), OrchestratorError> {
        if outcome.failed.is_none() && outcome.paused.is_none() {
            self.append(run, JournalEvent::RunCompleted).await?;
        }
        Ok(())
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
        fold: &Fold,
        state: &mut DriveState,
    ) -> Result<(), OrchestratorError> {
        match result {
            NodeExec::Completed(output) => {
                // Publish to the blackboard BEFORE moving `output` into `outputs`
                // (§8); fold-guarded so a memoized replay does not re-publish.
                self.publish_context(run, &node.id, &output, fold).await?;
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

    /// The sorted planner library: registry agents whose `area == PLANNER_AREA`, as
    /// `AgentRef`s (sorted by name for deterministic selection).
    fn planner_candidates(&self) -> Vec<AgentRef> {
        let mut c: Vec<AgentRef> = self
            .registry
            .agents()
            .filter(|a| a.area == PLANNER_AREA)
            .map(|a| AgentRef(a.name.clone()))
            .collect();
        c.sort_by(|x, y| x.0.cmp(&y.0));
        c
    }

    /// Enforce the expansion caps (§4.5) against the run-scoped counters, then tally
    /// the new expansion. A breach is a hard `Err` (self-DoS backstop); on success the
    /// counters advance by one expansion + `g.nodes.len()` nodes.
    fn check_expansion_budget(&self, g: &Graph) -> Result<(), OrchestratorError> {
        use std::sync::atomic::Ordering::Relaxed;
        if self.expansion_counters.expansions.load(Relaxed) + 1 > self.max_expansions {
            return Err(OrchestratorError::GlobalCapExceeded {
                cap: "max_expansions".into(),
                limit: self.max_expansions,
            });
        }
        if self.expansion_counters.nodes.load(Relaxed) + g.nodes.len() > self.max_nodes {
            return Err(OrchestratorError::GlobalCapExceeded {
                cap: "max_nodes".into(),
                limit: self.max_nodes,
            });
        }
        self.expansion_counters.expansions.fetch_add(1, Relaxed);
        self.expansion_counters
            .nodes
            .fetch_add(g.nodes.len(), Relaxed);
        Ok(())
    }

    /// Execute one node to a terminal result. A `ModelCall`'s structural effect id
    /// is keyed by the node's **id** (`effect_id(&node.id.0, 0, 0)` — node ids are
    /// unique within a graph and namespaced across nesting, e.g. `"{sub}/n1"`), so
    /// a nested `Subgraph`'s inner `ModelCall` can never collide with an outer one
    /// (an empty-prefix, index-based id would, since each fresh `drive`'s ready-set
    /// index restarts at 0). A memoized `ModelCall` replays with no gateway call and
    /// no new journal event; a live one journals `NodeStarted → EffectRecorded →
    /// NodeCompleted`. An `Agent` node delegates to [`drive_agent`](Self::drive_agent),
    /// which owns its own per-turn journaling. A determinism violation propagates as
    /// `Err` (halting the run before any gateway call). `prior_outputs` carries the
    /// outputs of already-completed nodes this round advances past — a `Consolidate`
    /// reads its Map's result from it.
    ///
    /// `nested` is [`drive`](Self::drive)'s position flag, forwarded untouched to the one
    /// arm that cares — see that function's doc for why the human-backed-role rule is
    /// decided on position rather than on the calling function.
    async fn run_node(
        &self,
        run: RunId,
        node: &orchestrator_core::Node,
        fold: &Fold,
        prior_outputs: &HashMap<NodeId, serde_json::Value>,
        nested: bool,
    ) -> Result<NodeExec, OrchestratorError> {
        match &node.kind {
            NodeKind::ModelCall { chain, payload } => {
                let eid = effect_id(&node.id.0, 0, 0);
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
                // SP-DATA-5: the ModelCall producer routes through the single metered
                // chokepoint — the budget gate cannot be bypassed here.
                match self.dispatch_metered(&request, &fold.meter()).await {
                    Ok(Ok(response)) => {
                        // SP-4 s2: route through the shared redaction chokepoint so a
                        // ModelCall node whose model echoes a secret is scrubbed too.
                        let output = self.model_output(&response);
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
                                // SP-DATA-5: the ModelCall producer — the real usage the
                                // provider reported, converted at the boundary.
                                usage: response.usage.map(content::convert_usage),
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
                    // SP-DATA-5: the chokepoint refused before spending (budget
                    // exhausted ⇒ a durable HOTL pause; unmetered ⇒ a node failure).
                    // `record_refusal` already journaled it.
                    Ok(Err(refusal)) => match self.record_refusal(run, &node.id, refusal).await? {
                        dispatch::RefusalKind::Paused(reason) => Ok(NodeExec::Paused { reason }),
                        dispatch::RefusalKind::Failed(message) => Ok(NodeExec::Failed {
                            message,
                            output: None,
                        }),
                    },
                    Err(error) => match classify_gateway_error(&error) {
                        // A fully-gated chain with a timed re-eligibility (§11.2):
                        // durable pause (resumable), never a bare fail. On resume
                        // the node re-attempts (no `EffectRecorded` was journaled).
                        GatewayDisposition::Pause {
                            resume_after,
                            reason,
                        } => {
                            self.append(
                                run,
                                JournalEvent::RunPaused {
                                    reason: reason.clone(),
                                    resume_after: Some(resume_after),
                                },
                            )
                            .await?;
                            Ok(NodeExec::Paused { reason })
                        }
                        GatewayDisposition::Fail(message) => {
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
                    },
                }
            }
            NodeKind::Agent {
                agent,
                input,
                phase,
            } => {
                let context = self.resolve_context(node).await?;
                match self
                    .drive_agent(
                        run,
                        &node.id,
                        agent,
                        input,
                        &context,
                        fold,
                        phase.as_deref(),
                        // The ONE site where SP-6 s3's human-backed role can be legal —
                        // and only when this drive is the RUN's own graph. `!nested`, not
                        // a hardcoded `true`: `drive_nested` re-enters `drive` → `run_node`
                        // for every `Subgraph`, `Branch` arm, `Loop`-`Subgraph` body and
                        // planner-spliced `Expand` graph, so a literal `true` here declared
                        // every one of those Agent nodes top-level. Review drove
                        // `Loop { body: Subgraph([Agent -> human]) }` against the literal
                        // and got two real journaled questions, at `lp/0/review` and
                        // `lp/1/review`.
                        !nested,
                    )
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
            NodeKind::Map { .. } => self.run_map(run, node, fold).await,
            NodeKind::Consolidate { .. } => {
                self.run_consolidate(run, node, prior_outputs, fold).await
            }
            NodeKind::Loop { .. } => self.run_loop(run, node, fold).await,
            NodeKind::Subgraph { .. } => self.run_subgraph(run, node, fold).await,
            NodeKind::Branch { .. } => self.run_branch(run, node, prior_outputs, fold).await,
            NodeKind::Expand { .. } => self.run_expand(run, node, fold).await,
            NodeKind::AwaitSignal { timeout } => {
                self.run_await_signal(run, node, *timeout, fold).await
            }
            // SP-6 s2: the typed gate, over the SAME shared waiting machinery as
            // `AwaitSignal` above (`gate_precheck`/`wait_or_expire`/`pause_awaiting`).
            // Like every other arm here it fails the NODE rather than panicking — a panic
            // in this match is not local: it unwinds through `Scheduler::tick`, which has
            // already claimed a batch of runs and taken their leases, leaving
            // `(Waking, next_wake: None)` rows that every later `tick()` reclaims and
            // dies on again.
            NodeKind::HumanGate { options, timeout } => {
                self.run_human_gate(run, node, options, *timeout, fold)
                    .await
            }
        }
    }

    /// Append one event, mapping a journal-backend error to a fatal
    /// `OrchestratorError::Journal` (strict — a journal write failure aborts the
    /// run; it is never swallowed). Returns the authoritative `Seq`.
    async fn append(&self, run: RunId, event: JournalEvent) -> Result<Seq, OrchestratorError> {
        // Clone for the post-journal hook match ONLY when hooks are wired, so the
        // no-hooks path stays allocation-free and byte-identical.
        let hook_event = self.hooks.as_ref().map(|_| event.clone());
        let seq = self
            .journal
            .append(run, event)
            .await
            .map_err(OrchestratorError::Journal)?;
        // Best-effort observability (§15). Fired AFTER a successful journal write
        // (a failed write surfaces its error and fires nothing). Because these fire
        // at the append site — which a resumed completed prefix does NOT re-hit
        // (fold-guarded) — hooks are replay-suppressed for free.
        if let (Some(h), Some(ev)) = (&self.hooks, &hook_event) {
            match ev {
                JournalEvent::RunStarted { .. } => h.on_run_started(run).await,
                JournalEvent::RunCompleted => h.on_run_completed(run).await,
                JournalEvent::RunPaused { reason, .. } => h.on_run_paused(run, reason).await,
                JournalEvent::NodeStarted { node } => h.on_node_started(run, node).await,
                JournalEvent::NodeCompleted { node } => h.on_node_completed(run, node).await,
                JournalEvent::NodeFailed { node, error } => {
                    h.on_node_failed(run, node, error).await
                }
                JournalEvent::NodeSkipped { node } => h.on_node_skipped(run, node).await,
                JournalEvent::ContextWrite { scope, key, .. } => {
                    h.on_context_write(run, scope, key).await
                }
                JournalEvent::PlanExpanded {
                    node,
                    subgraph,
                    node_plans,
                } => h.on_plan_expanded(run, node, subgraph, node_plans).await,
                JournalEvent::PlannerSelected { node, agent } => {
                    h.on_planner_selected(run, node, agent).await
                }
                _ => {}
            }
        }
        Ok(seq)
    }

    /// Publish a completed node's output to the blackboard (§8): `put` it under
    /// `Run/node.id` (bytes → CAS, ref kept) and journal a `ContextWrite`.
    /// Fold-guarded — a memoized replay on resume (key already in `fold.context`)
    /// is skipped, so it never re-`put`s (which would collide) or re-journals. No
    /// context store wired ⇒ a no-op (behavior byte-identical).
    async fn publish_context(
        &self,
        run: RunId,
        node_id: &NodeId,
        output: &serde_json::Value,
        fold: &Fold,
    ) -> Result<(), OrchestratorError> {
        let Some(ctx) = &self.context else {
            return Ok(());
        };
        let key = ContextKey(node_id.0.clone());
        if fold.context.contains_key(&(Scope::Run, key.clone())) {
            return Ok(());
        }
        let r = ctx.put(Scope::Run, key, output.clone()).await?;
        self.append(
            run,
            JournalEvent::ContextWrite {
                scope: r.scope.clone(),
                key: r.key.clone(),
                content: r.content.clone(),
                summary: r.summary.clone(),
                seq: 0,
            },
        )
        .await?;
        Ok(())
    }

    /// Rehydrate the injected blackboard from folded `ContextWrite`s on resume —
    /// `insert_ref` only, no blob load; the CAS persists across the crash seam, so
    /// a later `load` reads the value back. No context store wired ⇒ a no-op.
    async fn rehydrate_context(&self, fold: &Fold) -> Result<(), OrchestratorError> {
        let Some(ctx) = &self.context else {
            return Ok(());
        };
        for r in fold.context.values() {
            ctx.insert_ref(r.clone()).await?;
        }
        Ok(())
    }

    /// Resolve a node's dependency context from the blackboard (§8, D2): the
    /// Run-scoped output of each **`Hard`** dependency, in declared order. Reads
    /// are restricted to `Hard` deps (not all declared deps, and not all-Run) so a
    /// resume is replay-stable: a `Hard` dep must have `Completed` — and therefore
    /// published its `ContextWrite` — before this node runs, so its entry is
    /// present and value-stable across a resume, and the resolved context (hence
    /// the agent prompt and its input-hash) is byte-identical.
    ///
    /// `Soft` deps are deliberately EXCLUDED: a `Soft` dep only needs to be
    /// terminal, which includes `Failed`/`Skipped` (no `ContextWrite`). Since a
    /// failed/skipped node carries no memo and re-runs on resume, its terminal
    /// state — and thus its blackboard presence — can flip across a crash, which
    /// would change a dependent's prompt and trip the determinism fence. Reading
    /// only `Hard` deps keeps the resolved context a pure function of the journal.
    /// No store ⇒ empty.
    async fn resolve_context(
        &self,
        node: &orchestrator_core::Node,
    ) -> Result<Vec<(ContextKey, serde_json::Value)>, OrchestratorError> {
        let Some(ctx) = &self.context else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for dep in &node.deps {
            if dep.kind != orchestrator_core::EdgeKind::Hard {
                continue;
            }
            let key = ContextKey(dep.on.0.clone());
            if let Some(r) = ctx.get(Scope::Run, key.clone()).await? {
                out.push((key, ctx.load(&r).await?));
            }
        }
        Ok(out)
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
