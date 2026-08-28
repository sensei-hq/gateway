use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::content::{ContentRef, Digest, EffectOutput};
use crate::effect::{EffectClass, EffectId};
use crate::error::JournalError;
use crate::graph::{GateOption, Graph};
use crate::ids::{NodeId, RunId, Seq};
use crate::plan::NodePlan;

/// The durable journal format / effect-id scheme version. A persisted journal stamped with a
/// different value fences loudly on resume (never a silent mis-fold). Bump on any effect-id or
/// journal-serialization break.
pub const FORMAT_VERSION: i32 = 1;

/// The largest human-supplied answer, and the largest AUTHORED half of a journaled
/// question, in bytes.
///
/// **Not the whole journaled question** — that is bounded by
/// `MAX_HUMAN_TEXT_BYTES + MAX_HUMAN_CONTEXT_BYTES`, and the summary line used to say
/// otherwise while the body below said the opposite twenty lines later. The conflation is
/// the exact one this constant's own doc exists to prevent, so it is corrected at the first
/// sentence rather than only in the small print.
///
/// SP-6 s3. It lives in `orchestrator-core` because BOTH the executor and `torii` need
/// it and neither can borrow the other's bound. `torii` already has one —
/// `cmd::run::MAX_PAYLOAD_BYTES`, enforced by `cmd::run::check_payload_size` — but the
/// executor cannot reach it: `sensei-torii` depends on `sensei-orchestrator` and
/// `sensei-orchestrator-core`, so a dependency the other way is a CYCLE the crate graph
/// cannot express, not merely a visibility problem. (`check_payload_size` is also
/// `pub(crate)`, and takes a `serde_json::Value` rather than a `&str` — but the cycle is
/// what makes reuse impossible.) One constant here, two call sites, no duplicated
/// number.
///
/// 4 KiB matches `torii`'s `MAX_PAYLOAD_BYTES` and the executor's default
/// `cas_threshold` (`Executor::new`, `executor/mod.rs`). The bound is load-bearing
/// rather than theoretical for the PROMPT: the AUTHORED part of a question is the system
/// prompt plus every activated skill plus the node's input, routinely multi-KB.
///
/// **It bounds the AUTHORED part of a question only — never the whole composed question.**
/// The whole-slice review of s3 found it charged against the rendered `## Context` section
/// too, and that section is RUN DATA: `assemble_prompt` renders every Hard dependency's
/// full materialized output into it verbatim. A human-backed node downstream of any node
/// that produced ~1000 tokens therefore failed TERMINALLY, after the upstream tokens were
/// already spent, with a message naming three config fields that were not the cause. See
/// [`MAX_HUMAN_CONTEXT_BYTES`], which is the bound that half gets, and the reason the two
/// are different numbers with different failure modes.
pub const MAX_HUMAN_TEXT_BYTES: usize = 4096;

/// The largest rendered `## Context` section a journaled question may carry, in bytes.
///
/// SP-6 s3, added by the whole-slice review. It exists because the `## Context` section has
/// a DIFFERENT OWNER from the rest of the question: the agent's `system_prompt`, its skill
/// bodies and the node's input are written by the config author, who can trim them, while
/// `## Context` is whatever the upstream nodes happened to produce. Bounding a value nobody
/// can bound at config time with a bound whose breach is a terminal `NodeFailed` makes an
/// ordinary verbose model answer unrecoverable — and the executor's own default
/// `cas_threshold` is 4096, i.e. this codebase already treats a >4 KiB effect output as
/// normal enough to warrant CAS splitting rather than refusal.
///
/// So the two halves are bounded by two different RULES, not just two numbers: the authored
/// half fails loudly against [`MAX_HUMAN_TEXT_BYTES`] (a config error, actionable), and this
/// half is TRUNCATED per dependency with a visible marker (run data, degraded honestly). The
/// durable row stays bounded either way, which was the cap's real justification: a question
/// is re-decoded by every drive, every `torii run list-paused` and every fold for the life
/// of the run.
///
/// 32 KiB — eight times the answer cap. Generous enough that the ordinary case this was
/// found by (one verbose upstream model answer) is never truncated at all, and small enough
/// that a person can still read the row and a `jsonb` column is not being used as a blob
/// store. A human-backed question is bounded overall by
/// `MAX_HUMAN_TEXT_BYTES + MAX_HUMAN_CONTEXT_BYTES`.
pub const MAX_HUMAN_CONTEXT_BYTES: usize = 32 * 1024;

/// A compacted per-child record (§5.3): after a `Map`'s `Consolidate` completes,
/// each child's full `EffectRecorded` collapses to this small shape and leaves
/// the hot fold path. The child's content stays retrievable from the
/// `ContentStore` by [`digest`](CompactChild::digest) — never dropped. The
/// `input_hash` lets the resume fold rebuild the child's memo entry (as a
/// content ref) so a replay still re-spends nothing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompactChild {
    pub index: usize,
    pub status: ChildStatus,
    /// The content address of the child's output — `Some` for an `Ok` child,
    /// `None` for a `Failed` one (which never journaled an output).
    pub digest: Option<Digest>,
    /// The child effect's determinism key (`Ok` children only) — feeds memo
    /// reconstruction on resume.
    pub input_hash: Option<String>,
    /// SP-DATA-5: the tokens the child's own `EffectRecorded` carried, kept here
    /// because that record is being DELETED.
    ///
    /// Without it a `Consolidate` over a `ModelCall` `Map` erased that Map's spend
    /// from the durable ledger permanently: the next drive folded a base short by the
    /// children's tokens and the run spent past its cap with nothing loud anywhere —
    /// the same "counter restarts at zero" failure the journal-as-ledger design
    /// exists to prevent. Compaction is a representation change; it must be
    /// spend-preserving, exactly as it is already memo-preserving via `digest` +
    /// `input_hash`.
    ///
    /// `None` for a `Failed` child (it journaled no record) and for any pre-fix
    /// `MapCompacted`, which still deserializes and folds as it always did — those
    /// runs' children's spend is already gone and this cannot invent it.
    #[serde(default)]
    pub usage: Option<crate::budget::TokenUsage>,
}

/// A compacted child's terminal status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChildStatus {
    Ok,
    Failed,
}

/// Provenance + freshness of an `Observation` effect (§7.1). `content_hash` (the
/// third provenance element) is derived — the recorded output's digest — not stored.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObservationMeta {
    pub fetched_at: chrono::DateTime<chrono::Utc>,
    pub ttl_secs: u64,
    pub source: String,
}

/// An append-only event in a run's durable journal. Folding a run's events
/// reconstructs its state for deterministic resume.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JournalEvent {
    RunStarted {
        version: String,
        /// SP-DATA-5: the run's token cap, journaled so a cross-process resume folds
        /// the SAME cap. `None` (and any pre-SP-DATA-5 journal) ⇒ unbudgeted, and the
        /// gate never fires — byte-identical to before.
        #[serde(default)]
        budget: Option<crate::budget::TokenBudget>,
    },
    NodeStarted {
        node: NodeId,
    },
    EffectRecorded {
        node: NodeId,
        effect_id: EffectId,
        class: EffectClass,
        input_hash: String,
        seq: Seq,
        /// The effect's output, carried **inline** for small payloads or as a
        /// content-addressed [`ContentRef`](crate::content::ContentRef) for
        /// over-threshold ones (§7.4). The fold reads this without loading blobs.
        output: EffectOutput,
        /// Set only for `Observation` effects (§7.1): freshness + provenance so a
        /// resume can decide replay-vs-re-read. `None` for Pure/Mutation.
        observation: Option<ObservationMeta>,
        /// SP-DATA-5: tokens this effect actually consumed, as reported by the
        /// provider. Rides on THIS event rather than its own so spend and the effect
        /// it belongs to land in ONE atomic append — two appends could be torn by a
        /// crash. `None` for non-model effects and for any pre-SP-DATA-5 journal.
        #[serde(default)]
        usage: Option<crate::budget::TokenUsage>,
    },
    /// The intent phase of a two-phase `Mutation` (§7.3), appended BEFORE the side
    /// effect. On resume an `EffectIntent` with no matching `EffectRecorded` is
    /// IN-DOUBT → reconcile, never blind re-run or blind memoize.
    EffectIntent {
        node: NodeId,
        effect_id: EffectId,
        idempotency_key: String,
        args_hash: String,
        seq: Seq,
    },
    NodeCompleted {
        node: NodeId,
    },
    NodeFailed {
        node: NodeId,
        error: String,
    },
    /// A node was skipped without running because a `Hard` dependency ended
    /// `Failed` or `Skipped` — cascade-skip (§3.3). Journaled so a skip is never
    /// silent; surfaced in `RunOutcome.skipped`.
    NodeSkipped {
        node: NodeId,
    },
    /// A `Map` node fanned out over `child_count` items (§3.4). The child
    /// manifest is fixed by the node's `over`, so this is deterministic and
    /// order-independent; each child's own effects follow under the structural
    /// path `"{node}/{i}"`.
    MapExpanded {
        node: NodeId,
        child_count: usize,
    },
    /// A completed `Map`'s per-child `EffectRecorded` records were compacted
    /// (§5.3) once its `Consolidate` finished: the children collapse to a small
    /// `{index, status, digest}` manifest (content stays addressable in the CAS).
    /// The fold rebuilds the children's memo (as content refs) from this, so the
    /// Map replays on resume without re-spending.
    MapCompacted {
        node: NodeId,
        children: Vec<CompactChild>,
    },
    /// A runtime graph expansion (§7.2/§7.6/§10.3): node `node` produced `subgraph`.
    /// Journaled BEFORE the nested graph is driven, so a crash mid-expansion resumes
    /// with the identical structure. The resume fold reconstructs the spliced graph
    /// from this — the memo, but for graph structure. `subgraph` carries LOCAL ids
    /// (namespaced under `node` at drive time), so the event is position-independent.
    PlanExpanded {
        node: NodeId,
        subgraph: Graph,
        /// Per-node plan metadata (local ids) — the self-describing side-map (§4.1).
        /// Serde-default so a pre-4A `PlanExpanded` (no field) still deserializes.
        #[serde(default)]
        node_plans: HashMap<NodeId, NodePlan>,
    },
    /// The planner-selection decision (SP-3 s4B): node `node` selected planner
    /// `agent`. Journaled BEFORE driving the planner, so a mid-plan resume reuses the
    /// same planner — the memo for the selection (symmetric with `PlanExpanded`).
    PlannerSelected {
        node: NodeId,
        agent: crate::registry::AgentRef,
    },
    /// A shared-scope blackboard publish (§8). Journaled so a resume rebuilds the
    /// `ContextStore` (as refs, no blob load) via
    /// [`ContextStore::insert_ref`](crate::context::ContextStore::insert_ref). The
    /// `content` is a CAS ref — never an inline blob.
    ContextWrite {
        scope: crate::context::Scope,
        key: crate::context::ContextKey,
        content: ContentRef,
        summary: Option<String>,
        seq: Seq,
    },
    RunCompleted,
    RunPaused {
        reason: String,
        resume_after: Option<chrono::DateTime<chrono::Utc>>,
    },
    /// SP-DATA-5: an operator raised (or lowered) the run's cap. Required, not
    /// cosmetic: the budget is journaled on `RunStarted`, so without this a woken run
    /// folds the ORIGINAL cap and immediately re-pauses — permanently stuck. Latest
    /// value wins; lowering below current spend is a legitimate way to halt a run.
    BudgetRaised {
        new_total_tokens: u64,
    },
    /// SP-6 s1: an `AwaitSignal` node began waiting, recording its ABSOLUTE deadline.
    ///
    /// This exists as its own node-keyed event rather than relying on
    /// `RunPaused.resume_after` because that field is not node-keyed and a run pauses
    /// for many unrelated reasons over its life. Recording the absolute instant here is
    /// what stops the deadline being recomputed as `now + timeout` on every resume —
    /// which would push it forward forever, so a run force-woken every ten minutes with
    /// a one-hour timeout would NEVER expire.
    SignalAwaited {
        node: NodeId,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
    },
    /// SP-6 s1: an external signal arrived for an `AwaitSignal` node. Folded by node id,
    /// so the node reads its answer and never re-asks — the same shape
    /// `PlannerSelected` uses for a planner choice. Last delivery wins while the node is
    /// still paused; once it has completed, the node is folded complete and never
    /// re-executes, so a later signal changes nothing.
    SignalReceived {
        node: NodeId,
        payload: serde_json::Value,
    },
    /// SP-6 s2: a `HumanGate` has begun asking, carrying the MENU the human was shown.
    ///
    /// The options are journaled rather than re-read from the graph for the same reason
    /// s1 journals the deadline: a human was shown a menu, and validating their answer
    /// against a *different* menu later is simply wrong. Nothing BINDS the graph handed
    /// to a later `Executor::start` to the one the human was shown: there is no graph
    /// fence (the SP-DATA-2 config-version fence covers the registry, not the graph),
    /// and the executor cannot see `SchedulerStore` at all — so an author who edits the
    /// graph between drives silently rewrites the menu unless the offer is journaled
    /// here. (`scheduled_runs.graph` happens to hold a copy on the `worker serve` path,
    /// but the executor cannot read it — that table is the scheduler's, not the
    /// executor's.)
    ///
    /// The full [`GateOption`]s, not just their names: the OUTCOME the human was shown
    /// ("reject will stop the run") is as much a part of the offer as the name. If only
    /// names were journaled, an author flipping `reject` from `Fail` to `Complete` after
    /// a human rejected would silently change what their recorded answer MEANT.
    ///
    /// FIRST record wins when folded, exactly as `SignalAwaited` does — overwriting the
    /// deadline is the never-expires bug.
    GateAwaited {
        node: NodeId,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
        options: Vec<GateOption>,
    },
    /// SP-6 s2: a human picked one of a `HumanGate`'s options.
    ///
    /// A `HumanGate` is answerable ONLY by this event, never by `SignalReceived` — if a
    /// raw signal could answer one, `torii run signal --payload '{}'` would bypass every
    /// validation the slice adds.
    ///
    /// `actor` is ATTRIBUTION, NOT AUTHENTICATION: it is whatever string the caller
    /// supplied, so this answers "who claimed to decide", not "who decided". `note` is
    /// `Option` because a `Complete` decision legitimately has none; the CLI separately
    /// requires one for a `Fail` option (a documentation rule, not a safety rule).
    GateDecided {
        node: NodeId,
        option: String,
        actor: String,
        note: Option<String>,
    },
    /// SP-6 s3: a human-backed `Agent` node has begun asking, carrying the QUESTION.
    ///
    /// The prompt is journaled rather than recomposed, for two of the three reasons s2
    /// journals the menu on [`JournalEvent::GateAwaited`] — but NOT for its third, which
    /// does not transfer and must not be repeated here.
    ///
    /// It DOES transfer that an operator must be able to read the question off the
    /// journal alone. Recomposing it needs `assemble_prompt` (`orchestrator`), which
    /// takes a resolved `Registry` AND the run's already-materialized dependency
    /// outputs for its `## Context` section; `torii`'s read path has neither — the light
    /// boot tier carries a `PostgresConfigSource` (raw config rows, for `config diff`)
    /// and a journal, not a `Registry`, a blackboard, or the executor. And it DOES
    /// transfer that fixing the question at ask time is what lets a late answer be
    /// honoured against the question actually given.
    ///
    /// What does NOT transfer is s2's drift argument. s2 journals the menu because
    /// nothing fences the GRAPH between drives. The REGISTRY is fenced: the SP-DATA-2
    /// config-version fence pins `{version}#cfg{generation}` on `RunStarted`, so a
    /// `config push` that changes an agent's `system_prompt` or skills bumps the
    /// generation and the paused run's next drive is REFUSED with
    /// `VersionFenceMismatch` — it never resumes at all, let alone with a recomposed
    /// question (`Executor::pinned` + the fence in `start_inner`; proven by
    /// `reload_bumps_the_run_version_and_fences_in_flight_resume`, and it is why
    /// SP-DATA-4's review found that a `config push` terminally strands every paused
    /// run). Claiming a silent re-ask here would invert that: the failure mode config
    /// drift actually produces is loud refusal, not a second human seeing a different
    /// question.
    ///
    /// It carries the MODEL-EQUIVALENT question, which is more than `assemble_prompt`'s
    /// output: the agent's `system_prompt`, plus each activated skill's body, plus the
    /// rendered `## Context` section of resolved dependency outputs, plus `\n\n## Task\n`
    /// and the node's INPUT. The input is there because `assemble_prompt` never returns it —
    /// it takes the query only to evaluate `activation.is_active`, and the model path
    /// supplies it separately as the first user message — so a question without it asked a
    /// reviewer about a contract nobody named. Design §5.4's rule is one-directional: never
    /// show the human LESS than the model would have had. (An earlier version of this
    /// paragraph described `assemble_prompt`'s output alone, which stopped being true when
    /// s3's own Task 4 review added the `## Task` section.)
    ///
    /// A writer MUST bound it before appending — an unbounded question is a durable write,
    /// re-decoded by every drive, every `torii run list-paused` and every fold for the life
    /// of the run — but **by two different rules for its two halves**, because they have two
    /// different owners:
    ///
    /// - the AUTHORED bytes (`system_prompt` + skills + the node input) against
    ///   [`MAX_HUMAN_TEXT_BYTES`], loudly, since a config author can trim them;
    /// - the `## Context` bytes against [`MAX_HUMAN_CONTEXT_BYTES`], by TRUNCATION with a
    ///   visible marker, since they are whatever the upstream nodes produced and no operator
    ///   can bound them at config time.
    ///
    /// Charging one cap against both is not a smaller version of this rule; it is a
    /// different behaviour, and it was the s3 whole-slice review's worst finding — an
    /// ordinary verbose upstream killed the node terminally after its tokens were already
    /// spent. See [`MAX_HUMAN_CONTEXT_BYTES`].
    ///
    /// A writer must also REDACT it before appending (design §6), and `run_human_agent`
    /// does: it runs the executor's own redactor over the WHOLE composed question and
    /// appends that value. s3 originally shipped `prompt: prompt.to_string()`, which made
    /// this row the one place a credential in a `system_prompt` or a skill body reached
    /// durable storage in the clear — nothing upstream scrubs it (`torii config push`
    /// redacts nothing). The residue worth knowing: `Executor::with_redactor` is opt-in and
    /// defaults to `None`, so a library embedder that wires no redactor still writes the
    /// question as composed.
    ///
    /// FIRST record wins when folded, exactly as `SignalAwaited`/`GateAwaited` do —
    /// overwriting the deadline is the never-expires bug s1 documents.
    AgentAwaited {
        node: NodeId,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
        prompt: String,
    },
    /// SP-6 s3: a human answered a human-backed `Agent` node.
    ///
    /// `text` becomes the node's output under the `"text"` key — deliberately the same
    /// key a model-backed `Agent` produces (`finish_agent`, `executor/agent.rs`), so
    /// every existing reader of that key consumes a human answer without knowing it was
    /// human: [`BranchCond::TextContains`](crate::graph::BranchCond::TextContains) and
    /// [`LoopGate::TextContains`](crate::graph::LoopGate::TextContains) both read
    /// `output["text"]`, and a dependent's `## Context` section renders the output as-is.
    ///
    /// `actor` is ATTRIBUTION, NOT AUTHENTICATION — it is whatever string the caller
    /// supplied — and it matters more here than on [`JournalEvent::GateDecided`]: a
    /// gate's actor is an audit trail, whereas this one is recorded alongside text that
    /// lands in the node's OUTPUT and so flows on into downstream model prompts.
    ///
    /// **`actor` is part of the node's OUTPUT, not journal-only attribution.** The
    /// completed node yields `{"text", "actor"}` (design §4 / AC2) — a SECOND canonical
    /// Agent-node shape alongside the model-backed `{"model", "text"}`. That is a
    /// decision the writer of this event cannot make alone, because the executor
    /// re-projects Agent outputs on the terminal-resume path
    /// (`project_agent_outputs`, `orchestrator::executor::support`), and its original
    /// rewrite-everything-to-`{model, text}` rule would have dropped the `actor` and
    /// invented `model: null` — but only on that ONE path, so the same finished run
    /// would report a different output depending on when it was read. That function now
    /// passes an output carrying an `actor` through untouched. A writer changing this
    /// event's field set must keep the two in step.
    AgentAnswered {
        node: NodeId,
        text: String,
        actor: String,
    },
}

/// A round-boundary checkpoint of a run's state (§7.4). Written to the journal's
/// snapshot store (out-of-band — NOT an event in the log, so the control-flow
/// event order stays byte-identical) after each scheduling round; the latest
/// wins. A resume seeds from the latest snapshot and folds only the journal
/// **tail** (events with `Seq >` [`seq`](Snapshot::seq)), bounding fold cost for
/// wide/long runs.
///
/// Carries the completed/skipped node sets and each completed node's output (as
/// a ref-or-inline [`EffectOutput`], so large outputs stay lean). The per-effect
/// memo for a partially-completed tail node is rebuilt by folding the tail, so
/// it is not stored here; the blackboard's `context_refs` are deferred until the
/// executor writes to the `ContextStore`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Snapshot {
    /// The journal `Seq` this snapshot covers up to; a resume folds events with
    /// `Seq >` this.
    pub seq: Seq,
    pub completed: Vec<NodeId>,
    pub skipped: Vec<NodeId>,
    /// Each completed node's output, keyed by node id (ref-or-inline).
    pub outputs: Vec<(NodeId, EffectOutput)>,
    /// Tokens spent at `seq`, and the cap in force there — the SP-DATA-5 ledger
    /// reduced to the two scalars a tail-only fold cannot re-derive.
    ///
    /// Carried because this struct's own contract above ("a resume folds events with
    /// `Seq >` this") is a loaded gun otherwise. Wire that optimisation without these
    /// and a resume drops every `EffectRecorded.usage` at or below `seq` AND
    /// `RunStarted.budget`, which lives at the very first seq — so the run resumes
    /// having apparently spent nothing, with `budget: None` and a gate that can never
    /// fire. Uncapped, silently. `write_snapshot` runs at EVERY round boundary, so
    /// this would be the common path, not an edge case.
    ///
    /// The defect is a third instance of the family SP-DATA-5's review already found
    /// twice (Map compaction erasing children's spend; the fold summing instead of
    /// keying by effect id). It is recorded here rather than left to the future
    /// because a tail-only fold that compiles would pass the entire suite.
    ///
    /// `#[serde(default)]` on both: snapshots written before this field existed
    /// deserialize as `(0, None)`, which is exactly the pre-existing behaviour.
    #[serde(default)]
    pub spent: u64,
    #[serde(default)]
    pub budget: Option<u64>,
}

/// The durable-journal seam. Slice 1 ships an in-memory implementation; a
/// `PostgresJournal` implements this same trait in a later slice.
///
/// `append` is strict: a write error is surfaced (fatal/pause), never swallowed.
#[async_trait::async_trait]
pub trait ExecutionJournal: Send + Sync {
    async fn append(&self, run: RunId, event: JournalEvent) -> Result<Seq, JournalError>;
    async fn load(&self, run: RunId) -> Result<Vec<(Seq, JournalEvent)>, JournalError>;

    /// Load only the journal **tail** — events with `Seq > since`. The default
    /// filters [`load`](Self::load); a persistent backend overrides this with an
    /// indexed range query. Powers snapshot-resume (fold the tail, not the whole
    /// log).
    async fn load_since(
        &self,
        run: RunId,
        since: Seq,
    ) -> Result<Vec<(Seq, JournalEvent)>, JournalError> {
        Ok(self
            .load(run)
            .await?
            .into_iter()
            .filter(|(seq, _)| *seq > since)
            .collect())
    }

    /// Persist the latest round-boundary [`Snapshot`] for `run` (latest wins).
    /// The default is a no-op — a backend without snapshot support simply folds
    /// from the start (the slice-1/2 path); [`InMemoryJournal`] overrides it.
    async fn snapshot(&self, _run: RunId, _snap: Snapshot) -> Result<(), JournalError> {
        Ok(())
    }

    /// The latest [`Snapshot`] for `run`, or `None` if none was written. The
    /// default returns `None` (fold-from-start).
    async fn latest_snapshot(&self, _run: RunId) -> Result<Option<Snapshot>, JournalError> {
        Ok(None)
    }

    /// Compaction primitive (§5.3): remove the events at `remove_seqs` and append
    /// `add` in one step. Generic — the executor picks the seqs (a completed
    /// Map's per-child `EffectRecorded`) and `add` (a `MapCompacted` manifest);
    /// the journal stays oblivious to Map semantics. The default is a graceful
    /// no-removal append (a backend without compaction keeps the child records but
    /// still records the manifest); [`InMemoryJournal`] overrides it to remove.
    async fn compact(
        &self,
        run: RunId,
        _remove_seqs: &[Seq],
        add: JournalEvent,
    ) -> Result<(), JournalError> {
        self.append(run, add).await.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::ObservationMeta;
    use crate::{EffectClass, EffectOutput, JournalEvent, NodeId, effect_id};

    /// An OLD journal — serialized before this slice — must still deserialize, with
    /// the new fields absent rather than erroring. If this fails, the change is a
    /// format break and FORMAT_VERSION must be bumped; the whole additivity claim
    /// rests here.
    ///
    /// These literals are NOT hand-written guesses: they are the actual output of
    /// `serde_json::to_string` on `RunStarted`/`EffectRecorded` built against the
    /// pre-SP-DATA-5 code (captured via a throwaway probe test before the `budget`/
    /// `usage` fields existed), i.e. genuine old events.
    #[test]
    fn an_old_journal_event_deserializes_with_the_new_fields_absent() {
        let old_started = r#"{"RunStarted":{"version":"v1"}}"#;
        let e: JournalEvent =
            serde_json::from_str(old_started).expect("old RunStarted still loads");
        match e {
            JournalEvent::RunStarted { budget, .. } => assert!(budget.is_none()),
            other => panic!("wrong variant: {other:?}"),
        }

        let old_recorded = r#"{"EffectRecorded":{"node":"n1","effect_id":"02e75a6544f3138fc1819276dc04aebeffe74eaf2fe8d4be23265db5cc84cfe3","class":"Pure","input_hash":"h","seq":0,"output":{"Inline":null},"observation":null}}"#;
        let e: JournalEvent =
            serde_json::from_str(old_recorded).expect("old EffectRecorded still loads");
        match e {
            JournalEvent::EffectRecorded { usage, .. } => assert!(usage.is_none()),
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn a_budget_round_trips_through_the_journal() {
        let e = JournalEvent::RunStarted {
            version: "v1".into(),
            budget: Some(crate::budget::TokenBudget {
                total_tokens: 50_000,
            }),
        };
        let s = serde_json::to_string(&e).expect("serializes");
        let back: JournalEvent = serde_json::from_str(&s).expect("round-trips");
        match back {
            JournalEvent::RunStarted {
                budget: Some(b), ..
            } => {
                assert_eq!(b.total_tokens, 50_000)
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn journal_event_roundtrips() {
        let e = JournalEvent::EffectRecorded {
            node: NodeId("n1".into()),
            effect_id: effect_id("", 0, 0),
            class: EffectClass::Pure,
            input_hash: "abc".into(),
            seq: 1,
            output: EffectOutput::Inline(serde_json::json!({"text":"hi"})),
            observation: None,
            usage: None,
        };
        let s = serde_json::to_string(&e).unwrap();
        let back: JournalEvent = serde_json::from_str(&s).unwrap();
        assert!(matches!(back, JournalEvent::EffectRecorded { .. }));
    }

    #[test]
    fn effect_intent_and_observation_meta_roundtrip() {
        let intent = JournalEvent::EffectIntent {
            node: NodeId("n1".into()),
            effect_id: effect_id("n1", 0, 1),
            idempotency_key: "k".into(),
            args_hash: "h".into(),
            seq: 0,
        };
        let s = serde_json::to_string(&intent).unwrap();
        assert!(matches!(
            serde_json::from_str::<JournalEvent>(&s).unwrap(),
            JournalEvent::EffectIntent { .. }
        ));

        let obs = ObservationMeta {
            fetched_at: chrono::Utc::now(),
            ttl_secs: 60,
            source: "search".into(),
        };
        let rec = JournalEvent::EffectRecorded {
            node: NodeId("n1".into()),
            effect_id: effect_id("n1", 0, 1),
            class: EffectClass::Observation,
            input_hash: "h".into(),
            seq: 0,
            output: EffectOutput::Inline(serde_json::json!({"x":1})),
            observation: Some(obs),
            usage: None,
        };
        assert!(matches!(
            serde_json::from_str::<JournalEvent>(&serde_json::to_string(&rec).unwrap()).unwrap(),
            JournalEvent::EffectRecorded {
                observation: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn context_write_event_roundtrips() {
        use crate::content::{ContentRef, Digest};
        use crate::context::{ContextKey, Scope};
        let e = JournalEvent::ContextWrite {
            scope: Scope::Run,
            key: ContextKey("n1".into()),
            content: ContentRef {
                digest: Digest("d".into()),
                size: 3,
                summary: None,
            },
            summary: None,
            seq: 0,
        };
        let s = serde_json::to_string(&e).unwrap();
        assert!(matches!(
            serde_json::from_str::<JournalEvent>(&s).unwrap(),
            JournalEvent::ContextWrite { .. }
        ));
    }

    #[test]
    fn plan_expanded_event_roundtrips() {
        use crate::graph::{Graph, Node, NodeKind};
        let e = JournalEvent::PlanExpanded {
            node: NodeId("e".into()),
            subgraph: Graph {
                nodes: vec![Node {
                    id: NodeId("n1".into()),
                    kind: NodeKind::ModelCall {
                        chain: "c".into(),
                        payload: serde_json::json!({ "prompt": "hi" }),
                    },
                    deps: vec![],
                }],
            },
            node_plans: std::collections::HashMap::new(),
        };
        let s = serde_json::to_string(&e).unwrap();
        let back: JournalEvent = serde_json::from_str(&s).unwrap();
        match back {
            JournalEvent::PlanExpanded { node, subgraph, .. } => {
                assert_eq!(node, NodeId("e".into()));
                assert_eq!(subgraph.nodes.len(), 1);
            }
            other => panic!("expected PlanExpanded, got {other:?}"),
        }
    }

    #[test]
    fn planner_selected_event_roundtrips() {
        let e = JournalEvent::PlannerSelected {
            node: NodeId("e".into()),
            agent: crate::registry::AgentRef("planner".into()),
        };
        let s = serde_json::to_string(&e).unwrap();
        match serde_json::from_str::<JournalEvent>(&s).unwrap() {
            JournalEvent::PlannerSelected { node, agent } => {
                assert_eq!(node, NodeId("e".into()));
                assert_eq!(agent.0, "planner");
            }
            other => panic!("expected PlannerSelected, got {other:?}"),
        }
    }

    /// Additivity: this slice adds two NEW VARIANTS, not new fields. An old reader
    /// cannot know them, but a NEW reader must still load every OLD event unchanged —
    /// that is what keeps FORMAT_VERSION at 1.
    #[test]
    fn adding_the_signal_events_does_not_break_old_event_loading() {
        let old = r#"{"RunStarted":{"version":"v1"}}"#;
        let e: JournalEvent = serde_json::from_str(old).expect("old RunStarted still loads");
        assert!(matches!(e, JournalEvent::RunStarted { .. }));
    }

    #[test]
    fn the_signal_events_round_trip() {
        let awaited = JournalEvent::SignalAwaited {
            node: NodeId("gate".into()),
            deadline: Some(chrono::DateTime::<chrono::Utc>::from_timestamp(3_000_000, 0).unwrap()),
        };
        let s = serde_json::to_string(&awaited).expect("serializes");
        let back: JournalEvent = serde_json::from_str(&s).expect("round-trips");
        match back {
            JournalEvent::SignalAwaited { node, deadline } => {
                assert_eq!(node.0, "gate");
                assert!(deadline.is_some());
            }
            other => panic!("wrong variant: {other:?}"),
        }

        let received = JournalEvent::SignalReceived {
            node: NodeId("gate".into()),
            payload: serde_json::json!({"decision": "approved"}),
        };
        let s = serde_json::to_string(&received).expect("serializes");
        let back: JournalEvent = serde_json::from_str(&s).expect("round-trips");
        match back {
            JournalEvent::SignalReceived { payload, .. } => {
                assert_eq!(payload["decision"], "approved")
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    /// SP-6 s2: both new variants round-trip, and — the load-bearing half — they are
    /// new VARIANTS, so an event written by an older binary still loads. That is what
    /// keeps `FORMAT_VERSION` at 1.
    #[test]
    fn the_gate_events_round_trip_without_a_format_bump() {
        use crate::graph::{GateOption, GateOutcome};

        let awaited = JournalEvent::GateAwaited {
            node: NodeId("release".into()),
            deadline: Some(chrono::DateTime::<chrono::Utc>::from_timestamp(3_000_000, 0).unwrap()),
            options: vec![
                GateOption {
                    name: "ship".into(),
                    outcome: GateOutcome::Complete,
                },
                GateOption {
                    name: "hold".into(),
                    outcome: GateOutcome::Fail,
                },
            ],
        };
        let s = serde_json::to_string(&awaited).expect("serializes");
        match serde_json::from_str::<JournalEvent>(&s).expect("round-trips") {
            JournalEvent::GateAwaited {
                node,
                deadline,
                options,
            } => {
                assert_eq!(node.0, "release");
                assert!(deadline.is_some());
                assert_eq!(options.len(), 2);
                assert_eq!(options[0].name, "ship");
                // Both outcomes are durable AND DISTINCT — a menu whose options are all
                // `Complete` passes even if the two variants collapse into one on the wire.
                assert_eq!(options[0].outcome, GateOutcome::Complete);
                assert_eq!(options[1].outcome, GateOutcome::Fail);
            }
            other => panic!("wrong variant: {other:?}"),
        }

        let decided = JournalEvent::GateDecided {
            node: NodeId("release".into()),
            option: "ship".into(),
            actor: "alice".into(),
            note: Some("capped at 5k".into()),
        };
        let s = serde_json::to_string(&decided).expect("serializes");
        match serde_json::from_str::<JournalEvent>(&s).expect("round-trips") {
            JournalEvent::GateDecided {
                node,
                option,
                actor,
                note,
            } => {
                assert_eq!(node.0, "release");
                assert_eq!(option, "ship");
                assert_eq!(actor, "alice");
                assert_eq!(note.as_deref(), Some("capped at 5k"));
            }
            other => panic!("wrong variant: {other:?}"),
        }

        // A `note`-less decision is legal: a Complete option needs no reason.
        let terse = JournalEvent::GateDecided {
            node: NodeId("release".into()),
            option: "ship".into(),
            actor: "ci".into(),
            note: None,
        };
        let s = serde_json::to_string(&terse).expect("serializes");
        assert!(serde_json::from_str::<JournalEvent>(&s).is_ok());
    }

    /// SP-6 s3: both variants round-trip, and — the load-bearing half — they are new
    /// VARIANTS, so an event written by an older binary still loads and
    /// `FORMAT_VERSION` stays 1.
    #[test]
    fn the_human_agent_events_round_trip() {
        let awaited = JournalEvent::AgentAwaited {
            node: NodeId("review".into()),
            deadline: Some(chrono::DateTime::<chrono::Utc>::from_timestamp(3_000_000, 0).unwrap()),
            prompt: "Does this contract permit sub-processing?".into(),
        };
        let s = serde_json::to_string(&awaited).expect("serializes");
        match serde_json::from_str::<JournalEvent>(&s).expect("round-trips") {
            JournalEvent::AgentAwaited {
                node,
                deadline,
                prompt,
            } => {
                assert_eq!(node.0, "review");
                assert!(deadline.is_some());
                assert!(prompt.contains("sub-processing"));
            }
            other => panic!("wrong variant: {other:?}"),
        }

        let answered = JournalEvent::AgentAnswered {
            node: NodeId("review".into()),
            text: "Yes, clause 7.2 permits it.".into(),
            actor: "alice".into(),
        };
        let s = serde_json::to_string(&answered).expect("serializes");
        match serde_json::from_str::<JournalEvent>(&s).expect("round-trips") {
            JournalEvent::AgentAnswered { node, text, actor } => {
                assert_eq!(node.0, "review");
                assert!(text.contains("7.2"));
                assert_eq!(actor, "alice");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    /// No doc comment in this file may link a `JournalEvent` variant by its BARE name.
    ///
    /// An enum variant is not in scope as a bare rustdoc path, so a link whose whole
    /// target is `GateAwaited` does not resolve: rustdoc emits `unresolved link` and
    /// renders the text verbatim, so the reader silently loses the cross-reference.
    /// (This doc deliberately never writes that broken form out — the scan below is
    /// textual and cannot tell a specimen from an offender.) The variants are
    /// this crate's most-linked items — every new event's doc points at the one it was
    /// modelled on — which is exactly why this keeps happening: SP-6 s3 Task 2 added
    /// two (`GateAwaited`, `GateDecided`) and took this crate from 5 rustdoc warnings
    /// to 7 before review caught it. `cargo doc`'s warnings are not part of
    /// `cargo test`, so nothing but a guard like this one fails on them.
    ///
    /// The fix at every site is the qualified form `[`JournalEvent::GateAwaited`]`,
    /// which resolves. This test reads the enum's own source for the variant list
    /// rather than hard-coding it, so a variant added later is covered on arrival.
    #[test]
    fn no_doc_comment_links_a_journal_event_variant_by_its_bare_name() {
        let src = include_str!("journal.rs");

        // Variant names: the 4-space-indented `Name {` / `Name,` lines inside the
        // `pub enum JournalEvent { .. }` block, which ends at the first column-0 `}`.
        let variants: Vec<&str> = src
            .lines()
            .skip_while(|l| !l.starts_with("pub enum JournalEvent {"))
            .skip(1)
            .take_while(|l| !l.starts_with('}'))
            .filter_map(|l| {
                let name = l.strip_prefix("    ")?;
                let name = name.strip_suffix(" {").or_else(|| name.strip_suffix(','))?;
                name.chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_uppercase())
                    .then_some(name)
                    .filter(|n| n.chars().all(|c| c.is_alphanumeric() || c == '_'))
            })
            .collect();
        assert!(
            variants.len() > 10 && variants.contains(&"GateAwaited"),
            "variant scrape broke — it found {variants:?}, so the guard below would \
             pass vacuously"
        );

        // Every ``[`target`]`` NOT followed by `(` — i.e. a link with no explicit
        // path, which must therefore resolve `target` on its own.
        let mut offenders: Vec<(usize, &str)> = Vec::new();
        for (i, line) in src.lines().enumerate() {
            let mut rest = line;
            while let Some(open) = rest.find("[`") {
                let after = &rest[open + 2..];
                let Some(close) = after.find("`]") else { break };
                let target = &after[..close];
                let tail = &after[close + 2..];
                if !tail.starts_with('(') && variants.contains(&target) {
                    offenders.push((i + 1, target));
                }
                rest = tail;
            }
        }
        assert!(
            offenders.is_empty(),
            "bare-name links to JournalEvent variants do not resolve; qualify them as \
             [`JournalEvent::<Variant>`]: {offenders:?}"
        );
    }
}
