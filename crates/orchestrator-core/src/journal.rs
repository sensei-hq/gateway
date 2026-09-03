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
    /// appends that value. s3 originally shipped `prompt: prompt.to_string()`, which let a
    /// credential in a `system_prompt` or a skill body into the JOURNAL in the clear —
    /// nothing between the authored config and this append scrubs it (`torii config push`
    /// redacts nothing). s3's own note called this row "the one place" such a credential
    /// reached durable storage; it is not, since the agent's markdown and the
    /// `config_agents` jsonb hold it verbatim already. It is the last door into the copy
    /// that is read BACK. The residue worth knowing: `Executor::with_redactor` is opt-in and
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
    /// SP-6 s4: a `GateSpec::Human` loop gate has begun asking, carrying the QUESTION and
    /// the MENU it published.
    ///
    /// The menu is journaled for s2's reason, which transfers exactly: an operator's
    /// answer must keep meaning what it meant when they were asked, and nothing BINDS
    /// the graph handed to a later `Executor::start` to the one the human was shown.
    /// There is no graph fence (the SP-DATA-2 config-version fence covers the registry,
    /// not the graph), so reading the graph's menu at decision time would let an author
    /// flip an option's `stops` after a human picked it and silently invert their
    /// decision. The concrete vector on the shipped `worker serve` path is the
    /// `scheduled_runs.graph` row, which `Scheduler::tick` re-drives from and an operator
    /// can edit between drives; a library embedder simply passing a different `Graph` to
    /// the next `start` is the same hazard with no table involved.
    ///
    /// The general form is deliberate. An earlier version of this paragraph enumerated
    /// three vectors and two of them were false, which is worse than saying less: a
    /// resubmitted `run submit` cannot re-drive an existing run at all (`cmd::run::submit`
    /// pre-checks `Scheduler::status`, and `SchedulerStore::enqueue` is the real guard —
    /// `on conflict do nothing` plus a `rows_affected == 0` error), and a runtime `Expand`
    /// subgraph is the ONE path that is bound, since `PlanExpanded` journals the subgraph
    /// before it is driven and `drive_expand_with` reuses `fold.expansions` verbatim
    /// rather than re-invoking the planner. (`Expand` is still a TRUST point — an
    /// untrusted planner can author the menu in the first place, see design §7 — but it
    /// is not a DRIFT point, and conflating the two is what made the list wrong.)
    ///
    /// The prompt is journaled for s3's reason: an operator must be able to read the
    /// question off the journal alone, and `torii`'s read path has no `Registry` and no
    /// blackboard with which to recompose it.
    ///
    /// `prompt` carries the same MODEL-EQUIVALENT question
    /// [`JournalEvent::AgentAwaited`] does, so it inherits that variant's writer
    /// obligations verbatim — they are restated here rather than left to a pointer,
    /// because a writer who adds a second append site reads THIS doc:
    ///
    /// - **Bound it by two rules, not one.** The AUTHORED bytes (the role's
    ///   `system_prompt` + activated skill bodies + the menu-derived `## Task` ask) fail
    ///   LOUDLY over [`MAX_HUMAN_TEXT_BYTES`]; the `## Context` bytes are TRUNCATED with a
    ///   visible marker against [`MAX_HUMAN_CONTEXT_BYTES`]. Charging one cap against both
    ///   is not a smaller version of the rule but a different behaviour, and it was the s3
    ///   whole-slice review's worst finding — an ordinary verbose upstream killed the node
    ///   terminally after its tokens were already spent. It bites harder here than at s3's
    ///   site: a loop gate's `## Context` is a model iteration's output essentially
    ///   always.
    ///
    ///   The third authored term is where this restatement is NOT verbatim, and getting it
    ///   verbatim was a bug in an earlier version of this bullet. At `AgentAwaited`'s site
    ///   the third term is the node INPUT; at a loop gate's it is `gate_ask(menu)`, the ask
    ///   synthesized from the option names, and the iteration output is the whole of the
    ///   `## Context` half. Naming the iteration data as the loudly-capped term inverts the
    ///   rule the bullet exists to state — which is why the executor's own failure message
    ///   says "trim the gate role's system prompt, its skills, or the menu option names".
    /// - **REDACT it before appending** (design §6), through the executor's own
    ///   `Redactor`, then clamp — `[REDACTED]` is longer than the shortest span it
    ///   replaces. Nothing upstream scrubs the authored halves: `torii config push` writes
    ///   an agent's `system_prompt` and a skill's body to `config_agents`/`config_skills`
    ///   as jsonb, verbatim. So this is the last unscrubbed door into the JOURNAL for a
    ///   credential in one of them — not the one place it reaches durable storage at all,
    ///   which is what an earlier version of this bullet claimed and the config tables
    ///   already disprove. The journal's copy is the one read BACK: folded on every drive,
    ///   printed by the operator surfaces, and shown to the person. s3 shipped the
    ///   unredacted form first and its review caught it.
    /// - **Redact the `menu` too, and refuse a menu that redaction makes AMBIGUOUS.** This
    ///   obligation is s4's own — `AgentAwaited` has no menu — and it is the one the first
    ///   shipped append site missed: `prompt` quotes the option names (through `gate_ask`)
    ///   and was scrubbed, while `menu` was appended straight from the graph, so one author
    ///   string was clean in one durable field and plaintext in another on the same write.
    ///   Option names arrive by a DIFFERENT unscrubbed intake from the prompt's: the graph
    ///   file `torii run submit --graph` deserializes. `torii config push` never carries
    ///   one — the menu lives on the graph so `validate_dag` can see it — and
    ///   `Scheduler::submit` has already put that graph in `scheduled_runs.graph`, in the
    ///   clear, before the executor drives. This append governs the journal's copy.
    ///
    ///   The refusal is the non-obvious half. `menu` is not display text: it is the
    ///   vocabulary a decision is resolved against, and redacting it is only safe while the
    ///   names stay DISTINCT. `Graph::validate_dag` rejects a duplicate name, but it runs on
    ///   the authored graph and cannot see a redactor at all (the redactor is an executor
    ///   injection, so the same graph is legal under one executor and not another), and
    ///   redaction can re-create the duplicate: two credential-shaped names both collapse to
    ///   the placeholder, the resolver takes the first match, and an operator picking the
    ///   only name they were offered gets whichever `stops` came first. A silently inverted
    ///   decision is worse than either the leak or a loud failure, so the writer must check
    ///   the redacted names for collision and fail the gate BEFORE appending.
    ///
    /// None of the three is optional (design §6, AC15/AC16). The first two were recorded on
    /// the TYPE ahead of any code that honoured them, because the s4 plan builds the append
    /// site (Task 6) four tasks before the enforcement (Task 10) — the contract had to be
    /// legible to the writer who arrived first; the third was added after review found the
    /// shipped site honouring the prompt rule and not the menu one. All three are now
    /// honoured by `Executor::run_human_loop_gate`, and each is guarded by the test named
    /// for the mutation that undoes it:
    /// `an_oversized_authored_prompt_fails_the_loop_gate` (delete the authored-bytes
    /// check), `a_verbose_iteration_output_truncates_the_question_instead_of_killing_the_gate`
    /// (pass the iteration output as the seam's `input` instead of as a `context` entry —
    /// the reversal the plan's own Task 6 sketch had, which fails the gate on a perfectly
    /// ordinary 37 KiB model answer), `the_journaled_loop_gate_question_is_redacted`
    /// (swap the redactor for the identity),
    /// `a_credential_in_a_menu_option_name_never_reaches_the_journal` (append the graph's
    /// menu instead of the scrubbed one) and
    /// `a_menu_whose_option_names_collide_once_redacted_fails_the_gate_loudly` (delete the
    /// collision refusal). A SECOND append site owes all of them, and inherits none.
    ///
    /// FIRST record wins when folded, exactly as `SignalAwaited`/`GateAwaited`/
    /// `AgentAwaited` do — overwriting the deadline is the never-expires bug.
    LoopGateAwaited {
        node: NodeId,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
        prompt: String,
        menu: Vec<crate::graph::LoopGateOption>,
    },
    /// SP-6 s4: a human picked one of a loop gate's options.
    ///
    /// A loop gate is answerable ONLY by this event, never by `SignalReceived`,
    /// `GateDecided` or `AgentAnswered`. `SignalReceived` and `AgentAnswered` carry no
    /// option name at all, so either would bypass the menu match outright.
    /// [`JournalEvent::GateDecided`] is the near miss worth naming precisely, because it
    /// DOES carry an `option: String` and so looks menu-matchable: the two menus are not
    /// interchangeable vocabularies. A `HumanGate`'s [`GateOption`] carries an
    /// [`GateOutcome`](crate::graph::GateOutcome) of `{Complete, Fail}`; a loop gate's
    /// [`LoopGateOption`](crate::graph::LoopGateOption) carries `stops`, and "continue"
    /// has no representation in the former at all. A decision recorded as `GateDecided`
    /// at a loop-gate node would therefore fold into the wrong side-map
    /// (`Fold::gate_decisions`, `executor/support.rs`) and be validated against the wrong
    /// menu — which is the cross-kind refusal the s4 plan's Tasks 9 and 12 have to
    /// enforce, in the executor and at the CLI respectively.
    ///
    /// (`GateDecided` does not itself carry a `GateOutcome` — it is `{node, option, actor,
    /// note}`, and the outcome lives on the MENU, `GateAwaited.options`. An earlier
    /// version of this doc said it did. The reason to keep the events apart is the menu
    /// vocabulary, not the decision's field set.)
    ///
    /// `actor` is ATTRIBUTION, NOT AUTHENTICATION: whatever string the caller supplied,
    /// so it answers "who claimed to decide", never "who decided", and must not be
    /// branched on. A writer must REDACT it before appending, exactly as it redacts the
    /// question — s3's whole-slice review found an unredacted `--as` to be a real
    /// plaintext leak on this exact field (design §6; see the commentary at
    /// `torii/src/cmd/human.rs`).
    ///
    /// **That obligation is the APPENDING writer's alone, and nothing downstream is a
    /// second line of defence.** Recorded here because the opposite is the natural
    /// assumption: the executor scrubs the QUESTION at its own chokepoint
    /// (`fail_loop_gate`, and `redact_and_clamp` on [`JournalEvent::LoopGateAwaited`]), so
    /// a reader may expect the actor to be caught somewhere on the way through too. It is
    /// not, and structurally cannot be. `Executor::run_human_loop_gate` reads only
    /// `option` off this event; it interpolates the actor into no message and puts it in
    /// no node output. Contrast both siblings, which is why the difference is worth
    /// stating: [`JournalEvent::GateDecided`]'s actor IS interpolated by `run_human_gate`
    /// into its rejection `NodeFailed` (so it passes a redacting chokepoint on the way
    /// out), and [`JournalEvent::AgentAnswered`]'s becomes half the node's OUTPUT and is
    /// redacted there. A loop gate's actor is written once and read only by an operator
    /// surface, so whatever reaches this field is what an audit reads forever.
    ///
    /// The redaction therefore belongs at torii's decide path, and it has to be ADDED
    /// there rather than inherited: `cmd::human::answer` redacts `--as` through
    /// `redact_answer`, while `cmd::gate::decide` — the verb that will write THIS event —
    /// deliberately does not, measuring the actor `Measured::AsGiven` on the ground that
    /// `GateDecided.actor` goes through no redaction. A loop-gate branch bolted onto that
    /// path inherits the gap silently, which is precisely how the s3 leak happened.
    ///
    /// **Why `actor` is REQUIRED**, a plain `String` exactly like
    /// [`JournalEvent::GateDecided`]'s and [`JournalEvent::AgentAnswered`]'s. A gate
    /// decision is an APPROVAL, and an approval always records who claimed to give it.
    /// That is not an analogy to s2: it is the same argument the s4 design makes for
    /// itself one section earlier (§3, "Expiry vs decision"), where reading expiry before
    /// the decision is justified on the ground that answering `continue` **authorizes
    /// another iteration of spend**. An authorization with no attribution at all is the
    /// row an audit cannot use. So the type refuses to express one.
    ///
    /// The field carried an `Option<String>` from its introduction (s4 Task 3) until it
    /// was narrowed here. The narrowing belongs to no task of the s4 plan: it landed OUT
    /// OF BAND, between that plan's Tasks 4 and 5, because it changes the JOURNAL's shape
    /// and such a change is cheap only while nothing writes the event (Task 6 is the
    /// first writer). The spec that specified the `Option` never argued for it, and no
    /// reading of the slice's own reasoning supports it — a loop gate's decider is
    /// exactly as attributable as a `HumanGate`'s. Since no journal holds a `None` to
    /// migrate, a stored row without the field now fails to deserialize, loudly, which is
    /// the correct treatment of an approval whose attribution was lost. (An earlier doc
    /// justified the `Option` as room for "an automated operator on a schedule". That is
    /// wrong twice over: an automated operator has a name, and naming it is what
    /// `actor_or` exists for.)
    ///
    /// The remaining degenerate value is `""`, and nothing normalises it away — the fold
    /// stores what was appended, so an audit reader sees exactly the string the writer
    /// chose. It is a WRITER BUG rather than a legal encoding of "anonymous": torii's
    /// `cmd::gate::actor_or`/`actor_or_user` never yield an empty actor (an unresolvable
    /// one is named `unknown`, precisely because a blank audit row is indistinguishable
    /// from a bug), and the s4 CLI decide path MUST route through them for that reason.
    /// An embedder appending directly owes the same discipline.
    LoopGateDecided {
        node: NodeId,
        option: String,
        actor: String,
    },
    /// SP-6 s4: a drive HONOURED a loop gate's decision while that gate was still live —
    /// the durable half of "the decision was made in time".
    ///
    /// **It exists because a `Loop` re-derives every iteration's gate on every drive, and
    /// the SLA is per-GATE while the clock is global.** `run_loop` re-enters
    /// `for i in 0..max_iters` from zero on each wake, so iteration 0's gate is
    /// recomputed forever; `wait_or_expire_by_id` answers only from `now >= recorded
    /// deadline`, and knows nothing about a decision an earlier drive already read,
    /// honoured and spent an iteration against. Without this row, the moment wall-clock
    /// passed the FIRST gate's deadline the whole `Loop` died — even though every person
    /// had answered inside their own gate's SLA — taking every earlier iteration's tokens
    /// with it, unrecoverably. Reproduced twice by review: a 3-iteration loop whose
    /// operator answered at +30m and +70m under a 1h SLA, and a loop that had already
    /// CONVERGED and was retroactively killed a day later when a downstream signal woke
    /// the run.
    ///
    /// It is the SUCCESS mirror of the `NodeFailed` the expiry path writes, and it is
    /// written for the same reason: a verdict is settled by the drive that produces it and
    /// READ BACK afterwards, never re-derived against a moving clock. The alternative —
    /// reading the decision before the deadline — is the s3 ordering this slice
    /// deliberately inverts (§3, "Expiry vs decision"), and it reopens the hole AC8 exists
    /// for: a "continue" arriving after the SLA would authorize another iteration of
    /// spend. Ordering the row this way keeps both properties at once, because an
    /// UNSETTLED gate still meets the clock first.
    ///
    /// `option` is the name that was honoured, and the replay resolves THAT against the
    /// journaled menu rather than re-reading `LoopGateDecided`. The difference is
    /// deliberate and narrow: `LoopGateDecided` is folded LAST-wins so an operator can
    /// correct a decision *before the run resumes*, and this row is exactly the line
    /// after which "before" has passed — the loop has already spent an iteration on the
    /// strength of the answer, so a later correction must not retroactively move where
    /// the loop converged.
    ///
    /// FIRST record wins when folded. The executor writes at most one (it reads the row
    /// back ahead of everything that could write another), so a second can only come from
    /// a journal the executor did not write, and the first is the one that describes what
    /// actually happened.
    ///
    /// Additive, like the pair above: `FORMAT_VERSION` stays 1.
    LoopGateSettled {
        node: NodeId,
        option: String,
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
    use super::{FORMAT_VERSION, ObservationMeta};
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

    /// AC20 — the durable format version is PINNED, so it cannot move unannounced.
    ///
    /// Named for what it checks rather than for the slice that added it, because a
    /// constant comparison cannot observe a slice: no change to any variant can fail
    /// this, and a legitimate future bump will. The first version of this test was
    /// called `the_loop_gate_variants_do_not_move_the_format_version`, which promised an
    /// additivity guard it could not deliver — under a genuine wire break
    /// (`#[serde(rename_all = "snake_case")]` on the enum, say) it stays GREEN.
    ///
    /// Additivity is proven elsewhere and deliberately not restated here:
    /// `an_old_journal_event_deserializes_with_the_new_fields_absent` and
    /// `adding_the_signal_events_does_not_break_old_event_loading` decode genuine
    /// pre-slice JSON literals, and those are the two that redden on that mutation.
    /// Nothing else in the workspace asserts `FORMAT_VERSION`; the only other readers
    /// are `PostgresJournal`'s resume fence and its `IncompatibleFormat` error.
    ///
    /// **Nothing in this crate notices that a variant was ADDED.** An earlier version of
    /// this doc claimed "the existing variant-count assertion in this module" did; there
    /// is no such assertion. The `variants.len() > 10` check in
    /// `no_doc_comment_links_a_journal_event_variant_by_its_bare_name` is a LOWER bound
    /// that guards its own scrape against silently finding nothing, and 24 variants
    /// against a bound of 10 cannot detect one more. `fold_journal`
    /// (`orchestrator::executor::support`) carries a `_` catch-all and absorbs an
    /// unknown variant silently. The one real detector is the exhaustive `label` helper
    /// in `crates/orchestrator/src/executor/tests.rs` — and being `#[cfg(test)]`, it is
    /// invisible to `cargo build --workspace`; only `--all-targets` compiles it.
    /// (Verified: with its three loop-gate arms deleted, `cargo build --workspace` exits 0
    /// while `cargo check --workspace --all-targets` exits 101 with one `E0004`. It earned
    /// its keep again when the s4 review added `LoopGateSettled`: that arm was the single
    /// compile error the whole `--all-targets` build produced.)
    #[test]
    fn the_durable_journal_format_version_is_pinned_at_1() {
        assert_eq!(
            FORMAT_VERSION, 1,
            "the durable journal format version moved. Adding a VARIANT must never move \
             it — if this failed alongside a new variant, that variant is not additive \
             and the change is a format break. A DELIBERATE break edits this assertion \
             too, together with the resume fence's migration story \
             (`JournalError::IncompatibleFormat`)."
        );
    }

    /// The THREE variants round-trip, carrying everything an operator needs to see the
    /// question, the menu, the deadline and the decision off the journal alone — plus, for
    /// the executor, which decision a drive already HONOURED.
    ///
    /// Every field is asserted BY VALUE, and that is the point of the test rather than
    /// pedantry. The version this slice first shipped asserted the decided half with
    /// `matches!(.., JournalEvent::LoopGateDecided { .. })` — the variant TAG only — and
    /// built the awaited half with `deadline: None`, which `assert!(deadline.is_none())`
    /// cannot tell apart from a DROPPED field, because `None` is exactly what serde's
    /// default yields. Mutation-proven vacuous: `#[serde(skip)]` on
    /// `LoopGateDecided::option`, on its `actor`, and on `LoopGateAwaited::deadline` each
    /// left it green. Those three are the fields the slice turns on — `option` is the
    /// decision Task 6 matches against the journaled menu (a lost value becomes `""` and
    /// fails the match per AC14b), and `deadline` is the input to the
    /// expiry-before-decision rule — so the guard has to be able to see them. All three
    /// mutations redden this version.
    ///
    /// The `actor` half also pins the field's REQUIREDNESS, not just its value: see the
    /// unattributed-row case below, which is the only thing standing between the
    /// variant's "an approval always records who claimed to give it" contract and a
    /// one-attribute regression back to a silently blank audit row.
    #[test]
    fn the_loop_gate_events_round_trip() {
        let asked_at =
            chrono::DateTime::<chrono::Utc>::from_timestamp(3_000_000, 0).expect("a valid instant");
        let awaited = JournalEvent::LoopGateAwaited {
            node: NodeId("lp/0/__gate__".into()),
            deadline: Some(asked_at),
            prompt: "Continue?".into(),
            menu: vec![crate::graph::LoopGateOption {
                name: "done".into(),
                stops: true,
            }],
        };
        let json = serde_json::to_string(&awaited).expect("serialises");
        let back: JournalEvent = serde_json::from_str(&json).expect("deserialises");
        match back {
            JournalEvent::LoopGateAwaited {
                node,
                prompt,
                menu,
                deadline,
            } => {
                assert_eq!(node.0, "lp/0/__gate__");
                assert_eq!(prompt, "Continue?");
                assert_eq!(menu.len(), 1);
                // The NAME as well as the flag: the name is what `torii run gate decide
                // --option` is matched against, so a menu that survives the wire without
                // its names is a menu no operator can answer.
                assert_eq!(menu[0].name, "done");
                assert!(menu[0].stops);
                // The exact instant, not merely `is_some()` — this is the field whose
                // silent loss is the never-expires bug the variant's own doc names.
                assert_eq!(deadline, Some(asked_at));
            }
            other => panic!("wrong variant: {other:?}"),
        }

        let decided = JournalEvent::LoopGateDecided {
            node: NodeId("lp/0/__gate__".into()),
            option: "done".into(),
            actor: "jerry".into(),
        };
        let json = serde_json::to_string(&decided).expect("serialises");
        let back: JournalEvent = serde_json::from_str(&json).expect("deserialises");
        match back {
            JournalEvent::LoopGateDecided {
                node,
                option,
                actor,
            } => {
                assert_eq!(node.0, "lp/0/__gate__");
                assert_eq!(option, "done");
                assert_eq!(actor, "jerry");
            }
            other => panic!("wrong variant: {other:?}"),
        }

        // A decision whose `actor` field is ABSENT from the row must FAIL to decode, not
        // decode as `""`. This is the guard on the variant's central doc claim — that the
        // type refuses to express an unattributed approval — and it is a real guard, not
        // a restatement of serde's defaults, because one attribute (`#[serde(default)]`
        // on `actor`) turns the refusal back into a silent `""` and there is otherwise
        // nothing to notice. It replaces the `actor: None` case this test carried while
        // the field was an `Option`, whose premise (an anonymous decision is a legal
        // shape) is exactly what the narrowing removed.
        //
        // Hand-written JSON rather than a serialised value: the shape being pinned is one
        // the type can no longer construct, which is the point.
        let unattributed = r#"{"LoopGateDecided":{"node":"lp/0/__gate__","option":"again"}}"#;
        let err = serde_json::from_str::<JournalEvent>(unattributed)
            .expect_err("an approval with no attribution must not decode");
        assert!(
            err.to_string().contains("actor"),
            "the failure must name the missing field so an operator reading a decode \
             error can see WHICH row is unusable; got: {err}"
        );

        // …and the degenerate value that IS still expressible, `""`, round-trips
        // VERBATIM. Nothing in the wire format normalises it to `unknown` or drops it:
        // resolving an unresolvable actor to a name is torii's job at the WRITE side
        // (`cmd::gate::actor_or`), and a reader inventing one here would launder a writer
        // bug into a plausible-looking audit row.
        let claimed_empty = JournalEvent::LoopGateDecided {
            node: NodeId("lp/0/__gate__".into()),
            option: "again".into(),
            actor: String::new(),
        };
        let json = serde_json::to_string(&claimed_empty).expect("serialises");
        match serde_json::from_str::<JournalEvent>(&json).expect("deserialises") {
            JournalEvent::LoopGateDecided { option, actor, .. } => {
                assert_eq!(option, "again");
                assert_eq!(actor, "", "an empty actor is preserved, never re-labelled");
            }
            other => panic!("wrong variant: {other:?}"),
        }

        // The settlement, whose `option` is the field the whole variant exists to carry:
        // it is what the replay resolves against the journaled menu, so a value lost on
        // the wire becomes `""`, matches nothing in any menu, and turns a settled gate
        // into a terminal `NodeFailed` — killing a `Loop` that had already succeeded, on
        // the very resume path the variant was added to protect. Asserted BY VALUE for
        // the reason the two above are, and mutation-proven the same way
        // (`#[serde(skip)]` on `option` reddens this).
        let settled = JournalEvent::LoopGateSettled {
            node: NodeId("lp/0/__gate__".into()),
            option: "again".into(),
        };
        let json = serde_json::to_string(&settled).expect("serialises");
        match serde_json::from_str::<JournalEvent>(&json).expect("deserialises") {
            JournalEvent::LoopGateSettled { node, option } => {
                assert_eq!(node.0, "lp/0/__gate__");
                assert_eq!(option, "again");
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
            variants.len() > 10
                && variants.contains(&"GateAwaited")
                && variants.contains(&"LoopGateAwaited"),
            "the scrape found {variants:?}. Either it broke — in which case the guard \
             below would pass vacuously, which is what this check exists to prevent — \
             or one of the two named sentinels was legitimately REMOVED, in which case \
             pick a different one. This is a LOWER bound and two spot-checks, not a \
             variant census: it cannot notice a variant being added, and nothing in \
             this crate can (see \
             `the_durable_journal_format_version_is_pinned_at_1`)."
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
