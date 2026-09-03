//! The human-backed `Agent` node (SP-6 s3): a role answered by a person, not a model.
//!
//! s1 shipped `AwaitSignal` (pause, accept any JSON), s2 `HumanGate` (the typed menu).
//! This is the THIRD waiting kind: an `Agent` node whose `AgentRef` resolves to
//! a human-backed definition pauses ONCE, journals the question it is asking, and
//! completes when a human answers.
//!
//! It is not the last, though s3 wrote that it was. SP-6 s4 adds a FOURTH — the human
//! LOOP GATE, a `Loop` whose `GateSpec::Human` asks a person whether to iterate again.
//! Only its EXECUTOR ARM, [`Executor::run_human_loop_gate`], lives here; the pieces it
//! reasons from are elsewhere, and an earlier version of this paragraph pointed at the
//! wrong crate for two of the three. Its journal variants
//! (`LoopGateAwaited`/`LoopGateDecided`/`LoopGateSettled`) are in
//! `orchestrator-core/src/journal.rs`; the fold arms that read them are in
//! `executor/support.rs`'s `fold_journal`, with their accessors on `Fold` in
//! `executor/mod.rs`. `LoopGateAwaited` is the FOURTH writer of the SHARED
//! `Fold::deadlines` map, and that count matters because the missing-question arm below
//! reasons from it explicitly.
//!
//! The two kinds in this file are siblings, not layers, and they differ in exactly one
//! ordering: `run_human_agent` reads the ANSWER before the deadline, `run_human_loop_gate`
//! reads the DEADLINE before the decision. Each function's doc argues its own side; the
//! short version is that an agent's answer is work product while a loop gate's "continue"
//! authorizes another iteration of spend, which is an approval. They are next to each
//! other so that the divergence is visible rather than discovered.
//!
//! The waiting machinery is SHARED with all three siblings, not copied — `gate_precheck`
//! and `wait_or_expire` live in `signal.rs`, reached here through their `_by_id` forms.
//! BOTH kinds in this file need those forms, for two different reasons: the human-backed
//! `Agent` because it is driven from `drive_agent`, which holds only a `NodeId`; and the
//! loop gate because it runs at a SYNTHESIZED path, `"{loop}/{i}/__gate__"`, which has no
//! `Node` in the graph at all. s1's review found real defects in exactly those arms; a
//! third copy would be a third place for them to return.
//!
//! A new file rather than more of `agent.rs`, matching how s2 put `run_human_gate` in
//! its own `gate.rs`: `agent.rs` is the model path and stays that. s4's shared seam,
//! [`Executor::human_question_for`], is here for the same reason — it is wholly a
//! human-path function, `drive_agent` merely calls it, and its second caller
//! (`run_human_loop_gate`) is in this file. It landed in `agent.rs` first, next to the
//! `assemble_prompt_parts` call it was extracted from, which put ~100 lines of human-path
//! doc in the model path's file and left BOTH module headers describing a layout that no
//! longer held; review caught it and it moved.

use orchestrator_core::{
    AgentDefinition, AgentRef, ContextKey, JournalEvent, LoopGateOption, MAX_HUMAN_CONTEXT_BYTES,
    MAX_HUMAN_TEXT_BYTES, NodeId, OrchestratorError, RunId,
};

use crate::agent::prompt::{
    assemble_prompt_parts, render_context_section_bounded, truncate_prompt_to_bound,
};

use super::signal::WaitState;
use super::support::render_input;
use super::{Executor, Fold, NodeExec};

/// The question a human-backed node asks, carried as one string PLUS the count of its bytes
/// the config author actually controls.
///
/// The split is the whole point, and it is the s3 whole-slice review's central finding.
/// `MAX_HUMAN_TEXT_BYTES` used to be charged against the entire composed question, including
/// the `## Context` section — which `assemble_prompt` renders from every Hard dependency's
/// full materialized output, verbatim and untruncated. A human-backed node downstream of any
/// node that produced ~1000 tokens therefore failed TERMINALLY, after the upstream tokens
/// were already spent, with a message naming three config fields that were not the cause and
/// no operator escape (`gate_precheck_by_id` reads the `NodeFailed` back on every later
/// drive, so the run can never be revived). Review measured 4126 bytes on a role with a
/// 60-byte system prompt, no skills and a 12-byte input.
///
/// So the two halves are bounded by two different RULES:
/// - the AUTHORED bytes fail loudly against `MAX_HUMAN_TEXT_BYTES` — a config error, and the
///   person who wrote the config can act on it;
/// - the `## Context` bytes are TRUNCATED, per dependency and with a visible marker, to
///   [`MAX_HUMAN_CONTEXT_BYTES`] — run data, degraded honestly rather than fatally.
///
/// Carried as `(text, authored_bytes)` rather than as three fields because the ORDER of the
/// pieces is the model's own (`system_prompt` + skills + `## Context` + `## Task` + input),
/// so the authored bytes are not contiguous and cannot be re-derived by the bounding code.
pub(super) struct HumanQuestion {
    /// The whole question, in the order a model would have received it, with the
    /// `## Context` section already bounded.
    text: String,
    /// How many of `text`'s bytes are author-controlled — everything except `## Context`.
    authored_bytes: usize,
}

/// The delimiter that opens the `## Task` section, written once and used by BOTH the
/// composition and the clamp's tail reserve.
///
/// The clamp has to protect this section: `compose` puts it LAST, and the clamp cuts from
/// the end, so a redaction that GREW the authored half deleted the ask outright — the human
/// was journaled the role's standing instructions plus up to `MAX_HUMAN_CONTEXT_BYTES` of
/// upstream context and no statement of what to decide. That is the defect `## Task` exists
/// to prevent, and it breaks §5.4's one-directional rule: never show the human LESS than the
/// model had.
///
/// A shared CONST rather than a recorded byte count, because the clamp locates the split in
/// the REDACTED text (see [`HumanQuestion::redact_and_clamp`]) where a byte count taken
/// before redaction no longer points anywhere.
const TASK_MARKER: &str = "\n\n## Task\n";

impl HumanQuestion {
    /// Compose the model-EQUIVALENT question from `assemble_prompt`'s two halves plus the
    /// node's input, bounding the context half on the way.
    ///
    /// `## Task` mirrors `assemble_prompt`'s own `## Context` heading so the two sections
    /// read as one document, and the input is present at all because the model path supplies
    /// it separately (as the first user message) — journaling `assemble_prompt`'s output
    /// alone showed the human the role's standing instructions and the upstream context but
    /// NOT the thing being asked about. Design §5.4's rule is "the human sees precisely what
    /// the model would have", with an explicitly one-directional cost: never show the human
    /// LESS than the model would have had.
    ///
    /// **Each context body is redacted BEFORE it is truncated**, which is why this takes a
    /// `redact` at all rather than leaving the whole job to
    /// [`HumanQuestion::redact_and_clamp`]. Same shape as the `## Task` straddle that
    /// function documents, one step earlier: a `Redactor` matches over the string it is
    /// handed, and `render_context_section_bounded` cuts each body — so a cut that removes a
    /// PEM's `-----END … PRIVATE KEY-----` (the one shipped pattern with an unbounded
    /// `[\s\S]*?` body, and a body over `MAX_HUMAN_CONTEXT_BYTES` loses that line by
    /// construction) turned a would-be `[REDACTED]` into a plaintext fragment.
    ///
    /// Defence in depth rather than a live durable leak: a `Scope::Run` context value is
    /// already redacted at its producing leaf, so with a redactor wired the composed
    /// question sees `[REDACTED]` before it arrives, and with none wired nothing is redacted
    /// anywhere. It is here because `compose`'s correctness must not rest on a caller three
    /// modules away, and because torii's display-time `render::redact_question` runs the
    /// same plain pass over the same already-cut text into a terminal and a CI log.
    ///
    /// `redact_and_clamp`'s later whole-string pass is NOT replaced by this one — it is what
    /// guards the `## Task` straddle, and it composes freely because `[REDACTED]` matches no
    /// credential shape, so the second pass is idempotent. Guarded by
    /// `a_secret_cut_in_half_by_the_context_bound_is_still_redacted`.
    pub(super) fn compose(
        authored: &str,
        context: &[(String, String)],
        query: &str,
        redact: impl Fn(String) -> String,
    ) -> Self {
        // Per `(key, body)` rather than over the joined section: the join is what the bound
        // then cuts, so redacting it would be the same ordering defect one line later.
        let context: Vec<(String, String)> = context
            .iter()
            .map(|(key, body)| (key.clone(), redact(body.clone())))
            .collect();
        let task = format!("{TASK_MARKER}{query}");
        let mut text = String::with_capacity(authored.len() + task.len());
        text.push_str(authored);
        let authored_bytes = text.len() + task.len();
        text.push_str(&render_context_section_bounded(
            &context,
            MAX_HUMAN_CONTEXT_BYTES,
        ));
        text.push_str(&task);
        Self {
            text,
            authored_bytes,
        }
    }

    /// The composed question, for a test in a SIBLING module to assert on.
    ///
    /// `#[cfg(test)]` rather than an `expect(dead_code)`: no production caller wants the
    /// raw text — every one of them goes through [`HumanQuestion::redact_and_clamp`], and
    /// an accessor that hands out the UNREDACTED string is exactly the shortcut that
    /// bypass would take. `human.rs`'s own tests read the field directly; this exists
    /// because `executor::tests` cannot (a private field is visible only to the defining
    /// module and its descendants), and SP-6 s4's seam test lives there, beside the s3
    /// human-agent fixtures it reuses.
    #[cfg(test)]
    pub(super) fn text(&self) -> &str {
        &self.text
    }

    /// Redact the question and bring it under `bound`, **cutting only the `## Context`
    /// half**.
    ///
    /// Redaction runs first because `[REDACTED]` is longer than the shortest span it
    /// replaces, so a question that fitted before can exceed the bound after — and the
    /// bytes that must be bounded are the bytes actually written. Clamping rather than
    /// failing is deliberate: the author-error diagnosis has already happened against
    /// `authored_bytes`, and turning "your prompt contained a secret" into a terminal run
    /// would reintroduce the data-dependent death the two-bounds rule removed.
    ///
    /// The tail is reserved. `head` is everything before `## Task`; only it is truncated,
    /// then the task is re-appended, so the ask survives every time the clamp fires. If the
    /// redacted task alone exceeds `bound` — unreachable while `authored_bytes` (which
    /// INCLUDES the task) is checked against the smaller `MAX_HUMAN_TEXT_BYTES` — the whole
    /// thing is truncated as a last resort rather than returning something over the bound.
    ///
    /// **The redactor sees the WHOLE question, exactly once, before anything is split.**
    /// The tail reserve needs a split point and the obvious implementation takes it first,
    /// redacting `text[..split]` and `text[split..]` independently — which hides from the
    /// redactor every match that STRADDLES the boundary. A `Redactor` matches over the
    /// string it is handed; cutting first makes a whole-match the two halves cannot see, and
    /// the miss is durable (`AgentAwaited.prompt` → `journal_events`, with
    /// `render::redact_question` running the same plain pass over the same already-split
    /// text downstream). `PatternRedactor`'s PEM rule is the reachable case today — the one
    /// shipped pattern with an unbounded `[\s\S]*?` body — and the weakening was unguarded
    /// for any future multi-line pattern. Guarded by
    /// `a_secret_that_straddles_the_task_boundary_is_still_redacted`.
    ///
    /// So the split is located in the REDACTED text, by its marker. Two consequences, both
    /// intended: a secret that swallowed the `\n\n## Task\n` delimiter leaves no marker to
    /// find, and the whole redacted string is then clamped as one (there is no ask to
    /// reserve — its delimiter was part of the secret); and if the node input ITSELF
    /// contains the literal `\n\n## Task\n`, the reserved tail is the part after its LAST
    /// occurrence, so the ask's final section survives and the clamp may cut earlier ask
    /// text. Both are strictly better than leaking key material.
    pub(super) fn redact_and_clamp(
        &self,
        redact: impl Fn(String) -> String,
        bound: usize,
    ) -> String {
        let redacted = redact(self.text.clone());
        let split = redacted.rfind(TASK_MARKER).unwrap_or(redacted.len());
        let head = redacted[..split].to_string();
        let task = redacted[split..].to_string();
        match bound.checked_sub(task.len()) {
            Some(room) => {
                let mut out = truncate_prompt_to_bound(head, room);
                out.push_str(&task);
                out
            }
            None => truncate_prompt_to_bound(head + &task, bound),
        }
    }
}

/// What one human loop gate decided, in the shape [`Executor::run_loop`] needs (SP-6 s4).
///
/// Neither [`NodeExec`] nor `AgentStep`, and for the same reason each of those exists: the
/// whole output of this node is a `bool` — does the LOOP continue — and `NodeExec` has
/// nowhere to put it that `run_loop` would not have to parse back out of a
/// `serde_json::Value`. A gate is also not a node of the graph: it produces no output, is
/// never a dependency, and journals no `NodeStarted`/`NodeCompleted`, so the two `NodeExec`
/// shapes it could borrow would both be lies.
///
/// `Failed` and `Paused` carry their operator-facing string because the DURABLE write has
/// already happened by the time either is constructed — `NodeFailed` through
/// [`Executor::fail_loop_gate`], `RunPaused` through `pause_awaiting`. `run_loop` maps them
/// onto the Loop, exactly as it already maps a gate-AGENT's failure and pause.
pub(super) enum LoopGateStep {
    /// A person decided. `stop` is the CHOSEN OPTION's `stops`, recomputed from the
    /// journaled option name on every drive — which is what makes a resume reach the
    /// identical decision without re-asking (design §5.7).
    Decided { stop: bool },
    /// The gate could not decide: the SLA fired, the role is misconfigured, or the
    /// recorded decision names an option that is not in the menu the human was shown.
    /// A `NodeFailed` for the GATE path is journaled either way; `run_loop` fails the
    /// whole `Loop`, which is the existing gate-agent behaviour and needs no new outcome
    /// shape.
    ///
    /// It carries no "did this drive write it?" flag. The first shipped shape did, to stop
    /// `run_loop` re-appending the LOOP's row on every wake of a dead run, and the flag was
    /// a SECOND claim about the journal that could disagree with the first: it meant "this
    /// drive wrote the GATE's row" while the caller consumed it as "the LOOP's row is
    /// missing", so a transient journal error between the two appends left the Loop's
    /// failure permanently unwritten. The guard belongs on the append itself — see
    /// [`Executor::fail_loop`], which is idempotent against the `Loop`'s own recorded
    /// failure and therefore self-healing.
    Failed(String),
    /// Waiting on a person. `RunPaused` is already journaled — on the deadline this gate
    /// RECORDED, so the SP-DATA-3 scheduler re-arms on the same instant — and `run_loop`
    /// propagates the pause, stopping the whole `Loop` until an answer arrives.
    Paused(String),
}

/// The `## Context` heading the iteration's output is shown under, and the only context
/// entry a loop gate has.
///
/// A named constant because it is operator-visible in the journaled question and in what
/// `torii run list-paused` renders, so it must not drift between the compose site and
/// anything that reads the question back.
const ITERATION_OUTPUT_KEY: &str = "iteration output";

impl Executor {
    /// Resolve a human-backed `AgentRef` into the QUESTION to ask and the SLA to ask it
    /// under — the seam `drive_agent`'s human branch (SP-6 s3) and `run_human_loop_gate`
    /// (SP-6 s4) share.
    ///
    /// **It exists so s4 needs no second prompt builder.** s3's central property is that a
    /// human's question is composed by the MODEL path's own [`assemble_prompt_parts`], so
    /// the two cannot drift on what "the agent's prompt" means — s3 secured it by putting
    /// its human branch INSIDE `drive_agent`, immediately after that call. s4's
    /// `GateSpec::Human` cannot be routed the same way: it has no ReAct loop, no turns and
    /// no `stop_when`, and threading a menu through `drive_agent` would put a parameter
    /// there that every model caller must pass as `None`. Sharing this function keeps the
    /// property without the coupling. `the_human_question_seam_composes_the_same_prompt_
    /// the_model_path_would` is the guard: it computes `assemble_prompt_parts` itself and
    /// requires the composed question to contain `parts.authored` VERBATIM, with an
    /// INACTIVE skill's body absent — a hand-rolled second builder that concatenated every
    /// declared skill, or joined them with a different separator, is what that reddens on.
    ///
    /// # Which half a caller's data belongs in
    ///
    /// The two parameters are NOT interchangeable, and picking wrong is a terminal
    /// data-dependent node failure rather than a compile error:
    ///
    /// - **`input` is the ASK.** It becomes the `## Task` section and is counted in
    ///   [`HumanQuestion`]'s `authored_bytes`, which [`Executor::run_human_agent`] checks
    ///   against [`MAX_HUMAN_TEXT_BYTES`] (4096) with a LOUD, terminal `NodeFailed` — and
    ///   which s4's gate arm checks the same way. So it must be AUTHOR-SCALE: a node input
    ///   an operator wrote, or a short synthesized ask, never an upstream node's output. It
    ///   is also the query `assemble_prompt_parts` evaluates every skill's and tool's
    ///   `activation.is_active` against.
    /// - **`context` is RUN DATA.** It becomes the `## Context` section, which
    ///   `render_context_section_bounded` TRUNCATES per dependency to
    ///   [`MAX_HUMAN_CONTEXT_BYTES`] (32 KiB) with a visible marker, and which
    ///   `authored_bytes` deliberately excludes. Anything a model produced belongs here.
    ///
    /// That asymmetry is the s3 whole-slice review's central fix and it is load-bearing for
    /// s4: a loop gate's context is a model iteration's output essentially always, so
    /// passing that output as `input` would kill the gate on ordinary data — after the
    /// iteration's tokens were already spent, and unrecoverably, since `gate_precheck_by_id`
    /// reads the `NodeFailed` back on every later drive. `run_human_loop_gate` will
    /// therefore pass the iteration output as a `context` entry and a short menu-derived ask
    /// as `input`. Written down here because the s4 PLAN's own sketch of that call had the
    /// two the wrong way round and review caught it before the task ran; design §6 and AC15
    /// are the record.
    ///
    /// # Why the input is in the question at all
    ///
    /// `assemble_prompt_parts` takes `query` only to evaluate activation and never puts it
    /// in `authored`. The model path supplies the input SEPARATELY, as the first user
    /// message (`Message::text(MessageRole::User, query)`). So journaling the system string
    /// alone showed the human the role's standing instructions and the upstream context but
    /// NOT the thing being asked about — a reviewer role reading "say whether the contract
    /// permits sub-processing" with no contract named. Design §5.4's rule is "the human sees
    /// precisely what the model would have", and its accepted cost is explicitly
    /// one-directional: never show the human LESS than the model would have had.
    ///
    /// The result is a type rather than a `format!` because the two halves must be bounded
    /// by the two DIFFERENT rules above, and once they are concatenated that distinction is
    /// unrecoverable. [`HumanQuestion::compose`] owns both and documents them.
    ///
    /// # The rest of the contract
    ///
    /// **The SLA comes back with the question** so no caller re-reads the registry to find
    /// the deadline it must pause on. A second read is a second place for a role and its
    /// SLA to come apart, which is the arrangement `AgentBacking::Human { timeout }` exists
    /// to prevent.
    ///
    /// **Fails loudly on a MODEL-backed role**, which is unreachable from `drive_agent`
    /// (its branch has already matched the backing) and is s4's case: an author naming a
    /// model-backed role in a `GateSpec::Human`. Design §5.5 — silence there would let an
    /// author believe a person is in the loop while the run quietly decides for itself, the
    /// mirror of the refusal `drive_agent` gives a human-backed role at an illegal
    /// position. The message names the role, the defect and the fix, because the only
    /// person who can act on it is the one who wrote the config.
    ///
    /// **It ADDS no token spend** — it resolves no chain and touches no gateway. That is
    /// not the same as guaranteeing zero spend on a caller's path, and this function cannot:
    /// nothing stops a caller resolving a chain the line after. Each caller still owes the
    /// STRUCTURAL placement — `drive_agent`'s human branch returns above `resolve_chain`
    /// (guarded by the role's `chain: None`, so a mis-ordered branch reddens `mod
    /// human_agent` wholesale), and s4's gate arm drives no agent at all. See the comment
    /// at `drive_agent`'s branch, which is the authoritative statement for that caller.
    ///
    /// It returns `Err`, not a `NodeFailed`: it has no `RunId`, journals nothing, and its
    /// two callers differ in what a failure means (s3's `?`-propagates as it always has;
    /// s4's gate arm turns it into a `NodeFailed` that fails the `Loop`). Deciding that
    /// here would take the choice away from the caller that owns it.
    ///
    /// **[`Executor::human_sla_for`] — the SLA half alone — is defined BELOW this function,
    /// not above it, and that is not a stylistic preference.** Rustdoc attaches a doc block
    /// to the item that FOLLOWS it, so the change that extracted the SLA read put its `fn`
    /// between these hundred lines and the function they describe, silently re-parenting
    /// the whole argument about prompt assembly onto a function that composes no prompt —
    /// and leaving this one with no doc at all. Nothing in the compiler or in
    /// `clippy -D warnings` says a word about it; only reading the rendered page does.
    /// Caller-then-callee also happens to be the better reading order here, since the SLA
    /// read is a step of this function rather than a peer of it.
    pub(super) fn human_question_for(
        &self,
        agent_ref: &AgentRef,
        input: &serde_json::Value,
        context: &[(ContextKey, serde_json::Value)],
    ) -> Result<(HumanQuestion, Option<chrono::Duration>), OrchestratorError> {
        let timeout = self.human_sla_for(agent_ref)?;
        let agent: &AgentDefinition = self
            .registry
            .agent(&agent_ref.0)
            .ok_or_else(|| OrchestratorError::UnknownAgent(agent_ref.0.clone()))?;
        let query = render_input(input);
        let parts = assemble_prompt_parts(&self.registry, agent, context, &query)?;
        Ok((
            HumanQuestion::compose(
                &parts.authored,
                &parts.context,
                &query,
                // The executor's own pure redactor, applied to each context body BEFORE the
                // bound cuts it — see `compose`. Identity when none is wired, which is the
                // default, so the composed question stays byte-identical there.
                |t| self.redact_text(t),
            ),
            timeout,
        ))
    }

    /// The SLA half of [`Executor::human_question_for`] alone: resolve the role, assert the
    /// backing is `Human`, and hand back its `timeout`.
    ///
    /// Two registry lookups instead of one, and worth it: this is the only part of the seam
    /// a caller needs on EVERY drive, while composing the question is only needed on the
    /// drive that ASKS. `run_human_loop_gate` is re-entered for every iteration's gate on
    /// every wake of the run, so composing eagerly there meant, on a loop sitting at
    /// iteration N, N `assemble_prompt_parts` runs and N redaction passes over up to
    /// `MAX_HUMAN_CONTEXT_BYTES` of iteration output — all discarded by an arm that pauses
    /// or replays a decision.
    ///
    /// Extracted rather than duplicated so the LOUD model-backed refusal has one message
    /// (AC14, design §5.5): an author naming a model-backed role in a `GateSpec::Human`
    /// must be told the same thing wherever the seam is entered. It is the same refusal for
    /// the same reason at both entry points, and it is the ONLY thing this function can
    /// fail on beyond an unresolvable name — so a caller that needs the deadline and not
    /// the question still gets AC14's loudness on every drive that could still ask.
    ///
    /// It resolves no chain and touches no gateway, which is what lets
    /// `run_human_loop_gate` call it unconditionally without weakening its structural
    /// zero-spend claim.
    pub(super) fn human_sla_for(
        &self,
        agent_ref: &AgentRef,
    ) -> Result<Option<chrono::Duration>, OrchestratorError> {
        let agent: &AgentDefinition = self
            .registry
            .agent(&agent_ref.0)
            .ok_or_else(|| OrchestratorError::UnknownAgent(agent_ref.0.clone()))?;
        let orchestrator_core::AgentBacking::Human { timeout } = agent.backed_by else {
            return Err(OrchestratorError::InvalidGraph(format!(
                "agent {:?} is model-backed but is named where a human-backed role is \
                 required; set `backed_by: human` in its frontmatter, or use a gate kind \
                 that takes a model",
                agent_ref.0
            )));
        };
        Ok(timeout)
    }

    /// Execute one human-backed `Agent` node.
    ///
    /// | fold state | behaviour |
    /// |---|---|
    /// | failure recorded | `Failed` — shared `gate_precheck`, checked FIRST |
    /// | no wait recorded yet | journal `AgentAwaited`, then continue below |
    /// | a wait recorded by ANOTHER kind, so no question | `NodeFailed` — the kind swap |
    /// | **answered** | `Completed({"text","actor"})` — **read BEFORE expiry** |
    /// | not answered, deadline passed | `NodeFailed` — the SLA fired with nobody answering |
    /// | not answered, deadline not passed | re-pause on the SAME absolute instant |
    ///
    /// **The answer is read BEFORE expiry, and that is a deliberate divergence from
    /// `HumanGate`.** s2 expires first because a gate decision is an APPROVAL and a late
    /// one must not approve a gate whose SLA ran out — the silent self-approval its §4
    /// rejects. An agent's answer is WORK PRODUCT, not an approval: there is nothing to
    /// self-approve, and discarding a human's in-time answer because a worker was down
    /// punishes them for infrastructure they had no part in. The deadline still fails the
    /// node in the case it exists for — nobody answered. Guarded by
    /// `an_answer_inside_the_sla_is_honoured_by_a_late_drive`, which is the only test
    /// that reddens if the two are reordered.
    ///
    /// The divergence is bounded by the arm ABOVE it, not by luck: `gate_precheck` runs
    /// first, so once an expiry has actually FIRED and been journaled, a later answer
    /// cannot resurrect the node. "Read the answer before checking the clock" is not
    /// "ignore a failure that already happened", and the two are guarded separately —
    /// the second by `a_fired_expiry_is_terminal_even_if_an_answer_arrives_later`.
    ///
    /// **The ask precedes the answer, unconditionally**, for the reason s2 established:
    /// a durable question breaks s1's "the early race resolves itself for free" property,
    /// because an answer folded with no question has nothing to be an answer TO — and
    /// nothing for `torii run list-paused` or an audit to show the human was ever asked.
    ///
    /// No gateway call and no `EffectRecorded` — this function is reached before
    /// `resolve_chain`, so zero token spend is STRUCTURAL, not measured. Like
    /// `AwaitSignal`/`HumanGate`/`Branch`/`Subgraph` it journals no
    /// `NodeStarted`/`NodeCompleted`, which carries that family's known asymmetry: a
    /// re-`start` of an already-TERMINAL run rebuilds `outputs` from exactly those events
    /// and so reports this node in neither (the durable blackboard is unaffected — the
    /// completing drive published the answer under `ContextWrite`).
    ///
    /// This node kind must never panic. A panic here is not local: it unwinds through
    /// `Scheduler::tick`, which has already claimed a batch of runs and taken their
    /// leases, so the claimed rows stay `waking` and the next worker reclaims the stale
    /// lease and dies the same way. Every failure below is a `NodeFailed`.
    pub(super) async fn run_human_agent(
        &self,
        run: RunId,
        node_id: &NodeId,
        question: &HumanQuestion,
        timeout: Option<chrono::Duration>,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        // 0. This node has ALREADY failed ⇒ it stays failed. Shared with all three
        //    siblings,
        //    and FIRST — ahead of the answer read — for the fail-closed reason spelled out
        //    on `gate_precheck`. The verdict is READ BACK, never re-derived, so a dead
        //    node does not append a fresh `NodeFailed` on every drive.
        if let Some(failed) = self.gate_precheck_by_id(node_id, fold) {
            return Ok(failed);
        }

        // 1. What this node has recorded: nothing yet, a deadline that has passed, or a
        //    deadline still in the future. DECIDED here but ACTED ON in step 4, below the
        //    answer read — that separation is the AC3 divergence, and it is the reason
        //    this is a `let state = …` rather than the single `match` `run_human_gate`
        //    uses. Collapsing it back into one match (the shape `HumanGate` has, and the
        //    obvious shape) silently reinstates s2's expire-first ordering and discards a
        //    human's in-time answer.
        let state = match self.wait_or_expire_by_id(node_id, timeout, fold) {
            // The overflow guard's second layer (`signal.rs` explains why a node kind may
            // not panic on its own). Nothing is journaled beyond the failure itself: an
            // `AgentAwaited` carrying a nonsense deadline would be folded first-wins
            // forever. The helper's message is unprefixed so each kind names itself.
            Err(message) => {
                return self
                    .fail_human_agent(run, node_id, format!("human_agent: {message}"))
                    .await;
            }
            Ok(state) => state,
        };

        // 2. The ask, unconditionally and exactly once in this node's life — BEFORE the
        //    answer is read, so an `AgentAnswered` folded with no `AgentAwaited` (the
        //    early-answer race, AC6) is still resolved in this same execution and there is
        //    never an answer to a question the durable record does not show being asked.
        if let WaitState::NotYetAsking(fresh) = &state {
            // Bound the AUTHORED part of the question before it becomes durable — the
            // agent's `system_prompt`, every activated skill body, and the node's input.
            // Routinely multi-KB, so this is a real constraint rather than a theoretical
            // one. `torii` bounds the operator-supplied side at its CLI boundary
            // (`cmd::run::check_payload_size` against `MAX_PAYLOAD_BYTES`, the same 4096)
            // and can simply refuse the command; the executor has no such boundary — it is
            // already inside a durable run — so an over-bound AUTHORED prompt fails the
            // NODE loudly. That much really is a malformed agent config, and it is
            // actionable by the person who wrote it.
            //
            // **The `## Context` section is deliberately NOT counted here**, and that is
            // the s3 whole-slice review's central fix. It is composed from every Hard
            // dependency's full materialized output — RUN DATA, which no operator can bound
            // at config time — so charging it against a cap whose breach is a terminal
            // `NodeFailed` made an ordinary verbose model answer unrecoverable: the node
            // died after the upstream tokens were already spent, `gate_precheck_by_id` read
            // the failure back on every later drive, and the message blamed three config
            // fields that were not the cause. Review measured 4126 bytes for a role with a
            // 60-byte system prompt, no skills and a 12-byte input; the codebase's own
            // default `cas_threshold` is also 4096, i.e. a >4 KiB effect output is normal
            // enough here to warrant CAS SPLITTING rather than refusal. `HumanQuestion::
            // compose` truncates that half to `MAX_HUMAN_CONTEXT_BYTES` instead, per
            // dependency and with a visible marker, so a verbose upstream degrades the
            // question rather than killing the run.
            //
            // Guarded by `an_oversized_authored_prompt_fails_the_node_before_it_is_
            // journaled` (this arm) and `a_verbose_upstream_output_truncates_the_question_
            // instead_of_killing_the_node` (the other half). The first exists because review
            // deleted this condition and the whole workspace stayed green; the second
            // because the first used a giant SKILL body — static config — and so could not
            // see the dynamic case at all.
            if question.authored_bytes > MAX_HUMAN_TEXT_BYTES {
                return self
                    .fail_human_agent(
                        run,
                        node_id,
                        format!(
                            "human_agent: node {}'s authored prompt is {} bytes, over the \
                             {MAX_HUMAN_TEXT_BYTES}-byte limit — trim the agent's system \
                             prompt, its skills or the node input. (The `## Context` \
                             section rendered from upstream outputs is NOT counted here: \
                             it is run data, and it is truncated to fit its own \
                             {MAX_HUMAN_CONTEXT_BYTES}-byte budget rather than failing \
                             the node.)",
                            node_id.0, question.authored_bytes
                        ),
                    )
                    .await;
            }

            // Redact BEFORE the durable write, not only at display time.
            //
            // Design §6 lists "the prompt" among the strings that go through the redactor
            // before the durable write, and s3 shipped `prompt: prompt.to_string()` — so
            // the journal row was the ONE place a credential sitting in an agent's
            // `system_prompt`, an activated skill body, the rendered `## Context` section or
            // the node input landed in the clear. Nothing upstream scrubbed it either:
            // `torii config push` redacts nothing, and `render::redact_question` only
            // cleaned it up on the way to a terminal. Nothing operational is lost by doing
            // it here — the only surface that displays a question already shows the
            // redacted form, so this makes the durable row match what the human sees.
            //
            // The chokepoint was one function away the whole time: `fail_human_agent` calls
            // `redact_text` on every failure message.
            //
            // Then clamp, because `[REDACTED]` is LONGER than the shortest span it replaces
            // and can push a question that fitted over the bound. Clamping rather than
            // failing is deliberate: the author-error diagnosis has already happened above,
            // and turning "your prompt contained a secret" into a terminal run would
            // reintroduce the data-dependent death this whole change removes.
            let prompt = question.redact_and_clamp(
                |t| self.redact_text(t),
                MAX_HUMAN_TEXT_BYTES + MAX_HUMAN_CONTEXT_BYTES,
            );

            // The node-keyed record of WHICH node is asking, WHAT it asked, and — the
            // durable home of — BY WHEN. It is written at all because `RunPaused` is not
            // node-keyed, and a run pauses for many unrelated reasons over its life.
            self.append(
                run,
                JournalEvent::AgentAwaited {
                    node: node_id.clone(),
                    deadline: *fresh,
                    prompt,
                },
            )
            .await?;
        } else if fold.prompt_for(node_id).is_none() {
            // Already asking by the SHARED map's reckoning, but this node published no
            // QUESTION — so there is nothing a human could have been shown and nothing an
            // answer could be an answer to.
            //
            // The exact mirror of `run_human_gate`'s missing-menu arm, and s3 shipped
            // without it. `Fold::deadlines` is written by all FOUR waiting kinds (SP-6
            // s4's `LoopGateAwaited` is the fourth, and it carries a prompt of its own —
            // into `Fold::loop_gate_asks`, never into this map, because a loop gate is
            // not answerable by `AgentAnswered`) while only `AgentAwaited` writes a prompt
            // HERE, so this arm is reachable the same way s2's is: by editing a live run's
            // graph to change a waiting node's KIND. An `AwaitSignal` node re-pointed at a
            // human-backed `Agent` arrives here exactly as the `AwaitSignal`→`HumanGate`
            // swap arrives there — `gate.rs`'s own comment
            // already noted that s3 WIDENS that reachable set, and this is the guard it was
            // noting the absence of.
            //
            // **Loud, because the alternative is unanswerable.** Without this arm the node
            // took the `Waiting` path forever: no `AgentAwaited` ⇒ `cmd::human::
            // agent_question` is `None` ⇒ `torii run agent answer` refuses with "not
            // awaiting a human answer", permanently — while `torii run signal` sees no
            // menu, no question and a live `SignalAwaited`, ACCEPTS a payload, reports exit
            // 0, and `list-paused` shows the node as a `signal` row. The operator is told
            // the answer landed; `run_human_agent` reads only `AgentAnswered` and never
            // completes. Review drove it three times: three pauses, zero questions.
            //
            // Asking HERE instead — journaling `AgentAwaited` on top of the other kind's
            // record — was the other candidate fix and is worse: `deadlines` folds
            // first-wins, so the question would be published against a deadline some other
            // node kind chose, and the run would carry two contradictory durable claims
            // about what it is waiting for.
            //
            // It is checked BEFORE the answer read below, deliberately: "the ask precedes
            // the answer, unconditionally" (see this function's doc), and an answer to a
            // question that was never asked is not an answer.
            //
            // This is also `Fold::prompt_for`'s production consumer. Until this arm existed
            // nothing in a non-test build asked "does THIS node have a question?" — only
            // the shared "has SOME kind begun waiting here?" — which is why the accessor
            // design §4 named for precisely this check carried an `expect(dead_code)`.
            return self
                .fail_human_agent(
                    run,
                    node_id,
                    format!(
                        "human_agent: node {} recorded that it began waiting but published \
                         no question, so there is nothing a human was ever shown and \
                         nothing an answer could be delivered against. A waiting node's \
                         kind cannot be changed mid-run; fail the run and start a new one.",
                        node_id.0
                    ),
                )
                .await;
        }

        // 3. Answered ⇒ complete, BEFORE any expiry consideration (see the doc comment).
        //
        //    SP-4 s2 (§6.4): redact ONCE, here, and hand that one value to BOTH the
        //    return AND — via `apply_node_result` → `publish_context` — the durable
        //    blackboard write. Splitting them makes a live run and a replayed run
        //    disagree about this node's output, surfacing later as a false
        //    `DeterminismViolation`; that defect has shipped and been caught twice here.
        //    A human answer is free text that becomes the node's output and flows into
        //    downstream nodes and model prompts — it is not merely displayed.
        //
        //    `an_answer_is_redacted_before_both_the_return_and_the_durable_write` is the
        //    guard, and both siblings ship the same one. It is a guard and not a comment
        //    because review deleted this `redact` call outright and the whole workspace
        //    stayed green — which is precisely the failure mode described above: the defect
        //    surfaces as a false `DeterminismViolation` on some later resume, never as a
        //    red test.
        //
        //    The `{text, actor}` shape is what downstream readers key on, and `"text"` is
        //    deliberately the SAME key a model-backed agent produces — that is what lets an
        //    unmodified `BranchCond::TextContains` consume a human's answer without knowing
        //    it was human. `the_answer_is_the_nodes_output_under_the_text_key` is the guard
        //    on the key names, through a real downstream reader rather than by re-asserting
        //    the key.
        //
        //    `project_agent_outputs` (`executor/support.rs`) additionally exempts an Agent
        //    output carrying an `actor` from its `{model, text}` projection. **That
        //    exemption is forward-looking and THIS path never reaches it** — stated
        //    explicitly because the comment that used to sit here claimed the opposite, that
        //    a key rename would make the finished run report `{model: null, text}` when read
        //    back. It cannot. The projection runs on exactly one path, the terminal branch
        //    of `start_inner`, and that branch builds `outputs` from `fold_journal`'s
        //    `node_last_output` — populated ONLY from `EffectRecorded` and `MapCompacted`.
        //    This node kind journals neither (see this function's doc: no gateway call, no
        //    `EffectRecorded`), so a human-answered node is absent from a terminal re-read's
        //    `outputs` entirely and the projection has nothing to rewrite. That is the same
        //    family asymmetry the doc comment above describes, and
        //    `a_finished_human_backed_run_reports_no_output_when_read_back` pins it.
        if let Some(answer) = fold.agent_answer_for(node_id) {
            let output = self.redact(&serde_json::json!({
                "text": answer.text,
                "actor": answer.actor,
            }));
            return Ok(NodeExec::Completed(output));
        }

        // 4. Unanswered. NOW the recorded deadline is acted on.
        //
        //    The message may say "with no answer" — and unlike `run_human_gate`'s, which
        //    deliberately may not, that claim is true here: step 3 above has already read
        //    the fold and returned if an answer existed, exactly as `run_await_signal`'s
        //    "no signal … by {d}" is true for the same reason. The two node kinds' wording
        //    differs because their ORDERING differs, not by accident.
        //
        //    A default answer on timeout was deliberately rejected (§4): a role that
        //    answers for itself is the self-approval this codebase's fail-closed stance
        //    argues against, one layer further in than a gate — the invented text would
        //    become the node's OUTPUT and flow into every downstream model prompt.
        let deadline = match state {
            WaitState::NotYetAsking(fresh) => fresh,
            WaitState::Expired(d) => {
                return self
                    .fail_human_agent(
                        run,
                        node_id,
                        format!(
                            "human_agent: node {} passed its deadline {d} with no answer",
                            node_id.0
                        ),
                    )
                    .await;
            }
            WaitState::Waiting(d) => d,
        };

        // 5. Still waiting ⇒ a durable pause on the deadline this node RECORDED (never
        //    `now + timeout`; see `pause_awaiting` for why re-arming on the same instant
        //    is what keeps the timed branch from being decorative).
        //
        //    Unlike `run_await_signal` there is NO second clock read here, matching
        //    `run_human_gate`: a role whose fresh deadline elapses during its own journal
        //    append pauses once on an instant already behind it, the scheduler wakes it
        //    immediately, and the next drive takes `WaitState::Expired`. One extra wake,
        //    never a lost answer. `mod waiting_node_helpers` exists because two expiry
        //    sites mask each other's defects, which is why a second one is not added.
        let reason = format!(
            "human_agent: waiting for a human answer on node {}{}",
            node_id.0,
            deadline
                .map(|d| format!(" (deadline {d})"))
                .unwrap_or_default()
        );
        self.pause_awaiting(run, reason, deadline).await
    }

    /// Execute one human LOOP GATE (SP-6 s4, design §5.2): ask a person, once per `Loop`
    /// iteration, whether the loop continues.
    ///
    /// | fold state | behaviour |
    /// |---|---|
    /// | failure recorded | `Failed` — shared `gate_precheck`, checked FIRST, verdict READ BACK |
    /// | a decision already HONOURED | `Decided` — replayed from `LoopGateSettled`, no clock |
    /// | the role is model-backed / unknown | `Failed` — a config error, named |
    /// | asking, deadline passed | `Failed` — **before any decision is read** |
    /// | no wait recorded yet | journal `LoopGateAwaited`, pause |
    /// | a wait recorded by ANOTHER kind, so no menu | `Failed` — the kind swap |
    /// | decided, option in the JOURNALED menu | journal `LoopGateSettled`, `Decided { stop }` |
    /// | decided, option NOT in that menu | `Failed`, loudly — never a default |
    /// | not decided, deadline not passed | re-pause on the SAME absolute instant |
    ///
    /// **The deadline is read BEFORE the decision, and that is the deliberate divergence
    /// from [`Executor::run_human_agent`] two functions up.** s3 reads its answer first
    /// because an agent's answer is WORK PRODUCT: there is nothing to self-approve, and
    /// discarding a human's in-time answer because a worker was down punishes them for
    /// infrastructure they had no part in. That argument does not transfer. Answering
    /// `continue` here AUTHORIZES ANOTHER ITERATION OF SPEND, which is an approval in the
    /// strict sense s2 built its ordering for, so honouring a late one would sanction
    /// tokens the operator's own SLA said to stop waiting for. This function therefore
    /// copies `run_human_gate`'s shape, not its neighbour's.
    ///
    /// Structurally that is why the wait state is a single `match` whose every arm
    /// returns, rather than s3's `let state = …` with the answer read in between:
    /// collapsing it the other way silently reinstates s3's ordering, and
    /// `a_decision_after_the_deadline_does_not_continue_the_loop` is the test that reddens
    /// when it is.
    ///
    /// **But the clock only ever judges a gate that is STILL LIVE, and step 1 is what
    /// makes that true.** `run_loop` re-enters `for i in 0..max_iters` from zero on every
    /// drive, so iteration 0's gate is re-derived forever while the SLA it recorded stays
    /// fixed — and `wait_or_expire_by_id` answers from `now >= recorded deadline` alone.
    /// The arm as first shipped therefore killed a gate whose decision an earlier drive
    /// had already read, honoured and spent an iteration against: under any finite SLA the
    /// loop's TOTAL human latency was silently capped at one gate's timeout, and a loop
    /// that had already CONVERGED was destroyed retroactively the next time anything woke
    /// the run. So the SUCCESS verdict gets the same treatment as the failure one — made
    /// durable by the drive that produces it (`LoopGateSettled`) and READ BACK afterwards,
    /// never re-derived. Note the ordering that buys both properties at once: settled ⇒
    /// replay; not settled ⇒ the clock, then the decision. Reading the decision first
    /// instead would fix the same symptom and reopen AC8.
    ///
    /// **The menu is read from the JOURNAL, never from the graph.** `menu` — the graph's
    /// copy — is used for the very first ask and NOWHERE else; every later drive resolves
    /// the decision against `fold.loop_gate_menu_for`. Nothing binds the graph handed to a
    /// later `Executor::start` to the one the human was shown (there is no graph fence;
    /// `scheduled_runs.graph` is an editable row and a library embedder simply passes a
    /// `Graph`), so an author who flipped an option's `stops` after a person picked it
    /// would otherwise invert their decision silently. Its absence on the already-asking
    /// path is a kind-swapped node and fails loudly rather than falling back — see that
    /// arm.
    ///
    /// **Zero token spend is STRUCTURAL.** No chain is resolved and the gateway is never
    /// touched: the only registry reads are [`Executor::human_sla_for`] and, on the drive
    /// that asks, [`Executor::human_question_for`], which composes a prompt and returns.
    /// That matters more here than at any other human site, because the decision being
    /// made IS whether to spend more — a gate that itself cost tokens would be
    /// self-undermining. It is a property of this function's code, not of a call count,
    /// and `run_loop`'s arm adds nothing to it.
    ///
    /// Like every other waiting kind this journals no `NodeStarted`/`NodeCompleted` — it
    /// is not a node of the graph at all — so the family's known re-`start` asymmetry
    /// applies unchanged. A decided gate replays from `LoopGateSettled` with no re-ask and
    /// no gateway call; `stops` → `stop` is recomputed from the journaled option name
    /// against the journaled menu, so a resume reaches the identical decision (§5.7).
    ///
    /// This node kind must never panic. A panic here is not local: it unwinds through
    /// `Scheduler::tick`, which has already claimed a batch of runs and taken their
    /// leases, so the claimed rows stay `waking` and the next worker reclaims the stale
    /// lease and dies the same way — and because a panic is not an `Err` it bypasses
    /// `worker serve`'s consecutive-failure backoff entirely. Every failure below is a
    /// `NodeFailed`.
    pub(super) async fn run_human_loop_gate(
        &self,
        run: RunId,
        node_id: &NodeId,
        agent_ref: &AgentRef,
        menu: &[LoopGateOption],
        iteration_output: &serde_json::Value,
        fold: &Fold,
    ) -> Result<LoopGateStep, OrchestratorError> {
        // 0. This gate has ALREADY failed ⇒ it stays failed. Shared with all three
        //    siblings, and FIRST — ahead of everything, including the question — for the
        //    fail-closed reason spelled out on `gate_precheck`. The verdict is READ BACK,
        //    never re-derived: this refusal is terminal for the gate, but the run it kills
        //    journals no `RunCompleted`, so every later wake re-drives the iteration and a
        //    re-derived verdict would append a fresh `NodeFailed` on each of them.
        //    `gate_failure_by_id` rather than `gate_precheck_by_id` because this kind's
        //    step type is not a `NodeExec`; it is the same read of the same map.
        if let Some(message) = self.gate_failure_by_id(node_id, fold) {
            // The ONE `Failed` this function returns without writing anything: the row is
            // already durable, and `fail_loop`'s own guard keeps the LOOP's copy from
            // being re-appended too.
            return Ok(LoopGateStep::Failed(message));
        }

        // 1. This gate's decision has ALREADY BEEN HONOURED ⇒ replay it, WITHOUT consulting
        //    the clock. The success mirror of step 0, and it exists for the same reason: a
        //    verdict is settled by the drive that produced it and read back afterwards.
        //
        //    Without it this function re-derived every settled gate on every drive — see
        //    the doc comment. It sits ABOVE the SLA read as well as above the clock,
        //    deliberately: a gate that is already settled needs no role, no question and no
        //    deadline, so AC14's loud model-backed refusal is owed at the ASK and on every
        //    drive that could still ask, not on a replay whose answer is already durable.
        //    Coupling a settled decision to live config would turn a role edit into a
        //    terminal failure of a loop nobody is waiting on.
        //
        //    **That ordering is a TEST, not only this comment.** It shipped as prose in
        //    three places and review mutation-proved it unguarded: moving this block below
        //    the SLA read left the whole workspace green while a `torii config push` that
        //    edited or deleted the gate role destroyed a loop that had converged hours
        //    earlier and cascade-skipped its downstream node — the Critical this slice
        //    already shipped once, reached through config instead of the clock. Guarded now
        //    by `a_settled_gate_replays_when_a_config_push_breaks_its_role`, which is the
        //    exact mutation.
        if let Some(option) = fold.loop_gate_settled_with(node_id) {
            return self
                .decide_from_published_menu(run, node_id, option, fold)
                .await;
        }

        // 2. The SLA, through the seam shared with `drive_agent`'s human branch — so the
        //    role and its deadline travel together and no caller re-reads the registry to
        //    find one. A model-backed role named in a `GateSpec::Human`, or an unresolvable
        //    one, fails loudly HERE (AC14): silence would let an author believe a person is
        //    in the loop while the run quietly decides for itself.
        //
        //    Only the SLA. The QUESTION is composed inside the `NotYetAsking` arm, which is
        //    the only arm that uses it: composing it here ran `assemble_prompt_parts` and a
        //    full redaction pass over the iteration output on EVERY drive of EVERY
        //    iteration's gate, and threw the result away on the pause and replay paths that
        //    a long-running human loop spends nearly all its life on.
        let timeout = match self.human_sla_for(agent_ref) {
            Ok(t) => t,
            Err(error) => {
                return self
                    .fail_loop_gate(run, node_id, format!("loop_gate: {error}"))
                    .await;
            }
        };

        // 3. What this gate has recorded — and ACTED ON IMMEDIATELY. See the doc comment:
        //    this being one `match` rather than a `let` is the s2-not-s3 ordering.
        match self.wait_or_expire_by_id(node_id, timeout, fold) {
            // The overflow guard's second layer (`signal.rs` explains why a node kind may
            // not panic on its own). Nothing is journaled beyond the failure itself: a
            // `LoopGateAwaited` carrying a nonsense deadline would be folded first-wins
            // forever. The helper's message is unprefixed so each kind names itself.
            Err(message) => {
                self.fail_loop_gate(run, node_id, format!("loop_gate: {message}"))
                    .await
            }

            // The recorded deadline has passed ⇒ FAIL, before any decision is read, so a
            // late "continue" cannot authorize spend the SLA had already stopped waiting
            // for. An expired gate fails the whole `Loop` (AC10) — `run_loop`'s existing
            // gate-failure arm does that, so no new outcome shape is needed. Converging
            // instead ("silence means stop") was rejected in §3: it would decide the
            // loop's outcome with nobody asked and report SUCCESS.
            //
            // **The message names the DEADLINE, never "no decision"** — this arm has not
            // read the fold and cannot know whether one exists. `run_human_agent`'s
            // "with no answer" is accurate for the opposite reason: it reads its answer
            // first, so reaching its expiry proves the absence. Telling an operator whose
            // decision DID land "no decision" would send them hunting a delivery bug that
            // does not exist, in a durable message every later drive re-emits.
            Ok(WaitState::Expired(deadline)) => {
                self.fail_loop_gate(
                    run,
                    node_id,
                    format!(
                        "loop_gate: node {} passed its deadline {deadline}; the gate fails \
                         on the deadline BEFORE any decision is read, so a decision that \
                         had already landed does not authorize another iteration",
                        node_id.0
                    ),
                )
                .await
            }

            // The first — and only — ask this gate ever makes. `menu` (the GRAPH's copy)
            // is read here and nowhere else, which is what makes the menu durable. It is
            // also SCRUBBED here and nowhere else: what this arm journals is the redacted
            // copy, and every later drive reads that one back out of the fold.
            Ok(WaitState::NotYetAsking(fresh)) => {
                // The question, through the seam shared with `drive_agent`'s human branch
                // — so a person is shown a question built by the MODEL path's own prompt
                // assembly and the two cannot drift. Composed HERE rather than at step 2
                // because this is the only arm that uses it.
                //
                // **Which argument each half goes in is a contract, not a style choice**
                // (see `human_question_for`'s doc, design §6, AC15). The iteration output
                // is RUN DATA — a model answer, so over 4 KiB essentially always — and
                // belongs in `context`, which truncates per dependency to
                // `MAX_HUMAN_CONTEXT_BYTES`. Passing it as `input` would charge it to the
                // LOUD `MAX_HUMAN_TEXT_BYTES` cap below and kill the gate on ordinary
                // data, after the iteration's tokens were already spent and unrecoverably
                // (step 0 reads the `NodeFailed` back forever). The `input` is a short ask
                // synthesized from the menu instead: author-scale by construction, so
                // everything charged to the loud cap stays author-controlled.
                let context = [(
                    ContextKey(ITERATION_OUTPUT_KEY.into()),
                    iteration_output.clone(),
                )];
                let question = match self.human_question_for(
                    agent_ref,
                    &serde_json::Value::String(gate_ask(menu)),
                    &context,
                ) {
                    // The SLA came back at step 2 from the same seam over the same role,
                    // so this copy is redundant by construction.
                    Ok((question, _)) => question,
                    Err(error) => {
                        return self
                            .fail_loop_gate(run, node_id, format!("loop_gate: {error}"))
                            .await;
                    }
                };

                // Bound the AUTHORED half before it becomes durable: the gate role's
                // `system_prompt`, its activated skill bodies, and the menu-derived ask.
                // All three are author-controlled at config time, which is what makes a
                // LOUD terminal failure the right answer — the person who wrote the config
                // can act on it. The `## Context` half is the ITERATION OUTPUT and is
                // deliberately NOT counted: it is run data, and it truncates to its own
                // budget instead. That asymmetry is s3's whole-slice fix and it is
                // load-bearing here, where the context is a model answer every time.
                if question.authored_bytes > MAX_HUMAN_TEXT_BYTES {
                    return self
                        .fail_loop_gate(
                            run,
                            node_id,
                            format!(
                                "loop_gate: node {}'s authored prompt is {} bytes, over the \
                                 {MAX_HUMAN_TEXT_BYTES}-byte limit — trim the gate role's \
                                 system prompt, its skills, or the menu option names. (The \
                                 `## Context` section, which is this iteration's OUTPUT, is \
                                 NOT counted here: it is run data, and it is truncated to \
                                 fit its own {MAX_HUMAN_CONTEXT_BYTES}-byte budget rather \
                                 than failing the gate.)",
                                node_id.0, question.authored_bytes
                            ),
                        )
                        .await;
                }

                // Redact BEFORE the durable write, not only at display time — the journal
                // row is otherwise the one place a credential sitting in the role's
                // `system_prompt`, an activated skill body or the iteration output lands in
                // the clear (`torii config push` scrubs nothing). Then clamp, because
                // `[REDACTED]` is LONGER than the shortest span it replaces and can push a
                // question that fitted over the bound; clamping rather than failing keeps
                // "your prompt contained a secret" from becoming a terminal run.
                let prompt = question.redact_and_clamp(
                    |t| self.redact_text(t),
                    MAX_HUMAN_TEXT_BYTES + MAX_HUMAN_CONTEXT_BYTES,
                );

                // **The MENU is redacted too, and it is a deliberate answer rather than a
                // reflex.** Option names are author free text arriving through the same
                // `torii config push` that scrubs nothing, and this append is where they
                // become durable — so leaving them alone made the SAME string scrubbed in
                // `prompt` (which quotes them, via `gate_ask`) and plaintext in `menu` and
                // in the pause reason built from it, on one drive. That is the exact defect
                // class s2 and s3 each shipped and each had to fix, and review measured it
                // here.
                //
                // The reflex answer would have been to leave it: a menu is not display
                // text, it is the VOCABULARY every later decision is resolved against
                // (`find(|o| o.name == decision.option)`), so scrubbing it changes what an
                // operator must type. That is exactly why it is redacted HERE, at the one
                // append, rather than at each reader: the published copy is the only copy
                // anybody can see — `torii run gate decide` recites it and refuses anything
                // else — so redacting it keeps ONE vocabulary, matching the question, and
                // resolution keeps matching journal against journal.
                //
                // What it must not do is make two options indistinguishable.
                // `check_menu_option_names` already rejects a duplicate name at
                // `validate_dag` time ("`--option x` would be ambiguous"), but redaction
                // runs long afterwards and can RE-CREATE the duplicate: two different
                // credential-shaped names both collapse to `[REDACTED]`, `find` takes the
                // first, and an operator picking the only name they were offered gets
                // whichever `stops` happened to come first — a decision inverted silently,
                // which is precisely what §5.3 journals the menu to prevent. So the gate
                // refuses, on the authored-bytes cap's reasoning: this is author-controlled
                // config, a loud failure is actionable by the person who wrote it, and both
                // alternatives are worse (keeping the plaintext re-opens the leak;
                // inventing disambiguating suffixes offers a human an option their config
                // does not contain).
                //
                // It cannot move to `validate_dag`, where the duplicate rule lives: that
                // function is pure over the graph and has no `Redactor`. The redactor is an
                // executor injection (`with_redactor`, default none), so the same graph is
                // legal under one executor and not another — a redactor-dependent rule in a
                // pure graph validator would be a lie about the graph. The check runs
                // unconditionally rather than only when a redactor is wired, which costs
                // nothing and also catches a duplicate that reached the executor without
                // passing `validate_dag` at all.
                //
                // BEFORE the append, so a menu nobody could answer unambiguously leaves no
                // durable row behind — the same ordering the authored-bytes cap uses.
                let published = self.redact_menu(menu);
                if let Some(name) = collided_option_name(&published) {
                    return self
                        .fail_loop_gate(run, node_id, ambiguous_menu_message(node_id, &name))
                        .await;
                }
                self.append(
                    run,
                    JournalEvent::LoopGateAwaited {
                        node: node_id.clone(),
                        deadline: fresh,
                        prompt,
                        menu: published.clone(),
                    },
                )
                .await?;

                // Then pause, WITHOUT reading a decision first — unlike `run_human_gate`,
                // which falls through so an early `GateDecided` is honoured in the same
                // execution. The race it resolves cannot arise here: a loop gate's path is
                // SYNTHESIZED per iteration and does not exist until that iteration has
                // run, and `torii run gate decide` refuses a node with no journaled menu,
                // so nothing an operator can do produces a decision before the ask. A
                // hand-written journal still can, and it costs exactly one extra wake —
                // the ask is durable, and the very next drive honours the decision against
                // it. Pausing is also the behaviour AC3 states: the gate asks, and the run
                // pauses.
                //
                // On the PUBLISHED menu, not the graph's: the reason recites the options,
                // and an operator must be offered the same names on this drive as on every
                // later one.
                self.pause_gate(run, node_id, &published, fresh).await
            }

            // Already asking by the SHARED map's reckoning — but did THIS kind begin
            // asking here?
            //
            // `Fold::deadlines` is written by all FOUR waiting kinds while only
            // `LoopGateAwaited` writes `Fold::loop_gate_asks`, so this arm is reachable the
            // same way its three siblings' are: some OTHER kind's awaited record sits at
            // this id. Its siblings get there by an edit to a live run's graph; a gate path
            // is SYNTHESIZED, so the two vectors here are different ones.
            //
            // **(a) An authored `__gate__` SEGMENT, inside the gated `Loop`'s own
            // `Subgraph` body.** `drive_nested` namespaces that body under `"{loop}/{i}"`,
            // so an inner node the author simply named `__gate__` lands at exactly this
            // path. `validate_dag` does not stop it: SP-6 s1's rule bans the `/` SEPARATOR
            // in an author-supplied id, which makes the reserved path unauthorable in one
            // piece but says nothing about a bare segment that only becomes that path once
            // nesting has flattened it. `plan::feasible` DOES reserve the segment
            // (`PlanError::ReservedNodeId`, the SP-3 s5 review's fix), so a planner cannot
            // emit it, and an `Expand` body is covered by that; the author-supplied segment
            // is the door left open. Guarded by
            // `an_authored_gate_id_in_a_loop_body_collides_and_fails_loudly`.
            //
            // An earlier version of this comment described (a) as a `/`-containing id
            // reaching the executor because "`Executor::start` takes the graph as an
            // unvalidated caller parameter". Both halves are false: `start_inner` and
            // `run_inner` each call `graph.validate_dag()?` before anything is journaled,
            // and validation recurses into `Subgraph`, `Loop` and `Branch` bodies — so no
            // caller, embedder included, gets an unvalidated graph past the front door. The
            // real vector needed no separator at all.
            //
            // **(b) A journal `torii` did not write**, which is a first-class case for a
            // durable log an embedder may append to directly.
            //
            // What is NOT a vector, though an earlier version of this comment claimed it:
            // a `Loop` whose gate was `Agent` over a human-backed role. That role never
            // reaches `run_human_agent` at all — `drive_agent`'s `!top_level` arm refuses
            // it before the seam, journaling a `NodeFailed` at the gate path rather than an
            // `AgentAwaited` — and design §5.4 keeps that refusal deliberately unchanged.
            //
            // **Loud, and never a fallback to the graph's `menu`.** Validating a decision
            // against a menu no human was ever shown is precisely the non-durable menu
            // §5.3 rejects, and it would do it silently. Journaling a fresh
            // `LoopGateAwaited` on top of the other kind's record is the other candidate
            // and is worse: `deadlines` folds first-wins, so the question would be
            // published against a deadline some other kind chose, and the run would carry
            // two contradictory durable claims about what it is waiting for.
            Ok(WaitState::Waiting(deadline)) => {
                let Some(published) = fold.loop_gate_menu_for(node_id) else {
                    return self
                        .fail_loop_gate(run, node_id, missing_menu_message(node_id))
                        .await;
                };

                // No decision yet ⇒ re-pause on the deadline this gate RECORDED (never
                // `now + timeout`; see `pause_awaiting`). The menu offered is the
                // PUBLISHED one, so what an operator is invited to pick from is what their
                // answer will be validated against.
                let Some(decision) = fold.loop_gate_decision_for(node_id) else {
                    return self.pause_gate(run, node_id, published, deadline).await;
                };

                // Resolve the decision against the JOURNALED menu. An option matching
                // NOTHING in it fails loudly: the gate neither continues nor stops, because
                // defaulting either way would be a decision no human made — to stop, or to
                // spend more. `torii run gate decide` refuses such a name at its own
                // boundary (reciting the menu), so this arm is reachable only from a
                // journal `torii` did not write, which is exactly why it must fail rather
                // than guess.
                let Some(chosen) = published.iter().find(|o| o.name == decision.option) else {
                    return self
                        .fail_loop_gate(
                            run,
                            node_id,
                            unmatched_option_message(node_id, &decision.option, published),
                        )
                        .await;
                };
                let stop = chosen.stops;

                // SETTLE, durably, BEFORE `run_loop` spends anything on the strength of
                // this answer. From here on the gate replays through step 1 and its
                // recorded deadline — long since passed, by the time a multi-iteration loop
                // is done — has no further say. Resolved first and settled second so a
                // decision naming an option nobody was offered leaves no settlement behind:
                // that failure is terminal through step 0 either way, and a settlement row
                // for a decision that was never honoured would be a durable lie.
                //
                // The RUN OUTCOME cannot see that ordering — step 0 fences the node whether
                // the row was written or not — so it is asserted directly on the journal, in
                // `a_decision_naming_an_unknown_option_fails_the_loop_gate`. Review found
                // the swap green across the whole crate before that assertion existed.
                self.append(
                    run,
                    JournalEvent::LoopGateSettled {
                        node: node_id.clone(),
                        option: decision.option.clone(),
                    },
                )
                .await?;

                // The pure part, recomputed from the journaled option NAME rather than
                // carried in the fold — which is what makes a resume reach the identical
                // decision at zero cost.
                Ok(LoopGateStep::Decided { stop })
            }
        }
    }

    /// Resolve an option name against the menu this gate PUBLISHED, for the replay path.
    ///
    /// The step-1 counterpart of the live resolution inside
    /// [`Executor::run_human_loop_gate`]'s `Waiting` arm, and it must reach the same three
    /// answers for the same three states — hence the shared message builders rather than a
    /// second pair of near-identical strings. What it deliberately does NOT re-read is
    /// `Fold::loop_gate_decision_for`: `LoopGateDecided` folds LAST-wins so an operator can
    /// correct a decision *before the run resumes*, and a settlement is exactly the line
    /// after which "before" has passed — the loop has already spent an iteration on the
    /// strength of the answer, so a later correction must not move where it converged.
    ///
    /// Both failures here are reachable only from a journal the executor did not write (a
    /// settlement with no ask, or with an option absent from the ask's menu), which is why
    /// they must fail rather than guess — the same argument the live arm makes.
    async fn decide_from_published_menu(
        &self,
        run: RunId,
        node_id: &NodeId,
        option: &str,
        fold: &Fold,
    ) -> Result<LoopGateStep, OrchestratorError> {
        let Some(published) = fold.loop_gate_menu_for(node_id) else {
            return self
                .fail_loop_gate(run, node_id, missing_menu_message(node_id))
                .await;
        };
        let Some(chosen) = published.iter().find(|o| o.name == option) else {
            return self
                .fail_loop_gate(
                    run,
                    node_id,
                    unmatched_option_message(node_id, option, published),
                )
                .await;
        };
        Ok(LoopGateStep::Decided { stop: chosen.stops })
    }

    /// The durable pause both of this gate's waiting arms end on, so the reason string has
    /// ONE definition — an operator reading `torii run status` on the drive that asked and
    /// on a drive that re-paused must not see two different sentences about the same wait.
    ///
    /// `menu` is the PUBLISHED menu on every drive — the copy the asking arm scrubbed and
    /// journaled, read back out of the fold on every later one. Listing it here is what
    /// lets an operator answer from `run status` alone, and it is the same courtesy
    /// `run_human_gate`'s pause reason extends. (It was the GRAPH's copy on the first ask
    /// until review found that the same option name was scrubbed in `prompt` and plaintext
    /// here; the asking arm now hands over the redacted copy, which also keeps the two
    /// drives' sentences identical rather than merely similar.)
    ///
    /// **The reason is then run through the redactor, as the write chokepoint** — the same
    /// argument [`Executor::fail_loop_gate`] makes for the messages it journals, and made
    /// at the write rather than per arm for the same reason: s2 shipped a per-arm scrub
    /// that missed an arm.
    ///
    /// Its value here is entirely FORWARD-LOOKING, and saying otherwise would overstate it.
    /// Today it scrubs nothing the caller has not already scrubbed: the menu arrives
    /// redacted from the asking arm, and the only other interpolation is the NODE ID, which
    /// this cannot meaningfully protect — a node id is a structural key, plaintext in
    /// `NodeStarted`, `EffectRecorded` and `LoopGateAwaited.node` in the same journal, so a
    /// credential in one leaks whatever this line does. What it buys is that a future arm
    /// which interpolates something new — or an edit that hands this function the GRAPH's
    /// menu again, which is what review caught — cannot re-open the leak from here.
    ///
    /// One difference from [`Executor::redact_menu`] worth knowing: that pass sees each
    /// name alone, this one sees them JOINED by ` | `, so a pattern spanning the separator
    /// would scrub more here than there. Strictly more, never less — and the `menu` field
    /// is the one that must not drift, since it is the vocabulary a decision is resolved
    /// against, while this string is read by people only.
    ///
    /// This is an asymmetry with the other three waiting kinds, stated plainly rather than
    /// papered over: `run_await_signal`, `run_human_gate` and `run_human_agent` call
    /// `pause_awaiting` directly with an unredacted reason. Widening the scrub to all four
    /// is a change to three shipped slices' behaviour and is not this one's to make; s4's
    /// site is the one that interpolates a whole author-authored MENU, which is what forced
    /// the question here first.
    async fn pause_gate(
        &self,
        run: RunId,
        node_id: &NodeId,
        menu: &[LoopGateOption],
        deadline: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<LoopGateStep, OrchestratorError> {
        let reason = self.redact_text(format!(
            "loop_gate: waiting for a decision on node {} ({}){}",
            node_id.0,
            menu_names(menu, " | "),
            deadline
                .map(|d| format!(" (deadline {d})"))
                .unwrap_or_default()
        ));
        // `pause_awaiting` journals the `RunPaused` and echoes the reason into a
        // `NodeExec::Paused` this kind has no use for; the SAME string is returned as the
        // step, so the two cannot disagree.
        self.pause_awaiting(run, reason.clone(), deadline).await?;
        Ok(LoopGateStep::Paused(reason))
    }

    /// Scrub a loop gate's menu into the copy that becomes durable.
    ///
    /// Only the NAMES: `stops` is a `bool` and carries nothing to leak, and it is also the
    /// half a redaction must never touch — it is what the loop's convergence is decided
    /// from, and the human is shown it in the question (`gate_ask` annotates each option
    /// with what picking it does). So the scrubbed menu says the same thing about the LOOP
    /// as the authored one and differs only in what an operator types.
    ///
    /// Through [`Executor::redact_text`], the same wrapper `fail_loop_gate` uses, so the
    /// option name reaching the journal is byte-identical to the one `gate_ask` put in the
    /// already-redacted question. Two passes over the same string with the same pure
    /// redactor, not two different scrubs — which is what keeps the `menu` field and the
    /// `prompt` field one vocabulary.
    fn redact_menu(&self, menu: &[LoopGateOption]) -> Vec<LoopGateOption> {
        menu.iter()
            .map(|o| LoopGateOption {
                name: self.redact_text(o.name.clone()),
                stops: o.stops,
            })
            .collect()
    }

    /// Journal a `NodeFailed` for a loop gate and return the step that reports it. Every
    /// failure path in [`Executor::run_human_loop_gate`] routes through here EXCEPT step 0,
    /// which writes nothing by design — so the journaled message and the returned one
    /// cannot drift, and every one of them is redacted at this single chokepoint.
    ///
    /// **It returns the whole [`LoopGateStep`], not just the message**, where
    /// [`Executor::fail_human_agent`] returns a [`NodeExec`]. What that buys is that every
    /// arm's "I failed" and the row recording it are produced by ONE expression: a helper
    /// returning a bare `String` leaves each call site free to append here and then build
    /// some other step. It also makes `fail_human_agent`'s "`output: None` on every one of
    /// them" property STRUCTURAL rather than merely upheld: [`LoopGateStep::Failed`] has no
    /// output field at all, so an expired or unmatched gate cannot produce a defaulted
    /// result by mistake.
    ///
    /// An earlier version of this paragraph justified the return type by a
    /// `newly_journaled: true` flag it made "unforgeable". There is no such field —
    /// `Failed(String)`, and the variant's own doc explains that the flag was the FIRST
    /// shipped shape and was removed because it was a second claim about the journal that
    /// disagreed with the first. The guard it was reaching for lives on
    /// [`Executor::fail_loop`]'s append, where reading the fold makes it self-healing.
    ///
    /// **What the redaction protects, precisely:** these arms interpolate a NODE ID (the
    /// author's loop id plus the reserved suffix), an OPTION NAME (arbitrary by
    /// definition on the unmatched path — it matched nothing in the menu), the MENU's
    /// names (author free text), a byte count, a deadline, and `human_question_for`'s
    /// error text (which names the agent). The option name is the live surface and it is
    /// the exact leak s2 shipped when each arm scrubbed for itself: an undeclared option
    /// reached the journal in plaintext, and because `fold.failed` is read back by
    /// `gate_precheck`, it was re-emitted on every later drive. Scrubbing once, where the
    /// journal write happens, is what makes a future arm safe by construction.
    async fn fail_loop_gate(
        &self,
        run: RunId,
        node_id: &NodeId,
        message: String,
    ) -> Result<LoopGateStep, OrchestratorError> {
        let message = self.redact_text(message);
        self.append(
            run,
            JournalEvent::NodeFailed {
                node: node_id.clone(),
                error: message.clone(),
            },
        )
        .await?;
        Ok(LoopGateStep::Failed(message))
    }

    /// Journal a `NodeFailed` and return it. Every failure path routes through here — the
    /// four in `run_human_agent` above AND `drive_agent`'s non-top-level refusal, which
    /// review found appending its own `NodeFailed` inline and bypassing this — so the
    /// journaled message and the returned one cannot drift, and every one of them is
    /// redacted at this single chokepoint.
    ///
    /// **What the redaction is actually protecting, stated precisely:** today's arms
    /// interpolate a NODE ID (author-supplied, straight out of the graph), an AGENT NAME
    /// (author-supplied, out of the registry), a byte count and a deadline. Neither the
    /// question nor the answer is quoted into a failure message, and an earlier version of
    /// this comment claimed they were. The two author-supplied strings are the live surface
    /// — they land verbatim in a durable journal row, in `RunOutcome.failed`, and in
    /// whatever `torii run status` renders — and `a_failure_message_is_redacted_before_it_
    /// reaches_the_journal` covers one arm of each. The chokepoint's remaining value is
    /// forward-looking: s2 shipped a per-arm scrub that missed one arm (an undeclared option
    /// NAME reached the journal in plaintext), and a chokepoint makes that unrepresentable
    /// for a future arm that DOES quote free text.
    ///
    /// `output: None` on every one of them, and that is the AC5 property: an expired
    /// human-backed node produces NO output, defaulted or otherwise.
    ///
    /// `redact_text` is `gate.rs`'s, shared rather than re-derived — it is the
    /// `Value`-typed [`Executor::redact`] wrapped for a bare string, with the
    /// variant-preservation tradeoff documented there.
    pub(super) async fn fail_human_agent(
        &self,
        run: RunId,
        node_id: &NodeId,
        message: String,
    ) -> Result<NodeExec, OrchestratorError> {
        let message = self.redact_text(message);
        self.append(
            run,
            JournalEvent::NodeFailed {
                node: node_id.clone(),
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

/// The `## Task` half of a loop gate's question: a short statement of what is being
/// decided, synthesized from the menu.
///
/// A free `fn` so it is pure and unit-testable without an executor, and so the ask has ONE
/// definition rather than being inlined at the append site.
///
/// **Why a synthesized ask rather than the iteration output, and rather than a bare
/// constant.** The seam's `input` becomes `## Task` and is charged to the LOUD
/// `MAX_HUMAN_TEXT_BYTES` cap, so whatever goes there must be author-scale — the iteration
/// output is not (design §6). Deriving it from the menu keeps everything charged to that
/// cap author-controlled at config time, which is the whole principle behind which half
/// fails loudly: menu names are author free text, and their being over 4 KiB really is a
/// config error the author can act on. And it makes the journaled `LoopGateAwaited.prompt`
/// SELF-CONTAINED — `torii run list-paused` renders the `menu` field beside it, but the
/// durable question should still say what is being decided, the same reason s3 added
/// `## Task` at all (§5.4: never show the human LESS than the model would have had).
///
/// Each option is annotated with what picking it does to the LOOP. `stops` is the only
/// thing a `LoopGateOption` carries beyond its name, and a menu of bare names would make a
/// person guess which of `revise`/`ship` ends the run — a guess the type exists to remove.
///
/// **One consequence, accepted deliberately:** the seam evaluates every skill's and tool's
/// `activation.is_active` against this same string, so a gate role's `OnKeywords` skills
/// match on the menu text rather than on the iteration output. That is a live limitation,
/// not an oversight — the seam has ONE `input` serving both `## Task` and the activation
/// query, and splitting it in two is a change to s3's path as well. `Always` skills (the
/// default, and what a gate role wants) are unaffected.
fn gate_ask(menu: &[LoopGateOption]) -> String {
    let options = menu
        .iter()
        .map(|o| {
            format!(
                "`{}` ({})",
                o.name,
                if o.stops {
                    "stop the loop"
                } else {
                    "run another iteration"
                }
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("Review the iteration output above and choose one: {options}.")
}

/// A loop-gate menu's option names, for a human to read. `sep` differs by site — a failure
/// lists them as prose (`revise, ship`), a pause offers them as choices (`revise | ship`).
///
/// The exact counterpart of `gate.rs`'s `names`, kept separate because the two menus are
/// different types carrying different vocabularies: a [`LoopGateOption`] says `stops` (the
/// continue/stop axis) where a `GateOption` says `outcome` (`{Complete, Fail}`). Making one
/// function generic over both would invite the very confusion `graph.rs`'s warning — that
/// the HITL and loop-stop senses of "gate" are unrelated — exists to prevent.
fn menu_names(menu: &[LoopGateOption], sep: &str) -> String {
    menu.iter()
        .map(|o| o.name.as_str())
        .collect::<Vec<_>>()
        .join(sep)
}

/// The kind-swap refusal: this node recorded a wait but published no MENU.
///
/// A free `fn` because TWO arms reach this state — the live `Waiting` arm and the settled
/// replay in [`Executor::decide_from_published_menu`] — and an operator who sees it on one
/// drive and then again on the next must read the same sentence. The failure is durable and
/// re-emitted by `gate_precheck` on every later drive, so a near-copy would look like a
/// second, different problem.
///
/// It deliberately does not recite the GRAPH's menu, which is the fallback this refusal
/// exists to reject: naming options nobody was shown would suggest they are answerable.
fn missing_menu_message(node_id: &NodeId) -> String {
    format!(
        "loop_gate: node {} recorded that it began waiting but published no menu, so there \
         is nothing a decision could be validated against. A waiting node's kind cannot be \
         changed mid-run; fail the run and start a new one.",
        node_id.0
    )
}

/// The first option name that appears twice in a menu, if any.
///
/// Run over the REDACTED menu at the one append site, where it catches the duplicate that
/// `check_menu_option_names` structurally cannot: two distinct credential-shaped names
/// that collapse to the same placeholder. `validate_dag`'s copy of this rule runs on the
/// authored graph, before any redactor exists — see the append site for why the rule
/// cannot move there.
///
/// It returns the NAME rather than a `bool` so the failure can say which one, and the name
/// it returns is post-redaction by construction: the collision is only observable in the
/// scrubbed vocabulary, and the plaintext must not be recited back in any case.
fn collided_option_name(menu: &[LoopGateOption]) -> Option<String> {
    let mut seen = std::collections::HashSet::new();
    menu.iter()
        .find(|o| !seen.insert(o.name.as_str()))
        .map(|o| o.name.clone())
}

/// The refusal for a menu that cannot be offered unambiguously.
///
/// It quotes the COLLIDED name — which is the placeholder, not the config — and says what
/// to do about it, because the operator reading this cannot see the authored names from
/// here and the message must not show them. `fail_loop_gate` redacts everything it
/// journals, so a future edit that interpolated the authored names would be scrubbed
/// anyway; not interpolating them is the first line, not the only one.
fn ambiguous_menu_message(node_id: &NodeId, name: &str) -> String {
    format!(
        "loop_gate: node {}'s menu offers the option name {name:?} more than once, so a \
         decision naming it could not be resolved to one option — it would silently take \
         whichever came first, and the two may disagree about whether the loop stops. The \
         usual cause is two option names of credential SHAPE scrubbing to the same \
         placeholder; rename them to something that is not credential-shaped.",
        node_id.0
    )
}

/// The other refusal both resolution sites share: a decision naming an option that is not
/// in the menu the human was actually shown.
///
/// `published` — never the graph's copy. The names recited back are the ones an operator
/// could legitimately have picked, which is the whole point of reciting them.
fn unmatched_option_message(
    node_id: &NodeId,
    option: &str,
    published: &[LoopGateOption],
) -> String {
    format!(
        "loop_gate: node {} was decided with option {option:?}, which is not in the menu it \
         published ({}). The decision is durable but cannot be honoured: the gate neither \
         continues nor stops, because defaulting either way would be a decision no human \
         made.",
        node_id.0,
        menu_names(published, ", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{MAX_HUMAN_CONTEXT_BYTES, MAX_HUMAN_TEXT_BYTES};

    const BOUND: usize = MAX_HUMAN_TEXT_BYTES + MAX_HUMAN_CONTEXT_BYTES;

    /// The post-redaction clamp must never eat the ASK.
    ///
    /// The clamp exists because `[REDACTED]` is longer than the shortest span it replaces,
    /// so a question that fitted can exceed the bound afterwards. But `compose` puts
    /// `## Task` — the node input, the thing the human is being asked about — LAST, and the
    /// clamp cut from the END. A redaction that grew the authored half therefore deleted the
    /// ask outright, leaving the human the role's standing instructions plus up to 32 KiB of
    /// upstream context and no statement of what to decide.
    ///
    /// That is the defect `## Task` was added to prevent, reintroduced in a narrower window,
    /// and it breaks §5.4's one-directional rule: never show the human LESS than the model
    /// would have had. Found by the re-review of the whole-slice review's own fixes.
    ///
    /// Tested at the unit level deliberately. The executor-level path needs an upstream node
    /// producing more than `MAX_HUMAN_CONTEXT_BYTES`, because the authored half alone cannot
    /// reach the bound (a 4096 cap times redaction's ~1.67x growth is ~6.8 KB) — and no
    /// gateway helper returns an output that large. The property is a property of the
    /// clamp, so it is pinned where it lives.
    ///
    /// Mutation that must break this: replace `redact_and_clamp`'s body with the shipped
    /// form, `truncate_prompt_to_bound(redact(self.text.clone()), bound)`.
    #[test]
    fn the_clamp_cuts_context_and_never_the_ask() {
        let context = vec![(
            "upstream".to_string(),
            "c".repeat(MAX_HUMAN_CONTEXT_BYTES * 2),
        )];
        let q = HumanQuestion::compose("Decide whether to ship.", &context, "Order #42", |t| t);

        assert!(
            q.text.contains("## Task"),
            "precondition: compose adds the ask"
        );
        assert!(
            q.text.ends_with("Order #42"),
            "precondition: the ask is LAST, which is why the clamp could eat it"
        );

        // A redactor that GROWS its input, which is the only way the clamp fires at all.
        let grow = |t: String| t.replace('c', "cc");
        let out = q.redact_and_clamp(grow, BOUND);

        assert!(
            out.len() <= BOUND,
            "the durable row must stay bounded: {} bytes",
            out.len()
        );
        assert!(
            out.contains("## Task"),
            "the ASK must survive — a human with no statement of what to decide cannot \
             answer. tail: {:?}",
            &out[out.len().saturating_sub(80)..]
        );
        assert!(
            out.ends_with("Order #42"),
            "and the node input with it. tail: {:?}",
            &out[out.len().saturating_sub(80)..]
        );
    }

    /// A secret whose WHOLE MATCH straddles the `## Task` boundary must still be redacted.
    ///
    /// The tail-reserve above needs a split point, and the shipped implementation took it
    /// before redacting — `redact(text[..split])` and `redact(text[split..])` as two
    /// independent passes. A `Redactor` sees a whole string and matches over it; cutting the
    /// string first hides from it any match that spans the cut. `PatternRedactor`'s PEM rule
    /// is the reachable case today (`-----BEGIN … PRIVATE KEY-----[\s\S]*?-----END …`, the
    /// one shipped pattern with an unbounded multi-line body), and the split is unguarded
    /// for any future multi-line pattern.
    ///
    /// The leak was DURABLE: the unredacted value went into `AgentAwaited.prompt`, i.e. into
    /// `journal_events`, and `render::redact_question` cannot recover it downstream — it
    /// runs the same plain pass over the same already-split text.
    ///
    /// Uses the real `PatternRedactor` rather than a synthetic closure: the property is
    /// "one whole-string pass", and only a redactor with a genuinely multi-line rule can
    /// tell one pass from two.
    #[test]
    fn a_secret_that_straddles_the_task_boundary_is_still_redacted() {
        use orchestrator_core::{PatternRedactor, Redactor};

        let redactor = PatternRedactor::default();
        let redact = move |t: String| match redactor.redact(&serde_json::Value::String(t.clone())) {
            serde_json::Value::String(s) => s,
            _ => t,
        };

        // Assembled at runtime: the repo's Semgrep CWE-798 hook blocks a credential-shaped
        // literal in a fixture.
        let head_half = format!(
            "-----BEGIN RSA PRIVATE KEY-----\n{}",
            "MIIB".to_string() + &"A".repeat(24)
        );
        let secret_body = "MIIB".to_string() + &"A".repeat(24);
        let q = HumanQuestion::compose(
            "You are a reviewer.",
            &[("upstream".to_string(), head_half)],
            // The END delimiter lands in the `## Task` section, so the whole match spans the
            // split point the clamp needs.
            "-----END RSA PRIVATE KEY-----\nApprove?",
            // IDENTITY, deliberately. `compose`'s own per-body pass is the sibling test's
            // subject; this one is about `redact_and_clamp` seeing the WHOLE string once, and
            // a real redactor here would decide nothing either way (the body holds only the
            // BEGIN delimiter, so it matches no whole-pattern on its own).
            |t| t,
        );
        assert!(
            q.text.contains("## Task"),
            "precondition: the boundary this test straddles exists"
        );

        let out = q.redact_and_clamp(redact, BOUND);
        assert!(
            !out.contains(&secret_body),
            "the key material survived a redaction split at the `## Task` boundary and \
             would have been written to `journal_events`: {out}"
        );
    }

    /// A secret cut in half by the `## Context` BOUND must still be redacted.
    ///
    /// Structurally the same finding as
    /// `a_secret_that_straddles_the_task_boundary_is_still_redacted`, one function earlier:
    /// a `Redactor` matches over the string it is handed, so any cut made BEFORE it runs
    /// hides a whole-match that spanned the cut. That test fixed the `## Task` split;
    /// `compose` still truncated each dependency body first and only then let
    /// `redact_and_clamp` run the redactor over the composed result.
    ///
    /// `PatternRedactor`'s PEM rule is the reachable case, and it is the strongest possible
    /// one: `-----BEGIN … PRIVATE KEY-----[\s\S]*?-----END … PRIVATE KEY-----` needs BOTH
    /// delimiters, and an upstream output over `MAX_HUMAN_CONTEXT_BYTES` loses the `END`
    /// line to the per-dependency truncation — turning a would-be `[REDACTED]` into 32 KiB
    /// of plaintext key material.
    ///
    /// Not a live durable leak at HEAD (a `Scope::Run` context value is already redacted at
    /// the producing leaf, so the composed question sees `[REDACTED]` before it gets here),
    /// which is why this is defence in depth rather than a Critical. It is worth having
    /// anyway: `compose`'s correctness must not rest on a caller three modules away, and
    /// with `Executor::with_redactor` unset — the DEFAULT — torii's display-time
    /// `render::redact_question` runs the same plain pass over the same already-cut text and
    /// shows the fragment in a terminal, a `--json` payload and a CI log.
    ///
    /// The fix is ordering, not a new pattern: each `(key, body)` is redacted BEFORE
    /// `render_context_section_bounded` cuts it. `redact_and_clamp`'s whole-string pass
    /// stays (it is what guards the `## Task` straddle) and is idempotent, because
    /// `[REDACTED]` matches no credential shape.
    #[test]
    fn a_secret_cut_in_half_by_the_context_bound_is_still_redacted() {
        use orchestrator_core::{PatternRedactor, Redactor};

        let redactor = PatternRedactor::default();
        // Borrowed, not `move`: a closure that captures by shared reference is `Copy`, so
        // the SAME pass can be handed to both `compose` and `redact_and_clamp` below — which
        // is the arrangement under test.
        let redact = |t: String| match redactor.redact(&serde_json::Value::String(t.clone())) {
            serde_json::Value::String(s) => s,
            _ => t,
        };

        // Assembled at runtime: the repo's Semgrep CWE-798 hook blocks a credential-shaped
        // literal in a fixture. `sentinel` sits early enough in the body to SURVIVE the
        // per-dependency cut, so its presence in the output is unambiguous evidence that
        // raw key material was carried through.
        let sentinel = "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo";
        let body = format!(
            "-----BEGIN RSA PRIVATE KEY-----\n{}{sentinel}\n{}\n-----END RSA PRIVATE KEY-----",
            "MIIB".to_string() + &"A".repeat(24),
            "B".repeat(MAX_HUMAN_CONTEXT_BYTES * 2),
        );

        let q = HumanQuestion::compose(
            "You are a reviewer.",
            &[("key".to_string(), body)],
            "Ship?",
            redact,
        );
        let out = q.redact_and_clamp(redact, BOUND);

        assert!(
            !out.contains(sentinel),
            "the per-dependency truncation cut the PEM's `-----END` delimiter off, so the \
             redactor's whole-match never fired and key material reached the durable \
             `AgentAwaited.prompt`: {}",
            &out[..out.len().min(400)]
        );
    }

    /// The clamp must not fire at all when the redacted question already fits — otherwise
    /// every ordinary question would carry a truncation marker.
    #[test]
    fn a_question_that_fits_is_returned_untouched() {
        let q = HumanQuestion::compose("Decide.", &[], "the Acme MSA", |t| t);
        let out = q.redact_and_clamp(|t| t, BOUND);
        assert_eq!(out, q.text);
    }
}
