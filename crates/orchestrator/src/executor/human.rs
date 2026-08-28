//! The human-backed `Agent` node (SP-6 s3): a role answered by a person, not a model.
//!
//! s1 shipped `AwaitSignal` (pause, accept any JSON), s2 `HumanGate` (the typed menu).
//! This is the third and last waiting kind: an `Agent` node whose `AgentRef` resolves to
//! a human-backed definition pauses ONCE, journals the question it is asking, and
//! completes when a human answers.
//!
//! The waiting machinery is SHARED with both siblings, not copied — `gate_precheck` and
//! `wait_or_expire` live in `signal.rs`, reached here through their `_by_id` forms
//! because this node kind is driven from `drive_agent`, which holds only a `NodeId`.
//! s1's review found real defects in exactly those arms; a third copy would be a third
//! place for them to return.
//!
//! A new file rather than more of `agent.rs`, matching how s2 put `run_human_gate` in
//! its own `gate.rs`: `agent.rs` is the model path and stays that.

use orchestrator_core::{
    JournalEvent, MAX_HUMAN_CONTEXT_BYTES, MAX_HUMAN_TEXT_BYTES, NodeId, OrchestratorError, RunId,
};

use crate::agent::prompt::{render_context_section_bounded, truncate_prompt_to_bound};

use super::signal::WaitState;
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
    /// How many of `text`'s TRAILING bytes are the `## Task` section — the node input, i.e.
    /// the thing the human is actually being asked about.
    ///
    /// Recorded so the post-redaction clamp can protect it. Without this the clamp cut from
    /// the end, and `compose` puts `## Task` LAST, so a redaction that GREW the authored
    /// half deleted the ask outright: the human was journaled the role's standing
    /// instructions plus up to `MAX_HUMAN_CONTEXT_BYTES` of upstream context and no
    /// statement of what to decide. That is the defect `## Task` exists to prevent, and it
    /// breaks §5.4's one-directional rule — never show the human LESS than the model had.
    task_bytes: usize,
}

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
    pub(super) fn compose(authored: &str, context: &[(String, String)], query: &str) -> Self {
        let task = format!("\n\n## Task\n{query}");
        let mut text = String::with_capacity(authored.len() + task.len());
        text.push_str(authored);
        let authored_bytes = text.len() + task.len();
        text.push_str(&render_context_section_bounded(
            context,
            MAX_HUMAN_CONTEXT_BYTES,
        ));
        text.push_str(&task);
        Self {
            text,
            authored_bytes,
            task_bytes: task.len(),
        }
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
    pub(super) fn redact_and_clamp(
        &self,
        redact: impl Fn(String) -> String,
        bound: usize,
    ) -> String {
        let split = self.text.len() - self.task_bytes;
        let head = redact(self.text[..split].to_string());
        let task = redact(self.text[split..].to_string());
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

impl Executor {
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
        // 0. This node has ALREADY failed ⇒ it stays failed. Shared with both siblings,
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
            // without it. `Fold::deadlines` is written by all THREE waiting kinds while
            // only `AgentAwaited` carries a prompt, so this arm is reachable the same way
            // s2's is: by editing a live run's graph to change a waiting node's KIND. An
            // `AwaitSignal` node re-pointed at a human-backed `Agent` arrives here exactly
            // as the `AwaitSignal`→`HumanGate` swap arrives there — `gate.rs`'s own comment
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
        let q = HumanQuestion::compose("Decide whether to ship.", &context, "Order #42");

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

    /// The clamp must not fire at all when the redacted question already fits — otherwise
    /// every ordinary question would carry a truncation marker.
    #[test]
    fn a_question_that_fits_is_returned_untouched() {
        let q = HumanQuestion::compose("Decide.", &[], "the Acme MSA");
        let out = q.redact_and_clamp(|t| t, BOUND);
        assert_eq!(out, q.text);
    }
}
