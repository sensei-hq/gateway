//! The `HumanGate` node (SP-6 s2): the TYPED layer over s1's `AwaitSignal`.
//!
//! s1 accepts any JSON and hands it to the graph. This asks a human to pick one of an
//! enumerated menu, where each option declares its own outcome — so a rejection has real
//! semantics instead of merely being a value the author must remember to test for.
//!
//! The waiting machinery is SHARED with `AwaitSignal`, not copied: `gate_precheck` (the
//! fail-closed terminal guard) and `wait_or_expire` (the deadline durability) live in
//! `signal.rs`. s1's whole-slice review found real defects in exactly those two arms, and
//! a copy of either here would be a second place for them to come back — in the node kind
//! whose entire purpose is a human decision.

use orchestrator_core::{GateOption, GateOutcome, JournalEvent, Node, OrchestratorError, RunId};

use super::signal::WaitState;
use super::{Executor, Fold, NodeExec};

impl Executor {
    /// Execute one `HumanGate` node (design §6.2).
    ///
    /// | fold state | behaviour |
    /// |---|---|
    /// | failure recorded | `Failed` — shared arm 0, checked FIRST |
    /// | not yet asking | journal `GateAwaited`, then continue below |
    /// | asking, deadline passed (whatever was decided) | `NodeFailed` — the timeout, before any answer is read |
    /// | decided, option in the menu, `Complete` | `Completed({decision,actor,note})` |
    /// | decided, option in the menu, `Fail` | `NodeFailed`, naming who and why |
    /// | decided, option NOT in the menu | `NodeFailed`, loudly |
    /// | no decision | re-pause on the deadline this node RECORDED |
    ///
    /// **The ask always precedes the answer, and that ordering is load-bearing.** s1's
    /// early-signal race resolves itself for free because a signal delivered before the
    /// node first ran is simply already in the fold. A DURABLE menu breaks that: a
    /// `GateDecided` folded with no `GateAwaited` has nothing to validate against.
    /// Special-casing it — validating against the graph in that one path — would
    /// reintroduce exactly the non-durable menu §4 rejects. So the ask is journaled
    /// first, unconditionally, and the pending decision is then read against the menu
    /// just published: the early decision is still honoured in the same execution, and
    /// there is never a decision without a menu.
    ///
    /// **Validation is enforced HERE, and this is the layer that DECIDES it.** `torii run
    /// gate decide` now pre-checks the option against the journaled menu, which makes an
    /// undeclared option rare; it does not make this check redundant, because the CLI's
    /// pre-check and its append are not atomic and the library entry point bypasses the
    /// CLI entirely, so the CLI can report honestly but cannot stop the row existing. Same
    /// conclusion s1 reached for the terminal guard.
    ///
    /// **The `options` parameter is used for the very first ask and NOWHERE else.** Every
    /// later drive resolves the decision against `fold.menu_for`, so an author editing the
    /// graph mid-run cannot retroactively change what a human's recorded answer meant.
    /// This is the same argument s1 made for the deadline ("the deadline belongs to the
    /// RUN, not to the graph") and it is reachable for the same reason: `Executor::start`
    /// takes the graph as a caller parameter and never journals it.
    ///
    /// **Difference from `run_await_signal`, deliberate:** that node re-checks the clock
    /// after journaling a FRESH deadline, so a gate given a nanosecond to answer fails in
    /// the same execution instead of pausing on an instant already behind it. This one has
    /// no such second site (the `waiting_node_helpers` tests exist precisely because two
    /// expiry sites mask each other's defects), so a `HumanGate` whose fresh deadline
    /// elapses during its own journal append pauses once on a past instant; the scheduler
    /// wakes it immediately and the next drive takes `WaitState::Expired`. One extra wake,
    /// never a resurrection — the answer is still never read ahead of the expiry check.
    ///
    /// No gateway call and no `EffectRecorded`: the fold IS this node's memo, so a
    /// resumed run re-reads its decision at zero token cost by construction. Like
    /// `AwaitSignal`/`Branch`/`Subgraph` it journals no `NodeStarted`/`NodeCompleted`,
    /// which carries that family's known asymmetry: a re-`start` of an already-TERMINAL
    /// run rebuilds `outputs` from exactly those events and so reports this node in
    /// neither (the durable blackboard is unaffected — the completing drive published the
    /// decision under `ContextWrite`).
    ///
    /// This node kind must never panic. A panic here is not local: it unwinds through
    /// `Scheduler::tick`, which has already claimed a batch of runs and taken their
    /// leases, so the claimed rows stay `waking` and the next worker reclaims the stale
    /// lease and dies the same way. Every failure below is a `NodeFailed`.
    pub(super) async fn run_human_gate(
        &self,
        run: RunId,
        node: &Node,
        options: &[GateOption],
        timeout: Option<chrono::Duration>,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        // 0. This gate has ALREADY failed ⇒ it stays failed. Shared with `AwaitSignal`,
        //    and FIRST — ahead of the decision read — for the fail-closed reason spelled
        //    out on `gate_precheck`. The verdict is READ BACK, never re-derived, so a
        //    dead gate does not append a fresh `NodeFailed` on every drive.
        if let Some(failed) = self.gate_precheck(node, fold) {
            return Ok(failed);
        }

        // 1. The ask, before the answer — see the doc comment. This is also the ONLY
        //    place `options` is read, which is what makes the menu durable.
        let (deadline, menu) = match self.wait_or_expire(node, timeout, fold) {
            // The overflow guard's second layer (`signal.rs` explains why a node kind may
            // not panic on its own). Nothing is journaled beyond the failure itself: a
            // `GateAwaited` carrying a nonsense deadline would be folded first-wins
            // forever. The helper's message is unprefixed so each kind names itself.
            Err(message) => {
                return self
                    .fail_gate(run, node, format!("human_gate: {message}"))
                    .await;
            }
            // The node-keyed record of WHICH node is asking, WHAT it offered, and — the
            // durable home of — BY WHEN. `menu` is the same value we just journaled
            // rather than a re-read of the fold, because `fold` is a snapshot of the
            // journal as it stood when this drive began and cannot see this append.
            Ok(WaitState::NotYetAsking(fresh)) => {
                self.append(
                    run,
                    JournalEvent::GateAwaited {
                        node: node.id.clone(),
                        deadline: fresh,
                        options: options.to_vec(),
                    },
                )
                .await?;
                (fresh, options.to_vec())
            }
            // The recorded deadline has passed ⇒ FAIL, loudly, naming the node and the
            // instant — and BEFORE any decision is read, so a decision appended after the
            // deadline can never approve a gate whose SLA had in fact run out. (`torii run
            // gate decide` pre-checks the deadline too — against the journaled instant,
            // with the same `now >= d` boundary — but non-atomically, so it narrows the
            // window and never closes it. This arm remains the authority.) A default
            // "approved" payload on timeout was deliberately rejected (§4): a gate that
            // approves itself is the footgun this codebase's fail-closed stance argues
            // against.
            //
            // **The message names the DEADLINE, never "no decision"** — this arm has not
            // read the fold and so cannot know whether one exists. `AwaitSignal`'s "no
            // signal … by {d}" is accurate for the opposite reason: it reads its answer
            // BEFORE it checks expiry, so reaching its expiry proves the absence. Here the
            // ordering is reversed (that is what closes the self-approval-after-expiry
            // hole), and its accepted cost is that a decision delivered inside the SLA is
            // discarded if no drive folds it before the deadline. Telling THAT operator
            // "no decision" — the wording this arm shipped with — would send them hunting
            // a delivery bug that does not exist, in a durable message `torii run status`
            // renders and every later drive re-emits from the fold.
            Ok(WaitState::Expired(d)) => {
                return self
                    .fail_gate(
                        run,
                        node,
                        format!(
                            "human_gate: node {} passed its deadline {d}; the gate fails on \
                             the deadline BEFORE any decision is read, so a decision that \
                             had already landed does not approve it",
                            node.id.0
                        ),
                    )
                    .await;
            }
            // Already asking ⇒ the menu the human was ACTUALLY shown, read back from the
            // journal.
            //
            // Its absence here is not a case to paper over. `deadlines` is folded from
            // ALL THREE waiting kinds — `SignalAwaited`, `GateAwaited` and, since SP-6 s3,
            // `AgentAwaited` (one answer to "has this node begun asking?", shared by every
            // kind) — while only `GateAwaited` carries a menu, so this arm is reachable by
            // editing a live run's graph to swap a waiting node's KIND. s3 WIDENS that
            // reachable set rather than changing this arm's handling: an `Agent` node
            // backed by a human that has already asked, re-pointed at a `HumanGate`,
            // arrives here exactly like the s1 `AwaitSignal` swap does. Falling back to
            // the graph's `options` in any of those cases would validate a human's answer
            // against a menu no human was ever shown — the non-durable menu §4 rejects —
            // and would do it silently.
            Ok(WaitState::Waiting(d)) => {
                let Some(published) = fold.menu_for(&node.id) else {
                    return self
                        .fail_gate(
                            run,
                            node,
                            format!(
                                "human_gate: node {} recorded that it began waiting but \
                                 published no menu, so there is nothing a decision could be \
                                 validated against. A waiting node's kind cannot be changed \
                                 mid-run; fail the run and start a new one.",
                                node.id.0
                            ),
                        )
                        .await;
                };
                (d, published.to_vec())
            }
        };

        // 2. Resolve the decision against the JOURNALED menu.
        if let Some(decision) = fold.gate_decision_for(&node.id) {
            let Some(chosen) = menu.iter().find(|o| o.name == decision.option) else {
                return self
                    .fail_gate(
                        run,
                        node,
                        format!(
                            "human_gate: node {} was decided with option {:?}, which is not \
                             in the menu it published ({}). The decision is durable but \
                             cannot be honoured.",
                            node.id.0,
                            decision.option,
                            names(&menu, ", ")
                        ),
                    )
                    .await;
            };

            return match chosen.outcome {
                // SP-4 s2 (§6.4): redact ONCE and hand that one value to BOTH the node's
                // return AND — via `apply_node_result` → `publish_context` — the durable
                // blackboard write. Deriving both from a single application is the
                // determinism rule: live == journaled == replayed. Redacting on only one
                // of the two paths makes a live run and a replayed run disagree about this
                // node's output, which surfaces later as a false `DeterminismViolation`;
                // that exact defect has been shipped and caught twice in this codebase
                // already. `note` is operator free text and becomes this node's output,
                // flowing into downstream nodes and model prompts — it is not merely
                // displayed. Guarded by
                // `a_decision_is_redacted_before_both_the_return_and_the_durable_write`.
                //
                // Built INSIDE this arm: the `Fail` arm below never reads it, so hoisting
                // it made every rejection pay a full regex scan for a value it discards.
                GateOutcome::Complete => Ok(NodeExec::Completed(self.redact(&serde_json::json!({
                    "decision": decision.option,
                    "actor": decision.actor,
                    "note": decision.note,
                })))),
                GateOutcome::Fail => {
                    // WHO and WHY, both named: a rejection whose cause is unrecorded is
                    // useless in ops, and this text is what `torii run status` renders.
                    // The note is operator free text, so it is a leak surface — but the
                    // scrub lives in `fail_gate`, not here, so that no failure arm can
                    // skip it.
                    let reason = decision.note.as_deref().unwrap_or("no reason given");
                    let message = format!(
                        "human_gate: node {} rejected by {} ({}): {reason}",
                        node.id.0, decision.actor, decision.option
                    );
                    self.fail_gate(run, node, message).await
                }
            };
        }

        // 3. No decision yet ⇒ a durable pause on the deadline this node RECORDED (never
        //    `now + timeout`; see `pause_awaiting`). The reason lists the menu so an
        //    operator reading `torii run status` can answer without the graph in hand.
        let reason = format!(
            "human_gate: waiting for a decision on node {} ({}){}",
            node.id.0,
            names(&menu, " | "),
            deadline
                .map(|d| format!(" (deadline {d})"))
                .unwrap_or_default()
        );
        self.pause_awaiting(run, reason, deadline).await
    }

    /// Journal a `NodeFailed` for this gate and return it. Every failure path above goes
    /// through here so the journaled message and the returned one cannot drift.
    ///
    /// `output: None` on every one of them, and that is the AC5 property: an expired or
    /// rejected gate produces NO output, defaulted or otherwise.
    ///
    /// **The redaction is HERE, at the chokepoint, not at the call sites** — the same
    /// reason SP-4 s2 put it in `model_output` rather than in each of the four producers
    /// that feed it. Two of these messages interpolate operator-controlled text, and the
    /// second was missed when each arm scrubbed for itself: the `note` on a `Fail` option
    /// is free text by design, and the OPTION NAME on the undeclared path is arbitrary by
    /// definition, since it matched nothing in the menu. That one shipped a plaintext
    /// credential into the durable journal, into `RunOutcome.failed`, and into what
    /// `torii run status` renders — and because `fold.failed` is read back by
    /// `gate_precheck`, it was re-emitted on every later drive. Scrubbing once, where the
    /// journal write happens, is what makes a future failure arm safe by construction.
    ///
    /// Double-scrubbing a message that was already clean is a non-issue: `[REDACTED]`
    /// does not re-match any of the patterns.
    async fn fail_gate(
        &self,
        run: RunId,
        node: &Node,
        message: String,
    ) -> Result<NodeExec, OrchestratorError> {
        let message = self.redact_text(message);
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

    /// Run one operator-facing STRING through the configured redactor.
    ///
    /// [`Executor::redact`] is typed over `serde_json::Value`, so a bare message has to be
    /// wrapped and unwrapped. The non-string arm is not dead defensiveness:
    /// `Redactor::redact(&Value) -> Value` promises nothing about preserving the variant,
    /// and a third-party impl is free to return anything — only `PatternRedactor` happens
    /// to map a string to a string. That property must not be ASSUMED here, and an
    /// `unwrap` would turn a legal impl into a panic, which this file's rule forbids: a
    /// panic in a node kind unwinds through `Scheduler::tick` and poisons the worker.
    ///
    /// Keeping the original message is a deliberate TRADEOFF, not a safe default: against
    /// a redactor that changed the variant it would journal the unscrubbed text. The
    /// alternatives are worse — panicking is forbidden, and discarding the message loses
    /// the only record of why the run failed. A variant-changing redactor is malformed;
    /// the shipped `PatternRedactor` is not one, so this arm is unreachable in practice.
    /// `pub(super)` since SP-6 s3: `human.rs`'s `fail_human_agent` is the second failure
    /// chokepoint that needs it, and a second copy of the tradeoff above is a second place
    /// to get it wrong. It stays defined here rather than moving next to
    /// [`Executor::redact`] in `content.rs` because its whole justification is the
    /// failure-message chokepoint pattern this file introduced.
    pub(super) fn redact_text(&self, message: String) -> String {
        match self.redact(&serde_json::Value::String(message.clone())) {
            serde_json::Value::String(scrubbed) => scrubbed,
            _ => message,
        }
    }
}

/// The menu's option names, for a human to read. `sep` differs by site — a failure lists
/// them as prose (`ship, reject`), a pause offers them as choices (`ship | reject`).
fn names(menu: &[GateOption], sep: &str) -> String {
    menu.iter()
        .map(|o| o.name.as_str())
        .collect::<Vec<_>>()
        .join(sep)
}
