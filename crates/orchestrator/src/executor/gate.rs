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
    /// | asking, deadline passed | `NodeFailed` — the timeout, before any answer is read |
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
    /// **Validation is enforced HERE even though the CLI already checks.** `torii`'s
    /// check is non-atomic (it pre-checks, then appends) and the library entry point
    /// bypasses it entirely, so the CLI can report honestly but cannot stop the row
    /// existing. Same conclusion s1 reached for the terminal guard.
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
            // deadline (which `torii`'s non-atomic pre-check cannot prevent) can never
            // approve a gate whose SLA had in fact run out. A default "approved" payload
            // on timeout was deliberately rejected (§4): a gate that approves itself is
            // the footgun this codebase's fail-closed stance argues against.
            Ok(WaitState::Expired(d)) => {
                return self
                    .fail_gate(
                        run,
                        node,
                        format!("human_gate: no decision for node {} by {d}", node.id.0),
                    )
                    .await;
            }
            // Already asking ⇒ the menu the human was ACTUALLY shown, read back from the
            // journal.
            //
            // Its absence here is not a case to paper over. `deadlines` is folded from
            // BOTH `SignalAwaited` and `GateAwaited` (one answer to "has this node begun
            // asking?", for both waiting kinds) while only the latter carries a menu, so
            // this arm is reachable by editing a live run's graph to swap a waiting node's
            // KIND. Falling back to the graph's `options` there would validate a human's
            // answer against a menu no human was ever shown — the non-durable menu §4
            // rejects — and would do it silently.
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

            // SP-4 s2 (§6.4): redact ONCE and hand that one value to BOTH the node's
            // return AND — via `apply_node_result` → `publish_context` — the durable
            // blackboard write. Deriving both from a single application is the determinism
            // rule: live == journaled == replayed. Redacting on only one of the two paths
            // makes a live run and a replayed run disagree about this node's output, which
            // surfaces later as a false `DeterminismViolation`; that exact defect has been
            // shipped and caught twice in this codebase already. `note` is operator free
            // text and becomes this node's output, flowing into downstream nodes and model
            // prompts — it is not merely displayed.
            let output = self.redact(&serde_json::json!({
                "decision": decision.option,
                "actor": decision.actor,
                "note": decision.note,
            }));

            return match chosen.outcome {
                GateOutcome::Complete => Ok(NodeExec::Completed(output)),
                GateOutcome::Fail => {
                    // WHO and WHY, both named: a rejection whose cause is unrecorded is
                    // useless in ops, and this text is what `torii run status` renders.
                    // Redacted for the same reason the output is — the note is free text
                    // and this message reaches the journal — but redacted SEPARATELY,
                    // because a secret split across the template's boundaries is a
                    // different string from the one in the output object.
                    let reason = decision.note.as_deref().unwrap_or("no reason given");
                    let message = format!(
                        "human_gate: node {} rejected by {} ({}): {reason}",
                        node.id.0, decision.actor, decision.option
                    );
                    self.fail_gate(run, node, self.redact_text(message)).await
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
    async fn fail_gate(
        &self,
        run: RunId,
        node: &Node,
        message: String,
    ) -> Result<NodeExec, OrchestratorError> {
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
    /// wrapped and unwrapped. A non-string result cannot happen (the redactor is a
    /// value-preserving scrub) but is handled by keeping the original rather than by an
    /// `unwrap` — this file's rule is that no arm of a node kind may panic.
    fn redact_text(&self, message: String) -> String {
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
