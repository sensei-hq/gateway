//! The `AwaitSignal` node (SP-6 s1): the HITL primitive — pause until an external
//! signal arrives for this node, with an optional deadline that FAILS it.
//!
//! SP-DATA-4 shipped HOTL, human *on* the loop: an operator intervenes from outside
//! (`torii run cancel` / `run wake`) and the run does not know they exist. This is
//! HITL, human *in* the loop: the graph blocks on a human decision as a first-class
//! node. The substrate for the waiting already existed and is proven — `RunPaused`
//! with a `None` `resume_after` is the never-auto-woken class, and the durable
//! scheduler wakes a timed one. What did not exist was a way to carry an *answer*
//! back in: `force_wake` is a resume, not a decision. `SignalReceived` is.
//!
//! SP-6 s2 splits this node into the parts that are GENERIC to any waiting node kind —
//! [`Executor::gate_precheck`], [`Executor::wait_or_expire`], [`Executor::pause_awaiting`]
//! — and the part that is specific to `AwaitSignal`, which is only ever *what counts as
//! an answer* (here, a folded `SignalReceived` payload; for `HumanGate`, a folded
//! `GateDecided` outcome). The split is not tidiness. s1's whole-slice review found real
//! defects in exactly two of the arms below — the fail-closed terminal guard and the
//! deadline durability — and a copy of either in a second node kind is a second place for
//! those defects to come back.

use orchestrator_core::{JournalEvent, Node, NodeId, OrchestratorError, RunId};

use super::{Executor, Fold, NodeExec};

/// What a waiting node's shared machinery decided, when no answer is present.
///
/// Deliberately reports rather than acts: every variant leaves the JOURNALING to the
/// caller, because the event a node writes when it begins asking is the one thing that
/// is genuinely per-kind (`SignalAwaited` for `AwaitSignal`, `GateAwaited` — which also
/// carries the menu — for `HumanGate`). Folding that write into the shared helper would
/// force it to know every node kind, which is the coupling this split exists to avoid.
///
/// There is no `AlreadyFailed` variant, and that absence is a decision — see
/// [`Executor::wait_or_expire`]'s precondition.
pub(super) enum WaitState {
    /// Nothing is recorded for this node yet; the caller must journal its own
    /// "now asking" event, then continue with the deadline carried here.
    ///
    /// The payload is the FRESHLY COMPUTED absolute deadline (`None` for an indefinite
    /// gate). It is computed here and journaled by the caller precisely so that it is
    /// computed exactly ONCE in the node's whole life.
    NotYetAsking(Option<chrono::DateTime<chrono::Utc>>),
    /// The node is asking, the deadline it recorded has passed, and no answer arrived.
    /// Carries the recorded instant so the caller can name it in the failure.
    Expired(chrono::DateTime<chrono::Utc>),
    /// The node is asking and still has time — `None` for the indefinite human gate,
    /// which has no time to run out of. Carries the RECORDED deadline, which is what the
    /// caller must pause on; recomputing it is the defect this whole type exists to stop.
    Waiting(Option<chrono::DateTime<chrono::Utc>>),
}

impl Executor {
    /// Arm 0 of §6.2, shared by BOTH waiting node kinds: a folded `NodeFailed` for this
    /// node is TERMINAL, read back rather than re-derived, and — this is the load-bearing
    /// part — checked BEFORE the caller reads its answer.
    ///
    /// **Whole-slice review, Important.** `fold_journal` originally had no `NodeFailed`
    /// arm, so an expired gate was not terminal on resume, with two consequences, the
    /// second serious. (a) The run stays resumable while any OTHER node is paused, so
    /// every wake re-ran the gate, re-derived the same expiry and appended another
    /// `NodeFailed` for an already-dead node. (b) Worse: append one late `SignalReceived`
    /// and the re-run took the answer arm and COMPLETED — a run that had terminally failed
    /// on its deadline reached `RunCompleted` carrying the operator's
    /// `{"decision":"approved"}`. That is precisely the silent self-approval §4 rejects,
    /// arrived at by the back door. It is reachable: `torii run signal` pre-checks the
    /// gate's state and then appends, and nothing makes those two steps atomic — the CLI
    /// can report the outcome honestly but cannot stop the row existing, so the guard
    /// belongs HERE, in the executor.
    ///
    /// **Call this FIRST, before reading the node's answer.** That ordering is the whole
    /// decision, not an implementation detail: a signal (or a gate decision) is only an
    /// answer if it arrived while the node was still asking. Ordering the two by `Seq`
    /// instead would let an answer appended microseconds before the expiry — an operator
    /// answering against a snapshot the mid-flight drive had already passed — approve a
    /// gate whose deadline had in fact run out. The deadline is the contract; fail-closed
    /// is the only reading that does not turn a missed SLA into an approval.
    ///
    /// Shared rather than copied deliberately: a second copy of this is a second place for
    /// (b) to come back, in a node kind whose entire purpose is a human decision.
    ///
    /// This is the ONLY consumer family of `fold.failed` — see [`Fold::failed`]'s doc
    /// comment. A `NodeFailed` does NOT make a node terminal in general: a `ModelCall` or
    /// `Agent` whose provider died re-attempts on resume, by design and by test. A waiting
    /// node is the one kind whose failure is irreversible by construction, because the
    /// thing that failed is an instant that has passed.
    ///
    /// Returns `Some(Failed)` to be returned verbatim by the caller, or `None` to carry
    /// on. It journals NOTHING — the verdict is read back from the journal, so re-writing
    /// it is exactly consequence (a).
    pub(super) fn gate_precheck(&self, node: &Node, fold: &Fold) -> Option<NodeExec> {
        self.gate_precheck_by_id(&node.id, fold)
    }

    /// [`Executor::gate_precheck`] over a bare [`NodeId`], and the ONE implementation of
    /// it — the `&Node` form above is a two-line delegation.
    ///
    /// The split exists because SP-6 s3's human-backed `Agent` node is reached through
    /// `drive_agent`, which holds only a `&NodeId`: a `Map`/`Loop` child runs at the
    /// synthesized path `"{map}/{i}"`, which is a node id with no `Node` anywhere in the
    /// graph to correspond to it. Every word of the doc comment above applies here
    /// unchanged, because this IS that function.
    ///
    /// Duplicating the body instead would be the specific mistake this whole shared-helper
    /// arrangement exists to prevent: s1's whole-slice review found real defects in exactly
    /// these arms, and a second copy is a second place for them to return.
    pub(super) fn gate_precheck_by_id(&self, node: &NodeId, fold: &Fold) -> Option<NodeExec> {
        fold.failure_for(node).map(|error| NodeExec::Failed {
            message: error.to_string(),
            output: None,
        })
    }

    /// Arms 2–4 of §6.2, shared: read the recorded deadline or compute a fresh one, and
    /// report whether the node has begun asking, has expired, or is still waiting.
    ///
    /// **Precondition: the caller has already called [`Executor::gate_precheck`] and
    /// returned early on `Some`.** This function therefore does NOT re-check
    /// `fold.failure_for`, and that is a deliberate decision rather than an oversight. A
    /// copy of the check here would be strictly worse than none, because both callers read
    /// their answer BETWEEN the precheck and this call — so a check at this point runs
    /// *after* the answer read and does nothing whatsoever about the late-answer
    /// self-approval that motivated the guard. Its only effect would be to advertise a
    /// protection it does not provide, inviting a future node kind to treat
    /// `gate_precheck` as optional. The guard is only a guard where it currently is: first,
    /// unconditionally, ahead of the answer.
    ///
    /// **The deadline is READ from the fold, never recomputed.** The obvious implementation
    /// does `now + timeout` on every execution, and it is wrong in a way a naive test does
    /// not catch: every resume pushes the deadline forward, so a run force-woken every ten
    /// minutes with a one-hour timeout would NEVER expire. The absolute instant is fixed at
    /// the first execution, journaled by the caller, and folded (first-wins) thereafter.
    /// [`Fold::deadline_for`] is the durable half; this is the other half. It is also what
    /// makes `torii run wake` on a waiting node behave sanely instead of silently resetting
    /// the clock.
    ///
    /// `deadline_for` returns `Option<Option<_>>` and both layers carry weight: the OUTER
    /// one asks "has this node begun waiting?", the inner "by when?". A timeout-less gate
    /// (the common indefinite HITL shape) journals its awaited event with `deadline: None`
    /// and the fold remembers that `None` as a REAL value, so [`WaitState::NotYetAsking`]
    /// fires ONCE per node and never again. Making that node-keyed rather than
    /// deadline-keyed is what bounds it: a re-drive of a deadline-less gate is NOT
    /// human-bounded, because `drive` runs every ready node in a round even after one
    /// pauses — a dep-free sibling that pauses WITH a deadline in the same round leaves it
    /// as the last `RunPaused`, which is the one the scheduler takes `next_wake` from, so
    /// the whole run stays auto-wakeable while this gate waits on a human.
    ///
    /// `checked_add_signed`, not `+`: `chrono::Duration` reaches ~292 million years while
    /// `DateTime<Utc>` stops at year 262143, so the plain `+` PANICS on a large enough
    /// timeout — and a panic here is not local. It unwinds through `Scheduler::tick` (which
    /// has already claimed a batch of runs and taken their leases) and out of
    /// `worker::serve`'s in-task `ticker.tick()`, killing the worker; the claimed row stays
    /// `waking`, so the next worker reclaims the stale lease and dies the same way.
    /// `Graph::validate_dag` rejects such a timeout up front (`MAX_AWAIT_SIGNAL_TIMEOUT`)
    /// and that is the layer which keeps the durable row from ever existing — but
    /// `Executor::start` takes the graph as a caller parameter and nothing guarantees it
    /// was ever validated, so a node kind is not allowed to panic on its own. This is the
    /// second layer, which turns a slip past validation into a failed run rather than a
    /// killed worker.
    ///
    /// The overflow is reported as `Err(message)` — UNPREFIXED, so each caller can name
    /// its own node kind — and nothing is journaled by either side before the caller's
    /// `NodeFailed`. That ordering matters: an awaited event carrying a nonsense deadline
    /// would be folded first-wins forever.
    pub(super) fn wait_or_expire(
        &self,
        node: &Node,
        timeout: Option<chrono::Duration>,
        fold: &Fold,
    ) -> Result<WaitState, String> {
        self.wait_or_expire_by_id(&node.id, timeout, fold)
    }

    /// [`Executor::wait_or_expire`] over a bare [`NodeId`], and the ONE implementation of
    /// it — the `&Node` form above is a two-line delegation.
    ///
    /// Same reason as [`Executor::gate_precheck_by_id`]: SP-6 s3's human-backed `Agent`
    /// node is reached through `drive_agent`, which holds only a `&NodeId` (a `Map`/`Loop`
    /// child runs at the synthesized path `"{map}/{i}"`, which has no `Node` in the graph
    /// at all). The doc comment above — the precondition, the deadline durability, the
    /// `checked_add_signed` overflow argument — applies here unchanged, because this IS
    /// that function.
    pub(super) fn wait_or_expire_by_id(
        &self,
        node: &NodeId,
        timeout: Option<chrono::Duration>,
        fold: &Fold,
    ) -> Result<WaitState, String> {
        let Some(recorded) = fold.deadline_for(node) else {
            let fresh = match timeout {
                None => None,
                Some(t) => match self.clock.now().checked_add_signed(t) {
                    Some(instant) => Some(instant),
                    None => {
                        return Err(format!(
                            "node {} has a timeout ({t}) that overflows the representable \
                             instant range when added to now",
                            node.0
                        ));
                    }
                },
            };
            return Ok(WaitState::NotYetAsking(fresh));
        };
        match recorded {
            Some(d) if self.clock.now() >= d => Ok(WaitState::Expired(d)),
            other => Ok(WaitState::Waiting(other)),
        }
    }

    /// The durable pause both waiting kinds end on.
    ///
    /// `resume_after` carries the ORIGINAL absolute deadline (not `now + timeout`), so the
    /// durable scheduler re-arms on the same instant however many times the run is woken
    /// early — without which the whole timed branch would be decorative, never auto-woken,
    /// and the timeout would exist only on paper. `None` is SP-DATA-3's never-auto-woken
    /// class: only an answer or a `force_wake` moves it, which is precisely the indefinite
    /// human gate.
    ///
    /// The `reason` is echoed into `NodeExec::Paused` so the two can never disagree; it is
    /// operator-facing text (`torii run status` prints it), not a machine contract.
    pub(super) async fn pause_awaiting(
        &self,
        run: RunId,
        reason: String,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<NodeExec, OrchestratorError> {
        self.append(
            run,
            JournalEvent::RunPaused {
                reason: reason.clone(),
                resume_after: deadline,
            },
        )
        .await?;
        Ok(NodeExec::Paused { reason })
    }

    /// Execute one `AwaitSignal` node: a three-way read of the fold (design §6.2) —
    /// *signalled* / *not yet waiting* / *already waiting* — the last of which the table
    /// below splits by the shape of what was recorded.
    ///
    /// | fold state | behaviour |
    /// |---|---|
    /// | failure recorded | `Failed` — the expiry is READ back, never re-derived |
    /// | signal present | `Completed(payload)` — never re-asks |
    /// | no signal, nothing recorded | journal `SignalAwaited`, pause on `deadline` |
    /// | a wait recorded by ANOTHER kind, so no `SignalAwaited` | `NodeFailed` — the kind swap |
    /// | no signal, deadline recorded, `now >= deadline` | `NodeFailed` — the timeout, loudly |
    /// | no signal, deadline recorded, `now < deadline` | re-pause on the **same** deadline |
    /// | no signal, `None` recorded (indefinite gate) | re-pause, journaling nothing further |
    ///
    /// **The deadline is READ from the fold, never recomputed.** The obvious
    /// implementation does `now + timeout` on every execution, and it is wrong in a way
    /// a naive test does not catch: every resume pushes the deadline forward, so a run
    /// force-woken every ten minutes with a one-hour timeout would NEVER expire. The
    /// absolute instant is therefore fixed at the first execution, journaled as
    /// `SignalAwaited`, and folded (first-wins) thereafter. `fold.deadline_for` is the
    /// durable half; [`Executor::wait_or_expire`] is the other half. The last two table
    /// rows exist ONLY because of that durability, and it is what makes `torii run wake` on
    /// an awaiting node behave sanely instead of silently resetting the clock.
    ///
    /// **The deadline belongs to the RUN, not to the graph.** Once the first execution
    /// has recorded one, editing this node's `timeout` has no effect in EITHER direction:
    /// a run that recorded `Some(t)` still expires at `t` even if the graph now says
    /// `timeout: None`, and a run that recorded `None` never expires even if the graph
    /// now names an hour. That follows from AC1's durability plus the caller-supplied,
    /// unfenced graph (`Executor::start` takes the graph as a parameter and does not
    /// journal it — a pre-existing SP-DATA-3 property); it is the correct consequence,
    /// not a gap. To change a live gate's deadline, fail the run and start a new one.
    ///
    /// No `EffectRecorded` is written and no gateway call is made — the fold IS this
    /// node's memo, so a *resuming* run re-reads its answer for free (zero token re-spend
    /// by construction). Like `Branch`/`Subgraph`, it journals no
    /// `NodeStarted`/`NodeCompleted`; the three writes below are exactly the ones §6.2
    /// specifies. Known limitation (the fresh-vs-terminal asymmetry `run_subgraph`
    /// documents, and shared with it): because no `EffectRecorded`/`NodeCompleted` is
    /// journaled, a re-`start` of an already-TERMINAL run rebuilds `outputs`/`completed`
    /// from exactly those events and so reports this node in neither. The durable
    /// blackboard is unaffected — the completing drive published the payload under
    /// `ContextWrite` — and matching the house style is deliberate: journaling a
    /// `NodeCompleted` for `AwaitSignal` alone would make one node kind divergent.
    pub(super) async fn run_await_signal(
        &self,
        run: RunId,
        node: &Node,
        timeout: Option<chrono::Duration>,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        // 0. This gate has ALREADY failed ⇒ it stays failed. Shared with `HumanGate`, and
        //    FIRST — ahead of the signal read — for the fail-closed reason spelled out on
        //    `gate_precheck`.
        if let Some(failed) = self.gate_precheck(node, fold) {
            return Ok(failed);
        }

        // 1. The answer is already folded ⇒ complete, and never re-ask. This also
        //    resolves the early-signal race for free (§6.3): a signal delivered BEFORE
        //    the node first ran is simply already here, so there is no buffering, no
        //    ordering constraint and no special case.
        //
        //    This is the ONE arm that is genuinely per-node-kind — what counts as an
        //    answer — which is why it stays here rather than moving into the shared
        //    helpers around it.
        //
        //    SP-4 s2 (§6.4): redact ONCE, here, and hand that one value to BOTH the
        //    node's return AND — via `apply_node_result` → `publish_context` — the
        //    durable blackboard write. Deriving both from a single application is the
        //    determinism rule: live == journaled == replayed. Redacting on only one of
        //    the two paths would make a live run and a replayed run disagree about this
        //    node's output, which surfaces later as a false `DeterminismViolation`; that
        //    exact defect has been shipped and caught twice in this codebase already.
        //    A payload is not a credential channel (the broker is), and unlike a pause
        //    reason it does not merely get displayed — it becomes the node's output and
        //    flows into downstream nodes and model prompts.
        if let Some(payload) = fold.signal_for(&node.id) {
            return Ok(NodeExec::Completed(self.redact(payload)));
        }

        // 2. Not answered. Take what this node ALREADY recorded, or — only on the very
        //    first execution — compute the deadline once and journal the absolute instant.
        //    `wait_or_expire` decides; the journaling stays here because `SignalAwaited` is
        //    this kind's own event.
        let deadline = match self.wait_or_expire(node, timeout, fold) {
            // The overflow guard's second layer. Nothing has been journaled yet and
            // nothing is, beyond the failure itself: a `SignalAwaited` carrying a nonsense
            // deadline would be folded first-wins forever.
            Err(message) => {
                let message = format!("await_signal: {message}");
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
            // The node-keyed record of WHICH node is awaiting, and the durable home of the
            // deadline. It is written at all because `RunPaused` is not node-keyed, and a
            // run pauses for many unrelated reasons over its life.
            Ok(WaitState::NotYetAsking(fresh)) => {
                self.append(
                    run,
                    JournalEvent::SignalAwaited {
                        node: node.id.clone(),
                        deadline: fresh,
                    },
                )
                .await?;
                fresh
            }
            // 3. The recorded deadline has passed with no signal ⇒ FAIL, loudly, naming
            //    the node and the instant. Never a silent self-approval: a default payload
            //    on timeout was deliberately rejected (§4) — a gate that approves itself is
            //    exactly the footgun this codebase's fail-closed stance argues against.
            Ok(WaitState::Expired(d)) => {
                let message = format!("await_signal: no signal for node {} by {d}", node.id.0);
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
            // Already waiting by the SHARED map's reckoning — but did THIS kind begin
            // waiting here?
            //
            // `Fold::deadlines` is written by all four waiting kinds (SP-6 s4's
            // `LoopGateAwaited` is the fourth) while only `SignalAwaited` records
            // membership in `signal_asks`, so this arm is reachable exactly the way
            // `run_human_gate`'s missing-menu arm and `run_human_agent`'s
            // missing-question arm are: by editing a live run's graph to change a waiting
            // node's KIND (`Executor::start` takes the graph as an unfenced caller
            // parameter, and `scheduled_runs.graph` is an editable row). s2 and s3 each
            // shipped their side of that guard; this side was left open, and it is the one
            // whose failure mode is SILENT.
            //
            // **Loud, because the alternative is unanswerable.** Without this arm the node
            // took the `Waiting` path with the other kind's deadline — `None` for an
            // indefinite human gate or agent, which is SP-DATA-3's never-auto-woken class —
            // and re-paused forever. It cannot be rescued by any verb: since s3
            // `cmd::run::signal` REFUSES a node carrying an `AgentAwaited` and points the
            // operator at `run agent answer`, which is journal-only, appends `AgentAnswered`
            // and reports exit 0 — an event `run_await_signal` never reads. Every operator
            // surface says the answer landed; nothing ever completes. Only `run cancel`
            // moves it.
            //
            // Journaling a `SignalAwaited` here instead was the other candidate and is the
            // same bad trade `run_human_agent` records: `deadlines` folds first-wins, so the
            // ask would be published against a deadline another kind chose, and the run
            // would carry two contradictory durable claims about what it is waiting for.
            //
            // The `Expired` arm above needs no such check: it already fails loudly and
            // terminally, so a swapped node there is dead rather than stuck. And the answer
            // read at step 1 deliberately stays AHEAD of this — a node that has a payload is
            // not unanswerable, and moving it would break §6.3's early-signal race (a signal
            // folded before the node ever ran completes on the spot, journaling nothing).
            Ok(WaitState::Waiting(_)) if !fold.has_signal_ask(&node.id) => {
                let message = format!(
                    "await_signal: node {} recorded that it began waiting but published no \
                     SignalAwaited, so this run is waiting on a different node kind's record \
                     and no signal delivered here can ever be read. A waiting node's kind \
                     cannot be changed mid-run; fail the run and start a new one.",
                    node.id.0
                );
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
            Ok(WaitState::Waiting(d)) => d,
        };

        // A FRESHLY recorded deadline can ALREADY have passed, so the expiry check runs
        // once more here, over the deadline whichever arm produced.
        //
        // This is the SECOND site that decides expiry (`WaitState::Expired` is the first),
        // and the two read the clock at different moments. For an ALREADY-recorded deadline
        // that makes this check a re-read, which is harmless in the direction that matters:
        // if the two reads disagree it is because time moved between them, and the later
        // read is the truthful one — which is the one this check uses.
        //
        // Do NOT restate that as "the clock is monotonic". It is not: `SystemClock::now()`
        // is `Utc::now()`, wall time, so an NTP step BACKWARD can make read 1 land past the
        // deadline and read 2 before it. `wait_or_expire` has already returned `Expired` by
        // then and this code never runs — the node fails. That is the right direction for a
        // deadline (fail-closed: a gate whose SLA was observed to have run out does not get
        // un-expired by the clock being corrected), but it is a fail-closed property, not a
        // monotonicity guarantee, and nothing downstream may assume the latter.
        //
        // REACHABLE on the first execution — an earlier version of this comment claimed
        // otherwise ("`validate_dag` rejects a non-positive timeout, so a freshly computed
        // `now + timeout` is always in the future") and a reviewer disproved it.
        // `validate_dag` does bound the timeout at both ends, but a positive one is still
        // racing a moving clock: `wait_or_expire` fixes the deadline from one `now`, and
        // this reads the clock AGAIN, after an `await`ed journal append. Any timeout
        // shorter than that gap has already elapsed by the time we get here —
        // `timeout: Some(1ns)` against a real clock journals `SignalAwaited` and then
        // `NodeFailed` in a SINGLE execution (measured: `["RunStarted",
        // "SignalAwaited(gate)", "NodeFailed(gate)"]`). That is correct and loud — a gate
        // given a nanosecond to answer has genuinely expired — and it is written down so
        // nobody re-derives an "unreachable" guarantee this code does not make.
        if let Some(d) = deadline
            && self.clock.now() >= d
        {
            let message = format!("await_signal: no signal for node {} by {d}", node.id.0);
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

        // 4. Still waiting ⇒ a durable pause on the ORIGINAL deadline. See
        //    `pause_awaiting` for why re-arming on the same instant is what keeps the
        //    timed branch from being decorative.
        let reason = format!(
            "await_signal: waiting for a signal on node {}{}",
            node.id.0,
            deadline
                .map(|d| format!(" (deadline {d})"))
                .unwrap_or_default()
        );
        self.pause_awaiting(run, reason, deadline).await
    }
}
