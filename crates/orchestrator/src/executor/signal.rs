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

use orchestrator_core::{JournalEvent, Node, OrchestratorError, RunId};

use super::{Executor, Fold, NodeExec};

impl Executor {
    /// Execute one `AwaitSignal` node: a three-way read of the fold (design §6.2) —
    /// *signalled* / *not yet waiting* / *already waiting* — the last of which the table
    /// below splits by the shape of what was recorded.
    ///
    /// | fold state | behaviour |
    /// |---|---|
    /// | signal present | `Completed(payload)` — never re-asks |
    /// | no signal, nothing recorded | journal `SignalAwaited`, pause on `deadline` |
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
    /// durable half; this function is the other half. The last two table rows exist ONLY
    /// because of that durability, and it is what makes `torii run wake` on an awaiting
    /// node behave sanely instead of silently resetting the clock.
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
        // 1. The answer is already folded ⇒ complete, and never re-ask. This also
        //    resolves the early-signal race for free (§6.3): a signal delivered BEFORE
        //    the node first ran is simply already here, so there is no buffering, no
        //    ordering constraint and no special case.
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
        //    first execution — compute the deadline once from the timeout duration and
        //    journal the absolute instant.
        //
        //    The match is on `Option<Option<_>>` and both layers carry weight: the OUTER
        //    one asks "has this node begun waiting?", the inner "by when?". A timeout-
        //    less gate (the common indefinite HITL shape) journals `SignalAwaited
        //    { deadline: None }` and the fold remembers that `None` as a real value, so
        //    this arm fires ONCE per node and never again. Making it node-keyed rather
        //    than deadline-keyed is what bounds it: a re-drive of a deadline-less gate is
        //    NOT human-bounded, because `drive` runs every ready node in a round even
        //    after one pauses — a dep-free sibling that pauses WITH a deadline in the
        //    same round leaves it as the last `RunPaused`, which is the one the scheduler
        //    takes `next_wake` from, so the whole run stays auto-wakeable while this gate
        //    waits on a human. The event is written at all because it is the NODE-KEYED
        //    record of which node is awaiting; `RunPaused` is not node-keyed, and a run
        //    pauses for many unrelated reasons over its life.
        //
        //    `checked_add_signed`, not `+`: `chrono::Duration` reaches ~292 million years
        //    while `DateTime<Utc>` stops at year 262143, so the plain `+` PANICS on a
        //    large enough timeout — and a panic here is not local. It unwinds through
        //    `Scheduler::tick` (which has already claimed a batch of runs and taken their
        //    leases) and out of `worker::serve`'s in-task `ticker.tick()`, killing the
        //    worker; the claimed row stays `waking`, so the next worker reclaims the stale
        //    lease and dies the same way. `Graph::validate_dag` rejects such a timeout up
        //    front (`MAX_AWAIT_SIGNAL_TIMEOUT`) and that is the layer which keeps the
        //    durable row from ever existing — but `Executor::start` takes the graph as a
        //    caller parameter and nothing guarantees it was ever validated, so a node kind
        //    is not allowed to panic on its own. Fail loudly and locally instead: the run
        //    stops, the worker does not, and NOTHING is journaled first — a `SignalAwaited`
        //    carrying a nonsense deadline would be folded first-wins forever.
        let deadline = match fold.deadline_for(&node.id) {
            Some(recorded) => recorded,
            None => {
                let fresh = match timeout {
                    None => None,
                    Some(t) => match self.clock.now().checked_add_signed(t) {
                        Some(instant) => Some(instant),
                        None => {
                            let message = format!(
                                "await_signal: node {} has a timeout ({t}) that overflows \
                                 the representable instant range when added to now",
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
                    },
                };
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
        };

        // 3. The deadline has passed with no signal ⇒ FAIL, loudly, naming the node and
        //    the instant. Never a silent self-approval: a default payload on timeout was
        //    deliberately rejected (§4) — a gate that approves itself is exactly the
        //    footgun this codebase's fail-closed stance argues against.
        //
        //    REACHABLE on the first execution — an earlier version of this comment claimed
        //    otherwise ("`validate_dag` rejects a non-positive timeout, so a freshly
        //    computed `now + timeout` is always in the future") and a reviewer disproved
        //    it. `validate_dag` does bound the timeout at both ends, but a positive one is
        //    still racing a moving clock: step 2 fixes the deadline from one `now`, and
        //    this check reads the clock AGAIN, after an `await`ed journal append. Any
        //    timeout shorter than that gap has already elapsed by the time we get here —
        //    `timeout: Some(1ns)` against a real clock journals `SignalAwaited` and then
        //    `NodeFailed` in a SINGLE execution (measured: `["RunStarted",
        //    "SignalAwaited(gate)", "NodeFailed(gate)"]`). That is correct and loud — a
        //    gate given a nanosecond to answer has genuinely expired — and it is written
        //    down so nobody re-derives an "unreachable" guarantee this code does not make.
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

        // 4. Still waiting ⇒ a durable pause. `resume_after` carries the ORIGINAL
        //    absolute deadline (not `now + timeout`), so the durable scheduler re-arms
        //    on the same instant however many times the run is woken early — without
        //    which the whole `Some(deadline)` branch would be decorative, never
        //    auto-woken, and the timeout would exist only on paper. `None` (no timeout)
        //    is SP-DATA-3's never-auto-woken class: only a signal or a `force_wake`
        //    moves it, which is precisely the indefinite human gate.
        let reason = format!(
            "await_signal: waiting for a signal on node {}{}",
            node.id.0,
            deadline
                .map(|d| format!(" (deadline {d})"))
                .unwrap_or_default()
        );
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
}
