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
    /// Execute one `AwaitSignal` node: a three-way read of the fold (design §6.2).
    ///
    /// | fold state | behaviour |
    /// |---|---|
    /// | signal present | `Completed(payload)` — never re-asks |
    /// | no signal, no deadline recorded | journal `SignalAwaited`, pause on `deadline` |
    /// | no signal, deadline recorded, `now >= deadline` | `NodeFailed` — the timeout, loudly |
    /// | no signal, deadline recorded, `now < deadline` | re-pause on the **same** deadline |
    ///
    /// **The deadline is READ from the fold, never recomputed.** The obvious
    /// implementation does `now + timeout` on every execution, and it is wrong in a way
    /// a naive test does not catch: every resume pushes the deadline forward, so a run
    /// force-woken every ten minutes with a one-hour timeout would NEVER expire. The
    /// absolute instant is therefore fixed at the first execution, journaled as
    /// `SignalAwaited`, and folded (first-wins) thereafter. `fold.deadline_for` is the
    /// durable half; this function is the other half. The last table row exists ONLY
    /// because of that durability, and it is what makes `torii run wake` on an awaiting
    /// node behave sanely instead of silently resetting the clock.
    ///
    /// No `EffectRecorded` is written and no gateway call is made — the fold IS this
    /// node's memo, so a resumed run re-reads its answer for free (zero token re-spend
    /// by construction). Like `Branch`/`Subgraph`, it journals no
    /// `NodeStarted`/`NodeCompleted`; the three writes below are exactly the ones §6.2
    /// specifies.
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

        // 2. Not answered. Take the deadline this node ALREADY recorded, or — only on
        //    the very first execution — compute it once from the timeout duration and
        //    journal the absolute instant.
        //
        //    A timeout-less gate (the common indefinite HITL shape) journals
        //    `SignalAwaited { deadline: None }`, which the fold deliberately ignores, so
        //    this arm re-records it on each drive. That is bounded and accepted: with no
        //    deadline the run is in the never-auto-woken class, so a re-drive only ever
        //    follows a human `force_wake` — the same rate at which the `RunPaused` below
        //    is appended anyway. The event is still written because it is the
        //    NODE-KEYED record of which node is awaiting; `RunPaused` is not node-keyed,
        //    and a run pauses for many unrelated reasons over its life.
        let deadline = match fold.deadline_for(&node.id) {
            Some(recorded) => Some(recorded),
            None => {
                let fresh = timeout.map(|t| self.clock.now() + t);
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
        //    Unreachable on the first execution: `validate_dag` rejects a non-positive
        //    timeout, so a freshly computed `now + timeout` is always in the future.
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
