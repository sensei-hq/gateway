//! Best-effort observability hooks (§15). No-op defaults; a wired impl observes
//! run/node/agent/context lifecycle. Hooks never affect execution or determinism.

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::context::{ContextKey, Scope};
use crate::effect::EffectId;
use crate::graph::{GateOption, Graph, LoopGateOption};
use crate::ids::{NodeId, RunId};
use crate::plan::NodePlan;
use crate::registry::AgentRef;

/// Observation callbacks fired by the executor at lifecycle points. Every method
/// defaults to a no-op, so an impl overrides only what it cares about. Best-effort
/// (§11.1.3): a hook must not depend on execution and should be fast (it is awaited
/// inline). Anything execution depends on is a durable effect, not a hook.
#[async_trait::async_trait]
pub trait OrchestratorHooks: Send + Sync {
    async fn on_run_started(&self, _run: RunId) {}
    async fn on_run_completed(&self, _run: RunId) {}
    async fn on_run_paused(&self, _run: RunId, _reason: &str) {}
    async fn on_node_started(&self, _run: RunId, _node: &NodeId) {}
    async fn on_node_completed(&self, _run: RunId, _node: &NodeId) {}
    async fn on_node_failed(&self, _run: RunId, _node: &NodeId, _error: &str) {}
    async fn on_node_skipped(&self, _run: RunId, _node: &NodeId) {}
    async fn on_agent_started(&self, _run: RunId, _node: &NodeId, _agent: &str, _chain: &str) {}
    async fn on_agent_turn(&self, _run: RunId, _node: &NodeId, _turn: usize) {}
    async fn on_agent_tool_call(&self, _run: RunId, _node: &NodeId, _tool: &str) {}
    async fn on_context_write(&self, _run: RunId, _scope: &Scope, _key: &ContextKey) {}
    async fn on_plan_expanded(
        &self,
        _run: RunId,
        _node: &NodeId,
        _graph: &Graph,
        _node_plans: &HashMap<NodeId, NodePlan>,
    ) {
    }
    async fn on_planner_selected(&self, _run: RunId, _node: &NodeId, _agent: &AgentRef) {}

    // ------------------------------------------------------------------------------
    // AG-2: human-in-the-loop (SP-6) events.
    //
    // **"Awaited" hooks** fire right after the executor journals the node's ask
    // (`SignalAwaited`/`GateAwaited`/`AgentAwaited`/`LoopGateAwaited`), from inside the
    // executor's append — the same can't-miss, replay-suppressed site every run/node hook
    // uses. A waiting node journals its ask exactly ONCE in its life (the fold remembers it,
    // first-wins, and a resume re-pauses without re-asking), so an awaited hook fires once
    // per real ask and never when a later drive re-pauses on it. A `Loop`'s human gate asks
    // once per ITERATION, at that iteration's own `"{loop}/{i}/__gate__"` path, so it fires
    // once per iteration. The early-signal race is the one ask that never happens: a
    // `SignalReceived` folded before its `AwaitSignal` node first ran completes the node
    // without journaling `SignalAwaited`, so `on_signal_received` fires with no
    // `on_signal_awaited` before it. (A human-backed `Agent` always asks before it reads
    // an answer, so `on_agent_awaited` always precedes `on_agent_answered`.)
    //
    // **"Decided" hooks** have no executor write to mirror: the decision rows
    // (`SignalReceived`/`GateDecided`/`AgentAnswered`/`LoopGateDecided`) are appended by
    // ANOTHER process — torii's CLI writes the journal directly — and the executor only
    // sees one when its next drive folds it. So a decided hook fires on the drive that
    // FIRST HONOURS the decision (the drive whose node completes on it, fails on a
    // `GateOutcome::Fail` option, or settles the loop gate on it), exactly once across
    // resumes:
    //
    // - `AwaitSignal`, `HumanGate` and a human-backed `Agent` journal nothing when they
    //   complete on an answer, and every later drive of a still-live run re-completes them
    //   from the fold. So before firing, the honouring drive journals
    //   `JournalEvent::DecisionHookFired { node, decision }` — `decision` is the `Seq` of
    //   the decision row it reported — and a later drive skips the node while the fold
    //   still holds that same decision: that row, or a later row with IDENTICAL content (a
    //   retrying webhook or a double-submitted option decides nothing new, so it reports
    //   nothing). The row is written ONLY when hooks are wired — an executor
    //   with no hooks journals exactly what it did before — and best-effort: a failed
    //   write never fails the run, and the drive fires the hook anyway (skipping would
    //   lose it whenever that drive also finished the run).
    // - A loop gate is settled by the executor's own `LoopGateSettled`, which it writes at
    //   most once; `on_loop_gate_decided` then `on_loop_gate_settled` fire right after
    //   that write, and a later drive that replays the settled gate fires neither.
    //
    // A decision that is never honoured fires nothing: one that arrives after its gate's
    // deadline already expired it (the node fails; `on_node_failed` fires), one naming an
    // option the published menu does not contain (likewise), one overwritten by a
    // correction before any drive read it (only the decision actually honoured is
    // reported), one appended after the node had already failed (a failed waiting node is
    // terminal), or one appended after the run finished.
    //
    // A correction appended after a COMPLETED `AwaitSignal`/`HumanGate`/human `Agent`, while
    // the run is still live, IS honoured: those kinds journal no durable completion, the
    // decision rows fold LAST-wins, and the next drive re-completes the node on the new
    // decision (a `Fail` option then fails a gate that had completed). That drive reports
    // it — its decision row says something other than the row the marker recorded — so the
    // hook stream always names the decision behind what the node did. "Something other" is
    // compared against the decision LAST reported: a correction reverted before any drive
    // honoured it reports nothing, while one honoured and then reverted reports each change.
    // Content is the whole row the hook reports (a gate's actor and note, an answer's
    // actor), so the same option resubmitted by a different actor IS reported.
    //
    // Three edges are at-most-once / at-least-once rather than exactly-once, all outside
    // the hooks' control: a crash between the durable marker (`DecisionHookFired` or
    // `LoopGateSettled`) and the callback loses the callback; a decision honoured by a
    // drive that had NO hooks wired leaves no marker, so a later hooked drive that replays
    // the completed node reports it then (a deployment that wires hooks on every drive of
    // a run never sees this one); and a FAILED `DecisionHookFired` write leaves no marker
    // either, so the honouring drive fires anyway and a later drive of a still-live run
    // reports the same decision again. An identical REDELIVERY of a reported decision is
    // not a fourth edge: it fires nothing.
    //
    // Every string handed to a HITL hook has been through the executor's redactor. A
    // decided hook gets the same scrub the node's output gets. The agent and loop-gate
    // asks are redacted before they are journaled, so their awaited hooks receive exactly
    // the journaled row. `HumanGate` journals its graph-authored menu as-is (that row is
    // what a decision is validated against), so `on_gate_awaited` receives the option
    // names redacted AT DISPATCH — the same scrubbed names `on_gate_decided` reports, which
    // may differ from the journaled `GateAwaited.options`.
    // ------------------------------------------------------------------------------

    /// An `AwaitSignal` node began waiting (its `SignalAwaited` was just journaled), with
    /// the ABSOLUTE deadline it recorded (`None` = indefinite).
    async fn on_signal_awaited(
        &self,
        _run: RunId,
        _node: &NodeId,
        _deadline: Option<DateTime<Utc>>,
    ) {
    }
    /// An `AwaitSignal` node completed on its signal: `payload` is the (redacted) node
    /// output. Fires on the first drive that honours it, never on a replay.
    async fn on_signal_received(&self, _run: RunId, _node: &NodeId, _payload: &serde_json::Value) {}
    /// A `HumanGate` began asking (its `GateAwaited` was just journaled), with the
    /// deadline and the menu the human is shown — option names redacted (see above).
    async fn on_gate_awaited(
        &self,
        _run: RunId,
        _node: &NodeId,
        _deadline: Option<DateTime<Utc>>,
        _options: &[GateOption],
    ) {
    }
    /// A `HumanGate`'s decision was honoured: `option` is in the published menu. Fires for
    /// a `Complete` option and for a `Fail` one (before that node's `on_node_failed`).
    /// `actor` is attribution, not authentication.
    async fn on_gate_decided(
        &self,
        _run: RunId,
        _node: &NodeId,
        _option: &str,
        _actor: &str,
        _note: Option<&str>,
    ) {
    }
    /// A human-backed `Agent` node began asking (its `AgentAwaited` was just journaled),
    /// with the deadline and the journaled (redacted, bounded) question.
    async fn on_agent_awaited(
        &self,
        _run: RunId,
        _node: &NodeId,
        _deadline: Option<DateTime<Utc>>,
        _prompt: &str,
    ) {
    }
    /// A human-backed `Agent` node completed on its answer (`text`/`actor` redacted, as in
    /// the node output).
    async fn on_agent_answered(&self, _run: RunId, _node: &NodeId, _text: &str, _actor: &str) {}
    /// A `Loop`'s human gate began asking for iteration `node` (`"{loop}/{i}/__gate__"`),
    /// with the deadline, the journaled question and the (redacted) menu.
    async fn on_loop_gate_awaited(
        &self,
        _run: RunId,
        _node: &NodeId,
        _deadline: Option<DateTime<Utc>>,
        _prompt: &str,
        _menu: &[LoopGateOption],
    ) {
    }
    /// A loop gate's decision was honoured — fired by the drive that settles the gate,
    /// immediately before [`on_loop_gate_settled`](Self::on_loop_gate_settled).
    async fn on_loop_gate_decided(&self, _run: RunId, _node: &NodeId, _option: &str, _actor: &str) {
    }
    /// The executor journaled `LoopGateSettled` for this gate: `option` is the name the
    /// loop acted on (another iteration, or convergence).
    async fn on_loop_gate_settled(&self, _run: RunId, _node: &NodeId, _option: &str) {}

    // ------------------------------------------------------------------------------
    // AG-2 × AG-15: confirm-before-run tool calls and escalated questions — the same
    // contract as the hooks above, one human-in-the-loop occurrence per firing.
    //
    // `on_tool_confirm_awaited` and `on_agent_escalated` mirror executor writes
    // (`ToolConfirmAwaited`, `AgentEscalated`) and fire from inside the executor's append,
    // like every awaited hook: the ask is journaled once per CALL (first-wins, keyed by the
    // call's effect id) and an escalation once per hop (the executor never re-escalates to
    // an agent already in the chain), so a resume that re-pauses fires neither.
    //
    // `on_tool_confirm_decided` is a decided hook: `ToolConfirmDecided` is appended by
    // another process, so it fires on the drive that FIRST HONOURS the decision — the one
    // that runs the approved call or refuses the rejected one — exactly as
    // `on_gate_decided` does, with the same `DecisionHookFired` bookkeeping row, carrying
    // the call's `effect_id` because one agent node can ask about several calls. A
    // memoized call is never re-judged, but a call honoured and then NOT recorded (the
    // approved tool failed, and the node re-attempts on resume; a stale Observation
    // re-read) is judged again — and reports nothing while the decision is the one, or
    // repeats the content of the one, already reported. A decision the deadline beat is
    // never honoured (the call is refused as expired) and fires nothing.
    //
    // Strings are redacted as above: `arguments` is redacted before it is journaled, so
    // the awaited hook receives the journaled row; the tool name, the actor, the note and
    // the agent names pass through the same `redact_text` at dispatch.
    // ------------------------------------------------------------------------------

    /// An agent's call of a confirm-before-run tool began waiting for a human (its
    /// `ToolConfirmAwaited` was just journaled): `effect_id` names the CALL an operator
    /// answers, `arguments` is the redacted text the human approves, and `deadline` the
    /// absolute SLA recorded (`None` = indefinite).
    async fn on_tool_confirm_awaited(
        &self,
        _run: RunId,
        _node: &NodeId,
        _effect_id: &EffectId,
        _tool: &str,
        _arguments: &str,
        _deadline: Option<DateTime<Utc>>,
    ) {
    }
    /// A confirm-before-run call's decision was honoured: `approved` ran the tool, a
    /// rejection refused it to the model. Fires before the tool runs (so before that
    /// call's `on_agent_tool_call`). `actor` is attribution, not authentication; `actor`
    /// and `note` are redacted.
    async fn on_tool_confirm_decided(
        &self,
        _run: RunId,
        _node: &NodeId,
        _effect_id: &EffectId,
        _approved: bool,
        _actor: &str,
        _note: Option<&str>,
    ) {
    }
    /// A human-backed `Agent` node's question was escalated (its `AgentEscalated` was just
    /// journaled): `from` let its SLA lapse, `to` now holds the SAME question until
    /// `deadline`. The answer still arrives as `on_agent_answered`.
    async fn on_agent_escalated(
        &self,
        _run: RunId,
        _node: &NodeId,
        _from: &str,
        _to: &str,
        _deadline: Option<DateTime<Utc>>,
    ) {
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct Spy(Arc<Mutex<Vec<String>>>);
    #[async_trait::async_trait]
    impl OrchestratorHooks for Spy {
        async fn on_node_started(&self, _run: RunId, node: &NodeId) {
            self.0.lock().unwrap().push(format!("started({})", node.0));
        }
    }

    /// A default (no-op) impl and a partial override both compile and behave: the
    /// override records, defaulted methods do nothing.
    #[tokio::test]
    async fn default_is_noop_and_override_records() {
        struct NoOp;
        impl OrchestratorHooks for NoOp {}
        let run = RunId(uuid::Uuid::new_v4());
        NoOp.on_run_started(run).await; // no panic, returns ()

        let log = Arc::new(Mutex::new(Vec::new()));
        let spy = Spy(log.clone());
        spy.on_node_started(run, &NodeId("n1".into())).await;
        spy.on_run_started(run).await; // defaulted no-op → not recorded
        assert_eq!(*log.lock().unwrap(), vec!["started(n1)".to_string()]);
    }
}
