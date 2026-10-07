---
title: Progress Hooks
doctype: feature
module: orchestrator
status: partial
phase: 3
spec: SP-1
source: crates/orchestrator*
---

# Progress Hooks

> **Status: Partial (Phase 3 · SP-1).** Design §15; hooks-slice design
> [`../../superpowers/specs/2026-08-11-sp1-orchestrator-hooks-design.md`](../../superpowers/specs/2026-08-11-sp1-orchestrator-hooks-design.md).
> The `OrchestratorHooks` trait (`orchestrator-core`, `#[async_trait]`, **every
> method a no-op default**) is wired via `Executor::with_hooks(Arc<dyn
> OrchestratorHooks>)`. Implemented callbacks: **run** (`on_run_started`/
> `completed`/`paused`), **node** (`on_node_started`/`completed`/`failed`/
> `skipped`), **agent** (`on_agent_started{agent,chain}`/`turn`/`tool_call`),
> **context** (`on_context_write{scope,key}`), **planner** (`on_plan_expanded`,
> `on_planner_selected`) and — AG-2 (#86) — **human-in-the-loop** (`on_signal_awaited`/
> `received`, `on_gate_awaited`/`decided`, `on_agent_awaited`/`answered`,
> `on_loop_gate_awaited`/`decided`/`settled`; see [below](#human-in-the-loop-hooks-ag-2)),
> and for AG-15's confirm-before-run calls and escalations `on_tool_confirm_awaited`/`decided`
> and `on_agent_escalated` ([below](#confirm-before-run-and-escalation-hooks-ag-2--ag-15)).
> Run/node/context hooks fire from
> inside `Executor::append` (matched on the just-journaled event) — *can't-miss*
> and **replay-suppressed for free**: a resumed completed prefix isn't
> re-appended, so its hooks don't re-fire. Agent hooks fire on the **live** path
> in `drive_agent` (a memoized turn/tool replay skips them). Opt-in: no hooks
> wired ⇒ zero firing and a byte-identical journal (the event is cloned for the
> match only when hooks are wired).
>
> **Deferred:** `on_agent_stream_chunk` (no
> execute_stream), `on_agent_model_attempt` (needs the gateway attempts trail),
> `usage`/`cost` on completion (budget dormant), `on_run_resumed`,
> `on_agent_tool_result`, `on_node_started{kind}`, fold-time `replay: true` firing
> (+ the `replay` arg), and the **non-silent hook-error channel** (`HookError`
> event / diagnostics) + panic isolation (§11.1.3) — this slice's hooks return
> `()` and are awaited inline (best-effort; a hook must not depend on execution).

`OrchestratorHooks` — best-effort observability callbacks at run / graph / agent
/ context scope, with per-agent lifecycle emphasis. Hooks ≠ journal: hook
failures are isolated but **not silent** (logged/surfaced), never affecting
execution or determinism.

## Scenarios

```gherkin
Feature: Progress hooks
  Scenario: Per-agent lifecycle events fire
    Given an agent runs a ReAct step and a tool call
    Then on_agent_started, on_agent_step, on_agent_tool_call, on_agent_completed fire

  Scenario: The gateway fallover trail is bubbled live
    Given a model call tried fable then succeeded on opus
    Then on_agent_model_attempt reports both attempts

  Scenario: A hook error is isolated but not silent
    Given a hook implementation throws
    Then the run continues and the error is logged/surfaced (not swallowed)

  Scenario: Replay suppresses duplicate progress
    Given a run resumes and folds the journal
    Then hooks fired during the fold carry replay = true (UIs don't double-count)
```

## Human-in-the-loop hooks (AG-2)

Nine no-op-default methods cover the four SP-6 waiting kinds. The contract is **exactly once
per real occurrence, never on a resumed replay**; how each half achieves it differs, because
only one half has an executor write to mirror.

AG-15's human-in-the-loop events have three more under the same contract —
[below](#confirm-before-run-and-escalation-hooks-ag-2--ag-15).

| Hook | Fires when | Why it is exactly-once |
|---|---|---|
| `on_signal_awaited(node, deadline)` / `on_gate_awaited(node, deadline, options)` / `on_agent_awaited(node, deadline, prompt)` / `on_loop_gate_awaited(node, deadline, prompt, menu)` | right after the executor journals the node's ask, from inside `append` | a waiting node journals its ask once in its life (folded first-wins); a resume re-pauses without re-asking. A loop gate asks once per **iteration**, at `"{loop}/{i}/__gate__"` |
| `on_signal_received(node, payload)` / `on_gate_decided(node, option, actor, note)` / `on_agent_answered(node, text, actor)` | on the drive that **first honours** the answer — completes the node on it, or (a `Fail` gate option) fails it | see below: a `DecisionHookFired` bookkeeping row |
| `on_loop_gate_decided(node, option, actor)` then `on_loop_gate_settled(node, option)` | right after the drive that honours the decision journals `LoopGateSettled` | the executor writes `LoopGateSettled` at most once and every later drive reads it back first |

**The decisions are appended by another process** — torii's CLI writes `SignalReceived`/
`GateDecided`/`AgentAnswered`/`LoopGateDecided` straight into the journal — so the executor
only sees one on its next drive, and the row itself cannot say which drive first acted on
it. `AwaitSignal`, `HumanGate` and a human-backed `Agent` journal nothing when they complete
on an answer, and every later drive of a still-live run re-completes them from the fold. So
the honouring drive journals **`JournalEvent::DecisionHookFired { node, decision }`** before
firing — `decision` is the `Seq` of the decision row it reported — and every later drive,
folding it, fires nothing while the node still holds that decision — that row, or a later
row with **identical content** (a retrying webhook or a double-submitted option decides
nothing new, so it reports nothing). That row is:

- written **only when hooks are wired** — an executor with no hooks journals exactly what it
  did before (pinned by `an_unhooked_run_journals_exactly_what_it_did_before`);
- **bookkeeping, not an audit fact** — nothing but the hook dispatch reads it, and its
  absence does not mean an answer was never honoured;
- **best-effort** — a failed write never fails the node, and the drive fires the hook anyway
  (skipping it would lose the callback whenever that drive also finished the run); it is
  written once, not on every replay.

A decision that is **never honoured fires nothing**: one the deadline beat (the gate fails
first; `on_node_failed` fires), one naming an option outside the published menu, one
overwritten by a correction before any drive read it (only the honoured decision is
reported), one appended after the node already failed, or one appended after the run
finished. A correction appended after a **completed** `AwaitSignal`/`HumanGate`/human `Agent`
while the run is still live **is** honoured — those kinds journal no durable completion and
the decision rows fold LAST-wins, so the next drive re-completes the node on it (a `Fail`
option then fails a gate that had completed) — and that drive reports it, because its
decision row says something other than the row the marker recorded. The comparison is
against the decision **last reported** and covers the whole row the hook reports (a gate's
actor and note, an answer's actor): a correction reverted before any drive honoured it
reports nothing, one honoured and then reverted reports each change, and the same option
resubmitted by a different actor is reported. The early-signal
race asks nobody: a signal folded before its `AwaitSignal` first ran fires
`on_signal_received` with no `on_signal_awaited`. `on_gate_decided` for a `Fail` option fires
**before** that node's `on_node_failed`. Decided-hook strings pass through the executor's
redactor (the node output's scrub). The agent and loop-gate asks are redacted before they
are journaled, so their awaited hooks receive exactly the journaled row; a `HumanGate`
journals its graph-authored menu as-is (it is what a decision is validated against), so
`on_gate_awaited` receives the option names **redacted at dispatch** — the same names
`on_gate_decided` reports, which may differ from the journaled `GateAwaited.options`.

**Three edges are not exactly-once**, all outside the hooks' control: a crash between the
durable marker (`DecisionHookFired`/`LoopGateSettled`) and the callback **loses** the
callback; a decision honoured by a drive with **no** hooks wired leaves no marker, so a
later hooked drive that replays the completed node reports it then (a deployment that wires
hooks on every drive never sees this one); and a **failed** `DecisionHookFired` write leaves
no marker either, so the honouring drive fires anyway and a later drive of a still-live run
reports the same decision **again**. An identical **redelivery** of a reported decision is
not a fourth edge: it fires nothing.

Guarded by `crates/orchestrator/src/executor/tests/hitl_hooks.rs` — one resume test per kind,
each driving fresh executors over one journal with the decision appended between drives.

```gherkin
  Scenario: A human decision is reported once, not on every resume
    Given a HumanGate and an unanswered AwaitSignal sibling, both paused
    When an operator appends GateDecided and the run is driven three more times
    Then on_gate_decided fires on the first of those drives only
    And an executor without hooks journals no DecisionHookFired row
```

### Confirm-before-run and escalation hooks (AG-2 × AG-15)

AG-15's confirm-before-run tool calls and escalated questions fire three more no-op-default
hooks, under the contract above.

| Hook | Fires when | Why it is exactly-once |
|---|---|---|
| `on_tool_confirm_awaited(node, effect_id, tool, arguments, deadline)` | right after `ToolConfirmAwaited` is journaled, from inside `append` | the ask is journaled once per **call** (first-wins, keyed by the call's `effect_id`); a resume re-pauses without re-asking. One node can ask about several calls, one at a time — each fires |
| `on_tool_confirm_decided(node, effect_id, approved, actor, note)` | on the drive that **first honours** the decision — inside its deadline, just before the approved call runs (so before its `on_agent_tool_call`) or the rejected one is refused | a `DecisionHookFired { node, decision, effect_id }` row, **keyed per call** |
| `on_agent_escalated(node, from, to, deadline)` | right after `AgentEscalated` is journaled, from inside `append` | the executor journals a hop at most once per target per node; the escalated question's answer is the usual `on_agent_answered` |

A confirmed call is memoized once it runs or is refused, so a later drive never re-judges it
— **except** a call honoured and then left unrecorded: an approved Pure/Observation tool that
**failed** (the node re-attempts on resume and reads the same decision) or a stale
Observation re-read. That is what the per-call marker is for, and the rules are AG-2's: the
re-attempt reports nothing while the call still holds the reported decision or one with
identical content (an identical **redelivery** fires nothing), and a **correction** honoured
by the re-attempt is reported. The marker is keyed by `effect_id`, not by node, because two
calls on one node can be decided with identical content — a node-keyed dedupe would take the
second for a redelivery of the first. (An approved Mutation journals its `EffectIntent`
first, and an in-doubt call reconciles without returning to the confirmation.)

A decision the **deadline beat** is never honoured — the call is refused as expired — and
fires nothing; so does a call whose redacted arguments are too large to ask about (it is
refused without an ask). `arguments` is redacted before the ask is journaled, so the awaited
hook receives the journaled row; the tool name, the decision's actor and note, and the
escalation's agent names go through the same `redact_text` at dispatch. Only when hooks are
wired is the marker written: an unhooked run journals exactly what it did before. The three
non-exactly-once edges above apply unchanged.

Guarded by `crates/orchestrator/src/executor/tests/confirm_hooks.rs`.

```gherkin
  Scenario: A tool confirmation is reported once, even when the call is re-attempted
    Given an approved confirm-before-run call whose tool failed after the decision was reported
    When the decision is redelivered with identical content and the node re-attempts the call
    Then the call runs and on_tool_confirm_decided does not fire again
    And two calls on one node decided identically each fire their own decided hook
```

## Notes

- Anything execution depends on (e.g. a HITL notification) is a durable outbox effect, not a best-effort hook.
- **Ordering:** for an `Agent` node, `on_agent_started` fires at the start of the
  ReAct loop, *before* the node's `on_node_started` (which fires when the first
  live turn journals `NodeStarted`). A UI keying agent detail under a node row
  must tolerate the agent event arriving first.
- **`on_node_started` is at-least-once** for a node that crashed mid-execution: a
  `ModelCall` node that journaled `NodeStarted` but not its `EffectRecorded`
  genuinely re-executes on resume and re-fires `on_node_started`. Replay-
  suppression covers the *completed* prefix (a fully-recorded node/turn), not a
  node that never finished. Consumers should treat node-start as at-least-once.
