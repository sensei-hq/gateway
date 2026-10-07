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
> `on_loop_gate_awaited`/`decided`/`settled`; see [below](#human-in-the-loop-hooks-ag-2)).
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
the honouring drive journals **`JournalEvent::DecisionHookFired { node }`** before firing,
and every later drive, folding it, fires nothing. That row is:

- written **only when hooks are wired** — an executor with no hooks journals exactly what it
  did before (pinned by `an_unhooked_run_journals_exactly_what_it_did_before`);
- **bookkeeping, not an audit fact** — nothing but the hook dispatch reads it, and its
  absence does not mean an answer was never honoured;
- **best-effort** — a failed write skips that drive's hook (a later drive retries) and never
  fails the node; it is written once, not on every replay.

A decision that is **never honoured fires nothing**: one the deadline beat (the gate fails
first; `on_node_failed` fires), one naming an option outside the published menu, one
overwritten by a correction before any drive read it (only the honoured decision is
reported), or one appended after the node already completed or failed. The early-signal
race asks nobody: a signal folded before its `AwaitSignal` first ran fires
`on_signal_received` with no `on_signal_awaited`. `on_gate_decided` for a `Fail` option fires
**before** that node's `on_node_failed`. Decided-hook strings pass through the executor's
redactor (the node output's scrub); awaited hooks receive exactly what was journaled, which
was redacted before the append.

**Two edges are not exactly-once**, both outside the hooks' control: a crash between the
durable marker (`DecisionHookFired`/`LoopGateSettled`) and the callback **loses** the
callback; and a decision honoured by a drive with **no** hooks wired leaves no marker, so a
later hooked drive that replays the completed node reports it then. A deployment that wires
hooks on every drive never sees the second.

Guarded by `crates/orchestrator/src/executor/tests/hitl_hooks.rs` — one resume test per kind,
each driving fresh executors over one journal with the decision appended between drives.

```gherkin
  Scenario: A human decision is reported once, not on every resume
    Given a HumanGate and an unanswered AwaitSignal sibling, both paused
    When an operator appends GateDecided and the run is driven three more times
    Then on_gate_decided fires on the first of those drives only
    And an executor without hooks journals no DecisionHookFired row
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
