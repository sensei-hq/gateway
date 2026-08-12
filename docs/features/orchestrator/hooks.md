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
> **context** (`on_context_write{scope,key}`). Run/node/context hooks fire from
> inside `Executor::append` (matched on the just-journaled event) — *can't-miss*
> and **replay-suppressed for free**: a resumed completed prefix isn't
> re-appended, so its hooks don't re-fire. Agent hooks fire on the **live** path
> in `drive_agent` (a memoized turn/tool replay skips them). Opt-in: no hooks
> wired ⇒ zero firing and a byte-identical journal (the event is cloned for the
> match only when hooks are wired).
>
> **Deferred:** `on_plan_expanded` (no PlanDelta), `on_agent_stream_chunk` (no
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
