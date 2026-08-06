---
title: Progress Hooks
doctype: feature
module: orchestrator
status: planned
phase: 3
spec: SP-1
source: orchestrator-core (new)
---

# Progress Hooks

> **Status: Planned (Phase 3 · SP-1).** Design §15.

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
