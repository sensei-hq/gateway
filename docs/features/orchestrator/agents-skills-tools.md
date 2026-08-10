---
title: Agents · Skills · Tools
doctype: feature
module: orchestrator
status: partial
phase: 3
spec: SP-1, SP-2
source: orchestrator (new)
---

# Agents · Skills · Tools

> **Status: Partial (Phase 3 · SP-1 slice 2).** Design §6/§9.

Externally-configured **agents** (md+frontmatter: name, area, kind, chain(s),
tools, skills, subagents, system-prompt body), **skills** (injectable
instruction modules), and **tools** (executable capabilities with an effect
class + permissions). The agent runtime assembles a budgeted prompt, resolves
the role→chain, calls the gateway, and runs a ReAct/tool loop.

## Scenarios

```gherkin
Feature: Agent runtime
  Scenario: An agent's chain is resolved from its role/kind
    Given a coding-planner agent with chain "plan.frontier"
    Then its model calls route through the plan.frontier chain

  Scenario: Skills are composed into the system prompt
    Given an agent listing skills [clean-code, security-compliance]
    Then those skill modules appear in the assembled system prompt

  Scenario: The runtime executes tool calls the gateway returned
    Given the model returns a tool call for "fs.read"
    Then the orchestrator executes fs.read (the gateway does not) and feeds the result back

  Scenario: Prompt is budgeted to the smallest model in the chain
    Given a chain whose smallest model has a 32k context window
    Then prompt assembly fits within 32k (summarize/select, never silent truncation)
```

## Slice 2 (implemented)

- An in-memory **registry** (`AgentDefinition` / `SkillDef` / `ToolSpec`) with a
  md+frontmatter-subset parser (`from_frontmatter`) and `Registry::validate`
  (dangling agent/skill/tool refs are a loud load-time error).
- **Prompt assembly** (`assemble_prompt`: system-prompt body + each listed
  skill's body, in order) with **per-turn window budgeting** (`over_budget`) —
  halt-loud when a turn's estimate exceeds the chain's smallest context window;
  no silent truncation.
- A **Pure-only tool runtime** (`Tool` / `ToolRegistry` + the demo `calc` tool);
  Observation/Mutation tools are rejected loud (`ToolEffectDeferred`), never
  silently skipped or run early.
- `NodeKind::Agent`, driving a durable **ReAct loop** (`drive_agent`) where each
  turn's model call is a Pure `ModelCall` effect and each Pure tool call is its
  own Pure effect — so resume-without-re-spend (the durable-executor spine)
  extends into the loop, not just the top-level graph.

**Deferred:** Observation/Mutation tools + TTL/two-phase/reconcile (slice 4);
a filesystem directory loader for the registry; a summarize/select budgeting
strategy (today's over-budget turn halts rather than compacting);
blackboard/shared-context, `Map` fan-out, subagents, per-phase chains, and
streaming (slice 3+).

Source: `crates/orchestrator/src/agent/*` + `executor.rs` (`drive_agent`).

## Notes

- Tool execution + permissions are the orchestrator's job — the gateway only returns `tool_calls` (see [inference/tool-calling](../inference/tool-calling.md)).
