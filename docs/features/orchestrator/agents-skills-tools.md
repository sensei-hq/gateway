---
title: Agents · Skills · Tools
doctype: feature
module: orchestrator
status: planned
phase: 3
spec: SP-1, SP-2
source: orchestrator (new)
---

# Agents · Skills · Tools

> **Status: Planned (Phase 3 · SP-1/2).** Design §6/§9.

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

## Notes

- Tool execution + permissions are the orchestrator's job — the gateway only returns `tool_calls` (see [inference/tool-calling](../inference/tool-calling.md)).
