---
title: Shared Context
doctype: feature
module: orchestrator
status: planned
phase: 3
spec: SP-1
source: orchestrator (new)
---

# Shared Context

> **Status: Planned (Phase 3 · SP-1).** Design §8.

A scoped, durable blackboard (run / plan / node / agent). Reads resolve up the
scope chain; writes are journaled (globally sequenced). Entries hold **refs, not
blobs** (`digest → content-addressed store`). It is the substrate for
cross-role and fallback handoff — whichever model runs sees the accumulated
context.

## Scenarios

```gherkin
Feature: Shared context
  Scenario: A later role reads an earlier role's output
    Given the planner wrote findings to run scope
    When a refiner agent runs
    Then it reads those findings from the blackboard

  Scenario: Reads resolve up the scope chain
    Given a key set at run scope and not at node scope
    Then a node-scope read returns the run-scope value

  Scenario: A read miss is explicit, not a silent empty
    Given a required key is absent
    Then the read returns an explicit miss outcome

  Scenario: Fallback carries context to the next model
    Given model A is unavailable and the gateway falls over to model B
    Then B's assembled prompt still contains the accumulated context
```

## Notes

- Secrets/tokens are never stored in the durable blackboard.
- Large content is content-addressed; the journal fold never deserializes blobs.
