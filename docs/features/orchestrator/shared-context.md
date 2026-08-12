---
title: Shared Context
doctype: feature
module: orchestrator
status: partial
phase: 3
spec: SP-1
source: crates/orchestrator*
---

# Shared Context

> **Status: Partial (Phase 3 · SP-1).** Design §8; blackboard-wiring design
> [`../../superpowers/specs/2026-08-10-sp1-blackboard-wiring-design.md`](../../superpowers/specs/2026-08-10-sp1-blackboard-wiring-design.md).
> The scoped `ContextStore` blackboard is now **wired into the executor**
> (executor-managed / implicit): a completed node's output publishes to `Run`
> scope under `key = node.id` (journaled `ContextWrite`, entry held as a CAS
> ref), and an `Agent` node's prompt is assembled with its **declared
> dependencies'** outputs read from the blackboard (a `## Context` section).
> Opt-in — no `ContextStore` wired ⇒ every step is a no-op (behavior
> byte-identical). Resume rehydrates the store from folded `ContextWrite`s
> (refs, no blob load) via `ContextStore::insert_ref`.
>
> **Determinism:** reads are **dependency-scoped** (not all-of-`Run`), so a
> resumed run reproduces byte-identical prompts — a dependency's `ContextWrite`
> is journaled before the dependent runs, and the resolved context is an input to
> the turn hash (an edited upstream output halts loud with `DeterminismViolation`
> rather than mixing).
>
> **Deferred:** agent-facing `read_context`/`write_context` tools; active
> summarize/select budgeting (over-budget currently halts loud via
> `PromptOverBudget`, never truncates); `Scope::Node`/`Plan` reads + per-agent
> private scratch; TTL/as-of freshness; unifying `Consolidate`'s `prior_outputs`
> threading onto the blackboard; concurrent-write policies beyond reject.

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
