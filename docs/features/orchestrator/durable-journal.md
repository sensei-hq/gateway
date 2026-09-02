---
title: Durable Journal
doctype: feature
module: orchestrator
status: partial
phase: 3
spec: SP-1, SP-DATA-1, SP-DATA-5, SP-6-3
source: orchestrator-core · orchestrator-store
---

# Durable Journal

> **Status: Partial (Phase 3 · SP-1 · SP-DATA-1 · SP-DATA-5 · SP-6-3).** Design §7.
> This header said "Planned (SP-1)" long after the Postgres backend, the spend ledger
> and all three HITL waiting kinds had shipped; the [module README](README.md) row was
> the only place that stayed current.

A step-journal that makes a run resumable. Every nondeterministic/expensive op
is an **effect** classed by idempotency — **pure** (memoize forever),
**observation** (memoize + TTL + provenance), **mutation** (two-phase +
idempotency key + reconcile). Resume folds the journal, memoizes completed
effects (no token re-spend), and continues from the first incomplete node.

## Scenarios

```gherkin
Feature: Durable journal
  Scenario: Resume does not re-spend tokens on a pure effect
    Given a run whose first model call (pure) is journaled as completed
    When the process crashes and resumes
    Then the model call is memoized (not re-issued) and its output is reused

  Scenario: A mutation crashed between intent and record is reconciled
    Given a mutation effect with an EffectIntent but no EffectRecorded
    When the run resumes
    Then it neither blindly re-runs nor memoizes; it runs the reconcile path

  Scenario: Loop iterations get distinct effect ids
    Given a loop body re-entered for iteration 2
    Then iteration 2's effects do not memoize iteration 1's recorded outputs

  Scenario: Input-hash divergence halts loudly
    Given config changed so an effect's recomputed input hash differs
    Then resume halts with a determinism-violation (no silent memoize)

  Scenario: Quota exhaustion pauses with a wake-up time
    Given the gateway returns terminal quota with resume_after = T
    Then the run records a durable pause and resumes at T

  Scenario: A human-backed agent is asked exactly once (SP-6 s3)
    Given a human-backed Agent node that has journaled AgentAwaited
    When the run is resumed before anyone answers
    Then the folded prompt is the FIRST one recorded and the human is not re-asked

  Scenario: A human corrects an answer before the run resumes (SP-6 s3)
    Given two AgentAnswered events for the same node
    Then the LAST one is folded as the node's output
```

## SP-6 s3 — the human-answer events

Two variants extend the journal's HITL vocabulary, after s1's
`SignalAwaited`/`SignalReceived` and s2's `GateAwaited`/`GateDecided`. Not the last pair: SP-6
s4 adds `LoopGateAwaited`/`LoopGateDecided` for the human loop gate, documented here when that
slice lands.

- **`AgentAwaited { node, deadline, prompt }`** — the durable record of which node is
  asking, what it asked, and by when. It exists because `RunPaused` is not node-keyed and
  a run pauses for many unrelated reasons over its life. Folded **FIRST-wins**, exactly as
  `SignalAwaited`/`GateAwaited` are: overwriting the deadline is the never-expires bug s1
  documents, and the human was asked *this* question. It also writes the `deadlines` map
  shared by all **four** waiting kinds (s4's `LoopGateAwaited` is the fourth), so "has this
  node begun asking?" has one answer — while `prompt`, which lands in the `agent_prompts`
  map only this variant writes, answers the narrower "did the *human-backed agent* kind
  begin here?". `LoopGateAwaited` carries a prompt too, but into its own `loop_gate_asks`:
  a loop gate is not answerable by `AgentAnswered`, so the two questions must not share a
  slot.
- **`AgentAnswered { node, text, actor }`** — folded **LAST-wins**, so an operator can
  correct a mistaken answer before the run resumes. `text` becomes the node's output under
  the same `"text"` key a model-backed `Agent` produces, so an unmodified
  `BranchCond::TextContains` or `LoopGate::TextContains` consumes a human's answer without
  knowing it was human. `actor` is **attribution, not authentication** — whatever string
  the caller supplied — and it rides in the node's OUTPUT (`{"text","actor"}`), not merely
  in the audit trail, which is why the terminal-resume re-projection
  (`project_agent_outputs`) passes an output carrying an `actor` through untouched instead
  of rewriting it to `{model, text}`.

Both are **new variants of an existing enum**, so `FORMAT_VERSION` stays **1** — the same
additivity discipline s1, s2 and the spend ledger used.

`AgentAwaited.prompt` is **redacted before the durable write**: `Executor::run_human_agent`
runs the executor's own `Redactor` over the *whole* composed question and appends that
value, then clamps it. That matters because the question is composed from the agent's
`system_prompt` and its activated skill bodies, and nothing upstream scrubs those —
`torii config push` redacts nothing. `render::redact_question` is a **second, display-only**
pass on top (`torii run list-paused`, and the `--json` sink that serialises the awaiting row
wholesale).

The honest residue: `Executor::with_redactor` is **opt-in and defaults to `None`**, so a
library embedder that wires no redactor still writes the question as composed. `torii`'s
heavy tier wires `PatternRedactor` unconditionally, so the CLI path is covered.

## Notes

- Journal (correctness) is strict; hooks (observability) are best-effort — see [hooks](hooks.md).
- Large payloads live in a content-addressed store, not the control-flow log (design §7.4).
