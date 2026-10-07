---
title: Durable Journal
doctype: feature
module: orchestrator
status: partial
phase: 3
spec: SP-1, SP-DATA-1, SP-DATA-5, SP-6-4, SP-7b, AG-12, AG-15
source: orchestrator-core · orchestrator-store
---

# Durable Journal

> **Status: Partial (Phase 3 · SP-1 · SP-DATA-1 · SP-DATA-5 · SP-6-4 · SP-7b · AG-12 · AG-15).** Design §7.
> This header said "Planned (SP-1)" long after the Postgres backend, the spend ledger
> and the HITL waiting kinds had shipped; the [module README](README.md) row was
> the only place that stayed current. It then said SP-6-3 through the whole of s4, while
> promising an s4 section this page did not have — see below.

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
`SignalAwaited`/`SignalReceived` and s2's `GateAwaited`/`GateDecided`. Not the last pair — s4's
three are the section below. (This paragraph promised that section "when that slice lands", named
only two of the three variants, and stayed put after the slice landed; the missing one,
`LoopGateSettled`, is the fix for the Critical s4 found mid-build.)

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

## SP-6 s4 — the loop-gate events

Three variants for the human LOOP GATE — a `Loop` whose stop decision a person makes, once
per iteration, at the synthesized path `"{loop}/{i}/__gate__"`. That path exists in no
graph, which is why the journal is the *only* record that anything is waiting there.

- **`LoopGateAwaited { node, deadline, prompt, menu }`** — folded **FIRST-wins**, for both
  of the reasons its siblings are: overwriting the deadline is s1's never-expires bug, and
  the person was shown *this* question with *this* menu. It is the **fourth** writer of the
  shared `deadlines` map, and the `prompt` goes into its own `loop_gate_asks` slot rather
  than `agent_prompts` — a loop gate is not answerable by `AgentAnswered`, so the two
  questions must not share a slot. **`menu` is the durable vocabulary**, not display text:
  after the first ask every decision is resolved against the JOURNALED copy
  (`published.iter().find(|o| o.name == decision.option)`), because nothing binds the graph
  handed to a later `Executor::start` to the one the human saw — `scheduled_runs.graph` is
  an editable jsonb row. **Both `prompt` and `menu` are redacted before the append**, and a
  menu whose option names COLLIDE once redacted fails the gate loudly rather than offering
  two options under one name (the executor's `Redactor` is an injection, so `validate_dag`
  — pure over the graph — structurally cannot make that check).
- **`LoopGateDecided { node, option, actor }`** — folded **LAST-wins**, so an operator can
  correct a decision *before the run resumes*. `actor` is a required `String` and is
  **attribution, never authentication**; unlike `GateDecided.actor` it is **redacted by the
  appending writer** (`torii run gate decide`, `Measured::AfterRedaction`), because the
  executor reads only `option` off this event and so has no sink of its own to scrub.
  There is no `note` field, which is why the CLI *refuses* `--note` on this kind rather
  than dropping it.
- **`LoopGateSettled { node, option }`** — folded **FIRST-wins**, written by the drive that
  HONOURS a decision. It is the success mirror of reading a `NodeFailed` back instead of
  re-deriving it, and it exists because of a Critical this slice shipped and caught:
  `run_loop` re-enters `for i in 0..max_iters` from zero on every drive, so iteration 0's
  gate was re-derived forever against a deadline fixed at the ask — and with expiry read
  first, the moment wall-clock passed it an already-honoured gate reported `Expired` and
  killed the whole `Loop`, destroying loops that had converged hours earlier. Every later
  drive replays the settlement instead, **above the clock and above the SLA read**. It is
  also what BOUNDS `LoopGateDecided`'s LAST-wins rule: a correction reaches a gate right up
  to the drive that acts on it and not after, so a loop's convergence point cannot be moved
  retroactively under work already paid for.

All three are **new variants of an existing enum**, so `FORMAT_VERSION` stays **1**.

## AG-2 — `DecisionHookFired { node, decision, effect_id }`

Hooks bookkeeping, not an audit fact. Written **only when `OrchestratorHooks` are wired**, by
the drive that first honours an `AwaitSignal`/`HumanGate`/human-`Agent` answer, just before
it fires that node's decided hook. `decision` (`#[serde(default)]`) is the `Seq` of the
decision row it reported; folded LAST-wins per node, so every later drive — which
re-completes the node from the fold — fires nothing while the fold still holds that row or
a later row with identical content (a redelivery), and a correction honoured after it (a
row that says something else) is reported once more. Nothing else reads it: execution, the memo, outputs
and determinism checks are blind to it, and an unhooked executor never writes one (so its
absence does NOT mean an answer was never honoured). A loop gate needs none —
`LoopGateSettled` already marks its honouring drive. Additive: `FORMAT_VERSION` stays **1**.
See [hooks](hooks.md#human-in-the-loop-hooks-ag-2).

**`effect_id`** (AG-2 × AG-15, `#[serde(default, skip_serializing_if = "Option::is_none")]`):
`Some` marks one confirm-before-run CALL's `on_tool_confirm_decided`, with `decision` the
`Seq` of that call's `ToolConfirmDecided`, folded LAST-wins per call — not per node, since one
node asks about several calls. It matters only for a call honoured and then left unrecorded
(an approved tool that failed, re-attempted on resume): the same redelivery/correction rule
applies. A node marker serializes exactly as before (the field is omitted), and reusing the
variant leaves the journal's set of event kinds unchanged; a reader predating the field sees
a node marker on an agent node that has no decided hook of its own, which is inert. See
[hooks](hooks.md#confirm-before-run-and-escalation-hooks-ag-2--ag-15).

```gherkin
Feature: The human loop gate's journal (SP-6 s4)
  Scenario: A settled gate replays instead of re-expiring
    Given a loop gate whose decision an earlier drive honoured and journaled as LoopGateSettled
    When a later drive re-derives that iteration after the gate's deadline has passed
    Then the settlement is replayed and the Loop is not failed

  Scenario: A correction cannot move a convergence point already spent against
    Given a LoopGateSettled for the option a drive honoured
    When a later LoopGateDecided names a different option
    Then the next drive still converges where it did, and asks nobody again

  Scenario: The menu is read from the journal, not the graph
    Given a gate that journaled its menu and then the graph's menu is edited
    Then the decision is resolved against the JOURNALED menu

  Scenario: An option nobody was offered fails the gate loudly
    Given a LoopGateDecided naming an option absent from the journaled menu
    Then the gate fails the Loop — it neither continues nor stops
```

## SP-7b — the context-budget event

- **`ContextBudgeted { node, effect_id, budget_bytes, source_window, retained_bytes,
  dropped_deps, dropped_tools }`** — folded **FIRST-wins**, keyed by `EffectId`. One row per
  budgeted agent NODE, written BEFORE the model call.

**`budget_bytes` is the load-bearing field, and journaling the BUDGET rather than the CUT is
the whole design.** The truncator is pure and every other input to the cut is already
replay-stable — dependency context comes from CAS by digest, the authored half and the tool
activation from the pinned registry — so this integer was the only unfenced one, because it
derives from a model's `context_window` and `GatewayConfig` carries **no version field at
all**, which puts an operator's config edit outside the config fence entirely. Journaling it
makes the cut a pure function of journaled state on the FIRST drive as much as on a resume.
Journaling the cut itself would have to be rich enough to RECONSTRUCT bytes rather than verify
them, landing dependency text inline in the event.

It is **mandatory rather than defensive**: `drive` builds a fresh `DriveState::default()` and
`ready_nodes` never consults the fold, so every past turn's `agent_input_hash` is recomputed on
every partial resume, forever. A drifted budget means a drifted hash, a `DeterminismViolation`,
and a run left terminally `Failed` where `force_wake` — which matches only `status = 'paused'`
— cannot revive it. The **un-budgeted** turn needs the same fence and cannot get it from this
row's presence, since an in-window turn journals nothing: `memo.contains(eid) &&
!budgets.contains(eid)` is what proves turn 0 went out UN-cut and must be reproduced that way.

FIRST-wins is `entry().or_insert()`, and the hazard is proximity: the two nearest templates in
`fold_journal` — `expansions` and `selections` — are both LAST-wins `insert`, so the correct
discipline is one token away from the code most likely to be copied. A budget a later record
could move is not a fence. The remaining fields are **disclosure, not inputs**: `torii` and an
audit read them to learn a turn answered on a degraded prompt, and nothing reconstructs the cut
from them.

A new variant of an existing enum, so `FORMAT_VERSION` stays **1**.

```gherkin
Feature: The context budget's journal (SP-7b)
  Scenario: A budgeted turn replays after the window changes underneath it
    Given a turn budgeted and journaled against a 128k window
    When the gateway config is swapped for one with a different window and the run resumes
    Then the cut is reproduced from budget_bytes, the memo matches, and nothing is re-spent

  Scenario: An un-budgeted turn replays after the window shrinks under it
    Given a turn dispatched in-window, so no ContextBudgeted row exists
    When the window shrinks and the run resumes
    Then the memo without a budget row proves it went out un-cut, and it is joined un-cut again

  Scenario: The first budget wins
    Given two ContextBudgeted records for one effect id
    Then the fold keeps the FIRST — a later record cannot move a cut already hashed against
```

## AG-12 — the money cap

A run's budget can be denominated in **money** as well as tokens — either, both or neither
(`orchestrator_core::RunBudget`, submitted through `Executor::run_with_budget` /
`Scheduler::submit_with_budget`). torii derives the dollar figure from its
individual/group/org caps (torii#41); the engine only enforces it. Everything rides the
SP-DATA-5 ledger rather than beside it:

- **`RunStarted.money_budget: Option<MoneyBudget { total_micro_usd }>`** — the cap, in
  INTEGER micro-dollars (1 USD = 1 000 000). Never `f64` in the journal or the fold: a
  float ledger makes `spent >= cap` depend on the order the fold sums in.
- **`TokenUsage.cost_micro_usd: Option<u64>`** on each `EffectRecorded.usage` (and so on
  each `CompactChild.usage`) — the gateway's `InferenceResponse.actual_cost`, rounded UP to
  whole micro-dollars (representation noise below a nano-dollar is not charged). Keyed by
  effect id with the tokens, so a duplicate `Confirmed` record, a re-folded `MapCompacted`
  or a resume counts it once — the token ledger's idempotency, inherited rather than
  re-argued. **Recorded only while a money cap is in force**, so an unbudgeted or
  token-only run journals byte-identically to before.
- **`MoneyBudgetRaised { new_total_micro_usd }`** — the money twin of `BudgetRaised`: latest
  wins, lowering below spend halts the run. Its own variant because the caps are independent
  (`BudgetRaised.new_total_tokens` is required). It **moves** a money cap and never
  introduces one: on a run that started WITHOUT a money cap the fold ignores it. A drive in
  flight when such a raise lands folded "no cap" and journals every call uncosted, so a cap
  introduced then would be reported over spend the ledger never counted. A run that needs a
  money cap is submitted with one.
- **`Snapshot.spent_micro_usd` / `money_budget_micro_usd`** — the money half of the
  snapshot's ledger scalars, for the reason `spent`/`budget` are there.
- **`money_spend_of(events)`** — the folded `(spent_micro_usd, cap)`, the money twin of
  `spend_of`, for `torii run status`.

Every new field is `#[serde(default)]` and skipped when unset; `MoneyBudgetRaised` is an
additive variant. **`FORMAT_VERSION` stays 1** (pinned byte-for-byte by
`a_run_without_a_money_cap_journals_byte_identically`).

Enforcement is at the SAME metered-dispatch chokepoint as tokens (SP-DATA-5, see the
[overview](../../superpowers/orchestrator-overview.md)): a money cap takes the run's 1-permit
serialisation gate; `money_spent >= cap` pauses (the `budget: ` HOTL pause,
`resume_after: None`); otherwise `max_tokens` is clamped to what the remaining
micro-dollars buy after the per-request fee and the pessimistic input estimate, at the
chain's **worst-case** price (`Gateway::worst_case_pricing`, the componentwise max over
the chain — the clamp is set before selection), and below `MIN_OUTPUT_TOKENS` it pauses
instead — including when the fee and input estimate alone exceed what is left, which a
zero output price would otherwise read as unlimited output. On a money-only run whose
chain's declared `max_output_tokens` is itself under the floor, the pause says
`output limit: ` and names that limit — no money raise can release it. The residual overshoot is the token clamp's, priced:
`(actual_input − est_input) × input_price`, plus under one micro-dollar of rounding.

**Fail closed.** A chain with ANY model lacking `pricing` is refused **before** dispatch
under a money cap (`NodeFailed`, "unpriced model call"): `pricing: None` means *free* to
routing but *unmeasured* to a cap, since the gateway reports no cost for it. A free model
declares an explicit zero price. A response with usage but no USD cost is refused after
the call (`NodeFailed`, "uncosted"); one with no usage at all is the existing
`Unmetered` failure. Known gap: the gateway's `actual_cost` omits a model's
`per_request` fee, so the ledger under-counts it (the clamp reserves it per call).

```gherkin
Feature: The money cap (AG-12)
  Scenario: A run capped at $X pauses before exceeding it and resumes with zero re-spend
    Given a run capped at $0.10 on a chain priced at $0.1/1k in and $0.2/1k out
    When three $0.021 calls have been made
    Then the fourth is refused before dispatch and the run pauses with spent $0.063
    And after a MoneyBudgetRaised to $1 a wake runs only the remaining nodes

  Scenario: An unpriced chain under a money cap
    Given a money-capped run whose chain has a model with no pricing
    Then the node fails closed and the provider is never called

  Scenario: A token-only run on a priced chain
    Then no money field appears anywhere on its journal
```

## AG-15 — tool-confirmation and escalation events

Three additive variants (sensei-hq/gateway#90), so `FORMAT_VERSION` stays **1**; a journal
written before them folds unchanged.

- **`ToolConfirmAwaited { node, effect_id, tool, arguments, args_hash, deadline }`** — a call of
  a confirm-before-run tool (`AgentDefinition::confirm_tools`) has begun waiting for a human.
  Keyed by the CALL's `effect_id` (`effect_id(node, turn, k+1)`), not the node, because one
  agent node can ask about many calls. Appended before the tool does anything (before any
  `EffectIntent`). `arguments` is what the human approves, **redacted** before the append; a
  call whose redacted arguments exceed `MAX_HUMAN_TEXT_BYTES` is refused to the model instead
  of asked (approving truncated arguments is approving something nobody saw). The `deadline`
  is absolute and folded **FIRST-wins**, like every waiting record.
- **`ToolConfirmDecided { node, effect_id, approved, actor, note }`** — the human's answer,
  folded **LAST-wins** (correctable before the run resumes, like `GateDecided`). The deadline
  is checked BEFORE it is read, so a late approval runs nothing. Once an approved Mutation has
  journaled its `EffectIntent` the decision is settled: neither the deadline nor a later
  correction is consulted again, and an intent with no `EffectRecorded` reconciles in doubt. A rejection or an expiry is
  recorded as a Pure `EffectRecorded` carrying a terse `{"error":"not_confirmed"}` — the model
  sees neither `note` nor `actor`. `actor` is attribution, not authentication; who may answer
  is `torii`'s concern (torii#47).
- **`AgentEscalated { node, from, to, deadline }`** — a human-backed `Agent` node's holder
  (`from`) let its SLA expire unanswered and the question now waits on `from`'s `escalate_to`
  agent `to` until `deadline` (`to`'s own SLA from the escalation instant). The question is NOT
  re-journaled: `to` is asked the question `AgentAwaited` already holds, and answers with the
  same node-keyed `AgentAnswered`. Folded FIRST-wins per `to`, so a duplicated hop moves
  nothing; the current deadline is the last hop's.

The per-tool **call ceiling** has no event of its own: its count is derived from the agent's
own transcript, which the memoized turns replay, and a refused call is an ordinary Pure
`EffectRecorded` with `{"error":"call_limit_reached"}`.

```gherkin
Feature: Agent tool policy and escalation in the journal (AG-15)
  Scenario: A confirm-before-run call survives a resume
    Given an agent whose tool requires confirmation calls it
    Then ToolConfirmAwaited is journaled and the run pauses before the tool runs
    When the run is re-driven with no decision
    Then it pauses again without a second ask and with the same deadline
    When ToolConfirmDecided{approved: true} is appended and the run resumes
    Then the tool runs exactly once

  Scenario: An escalation survives a resume
    Given a human-backed agent with escalate_to whose SLA has passed
    Then AgentEscalated is journaled and the run pauses on the target's deadline
    When the run is re-driven inside that deadline
    Then nothing new is journaled and the deadline does not move
```

## Notes

- Journal (correctness) is strict; hooks (observability) are best-effort — see [hooks](hooks.md).
- Large payloads live in a content-addressed store, not the control-flow log (design §7.4).
