---
title: SP-6 s4 — the human loop gate (a person decides whether the loop continues)
doctype: design-spec
module: orchestrator
slice: SP-6-s4
status: approved
date: 2026-09-02
---

# SP-6 s4 — the human loop gate

## 1. Summary

A **`Loop` whose stop decision is made by a person**, from an enumerated menu, once per iteration.

`GateSpec` gains a third variant, `Human { agent, menu }`. After each iteration the executor
composes a question at the already-reserved path `"{loop}/{i}/__gate__"`, journals it together with
the menu, and pauses. An operator picks an option; the option says whether it stops the loop. A
`stops: true` pick converges the loop, a `stops: false` pick runs another iteration.

s3's design named this case in its own non-goals — *"the Loop-gate case ('a human decides whether
the loop continues') is the most valuable of the four and the obvious next slice; it is out of scope
here because it needs a `LoopGate` contract over `{"text","actor"}` that does not exist."* This
slice does not build that contract. It concludes there should not be one: a pure predicate over a
person's prose is the wrong shape, and §3 records the argument.

## 2. Goals / Non-goals

**Goals**

- `GateSpec::Human { agent: AgentRef, menu: Vec<LoopGateOption> }`, with
  `LoopGateOption { name, stops }`.
- **Static validation in `validate_dag`** — non-empty menu, unique names, at least one `stops: true`.
- Two new journal variants, `LoopGateAwaited { node, deadline, prompt, menu }` and
  `LoopGateDecided { node, option, actor }`. New *variants*, so `FORMAT_VERSION` stays 1.
- Expiry is read **before** the decision, and an expired undecided gate **fails the loop**.
- The question reuses s3's `assemble_prompt_parts` → `HumanQuestion::compose` through an extracted
  shared seam, so the person sees the iteration output as `## Context` under the same bounds.
- `torii run gate decide --node "{loop}/{i}/__gate__" --option <name>`; `run list-paused` renders
  the question and the menu.
- Zero token spend on the gate path, **structurally** — no chain is resolved.
- Additive: a graph with no `GateSpec::Human` is byte-identical.

**Non-goals**

- **Free-text reasoning alongside the pick.** Considered and deferred (§3). It is a second feature:
  the pick drives the loop, but threading the text into the next iteration's refine input touches
  the loop's input plumbing rather than the gate.
- **A human at the other refused `drive_agent` positions** — `MapBody`, `LoopBody`, `Consolidate`,
  planner. Each remains its own unbuilt feature and its `non_top_level_sites` row stands.
- **A human-backed agent inside `GateSpec::Agent`.** Still refused; the refusal message now names
  `GateSpec::Human` as the variant that would work. This is deliberate — see §5.4.
- Authorization (who may decide), N-of-M, non-CLI delivery, a distinct `RunStatus::Rejected` —
  inherited deferrals from s1/s2/s3.
- Asking once and holding the answer for the whole loop (§3, last row).

## 3. The decisions, and why

| Decision | Choice | Why |
|---|---|---|
| What the human is handed | **An enumerated menu** | The existing gate applies a *pure* `stop_when` predicate to the agent's answer. Feeding a person's prose to a substring predicate is a live footgun: `TextContains("stop")` matches *"I don't think we should stop yet"*. A menu makes the decision unambiguous, auditable, and recitable back to the operator on a bad `--option`. s2 already proved the durable-menu machinery. |
| Where the menu lives | **On the graph, in a new `GateSpec::Human`** | The decisive advantage is `validate_dag`: a menu with no stopping option means a loop that provably cannot converge and silently runs to `max_iters`, and only a graph-level menu can be caught **statically**. s3's §5.5 explicitly laments enforcing at runtime "because `validate_dag` cannot see the registry". Putting the menu on the `AgentDefinition` was rejected: a menu is a per-*site* decision wearing a per-*role* costume, it stays invisible to `validate_dag`, and `torii config push` (replace-all) would silently rewrite it — the failure mode s3's loud frontmatter rules exist to prevent. |
| Why an `AgentRef` at all | **The role earns its place twice** | Its `system_prompt` and activated skills frame the question, and its `backed_by: human { timeout }` supplies the SLA — so a role and its deadline still travel together, exactly as s3 established. A bare `question: String` on the variant would mean reimplementing prompt assembly and finding a second home for the timeout. |
| Reusing `stop_when` | **Rejected — no predicate at all** | Under a human backing `stop_when` would be either inert (the anti-pattern s3's loud frontmatter exists to prevent) or applied to a magic option-name vocabulary, where an author writing `TextContains("halt")` against a menu emitting `"stop"` gets a never-converging loop. `LoopGateOption.stops` says the thing directly. |
| The journal shape | **New `LoopGateAwaited`/`LoopGateDecided` + `LoopGateOption`** | `GateOutcome` is `{Complete, Fail}`; a loop gate needs `{Continue, Stop}`, and "continue" has no representation in it at all. Reinterpreting `Complete` as "stop the loop" would put two meanings in a two-variant enum, dependent on which node read it. Widening `GateAwaited.menu` was rejected as a `FORMAT_VERSION` break plus a migration of existing rows, a large bill for a naming convenience. New variants keep `FORMAT_VERSION` at 1 — the additivity s3 already proved. |
| Expiry vs decision | **Expiry read FIRST** (s2's ordering, **inverting s3's**) | s3 reads the answer first because an agent's answer is *work product* — nothing to self-approve. That argument does not transfer. "Continue" here **authorizes another iteration of spend**, which is an approval in the strict sense s2 built its ordering for; honouring a late "continue" would sanction tokens the operator's own SLA said to stop waiting for. §5.5 records this as a deliberate divergence, and it is guarded by a test that reddens if the two are reordered. |
| What expiry does | **Fails the loop** | `fanout.rs:560` already fails the whole `Loop` when a model gate-agent fails, so this needs no new outcome shape. Converging instead — "silence means stop" — would decide the loop's outcome with no human and report **success**: a default-on-timeout, which s2 considered and explicitly rejected. The accepted cost is real and named in §7: an unanswered question destroys a run whose earlier iterations did real work. |
| How often the human is asked | **Once per iteration** | This is the feature, not the s3 bug. s3's review found "a human re-answers every `Loop` iteration" reached *accidentally* through a one-node `Subgraph` wrapper, at a site never designed for it. Here it is authored deliberately at a site whose whole purpose is a per-iteration decision. Asking once and holding the answer for the loop's life was rejected: it cannot express "continue", which is the decision this slice exists for. |

## 4. Types

```rust
/// A `Loop`'s stop decision. `Pure` = the SP-1 pure predicate (no journaling); `Agent` = a
/// gate-agent over the iteration output, then a pure `stop_when` over the agent's answer;
/// `Human` = a person picking from an enumerated menu, once per iteration (SP-6 s4).
pub enum GateSpec {
    Pure(LoopGate),
    Agent { agent: AgentRef, stop_when: LoopGate },
    Human { agent: AgentRef, menu: Vec<LoopGateOption> },
}

/// One choice a [`GateSpec::Human`] offers, and what picking it does to the LOOP.
///
/// Deliberately NOT [`GateOption`]/[`GateOutcome`], whose `{Complete, Fail}` cannot express
/// "continue" — the one decision this variant exists for. The two menus are different
/// vocabularies for different questions and are kept apart on purpose; `graph.rs`'s existing
/// warning that the HITL and loop-stop senses of "gate" are unrelated still holds.
pub struct LoopGateOption {
    /// What the operator types: `torii run gate decide … --option <name>`.
    pub name: String,
    /// `true` converges the loop; `false` runs another iteration (subject to `max_iters`).
    pub stops: bool,
}
```

Journal (both additive variants; `FORMAT_VERSION` stays 1):

```rust
LoopGateAwaited { node: NodeId, deadline: Option<DateTime<Utc>>, prompt: String, menu: Vec<LoopGateOption> },
LoopGateDecided { node: NodeId, option: String, actor: Option<String> },
```

Fold rules, matching the family: **`LoopGateAwaited` first-wins** (the person was asked *this*
question, with *this* menu, by *this* deadline) and **`LoopGateDecided` last-wins** (an operator may
correct a decision before the run resumes). The `LoopGateAwaited` arm also writes the **shared
`deadlines` map** every waiting kind reads (`executor/mod.rs:195`).

## 5. Architecture

### 5.1 Where the arm sits

A third arm in `fanout.rs`'s gate match, beside `GateSpec::Pure` (`:546`) and `GateSpec::Agent`
(`:552`), driving the gate at the same reserved `"{loop}/{i}/__gate__"` path. `RESERVED_GATE_ID`
already exists (`orchestrator-core/src/plan.rs:22`) and `plan.rs` already rejects a planner-authored
node that collides with it, so an untrusted `Expand` planner cannot forge a gate node. That
reservation is a **precondition of this slice** and the plan re-verifies it rather than assuming it.

### 5.2 The order of operations

Mirrors `run_human_gate` (s2), **not** `run_human_agent` (s3):

0. **`gate_precheck_by_id`** — an already-failed node stays failed, and the verdict is **read back,
   never re-derived**. This is s3's unbounded-journal-growth fix and it applies verbatim: a refusal
   here is terminal for the node, but the run it kills journals no `RunCompleted`, so every later
   wake would otherwise re-drive the iteration and append a fresh `NodeFailed` to a dead node.
1. **`wait_or_expire_by_id`** → `WaitState`, **acted on immediately**. This is the s2/s3 divergence
   and the reason this is a single `match` rather than s3's split `let state = …`. Collapsing it
   the other way silently reinstates s3's ordering.
2. **`NotYetAsking`** → bound the authored half against `MAX_HUMAN_TEXT_BYTES` (loud `NodeFailed` —
   a config error, actionable by its author), redact, clamp, append `LoopGateAwaited`, pause.
3. **Decided** → match `option` against the **journaled** menu, then `stops` sets `converged`. A name
   matching **nothing** in the journaled menu is a loud `NodeFailed`, never a default and never a
   silent continue. `torii` refuses such a name at its own boundary (reciting the menu), so this arm
   is reachable only from a journal not written by `torii` — which is exactly why it must fail rather
   than guess. Defaulting either way would be a decision no human made: to stop, or to spend more.
4. **Expired, undecided** → `NodeFailed`, which `fanout.rs:560`'s existing arm turns into a failed
   `Loop`.

### 5.3 The menu is read from the journal, never from the graph

The graph supplies the menu at ask time; every read after that is of the **journaled** copy. This is
s2's rule and the reason is unchanged, in s2's own general form: **nothing binds the graph handed to
a later `Executor::start`** to the one the human was shown. There is no graph fence — the SP-DATA-2
config-version fence covers the registry, not the graph — so an operator's answer must be validated
against the menu it was given, not against whatever the graph says later. `torii` already works this
way (`cmd/gate.rs:262`, "the menu comes from the JOURNAL"), which is why its decide path needs
extending rather than rewriting.

The concrete vector on the shipped `worker serve` path is the **`scheduled_runs.graph` row**, which
`Scheduler::tick` re-drives from and which an operator can edit between drives; a library embedder
passing a different `Graph` to the next `start` is the same hazard with no table involved.
Correction to an earlier draft of this section, which listed three vectors of which two were false:
a **resubmitted `run submit` is not one** (`cmd::run::submit` pre-checks `Scheduler::status`, and
`SchedulerStore::enqueue` is the real guard — `on conflict do nothing` plus a `rows_affected == 0`
error, so a run id is submittable once), and a **runtime `Expand` subgraph is the one path that IS
bound** (`PlanExpanded` journals the subgraph before it is driven and `drive_expand_with` reuses
`fold.expansions` verbatim rather than re-invoking the planner). `Expand` remains a **trust** point —
an untrusted planner can author the menu in the first place, §7 — but it is not a **drift** point.

An option name in the journaled menu that no longer exists in the graph is therefore **honoured**,
and a name added to the graph after the ask is **not** offered. Both follow from the rule; both get
a test.

### 5.4 `GateSpec::Agent` keeps refusing a human backing

`drive_agent`'s `!top_level` arm (`agent.rs:118`) is untouched, and `fanout.rs`'s `GateSpec::Agent`
arm keeps passing the literal `false`. A human-backed role named in `GateSpec::Agent` still fails
loudly — with the message extended to name `GateSpec::Human` as the variant that would work, the
same cross-refusal shape torii uses ("naming the verb that WOULD work").

This is deliberate rather than incidental. Making `GateSpec::Agent` polymorphic over the backing
would reopen exactly what the s3 review closed: legality decided by position, not by caller. The
`non_top_level_sites` table keeps its `"GateSpec::Agent"` row, and the `Subgraph`-wrapper bypass
stays shut, because this slice adds no new path into `drive_agent` at all.

### 5.5 The shared seam

`GateSpec::Human` is **not** routed through `drive_agent` — it has no ReAct loop, no turns and no
`stop_when`, and threading a menu into `drive_agent` would put a parameter there that every model
caller must pass as `None`.

What the two share is extracted instead: a helper that resolves the `AgentRef`, asserts the backing
is `Human` (returning the timeout), runs `assemble_prompt_parts`, and composes a `HumanQuestion`.
`drive_agent`'s human branch and the new arm both call it. That keeps s3's central property — the
human's question is built by the *model path's own* prompt assembly, so the two cannot drift on what
"the agent's prompt" means — without a second prompt builder.

A role named in `GateSpec::Human` that is **model**-backed is a config error and fails loudly. The
mirror of §5.4, and for the same reason: silence would let an author believe a person is in the loop
while the run quietly decides for itself.

### 5.6 Zero token spend is structural

The arm never calls `resolve_chain` and never reaches the gateway, so no `EffectRecorded` is
journaled and no `usage` is folded. This matters more here than at any previous human site: the
decision being made *is* whether to spend more, so a gate that itself cost tokens would be
self-undermining. Asserted by a test, not by inspection.

### 5.7 Determinism and resume

A decided gate replays from `LoopGateDecided` with no re-ask and no gateway call, exactly as the
model gate-agent replays from its memo. The pure part — `stops` → `converged` — is recomputed from
the journaled option name, so a resume reaches the identical decision.

Like every other waiting kind this node journals no `NodeStarted`/`NodeCompleted`, and so carries
the family's known asymmetry: re-`start`ing an already-terminal run reports it in neither `outputs`
nor `completed`. Pre-existing, not new.

## 6. Bounds and safety

- **The authored half** of the question fails loudly over `MAX_HUMAN_TEXT_BYTES` (4096).
- **The `## Context` half** — the iteration's output, i.e. run data no operator can bound at config
  time — is **truncated** per dependency to `MAX_HUMAN_CONTEXT_BYTES` (32 KiB) with a visible
  marker. This is s3's whole-slice fix and it is load-bearing here: a loop gate's context is a model
  iteration's output essentially always, so charging one cap against both would kill the node on
  ordinary data, after the iteration's tokens were already spent.
- **Redaction before the durable write**, through the same chokepoint: the prompt, the failure
  messages, and `actor` (the leak s3's review caught on that exact field). Then clamp, because
  `[REDACTED]` is longer than the shortest span it replaces.
- **Menu option names are author free text.** They are recited back on a bad `--option`, so torii's
  existing `cap_chars` collapse-and-cap applies unchanged.
- **This node kind must never panic.** A panic unwinds through `Scheduler::tick`, which has already
  claimed a batch of runs and taken their leases; the claimed rows stay `waking` and the next worker
  reclaims the stale lease and dies identically. Every failure path is a `NodeFailed`.

## 7. Trust boundary and the accepted cost

**An unanswered gate destroys the run.** Expiry fails the loop, and the earlier iterations' work —
and their tokens — go with it. This is the deliberate fail-closed choice from §3, and it is the
sharpest cost in the slice. Two things bound it: the SLA is the role's own
`backed_by: human { timeout }`, so it is set by the person who owns the role; and `run list-paused`
surfaces the pending question with its deadline, so the run is visible before the deadline fires.

**A loop gate can pause a run many times.** A 10-iteration loop asks 10 questions and pauses 10
times. Each pause is an ordinary `RunPaused { resume_after: deadline }` that the SP-DATA-3 scheduler
wakes normally, so nothing new is required — but the operator-facing cost is real, and an author who
wants one decision for the whole loop does not have that here (§2, non-goals).

**`actor` is attribution, never authentication.** Inherited from s2 verbatim: it must not be
branched on, and nothing in this slice does.

**An untrusted planner can author a `GateSpec::Human`, and `feasible` will accept it.**
Found during Task 1's review, not at design time. `plan::feasible` validates a planner's
`plan.graph` — a full `Graph` — through `validate_dag`, and neither rejects a `Loop` whose
gate is `Human`. So a planner model can splice a node that asks a person a question. That is
**not** a new capability of this slice and not a reason to gate it: a planner could already
splice a `HumanGate` node, and the SP-6 s3 refusal it *cannot* get past — a human-backed
role at an illegal `drive_agent` position — is unchanged here.

What it did change is the cost of getting the temporary state wrong. Between Task 1 and
Task 7 the gate arm is a stub, and had that stub been the `unreachable!` the plan originally
specified, a planner's output could have **panicked the worker** — model-controlled, through
`Scheduler::tick`, stranding the claimed lease and re-killing every subsequent wake. It is a
`fail_loop` instead, so the worst case is a loud run-scoped failure. Recorded here because
"an untrusted planner reaches this" is the right first question for any new node behaviour,
and this slice's answer is yes.

## 8. Acceptance criteria

1. `GateSpec::Human { agent, menu }` and `LoopGateOption { name, stops }` exist; a graph using
   neither serialises byte-identically to today.
2. `validate_dag` **rejects** an empty menu, duplicate option names, and a menu with no
   `stops: true` option — including at every depth it already recurses into: a `Subgraph`
   node's graph and a `Loop`'s `Subgraph` body (block 2c), and a `Branch`'s arm graphs and
   its `default` (block **2d** — not 2c, which never sees a `Branch`). All four are
   asserted, each mutation-checked by disabling that one recursion.
3. After each iteration the gate journals one `LoopGateAwaited` at `"{loop}/{i}/__gate__"`, carrying
   the question, the deadline and the menu, and the run pauses.
4. A `stops: true` decision converges the loop; the `Loop` completes with the last iteration's
   output and `converged` is true.
5. A `stops: false` decision runs another iteration, and iteration `i+1` asks its **own** question at
   its own path.
6. `max_iters` still bounds a loop whose human keeps choosing `stops: false`.
7. The menu is read from the **journal**: mutating the graph's menu between the ask and the decision
   does not change what the answer means.
8. **Expiry is read BEFORE the decision** — a decision arriving after the deadline does not continue
   the loop. This is the test that reddens if the ordering is flipped to s3's.
9. A fired expiry is terminal: a later decision cannot resurrect the node, and re-driving appends no
   second `NodeFailed`.
10. An expired undecided gate **fails the `Loop`**, not just the gate node.
11. **Zero token spend** on the gate path: no `EffectRecorded`, no gateway call, no folded `usage`.
12. A decided gate **resumes from the journal** — no re-ask, no gateway call, identical decision.
13. A human-backed role in `GateSpec::Agent` still fails loudly, and the message names
    `GateSpec::Human`. The `non_top_level_sites` row stands.
14. A model-backed role in `GateSpec::Human` fails loudly.
14b. A journaled decision naming an option **absent from the journaled menu** fails the node loudly —
    it neither continues nor stops the loop.
15. An oversized **authored** prompt fails the node; a verbose **iteration output** truncates the
    question instead of killing it.
16. The journaled prompt and `actor` are redacted.
17. `torii run gate decide --node "{loop}/{i}/__gate__" --option <name>` decides a loop gate; a bad
    name recites the journaled menu; `run signal` and `run agent answer` refuse it, each naming the
    verb that would work.
18. `run list-paused` renders the loop gate's question and menu.
19. **Cross-process**: a loop gate awaited in one process, decided through `torii`, resumes and
    converges in another against a real Postgres.
20. `FORMAT_VERSION` is still 1.

## 9. Deferred / carry-forward

- **Free-text reasoning alongside the pick**, threaded into the next iteration's refine input. The
  richest version of this feature and the natural s5; it touches the loop's input plumbing, which is
  why it is not here.
- **Asking once for the whole loop** — a different gate kind, not a variant of this one.
- **Authorization, N-of-M, non-CLI delivery, `RunStatus::Rejected`** — the standing SP-6 deferrals,
  unchanged and not made worse by this slice.
- **A hook for `LoopGateAwaited`/`LoopGateDecided`.** They will fall through the `OrchestratorHooks`
  catch-all exactly as `SignalAwaited`/`GateAwaited`/`AgentAwaited` do. Not a regression; the same
  deliberate deferral, and the same observation that a HITL pause is what a live flow-tracking UX
  most wants.
- **The human at the other four refused positions** (`MapBody`, `LoopBody`, `Consolidate`, planner).
  Each remains its own feature; the planner one stays the sharpest, since a person would have to
  hand-author a machine-parseable plan graph.
