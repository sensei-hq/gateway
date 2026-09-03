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
- Expiry is read **before** the decision, and an **expired** gate **fails the loop** — decided or
  not. "Undecided" would describe a check the arm deliberately does not make; see §5.2 step 4.
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

Journal (all additive variants; `FORMAT_VERSION` stays 1):

```rust
LoopGateAwaited { node: NodeId, deadline: Option<DateTime<Utc>>, prompt: String, menu: Vec<LoopGateOption> },
LoopGateDecided { node: NodeId, option: String, actor: String },
LoopGateSettled { node: NodeId, option: String },
```

`LoopGateSettled` is a **third** variant this section did not originally specify. It was added by
the Tasks 6+7 review, which reproduced a Critical defect the design's own §5.7 claim ("a decided
gate replays … a resume reaches the identical decision") turned out to be false about. `run_loop`
re-enters `for i in 0..max_iters` from zero on every drive, so iteration 0's gate is re-derived
forever while the deadline it recorded stays fixed. With expiry read first — §3's decision, and the
right one — the moment wall-clock passed that deadline the *already-honoured* gate reported
`Expired` and the whole `Loop` died: a 3-iteration loop whose operator answered at +30m and +70m
under a 1h SLA, each strictly inside its **own** gate's SLA, failed at `lp/0/__gate__`; and a loop
that had already **converged** was destroyed a day later when a downstream signal woke the run. Any
multi-iteration human-gated loop with a finite SLA was unusable, and §7's "a 10-iteration loop asks
10 questions and pauses 10 times — nothing new is required" was wrong.

The fix is the SUCCESS mirror of what §5.2 step 0 already does for the FAILURE verdict: the drive
that honours a decision journals it, and every later drive reads it back instead of re-deriving it.
Reordering to read the decision first would fix the same symptom and reopen AC8, so the ordering is
**settled → clock → decision** rather than **decision → clock**. `option` is the name that was
honoured, and the replay resolves *that* against the journaled menu — not `LoopGateDecided`, whose
LAST-wins rule exists so an operator can correct a decision *before the run resumes*; a settlement
is exactly the line after which "before" has passed, since the loop has already spent an iteration
on the strength of the answer.

`actor` is a **required `String`**, exactly as `GateDecided.actor` and `AgentAnswered.actor` are.
This section originally specified `Option<String>` and never argued for it, which made it an
unargued inconsistency with the reasoning this same document uses one section earlier: §3's
"Expiry vs decision" row justifies reading expiry *before* the decision on the ground that
answering `continue` **authorizes another iteration of spend**. That is an approval in the strict
sense s2 built its ordering for, and s2 made `GateDecided.actor` required precisely because an
approval always records who claimed to give it. A loop gate's decider is exactly as attributable as
a `HumanGate`'s, so the two fields have the same type. Narrowed out of band — it is no task of the
plan, but a change landed between the plan's Tasks 4 and 5 — while nothing yet wrote the event and
there were therefore no `None` rows to migrate; Task 6 is the first writer.
(`actor` remains **attribution, never authentication** — whatever string the caller supplied, and
nothing branches on it. The remaining degenerate value, `""`, is a writer bug rather than a legal
"anonymous" encoding: `cmd::gate::actor_or_user` resolves an unnameable operator to `unknown`, and
neither the wire format nor the fold re-labels a blank one, so the two stay distinguishable in an
audit.)

Fold rules, matching the family: **`LoopGateAwaited` first-wins** (the person was asked *this*
question, with *this* menu, by *this* deadline) and **`LoopGateDecided` last-wins** (an operator may
correct a decision before the run resumes). The `LoopGateAwaited` arm also writes the **shared
`deadlines` map** every waiting kind reads (`executor/mod.rs:195`).

## 5. Architecture

### 5.1 Where the arm sits

A third arm in `fanout.rs`'s gate match, beside `GateSpec::Pure` (`:546`) and `GateSpec::Agent`
(`:552`), driving the gate at the same reserved `"{loop}/{i}/__gate__"` path. `RESERVED_GATE_ID`
already exists (`orchestrator-core/src/plan.rs:22`).

**This section originally said `plan.rs` "already rejects a planner-authored node that collides with
it, so an untrusted `Expand` planner cannot forge a gate node", and called that reservation a
precondition the plan had re-verified. The claim was false, and the re-verification recorded
"Confirmed."** `plan::feasible`'s reserved-id walk saw `plan.graph.nodes` and did not recurse, on
the reasoning that "nested ids namespace deeper under their parent path and can't collide". That
reasoning holds for `__plan__` and `__select__` — both sit under an `Expand`, which has no static
body — and fails for `__gate__`, because a `Loop`'s `Subgraph` body is namespaced under
`"{loop}/{i}"`. Measured by the whole-slice review:
`feasible(Loop { body: Subgraph([Node{id:"__gate__"}]), gate: Human })` returned `Ok(())`.

The same segment was authorable by hand: s1's `validate_dag` rule bans the `/` SEPARATOR, which
makes the reserved path unwritable in one piece and says nothing about a bare segment. Whether that
produced a loud failure or a silent one depended on the colliding node's KIND — a waiting kind
writes the shared `deadlines` map and trips the gate's missing-menu refusal, while a kind that
COMPLETES leaves the gate to ask normally at an id already carrying `NodeCompleted`, which
`torii`'s `signal_states` folds as terminal. The run then pauses on a question `run list-paused`
omits and `run gate decide` refuses as already completed: forever under the supported indefinite
SLA, and until the whole `Loop` dies under a finite one.

Both are closed now, and by rules that had to be ADDED:

- `Graph::validate_dag` block **1c** rejects a bare `__plan__`/`__gate__`/`__select__` node id (and
  a dep naming one) at every depth it recurses into. It covers an author and an untrusted planner
  in one rule, because `feasible` validates through this same function.
- `plan::feasible`'s reserved-id walk recurses into a `Subgraph` node's graph, a `Loop`'s
  `Subgraph` body and a `Branch`'s arms and `default`, mirroring `check_agent_refs`, so the planner
  still gets the typed `PlanError::ReservedNodeId(id)` rather than only a structural string.
- `run_human_loop_gate`'s `NotYetAsking` arm refuses to publish an ask at a path already carrying
  `NodeStarted`/`NodeCompleted`. This is the case no validator can reach: `GateSpec::Agent` drives
  a real agent at this same path and journals both events there, so an operator editing
  `scheduled_runs.graph` from `Agent` to `Human` between drives lands in it with each graph legal.

### 5.2 The order of operations

Mirrors `run_human_gate` (s2), **not** `run_human_agent` (s3):

0. **`gate_precheck_by_id`** — an already-failed node stays failed, and the verdict is **read back,
   never re-derived**. This is s3's unbounded-journal-growth fix and it applies verbatim: a refusal
   here is terminal for the node, but the run it kills journals no `RunCompleted`, so every later
   wake would otherwise re-drive the iteration and append a fresh `NodeFailed` to a dead node.
0b. **`Fold::loop_gate_settled_with`** — an already-HONOURED gate replays its decision, and the
   clock is not consulted. The success mirror of step 0, added by the Tasks 6+7 review (§4). It sits
   above the SLA read as well as above the clock: a settled gate needs no role, no question and no
   deadline, so a role edit cannot turn a loop nobody is waiting on into a terminal failure.
1. **`wait_or_expire_by_id`** → `WaitState`, **acted on immediately**. This is the s2/s3 divergence
   and the reason this is a single `match` rather than s3's split `let state = …`. Collapsing it
   the other way silently reinstates s3's ordering. Because of step 0b the clock now only ever
   judges a gate that is still LIVE, which is the only reading under which §3's ordering and §5.7's
   resume claim are both true.
2. **`NotYetAsking`** → bound the authored half against `MAX_HUMAN_TEXT_BYTES` (loud `NodeFailed` —
   a config error, actionable by its author), redact, clamp, append `LoopGateAwaited`, pause.
3. **Decided** → match `option` against the **journaled** menu, journal `LoopGateSettled`, then
   `stops` sets `converged`. The settlement is written *before* `run_loop` spends anything on the
   strength of the answer, and *after* the option resolves, so a decision naming an option nobody
   was offered leaves no settlement behind. A name matching **nothing** in the journaled menu is a
   loud `NodeFailed`, never a default and never a
   silent continue. `torii` refuses such a name at its own boundary (reciting the menu), so this arm
   is reachable only from a journal not written by `torii` — which is exactly why it must fail rather
   than guess. Defaulting either way would be a decision no human made: to stop, or to spend more.
4. **Expired** (decided or not) → `NodeFailed`, which `fanout.rs:560`'s existing arm turns into a
   failed `Loop`.

   **The arm does not — and must not — check decidedness here**, and this item said "Expired,
   undecided" until the Tasks 8–9 review caught it. That qualifier IS the s3 ordering §3 exists to
   refuse: the one-line "fix" that makes the code match it,
   `Ok(WaitState::Expired(d)) if fold.loop_gate_decision_for(node_id).is_none()`, is exactly what
   `a_decision_after_the_deadline_does_not_continue_the_loop` reddens on, and a reviewer chasing that
   red while holding the old wording would conclude the test was wrong. The arm has deliberately not
   read the fold at this point and structurally cannot know whether a decision exists — see the code
   comment at `human.rs`'s `Expired` arm, which gets this right and explains why the message names
   the DEADLINE and never "no decision".

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

**The refusal is owed on every drive that could still ask, not only on the drive that asks**, which
is why the arm reads the SLA at step 2 rather than inside the `NotYetAsking` arm that composes the
question. The vector is `torii config push` — replace-all against a live registry — so a role can be
edited from `backed_by: human` to `backed_by: model` while a run sits paused on its gate. On the
re-pause drive the arm composes nothing, so step 2 is the ONLY place the backing is checked; without
it the run re-pauses on a durable question addressed to a role that can no longer answer it, and a
`LoopGateDecided` appended afterwards would be honoured as though a person had picked. Task 9 found
this untested and it is now `a_role_edited_to_model_backed_mid_wait_fails_a_drive_that_does_not_ask`
— the ask-time test cannot reach it, because `human_question_for` re-raises the identical error and
the `NotYetAsking` arm's own `Err` branch catches it, so deleting step 2 leaves that test green.

Deliberately asymmetric with §5.2's step 0b: a gate that has already been **settled** replays ABOVE
the SLA read, so the same config edit cannot retroactively kill a loop nobody is waiting on. Live
gates refuse; settled ones are already answered and are none of the registry's business.

### 5.6 Zero token spend is structural

The arm never calls `resolve_chain` and never reaches the gateway, so no `EffectRecorded` is
journaled and no `usage` is folded. This matters more here than at any previous human site: the
decision being made *is* whether to spend more, so a gate that itself cost tokens would be
self-undermining. Asserted by a test, not by inspection.

### 5.7 Determinism and resume

A decided gate replays from `LoopGateSettled` with no re-ask and no gateway call, exactly as the
model gate-agent replays from its memo. The pure part — `stops` → `converged` — is recomputed from
the journaled option name against the journaled menu, so a resume reaches the identical decision.

**It replays from the SETTLEMENT, not from the decision, and that distinction is the whole of
§4's Critical fix.** A replay that re-derived the gate from `LoopGateDecided` would have to pass
the clock on the way, and the clock has no idea the answer was already honoured — which is how the
version of this section that named `LoopGateDecided` came to be false in practice for every
multi-iteration loop with a finite SLA. It also bounds `LoopGateDecided`'s LAST-wins rule: a
correction reaches a gate right up to the drive that acts on it, and not after, so a loop cannot
have its convergence point moved retroactively under work it has already done.

Like every other waiting kind this node journals no `NodeStarted`/`NodeCompleted`, and so carries
the family's known asymmetry: re-`start`ing an already-terminal run reports it in neither `outputs`
nor `completed`. Pre-existing, not new.

## 6. Bounds and safety

- **The authored half** of the question fails loudly over `MAX_HUMAN_TEXT_BYTES` (4096). For a gate
  that half is the role's `system_prompt`, its activated skill bodies, and the synthesized `## Task`
  ask (below) — all three author-controlled at config time, which is what makes a loud failure the
  right answer.
- **The `## Context` half** — the iteration's output, i.e. run data no operator can bound at config
  time — is **truncated** per dependency to `MAX_HUMAN_CONTEXT_BYTES` (32 KiB) with a visible
  marker. This is s3's whole-slice fix and it is load-bearing here: a loop gate's context is a model
  iteration's output essentially always, so charging one cap against both would kill the node on
  ordinary data, after the iteration's tokens were already spent.
- **Which seam argument each half comes from is therefore part of the contract, not an
  implementation detail.** `human_question_for(agent_ref, input, context)` puts `input` in
  `## Task` and charges it to the loud cap; `context` becomes `## Context` and truncates. So
  `run_human_loop_gate` passes the iteration output as a `context` entry and a short
  menu-derived ask as `input` — the reverse kills the gate on ordinary data. Task 5's review
  found the plan's Task 6 sketch written the reverse way, before it was executed; the rule is now
  on the seam's own doc comment.
- **Redaction before the durable write**, through the same chokepoint: the prompt, the failure
  messages, and `actor` (the leak s3's review caught on that exact field). Then clamp, because
  `[REDACTED]` is longer than the shortest span it replaces.
- **The `actor` half of that rule has a different OWNER from the other two, and Task 10 does not
  discharge it.** The prompt and the failure messages are the executor's: `run_human_loop_gate`
  redacts the question before appending `LoopGateAwaited`, and `fail_loop_gate` is the single
  chokepoint every failure arm routes through. `actor` never passes through the executor at all —
  the arm reads only `option` off `LoopGateDecided`, interpolates the actor into no message and
  puts it in no output, so there is no executor-side sink to scrub and no executor test that can
  assert the property. It is owed entirely by the APPENDING writer, which is Task 12's
  `torii run gate decide`, and it must be **added** there rather than inherited: `cmd::human::answer`
  redacts `--as` through `redact_answer`, while `cmd::gate::decide` deliberately does not (it
  measures the actor `Measured::AsGiven`, on the ground that `GateDecided.actor` goes through no
  redaction). A loop-gate branch bolted onto `decide` inherits that gap silently — which is exactly
  how the s3 leak this bullet cites happened. Recorded on `JournalEvent::LoopGateDecided`'s own doc
  as well, since that is where the next writer of the event looks.
- **Menu option names are author free text**, out of the GRAPH — `torii run submit --graph <file>`,
  which deserializes the file and scrubs nothing. Not `torii config push`: §4 above puts the menu on
  the graph so `validate_dag` can see it and rejects the registry as its home, so a pushed config
  carries no option name at all. They are recited back on a bad `--option`, so torii's existing
  `cap_chars` collapse-and-cap applies unchanged.
- **And they are therefore redacted at the `LoopGateAwaited` append, alongside the prompt.** The
  first shipped site scrubbed only the prompt, which quotes the same names through `gate_ask` — so
  one author string was clean in `prompt` and plaintext in `menu` and in the `RunPaused.reason`
  built from it, on one write. The append is the last line of defence **for the journal**, which is
  the precise claim: it is not the first durable copy of an option name, because `Scheduler::submit`
  calls `store.enqueue` before it drives and that writes the whole submitted graph into
  `scheduled_runs.graph` as jsonb, in the clear. The journal is the copy that is read BACK — folded
  by every later drive, printed by `run status` and `run list-paused`, shown to the person by
  `gate_ask`, and resolved against by a decision — while `scheduled_runs.graph` is the operator's own
  input, read by `claim_due` alone (`ScheduledRun`, what both observe commands return, has no graph
  field). `pause_gate` is handed the scrubbed copy on both its arms and runs the finished reason
  through the redactor as the write chokepoint — forward-looking only, since the menu now arrives
  clean and the other interpolation (the node id) is a structural key already plaintext in
  `NodeStarted` and `EffectRecorded`.
- **A menu that redaction makes ambiguous fails the gate, loudly, before the append.** `menu` is the
  vocabulary a decision is resolved against, not display text. `validate_dag` rejects duplicate
  option names but runs on the authored graph and has no `Redactor` — the redactor is an executor
  injection, so the same graph is legal under one executor and not another — and redaction can
  RE-CREATE the duplicate: two credential-shaped names collapse to one placeholder, `find` takes the
  first, and an operator picking the only name they were offered gets whichever `stops` came first.
  A silently inverted decision is exactly what §5.3 journals the menu to prevent, so it is refused
  on the authored-cap's reasoning: author-controlled config, actionable by the person who wrote it.
- **This node kind must never panic.** A panic unwinds through `Scheduler::tick`, which has already
  claimed a batch of runs and taken their leases; the claimed rows stay `waking` and the next worker
  reclaims the stale lease and dies identically. Every failure path is a `NodeFailed`.

## 7. Trust boundary and the accepted cost

**An unanswered gate destroys the run.** Expiry fails the loop, and the earlier iterations' work —
and their tokens — go with it. This is the deliberate fail-closed choice from §3, and it is the
sharpest cost in the slice. Two things bound it: the SLA is the role's own
`backed_by: human { timeout }`, so it is set by the person who owns the role; and `run list-paused`
surfaces the pending question with its deadline, so the run is visible before the deadline fires.

**And so does an ANSWERED one, if no drive happens between the answer and the deadline.** This
sentence read "an unanswered gate" alone until the Tasks 8–9 review, and neither bound above touches
the case: a decision journaled at t0+59m under a 1h SLA, first driven at t0+1h, fails on the
deadline. Both bounds address an operator who did not answer; this operator did.

The mechanism is structural, not a race. `LoopGateDecided` carries **no timestamp**, so the executor
cannot tell "answered at +59m" from "answered at +61m" and — by §3's ordering — refuses both rather
than honouring a late decision it cannot distinguish from an in-time one. Adding a timestamp to the
event is the obvious alternative and is **rejected**: a decision's time would then be attested by
whoever wrote the row, which makes the SLA enforceable only against honest clients, and the whole
expiry rule exists to fail closed against a journal `torii` did not write.

It is also the *ordinary* path, not a worker-outage path: `pause_awaiting` arms
`RunPaused { resume_after: deadline }` on the deadline instant itself, and `wait_or_expire` expires
at `now >= deadline`, so on a shipped `worker serve` the scheduler's wake IS the expiry drive. Sizing
an SLA from this section therefore has to budget for "will a person answer AND will a drive occur
before the deadline", not just the first half.

**The mitigation belongs at the CLI boundary, and Task 12 owes it — SHIPPED, and inherited rather
than written:** once `signal_states` folds `LoopGateAwaited`, a loop gate reads as
`SignalState::Awaiting { deadline }` and s2's existing arm refuses it, with
`a_loop_gate_decision_exactly_at_the_deadline_is_refused` /
`…_before_the_deadline_is_delivered` pinning both sides on this kind too.
`torii run gate decide` must
refuse to append a `LoopGateDecided` when the journaled `LoopGateAwaited.deadline` has already
passed, so the operator gets a visible refusal instead of a success message followed by a durable row
that kills their run. That is exactly what s2's `cmd::gate::decide` already does for `HumanGate`
(`SignalState::Awaiting { deadline: Some(d) } if now >= d`, with `a_decision_exactly_at_the_gates_
deadline_is_refused` pinning the boundary against the executor's own `>=`), so the loop-gate verb
inherits a proven shape rather than inventing one.

**A loop gate can pause a run many times.** A 10-iteration loop asks 10 questions and pauses 10
times. Each pause is an ordinary `RunPaused { resume_after: deadline }` that the SP-DATA-3 scheduler
wakes normally, so nothing new is required of the SCHEDULER — but the operator-facing cost is real,
and an author who wants one decision for the whole loop does not have that here (§2, non-goals).
(Something new *was* required of the executor, and this sentence originally said otherwise:
`LoopGateSettled` (§4). Without it the tenth question could never be reached, because the first
gate's deadline expired the whole `Loop` long before.)

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
10. An **expired** gate **fails the `Loop`**, not just the gate node — whether or not a decision was
    journaled (§5.2 step 4). AC8 is the same rule read from the other side.
11. **Zero token spend** on the gate path: no `EffectRecorded`, no gateway call, no folded `usage`.
12. A decided gate **resumes from the journal** — no re-ask, no gateway call, identical decision.
12b. A decision honoured **inside** its gate's SLA stays honoured however long the run lives: a
    multi-iteration loop whose total human latency exceeds one gate's timeout still converges, and a
    loop that has already converged is not re-killed by its own gate's stale deadline when a later
    wake re-drives the graph. The guarding tests must **advance the clock across iterations** — every
    s4 test written before this one held it fixed, which is why the suite was green over the defect.
12c. AC8 and AC12b hold **simultaneously, in one run**: a settled gate replays while a LATER
    iteration's gate that nobody answered still expires and still fails the `Loop`, and the failure
    names the gate that actually ran out. Guarded separately the two are invisible to the
    over-correction — suppress expiry once the loop has made progress — since the AC8 test has no
    settlement in its journal and neither AC12b test lets a gate expire.
13. A human-backed role in `GateSpec::Agent` still fails loudly, and the message names
    `GateSpec::Human`. The `non_top_level_sites` row stands.
14. A model-backed role in `GateSpec::Human` fails loudly — at the ask, **and on a drive that does
    not ask** (a role edited `human` → `model` while the run sits paused on its gate). §5.5.
14b. A journaled decision naming an option **absent from the journaled menu** fails the node loudly —
    it neither continues nor stops the loop.
15. An oversized **authored** prompt fails the node; a verbose **iteration output** truncates the
    question instead of killing it.
16. The journaled prompt, **the journaled `menu`** and `actor` are redacted. **Three halves with
    two owners, and only the first two are executor properties.** The prompt is redacted by
    `run_human_loop_gate` before the `LoopGateAwaited` append
    (`the_journaled_loop_gate_question_is_redacted`, mutation-proven against swapping the redactor
    for the identity). The MENU is redacted on the same append, and the pause `reason` built from
    it with it (`a_credential_in_a_menu_option_name_never_reaches_the_journal`, mutation-proven
    against appending `menu.to_vec()`); a menu whose option names COLLIDE once redacted fails the
    gate loudly rather than offering two options under one name
    (`a_menu_whose_option_names_collide_once_redacted_fails_the_gate_loudly`). `actor` reaches no
    executor sink at all — the
    arm reads only `option` off `LoopGateDecided` — so it is owed by the writer that appends the
    event, i.e. **Task 12's `torii run gate decide`**, and is acceptance-tested there. §6 records
    why it cannot be inherited from `cmd::gate::decide`'s existing actor handling. **SHIPPED**
    (`a_secret_shaped_actor_is_redacted_before_a_loop_gate_decision_is_journaled`), redact-then-cap
    as `cmd::human::answer` does, with `Measured::AfterRedaction` so the growth explanation names a
    transform the value really went through
    (`a_loop_gate_actor_that_only_exceeds_the_cap_after_redaction_is_rejected`). The `HumanGate`
    arm beside it keeps `AsGiven` and no scrub, which is the asymmetry §6 predicted.
    **Review-round correction:** the named test asserted the byte COUNT only, which both
    discriminants produce — swapping them left the whole torii suite green, so the sentence above
    named a guard that did not hold its property. It now asserts the WORDING (`once redacted`), and
    its `HumanGate` sibling `an_oversized_actor_is_rejected_before_anything_is_journaled` asserts
    the NEGATIVE, so the asymmetry is pinned from both ends.
17. `torii run gate decide --node "{loop}/{i}/__gate__" --option <name>` decides a loop gate; a bad
    name recites the journaled menu; `run signal` and `run agent answer` refuse it, each naming the
    verb that would work. **SHIPPED.** `gate_menu` returns a `PublishedMenu::{Human,Loop}` and
    everything up to the append is factored over the option NAMES; only the append itself branches,
    because the two events are not interchangeable. Two additions the criterion does not state:
    `--note` is REFUSED on a loop gate rather than dropped (the event has no note field), and the
    verb refuses a decision at or after the journaled deadline, the guard §7 says Task 12 owes —
    boundary pinned on both sides.
    **Review round, two gaps in "each naming the verb that would work".** (a) Both refusals named
    the VERB and nothing asserted they named the KIND: rewriting both `PublishedMenu::Loop` arms to
    say "a HumanGate" left the suite green, because `contains("run gate decide")` is satisfied by
    the `HumanGate` wording too — so the per-kind split, whose whole purpose is the message, was
    inert on the operator-facing side. Both tests now assert `Loop's human gate` and the negative.
    (b) `cmd::human::answer` did not refuse a menu-bearing node at all — it read the QUESTION
    first, so a journal with a `LoopGateAwaited` and an `AgentAwaited` at one id was ANSWERED
    (exit 0) at a path whose only reader reads `LoopGateDecided`. The `gate_menu` match now runs
    ahead of the question check, matching `run signal` and the listing. The reachable journal is
    the executor `Waiting` arm's case (b) — an embedder's direct append; the executor does not
    write that shape itself, and the fix's comment records the limit rather than overclaiming.
    **The operator's DISCOVERY surface was not swept either:** `run gate --help` still said
    "Decide a `HumanGate`" and `run agent --help` still counted the waiting kinds as three. Both
    now name the loop gate, guarded at the binary level in `tests/cli.rs`.
18. `run list-paused` renders the loop gate's question and menu. **SHIPPED**, as the FOURTH
    `AwaitingNode` shape: `options` and `question` both present, which is what tells a loop gate
    from the other three. Additive for a `--json` consumer, because `options`-present ⇒
    `gate decide` is evaluated first everywhere. A gate whose decision has been HONOURED
    (`LoopGateSettled`) leaves the listing — without that, `run_loop` re-deriving every iteration's
    gate leaves each decided one advertised for the life of the run.
    **Review round.** "Evaluated first everywhere" was false in one place and the `--json` claim
    had no test: `awaiting_nodes` resolved the loop-gate kind through a rule PARALLEL to
    `cmd::gate::gate_menu` and disagreed with it on a `GateAwaited` + `LoopGateAwaited` journal
    (the row said `loop gate: revise`; `decide` refused `revise` and recited `ship|hold`). It now
    CALLS `gate_menu` — one resolver, no second order — and the fourth shape's `--json` keys, the
    conditional header wording in both directions, the new cell's `one_line`/`cap_chars` and the
    `## Task` reserve's empty-label branch each have a mutation-proved guard. The redaction of the
    loop gate's question in the listing was a second, unguarded call site of `redact_question`;
    it is now `a_secret_shaped_loop_gate_question_is_redacted_in_the_listing`, asserted on the
    table AND on `--json`.
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
