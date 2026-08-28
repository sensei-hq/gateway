---
title: SP-6 s3 — human-as-Agent (a human answering where a model would)
doctype: design-spec
module: orchestrator
slice: SP-6-s3
status: draft
date: 2026-08-27
---

# SP-6 s3 — human-as-Agent

## 1. Summary

A **role in the registry that is answered by a person instead of a model**. An `Agent` node whose
`AgentRef` resolves to a human-backed definition pauses once, journals the question it is asking, and
completes when a human answers. Its output is `{"text", "actor"}` — the same `"text"` key a
model-backed `Agent` produces, so downstream nodes consume a human answer without knowing it was
human.

This is the last slice of SP-6. s1 shipped `AwaitSignal` (the general primitive: pause, accept any
JSON, fold it). s2 shipped `HumanGate` (the typed menu over it). s3 is the third and final waiting
kind.

## 2. Goals / Non-goals

**Goals**
- `AgentBacking::{Model, Human { timeout }}` on `AgentDefinition`, serde-defaulted to `Model`.
- `run_agent` short-circuits to a wait **before the ReAct loop starts** when the resolved agent is
  human-backed — so no gateway call is reachable at all.
- Two new journal variants: `AgentAwaited { node, deadline, prompt }` and
  `AgentAnswered { node, text, actor }`. New *variants*, so `FORMAT_VERSION` stays 1.
- `torii run agent answer <run> --node <id> (--text | --text-file) [--as who]`.
- `run list-paused` shows the question.
- Additive: a config with no human-backed agent is byte-identical.
- **Legal ONLY as a top-level `NodeKind::Agent`** — see §5.5 for the other four `drive_agent`
  callers and why each is rejected.

**Non-goals**
- **Tool use by a human-backed agent** — see §7, "the accepted cost".
- Multi-turn (a human standing in for the model at *each* turn). §4 records why the one-ask model
  was chosen instead.
- Human-as-fallback (ask a person only when the model fails) — needs a confidence/failure policy
  that does not exist, and couples to retry semantics. Its own slice if ever.
- **A human-backed agent as a `MapBody`, `LoopBody`, `GateSpec::Agent` or planner** (§5.5). The
  Loop-gate case ("a human decides whether the loop continues") is the most valuable of the four and
  the obvious next slice; it is out of scope here because it needs a `LoopGate` contract over
  `{"text","actor"}` that does not exist.
- Authorization (who may answer what), N-of-M, non-CLI delivery — inherited deferrals from s1/s2.

## 3. The decisions, and why

| Decision | Choice | Why |
|---|---|---|
| What a human replaces | **The whole node — one ask** | An `Agent` node is a multi-turn loop, so "a human answering where a model would" is ambiguous. One ask makes a human-backed agent *substitutable at the `AgentRef`*: swap a model reviewer for a person by editing config, graph unchanged. Per-turn substitution was rejected — it needs the human to author tool-call JSON, and it would put SP-4 grants in the position of gating a *human's* actions, which is a different trust question. |
| Where the marker lives | **`AgentDefinition.backed_by`** | The role and its SLA travel together ("legal-reviewer always has 48h"), the graph never changes, and SP-DATA-2's config fence already makes the registry durable and versioned. A `NodeKind::Agent.timeout` field was rejected: it is inert for every model agent and weakens substitutability. A sentinel `chain: "human"` was rejected as stringly-typed with nowhere to put the timeout. |
| Answer channel | **New `AgentAnswered` + `run agent answer`** | s2's bypass argument does not transfer — there is no menu to validate against, so a raw `run signal` would not skip validation. But it *would* skip **attribution**: `SignalReceived` has no `actor`, and s3's answer becomes the node's OUTPUT and flows into downstream prompts. A typed event also lets `list-paused` show the question and lets each verb refuse the other kinds. |
| Expiry vs answer | **Answer read FIRST** (s1's ordering, not s2's) | An agent's answer is **work product, not an approval**, so there is no self-approval to guard against — which is the entire reason s2 expires first. Discarding a human's in-time answer because a worker was down punishes them for infrastructure. The deadline still fails the node in the case it exists for: nobody answered. **This is a deliberate divergence from s2** and §6.2 records it as such. |
| The question | **Journaled on `AgentAwaited`** | Same argument that made s2's menu durable: an operator must see *what is being asked* without reading the graph and the registry, and fixing the question at ask time is what lets a late answer still be honoured. |

## 4. Types

```rust
// orchestrator-core/src/registry.rs
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub enum AgentBacking {
    #[default]
    Model,
    Human { timeout: Option<chrono::Duration> },
}

pub struct AgentDefinition {
    …
    #[serde(default)]
    pub backed_by: AgentBacking,
}
```

`#[serde(default)]` + `#[default] Model` is what keeps every existing agent, every `config_agents`
jsonb row and every registry test byte-identical.

```rust
// orchestrator-core/src/journal.rs — both NEW VARIANTS, FORMAT_VERSION stays 1
AgentAwaited  { node: NodeId, deadline: Option<DateTime<Utc>>, prompt: String }
AgentAnswered { node: NodeId, text: String, actor: String }
```

**Output on completion:** `{"text": "<answer>", "actor": "<who>"}`. The `"text"` key is deliberate —
it is exactly what a model-backed `Agent` produces, so `Consolidate`, `BranchCond::TextContains` and
a dependent's prompt assembly all consume it unchanged.

**Fold**, named concretely so the executor's call sites are not invented:

```rust
// executor/mod.rs, beside `gate_decisions` / `menus`
struct AgentAnswer { text: String, actor: String }

agent_answers: HashMap<NodeId, AgentAnswer>,   // LAST wins  (`insert`)
agent_prompts: HashMap<NodeId, String>,        // FIRST wins (`entry().or_insert`)

// accessors, matching the established `signal_for`/`gate_decision_for`/`menu_for` vocabulary
fn agent_answer_for(&self, node: &NodeId) -> Option<&AgentAnswer>
fn prompt_for(&self, node: &NodeId) -> Option<&str>
```

LAST wins for the answer (an operator can correct it before the run resumes); FIRST wins for the
prompt (the human was asked THIS question). **`fold_journal` must also add an `AgentAwaited` arm to
the SHARED `deadlines` map** — `entry().or_insert(*deadline)`, exactly as s2's `GateAwaited` arm does
at `mod.rs:166` — because `wait_or_expire` reads `deadline_for` and knows nothing about which kind
recorded it. Both arms explicit, never a catch-all.

## 5. Architecture

### 5.1 The branch is in `run_agent`, before any turn

`run_agent` already resolves the `AgentRef` through the registry. When the resolved definition is
`Human`, it short-circuits **before chain resolution, before the ReAct loop, before any tool
wiring**. A human-backed agent never touches the gateway, so **zero token spend is structural** — not
measured, unreachable.

It reuses the machinery s2 extracted rather than copying it: `gate_precheck` (the fail-closed
terminal guard), `wait_or_expire` (the deadline durability) and `pause_awaiting`. s1's review found
real defects in exactly those arms; a third copy would be a third place for them to return.

### 5.2 The fold read

| fold state | behaviour |
|---|---|
| failure recorded | `Failed` — `gate_precheck`, checked FIRST |
| no `AgentAwaited` yet | journal it (prompt + deadline), then continue below |
| **answered** | `Completed({"text","actor"})` — **read BEFORE expiry** |
| not answered, deadline passed | `NodeFailed` — the SLA fired with nobody answering |
| not answered, deadline not passed | re-pause on the **same** absolute instant |

**Row 3 sits above row 4, and that is the slice's one deliberate divergence from s2.** See §3.

**The ask precedes the answer, unconditionally** — the prompt is journaled first even when an answer
is already folded, so there is never an answer without a recorded question. s2 established this: a
durable record breaks s1's "the early race resolves itself for free" property, and special-casing it
would reintroduce the non-durable record the design rejects.

### 5.3 Validation, two layers

`torii run agent answer` validates **from the JOURNAL ONLY** — it folds `AgentAwaited` for the node
and refuses if there is none. It does **not** read the graph and does **not** consult the registry.

That is not a simplification, it is the only design that works, and the depth review established
why by reading the code:

* `SchedulerStore::status()` returns a `ScheduledRun` whose own doc says *"The observe DTO (**NOT**
  the graph)"* (`scheduler.rs:52`). **No trait method exposes a submitted run's graph for read-back**
  — only `enqueue`/`claim_due` touch `Graph`. A graph-reading CLI would need a new trait method in
  both store impls.
* Even with the graph, "is this node human-backed" is a **registry** question — the graph carries
  only an `AgentRef` name. `cmd/run.rs` and `cmd/gate.rs` import no `Registry`/`ConfigSource` today;
  only `cmd/config.rs` and `boot::heavy` do, and `boot::heavy` needs a live DB-backed
  `PostgresConfigSource`. Wiring that into a one-shot light-tier command is a large, unrelated cost.
* Neither approach handles a path-qualified id (`{map}/3`, `{loop}/2/__gate__`): the graph has the
  top-level node, never a per-child `NodeKind`.

Journal-only needs none of that and handles path-qualified ids for free. It is exactly what
`cmd/gate.rs::gate_menu` and `cmd/run.rs::signal_state` already do.

**Correction to the sibling specs:** s2's §6.3 says `gate decide` "reads the graph from
`scheduled_runs`". It does not — `gate_menu` folds `GateAwaited`. The implementation was right and
the spec sentence was wrong; s3 states the working rule instead of inheriting the error.

The executor re-checks and is authoritative: the CLI's check is non-atomic and the library entry
point bypasses it. Third application of s1's lesson.

**Cross-refusal is now three-way.** `run signal` → `AwaitSignal`; `run gate` → `HumanGate`;
`run agent answer` → a human-backed `Agent`. Each refuses the other two, naming the right verb.

### 5.4 The prompt is `assemble_prompt`'s output, composed before the backing check

The journaled `prompt` is the **full output of the existing `agent::prompt::assemble`** —
`system_prompt` + activated skills (`Activation::OnKeywords` gating unchanged) + the rendered
`## Context` section from predecessors. The human sees precisely what the model would have, which is
the literal reading of "a human answering where a model would", and it needs no new
prompt-building code that could drift from the model path.

**Ordering, and why it works:** `assemble_prompt` is called at `agent.rs:75`, immediately *before*
`resolve_chain` at `agent.rs:76`. It needs no chain. So the composition happens first, exactly where
it does today, and the human-backing branch sits between it and `resolve_chain` — the model path is
untouched and the human path reuses its work.

**Consequence, named because it is load-bearing:** an assembled prompt is routinely multi-KB, so
§6's size bound is a real constraint rather than a theoretical one. A role whose skills and context
exceed it fails loudly at first execution.

**Accepted cost:** skills written for a model may read oddly to a person. That is preferred to
showing the human *less* than the model would have had, which would let them answer without context
the model would have seen.

### 5.5 A human-backed agent is legal ONLY as a top-level `Agent` node

`drive_agent` is the shared choke point for **five** call sites, not one: `run_node`
(`mod.rs:1128`), `run_map`/`run_consolidate` (`MapBody::Agent`, `fanout.rs:183,269`), `run_loop`'s
body (`LoopBody::Agent`, `fanout.rs:488`), `run_loop`'s **gate** (`GateSpec::Agent`, the reserved
`"{loop}/{i}/__gate__"` path, `fanout.rs:553`), and `expand.rs`'s `drive_planner_agent`
(`PlannerRef::Agent`/`Select`, `expand.rs:48`).

Mechanically the pause already composes at all of them — `AgentStep::Paused` becomes
`MapChildPaused` and pauses the whole Map (`fanout.rs:291-337`); a Loop body/gate pause propagates
straight out. **But each is a different feature**, and only the first is in scope:

| site | if human-backed | verdict |
|---|---|---|
| `NodeKind::Agent` | one ask, one answer | **LEGAL** |
| `MapBody::Agent` | N concurrent human asks before one Map completes | rejected |
| `LoopBody::Agent` | a human re-answers every iteration | rejected |
| `GateSpec::Agent` | a human decides loop continuation; must satisfy `LoopGate` over `{"text","actor"}` | rejected |
| `PlannerRef::Agent`/`Select` | the answer goes to `parse_plan(text)` — the human must hand-author a machine-parseable plan **graph** | rejected |

`Registry::validate()` cannot see the graph, so the rejection lives in **`Graph::validate_dag`**,
which already recurses into `Map`/`Loop`/`Branch` bodies — but `validate_dag` is pure over the graph
and does not know which `AgentRef`s are human-backed. **So the check needs the registry**, and the
only place both are in hand is the executor's config-load path.

Resolution: the rejection is enforced where the agent is resolved — `drive_agent` fails the node
loudly (`NodeFailed`, naming the site and the role) when a human-backed agent is reached from any
caller but `run_node`. `run_node` passes a flag; the other four do not. This is a runtime check
rather than a load-time one, and that is a stated limitation: a graph that misuses a human-backed
agent validates and then fails on first execution of that node. Making it load-time needs a
registry-aware graph validation pass that does not exist and that neither sibling slice needed.

### 5.6 The `on_agent_started` hook does not fire for a human-backed agent

`agent.rs:97` calls `h.on_agent_started(run, node_id, &agent_ref.0, &ar.chain)` — it requires a
resolved `chain: &str`, which a human-backed agent by construction never has. Since the branch sits
before `resolve_chain`, there is no chain to pass and the hook is **not** called. Recorded rather
than left to be discovered: the hook's contract is "an agent turn is starting against this chain",
and a human-backed node starts no turn against any chain. A `HumanAwaited` hook is deferred with the
rest (§9).

### 5.7 Determinism

Redact **once** at the fold-read; hand that one value to both the node's return and
`publish_context`. Splitting them makes a live run and a replayed run disagree about the node's
output, surfacing as a false `DeterminismViolation` — shipped and caught twice in this codebase.

No gateway call and no `EffectRecorded`: the fold IS the memo, so a resumed run re-reads its answer
at zero token cost.

## 6. Bounds and safety

- **`--text` and the journaled `prompt` are both size-capped**, but they cannot share torii's
  helper. `check_payload_size`/`MAX_PAYLOAD_BYTES` are `pub(crate)` in `crates/torii/src/cmd/run.rs`,
  and **`orchestrator` does not depend on `torii`** — that is a reverse dependency the crate graph
  cannot express, not merely a visibility problem. So: a new `pub const MAX_HUMAN_TEXT_BYTES` in
  **`orchestrator-core`**, used by BOTH the executor (bounding the composed `prompt` before it is
  journaled) and torii (bounding `--text`/`--text-file`). One constant, two call sites, no
  duplicated number.
- **The prompt's over-bound behaviour is a `NodeFailed`, not a CLI refusal.** The prompt is composed
  inside `drive_agent`, where no exit-2 path exists. s1 §6.5's `ContentStore`/`split_output` route
  was considered and rejected for this field: `AgentAwaited.prompt` is a bare `String` with no
  ref-or-inline alternative (the same shape argument s1 made for `SignalReceived.payload`), and a
  question too large to journal is a malformed agent config, which should fail loudly at first
  execution rather than silently spill to CAS.
- **`--as` defaults to `$USER`**, reusing `cmd::gate::actor_or`/`actor_or_user` verbatim rather than
re-deriving it — never-empty, falling back to `"unknown"`. One definition of "who answered" across
both human-facing verbs.

**`--text-file` content takes the identical redact-then-cap ordering as `--text`** — read the file,
redact, THEN check the size against the redacted value (`Measured::AfterRedaction`). Stated rather
than assumed because that exact ordering shipped wrong twice in this feature: s1 capped
pre-redaction while writing post-redaction, and s2 repeated the shape.

**`--text-file` ships in this slice**, not as a follow-up. s1 added `--payload` argv-only and a
  review caught the secret-in-`ps` exposure; s2 repeated it with `--note`. An agent's answer is the
  longest free text of the three and the most likely to be pasted from elsewhere.
- **Every operator-facing string** — the answer, the actor, the prompt, the question in
  `list-paused` — goes through `render::one_line` and a cap, and through the redactor before the
  durable write.
- **`Registry::validate()` gains three rules and must SKIP one it already has.**
  - **Skip:** `validate()` today unconditionally requires every agent to resolve a chain
    (`agent.chain.is_none() && chain_binding(..).is_none()` ⇒ `UnknownChainRef`, `registry.rs:450-475`).
    That runs at **config-load time**, independent of the runtime short-circuit, so it would reject
    most human-backed agents before any node ever executed. It must not apply to `Human` backing —
    a human-backed role has no chain by construction, and forcing a dummy binding would be a lie in
    the config.
  - **Reject `tools`** on a human-backed agent: the loop that would use them never runs, and a grant
    that grants nothing is the confused-deputy shape SP-4 s1 argues against.
  - **Reject an empty `system_prompt`**: the prompt IS the question.
  - **Reject a `Human` agent used anywhere but a top-level `Agent` node** — see §5.5.
- **`AgentBacking::Human { timeout }` is bounded** by the same century rule
  `MAX_AWAIT_SIGNAL_TIMEOUT` applies to `AwaitSignal`/`HumanGate`. That bound lives in
  `Graph::validate_dag`, which is pure over the `Graph` and never sees the registry — so the check
  must be added to `Registry::validate()` instead, reusing the same constant. Without it the
  overflow is caught only at runtime by `wait_or_expire`'s `checked_add_signed` (which fails the
  node rather than panicking, so this degrades safely) — but both sibling slices treated the
  up-front bound as worth naming.

## 7. Trust boundary and the accepted cost

**`actor` is ATTRIBUTION, NOT AUTHENTICATION.** Inherited from s2, and it matters more here: the
string lands in the node's *output* and flows into downstream model prompts, not just an audit
trail. Anyone who can reach the database can write any actor string. It must never be branched on as
an access control.

**A human-backed agent cannot use tools.** It answers once; it cannot read a file or call an API
mid-thought the way a model-backed agent can. This follows from the one-ask model (§3) and is the
right trade for this slice — but it means "substitutable with a model agent" is true for **output
shape** and false for **capability**. Swapping a tool-using model agent for a human one will
silently stop the tools firing, which is why §6's `validate` rule rejects the config outright rather
than ignoring the declaration.

## 8. Acceptance criteria

Each names the mutation that must break it — this project has produced seventeen tests that did not
guard the line they appeared to, and every one was caught by asking that question.

| AC | Proven by | Mutation that must break it |
|---|---|---|
| **AC1** Never calls the gateway | Drive one; `calls == 0` **and** the node still produces its answer | Fall through to the ReAct loop → calls > 0 |
| **AC2** Output is `{"text","actor"}` | A downstream `BranchCond::TextContains` matches it unchanged | Rename the `text` key → the Branch takes `default` |
| **AC3** Answer read BEFORE expiry | Answer inside the SLA, drive after it → `Completed` | Move the answer-read after `wait_or_expire` → fails |
| **AC4** A fired expiry is terminal | Expire, then answer → stays `Failed` | Drop `gate_precheck` → the late answer completes it |
| **AC5** No default answer on expiry | Expiry → `NodeFailed`, never an output | Any default → fails |
| **AC6** Ask precedes answer | An answer folded with no ask still resolves, and the prompt is journaled | Gate the ask on "unanswered" → no `AgentAwaited` |
| **AC7** Three-way cross-refusal | Each verb refused on the other two kinds | Drop a kind check → the refusal vanishes |
| **AC8** `validate` rejects tools / empty prompt | Both configs refused | Drop either check → accepted |
| **AC9** Redaction on both paths | A secret in `--text` scrubbed in the journal AND the output | Redact one → live/replayed diverge |
| **AC10** Bounds | Oversized `--text` and oversized prompt both refused, zero rows | Remove either cap → a durable oversized row |
| **AC11** `--text-file` keeps the answer out of argv | Read the child's own `ps` line | Argv-only delivery → the sentinel appears |
| **AC12** Zero re-spend on resume | Answered node replays from the fold **and still produces the answer** | Break the fold read → this must be among the reds |
| **AC13** Cross-process e2e (Postgres) | submit → pause → `list-paused` shows the question → answer in process B → fresh `worker serve --once` completes it | Swap the answer for a bare `wake` → stays `Paused` |
| **AC15** A human-backed agent is rejected at every non-top-level site | Each of `MapBody`/`LoopBody`/`GateSpec::Agent`/planner fails the node loudly, naming the site | Drop the caller flag → a human-backed planner reaches `parse_plan` |
| **AC16** `validate()` skips the chain requirement for `Human` | A human-backed agent with no `chain` and no binding loads | Leave the chain check unconditional → every human-backed config is rejected at load |
| **AC17** The journaled prompt is `assemble_prompt`'s output | The prompt contains an activated skill's body and the `## Context` section | Compose `system_prompt` alone → the skill text is absent |
| **AC14** Additivity | No human-backed agent ⇒ byte-identical; suite stays **1505** + new | — (the baseline guard) |

**AC13 is `DATABASE_URL`-gated.** It returns early without one and is therefore **counted as passed
while having exercised nothing**; the raw-stderr `SKIP` line is the only signal. Stated because s2's
spec claimed the test was `#[ignore]`d, which was false — `#[ignore]` cannot be conditioned on an
env var.

**AC12 must assert the answer is still produced**, not merely `calls == 0`. s2 shipped exactly that
test asserting only the call count, and because no gateway path was reachable from the node it
passed whether the node worked or was completely broken.

**AC14's 1505 is measured** at `790f180`, verified both with and without a database — not carried
forward from a previous slice's spec. s1's spec recorded a baseline wrong by 77 tests.

## 9. Deferred / carry-forward

- **Tool use by a human-backed agent**, and the multi-turn model (§2, §7).
- **Human-as-fallback** — ask a person only when the model fails.
- **Authorization** — who may answer which node. Needs an identity model that does not exist.
- **`RunStatus::Rejected`**, non-CLI delivery, N-of-M approval — inherited from s1/s2.
- **No `OrchestratorHooks` callback** fires for `AgentAwaited`/`AgentAnswered`, matching every other
  signal and gate event. Not a regression, but a HITL pause is exactly what a live flow-tracking UX
  would want.
- **One SLA per role, not per use site** — the cost of putting `timeout` on `AgentDefinition` (§3).
