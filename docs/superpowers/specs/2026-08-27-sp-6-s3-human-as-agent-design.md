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

**Non-goals**
- **Tool use by a human-backed agent** — see §7, "the accepted cost".
- Multi-turn (a human standing in for the model at *each* turn). §4 records why the one-ask model
  was chosen instead.
- Human-as-fallback (ask a person only when the model fails) — needs a confidence/failure policy
  that does not exist, and couples to retry semantics. Its own slice if ever.
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

**Fold:** `agent_answers` (LAST wins — an operator can correct an answer before the run resumes) and
`agent_prompts` (FIRST wins, beside the existing shared `deadlines` map — the human was asked THIS
question).

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

`torii run agent answer` reads the graph from `scheduled_runs`, confirms the node is a human-backed
`Agent`, and refuses before writing. The executor re-checks and is authoritative: the CLI's check is
non-atomic and the library entry point bypasses it. Third application of s1's lesson.

**Cross-refusal is now three-way.** `run signal` → `AwaitSignal`; `run gate` → `HumanGate`;
`run agent answer` → a human-backed `Agent`. Each refuses the other two, naming the right verb.

### 5.4 Determinism

Redact **once** at the fold-read; hand that one value to both the node's return and
`publish_context`. Splitting them makes a live run and a replayed run disagree about the node's
output, surfacing as a false `DeterminismViolation` — shipped and caught twice in this codebase.

No gateway call and no `EffectRecorded`: the fold IS the memo, so a resumed run re-reads its answer
at zero token cost.

## 6. Bounds and safety

- **`--text` and the journaled `prompt` are both size-capped** before the append, reusing
  `check_payload_size` and `MAX_PAYLOAD_BYTES`. s2 shipped `--note` unbounded and the review caught
  it; `system_prompt` is unbounded config, so the prompt needs the same bound.
- **`--text-file` ships in this slice**, not as a follow-up. s1 added `--payload` argv-only and a
  review caught the secret-in-`ps` exposure; s2 repeated it with `--note`. An agent's answer is the
  longest free text of the three and the most likely to be pasted from elsewhere.
- **Every operator-facing string** — the answer, the actor, the prompt, the question in
  `list-paused` — goes through `render::one_line` and a cap, and through the redactor before the
  durable write.
- **`validate` rejects** a human-backed agent that declares `tools` (a grant that grants nothing is
  the confused-deputy shape SP-4 s1 argues against) or that has an empty `system_prompt` (the prompt
  IS the question).

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
