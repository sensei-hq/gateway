---
title: SP-6 s1 — AwaitSignal (the HITL primitive)
doctype: design-spec
module: orchestrator
slice: SP-6-s1
status: approved
date: 2026-08-24
---

# SP-6 s1 — `AwaitSignal`

## 1. Summary

A graph node that **pauses until an external signal arrives**, with an optional deadline. The signal's
payload is journaled and folded, so the node reads its answer from the fold and **never re-asks** on
resume — the same shape `PlannerSelected` already uses for a planner choice.

This is the general HITL primitive. `HumanGate` (s2) is a typed approve/reject wrapper over it, and
human-as-Agent (s3) follows. Building the primitive first forces the journaled-answer question to be
settled once rather than three times.

## 2. HITL is not HOTL — the distinction this slice turns on

SP-DATA-4 shipped **HOTL**, human *on* the loop: an operator intervenes from **outside** via
`torii run cancel` / `run wake`. The run does not know they exist.

SP-6 is **HITL**, human *in* the loop: the graph itself **blocks on a human decision as a first-class
node**. The run is designed to wait.

The substrate for the waiting already exists and is proven — `RunPaused { resume_after: None }` is the
never-auto-woken class, `torii run list-paused` surfaces it, the durable scheduler wakes a deadline.
What does **not** exist is a way to carry an *answer* back in: `force_wake` is a resume, not a
decision. That gap is this slice.

## 3. Goals / Non-goals

**Goals**
- `NodeKind::AwaitSignal { timeout: Option<Duration> }`.
- A journaled, folded answer, so a resumed run never re-asks.
- An optional deadline that **fails** the node — never a silent self-approval.
- `torii run signal <run> --node <node> --payload <json>`, reporting the effect it achieved.
- Additive: a graph with no `AwaitSignal` node is byte-identical.

**Non-goals (deferred, §8)**
- `HumanGate`'s typed decisions (s2) and human-as-Agent (s3).
- A business-level signal key distinct from the node id.
- Signal delivery over anything but the CLI (no webhook, no HTTP — that needs the auth model SP-DATA-4 deferred).
- Multi-party approval (N-of-M sign-off).

## 4. The decisions, and why

| Decision | Choice | Why |
|---|---|---|
| Addressing | **By node id** | Already path-qualified and unique (`{map}/{i}`, `{loop}/{i}/…`), so a Map child awaiting its own signal is unambiguous. A business key is deferred until an external system needs to address a node without knowing its path. |
| Timeout | **Optional deadline that FAILS the node** | Composes with the durable scheduler (`resume_after: Some`). A default-payload-on-timeout was rejected: a gate that silently self-approves is exactly the footgun this codebase's fail-closed stance argues against. |
| Duplicate signal | **Last wins, reported honestly** | Lets an operator correct a mistaken decision before the run resumes. Once the node has completed it is folded complete and never re-executes, so a later signal changes nothing — and `torii` must say so rather than implying it landed. |
| Payload secrets | **Redacted at production** | §6.4. |

## 5. Architecture

```
orchestrator-core
  journal.rs   SignalAwaited  { node: NodeId, deadline: Option<DateTime<Utc>> }
               SignalReceived { node: NodeId, payload: serde_json::Value }
  graph.rs     NodeKind::AwaitSignal { timeout: Option<chrono::Duration> }

orchestrator
  executor     Fold gains  signals:   HashMap<NodeId, serde_json::Value>
                           deadlines: HashMap<NodeId, DateTime<Utc>>
               run_node's AwaitSignal arm — a three-way fold read (§6.2)

torii
  run signal <run-id> --node <node-id> --payload <json>
  run list-paused   — shows the awaiting node and its deadline
```

Both events are keyed by `NodeId` and folded exactly as `PlannerSelected` is
(`fold.selections`), so the mechanism is not new — only the oracle is slower.

## 6. The design, and the trap in it

### 6.1 The absolute deadline MUST be journaled, not recomputed

The obvious implementation computes `now + timeout` each time the node executes. That is wrong, and
wrong in a way a naive test will not catch: **every resume pushes the deadline forward**, so a run
force-woken every ten minutes with a one-hour timeout *never expires*.

The deadline is therefore fixed at first execution, journaled as `SignalAwaited`, and folded
thereafter. It lives in its own node-keyed event rather than in `RunPaused.resume_after` because that
field is not node-keyed and a run pauses for many unrelated reasons over its life.

This is the same failure shape as SP-DATA-5's frozen `Fold::default()` spend, and it is called out
here because that one survived five tasks and their reviews before an e2e caught it.

### 6.2 The node is a three-way fold read

| Fold state | Behaviour |
|---|---|
| signal present | `Completed(payload)` — never re-asks |
| no signal, no deadline recorded | journal `SignalAwaited`, `Paused { resume_after: deadline }` |
| no signal, deadline recorded, `now >= deadline` | `NodeFailed` — the timeout fired, loudly |
| no signal, deadline recorded, `now < deadline` | re-pause with the **same** absolute deadline |

The last row exists only because the deadline is durable, and it is what makes `torii run wake` on an
awaiting node behave sanely instead of silently resetting the clock.

### 6.3 The early-signal race resolves itself

A signal delivered *before* the node first executes is simply already in the fold when it runs, so the
node completes immediately. No buffering, no ordering constraint, no special case — a direct
consequence of the fold being the source of truth.

### 6.4 The payload is redacted at production, and is not a credential channel

A human can paste anything into `--payload`, and unlike a pause reason this value does not merely get
*displayed*: it becomes the node's output and flows into downstream nodes and model prompts.

So the s2 `Redactor` is applied **before both the journal write and the node's return** — s2's
determinism rule, giving live == journaled == replayed. The cost is near zero, because the redactor
matches credential *shapes* and a legitimate `{"decision":"approved"}` does not match any of them.

**A signal is not a credential channel.** The credential broker is. This is documented in the CLI help,
because the failure mode otherwise is a human pasting a token that lands in durable storage *and* in a
model prompt.

### 6.5 Payload size is capped

An unbounded JSON blob in a journal row is a durable footgun. Over-threshold payloads route through the
`ContentStore` exactly as any other large effect output does (`split_output`'s existing threshold), or
are rejected at the CLI — whichever matches the established behaviour. The implementation must follow
the existing convention rather than invent a second one.

### 6.6 Honest reporting, per the established rule

`torii run signal` must report the effect it achieved, not that a call returned `Ok`:

- node awaiting → `signalled: <node> (the run will resume on the next worker tick)` — note it does not
  claim the run resumed; a worker tick does the driving, exactly as `run wake` learned to say.
- node already completed → `not delivered: <node> already completed` (exit 2).
- node not awaiting → `not delivered: <node> is <state>` (exit 2).
- unknown run → exit 2.

## 7. Failure modes and testing

| Case | Behaviour |
|---|---|
| Signal to a completed node | No effect; reported as not delivered |
| Duplicate signal while paused | Last wins |
| Deadline fires, no signal | `NodeFailed`, loud |
| Force-woken before the deadline | Re-pauses with the same absolute deadline |
| `AwaitSignal` in a Map fan-out | One pause per awaiting child — the accepted shape from SP-DATA-5 §6.3a |
| No `AwaitSignal` in the graph | Byte-identical; the workspace suite stays at 1340 |

**Acceptance criteria.** Each names the mutation that must break it — this project has produced nine
tests that did not guard the line they appeared to, and every one was caught by asking that question.

- **AC1 — the deadline never moves.** Force-wake an awaiting node three times across a simulated hour;
  the folded deadline is unchanged each time and the node re-pauses. *Mutation:* recompute
  `now + timeout` at each execution; this must fail. **This is the slice's most important test.**
- **AC2 — the answer is folded, never re-asked.** A signalled node completes; on a subsequent resume it
  is not re-executed and no second `SignalAwaited` is journaled. *Mutation:* read the signal from
  memory rather than the fold; must fail.
- **AC3 — early delivery.** A signal journaled before the node first runs completes it immediately.
- **AC4 — the timeout fails loudly.** Deadline reached with no signal ⇒ `NodeFailed` naming the node and
  the deadline; the run does **not** complete.
- **AC5 — honest reporting**, all four paths in §6.6, each asserting the observed state after the call
  rather than the call's `Ok`.
- **AC6 — payload redaction.** A payload containing a credential shape is redacted in **both** the
  journaled event and the node's output. *Mutation:* redact only on the journal path; the output
  assertion must fail.
- **AC7 — cross-process e2e.** Process A runs a graph to an `AwaitSignal` pause; `torii run list-paused`
  shows the node and deadline; `torii run signal` delivers; a **fresh** worker completes the run with
  **zero re-spend** of the completed prefix, asserted by an attributable call counter.
- **AC8 — additivity.** No `AwaitSignal` node ⇒ `cargo test --workspace` stays at 1340 + the new tests,
  green with and without `DATABASE_URL` at default parallelism.

## 8. Deferred / carry-forward

- **`HumanGate`** (s2) — typed approve/reject/choose over this primitive.
- **human-as-Agent** (s3) — a human answering where an `Agent` node would call a model.
- **A business-level signal key** distinct from the node id, for external systems that cannot know a
  node path.
- **Non-CLI delivery** (webhook/HTTP) — needs the auth model SP-DATA-4 deferred.
- **Multi-party approval** (N-of-M), and per-signal authorization (who may signal what).
- **A default payload on timeout** — deliberately rejected for s1 (§4); if it is ever added it must be
  opt-in per node and impossible to configure on a `HumanGate`.

## 9. Files touched

- `crates/orchestrator-core/src/journal.rs` — two events; `graph.rs` — one `NodeKind` variant.
- `crates/orchestrator/src/executor/` — `Fold` gains two maps; `run_node`'s `AwaitSignal` arm;
  `fold_journal` arms for both events.
- `crates/torii/src/cmd/run.rs`, `src/main.rs` — `run signal`; `list-paused` shows the awaiting node.
- `crates/torii/tests/e2e_pg.rs` — AC7.
