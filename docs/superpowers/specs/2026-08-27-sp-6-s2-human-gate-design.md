---
title: SP-6 s2 — HumanGate (typed decisions over the HITL primitive)
doctype: design-spec
module: orchestrator
slice: SP-6-s2
status: approved
date: 2026-08-27
---

# SP-6 s2 — `HumanGate`

## 1. Summary

A graph node that **asks a human to pick one of an enumerated set of options**, where each option
declares its own outcome: the run either carries the decision forward or terminates on it.

s1 shipped `AwaitSignal`, the general primitive: pause, accept any JSON, fold it, never re-ask. s2 is
the typed layer over it. The waiting machinery, the durable deadline and the fail-closed expiry are
**shared with s1, not copied** — s1's whole-slice review found real defects in exactly those arms, and
a second copy would be a second place for them to come back.

## 2. What composition already gives, and what it does not

An author can hand-roll approve/reject today: `AwaitSignal` → `Branch` on
`BranchCond::FieldEquals("decision", "approved")`. So s2 has to earn its place. Four things
composition does not give:

1. **Validation at the gate.** `torii run signal` accepts any JSON, so
   `--payload '{"decison":"aproved"}'` is journaled, exits 0, matches no `Branch` arm, and falls to
   the default arm — an unreviewed change proceeding as if reviewed.
2. **Reject with real semantics.** A rejection today merely *produces a value*; the author must
   remember to test it. Forgetting is silent.
3. **Typed operator verbs.** Hand-writing decision JSON makes the approval surface a raw data
   channel, with the argv hazard that motivated `--payload-file`.
4. **Choose-one-of-N with static exhaustiveness.** An enumerated menu lets `validate_dag` prove every
   decision is handled, turning a 3am surprise into a graph-validation error.

## 3. Goals / Non-goals

**Goals**

- `NodeKind::HumanGate { options: Vec<GateOption>, timeout: Option<Duration> }`.
- Per-option outcome: `Complete` (flows on) or `Fail` (node fails, dependents cascade-skip).
- A **durable menu** — the options the human was shown are journaled, not re-read from the graph.
- Two-layer validation: the CLI refuses before writing; the executor is authoritative.
- Attribution: a self-asserted `actor` and a free-text `note`/`reason` on every decision.
- `torii run gate approve|reject|decide`, and `run list-paused` showing each gate's menu.
- Additive: a graph with no `HumanGate` is byte-identical.

**Non-goals (deferred, §9)**

- **Authorization** — who *may* decide *what*. Needs an identity/authentication model that does not
  exist anywhere in this codebase. §7 states the trust boundary explicitly.
- A distinct terminal `RunStatus::Rejected` (see §4, "the accepted cost").
- Non-CLI delivery (webhook/HTTP), inherited from s1's deferral.
- Multi-party (N-of-M) approval.
- human-as-Agent (s3).

## 4. The decisions, and why

| Decision | Choice | Why |
|---|---|---|
| Node kind | **A new `NodeKind::HumanGate`** | Not sugar that desugars to `AwaitSignal + Branch`: the graph is caller-supplied and never journaled (SP-DATA-3), so a desugaring would re-run on every drive and have to be perfectly deterministic or resume breaks — and errors and `list-paused` would name synthetic nodes the author never wrote, whose ids would need the `/` separator `validate_dag` now bans for authors. |
| Decision event | **A new `GateDecided` variant** | A *variant*, not a new field, so **`FORMAT_VERSION` stays 1** — s1's proven trick. `actor` and `note` get real fields instead of being a convention inside an untyped payload, and a typed decision stays distinguishable from a raw signal in the audit trail. |
| Answering a gate | **Only `GateDecided`** | If a `SignalReceived` could answer a `HumanGate`, `torii run signal --payload '{}'` would bypass every validation this slice adds. `run_human_gate` reads only `gate_decision_for`; a stray `SignalReceived` on a gate is ignored and visible as an orphan. |
| Outcome model | **Per-option `Complete \| Fail`** | Reuses the existing terminal machinery exactly — no new `RunStatus`, no store change, no dbd migration. s2 stays a layer over s1. |
| The menu | **Journaled at ask time (`GateAwaited`)** | A human was shown a menu; validating their answer against a *different* menu later is simply wrong. Exactly parallel to s1's durable deadline ("the deadline belongs to the RUN, not to the graph"), and it makes §6.3's undeclared-option arm nearly unreachable rather than merely unlikely. |
| Exhaustiveness | **Conditional** | Only fires when a `Branch.on` names a `HumanGate`. `validate_dag` is deliberately syntactic — the `/` ban was chosen *"rather than detecting post-namespacing collisions"* — so an unconditional cross-node rule would break that stance, and requiring a `Branch` on every gate would put ceremony on the common approve-or-stop shape. |
| Attribution | **Self-asserted `actor`, not authentication** | The journal is already the durable audit substrate, so the field is nearly free; real authz needs an auth model that does not exist. §7 says so in those words. |
| Timeout | **Fail-closed, no default payload, not configurable** | s1 §8 mandates it: *"if it is ever added it must be opt-in per node and impossible to configure on a `HumanGate`."* |

**The accepted cost, stated rather than buried.** A `Fail` option and a dead provider both surface as
`RunStatus::Failed`, so they are indistinguishable **by status** — distinguishable only by the reason
text, which `torii run status` renders. That matters for anything filtering on status: a script, or
the terminal allowlist in `count_terminal_before`/`prune_terminal`. (An earlier revision of this
paragraph said `run list-paused` fails to distinguish them. That was a category error caught in
review: `list_paused` filters `status='paused'` and both cases are TERMINAL, so neither ever appears
there at all.) A distinct `Rejected` status would be more truthful but reaches `orchestrator-core`,
both `SchedulerStore` impls, the dbd `CHECK` constraint (a real migration) and torii's rendering —
which would stop s2 being a layer over s1. Deferred deliberately, not overlooked.

## 5. Types

```rust
// orchestrator-core/src/graph.rs
NodeKind::HumanGate {
    options: Vec<GateOption>,
    timeout: Option<chrono::Duration>,   // same bounds as AwaitSignal
}

pub struct GateOption { pub name: String, pub outcome: GateOutcome }

pub enum GateOutcome {
    /// The decision becomes this node's output; dependents run.
    Complete,
    /// `NodeFailed`; hard-edge dependents cascade-skip.
    Fail,
}
```

**The node's output on `Complete`:**

```json
{ "decision": "ship", "actor": "alice", "note": "capped at $5k" }
```

That shape is deliberate — `BranchCond::FieldEquals("decision", "ship")` already matches it, so
`Branch` is reused verbatim with no new condition type. `note` is included because it is usually the
substance of the decision and downstream nodes need it. Both `actor` and `note` are free text and go
through the SP-4 s2 redactor at the fold-read, exactly as s1's payload does.

**Journal (both new variants; `FORMAT_VERSION` stays 1):**

```rust
GateAwaited  { node: NodeId, deadline: Option<DateTime<Utc>>, options: Vec<GateOption> }
GateDecided  { node: NodeId, option: String, actor: String, note: Option<String> }
```

`GateAwaited` carries the full `GateOption`s, **not just their names**. Writing AC1's test
surfaced why: with names alone, the executor could check *membership* against the journaled menu but
would still have to read the **outcome** from the graph — so an author flipping `reject` from `Fail`
to `Complete` after a human rejected would silently change what their recorded answer meant. The
outcome the human was shown ("reject stops the run") is as much a part of the offer as the name.

`GateAwaited` replaces `SignalAwaited` for this node kind and carries the durable menu. `Fold` gains
`gate_decisions: HashMap<NodeId, GateDecision>`, folded **last-wins** like signals so an operator can
correct a decision before the run resumes, and the existing `deadlines` map learns `GateAwaited`
(**first-wins**, unchanged — overwriting *is* the never-expires bug).

## 6. Architecture

### 6.1 The executor splits s1's node into three pieces

s1's `run_await_signal` is six arms and the review found defects in two of them. s2 does not copy it:

| piece | shared | what |
|---|---|---|
| `gate_precheck` | **yes** | arm 0 — a folded `NodeFailed` ⇒ stays failed, checked before anything else |
| `wait_or_expire` | **yes** | arms 2–4 — read-or-journal the absolute deadline, expire loudly, else carry the recorded instant |
| answer-read | per-kind | `signal_for` vs `gate_decision_for` + option→outcome mapping |

The two bug-prone parts — the fail-closed terminal guard and the deadline durability — exist **once**.
`wait_or_expire` REPORTS a `WaitState` and leaves the append to its caller rather than taking an
event constructor, because the awaiting event is the one genuinely per-kind write (`SignalAwaited`
vs `GateAwaited`, the latter also carrying the menu).

**Those rows are in `run_human_gate`'s execution order, and where the answer-read sits is the one
thing the two kinds do differently.** `run_human_gate` reads its decision **last**, after
`wait_or_expire` has had its chance to return `Expired`, so a decision can never approve a gate whose
recorded deadline has passed — whichever order the two rows landed on the journal.
`run_await_signal` reads its signal **second**, between the precheck and `wait_or_expire`, so a
folded signal completes the node even if the deadline has since passed, provided no earlier drive had
already expired it (once one has, `gate_precheck` makes it terminal for both kinds). s2 takes the
stricter order deliberately: a gate whose SLA ran out must not be approvable, and the typed command
is able to say "too late" up front — `torii run gate decide` pre-checks the same journaled instant
with the same `now >= d` boundary, where `torii run signal` has no deadline pre-check at all.

### 6.2 The fold read

| fold state | behaviour |
|---|---|
| failure recorded | `Failed` — arm 0, shared, checked **first** |
| **no menu journaled yet** | **journal `GateAwaited` FIRST, then continue to the rows below** |
| **deadline passed (whatever was decided)** | `NodeFailed` — the timeout, **before any decision is read** |
| decided, option in the journaled menu, `Complete` | `Completed({decision, actor, note})` |
| decided, option in the journaled menu, `Fail` | `NodeFailed("human_gate: node <id> rejected by <actor> (<option>): <reason>")` |
| decided, option **not** in the journaled menu | `NodeFailed`, loudly — §6.3 |
| not decided, deadline not passed | re-pause on the **same** absolute instant |

**The accepted cost of that row order, stated rather than buried.** A decision delivered *inside* the
SLA is DISCARDED if no drive folds it before the deadline: the gate fails on the deadline having
never looked at the fold. The window is not zero — `torii run gate decide` appends and then
`force_wake`s, so it is however long a worker takes to claim that wake. Buying the window back means
reading the decision first, as `run_await_signal` does, and that is exactly the
self-approval-after-expiry hole s1's whole-slice review closed; fail-closed wins. Note that the
failure text says the **deadline** passed and NOT "no decision" — the wording this arm shipped with
said the latter, which is factually false in precisely this case (a decision does exist) and would
send an operator hunting a delivery bug that does not exist, in a durable message `torii run status`
renders and every later drive re-emits. Corrected in `executor/gate.rs`.

**The ask always happens before the answer is read, and that ordering is load-bearing.** s1's
early-signal race "resolves itself for free" because a signal delivered before the node first ran is
simply already in the fold. A durable menu breaks that for free-ness: a `GateDecided` folded with no
`GateAwaited` has nothing to validate against. Rather than special-case it — validating against the
graph in that one path, which would reintroduce exactly the non-durable menu §4 rejects — the node
journals the ask first, unconditionally, and then reads the pending decision against the menu it just
published. The early decision is still honoured in the same execution, so s1's property is preserved;
there is simply never a decision without a menu.

Reachable only by a bypass in practice: the CLI reads the menu from `GateAwaited` to validate, so it
refuses an early decision with *"that node is not awaiting a decision yet"*. The arm exists because
the executor may not assume the CLI was the writer.

### 6.3 Validation bites at write time, and again at read time

The **CLI** reads the graph from `scheduled_runs` (SP-DATA-3 put it there), finds the node, checks the
option against the journaled `GateAwaited` menu, and refuses before anything is journaled — exit 2,
zero rows written.

The **executor** re-checks at the fold-read and is authoritative, because the CLI's check is
non-atomic and the library entry point bypasses it entirely. This is s1's own lesson: *"the CLI can
report the outcome honestly but cannot stop the row existing, so the guard belongs HERE."*

An undeclared option reaching the executor **fails the node loudly**, naming the option and the
journaled menu. It is never ignored: ignoring would leave the gate waiting while the operator was told
their decision landed — the silently-ineffective shape s1's review kept finding.

### 6.4 Determinism

Redact **once**, at the fold-read, and hand that one value to both the node's return and
`publish_context`. s1's rule verbatim: splitting them makes a live run and a replayed run disagree
about the node's output, which surfaces later as a false `DeterminismViolation`. That defect has
shipped and been caught twice in this codebase.

No gateway call and no `EffectRecorded` — the fold **is** this node's memo, so a resumed run re-reads
its decision at zero token cost by construction.

## 7. Trust boundary

**`actor` is ATTRIBUTION, not AUTHENTICATION.** It is whatever string the caller supplied (defaulting
to `$USER`). Anyone who can reach the database can write any actor string. The audit trail therefore
answers *"who claimed to approve"*, not *"who approved"*. Real authorization — identities, roles,
per-gate allowlists, enforcement — needs an authentication model that does not exist anywhere in this
codebase and is its own slice, arguably its own SP.

This is recorded so nobody mistakes the field for an access control and builds a policy on it.

## 8. Operator surface

```
torii run gate approve <run> --node <id> [--as who] [--note text]
torii run gate reject  <run> --node <id> [--as who]  --reason text
torii run gate decide  <run> --node <id> --option <name> [--as who] [--note text]
```

`approve`/`reject` are sugar for `--option approve` / `--option reject` and work only when the gate
declares options by those names. When it does not, the refusal names the real menu, read from the
journaled `GateAwaited`:

```
not delivered: gate "release" has no option "approve".
               Its options are: ship, hold, escalate.
               Use: torii run gate decide <run> --node release --option ship
```

**`--reason` is required on any option whose outcome is `Fail`.** Failing a run without recording why
is the ops equivalent of a bare `catch {}`. On `Complete` options `--note` stays optional.

This one rule is **CLI-layer only**, deliberately, and it is the single place §6.3's two-layer
discipline does not apply. `GateDecided.note` stays `Option<String>` because a `Complete` decision
legitimately has none, so the executor cannot distinguish "no reason given" from "none required"
without re-reading the graph — and an absent reason is a documentation failure, not a safety one. A
library caller that writes a reasonless `Fail` gets a terminal node with an empty reason, which is
ugly and honest. Contrast an undeclared option, which *is* a safety failure and is therefore enforced
in both layers.

**`--as` defaults to `$USER`**, overridable. A CI job recording `actor: "ci"` is the truthful answer.
The help text states it is not authentication.

**`run list-paused` learns gates** rather than gaining a sibling `gate list` — it is already the "what
needs me" command, and `GateAwaited` carries the menu, so it needs no graph load:

```
RUN     NODE      AWAITING                    DEADLINE
7f3a…   release   gate: ship|hold|escalate    2026-08-29T12:00Z
9c1b…   legal     signal                      —
```

**Cross-refusal.** `run signal` on a `HumanGate` is refused (exit 2) pointing at `run gate`, and
`run gate` on an `AwaitSignal` is refused pointing at `run signal`. Without it, raw JSON bypasses every
validation in this slice.

**Accepted risk.** `--note` and `--reason` are argv, so the `ps` / shell-history exposure that
motivated `--payload-file` applies. No `--note-file` is added: a rejection reason is prose, not a
credential channel, and the redactor covers a pasted secret shape. The help text says so plainly, as
`--payload`'s now does.

## 9. Acceptance criteria

Each names the mutation that must break it — this project has produced nine tests that did not guard
the line they appeared to, and every one was caught by asking that question.

| AC | Proven by | Mutation that must break it |
|---|---|---|
| **AC1** The menu is durable | Change the graph's options after the ask; the recorded decision still resolves against the journaled menu | Read options from the graph → fails |
| **AC2** Per-option outcome | `Complete` flows to dependents; `Fail` cascade-skips hard-edge dependents | Map every option to `Complete` → the reject test fails |
| **AC3** Undeclared option fails loudly | A hand-written `GateDecided` with a bogus option | Ignore-and-keep-waiting → fails |
| **AC4** A decision after expiry never resurrects the gate | Expire, then decide; the node stays `Failed` | Move `gate_precheck` after the answer-read → fails |
| **AC5** No default-on-timeout | Expiry produces `NodeFailed`, never an output | Any default payload on expiry → fails |
| **AC6** Conditional exhaustiveness | Missing arm rejected; undeclared arm name rejected; **a gate with no `Branch` is legal** | Drop the check → the first two pass when they must not |
| **AC7** Verb/kind cross-refusal | `run signal` on a gate; `run gate` on an `AwaitSignal` | Drop the kind check → both refusals vanish |
| **AC8** The CLI refuses before appending | An undeclared option leaves **zero** journal rows | Append-then-validate → a durable row appears |
| **AC9** Redaction on both paths | A secret-shaped `note` scrubbed in the journaled event **and** the node's output | Redact only one → live/replayed diverge |
| **AC10** `--reason` required on a `Fail` option | Reject without it is refused | Make it optional → fails |
| **AC11** Zero re-spend on resume | A decided gate replays from the fold; gateway `calls == 0` | Re-ask on resume → calls > 0 |
| **AC12** Cross-process e2e (Postgres) | submit → pause → `list-paused` shows the menu → decide in process B → a fresh `worker serve --once` completes it | Swap `gate decide` for a bare `wake` → the run stays `Paused` |
| **AC14** The ask precedes the answer | A `GateDecided` folded with no `GateAwaited` still resolves: the menu is journaled first, then the decision validates against it, in one execution | Read the decision before journaling the ask → the early-decision test fails with "no menu" |
| **AC13** Additivity | No `HumanGate` ⇒ byte-identical; the workspace suite stays at **1427** + the new tests | — (the baseline guard) |

**Placement.** `graph.rs` for AC1/AC6 · `executor/tests.rs` for AC2–AC5, AC11, AC14 · `cmd/run.rs` +
`tests/cli.rs` for AC7/AC8/AC10 · `redact.rs` + executor for AC9 · `e2e_pg.rs` for AC12.

**AC12 is dev/CI-gated, and the mechanism is WEAKER than `#[ignore]` — worth stating precisely,
because an earlier revision of this paragraph named a mechanism that does not exist.** It requires
Docker Postgres. There is no `#[ignore]` anywhere in `crates/torii` (`rg '#\[ignore' crates/torii`
returns nothing), and there could not be: `#[ignore]` is a compile-time attribute and cannot be
conditioned on an environment variable. What the test actually does is call `db_url()`, which returns
`None` when `DATABASE_URL` is unset or blank, and **return early**. So AC12 is `DATABASE_URL`-guarded
and is **counted among the PASSING tests while having exercised nothing** — the green total says
nothing about whether it ran. The only signal that it skipped is a `SKIP <test name>: DATABASE_URL
not set` line written to the RAW stderr (deliberately not `eprintln!`, which libtest captures and
discards for a passing test). Stated because s1's review flagged exactly this pattern — an acceptance
criterion covered only by a test that does not normally run — and a pass that proves nothing is the
sharper version of it.

**AC13's 1427 is measured**, from `env -u DATABASE_URL cargo test --workspace` at
`103ef28` — not carried forward from a previous slice's spec. s1's spec recorded a baseline that was
wrong by 77 tests, and the next slice would have inherited it. **Landed: 1497 passed / 0 failed /
7 ignored, exit 0** — measured the same way, on the branch tip, and the baseline s3 must not regress.

## 10. Deferred / carry-forward

- **Authorization** (who may decide what) — §7. Needs an identity + authentication model. Its own slice.
- **`RunStatus::Rejected`** — a business rejection and an infrastructure failure are currently
  indistinguishable at run level. §4, "the accepted cost".
- **human-as-Agent** (s3) — a human answering where an `Agent` node would call a model.
- **Non-CLI delivery** (webhook/HTTP) and **N-of-M approval** — inherited from s1 §8.
- **No `OrchestratorHooks` callback** fires for `GateAwaited`/`GateDecided`, matching s1's signal
  events and several pre-existing ones. Not a regression, but a HITL pause is exactly the event a
  live flow-tracking UX would want, so it is recorded as deliberate.
- **`--note-file`** — not added; §8 states the reasoning and the accepted risk.
