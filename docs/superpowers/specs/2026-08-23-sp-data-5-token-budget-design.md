---
title: SP-DATA-5 — per-run token budget (durable ledger, pause on exhaustion)
doctype: design-spec
module: orchestrator
slice: SP-DATA-5
status: approved
date: 2026-08-23
---

# SP-DATA-5 — per-run token budget

## 1. Summary

A **per-run token budget**, set at submit, enforced by the executor, and **durably ledgered in the
journal** so it survives a cross-process resume without re-spending. Exhaustion pauses the run
durably (`resume_after: None`, the HOTL class), so an operator raises the cap with
`torii run wake --budget-tokens N` and the run resumes with **zero re-spend** of its completed prefix.

The crux: **the journal is the ledger.** Spend rides on `EffectRecorded` and is summed during the
same fold that rebuilds the effect memo. Nothing else could give the property this stack requires —
an in-memory counter would restart at zero on every resume, letting a run re-spend its full budget on
each wake, which is the exact class of bug SP-DATA-1/2/3 exist to prevent.

## 2. Motivation

The plumbing is half-built and the missing half is the whole slice:

- `kernel::types::cost::TokenUsage { input_tokens, output_tokens, total_tokens }` exists, and every
  response type carries `usage: Option<TokenUsage>` (`kernel/src/types/io.rs:23,38,54`).
- `crates/gateway/src/budget.rs` exists but solves a **different** problem — `estimate_cost` and
  `filter_by_budget` rank *candidate models* during selection. It is pre-flight model choice, not a
  spend ledger, and it does not survive a request.
- **The executor never reads `response.usage`.** `dispatch_model_turn`
  (`crates/orchestrator/src/executor/agent.rs:783`) consumes `response.tool_calls` and discards the
  usage. No journal event carries spend.

So today a run can spend without limit, and nothing records what it spent.

## 3. Goals / Non-goals

**Goals**
- A per-run token budget supplied at submit and journaled with the run.
- A durable spend ledger folded from the journal, correct across any number of resumes.
- Exhaustion → durable pause, resumable after an operator raises the cap.
- Fail closed: with a budget set, a call that cannot be metered is refused, not spent blind.
- Additive: no budget set ⇒ behaviour byte-identical.

**Non-goals (deferred, §8)**
- Money/cost denomination (needs durable per-model pricing; §8 records the path).
- Fleet-wide or per-tenant budgets.
- Pre-flight estimation to prevent overshoot (output tokens are unknowable before the call).
- Budget-aware model selection (that is `gateway/budget.rs`'s existing concern, unchanged).

## 4. The four decisions, and why

| Decision | Choice | Why |
|---|---|---|
| Exhaustion action | **Durable pause** | Reuses proven `RunPaused` machinery; the run survives with zero re-spend, and torii already has the operator surface. Failing terminally would discard all completed work. |
| Scope | **Per-run, set at submit** | Smallest coherent unit; needs no new durable table; composes with the existing fence and resume. Fleet-wide can layer on later. |
| Unmetered call | **Refuse (fail closed)** | Consistent with every other unenforceable guarantee here — the sandbox refuses without confinement, `shell` refuses without a sandbox, the fence refuses on drift. A budget you cannot measure is not a budget. |
| Denomination | **Tokens** | Exact integers, provider-reported, no pricing table to keep current, no `f64` accumulation in a durable ledger. Honest tradeoff: this caps *consumption*, not spend. |

## 5. Architecture

```
orchestrator-core
  journal.rs    EffectRecorded { …, usage: Option<TokenUsage> }   #[serde(default)]
                RunStarted     { …, budget: Option<TokenBudget> } #[serde(default)]
                BudgetRaised   { new_total_tokens: u64 }          (new event)
  budget.rs     TokenBudget { total_tokens: u64 }                 (new, small)

orchestrator
  executor      Fold gains `spent: u64` and `budget: Option<u64>`
                ONE dispatch chokepoint gates on spent >= budget
                usage captured where the response is received

torii
  run submit --budget-tokens N        (rejects 0)
  run status                          shows spent / budget
  run wake --budget-tokens N          appends BudgetRaised, then force_wake
```

### 5.1 Why usage rides on `EffectRecorded` rather than its own event

A separate `SpendRecorded` event would be additive without touching an existing event's shape, which
is superficially attractive. It is wrong: it makes **two appends per model call**, so a crash between
them leaves a torn pair — spend recorded without its effect, or an effect whose spend vanished.
Putting `usage` inside `EffectRecorded` makes them atomically one append. That single property
decides it.

A separate `run_spend` table was also rejected: it needs its own concurrency story and its own resume
semantics, and it creates two sources of truth for one fact. When the table and the journal disagree,
there is no principled answer to which is right. Riding the journal cannot have that question.

## 6. The fold — and the two traps in it

### 6.1 Sum from the MEMO, not from the event stream

The obvious implementation — sum `usage` across all `EffectRecorded` events — is **wrong**. The
two-phase Mutation path can append an `EffectRecorded` for an effect whose `EffectIntent` was already
journaled, and an in-doubt `Confirmed` reconcile can produce a second record for the same `effect_id`.
The memo is keyed by `effect_id` and absorbs the duplicate harmlessly, but a naive sum would
**double-count that spend on every resume**, with the overcount growing on each wake.

So the fold sums usage across **memo entries**. That is idempotent by construction: one entry per
effect id, however many events produced it.

### 6.2 The gate needs ONE chokepoint, not four

This codebase has already been bitten by exactly this shape. The SP-4 s2 review found the secret
redactor wired into only **1 of the 4** model-output producers, and the fix was a shared
`model_output` chokepoint. A budget gate patched independently into `dispatch_model_turn`, the
`ModelCall` node, the `Map` item path and `Consolidate` will have the same bug — and here the failure
is worse, because an ungated path silently spends past the cap rather than merely failing to redact.

The gate goes at a single dispatch chokepoint. If one does not exist, creating it is part of this
slice, and §7's test matrix proves each of the four producers reaches it.

### 6.3 What counts — and the one path that does NOT

Everything routed through the executor's own gateway on the same `run_id`: Map children, nested
`Subgraph`s, and the planner sub-run at `{expand}/__plan__`. A planner that burns the budget counts
against it.

**Known gap, found during Task 3 and deliberately not closed: `LlmPlannerSelector::select` is
unbudgeted and invisible to the ledger.** It is a fifth model-call site
(`crates/orchestrator/src/executor/selector.rs:70`) that the chokepoint cannot reach by construction:
it holds its **own** `Arc<Gateway>` rather than the executor's field, and the `PlannerSelector` trait
method takes only `(goal, candidates)` — no run, node, or fold context. Its call is also deliberately
**not a journaled effect** (only the resulting `PlannerSelected` is), so usage capture cannot ledger it
either. It costs one call per `PlannerRef::Select` node.

Closing it requires changing a core trait signature — a design decision, not mechanical wiring — so it
is deferred (§8). It is recorded here rather than left implicit because "a producer nobody routed
through the chokepoint" is precisely the failure this slice's gate exists to prevent, and a spec that
claimed full coverage while this path existed would be worse than one that names it.

### 6.3a A gated Map fan-out journals one pause per child

Empirically: 3 live children under an exhausted budget produce 3 `RunPaused` events and 0 gateway
calls. Harmless — `fold_journal` has no `RunPaused` arm, the scheduler reads the last one, all are
byte-identical, and `apply_node_result` takes the first — but the journal noise is proportional to
fan-out width. This is the same shape the pre-existing in-doubt Agent-child pause already has; the
difference is that budget exhaustion hits *every* child at once where an in-doubt Mutation usually hits
one. Deduping would need either a second gate site outside the chokepoint (the very thing §6.2
forbids) or shared per-run mutable state, so it is accepted.

### 6.4 `BudgetRaised`, and why it is required rather than nice-to-have

The budget is journaled on `RunStarted`, so a resumed run folds the **original** cap. Without a way to
change it, an operator who raises the cap and wakes the run gets one that immediately re-pauses —
permanently stuck, and the whole feature useless.

`BudgetRaised { new_total_tokens }` is appended by `torii run wake --budget-tokens N`, folded like
everything else, **latest value wins**. It also leaves an audit trail in the journal of when a cap
moved. Lowering it below current spend is legitimate and is a reasonable way to halt a runaway run at
its next call.

### 6.5 The gate is a floor-trigger, not a ceiling

The gate tests already-accumulated spend **before** each call, so a budget can be overshot by at most
one call — output tokens are unknowable before the call returns. `--budget-tokens` therefore means
"stop once you have spent this much", not "never exceed this". Documented as such in the CLI help;
calling it a hard cap would be a lie.

### 6.6 The ledger must be LIVE within a drive — two defects found in Task 6 and closed

Writing AC6 exposed that §6.5's "at most one call" was **not true as built**. The real bound was
"everything one drive can reach", and a freshly submitted run was **un-gateable entirely**: a
2-node graph under a 100-token cap spent 300 tokens and reported `Completed`. Two independent causes,
both in the wiring rather than in any of §6's decisions:

1. **The fresh-run fold had no budget.** `run_inner` journals `RunStarted{budget}` and then drove the
   graph with `Fold::default()`, whose `budget` is `None`. The cap was durably recorded and then never
   consulted on the one drive it was set for. (A *resume* was fine — `fold_journal` reads it back.)
2. **`Fold` is built once per drive and shared as `&Fold`.** So `fold.spent()` — which Task 3's plan
   named explicitly as the value to pass — is frozen at the drive's starting value. Node 2 gated
   against node 1's *pre-call* ledger; a ReAct agent would burn every one of `max_steps` turns against
   the ledger as it stood before turn 0. On a fresh run that frozen value is 0, permanently.

Together these meant the gate could only ever fire at a **drive boundary** — i.e. on a resume of a run
that had already paused for some *other* reason. The four Task 3 producer tests did not catch it
because each seeds an already-exhausted journal and resumes, which is exactly the one path that worked.

**The fix keeps the single-chokepoint property.** `dispatch_metered` now takes a `Meter<'_>` — a
borrowed view of `(journaled_base, budget, &AtomicU64 live)` — instead of two copied scalars, and
**charges the call back to the ledger itself**, on a successful response, at the same chokepoint that
reads it. So a producer can neither bypass the gate nor forget to account for what it spent, and the
by-effect-id idempotency is untouched: the live counter is zero at the start of every drive and is
subsumed into the journaled base by the next fold, so the two halves can never double-count one call.
`run_inner` additionally seeds the fresh fold's `budget` from the `RunStarted` it just wrote.
`Relaxed` ordering, because a `Map` fan-out shares one view across concurrent children and the gate is
a spend backstop, not a synchronization primitive (§6.3a already accepts concurrent children passing
together).

Mutation-verified both ways — drop the seeded budget, or stop charging the ledger — and in each case
the two new tests fail with the whole graph completed. Unbudgeted runs are unaffected (`budget: None`
⇒ the gate never fires, and the counter is inert), which is why the pre-existing 1325 stayed green.

## 7. Failure modes and testing

| Case | Behaviour |
|---|---|
| Unmetered call, budget set | `NodeFailed`, loud, names the provider |
| Unmetered call, **no** budget | Unchanged; usage ignored; byte-identical |
| Overshoot | Bounded by one call (§6.5) |
| Old journal | `usage: None` + `budget: None` ⇒ ungated and unmetered. **Verify** no `FORMAT_VERSION` bump is needed rather than assuming it |
| `--budget-tokens 0` | Rejected at submit, consistent with `--interval 0s` and `TORII_POOL_SIZE=0` |
| Sum overflow | `u64` **checked** arithmetic — a saturating sum would silently cap the ledger and under-report spend |
| Resume at/over budget | Pauses immediately without dispatching |

**Acceptance criteria.** This slice has a history of tests that did not guard their line — five in
SP-DATA-4 alone — so each of these names the mutation that must break it.

- **AC1 — the double-count guard.** Pause on budget → resume → pause again reports the **same**
  `spent`, not double. *Mutation:* switch the fold to sum from the event stream; this must fail.
- **AC2 — all four producers gated.** One test each for `dispatch_model_turn`, `ModelCall`, `Map`
  item, `Consolidate`. *Mutation:* remove the gate from the chokepoint; all four must fail. If any
  producer cannot reach the gate, that is a finding, not a test to skip.
- **AC3 — unmetered refusal, both ways.** Budget set + `usage: None` ⇒ `NodeFailed` naming the
  provider; no budget + `usage: None` ⇒ proceeds unchanged.
- **AC4 — `BudgetRaised` round trip.** Pause at cap → raise → wake → completes.
- **AC5 — old-journal fold.** A journal serialized without the new fields deserializes and folds
  cleanly, with `spent = 0` and no gate.
- **AC6 — cross-process e2e.** Submit with a budget; exhaust it; a **fresh** worker pauses it;
  `torii run status` shows spent/budget; `wake --budget-tokens N` completes it with **zero re-spend**
  of the completed prefix, asserted by an attributable call counter (not a global `calls.len()` — the
  `scheduled_runs` table is shared across tests).
- **AC7 — additivity.** No budget set anywhere ⇒ `cargo test --workspace` stays at **1302 passed /
  0 failed** plus the new tests, green with and without `DATABASE_URL` at default parallelism.

## 8. Deferred / carry-forward

- **Money denomination.** `Cost` and `ModelPricing` already exist; what is missing is durable,
  current per-model pricing. The token ledger is designed so a cost model can price it
  retrospectively — the journal records raw counts, which is the honest primitive.
- **Budgeting `LlmPlannerSelector::select`** (§6.3). It needs a `PlannerSelector` trait signature
  carrying run/fold context, and a decision about whether the selector's call should become a
  journaled effect so it can be ledgered at all. Bounded exposure today: one call per
  `PlannerRef::Select` node.
- **Fleet-wide / per-tenant budgets**, and a precedence rule against the per-run cap.
- **Pre-flight estimation** to eliminate the one-call overshoot.
- **Budget-aware scheduling** — e.g. refusing to wake a run whose remaining budget cannot plausibly
  finish it.
- **Spend visibility beyond a single run** — an aggregate across runs is a reporting concern needing
  its own query surface.

## 9. Files touched

- `crates/orchestrator-core/src/journal.rs` — two fields, one event.
- `crates/orchestrator-core/src/budget.rs` (new) — `TokenBudget`; `lib.rs` re-export.
- `crates/orchestrator/src/executor/` — `Fold` gains `spent`/`budget`; the dispatch chokepoint and
  its gate; usage capture at the response sites.
- `crates/torii/src/cmd/run.rs`, `src/main.rs` — `--budget-tokens` on submit and wake; `status` shows
  spent/budget.
- `crates/torii/tests/e2e_pg.rs` — AC6.
