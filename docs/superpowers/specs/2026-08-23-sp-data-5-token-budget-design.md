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

### 6.3 What counts

Everything on the same `run_id`, deliberately: Map children, nested `Subgraph`s, and the planner
sub-run at `{expand}/__plan__`. A planner that burns the budget must count against it.

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
