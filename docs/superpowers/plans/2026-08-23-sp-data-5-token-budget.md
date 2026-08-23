# SP-DATA-5 — per-run token budget Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A per-run token budget set at submit, enforced by the executor, durably ledgered in the journal so it survives cross-process resume without re-spending; exhaustion pauses the run for an operator to raise the cap.

**Architecture:** The journal IS the ledger. `usage` rides on `EffectRecorded` (one atomic append with the effect it belongs to), the budget rides on `RunStarted`, and a new `BudgetRaised` event lets an operator move the cap. The fold accumulates spend into a `HashMap<EffectId, TokenUsage>` — keyed by effect id, so a duplicate record overwrites rather than double-counts. A single new metered-dispatch chokepoint gates all four `gateway.execute()` sites.

**Tech Stack:** Rust 2024, `serde` with `#[serde(default)]` for backward-compatible journal fields, existing `orchestrator-core`/`orchestrator`/`torii` crates, Docker `postgres:16`.

**Spec:** `docs/superpowers/specs/2026-08-23-sp-data-5-token-budget-design.md`

**Baseline that must not regress:** `cargo test --workspace` = **1302 passed / 0 failed**, green with AND without `DATABASE_URL` at default parallelism; `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings` both exit 0.

**Database:** container `torii-pg`, port 5433, trust auth, schema applied. `DATABASE_URL=postgres://postgres@localhost:5433/postgres` — **env does not persist between shell calls; prefix every command.** DB tests skip vacuously without it and print `SKIP <test>: DATABASE_URL not set` to fd 2.

---

## Verified facts every task depends on

Confirmed against the source; do not re-investigate:

- `kernel::types::cost::TokenUsage { input_tokens: u32, output_tokens: u32, total_tokens: u32 }`, derives `Debug, Clone, Default, Serialize, Deserialize`.
- `kernel::types::request::InferenceResponse` carries `usage: Option<TokenUsage>`.
- `JournalEvent::RunStarted { version: String }` and `JournalEvent::EffectRecorded { node, effect_id, class, input_hash, seq, output, observation }` — `crates/orchestrator-core/src/journal.rs`.
- `JournalEvent::RunPaused { reason: String, resume_after: Option<DateTime<Utc>> }`.
- `pub const FORMAT_VERSION: i32 = 1;` — `journal.rs:15`.
- `Fold` is a private struct in `crates/orchestrator/src/executor/mod.rs`; `memo: HashMap<EffectId, (String, EffectOutput)>`.
- `fold_journal(events: &[(Seq, JournalEvent)]) -> (Fold, HashMap<NodeId, EffectOutput>, Vec<NodeId>)` — `crates/orchestrator/src/executor/support.rs:67`.
- **Four `gateway.execute()` sites, no shared dispatch helper:** `agent.rs:791` (ReAct turn), `fanout.rs:112` (Map item), `fanout.rs:607` (Consolidate), `mod.rs:784` (`ModelCall` node).
- `model_output(&self, resp: &InferenceResponse) -> serde_json::Value` — `content.rs:53`, the shared OUTPUT chokepoint, called from those same four sites.
- `support::build_request(chain, payload) -> InferenceRequest` — `support.rs:201`.

---

## File structure

```
orchestrator-core
  src/budget.rs      NEW — TokenBudget { total_tokens: u64 }, small and pure
  src/journal.rs     +2 fields (serde-default), +1 event
  src/lib.rs         re-export

orchestrator
  src/executor/mod.rs      Fold gains `usage` + `budget`; ModelCall site
  src/executor/support.rs  fold_journal accumulates usage + budget
  src/executor/dispatch.rs NEW — the metered-dispatch chokepoint + the gate
  src/executor/agent.rs    ReAct site routes through it
  src/executor/fanout.rs   Map + Consolidate sites route through it

torii
  src/cmd/run.rs     --budget-tokens on submit; spent/budget in status; wake raises
  src/main.rs        clap flags
  tests/e2e_pg.rs    AC6
```

---

## Task 1: `TokenBudget` + the journal fields

**Files:** create `crates/orchestrator-core/src/budget.rs`; modify `crates/orchestrator-core/src/journal.rs`, `crates/orchestrator-core/src/lib.rs`.

- [ ] **Step 1: Write the failing test**

Append to `crates/orchestrator-core/src/journal.rs`'s test module (create one if absent, mirroring a sibling module's style):

```rust
    /// An OLD journal — serialized before this slice — must still deserialize, with the
    /// new fields absent rather than erroring. If this fails, the change is a format
    /// break and FORMAT_VERSION must be bumped; the whole additivity claim rests here.
    #[test]
    fn an_old_journal_event_deserializes_with_the_new_fields_absent() {
        let old_started = r#"{"RunStarted":{"version":"v1"}}"#;
        let e: JournalEvent = serde_json::from_str(old_started).expect("old RunStarted still loads");
        match e {
            JournalEvent::RunStarted { budget, .. } => assert!(budget.is_none()),
            other => panic!("wrong variant: {other:?}"),
        }

        let old_recorded = r#"{"EffectRecorded":{
            "node":"n1","effect_id":"e1","class":"Pure","input_hash":"h",
            "seq":0,"output":{"Inline":null},"observation":null}}"#;
        let e: JournalEvent = serde_json::from_str(old_recorded).expect("old EffectRecorded still loads");
        match e {
            JournalEvent::EffectRecorded { usage, .. } => assert!(usage.is_none()),
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn a_budget_round_trips_through_the_journal() {
        let e = JournalEvent::RunStarted {
            version: "v1".into(),
            budget: Some(TokenBudget { total_tokens: 50_000 }),
        };
        let s = serde_json::to_string(&e).expect("serializes");
        let back: JournalEvent = serde_json::from_str(&s).expect("round-trips");
        match back {
            JournalEvent::RunStarted { budget: Some(b), .. } => {
                assert_eq!(b.total_tokens, 50_000)
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }
```

The exact JSON for `EffectRecorded` above may not match the real serde representation of `EffectOutput`/`EffectClass`. **Before writing the test, serialize a real `EffectRecorded` with `serde_json::to_string` and print it**, then hand-edit that string to remove the new field — that gives you a genuine "old" event rather than a guess. Report the actual string you used.

- [ ] **Step 2: Run it and confirm it fails**

Run: `cargo test -p sensei-orchestrator-core journal 2>&1 | tail -20; echo "exit=$?"`
Expected: fails to compile — `TokenBudget` not found, no field `budget`/`usage`.

- [ ] **Step 3: Create `crates/orchestrator-core/src/budget.rs`**

```rust
//! SP-DATA-5: the per-run token budget. Deliberately tiny and pure — the ledger
//! lives in the journal (see `JournalEvent::EffectRecorded.usage`), not here.

use serde::{Deserialize, Serialize};

/// A per-run cap on total tokens, journaled on `RunStarted` and raisable via
/// `BudgetRaised`.
///
/// This caps CONSUMPTION, not spend: 50k tokens costs very different amounts across
/// models. Money denomination is deferred (spec §8) because it needs durable,
/// current per-model pricing, and a stale price would silently make the cap wrong.
///
/// It is a FLOOR-TRIGGER, not a ceiling. The gate tests already-accumulated spend
/// before each call, and output tokens are unknowable before the call returns, so a
/// budget can be overshot by at most one call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenBudget {
    pub total_tokens: u64,
}
```

Add `pub mod budget;` and `pub use budget::TokenBudget;` to `lib.rs`.

- [ ] **Step 4: Extend the journal**

In `journal.rs`, add to `RunStarted`:

```rust
        /// SP-DATA-5: the run's token cap, journaled so a cross-process resume folds
        /// the SAME cap. `None` (and any pre-SP-DATA-5 journal) ⇒ unbudgeted, and the
        /// gate never fires — byte-identical to before.
        #[serde(default)]
        budget: Option<crate::budget::TokenBudget>,
```

and to `EffectRecorded`:

```rust
        /// SP-DATA-5: tokens this effect actually consumed, as reported by the
        /// provider. Rides on THIS event rather than its own so spend and the effect
        /// it belongs to land in ONE atomic append — two appends could be torn by a
        /// crash. `None` for non-model effects and for any pre-SP-DATA-5 journal.
        #[serde(default)]
        usage: Option<kernel::types::cost::TokenUsage>,
```

**`orchestrator-core` does not currently depend on `kernel`** — check `crates/orchestrator-core/Cargo.toml`. If it does not, do NOT add the dependency: `orchestrator-core` is deliberately free of workspace deps ("depends on nothing else in the workspace", per its lib.rs doc). Instead define a local mirror in `budget.rs`:

```rust
/// Mirrors `kernel::types::cost::TokenUsage`. Defined locally because
/// `orchestrator-core` deliberately depends on nothing else in the workspace; the
/// executor converts at the boundary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub total_tokens: u32,
}
```

and use `crate::budget::TokenUsage` in the event. Report which path you took and why.

Add the new event beside `RunPaused`:

```rust
    /// SP-DATA-5: an operator raised (or lowered) the run's cap. Required, not
    /// cosmetic: the budget is journaled on `RunStarted`, so without this a woken run
    /// folds the ORIGINAL cap and immediately re-pauses — permanently stuck. Latest
    /// value wins; lowering below current spend is a legitimate way to halt a run.
    BudgetRaised {
        new_total_tokens: u64,
    },
```

- [ ] **Step 5: Verify, and settle the FORMAT_VERSION question**

Run: `cargo test -p sensei-orchestrator-core > /tmp/t1.log 2>&1; echo "exit=$?"` — expect 0.

Then state explicitly, based on the old-journal test passing: **is a `FORMAT_VERSION` bump needed?** The spec says verify rather than assume. If the old-journal test passes, no bump — say so and leave `FORMAT_VERSION` at 1. If it fails, stop and report; a bump is a much bigger decision than this task.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-core
git commit -m "feat(core): SP-DATA-5 (1/6) — TokenBudget + journaled usage, budget, BudgetRaised"
```

---

## Task 2: the fold accumulates spend — and does not double-count

**Files:** modify `crates/orchestrator/src/executor/mod.rs` (`Fold`), `crates/orchestrator/src/executor/support.rs` (`fold_journal`).

**The trap this task exists to avoid.** Summing `usage` across the raw event stream is wrong: the two-phase Mutation path can append a second `EffectRecorded` for one `effect_id` (an in-doubt `Confirmed` reconcile), and a naive sum would double-count that spend on **every** resume, growing with each wake. Keying by `effect_id` makes it idempotent by construction.

Rather than thread usage into `memo`'s tuple (which would touch every memo access site), add a sibling map with the same key.

- [ ] **Step 1: Write the failing tests**

In `support.rs`'s test module:

```rust
    /// THE guard. Two `EffectRecorded` events for the SAME effect_id — reachable via the
    /// two-phase Mutation path's in-doubt `Confirmed` reconcile — must count ONCE.
    /// Summing the event stream instead of keying by effect id double-counts here, and
    /// the overcount compounds on every resume.
    #[test]
    fn duplicate_effect_records_count_their_usage_only_once() {
        let usage = TokenUsage { input_tokens: 100, output_tokens: 50, total_tokens: 150 };
        let ev = |seq: Seq| {
            (
                seq,
                JournalEvent::EffectRecorded {
                    node: NodeId("n1".into()),
                    effect_id: EffectId("same-id".into()),
                    class: EffectClass::Mutation,
                    input_hash: "h".into(),
                    seq,
                    output: EffectOutput::Inline(serde_json::Value::Null),
                    observation: None,
                    usage: Some(usage),
                },
            )
        };
        let (fold, _, _) = fold_journal(&[ev(0), ev(1)]);
        assert_eq!(
            fold.spent(),
            150,
            "one effect id must contribute its usage once, not once per event"
        );
    }

    #[test]
    fn distinct_effects_sum_their_usage() {
        let mk = |id: &str, total: u32, seq: Seq| {
            (
                seq,
                JournalEvent::EffectRecorded {
                    node: NodeId("n1".into()),
                    effect_id: EffectId(id.into()),
                    class: EffectClass::Pure,
                    input_hash: "h".into(),
                    seq,
                    output: EffectOutput::Inline(serde_json::Value::Null),
                    observation: None,
                    usage: Some(TokenUsage {
                        input_tokens: 0,
                        output_tokens: 0,
                        total_tokens: total,
                    }),
                },
            )
        };
        let (fold, _, _) = fold_journal(&[mk("a", 100, 0), mk("b", 250, 1)]);
        assert_eq!(fold.spent(), 350);
    }

    #[test]
    fn a_budget_is_folded_from_run_started_and_the_latest_raise_wins() {
        let evs = vec![
            (0, JournalEvent::RunStarted {
                version: "v1".into(),
                budget: Some(TokenBudget { total_tokens: 1_000 }),
            }),
            (1, JournalEvent::BudgetRaised { new_total_tokens: 5_000 }),
            (2, JournalEvent::BudgetRaised { new_total_tokens: 2_000 }),
        ];
        let (fold, _, _) = fold_journal(&evs);
        assert_eq!(
            fold.budget(),
            Some(2_000),
            "latest wins — lowering is a legitimate way to halt a run"
        );
    }

    #[test]
    fn an_unbudgeted_run_folds_no_budget_and_no_spend() {
        let evs = vec![(0, JournalEvent::RunStarted { version: "v1".into(), budget: None })];
        let (fold, _, _) = fold_journal(&evs);
        assert_eq!(fold.budget(), None);
        assert_eq!(fold.spent(), 0);
    }
```

Adjust the constructor literals to whatever the real `EffectOutput`/`EffectClass`/`EffectId` shapes require — read the neighbouring `fold_journal_captures_plan_expansions` test for the established idiom rather than guessing.

- [ ] **Step 2: Run and confirm they fail**

Run: `cargo test -p sensei-orchestrator support:: 2>&1 | tail -20; echo "exit=$?"`
Expected: fails to compile — no field `usage`, no method `spent`/`budget`.

- [ ] **Step 3: Implement**

Add to `Fold` in `mod.rs`:

```rust
    /// SP-DATA-5 spend ledger, keyed by effect id — NOT a running total over events.
    /// The two-phase Mutation path can append a second `EffectRecorded` for one id (an
    /// in-doubt `Confirmed` reconcile); keying absorbs that, a sum would double-count
    /// it on every resume.
    usage: HashMap<EffectId, orchestrator_core::TokenUsage>,
    /// The effective cap: `RunStarted.budget`, then the latest `BudgetRaised`.
    budget: Option<u64>,
```

and the two accessors:

```rust
impl Fold {
    /// Total tokens this run has spent, folded from the journal. Idempotent across
    /// any number of resumes because it sums over effect ids, not events.
    fn spent(&self) -> u64 {
        self.usage
            .values()
            .map(|u| u64::from(u.total_tokens))
            .fold(0u64, |acc, t| acc.saturating_add(t))
    }

    fn budget(&self) -> Option<u64> {
        self.budget
    }
}
```

Note `saturating_add` here is deliberate and is the ONE place saturation is acceptable: overflowing `u64` by summing `u32` token counts would require ~4 billion maximal effects, and saturating high makes the gate MORE conservative (it would pause), whereas a wrapping add could silently reset the ledger to near-zero and let a run spend unbounded. Put that reasoning in the comment.

In `fold_journal`, capture `usage` in the `EffectRecorded` arm (`fold.usage.insert(effect_id.clone(), u)` when `Some`) and add arms for `RunStarted { budget, .. }` (set `fold.budget = budget.map(|b| b.total_tokens)`) and `BudgetRaised { new_total_tokens }` (set `fold.budget = Some(*new_total_tokens)`).

- [ ] **Step 4: Verify, then mutation-verify the guard**

Run: `cargo test -p sensei-orchestrator support:: ; echo "exit=$?"` — expect 0.

Then **prove the double-count test guards its line**: temporarily change `spent()` to sum from a `Vec` you accumulate per event instead of the keyed map (or simply make `fold_journal` push into a `Vec<TokenUsage>` and sum that), confirm `duplicate_effect_records_count_their_usage_only_once` FAILS with 300 vs 150, then restore. Report both outputs. A test that passes either way is worthless.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor
git commit -m "feat(orchestrator): SP-DATA-5 (2/6) — fold the spend ledger by effect id, not by event"
```

---

## Task 3: the metered-dispatch chokepoint and the gate

**Files:** create `crates/orchestrator/src/executor/dispatch.rs`; modify `mod.rs` (declare the module, `ModelCall` site at ~784), `agent.rs` (~791), `fanout.rs` (~112 and ~607).

**Why a new file.** There are four `gateway.execute()` sites and no shared helper. SP-4 s2's review found the secret redactor wired into only 1 of 4 model-output producers; patching a gate into four sites independently will reproduce that, and here an ungated path silently spends past the cap.

- [ ] **Step 1: Write the failing tests — one per producer**

In `crates/orchestrator/src/executor/tests.rs`, add four tests, one per path, each: a run with a small budget already exhausted by a seeded journal, driven so that the producer under test would dispatch, asserting the run PAUSES with a budget reason and the gateway call counter stays at 0.

Use the existing `recording_gateway()` from `test_support` for the counter and follow the seeding idiom of the neighbouring resume tests. Name them explicitly so a missing one is obvious:

```
budget_gate_stops_the_react_turn_producer
budget_gate_stops_the_model_call_node_producer
budget_gate_stops_the_map_item_producer
budget_gate_stops_the_consolidate_producer
```

- [ ] **Step 2: Run and confirm all four fail**

Run: `cargo test -p sensei-orchestrator budget_gate 2>&1 | tail -25; echo "exit=$?"`
Expected: all four fail — the run completes and spends instead of pausing.

- [ ] **Step 3: Create the chokepoint**

`crates/orchestrator/src/executor/dispatch.rs`:

```rust
//! SP-DATA-5: the single metered-dispatch chokepoint.
//!
//! Every model call in the executor routes through here so the budget gate cannot be
//! bypassed by a new producer. This mirrors the `model_output` chokepoint on the
//! OUTPUT side and exists for the same reason: SP-4 s2's review found the secret
//! redactor wired into only 1 of the 4 producers. Here the failure would be worse —
//! an ungated path spends real tokens past the operator's cap, silently.

use crate::executor::Executor;
use kernel::types::request::{InferenceRequest, InferenceResponse};

/// Why a metered dispatch refused to run.
pub(super) enum Refusal {
    /// The run has already spent its budget. Carries (spent, budget) for the message.
    BudgetExhausted { spent: u64, budget: u64 },
    /// A budget is set but the provider did not report usage, so this call's spend
    /// would be invisible. Fail closed (spec §4): a budget you cannot measure is not
    /// a budget.
    Unmetered { model: String },
}

impl Executor {
    /// Gate on the folded spend, then dispatch. `spent`/`budget` come from the fold,
    /// so they are correct across any number of resumes.
    pub(super) async fn dispatch_metered(
        &self,
        request: &InferenceRequest,
        spent: u64,
        budget: Option<u64>,
    ) -> Result<Result<InferenceResponse, Refusal>, gateway::GatewayError> {
        if let Some(cap) = budget
            && spent >= cap
        {
            return Ok(Err(Refusal::BudgetExhausted { spent, budget: cap }));
        }
        let response = self.gateway.execute(request).await?;
        if budget.is_some() && response.usage.is_none() {
            return Ok(Err(Refusal::Unmetered { model: response.model.clone() }));
        }
        Ok(Ok(response))
    }
}
```

Verify `gateway::GatewayError` is the real error type the four sites already match on, and match their existing error handling shape rather than inventing one — read `agent.rs:791`'s `match` first. If the sites handle errors differently from each other, keep `dispatch_metered`'s signature aligned with the most common shape and adapt at the minority site.

Declare `mod dispatch;` in `mod.rs`.

- [ ] **Step 4: Route all four sites through it**

Replace each `self.gateway.execute(&request).await` with `self.dispatch_metered(&request, fold.spent(), fold.budget()).await`, threading the fold through if a site does not already have it. Handle `Refusal::BudgetExhausted` by producing a `RunPaused { reason, resume_after: None }` — `None` is the HOTL class, correct here because no amount of waiting fixes an exhausted budget, only an operator decision does. Reason text:

```
format!("budget: {spent} of {budget} tokens spent; raise it with `torii run wake --budget-tokens N`")
```

Handle `Refusal::Unmetered` as a `NodeFailed` naming the model (Task 4 tests this).

- [ ] **Step 5: Verify, then mutation-verify the coverage**

Run: `cargo test -p sensei-orchestrator budget_gate; echo "exit=$?"` — expect 0, four passing.

Then **prove all four tests guard the chokepoint**: temporarily remove the `spent >= cap` check from `dispatch_metered`, confirm **all four** fail, restore. If fewer than four fail, a producer is not routed through the chokepoint — find it and route it. Report the count.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor
git commit -m "feat(orchestrator): SP-DATA-5 (3/6) — one metered-dispatch chokepoint gating all four producers"
```

---

## Task 4: fail closed on an unmetered call, and capture usage

**Files:** modify `crates/orchestrator/src/executor/agent.rs`, `fanout.rs`, `mod.rs` (thread usage into the effect record); `tests.rs`.

- [ ] **Step 1: Write the failing tests**

```rust
    /// Fail closed: with a budget set, a call we cannot meter is refused rather than
    /// spent blind — consistent with the sandbox, `shell`, and the fence.
    #[tokio::test]
    async fn an_unmetered_call_fails_the_node_when_a_budget_is_set() { /* … */ }

    /// …and changes nothing when no budget is set. This is the additivity guarantee:
    /// every existing run is unbudgeted, so today's behaviour must be untouched.
    #[tokio::test]
    async fn an_unmetered_call_is_ignored_when_no_budget_is_set() { /* … */ }

    /// Usage reported by the provider reaches the journal, so a resume folds it.
    #[tokio::test]
    async fn reported_usage_is_journaled_on_the_effect_record() { /* … */ }
```

You will need a gateway double that returns `usage: None` and one that returns `Some(..)`. Check whether `test_support`'s existing doubles set `usage`; if they hardcode `None`, add a `metered_gateway(usage)` helper there rather than duplicating an adapter — `test_support` is already exposed under the `test-support` feature.

- [ ] **Step 2: Run and confirm they fail**

Run: `cargo test -p sensei-orchestrator unmetered 2>&1 | tail -20; echo "exit=$?"`

- [ ] **Step 3: Implement**

At each of the four sites, thread `response.usage` (converted to `orchestrator_core::TokenUsage` if you took the local-mirror path in Task 1) into the `EffectRecorded` append. Find the record call each site uses — they differ — and add the field.

- [ ] **Step 4: Verify, then commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor
git commit -m "feat(orchestrator): SP-DATA-5 (4/6) — journal reported usage; refuse an unmetered call under a budget"
```

---

## Task 5: the torii surface

**Files:** modify `crates/torii/src/cmd/run.rs`, `crates/torii/src/main.rs`.

- [ ] **Step 1: Write the failing tests**

Pure tests first (DB-free): `--budget-tokens 0` is rejected with an actionable message; a positive value parses. Then the `wake` path: waking with `--budget-tokens N` must append a `BudgetRaised` before `force_wake`, and waking WITHOUT it must not.

For the wake test, the existing `RacingStore`/`InMemorySchedulerStore` pattern in `cmd/run.rs` gives you a DB-free harness; you will also need an `InMemoryJournal` to assert the appended event.

- [ ] **Step 2: Run and confirm they fail**

- [ ] **Step 3: Implement**

- `run submit --budget-tokens <N>` — reject 0 (consistent with `--interval 0s` and `TORII_POOL_SIZE=0`), pass the budget into the submit path so it lands on `RunStarted`.
- `run status` — show `spent / budget` when a budget is set, and nothing extra when not (so existing output is unchanged for unbudgeted runs; the existing render tests must still pass).
- `run wake --budget-tokens <N>` — append `BudgetRaised { new_total_tokens: N }` to the journal, THEN `force_wake`. Order matters: waking first could let a worker claim and re-pause the run before the raise lands.

That ordering point deserves a comment in the code — it is a real race, not a stylistic preference.

- [ ] **Step 4: Verify, then commit**

```bash
cargo fmt --all
git add crates/torii/src
git commit -m "feat(torii): SP-DATA-5 (5/6) — --budget-tokens on submit and wake; spent/budget in status"
```

---

## Task 6: cross-process e2e, docs, final verification

**Files:** modify `crates/torii/tests/e2e_pg.rs`; `docs/superpowers/specs/2026-08-22-...` (§10 unaffected), `docs/superpowers/orchestrator-overview.md`.

- [ ] **Step 1: The e2e (AC6)**

Mirror the existing cross-process tests in that file, including the attributable-marker technique — a bare `calls.len()` is flaky because `tick()` drives the whole due set of the shared `scheduled_runs` table. Take the `SCHEDULED_RUNS` guard.

Submit with a small budget against a metered gateway; exhaust it; assert the run is `paused` with a budget reason and `next_wake` NULL (the HOTL class); `torii run status` shows spent/budget; `wake --budget-tokens <larger>` then a fresh-process `serve --once` completes it; and **zero re-spend** of the completed prefix, asserted by the attributable counter.

- [ ] **Step 2: Prove the e2e discriminates**

Temporarily raise the seeded budget so the gate never fires; confirm the test fails (the run completes instead of pausing). Restore. Report both.

- [ ] **Step 3: Docs**

Add an SP-DATA-5 bullet to `orchestrator-overview.md`'s decision log in the established dense style, covering: the journal-as-ledger crux; fold-by-effect-id and why (the duplicate-record double-count); the single dispatch chokepoint and the s2 precedent that motivated it; `BudgetRaised` and why it is required; fail-closed on unmetered; tokens-not-money with the tradeoff named; and the floor-trigger-not-ceiling property. Add the spec + plan to the index table. Update the SP-DATA feature-status line: **all five slices done**.

- [ ] **Step 4: Final verification**

```bash
cargo fmt --all --check;                                              echo "fmt=$?"
cargo clippy --workspace --all-targets -- -D warnings;                echo "clippy=$?"
cargo test --workspace;                                               echo "ws-nodb=$?"
DATABASE_URL=postgres://postgres@localhost:5433/postgres cargo test --workspace; echo "ws-db=$?"
DATABASE_URL=... cargo test -p sensei-orchestrator --features postgres-tests -- --test-threads=1; echo "orch=$?"
DATABASE_URL=... cargo test -p sensei-torii;                          echo "torii=$?"
```

All must exit 0, and the workspace count must be **1302 + the new tests**, green both ways.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "test(torii): SP-DATA-5 (6/6) — cross-process budget e2e; docs"
```

---

## Self-Review

**Spec coverage.** §4's four decisions → Tasks 1 (budget type, journaled), 3 (pause on exhaustion, fail-closed refusal), 5 (per-run at submit). §5 architecture → the file structure above. §6.1 fold-from-memo → Task 2, with the mutation check. §6.2 one chokepoint → Task 3, with a per-producer test matrix and a mutation check that all four fail. §6.3 what counts → follows from folding the whole journal; no extra task. §6.4 `BudgetRaised` → Tasks 1 and 5. §6.5 floor-trigger → documented in Task 1's doc comment and Task 5's CLI help. §7 failure modes → Tasks 1 (old journal, FORMAT_VERSION), 3 (overshoot, resume-at-budget), 4 (unmetered both ways), 5 (`--budget-tokens 0`). AC1→T2, AC2→T3, AC3→T4, AC4→T5, AC5→T1, AC6→T6, AC7→T6.

**Placeholders:** none. Task 4's and Task 5's test bodies are described rather than fully written because they depend on harness shapes the implementer must read first (`test_support`'s doubles, the submit path's signature) — each says exactly what to read and what to assert, which is the honest form here rather than inventing code against an unverified API.

**Type consistency:** `TokenBudget { total_tokens: u64 }` in Task 1 is used unchanged in Tasks 2 and 5. `Fold::spent() -> u64` and `Fold::budget() -> Option<u64>` from Task 2 are the exact signatures Task 3 calls. `Refusal::{BudgetExhausted, Unmetered}` from Task 3 is handled in Tasks 3 and 4.

**Two risks named:**
1. Task 1's `orchestrator-core`-must-not-depend-on-`kernel` constraint may force the local `TokenUsage` mirror. That changes the type in Tasks 2–4; the plan says to report which path was taken so downstream tasks use the right one.
2. Task 3 changes four call sites whose error handling may differ. If they diverge more than expected, the chokepoint's signature may need adjusting — the plan says to read `agent.rs:791` first and align, rather than forcing a shape.
