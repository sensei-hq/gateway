# SP-DATA-5 Budget Clamp Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** On a budgeted run, clamp `Payload::Chat.max_tokens` to what the remaining budget can afford, so the provider enforces the cap instead of our arithmetic.

**Architecture:** One change, at one chokepoint. `Executor::dispatch_metered` (`crates/orchestrator/src/executor/dispatch.rs`) is the single place every model-call producer passes through. After the existing `spent >= cap` check and before `gateway.execute`, a budgeted `Chat` request gets `max_tokens = remaining − pessimistic_input_estimate`; below a floor it takes the existing `BudgetExhausted` durable pause instead of issuing a doomed call. Unbudgeted runs are untouched.

**Tech Stack:** Rust, `tokio`, `tracing`. Tests are plain `cargo test` with `assert!`/`assert_eq!`.

**Spec:** `docs/superpowers/specs/2026-09-03-sp-data-5-budget-clamp-design.md` — criteria referenced as `AC1`…`AC13`.

---

## Preconditions (verified 2026-09-03 — re-verify if time has passed)

- `dispatch_metered(&self, request: &InferenceRequest, meter: &Meter<'_>)` lives at
  `crates/orchestrator/src/executor/dispatch.rs:186`. Its body: acquire the gate if budgeted →
  `let spent = meter.spent()` → `if spent >= cap { return BudgetExhausted }` →
  `self.gateway.execute(request)` → usage check → `meter.record(...)`. **Confirmed.**
- `Meter` exposes `budget() -> Option<u64>` and `spent() -> u64`. **Confirmed** (`dispatch.rs:80`).
- `Payload::Chat { messages, system, max_tokens: Option<u32>, temperature, tools }` —
  `crates/kernel/src/types/request.rs:281`. **Confirmed.**
- `ChatRequest.max_tokens: Option<u32>` reaches the adapter — `crates/kernel/src/types/io.rs`.
  **Confirmed**, so the clamp is observable end-to-end in a test double.
- The orchestrator sets `max_tokens: None` at every site: `support.rs:499`, `support.rs:538`,
  `dispatch.rs:446`, `test_support.rs:785`. **Confirmed** — nothing today sets a real value.
- `est_tokens(s) = s.chars().count() / 4` at `crates/orchestrator/src/agent/prompt.rs:284`, used by
  `over_budget` for the window-fit check. **Confirmed.**
- `input_hash` is computed over `{chain, system, user}`, NOT over `max_tokens` — so the clamp
  cannot trip `DeterminismViolation`. **Confirmed**; Task 7 re-proves it as a test.
- `LatencyMeteredAdapter` (`crates/orchestrator/src/test_support.rs:323`) logs
  `(req.model, prompt)` and does **not** observe `max_tokens`. Task 2 adds a fixture that does.

## Working rules for every task

- **Red first.** Write the test, run it, see it fail *for the stated reason*, then implement. A test
  that passes before the implementation is a finding — say so rather than moving on.
- `cargo fmt --all` before every commit. The pre-commit hook is fmt-check + workspace
  `clippy -D warnings` and runs **no tests** — run `cargo test --workspace` yourself.
- Verify **real** exit codes: `cmd > /tmp/x.log 2>&1; echo "exit=$?"`. Never judge from a piped `tail`.
- **Never** run the DB suite against `$DATABASE_URL` — it is remote Supabase. No task here needs a DB.
- Match the house comment style: long doc comments that argue WHY and record rejected alternatives.
  A comment asserting something false is worse than no comment.

## File structure

| File | Responsibility | Task |
|---|---|---|
| `crates/orchestrator-core/src/budget.rs` | `MIN_OUTPUT_TOKENS` | 1 |
| `crates/orchestrator/src/agent/prompt.rs` | `est_input_tokens_pessimistic` | 3 |
| `crates/orchestrator/src/executor/dispatch.rs` | the clamp, the floor, the two signals | 4, 5, 6 |
| `crates/orchestrator/src/test_support.rs` | a fixture that observes and honours `max_tokens` | 2 |
| `crates/orchestrator/src/executor/tests.rs` | behaviour tests | 4–8 |

---

### Task 1: The floor constant

**Files:**
- Modify: `crates/orchestrator-core/src/budget.rs`
- Modify: `crates/orchestrator-core/src/lib.rs` (re-export)

- [ ] **Step 1: Add the constant**

Append to `budget.rs`, after `TokenUsage`:

```rust
/// The smallest output allowance worth spending input tokens on (SP-DATA-5 clamp).
///
/// Below this, `dispatch_metered` refuses rather than clamping: a reply truncated to a
/// handful of tokens still costs the full input, arrives mid-sentence, and flows
/// downstream as work product with no signal that it was cut short. The existing
/// `BudgetExhausted` pause is louder, recoverable (`torii run wake --budget-tokens`),
/// and already built.
///
/// One constant rather than a per-agent knob, deliberately: a gate agent answering one
/// word needs far less and a planner emitting a graph needs far more, so this WILL be
/// wrong for somebody — which is the argument for the deferred per-role setting (spec
/// §8), not for tuning this number without data.
pub const MIN_OUTPUT_TOKENS: u64 = 256;
```

- [ ] **Step 2: Re-export**

In `crates/orchestrator-core/src/lib.rs`, find the `pub use budget::{...}` list and add
`MIN_OUTPUT_TOKENS`, keeping the existing ordering.

- [ ] **Step 3: Verify it compiles**

Run: `cargo build -p sensei-orchestrator-core > /tmp/t1.log 2>&1; echo "exit=$?"`
Expected: `exit=0`.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-core/src/budget.rs crates/orchestrator-core/src/lib.rs
git commit -m "feat(core): MIN_OUTPUT_TOKENS, the clamp's floor

Below this the gate refuses rather than clamping. A reply truncated to a
handful of tokens still costs the full input, arrives mid-sentence, and
flows downstream as work product with no signal it was cut short."
```

---

### Task 2: A test fixture that observes and honours `max_tokens`

**Files:**
- Modify: `crates/orchestrator/src/test_support.rs`

Every later task's assertions need this. `LatencyMeteredAdapter` logs only `(model, prompt)`, so
today no test can see the clamp.

- [ ] **Step 1: Add the adapter**

Add to `test_support.rs`, beside `LatencyMeteredAdapter`:

```rust
/// Records the `max_tokens` each call carried, and HONOURS it in the usage it reports —
/// `output_tokens = min(scripted_output, max_tokens)`.
///
/// Both halves are load-bearing. Recording is what lets a test assert the clamp was
/// applied at all; honouring is what makes "a budgeted run does not exceed its cap" a
/// real end-to-end claim rather than an assertion about a number we invented. A double
/// that ignored `max_tokens` would let the clamp be deleted with the suite green.
pub struct ClampObservingAdapter {
    /// One entry per call: the `max_tokens` the request carried.
    pub seen: Arc<Mutex<Vec<Option<u32>>>>,
    pub input_tokens: u32,
    /// What the model WOULD emit unclamped; the reported output is capped by `max_tokens`.
    pub scripted_output: u32,
}

impl Model for ClampObservingAdapter {
    fn id(&self) -> &str {
        "r"
    }
}

#[async_trait]
impl ChatModel for ClampObservingAdapter {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError> {
        self.seen
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(req.max_tokens);
        let output = match req.max_tokens {
            Some(cap) => self.scripted_output.min(cap),
            None => self.scripted_output,
        };
        Ok(ChatResponse {
            content: Some("canned-response".into()),
            tool_calls: Vec::new(),
            usage: Some(TokenUsage {
                input_tokens: self.input_tokens,
                output_tokens: output,
                total_tokens: self.input_tokens + output,
            }),
            model: req.model.clone(),
            degraded: false,
        })
    }
}

/// A gateway on chain `"c"` whose adapter records and honours `max_tokens`.
/// Returns the shared `seen` log so a test can assert what reached the provider.
pub async fn clamp_observing_gateway(
    input_tokens: u32,
    scripted_output: u32,
) -> (Gateway, Arc<Mutex<Vec<Option<u32>>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let adapters = AdapterRegistry::new();
    adapters
        .register_chat(Arc::new(ClampObservingAdapter {
            seen: seen.clone(),
            input_tokens,
            scripted_output,
        }))
        .await;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    (Gateway::new(single_chain_config(), adapters, cb), seen)
}
```

Check the imports already in `test_support.rs` (`Arc`, `Mutex`, `TokenUsage`, `ChatRequest`,
`ChatResponse`, `RouterConfig`, `Model`, `ChatModel`, `async_trait`) and add only what is missing.

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p sensei-orchestrator --all-targets > /tmp/t2.log 2>&1; echo "exit=$?"`
Expected: `exit=0`. If `clippy` later flags the struct as unused, that resolves in Task 4.

- [ ] **Step 3: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/test_support.rs
git commit -m "test(orchestrator): a fixture that observes and honours max_tokens

LatencyMeteredAdapter logs only (model, prompt), so no test can currently
see a clamp. This one records what max_tokens each call carried AND caps
its reported output by it — the second half is what makes 'a budgeted run
does not exceed its cap' an end-to-end claim rather than an assertion
about a number we made up."
```

---

### Task 3: The pessimistic input estimate

**Files:**
- Modify: `crates/orchestrator/src/agent/prompt.rs`
- Test: `crates/orchestrator/src/agent/prompt.rs` (its existing inline test module)

- [ ] **Step 1: Write the failing tests**

```rust
/// AC8 — the budget estimate is never below the window-fit one, on prose AND on the
/// JSON-heavy text that is the whole reason it exists.
///
/// `est_tokens` is `chars / 4`. English prose is roughly that; JSON tool schemas and
/// materialized `## Context` outputs tokenize nearer 3 chars/token, so `chars / 4`
/// UNDER-counts exactly where the orchestrator's prompts are heaviest. Clamping on an
/// under-count overshoots by the error, so the budget path needs the bias inverted.
#[test]
fn the_pessimistic_estimate_is_never_below_the_window_fit_one() {
    let prose = "The quick brown fox jumps over the lazy dog, repeatedly and at length.";
    let json = r#"{"name":"fs_write","parameters":{"type":"object","properties":{"path":{"type":"string"},"contents":{"type":"string"}},"required":["path","contents"]}}"#;
    for s in [prose, json, "", "a"] {
        assert!(
            est_tokens_pessimistic(s) >= est_tokens(s),
            "pessimistic must not undercut the window-fit estimate for {s:?}: {} < {}",
            est_tokens_pessimistic(s),
            est_tokens(s)
        );
    }
    assert!(
        est_tokens_pessimistic(json) > est_tokens(json),
        "and must be strictly higher on JSON, which is the case it exists for"
    );
}

/// The empty string costs nothing under either estimate — a boundary a `saturating`
/// arithmetic bug would otherwise hide.
#[test]
fn the_pessimistic_estimate_of_nothing_is_zero() {
    assert_eq!(est_tokens_pessimistic(""), 0);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sensei-orchestrator est_tokens_pessimistic > /tmp/t3.log 2>&1; echo "exit=$?"; tail -20 /tmp/t3.log`
Expected: a COMPILE ERROR — `cannot find function est_tokens_pessimistic`.

- [ ] **Step 3: Implement**

Add to `prompt.rs`, immediately after `est_tokens`:

```rust
/// A deliberately pessimistic token estimate, for the BUDGET path only.
///
/// `est_tokens`'s `chars / 4` is the standard rough figure for English prose. The
/// orchestrator's prompts are not mostly English prose: they carry JSON tool schemas and
/// a `## Context` section rendered from upstream outputs, and JSON tokenizes nearer 3
/// chars/token. So `chars / 4` UNDER-counts precisely where these prompts are heaviest.
///
/// Under-counting is harmless for `est_tokens`'s own caller (a window-fit check that
/// logs and proceeds) and harmful here: the clamp sets `max_tokens = remaining − est`,
/// so an estimate that is too low leaves an allowance that is too high and the cap is
/// overshot by the error. This function inverts the bias — it is wrong in the direction
/// of refusing early rather than overspending.
///
/// `chars / 3` rather than a multiplier on `est_tokens`, so the two are independent: a
/// later change to the window-fit heuristic must not silently move the budget's floor.
/// Neither is a real tokenizer (spec §8 records why one was deferred, and that the
/// fallback it would need is this function).
pub fn est_tokens_pessimistic(s: &str) -> usize {
    s.chars().count().div_ceil(3)
}
```

**Correction applied when this task shipped:** the doc-comment sketch above says under-counting is
harmless for `est_tokens`'s caller, "a window-fit check that logs and proceeds". That is false —
`over_budget` HALTS, raising `PromptOverBudget` and journaling `NodeFailed` (`agent.rs:368`) — and
`est_tokens` has two callers, not one (`over_budget` and `est_prompt_tokens`). The shipped comment
makes the stronger and true argument instead: the two estimates want OPPOSITE biases, since an
over-count halts a turn that would have fitted while an under-count overspends.

- [ ] **Step 4: Run to verify passing**

Run: `cargo test -p sensei-orchestrator est_tokens_pessimistic > /tmp/t3b.log 2>&1; echo "exit=$?"; grep -E '^test result' /tmp/t3b.log`
Expected: `exit=0`, 2 passed.

- [ ] **Step 5: Confirm the window-fit path is untouched (AC9)**

Run: `cargo test -p sensei-orchestrator over_budget > /tmp/t3c.log 2>&1; echo "exit=$?"; grep -E '^test result' /tmp/t3c.log`
Expected: `exit=0`, the existing tests pass **unmodified**. If you changed `est_tokens` or
`over_budget`, revert — this task adds a function, it does not alter one.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/agent/prompt.rs
git commit -m "feat(orchestrator): a pessimistic token estimate for the budget path

est_tokens is chars/4 — the rough figure for English prose. The
orchestrator's prompts carry JSON tool schemas and materialized context
outputs, which tokenize nearer 3 chars/token, so chars/4 UNDER-counts
exactly where they are heaviest.

Harmless for the window-fit caller (which logs and proceeds), harmful for
a clamp: too low an estimate leaves too high an allowance and the cap is
overshot by the error. This inverts the bias. Independent of est_tokens
rather than a multiplier on it, so changing the window-fit heuristic
cannot silently move the budget's floor."
```

---

### Task 4: The clamp

**Files:**
- Modify: `crates/orchestrator/src/executor/dispatch.rs:186-217` (`dispatch_metered`)
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the failing tests (AC1, AC2)**

Add a `mod budget_clamp` to `tests.rs`, next to the existing budget tests:

```rust
/// AC1 — a budgeted Chat request reaches the provider with `max_tokens` set to what the
/// remaining budget can afford.
#[tokio::test]
async fn a_budgeted_call_reaches_the_provider_clamped() {
    let (gateway, seen) = clamp_observing_gateway(10, 5_000).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    journal
        .append(run, run_started_with_budget(10_000))
        .await
        .unwrap();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let (graph, ..) = two_node_graph("a", "b");
    exec.start(run, &graph).await.expect("drives");

    let seen = seen.lock().unwrap_or_else(|e| e.into_inner()).clone();
    assert!(!seen.is_empty(), "the provider was called");
    assert!(
        seen.iter().all(|m| m.is_some()),
        "every budgeted call carries a clamp: {seen:?}"
    );
    assert!(
        seen[0].unwrap() < 10_000,
        "the clamp is below the cap — the prompt's own estimate is subtracted: {:?}",
        seen[0]
    );
}

/// AC2 — an UNBUDGETED run is byte-identical: no clamp, and the estimator is never
/// consulted. This is SP-DATA-5's standing additivity guarantee and the cheapest
/// regression test in the slice.
#[tokio::test]
async fn an_unbudgeted_call_is_not_clamped() {
    let (gateway, seen) = clamp_observing_gateway(10, 5_000).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let (graph, ..) = two_node_graph("a", "b");
    exec.start(run, &graph).await.expect("drives");

    let seen = seen.lock().unwrap_or_else(|e| e.into_inner()).clone();
    assert!(!seen.is_empty(), "the provider was called");
    assert!(
        seen.iter().all(|m| m.is_none()),
        "no budget ⇒ max_tokens stays None: {seen:?}"
    );
}
```

Check the real names of `run_started_with_budget` and `two_node_graph` in `tests.rs` and use what
is actually there — these are the helpers the existing budget tests use.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sensei-orchestrator budget_clamp > /tmp/t4.log 2>&1; echo "exit=$?"; tail -25 /tmp/t4.log`
Expected: `a_budgeted_call_reaches_the_provider_clamped` FAILS — `every budgeted call carries a
clamp: [None, None]`. `an_unbudgeted_call_is_not_clamped` passes already; that is correct, it is a
regression guard, not a red test.

- [ ] **Step 3: Implement the clamp**

In `dispatch_metered`, replace the line `let response = self.gateway.execute(request).await?;` with
the block below. **The shipped version differs in two ways the whole-slice review forced** — it
bounds the emitted value by `Gateway::min_max_output_tokens(chain)`, and `cap - spent` is a
`checked_sub` behind a `debug_assert!` rather than a bare subtraction. See the round-1 section at
the end of this plan; read the source, not this sketch.

```rust
        // SP-DATA-5 clamp. The gate above is a FLOOR-TRIGGER — it refuses once `spent`
        // has already passed the cap — so without this a single call could overshoot by
        // whatever the provider's default output limit happens to be. Setting
        // `max_tokens` moves enforcement from our arithmetic to the provider's: the call
        // CANNOT return more than the remaining budget affords.
        //
        // Only for a budgeted run, and only for `Chat` — `Embed`/`Stt` have no
        // `max_tokens` to set and keep the floor-trigger behaviour unchanged.
        //
        // The request is CLONED and the clone modified: the caller's request must not
        // change under it. That is safe for the memo fence because `input_hash` covers
        // `{chain, system, user}`, not `max_tokens` (see `SelectorDispatch::complete` and
        // `support::input_hash`) — so a clamp that differs between drives still hashes
        // identically and replays from its memo rather than raising
        // `DeterminismViolation`. Guarded by
        // `a_clamped_call_replays_from_its_memo_when_the_budget_moved`.
        let clamped;
        let request = match (meter.budget(), &request.payload) {
            (Some(cap), Payload::Chat { system, messages, tools, .. }) => {
                let est = est_input_tokens(system.as_deref(), messages, tools);
                let allowance = (cap - spent).saturating_sub(est);
                if allowance < orchestrator_core::MIN_OUTPUT_TOKENS {
                    return Ok(Err(Refusal::BudgetExhausted { spent, budget: cap }));
                }
                let mut r = request.clone();
                if let Payload::Chat { max_tokens, .. } = &mut r.payload {
                    // NEVER widen: a caller's own limit wins when it is lower. A clamp
                    // that could raise a caller's ceiling is the "the tool supplies argv
                    // but cannot widen the policy" rule SP-4 s4 established. Today every
                    // orchestrator site passes `None`, so this guards a future caller.
                    let want = u32::try_from(allowance).unwrap_or(u32::MAX);
                    *max_tokens = Some(max_tokens.map_or(want, |caller| caller.min(want)));
                }
                clamped = r;
                &clamped
            }
            _ => request,
        };
        let response = self.gateway.execute(request).await?;
```

`cap - spent` is safe without `saturating_sub`: the gate immediately above returned when
`spent >= cap`. Add `use kernel::types::request::Payload;` if it is not already imported, and
write `est_input_tokens` as a private helper in this module:

```rust
/// The pessimistic input estimate over everything the provider will see: the system
/// prompt, every message body, and the tool schemas. Tool schemas are pure JSON and are
/// the worst case for a chars-per-token heuristic, which is why they are counted rather
/// than waved off as small — `over_budget` already counts them for the same reason.
fn est_input_tokens(
    system: Option<&str>,
    messages: &[kernel::types::request::Message],
    tools: &[kernel::types::request::ToolDefinition],
) -> u64 {
    let est = |s: &str| crate::agent::prompt::est_tokens_pessimistic(s) as u64;
    let mut total = system.map_or(0, est);
    for m in messages {
        total += est(m.content.as_text());
    }
    for t in tools {
        total += est(&t.name) + est(&t.description) + est(&t.parameters.to_string());
    }
    total
}
```

Check `ToolDefinition`'s real field names in `crates/kernel/src/types/request.rs` and match them —
`over_budget` in `prompt.rs` already does this and is the reference.

- [ ] **Step 4: Run to verify passing**

Run: `cargo test -p sensei-orchestrator budget_clamp > /tmp/t4b.log 2>&1; echo "exit=$?"; grep -E '^test result' /tmp/t4b.log`
Expected: `exit=0`, 2 passed.

- [ ] **Step 5: Run the whole suite — the existing budget tests are the real gate**

Run: `cargo test --workspace > /tmp/t4c.log 2>&1; echo "exit=$?"; grep -E '^test result' /tmp/t4c.log | tail -3`

**This step's original expectation ("`exit=0`, 0 failed … fix the clamp, not the test") was
WRONG, and Task 4 corrected it in place.** Seven existing budget tests reddened, all with the
same symptom — zero gateway calls where the test expected one or more:

`a_fresh_budgeted_run_pauses_mid_drive_after_one_call`, `spending_exactly_the_cap_stops_the_run`,
`a_budgeted_agent_stops_between_react_turns`,
`a_budgeted_map_fanout_dispatches_exactly_one_child_before_the_gate_fires`,
`a_compacted_map_cannot_let_a_budgeted_run_overshoot_across_drives`,
`an_unmetered_call_fails_the_node_when_a_budget_is_set`,
`a_re_driven_selector_replays_its_call_instead_of_respending`.

The cause is not a defect: every one of them uses a cap of 100–700 tokens, chosen when no floor
existed, and `MIN_OUTPUT_TOKENS` is 256 — so the clamp refuses those runs *before the first call*.
That is spec §6's named accepted cost, "a run can now pause where it previously completed",
landing on stale fixtures. The fix is the fixture: each test's token magnitudes are multiplied by
a common factor (10, except 16/3 for `spending_exactly_the_cap_stops_the_run`, whose two calls
must still land exactly on the cap), preserving every ratio, call count, pause site and reason
string. The reasoning is recorded once, on `run_started_with_budget`'s doc comment.

The gate was **not** weakened to accommodate this, and that was verified rather than asserted:
delete the `spent >= cap` block and eleven tests redden, nine of them on the clamp's own
`debug_assert!` ("the `spent >= cap` gate was bypassed or reordered"). That assertion replaced a
bare `cap - spent` in review round 1 — the overflow panic it relied on is debug-only, and a release
build would have wrapped instead. See the round-1 section below.

**⚠️ One coverage loss, carried to Task 6.** `spending_exactly_the_cap_stops_the_run`'s documented
mutation (`spent >= cap` → `spent > cap`) **no longer reddens it** — re-run to confirm, not
reasoned about. At `spent == cap` the clamp's floor sees `allowance == 0` and returns an identical
`BudgetExhausted`, so the two arms are observationally the same for a `Chat` payload. The boundary
is now covered twice rather than left unsafe, but nothing distinguishes `>` from `>=` any more.
The only payload the clamp skips is a non-`Chat` one, so **Task 6's
`a_non_chat_payload_is_gated_but_not_clamped` should carry that guard**: land an `Embed` dispatch
exactly on the cap and mutation-check `>=` → `>` there.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/dispatch.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): clamp a budgeted call to what the budget affords

The gate is a floor-trigger — it refuses once spent has already passed the
cap — so a single call could overshoot by whatever the provider's default
output limit is. Setting max_tokens moves enforcement to the provider: the
call cannot return more than remaining affords.

Clones the request rather than mutating the caller's. Safe for the memo
fence because input_hash covers {chain, system, user}, not max_tokens.

Never widens a caller's own max_tokens — takes the min. Every orchestrator
site passes None today, so that guards a future caller."
```

---

### Task 5: The floor

**Files:**
- Test: `crates/orchestrator/src/executor/tests.rs`

The refusal shipped in Task 4's code. This task proves it, including the part most likely to be
silently wrong.

- [ ] **Step 1: Write the failing tests (AC4, AC5)**

```rust
/// AC4 — below the floor the gate refuses, and **makes no gateway call**. Asserted on
/// the call log, not on the outcome: a test that only checked the pause would pass even
/// if we spent the input tokens first and threw the reply away, which is the exact waste
/// the floor exists to prevent.
#[tokio::test]
async fn below_the_floor_the_gate_refuses_without_calling_the_provider() {
    // A cap barely above MIN_OUTPUT_TOKENS: the prompt's own estimate pushes the
    // allowance under the floor.
    let cap = orchestrator_core::MIN_OUTPUT_TOKENS + 5;
    let (gateway, seen) = clamp_observing_gateway(10, 100).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    journal
        .append(run, run_started_with_budget(cap))
        .await
        .unwrap();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let (graph, ..) = two_node_graph("a", "b");
    let out = exec.start(run, &graph).await.expect("drives");

    assert!(out.paused.is_some(), "the run pauses on the budget: {out:?}");
    assert!(
        seen.lock().unwrap_or_else(|e| e.into_inner()).is_empty(),
        "and NO call was made — the point of the floor is not paying for a doomed reply"
    );
}

/// AC5 — an estimate larger than the whole remaining budget must not wrap. With
/// `saturating_sub` the allowance is 0, which is below the floor, so the run pauses.
/// Without it the subtraction underflows and the allowance becomes enormous — the clamp
/// would then be wider than the cap, i.e. worse than no clamp at all.
#[tokio::test]
async fn an_estimate_larger_than_the_budget_does_not_wrap() {
    let (gateway, seen) = clamp_observing_gateway(10, 100).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    journal
        .append(run, run_started_with_budget(1))
        .await
        .unwrap();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let (graph, ..) = two_node_graph("a", "b");
    let out = exec.start(run, &graph).await.expect("drives");

    assert!(out.paused.is_some(), "a 1-token budget pauses: {out:?}");
    assert!(
        seen.lock().unwrap_or_else(|e| e.into_inner()).is_empty(),
        "no call was made"
    );
}
```

- [ ] **Step 2: Run**

Run: `cargo test -p sensei-orchestrator -- below_the_floor an_estimate_larger > /tmp/t5.log 2>&1; echo "exit=$?"; grep -E '^test result' /tmp/t5.log`
Expected: `exit=0`, 2 passed — Task 4 implemented both behaviours. **A green first run is
acceptable here only because Step 3 proves the tests are not vacuous.**

- [ ] **Step 3: Mutation-prove both**

Each mutation applied to `dispatch.rs`, run, then **reverted**. Confirm the tree is clean with
`git status -s` afterwards.

| Mutation | Must redden |
|---|---|
| Delete the `if allowance < MIN_OUTPUT_TOKENS { … }` block | `below_the_floor_the_gate_refuses_without_calling_the_provider` |
| `(cap - spent).saturating_sub(est)` → `(cap - spent) - est` | `an_estimate_larger_than_the_budget_does_not_wrap` (panics on underflow in debug) |

Run each as: `cargo test -p sensei-orchestrator -- below_the_floor an_estimate_larger > /tmp/m.log 2>&1; echo "exit=$?"`
Expected under each mutation: `exit=101`. Quote the failure. If a mutation leaves the suite green,
the test is not guarding what it claims — fix it before continuing.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/tests.rs
git commit -m "test(orchestrator): the clamp's floor, mutation-proven

Below the floor the gate refuses AND makes no call — asserted on the call
log, because a test checking only the pause would pass even if we spent
the input tokens first and discarded the reply, which is the waste the
floor exists to prevent.

And the estimate-exceeds-budget case does not wrap: without saturating_sub
the allowance underflows to something enormous and the clamp becomes wider
than the cap, worse than no clamp at all. Both proven by mutation."
```

---

### Task 6: Never widen, and non-`Chat` payloads

**Files:**
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the tests (AC3, AC6)**

AC3 needs a caller-supplied `max_tokens`, which no orchestrator site sets — so test
`dispatch_metered` directly rather than through `start`. Find how the existing budget tests
construct a `Meter` and an `InferenceRequest` and follow that shape.

```rust
/// AC3 — a caller's own `max_tokens` is never WIDENED. Both directions, because a `min`
/// written the wrong way round passes a one-sided test.
///
/// A clamp that could raise a caller's ceiling is the "the tool supplies argv but cannot
/// widen the policy" rule SP-4 s4 established. Nothing in the orchestrator sets
/// `max_tokens` today, so this guards a future caller rather than a present one.
#[tokio::test]
async fn a_callers_max_tokens_is_never_widened() {
    // Caller asks for LESS than the allowance ⇒ the caller's value survives.
    let seen = dispatch_once_with(Some(16), 100_000).await;
    assert_eq!(seen, Some(16), "a lower caller value must win");

    // Caller asks for MORE than the allowance ⇒ the allowance wins.
    let seen = dispatch_once_with(Some(1_000_000), 100_000).await;
    assert!(
        seen.unwrap() < 1_000_000,
        "a higher caller value must be clamped down, not honoured: {seen:?}"
    );
}

/// AC6 — a non-`Chat` payload on a budgeted run is untouched and still gated by the
/// pre-existing rule. `Embed` has no `max_tokens` to set; the clamp must skip it rather
/// than panic or refuse.
#[tokio::test]
async fn a_non_chat_payload_is_gated_but_not_clamped() {
    // Build a budgeted Embed request through dispatch_metered and assert it dispatches
    // normally (no panic, no BudgetExhausted while under cap).
}
```

Write `dispatch_once_with(caller_max: Option<u32>, cap: u64) -> Option<u32>` as a local helper that
builds a budgeted `Meter`, calls `dispatch_metered` once against `clamp_observing_gateway`, and
returns the single recorded `max_tokens`. **Fill in `a_non_chat_payload_is_gated_but_not_clamped`'s
body** using the same helper shape with a `Payload::Embed { texts: vec!["x".into()] }`; the stub
above is a signature sketch, not a finished test — do not leave it empty.

**⚠️ Carried forward from Task 4 — this test must also re-home a guard the clamp took away.**
`spending_exactly_the_cap_stops_the_run` used to be the only thing pinning `spent >= cap` against
`spent > cap`, and it no longer is: at `spent == cap` the clamp's floor computes `allowance == 0`
and returns an identical `BudgetExhausted`, so the mutation leaves it green (verified by running
it, not by reasoning). A non-`Chat` payload is the only one the clamp SKIPS, so this test is now
the natural home for that boundary: dispatch an `Embed` with the meter sitting exactly on the cap,
assert it is refused, and mutation-check `spent >= cap` → `spent > cap` reddens it. Without that,
the `>=` boundary ships unguarded for the first time since it was pinned.

- [ ] **Step 2: Run to verify failure, then implement if needed**

Run: `cargo test -p sensei-orchestrator -- never_widened non_chat_payload > /tmp/t6.log 2>&1; echo "exit=$?"; tail -20 /tmp/t6.log`
Task 4's code should satisfy both. If either fails, fix `dispatch.rs`.

- [ ] **Step 3: Mutation-prove the `min`**

Mutate `caller.min(want)` → `caller.max(want)`, run, confirm
`a_callers_max_tokens_is_never_widened` reddens, revert. Quote the failure.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/tests.rs
git commit -m "test(orchestrator): the clamp never widens, and skips non-Chat

Both directions of the min, because one written backwards passes a
one-sided test — mutation-proven with caller.min -> caller.max.

Embed has no max_tokens to set: the clamp skips it and the pre-existing
floor-trigger gate still applies."
```

---

### Task 7: The memo fence holds under a moving clamp

**Files:**
- Test: `crates/orchestrator/src/executor/tests.rs`

The precondition Task 4's comment leans on. If it is ever false, every budgeted resume becomes a
hard halt — so it gets a test rather than a comment.

- [ ] **Step 1: Write the test (AC13)**

```rust
/// AC13 — a clamped call replays from its memo on resume even though the clamp VALUE
/// differs between drives.
///
/// `input_hash` covers `{chain, system, user}` — the semantic inputs — not `max_tokens`.
/// So a second drive whose remaining budget (and therefore whose clamp) has moved still
/// hashes identically and replays rather than raising `DeterminismViolation`.
///
/// This is a precondition, not a nicety: fold `max_tokens` into that hash and EVERY
/// budgeted resume becomes a hard halt. The test exists so that change cannot land
/// quietly.
#[tokio::test]
async fn a_clamped_call_replays_from_its_memo_when_the_budget_moved() {
    let (gateway, seen) = clamp_observing_gateway(10, 100).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    journal
        .append(run, run_started_with_budget(50_000))
        .await
        .unwrap();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let (graph, ..) = two_node_graph("a", "b");
    exec.start(run, &graph).await.expect("first drive");
    let after_first = seen.lock().unwrap_or_else(|e| e.into_inner()).len();

    // Raise the budget so a re-drive would compute a DIFFERENT clamp, then re-drive.
    journal
        .append(run, budget_raised(500_000))
        .await
        .unwrap();
    let out = exec.start(run, &graph).await.expect("resumes");

    assert!(out.failed.is_none(), "no DeterminismViolation: {:?}", out.failed);
    assert_eq!(
        seen.lock().unwrap_or_else(|e| e.into_inner()).len(),
        after_first,
        "the completed calls replayed from their memos — no new provider call"
    );
}
```

Check the real name of the `BudgetRaised` helper in `tests.rs` and use it.

- [ ] **Step 2: Run**

Run: `cargo test -p sensei-orchestrator a_clamped_call_replays > /tmp/t7.log 2>&1; echo "exit=$?"; grep -E '^test result' /tmp/t7.log`
Expected: `exit=0`, 1 passed.

- [ ] **Step 3: Prove it is a real guard**

In `support::input_hash`'s caller, temporarily add `max_tokens` to the hashed JSON. Re-run:
the test must FAIL with `DeterminismViolation`. Revert; confirm `git status -s` is clean.

If it does NOT fail, the test is not exercising resume — fix it before continuing.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/tests.rs
git commit -m "test(orchestrator): a moving clamp does not break the memo fence

input_hash covers {chain, system, user}, not max_tokens, so a clamp that
differs between drives still replays. A precondition rather than a
nicety: fold max_tokens into that hash and every budgeted resume becomes
a hard halt. Proven by doing exactly that and watching this redden."
```

---

### Task 8: The two signals, and the arithmetic claim

**Files:**
- Modify: `crates/orchestrator/src/executor/dispatch.rs`
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the test (AC7)**

```rust
/// AC7 — the spec's §4 claim, asserted as arithmetic rather than prose: a budgeted run's
/// total spend does not exceed `cap + (actual_input − est_input)`.
///
/// The fixture's adapter HONOURS `max_tokens`, so this is an end-to-end measurement, not
/// an assertion about a number we invented. Against today's behaviour the difference is
/// the point: unclamped, a single call could overshoot by the provider's whole default
/// output limit (here 5000 against a 2000 cap).
#[tokio::test]
async fn a_budgeted_run_does_not_overshoot_beyond_the_estimate_error() {
    let (gateway, _seen) = clamp_observing_gateway(10, 5_000).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let cap = 2_000u64;
    journal
        .append(run, run_started_with_budget(cap))
        .await
        .unwrap();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let (graph, ..) = two_node_graph("a", "b");
    exec.start(run, &graph).await.expect("drives");

    let spent = total_spend_from_journal(&journal, run).await;
    assert!(
        spent <= cap + 64,
        "spend {spent} must not exceed the cap {cap} by more than the input-estimate \
         error; unclamped this run would have spent ~5000 per call"
    );
}
```

Write `total_spend_from_journal` as a local helper summing `EffectRecorded.usage.total_tokens`,
or reuse whatever the existing budget tests already use for this — check first.

- [ ] **Step 2: Add the two signals (AC10, AC11)**

In `dispatch_metered`, after `let Some(usage) = &response.usage else { … };` and before
`meter.record(...)`:

```rust
        // Two DISTINCT diagnostics, deliberately `tracing` records and not journal
        // events: they describe our own estimator, not run state. Nothing folds them, no
        // resume depends on them, and no operator decision keys on them — so making them
        // durable would cost a FORMAT_VERSION concern and a fold arm to carry what the
        // ledger already implies (`usage` is journaled; `allowance` is recomputable).
        if let Some(allowance) = clamp_applied {
            if u64::from(usage.output_tokens) >= allowance {
                // The reply was cut short by OUR budget, not by the model finishing.
                // Inferred from the token count because `InferenceResponse` carries no
                // finish reason — only a streaming chunk does.
                tracing::info!(
                    allowance,
                    output_tokens = usage.output_tokens,
                    "budget clamp bit: the reply was truncated by the run's token budget"
                );
            }
            if u64::from(usage.input_tokens) > est_used {
                // The residual overshoot the spec's §4 bounds. Emitted so the estimator's
                // error is measurable in production rather than assumed.
                tracing::warn!(
                    estimated = est_used,
                    actual = usage.input_tokens,
                    "budget clamp under-estimated the input; the cap may be exceeded by \
                     the difference"
                );
            }
        }
```

This needs `clamp_applied: Option<u64>` and `est_used: u64` bound in the clamp block from Task 4 —
extend that block to set them (`None`/`0` on the unbudgeted and non-`Chat` paths).

- [ ] **Step 3: Run**

Run: `cargo test --workspace > /tmp/t8.log 2>&1; echo "exit=$?"; grep -E '^test result' /tmp/t8.log | tail -3`
Expected: `exit=0`, 0 failed.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/dispatch.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): measure the clamp instead of trusting it

Two tracing records, not journal events: they describe our estimator, not
run state. Nothing folds them and no resume depends on them, so making
them durable would cost a FORMAT_VERSION concern to carry what the ledger
already implies.

'The clamp bit' is inferred from output_tokens >= allowance, because
InferenceResponse carries no finish reason — only a streaming chunk does.

And the §4 claim is now arithmetic in a test: spend stays within
cap + (actual_input - est_input). Unclamped that run would have spent
~5000 tokens per call against a 2000 cap."
```

---

### Task 9: Docs and the release gate

- [ ] **Step 1: Full verification, real exit codes**

```bash
cargo test --workspace > /tmp/gate1.log 2>&1; echo "exit=$?"; grep -E '^test result' /tmp/gate1.log | awk '{p+=$4; f+=$6; i+=$8} END {print "passed="p" failed="f" ignored="i}'
cargo clippy --workspace --all-targets -- -D warnings > /tmp/gate2.log 2>&1; echo "exit=$?"
cargo fmt --all --check; echo "exit=$?"
```
All three must be `exit=0`. Record the counts.

- [ ] **Step 2: Doc-link baseline**

```bash
cargo clean --doc && cargo doc --workspace --no-deps --document-private-items 2>&1 | grep -c 'unresolved link'
```
Expected: **16**, the current baseline. Higher means this slice added broken links — fix them.

- [ ] **Step 3: Update the SP-DATA-5 spec's §8**

`docs/superpowers/specs/2026-08-23-sp-data-5-token-budget-design.md` §8 lists
"**Pre-flight estimation** to eliminate the one-call overshoot" as deferred. Mark it addressed,
linking to `2026-09-03-sp-data-5-budget-clamp-design.md`, and state precisely what changed: the
overshoot is now **bounded by the input-estimate error and biased toward refusing early**, not
eliminated. Do not write "eliminated" — §2's non-goal and §4 both say otherwise, and this
codebase has spent a slice on exactly that class of false claim.

Also update §2's non-goals bullet ("Pre-flight estimation to prevent overshoot (output tokens are
unknowable before the call)") — the reasoning was sound but the conclusion no longer holds, because
clamping bounds the cost without predicting it. Say that, rather than deleting the line.

- [ ] **Step 4: Update the overview**

`docs/superpowers/orchestrator-overview.md`'s SP-DATA-5 entry ends with a "Still open from s5"
list naming pre-flight estimation. Move it to done with a one-line description in the house style.

**Review round 1 did the rest of this sweep already** — four surfaces described the pre-clamp
contract and none said a budgeted run can now pause with `spent < cap`, which is the wrong mental
model for debugging one that paused at zero spent. All four now say both things (the output half is
bounded by `max_tokens`; the run can refuse before the cap):

- `crates/orchestrator-core/src/budget.rs` — `TokenBudget`'s "FLOOR-TRIGGER … overshot by at most
  one call"
- `crates/orchestrator/src/executor/dispatch.rs` — `dispatch_metered`'s own contract, which said
  "output tokens are unknowable until the call returns" forty lines above the block that bounds them
- `crates/torii/src/main.rs` — the `--budget-tokens` help, which is what an operator actually reads
- `crates/torii/tests/e2e_pg.rs` — the AC6 e2e's step 1

So Step 4 is the overview entry only. Re-check the four above rather than assuming.

- [ ] **Step 5: Checkpoint**

Rewrite `docs/CHECKPOINT.md` (**under 40 lines**, one current entry): what shipped, the measured
numbers from Step 1, the next command. Note the sensei daemon is not running, so it is the only
durable record.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add docs/
git commit -m "docs: the budget clamp, and the claim it is careful not to make"
```

---

---

## Whole-slice review round 1 — what changed, and what the plan had wrong

Three reviewers read Tasks 1–4 at `c301901` and raised twenty findings at Minor or above. Every one
is addressed on `develop`; the ones that changed the DESIGN rather than a comment are recorded here
so a later reader does not have to reconstruct them from the log.

**Two Criticals, both real.**

1. *The clamp had no upper bound.* `allowance = remaining − est` is a pure budget figure, so at a
   cap of 10240 it emitted `Some(10239)` and the fixture provider answered a 400: a run that
   succeeds unbudgeted hard-FAILED at its first call the moment an operator set `--budget-tokens`.
   Fixed with a new read-only `Gateway::min_max_output_tokens(chain)` — the output twin of
   `min_context_window` — and `min(allowance, ceiling)` in the clamp. See the spec's §5.2. Task 2's
   `ClampObservingAdapter` now REFUSES an over-large `max_tokens` so this cannot regress unseen.
2. *The Postgres AC6 e2e was left un-rescaled.* `CAP = 100 / PER_CALL = 150` in
   `crates/torii/tests/e2e_pg.rs` falls under the floor, so the first call never dispatched. It is
   `#[cfg_attr(not(have_database_url), ignore)]`, so the local suite structurally cannot see it and
   CI would have gone red on a required check. Scaled ×10 and verified with an in-process replica.

**Task 9 Step 1's gate list is incomplete.** `cargo test --workspace` on a dev box SKIPS the
Postgres suites. Any change to budget behaviour must also be reasoned about (or replicated in
process) for:

- `crates/torii/tests/e2e_pg.rs` — `a_budget_exhausted_run_is_raised_by_an_operator_and_completes_in_a_fresh_process`

It is the only DB-gated test in the workspace that sets a `TokenBudget`; the other five in
`executor/tests.rs` and the `orchestrator-store` suites do not.

**Tasks 5, 6 and most of 8 landed in this round**, because the findings that named them were
"UNTESTED" findings and the remedy is the test. All the mutations the plan asks for were run and
their failures are quoted in the commit messages. What is NOT yet landed from Task 8: the two
`tracing` signals (AC10/AC11). No finding touched them and they need no behaviour change.

**Three plan/spec claims corrected in place:**

- Task 4 Step 5's rescale factor was "ten, except 16/3". It was also 100 for one fixture and now
  needs to be 50 for another (`a_re_driven_selector_replays_its_call_instead_of_respending`, whose
  cap of 1000 left the second of its five drives under the floor, so the mutation the test
  documents never reached either of its assertions).
- Task 4 Step 5's mutation evidence quoted "attempt to subtract with overflow". That tripwire was
  DEBUG-ONLY — the workspace release profile sets no `overflow-checks` — so `cap - spent` is now a
  `checked_sub` behind an explicit `debug_assert!`: loud in debug, fail-closed in release.
- Task 6's carried-forward `>=` boundary is re-homed onto the `Embed` path as planned, AND is
  pinned again on `spending_exactly_the_cap_stops_the_run`, because the floor no longer renders the
  same message as the gate.

**One direction not followed, and why.** Finding 1 proposed that as "a floor of correctness" the
clamp should also take `min(…, 1024)`, since three adapters substitute that for a `None`
`max_tokens` and the clamp therefore widens their per-call ceiling. Rejected: it would hard-cap
every budgeted reply at another provider's arbitrary fallback constant, silently truncating
budgeted runs on the `openai_compat` and local paths where `None` means the model's own maximum,
and would import that constant into the orchestrator. The widening is real, bounded by the model's
own limit, and is now written down as an accepted cost in the spec's §6 rather than fixed by
creating a worse defect.

## Self-review

**Spec coverage** — every AC maps to a task:

| AC | Task | AC | Task |
|---|---|---|---|
| AC1 clamped request | 4 | AC8 pessimistic ≥ window-fit | 3 |
| AC2 unbudgeted byte-identical | 4 | AC9 window-fit unchanged | 3 |
| AC3 never widen | 6 | AC10 clamp-bit signal | 8 |
| AC4 floor refuses, no call | 5 | AC11 estimate-wrong signal | 8 |
| AC5 no wrap | 5 | AC12 usage still journaled | 4 (suite) |
| AC6 non-`Chat` untouched | 6 | AC13 memo fence | 7 |
| AC7 arithmetic claim | 8 | | |

**Gap found and closed:** AC12 (a clamped call still journals its real `usage` and folds by effect
id) had no dedicated test — it was riding on "the existing budget suite still passes". That is
weaker than it sounds, because those tests run *unclamped* paths. Add to Task 8:

```rust
/// AC12 — a CLAMPED call still journals its real usage and folds by effect id. The
/// existing budget suite proves this for unclamped calls only, so without this the fold
/// could silently mis-handle a clamped one.
#[tokio::test]
async fn a_clamped_call_still_journals_its_real_usage() {
    let (gateway, seen) = clamp_observing_gateway(10, 5_000).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    journal.append(run, run_started_with_budget(2_000)).await.unwrap();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let (graph, ..) = two_node_graph("a", "b");
    exec.start(run, &graph).await.expect("drives");

    let allowance = seen.lock().unwrap_or_else(|e| e.into_inner())[0].unwrap();
    let events = journal.load(run).await.unwrap();
    let recorded: Vec<_> = events
        .iter()
        .filter_map(|(_, e)| match e {
            JournalEvent::EffectRecorded { usage: Some(u), .. } => Some(*u),
            _ => None,
        })
        .collect();
    assert!(!recorded.is_empty(), "usage was journaled");
    assert_eq!(
        u32::from(recorded[0].output_tokens),
        allowance.min(5_000),
        "the journaled output is the CLAMPED count the provider really returned, not \
         the unclamped script"
    );
}
```

**Placeholder scan:** one sketch remained — `a_non_chat_payload_is_gated_but_not_clamped` in
Task 6 — and Step 1 now explicitly instructs filling it in rather than leaving the stub.

**Type consistency:** `MIN_OUTPUT_TOKENS: u64`, `est_tokens_pessimistic(&str) -> usize`,
`est_input_tokens(Option<&str>, &[Message], &[ToolDefinition]) -> u64`,
`clamp_observing_gateway(u32, u32) -> (Gateway, Arc<Mutex<Vec<Option<u32>>>>)`,
`clamp_applied: Option<u64>`, `est_used: u64`. Each defined once and used consistently. Note the
`usize`→`u64` conversion in `est_input_tokens` and the `u64`→`u32` conversion at the `max_tokens`
boundary (`u32::try_from(...).unwrap_or(u32::MAX)`) — both deliberate and both in Task 4.

**Fixture-name caveat:** Tasks 4–8 use `run_started_with_budget`, `two_node_graph`, `budget_raised`
and a spend-summing helper, modelled on the existing budget tests. Confirm each real name in
`crates/orchestrator/src/executor/tests.rs` before writing — adapt to what is there rather than
inventing a parallel set.
