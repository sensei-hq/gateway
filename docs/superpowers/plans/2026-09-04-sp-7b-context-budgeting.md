# SP-7b Context Budgeting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An agent turn whose prompt exceeds every candidate's context window is degraded and dispatched instead of refused, with the cut provably reproducible on every resume.

**Architecture:** A pure planner turns the chain's largest context window into a byte budget for the `system` half; the existing pure truncator applies it; the BUDGET INTEGER (not the cut) is journaled FIRST-wins before dispatch and replayed on later drives, which makes the cut a function of journaled state alone. A floor refuses via the HOTL pause when too little survives.

**Tech Stack:** Rust. Tests are plain `cargo test` with `assert!`/`assert_eq!`/`matches!`.

**Spec:** `docs/superpowers/specs/2026-09-04-sp-7b-context-budgeting-design.md` — criteria referenced as `AC1`…`AC12`.

---

## Preconditions (verified at HEAD `f181abe`, 2026-09-04 — re-verify if time has passed)

- `PromptParts { authored: String, context: Vec<(String, String)>, tools: Vec<ToolDefinition> }` and
  `pub fn join(self) -> (String, Vec<ToolDefinition>)` — `crates/orchestrator/src/agent/prompt.rs:15-45`. **Confirmed.**
- `render_context_section_bounded(entries: &[(String, String)], budget: usize) -> String` —
  `prompt.rs:251`. Pure. Its one production caller is the human path, `executor/human.rs:150`. **Confirmed.**
- `render_context_section(entries) -> String` (unbounded) — `prompt.rs:111`. **Confirmed.**
- In `drive_agent`: `assemble_prompt_parts` at `executor/agent.rs:113`, the human-backed `return` at
  `agent.rs:256-262`, `parts.join()` at `agent.rs:271`, `resolve_chain` at `agent.rs:273`, the
  `messages` seed at `agent.rs:285`. **Confirmed — note `join` precedes `resolve_chain`.**
- `let eid = effect_id(...)` at `agent.rs:383`, `let ih = agent_input_hash(...)` at `agent.rs:384`,
  memo compare and `DeterminismViolation` at `agent.rs:385-393`. **Confirmed.**
- Agent turn output shape: `serde_json::json!({ "model": model, "text": text })` — `agent.rs:460`.
  Already two keys, so a third is additive. **Confirmed.**
- `Fold` is `struct Fold` at `crates/orchestrator/src/executor/mod.rs:133`, fields include
  `memo`, `started`, `completed`, `skipped`, `intents`, `observations`, `context`, `expansions`,
  `selections`, `signals`. **Confirmed.**
- FIRST-wins fold idiom is `fold.<map>.entry(k).or_insert(v)` — e.g. `support.rs:219`. LAST-wins is
  `insert`. `fold.expansions`/`fold.selections` are LAST-wins, so do NOT copy them. **Confirmed.**
- `enum ToolOutcome<T> { Ok(T), Failed(String), Paused(String) }` — `agent.rs:23`.
  `AgentStep::Paused(reason)` becomes `NodeExec::Paused { reason }` at `mod.rs:1500`. **Confirmed.**
- `pause_awaiting(run, reason, deadline) -> Result<NodeExec, _>` — `signal.rs:254`. Returns
  `NodeExec`, so `drive_agent` (which returns `AgentStep`) cannot reuse it; mirror its 6-line body.
  **Confirmed.**
- `MIN_OUTPUT_TOKENS: u64 = 256` — `crates/orchestrator-core/src/budget.rs:61`. **Confirmed.**
- `estimate_input_tokens_pessimistic` is `chars.div_ceil(3)` saturating to `u32::MAX` —
  `crates/gateway/src/engine/util.rs:331`; re-exported `crates/gateway/src/lib.rs:34`. **Confirmed.**
- Window accessors on `Gateway`: `min_context_window` (`engine/mod.rs:213`),
  `min_serving_context_window` (`:312`), `min_max_output_tokens` (`:382`). There is **no**
  `max_context_window`. **Confirmed.**
- `GatewayConfig` has no version field — `rg -c version crates/kernel/src/types/config.rs` →
  **exit 1, zero matches**. **Confirmed.** (This is why the budget must be journaled: §4.2.)
- `fn label(event: &JournalEvent)` at `crates/orchestrator/src/executor/tests.rs:2650` is
  **exhaustive with no `_` catch-all** — the single site a new variant compile-errors, and
  `cargo build --workspace` does not compile it. Every other `match` on `JournalEvent`, including
  `fold_journal`, has a catch-all. **Confirmed by its own comment at `tests.rs:2700-2704`.**
- torii reads `JournalEvent::` in three files: `cmd/run.rs` (59 hits), `cmd/gate.rs` (27),
  `cmd/human.rs` (22). **Confirmed.**

## Working rules for every task

- **Red first.** Write the test, run it, watch it fail *for the stated reason*, then implement. A test
  that passes before the implementation is a finding — say so rather than moving on.
- `cargo fmt --all` before every commit. The pre-commit hook is fmt-check + workspace
  `clippy -D warnings` and runs **no tests** — run `cargo test --workspace` yourself.
- Verify **real** exit codes: `cmd > /tmp/x.log 2>&1; echo "exit=$?"`. Never judge from a piped `tail`.
- **NEVER** use backticks inside `git commit -m "..."` — the shell substitutes them and silently
  mangles the message. Use `git commit -F <file>`.
- **NEVER** run the DB suite against `$DATABASE_URL` — it is remote Supabase. No task here needs a DB.
- Match the house comment style: long doc comments that argue WHY. A comment asserting something
  false is worse than no comment.
- The orchestrator package is `sensei-orchestrator`; the gateway package is `sensei-gateway`. Use
  `cargo test -p sensei-orchestrator --lib` for a fast loop.

## File structure

| File | Responsibility | Task |
|---|---|---|
| `crates/gateway/src/engine/mod.rs` | `max_context_window` accessor | 1 |
| `crates/orchestrator-core/src/budget.rs` | `CONTEXT_FLOOR_FRACTION` | 2 |
| `crates/orchestrator/src/agent/prompt.rs` | `ContextCut`, `BudgetPlan`, `plan_budget`, measured renderer, `join_bounded` | 2, 3 |
| `crates/orchestrator-core/src/journal.rs` | `ContextBudgeted` variant | 4 |
| `crates/orchestrator/src/executor/mod.rs` | `Fold.context_budgets` + fold arm | 4 |
| `crates/orchestrator/src/executor/support.rs` | the fold arm body | 4 |
| `crates/orchestrator/src/executor/tests.rs` | `label` arm; all executor tests | 4–8 |
| `crates/orchestrator/src/executor/agent.rs` | the wiring: reorder, budget, append, cut, floor, output key, warn | 5–7 |
| docs | overview, feature docs, the two prompt.rs docs, checkpoint | 8 |

---

### Task 1: `Gateway::max_context_window`

**Files:**
- Modify: `crates/gateway/src/engine/mod.rs` (add after `min_serving_context_window`, which ends at `:324`)
- Test: `crates/gateway/src/engine/tests.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/gateway/src/engine/tests.rs`:

```rust
/// AC1 — `max_context_window` is the LARGEST window in the chain.
///
/// The budget's target: shrinking a prompt to the largest window is the least cutting that still
/// fits something, and the per-candidate `ContextWindowGate` remains the authority afterwards, so a
/// smaller candidate is simply skipped. Asserted against a heterogeneous chain, because on a
/// homogeneous one this function and `min_context_window` agree and the test would pass for the
/// wrong reason.
#[tokio::test]
async fn max_context_window_is_the_largest_window_in_the_chain() {
    let gw = ab_gateway(window_chain_config(128_000, 8_192));
    register_noop(&gw).await;
    assert_eq!(
        gw.max_context_window("c").await,
        Some(128_000),
        "the LARGEST window, not the smallest — the smallest is what min_context_window answers"
    );
    assert_ne!(
        gw.max_context_window("c").await,
        gw.min_context_window("c").await,
        "and the fixture must be heterogeneous or this test cannot distinguish the two folds"
    );
    assert_eq!(
        gw.max_context_window("nosuch").await,
        None,
        "an unknown chain has no answer"
    );
}
```

- [ ] **Step 2: Run it and watch it fail**

```
cargo test -p sensei-gateway --lib -- max_context_window_is_the_largest_window_in_the_chain > /tmp/t1.log 2>&1; echo "exit=$?"
```
Expected: **compile error** — `no method named max_context_window`.

- [ ] **Step 3: Implement**

In `crates/gateway/src/engine/mod.rs`, immediately after `min_serving_context_window`:

```rust
    /// The LARGEST `context_window` among a chain's models. `None` if the chain is unknown or
    /// has no resolvable models.
    ///
    /// The SP-7b context budget's target, and the fold is `max` for a reason worth stating
    /// because every sibling accessor here folds `min`. Those bound a value that must be safe
    /// for whichever candidate selection eventually returns, so they take the worst case. This
    /// one answers a different question — "how much prompt could ANY model on this chain hold?"
    /// — and shrinking a prompt to that figure is the LEAST cutting that still fits somebody.
    ///
    /// Safe despite being the most permissive fold, because it does not decide admission:
    /// `ContextWindowGate` still asks per candidate afterwards, so a prompt budgeted to 128k on
    /// a `[128k, 8k]` chain simply gets the 8k entry skipped and lands on the 128k one — which
    /// is exactly SP-7a's designed behaviour. This is NOT the "bound by the chain's largest"
    /// alternative the SP-7a follow-on spec rejected for the CLAMP (§3): that one was rejected
    /// because `max_tokens` must be safe for the SELECTED candidate, and nothing re-checked it.
    /// Here the input is being shrunk so that at least one candidate can hold it, and the
    /// per-candidate check still runs.
    pub async fn max_context_window(&self, chain: &str) -> Option<u32> {
        let cfg = self.config.read().await;
        let chain = cfg.chains.get(chain)?;
        chain
            .models
            .iter()
            .filter_map(|entry| cfg.models.get(&entry.model))
            .map(|m| m.context_window)
            .max()
    }
```

- [ ] **Step 4: Run it and watch it pass**

```
cargo test -p sensei-gateway --lib -- max_context_window > /tmp/t1.log 2>&1; echo "exit=$?"
```
Expected: `exit=0`, 1 passed.

- [ ] **Step 5: Mutation-check the fold**

Change `.max()` to `.min()`. Re-run. Expected: **FAIL** on the first assertion. Restore `.max()`.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/gateway/src/engine/mod.rs crates/gateway/src/engine/tests.rs
git commit -F /tmp/msg-t1.txt
```
with `/tmp/msg-t1.txt`:
```
feat(gateway): max_context_window, the SP-7b budget target

The largest window in a chain, and the only accessor here that folds max
rather than min. The siblings bound a value that must be safe for whichever
candidate wins, so they take the worst case; this answers "how much prompt
could ANY model on this chain hold", and shrinking to it is the least cutting
that still fits somebody. Safe because it does not decide admission --
ContextWindowGate still asks per candidate, so an 8k entry is skipped and the
request lands on the 128k one.

Mutation-verified: folding min instead reddens the test.
```

---

### Task 2: `CONTEXT_FLOOR_FRACTION` and the pure budget planner

**Files:**
- Modify: `crates/orchestrator-core/src/budget.rs` (after `MIN_OUTPUT_TOKENS` at `:61`)
- Modify: `crates/orchestrator/src/agent/prompt.rs`
- Test: `crates/orchestrator/src/agent/prompt.rs` (its `mod tests`)

- [ ] **Step 1: Write the failing tests**

Append to `prompt.rs`'s `mod tests`:

```rust
    /// The planner is PURE and its arithmetic mirrors the gateway estimator exactly.
    ///
    /// `estimate_input_tokens_pessimistic` is `ceil(bytes/3)`, so a token budget T is exactly a
    /// byte budget of 3T over the counted parts. These cases pin that identity rather than a
    /// hand-tuned constant: if the estimator's divisor ever changes, `available_context_bytes`
    /// must change with it and this test is what says so.
    #[test]
    fn the_planner_converts_a_token_window_into_a_byte_budget() {
        // window 4096, reserve 256 for output, transcript 96 tokens ⇒ 3744 tokens ⇒ 11232 bytes.
        assert_eq!(available_context_bytes(4096, 96), Some(11_232));
        // The reserve and the transcript together exceed the window ⇒ nothing to budget.
        assert_eq!(available_context_bytes(4096, 4096), None);
        assert_eq!(available_context_bytes(100, 0), None, "a window under the output reserve");
    }

    /// Tool schemas are dropped WHOLE and from the END of the activation order.
    ///
    /// Whole because a schema truncated mid-JSON is an invalid tool definition the provider
    /// rejects with a 400 — a degradation turned into a hard failure. From the end because that
    /// is the reverse of the order `assemble_prompt_parts` produced them in, which is the
    /// activation policy's own ranking; size- or name-ordered would be stable too but would
    /// discard that ranking.
    #[test]
    fn tool_schemas_are_dropped_whole_from_the_end_until_the_context_floor_fits() {
        let tools = vec![tool_def("alpha", 300), tool_def("beta", 300), tool_def("gamma", 300)];
        // Room for authored(0) + one tool(≈300) + a 500-byte context floor.
        let plan = plan_budget(900, 0, &tools, 500).expect("above the floor");
        assert_eq!(
            plan.dropped_tools,
            vec!["gamma".to_string(), "beta".to_string()],
            "the LAST-activated schemas go first, whole, in that order"
        );
        assert!(
            plan.context_budget_bytes >= 500,
            "and enough room is freed for the context floor: {}",
            plan.context_budget_bytes
        );
    }

    /// A node with NO dependencies is never refused for retaining none of them.
    #[test]
    fn a_node_with_no_context_is_never_below_the_floor() {
        assert!(retained_meets_floor(0, 0), "0 requested ⇒ the ratio is undefined, not failing");
    }

    /// The floor is a fraction of the REQUESTED body bytes.
    #[test]
    fn the_floor_rejects_a_cut_that_keeps_less_than_the_fraction() {
        assert!(retained_meets_floor(1000, 250), "exactly the fraction is admitted");
        assert!(!retained_meets_floor(1000, 249), "a byte under it is refused");
    }
```

And this helper beside them:

```rust
    /// A `ToolDefinition` whose ESTIMATOR-COUNTED bytes are exactly `bytes`.
    ///
    /// The estimator counts `name + description.unwrap_or("") + input_schema.to_string()`
    /// (`gateway/src/engine/util.rs:334-336`), so padding the description is what moves the
    /// figure. `input_schema: json!({})` stringifies to `{}` — two bytes — and `description` is
    /// `Option<String>`, so the arithmetic is `name.len() + pad + 2`.
    fn tool_def(name: &str, bytes: usize) -> ToolDefinition {
        let pad = bytes.saturating_sub(name.len() + 2);
        ToolDefinition {
            name: name.to_string(),
            description: Some("d".repeat(pad)),
            input_schema: serde_json::json!({}),
        }
    }
```

- [ ] **Step 2: Run and watch them fail**

```
cargo test -p sensei-orchestrator --lib -- prompt::tests::the_planner prompt::tests::tool_schemas_are_dropped prompt::tests::a_node_with_no_context prompt::tests::the_floor_rejects > /tmp/t2.log 2>&1; echo "exit=$?"
```
Expected: **compile errors** — `available_context_bytes`, `plan_budget`, `retained_meets_floor`,
`BudgetPlan` not found. If `ToolDefinition`'s fields differ from the helper above, fix the helper
from the real definition in `crates/kernel/src/types/request.rs` and say so in the commit.

- [ ] **Step 3: Add the constant**

In `crates/orchestrator-core/src/budget.rs`, after `MIN_OUTPUT_TOKENS`:

```rust
/// The fraction of an agent's requested dependency context that must SURVIVE budgeting for the
/// turn to be worth dispatching. Below it, SP-7b refuses instead of degrading.
///
/// **This number is a judgment call and there is no evidence behind it yet.** The argument for
/// having a floor at all is sound: unbounded degradation answers a 200 000-token question from 4%
/// of its context, confidently, and the model has no way to know it is unqualified — it will
/// answer anyway, and that answer flows downstream as work product. The argument for `0.25`
/// specifically is only that a quarter of what the graph decided the agent needed is the point
/// where "answering" starts to look like guessing.
///
/// The AC10 `tracing::warn!` exists to replace this guess with a measurement. When there is
/// fleet data on real degradation ratios, set it from that and delete this paragraph.
pub const CONTEXT_FLOOR_FRACTION: f64 = 0.25;
```

- [ ] **Step 4: Implement the planner**

Add to `crates/orchestrator/src/agent/prompt.rs`:

```rust
/// What a budgeted `## Context` section actually cost, measured as it was rendered.
///
/// Returned alongside the rendered string rather than parsed back out of it: the bodies are
/// arbitrary run data and are free to contain the very `### ` headings a parser would key on, so
/// recomputing these figures after the fact would be re-parsing text a dependency controls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextCut {
    /// The sum of the raw entry BODIES before rendering — headings and separators excluded, so
    /// both sides of the floor ratio measure the same thing.
    pub requested_bytes: usize,
    /// The body bytes actually emitted. Excludes headings, truncation markers and the
    /// `(N of M dependencies shown)` tail — a marker is not retained content, and counting it
    /// would let a section consisting entirely of markers pass the floor.
    pub retained_bytes: usize,
    pub deps_shown: usize,
    pub deps_total: usize,
}

/// The plan for one budgeted turn: how many bytes the `## Context` section may use, and which
/// tool schemas were dropped whole to make room.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetPlan {
    pub context_budget_bytes: usize,
    /// Tool names, in the order they were dropped (reverse activation order).
    pub dropped_tools: Vec<String>,
}

/// The bytes available to the whole `system` half, from the window and the transcript.
///
/// `window - MIN_OUTPUT_TOKENS - transcript_tokens`, converted to bytes by `× 3`. The `× 3` is
/// EXACT, not a fudge factor: `estimate_input_tokens_pessimistic` is `ceil(bytes / 3)`
/// (`gateway/src/engine/util.rs`), so a token budget of `T` is precisely a byte budget of `3T`
/// over the parts that estimator counts. `MIN_OUTPUT_TOKENS` is reserved so a degraded turn still
/// has room for a usable reply rather than being cut off mid-sentence.
///
/// `None` when the transcript plus the reserve already fills the window — there is nothing to
/// budget, and the caller must refuse rather than dispatch a prompt with no room for context.
pub fn available_context_bytes(window: u32, transcript_tokens: u32) -> Option<usize> {
    let reserve = u32::try_from(orchestrator_core::MIN_OUTPUT_TOKENS).unwrap_or(u32::MAX);
    let spare = window
        .checked_sub(reserve)?
        .checked_sub(transcript_tokens)?;
    if spare == 0 {
        return None;
    }
    Some(spare as usize * 3)
}

/// The estimator-counted byte weight of one tool schema.
///
/// Mirrors `estimate_input_tokens_pessimistic`'s tools term exactly — `name + description +
/// input_schema.to_string()`. It is a separate function from the estimator rather than a call
/// into it because the estimator answers in TOKENS over a whole payload, and dropping schemas
/// needs a per-schema BYTE figure. If the estimator's tools term ever changes, this must change
/// with it, and `the_planner_converts_a_token_window_into_a_byte_budget` is the test that fails.
fn tool_bytes(t: &ToolDefinition) -> usize {
    // `description` is `Option<String>` (`kernel/src/types/request.rs:202-203`), and the estimator
    // prices an absent one at ZERO rather than skipping the tool. Mirrored exactly, including that.
    t.name.len()
        + t.description.as_ref().map(|d| d.len()).unwrap_or(0)
        + t.input_schema.to_string().len()
}

/// Decide the context budget, dropping whole tool schemas from the END of the activation order
/// until the context floor fits.
///
/// Pure over its four arguments — no clock, no config, no window read. That purity is what makes
/// the journaled-budget determinism argument work (spec §4.2): the caller journals
/// `available_bytes`, and every later drive reproduces this plan from it.
///
/// `None` means the floor cannot be met however many schemas are dropped, and the caller must
/// refuse.
pub fn plan_budget(
    available_bytes: usize,
    authored_bytes: usize,
    tools: &[ToolDefinition],
    requested_context_bytes: usize,
) -> Option<BudgetPlan> {
    // The authored half is never cut (spec §5.2), so it comes off the top.
    let room = available_bytes.checked_sub(authored_bytes)?;
    // The least context worth dispatching. Computed from the floor rather than from a constant
    // so the two can never disagree: a plan that met some other minimum and then failed the
    // floor check would refuse AFTER doing all the work.
    let floor = floor_bytes(requested_context_bytes);
    let mut kept = tools.len();
    let mut dropped_tools = Vec::new();
    loop {
        let tool_total: usize = tools[..kept].iter().map(tool_bytes).sum();
        if room.saturating_sub(tool_total) >= floor {
            return Some(BudgetPlan {
                context_budget_bytes: room - tool_total,
                dropped_tools,
            });
        }
        if kept == 0 {
            // Every schema is gone and the floor still does not fit.
            return None;
        }
        kept -= 1;
        dropped_tools.push(tools[kept].name.clone());
    }
}

/// The minimum retained body bytes for a turn to be worth dispatching.
fn floor_bytes(requested_context_bytes: usize) -> usize {
    (requested_context_bytes as f64 * orchestrator_core::CONTEXT_FLOOR_FRACTION).ceil() as usize
}

/// Whether an achieved cut clears the floor.
///
/// `requested == 0` is TRUE, not a division by zero: an agent with no dependencies has nothing to
/// retain and must not be refused for retaining none of it. That case is reachable on any
/// dependency-free agent node whose transcript alone crowds the window.
pub fn retained_meets_floor(requested_bytes: usize, retained_bytes: usize) -> bool {
    if requested_bytes == 0 {
        return true;
    }
    retained_bytes >= floor_bytes(requested_bytes)
}
```

Add `CONTEXT_FLOOR_FRACTION` to `orchestrator-core`'s re-exports beside `MIN_OUTPUT_TOKENS` if that
crate re-exports it from `lib.rs`; check with
`rg -n 'MIN_OUTPUT_TOKENS' crates/orchestrator-core/src/lib.rs`.

- [ ] **Step 5: Run and watch them pass**

```
cargo test -p sensei-orchestrator --lib -- prompt::tests > /tmp/t2.log 2>&1; echo "exit=$?"
```
Expected: `exit=0`, all `prompt::tests` passing including the pre-existing ones.

- [ ] **Step 6: Mutation-check the drop order**

Change `dropped_tools.push(tools[kept].name.clone())` to push `tools[0].name.clone()`. Re-run.
Expected: **FAIL** on `tool_schemas_are_dropped_whole_from_the_end_until_the_context_floor_fits`.
Restore.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-core/src/budget.rs crates/orchestrator/src/agent/prompt.rs
git commit -F /tmp/msg-t2.txt
```
`/tmp/msg-t2.txt`:
```
feat(orchestrator): the pure SP-7b budget planner and the context floor

available_context_bytes converts a token window into a byte budget by x3,
which is exact rather than a fudge: the pessimistic estimator is
ceil(bytes/3), so a token budget T is precisely a byte budget of 3T over the
parts it counts. MIN_OUTPUT_TOKENS is reserved so a degraded turn still has
room for a usable reply.

plan_budget drops whole tool schemas from the END of the activation order
until the context floor fits. Whole because a schema truncated mid-JSON is an
invalid tool definition the provider answers with a 400 -- a degradation
turned into a hard failure. From the end because that is the reverse of the
order assemble_prompt_parts produced them in, which is the activation
policy's own ranking.

Everything here is pure over its arguments. That purity is the determinism
argument: the caller journals available_bytes and every later drive
reproduces the same plan from it.

CONTEXT_FLOOR_FRACTION is 0.25 and its doc says plainly that the number is a
judgment call with no evidence behind it, and that the AC10 warn exists to
replace it with a measurement.

requested == 0 clears the floor rather than dividing by zero -- a
dependency-free agent must not be refused for retaining none of nothing.

Mutation-verified: dropping from the front instead of the end reddens the
drop-order test.
```

---

### Task 3: The measured bounded renderer and `join_bounded`

**Files:**
- Modify: `crates/orchestrator/src/agent/prompt.rs`
- Test: `crates/orchestrator/src/agent/prompt.rs` (`mod tests`)

- [ ] **Step 1: Write the failing tests**

```rust
    /// The measured renderer returns the SAME string as the existing bounded one, plus the counts.
    ///
    /// This is the no-regression half: `render_context_section_bounded` has one production caller
    /// today (the human path) and six reviewed behaviours, so the measured variant must be the
    /// same function with an extra return value, not a reimplementation.
    #[test]
    fn the_measured_renderer_matches_the_bounded_one_byte_for_byte() {
        let entries = vec![
            ("A".to_string(), "a".repeat(500)),
            ("B".to_string(), "b".repeat(500)),
        ];
        for budget in [50usize, 200, 1000, 5000] {
            let (measured, _cut) = render_context_section_measured(&entries, budget);
            assert_eq!(
                measured,
                render_context_section_bounded(&entries, budget),
                "the two renderers must not drift at budget {budget}"
            );
        }
    }

    /// `retained_bytes` counts BODY bytes only — not headings, not markers, not the tail.
    #[test]
    fn retained_bytes_excludes_headings_and_markers() {
        let entries = vec![("A".to_string(), "a".repeat(1000))];
        let (out, cut) = render_context_section_measured(&entries, 200);
        assert_eq!(cut.requested_bytes, 1000);
        assert!(cut.retained_bytes < 200, "bounded below the budget: {}", cut.retained_bytes);
        assert_eq!(
            cut.retained_bytes,
            out.matches('a').count(),
            "retained counts exactly the body bytes emitted — every 'a' and nothing else, so \
             the heading, the marker and the tail are all excluded"
        );
    }

    /// `join_bounded` cuts the context, drops the planned schemas, and never touches `authored`.
    #[test]
    fn join_bounded_cuts_context_and_drops_schemas_but_never_authored() {
        // NOTE (amended after review): 8 bytes of `authored` against a 200-byte context budget
        // leaves the bound NEVER IN PLAY, so `starts_with` cannot tell "never cut" from "shorter
        // than the budget anyway" — AC8 was unpinned and two mutations of `join_bounded` left all
        // 442 orchestrator tests green. Use authored bytes LARGER than the budget (508 against
        // 200, as shipped in 2e844d6), take `starts_with` against a saved copy, and assert both
        // that the joined prompt EXCEEDS the budget and that what follows `authored` is exactly
        // the measured section.
        let parts = PromptParts {
            authored: "AUTHORED".to_string(),
            context: vec![("A".to_string(), "a".repeat(1000))],
            tools: vec![tool_def("alpha", 100), tool_def("beta", 100)],
        };
        let plan = BudgetPlan {
            context_budget_bytes: 200,
            dropped_tools: vec!["beta".to_string()],
        };
        let (system, tools, cut) = parts.join_bounded(&plan);
        assert!(system.starts_with("AUTHORED"), "authored bytes are never cut: {system}");
        assert_eq!(
            tools.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
            vec!["alpha"],
            "the planned schema is dropped WHOLE and the survivor is intact"
        );
        assert_eq!(cut.deps_total, 1);
        assert!(cut.retained_bytes < cut.requested_bytes, "and the context really was cut");
    }
```

- [ ] **Step 2: Run and watch them fail**

```
cargo test -p sensei-orchestrator --lib -- prompt::tests::the_measured_renderer prompt::tests::retained_bytes_excludes prompt::tests::join_bounded_cuts > /tmp/t3.log 2>&1; echo "exit=$?"
```
Expected: **compile error** — `render_context_section_measured` and `join_bounded` not found.

- [ ] **Step 3: Refactor the existing renderer into the measured one**

Rename the body of `render_context_section_bounded` to `render_context_section_measured`, returning
`(String, ContextCut)`, and make the old name a thin wrapper. **Do not change any of its six
behaviours.** Track `requested` as the sum of `body.len()` over all entries, and `retained` as the
sum of the bytes each `truncate_with_marker` call actually emitted from the body — compute that as
`emitted.len()` minus the marker length when a marker was added, which means
`truncate_with_marker` needs to report it. Simplest faithful approach: compute
`retained_for_entry = min(body.len(), room_before_marker)` from the same `room` the loop already
computes, and sum those; then subtract nothing, because that figure is by construction
body-bytes-only.

```rust
/// [`render_context_section_bounded`]'s work, with the counts it computed on the way out.
///
/// The counts cannot be recovered from the returned string: dependency bodies are arbitrary run
/// data and may contain the very `### ` headings a parser would key on, so measuring afterwards
/// would mean re-parsing text a dependency controls. SP-7b's floor is decided on these numbers,
/// so they are returned rather than re-derived.
pub fn render_context_section_measured(
    entries: &[(String, String)],
    budget: usize,
) -> (String, ContextCut) {
    // ... the existing body of render_context_section_bounded, unchanged, plus:
    //   let requested: usize = entries.iter().map(|(_, b)| b.len()).sum();
    //   let mut retained = 0usize;
    //   inside the loop, after computing `room`:
    //     retained += body.len().min(room);
    //   and `deps_shown` from the same `ends`-based degradation logic that produces the tail.
}

/// The human path's renderer: [`render_context_section_measured`] without the counts.
///
/// Kept as its own name because `executor/human.rs` calls it and SP-6 s3's tests pin it. A wrapper
/// rather than a duplicate so the two can never drift — the six reviewed behaviours live in one
/// place.
pub fn render_context_section_bounded(entries: &[(String, String)], budget: usize) -> String {
    render_context_section_measured(entries, budget).0
}
```

- [ ] **Step 4: Implement `join_bounded`**

```rust
impl PromptParts {
    /// [`Self::join`]'s budgeted sibling: the model path's answer to a prompt no candidate can
    /// hold.
    ///
    /// `join`'s doc argues the model path must never truncate, "so a model is never silently
    /// asked about half a document". The operative word is SILENTLY, and SP-7b answers it on four
    /// channels rather than by keeping the refusal: the per-entry marker and the
    /// `(N of M dependencies shown)` tail this renderer already emits, the `ContextBudgeted`
    /// journal record, an additive `context_budgeted` key on the node's output, and an operator
    /// warn. See the spec's §5.5.
    ///
    /// `authored` is never cut (spec §5.2) — those are the config author's own bytes and they can
    /// trim them, which is the same asymmetry that made `PromptParts` two halves in the first
    /// place. Tool schemas are dropped WHOLE, per `plan.dropped_tools`, because a schema
    /// truncated mid-JSON is an invalid tool definition.
    pub fn join_bounded(self, plan: &BudgetPlan) -> (String, Vec<ToolDefinition>, ContextCut) {
        let (section, cut) =
            render_context_section_measured(&self.context, plan.context_budget_bytes);
        let mut system = self.authored;
        system.push_str(&section);
        let tools = self
            .tools
            .into_iter()
            .filter(|t| !plan.dropped_tools.contains(&t.name))
            .collect();
        (system, tools, cut)
    }
}
```

- [ ] **Step 5: Run the whole prompt module**

```
cargo test -p sensei-orchestrator --lib -- prompt:: > /tmp/t3.log 2>&1; echo "exit=$?"
```
Expected: `exit=0`. **Every pre-existing `prompt::tests` test must still pass** — especially
`a_prompt_clamp_never_overruns_its_bound` and
`a_context_section_that_drops_dependencies_says_how_many`. If either fails, the refactor changed a
behaviour and must be corrected, not the test.

- [ ] **Step 6: Run the human path's tests**

```
cargo test -p sensei-orchestrator --lib -- human > /tmp/t3b.log 2>&1; echo "exit=$?"
```
Expected: `exit=0`. The wrapper must leave the human path byte-identical.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/agent/prompt.rs
git commit -F /tmp/msg-t3.txt
```
`/tmp/msg-t3.txt`:
```
feat(orchestrator): a measured bounded renderer and PromptParts::join_bounded

render_context_section_bounded becomes a thin wrapper over
render_context_section_measured, which returns the counts it computed on the
way out. A wrapper rather than a duplicate: the six reviewed behaviours stay
in one place and the human path -- its only production caller -- is
byte-identical.

The counts are returned rather than re-derived because they cannot be
recovered from the string: dependency bodies are arbitrary run data and may
contain the very "### " headings a parser would key on, so measuring
afterwards means re-parsing text a dependency controls. SP-7b decides its
floor on these numbers.

join_bounded is join's budgeted sibling. join's doc argues the model path
must never truncate "so a model is never silently asked about half a
document" -- the operative word is SILENTLY, and the four disclosure channels
are the answer. authored is never cut; schemas are dropped whole.
```

---

### Task 4: The `ContextBudgeted` journal variant and its FIRST-wins fold

**Files:**
- Modify: `crates/orchestrator-core/src/journal.rs`
- Modify: `crates/orchestrator/src/executor/mod.rs:133` (`Fold`)
- Modify: `crates/orchestrator/src/executor/support.rs` (`fold_journal`)
- Modify: `crates/orchestrator/src/executor/tests.rs:2650` (`label`)
- Test: `crates/orchestrator-core/src/journal.rs` (`mod tests`), `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the failing tests**

In `crates/orchestrator-core/src/journal.rs`'s `mod tests`:

```rust
    /// AC12 — the new variant round-trips and `FORMAT_VERSION` stays 1.
    #[test]
    fn the_context_budgeted_event_round_trips() {
        let ev = JournalEvent::ContextBudgeted {
            node: NodeId("n1".into()),
            effect_id: EffectId("eid-1".into()),
            budget_bytes: 11_232,
            source_window: 4096,
            retained_bytes: 900,
            dropped_deps: 2,
            dropped_tools: vec!["gamma".into()],
        };
        let json = serde_json::to_string(&ev).expect("serialises");
        assert!(json.contains("ContextBudgeted"), "externally tagged: {json}");
        let back: JournalEvent = serde_json::from_str(&json).expect("deserialises");
        assert_eq!(format!("{back:?}"), format!("{ev:?}"));
        assert_eq!(FORMAT_VERSION, 1, "an additive variant must not bump the format fence");
    }
```

In `crates/orchestrator/src/executor/support.rs`'s `mod tests`:

```rust
    /// AC6 — `ContextBudgeted` folds FIRST-wins.
    ///
    /// A budget that a later event can rewrite is not a fence: the whole determinism argument is
    /// that drive 2 reproduces drive 1's cut, and LAST-wins would let a second record silently
    /// move the cut a completed turn was hashed against. The hazard is that the two nearest
    /// templates -- `fold.expansions` and `fold.selections` -- are both LAST-wins `insert`, so
    /// the correct discipline is a one-token difference from the code most likely to be copied.
    #[test]
    fn the_first_context_budget_wins() {
        use orchestrator_core::{EffectId, JournalEvent, NodeId};
        let eid = EffectId("eid-1".into());
        let ev = |budget_bytes: u64| {
            JournalEvent::ContextBudgeted {
                node: NodeId("n1".into()),
                effect_id: eid.clone(),
                budget_bytes,
                source_window: 4096,
                retained_bytes: 0,
                dropped_deps: 0,
                dropped_tools: vec![],
            }
        };
        let (fold, _last, _completed) = fold_journal(&[(0, ev(1000)), (1, ev(9999))]);
        assert_eq!(
            fold.context_budgets.get(&eid),
            Some(&1000),
            "the FIRST budget wins — a later record must not move a cut already hashed against"
        );
    }
```

- [ ] **Step 2: Run and watch them fail**

```
cargo test -p sensei-orchestrator-core --lib -- the_context_budgeted_event_round_trips > /tmp/t4.log 2>&1; echo "exit=$?"
cargo test -p sensei-orchestrator --lib -- the_first_context_budget_wins > /tmp/t4b.log 2>&1; echo "exit=$?"
```
Expected: **compile errors** — no `ContextBudgeted` variant, no `fold.context_budgets`.
Confirm the orchestrator-core package name first with `grep '^name' crates/orchestrator-core/Cargo.toml`.

- [ ] **Step 3: Add the variant**

In `crates/orchestrator-core/src/journal.rs`, beside the other SP-6/SP-7 variants:

```rust
    /// SP-7b: the byte budget an over-window agent turn's `## Context` half was cut to.
    ///
    /// **The `budget_bytes` field is the load-bearing one, and it is journaled rather than the
    /// CUT for a reason.** The truncator is pure and every other input to the cut is already
    /// replay-stable — context comes from CAS by digest, `authored` and tool activation from the
    /// pinned registry — so this integer was the only unfenced input, because it is derived from
    /// a model's `context_window` and `GatewayConfig` carries no version field at all. Journaling
    /// it makes the cut a pure function of two journaled values on the FIRST drive as well as on
    /// resume, which is what the SP-7a spec's §5 obligation actually asks for. Journaling the cut
    /// itself would only make a candidate-dependent decision reproducible, which is the half of
    /// that obligation journaling does not answer — and it would have to be rich enough to
    /// RECONSTRUCT bytes rather than verify them, landing inline in the event jsonb.
    ///
    /// It is mandatory rather than defensive. `drive` builds a fresh `DriveState::default()` and
    /// `ready_nodes` never consults the fold, so every past turn's `agent_input_hash` is
    /// recomputed on every partial resume, forever; a mismatch is a `DeterminismViolation`, and
    /// that leaves the run terminally `Failed` where `force_wake` cannot revive it. A drifted
    /// budget would convert a verbose prompt into a permanently dead run.
    ///
    /// FIRST record wins when folded — `entry().or_insert()`, like the `*Awaited` family and
    /// NOT like `PlanExpanded`/`SelectorDispatch`, which are both `insert`. A budget a later
    /// record could move is not a fence.
    ///
    /// The remaining fields are DISCLOSURE, not inputs: they are what `torii` and an audit read
    /// to learn a turn answered on a degraded prompt, and nothing reconstructs the cut from them.
    ContextBudgeted {
        node: NodeId,
        effect_id: EffectId,
        budget_bytes: u64,
        source_window: u32,
        retained_bytes: u64,
        dropped_deps: u32,
        dropped_tools: Vec<String>,
    },
```

- [ ] **Step 4: Add the fold side-map**

In `crates/orchestrator/src/executor/mod.rs`, inside `struct Fold` (`:133`):

```rust
    /// SP-7b: the journaled `## Context` byte budget per effect id, FIRST-wins.
    ///
    /// Keyed by `EffectId` rather than `NodeId` because an agent node has one budget PER TURN and
    /// the turn is what the effect id encodes. Readable strictly before any prompt bytes exist:
    /// `effect_id` is pure over `{parent_path, loop_iteration, local_index}` and is computed one
    /// line before `agent_input_hash` (`agent.rs:383-384`), which is the ordering the whole design
    /// depends on.
    context_budgets: HashMap<EffectId, u64>,
```

In `crates/orchestrator/src/executor/support.rs`'s `fold_journal`:

```rust
            // SP-7b. FIRST wins — `or_insert`, NOT `insert`. See the variant's doc: a budget a
            // later record could move is not a fence, and the two nearest templates in this same
            // function (`expansions`, `selections`) are both LAST-wins, so this one token is the
            // whole difference from the code most likely to be copied here.
            JournalEvent::ContextBudgeted {
                effect_id,
                budget_bytes,
                ..
            } => {
                fold.context_budgets
                    .entry(effect_id.clone())
                    .or_insert(*budget_bytes);
            }
```

- [ ] **Step 5: Add the `label` arm**

`fold_journal` has a `_` catch-all so it will NOT complain; `label` in `tests.rs:2650` is the ONE
exhaustive match and WILL. Add:

```rust
        JournalEvent::ContextBudgeted { node, .. } => format!("ContextBudgeted({})", node.0),
```

- [ ] **Step 6: Run and watch them pass, then build all targets**

```
cargo test -p sensei-orchestrator-core --lib -- the_context_budgeted_event_round_trips > /tmp/t4.log 2>&1; echo "exit=$?"
cargo test -p sensei-orchestrator --lib -- the_first_context_budget_wins > /tmp/t4b.log 2>&1; echo "exit=$?"
cargo build --workspace --all-targets > /tmp/t4c.log 2>&1; echo "exit=$?"
```
All three expected `exit=0`. The third is what catches the `label` arm — a plain
`cargo build --workspace` does **not** compile `tests.rs`.

- [ ] **Step 7: Mutation-check the fold discipline**

Change `.entry(...).or_insert(*budget_bytes)` to `.insert(effect_id.clone(), *budget_bytes)`.
Re-run `the_first_context_budget_wins`. Expected: **FAIL** with `Some(9999)`. Restore.

- [ ] **Step 8: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-core/src/journal.rs crates/orchestrator/src/executor/mod.rs crates/orchestrator/src/executor/support.rs crates/orchestrator/src/executor/tests.rs
git commit -F /tmp/msg-t4.txt
```
`/tmp/msg-t4.txt`:
```
feat(orchestrator): journal the SP-7b context budget, FIRST-wins

The budget integer is the load-bearing field and the reason the event exists.
The truncator is pure and every other input to the cut is already
replay-stable -- context from CAS by digest, authored and tool activation
from the pinned registry -- so this integer was the only unfenced one,
because it derives from a model's context_window and GatewayConfig carries no
version field at all. Journaling it makes the cut a pure function of two
journaled values on drive 1 as well as on resume.

Mandatory rather than defensive: drive builds a fresh DriveState::default()
and ready_nodes never consults the fold, so every past turn's hash is
recomputed on every partial resume forever, and a DeterminismViolation leaves
the run terminally Failed where force_wake cannot revive it.

Folded FIRST-wins with entry().or_insert. The hazard is that the two nearest
templates in the same function -- expansions and selections -- are both
LAST-wins insert, so the correct discipline is one token away from the code
most likely to be copied. Mutation-verified: insert reddens the guard.

Keyed by EffectId, not NodeId: an agent node has one budget per TURN, and the
effect id is what encodes the turn. It is also readable before any prompt
bytes exist, which is the ordering the design depends on.

Additive variant, so FORMAT_VERSION stays 1. The label arm in tests.rs is the
single exhaustive match on JournalEvent in the workspace and the only compile
error -- and cargo build --workspace does not compile it, so this was found
with --all-targets deliberately rather than by the compiler complaining.
```

---

### Task 5: Wire the budget into `drive_agent` — compute, journal, replay

**Files:**
- Modify: `crates/orchestrator/src/executor/agent.rs:265-290`
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the failing test (AC2)**

```rust
/// AC2 — an over-window agent turn is DEGRADED and dispatched, not refused.
///
/// The fixture is the two-window chain and a prompt whose dependency context pushes it past both
/// windows. Before SP-7b this halted: the gate skipped every candidate and the run paused. Now the
/// context is cut to fit the LARGEST window and the turn completes.
///
/// Asserted on what reached the PROVIDER, not on the orchestrator's arithmetic — a budget that
/// satisfies every assertion phrased in its own terms and still overflows the real window is the
/// failure this AC exists to exclude, and SP-DATA-5's AC10 was added after review found exactly
/// that.
#[tokio::test]
async fn an_over_window_agent_turn_is_budgeted_and_dispatched() {
    let (gateway, calls, _models, ests) = two_window_clamp_observing_gateway(10, 100).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let out = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(over_window_agent_registry())
        .start(run, &oversized_context_graph())
        .await
        .expect("drives");

    assert!(out.paused.is_none(), "it must not halt any more: {:?}", out.paused);
    assert!(out.failed.is_none(), "nor fail: {:?}", out.failed);
    assert!(
        !calls.lock().unwrap_or_else(|e| e.into_inner()).is_empty(),
        "the turn really was dispatched"
    );
    let seen = ests.lock().unwrap_or_else(|e| e.into_inner()).clone();
    assert!(
        seen.iter().all(|e| *e <= TWO_WINDOW_BIG),
        "and every dispatched request fits the largest window, measured by the GATEWAY's own \
         estimator on what the provider received: {seen:?}"
    );
}
```

`oversized_context_graph()` is a helper to add beside it: an `A → B` graph where `A` is a
`model_call` producing a body far larger than `TWO_WINDOW_SMALL × 3` bytes and `B` is
`agent_node_with_deps("B", "a", "refine", vec![Dep::hard("A")])`. Model it on the existing
`oversized_dependency_context_halts_over_budget_never_truncates` fixture
(`tests.rs:4810-4835`), which already builds exactly this shape with
`scripted_gateway(vec![final_response(&"x".repeat(100_000))])`.

- [ ] **Step 2: Run and watch it fail**

```
cargo test -p sensei-orchestrator --lib -- an_over_window_agent_turn_is_budgeted_and_dispatched > /tmp/t5.log 2>&1; echo "exit=$?"
```
Expected: **FAIL** — `out.paused` is `Some`, because the gate still refuses the un-budgeted prompt.

- [ ] **Step 3: Reorder `resolve_chain` above the join**

In `agent.rs`, move `let chain = self.registry.resolve_chain(agent, phase)?.to_string();` from
`:273` to immediately **before** the `parts.join()` call at `:271`. Add:

```rust
        // `resolve_chain` moved ABOVE the join for SP-7b: the budget is derived from the chain's
        // largest context window, so the chain must be resolved before the `## Context` section is
        // rendered. It is still BELOW the human-backed `return` above, which is the placement that
        // matters — that arm resolves no chain at all, so a human-backed role's zero token spend
        // stays STRUCTURAL rather than measured. Moving this line above that return would silently
        // destroy the property SP-6 s3's comment at the top of this function claims.
```

- [ ] **Step 4: Compute-or-replay the budget and join**

Replace the `let (system, tools) = parts.join();` line with:

```rust
        // SP-7b. The turn's effect id for turn 0 — the same coordinates `agent_turn_output`
        // recomputes at `agent.rs:383`, so the budget is keyed identically on every drive.
        let budget_eid = effect_id(&node_id.0, 0, 0);
        let requested_context_bytes: usize =
            parts.context.iter().map(|(_, b)| b.len()).sum();

        // REPLAY FIRST. A journaled budget is read back and used verbatim, which is what makes
        // the cut a function of journaled state: on this path the window is never read at all, so
        // an operator editing a model's `context_window` cannot disturb a turn already taken.
        let journaled = fold.context_budgets.get(&budget_eid).copied();

        let (system, tools, cut) = match journaled {
            Some(available) => {
                let plan = plan_budget(
                    available as usize,
                    parts.authored.len(),
                    &parts.tools,
                    &parts.context,
                )
                .ok_or_else(|| OrchestratorError::Internal {
                    message: format!(
                        "a journaled context budget of {available} bytes no longer meets the \
                         floor for node {}; the cut is not reproducible",
                        node_id.0
                    ),
                })?;
                let (s, t, c) = parts.join_bounded(&plan);
                (s, t, Some(c))
            }
            None => {
                // Not yet budgeted. Ask whether it needs to be, which requires the window.
                let window = self.gateway.max_context_window(&chain).await;
                let unbounded_tokens = /* see step 5 */ 0u32;
                match window {
                    Some(w) if unbounded_tokens > w => {
                        let available = available_context_bytes(w, /* transcript */ 0);
                        // ... plan, journal, join_bounded — see step 5
                        unimplemented!("step 5")
                    }
                    // Fits, or an unknown chain: today's path exactly.
                    _ => {
                        let (s, t) = parts.join();
                        (s, t, None)
                    }
                }
            }
        };
```

**Note to the implementer:** the `unimplemented!` above is a signpost inside this plan, not
something to commit. Step 5 replaces the whole `None` arm with real code; do not run the suite
between steps 4 and 5.

- [ ] **Step 5: Complete the first-drive arm**

Replace the `None` arm's body with:

```rust
            None => {
                // The window is read ONLY here — on a turn that has never been budgeted. Every
                // later drive takes the `Some` arm above and never asks the gateway again.
                let window = self.gateway.max_context_window(&chain).await;
                // The un-budgeted prompt, priced by the SAME estimator selection will use. Built
                // from the joined form because that is what would be dispatched; `join` consumes
                // `parts`, so this measures a clone.
                let probe = build_chat_request(
                    &chain,
                    &render_unbounded_system(&parts),
                    vec![Message::text(MessageRole::User, &query)],
                    parts.tools.clone(),
                );
                let unbounded = gateway::estimate_input_tokens_pessimistic(&probe.payload);
                match window {
                    Some(w) if unbounded > w => {
                        let transcript = gateway::estimate_input_tokens_pessimistic(
                            &build_chat_request(
                                &chain,
                                "",
                                vec![Message::text(MessageRole::User, &query)],
                                Vec::new(),
                            )
                            .payload,
                        );
                        let Some(available) = available_context_bytes(w, transcript) else {
                            return Ok(self
                                .pause_context_floor(run, node_id, w, requested_context_bytes, 0)
                                .await?);
                        };
                        let Some(plan) = plan_budget(
                            available,
                            parts.authored.len(),
                            &parts.tools,
                            &parts.context,
                        ) else {
                            return Ok(self
                                .pause_context_floor(run, node_id, w, requested_context_bytes, 0)
                                .await?);
                        };
                        let (s, t, c) = parts.join_bounded(&plan);
                        if !retained_meets_floor(c.requested_bytes, c.retained_bytes) {
                            return Ok(self
                                .pause_context_floor(
                                    run,
                                    node_id,
                                    w,
                                    c.requested_bytes,
                                    c.retained_bytes,
                                )
                                .await?);
                        }
                        // Journal BEFORE the model call, and use the LOCAL plan on this drive.
                        // `Fold` is built once per drive from one `journal.load` and is never
                        // refreshed, so reading this back from the fold here would read a STALE
                        // fold and recompute. This is `drive_expand_with`'s shape (expand.rs:284-293).
                        self.append(
                            run,
                            JournalEvent::ContextBudgeted {
                                node: node_id.clone(),
                                effect_id: budget_eid.clone(),
                                budget_bytes: available as u64,
                                source_window: w,
                                retained_bytes: c.retained_bytes as u64,
                                dropped_deps: (c.deps_total - c.deps_shown) as u32,
                                dropped_tools: plan.dropped_tools.clone(),
                            },
                        )
                        .await?;
                        (s, t, Some(c))
                    }
                    _ => {
                        let (s, t) = parts.join();
                        (s, t, None)
                    }
                }
            }
```

`render_unbounded_system(&parts)` is a small helper to add in `prompt.rs`:

```rust
/// The joined `system` string WITHOUT consuming the parts — for pricing a prompt before deciding
/// whether to budget it. `join` takes `self`, and the probe must not destroy the parts the real
/// join still needs.
pub fn render_unbounded_system(parts: &PromptParts) -> String {
    let mut system = parts.authored.clone();
    system.push_str(&render_context_section(&parts.context));
    system
}
```

Thread `cut` into `AgentRun` as `context_cut: Option<ContextCut>` so Task 7 can read it for the
output key and the warn. Add the field to the `AgentRun` struct and to its construction at
`agent.rs:274-283`.

- [ ] **Step 6: Add the floor pause helper (AC9)**

`pause_awaiting` (`signal.rs:254`) returns `NodeExec`, but `drive_agent` returns `AgentStep`, so it
cannot be reused. Mirror its body in `agent.rs`:

```rust
    /// SP-7b's floor: refuse rather than answer from almost nothing.
    ///
    /// `resume_after: None` is the HOTL pause class, and it is the SAME class the M1 reversal
    /// established for an over-window run — deliberately, because the alternative was just
    /// removed for being unrecoverable: `force_wake` matches only `status = 'paused'`,
    /// `torii run wake` reports "not queued", and `submit` refuses a used id, so a `NodeFailed`
    /// here would leave every completed node's memo and spend durable and unreachable.
    ///
    /// The remedy this names is a config change, so no deadline is carried: nothing about waiting
    /// makes a model's window bigger, and a timed wake would return the run to the identical
    /// refusal forever.
    async fn pause_context_floor(
        &self,
        run: RunId,
        node_id: &NodeId,
        window: u32,
        requested: usize,
        retained: usize,
    ) -> Result<AgentStep, OrchestratorError> {
        let pct = (orchestrator_core::CONTEXT_FLOOR_FRACTION * 100.0).round() as u32;
        let reason = format!(
            "context budget: this turn's dependency context is {requested} bytes and only \
             {retained} survive a budget for the largest model in this chain ({window}-token \
             context window) — under the {pct}% floor, so the reply would be built on almost \
             nothing. Waiting does not move this: shorten the upstream output, split the node, \
             or put a model with a larger window in this chain."
        );
        self.append(
            run,
            JournalEvent::RunPaused {
                reason: reason.clone(),
                resume_after: None,
            },
        )
        .await?;
        Ok(AgentStep::Paused(reason))
    }
```

- [ ] **Step 7: Run the target test, then the whole crate**

```
cargo test -p sensei-orchestrator --lib -- an_over_window_agent_turn_is_budgeted_and_dispatched > /tmp/t5.log 2>&1; echo "exit=$?"
cargo test -p sensei-orchestrator --lib > /tmp/t5b.log 2>&1; echo "exit=$?"
```
The first must be `exit=0`. The second will FAIL on
`oversized_dependency_context_halts_over_budget_never_truncates` and
`an_over_window_agent_prompt_pauses_the_run_with_the_gateways_diagnosis` — **expected**, and Task 8
resolves them. Record the exact failure list in the commit rather than fixing them here.

- [ ] **Step 8: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/agent.rs crates/orchestrator/src/agent/prompt.rs crates/orchestrator/src/executor/tests.rs
git commit -F /tmp/msg-t5.txt
```
`/tmp/msg-t5.txt`:
```
feat(orchestrator): budget an over-window agent turn instead of refusing it

An over-window agent turn is now cut to fit the chain's largest window and
dispatched. AC2 asserts it on what reached the PROVIDER, priced by the
gateway's own estimator, not on the orchestrator's arithmetic -- a budget
that satisfies every assertion phrased in its own terms and still overflows
the real window is the failure that AC exists to exclude.

Two orderings are load-bearing and both are commented at the site:

resolve_chain moves ABOVE the join, because the budget derives from the
chain's window and the section must be rendered after it is known. It stays
BELOW the human-backed return, which is the placement that matters: that arm
resolves no chain, so a human-backed role's zero token spend stays structural
rather than measured.

The ContextBudgeted append happens BEFORE the model call, and the writing
drive uses its LOCAL plan rather than reading the fold back. Fold is built
once per drive from one journal.load and never refreshed, so reading it back
mid-drive reads a stale fold and recomputes. This is drive_expand_with's
shape.

Replay comes first: a journaled budget is used verbatim and the window is
never read on that path, so an operator editing a model's context_window
cannot disturb a turn already taken.

The floor pauses with resume_after: None -- the same HOTL class the M1
reversal established, deliberately, since a NodeFailed here would be
unrecoverable. pause_awaiting could not be reused: it returns NodeExec and
drive_agent returns AgentStep.

KNOWN FAILING, resolved in the guard-test task:
  oversized_dependency_context_halts_over_budget_never_truncates
  an_over_window_agent_prompt_pauses_the_run_with_the_gateways_diagnosis
```

---

### Task 6: The replay property (AC4) — the slice's central claim

> **DONE, in Task 5's review round — do not write it twice.** Review found that NOTHING pinned
> either half of the replay mechanism (the append and the read could both be deleted with the whole
> suite green), and the unpinned failure mode is the unrecoverable one, so this task was pulled
> forward rather than deferred. `a_budgeted_turn_replays_after_the_window_changes_underneath_it`
> exists with the name below, and it asserts more than this task specified: one `ContextBudgeted`
> row across BOTH drives, one `EffectRecorded` for turn 0, zero re-spend, and the row still
> carrying drive 1's `source_window`.
>
> Two deviations from the sketch below, both forced by the fixtures:
> - `window_chain_config(64_000, 8_192)` does not exist and `update_config` on the drive-1 gateway
>   cannot re-script the adapter, so drive 2 runs a SECOND gateway over
>   `wide_window_scripted_gateway(300_000, …)` — a real config swap, and one WIDE enough to serve
>   drive 1's cut so the resume COMPLETES rather than merely not-dying.
> - Drive 1 must die mid-node (script exhausted at turn 1), or the completed run short-circuits the
>   resume and the hash is never recomputed — the same trap
>   `an_agent_turn_replays_from_its_memo_though_selection_may_differ` records.
>
> Its complement was added beside it: `an_unbudgeted_turn_replays_after_the_window_shrinks_under_it`,
> for the case AC4 does not cover and the spec had missed (see §4.1).

**Files:**
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the failing test**

```rust
/// AC4 — **the slice's central claim.** A budgeted turn replays from its memo even when the
/// window has CHANGED underneath it.
///
/// The changed window is what makes this non-vacuous. Pinning a replay with an unchanged window
/// would pass without any of the journaling: the recomputed budget would happen to match. So drive
/// 1 budgets against a large window, the gateway config is then swapped for one with a different
/// window, and drive 2 must still replay drive 1's cut — because it reads `budget_bytes` out of
/// the journal and never asks the gateway at all.
///
/// Without the journaling this is not a soft failure: the recomputed cut differs, so
/// `agent_input_hash` differs from the memo, `agent_turn_output` returns `DeterminismViolation`,
/// and per the M1-reversal work that leaves the run terminally `Failed` with no supported command
/// able to revive it.
#[tokio::test]
async fn a_budgeted_turn_replays_after_the_window_changes_underneath_it() {
    let (gateway, calls, _models, _ests) = two_window_clamp_observing_gateway(10, 100).await;
    let gateway = Arc::new(gateway);
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = oversized_context_graph();

    // Drive 1: budgets and completes.
    Executor::new(gateway.clone(), Arc::new(journal.clone()), "v1")
        .with_registry(over_window_agent_registry())
        .start(run, &graph)
        .await
        .expect("drive 1");
    let calls_after_1 = calls.lock().unwrap_or_else(|e| e.into_inner()).len();
    assert!(calls_after_1 > 0, "drive 1 dispatched");
    let budgets: Vec<u64> = journal
        .load(run)
        .await
        .unwrap()
        .iter()
        .filter_map(|(_, e)| match e {
            JournalEvent::ContextBudgeted { budget_bytes, .. } => Some(*budget_bytes),
            _ => None,
        })
        .collect();
    assert_eq!(budgets.len(), 1, "exactly one budget journaled (AC3): {budgets:?}");

    // Move the window. This is the drift the journaling exists to survive.
    // `update_config` returns `()` — it swaps under a write lock and validates NOTHING
    // (`gateway/src/engine/mod.rs:512`), which is itself why the budget has to be journaled: this
    // edit is invisible to the config-version fence, since `GatewayConfig` has no version field.
    gateway
        .update_config(window_chain_config(64_000, 8_192))
        .await;

    // Drive 2: must replay, not re-dispatch and not halt.
    let out2 = Executor::new(gateway.clone(), Arc::new(journal.clone()), "v1")
        .with_registry(over_window_agent_registry())
        .start(run, &graph)
        .await
        .expect("drive 2 must not return DeterminismViolation");
    assert!(out2.failed.is_none(), "no failure on replay: {:?}", out2.failed);
    assert_eq!(
        calls.lock().unwrap_or_else(|e| e.into_inner()).len(),
        calls_after_1,
        "and ZERO re-spend — the turn replayed from its memo rather than being re-dispatched \
         against a budget recomputed from the new window"
    );
}
```

Both `update_config` (returns `()`, no validation) and `try_update_config` (returns
`Result<(), GatewayError>`) exist at `crates/gateway/src/engine/mod.rs:512` and `:521`. Use the
former: this test wants the UNVALIDATED swap, because that is the path an operator's config edit
actually takes and the one the fence cannot see.

- [ ] **Step 2: Run and watch it fail**

```
cargo test -p sensei-orchestrator --lib -- a_budgeted_turn_replays_after_the_window_changes > /tmp/t6.log 2>&1; echo "exit=$?"
```
Expected: **PASS**, because Task 5 already implemented the replay. **If it passes on arrival, that
is correct and expected here** — this task is the guard for work already done, so its
non-vacuity must be established by mutation instead (step 3). Say so explicitly rather than
treating the pass as a red-first failure.

- [ ] **Step 3: Mutation-verify — the important step**

Apply each mutation, run the test, restore:

1. **Ignore the journal:** change `let journaled = fold.context_budgets.get(&budget_eid).copied();`
   to `let journaled: Option<u64> = None;`. Expected: **FAIL** — drive 2 recomputes from the new
   window, the cut differs, `DeterminismViolation`. This is the mutation that proves the whole
   design.
2. **Fold LAST-wins:** already covered by Task 4's guard.

Record both outcomes in the commit message.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/tests.rs
git commit -F /tmp/msg-t6.txt
```
`/tmp/msg-t6.txt`:
```
test(orchestrator): pin the SP-7b replay property against a CHANGED window

The slice's central claim, and the changed window is what makes it
non-vacuous: pinning a replay with an unchanged window passes without any of
the journaling, because the recomputed budget happens to match. Drive 1
budgets against a 128k window, the gateway config is swapped, and drive 2
replays drive 1's cut with zero re-spend because it reads budget_bytes out of
the journal and never asks the gateway.

Passed on arrival, which is correct -- task 5 implemented the replay -- so
non-vacuity is established by mutation rather than by a red run, and this
commit says so rather than claiming a red-first cycle it did not have.

Mutation-verified: forcing the journaled lookup to None makes drive 2
recompute from the new window, the cut differs, and the run dies with
DeterminismViolation. That is the failure the journaling exists to prevent,
and per the M1-reversal work it is unrecoverable.
```

---

### Task 7: Disclosure — the output key and the operator warn (AC10)

> **SHIPPED, and here is how it deviates — read this before trusting the steps below.**
>
> - **Step 1's test fixture does not work.** It reaches for
>   `two_window_clamp_observing_gateway` and wires neither a `ContentStore` nor a
>   `ContextStore`. `ClampObservingAdapter` answers every call with one fixed short body, so A
>   never produces an oversized output; and `resolve_context` returns EMPTY without a
>   `ContextStore` (`executor/mod.rs`), so B would get no dependency context either way. The
>   shipped test uses AC2's fixture instead — `two_window_scripted_window_watching_gateway`
>   plus both stores — which is the only combination in the harness that can produce a
>   degraded turn at all.
> - **Channel 1 IS asserted, contrary to step 1's note.** The plan deferred it to
>   `prompt::tests`, which proves the RENDERER emits a marker but not that the marked bytes
>   reached the provider — and "the model was told" is the channel the floor's whole argument
>   rests on. `ScriptedAdapter` gained a `SystemLog`, and
>   `two_window_scripted_window_watching_gateway` returns it (a wider return rather than a
>   fourth constructor: the sibling's own "thirty callers" argument for splitting does not
>   apply to a function with three).
> - **The `N of M dependencies shown` tail is NOT asserted, and cannot be here.** It announces
>   DROPPED entries; a one-dependency fixture truncates rather than drops (`dropped_deps == 0`).
>   AC10 and §5.5 were amended to stop conflating the two model-facing signals.
> - **Step 4's warn is asserted against the JOURNAL ROW**, not against re-derived arithmetic, so
>   a warn wired to the wrong field cannot agree with a copy of its own numbers. Mutation:
>   swapping `requested_bytes` and `retained_bytes` reddens it.
> - **Step 6's AC11 test passed on arrival**, as a no-regression test should. Mutation-verified
>   two ways: emitting the output key unconditionally, and warning wherever the window is READ
>   rather than where a cut is TAKEN, each redden exactly one of its absence assertions.
> - **Carry-forward (a) changed the CODE, not just a comment.** `pause_context_floor` takes
>   `retained: Option<usize>`; the pre-render `FloorUnreachable` arm passes `None`. See AC9.
>
> **A false literal found while doing this**, recorded because that is now the fourth in this
> slice: the AC2 test's comment decomposing the 66-byte section overhead said the marker read
> `of 700030 bytes shown`. It is 700 027 — A's stored context is its whole output VALUE, so the
> envelope is `22 + "small".len()`. No assertion consumed the literal, which is why nothing
> caught it.

**Files:**
- Modify: `crates/orchestrator/src/executor/agent.rs:460` (the output shape) and the budget site
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the failing test**

```rust
/// AC10 — all four disclosure channels fire on a degraded turn.
///
/// The hazard the old docs name is a degraded answer flowing downstream as work product
/// indistinguishable from a full one. Four channels answer it, and this test is the only place
/// their COMPOSITION is asserted; each is individually cheap to break without the others
/// noticing.
#[tokio::test]
async fn a_degraded_turn_discloses_on_every_channel() {
    let (gateway, _calls, _models, _ests) = two_window_clamp_observing_gateway(10, 100).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let out = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(over_window_agent_registry())
        .start(run, &oversized_context_graph())
        .await
        .expect("drives");

    // 3. Downstream: an ADDITIVE key, so an unmodified BranchCond::TextContains still works.
    let b = out
        .outputs
        .get(&NodeId("B".into()))
        .expect("the budgeted node completed");
    assert_eq!(
        b.get("context_budgeted").and_then(|v| v.as_bool()),
        Some(true),
        "the output must say the context was degraded: {b}"
    );
    assert!(
        b.get("text").is_some(),
        "and `text` must be untouched beside it — additive, like SP-6 s3's `actor`: {b}"
    );

    // 2. The journal.
    let events = journal.load(run).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|(_, e)| matches!(e, JournalEvent::ContextBudgeted { .. }))
            .count(),
        1,
        "exactly one ContextBudgeted (AC3)"
    );
}
```

Channel 1 (the prompt marker) is asserted by `prompt::tests`; channel 4 (the warn) is asserted in
step 4.

- [ ] **Step 2: Run and watch it fail**

```
cargo test -p sensei-orchestrator --lib -- a_degraded_turn_discloses_on_every_channel > /tmp/t7.log 2>&1; echo "exit=$?"
```
Expected: **FAIL** — no `context_budgeted` key.

- [ ] **Step 3: Add the output key**

At `agent.rs:460`, replace

```rust
            serde_json::json!({ "model": model, "text": text }),
```

with

```rust
            {
                // SP-7b channel 3. ADDITIVE, and that is load-bearing: the output stays
                // `{"model", "text"}` plus one key, so an unmodified `BranchCond::TextContains`
                // consumes a degraded answer exactly as before — the same discipline SP-6 s3 used
                // when it added `actor` to a human-backed agent's output. Emitted only when the
                // turn WAS degraded, so an in-window turn's output is byte-identical (AC11).
                let mut out = serde_json::json!({ "model": model, "text": text });
                if ar.context_cut.is_some()
                    && let Some(obj) = out.as_object_mut()
                {
                    obj.insert("context_budgeted".to_string(), serde_json::Value::Bool(true));
                }
                out
            },
```

- [ ] **Step 4: Add the warn**

Immediately after the `ContextBudgeted` append in `agent.rs`:

```rust
                        // SP-7b channel 4. The instrument, in SP-DATA-5's AC11 style: the floor
                        // fraction is a guess, and this is what will replace it with a number.
                        // WARN rather than info because a degraded answer is a real reduction in
                        // the work product's quality and an operator who never sees one cannot
                        // know it is happening.
                        tracing::warn!(
                            node = %node_id.0,
                            window = w,
                            requested_bytes = c.requested_bytes,
                            retained_bytes = c.retained_bytes,
                            dropped_deps = c.deps_total - c.deps_shown,
                            dropped_tools = plan.dropped_tools.len(),
                            "SP-7b: agent context budgeted — this turn answers on a reduced context"
                        );
```

- [ ] **Step 5: Run and watch it pass**

```
cargo test -p sensei-orchestrator --lib -- a_degraded_turn_discloses_on_every_channel > /tmp/t7.log 2>&1; echo "exit=$?"
```
Expected: `exit=0`.

- [ ] **Step 6: Add the AC11 no-regression test**

```rust
/// AC11 — an IN-WINDOW turn is unchanged where it matters.
///
/// Deliberately NOT claiming "nothing new runs": deciding whether a prompt is over-window requires
/// knowing the window, so `max_context_window` is read on every agent turn, in-window ones
/// included. That read is a `config.read().await` returning a `u32` — no allocation, no I/O — and
/// it is the honest cost of the feature. An earlier draft of this AC claimed the accessor is not
/// called at all, which is false by construction.
#[tokio::test]
async fn an_in_window_agent_turn_is_unchanged() {
    let (gateway, calls) = clamp_observing_gateway(10, 100).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let out = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(tool_agent_registry())
        .start(run, &Graph { nodes: vec![agent_node("n1", "a", "hi")] })
        .await
        .expect("drives");

    let events = journal.load(run).await.unwrap();
    assert!(
        !events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::ContextBudgeted { .. })),
        "no budget is journaled for a prompt that fits"
    );
    let n1 = out.outputs.get(&NodeId("n1".into())).expect("completed");
    assert!(
        n1.get("context_budgeted").is_none(),
        "and no disclosure key is added: {n1}"
    );
    assert_eq!(
        calls.lock().unwrap_or_else(|e| e.into_inner()).len(),
        1,
        "and exactly one provider call, as before"
    );
}
```

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/agent.rs crates/orchestrator/src/executor/tests.rs
git commit -F /tmp/msg-t7.txt
```
`/tmp/msg-t7.txt`:
```
feat(orchestrator): disclose a budgeted turn on all four channels

The hazard the old docs name is a degraded answer flowing downstream as work
product indistinguishable from a full one. The prompt marker and the
"N of M dependencies shown" tail come free from the reused truncator; this
adds the other two.

The output key is ADDITIVE and that is load-bearing: the output stays
{"model","text"} plus one key, so an unmodified BranchCond::TextContains
consumes a degraded answer exactly as before -- the discipline SP-6 s3 used
when it added actor. Emitted only when the turn was degraded, so an in-window
turn's output is byte-identical.

The warn is in SP-DATA-5's AC11 style and is the instrument that will replace
CONTEXT_FLOOR_FRACTION's guess with a measurement. WARN rather than info: a
degraded answer is a real reduction in quality and an operator who never sees
one cannot know it is happening.

AC11 claims only what is true. It does NOT claim nothing new runs, because
deciding whether a prompt is over-window requires knowing the window, so the
accessor is read on every agent turn. The spec records that an earlier draft
claimed otherwise and was false by construction.
```

---

### Task 8: Split the guard test, and the doc sweep

> **SHIPPED. What was actually left to do, and what was already done — steps 1 and 2 were BOTH
> already discharged, and re-doing either would have produced a second version of one claim.**
>
> - **Step 1 was fully discharged by Task 5's fix commit `16a344e`**, not merely "largely moot".
>   `oversized_dependency_context_halts_over_budget_never_truncates` already halts below the
>   floor, its doc already attributes the halt to SP-7b's floor rather than the gateway, AND the
>   `context budget: ` prefix assertion was already there — `git log -S 'context budget: '` on
>   `tests.rs` names `16a344e` and `5781e3e`, no third commit. What this task added is the one
>   thing the amendment left open: the NAME judgment, recorded in the doc (every clause still
>   holds literally for this fixture; what it must not be read as is a claim about the model path
>   in general, and the sibling that IS that case is named).
> - **Step 2's either/or resolves to "it still pauses, unchanged", and the fixture says why.**
>   `over_window_agent_registry` is a 100 000-byte `system_prompt` with NO dependencies, so
>   `plan_budget` answers `AuthoredOverBudget`, the un-cut prompt goes to the gate, and the
>   name `an_over_window_agent_prompt_pauses_the_run_with_the_gateways_diagnosis` is still exact.
>   It gained the SP-7b paragraph the step asked for, plus a `!starts_with("context budget: ")`
>   PREMISE assertion, since the run now has two refusals that both name the window.
>   Mutation-verified by routing the `AuthoredOverBudget` arm to `pause_context_floor`: the
>   premise reddens first, printing the absurdity itself — *node n1's dependency context is 0
>   bytes*. **Stated honestly, it is not the only net:** that mutation also reddens the estimate
>   substring, because the floor message carries neither the estimate nor the `route to a model…`
>   remedy. What the premise buys is a failure that says "wrong component" rather than "missing
>   number", and the test's doc says exactly that rather than claiming a guard it does not have.
> - **Carry-forwards (a) and (b) were ALREADY CLOSED before this task started** — (a) in Task 7's
>   round (`pause_context_floor` takes `retained: Option<usize>`, pinned by
>   `the_planner_floor_refusal_reports_no_retained_figure_it_never_measured`), (b) in both places
>   it lived (`available_context_bytes`'s doc and the spec's §2 bullet both now say **at most**
>   and both record that an earlier revision said "exactly"). Re-verified by reading, not
>   re-written.
> - **Beyond the brief:** `durable-journal.md` had no `ContextBudgeted` section at all. The sweep
>   pattern could not find that, because a missing page is not a false sentence — but this slice
>   added a journal variant and a fold discipline to the page whose entire subject is journal
>   variants and fold disciplines. Added, with the three tests that pin it.
> - **Deliberately NOT touched:** the dated SP-7a spec/plan documents that describe SP-7b as a
>   follow-on (`2026-09-03-…-design.md:43,141,290`, `2026-09-04-sp-7a-serving-window-bound-…:48,240`).
>   They were true when written and they are the record of a decision, not a live status page;
>   `orchestrator-overview.md`'s §4 index and §5 SP-7 entry are the live surface and both now say
>   SHIPPED. `min_context_window`'s doc was left alone per the standing instruction.

> **Amended after Task 5's review round — read this before step 1.**
>
> Task 5 shipped work this task's steps assumed it would inherit, and moved two decisions:
>
> - **Step 1 is largely MOOT.** `oversized_dependency_context_halts_over_budget_never_truncates`
>   never went green-on-a-completion: its fixture (a 100 000-byte dependency against a 4096-token
>   window) already falls below the floor, so it halts at SP-7b's floor rather than the gate and
>   needed no resizing. Task 5 rewrote its doc to say so and added the `context budget: ` prefix
>   assertion. What remains for step 1 is a judgment call on whether the NAME still reads right,
>   nothing more.
> - **Step 2 is also moot.** `an_over_window_agent_prompt_pauses_the_run_with_the_gateways_diagnosis`
>   still pauses, unchanged: its fixture has no dependency context, so per §5.3 no cut can fit and
>   the refusal stays the gate's. Neither of the two tasks-5-was-told-to-expect failures ever
>   materialized.
> - **AC9's test must key on the `context budget: ` prefix, not on the window.** Both refusals name
>   the window, which is precisely why the guard test above kept passing when its refusal changed
>   owner. `the_context_floor_pause_is_recoverable_and_spends_nothing` (added in Task 5) already
>   asserts the whole AC9 shape — one pause row, `resume_after: None`, no `NodeFailed`, no budget
>   row, no spend — so what AC9 still owes is the second REFUSAL CONDITION, and §5.3 was amended:
>   a non-positive budget and an over-budget authored half are the GATE's refusal (AC5), not the
>   floor's. Do not write an AC9 test against the pre-amendment wording.
> - **`the_planner_converts_a_token_window_into_a_byte_budget` was NOT the test extended** for this
>   task's self-review item about the transcript decomposition;
>   `the_byte_budget_is_the_gateway_estimators_own_arithmetic` was, since it is the one that calls
>   the estimator. Done in Task 5's review round, not outstanding.

**Files:**
- Modify: `crates/orchestrator/src/executor/tests.rs:4794-4872` (the guard test)
- Modify: `crates/orchestrator/src/executor/tests.rs` (the SP-7a agent-path pause test)
- Modify: `crates/orchestrator/src/agent/prompt.rs` (the two docs that argue against truncating)
- Modify: `docs/superpowers/orchestrator-overview.md`, `docs/features/orchestrator/README.md`,
  `docs/features/orchestrator/shared-context.md:31`,
  `docs/features/orchestrator/agents-skills-tools.md:284`

- [ ] **Step 1: Resize the guard test's fixture so it still halts**

`oversized_dependency_context_halts_over_budget_never_truncates` asserts B produced NO output. Keep
that assertion and the name, and resize the fixture so the retained context falls BELOW the floor —
the invariant moves from the window to the floor rather than being relaxed. Its doc gains:

```rust
/// **SP-7b moved where this halts, and deliberately did not relax it.** Before SP-7b any
/// over-window context halted; now a moderately-over prompt is budgeted and answered
/// (`an_over_window_agent_turn_is_budgeted_and_dispatched`). So this fixture is sized to fall
/// below `CONTEXT_FLOOR_FRACTION`, where the halt survives — and the assertion that B produced no
/// output is unchanged, because that is the property worth keeping: half a document never becomes
/// work product. What changed is that "half" now has a number.
```

Update the pause reason assertion from `4096-token context window` to match
`pause_context_floor`'s wording (`context budget: `).

- [ ] **Step 2: Update the SP-7a agent-path pause test**

`an_over_window_agent_prompt_pauses_the_run_with_the_gateways_diagnosis` drives
`recording_gateway()` with `over_window_agent_registry()`. Under SP-7b that prompt is budgeted, so
it completes. Decide from the fixture which is true and adapt honestly:
- if its context can be budgeted above the floor, the test becomes a BUDGETED completion and its
  name must change (it no longer pauses);
- if it cannot, it keeps pausing but with the new floor reason.

Whichever holds, the test's doc must say that SP-7b moved it and why. Do not delete it.

- [ ] **Step 3: Rewrite the two prompt.rs docs**

`PromptParts::join` and `render_context_section_bounded` both argue the model path must never
truncate. Rewrite both to say what is now true: the model path truncates when nothing can hold the
prompt, the operative word in the old objection was SILENTLY, and the four channels are the answer.
Point each at `join_bounded` and at the spec's §5.5.

- [ ] **Step 4: Sweep the doc surfaces**

```
rg -n --no-ignore -g '!target' 'SP-7b|never truncat|half a document' docs/ crates/ > /tmp/sweep.log; echo "exit=$?"
```
Every hit that states SP-7b is unbuilt, or that the model path never truncates, is now false. Fix
each. `shared-context.md:31` and `agents-skills-tools.md:284` both promise this slice explicitly.

- [ ] **Step 5: The release gate**

```
cargo fmt --all --check; echo "fmt=$?"
cargo clippy --workspace --all-targets -- -D warnings > /tmp/clippy.log 2>&1; echo "clippy=$?"
cargo test --workspace > /tmp/suite.log 2>&1; echo "test=$?"
awk '/^test result:/ {p+=$4; f+=$6; i+=$8} END {printf "passed=%d failed=%d ignored=%d\n", p, f, i}' /tmp/suite.log
cargo doc --workspace --no-deps --document-private-items > /tmp/doc.log 2>&1; echo "doc=$?"
grep -c 'unresolved link' /tmp/doc.log
```
Expected: `fmt=0`, `clippy=0`, `test=0`, `failed=0`, unresolved links **16** (the baseline — a
higher number is new breakage and must be fixed).

- [ ] **Step 6: Commit**

```bash
git add -A crates/ docs/
git commit -F /tmp/msg-t8.txt
```
`/tmp/msg-t8.txt`:
```
test+docs: move the anti-truncation invariant to the floor, not away

The guard test asserting "half a document never becomes work product" is
SPLIT rather than relaxed. Its fixture is resized so the retained context
falls below CONTEXT_FLOOR_FRACTION, where the halt survives and the
no-output assertion is unchanged -- the property worth keeping, now with a
number attached to "half". A sibling asserts that a moderately-over prompt is
budgeted and answered.

PromptParts::join and render_context_section_bounded both argued the model
path must never truncate. Rewritten: the operative word was SILENTLY, and the
four disclosure channels are the answer. Both now point at join_bounded.

Doc surfaces that promised SP-7b as unbuilt, or that stated the model path
never truncates, are corrected -- shared-context.md and
agents-skills-tools.md promised this slice by name.

Release gate: fmt 0, clippy -D warnings 0, cargo test --workspace real exit
0, cargo doc private-item unresolved links at the 16 baseline.
```

---

## Self-review

**Spec coverage:** AC1 → Task 1. AC2 → Task 5. AC3 → Tasks 5, 7. AC4 → Task 6. AC5 → Tasks 2, 3
(purity of `plan_budget` and the measured renderer). AC6 → Task 4. AC7 → Tasks 2, 3. AC8 → Task 3.
AC9 → Task 5 step 6 + Task 8 step 1. AC10 → Task 7. AC11 → Task 7 step 6. AC12 → Task 4.
**All twelve covered.**

**Type consistency:** `BudgetPlan { context_budget_bytes: usize, dropped_tools: Vec<String> }` and
`ContextCut { requested_bytes, retained_bytes, deps_shown, deps_total }` are defined in Task 2 and
used unchanged in Tasks 3, 5 and 7. `available_context_bytes(u32, u32) -> Option<usize>`,
`plan_budget(usize, usize, &[ToolDefinition], &[(String, String)]) -> Option<BudgetPlan>` and
`retained_meets_floor(usize, usize) -> bool` are consistent across every use.
`ContextBudgeted.budget_bytes` is `u64` in the journal and cast at both boundaries, which is
deliberate — journal integers are `u64` throughout — and the casts are named in Task 5.

**Known soft spots, stated rather than hidden:**
- Task 5 step 5 computes the transcript estimate by building a probe request with an empty system
  and no tools. That is a reasonable decomposition of the estimator's sum but it is NOT verified
  against `estimate_input_tokens_pessimistic`'s exact arithmetic. **The implementer must verify it
  and adjust**, and Task 2's `the_planner_converts_a_token_window_into_a_byte_budget` is the test
  that should be extended to pin it.

  **CLOSED in Task 5's review round, in a different test than named here.** The probe is now the
  production function `prompt::transcript_estimate`, and the guard is
  `the_byte_budget_is_the_gateway_estimators_own_arithmetic` — the test that actually calls the
  estimator. `the_planner_converts_a_token_window_into_a_byte_budget` could not have pinned it: it
  asserts a hand-derived literal over this crate's `× 3` and never calls the estimator, which is a
  correction its own doc already carried. What the guard pins: the transcript figure taken off the
  window equals what the estimator charges those same messages inside the real payload, in either
  direction (a probe one token light reddens it, `3841` against `3840`). What it cannot pin, by
  algebra rather than by omission: any term the estimator charges over the MESSAGES cancels,
  because the probe carries the same messages — including the per-message overhead this note's
  reviewer expected to be the hazard.
- `oversized_context_graph()` is specified by reference to an existing fixture rather than written
  out, because its sizing depends on `TWO_WINDOW_SMALL` arithmetic the implementer must compute
  from the constants. Task 5 step 1 says which fixture to model it on.
- Task 8 step 2 leaves a genuine either/or that depends on fixture arithmetic. It is written as a
  decision with both branches specified rather than as a guess.

**Fixed during self-review, recorded because each would have failed at compile time:**
- `ToolDefinition.description` is `Option<String>`, not `String`
  (`kernel/src/types/request.rs:202-203`). Both `tool_bytes` and the `tool_def` test helper assumed
  `String`. `tool_bytes` now mirrors the estimator exactly, including that an ABSENT description is
  priced at zero rather than skipping the tool.
- `Gateway::update_config` returns `()`, not a `Result` (`engine/mod.rs:512`). Task 6's
  `.expect("config swap")` would not have compiled. The plan now also says why that method is the
  right one for the test: it is the unvalidated swap, which is the path an operator's edit takes and
  the one the fence cannot see.

**Amended 2026-09-04 after Task 2's review — `plan_budget` takes the ENTRIES, not a byte total.**
Its real signature is
`plan_budget(available_bytes: usize, authored_bytes: usize, tools: &[ToolDefinition], entries: &[(String, String)]) -> Option<BudgetPlan>`.
Task 2's review found the original signature caused a UNIT MISMATCH: the floor is a fraction of the
raw entry BODIES, but the budget the renderer consumes bounds the whole rendered SECTION — the
`"\n\n## Context"` head, a `### {key}` heading per entry and a truncation marker per truncated entry
all come out of it. A plan approved AT the floor therefore rendered BELOW it (one 1000-byte
dependency approved at 250 retained 189), and the schema-drop loop stopped as soon as the SECTION
bytes cleared a BODY-byte floor — **turning turns that could have been degraded into refusals**,
which is the opposite of this slice's purpose. Taking the entries lets the planner subtract
`context_section_overhead` and compare in the floor's own unit. Task 5's two call sites above pass
`&parts.context`. The Task 1 test's `plan_budget(900, 0, &tools, 500)` at line 239 is illustrative
only and its 4th argument is stale; use entries whose bodies sum to the intended figure.
