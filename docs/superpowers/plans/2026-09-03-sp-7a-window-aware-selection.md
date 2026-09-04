# SP-7a Window-Aware Selection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop failing prompts that a model in the chain could serve, by making model selection aware of each candidate's context window.

**Architecture:** A sixth `AdmissionGate` in `crates/gateway/src/gates/`, mirroring `BudgetGate`. `SelectionCtx` gains a second, deliberately pessimistic token estimate that counts tool schemas and tool calls; the gate skips any candidate whose `context_window` cannot hold it. When every candidate is skipped, the existing `all_gated_error` path turns that into `AllGated` — a durable pause when some gate is TIMED, and a terminal human-action failure when every gate is terminal, which is the over-window case (see the review section below; the original line here said "durable pause" flatly and that was wrong). The orchestrator's pre-dispatch `min_context_window` check and its terminal `PromptOverBudget` failure are deleted.

**Tech Stack:** Rust. Tests are plain `cargo test` with `assert!`/`assert_eq!`/`matches!`.

**Spec:** `docs/superpowers/specs/2026-09-03-sp-7a-window-aware-selection-design.md` — criteria referenced as `AC1`…`AC10`.

---

## Preconditions (verified 2026-09-03 — re-verify if time has passed)

- `AdmissionGate` trait, `CandidateView`, `SelectionCtx`, `GateVerdict` all in
  `crates/gateway/src/gates/mod.rs:26-54`. **Confirmed.**
- The gate list is built in `ModelSelectionService::new`, `crates/gateway/src/selection.rs:84-95`,
  currently five gates in order: `CapabilityGate`, `ConnectionCooldownGate`, `CircuitBreakerGate`,
  `ModelLockoutGate`, `BudgetGate`. **Confirmed.**
- `SelectionCtx` is constructed at `selection.rs:148-157` from `SelectionCriteria`
  (`selection.rs:20-27`), which already carries `input_tokens: Option<u32>`. **Confirmed.**
- `estimate_input_tokens` is `crates/gateway/src/engine/util.rs`, `(message_chars + system_chars) / 4`
  — it does **not** count tool schemas. Called once at `engine/execute.rs:43`. **Confirmed.**
- `SkipReason` (`crates/gateway/src/skip_reason.rs:17`) and its `gate_status()` (`:59`) classify each
  reason `Timed` / `Terminal(HumanAction)` / `Structural`. `OverBudget` is
  `Terminal(HumanAction::RaiseBudget)`. **Confirmed.**
- `HumanAction` is in **`crates/kernel/src/types/error.rs:9`** — `TopUpCredits`, `RotateCredential`,
  `RaiseBudget`. None fits "your prompt is bigger than every model's window". **Confirmed.**
- `all_gated_error` (`crates/gateway/src/engine/exhaustion.rs:48`) turns an all-skipped selection
  into an `AllGated` when no `HardFailure` contributed. **Confirmed** — and re-read during the
  Task 1–4 review: `resume_after` comes from the TIMED skips only, so it is a durable pause only
  when at least one gate is timed. All-terminal ⇒ `resume_after: None` ⇒ the orchestrator FAILS the
  node. The original wording of this precondition ("into a durable pause") is what the slice's
  headline claim was built on, and it was too loose.
- The orchestrator halt is `executor/agent.rs:368-378`: `over_budget(ar.min_win, …)` →
  `OrchestratorError::PromptOverBudget` → `NodeFailed` → `ToolOutcome::Failed`. `min_win` is set at
  `agent.rs:271` from `gateway.min_context_window(&chain)`. **Confirmed.**

## Review of Tasks 1–4 (2026-09-03) — what changed, and what it moved into later tasks

Five reviewers went over the four landed commits. Two findings were Critical, and both were the
same one: **the slice's headline claim did not hold.** The corrections are in the spec; the parts
that land in this plan:

- **`AllGated` is not a pause when every gate is terminal.** `all_gated_error` takes `resume_after`
  from TIMED skips only, and `classify_gateway_error` pauses only on `Some(t)`. An
  over-window-everything request was, and remains, a terminal `NodeFailed`. Spec §3 / §2 / AC3 are
  corrected; the two comments in `skip_reason.rs` that asserted otherwise are rewritten. Reversing
  that behaviour is now an explicit **deferred** item with its argument, not an assumed benefit.
- **`AllGated` rendered neither `skipped` nor `human_action`.** Every number this slice adds was
  being dropped at the orchestrator boundary, which would have made Task 6 a diagnostics
  REGRESSION against `PromptOverBudget`. Fixed in `kernel::types::error` with its own test; this is
  what makes the corrected AC3 worth having.
- **The estimator priced an assistant turn's `tool_calls` at zero** — the ReAct loop's own shape.
  Now counted. Attachments stay uncounted, and the doc and spec §4 now say so plainly instead of
  letting "pessimistic" imply completeness.
- **Six tests could not fail** (the tools term guarded only as a whole, `/3` and `div_ceil`
  unpinned, bytes-vs-chars untested, Stt's AC10 half satisfied by any value, the gate's window
  frozen to a constant, the message's two numbers swappable). Each now has a named mutation that
  reddens it, quoted in its commit.

**Still open, and owned by the later tasks:** the composed `engine::execute` wiring test (Task 5),
the enumeration sweep now that a sixth gate exists (Task 5 + 7), and the SP-DATA-5 clamp
interaction that bounds where AC1 applies (Task 6, step 0).

## Working rules for every task

- **Red first.** Write the test, run it, see it fail *for the stated reason*, then implement. A test
  that passes before the implementation is a finding — say so rather than moving on.
- `cargo fmt --all` before every commit. The pre-commit hook is fmt-check + workspace
  `clippy -D warnings` and runs **no tests** — run `cargo test --workspace` yourself.
- Verify **real** exit codes: `cmd > /tmp/x.log 2>&1; echo "exit=$?"`. Never judge from a piped `tail`.
- **Never** run the DB suite against `$DATABASE_URL` — it is remote Supabase. No task here needs a DB.
- Match the house comment style: long doc comments that argue WHY. A comment asserting something
  false is worse than no comment.

## File structure

| File | Responsibility | Task |
|---|---|---|
| `crates/kernel/src/types/error.rs` | `HumanAction::UseLargerContextWindow` | 1 |
| `crates/gateway/src/skip_reason.rs` | `SkipReason::OverContextWindow` + Display + `gate_status` | 2 |
| `crates/gateway/src/engine/util.rs` | `estimate_input_tokens_pessimistic` | 3 |
| `crates/gateway/src/gates/context_window.rs` | the gate itself (**new file**) | 4 |
| `crates/gateway/src/gates/mod.rs` | `SelectionCtx.input_tokens_pessimistic`, `pub mod` | 4 |
| `crates/gateway/src/selection.rs` | criteria field, ctx construction, gate registration | 5 |
| `crates/gateway/src/engine/execute.rs` | compute + pass the pessimistic estimate | 5 |
| `crates/orchestrator/src/executor/agent.rs` | delete the pre-check and `min_win` | 6 |
| `crates/orchestrator-core/src/error.rs` | delete `PromptOverBudget` | 6 |
| docs | overview, feature docs, checkpoint | 7 |

---

### Task 1: `HumanAction::UseLargerContextWindow`

**Files:**
- Modify: `crates/kernel/src/types/error.rs:9-16`

- [x] **Step 1: Add the variant**

```rust
    /// Every candidate's context window is smaller than the request — no waiting helps,
    /// and no budget change helps either. The human must route to a model with a larger
    /// window (widen or reorder the chain) or make the request smaller.
    ///
    /// Distinct from `RaiseBudget`, which is about MONEY: an over-budget skip is fixed by
    /// spending more at the same model, and this one cannot be fixed by spending at all.
    /// Rendering them alike would send an operator to the wrong lever.
    UseLargerContextWindow,
```

- [x] **Step 2: Verify it compiles**

Run: `cargo build -p sensei-kernel > /tmp/t1.log 2>&1; echo "exit=$?"`
Expected: `exit=0`. If any `match` over `HumanAction` is exhaustive elsewhere, the workspace build in Step 3 will name it.

- [x] **Step 3: Find every exhaustive match**

Run: `cargo build --workspace > /tmp/t1b.log 2>&1; echo "exit=$?"; grep -A6 'non-exhaustive\|E0004' /tmp/t1b.log | head -20`

Any site that renders `HumanAction` for an operator needs an arm. Add one that says: use a model
with a larger context window, or send less. Do **not** collapse it into the `RaiseBudget` arm.

- [x] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/kernel/src/types/error.rs
git commit -m "feat(kernel): HumanAction::UseLargerContextWindow

Distinct from RaiseBudget, which is about money. An over-window skip
cannot be fixed by spending more at the same model; the operator has to
route to a bigger window or send less. Rendering them alike would point
them at the wrong lever."
```

---

### Task 2: `SkipReason::OverContextWindow`

**Files:**
- Modify: `crates/gateway/src/skip_reason.rs` — the enum (`:17`), its `Display` (`:45` area), `gate_status` (`:74` area), and the inline tests

- [x] **Step 1: Write the failing test**

Add to `skip_reason.rs`'s `mod tests`:

```rust
    /// An over-window skip is TERMINAL and points at the window, not at money.
    ///
    /// `Timed` would be wrong — no deadline passes that makes a model's window bigger —
    /// and `Structural` would be wrong too, because the candidate is perfectly well
    /// configured; it is this REQUEST that does not fit it. Terminal is the classification
    /// that carries a `HumanAction`; a Structural skip contributes nothing to
    /// `all_gated_error` and would surface as a bare `NoCandidates`.
    ///
    /// (As SHIPPED this doc says more than the draft did, and less: the review found that
    /// Terminal does NOT make the run pausable either — see the spec's §3 row. The
    /// classification is unchanged; only the reason for it was wrong.)
    #[test]
    fn over_context_window_is_terminal_and_names_the_window_remedy() {
        let r = SkipReason::OverContextWindow {
            estimated: 20_000,
            window: 8_192,
        };
        assert!(
            matches!(
                r.gate_status(),
                GateStatus::Terminal(HumanAction::UseLargerContextWindow)
            ),
            "over-window must be terminal with the window remedy, got {:?}",
            r.gate_status()
        );
        let shown = r.to_string();
        assert!(
            shown.contains("20000") && shown.contains("8192"),
            "the message must name BOTH the estimate and the window it exceeded, so an \
             operator can see how far over they are: {shown}"
        );
    }
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p sensei-gateway over_context_window_is_terminal > /tmp/t2.log 2>&1; echo "exit=$?"; tail -20 /tmp/t2.log`
Expected: a COMPILE ERROR — `no variant named OverContextWindow`.

- [x] **Step 3: Add the variant**

In the `SkipReason` enum, after `OverBudget`:

```rust
    /// The candidate's context window cannot hold this request's estimated input.
    ///
    /// Carries both numbers because the remedy depends on the gap: a request slightly
    /// over a small model's window is a routing problem, and one over every window is a
    /// prompt problem. A single "too big" would not distinguish them.
    OverContextWindow {
        estimated: u32,
        window: u32,
    },
```

In `Display`:

```rust
            SkipReason::OverContextWindow { estimated, window } => {
                write!(
                    f,
                    "estimated {estimated} input tokens exceeds the model's {window}-token \
                     context window"
                )
            }
```

In `gate_status`, beside the `OverBudget` arm:

```rust
            SkipReason::OverContextWindow { .. } => {
                GateStatus::Terminal(HumanAction::UseLargerContextWindow)
            }
```

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p sensei-gateway skip_reason > /tmp/t2b.log 2>&1; echo "exit=$?"; grep -E '^test result' /tmp/t2b.log`
Expected: `exit=0`, all pass. The pre-existing `gate_status_classifies_each_reason` must still pass **unmodified** — if it reddens, you changed an existing classification.

- [x] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/gateway/src/skip_reason.rs
git commit -m "feat(gateway): SkipReason::OverContextWindow

Terminal, not Timed — no deadline makes a window bigger — and not
Structural, because the candidate is well configured; it is the REQUEST
that does not fit it. Carries both numbers, because a request slightly
over one small model's window is a routing problem while one over every
window is a prompt problem, and 'too big' cannot tell them apart."
```

---

### Task 3: The pessimistic estimate

**Files:**
- Modify: `crates/gateway/src/engine/util.rs`

- [x] **Step 1: Write the failing tests**

In `util.rs`'s test module:

```rust
    /// The window estimate must be >= the cost estimate for the same payload, and
    /// strictly greater once tool schemas are present.
    ///
    /// `estimate_input_tokens` counts messages + system at `chars/4`. It omits tool
    /// schemas entirely — and an agent's activated schemas routinely outweigh its prompt.
    /// For COST that is optimistic pricing; for a WINDOW it admits a candidate the
    /// request does not fit, which is the failure the gate exists to prevent. So this one
    /// counts the schemas and uses the JSON-ish `/3` rather than the prose `/4`. (As
    /// SHIPPED it also counts an assistant turn's `tool_calls`, which the draft missed and
    /// the review caught, and divides BYTES rather than characters.)
    #[test]
    fn the_pessimistic_estimate_counts_tools_and_never_undercuts_the_cost_estimate() {
        let no_tools = Payload::Chat {
            messages: vec![Message::user("hello there, this is a prompt")],
            system: Some("you are a helpful assistant".into()),
            max_tokens: None,
            temperature: None,
            tools: Vec::new(),
        };
        assert!(
            estimate_input_tokens_pessimistic(&no_tools) >= estimate_input_tokens(&no_tools),
            "must never undercut the cost estimate even with no tools"
        );

        let with_tools = Payload::Chat {
            messages: vec![Message::user("hello there, this is a prompt")],
            system: Some("you are a helpful assistant".into()),
            max_tokens: None,
            temperature: None,
            tools: vec![ToolDefinition {
                name: "fs_write".into(),
                description: "Write a file to the workspace".into(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "contents": { "type": "string" }
                    },
                    "required": ["path", "contents"]
                }),
            }],
        };
        assert!(
            estimate_input_tokens_pessimistic(&with_tools)
                > estimate_input_tokens_pessimistic(&no_tools),
            "adding a tool schema must raise the estimate — the schemas are exactly what \
             the cost estimator omits"
        );
    }
```

Check `ToolDefinition`'s real field names in `crates/kernel/src/types/request.rs` and `Message`'s
real constructor in the same file before writing — adapt to what is there rather than assuming.

- [x] **Step 2: Run to verify failure**

Run: `cargo test -p sensei-gateway the_pessimistic_estimate_counts_tools > /tmp/t3.log 2>&1; echo "exit=$?"; tail -20 /tmp/t3.log`
Expected: COMPILE ERROR — `cannot find function estimate_input_tokens_pessimistic`.

- [x] **Step 3: Implement**

Add to `util.rs`, beside `estimate_input_tokens`:

```rust
/// A deliberately pessimistic input estimate, for the CONTEXT-WINDOW gate only.
///
/// Two differences from [`estimate_input_tokens`], and both matter in the same direction:
///
/// 1. **It counts tool schemas.** The cost estimator sums messages + system and stops. An
///    agent's activated schemas routinely outweigh its prompt, so omitting them is not a
///    rounding error — it is most of the payload on exactly the requests this gate exists
///    to catch.
/// 2. **`/3`, not `/4`, over BYTES.** The `/4` figure is the rough one for English
///    prose; JSON tokenizes nearer 3 chars/token, and schemas are pure JSON.
///
/// Kept SEPARATE rather than widening the shared estimator, because the two gates want
/// opposite biases over the same payload: an under-count is optimistic pricing for the
/// cost gate and an admitted-but-doesn't-fit candidate for this one. Changing the shared
/// figure would silently make every `BudgetGate` decision more conservative — a real
/// improvement, and a different slice's call.
pub(super) fn estimate_input_tokens_pessimistic(payload: &Payload) -> u32 {
    let chars: usize = match payload {
        Payload::Chat {
            messages,
            system,
            tools,
            ..
        } => {
            let msg: usize = messages.iter().map(|m| m.as_text().len()).sum();
            let sys: usize = system.as_ref().map(|s| s.len()).unwrap_or(0);
            let tls: usize = tools
                .iter()
                .map(|t| {
                    t.name.len()
                        + t.description.as_ref().map(|d| d.len()).unwrap_or(0)
                        + t.input_schema.to_string().len()
                })
                .sum();
            msg + sys + tls
        }
        Payload::Embed { texts } => texts.iter().map(|t| t.len()).sum(),
        Payload::Stt { .. } => 0,
        Payload::Tts { text, .. } => text.len(),
        Payload::ImageGenerate { prompt, .. } | Payload::VideoGenerate { prompt, .. } => {
            prompt.len()
        }
    };
    u32::try_from(chars.div_ceil(3)).unwrap_or(u32::MAX)
}
```

**Corrected against the real types while implementing** (the draft above now matches what shipped):
`ToolDefinition` is `{ name: String, description: Option<String>, input_schema: Value }`, not
`{ description: String, parameters: Value }`; `Message` exposes `as_text()` directly. And the
draft's `_ => 0` catch-all was **wrong** — it contradicted the instruction below it and would have
made the pessimistic estimate *under*-count a `Tts` / `ImageGenerate` / `VideoGenerate` payload
(0 against the cost estimate's `chars/4`), inverting AC7, the one ordering the gate's safety rests
on. Every non-chat arm mirrors `estimate_input_tokens` at `/3`; `Stt` alone stays 0 in both,
because audio bytes are not characters.

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p sensei-gateway estimate_input_tokens > /tmp/t3b.log 2>&1; echo "exit=$?"; grep -E '^test result' /tmp/t3b.log`
Expected: `exit=0`. The existing `estimate_input_tokens` tests must pass **unmodified**.

- [x] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/gateway/src/engine/util.rs
git commit -m "feat(gateway): a pessimistic input estimate that counts tool schemas

estimate_input_tokens sums messages + system at chars/4 and omits tool
schemas entirely. An agent's activated schemas routinely outweigh its
prompt, so for a WINDOW check that under-count admits a candidate the
request does not fit — the failure the gate exists to prevent. For COST
the same under-count is merely optimistic pricing.

Separate rather than widening the shared estimator: the two gates want
opposite biases over one payload, and changing the shared figure would
silently make every BudgetGate decision more conservative."
```

---

### Task 4: The gate

**Files:**
- Create: `crates/gateway/src/gates/context_window.rs`
- Modify: `crates/gateway/src/gates/mod.rs` — `pub mod context_window;` and the new `SelectionCtx` field

- [x] **Step 1: Write the failing test**

In the new file's `mod tests`, modelled on `budget.rs`'s test module — read that first for the
`CandidateView` / `SelectionCtx` fixture shape and reuse it:

```rust
    /// AC5 — a missing estimate ADMITS. The gate is not a filter on absent data.
    ///
    /// Mirrors `BudgetGate`, which admits a model with no pricing: an absent estimate is
    /// not evidence of a problem, and skipping on it would refuse every request that did
    /// not carry one.
    #[test]
    fn no_estimate_admits() {
        let mc = model_with_window(8_192);
        assert!(matches!(
            ContextWindowGate.evaluate(&cand(&mc), &ctx(None)),
            GateVerdict::Admit
        ));
    }

    /// A request inside the window admits; one over it is skipped with BOTH numbers.
    #[test]
    fn over_window_skips_and_under_window_admits() {
        let mc = model_with_window(8_192);
        assert!(matches!(
            ContextWindowGate.evaluate(&cand(&mc), &ctx(Some(8_192))),
            GateVerdict::Admit
        ));
        match ContextWindowGate.evaluate(&cand(&mc), &ctx(Some(8_193))) {
            GateVerdict::Skip(SkipReason::OverContextWindow { estimated, window }) => {
                assert_eq!(estimated, 8_193);
                assert_eq!(window, 8_192);
            }
            other => panic!("expected an OverContextWindow skip, got {other:?}"),
        }
    }
```

`8_192` admitting and `8_193` skipping pins the boundary as `est > window`, not `>=` — a request
exactly filling the window leaves no room for output, but that is the **clamp's** concern
(SP-DATA-5 bounds `max_tokens` by the window), not this gate's. Say so in a comment so the next
reader does not "fix" it.

- [x] **Step 2: Run to verify failure**

Run: `cargo test -p sensei-gateway context_window > /tmp/t4.log 2>&1; echo "exit=$?"; tail -20 /tmp/t4.log`
Expected: COMPILE ERROR — the module does not exist.

- [x] **Step 3: Add the field to `SelectionCtx`**

In `gates/mod.rs`, after `input_tokens`:

```rust
    /// The pessimistic estimate, for [`context_window::ContextWindowGate`] only.
    ///
    /// A SECOND field rather than a replacement for `input_tokens`: the cost gate and the
    /// window gate want opposite biases over the same payload (see
    /// `engine::util::estimate_input_tokens_pessimistic`), and collapsing them to one
    /// number is exactly what that reasoning rules out.
    pub input_tokens_pessimistic: Option<u32>,
```

Add `pub mod context_window;` to the module list.

- [x] **Step 4: Write the gate**

```rust
use super::{AdmissionGate, CandidateView, GateVerdict, SelectionCtx};
use crate::skip_reason::SkipReason;

/// Gate: the candidate's `context_window` must be able to hold this request's estimated
/// input.
///
/// The sixth gate, and the one that makes the orchestrator's old pre-dispatch check
/// unnecessary. That check tested the prompt against `min_context_window(chain)` — the
/// SMALLEST window in the chain — and failed the node terminally, so a chain of
/// `[gpt-4o 128k, fallback 8k]` refused a 20k prompt the primary would have served. Here
/// the question is asked per CANDIDATE, which is the only place it has a correct answer.
///
/// `None` admits, matching `BudgetGate`'s treatment of a model with no pricing: an absent
/// estimate is not evidence of a problem.
///
/// Strictly `>`: a request that exactly fills the window is admitted. It leaves no room
/// for output, but bounding output is the SP-DATA-5 clamp's job (it caps `max_tokens` by
/// the window), and duplicating that judgement here would skip candidates the clamp can
/// still serve.
pub struct ContextWindowGate;

impl AdmissionGate for ContextWindowGate {
    fn name(&self) -> &'static str {
        "context_window"
    }

    fn evaluate(&self, c: &CandidateView<'_>, x: &SelectionCtx<'_>) -> GateVerdict {
        match x.input_tokens_pessimistic {
            Some(est) if est > c.model_config.context_window => {
                GateVerdict::Skip(SkipReason::OverContextWindow {
                    estimated: est,
                    window: c.model_config.context_window,
                })
            }
            _ => GateVerdict::Admit,
        }
    }
}
```

- [x] **Step 5: Run — expect every OTHER `SelectionCtx` construction to break**

Run: `cargo build --workspace --all-targets > /tmp/t4b.log 2>&1; echo "exit=$?"; grep -c 'missing field' /tmp/t4b.log`

Adding a field breaks every struct literal. Fix each by threading `input_tokens_pessimistic`
through; in test fixtures `None` is correct unless the test is about this gate. Then:

Run: `cargo test -p sensei-gateway context_window > /tmp/t4c.log 2>&1; echo "exit=$?"; grep -E '^test result' /tmp/t4c.log`
Expected: `exit=0`, 2 passed.

- [x] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/gateway/src/gates/context_window.rs crates/gateway/src/gates/mod.rs
git commit -m "feat(gateway): ContextWindowGate

Asks per CANDIDATE what the orchestrator asked once against the chain's
SMALLEST window. None admits, mirroring BudgetGate on a model with no
pricing. Strictly >, because bounding output is the clamp's job and
duplicating that judgement here would skip candidates it can still serve."
```

---

### Task 5: Register the gate and plumb the estimate

**Files:**
- Modify: `crates/gateway/src/selection.rs:20-27` (criteria), `:84-95` (gate list), `:148-157` (ctx)
- Modify: `crates/gateway/src/engine/execute.rs:43-51`

- [x] **Step 1: Write the failing test (AC1, AC3, AC4)**

In `selection.rs`'s test module — read its existing fixtures for how a two-model chain config is
built and reuse them:

```rust
    /// AC1 — a heterogeneous chain serves a prompt only the primary can hold.
    ///
    /// This is the whole slice in one assertion. Before it, the orchestrator refused this
    /// request outright against the chain's 8k MINIMUM, never asking the 128k primary.
    #[test]
    fn a_chain_serves_a_prompt_only_its_larger_model_can_hold() {
        let cfg = two_model_chain_windows(128_000, 8_192);
        let svc = service(&cfg);
        let result = svc.select_all(&criteria_with_pessimistic(Some(20_000)));
        let admitted: Vec<_> = result.all_candidates.iter().map(|c| c.model.clone()).collect();
        assert!(
            admitted.contains(&"big".to_string()),
            "the 128k model must be admitted: {admitted:?}"
        );
        assert!(
            !admitted.contains(&"small".to_string()),
            "the 8k model cannot hold 20k and must be skipped: {admitted:?}"
        );
    }

    /// AC3 — over EVERY window is an all-gated selection, recorded with a typed reason
    /// per candidate rather than degrading to a bare `NoCandidates`. (What the caller
    /// then does with it is asserted at the engine boundary — see the self-review test —
    /// and it is a terminal failure, not a pause.)
    #[test]
    fn a_prompt_over_every_window_gates_every_candidate() {
        let cfg = two_model_chain_windows(128_000, 8_192);
        let svc = service(&cfg);
        let result = svc.select_all(&criteria_with_pessimistic(Some(200_000)));
        assert!(
            result.all_candidates.is_empty(),
            "nothing can hold 200k: {:?}",
            result.all_candidates
        );
        assert!(
            result.skipped.iter().any(|s| matches!(
                s.reason,
                SkipReason::OverContextWindow { .. }
            )),
            "and the skips must say why: {:?}",
            result.skipped
        );
    }

    /// AC4 — an in-window request selects exactly as before. The additivity guarantee.
    #[test]
    fn an_in_window_request_selects_unchanged() {
        let cfg = two_model_chain_windows(128_000, 8_192);
        let svc = service(&cfg);
        let with = svc.select_all(&criteria_with_pessimistic(Some(1_000)));
        let without = svc.select_all(&criteria_with_pessimistic(None));
        let names = |r: &SelectionResult| -> Vec<String> {
            r.all_candidates.iter().map(|c| c.model.clone()).collect()
        };
        assert_eq!(
            names(&with),
            names(&without),
            "a request that fits every window must select the same candidates, in the \
             same order, as one carrying no estimate at all"
        );
    }
```

Write `two_model_chain_windows(big: u32, small: u32) -> GatewayConfig` and
`criteria_with_pessimistic(est: Option<u32>) -> SelectionCriteria` as local helpers, modelled on the
existing fixtures in that module. Check `SelectionResult`'s real field names before using
`all_candidates` / `skipped`.

- [x] **Step 2: Run to verify failure**

Run: `cargo test -p sensei-gateway -- a_chain_serves_a_prompt a_prompt_over_every_window an_in_window_request > /tmp/t5.log 2>&1; echo "exit=$?"; tail -20 /tmp/t5.log`
Expected: COMPILE ERROR — `SelectionCriteria` has no `input_tokens_pessimistic`.

- [x] **Step 3: Plumb it**

1. `SelectionCriteria` gains `pub input_tokens_pessimistic: Option<u32>,`.
2. The `SelectionCtx` literal at `:148` gains
   `input_tokens_pessimistic: criteria.input_tokens_pessimistic,`.
3. The gate list gains `Box::new(crate::gates::context_window::ContextWindowGate),` **after**
   `BudgetGate`. Order matters for which reason an all-gated message reports first; putting the
   window check last keeps every cheaper structural/health skip ahead of it, and a request that is
   over-window at a model that is also circuit-open should report the breaker, which is the
   recoverable one.
4. `engine/execute.rs:43` computes both:

```rust
        let input_tokens = estimate_input_tokens(&request.payload);
        let input_tokens_pessimistic = estimate_input_tokens_pessimistic(&request.payload);
```

and the `SelectionCriteria` literal gains `input_tokens_pessimistic: Some(input_tokens_pessimistic),`.

**This step needs a test of its own, and it is the one thing the Task 1–4 review could not
close.** `engine/util.rs`'s tests now compose payload → estimator → `SelectionCtx` → gate, so a
unit mismatch or a dropped estimator term reddens there. What none of them can see is THIS
assignment: passing `Some(input_tokens)` here — the cost figure — compiles, keeps every existing
test green, and silently admits exactly the over-window requests the slice exists to catch. Add a
test at the `engine::execute` boundary that builds a `Payload::Chat` whose TOOL SCHEMAS alone
exceed a small candidate's window (see
`util.rs::tool_schemas_alone_push_a_request_over_a_small_candidates_window` for a fixture that
measures ~11.5k tokens from 80 schemas and ~40 bytes of prose) and asserts the small model is
skipped while the large one is selected. Mutation to run before believing it: change this line to
`Some(input_tokens)` and watch it redden.

5. **Name the sixth gate everywhere the five are enumerated.** Registering it makes six prose
   sites wrong at once, and they are wrong in the file that owns the pipeline. The
   `ModelLockoutGate` omission in the same four `selection.rs` sites was fixed during the Task 1–4
   review, so these now read "capability, connection cooldown, circuit breaker, model lockout,
   budget" and need `context_window` added:

   - `crates/gateway/src/selection.rs` — the `ModelSelectionService` type doc, the `gates` field
     doc, `validate_direct`'s doc, `validate_chain_entry`'s doc (four sites);
   - `crates/gateway/src/engine/execute.rs` — the selection-empty branch's cause list;
   - `crates/gateway/src/engine/stream.rs` — the same list in its mirror branch.

   Also drop "not registered yet: Task 5 adds it" from `gates/context_window.rs`'s type doc, which
   becomes false the moment this task lands.

6. **Delete the `#[allow(dead_code)]` above `estimate_input_tokens_pessimistic` in
   `engine/util.rs`.** Task 3 added it because nothing in a non-test build called the function
   between Task 3 and this one, and the pre-commit gate is `clippy --workspace --all-targets
   -D warnings`. This step is what makes it unnecessary; leaving it would silence a real signal
   for the next person who breaks the plumbing. If clippy then reports the function as dead,
   step 4 above did not actually land.

- [x] **Step 4: Fix every other `SelectionCriteria` construction**

Run: `cargo build --workspace --all-targets > /tmp/t5b.log 2>&1; echo "exit=$?"`
Fix each missing-field error. `None` is correct in fixtures that are not about this gate.

- [x] **Step 5: Run to verify passing**

Run: `cargo test -p sensei-gateway > /tmp/t5c.log 2>&1; echo "exit=$?"; grep -E '^test result' /tmp/t5c.log | tail -3`
Expected: `exit=0`, 0 failed.

- [x] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/gateway/src/selection.rs crates/gateway/src/engine/execute.rs
git commit -m "feat(gateway): register ContextWindowGate and plumb the estimate

Registered AFTER BudgetGate so every cheaper structural and health skip is
reported ahead of it: a candidate that is both over-window and
circuit-open should surface the breaker, which is the recoverable one."
```

---

### Task 6: Delete the orchestrator's pre-check

**Files:**
- Modify: `crates/orchestrator/src/executor/agent.rs:271` (`min_win`), `:368-378` (the halt), and `AgentRun`'s `min_win` field
- Modify: `crates/orchestrator-core/src/error.rs:76` (`PromptOverBudget`)

- [x] **Step 0: Verify what the pre-check is actually shielding on a BUDGETED run**

Do this BEFORE deleting anything. The Task 1–4 review established by reading `dispatch.rs` that on
a budgeted run the SP-DATA-5 clamp refuses **before `Gateway::execute` is ever called**:

```text
window  = min_context_window(chain).saturating_sub(est)   // chain MINIMUM
ceiling = min(min_max_output_tokens(chain), window)
if ceiling < MIN_OUTPUT_TOKENS  =>  Refusal::BudgetExhausted { cause: BelowFloor }
```

For AC1's own chain `[big 128k, small 8k]` with a 20k prompt that is `8192 − 20000 = 0`, `0 < 256`,
and the run refuses with a message the clamp's own comment admits "names a raise that will not
help". So deleting the pre-check does not make AC1 work on a budgeted run — the gate never gets
asked.

Write a test at the `dispatch_metered` boundary pinning that this is the behaviour (budgeted +
over-chain-minimum ⇒ `BelowFloor`, unbudgeted ⇒ reaches the gateway and selects the big model), so
the boundary of the slice's benefit is a fact in the suite rather than a paragraph. Then record it
in the commit message and in Task 7's docs sweep. Do **not** try to fix the clamp here: moving its
window term to the selected candidate is the clamp spec's own §8 item and needs selection to have
already happened.

- [x] **Step 1: Write the failing test (AC9)**

In `crates/orchestrator/src/executor/tests.rs`:

```rust
    /// AC9 — a resumed agent turn replays from its memo across a drive where selection
    /// could have picked a different candidate.
    ///
    /// This is the property that makes SP-7a safe to ship without SP-7b:
    /// `agent_input_hash` covers `{chain, system, messages, tools}`, and this slice
    /// changes NO prompt bytes — it picks a different model WITHIN the same chain, and
    /// the chain string is what the hash carries. Truncation would change `system` and
    /// therefore the key, which is exactly why it is a separate slice.
    #[tokio::test]
    async fn an_agent_turn_replays_from_its_memo_though_selection_may_differ() {
        let (gateway, calls) = scripted_gateway(vec!["done".into()]).await;
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
            .with_registry(tool_agent_registry());
        let graph = Graph {
            nodes: vec![Node {
                id: NodeId("n1".into()),
                kind: NodeKind::Agent {
                    agent: AgentRef("a".into()),
                    input: serde_json::json!("hi"),
                    phase: None,
                },
                deps: vec![],
            }],
        };
        exec.start(run, &graph).await.expect("first drive");
        let after_first = calls.lock().unwrap_or_else(|e| e.into_inner()).len();

        let out = exec.start(run, &graph).await.expect("resumes");
        assert!(out.failed.is_none(), "no DeterminismViolation: {:?}", out.failed);
        assert_eq!(
            calls.lock().unwrap_or_else(|e| e.into_inner()).len(),
            after_first,
            "the completed turn replayed from its memo — no second provider call"
        );
    }
```

Check `scripted_gateway` and `tool_agent_registry`'s real names and signatures in `tests.rs` first.

- [x] **Step 2: Run — expect PASS, then prove it is not vacuous**

Run: `cargo test -p sensei-orchestrator an_agent_turn_replays_from_its_memo > /tmp/t6.log 2>&1; echo "exit=$?"`
Expected: `exit=0`. It passes on arrival because the property already holds — the point is to pin it
before deleting the pre-check, so a later change that *does* alter prompt bytes reddens here.

Prove it: in a throwaway worktree (`git worktree add /tmp/gw-ac9 HEAD`), fold anything
request-dependent into `support::agent_input_hash`'s hashed string, re-run, confirm it FAILS with
`DeterminismViolation`, then `git worktree remove --force /tmp/gw-ac9`. Quote the failure.

- [x] **Step 3: Delete the pre-check**

In `agent.rs`, remove:
- the `min_win` field from `AgentRun` and its initialiser at `:271`;
- the `let min_win = self.gateway.min_context_window(&chain).await;` line;
- the whole `if over_budget(...) { ... }` block at `:368-378`, including the `NodeFailed` append.

Remove the now-unused `over_budget` / `est_prompt_tokens` imports if nothing else uses them —
`cargo clippy -D warnings` will name them.

**Leave `over_budget` and `est_tokens` in `agent/prompt.rs` alone unless clippy reports them dead.**
*(Superseded: clippy CANNOT report either — both are `pub` in a `pub mod`, so `dead_code` never
fires. `over_budget` was deleted in Task 6 on judgement; `est_tokens` was deleted in the review
round for the same reason. See the Task 5-6 notes below.)*
If they become dead, delete them and say so; a function kept only because it might be wanted is the
thing this codebase argues against.

- [x] **Step 4: Delete `PromptOverBudget`**

Remove the variant from `crates/orchestrator-core/src/error.rs`. A variant no code can construct is
a claim the type makes that the code does not honour.

Run: `cargo build --workspace --all-targets > /tmp/t6b.log 2>&1; echo "exit=$?"`
Fix every reference the compiler names, including tests asserting it.

- [x] **Step 5: Run the whole suite**

Run: `cargo test --workspace > /tmp/t6c.log 2>&1; echo "exit=$?"; grep -E '^test result' /tmp/t6c.log | awk '{p+=$4;f+=$6;i+=$8} END {print "passed="p" failed="f" ignored="i}'`

Any orchestrator test that asserted the old halt will redden. **Those tests are now testing deleted
behaviour** — read each one and decide deliberately: if it asserted "an over-window prompt fails the
node", the behaviour genuinely moved to the gateway and the test's home moves with it. Do not simply
delete an assertion to get green; say in the commit which tests moved and which were removed.

- [x] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/agent.rs crates/orchestrator-core/src/error.rs crates/orchestrator/src/executor/tests.rs
git commit -m "refactor(orchestrator): the window check belongs to selection

Deletes the pre-dispatch min_context_window halt and PromptOverBudget.
The orchestrator was guessing ahead of the selector against the chain's
SMALLEST window and failing the node terminally; the gateway now asks the
question per candidate, where it has a correct answer, and an
over-everything request becomes an AllGated naming every candidate's own
window and the estimate that exceeded it. Still terminal (AllGated with no
timed gate does not pause), but diagnosed per candidate instead of against
the chain minimum.

PromptOverBudget is removed rather than left unconstructed: a variant no
code can produce is a claim the type makes and the code does not honour."
```

---

## What Tasks 5–6 shipped, and where they departed from the draft above

Both landed (`b6c2b73`, `3528d05`); workspace **1708 passed / 0 failed / 56 ignored**,
`clippy --workspace --all-targets -D warnings` and `fmt --check` clean.

**Task 5 also wired `engine::execute_stream`.** The draft named only `execute.rs`.
`execute_stream` builds its own `SelectionCriteria` in its own copy of the block, so
wiring one and not the other would have left every streaming caller ungated *and* made
`stream.rs`'s own selection-empty comment false the moment the sixth gate was named in
it. Streaming is also where an unfit candidate costs most — the provider's 400 lands
after the caller has committed. Pinned by
`execute_stream_gates_on_the_context_window_like_execute`.

**The Task-5 wiring test needed a fixture the draft's over-everything payload could not
provide.** A 600 KB prose request is over-window on BOTH estimates, so it cannot tell
them apart: the first version of the stream test passed with the cost estimate wired in.
Both wiring tests now use `schema_heavy_chat_payload()` — 80 tool schemas, 14 bytes of
prose — whose cost figure FITS the 8 k candidate and whose pessimistic figure does not,
and the fixture asserts both halves of that itself so no caller silently loses its
discriminating power.

**Task 6's AC9 test as drafted could not fail.** Drive-to-completion-then-`start`-again
is a *terminal* resume: it returns the folded outcome without re-entering the ReAct loop,
so `agent_input_hash` is never recomputed and the memo comparison never runs. Written
that way it stayed GREEN with `SystemTime::now()` folded into the hashed string. It is
now a MID-node crash (turn 0 journaled, turn 1 dies on an exhausted script), and the same
mutation produces `DeterminismViolation { node: "n1" }`.

**Three helpers died with the halt.** `executor::support::est_prompt_tokens` (clippy named
it) and `agent::prompt::over_budget` — which *was* the chain-minimum check, so keeping it
would leave a second window check available to be called beside the per-candidate one —
went with Task 6. `agent::prompt::est_tokens` shipped one commit longer, documented as
having no production caller, and **the review round deleted it too**: `pub` in a `pub mod`
hides it from `dead_code`, so clippy could never report what it had become, and the commit's
own argument against keeping `over_budget` ("a function kept because it might be wanted
again is exactly the kind of thing that gets called again by mistake") applies with more
force to the `chars/4` UNDER-count the window check's whole failure history came from. The
"prose baseline" justification did not survive either — `est_tokens_pessimistic` is
`chars/3` outright rather than a multiplier on it, so the two were already independent. Its
two tests went with it, the ordering one subsumed by an absolute assertion in
`the_pessimistic_estimate_is_chars_over_three_rounded_up`.

**The two reddened orchestrator tests were both updated in place, and one was removed:**

| Test | Disposition |
|---|---|
| `agent_node_halts_over_budget_before_any_gateway_call` | **Renamed** `an_over_window_agent_prompt_fails_the_node_with_the_gateways_diagnosis`. Same fixture, same two claims (fails the node; zero provider calls — selection refuses before an adapter is reached). New wording, plus assertions on the window, the estimate and the remedy the old message never carried. |
| `oversized_dependency_context_halts_over_budget_never_truncates` | **Kept, name and all.** Its invariant is "never silently truncated" and it is untouched: the model path's `## Context` is unbounded. Only who NOTICES moved, so the assertion now requires the halt to name the window rather than say "over budget", which read as a money problem. |
| `prompt::tests::over_budget_true_when_estimate_exceeds_window_and_false_otherwise` | **Removed with the function.** It asserted a chain-MINIMUM answer, which is the thing replaced rather than a property that moved. Its "tiny window → over" and "large window → not over" cases are `gates::context_window::over_window_skips_and_under_window_admits` (per candidate, so one request gets two answers). Its third case — "unknown window (`min_context_window` → `None`) → never a hard fail" — is **unreachable post-slice, not relocated**: the gate reads a resolved candidate's `ModelConfig.context_window`, a plain required `u32`, so there is no absent-window branch to have. *(Corrected by review: this row and the tombstone comment both said the case "is `no_estimate_admits`", which covers an absent ESTIMATE and is a different question.)* The only surviving consumer of an OPTIONAL window is the SP-DATA-5 clamp's chain fold in `executor/dispatch.rs` (`(a, b) => a.or(b)`), untouched here. |

**Step 0's boundary is now a test**, `a_budgeted_run_is_refused_by_the_clamp_before_the_window_gate_is_asked`
(**renamed in the serving-window follow-on** to
`an_over_every_window_prompt_is_refused_by_the_gate_budgeted_or_not`, where both arms land on the
GATE — see that slice's spec for what moved):
unbudgeted ⇒ the gateway gates and the node FAILS; budgeted ⇒ the SP-DATA-5 clamp refuses
first and the run PAUSES. Proven non-vacuous by dropping the clamp's window term
(`(Some(a), Some(b)) => Some(a)`), which makes the budgeted arm fail instead of pause.

**Corrected by review: that pause is an outcome-class FLIP, not a no-op.** The deleted halt
ran before `dispatch_metered`, so a budgeted over-window run used to end in a terminal
`NodeFailed` naming the window; now it ends in a `RunPaused` that named the cap — measured
identically at 1e6, 1e8 and `u64::MAX`, because the window term never reads the cap. The
test had been pinning the misleading message (`assert!(!reason.contains("context window"))`).
Fixed by making the clamp's `BelowFloor` refusal carry the binding window and say so:
`"context window: … the budget is not the binding term … no cap raise clears this"`, with no
`--budget-tokens` remedy. The pause CLASS is kept deliberately (see the spec's §8). Pinned by
mutating `binding_window` to `None`, which restores the budget wording and reddens.

---

### Task 7: Docs and the gate

- [x] **Step 1: Verification, real exit codes**

```bash
cargo test --workspace > /tmp/g1.log 2>&1; echo "exit=$?"
cargo clippy --workspace --all-targets -- -D warnings > /tmp/g2.log 2>&1; echo "exit=$?"
cargo fmt --all --check; echo "exit=$?"
```
All three `exit=0`. Report exact counts. No database work is needed — do not start a container.

- [x] **Step 2: Doc-link baseline**

```bash
cargo clean --doc && cargo doc --workspace --no-deps --document-private-items 2>&1 | grep -c 'unresolved link'
```
Baseline **16**. Higher means this slice added broken links.

- [x] **Step 3: The documentation sweep**

- **`crates/orchestrator/src/agent/prompt.rs:212-221`** — the doc comment on the bounded renderer
  says the model path "HALTS rather than truncating" and that an over-window call "can be retried
  against a bigger chain". The first half is now false and the second is now TRUE for the first
  time. Rewrite both, and say which slice made each true.
- **`docs/superpowers/orchestrator-overview.md:229`** — the SP-7 line reads "active summarize/select
  + retrieval-ranked/semantic activation (today: over-budget halts loud, never truncates)". Record
  SP-7a as shipped, and that SP-7's original bundle is now three slices (a: selection, b: context
  budgeting, c: semantic activation) with the reason for the split.
- **`docs/features/orchestrator/agents-skills-tools.md`** — check whether it describes the
  over-window behaviour; if it does, it is now wrong. *(It did, in three places: `:62`, `:247`
  ("per-turn window budgeting (`over_budget`)" listed as an implemented Slice 2 component) and
  `:260`. All three rewritten in the review round.)*
- **`docs/features/orchestrator/shared-context.md:31-32`** — "over-budget currently halts loud via
  `PromptOverBudget`, never truncates". Named explicitly because the generic grep bullet below is
  the only thing that would have caught it, and a partial Task 7 would have shipped it. Rewritten.
- **`crates/orchestrator/src/executor/dispatch.rs`** — THREE comments describe `over_budget` /
  `est_prompt_tokens` as live code (`:155`, `:174-179`, `:491`). The `:174` paragraph is a whole
  design rationale ("deliberately NOT shared … those want the opposite bias") resting on a function
  that no longer exists, and its argument is wrong twice over: the successor estimator wants the
  SAME bias and counts `tool_calls` too. The real reason the two are separate is the crate
  dependency direction. All three rewritten in the review round.
- **`crates/gateway/src/engine/mod.rs`** — `min_context_window`'s doc ("used by the agent runtime
  … selection is untouched") is false in both halves after Tasks 5 and 6. Rewritten to name the
  SP-DATA-5 clamp as its one production caller.
- Grep for other surfaces: `rg -n 'PromptOverBudget|min_context_window|over_budget' --no-ignore -g '!target' crates/ docs/`

- [x] **Step 4: Checkpoint**

Rewrite `docs/CHECKPOINT.md` (**under 40 lines**, one current entry): what shipped, the measured
numbers, the next command. Note the sensei daemon is not running, so it is the only durable record.

- [x] **Step 5: Commit**

```bash
cargo fmt --all
git add docs/ crates/orchestrator/src/agent/prompt.rs
git commit -m "docs: window-aware selection, and the two claims it made false"
```

---

## What Task 7 shipped, and where it departed from the draft above

**The gate is green.** `cargo test --workspace` **1714 passed / 0 failed / 56 ignored, exit 0**
across 35 suites; `clippy --workspace --all-targets -- -D warnings` exit 0; `fmt --all --check`
exit 0; `cargo clean --doc && cargo doc --workspace --no-deps --document-private-items` reports
**16** unresolved links, exactly the baseline, so this slice added none. No container was started
and `$DATABASE_URL` was never read.

**Two of the four named sweep targets were already correct, and saying so is the point.** The task
brief was written against the PRE-review tree, so it described `agent/prompt.rs:212-221` as still
claiming the model path "HALTS rather than truncating" and that an over-window call "can be retried
against a bigger chain", and `orchestrator-overview.md:229` as still reading "today: over-budget
halts loud, never truncates". The review round had already rewritten both. `dispatch.rs`,
`engine/mod.rs`, `shared-context.md` and `agents-skills-tools.md`'s three named sites likewise.
They were re-read rather than re-touched — re-asserting a correction is how a doc acquires two
slightly different versions of one claim.

**Two stale sites the earlier rounds missed, and neither was on the brief's list:**

| Site | What was wrong |
|---|---|
| `docs/features/orchestrator/README.md:22` | The feature table's Agents row still advertised "prompt assembly + per-turn window budget" as a shipped capability. That IS the deleted `over_budget` halt, named as a feature in the index a reader hits first. |
| `docs/features/orchestrator/agents-skills-tools.md` Gherkin | A `Feature: Agent runtime` scenario read "Prompt is budgeted to the smallest model in the chain / Given a chain whose smallest model has a 32k context window / Then prompt assembly fits within 32k". That is the *precise* behaviour this slice deleted, stated as an executable-looking acceptance criterion. Replaced by two scenarios that describe what the gate actually does, with the old text retained as a `#` comment saying which slice killed it and why. |

The prose above the scenarios ("The agent runtime assembles a budgeted prompt") went with it.

**The overview line gained the split argument the draft only gestured at.** The brief asked for "the
reason for the split: truncation changes `agent_input_hash` and selection does not". Verified before
writing rather than paraphrased: `executor::support::agent_input_hash` hashes
`format!("{chain}|{system}|{messages}|{tools}")` — the chain STRING, never the candidate selection
resolves out of it. So 7a is safe to ship alone because a turn memoized before the gate existed
replays byte-identically after it; 7b rewrites `system`/`messages` and moves the key, so it owes a
resume story 7a does not. Three missing slice-table rows (SP-6 s3, SP-6 s4, SP-7a) were added at the
same time — the table stopped at SP-6 s2 while the prose two paragraphs below it claimed all four
SP-6 slices merged.

**One Rust comment was tightened, so this is not a docs-only commit.** `prompt.rs`'s
`est_tokens_pessimistic` doc argued the opposite-bias case with "a window check asks 'will this
prompt fit', where an over-count halts a turn that would in fact have fitted". Post-SP-7a an
over-count SKIPS a candidate; it fails the node only when it skips the last one. The paragraph
that follows already said the window half had moved to the gateway, so the sentence was not
load-bearing — but it stated the wrong consequence of this slice's own change, in the file the
change emptied out, which is exactly the class of comment the review rounds hunt.

**Not changed, deliberately:** the historical spec and plan documents under `docs/superpowers/`
(`sp1-slice2-agent-runtime-design.md`, `sp2-activation-policy-design.md`, the SP-DATA-5 plan and
the rest) still describe `PromptOverBudget`/`over_budget` as live. They are dated records of what a
past slice decided, not statements about today's tree, and rewriting them would destroy the audit
trail this slice's own review depended on. The live surfaces — `docs/features/**`, the overview,
and every code comment — are the ones held to current truth.

---

## Self-review

**Spec coverage** — every AC maps to a task:

| AC | Task | AC | Task |
|---|---|---|---|
| AC1 heterogeneous chain serves | 4 (unit), 5 (composed) | AC6 counts schemas + tool calls | 3 (composed, in `util.rs`) |
| AC2 skip records both numbers | 2, 4 | AC7 pessimistic ≥ cost, unit pinned absolutely | 3 |
| AC3 `AllGated` names the numbers | 1 (`Display`), 5 (engine boundary) | AC8 `PromptOverBudget` gone | 6 |
| AC4 in-window byte-identical | 5 | AC9 memo replays | 6 |
| AC5 no estimate admits (vs a ZERO window) | 4 | AC10 Stt unaffected / Embed gated | 3 |

Three rows moved during the Task 1–4 review. **AC6** was booked to Task 3 on the strength of a
test that proved the estimator counts schemas, while a separate Task 4 test proved the gate skips
on a number — nothing joined them, which is exactly how the `tool_calls` gap survived; the
composed form now lives in `util.rs`. **AC10** was booked to a test that could not fail for it
(Stt's `pess >= cost` is satisfied by any value when `cost == 0`) and asserted "unaffected" of a
payload kind that is in fact gated; both halves are now pinned, one as an absolute zero and one as
a deliberate skip. **AC3** lost its pause claim and gained a `Display` obligation in the kernel.

**Gap found and closed:** AC3 says the all-gated case must be recorded as `AllGated`, but Task 5's
selection-level test only asserts that selection admits nothing and records the reason — it never
reaches `all_gated_error`, so nothing pins what the CALLER receives. Add to Task 5:

```rust
    /// AC3, the half the selection-level test cannot see: an all-gated selection must
    /// reach the caller as an `AllGated` naming the cause and the remedy, not as a bare
    /// `NoCandidates`. Asserted at the engine boundary, because `all_gated_error` is
    /// what makes the distinction and it lives there.
    ///
    /// Both the typed variant AND the rendered string, deliberately. The typed check is
    /// the contract; the rendered one is what an operator actually gets, because
    /// `classify_gateway_error` builds its `NodeFailed` reason from `err.to_string()`
    /// and nothing downstream destructures the error.
    #[tokio::test]
    async fn a_request_over_every_window_is_all_gated_with_the_numbers() {
        let cfg = two_model_chain_windows(128_000, 8_192);
        let gw = gateway_with(cfg);
        let err = gw
            .execute(&chat_request_of_length(200_000))
            .await
            .expect_err("nothing can hold it");
        let GatewayError::AllGated { skipped, human_action, .. } = &err else {
            panic!(
                "an all-gated selection must not degrade to another error — \
                 NoCandidates is the structural 'nothing is configured' case and tells \
                 an operator nothing about what to do: {err:?}"
            );
        };
        assert_eq!(*human_action, Some(HumanAction::UseLargerContextWindow));
        assert!(
            skipped.iter().any(|s| s.contains("8192-token context window")),
            "the diagnostics must name a candidate's own window: {skipped:?}"
        );
        assert!(
            format!("{err}").contains("8192-token context window"),
            "and it must survive Display, which is the only channel that reaches a \
             NodeFailed: {err}"
        );
    }
```

Write `gateway_with` and `chat_request_of_length` from the existing engine-test fixtures.

**Two corrections to the draft this replaces**, both from the Task 1–4 review:

- it asserted `format!("{err}").contains("context window")`, which **could not pass**:
  `AllGated`'s `#[error]` rendered only "all candidates gated, human action required" and dropped
  `skipped` entirely. That is now fixed in `kernel::types::error`, so the assertion is
  reachable — and it is written against a labelled substring rather than a bare number so a
  swapped-placeholder message cannot satisfy it;
- its first assertion (`!matches!(err, NoCandidates)`) was far weaker than the AC3 it claimed to
  close: `AllGated{None}` satisfies it while still hard-failing the node. The destructuring form
  above says what is actually required. The pause-vs-fail question it seemed to be about is
  **not** AC3 any more — see the spec's §3 row and §8.

**Placeholder scan:** none. Every code step carries real code. Three steps say "check the real
fixture names before writing" — that is an instruction to verify against the codebase, not a
deferred decision.

**Type consistency:** `HumanAction::UseLargerContextWindow`, `SkipReason::OverContextWindow
{ estimated: u32, window: u32 }`, `estimate_input_tokens_pessimistic(&Payload) -> u32`,
`SelectionCtx.input_tokens_pessimistic: Option<u32>`, `SelectionCriteria.input_tokens_pessimistic:
Option<u32>`, `ContextWindowGate`. Each defined once and used consistently.
