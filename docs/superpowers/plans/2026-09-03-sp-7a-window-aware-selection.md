# SP-7a Window-Aware Selection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop failing prompts that a model in the chain could serve, by making model selection aware of each candidate's context window.

**Architecture:** A sixth `AdmissionGate` in `crates/gateway/src/gates/`, mirroring `BudgetGate`. `SelectionCtx` gains a second, deliberately pessimistic token estimate that counts tool schemas; the gate skips any candidate whose `context_window` cannot hold it. When every candidate is skipped, the existing `all_gated_error` path turns that into a durable `AllGated` pause. The orchestrator's pre-dispatch `min_context_window` check and its terminal `PromptOverBudget` failure are deleted.

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
  into a durable pause when no `HardFailure` contributed. **Confirmed.**
- The orchestrator halt is `executor/agent.rs:368-378`: `over_budget(ar.min_win, …)` →
  `OrchestratorError::PromptOverBudget` → `NodeFailed` → `ToolOutcome::Failed`. `min_win` is set at
  `agent.rs:271` from `gateway.min_context_window(&chain)`. **Confirmed.**

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

- [ ] **Step 1: Add the variant**

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

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p sensei-kernel > /tmp/t1.log 2>&1; echo "exit=$?"`
Expected: `exit=0`. If any `match` over `HumanAction` is exhaustive elsewhere, the workspace build in Step 3 will name it.

- [ ] **Step 3: Find every exhaustive match**

Run: `cargo build --workspace > /tmp/t1b.log 2>&1; echo "exit=$?"; grep -A6 'non-exhaustive\|E0004' /tmp/t1b.log | head -20`

Any site that renders `HumanAction` for an operator needs an arm. Add one that says: use a model
with a larger context window, or send less. Do **not** collapse it into the `RaiseBudget` arm.

- [ ] **Step 4: Commit**

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

- [ ] **Step 1: Write the failing test**

Add to `skip_reason.rs`'s `mod tests`:

```rust
    /// An over-window skip is TERMINAL and points at the window, not at money.
    ///
    /// `Timed` would be wrong — no deadline passes that makes a model's window bigger —
    /// and `Structural` would be wrong too, because the candidate is perfectly well
    /// configured; it is this REQUEST that does not fit it. Terminal-with-a-remedy is the
    /// only classification that produces an actionable `AllGated` pause.
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

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p sensei-gateway over_context_window_is_terminal > /tmp/t2.log 2>&1; echo "exit=$?"; tail -20 /tmp/t2.log`
Expected: a COMPILE ERROR — `no variant named OverContextWindow`.

- [ ] **Step 3: Add the variant**

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

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p sensei-gateway skip_reason > /tmp/t2b.log 2>&1; echo "exit=$?"; grep -E '^test result' /tmp/t2b.log`
Expected: `exit=0`, all pass. The pre-existing `gate_status_classifies_each_reason` must still pass **unmodified** — if it reddens, you changed an existing classification.

- [ ] **Step 5: Commit**

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

- [ ] **Step 1: Write the failing tests**

In `util.rs`'s test module:

```rust
    /// The window estimate must be >= the cost estimate for the same payload, and
    /// strictly greater once tool schemas are present.
    ///
    /// `estimate_input_tokens` counts messages + system at `chars/4`. It omits tool
    /// schemas entirely — and an agent's activated schemas routinely outweigh its prompt.
    /// For COST that is optimistic pricing; for a WINDOW it admits a candidate the
    /// request does not fit, which is the failure the gate exists to prevent. So this one
    /// counts the schemas and uses the JSON-ish `chars/3` rather than the prose `chars/4`.
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

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sensei-gateway the_pessimistic_estimate_counts_tools > /tmp/t3.log 2>&1; echo "exit=$?"; tail -20 /tmp/t3.log`
Expected: COMPILE ERROR — `cannot find function estimate_input_tokens_pessimistic`.

- [ ] **Step 3: Implement**

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
/// 2. **`chars / 3`, not `chars / 4`.** The `/4` figure is the rough one for English
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

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p sensei-gateway estimate_input_tokens > /tmp/t3b.log 2>&1; echo "exit=$?"; grep -E '^test result' /tmp/t3b.log`
Expected: `exit=0`. The existing `estimate_input_tokens` tests must pass **unmodified**.

- [ ] **Step 5: Commit**

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

- [ ] **Step 1: Write the failing test**

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

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sensei-gateway context_window > /tmp/t4.log 2>&1; echo "exit=$?"; tail -20 /tmp/t4.log`
Expected: COMPILE ERROR — the module does not exist.

- [ ] **Step 3: Add the field to `SelectionCtx`**

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

- [ ] **Step 4: Write the gate**

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

- [ ] **Step 5: Run — expect every OTHER `SelectionCtx` construction to break**

Run: `cargo build --workspace --all-targets > /tmp/t4b.log 2>&1; echo "exit=$?"; grep -c 'missing field' /tmp/t4b.log`

Adding a field breaks every struct literal. Fix each by threading `input_tokens_pessimistic`
through; in test fixtures `None` is correct unless the test is about this gate. Then:

Run: `cargo test -p sensei-gateway context_window > /tmp/t4c.log 2>&1; echo "exit=$?"; grep -E '^test result' /tmp/t4c.log`
Expected: `exit=0`, 2 passed.

- [ ] **Step 6: Commit**

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

- [ ] **Step 1: Write the failing test (AC1, AC3, AC4)**

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

    /// AC3 — over EVERY window is an all-gated selection, which the caller turns into a
    /// durable pause rather than a hard failure.
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

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sensei-gateway -- a_chain_serves_a_prompt a_prompt_over_every_window an_in_window_request > /tmp/t5.log 2>&1; echo "exit=$?"; tail -20 /tmp/t5.log`
Expected: COMPILE ERROR — `SelectionCriteria` has no `input_tokens_pessimistic`.

- [ ] **Step 3: Plumb it**

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

5. **Delete the `#[allow(dead_code)]` above `estimate_input_tokens_pessimistic` in
   `engine/util.rs`.** Task 3 added it because nothing in a non-test build called the function
   between Task 3 and this one, and the pre-commit gate is `clippy --workspace --all-targets
   -D warnings`. This step is what makes it unnecessary; leaving it would silence a real signal
   for the next person who breaks the plumbing. If clippy then reports the function as dead,
   step 4 above did not actually land.

- [ ] **Step 4: Fix every other `SelectionCriteria` construction**

Run: `cargo build --workspace --all-targets > /tmp/t5b.log 2>&1; echo "exit=$?"`
Fix each missing-field error. `None` is correct in fixtures that are not about this gate.

- [ ] **Step 5: Run to verify passing**

Run: `cargo test -p sensei-gateway > /tmp/t5c.log 2>&1; echo "exit=$?"; grep -E '^test result' /tmp/t5c.log | tail -3`
Expected: `exit=0`, 0 failed.

- [ ] **Step 6: Commit**

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

- [ ] **Step 1: Write the failing test (AC9)**

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

- [ ] **Step 2: Run — expect PASS, then prove it is not vacuous**

Run: `cargo test -p sensei-orchestrator an_agent_turn_replays_from_its_memo > /tmp/t6.log 2>&1; echo "exit=$?"`
Expected: `exit=0`. It passes on arrival because the property already holds — the point is to pin it
before deleting the pre-check, so a later change that *does* alter prompt bytes reddens here.

Prove it: in a throwaway worktree (`git worktree add /tmp/gw-ac9 HEAD`), fold anything
request-dependent into `support::agent_input_hash`'s hashed string, re-run, confirm it FAILS with
`DeterminismViolation`, then `git worktree remove --force /tmp/gw-ac9`. Quote the failure.

- [ ] **Step 3: Delete the pre-check**

In `agent.rs`, remove:
- the `min_win` field from `AgentRun` and its initialiser at `:271`;
- the `let min_win = self.gateway.min_context_window(&chain).await;` line;
- the whole `if over_budget(...) { ... }` block at `:368-378`, including the `NodeFailed` append.

Remove the now-unused `over_budget` / `est_prompt_tokens` imports if nothing else uses them —
`cargo clippy -D warnings` will name them.

**Leave `over_budget` and `est_tokens` in `agent/prompt.rs` alone unless clippy reports them dead.**
If they become dead, delete them and say so; a function kept only because it might be wanted is the
thing this codebase argues against.

- [ ] **Step 4: Delete `PromptOverBudget`**

Remove the variant from `crates/orchestrator-core/src/error.rs`. A variant no code can construct is
a claim the type makes that the code does not honour.

Run: `cargo build --workspace --all-targets > /tmp/t6b.log 2>&1; echo "exit=$?"`
Fix every reference the compiler names, including tests asserting it.

- [ ] **Step 5: Run the whole suite**

Run: `cargo test --workspace > /tmp/t6c.log 2>&1; echo "exit=$?"; grep -E '^test result' /tmp/t6c.log | awk '{p+=$4;f+=$6;i+=$8} END {print "passed="p" failed="f" ignored="i}'`

Any orchestrator test that asserted the old halt will redden. **Those tests are now testing deleted
behaviour** — read each one and decide deliberately: if it asserted "an over-window prompt fails the
node", the behaviour genuinely moved to the gateway and the test's home moves with it. Do not simply
delete an assertion to get green; say in the commit which tests moved and which were removed.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/agent.rs crates/orchestrator-core/src/error.rs crates/orchestrator/src/executor/tests.rs
git commit -m "refactor(orchestrator): the window check belongs to selection

Deletes the pre-dispatch min_context_window halt and PromptOverBudget.
The orchestrator was guessing ahead of the selector against the chain's
SMALLEST window and failing the node terminally; the gateway now asks the
question per candidate, where it has a correct answer, and an
over-everything request becomes a durable AllGated pause instead.

PromptOverBudget is removed rather than left unconstructed: a variant no
code can produce is a claim the type makes and the code does not honour."
```

---

### Task 7: Docs and the gate

- [ ] **Step 1: Verification, real exit codes**

```bash
cargo test --workspace > /tmp/g1.log 2>&1; echo "exit=$?"
cargo clippy --workspace --all-targets -- -D warnings > /tmp/g2.log 2>&1; echo "exit=$?"
cargo fmt --all --check; echo "exit=$?"
```
All three `exit=0`. Report exact counts. No database work is needed — do not start a container.

- [ ] **Step 2: Doc-link baseline**

```bash
cargo clean --doc && cargo doc --workspace --no-deps --document-private-items 2>&1 | grep -c 'unresolved link'
```
Baseline **16**. Higher means this slice added broken links.

- [ ] **Step 3: The documentation sweep**

- **`crates/orchestrator/src/agent/prompt.rs:212-221`** — the doc comment on the bounded renderer
  says the model path "HALTS rather than truncating" and that an over-window call "can be retried
  against a bigger chain". The first half is now false and the second is now TRUE for the first
  time. Rewrite both, and say which slice made each true.
- **`docs/superpowers/orchestrator-overview.md:229`** — the SP-7 line reads "active summarize/select
  + retrieval-ranked/semantic activation (today: over-budget halts loud, never truncates)". Record
  SP-7a as shipped, and that SP-7's original bundle is now three slices (a: selection, b: context
  budgeting, c: semantic activation) with the reason for the split.
- **`docs/features/orchestrator/agents-skills-tools.md`** — check whether it describes the
  over-window behaviour; if it does, it is now wrong.
- Grep for other surfaces: `rg -n 'PromptOverBudget|min_context_window|over_budget' --no-ignore -g '!target' crates/ docs/`

- [ ] **Step 4: Checkpoint**

Rewrite `docs/CHECKPOINT.md` (**under 40 lines**, one current entry): what shipped, the measured
numbers, the next command. Note the sensei daemon is not running, so it is the only durable record.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add docs/ crates/orchestrator/src/agent/prompt.rs
git commit -m "docs: window-aware selection, and the two claims it made false"
```

---

## Self-review

**Spec coverage** — every AC maps to a task:

| AC | Task | AC | Task |
|---|---|---|---|
| AC1 heterogeneous chain serves | 5 | AC6 counts tool schemas | 3 |
| AC2 skip records both numbers | 2, 4 | AC7 pessimistic ≥ cost estimate | 3 |
| AC3 all-gated durable pause | 5 | AC8 `PromptOverBudget` gone | 6 |
| AC4 in-window byte-identical | 5 | AC9 memo replays | 6 |
| AC5 no estimate admits | 4 | AC10 Embed/Stt unaffected | 3 |

**Gap found and closed:** AC3 says the all-gated case must be a **durable pause**, but Task 5's test
only asserts that selection admits nothing and records the reason — it never reaches
`all_gated_error`, so nothing pins that an over-window-everything request is *recoverable* rather
than a hard failure. That is the single most valuable behavioural claim in the slice. Add to Task 5:

```rust
    /// AC3, the half the selection-level test cannot see: an all-gated selection must
    /// reach the caller as a RECOVERABLE error, not a bare `NoCandidates`.
    ///
    /// This is the whole improvement over the old terminal `NodeFailed` — the run
    /// survives and an operator can widen the chain and wake it. Asserted at the engine
    /// boundary, because `all_gated_error` is what makes the distinction and it lives
    /// there.
    #[tokio::test]
    async fn a_request_over_every_window_is_recoverable_not_a_hard_failure() {
        let cfg = two_model_chain_windows(128_000, 8_192);
        let gw = gateway_with(cfg);
        let err = gw
            .execute(&chat_request_of_length(200_000))
            .await
            .expect_err("nothing can hold it");
        assert!(
            !matches!(err, GatewayError::NoCandidates { .. }),
            "an all-gated selection must not degrade to a bare NoCandidates — that is \
             the structural 'nothing is configured' error, and it tells an operator \
             nothing about what to do: {err:?}"
        );
        assert!(
            format!("{err}").contains("context window"),
            "and the error must name the cause: {err}"
        );
    }
```

Write `gateway_with` and `chat_request_of_length` from the existing engine-test fixtures.

**Placeholder scan:** none. Every code step carries real code. Three steps say "check the real
fixture names before writing" — that is an instruction to verify against the codebase, not a
deferred decision.

**Type consistency:** `HumanAction::UseLargerContextWindow`, `SkipReason::OverContextWindow
{ estimated: u32, window: u32 }`, `estimate_input_tokens_pessimistic(&Payload) -> u32`,
`SelectionCtx.input_tokens_pessimistic: Option<u32>`, `SelectionCriteria.input_tokens_pessimistic:
Option<u32>`, `ContextWindowGate`. Each defined once and used consistently.
