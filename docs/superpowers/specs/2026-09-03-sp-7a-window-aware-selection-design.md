---
title: SP-7a — window-aware selection (stop failing prompts the primary model could serve)
doctype: design-spec
module: gateway
slice: SP-7a
status: approved
date: 2026-09-03
---

# SP-7a — window-aware selection

## 1. Summary

An agent prompt that exceeds the model's context window is a **terminal `NodeFailed`** today
(`OrchestratorError::PromptOverBudget`, `executor/agent.rs:369`). The check runs in the
orchestrator, before dispatch, against `Gateway::min_context_window(chain)` — **the smallest window
in the chain**.

Nothing anywhere routes by window. `ModelSelectionService::select_all` admits candidates through a
set of `AdmissionGate`s — capability, budget, cooldown, circuit-breaker, lockout — and none of them
looks at `context_window`.

So on a chain of `[gpt-4o 128k, small-fallback 8k]`, a 20k-token prompt fails terminally, even
though the primary would have served it. The halt's own doc comment justifies itself by saying such
a call "can be retried against a bigger chain" (`agent/prompt.rs:219`) — **that retry does not
exist**, automatically or otherwise.

This slice adds a `ContextWindowGate` beside the other five, and deletes the orchestrator's
pre-check.

## 2. Goals / Non-goals

**Goals**
- A `ContextWindowGate` in `crates/gateway/src/gates/`, mirroring `BudgetGate`.
- A `SkipReason::OverContextWindow { estimated, window }`.
- Remove the orchestrator's `min_context_window` pre-check and its `PromptOverBudget` halt.
- A prompt no candidate can hold becomes a durable **`AllGated` pause**, not a terminal failure.
- Additive for every in-window request: selection is byte-identical.

**Non-goals**
- **Truncating or summarising an over-window prompt.** That is SP-7b, and it is separate for a
  concrete reason, not tidiness — see §5.
- **Semantic / retrieval-ranked activation.** The overview bundles this into "SP-7"; it needs
  embedding infrastructure and changes *which* skills activate rather than what happens when they
  do not fit. Its own slice (SP-7c).
- Changing the cost `BudgetGate`, fallback triggers, or the selection algorithm's shape.

## 3. The decisions, and why

| Decision | Choice | Why |
|---|---|---|
| Mechanism | **A sixth `AdmissionGate`** | The pattern exists and fits exactly: `SelectionCtx` already carries `input_tokens`, `GateVerdict::Skip(SkipReason)` already carries a typed reason, and `all_gated_error` already turns an all-skipped selection into a durable pause. A bespoke filter would duplicate all three. |
| Where the check lives | **Gateway, not orchestrator** | Selection is the gateway's job and the orchestrator was guessing ahead of it. Every gateway caller benefits, and the duplicate `min_context_window` read disappears rather than being kept in sync. |
| Over-everything outcome | **`AllGated` durable pause** | Strictly better than today's terminal `NodeFailed`: the run survives, and an operator can widen the chain and wake it. This falls out of the existing `all_gated_error` path — it is not new machinery. |
| The estimate | **A pessimistic one, including tool schemas** | See §4. The existing `estimate_input_tokens` is `chars/4` over messages + system ONLY, so it silently omits tool schemas — and for a window gate an under-count admits a model the prompt does not fit, which is the failure the gate exists to prevent. |

## 4. The estimate, and the bias that matters

`engine/util.rs::estimate_input_tokens` computes `(message_chars + system_chars) / 4`. Two problems
for this use:

1. **It omits tool schemas entirely.** An agent's activated schemas routinely outweigh its prompt —
   the SP-DATA-5 clamp's own comment records exactly that.
2. **`chars/4` is the prose figure.** JSON tokenizes nearer 3 chars/token, so it under-counts most
   on the very content it omits.

For the cost `BudgetGate` an under-count is merely optimistic pricing. For a window gate it is the
defect: it admits a candidate the prompt does not fit, and the provider answers 400.

**So the window gate uses its own pessimistic figure**, and `estimate_input_tokens` is left alone.
Not because sharing would be wrong in principle, but because widening the shared estimator silently
changes every cost estimate the `BudgetGate` makes, which is a different slice's decision to take.
The two want opposite biases for the same reason `est_tokens` and `est_tokens_pessimistic` are two
functions in the orchestrator: window-fit asks "what is the worst this could be", cost asks "what
will this probably run to".

The accepted cost, stated plainly: a pessimistic estimate can **skip a model the prompt would
actually have fitted**, sending the request to a larger, likely costlier candidate. That is the
cheaper error — the alternative is a provider 400 — and it is visible, because the skip is recorded
with both numbers.

## 5. Why truncation is NOT in this slice

`support::agent_input_hash` covers `{chain, system, messages, tools}`, and `system` **contains the
rendered `## Context` section**. So truncating context changes the determinism key.

That makes truncation categorically different from this slice, and from the SP-DATA-5 clamp — the
clamp was safe precisely because `max_tokens` is *not* hashed. If a drive truncates because a small
model was selected and a later drive does not because a large one was, the memo hash mismatches and
the run dies with `DeterminismViolation` — a hard halt strictly worse than the failure being fixed.

**This slice changes no prompt bytes.** It picks a different model *within* the same chain, and the
chain string is what the hash carries. A resume re-selects and replays from its memo exactly as
today.

SP-7b owes an argument that what gets cut is a function of journaled state alone, never of which
candidate this drive happened to select. It is a separate spec because that argument is the hard
part, and it should not ride along unexamined inside a selection change.

## 6. Architecture

### 6.1 The gate

`crates/gateway/src/gates/context_window.rs`, mirroring `budget.rs`.

**`input_tokens_pessimistic` is a NEW field on `SelectionCtx`**, added by this slice —
`SelectionCtx` carries `input_tokens: Option<u32>` today and nothing else about size. A second
field rather than a replacement, because §4's whole argument is that the cost gate and the window
gate want opposite biases over the same payload; collapsing them to one number is the thing that
argument rules out. Both are computed once, at the same place `estimate_input_tokens` is called now
(`engine/execute.rs:43`).

```rust
impl AdmissionGate for ContextWindowGate {
    fn name(&self) -> &'static str { "context_window" }

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

`None` admits, matching how `BudgetGate` treats a model with no pricing: an absent estimate is not
evidence of a problem, and a gate that skipped on missing data would refuse every request that did
not carry one.

### 6.2 What the orchestrator loses

`executor/agent.rs`'s `over_budget` call, its `PromptOverBudget` append, and the `min_win` field
threaded through `AgentRun` all go. `min_context_window` itself stays — the SP-DATA-5 clamp uses it,
and §8 records why it is still the right bound there for now.

`OrchestratorError::PromptOverBudget` becomes unconstructed. It is **removed**, not left dangling: a
variant no code can produce is a claim the type makes and the code does not honour, and the whole
point of this slice is that the orchestrator no longer owns this decision.

### 6.3 What an operator sees

A prompt no candidate can hold produces `AllGated`, whose reason enumerates the per-candidate skips
— so the message names each model's window and the estimate that exceeded it, rather than today's
single `min_win` figure that may belong to a model the request never wanted.

## 7. Acceptance criteria

1. A chain `[big 128k, small 8k]` serves a ~20k-token prompt — it selects `big` and succeeds, where
   today it fails terminally.
2. The skip is recorded against `small` with both the estimate and that model's window.
3. A prompt exceeding EVERY candidate's window yields a durable **`AllGated`** pause, not a
   `NodeFailed`, and its message names at least one window and the estimate.
4. An in-window request selects **byte-identically** to today — same candidate, same order.
5. A request with no `input_tokens` estimate admits every candidate (the gate is not a filter on
   missing data).
6. The gate counts **tool schemas**; a request whose schemas alone push it over a candidate's window
   is skipped for that candidate. Proven by removing the tools term and watching this fail.
7. The pessimistic estimate is **≥** the existing `estimate_input_tokens` for the same payload.
8. `OrchestratorError::PromptOverBudget` no longer exists, and no test asserts it.
9. `agent_input_hash` is unchanged for a given prompt — a resumed agent turn replays from its memo
   across a drive where a *different* candidate was selected.
10. Embed/Stt payloads are unaffected: the gate applies to what the estimator can measure and admits
    otherwise.

## 8. Deferred

- **SP-7b — context budgeting** for the case where no candidate can hold the prompt, with the
  determinism argument §5 describes.
- **SP-7c — semantic / retrieval-ranked activation.**
- **Bounding the SP-DATA-5 clamp by the SELECTED model's window** rather than the chain minimum.
  That is the clamp spec's own §8 item, and this slice makes it reachable for the first time: once
  selection is window-aware, a post-selection bound is available. Not taken here, because the clamp
  runs before selection and moving it downstream is its own change.
- Widening `estimate_input_tokens` itself to count tool schemas, which would make the cost
  `BudgetGate` more conservative too — a real improvement, and a different slice's call.
