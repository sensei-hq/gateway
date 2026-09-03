---
title: SP-DATA-5 follow-on — clamping the one-call budget overshoot
doctype: design-spec
module: orchestrator
slice: SP-DATA-5-clamp
status: approved
date: 2026-09-03
---

# SP-DATA-5 follow-on — clamping the one-call budget overshoot

## 1. Summary

SP-DATA-5's budget gate is a **floor-trigger**: it refuses when `spent >= cap` *before* a call,
so a run can exceed its cap by at most one call. The slice recorded that honestly and deferred
the fix as "pre-flight estimation", on the ground that "output tokens are unknowable before the
call" (§2 non-goals, §8).

That framing assumed we must *predict* the cost. We do not have to — we can **bound** it. Every
`Payload::Chat` carries `max_tokens: Option<u32>`, and the orchestrator sets it to `None` at
**all four** producer call sites (`support.rs:499`, `support.rs:538`, `dispatch.rs:446`, plus the
test-support builder). So today output is bounded only by whatever the adapter substitutes for
`None`.

> **Correction, from the whole-slice review, itself corrected in round 2.** This section first
> said that overshoot by one call "can mean a model's entire output limit". The first correction
> replaced that with "true only of `openai_compat` and the local engine; `anthropic`, `gemini` and
> `bedrock` substitute 1024" — which is wrong for two of the five, and the second round measured
> each adapter rather than reading its constant. What `max_tokens: None` actually means:
>
> | family | `None` ⇒ | source |
> |---|---|---|
> | `openai_compat` | field OMITTED (`skip_serializing_if`) ⇒ the model's own maximum | `openai_compat/mod.rs:73`, `:128`; `convert.rs:31` |
> | `anthropic` | `unwrap_or(DEFAULT_MAX_TOKENS)` = **1024** | `anthropic/mod.rs:155`, `:218` |
> | `bedrock` | `unwrap_or(DEFAULT_MAX_TOKENS)` = **1024** | `bedrock/mod.rs:205`, `:271` |
> | `gemini` | **no `generationConfig` at all** ⇒ Gemini's own default | `gemini.rs:636`, `:685` |
> | local engine | `unwrap_or(default_max_tokens)` = **512** | `llama_cpp/mod.rs:398`, `:597`, `:142` |
>
> Gemini is the one that needs explaining: `generation_config` is built only when
> `(max_tokens.is_some() || temperature.is_some())`, and every orchestrator producer sends
> `temperature: None` (`support.rs:500`, `support.rs:539`, `dispatch.rs`'s selector request). So
> with `max_tokens: None` the `DEFAULT_MAX_TOKENS = 1024` at `gemini.rs:43` is never reached. The
> consequence for the design is in §6, and the DIRECTION of the change is not the one first
> recorded: the clamp widens on `anthropic`, `bedrock` and the local engine, and narrows on
> `openai_compat` and `gemini`.

This slice sets `max_tokens` on budgeted runs to what the remaining budget can afford, bounded by
the model's own maximum output. Enforcement moves from our arithmetic to the provider's: the call
*cannot* return more than the cap allows.

## 2. Goals / Non-goals

**Goals**
- On a budgeted run, clamp `Payload::Chat.max_tokens` to `remaining − est_input`, and never above
  the chain's own smallest `max_output_tokens`.
- Refuse below a `MIN_OUTPUT_TOKENS` floor, through the **existing** `Refusal::BudgetExhausted`
  durable-pause path — no new refusal kind, no new operator verb, no new terminal state.
- A pessimistic input estimate for the budget path, distinct from the window-fit estimate.
- Never widen a caller-supplied `max_tokens`.
- Emit a signal when the clamp actually bit, and when our estimate was wrong.
- Additive: `budget: None` ⇒ the request is byte-identical.

**Non-goals**
- **Eliminating the overshoot.** See §4 — we bound it and bias it safe. Claiming elimination would
  be false, and this codebase has spent a slice on exactly that class of false claim.
- A real tokenizer. Considered (§3) and deferred; the fallback path it needs is this design anyway.
- A per-agent `min_output_tokens`. The floor is one constant; a per-role knob can be added later
  without redesign.
- Payload kinds other than `Chat`. `Embed`/`Stt` have no `max_tokens` and keep today's gate.

## 3. The decisions, and why

| Decision | Choice | Why |
|---|---|---|
| Mechanism | **Clamp the request** | Do not predict the cost; bound it. `max_tokens` makes the provider enforce the cap, so the overshoot collapses to the input-estimate error alone. Estimate-and-refuse cannot be sound: it either under-estimates (the bug survives) or pads pessimistically (refuses calls that would have fit, stranding runs with budget left) — a permanent tax on every budgeted run. |
| Below the floor | **Refuse, via the existing pause** | A truncated reply is worse than no reply: it costs real input tokens, produces a mid-sentence answer that flows downstream as work product, and signals nothing. The pause is loud, already built, and recoverable with `torii run wake --budget-tokens`. Fail-closed, matching the slice's stance everywhere else. |
| Estimate soundness | **Over-estimate, and claim only what is true** | `est_tokens` is `chars / 4`; the orchestrator's prompts are full of JSON tool schemas and materialized `## Context` outputs, which tokenize nearer 3 chars/token — so it UNDER-counts on the common case, and clamping on an under-count overshoots by the error. A pessimistic estimate biases the residual toward refusing early rather than overspending. A real tokenizer is model-specific (vocab per model, a per-chain mapping, and a fallback for unknown models *which is this heuristic*), so it is strictly additional work on top of this design, not instead of it. |
| Caller's `max_tokens` | **Take the `min`, never widen** | A clamp that could raise a caller's limit is the "the tool supplies argv but cannot widen the policy" rule SP-4 s4 spent a slice establishing. Today every orchestrator site passes `None`, so this is defence against a future caller, and it is one `min`. |

## 4. What we are allowed to claim

**The overshoot is bounded by the input-estimate error, and biased toward refusing early. It is
not eliminated.** The spec says this in those words, and so must every doc comment and commit
message in the slice.

The arithmetic: with `max_tokens = remaining − est_input`, actual total is
`actual_input + output ≤ actual_input + (remaining − est_input)`, which exceeds `remaining` by
exactly `actual_input − est_input`. A pessimistic estimate makes that term ≤ 0 in the common case.
It is not provably ≤ 0 for all inputs, which is why the claim is "bounded and biased", not "zero".

Against today, the improvement is the whole point: the unbounded half (output) becomes bounded by
construction, and the remaining exposure is a bounded estimation error rather than a model's full
output limit.

**We can measure our own error in production.** The provider returns the real input count in
`usage`, so `actual_input > est_input` is directly observable at the same chokepoint that made the
estimate. That turns the heuristic from an article of faith into something with a feedback loop.

## 5. Architecture

### 5.1 Where it sits

Inside `Executor::dispatch_metered` (`executor/dispatch.rs`), after the existing
`spent >= cap` check and before `self.gateway.execute(request)`. That is already the single
chokepoint every model-call producer goes through — SP-DATA-5's central structural claim, and the
reason this needs no per-producer change.

The clamp needs a mutable request. `dispatch_metered` takes `&InferenceRequest`, so it
**clones locally and modifies the clone** — the caller's request is not disturbed, and no
signature changes.

**The clamp cannot trip the determinism fence, and this is why.** `input_hash` is computed over
`{chain, system, user}` (`dispatch.rs`, via `support::input_hash`) — the SEMANTIC inputs — not
over the whole `InferenceRequest` and specifically not over `max_tokens`. So a call whose clamp
differs between drives still hashes identically and replays from its memo rather than raising
`DeterminismViolation`. That is the correct behaviour twice over: a memoized call is not
re-dispatched at all, and a genuinely re-driven call *should* see the budget as it now stands.
Verified against the code rather than assumed; a plan step re-checks it, because a future change
that folded transport parameters into the hash would turn every budgeted resume into a hard halt.

**There are THREE determinism keys, and each needs its own guard.** They are two functions across
three CALL SITES: `support::input_hash(chain, payload)` is called by `run_node`'s `ModelCall` arm
(and the `Map` producer) and again — with a `{system, user}` payload — by
`SelectorDispatch::complete`, while `support::agent_input_hash`
(`{chain}|{system}|{messages}|{tools}`) is called only by `agent_turn_output`. A guard therefore
attaches to a call site, not to a function: a budget term folded in at one leaves the other two
hashing as before, so one resume test pins one site.

The slice first shipped a single `ModelCall` resume test whose comment claimed all three. Folding
the remaining budget in at `agent_turn_output` took the whole orchestrator suite green, so the agent
site — a budgeted agent that pauses mid-ReAct-loop, is raised, and must replay turn 0 from its memo
— needed its own test. That sequence is the design's OWN recovery story, which makes it the worst
one to leave unguarded: the raise that exists to un-stick the run would be the thing that halts it.
The selector site was measured as the control and is already guarded.

### 5.2 The rule

For a budgeted run with a `Payload::Chat` only:

```
remaining  = cap − spent                       // cap > spent: the existing gate already refused otherwise
est_input  = est_input_tokens_pessimistic(&payload)
allowance  = remaining.saturating_sub(est_input)
ceiling    = gateway.min_max_output_tokens(chain)      // None ⇒ no ceiling from here

if allowance < MIN_OUTPUT_TOKENS  →  Refusal::BudgetExhausted { .., cause: BelowFloor }
else                              →  max_tokens = min(allowance, ceiling ?? ∞, caller's max_tokens ?? ∞)
```

`saturating_sub` matters: `est_input` can exceed `remaining`, and the floor check must see `0`
rather than wrap.

**The `ceiling` term was added by the whole-slice review, and it is not optional.** `allowance` is
a pure budget figure that knows nothing about the model: for any realistic whole-run cap it exceeds
every current model's maximum output, and the providers reject that — Anthropic with a 400
`invalid_request_error` — while every adapter here forwards the value verbatim. Without it, setting
a budget HARD-FAILED the first call of a run that succeeds unbudgeted (measured: a cap of 10240 sent
`Some(10239)` and the node failed). It is a `min` over the CHAIN, not over the selected model,
because the clamp runs before selection and a request that fails over lands on a different entry;
`None` (unknown chain) means "no ceiling from here", matching `over_budget`'s treatment of an
unknown context window.

**The floor check is ordered before the ceiling deliberately.** The floor asks whether the BUDGET
can buy a useful reply; a model whose own limit is below `MIN_OUTPUT_TOKENS` is not a budget problem
and must not be refused as one.

The floor's refusal reuses `Refusal::BudgetExhausted` as §2 requires, but carries a `cause` so the
two situations do not render as one message. Reusing the exhausted wording reported a spend that
did not happen ("0 of 300 tokens spent" on a fresh run) and gave no hint of how far the cap must be
raised.

**The raise the refusal names, and why it is not derived from the allowance.** AC4 asks for "the
smallest cap that would unblock the call". A dispatch needs `(cap − spent) − est ≥ floor`, so that
cap is:

```
needed = spent + est_input + MIN_OUTPUT_TOKENS
```

The tempting one-liner `budget + (floor − allowance)` is algebraically identical whenever
`est_input ≤ remaining`, and silently wrong when it is not — which is precisely the AC5 saturating
branch. `allowance` is `remaining.saturating_sub(est_input)`, so once `est_input ≥ remaining` the
saturation has already destroyed `est_input − remaining` and the allowance-derived figure
understates the raise by exactly that much. It shipped that way: a cap of 1 against a 15-token
estimate reported "raise the cap to at least 257" when the true answer is 271, and each operator
round trip bought only another `MIN_OUTPUT_TOKENS`. Because the pause is `resume_after: None`,
every one of those round trips is a manual `BudgetRaised` + `force_wake`. `BudgetRefusal::BelowFloor`
therefore carries `est_input` alongside `allowance`, and the message is computed from the same three
terms the refusal itself used.

"At least" remains the right hedge, but only for the reason stated: a LATER, longer prompt can still
land under the floor at the new cap. It is not a hedge against this call, which the figure above
unblocks exactly.

### 5.3 The estimate

A new pessimistic estimator for the budget path, over the whole request the provider will see:
the system prompt, every message, and the tool schemas (which the existing `over_budget` already
counts, and which are pure JSON — the worst case for `chars / 4`).

**The existing `est_tokens` keeps its window-fit caller unchanged.** The two want opposite biases:
window-fit asks "will this fit" and wants to avoid false alarms; the budget asks "what is the worst
this costs" and wants to avoid under-counting. One function cannot serve both, and merging them
would silently change the window-fit behaviour this slice has no business touching.

### 5.4 Observability

Two distinct signals, and they mean different things:

- **The clamp bit** — `usage.output_tokens >= emitted_max_tokens`, i.e. the reply stopped at the
  limit we imposed and so was almost certainly cut short rather than finished.
  `InferenceResponse` carries **no `finish_reason`** (only a streaming chunk does), so this
  inference is the available signal; the plan must not claim a provider stop-reason it cannot read.
- **The estimate was wrong** — `usage.input_tokens > est_input`, the residual-overshoot case §4
  bounds.

> **Correction, from implementing Task 8.** This section and AC10 both first said the clamp-bit
> condition was `output_tokens == allowance`. That is wrong in two ways and would have shipped a
> signal that is silent on the cases it exists for.
>
> First, `allowance` is not what the provider was told. §5.2 emits
> `min(allowance, ceiling, caller's)`, so on any chain whose model limit sits below the allowance
> — the common case for a large cap, and the exact situation the ceiling term was added for — a
> reply can never reach `allowance` at all and the signal would never fire. It is compared against
> the value actually SENT. Both numbers are logged, so a reader can still tell WHICH bound bit:
> equal means the budget truncated the reply, `emitted < allowance` means the model's own limit (or
> a caller's own `max_tokens`) did and the budget merely did not prevent it. That distinction is
> also why the emitted record does not claim "truncated by the run's token budget" — on three of
> the five provider families that sentence would often be false.
>
> Second, `==` rather than `>=` fails silent: a provider that returned one token more than it was
> allowed is precisely the thing worth knowing about, and `==` would drop it between the two
> comparisons.

Both are emitted at the chokepoint, and **both are `tracing` records, not journal events.**

That is a decision, not a deferral. These are diagnostics about our own estimator, not run STATE:
nothing folds them, no resume depends on them, and no operator decision is keyed on them. A
journal event would make them durable format — a `FORMAT_VERSION` concern, a fold arm, and a
row on every clamped call in every budgeted run — to carry information the ledger already implies,
since `usage` is journaled and `allowance` is recomputable from the fold. The signals earn their
place by being cheap; making them durable would cost more than they are worth.

If measurement later shows the estimator needs tuning, the durable question can be revisited then
with data — which is the whole point of emitting them now.

### 5.5 Additivity

`budget: None` ⇒ no clamp, no estimate computed, `max_tokens` untouched. Every existing run's
request bytes are unchanged, and the estimator is not even called. This is SP-DATA-5's standing
guarantee and the slice's cheapest regression test.

## 6. Accepted costs

**A budgeted run's replies get shorter as it nears its cap.** That is inherent to clamping, and it
is the honest trade for an enforced cap. §5.4's clamp-bit signal is what stops it being silent —
a node whose answer was truncated by budget must be distinguishable from one that finished.

**A run can now pause where it previously completed.** With the floor, a run whose remaining budget
cannot buy a useful answer pauses instead of spending the last of it on a truncated one. That is
the intended behaviour, but it is a behaviour CHANGE for budgeted runs and belongs in the release
notes, not just the code.

**The floor is a guess.** `MIN_OUTPUT_TOKENS` in the low hundreds suits prose; a gate agent
answering one word needs far less, and a planner emitting a graph needs far more. One constant will
be wrong for somebody, which is the argument for the deferred per-agent knob.

**On three of the five provider families, a budgeted call's output ceiling RISES — and they are
not the three this section first named.** `anthropic` and `bedrock` substitute their own 1024 for a
`None` `max_tokens`, and the local engine substitutes its configured `default_max_tokens` (512 from
the chat builder), so an unbudgeted call on those three is capped low today. A budgeted one is
capped at `min(remaining − est, chain's smallest max_output_tokens)`, which for a large cap is that
chain limit — 2048 to 8192 across the shipped example catalog and presets. Adding a budget can
therefore make an individual
reply LONGER, even though the run's total is now enforced where before it was not.

The other two NARROW. `openai_compat` omits the field when it is `None`, and `gemini` sends no
`generationConfig` at all (see §1's corrected table), so on both an unbudgeted call gets the
model's or the provider's own maximum and a budgeted one gets our value instead. §1 first recorded
gemini among the widening three and the local engine among the pass-throughs; both were wrong, and
the release-notes sentence had the direction of the change backwards for gemini as a result.

This was considered and accepted rather than overlooked. The alternative — also taking
`min(…, 1024)` so the clamp can only ever narrow — would hard-cap every budgeted reply at another
provider's arbitrary fallback constant, silently truncating budgeted runs on the `openai_compat`
and `gemini` paths where `None` means the model's or the provider's own maximum, and would import
that constant into the orchestrator. That trades a surprising-but-bounded widening for a silent
truncation, which is worse. The run's total spend stays bounded either way; only the per-call shape
moves.

**On a HETEROGENEOUS chain a budgeted run's replies are narrowed from the FIRST call, independent
of the cap.** `ceiling` is `min` over every entry in the chain, so a budgeted run on
`[gpt-4o 16384, small-fallback 4096]` emits `max_tokens: 4096` on the primary throughout, however
much budget remains — where the same run unbudgeted would get 16384 on an `openai_compat` router.
This is a different effect from the shortening below, which is a near-the-cap one; an operator
setting `--budget-tokens 1000000` on such a chain should expect it. Not silent (the clamp-bit
`tracing::info!` logs both bounds), and the alternative — bounding by the SELECTED model, after
selection — moves the clamp downstream of the selector and is §8 work. It is accepted here because
the clamp runs before selection deliberately: a request that fails over lands on a different entry,
and a value the fallback would reject turns a survivable failover into a hard 400.

**The pessimism assumes a Latin script.** `chars / 3` over-counts only where a token is worth three
or more characters. CJK, Cyrillic and emoji run nearer 1–3 tokens PER character, so on such a prompt
the estimate under-counts by a multiple, §4's residual (`actual_input − est_input`) becomes a
fraction of the prompt rather than a small error, and the "biased toward refusing early" half of the
claim flips sign. The bound does not vanish, but it widens by a factor a reader could not otherwise
anticipate from these docs. The AC11 `budget clamp under-estimated the input` warning is the
mitigation and a non-Latin prompt is exactly the case it is expected to fire on; the real tokenizer
(§8) is the fix, and this paragraph is the honest cost of deferring it.

**A model with `max_output_tokens: 0` is now rejected at config validation.** `min` over the chain
means one such entry gives `ceiling = Some(0)` and the clamp would emit `max_tokens: Some(0)` —
"generate nothing" — on every budgeted `Chat` call, and the floor cannot catch it because the floor
runs first, by design. The field is a plain required `u32` and several fixtures in this repo use `0`
as don't-care filler, so it was reachable. Rejected in `collect_validation_errors` rather than
special-cased in the clamp, because zero output is broken for every reader of the field (`budget.rs`
prices such a model at zero and it wins any cheapest-first selection). The unchecked `Gateway::new`
/ `update_config` paths still validate nothing, by their own documented design.

**A budget below `MIN_OUTPUT_TOKENS` is now refused by the CLI**, not accepted and paused on
immediately. `parse_budget_tokens` previously rejected only `0`, on the stated ground that a budget
which can never dispatch a call belongs on the precondition side; the floor widened that range.

## 7. Acceptance criteria

1. A budgeted `Chat` request reaches the provider with `max_tokens = Some(remaining − est_input)`,
   and **never above the chain's own smallest `max_output_tokens`** — asserted against a double
   that REFUSES an over-large value the way a real provider does.
2. An **unbudgeted** run's request is byte-identical to today, and the estimator is not called.
3. A caller-supplied `max_tokens` is never widened — `min` is taken, proven with a caller value
   both above and below the allowance.
4. `allowance < MIN_OUTPUT_TOKENS` ⇒ `Refusal::BudgetExhausted`, the durable pause, and **no
   gateway call is made** (asserted on a call counter, not on the outcome alone). Its reason names
   the allowance, the floor and the smallest cap that would unblock the call — distinguishable from
   an exhausted cap, which is a different situation with a different remedy.
5. `est_input > remaining` does not wrap: the floor sees `0` and refuses — **and the raise that
   refusal names is APPLIED and the call then goes out.** Asserting the pause alone is what let the
   allowance-derived arithmetic of §5.2 ship: on this branch, and only on this branch, the figure was
   wrong, and re-driving at exactly the cap the message asked for is the assertion that catches it.
6. A non-`Chat` payload on a budgeted run is unchanged and still gated by the existing rule.
7. Against a fake provider that honours `max_tokens`, a budgeted run's total spend does not exceed
   `cap + (actual_input − est_input)` — the §4 claim, asserted as arithmetic rather than prose.
8. The pessimistic estimator returns **≥** `est_tokens` for the same text, on both prose and a
   JSON-heavy tool schema, and its DIVISOR is pinned from both sides — `≥ chars/3` and
   `≤ chars/3` rounded up. The strict inequality alone is not enough: `div_ceil(4)` still beats
   floor-division by a token on most lengths, so the sign of the difference says nothing about its
   size, which is the whole point.

8b. `est_input_tokens` counts every part of what the provider is sent — system prompt, message
   bodies, an assistant turn's `tool_calls` (name AND arguments), and each tool's name, description
   and JSON schema — each term proven by deleting it alone.
9. `est_tokens`'s existing window-fit behaviour is unchanged (its own tests still pass untouched).
10. The clamp-bit signal fires when `output_tokens >= emitted_max_tokens` and not otherwise —
    against the value SENT, not against `allowance`; see §5.4's correction for why the original
    wording would have been silent on the cases the signal exists for. Pinned on a fixture where the
    two operands DIFFER (the model ceiling binding below a large cap), because every other signal
    test drives `emitted == allowance` and cannot tell the comparisons apart.
11. The estimate-wrong signal fires when `input_tokens > est_input` and not otherwise. "Not
    otherwise" includes an UNBUDGETED call, where no estimate is made at all: both signals sit
    inside the clamp's own `if let`, and a test pins them there. The "not otherwise" test carries a
    positive CONTROL — a third drive whose signal must fire — because `set_default` is thread-local
    and an assertion that a capture is empty is satisfied just as well by a dead capture.
12. A clamped call still journals its real `usage` and folds by effect id exactly as before.
13. **A clamped call replays from its memo on resume rather than raising `DeterminismViolation`** —
    §5.1's fence argument, asserted by resuming a run whose remaining budget (and therefore whose
    clamp) has changed between drives. **Twice: once through `input_hash` (a `ModelCall` graph) and
    once through `agent_input_hash` (a budgeted agent paused mid-ReAct-loop, raised, and replaying
    turn 0).** One test cannot cover both — they are separate functions with separate callers, and
    the agent leg took the mutation green while a comment claimed otherwise.

## 8. Deferred

- A real tokenizer for an exact clamp (needs per-model vocabs, a per-chain mapping, and this
  heuristic as its fallback regardless).
- A per-agent `min_output_tokens`, for roles whose useful answer is much shorter or longer than the
  default.
- Self-calibration: using observed `actual_input / est_input` per chain to tune the estimate.
- The other SP-DATA-5 carry-forwards, untouched here: money denomination, fleet/per-tenant budgets,
  budget-aware scheduling, cross-run spend reporting, and the `RunPaused`-per-gated-child noise.
