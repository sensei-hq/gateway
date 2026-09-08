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
- A prompt no candidate can hold fails with **per-candidate diagnostics** — each model's own
  window and the estimate that exceeded it — instead of the chain-minimum guess. It is still a
  terminal failure; see the §3 decision row, which corrects an earlier draft of this line.
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
| Mechanism | **A sixth `AdmissionGate`** | The pattern exists and fits exactly: `SelectionCtx` already carries `input_tokens`, `GateVerdict::Skip(SkipReason)` already carries a typed reason, and `all_gated_error` already aggregates an all-skipped selection into one `AllGated` carrying every reason and a remedy (a durable pause when a gate is timed; see the row below for the terminal case). A bespoke filter would duplicate all three. |
| Where the check lives | **Gateway, not orchestrator** | Selection is the gateway's job and the orchestrator was guessing ahead of it. Every gateway caller benefits, and the duplicate `min_context_window` read disappears rather than being kept in sync. |
| Over-everything outcome | **`AllGated`, terminal, with per-candidate diagnostics** | **Corrected after review — the first draft of this row said "durable pause", and that was wrong.** `all_gated_error` takes `resume_after` from the TIMED skips alone, so an all-over-window selection is `AllGated { resume_after: None }`, and the orchestrator's `classify_gateway_error` pauses only on `Some(t)`; everything else is `Fail` → `NodeFailed`. That is a deliberate prior decision, not an oversight: risk **M1** in `docs/design/selection-policy-pipeline.md` resolved terminal-only exhaustion as "fail-fast human-action, never pause", and `GatewayError::AllGated`'s own doc says the caller must not pause forever. So the outcome is still terminal; what changes is the DIAGNOSIS (see §6.3), and the improvement is real but smaller than the draft claimed. Reversing M1 is out of scope here — see §8. **Corrected AGAIN 2026-09-04: M1 WAS reversed, one slice later.** The draft's instinct was right and its reasoning was not — a durable pause is the correct outcome, but because the run must stay REACHABLE (`force_wake` matches `status = 'paused'` and nothing revives a terminal run), not because waiting helps. `classify_gateway_error` now pauses indefinitely on a `resume_after: None` that carries a `human_action`, so an all-over-window selection is a HOTL pause naming `UseLargerContextWindow`. See M1 in `docs/design/selection-policy-pipeline.md`. |
| The estimate | **A pessimistic one, including tool schemas** | See §4. The existing `estimate_input_tokens` is `chars/4` over messages + system ONLY, so it silently omits tool schemas — and for a window gate an under-count admits a model the prompt does not fit, which is the failure the gate exists to prevent. |

## 4. The estimate, and the bias that matters

`engine/util.rs::estimate_input_tokens` computes `(message_chars + system_chars) / 4`. Three
problems for this use:

1. **It omits tool schemas entirely.** An agent's activated schemas routinely outweigh its prompt —
   the SP-DATA-5 clamp's own comment records exactly that.
2. **It omits an assistant turn's `tool_calls`.** Those turns carry an empty `content`, so a sum
   over `as_text()` prices them at zero — and the ReAct loop appends one every turn and re-sends
   the whole transcript on every turn after (`executor/agent.rs`). A serialized plan or an
   `fs_write` body is the largest thing the loop produces. **Added after review**: the first
   implementation of the pessimistic estimator inherited this gap, so a chat whose only bulk was a
   100 KB tool-call argument estimated 0.
3. **`chars/4` is the prose figure.** JSON tokenizes nearer 3 chars/token, so it under-counts most
   on the very content it omits.

For the cost `BudgetGate` an under-count is merely optimistic pricing. For a window gate it is the
defect: it admits a candidate the prompt does not fit, and the provider answers 400.

**What the pessimistic estimate does NOT count, stated so "pessimistic" is not read as
"complete":** `Message::attachments`. There is no honest token model for media in this crate — the
only measurable quantity is the `MediaSource` string, and for a `Base64` source that over-counts by
two to three orders of magnitude (a 1 MB image is ~1.4 M base64 bytes — ~466 k "tokens" at `/3` —
against a per-image cost providers publish in the low thousands), while a `Url` source's length has
no relationship to its cost at all. Either would reproduce the failure
the `Stt` arm avoids: an estimate so large that every candidate is skipped and a serviceable
request becomes a terminal `AllGated`. So the estimate is an upper bound on a request's **text**,
not on the request; a multimodal call can still be admitted to a model its images push over. That
is the status quo (nothing gates on the window today) rather than a regression, and no producer in
this workspace attaches media — but a caller that starts to owes the estimator a per-attachment
token term, added after the divide. Recorded in §8.

The numerator is UTF-8 **bytes**, not characters (`str::len()`), which is further margin in the
safe direction and largest exactly where a chars-per-token heuristic is weakest: CJK text is 3
bytes per character and tokenizes near 1 token per character.

**The estimate is per INFERENCE UNIT, not per request, and on two payload kinds that differs from
what the cost estimator measures.** Added after review, because the first implementation summed an
embedding BATCH and priced a TTS prompt, and both were wrong in the same way: they answered "how
much text is in this request" where the gate asks "what is one candidate's `context_window` being
asked to hold". For `Embed` the answer is the largest single input; for `Tts` / `ImageGenerate` /
`VideoGenerate` there is no answer, because those models publish no context window at all, and the
honest return is `0` — the same way the `Stt` arm has always said "not applicable". AC10 states
each arm and its test. The consequence for §4's ordering argument is recorded in AC7: it holds
where the two functions measure the same quantity, which is `Chat`.

**So the window gate uses its own pessimistic figure**, and `estimate_input_tokens` is left alone.
Not because sharing would be wrong in principle, but because widening the shared estimator silently
changes every cost estimate the `BudgetGate` makes, which is a different slice's decision to take.
The two want opposite biases for the same reason `est_tokens` and `est_tokens_pessimistic` are two
functions in the orchestrator: window-fit asks "what is the worst this could be", cost asks "what
will this probably run to".

> **Correction, 2026-09-04** — the analogy has expired; the conclusion has not. Both orchestrator
> functions are now DELETED (`est_tokens` with its callers in this slice, `est_tokens_pessimistic`
> in the serving-window follow-on), and the pair was cost-vs-window rather than a live illustration
> of opposite biases: a BUDGET estimate wants pessimism for the same reason a window estimate does
> — an over-count refuses early, and an under-count overspends. That is why the follow-on could
> hand the budget clamp the window estimator with no bias conflict. What stands unchanged is the
> reason `estimate_input_tokens` and `estimate_input_tokens_pessimistic` are still two functions in
> the GATEWAY: the cost gate prices, the window gate fits, and on `Embed` they do not even measure
> the same quantity (sum vs max).

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

> **Correction, 2026-09-04** — the clamp no longer uses it. The serving-window follow-on
> (`2026-09-04-sp-7a-serving-window-bound-design.md`) moved the clamp's window term to
> `min_serving_context_window(chain, est)`, so `min_context_window` has NO production caller; it is
> kept as a `pub` read accessor and as the contrast the sibling's argument is made against, and its
> own doc says so. See that spec's §7 for the standing decision not to delete it.

`OrchestratorError::PromptOverBudget` becomes unconstructed. It is **removed**, not left dangling: a
variant no code can produce is a claim the type makes and the code does not honour, and the whole
point of this slice is that the orchestrator no longer owns this decision.

### 6.3 What an operator sees

A prompt no candidate can hold produces `AllGated`, whose reason enumerates the per-candidate skips
— so the message names each model's window and the estimate that exceeded it, rather than today's
single `min_win` figure that may belong to a model the request never wanted.

**This required a change to `GatewayError::AllGated` that the first draft assumed for free.** Its
`#[error(...)]` rendered only `"all candidates gated{, resume after t | , human action required}"`
and dropped both the `skipped` vector and the `human_action`. Since `classify_gateway_error` builds
its `NodeFailed` reason from `err.to_string()`, every number this slice adds was being discarded at
the orchestrator boundary — the replacement message would have named strictly *less* than today's
`PromptOverBudget` ("prompt over budget at node N turn T: est 20000 > window 8192"). The variant
now renders all three fields, and `HumanAction` gained a `Display` so "human action required" says
*which*. Pinned by
`kernel::types::error::tests::all_gated_renders_each_candidates_reason_and_the_remedy`.

Since the outcome is terminal (§3), this diagnosis is the whole of what an operator gets, which is
why it is the part with a test rather than a sentence.

**A second `all_gated_error` change, found by review:** it nulled `human_action` whenever any timed
gate set a `resume_after` ("a timed retry wins over the terminal remedy"). That was survivable while
every terminal reason was a credential or a cap — those are at least plausibly related to waiting —
and stopped being so here, because the window is the first reason no elapsed time can EVER clear. A
chain with one breaker-open candidate and one whose window is too small produced a five-minute
pause, a wake, and only then the diagnosis that had been true from the start. The two fields are now
independent: `resume_after` still decides the pause (the caller keys on it alone, unchanged), and
`human_action` says what will still be wrong after the wake. `Display` renders both.

**And one more step was needed for that to reach anyone.** `classify_gateway_error`'s Pause arm
built its reason by hand — `"all candidates gated; resume after {t}"` — discarding the skips and
the remedy that its own Fail arm passes through as `err.to_string()`. So the recoverable case told
an operator strictly less than the terminal one, and the change above would have been invisible on
the one path it was made for. The Pause arm now uses `err.to_string()` too, which is what gets
journaled into `RunPaused` and read back by `torii status`.

## 7. Acceptance criteria

1. A chain `[big 128k, small 8k]` serves a ~20k-token prompt — it selects `big` and succeeds, where
   today it fails terminally.
2. The skip is recorded against `small` with both the estimate and that model's window.
3. A prompt exceeding EVERY candidate's window yields **`AllGated`** rather than a bare
   `NoCandidates`, and its rendered message names at least one model's window, the estimate that
   exceeded it, and the remedy. **Revised after review:** it does NOT become a pause — see the §3
   decision row. The claim that survives is the diagnosis, and it is asserted on
   `AllGated`'s `Display`, not merely on the typed fields, because `Display` is the only channel
   that reaches the orchestrator's `NodeFailed`.
4. An in-window request selects **byte-identically** to today — same candidate, same order.
5. A request with no `input_tokens` estimate admits every candidate (the gate is not a filter on
   missing data).
6. The gate counts **tool schemas** and an assistant turn's **`tool_calls`**; a request whose
   schemas alone push it over a candidate's window is skipped for that candidate. Proven
   *composed* — from a real `Payload` through the estimator and the gate to a verdict — because
   the two halves passing separately is what let the `tool_calls` gap ship. Removing the tools
   term must make it fail.
7. For a **`Chat`** payload the pessimistic estimate is **≥** the existing
   `estimate_input_tokens`, and its unit is pinned by at least one ABSOLUTE assertion
   (`ceil(bytes/3)`) — an ordering-only test cannot see the divisor change. **Revised after
   review: the ordering is asserted for `Chat` alone, not for every payload kind**, because on the
   other kinds the two functions measure different QUANTITIES on purpose (AC10). Each of those
   arms is pinned by an absolute figure instead, which is strictly stronger than the ordering it
   replaces.
8. `OrchestratorError::PromptOverBudget` no longer exists, and no test asserts it.
9. `agent_input_hash` is unchanged for a given prompt — a resumed agent turn replays from its memo
   across a drive where a *different* candidate was selected. Proven with a second drive whose
   config resolves the SAME chain name to a different model (a config reload between processes),
   not with a relabelled response.
10. **The gate applies to the payload kinds a `context_window` actually bounds, and to no others.**
    Revised twice after review; the current statement is:
    - **`Chat`** — gated, on the whole conversation + system + tool schemas.
    - **`Embed`** — gated, on the **largest single text in the batch**, not the sum. An embeddings
      request is a batch: `to_embed_request` forwards `texts` verbatim and the providers apply the
      model's max-input-tokens limit to each ARRAY ELEMENT, with a separate and far larger
      per-request aggregate. Summing terminally refused 100 RAG chunks of 300 bytes against an 8 k
      model — each ~1.2% of the window — with a remedy ("use a larger-window model") that is not
      the fix. Pinned both ways: one oversized text skips an 8 k candidate and admits a 128 k one;
      a batch of individually small texts is admitted whatever its total.
    - **`Stt`** — not gated (estimate 0), asserted against a candidate whose window is **zero**,
      since any non-zero estimate would otherwise pass.
    - **`Tts` / `ImageGenerate` / `VideoGenerate`** — **not gated**, same zero-window assertion.
      `context_window` is a chat-model concept and these models do not publish one; what they
      publish is a maximum prompt LENGTH in characters, a different bound in a different unit that
      this config has no field for. Gating on the filler value would make every such request a
      terminal `AllGated` naming a lever the operator cannot pull. §8 records the real fix.
11. **`context_window == 0` is a config error** (`collect_validation_errors`), mirroring
    SP-DATA-5's Rule 5 for `max_output_tokens`. Added after review: SP-7a is what turns this field
    into a hard-fail selection input, and `0` is both existing fixture filler and the natural
    "not applicable" entry.
12. **A terminal remedy survives a timed wake.** `all_gated_error` no longer nulls `human_action`
    when `resume_after` is `Some`. Added after review: the window is the first terminal reason no
    elapsed time can EVER clear, so a chain with one breaker-open candidate and one too-small one
    used to pause, wake, and only then report a problem that was true from the start.

## 8. Deferred

- **SP-7b — context budgeting** for the case where no candidate can hold the prompt, with the
  determinism argument §5 describes.
- **SP-7c — semantic / retrieval-ranked activation.**
- **Bounding the SP-DATA-5 clamp by the SELECTED model's window** rather than the chain minimum.
  That is the clamp spec's own §8 item, and this slice makes it reachable for the first time: once
  selection is window-aware, a post-selection bound is available. Not taken here, because the clamp
  runs before selection and moving it downstream is its own change.

  **A review found this is not merely a nicety — it bounds where SP-7a's benefit applies.** On a
  BUDGETED run the clamp computes `window = min_context_window(chain) − est` and refuses with
  `Refusal::BudgetExhausted { cause: BelowFloor }` when the resulting ceiling is under
  `MIN_OUTPUT_TOKENS` (`executor/dispatch.rs`). For AC1's own example — chain `[big 128k, small
  8k]`, a 20 k prompt — that is `8192.saturating_sub(20000) = 0`, `0 < 256`, so the request is
  refused in the orchestrator **before `Gateway::execute` is called** and the new gate never runs.
  So: **AC1 holds on an unbudgeted run — every pre-SP-DATA-5 run, and the default — and does not
  hold on a budgeted one until the clamp's window term moves to the selected candidate.** Task 6
  must not delete the orchestrator pre-check while believing otherwise.

  **And deleting the pre-check CHANGED this arm's outcome class, which an earlier version of this
  paragraph said it did not.** The halt ran in `agent_turn_output` before `dispatch_metered`, so on
  a budgeted run it fired first and produced a terminal `NodeFailed` naming the window. With it
  gone the clamp gets there first and the run PAUSES — measured identically at caps of 1e6, 1e8 and
  `u64::MAX`, because the window term never reads the cap, so the pause named a lever that could
  not move it. Fixed within this slice by wording rather than by reversing the flip: when the
  window is the binding term the refusal now says so, names the window, and does NOT offer
  `torii run wake --budget-tokens N`. The pause CLASS is kept deliberately — `resume_after: None`
  is SP-DATA-3's HOTL class and preserves a run an operator can widen the chain for, where a node
  failure destroys it. Asserted at the time in
  `a_budgeted_run_is_refused_by_the_clamp_before_the_window_gate_is_asked`, including the
  cap-independence — **both that test and that pause class moved in the follow-on; see the
  correction below.**

  > **Correction, 2026-09-04 — this whole item is CLOSED, and not the way it predicted.** The
  > follow-on slice (`2026-09-04-sp-7a-serving-window-bound-design.md`) made the clamp's window
  > term `min_serving_context_window(chain, est) − est`: the minimum over exactly the candidate set
  > the gate admits. So **AC1 now holds on a BUDGETED run too**, and it holds *without* moving the
  > clamp downstream of selection — the paragraph above says that move is what it would take, and
  > that turned out to be one way rather than the only way. A pre-selection fold over the admitted
  > set is safe for whichever candidate wins, because every member of the set is at least the
  > minimum over it.
  >
  > Two things the follow-on changed that this text still describes in the old terms: the term is
  > no longer `min_context_window(chain) − est`, and the `[128k, 8k]` / 20 k example no longer
  > refuses at all — it dispatches to `big`. What remains deferred is the strictly more precise
  > post-selection bound (the clamp spec's §8), which is now an optimisation rather than the fix.
  >
  > The refusal that survives means something narrower: the prompt FITS the smallest window that
  > can hold it, and the shortfall is OUTPUT room. The "nothing fits anything" case is the gate's
  > now, and that handed a budgeted run a terminal `NodeFailed` where it used to get a durable
  > pause — recorded as the one user-visible regression in the follow-on's §8, **and CLOSED there
  > on 2026-09-04 by reversing M1**: an `AllGated` naming a `human_action` is now the indefinite
  > HOTL pause, so both arms halt recoverably with the gate's per-candidate remedy.
  >
  > Also corrected there: the clamp and the gate were computing `est` with two different functions,
  > which broke the soundness argument in a narrow but reachable band. They are one function now
  > (follow-on §4.1).
  >
  > **The identifier this item cites is gone, and the property it cited went unguarded for a
  > commit.** `a_budgeted_run_is_refused_by_the_clamp_before_the_window_gate_is_asked` was renamed
  > `an_over_every_window_prompt_is_refused_by_the_gate_budgeted_or_not`, and it changed meaning
  > with its name: the fits-nothing scenario now lands on the GATE at both caps rather than on the
  > clamp. The surviving `BelowFloor { window: Some(_) }` arm is
  > `a_prompt_that_fits_but_leaves_no_room_for_output_names_the_serving_window`, which drove ONE
  > cap — so the cap-independence this sentence claims was asserted became unpinned in the move.
  > The follow-on's review caught it (a mutation gating the refusal on `remaining` passed the whole
  > workspace) and closed it with `a_serving_window_refusal_is_unmoved_by_the_cap`, which drives
  > 1e6 and `u64::MAX / 2` and compares the two refusals with the ledger clause stripped.
  >
  > Corrected at the same time: the message's remedy. It read "send less input, or put a model with
  > a larger window in this chain", and the second clause could never work — the term is
  > `min { w ∈ chain : w >= est }` and **adding to a set cannot raise its minimum**. It now says to
  > remove or replace the entry it names, says outright that adding a larger model alongside cannot
  > help, and qualifies "send less input" as conditional on that same entry staying the smallest
  > one that can hold the prompt.
- **A per-attachment token term for the pessimistic estimate** (§4). Owed by the first caller that
  attaches media; the term belongs in tokens, added after the divide, and must not be derived from
  the base64 length.
- **A real input bound for the non-chat capabilities** (AC10). `Tts` / `ImageGenerate` /
  `VideoGenerate` DO have provider-published input maxima — a prompt character limit — but it is
  not `context_window` and `ModelConfig` has no field for it. Until one exists those payload kinds
  are exactly as ungated as they were before this slice, which is the status quo rather than a
  regression; the failure mode is a provider 400 on an over-long prompt. The same gap covers an
  `Embed` batch's per-request AGGREGATE limit, which is separate from the per-item limit AC10
  gates on.
- **Splitting an oversized `Embed` batch** rather than refusing the request. The gate now judges
  the largest single input, so an over-limit batch of small texts is admitted and reaches the
  provider intact; a batch containing one over-limit text is refused whole, where splitting it out
  would serve the rest.
- **Making a deadline-less `AllGated` pausable** — i.e. reversing risk M1 of the selection-policy
  design so `classify_gateway_error` pauses on `AllGated { resume_after: None, human_action:
  Some(_) }` and an operator wakes it with SP-DATA-3's `force_wake` (which already models exactly
  this shape: NULL `next_wake`, never auto-woken). That would make §2's original "the run survives
  and an operator can widen the chain and wake it" true. It is deliberately NOT in this slice: it
  changes behaviour for every terminal gate — top-up-credits, rotate-credential, raise-budget —
  not just for the window, and it needs its own argument about who is responsible for a pause with
  no deadline and what happens to a fleet of them.
- Widening `estimate_input_tokens` itself to count tool schemas, which would make the cost
  `BudgetGate` more conservative too — a real improvement, and a different slice's call.
