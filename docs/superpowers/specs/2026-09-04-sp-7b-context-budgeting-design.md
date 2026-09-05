---
title: SP-7b — context budgeting (degrade an over-window prompt instead of refusing it)
doctype: design-spec
module: orchestrator + gateway
slice: SP-7b
status: approved
date: 2026-09-04
---

# SP-7b — context budgeting

## 1. Summary

An agent turn whose assembled prompt exceeds every candidate's context window is **refused**.
Since SP-7a the refusal is the gateway's `ContextWindowGate`, per candidate, with real diagnostics;
since the M1 reversal (2026-09-04) it is a recoverable HOTL pause rather than a terminal failure.
What it is not, and has never been, is an **answer**.

This slice makes such a turn proceed on a reduced context, and it does so under a hard constraint
SP-7a's spec recorded a day earlier:

> SP-7b owes an argument that what gets cut is a function of journaled state alone, never of which
> candidate this drive happened to select.
> — `2026-09-03-sp-7a-window-aware-selection-design.md:141`

§4 is that argument. It is the reason this is a separate slice, and it is discharged **literally**
rather than in effect: the cut is a pure function of two journaled values.

**Most of the mechanism already ships.** `render_context_section_bounded`
(`crates/orchestrator/src/agent/prompt.rs:251-293`) is a reviewed, pure, byte-budgeted truncator —
even split across dependencies, heading never cut, per-entry `(truncated: X of Y bytes shown)`
marker charged against the entry's own room, cut taken on the last COMPLETE entry boundary, and an
honest `(N of M dependencies shown)` tail. Its parameter type is `&[(String, String)]`, which is
exactly the element type of `PromptParts.context` (`prompt.rs:19`). It has one production caller
today: the HUMAN path (`executor/human.rs:150`). `PromptParts` was split into
`authored` / `context` / `tools` in SP-6 s3 for precisely this reason — so a caller that must bound
the run-data half can do so per dependency.

So this slice is mostly a **mechanism change at one seam plus a determinism argument**, not new
machinery.

## 2. Goals / Non-goals

**Goals**
- An agent turn over every candidate's window is degraded and DISPATCHED, not refused, while enough
  context survives to be worth answering from.
- The cut is a pure function of journaled state alone (§4), so no resume can recompute a different
  one — which, given §4.3, would otherwise strand the run terminally.
- A floor: below it the turn is still refused, via the HOTL pause class, because an answer built on
  almost nothing is worse than an honest halt.
- Degradation is disclosed on four channels: the model, the journal, the node's output, the operator.
- `FORMAT_VERSION` stays 1.

**Non-goals**
- **The ReAct transcript (`messages`).** It is usually the DOMINANT term in an over-window prompt, so
  excluding it means this slice does not fix every over-window turn. Deliberate, for a reason that is
  not scope-fatigue: `messages` is already not a pure function of journaled state (§4.5), and
  compacting a growing transcript is a different mechanism (the master spec's "restart from a
  summarized checkpoint", `2026-08-06-sensei-orchestrator-design.md:202`).

  **A consequence review made explicit: a budgeted node is effectively SINGLE-TURN.** §5.1 spends
  the window down to `MIN_OUTPUT_TOKENS`, so a budgeted turn 0 is dispatched at **at most**
  `window − 256` tokens and the transcript's GUARANTEED headroom is 256 tokens = 768 bytes. An
  agent that then calls a tool re-sends `system` plus the assistant turn (whose
  `tool_calls.name + arguments` the estimator counts) plus the tool result; past 768 bytes of that,
  every candidate is gated and the run takes the `AllGated` HOTL pause — after paying for turn 0.
  Excluding the transcript from being BUDGETED was always the plan; leaving it no room is the part
  that was not stated. Reserving a growth allowance is the fix and it needs a number nobody has
  evidence for, so the behaviour is pinned by a two-turn test instead
  (`a_budgeted_agent_that_calls_a_tool_busts_the_window_on_the_next_turn`) and a later change to it
  is a visible one.

  **A BOUND, not an equality — an earlier revision of this bullet said "exactly".** The budget is
  handed out by `available_context_bytes`; whether the render SPENDS it is
  `render_context_section_measured`'s business, and it splits the budget evenly across the entries
  and never redistributes an unused share (§5.2 records the same limitation from the planning
  side). A node with unevenly sized dependencies — a 10-byte one beside a 10-KiB one — dispatches
  strictly UNDER the bound with correspondingly more transcript room. The pinning test's fixture
  has ONE dependency, which cannot be split unevenly, so the equality is tight there and is
  asserted there rather than claimed here for every shape.
- **Model-call summarization.** No summarizer effect, at produce time or consume time. §9 records
  both shapes and why neither is needed to deliver this slice's promise.
- **`Message.attachments`.** Counted by neither the estimator nor any truncator. Pre-existing.
- **SP-7c** (semantic / retrieval-ranked activation). Unchanged.

## 3. The decisions, and why

| Decision | Choice | Why |
|---|---|---|
| What is journaled | **The BUDGET, not the cut** | The truncator is pure and every other input is already replay-stable, so the budget integer is the ONLY unfenced input. Journaling it makes the cut a function of journaled state *literally*, including on drive 1 — where journaling the cut itself would only make a candidate-dependent decision reproducible, not candidate-independent, which is the half of SP-7a §5 that journaling does not address. It also keeps the record tiny: a cut description rich enough to RECONSTRUCT bytes (a digest cannot — the recompute must reproduce, not verify) would land inline in the `event` jsonb, since only `ContextWrite.content` can carry a CAS ref for arbitrary text. |
| Which window | **The chain's LARGEST** (`max_context_window`) | Least cutting that still fits something, which is what availability wants. Safe because the GATE remains the per-candidate authority: shrinking to the largest window means a smaller candidate is skipped and selection lands on the larger one, which is exactly SP-7a's designed behaviour. Note this is NOT the clamp's rejected "bound by the chain's largest" — that was rejected because `max_tokens` must be safe for whichever candidate is SELECTED. Here we are shrinking the INPUT so at least one candidate can hold it, and the per-candidate check still runs afterwards. |
| Cut order | **`context` entries, then whole tool schemas; never `authored`** | The repo ships fixtures at both extremes — tools ≈ 100% of the payload (`gateway/src/engine/tests.rs:4395-4431`) and system ≈ 100% (`orchestrator/src/executor/tests.rs:4831`) — so cutting context alone is not generally sufficient. `authored` is the author's own bytes: they can trim it, and the human path already fails loudly on it rather than truncating (`MAX_HUMAN_TEXT_BYTES`). |
| Tool schemas | **Dropped WHOLE, in reverse activation order** | A schema truncated mid-JSON is an invalid tool definition the provider rejects, turning a degradation into a 400. Reverse activation order is deterministic over the pinned registry and explainable: the last thing the activation policy chose is the first to go. |
| The floor | **A fixed fraction of the requested context bytes** (`CONTEXT_FLOOR_FRACTION`), plus a non-positive budget | Unbounded degradation answers a 200k-token question from 4% of its context, confidently, with no way for the model to know it is unqualified. The fraction is a JUDGMENT CALL with no evidence behind it yet; AC10's operator warn exists to replace the guess with a measurement. Not per-agent configurable — that adds a config field, its validation and a fence interaction, for a knob nobody has asked for. |
| Who refuses at the floor | **The orchestrator, not the gate** | Once the prompt is shrunk the gate would ADMIT it, so the gate can no longer be the refuser. The refusal must produce the same HOTL pause class the M1 reversal established, or SP-7b reintroduces the unrecoverable-terminal defect in a new place. |
| Fold discipline | **FIRST-wins**, `entry().or_insert` | A budget that can be retroactively rewritten is not a fence. Note the hazard: the two nearest templates (`fold.expansions`, `fold.selections`, `executor/support.rs:197-202`) both fold LAST-wins, so the correct discipline is a ONE-TOKEN difference from the code being copied and is invisible on inspection. AC6 exists to pin it. |

## 4. Why the cut is a function of journaled state alone

### 4.1 The obligation, restated precisely

Let `cut` be the bytes removed from the assembled prompt. The obligation is that for any two drives
`d1`, `d2` of the same run reaching the same turn, `cut(d1) == cut(d2)` — regardless of config
reloads, binary upgrades, gateway catalog edits, breaker state, or which candidate selection
returned.

**Including `cut = ∅`, which the first draft of this section missed and review caught.** The
obligation is symmetric: a turn dispatched UN-cut must stay un-cut on every later drive. That case
journals nothing (AC11), so absence of a `ContextBudgeted` row cannot distinguish "was not
budgeted" from "not budgeted yet" — and since this slice makes `max_context_window(chain)` an
input to the prompt for the first time, a window that SHRINKS between drives would retroactively
cut a turn already on the wire, with the same terminal §4.3 consequence as budget drift and the
same durability (the spurious row is appended before the memo hash is checked, so restoring the
config does not recover the run).

The fix needs no second event, because the journal already records the decision. `ContextBudgeted`
is appended strictly before the turn's `EffectRecorded`, so

```
memo has effect_id(node,0,0)  ∧  no budget row for that key   ⟹   turn 0 was dispatched UN-cut
```

and on that reading the window is not read at all. So the two fences are: the budget integer for a
cut turn, and the MEMO for an un-cut one.

### 4.2 The inputs, and where each comes from

`cut = f(context_entries, authored, tool_schemas, budget_bytes)`.

| Input | Provenance | Replay-stable? |
|---|---|---|
| `context_entries` | `resolve_context` is Hard-dep-only from `Scope::Run`, loaded by CAS digest (`executor/mod.rs:1638-1651`) | **Yes** — content-addressed |
| `authored` | Pinned registry (agent `system_prompt` + activated skill bodies) | **Conditionally** — see below |
| `tool_schemas` | Pinned registry activation | **Conditionally** — same |
| `budget_bytes` | Derived from `max_context_window(chain)` — a `GatewayConfig` read | **NO** |

The last row is the whole problem, and it is worse than "not fenced". Verified: `GatewayConfig`
carries no version or generation field at all (`rg -c version crates/kernel/src/types/config.rs` →
**exit 1, zero matches**), and the `#cfg{gen}` fence is built from `RegistryConfig { agents, skills,
tools, chain_bindings }` (`orchestrator-core/src/registry.rs:264-269`) with **no catalog term**. So
an operator editing a model's `context_window` between drives changes the budget with nothing
loud anywhere.

Journaling `budget_bytes` moves that row to **Yes**, and then all four inputs are journaled or
fenced. Since `f` is pure — `prompt.rs` imports `ToolDefinition`, four `orchestrator_core` types
and (for `transcript_estimate`) the gateway's own estimator, which is a `match` over a payload;
no clock, env, rand, statics or interior mutability — the conclusion follows.

**A fifth input, found in review: `f` itself.** `dropped_tools` is decided by `plan_budget`, which
reads `CONTEXT_FLOOR_FRACTION` — a constant §5.3 says exists to be re-tuned — plus the section
overhead reservation and the per-schema byte mirror. So re-running the PLANNER on a later drive
made the cut a function of the binary, and re-tuning the fraction would have killed every
in-flight budgeted run on its next resume. The record already carries `dropped_tools`, so the
replay reads that back and derives the section budget as `available − authored − Σ kept schemas`,
which shares none of the deciding arithmetic (`prompt::replayed_plan`). What remains shared is the
RENDERER, and that is irreducible: both drives must render the same bytes from the same budget.

**The "Conditionally" in rows 2 and 3 is not hedging, and this slice does not repair it.** The
`#cfg{gen}` suffix is appended by `pinned()` only when a `RegistryHandle` supplies a generation; on a
`ConfigSource` whose `version()` returns `None` the generation falls back to a process-local counter,
which SP-DATA-2 established is meaningless across processes in both directions. So on an unversioned
source, an edit to an agent's `system_prompt` or its activated skills between drives changes
`authored` with no `VersionFenceMismatch` — and, per §4.3, that is a terminal
`DeterminismViolation`. This is **pre-existing** and applies to every agent turn today, budgeted or
not: `authored` is already hashed into `agent_input_hash`. SP-7b neither worsens nor fixes it, and
the claim in §4.1 should be read as holding under a versioned `ConfigSource` — which
`PostgresConfigSource` is (`version()` always returns `Some`, absent ⇒ `Some(0)`).

### 4.3 Why this is mandatory rather than defensive

A recomputed budget is not a cosmetic risk. Verified from both sides:

1. `drive` builds a fresh `DriveState::default()` (`executor/mod.rs:1126`) and passes only
   `completed`/`terminal` to `ready_nodes` (`:1128`), which takes no `Fold` parameter
   (`support.rs:21-36`). `state.completed` is written at exactly ONE site — `apply_node_result`'s
   `Completed` arm (`mod.rs:1195`), i.e. a node completing in THIS drive. There is no fold-seeding
   site.
2. So a resume re-drives the whole graph, and `agent_turn_output` recomputes
   `agent_input_hash` at `agent.rs:384` for every past turn, **forever**.
3. A mismatch returns `Err(OrchestratorError::DeterminismViolation)` (`agent.rs:387-392`).

And that error is terminal: the run lands `Failed` in the scheduler store
(`orchestrator/src/scheduler.rs:167-176`), `force_wake` matches only `status = 'paused'`, and
`torii run wake` refuses. **No supported command revives it** — the same unrecoverability the M1
reversal was just fixed to remove. A drifted budget would therefore convert a verbose prompt into a
permanently dead run.

### 4.4 The ordering proof

The budget must be journaled and read back BEFORE the hash exists. It can be:

- `effect_id` is pure over three STRUCTURAL coordinates —
  `sha256_hex("{parent_path}|{loop_iteration}|{local_index}")`, `orchestrator-core/src/effect.rs:20-24`
  — and takes no prompt input.
- It is computed **one line before** the hash: `let eid = …` at `agent.rs:383`, `let ih = …` at
  `agent.rs:384`. Verified at HEAD.
- `Executor::append` is `&self` and async (`mod.rs:1531`) with no ordering guard, and is called from
  eight sibling submodules.

So the sequence `fold.budget_for(&eid)` → apply the cut → assemble → hash → memo-compare is
available, and the read is keyed on something that exists before any prompt bytes do.

**The trap, which the naive implementation hits silently:** `Fold` is built exactly once per drive
from one `journal.load` (`mod.rs:1034`) and passed as an immutable `&Fold`, never refreshed
mid-drive. "Append the budget, then read it back from the fold" therefore reads a STALE fold and
recomputes on the writing drive. The writing drive must carry a LOCAL value and only later drives
read the fold — which is exactly `drive_expand_with`'s shipped shape (`expand.rs:117-295`: append at
`:284-292`, then use the local value at `:293`).

### 4.5 What this does NOT make deterministic, stated so it is not read as complete

Journaling the budget fixes **budget drift**. It does not fix **input drift**, and one instance
already exists in the codebase: `execute_tool_effect` computes
`let stale = class == EffectClass::Observation && !self.observation_fresh(ar, teid);`
(`agent.rs:549`) and on `stale` falls through the memo-replay return to a live tool call;
`observation_fresh` reads `self.clock.now()` (`agent.rs:781`). So a re-fetched Observation can change
the transcript by wall-clock, and applying an identical cut to different bytes yields different
bytes.

This is pre-existing and is NOT SP-7b's to fix. It is, however, a direct argument for §2's exclusion
of `messages`: budgeting the transcript would build a determinism claim on top of an input that
already drifts.

## 5. Architecture

### 5.1 Budget derivation, and a required reorder

```
budget_bytes = 3 × (max_context_window(chain) − MIN_OUTPUT_TOKENS − est_tokens(messages))
```

- `max_context_window(chain)` — a NEW async accessor on `Gateway`, structurally a sibling of
  `min_context_window` (`engine/mod.rs:213`), `min_serving_context_window` (`:312`) and
  `min_max_output_tokens` (`:382`). `None` (unknown chain, no models) ⇒ no budget ⇒ assemble
  unbounded exactly as today.
- `MIN_OUTPUT_TOKENS` is `256` (`orchestrator-core/src/budget.rs:61`) — reserved so a degraded turn
  still has room for a usable reply.
- `est_tokens(messages)` comes from `gateway::estimate_input_tokens_pessimistic`, the ONE shared
  estimator (`engine/util.rs:273-331`, re-exported `lib.rs:34`). The orchestrator's own two
  estimators were deleted for exactly this reason (tombstone `prompt.rs:299-323`); this slice must
  not add a fourth.
- **The `×3` is exact, not a fudge.** The estimator is `chars.div_ceil(3)` (`util.rs:331`), so a
  token budget `T` is precisely a byte budget of `3T` over the counted parts.

**The reorder.** `parts.join()` runs at `agent.rs:271` and `resolve_chain` at `:273` — the chain is
not resolved at the join site, so the window cannot be read there. `resolve_chain` moves ahead of the
join. Note also that `pub fn join(self)` CONSUMES the parts, so the bounded variant takes the budget
as a parameter rather than the caller rendering twice to measure.

`MAX_HUMAN_CONTEXT_BYTES` is explicitly NOT reused as the budget: at 32 KiB it is 10 923 pessimistic
tokens, ~33% OVER the entire 8192-token shipped preset window (`gateway/src/catalog/presets.rs`).

### 5.2 The cutting order

1. `context` entries via `render_context_section_bounded(entries, budget)` — unchanged, reused.
2. If still over, drop whole tool schemas until it fits. **"Reverse activation order" means the
   reverse of the order the schemas appear in `PromptParts.tools`**, which is the order
   `assemble_prompt_parts` produced them in and therefore a pure function of the pinned registry and
   the activation policy — not a size-based or alphabetical order, either of which would be stable
   too but would discard the policy's own ranking.
3. `authored` is never cut.

A known limitation of step 1, inherited rather than introduced: `render_context_section_bounded`
splits the budget EVENLY and never redistributes an unused share, so a mixed-size dependency set
wastes most of the budget (three 10-byte entries and one 10-KiB entry give each a quarter). No
shipped test covers a mixed-size set. This slice reuses the truncator as-is and records the gap;
redistribution is a behaviour change to a function the human path also calls, and it belongs in its
own change with its own test rather than riding along here.

### 5.3 The floor

Two byte counts, defined so AC9 is unambiguous:
- `requested_context_bytes` — the sum of the raw entry BODIES in `PromptParts.context`, before any
  rendering (so headings and separators are excluded from both sides of the ratio).
- `retained_context_bytes` — the sum of the body bytes actually emitted, excluding headings, markers
  and the `N of M` tail. Truncation markers are not retained content and must not count toward the
  floor, or a section consisting entirely of markers would pass it.

`CONTEXT_FLOOR_FRACTION = 0.25`, as a named constant beside `MIN_OUTPUT_TOKENS`. The value is a
judgment call with no evidence behind it; AC10's warn is what replaces it with a measurement.

**The rule, corrected after review: SP-7b refuses only when a cut that FITS exists and retains too
little.** Everything else is the GATE's refusal (AC5), because the un-cut prompt is over the
LARGEST window by the arm's own guard — the same predicate `ContextWindowGate` skips a candidate
on — so falling through refuses per candidate, with real window figures and a remedy, and
dispatches nothing.

So the orchestrator refuses when:
- `retained_context_bytes < CONTEXT_FLOOR_FRACTION × requested_context_bytes` on the MEASURED cut, or
- no cut meets that floor at any tool-schema count, while the authored half DOES fit the budget.

And it falls through to the gate when no cut can fit at all:
- `budget_bytes <= 0` — the transcript plus the reserve already fills the largest window; or
- `authored_bytes > budget_bytes` — the authored half is never cut (§5.2), so no degradation is
  available.

The first draft of this section listed `budget_bytes <= 0` as an unconditional refusal and made no
distinction for the authored half. Both were wrong the same way: the floor pause would blame the
dependency context and the 25% floor for a refusal neither caused, name remedies (shorten the
upstream output, split the node) that cannot work, and displace the gateway's per-candidate
diagnosis. It was also non-monotonic — the same agent with ZERO dependency bytes fell through and
got the accurate answer, and adding one 100-byte dependency replaced it with a worse and false one.
`prompt::BudgetRefusal` is the type that now carries the distinction.

Note `requested_context_bytes > 0` is a CONSEQUENCE rather than a side condition: the floor of a
dependency-free node is zero and a zero floor is always met, so a floor refusal implies a non-empty
context and the pause can never report "0 bytes requested".

The refusal is raised by the orchestrator and must produce `RunPaused { resume_after: None }` — the
HOTL class — with a reason prefixed `context budget: ` and naming the node, the window, the
requested and retained byte counts, and the remedy. It must NOT produce a `NodeFailed`. The prefix
is load-bearing for tests: both refusals name the window, so a substring assertion on the window
alone cannot tell the floor pause from the gate's.

### 5.4 The journal event

```rust
ContextBudgeted {
    node: NodeId,
    effect_id: EffectId,
    budget_bytes: u64,      // load-bearing: the replayed input
    source_window: u32,     // disclosure/audit: which window it came from
    retained_bytes: u64,
    dropped_deps: u32,
    dropped_tools: Vec<String>,
}
```

Additive variant ⇒ `FORMAT_VERSION` stays 1, via externally-tagged serde over an `event jsonb`
column with no size constraint (`orchestrator-core/src/journal.rs:128-129`, `:15`). Appended BEFORE
dispatch, fold-guarded, folded FIRST-wins into a side-map keyed by `effect_id`.

`budget_bytes` **and `dropped_tools`** are the replay inputs, so both are folded and the FIRST-wins
latch covers both (§4.2's fifth input). `source_window`, `retained_bytes` and `dropped_deps` are
the audit channel and are deliberately NOT folded, so no reader can mistake a disclosure figure for
a replay input.

The key is `effect_id(node, 0, 0)` — **one budget per agent NODE, not per turn.** `system` is
assembled once, above the ReAct turn loop, and is turn-invariant; only `messages` grows and the
transcript is out of scope (§2), so journaling inside the loop would make `system` a function of
the transcript. What the `EffectId` key buys is separation of distinct nodes, `Map` children and
`Loop` iterations, which a bare `NodeId` would collide across.

**Two known traps to close deliberately:**
- `fold_journal` ends in `_ => {}`, so a new variant that is never folded compiles and silently does
  nothing.
- A new variant compile-errors in exactly ONE place — `label` in `tests.rs` — which
  `cargo build --workspace` never compiles. So torii's readers (`filter_map` closures plus a
  `_ => {}`) must be widened deliberately, not when the compiler complains.

### 5.5 Disclosure, four channels

| Channel | Mechanism |
|---|---|
| The model | The existing `truncate_with_marker` per-entry marker, when an entry is TRUNCATED; the `(N of M dependencies shown)` tail, when whole entries are DROPPED. Two signals for two degradations — a turn exhibits whichever it suffered, and a one-dependency cut exhibits only the first (AC10) |
| The journal | `ContextBudgeted`, above |
| Downstream | An additive `context_budgeted: true` key on the agent node's output. ADDITIVE is load-bearing: the output stays `{"text": …}` plus a key, so an unmodified `BranchCond::TextContains` keeps working — the same discipline SP-6 s3 used for `actor` |
| The operator | A `tracing::warn!` naming the window, requested and retained bytes and the dropped counts, in the style of SP-DATA-5's AC11 clamp warn — the instrument that turns `CONTEXT_FLOOR_FRACTION` from a guess into a measurement |

## 6. Acceptance criteria

1. `Gateway::max_context_window(chain)` returns the largest `context_window` among a chain's models;
   `None` for an unknown chain and for a chain with no models.
2. An agent turn whose assembled prompt exceeds every candidate's window but whose context can be
   budgeted above the floor **completes**, and the dispatched request fits: measured as
   `gateway_est(request) <= max_context_window(chain)`, with `gateway_est` computed by the gateway's
   own estimator on the request the PROVIDER received — never recomputed by the test from the
   orchestrator's arithmetic. A budget that satisfies every assertion phrased in its own terms and
   still overflows the real window is the failure mode this AC exists to exclude, and it is the one
   SP-DATA-5's AC10 was added for after review found exactly it.
3. The budget is journaled as `ContextBudgeted` BEFORE the model call, exactly once per agent
   NODE — keyed `effect_id(node, 0, 0)`, per §5.4. This read "once per turn" until AC10's work
   noticed it; §5.4 already said the opposite in bold, and the shipped assertion is
   `rows.len() == 1` for a node, so only this line was wrong. The two coincide on a
   single-turn node, which is every fixture in the slice, so nothing caught it.
4. **The replay property.** A run whose turn was budgeted on drive 1, resumed on drive 2 with
   `max_context_window` returning a DIFFERENT value, replays from its memo: no
   `DeterminismViolation`, no second provider call. This is the slice's central claim, and the
   changed window is what makes the test non-vacuous — pinning it with an unchanged window would
   pass without the journaling.
5. The cut is byte-identical for identical `(context_entries, budget_bytes)` regardless of chain,
   candidate or clock.
6. `ContextBudgeted` folds FIRST-wins: a second event for the same `effect_id` does NOT change the
   replayed budget.
7. Tool schemas are dropped WHOLE — no dispatched request ever carries a partial schema — and in
   reverse activation order.
8. `authored` bytes are never cut.
9. **The floor halts, recoverably.** A prompt whose retained context would fall below
   `CONTEXT_FLOOR_FRACTION` — measured, or unreachable at every tool-schema count while the
   authored half DOES fit — produces `RunPaused { resume_after: None }` with a reason prefixed
   `context budget: ` and naming the node, the window and every byte count it actually
   MEASURED, and **no** `NodeFailed`. Exactly one durable pause row, and no provider call for
   that node.

   **Only the measured arm reports a retained figure, and the wording earlier in this AC's life
   ("both byte counts") was what licensed the bug.** The planner arm (`FloorUnreachable`)
   refuses before `render_context_section_measured` runs, so it has nothing to report and used
   to pass a literal `0`. That is wrong by most of the budget on this slice's own fixture — a
   100 023-byte dependency against a 4096-token window leaves `3 × (4096 − 256 − 2) = 11 514`
   bytes to render into, and the refusal is that even ALL of them are under the 25 006 the 25%
   floor demands. (Stated as that bound rather than as a survivor count: a survivor count is
   precisely what this arm cannot know.) "0 of 100 023 survive" reads as a total loss and sizes
   the operator's remedy against a number nothing measured. The planner arm now states what it
   established instead: of the cuts that FIT, none clears the floor.

   **A non-positive budget and an over-budget authored half are the GATE's refusal, not this
   one** (§5.3, AC5): no cut fits, so the un-cut prompt goes to selection and the operator gets
   the per-candidate diagnosis instead of a false floor report. A test for this AC must key on
   the `context budget: ` prefix, not on the window figure, which BOTH refusals carry — that
   substring is why the pre-existing guard test kept passing when its refusal changed owner.
10. All four disclosure channels fire on a degraded turn: the DISPATCHED prompt carries the
    truncation marker, `ContextBudgeted` is journaled, the node output carries
    `context_budgeted: true` alongside an unchanged `text` key, and the warn is emitted — its
    figures agreeing with the journal row, plus `requested_bytes`, which the row does not carry
    and which is what makes the warn a measurement of the RATIO the floor is a guess at.
    Asserted together in one test (`a_degraded_turn_discloses_on_every_channel`), because each
    channel is individually cheap to break without the others noticing.

    **The `N of M dependencies shown` tail is a FIFTH signal on the same channel, not part of
    this one, and an earlier wording of this AC conflated them.** The marker announces that an
    entry was TRUNCATED; the tail announces that whole entries were DROPPED. They fire on
    different degradations and a one-dependency fixture cannot produce the second at all
    (`dropped_deps == 0`), so demanding both in one test would be demanding something no single
    turn need exhibit. The tail is pinned by
    `a_context_section_that_drops_dependencies_says_how_many`.
11. An in-window turn's PROMPT is **byte-identical** to today, and nothing durable changes: no
    `ContextBudgeted` journaled, no `context_budgeted` key on the output, no warn, and the same
    number of provider calls carrying the same bytes. **And it stays byte-identical on every
    later drive, even if the window shrinks under it** — the memo is the fence for that half
    (§4.1), so no new journal row is needed and this AC is unweakened.

    **Deliberately NOT claimed: that nothing new runs.** An earlier draft of this AC said "the window
    accessor is not even called", which is false by construction — deciding whether a prompt is
    over-window requires knowing the window, so `max_context_window(chain)` is read on EVERY agent
    turn, in-window ones included. That read is a `self.config.read().await` returning a `u32`, no
    allocation and no I/O, and it is the honest cost of the feature. Writing the stronger claim would
    have made this AC unfalsifiable-looking while being trivially false, which is the specific defect
    review has caught in this project's specs three times now.
12. `FORMAT_VERSION` is still 1, and a journal written before this slice loads and folds unchanged.

## 7. The guard test this slice must change, and why that is honest

`oversized_dependency_context_halts_over_budget_never_truncates`
(`executor/tests.rs:4817-4872`) asserts, unqualified:

```rust
assert!(
    !out.outputs.contains_key(&NodeId("B".into())),
    "and B produced NO output — the point of halting is that half a document never \
     becomes work product, whichever class the halt has"
);
```

Under this slice that node completes. The assertion inverts, and this is the strongest recorded
objection to the whole design — it is a semantic change to a guard test, not doc drift.

Resolution: **split the test rather than relax it.** Its invariant survives where it matters.
- The existing name and fixture are resized so the retained context falls BELOW the floor, so it
  still asserts a loud halt and no output — the "half a document never becomes work product"
  property, now pinned at the floor rather than at the window.
- A new sibling asserts that a MODERATELY over-window prompt completes with all four disclosures.

`PromptParts::join`'s doc and `render_context_section_bounded`'s doc both argue the model path must
never truncate ("a model is never silently asked about half a document"). The operative word is
**silently**, and §5.5 is the answer to it: four channels, one of them durable. Both docs must be
rewritten to say so rather than left contradicting the code.

## 8. What changes for an operator

An over-window agent turn that used to pause now answers, and says so — in the prompt, in the
journal, on the node's output, and in the log. A turn degraded past the floor still pauses, still
force-wakeable, with a reason naming the window and, when the section was actually rendered, how
much context survived; when the floor was unreachable before any render, the reason says that
instead of inventing a survivor count (AC9).

The new risk is the one the four channels exist to manage: a degraded answer is still an answer, and
a consumer that ignores `context_budgeted` will treat it as a full one.

## 9. Deferred

- **Transcript compaction** (`messages`), including the master spec's "restart from a summarized
  checkpoint" (`2026-08-06-sensei-orchestrator-design.md:202`). Needs the §4.5 clock-dependence
  addressed first, and it is a different mechanism.
- **Summarization via a model call.** Two shapes, both mapped:
  - *Produce time*, on `ContextWrite.summary` — a field that ALREADY round-trips
    journal→fold→store (`journal.rs:229`, folded `support.rs:148-162`, rehydrated by `insert_ref`)
    with only the READ missing (`resolve_context` calls `ctx.load(&r)` unconditionally at
    `mod.rs:1648` and never consults `r.summary`). This is the only determinism story needing no
    qualifier at all. It is budget-blind by construction — the producer cannot know its reader's
    budget — and `ContextStore::put` is a LOUD `ContextKeyCollision` on re-write
    (`orchestrator-store/src/postgres.rs:378-398`), so a summary cannot be revised later.
  - *Consume time*, as a sixth `dispatch_metered` producer at a reserved `{node}/__compact__`
    sub-path, modelled on `SelectorDispatch` (`dispatch.rs:1152-1359`). Owes the module's standing
    three tests (budget gate, redaction leaf, memo replay) — the selector shipped with only the
    first and the review found exactly the defect the missing memo guard would have caught.
- **Blackboard design D5 is FALSE in code** — it claims each dependency contributes its `summary` if
  present. Nothing populates it (`rg 'summary: Some' crates/` → zero hits) and nothing reads it.
  Recorded here because it was found while scoping this slice.
- **`Message.attachments`** in the estimator and the truncator.
- **A hook for `ContextBudgeted`.** Consistent with the standing carry-forward that no hook fires for
  the signal/gate/loop-gate events either.
- **`Gateway`'s catalog in the version fence.** §4.2 shows `GatewayConfig` has no version field, so
  a model's `context_window` is covered by neither the fence nor any hash. This slice routes AROUND
  that by journaling the budget; it does not close it, and every other window-derived decision
  remains exposed.
