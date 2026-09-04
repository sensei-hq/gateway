---
title: SP-7a follow-on — bound the clamp by the window that can actually serve the request
doctype: design-spec
module: gateway + orchestrator
slice: SP-7a-serving-window
status: approved
date: 2026-09-04
---

# SP-7a follow-on — the serving-window bound

## 1. Summary

**SP-7a's benefit does not reach a budgeted run**, and the reason is a bound SP-DATA-5 introduced
one day earlier.

The SP-DATA-5 clamp sets `max_tokens` before dispatch, so it must pick a value safe for whichever
candidate selection eventually picks. It bounds by `min_context_window(chain) − est` — the
**smallest window in the whole chain**. On SP-7a's own AC1 example — chain `[big 128k, small 8k]`,
a 20 k-token prompt — that is `8192.saturating_sub(20000) = 0`, which falls under
`MIN_OUTPUT_TOKENS` and refuses with `BudgetRefusal::BelowFloor` **inside the orchestrator, before
`Gateway::execute` is called**. The new `ContextWindowGate` never runs.

So the clamp and the gate disagree about the same request, and the clamp wins because it is
upstream. Unbudgeted runs get SP-7a; budgeted runs get the old failure with a budget-flavoured
message.

The fix is one bound: the clamp should reason over the candidates that **can serve the request**,
which is exactly the set the gate admits.

## 2. Goals / Non-goals

**Goals**
- `Gateway::min_serving_context_window(chain, est)` — the smallest `context_window` among a chain's
  models whose window is at least `est`.
- The clamp's window term becomes `min_serving_context_window(chain, est) − est`.
- When nothing can serve the request, the clamp contributes **no window term** and the refusal
  becomes the gate's, with per-candidate diagnostics.
- `BelowFloor`'s window wording is rewritten: a window that binds now means *shorter replies*, not
  *unreachable*.
- Additive: an unbudgeted run is untouched (the clamp does not run at all).

**Non-goals**
- **Moving the clamp downstream of selection.** That is the most precise answer and it is the
  clamp spec's §8 wording, but it needs the run's budget plumbed into the gateway, and the budget
  is orchestrator state the gateway deliberately knows nothing about. §3 records why the
  serving-set bound gets the same safety without that.
- SP-7b (context budgeting), SP-7c (semantic activation), the M1 reversal. Unchanged.

## 3. The decisions, and why

| Decision | Choice | Why |
|---|---|---|
| What the clamp bounds by | **The smallest window among candidates that can serve `est`** | The gate admits a candidate only when `window >= est`, so the minimum over THAT set is safe for whichever member selection picks — without knowing which. It is also never negative by construction, which is the specific way the chain-minimum fails: `8192 - 20000` saturates to `0` and trips the floor. The two components stop disagreeing because they are finally reasoning over the same candidate set. |
| Nothing can serve it | **No window term; the gate refuses** | This is the handover SP-7a was built for. The clamp's `BelowFloor` is budget-flavoured and names a cap raise; the doc on that field already admits "the window arm cannot be cleared by any cap at all". Keeping the early refusal would leave two components refusing one condition in two vocabularies, and the one that fires first is the one that misdirects. One failure, one owner. |
| Not the chain's LARGEST window | **Rejected** | Swapping `min` for `max` is a one-call change and is *only* safe because the gate happens to filter the small models out first. It would silently depend on gate ordering and on the gate being registered at all — and if either changed, the clamp would send a big model's `max_tokens` to a small one, which is the provider-400 class this whole line of work exists to prevent. |
| Not moving the clamp post-selection | **Deferred, not rejected** | It is strictly more precise, and it stays in the clamp spec's §8. But it requires the budget to cross a boundary that currently separates the two crates cleanly, and the serving-set bound delivers the same safety property for this defect. |

## 4. Why the bound is sound

Let `S = { c in chain : c.context_window >= est }` — the set `ContextWindowGate` admits — **where
`est` is ONE value that the clamp and the gate both use.** That premise is not decoration; §4.1
records that it shipped violated and how it is now met.

- Selection can only return a member of `S` (the gate skips every non-member, on the same `est`).
- `min_serving_context_window` returns `min { c.context_window : c in S }`.
- For any `c in S`, `c.context_window >= min(S) `, so bounding output by `min(S) − est` leaves at
  least that much room in every admissible candidate.
- Every member satisfies `window >= est`, so `min(S) − est >= 0` — the saturation that produced the
  bug cannot occur.

`S` empty ⇒ `None` ⇒ no window term, and selection will admit nothing, so the request never reaches
a provider regardless.

**The coupling this creates, stated plainly:** the bound's soundness depends on the gate actually
being registered. If it were removed, the clamp would bound by a window belonging to a model that
could then be selected without fitting. That is a real coupling between two crates, and it is why
the alternative "bound by the chain's largest" was rejected — this version at least degrades to
*over*-bounding rather than under-bounding, because `min(S) >= min(chain)` for the entries that
remain.

### 4.1 Correction — the shared `est` was NOT a detail, and the first version shipped without it

Everything above says `est`. The implementation gave the two halves two different numbers, and the
whole-slice review found it: `dispatch::est_input_tokens` applied `ceil` **per string** over
CHARACTERS and summed; `gateway::engine::util::estimate_input_tokens_pessimistic` sums BYTES and
applies `ceil` **once**. On ASCII `Σ ceil(Lᵢ/3) >= ceil(Σ Lᵢ/3)`, so the clamp's figure was the
larger one and `S_clamp` was a strict SUBSET of the set selection drew from. The first bullet above
was therefore false, and so was the `S`-empty handover — "nothing can serve it" was being decided
on a number the gate did not use.

Two reachable failures, both reproduced end to end, both ending in
`prompt + max_tokens > context_window` — a provider 400 that arrives as a terminal `NodeFailed` on
a budgeted run, on a call an unbudgeted run serves (unbudgeted omits `max_tokens` from the wire):

1. **`S_clamp` empty while `S_gate` is not.** On the homogeneous 4096 chain — the one AC7 calls
   unchanged — a 600-message payload estimating 4200 per-string and 3800 in total left the clamp
   with nothing able to serve the request, so it contributed no window term at all and bounded
   `max_tokens` by the 1024 output limit alone. The gate then admitted the model at 3800 and the
   request went out asking for 4824 against a 4096 window. **Strictly worse than the defect this
   slice fixed**: the parent commit refused the same request with a recoverable pause.
2. **`S_clamp` missing the small candidate `S_gate` keeps.** On `[small 4096 pri-1, big 200 000]`
   the same payload put `min(S_clamp) = 200 000` while selection returned `small` — a big model's
   `max_tokens` sent to a small one, which is exactly the outcome §3's decision row rejects the
   `max` alternative for, reached by another route.

**Fixed by making the two one function, not by aligning two.** `dispatch::est_input_tokens` and its
own last dependency `agent::prompt::est_tokens_pessimistic` are DELETED;
`estimate_input_tokens_pessimistic` is `pub` and the clamp calls it on the very `Payload` it is
about to dispatch, which is the same payload `Gateway::execute` estimates. Aligning the formulas by
hand was rejected: it leaves an invariant between two crates that nothing enforces, and the drift
ran the OTHER way on multi-byte text (bytes there, chars here), so neither figure bounded the other
and no slack constant could have covered it.

Two side effects worth recording. The clamp's `est` is now over BYTES, which is more margin
everywhere and much more where the old figure was weakest (CJK is 3 bytes/char and tokenizes near
1 token/char — `chars/3` under-counted it threefold). And AC10 below is the criterion this
correction adds.

## 5. What the operator sees, and what changes about it

`BelowFloor` carries `window: Option<u32>`, and its doc says a `Some` means the refusal "cannot be
cleared by any cap at all (the window term is `min_context_window(chain) − est`, which never reads
the cap)".

So the wording is rewritten rather than rewired. A window that BINDS THE CEILING is no longer a
dead end — it now usually just means a shorter reply, and the run proceeds. Where the request
genuinely fits nothing, the message comes from `AllGated` instead and names each candidate's window.

### Correction, made during implementation

This section originally said the "cannot be cleared by any cap" claim becomes **false** after the
change, and AC8 was written from that. Checked against the code, it does not:

- The window term is floor-checked at `ceiling < MIN_OUTPUT_TOKENS`, and `ceiling` is
  `min(min_max_output_tokens, window_term)`. Neither reads the cap. So a `BelowFloor` carrying
  `window: Some(_)` still arrives identically at a cap of 1e6 and at `u64::MAX`.
- What the section is really describing — "a window that binds means the reply will simply be
  short" — is the case where the window term binds the ceiling *above* the floor. That path emits a
  smaller `max_tokens` and never reaches this message at all.

What IS false after the change, and what the rewrite therefore fixes:

1. the parenthetical naming `min_context_window(chain) − est`, which is no longer the term;
2. "the chain's smallest context window of {window}" in the operator message — it is now the
   smallest window that can HOLD the input;
3. the remedy "route to a chain whose smallest model has a larger window", which on a heterogeneous
   chain points at a model already filtered out of the decision;
4. the implicature that a `Some` means the prompt does not fit. It now means the opposite: the
   prompt fits (`w >= est` by construction) and the shortfall is OUTPUT room. The fits-nothing case
   has moved to the gate.

**AC8 is amended accordingly** (see §6). Deleting the cap sentence outright would have shipped a
new false comment in place of an old one, and would have cost an operator a manual
`BudgetRaised` + `force_wake` round trip to rediscover it.

## 6. Acceptance criteria

1. `min_serving_context_window(chain, est)` returns the smallest window **at or above** `est`;
   `None` when no entry qualifies; `None` for an unknown chain.
2. It never returns a window below `est` — proven on a chain whose entries straddle the estimate.
3. **A BUDGETED run on `[big 128k, small 8k]` with a ~20 k prompt SUCCEEDS**, selecting `big`. This
   is SP-7a's AC1, which passes unbudgeted and fails budgeted today — the slice's whole point.
4. The clamp's emitted `max_tokens` leaves the prompt room inside the window of the candidate that
   actually WON. **Amended** — it read "at or below `big`'s window minus the estimate", and on the
   AC3 fixture that is `emitted <= 191 808` against an output limit of 1024, which no
   implementation can fail. The review mutation-proved the vacuity: dropping the `− est` term, and
   even folding the accessor with `.max()` instead of `.min()`, left it green. The criterion now
   requires a fixture where the WINDOW term is the binding ceiling on the winning candidate, and is
   carried by `the_serving_window_bound_is_safe_for_the_smallest_candidate_the_gate_admits`
   (`est + emitted == 4096` exactly, on the run that lands on `small`), which reddens under both
   mutations. The AC3 fixture keeps only what it can falsify: the routing, and
   `emitted == max_output_tokens`.
5. A budgeted run whose prompt exceeds EVERY window is refused by the **gate** (`AllGated`, naming
   a window), not by the clamp's `BelowFloor`.
6. An unbudgeted run is byte-identical — the clamp does not run.
7. A homogeneous chain (all windows equal) behaves exactly as before, since `min(S) == min(chain)`
   whenever every entry qualifies.
8. `BelowFloor`'s window wording no longer describes the bound as the CHAIN's smallest window, no
   longer offers "route to a chain whose smallest model has a larger window", and states that the
   input FITS the window it names; no test asserts the old wording. **Amended** from "no longer
   claims the refusal cannot be cleared by any cap" — see §5's correction: that claim is still
   true, and the message keeps it. Both negatives are asserted: the review found the remedy clause
   unpinned (the two forbidden strings share no substring, so restoring the old remedy verbatim
   left the suite green) and it now has its own assertion, mutation-proven.

   > **Corrected again, 2026-09-04, by this slice's own whole-slice review — the REPLACEMENT remedy
   > was false in the same way as the remedy it replaced.** It read "send less input, or put a
   > model with a larger window in this chain". The term is `min { w ∈ chain : w >= est }`, and
   > **adding an element to a set cannot raise its minimum**: a larger entry leaves the bound
   > exactly where it was, and a smaller one that still holds the input LOWERS it. Demonstrated
   > rather than argued by
   > `adding_a_larger_model_to_the_chain_cannot_clear_a_serving_window_refusal`, which drives one
   > prompt down the `{4096}` chain and the `{4096, 200 000}` chain and gets **byte-identical**
   > refusals. The message now names the guaranteed remedy — remove or replace the entry it names,
   > which strictly raises the minimum of the serving set (or empties it, handing the refusal to
   > the gate with per-candidate diagnostics) — states that adding a larger model alongside cannot
   > help, and qualifies "send less input" as conditional on that same entry staying the smallest
   > that can hold the prompt.
   >
   > The cap-independence sentence this AC chose to KEEP was itself unpinned by the same move:
   > shifting the two-cap drive onto the gate path left the surviving `BelowFloor` arm at one cap,
   > and a mutation gating the refusal on `remaining` passed all 427 orchestrator tests, as did
   > deleting the clause. Now pinned by `a_serving_window_refusal_is_unmoved_by_the_cap` (1e6 and
   > `u64::MAX / 2`, compared with the ledger clause stripped), which is the only test of the 427
   > that catches either mutation.
9. The output-limit term (`min_max_output_tokens`) still applies independently — a serving window
   larger than the model's output limit does not widen `max_tokens`. A TIE between the two terms
   counts as window-bound, so the refusal keeps the window's wording; that rule is stated in the
   clamp's comment and was unguarded, and is now pinned by
   `a_tie_between_the_output_limit_and_the_window_term_names_the_window` (which needs a fixture
   whose output limit is under the floor, since a tie only becomes observable through a refusal).

   > **Corrected by review, 2026-09-04:** naming the window is the right CLASSIFICATION and was not
   > a sufficient MESSAGE. With both terms on the same sub-floor figure, clearing the window half
   > leaves the output half binding — so on a tie *both* of the remedies the window arm offers fail,
   > and this AC's own justification ("it tells the operator the two things that do move it") was
   > wrong twice over. The tie now additionally names the chain's smallest declared
   > `max_output_tokens` as a co-cause, carried by `output_limit_ties` on `BelowFloor` and asserted
   > in the same test. The deferred THIRD arm — a refusal where the output limit binds alone — is
   > unchanged and still reaches the operator in budget wording.
10. **The clamp and the gate judge one request by one `est`.** Added by §4.1's correction. Asserted
    across the crate boundary rather than as an equality between two functions, because there is
    only one function now: for a dispatched call,
    `gateway_est(request) + max_tokens <= context_window(model dispatched to)`, with `gateway_est`
    measured by the gateway's own estimator on the request the provider received.

## 7. Deferred

- Moving the clamp downstream of selection, for a bound on the ACTUAL selected model (clamp spec
  §8; this slice makes the gap smaller but does not close it). **The citation was DANGLING until
  the release gate.** §2 and §3 above, the SP-7a selection spec's §8, `orchestrator-overview.md`,
  `executor/agent.rs` and `engine/mod.rs` all name this "the clamp spec's §8 item/wording" — but
  the clamp spec's §8 did not carry it, and its §5.2 did not describe a window term at all: the
  term arrived in that slice's OWN whole-slice review, after its spec was written, and nothing went
  back to the spec. So five files pointed a reader at an item that was not there, which is worse
  than no pointer — it reads as "already recorded, someone else's problem", which is how a residual
  survives three slices unexamined. The item is now in the clamp spec's §8 with its provenance, and
  that §5.2 describes both model terms and this slice's change to the window one.
- SP-7b context budgeting, SP-7c semantic activation, the M1 reversal.
- **A sub-floor `min_max_output_tokens` still renders as the BUDGET arm.** `binding_window`
  discriminates the window term from the budget, and there are two model bounds: when the OUTPUT
  limit is what lands under `MIN_OUTPUT_TOKENS`, `window` is `None` and the operator is told to
  raise a cap that the output term does not read either. Reachable — `collect_validation_errors`
  Rule 5 rejects only `max_output_tokens == 0` — and left because closing it means a THIRD message
  arm with a third remedy ("drop that entry, or raise its declared limit"), which is a wording
  decision this review did not ask for. The field's doc names it as a known misdirection rather
  than claiming to discriminate every model bound.
- Deleting `Gateway::min_context_window`. The clamp was its last production caller, so it now has
  none. Kept deliberately: it is a `pub` read accessor on a library type, removing it is a breaking
  change for no gain, and its own test asserts the number the serving bound stopped using — which
  is how a silent revert to the chain minimum stays visible. Its doc says all of this so the next
  reader does not have to re-derive it.

### Removed from this list: "teach the gate and the clamp to share one estimate value"

It was deferred with this reason: *"they agree today because both use the pessimistic estimator,
but nothing enforces it. They do at least agree in the safe DIRECTION: the gateway's estimator adds
tool schemas the clamp's omits, so the gate's figure is never smaller, and a request the clamp
thinks nothing can serve is one the gate skips too."*

**Every clause of that was false, and it is the sentence that let the Critical in §4.1 ship.**

- The clamp's estimator did NOT omit tool schemas. `dispatch::est_input_tokens` summed
  `est(name) + est(description) + est(schema.to_string())` over every tool, and its own doc said so
  in its first sentence.
- The direction was inverted. Measured on this repo's own `tool_agent_registry` fixture the clamp
  said 60 and the gateway said 59; on a 600-message ASCII payload, 4200 against 3800. The two
  formulas differ in ROUNDING (per string vs once) and in UNIT (chars vs bytes), and those push
  opposite ways — per-string rounding makes the clamp's figure larger on Latin text, byte counting
  makes the gateway's larger on multi-byte text. **Neither figure bounded the other in either
  direction.**
- "A request the clamp thinks nothing can serve is one the gate skips too" is the precise inverse of
  what happens: the clamp's serving set empties FIRST, and the gate admits models in the gap.

So this was never a tidy-up; it was a prerequisite for §4's soundness argument, and it is now
CLOSED rather than deferred (§4.1). Recorded here rather than deleted because a deferral list is
where the next author looks to decide whether a residual is benign, and "we checked and it was
safe" is the most expensive kind of wrong entry to leave there.

## 8. What changed for an operator, in one line

A budgeted run's over-window failure used to be a durable `RunPaused { resume_after: None }`
carrying budget wording; on a prompt nothing in the chain can serve it is now a terminal
`NodeFailed` carrying the gate's per-candidate diagnosis and `HumanAction::UseLargerContextWindow`.
That is a recoverable outcome traded for a terminal one, and it is the right trade because the
pause was preserving a run against a remedy it could not name — `torii run wake --budget-tokens N`
has never made a prompt fit a window. Recorded here because it is the one user-visible regression
in the slice.
