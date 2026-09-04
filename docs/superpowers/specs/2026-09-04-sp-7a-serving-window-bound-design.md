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

Let `S = { c in chain : c.context_window >= est }` — the set `ContextWindowGate` admits.

- Selection can only return a member of `S` (the gate skips every non-member).
- `min_serving_context_window` returns `min { c.context_window : c in S }`.
- For any `c in S`, `c.context_window >= min(S) `, so bounding output by `min(S) − est` leaves at
  least that much room in every admissible candidate.
- Every member satisfies `window >= est`, so `min(S) − est >= 0` — the saturation that produced the
  bug cannot occur.

`S` empty ⇒ `None` ⇒ no window term, and selection will admit nothing, so the request never reaches
a provider regardless.

**The one coupling this creates, stated plainly:** the bound's soundness depends on the gate
actually being registered and using the same estimate. If the gate were removed, the clamp would
bound by a window belonging to a model that could then be selected without fitting. That is a real
coupling between two crates, and it is why the alternative "bound by the chain's largest" was
rejected — this version at least degrades to *over*-bounding rather than under-bounding, because
`min(S) >= min(chain)` for the entries that remain.

## 5. What the operator sees, and what changes about it

`BelowFloor` carries `window: Option<u32>`, and its doc says a `Some` means the refusal "cannot be
cleared by any cap at all". That was true when the term was the chain minimum, and it is **false**
after this change: a window that binds now means the request fits *some* model and the reply will
simply be short.

So the wording is rewritten rather than rewired. A window-bound clamp is no longer a dead end, and
the message must not tell an operator it is. Where the request genuinely fits nothing, the message
comes from `AllGated` instead and names each candidate's window.

## 6. Acceptance criteria

1. `min_serving_context_window(chain, est)` returns the smallest window **at or above** `est`;
   `None` when no entry qualifies; `None` for an unknown chain.
2. It never returns a window below `est` — proven on a chain whose entries straddle the estimate.
3. **A BUDGETED run on `[big 128k, small 8k]` with a ~20 k prompt SUCCEEDS**, selecting `big`. This
   is SP-7a's AC1, which passes unbudgeted and fails budgeted today — the slice's whole point.
4. The clamp's emitted `max_tokens` on that run is at or below `big`'s window minus the estimate.
5. A budgeted run whose prompt exceeds EVERY window is refused by the **gate** (`AllGated`, naming
   a window), not by the clamp's `BelowFloor`.
6. An unbudgeted run is byte-identical — the clamp does not run.
7. A homogeneous chain (all windows equal) behaves exactly as before, since `min(S) == min(chain)`
   whenever every entry qualifies.
8. `BelowFloor`'s window wording no longer claims the refusal cannot be cleared by any cap, and no
   test asserts the old wording.
9. The output-limit term (`min_max_output_tokens`) still applies independently — a serving window
   larger than the model's output limit does not widen `max_tokens`.

## 7. Deferred

- Moving the clamp downstream of selection, for a bound on the ACTUAL selected model (clamp spec
  §8; this slice makes the gap smaller but does not close it).
- SP-7b context budgeting, SP-7c semantic activation, the M1 reversal.
- Teaching the gate and the clamp to share one estimate value rather than each computing its own —
  they agree today because both use the pessimistic estimator, but nothing enforces it.
