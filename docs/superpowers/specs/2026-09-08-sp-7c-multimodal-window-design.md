# SP-7c — multimodal window correctness

**Status:** design, awaiting approval
**Slice:** SP-7c (after SP-7a window-aware selection, SP-7b context budgeting, SP-DOC-1)
**Scope chosen by the user:** the estimator and the gate only. No producer, no new feature surface.

---

## 1. Summary

`estimate_input_tokens_pessimistic` does not count `Message::attachments` at all. Every candidate
gate decision on a multimodal request is therefore made on the request's TEXT alone, and the
per-candidate `ContextWindowGate` can admit a candidate whose window the images push past — a
request the system has declared fits, which the provider then rejects.

This slice charges each attachment a declared per-image token cost, so the gate's input is an upper
bound on the whole request rather than on its text.

---

## 2. The premise, stated honestly

**The hole is real but LATENT, and the slice is scoped on that basis.**

- Four adapters translate attachments to provider-native shapes and put them on the wire:
  `anthropic/convert.rs`, `openai_compat/convert.rs`, `gemini.rs`, `bedrock/convert/request.rs`.
  The capability is shipped.
- **No production code in this workspace ever populates `attachments`.** Every one of the 20
  `with_attachment` call sites is a test. `executor/agent.rs` and `executor/dispatch.rs` both pass
  `Vec::new()`, and `dispatch.rs` says so outright ("attachments omitted from both because no
  orchestrator producer populates them").

So no orchestrator run can reach this today. What can reach it is any consumer of the `gateway`
library — it is a `[lib]` crate, and the adapters' multimodal support is part of its published
behaviour. A shipped capability whose window guard is known-wrong is a defect whether or not this
repo's own callers exercise it; the first caller to attach an image inherits the bug rather than
discovering it.

**This is recorded because the scope depends on it.** A latent hole justifies fixing the estimator
and the gate. It does NOT justify building a producer, and this slice does not.

---

## 3. Goals / non-goals

**Goals**

- The gate stops admitting candidates whose window a request's attachments would exceed.
- `estimate_input_tokens_pessimistic`'s contract becomes an upper bound on a `Chat` request, not
  on its text — and the doc says so, replacing the paragraph that currently records the gap.
- A future media kind cannot be silently priced at zero.

**Non-goals**

- No orchestrator producer. `attachments` stays empty on every executor path; a run's behaviour is
  byte-identical.
- No per-provider cost model. One pessimistic number, not four.
- No image decoding, dimension probing, or network fetch. The estimator stays pure and sync.
- No change to the cost estimator (`estimate_input_tokens`). It answers a different question and
  wants the opposite bias; SP-7b's §"Why a third estimator" argument applies unchanged.

---

## 4. The decisions, and why

### D1 — A declared per-attachment constant, not a measurement

The only quantity available in-process is the `MediaSource` string, and the existing doc already
rules both shapes out:

- `Base64`: over-counts by two to three orders of magnitude (a 1 MB image is ~1.4 M base64 bytes →
  ~466 k "tokens" at `/3`, against a per-image cost providers publish in the low thousands).
- `Url`: its length has no relationship to the cost at all.

Dimensions would give a real model, and we cannot have them: a `Url` is not fetched (the provider
fetches it), and decoding a `Base64` image to read its header would make a pure, sync, allocation-
light function do image parsing on the hot path — and still fail on the `Url` case. **A constant is
the only honest option**, so the question is which constant, not which formula.

### D2 — The constant is the documented high-res maximum: 4784 tokens

Anthropic publishes a per-image ceiling that moves with the vision tier:

| Tier | Long edge | Max tokens/image |
|---|---|---|
| Opus 4.6 and earlier, Sonnet 4.6 | 1568 px | ~1600 |
| Opus 4.7 / 4.8 / Opus 5, Sonnet 5 (high-res) | 2576 px | **~4784** |

The estimator must not under-count for ANY candidate in a chain, and a chain routinely mixes
tiers, so the bound is the largest published figure: **4784 tokens per attachment**. That also
covers the other three adapters' providers, whose published per-image costs sit below it.

Named `MAX_TOKENS_PER_ATTACHMENT`, with the table above beside it, so the next person to change it
knows what it is a bound on. It is deliberately a ceiling and not an average: the failure this
slice fixes is admitting a request that does not fit, and only a ceiling prevents that.

### D3 — Charged in TOKENS, after the divide

The existing doc already prescribes the shape — "the term belongs in tokens, added after the
divide, not in bytes before it" — and it is right: the `/3` divisor is a bytes→tokens heuristic for
TEXT, and running a per-image token figure back through it would silently divide the ceiling by
three. The attachment term is added to the token total the text arithmetic produces.

### D4 — Exhaustive match, no wildcard arm

`MediaAttachment` has exactly one variant today (`Image`). The count is written as an exhaustive
`match` rather than `attachments.len() * K`, so adding an `Audio` or `Document` variant fails to
compile here. Pricing a new media kind at zero by omission is precisely the defect this slice
exists to remove; the compiler should refuse to let it recur.

### D5 — The `Stt` precedent bounds the risk, and is not breached

The `Stt` arm refuses to invent a number because an estimate so large that every candidate is
skipped turns a serviceable request into a terminal `AllGated`. That failure mode is the reason to
check this decision, not a reason to reject it: at 4784 tokens, ten images cost ~48 k tokens —
comfortably inside every current model's window, and inside the 200 k of the smallest. The
over-count is bounded and proportional, unlike the base64 shape it replaces.

Direction of the residual error is stated plainly: a small image on a low-res-tier model is
over-counted by roughly 3×. That biases toward routing to a larger-window candidate — the safe
direction for a gate whose job is to not put an over-window request on the wire.

---

## 5. Architecture

One function changes: `estimate_input_tokens_pessimistic` (`crates/gateway/src/engine/util.rs`),
`Payload::Chat` arm only. Every other arm is untouched.

```
tokens = ceil(text_bytes / 3)                     // unchanged
       + Σ over messages, Σ over attachments      // NEW
             match att { Image { .. } => MAX_TOKENS_PER_ATTACHMENT }
```

Saturating arithmetic on the add: the function returns `u32` and already saturates at the
`u32::try_from(..).unwrap_or(u32::MAX)` boundary. A pathological attachment count must clamp, not
wrap — a wrapped total would under-count, which is the one direction the contract forbids.

**No caller changes.** The `ContextWindowGate` and the SP-DATA-5 budget clamp both consume this
function's output and both inherit the fix. The clamp's soundness argument is unaffected in
direction: the estimate rises, so `remaining − est_input` falls, so `max_tokens` is bounded more
conservatively. SP-7b's `plan_budget` also consumes it via the same chokepoint.

**Doc surgery.** The "What it does NOT count" section currently records this gap as a decision. It
is replaced by a statement of what IS counted and why the constant is what it is; the old text is
quoted and marked superseded, per the amendment convention.

---

## 6. Acceptance criteria

1. **AC1 — an attachment is counted.** `estimate_input_tokens_pessimistic` over a `Chat` payload
   with one image returns the text estimate plus `MAX_TOKENS_PER_ATTACHMENT`.
2. **AC2 — counted per attachment, across messages.** N attachments spread over M messages cost
   `N × MAX_TOKENS_PER_ATTACHMENT`.
3. **AC3 — the source shape does not change the charge.** A `Url` and a `Base64` attachment cost
   the same. Pinned explicitly, because charging by string length is the failure mode being
   removed, and a 1 MB base64 blob must not price differently from a short URL.
4. **AC4 — text-only is byte-identical.** A payload with no attachments returns exactly what it
   returns today. This is the no-regression guard for every existing caller.
5. **AC5 — the gate refuses what it used to admit.** A candidate whose window the text fits but the
   text-plus-images does not is now SKIPPED. Asserted through the gate, not the estimator, since
   admitting an over-window candidate is the defect and the estimator is only its cause.
6. **AC6 — saturating, not wrapping.** An attachment count large enough to overflow `u32` clamps to
   `u32::MAX`.
7. **AC7 — the executor is unchanged.** The full workspace suite is unchanged in count and result;
   no orchestrator path constructs an attachment, so no run behaves differently.

Every AC is mutation-verified: each must redden on a stated one-line mutation of the source.

---

## 7. What changes for an operator

Nothing, today — no run can construct an attachment. For a `gateway` library consumer that does:
a multimodal request that previously reached a provider and failed there is now either routed to a
candidate whose window holds it, or refused by the gate before any spend, with the same
per-candidate diagnosis and `UseLargerContextWindow` remedy every other over-window refusal
carries.

---

## 8. Deferred

- **A real per-image cost model** (dimensions, tiles, per-provider formulas). Needs image decoding
  or a provider-side count endpoint; the ceiling is correct and cheap until a caller's cost profile
  makes the over-count matter.
- **Tier-aware ceilings** — charging 1600 against a low-res-tier candidate and 4784 against a
  high-res one. The gate is per-candidate, so the plumbing exists; it trades a bounded over-count
  for a per-model table that must track model releases. Not worth it before a real producer exists.
- **An orchestrator producer.** Making agent turns carry media is a feature, and it touches the
  prompt assembler, `agent_input_hash` determinism, the journal shape, and redaction. Its own slice.
- **`Stt` and the non-`Chat` arms.** Unchanged; they answer in a different unit or not at all.
