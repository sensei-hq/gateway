---
title: SP-ROUTE-1.2 — streaming parity for metering and explanation
doctype: design-spec
module: gateway
slice: SP-ROUTE-1.2
status: draft
date: 2026-09-19
closes: SP-ROUTE-1 carry-forwards 3, 4, 5
---

# SP-ROUTE-1.2 — streaming parity for metering and explanation

## 1. Why this exists

Three SP-ROUTE-1 carry-forwards are the same defect wearing three hats: **the streaming path was
built before the metering and observability concerns existed, and never caught up.** All three
live in one function, `Gateway::execute_stream`.

The shape is unusually clear, because the fix for a *sibling* concern is already in the file with
its reasoning written out — and the three open items sit directly beneath it, unfixed.

`stream.rs:356-361`, on the performance dispatch:

> MUST run before `yield StreamEvent::Done` below, not after: in an `async_stream` generator, code
> placed after a `yield` runs only on the NEXT poll, and a real consumer that stops polling once it
> sees the terminal `Done` (as any sane one does) would never resume this generator far enough to
> run a dispatch placed after it — the verdict would silently vanish.

Fifty lines below that comment, `insert_inference_call` runs **after** the yield.

The same comment explains why throughput must be measured from `attempt_start` rather than
`stream_start`, because `execute` writes the same key with the whole call's wall time and pooling
two unlike spans into one mean is what SP-ROUTE-1 §6.3 forbids. `InferenceCall.duration_ms` on
`stream.rs:400` uses `stream_start`.

## 2. The three

### 2.1 A streamed request may never be metered at all

`insert_inference_call` (`stream.rs:415`) is placed after `yield StreamEvent::Done`
(`stream.rs:408`). A consumer that stops polling on the terminal event — which is the normal shape
for an SSE handler, and what `collect_stream` in the test suite does **not** do, because it drains
to `None` — never resumes the generator, so the row is never written.

This is billing data. A non-streaming request is always metered; a streamed one is metered only if
the caller happens to over-poll.

**Fix:** move the write above the `yield`, exactly as the performance dispatch already is. The
comment that justifies it is already in the file.

### 2.2 Persisted streaming durations are not comparable with unary ones

`InferenceCall.duration_ms` uses `stream_start.elapsed()` — generation time only, excluding
acquisition. `execute` writes the same column with the whole call's wall time. So the
`inference_calls` table mixes two quantities under one name, and any analytics over it is wrong in
a way nothing surfaces.

**Fix:** use `attempt_start.elapsed()`, matching both `execute` and the throughput decision this
file already made.

**A discontinuity this creates, accepted deliberately:** rows written before this slice carry
generation-only durations for streamed calls; rows after carry total. Analytics spanning the
boundary will see a step change. That is a one-time cost to end an ongoing one — the column is
analytics data rather than a fence, and leaving it means every future row stays wrong. Documented
in `upgrading.md` rather than silently absorbed.

### 2.3 A streamed request has no routing explanation

SP-ROUTE-1 Task 11 put a `RoutingDecision` on `InferenceResponse::routing` — the artifact a caller
holds. `execute_stream` returns a stream of `StreamEvent`s, so a streaming caller has no way to ask
why its provider was chosen. **Preferences apply**: filtering and ordering work identically. Only
the explanation is missing.

**Fix:** carry it on `StreamEvent::Done`, beside `model`, `tokens` and `cost` — the terminal event
already carries what-happened, and this is part of that.

This is a public enum change; any exhaustive match on `Done`'s fields breaks.

## 3. Why one slice and not three

They share a root cause, a file, and a function. Two of them are about the same question — which
span a duration measures — and the third's fix lands in the same terminal-event block the first
one moves. Splitting them means three sets of streaming fixtures and three review passes over the
same forty lines.

The remaining SP-ROUTE-1 carry-forward — consensus legs dropping routing preferences — is
deliberately **not** here. It is a different file, a different question (propagation through
request-building), and arguably not a defect at all: consensus legs already drop `budget` and
`auth`, so the exclusion is consistent and may be intended.

## 4. The testing problem this slice must solve first

**Every streaming test in the suite drains to `None`.** `collect_stream` loops
`while let Some(ev) = stream.next().await`, so no existing test behaves like a real SSE consumer,
and none of them can observe §2.1 at all. A fix verified only against `collect_stream` proves
nothing.

So this slice needs a consumer fixture that **stops polling at `Done`** — takes events until it
sees the terminal one, then drops the stream. That fixture is what makes §2.1's test real, and it
is the first thing to build.

This is the same lesson SP-ROUTE-1 learned twice: a fixture that does not behave like production
cannot test production. There, every performance fixture returned a constant and could not see a
live-store read inside a comparator.

## 5. Acceptance criteria

| # | Criterion |
|---|---|
| AC1 | A consumer that stops polling at `Done` still produces a metering row — asserted with a fixture that genuinely stops, not `collect_stream` |
| AC2 | Moving the write back below the `yield` makes AC1 fail (the fixture has teeth) |
| AC3 | A streamed call's persisted `duration_ms` includes the acquisition span, and is the same quantity `execute` records |
| AC4 | `StreamEvent::Done` carries the `RoutingDecision`, and it is the one selection produced — not a re-derivation |
| AC5 | The decision on `Done` matches what `execute` would report for the same request and chain |
| AC6 | An existing `collect_stream`-based test still passes unchanged — the fix is additive for over-polling consumers |
| AC7 | Suite green; clippy and fmt clean under both toolchains |

## 6. Out of scope

- Consensus legs dropping routing preferences (§3).
- Backfilling or migrating existing `inference_calls` rows (§2.2) — the discontinuity is
  documented, not repaired.
- Any change to what preferences *do* on the streaming path. They already work; only the
  explanation is absent.
