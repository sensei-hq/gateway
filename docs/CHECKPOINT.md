# Checkpoint

**SP-ROUTE-1 brainstorm COMPLETE — design spec committed, awaiting review.** On `develop`
(`2d18fb3`, docs-only). Spec:
`docs/superpowers/specs/2026-09-17-sp-route-1-provider-routing-preferences-design.md`.

## The slice

An OpenRouter-shaped provider-routing surface on `InferenceRequest`: `sort`
(price/latency/throughput), `only`/`ignore` filters, explicit `order`, and price-weighted
uptime-aware selection as the **default**. Lands on `RoutingStrategy` (`strategy.rs:5`) — the
seam SP-0 reserved for exactly this.

## Decisions (§3, D1–D8)

Per-request not per-chain · all four knobs · separate `routers`/`models` lists · weighted-random
IS the default **but scoped to equal-priority groups** · in-memory rolling window for metrics ·
mid-stream failure fix folded in · preferences deliberately do **not** reach orchestrator nodes
(they would have to join `input_hash` or a resume replays a memo from the old policy).

## Three facts that stopped it being a literal transplant

1. Chain entries are different **models**, not interchangeable providers of one model — so
   whole-chain inverse-square weighting inverts authored intent. Hence equal-priority grouping;
   and since `assemble()` reassigns strictly distinct priorities (`assemble.rs:145-154`), the new
   default is **byte-identical on every chain that exists today**.
2. `estimate_cost` returns `None` for unpriced models (`selection.rs:169`) and the repo is full of
   free local ones ⇒ `1/cost²` undefined. Free-first is that formula's limit taken honestly.
3. `format!("{router}:{model}")` **cannot be parsed back** (model ids contain colons), ruling out
   a flat selector namespace.

## Defect found + folded in (D7/§7, AC9)

Streaming `dispatch_outcome(success=true)` fires the instant a stream is **obtained**
(`stream.rs:230`), and the mid-stream error path (`stream.rs:250-259`) returns **without
dispatching** — so a stream that dies halfway is recorded to every health recorder as a *success*.
Fixed here because §5.1 weights on reliability. Accepted change: mid-stream failures now count
toward the breaker for the first time.

**Next:** `superpowers:writing-plans` over the spec, once the user approves it.

## State

Open questions: none blocking. Known-broken: nothing — docs-only, suite untouched, pre-commit
fmt + clippy green. Prior checkpoint pointed at SP-7b/`build`; that is **complete and on `main`**
(PR #54/#59) — stale state, not abandoned work.
