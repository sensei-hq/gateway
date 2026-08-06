---
title: Observability — Module Reference
doctype: module
module: observability
status: implemented
---

# Observability

What a caller and an operator can see: the per-attempt trace on every request,
and the consumer-implemented persistence seam that durably records calls/traces.

## Status

| Feature | Status | Source | Notes |
|---|---|---|---|
| [Tracing & attempts](tracing-and-attempts.md) | Implemented | `crates/kernel/src/types/request.rs` | `Attempt`/`AttemptStatus`; full trail on success, `attempts_detail` on failure |
| [Persistence store](persistence-store.md) | Implemented | `crates/gateway/src/store.rs` | `GatewayStore` trait; best-effort metering |

## Notes

- `GatewayStore` is best-effort (a store error is logged, not propagated). This is deliberate for *metering*; the orchestrator's durable journal is the opposite (strict) — see [orchestrator/durable-journal](../orchestrator/durable-journal.md).
- Orchestrator-level progress (per-agent hooks) is separate — see [orchestrator/hooks](../orchestrator/hooks.md).
