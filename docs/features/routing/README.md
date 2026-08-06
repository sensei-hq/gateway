---
title: Routing — Module Reference
doctype: module
module: routing
status: partial
---

# Routing

How a request becomes a provider call, and how the candidate walk stays resilient:
router/model selection, fallback chains, and the health gates that skip
unavailable candidates (circuit breaker + connection cooldown + model lockout).

## Status

| Feature | Status | Source | Notes |
|---|---|---|---|
| [Routing & selection](routing-and-selection.md) | Implemented | `crates/gateway/src/selection.rs` | 3 routing modes; `api_model_id` resolution |
| [Fallback chains](fallback-chains.md) | Implemented | `crates/gateway/src/engine.rs` | priority walk; `FallbackTrigger` continue-vs-break |
| [Circuit breaker](circuit-breaker.md) | Implemented | `crates/gateway/src/circuit_breaker.rs` | per-`router:model`; in-memory |
| [Connection cooldown](connection-cooldown.md) | Planned (Phase 1 · SP-0) | `crates/gateway/src/selection.rs` | router-level skip on connection faults |
| [Model lockout](model-lockout.md) | Planned (Phase 1 · SP-0) | `crates/gateway/src/selection.rs` | per-reason cooldowns + escalation + classification |
| [Quota demote-to-tier](quota-demote-to-tier.md) | Planned (Phase 1 · SP-0) | `crates/gateway/src/engine.rs` | quota falls over instead of terminating; `resume_after` |

## Notes

- The three health gates (breaker / cooldown / lockout) are distinct granularities of the same idea — skip a candidate that cannot currently succeed — and all live in `validate_chain_entry`.
- SP-0 touches the same hot path as issue #39 (engine.rs/selection.rs refactor); sequence together.
- Gate state is in-memory/per-process today; a future seam can persist it for multi-instance sharing.
