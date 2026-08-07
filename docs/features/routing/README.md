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
| [Connection cooldown](connection-cooldown.md) | Implemented (Phase 1 · SP-0) | `crates/gateway/src/gates/cooldown.rs` | router-level skip on connection faults |
| [Model lockout](model-lockout.md) | Implemented (Phase 1 · SP-0) | `crates/gateway/src/gates/lockout.rs` | per-reason cooldowns + escalation + classification |
| [Quota demote-to-tier](quota-demote-to-tier.md) | Implemented (Phase 1 · SP-0) | `crates/gateway/src/engine/execute.rs` | quota falls over instead of terminating; `resume_after` |
| Resilience config | Implemented (Phase 1 · SP-0 (f)) | `crates/gateway/src/resilience.rs` | `ResilienceConfig` / `Gateway::with_resilience`: tunable durations, bounded eviction, deterministic jitter |

## Notes

- The three health gates (breaker / cooldown / lockout) are distinct granularities of the same idea — skip a candidate that cannot currently succeed — and run in the shared admission-gate / health-recorder pipeline.
- SP-0 touches the same hot path as issue #39 (engine.rs/selection.rs refactor); sequence together.
- Gate state is in-memory/per-process today; a future seam can persist it for multi-instance sharing.
- **SP-0 (health gates) is complete.** The gates are operator-tunable via `ResilienceConfig` / `Gateway::with_resilience` (cooldown/lockout durations, a bounded eviction cap, deterministic per-endpoint jitter); defaults reproduce the prior hardcoded behavior exactly. Deferred beyond SP-0 (planned, NOT implemented): a calendar-clock exact quota reset boundary (the fixed ~1h default is a self-correcting approximation), an opaque `EndpointKey` (the `router:model` string key is used throughout), and open `.with_gate` / `.with_recorder` composition hooks (added when an external consumer needs a custom gate/recorder).
