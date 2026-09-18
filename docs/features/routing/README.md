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
| [Provider routing preferences](provider-preferences.md) | Implemented (SP-ROUTE-1) | `crates/gateway/src/strategy.rs`, `crates/gateway/src/gates/routing_policy.rs` | per-request `sort` / `only` / `ignore` / `order`; price-weighted default within equal-priority groups; `InferenceResponse::routing` |
| [Fallback chains](fallback-chains.md) | Implemented | `crates/gateway/src/engine.rs` | priority walk; `FallbackTrigger` continue-vs-break |
| [Circuit breaker](circuit-breaker.md) | Implemented | `crates/gateway/src/circuit_breaker.rs` | per-`router:model`; in-memory |
| [Connection cooldown](connection-cooldown.md) | Implemented (Phase 1 · SP-0) | `crates/gateway/src/gates/cooldown.rs` | router-level skip on connection faults |
| [Model lockout](model-lockout.md) | Implemented (Phase 1 · SP-0) | `crates/gateway/src/gates/lockout.rs` | per-reason cooldowns + escalation + classification |
| [Quota demote-to-tier](quota-demote-to-tier.md) | Implemented (Phase 1 · SP-0) | `crates/gateway/src/engine/execute.rs` | quota falls over instead of terminating; `resume_after` |
| Resilience config | Implemented (Phase 1 · SP-0 (f); extended by SP-ROUTE-1) | `crates/gateway/src/resilience.rs` | `ResilienceConfig` / `Gateway::with_resilience`: tunable durations, bounded eviction, deterministic jitter, and (SP-ROUTE-1) `min_samples` / `perf_samples` / `perf_window` |

## Notes

- The three health gates (breaker / cooldown / lockout) are distinct granularities of the same idea — skip a candidate that cannot currently succeed — and run in the shared admission-gate / health-recorder pipeline. SP-ROUTE-1 adds the `RoutingPolicyGate`, which is **not** a fourth health gate: it pairs with no `HealthRecorder`, its `gate_status()` is `Structural`, and it therefore contributes no deadline to `AllGated.resume_after` — an over-narrow `only` is a terminal `NoCandidates`, not a pause. It is registered **first** in the admission-gate vector so a caller's own `only`/`ignore` exclusion is the reason reported for a multiply-gated candidate. That vector holds **seven** gates in all — routing policy, capability, connection cooldown, circuit breaker, model lockout, budget, context window — and `RoutingPolicyGate` is the seventh to be added, not the fourth of four.
- **Ordering is a seam, not a sort.** Since SP-ROUTE-1 the candidate order comes from a `RoutingStrategy` resolved **per request** (`selection.rs::strategy_for`), not from a bare `sort_by_key(priority)`. The default is price-weighted within equal-priority groups and is a no-op on any chain whose priorities are distinct — see [provider routing preferences](provider-preferences.md) for the determinism claim and the two ways a tie can arise unintentionally.
- SP-0 touches the same hot path as issue #39 (engine.rs/selection.rs refactor); sequence together.
- Gate state is in-memory/per-process today; a future seam can persist it for multi-instance sharing.
- **SP-0 (health gates) is complete.** The gates are operator-tunable via `ResilienceConfig` / `Gateway::with_resilience` (cooldown/lockout durations, a bounded eviction cap, deterministic per-endpoint jitter); defaults reproduce the prior hardcoded behavior exactly. Deferred beyond SP-0 (planned, NOT implemented): a calendar-clock exact quota reset boundary (the fixed ~1h default is a self-correcting approximation), an opaque `EndpointKey` (the `router:model` string key is used throughout), and open `.with_gate` / `.with_recorder` composition hooks (added when an external consumer needs a custom gate/recorder).
