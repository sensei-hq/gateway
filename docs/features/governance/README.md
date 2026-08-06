---
title: Governance — Module Reference
doctype: module
module: governance
status: partial
---

# Governance

Cost and quota control: token metering + budget filtering, subscription-tier
quotas, and the Phase-1/4 enhancements that add live usage metering, proactive
credential expiration tracking, and predicted-exhaustion lockout.

## Status

| Feature | Status | Source | Notes |
|---|---|---|---|
| [Budget & cost](budget-and-cost.md) | Implemented | `crates/gateway/src/engine.rs` | `ModelPricing`, estimate/actual cost, budget filter |
| [Subscription quota](subscription-quota.md) | Implemented | `crates/kernel/src/types/config.rs` | tier quotas, `check_quota` pre-flight |
| [Usage metering](usage-metering.md) | Planned (Phase 4 · SP-DATA) | data-tier | counters vs free/paid limits, reset windows |
| [Expiration tracking](expiration-tracking.md) | Planned (Phase 1 · SP-0) | gateway | proactive oauth/subscription/credits/free-tier-reset alerts |
| [Predicted lockout](predicted-lockout.md) | Planned (Phase 4 · SP-DATA) | data-tier + selection | pre-emptive lockout from usage trend, not just reactive 429 |

## Notes

- Reactive limit handling (lockout/cooldown) lives in [routing](../routing/README.md); this module is the *accounting* side that feeds it.
- Usage metering is stateful → data-tier (Phase 4); the pure gateway core stays stateless.
