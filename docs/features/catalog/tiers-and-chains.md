---
title: Tiers & Chains
doctype: feature
module: catalog
status: implemented
phase: 1
spec: SP-CAT
source: crates/gateway/src/catalog/{tiers,assemble}.rs
---

# Tiers & Chains

> **Status: Implemented (Phase 1 · SP-CAT).** Design §6.2/§12.2 (decision D13).

**Tiers and chains are orthogonal axes that compose.** A **tier** is a named
catalog segment — curated or attribute-derived from `auth_type` / cost band /
capability / `free_type` / locality — with an **intra-tier routing strategy**
(`headroom` / `least-used` / `cost` / `priority`). A **chain** is an ordered
list of **tier-refs** (or concrete models).

```
premium-reasoning {auth=oauth/cli, capability=reasoning}  → priority
cost-optimized    {cost=low, throughput=high}             → headroom/least-used
fallback-specialty{local | specialty}                     → priority
free              {free_type != none}                     → fill-first/headroom

plan.frontier = [premium-reasoning → cost-optimized]
research.bulk = [free → cost-optimized → fallback-specialty]
```

**Resolution split (keeps the gateway core pure):** static tier *membership*
expands into ordered candidates at config-assembly (`catalog::assemble`);
cross-segment fallover uses the existing trigger logic. Intra-tier *ordering* is
where the config-driven boundary shows: `priority` and `cost` are pure functions
of config and are live today, but the dynamic strategies `headroom`/`least-used`
need live lockout + usage state — i.e. the **persistence layer SP-CAT
deliberately holds off**. Those two therefore **stub to `priority` and emit a
`tracing::warn!`** (never a silent degrade; callers can also detect it via
`IntraTierStrategy::is_dynamic`). They are **not** functional as live-usage
orderings yet.

## Scenarios

```gherkin
Feature: Tiers & chains
  Scenario: A chain expands its tier-refs into ordered candidates
    Given chain research.bulk = [free, cost-optimized]
    And the free tier has models [f1, f2] and cost-optimized has [c1]
    When candidates are assembled
    Then the ordered candidates are [free models…, then cost-optimized models…]

  Scenario: Adding a model to a tier updates every chain that references it
    Given model f3 is added to the free tier
    Then research.bulk includes f3 without editing the chain

  # DEFERRED: headroom/least-used need live usage (persistence, held off); today
  # they stub to `priority` with a warning. See Notes.
  Scenario: Intra-tier strategy orders within a segment at request time
    Given the cost-optimized tier uses the headroom strategy
    Then within that segment the model with the most remaining quota is tried first

  Scenario: Attribute-derived membership picks up a matching model
    Given the premium-reasoning tier is derived by {auth=oauth, capability=reasoning}
    And a new oauth reasoning model is added to the catalog
    Then it becomes a member of premium-reasoning automatically
```

## Notes

- Implemented as pure config → config: `CatalogConfig` (tiers + tier-ref chains)
  → `catalog::assemble` → concrete `GatewayConfig` chains, in memory. Curated ∪
  derived membership and ordering live in `crates/gateway/src/catalog/tiers.rs`;
  chain expansion (dedup + ascending priority, fails loud) in
  `crates/gateway/src/catalog/assemble.rs`. The canonical worked example is the
  `worked_example_free_tier_research_chain` test.
- **`headroom`/`least-used` are deferred:** they need live usage state from the
  held-off persistence layer, so they currently stub to `priority` with a
  `tracing::warn!`. `priority` and `cost` are live.
- Supersedes the earlier flat "tiers = chains" shorthand.
- Reuses torii's `routing_policies` / `chain_models` / `chain_bindings`.
