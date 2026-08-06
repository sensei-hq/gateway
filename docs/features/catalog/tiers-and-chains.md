---
title: Tiers & Chains
doctype: feature
module: catalog
status: planned
phase: 1
spec: SP-CAT
source: catalog + crates/gateway/src/selection.rs
---

# Tiers & Chains

> **Status: Planned (Phase 1 · SP-CAT).** Design §6.2/§12.2 (decision D13).

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
expands into ordered candidates at config-assembly; dynamic intra-tier
*ordering* (`headroom`/`least-used`) is chosen at request time from live lockout
+ usage state; cross-segment fallover uses the existing trigger logic.

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

  Scenario: Intra-tier strategy orders within a segment at request time
    Given the cost-optimized tier uses the headroom strategy
    Then within that segment the model with the most remaining quota is tried first

  Scenario: Attribute-derived membership picks up a matching model
    Given the premium-reasoning tier is derived by {auth=oauth, capability=reasoning}
    And a new oauth reasoning model is added to the catalog
    Then it becomes a member of premium-reasoning automatically
```

## Notes

- Supersedes the earlier flat "tiers = chains" shorthand.
- Reuses torii's `routing_policies` / `chain_models` / `chain_bindings`.
