---
title: Reference Chains
doctype: feature
module: catalog
status: implemented
phase: 2
spec: SP-REF
source: crates/gateway/src/catalog/presets.rs
---

# Reference Chains

> **Status: Implemented (Phase 2 · SP-REF).** Builds on [Tiers & chains](tiers-and-chains.md)
> (SP-CAT): the reference tiers/chains are pure preset **data** consumed by the
> existing `catalog::assemble` — no change to SP-0 selection or `assemble`
> itself. Design `docs/superpowers/specs/2026-08-07-reference-chains-design.md`
> (§2 tiers, §3 chains, §4 API, §5 demo, §6 runnable).

**Reference chains are portable, attribute-derived presets** — four named tiers
and three named chains — so tests and the upcoming orchestrator have named,
runnable chains without every deployment reinventing them. Membership is derived
purely from model attributes (`free` / `cost_band` / `locality` / `auth_type` /
`tags`), so a catalog joins a tier by **tagging its models**, never by editing a
curated id list. Everything here is pure config (no I/O, no persistence).

## The four reference tiers

Each tier is **derive-only** (no curated members); membership resolves from the
attribute predicate. See [`reference_tiers`](../../../crates/gateway/src/catalog/presets.rs).

| Tier | Predicate (derived) | Intra-tier strategy |
|---|---|---|
| `free` | any declared free tier (`free_type != none`) | `priority` |
| `cost-optimized` | `cost_band = Low` | `cost` (cheapest-first) |
| `fallback-specialty` | `locality = Local` | `priority` |
| `premium-reasoning` | `auth_type = OauthCli` **and** tag `"reasoning"` | `priority` |

The `premium-reasoning` predicate is an **AND**: a model must be both OAuth-CLI
*and* carry the `"reasoning"` tag (an OAuth-CLI model without the tag, or a
tagged model on a different auth mechanism, is excluded).

## The three reference chains

Each chain is authored purely as **tier-refs**, capability `TextChat`, with the
standard fallback triggers (`rate_limit`, `timeout`, `provider_error`). See
[`reference_chains`](../../../crates/gateway/src/catalog/presets.rs).

| Chain | Tier order | Intent |
|---|---|---|
| `research.bulk` | `free → cost-optimized → fallback-specialty` | free-tier "research team": burn free allowances first, then cheap paid, then the local fallback |
| `plan.frontier` | `premium-reasoning → cost-optimized` | frontier reasoning for planning, cheap paid as the fallback |
| `code.exec` | `cost-optimized → premium-reasoning` | cheap execution first, frontier reasoning when it matters |

`assemble` flattens each chain's tier-refs (in order) into a concrete
`FallbackChainConfig`: members are expanded per tier, deduped by model id (first
occurrence wins), and assigned ascending 1-based `priority` by final position —
exactly the shape the SP-0 selector already consumes.

## The preset API

All four functions live in
[`crates/gateway/src/catalog/presets.rs`](../../../crates/gateway/src/catalog/presets.rs)
and return plain config values:

- `reference_tiers() -> HashMap<String, TierConfig>` — the four tiers above.
- `reference_chains() -> HashMap<String, TierChain>` — the three chains above.
- `with_reference_tiers_and_chains(routers, models, constraints) -> CatalogConfig`
  — drop the reference tiers + chains onto operator-supplied routers/models. The
  reference ids are a base; a caller can post-hoc override by editing the result.
- `demo_catalog() -> CatalogConfig` — a small, illustrative catalog (below).

## The illustrative demo catalog

`demo_catalog()` instantiates the presets with four representative tagged
models — a **runnable starting template, not a fixed roster**. The models and
routers are meant to be edited: a real deployment supplies its own catalog, and
because tier membership is attribute-derived, the reference chains pick the new
models up automatically.

| Model | Router | Attributes | Joins tier |
|---|---|---|---|
| `llama3.1-local` | `ollama` (keyless, `http://localhost:11434`) | `locality = Local` | `fallback-specialty` |
| `groq-llama-free` | `groq` (BYOK, `GROQ_API_KEY`) | keyless free tier | `free` |
| `deepseek-chat` | `deepseek` (BYOK, `DEEPSEEK_API_KEY`) | low pricing → `cost_band = Low` | `cost-optimized` |
| `claude-code` | `claude-cli` (OAuth-CLI) | `auth_type = OauthCli`, tag `"reasoning"` | `premium-reasoning` |

`assemble(demo_catalog())` therefore expands, for example:

- `research.bulk` → `[groq-llama-free, deepseek-chat, llama3.1-local]`
- `plan.frontier` → `[claude-code, deepseek-chat]`

The runnable example (`demo_reference_chain_drives_runnable_local_fallover` in
`presets.rs`) registers an adapter for the local `ollama` router only, then
executes `research.bulk`: the walk falls over `groq-llama-free` (no adapter) →
`deepseek-chat` (no adapter) → `llama3.1-local` (served) — proving the assembled
reference chain drives real SP-0 selection and fallover down to the runnable
local model.

## Behaviour

```gherkin
Feature: Reference chains expand and stay portable

  Scenario: A reference chain expands its tier-refs into concrete candidates
    Given the demo catalog's tagged models and the reference tiers/chains
    When I assemble the catalog
    Then research.bulk expands to [groq-llama-free, deepseek-chat, llama3.1-local]
    And each candidate is assigned ascending priority by position
    And plan.frontier expands to [claude-code, deepseek-chat]

  Scenario: Adding a tagged model updates every chain with no chain edit
    Given the demo catalog
    When I add a second OAuth-CLI, "reasoning"-tagged model
    And I re-assemble the catalog without editing any chain
    Then plan.frontier now includes the new model alongside claude-code
```

## Deferred

Consistent with SP-CAT, the **live-usage intra-tier strategies**
(`headroom` / `least-used`) still stub to `priority` and emit a `tracing::warn!`
— they need live lockout + usage state that lives in the deliberately held-off
**persistence layer** (SP-DATA, Phase 4). The reference presets and demo catalog
are pure in-memory config builders; nothing here is persisted.
