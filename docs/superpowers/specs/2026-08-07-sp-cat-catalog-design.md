---
title: SP-CAT — Free-Tier Catalog + Tiers × Chains (design)
doctype: spec
spec: SP-CAT
phase: 1
status: approved
implements_feature_docs:
  - docs/features/catalog/free-tier-catalog.md
  - docs/features/catalog/tiers-and-chains.md
  - docs/features/catalog/catalog-refresh.md
related:
  - docs/superpowers/specs/2026-08-06-sensei-orchestrator-design.md   # §6.2, §12.2, D10/D13
  - docs/features/catalog/README.md
---

# SP-CAT — Free-Tier Catalog + Tiers × Chains

**Goal:** Make **free-tier metadata** and a **tiers dimension** first-class in the catalog, and let **chains compose tiers** — so "add a model to a tier once → every chain that references it updates," free-tier maximization and lockout fall-over come for free, and free-tier budgets are audited (pool-deduped) against documented totals. Phase-1 (`SP-CAT`) delivers this as **pure config + one transform**; the DB persistence / live-usage / config-versioning layer is Phase-4 (`SP-DATA`).

**Builds on:** the completed **SP-0 health gates** (breaker / cooldown / lockout / `AllGated{resume_after}` / `ResilienceConfig`) — SP-CAT feeds them the `free` tier and richer chains, but does **not** touch the selection/gate pipeline.

**Approved decisions (brainstorm 2026-08-07):**
1. **Full tier×chain in one plan** (metadata + totals + expander), not metadata-only.
2. **Membership = curated ∪ attribute-derived** (both).
3. **Authoring = a separate `CatalogConfig` → `assemble()` → runtime `GatewayConfig`** (strategos/torii `assembleConfig()` pattern).
4. **`cost_band` derived from pricing, with an explicit override.**
5. **Per-model catalog metadata extends `ModelConfig`** (optional, serde-defaulted).

---

## 1. Architecture — expansion as a pure config-assembly transform

```
CatalogConfig (authoring: routers · models[+CatalogMeta] · tiers · tier-ref chains)
     │  catalog::assemble(catalog) -> Result<GatewayConfig, AssembleError>   (pure, no I/O)
     ▼
GatewayConfig (runtime: routers · models · concrete FallbackChainConfigs)
     │  Gateway::new / try_new / update_config  (unchanged)
     ▼
SP-0 selection pipeline (unchanged — sees only concrete chains)
```

`assemble` expands each tier-ref in a chain into that tier's **ordered member models**, flattening to the concrete `FallbackChainConfig` the SP-0 selector already consumes. **The gateway core and the SP-0 gate/strategy pipeline are not modified.** Tiers/tier-refs are an *authoring convenience resolved before the gateway runs* (design D13: "static membership expands at config-assembly"). The **dynamic** intra-tier ordering (`headroom`/`least-used` from live usage) is the Phase-4 extension — SP-CAT ships the static half plus a documented strategy seam.

**Rejected alternative:** teaching `ModelSelectionService` to resolve tier-refs at request time — it would couple the pure selection path (just built in SP-0) to catalog concerns and re-introduce per-request expansion cost. Keep expansion at assembly.

**Layering:** per-model `CatalogMeta` lives in the **kernel** (on `ModelConfig`); `CatalogConfig` + the tier types + `assemble` + totals live in a new **gateway `catalog` module** (`crates/gateway/src/catalog/`). `CatalogConfig` may migrate to a shared location when SP-DATA's `config_loader` produces it from the DB; for SP-CAT it's gateway-owned.

## 2. Catalog metadata (per model) — extends `ModelConfig` (kernel)

`ModelConfig` gains an optional, serde-defaulted field (absent ⇒ today's behavior exactly):

```rust
// kernel::types::config
pub struct ModelConfig { /* …existing… */ #[serde(default, skip_serializing_if="Option::is_none")] pub catalog: Option<CatalogMeta> }

pub struct CatalogMeta {
    pub free: Option<FreeTier>,
    pub auth_type: Option<AuthType>,   // api_key | oauth_cli | keyless
    pub cost_band: Option<CostBand>,   // override; else derived from pricing (§4)
    pub locality: Option<Locality>,    // local | cloud
}
pub struct FreeTier {
    pub free_type: FreeType,
    pub monthly_tokens: Option<u64>,
    pub credit_tokens: Option<u64>,
    pub pool_key: Option<String>,      // shared-quota pool; counted once in totals
    pub tos: TosVerdict,               // ok | caution | ambiguous  (proxy/relay use)
    pub trains_on_prompts: bool,       // privacy cost surfaced next to the quota
}
pub enum FreeType { RecurringDaily, RecurringMonthly, RecurringCredit, RecurringUncapped, OneTimeInitial, Keyless, Discontinued }
pub enum TosVerdict { Ok, Caution, Ambiguous }
pub enum AuthType   { ApiKey, OauthCli, Keyless }
pub enum CostBand   { Free, Low, Mid, High }
pub enum Locality   { Local, Cloud }
```

All enums derive `Serialize/Deserialize/Debug/Clone/Copy/PartialEq/Eq`. Traceability: fields mirror `docs/features/catalog/free-tier-catalog.md`.

## 3. Tiers — definition + membership (curated ∪ derived)

```rust
// gateway::catalog
pub struct TierConfig {
    pub strategy: IntraTierStrategy,      // §5
    pub members:  Vec<String>,            // curated model ids
    pub derive:   Option<TierPredicate>,  // attribute predicate (AND); members = curated ∪ derived
}
pub struct TierPredicate {                // every present field must match (AND); absent = don't care
    pub free: Option<FreeMatch>,          // Any | Type(FreeType)
    pub auth_type: Option<AuthType>,
    pub cost_band: Option<CostBand>,      // compared against the model's derived/override band (§4)
    pub capability: Option<Capability>,
    pub locality: Option<Locality>,
}
pub enum FreeMatch { Any, Type(FreeType) }
```

**Membership resolution** (`fn tier_members(tier, models) -> Vec<&ModelConfig>`): the curated ids (validated to exist) **unioned** with every model satisfying `derive` (if present), **deduped by model id**, curated-first. Determinism: derived matches are appended in a stable order (sorted by model id) so assembly is reproducible (no `HashMap`-iteration nondeterminism — the SP-0 `#80` lesson).

## 4. Cost band — derived from pricing, override wins

`fn cost_band(model) -> CostBand`: if `model.catalog.cost_band` is `Some`, use it; else derive from `model.pricing`:
- no pricing ⇒ `Free`;
- else by a documented blended $/1k threshold table (e.g. `< $0.001 ⇒ Low`, `< $0.01 ⇒ Mid`, `else High`). Thresholds live in one constant table, documented in the feature doc, and unit-tested at the boundaries.

`TierPredicate.cost_band` compares against this resolved band.

## 5. Intra-tier routing strategy

```rust
pub enum IntraTierStrategy { Priority, Cost, Headroom, LeastUsed }
```
- **`Priority`** — curated order first, then derived (by model id). Functional in SP-CAT.
- **`Cost`** — ascending resolved cost band, then pricing, then id (stable tiebreak). Functional in SP-CAT.
- **`Headroom` / `LeastUsed`** — need **live usage** (Phase-4 metering). In SP-CAT they **stub to `Priority`** and `assemble` emits a `log`/warning that the requested dynamic strategy is not yet live (no silent pretense — the SP-0 "no silent failures" rule). The seam: dynamic ordering is applied at request time by a future `RoutingStrategy` reading the SP-DATA usage store; SP-CAT's static expansion is the fallback ordering.

Ordering applies **within** a tier's expanded members during assembly. Cross-tier order follows the chain's tier-ref order (tier A's members, then tier B's).

## 6. Chains compose tiers — `CatalogConfig` + `assemble`

```rust
pub struct CatalogConfig {
    pub routers: HashMap<String, RouterConfig>,
    pub models:  HashMap<String, ModelConfig>,     // each may carry CatalogMeta
    pub tiers:   HashMap<String, TierConfig>,
    pub chains:  HashMap<String, TierChain>,        // tier-ref chains (authoring form)
    pub constraints: ConstraintsConfig,             // passthrough (AUTH), unchanged
}
pub struct TierChain {
    pub capability: Capability,
    pub refs: Vec<ChainRef>,                        // ordered tier-refs and/or concrete models
    pub fallback_triggers: Vec<FallbackTrigger>,
}
pub enum ChainRef { Tier(String), Model(ChainEntry) }

pub fn assemble(catalog: CatalogConfig) -> Result<GatewayConfig, AssembleError>;
```

**`assemble`** produces a runtime `GatewayConfig` whose `chains` are concrete `FallbackChainConfig`s:
1. For each `TierChain`, walk `refs` in order; expand a `Tier(id)` into `tier_members(id)` ordered by the tier's strategy, and pass a `Model(entry)` through as-is.
2. Flatten to a single ordered `Vec<ChainEntry>`; assign ascending `priority` by position (so the SP-0 `PriorityStrategy` preserves assembly order); **dedupe** a model that appears via multiple tiers (keep first occurrence — highest-priority tier wins).
3. Copy `routers`, `models`, `constraints` through unchanged.

**`AssembleError`** (fail loud, never silently drop): `UnknownTierRef`, `UnknownModelInTier`, `EmptyTierAfterResolution` (a referenced tier with zero members — misconfig, surfaced not silently skipped), `UnknownModelRef`. Validation runs before producing config; mirrors `GatewayBuilder::validate`'s collect-all-errors style.

**Entry point:** a `CatalogBuilder`/`assemble` that yields a `GatewayConfig` for `Gateway::new`/`try_new`/`update_config`. Non-tier callers (existing `GatewayBuilder`) are untouched — SP-CAT is additive.

## 7. Free-tier totals + refresh (re-audit, not fetch)

`fn free_tier_totals(models) -> FreeTierTotals`:
- **Steady recurring** headline = sum of recurring free budgets, **pool-deduped**: models sharing a `pool_key` count that pool's budget **once**; `RecurringUncapped` and `OneTimeInitial`/`RecurringCredit` are **excluded** from the steady headline.
- **Reported separately** (never summed into the headline): one-time/initial credits, uncapped providers, discontinued.
- Fields: `{ steady_recurring_tokens, one_time_tokens, uncapped_providers: Vec<..>, pools: Vec<(pool_key, tokens)> }`.

**"Refresh" in SP-CAT = re-audit/validation**, NOT an external fetch: a `catalog::audit(catalog)` pass recomputes totals, validates pool-dedup consistency, and a **test/CI gate** fails the build if a documented headline (in the feature doc / a checked-in fixture) drifts from `free_tier_totals` (the `free-tier-catalog.md` "docs-counts" scenario). External catalog fetch/import is SP-DATA.

## 8. Persistence stance + what SP-CAT deliberately does NOT do

**The app stays config-driven — no persistence.** DB persistence is a **separate layer, deliberately held off** (user directive, 2026-08-07). SP-CAT and every near-term slice (reference chains, the orchestrator) run purely on config in memory: a hand-authored (or programmatically built) `CatalogConfig` → `assemble` → `GatewayConfig`, hot-swapped via `update_config`. The pre-existing optional seams (`GatewayStore`/`VaultStore`) stay **default-off** and are not a dependency of this work. SP-DATA (the DB `config_loader` + tracking) remains a distinct, later, optional layer — **not** a near-term follow-up — and, when it lands, it targets this same `assemble` seam without changing the pure core.

Deferred, therefore, out of SP-CAT (and out of the config-driven program until/unless persistence is explicitly taken up):
- **DB persistence + external fetch/import** (`config_loader`, staging import) — a separate later layer that would produce a `CatalogConfig` and reuse this same `assemble`.
- **Live-usage-driven `Headroom`/`LeastUsed`** — needs a usage/metering store (persistence); **stays stubbed to `Priority`** with a loud warning + seam for as long as the program is config-driven-only. This is not a silent gap: `assemble` warns when a tier requests a dynamic strategy.
- **`config_versioning` + the replay version-fence** — a persistence concern; later.
- **Proactive expiration tracking** (`free_tier_reset` detection from 429+reset, etc.) — stateful; later.

## 9. Testing & acceptance

- **Gherkins** from `docs/features/catalog/{tiers-and-chains,free-tier-catalog}.md` become the acceptance tests: chain expands tier-refs into ordered candidates; adding a model to a tier updates every chain; attribute-derived membership picks up a matching model; pool-dedup counts a shared pool once; one-time credits excluded from steady; uncapped listed-not-summed; CI fails on docs drift.
- **Determinism:** assembly is reproducible (derived members sorted by id; no `HashMap`-order dependence — pin with a repeated-run test, per `#80`).
- **Loud failure:** each `AssembleError` variant has a test (unknown tier/model ref, empty tier).
- **Backward-compat:** a `CatalogConfig` with no tiers + all-concrete chains `assemble`s to a `GatewayConfig` identical to hand-authoring it; `ModelConfig` with no `catalog` behaves exactly as today.
- **Stub honesty:** a `Headroom`/`LeastUsed` tier assembles (falls back to `Priority`) AND emits the not-yet-live warning.

## 10. Design boundaries / SRP

- **Pure core preserved:** `assemble` is a pure function (config→config, no I/O); the gateway never persists; selection/gates untouched.
- **Additive:** kernel `ModelConfig`/`GatewayConfig` grow optional/defaulted fields; the new `catalog` module is self-contained; existing `GatewayBuilder` callers keep working.
- **Reuse-ready:** `CatalogConfig` + `assemble` are the seam SP-DATA's `config_loader` targets — the DB layer builds a `CatalogConfig`, SP-CAT expands it. One transform, two producers (hand-authored now, DB later).
