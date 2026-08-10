# SP-CAT — Free-Tier Catalog + Tiers × Chains Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make free-tier metadata and a tiers dimension first-class in the catalog, and let chains compose tiers — as **pure config + one transform**, with **no persistence** (design: `docs/superpowers/specs/2026-08-07-sp-cat-catalog-design.md`). "Add a model to a tier once → every chain that references it updates"; free-tier budgets audited (pool-deduped); the SP-0 selection/gate pipeline is untouched.

**Architecture:** A separate authoring `CatalogConfig` (routers · models[+`CatalogMeta`] · tiers · tier-ref chains) is expanded by a pure `catalog::assemble(catalog) -> Result<GatewayConfig, AssembleError>` into a runtime `GatewayConfig` whose chains are concrete `FallbackChainConfig`s (what the SP-0 selector already consumes). Per-model metadata extends `ModelConfig` (kernel, optional/serde-defaulted). Tier membership = curated ∪ attribute-derived; `cost_band` derived from pricing (override wins); intra-tier `Priority`/`Cost` functional, `Headroom`/`LeastUsed` stub-to-`Priority` **with a loud warning** (no live usage — the app is config-driven, no persistence).

**Tech Stack:** Rust, `crates/kernel` (config types) + `crates/gateway` (new `catalog` module). Contract per commit: existing tests stay green (all new fields optional/defaulted ⇒ behavior-preserving); `cargo test --workspace` green; `make check` clean. New behavior proven by new tests + the feature-doc Gherkins.

**No persistence:** everything is in-memory config. `GatewayStore`/`VaultStore` stay default-off; the DB `config_loader` (SP-DATA) is a separate held-off layer that would later produce a `CatalogConfig` and reuse this same `assemble`.

---

## File Structure

- **Modify `crates/kernel/src/types/config.rs`** — `CatalogMeta` + `FreeTier` + enums (`FreeType`/`TosVerdict`/`AuthType`/`CostBand`/`Locality`); `ModelConfig.catalog: Option<CatalogMeta>` (serde-defaulted). [Task 1]
- **Create `crates/gateway/src/catalog/mod.rs`** (+ `pub mod catalog;` in `lib.rs`) — `cost_band` [T2]; `free_tier_totals`/`FreeTierTotals` [T3]; `TierConfig`/`TierPredicate`/`FreeMatch`/`IntraTierStrategy`/`tier_members`/`order_members` [T4]; `CatalogConfig`/`TierChain`/`ChainRef`/`assemble`/`AssembleError` [T5]. (Split into submodules — `cost.rs`, `totals.rs`, `tiers.rs`, `assemble.rs` — if the file grows; keep each focused.)
- **Modify `docs/features/catalog/{README,free-tier-catalog,tiers-and-chains,catalog-refresh}.md`** — status flip + traceability. [Task 6]

---

### Task 1: Catalog metadata on `ModelConfig` (kernel)

**Files:** `crates/kernel/src/types/config.rs`.

- [ ] **Step 1: Failing test** (in `config.rs` tests):
```rust
#[test]
fn model_config_catalog_defaults_absent_and_roundtrips() {
    // Absent ⇒ None (backward-compatible: existing configs deserialize unchanged).
    let json = r#"{"id":"m","provider":"p","capabilities":["TextChat"],"context_window":8000,"max_output_tokens":1000}"#;
    let m: ModelConfig = serde_json::from_str(json).unwrap();
    assert!(m.catalog.is_none());
    // Present ⇒ roundtrips.
    let meta = CatalogMeta {
        free: Some(FreeTier {
            free_type: FreeType::RecurringDaily, monthly_tokens: Some(60_000_000),
            credit_tokens: None, pool_key: Some("gemini-flash".into()),
            tos: TosVerdict::Ok, trains_on_prompts: true,
        }),
        auth_type: Some(AuthType::ApiKey), cost_band: None, locality: Some(Locality::Cloud),
    };
    let m2 = ModelConfig { catalog: Some(meta), ..m.clone() };
    let s = serde_json::to_string(&m2).unwrap();
    let back: ModelConfig = serde_json::from_str(&s).unwrap();
    assert_eq!(back.catalog.as_ref().unwrap().free.as_ref().unwrap().pool_key.as_deref(), Some("gemini-flash"));
    assert!(matches!(back.catalog.as_ref().unwrap().free.as_ref().unwrap().free_type, FreeType::RecurringDaily));
}
```
- [ ] **Step 2:** `cargo test -p sensei-kernel catalog` → FAIL.
- [ ] **Step 3: Implement** in `config.rs` (all `#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]`; the small enums also `Copy, Eq`):
```rust
pub struct CatalogMeta {
    #[serde(default, skip_serializing_if = "Option::is_none")] pub free: Option<FreeTier>,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub auth_type: Option<AuthType>,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub cost_band: Option<CostBand>,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub locality: Option<Locality>,
}
pub struct FreeTier {
    pub free_type: FreeType,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub monthly_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub credit_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub pool_key: Option<String>,
    pub tos: TosVerdict,
    #[serde(default)] pub trains_on_prompts: bool,
}
#[derive(Copy, Eq)] pub enum FreeType { RecurringDaily, RecurringMonthly, RecurringCredit, RecurringUncapped, OneTimeInitial, Keyless, Discontinued }
#[derive(Copy, Eq)] pub enum TosVerdict { Ok, Caution, Ambiguous }
#[derive(Copy, Eq)] pub enum AuthType { ApiKey, OauthCli, Keyless }
#[derive(Copy, Eq)] pub enum CostBand { Free, Low, Mid, High }
#[derive(Copy, Eq)] pub enum Locality { Local, Cloud }
```
  and add `#[serde(default, skip_serializing_if = "Option::is_none")] pub catalog: Option<CatalogMeta>,` to `ModelConfig`.
- [ ] **Step 4: Verify** — `cargo test -p sensei-kernel` green (existing + new); `cargo test --workspace` green (every existing `ModelConfig { .. }` literal still compiles — the field is defaulted; but struct literals need the field: **update existing `ModelConfig { .. }` literals across the workspace to add `catalog: None`**, OR — cleaner — since there are many, verify whether they use `..Default::default()`; if they're full literals, add `catalog: None`). Report how many literals you touched. clippy `-D warnings` + fmt clean.
- [ ] **Step 5: Commit:** `feat(kernel): optional CatalogMeta (free-tier + attributes) on ModelConfig`.

---

### Task 2: `catalog` module + `cost_band` (derive from pricing, override wins)

**Files:** create `crates/gateway/src/catalog/mod.rs`; `crates/gateway/src/lib.rs` (`pub mod catalog;`).

- [ ] **Step 1: Failing test** (in `catalog/mod.rs`):
```rust
#[test]
fn cost_band_derives_from_pricing_override_wins() {
    use kernel::types::config::{CostBand, ModelPricing};
    let free = model_with_pricing(None);                 // no pricing → Free
    assert_eq!(cost_band(&free), CostBand::Free);
    let low = model_with_pricing(Some(ModelPricing { input_per_1k: 0.0002, output_per_1k: 0.0006, per_request: None })); // blended 0.0008 < 0.002
    assert_eq!(cost_band(&low), CostBand::Low);
    let mid = model_with_pricing(Some(ModelPricing { input_per_1k: 0.0008, output_per_1k: 0.004, per_request: None }));  // blended 0.0048 < 0.02
    assert_eq!(cost_band(&mid), CostBand::Mid);
    let high = model_with_pricing(Some(ModelPricing { input_per_1k: 0.02, output_per_1k: 0.06, per_request: None }));    // 0.08 ≥ 0.02
    assert_eq!(cost_band(&high), CostBand::High);
    // Explicit override wins over derivation.
    let mut over = high.clone();
    over.catalog = Some(CatalogMeta { cost_band: Some(CostBand::Low), ..Default::default() });
    assert_eq!(cost_band(&over), CostBand::Low);
}
```
  (Add a `model_with_pricing` test helper + derive `Default` for `CatalogMeta` in Task 1 if not already — a `#[derive(Default)]` on `CatalogMeta` is fine since all fields are `Option`.)
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement** in `catalog/mod.rs`:
```rust
use kernel::types::config::{CostBand, ModelConfig};

/// Blended $/1k cost band. An explicit `catalog.cost_band` override wins; else
/// derive from `pricing` via `input_per_1k + output_per_1k`:
///   no pricing ⇒ Free; < 0.002 ⇒ Low; < 0.02 ⇒ Mid; else High.
/// Thresholds are documented in `docs/features/catalog/tiers-and-chains.md` and
/// boundary-tested.
pub fn cost_band(model: &ModelConfig) -> CostBand {
    if let Some(band) = model.catalog.as_ref().and_then(|c| c.cost_band) {
        return band;
    }
    match model.pricing.as_ref() {
        None => CostBand::Free,
        Some(p) => {
            let blended = p.input_per_1k + p.output_per_1k;
            if blended <= 0.0 { CostBand::Free }
            else if blended < 0.002 { CostBand::Low }
            else if blended < 0.02 { CostBand::Mid }
            else { CostBand::High }
        }
    }
}
```
  Add `pub mod catalog;` to `lib.rs`.
- [ ] **Step 4: Verify** — cost_band tests pass (incl. the boundary values 0.002 / 0.02 exactly → Mid / High respectively per `<`); `cargo test -p sensei-gateway --lib` + `cargo test --workspace` green; clippy/fmt clean.
- [ ] **Step 5: Commit:** `feat(gateway): catalog::cost_band — derive band from pricing (override wins)`.

---

### Task 3: `free_tier_totals` (pool-deduped) + audit gate

**Files:** `catalog/mod.rs` (or `catalog/totals.rs`).

- [ ] **Step 1: Failing tests** — the `free-tier-catalog.md` Gherkins:
```rust
#[test]
fn free_tier_totals_pool_dedup_and_exclusions() {
    // Two models share pool_key "gemini-flash" 60M each → pool counted once (60M).
    // A one-time-initial 25M → excluded from steady, reported as one_time.
    // A recurring-uncapped → listed (uncapped_providers), never summed.
    let models = /* HashMap of the above + a plain recurring-daily 10M no pool */;
    let t = free_tier_totals(&models);
    assert_eq!(t.steady_recurring_tokens, 60_000_000 + 10_000_000); // pool once + the standalone daily
    assert_eq!(t.one_time_tokens, 25_000_000);
    assert_eq!(t.uncapped_providers.len(), 1);
    assert!(t.pools.iter().any(|(k, v)| k == "gemini-flash" && *v == 60_000_000));
}
```
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement**:
```rust
pub struct FreeTierTotals {
    pub steady_recurring_tokens: u64,      // pool-deduped; excludes one-time + uncapped
    pub one_time_tokens: u64,              // one-time/initial credits (reported, not in steady)
    pub uncapped_providers: Vec<String>,   // model ids of recurring-uncapped (listed, never summed)
    pub pools: Vec<(String, u64)>,         // pool_key → budget counted once (sorted by key, deterministic)
}

/// Pool-deduped free-tier headline. Steady recurring = sum over recurring free
/// budgets, counting each `pool_key` once; `RecurringUncapped` and
/// `OneTimeInitial`/`RecurringCredit` are excluded from steady (reported
/// separately). Deterministic (sorted). See `docs/features/catalog/free-tier-catalog.md`.
pub fn free_tier_totals(models: &std::collections::HashMap<String, ModelConfig>) -> FreeTierTotals { /* … */ }
```
  Logic: iterate models with `catalog.free`; classify by `free_type`; for pooled recurring, record the pool budget once (first-seen or max within the pool — pick MAX and document it, so an under-declared duplicate can't shrink the pool); non-pooled recurring add their `monthly_tokens`; one-time/credit → `one_time_tokens`; uncapped → `uncapped_providers`; discontinued → ignored. Sort `pools`/`uncapped_providers` for determinism.
- [ ] **Step 4: Docs-drift gate** — add a test that loads a small checked-in example free-tier catalog fixture (`crates/gateway/src/catalog/testdata/example_free_catalog.json` — a handful of models) and asserts `free_tier_totals` matches a documented headline constant; the test FAILS if the two drift (the `free-tier-catalog.md` "docs-counts" scenario, at unit-test granularity — no external fetch).
- [ ] **Step 5: Verify** — totals tests + drift gate pass; `cargo test --workspace` green; clippy/fmt clean.
- [ ] **Step 6: Commit:** `feat(gateway): catalog::free_tier_totals — pool-deduped free-tier headline + drift gate`.

---

### Task 4: Tiers — membership (curated ∪ derived) + intra-tier ordering

**Files:** `catalog/mod.rs` (or `catalog/tiers.rs`).

- [ ] **Step 1: Failing tests**:
  - `tier_members`: a curated list resolves (unknown curated id → surfaced, see Step 3); a `derive` predicate matches by each attribute (`free`, `auth_type`, `cost_band`, `capability`, `locality`); curated ∪ derived is deduped (a model in both appears once, curated-first); membership is **deterministic** across repeated calls (sorted derived).
  - `order_members`: `Priority` keeps curated-first-then-derived order; `Cost` sorts by ascending `cost_band` then pricing then id; `Headroom`/`LeastUsed` produce the SAME order as `Priority` (stub) AND cause a warning (assert via a captured log or a returned "degraded" flag — see Step 3).
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement**:
```rust
pub enum IntraTierStrategy { Priority, Cost, Headroom, LeastUsed }
pub enum FreeMatch { Any, Type(kernel::types::config::FreeType) }
pub struct TierPredicate {
    pub free: Option<FreeMatch>,
    pub auth_type: Option<kernel::types::config::AuthType>,
    pub cost_band: Option<kernel::types::config::CostBand>,
    pub capability: Option<kernel::types::capability::Capability>,
    pub locality: Option<kernel::types::config::Locality>,
}
pub struct TierConfig { pub strategy: IntraTierStrategy, pub members: Vec<String>, pub derive: Option<TierPredicate> }

/// A model matches a predicate iff every present field matches (AND); absent = don't-care.
fn matches(pred: &TierPredicate, model: &ModelConfig) -> bool { /* free/auth_type/cost_band(via cost_band())/capability(contains)/locality */ }

/// Curated ids (in order, each must exist) ∪ derived matches (sorted by id),
/// deduped by id (curated wins). Returns the model ids in membership order.
pub fn tier_members<'a>(tier: &TierConfig, models: &'a HashMap<String, ModelConfig>) -> Result<Vec<String>, AssembleError> { /* … */ }

/// Order a tier's members. Priority = membership order; Cost = ascending band/price/id.
/// Headroom/LeastUsed require live usage (persistence, held off) → fall back to Priority
/// and log a warning (returned alongside so `assemble` can surface it — see Task 5).
pub fn order_members(ids: Vec<String>, models: &HashMap<String, ModelConfig>, strategy: IntraTierStrategy) -> Vec<String> { /* … */ }
```
  For the stub warning: `order_members` (or `assemble`) emits `tracing::warn!` for `Headroom`/`LeastUsed` ("dynamic intra-tier strategy X not live in config-driven mode; using Priority"). The unit test asserts the fallback ORDER equals Priority; a separate test asserts the warning fires (use a `tracing` capture, or return a `Vec<Warning>` from `assemble` that the test inspects — prefer the returned warnings so it's testable without a subscriber). Unknown curated id → `AssembleError::UnknownModelInTier` (defined in Task 5; if Task 5 isn't merged yet, define `AssembleError` here in Task 4 since `tier_members` needs it).
- [ ] **Step 4: Verify** — membership + ordering + determinism + stub-warning tests pass; `cargo test --workspace` green; clippy/fmt clean.
- [ ] **Step 5: Commit:** `feat(gateway): catalog tiers — curated∪derived membership + intra-tier ordering (Headroom/LeastUsed stub+warn)`.

---

### Task 5: `CatalogConfig` + `assemble` (tier-refs → concrete chains)

**Files:** `catalog/mod.rs` (or `catalog/assemble.rs`), `engine`/config entry as needed.

- [ ] **Step 1: Failing tests** — the `tiers-and-chains.md` Gherkins:
  - **Expansion:** `chains = { research.bulk: [Tier("free"), Tier("cost-optimized")] }`, free = [f1, f2], cost-optimized = [c1] → assembled `GatewayConfig.chains["research.bulk"].models` = concrete `[f1, f2, c1]` with ascending `priority`.
  - **Add-a-model-updates-every-chain:** add f3 to the free tier's curated list (or a derived match) → the assembled `research.bulk` includes f3 without editing the chain.
  - **Dedup cross-tier:** a model in two referenced tiers appears once (first/highest-priority tier wins).
  - **Concrete + tier mix:** `[Model(c_special), Tier("free")]` → `[c_special, free…]`.
  - **Loud failure:** unknown tier ref → `AssembleError::UnknownTierRef`; a tier that resolves to zero members → `AssembleError::EmptyTierAfterResolution`; unknown concrete model ref → `AssembleError::UnknownModelRef`.
  - **Backward-compat:** a `CatalogConfig` with no tiers and all-concrete chains `assemble`s to a `GatewayConfig` equal to the hand-authored equivalent; a plain model with no `catalog` is unaffected.
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement**:
```rust
pub struct CatalogConfig {
    pub routers: HashMap<String, kernel::types::config::RouterConfig>,
    pub models:  HashMap<String, ModelConfig>,
    pub tiers:   HashMap<String, TierConfig>,
    pub chains:  HashMap<String, TierChain>,
    pub constraints: kernel::types::config::ConstraintsConfig, // passed through to GatewayConfig
}
pub struct TierChain { pub capability: Capability, pub refs: Vec<ChainRef>, pub fallback_triggers: Vec<FallbackTrigger> }
pub enum ChainRef { Tier(String), Model(ChainEntry) }

#[derive(Debug)] pub enum AssembleError { UnknownTierRef(String), UnknownModelInTier { tier: String, model: String }, UnknownModelRef(String), EmptyTierAfterResolution(String) }
// impl std::fmt::Display + std::error::Error, collect-all-errors style (Vec) OR first-error; prefer collecting all (mirrors GatewayBuilder::validate).

pub fn assemble(catalog: CatalogConfig) -> Result<GatewayConfig, Vec<AssembleError>> { /* … */ }
```
  `assemble`: for each `TierChain`, expand `refs` in order (`Tier(id)` → `order_members(tier_members(id))`, `Model(e)` → validate `e.model` exists, pass through); flatten to `Vec<ChainEntry>` deduped by model id (first wins), assigning ascending `priority = position as u8`; build `FallbackChainConfig { id, capability, models, fallback_triggers }`; copy `routers`/`models`/`constraints`. Collect ALL `AssembleError`s before returning. Surface any `order_members` stub warnings (return them or log; a test asserts a `Headroom` tier assembles + warns). Optionally run the existing `validate_config` on the output and fold its errors in.
- [ ] **Step 4: Verify** — all expansion/dedup/error/backward-compat tests pass; the assembled `GatewayConfig` drives a real `Gateway::new(...).execute(...)` in one end-to-end test (a `research.bulk` chain of free models serves via the first, matching SP-0 selection). `cargo test --workspace` green; clippy/fmt clean.
- [ ] **Step 5: Commit:** `feat(gateway): catalog::assemble — expand tier-ref chains into concrete GatewayConfig (fails loud)`.

---

### Task 6: Feature-doc status + worked example + module README

**Files:** `docs/features/catalog/{README,free-tier-catalog,tiers-and-chains,catalog-refresh}.md`; `catalog/mod.rs` (a doc-example test).

- [ ] **Step 1:** Add a documented, tested end-to-end example (a `catalog/mod.rs` test or a `///` doc-test on `assemble`): build a `CatalogConfig` with a `free` tier (curated + a derived predicate), a `cost-optimized` tier, and `research.bulk = [free, cost-optimized]`; `assemble`; assert the concrete chain ordering + that a newly-added free model appears without editing the chain. This is the runnable "worked example" the docs point to.
- [ ] **Step 2: Flip status** in `docs/features/catalog/`:
  - `free-tier-catalog.md`, `tiers-and-chains.md` → `status: implemented` (Phase 1 · SP-CAT); point `source:` at the real `crates/gateway/src/catalog/*.rs` + `crates/kernel/src/types/config.rs`.
  - `catalog-refresh.md` → mark the **re-audit + drift gate** implemented; keep external fetch/import marked deferred (a separate held-off persistence layer).
  - `README.md` status table → flip the three SP-CAT rows to Implemented; keep `config-versioning.md` (Phase-4) and note **persistence is a separate held-off layer** (config-driven only) in Notes.
  - Do NOT claim the deferred items as done: DB `config_loader`/persistence, live-usage `Headroom`/`LeastUsed`, `config_versioning`, expiration tracking.
- [ ] **Step 3: Verify** — `cargo test --workspace` green (incl. the doc-example test); `make check` clean; frontmatter (`doctype: feature`/`module`) intact.
- [ ] **Step 4: Commit:** `feat(gateway): SP-CAT worked example + docs flip (catalog/tiers implemented; persistence stays deferred)`.

---

## Self-Review

- **Spec coverage** (`docs/superpowers/specs/2026-08-07-sp-cat-catalog-design.md`): §2 metadata → T1; §4 cost_band → T2; §7 totals+audit → T3; §3/§5 tiers+strategy → T4; §6 CatalogConfig+assemble → T5; §9 Gherkins + §8 persistence stance → across T3/T4/T5 + T6 docs. Every `AssembleError` variant + the stub-warning + determinism are pinned.
- **No persistence / config-driven:** every deliverable is a pure fn or a config type; no store, no I/O (the totals drift gate reads a checked-in fixture, not an external source). `assemble` is pure config→config. Matches the user directive.
- **Behavior preservation:** `ModelConfig.catalog` and all tier/catalog types are new/optional; existing configs deserialize unchanged (T1 pins absent⇒None). The SP-0 selection/gate pipeline is not touched — `assemble` produces the same concrete `FallbackChainConfig` shape hand-authoring produces (T5 backward-compat test). The only mechanical churn is adding `catalog: None` to existing `ModelConfig { .. }` literals (T1 Step 4).
- **Type consistency:** `CostBand` (kernel, T1) derived by `cost_band` (T2), matched by `TierPredicate` (T4); `tier_members`/`order_members` (T4) consumed by `assemble` (T5); `AssembleError` (T4/T5) surfaced loud; `FreeType`/`pool_key` (T1) drive `free_tier_totals` (T3). `assemble` output `GatewayConfig`/`FallbackChainConfig`/`ChainEntry` are the existing kernel types the SP-0 selector reads.
- **Sequencing (each green + committed):** 1 kernel metadata (optional) → 2 cost_band (pure) → 3 totals (pure) → 4 tiers (uses cost_band) → 5 assemble (uses tiers) → 6 docs+example. `AssembleError` is introduced in T4 (first needed by `tier_members`) and extended in T5 — define it in T4 to avoid a forward reference. No broken intermediate.
- **Loud failure (no silent gaps):** `AssembleError` collects all misconfigs (unknown/empty tier refs); `Headroom`/`LeastUsed` emit a warning rather than silently pretending to be dynamic; the totals drift gate fails the build on doc/data drift.
- **Placeholder scan:** cost_band thresholds are pinned (0.002/0.02, boundary-tested); the totals fixture + headline constant are concrete (T3 Step 4); test bodies specified. The `free_tier_totals` pool-budget tie-break is pinned (MAX within a pool, documented).

## Execution Handoff

Subagent-driven in an isolated worktree off `develop`; per-task spec + code-quality review (behavior-adding T4 `assemble`/T5 and the totals T3 get the full treatment); final whole-branch review; `finishing-a-development-branch` → merge to `develop`. Then the config-driven program continues with **reference chains** (a free-tier "research team" built on these tiers) → **SP-1** orchestrator. Persistence (SP-DATA) stays a separate held-off layer.
