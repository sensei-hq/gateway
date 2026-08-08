# Reference Chains Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Ship portable, attribute-derived tier + chain presets (a free-tier "research team" + planning/coding chains) on SP-CAT's `assemble`, plus a small runnable demo catalog — so tests and the upcoming SP-1 orchestrator have named chains and a runnable config. Config-driven, **no persistence**. Design: `docs/superpowers/specs/2026-08-07-reference-chains-design.md`.

**Architecture:** One small SP-CAT extension — a `tags` attribute on `CatalogMeta` + a `tags` predicate — then everything is preset DATA (Rust fns returning `TierConfig`/`TierChain`/`CatalogConfig`) consumed by the existing `catalog::assemble`. No change to SP-0 selection or `assemble` itself.

**Tech Stack:** Rust, `crates/kernel` (`CatalogMeta.tags`) + `crates/gateway` (`catalog::presets` module). Contract per commit: existing tests green (new field defaults to `[]`); `cargo test --workspace` green; `make check` clean.

**Builds on:** SP-CAT (`CatalogMeta`, `TierConfig`/`TierPredicate`/`FreeMatch`/`IntraTierStrategy`, `CatalogConfig`/`TierChain`/`ChainRef`/`assemble`, `cost_band`).

---

## File Structure

- **Modify `crates/kernel/src/types/config.rs`** — add `#[serde(default)] pub tags: Vec<String>` to `CatalogMeta`. [Task 1]
- **Modify `crates/gateway/src/catalog/tiers.rs`** — add `tags: Option<Vec<String>>` to `TierPredicate`; `matches` gains the subset/AND tags check. [Task 1]
- **Create `crates/gateway/src/catalog/presets.rs`** (+ `mod presets; pub use presets::*;` in `catalog/mod.rs`) — `reference_tiers` [T2], `reference_chains` + `with_reference_tiers_and_chains` [T2], `demo_catalog` [T3].
- **Create `docs/features/catalog/reference-chains.md`** + update `docs/features/catalog/README.md`. [Task 3]

---

### Task 1: `tags` attribute + predicate (SP-CAT extension)

**Files:** `crates/kernel/src/types/config.rs`, `crates/gateway/src/catalog/tiers.rs`.

- [ ] **Step 1: Failing tests**
  - kernel (`config.rs` tests): a `CatalogMeta` JSON without `tags` deserializes with `tags == []` (backward-compatible); with `"tags":["reasoning"]` roundtrips.
  - gateway (`tiers.rs` tests): extend the derived-membership tests — `TierPredicate { tags: Some(vec!["reasoning".into()]), .. }` matches a model whose `catalog.tags` contains `"reasoning"`; excludes a model missing the tag; a two-tag predicate requires BOTH (subset/AND); combined with `auth_type` it's AND (a model with the tag but wrong auth is excluded).
- [ ] **Step 2:** `cargo test -p sensei-kernel catalog` + `cargo test -p sensei-gateway tiers` → FAIL.
- [ ] **Step 3: Implement**
  - `config.rs`: add to `CatalogMeta` `#[serde(default, skip_serializing_if = "Vec::is_empty")] pub tags: Vec<String>,`. (`Vec::default()` is `[]`; `Default` derive on `CatalogMeta` still works.)
  - `tiers.rs`: add to `TierPredicate` `#[serde(default, skip_serializing_if = "Option::is_none")] pub tags: Option<Vec<String>>,` and in `matches` add a check (keep the existing lazy AND style):
    ```rust
    let tags_ok = pred.tags.as_ref().is_none_or(|req| {
        let have = model.catalog.as_ref().map(|c| c.tags.as_slice()).unwrap_or(&[]);
        req.iter().all(|t| have.contains(t))     // every required tag present (AND)
    });
    ```
    and `&& tags_ok` in the final conjunction.
- [ ] **Step 4: Fix `CatalogMeta` literal churn** — adding a non-defaulted field breaks full `CatalogMeta { .. }` literals (a handful in the catalog tests). Add `tags: vec![]` (or `..Default::default()`) to each the compiler flags; report the count. Literals using `..Default::default()` need nothing.
- [ ] **Step 5: Verify** — `cargo test -p sensei-kernel` + `cargo test -p sensei-gateway --lib` green (274 + new); `cargo test --workspace` green; clippy `-D warnings` + fmt clean. Backward-compat: absent `tags` ⇒ `[]`.
- [ ] **Step 6: Commit:** `feat(catalog): tags attribute on CatalogMeta + tags predicate (AND subset)`.

---

### Task 2: Reference tier + chain presets

**Files:** create `crates/gateway/src/catalog/presets.rs`; `catalog/mod.rs` (`mod presets; pub use presets::*;`).

- [ ] **Step 1: Failing tests** (in `presets.rs`):
  - `reference_tiers()` returns the 4 tiers with the right predicates: `free` (`free: Some(FreeMatch::Any)`), `cost-optimized` (`cost_band: Some(CostBand::Low)`, strategy `Cost`), `fallback-specialty` (`locality: Some(Local)`), `premium-reasoning` (`auth_type: Some(OauthCli)` AND `tags: Some(["reasoning"])`). Assert each tier's `derive` fields + `strategy`.
  - **Membership via presets:** build a small `models` map (a free model, a cheap-priced model, a local model, an oauth+reasoning-tagged model) and assert `tier_members` for each reference tier resolves to exactly the intended model(s).
  - `reference_chains()` returns `research.bulk` = `[Tier("free"), Tier("cost-optimized"), Tier("fallback-specialty")]`, `plan.frontier` = `[Tier("premium-reasoning"), Tier("cost-optimized")]`, `code.exec` = `[Tier("cost-optimized"), Tier("premium-reasoning")]`, all `capability: TextChat`, `fallback_triggers` = `[RateLimit, Timeout, ProviderError]`. Assert refs + capability + triggers.
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement** `presets.rs`:
```rust
use std::collections::HashMap;
use kernel::types::capability::Capability;
use kernel::types::config::{AuthType, ConstraintsConfig, CostBand, FreeType, Locality, ModelConfig, RouterConfig};
use kernel::types::config::FallbackTrigger;
use crate::catalog::{CatalogConfig, ChainRef, FreeMatch, IntraTierStrategy, TierChain, TierConfig, TierPredicate};

fn derived(strategy: IntraTierStrategy, pred: TierPredicate) -> TierConfig {
    TierConfig { strategy, members: Vec::new(), derive: Some(pred) }
}

/// The 4 portable reference tiers (all attribute-derived; see the design §2).
pub fn reference_tiers() -> HashMap<String, TierConfig> {
    HashMap::from([
        ("free".into(),               derived(IntraTierStrategy::Priority, TierPredicate { free: Some(FreeMatch::Any), ..Default::default() })),
        ("cost-optimized".into(),     derived(IntraTierStrategy::Cost,     TierPredicate { cost_band: Some(CostBand::Low), ..Default::default() })),
        ("fallback-specialty".into(), derived(IntraTierStrategy::Priority, TierPredicate { locality: Some(Locality::Local), ..Default::default() })),
        ("premium-reasoning".into(),  derived(IntraTierStrategy::Priority, TierPredicate { auth_type: Some(AuthType::OauthCli), tags: Some(vec!["reasoning".into()]), ..Default::default() })),
    ])
}

fn chain(refs: Vec<ChainRef>) -> TierChain {
    TierChain { capability: Capability::TextChat, refs,
        fallback_triggers: vec![FallbackTrigger::RateLimit, FallbackTrigger::Timeout, FallbackTrigger::ProviderError] }
}

/// The 3 reference chains (tier-ref; see the design §3).
pub fn reference_chains() -> HashMap<String, TierChain> {
    let t = |id: &str| ChainRef::Tier(id.to_string());
    HashMap::from([
        ("research.bulk".into(), chain(vec![t("free"), t("cost-optimized"), t("fallback-specialty")])),
        ("plan.frontier".into(), chain(vec![t("premium-reasoning"), t("cost-optimized")])),
        ("code.exec".into(),     chain(vec![t("cost-optimized"), t("premium-reasoning")])),
    ])
}

/// Merge the reference tiers + chains onto operator-supplied routers/models.
/// Reference ids are a base; a caller can post-hoc override by editing the result.
pub fn with_reference_tiers_and_chains(
    routers: HashMap<String, RouterConfig>,
    models: HashMap<String, ModelConfig>,
    constraints: ConstraintsConfig,
) -> CatalogConfig {
    CatalogConfig { routers, models, tiers: reference_tiers(), chains: reference_chains(), constraints }
}
```
  (`TierPredicate` needs `#[derive(Default)]` — it was added in SP-CAT Task 4; confirm, else add it. `derived(..)` uses `..Default::default()` on `TierPredicate`.)
  Add `mod presets; pub use presets::*;` to `catalog/mod.rs`.
- [ ] **Step 4: Verify** — preset + membership + chain tests pass; `cargo test -p sensei-gateway --lib` green; `cargo test --workspace` green; clippy/fmt clean.
- [ ] **Step 5: Commit:** `feat(catalog): reference tier + chain presets (research.bulk, plan.frontier, code.exec)`.

---

### Task 3: Demo catalog + runnable end-to-end + docs

**Files:** `catalog/presets.rs`, `docs/features/catalog/reference-chains.md`, `docs/features/catalog/README.md`.

- [ ] **Step 1: Failing tests** (in `presets.rs`):
  - `demo_catalog()` returns a `CatalogConfig` (via `with_reference_tiers_and_chains`) with the illustrative models (design §5): `llama3.1-local` (router `ollama`, `catalog.locality: Local`, no pricing), `groq-llama-free` (router `groq`, `catalog.free: { free_type: Keyless, .. }`), `deepseek-chat` (router `deepseek`, low `pricing` → `cost_band Low`), `claude-code` (router `claude-cli`, `catalog.auth_type: OauthCli`, `catalog.tags: ["reasoning"]`). Routers have realistic urls; cloud ones use `api_key_env` (BYOK), `ollama` is keyless.
  - **Expansion:** `assemble(demo_catalog()).unwrap()` succeeds; `chains["research.bulk"].models` (by model id) == `["groq-llama-free", "deepseek-chat", "llama3.1-local"]` (free → cost → specialty, ascending priority); `chains["plan.frontier"].models` == `["claude-code", "deepseek-chat"]`; and `premium-reasoning`'s member is `claude-code` (via `auth_type + tags`).
  - **Portability:** adding a second reasoning-tagged oauth model to the demo's models (no chain edit) → `plan.frontier` now includes it.
  - **Runnable end-to-end:** register a `NoopAdapter` for the `ollama` router (reuse the engine-test harness pattern), `assemble(demo)` → `Gateway::new` → `execute` a `TextChat` request pinned to a chain whose first LIVE candidate is `llama3.1-local` (e.g. a one-entry chain of the local model, or assert selection reaches it) → served by it. (Keep the assertion about the assembled reference chain driving real SP-0 selection; the noop stands in for a live local model.)
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement** `demo_catalog()` in `presets.rs` (a helper `fn tagged_model(id, provider, meta, pricing) -> ModelConfig` + `fn router(url, key_env) -> RouterConfig` keeps it terse). Realistic urls (`http://localhost:11434` for ollama; provider base urls for the rest), `api_key_env` set for cloud routers, `enabled: true`. Document (doc-comment) that the models are illustrative and editable.
- [ ] **Step 4: Docs** — create `docs/features/catalog/reference-chains.md` (`doctype: feature`, `status: implemented`, `spec: SP-REF`): the 4 tiers + 3 chains table, the preset API, the demo catalog (illustrative), and a Gherkin-style scenario or two (chain expansion; add-a-model-updates-chains). Add a row to `docs/features/catalog/README.md`'s status table (Implemented). Note live-usage strategies + persistence stay deferred.
- [ ] **Step 5: Verify** — demo/expansion/portability/runnable tests pass; `cargo test -p sensei-gateway --lib` + `cargo test --workspace` green; clippy/fmt clean; `make check` clean.
- [ ] **Step 6: Commit:** `feat(catalog): demo catalog + runnable reference-chain example + docs`.

---

## Self-Review

- **Spec coverage** (`2026-08-07-reference-chains-design.md`): §1 tags → T1; §2 tiers + §3 chains + §4 API → T2; §5 demo + §6 runnable → T3; §7 deferred (stubs/persistence) noted in the T3 docs. Every reference tier's predicate + each chain's refs are pinned by tests.
- **Behavior preservation:** `CatalogMeta.tags` and `TierPredicate.tags` are new/defaulted (`[]`/`None`) ⇒ existing configs/tests unchanged except the mechanical `CatalogMeta` literal churn (T1 Step 4). Presets are pure data; `assemble`/SP-0 selection untouched.
- **Type consistency:** `tags` (kernel, T1) matched by `TierPredicate.tags` (T1) used in `reference_tiers` (T2); `reference_tiers`/`reference_chains` consumed by `with_reference_tiers_and_chains`/`demo_catalog` (T2/T3) → `assemble` (SP-CAT). Reference-tier predicates use existing SP-CAT attributes (`free`/`cost_band`/`locality`/`auth_type`) + the new `tags`.
- **Sequencing (each green + committed):** 1 tags extension (backward-compatible) → 2 presets (pure data + membership tests) → 3 demo + runnable + docs. No broken intermediate.
- **No persistence / config-driven:** presets + demo are in-memory config builders (no I/O). Runnable test uses a noop adapter, not a live network call.
- **Placeholder scan:** demo models are illustrative-by-design (documented); preset code is concrete; test expectations pinned (exact member id ordering).

## Execution Handoff

Subagent-driven in an isolated worktree off `develop`; per-task spec + code-quality review (T2 presets + T3 demo/runnable get the full treatment); final whole-branch review; `finishing-a-development-branch` → merge to `develop`. Then the config-driven program continues with **SP-1** — the orchestrator (agents/skills/tools + the durable deep-research-mini skeleton) that binds roles to these named chains. Persistence (SP-DATA) stays a separate held-off layer.
