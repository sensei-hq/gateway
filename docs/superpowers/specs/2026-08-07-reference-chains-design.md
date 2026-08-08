---
title: Reference Chains — portable tier/chain presets + demo catalog (design)
doctype: spec
spec: SP-REF
phase: 2
status: approved
related:
  - docs/superpowers/specs/2026-08-07-sp-cat-catalog-design.md   # SP-CAT (tiers/assemble this builds on)
  - docs/superpowers/specs/2026-08-06-sensei-orchestrator-design.md  # §6.2 tier/chain examples, D13
  - docs/features/catalog/tiers-and-chains.md
---

# Reference Chains

**Goal:** Ship a small set of **portable, attribute-derived tier + chain presets** (a free-tier "research team" plus planning/coding chains) built on SP-CAT's `assemble`, **plus a small runnable demo catalog** that instantiates them — so tests and the upcoming SP-1 orchestrator have real chains and a runnable config to ride. Config-driven, **no persistence**.

**Builds on:** SP-CAT (`CatalogMeta`, `TierConfig`/`TierPredicate`, `IntraTierStrategy`, `CatalogConfig` → `assemble`).

**Approved decisions (brainstorm 2026-08-07):**
1. **Form** = portable presets (Rust fns) **+** a small concrete demo catalog.
2. **"reasoning"** expressed via a new small `tags` attribute on `CatalogMeta` (+ a `tags` predicate), so `premium-reasoning` derives on `auth_type: OauthCli AND tag "reasoning"`.
3. **All three** reference chains (`research.bulk`, `plan.frontier`, `code.exec`).
4. **Demo models** = illustrative, clearly editable defaults.

---

## 1. SP-CAT extension — `tags` (small, backward-compatible)

- `kernel::…::CatalogMeta` gains `#[serde(default)] pub tags: Vec<String>` (absent ⇒ `[]` ⇒ today's behavior).
- `gateway::catalog::TierPredicate` gains `#[serde(default, skip_serializing_if="Option::is_none")] pub tags: Option<Vec<String>>`.
- `matches` gains a tags check: a model matches iff **every** tag in `pred.tags` is present in `model.catalog.tags` (subset / AND), combined (AND) with the existing fields. Absent predicate `tags` = don't-care.

This is the only type change; everything else is preset data + a demo builder.

## 2. Reference tier presets (portable, attribute-derived)

`gateway::catalog::presets::reference_tiers() -> HashMap<String, TierConfig>`:

| Tier id | `derive` predicate | `strategy` |
|---|---|---|
| `free` | `{ free: Some(Any) }` | `Priority` (`Headroom` is the intent; stubbed → Priority until live usage) |
| `cost-optimized` | `{ cost_band: Some(Low) }` | `Cost` |
| `fallback-specialty` | `{ locality: Some(Local) }` | `Priority` |
| `premium-reasoning` | `{ auth_type: Some(OauthCli), tags: Some(["reasoning"]) }` | `Priority` |

All are derive-only (empty curated `members`) so they populate from whatever the operator tags — portable across catalogs. (`cost-optimized` = `Low` band = **cheap paid**; free models land in `free` via the `Free` band / `free_type`, not here.)

## 3. Reference chain presets

`gateway::catalog::presets::reference_chains() -> HashMap<String, TierChain>` (all `capability: TextChat`, `fallback_triggers` = the standard recoverable set — `RateLimit`, `Timeout`, `ProviderError`):

- `research.bulk` = `[Tier(free) → Tier(cost-optimized) → Tier(fallback-specialty)]` — the headline free-tier research team (free first for cost, then cheap paid, then local specialty).
- `plan.frontier` = `[Tier(premium-reasoning) → Tier(cost-optimized)]` — frontier reasoning, cheap fallback.
- `code.exec` = `[Tier(cost-optimized) → Tier(premium-reasoning)]` — cheap execution, frontier fallback.

## 4. Preset API

```rust
pub fn reference_tiers()  -> HashMap<String, TierConfig>;   // §2
pub fn reference_chains() -> HashMap<String, TierChain>;    // §3
/// Merge the reference tiers + chains onto an operator's routers/models
/// (operator-supplied tiers/chains win on id collision, so presets are a base).
pub fn with_reference_tiers_and_chains(
    routers: HashMap<String, RouterConfig>,
    models:  HashMap<String, ModelConfig>,
    constraints: ConstraintsConfig,
) -> CatalogConfig;
```

The operator tags their own models (`free_type`/`auth_type`/`tags`/`locality`); the reference tiers derive membership; `assemble` expands the reference chains against them. Adding/removing a tagged model updates every reference chain — no chain edit.

## 5. Demo catalog (runnable, illustrative, editable)

`gateway::catalog::presets::demo_catalog() -> CatalogConfig` — a small concrete `CatalogConfig` that instantiates the reference tiers/chains with a handful of **illustrative** tagged models across a few routers. A **local, keyless** model makes it runnable without credentials; cloud entries are present for structure (BYOK via `api_key_env`). Illustrative seed (operators edit):

| Model | Router | Tags/attrs → lands in |
|---|---|---|
| `llama3.1-local` | `ollama` (local, keyless) | `locality: Local`, no pricing → **fallback-specialty** (runnable) |
| `groq-llama-free` | `groq` (BYOK) | `free: { free_type: Keyless }` → **free** |
| `deepseek-chat` | `deepseek` (BYOK) | pricing → `cost_band: Low` → **cost-optimized** |
| `claude-code` | `claude-cli` (oauth) | `auth_type: OauthCli`, `tags: ["reasoning"]` → **premium-reasoning** |

All four reference tiers non-empty; all three reference chains resolve. Documented as a starting template — nothing hardcoded that can't be swapped.

## 6. Testing & acceptance

- **Preset expansion:** `assemble(demo_catalog())` succeeds; `research.bulk` expands to `[groq-llama-free, deepseek-chat, llama3.1-local]` (free → cost-optimized → specialty, ascending priority); `plan.frontier` = `[claude-code, deepseek-chat]`; `premium-reasoning` derives `claude-code` via `auth_type + tags`.
- **Tag predicate:** a model with `tags: ["reasoning"]` + `auth_type: OauthCli` matches `premium-reasoning`; one missing either tag or auth is excluded (AND).
- **Portability:** `with_reference_tiers_and_chains(operator_models)` + a fresh tagged model → it joins the right reference tier + every chain that references it, no chain edit.
- **Runnable end-to-end:** register a noop adapter for the local router; `assemble(demo)` → `Gateway::new` → `execute` a `TextChat` pinned to a chain whose first live candidate is the local model → served by it (proves the assembled reference chain drives real SP-0 selection).
- **Determinism + backward-compat:** presets are deterministic; the new `tags` field defaults to `[]` (existing configs/tests unchanged except mechanical additions).

## 7. Deferred / boundaries

- **Live-usage `Headroom`/`LeastUsed`** on `free`/`cost-optimized` stay stubbed → `Priority`/`Cost` (persistence held off).
- **No persistence** — presets + demo are in-memory config builders; no DB, no fetch.
- **Role→chain binding** (agent `kind`/phase → chain name) is the **orchestrator's** job (SP-1), not here — reference chains just provide the named vocabulary (`research.bulk`, `plan.frontier`, `code.exec`) it will bind against.
- Demo model choices are illustrative; a real deployment supplies its own tagged catalog (or edits the demo).
