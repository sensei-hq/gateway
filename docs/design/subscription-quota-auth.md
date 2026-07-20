# Design: subscription/quota auth + metering (AUTH)

- **Status:** Approved (2026-07-18)
- **Crate:** `gateway` (cloud). No `local-providers`/`local-engine` changes.
- **Depends on:** the existing metering surface (`types/cost.rs`, `budget.rs`,
  `store.rs`) and the capability-trait engine (`engine.rs`).
- **Sequence:** last of FOUNDATION → HF-B → HF-A → **AUTH**.
- **Security:** most security-sensitive stream — the standing security gate
  (cargo audit + semgrep + credential/PII review) closes it out.

## 1. Goal

Support **subscription-style quota metering** — "N requests/day", "M tokens/week"
per team — **alongside** the existing per-token **dollar** metering, without
replacing it. A team on a tier gets a quota; the gateway enforces it (refusing a
call once exhausted) and records usage so burn-rate is queryable.

This also lands the deferred **burn-rate / `GatewayStore` wiring**: today the store
trait exists but the engine persists nothing (`docs/reviews/2026-07-17-…` item 13).
Wiring it is the prerequisite for both burn-rate and quota, so it comes first here.

## 2. Configuration model (decided) — config-at-init, one gateway, modality-scoped

Confirmed **D1 = A** (2026-07-18): constraints are **operator configuration provided
at gateway initialization**, alongside routers/models/chains — not resolved per
request. Once a `Gateway` is configured, **every call is bounded by what's
configured**; a runtime `try_update_config` swaps the whole picture atomically (as
today).

**One gateway, not a session per modality.** The existing design already proves
per-modality config in a single instance: `chains` is a keyed map, each entry scoped
to a `capability`. Constraints follow the *same shape* — a keyed map with a
capability dimension — so modality is a **scoping dimension inside the one config**,
not a separate session object. Session-per-modality was considered and rejected: it
would duplicate the per-capability config machinery (stateful handles, N configs to
keep coherent) to buy what capability-scoped config already provides, and it fights
the current stateless-per-call design (config in one `RwLock`).

### What still is NOT the gateway's job
The gateway is a **library**. It does **not** implement OAuth, validate tokens, or
resolve work-identity → tier. That's the **consumer + Kavach** (`project-kavach-auth`):
the consumer authenticates the caller, resolves their team + tier, and passes the
gateway a tiny **auth context** (opaque `subject_id` + `tier` label) per request.
The gateway then applies the **configured** constraints for that tier and records
usage against that subject. (Mirrors how the daemon injects `RouterConfig.api_key`
rather than the library fetching a secret.)

So the gateway-side AUTH surface is:
1. **Record** every call (wire `GatewayStore` into the engine).
2. **Enforce** the configured tier constraints for the request's subject.
3. **Attribute** recorded usage to `subject_id` / `tier`.

## 3. Current state (what we build on)

- **Metering is entirely dollar-based.** `Cost` / `CostEstimate` (USD),
  `Budget { daily_limit, monthly_limit, alert_threshold }` (USD), `budget.rs`
  affordability filtering (USD), `GatewayStore::get_spend_since → f64` (USD).
- **`request.budget: Option<f64>`** is a per-request USD *affordability* cap feeding
  `SelectionCriteria` (which model can I afford for THIS call). Quota is different: a
  rolling **window counter** over recorded history. Kept separate — **D5**.
- **`GatewayStore` is unwired.** `Gateway` holds only `config` / `adapters` /
  `circuit_breaker`; `execute()` computes `actual_cost` but **never persists an
  `InferenceCall`**. `InferenceCall` already has `session_id` + `project_id`
  (`Option<Uuid>`) but no subject/tier and no window aggregation beyond
  `get_spend_since`.
- **`InferenceRequest`** carries no caller identity.
- **`GatewayError::BudgetExceeded`** exists and is a `FallbackTrigger`; quota
  exhaustion is analogous but a **hard stop** (§5f).

## 4. Decisions (locked)

- **D1 = A** — constraints are gateway config (config-at-init), one gateway,
  modality-scoped (§2).
- **D2 = rolling windows** (`now − period`), reusing the store's `recorded_at >=
  since` filter. Calendar-aligned windows deferred.
- **D3 = soft/advisory limits** for v1 (tolerate bounded overshoot under
  concurrency); interface shaped so an atomic reserve/commit path is a later add (§6).
- **D4 = subject-level enforcement** for v1 (per-capability *limits* are
  configurable, but usage is aggregated per subject, not per model).
- **D5 = keep the per-request `budget` affordability filter separate** from windowed
  quota — they answer different questions.
- Quota metering is **additive**: dollar metering untouched. Public
  `Gateway::execute` facade unchanged. Empty constraints + no auth context ⇒
  byte-for-byte today's behaviour.

## 5. Design

### 5a. Wire `GatewayStore` into the engine (deferred prerequisite; ships first)

`Gateway` gains an **optional** store; absent ⇒ unchanged behaviour.

```rust
pub struct Gateway {
    config: Arc<RwLock<GatewayConfig>>,
    adapters: AdapterRegistry,
    circuit_breaker: CircuitBreakerManager,
    store: Option<Arc<dyn GatewayStore>>,   // NEW — None = today's behaviour
}
impl Gateway {
    pub fn with_store(mut self, store: Arc<dyn GatewayStore>) -> Self { … } // builder
}
```

At a terminal attempt (success **or** final failure) `execute` builds an
`InferenceCall` (it already holds adapter/model/tokens/cost/duration/status) and
calls `insert_inference_call`; `execute_stream` records once at its terminal `Done`.
Recording is **best-effort**: a store error is `tracing::warn!`-logged and **never**
fails the user's inference — metering must not take down the hot path.
`InferenceCall` gains `subject_id: Option<Uuid>` + `tier: Option<String>`.

### 5b. Auth context on the request (who — not the limits)

The *resolved* output of the consumer's OAuth step. Opaque ids, **no tokens, no
limits** (limits come from config).

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthContext {
    pub subject_id: Uuid,       // account quota against this team/tenant
    pub tier: Option<String>,   // selects the configured TierConstraints
}
// InferenceRequest gains:  #[serde(default, skip_serializing_if = "Option::is_none")]
//                          pub auth: Option<AuthContext>,
```

### 5c. Constraints in `GatewayConfig` — shaped like `chains`

```rust
// types/config.rs
pub struct GatewayConfig {
    pub routers:  HashMap<String, RouterConfig>,
    pub models:   HashMap<String, ModelConfig>,
    pub chains:   HashMap<String, FallbackChainConfig>,
    #[serde(default)]
    pub constraints: ConstraintsConfig,     // NEW — default empty ⇒ unlimited
}

/// Operator-configured subscription constraints. Empty ⇒ no enforcement (today).
#[derive(Default)]
pub struct ConstraintsConfig {
    /// Per-tier constraint sets, keyed by tier label (AuthContext.tier selects one).
    #[serde(default)] pub tiers: HashMap<String, TierConstraints>,
    /// Applied when a request has no tier, or a tier absent from `tiers`.
    #[serde(default)] pub default: Option<TierConstraints>,
}

pub struct TierConstraints {
    /// Windowed limits applied across all modalities.
    #[serde(default)] pub quota: Vec<QuotaLimit>,
    /// Optional per-modality additions (e.g. tighter image caps).
    #[serde(default)] pub per_capability: HashMap<Capability, Vec<QuotaLimit>>,
}
```

Quota vocabulary (integer counters — no f64 drift; dollar caps ride the same
mechanism as an integer-millis unit, while the existing f64 `Cost` USD path stays):

```rust
#[serde(rename_all = "snake_case")]
pub enum MeterUnit { Requests, InputTokens, OutputTokens, TotalTokens, CostUsdMillis }
#[serde(rename_all = "snake_case")]
pub enum Window { Day, Week, Month }   // rolling: start = now − period
pub struct QuotaLimit { pub unit: MeterUnit, pub window: Window, pub limit: u64 }
```

This keeps the config ergonomics parallel to fallback chains: both are keyed config
maps with a `capability` dimension. A windowed **dollar** cap is just
`QuotaLimit { unit: CostUsdMillis, window: Month, limit }`; the standalone `Budget`
struct is not used for enforcement (kept for compatibility / reporting).

### 5d. Enforcement — resolve tier → configured limits, pre-flight guard, record

Same instinct as HF-A's in-`pull` fit guard: **check before the expensive thing.**
In `execute` / `execute_stream`, after config load + candidate selection but
**before dispatching to a provider**:

1. If a store is set and `request.auth` is present, resolve the tier's constraints:
   `constraints.tiers.get(tier)` else `constraints.default`. None ⇒ skip to record.
2. Effective limits = `tier.quota` ∪ `tier.per_capability[request.capability]`.
3. For each `QuotaLimit`, query `get_usage_since(subject_id, now − window)` and
   refuse if `used[unit] + this_call_estimate > limit` with
   `GatewayError::QuotaExceeded { unit, window, limit, used }` — **before any
   provider call**. (`this_call_estimate`: `Requests`→1; input tokens→
   `estimate_input_tokens`; output tokens→0 pre-call; cost→the selection estimate.)
4. On the terminal attempt, record the `InferenceCall`; its tokens/cost/`+1 request`
   advance the same rows the pre-flight reads.

No store, or no `auth`, or empty constraints ⇒ step 1–3 skipped (today's behaviour).

### 5e. `GatewayStore` extension — one usage query

```rust
#[derive(Debug, Clone, Default)]
pub struct UsageTotals {
    pub requests: u64, pub input_tokens: u64, pub output_tokens: u64,
    pub total_tokens: u64, pub cost_usd_millis: u64,
}
#[async_trait]
pub trait GatewayStore: Send + Sync {
    // … existing methods unchanged …
    async fn get_usage_since(&self, subject_id: Uuid, since: DateTime<Utc>)
        -> Result<UsageTotals, GatewayError>;
}
```

`InMemoryStore` gets the trivial fold (mirrors `get_spend_since`); a real Postgres
impl (consumer-side) does `SUM(…) WHERE subject_id = $1 AND recorded_at >= $2`.
Existing `get_spend_since` / `get_spend_by_model_since` stay for dollar reporting.

### 5f. Errors + fallback semantics

New `GatewayError::QuotaExceeded { unit, window, limit, used }`;
`stream_error_code` → `"quota_exceeded"`. Unlike `BudgetExceeded` (per-model
affordability → pick a cheaper candidate), quota is **per-subject, not
per-provider** — no other candidate helps. So it is a **hard stop**: refused once,
up front, before the candidate loop; **not** a `FallbackTrigger`.

## 6. Concurrency: soft limits for v1 (D3)

"Read usage → call → record" is TOCTOU: concurrent requests for one subject can both
pass pre-flight and overshoot (by ~the in-flight concurrency). v1 accepts this
**bounded overshoot** (soft/advisory) — store stays read + append; matches the
"gross guard" pragmatism used for HF-A's fit heuristic. A later **hard** path adds
`reserve(subject, deltas) → Reservation` / `commit` / `release` to the trait with a
default that falls back to the soft path, so v1 isn't blocked. Documented, not an
oversight.

## 7. No-hardcoded-ops

Tiers, limits, windows are **operator config** in `GatewayConfig.constraints`
(empty by default, overridable at runtime) — exactly like routers/models/chains,
never compile-time constants.

## 8. Security considerations (standing gate)

- **No OAuth tokens in the gateway** — only opaque, resolved ids. Smaller secret
  surface by construction.
- `subject_id` is an opaque `Uuid`, not PII; keep it out of `info!`-level logs where
  practical (debug only); never log `AuthContext`/config wholesale.
- Recorded rows now carry subject attribution → a data-retention/PII note for the
  consumer's store impl (documented, not enforced by the library).
- Close with `cargo audit` + semgrep + a `{:?}`/tracing credential-leak sweep over
  the new types.

## 9. Build sequence

1. **Store wiring (no quota):** optional `store` + `with_store`; record
   `InferenceCall` in `execute` / `execute_stream` (best-effort); extend
   `InferenceCall` with `subject_id`/`tier` (`None` until step 3). Ships burn-rate
   standalone.
2. **Usage query:** `UsageTotals` + `get_usage_since` on the trait + `InMemoryStore`.
3. **Config + context types:** `MeterUnit`/`Window`/`QuotaLimit`/`TierConstraints`/
   `ConstraintsConfig` in `GatewayConfig`; `AuthContext` + `InferenceRequest.auth`;
   `QuotaExceeded` error + stream code; config validation for constraints.
4. **Enforcement:** pre-flight guard in `execute` + `execute_stream`; hard-stop
   semantics; tests (under / at / over limit; multi-unit; per-capability override;
   default-tier; no-store & no-auth no-ops; best-effort record on store error).
5. **Security gate** + a `docs/features/subscription-quota.md` feature page.

Each step is green + committed on its own; the facade stays stable throughout.

## 10. Non-goals

- No OAuth flow, token validation, session management, or plan catalog beyond the
  operator-configured limits (consumer + Kavach own identity/tier resolution).
- No calendar-aligned windows, no per-user sub-quotas, no hard/atomic reservation
  (deferred, §6).
- No change to dollar metering, model selection, or the circuit breaker.
- No `local-providers`/`local-engine` changes.
```
