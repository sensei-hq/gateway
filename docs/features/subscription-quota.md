# Feature: Subscription / Quota Metering (AUTH)

- **Status:** Reference (reflects code as of 2026-07-18)
- **Crate:** `gateway`
- **Primary sources:** `src/types/config.rs`, `src/types/request.rs`, `src/store.rs`, `src/types/error.rs`
- **Live wiring:** `src/engine.rs` (`Gateway::check_quota`, `record_call`)
- **Design:** `docs/design/subscription-quota-auth.md`

---

## 1. What this adds

Alongside the per-token **dollar** metering ([budget-and-cost](budget-and-cost.md)),
the gateway enforces **subscription-style quotas** — "N requests/day", "M
tokens/week" per team — and records every call so burn-rate is queryable. Dollar
metering is untouched; quota runs beside it on integer counters.

Three moving parts, all opt-in and additive:

1. **Recording** — the engine persists each terminal call to a `GatewayStore`.
2. **Enforcement** — a pre-flight guard refuses an over-quota subject before any
   provider is contacted.
3. **Attribution** — recorded usage is tagged with the caller's subject + tier.

With no store attached and no `auth` on the request, none of this engages and
behaviour is exactly as before.

## 2. Boundary — what the library does NOT do

The gateway is a library: it does **not** implement OAuth, validate tokens, or
resolve identity → tier. The consumer (with Kavach) authenticates the caller,
resolves their team + tier, and passes a small **auth context** per request. The
gateway applies the **operator-configured** limits for that tier and records usage.
(Same pattern as provider keys: the daemon injects `RouterConfig.api_key`; the
library never fetches a secret itself.)

## 3. Configuration (config-at-init) — `src/types/config.rs`

Constraints are provided at gateway initialization, alongside routers/models/chains,
via `GatewayConfig.constraints`. Empty ⇒ nothing is enforced.

```rust
pub struct ConstraintsConfig {
    pub tiers: HashMap<String, TierConstraints>, // keyed by tier label
    pub default: Option<TierConstraints>,        // used when tier absent/unknown
}
pub struct TierConstraints {
    pub quota: Vec<QuotaLimit>,                            // apply to all modalities
    pub per_capability: HashMap<Capability, Vec<QuotaLimit>>, // modality overrides
}
pub struct QuotaLimit { pub unit: MeterUnit, pub window: Window, pub limit: u64 }

pub enum MeterUnit { Requests, InputTokens, OutputTokens, TotalTokens, CostUsdMillis }
pub enum Window    { Day, Week, Month }   // rolling: start = now − period
```

`constraints` is `#[serde(default)]`, so existing configs (and configs from
older consumers) deserialize unchanged. A windowed **dollar** cap is expressed as
`QuotaLimit { unit: CostUsdMillis, .. }` — integer milli-USD (`cost_usd × 1000`),
so quota counters never touch `f64`; the dollar `Cost` path stays `f64`.

## 4. Auth context on the request — `src/types/request.rs`

```rust
pub struct AuthContext {
    pub subject_id: Uuid,       // team/tenant the call is metered against
    pub tier: Option<String>,   // selects the configured TierConstraints
}
// InferenceRequest.auth: Option<AuthContext>   (skipped on the wire when None)
```

Opaque ids only — no tokens. `None` ⇒ the call is recorded without a subject and no
quota is enforced.

## 5. Enforcement — `Gateway::check_quota` (`src/engine.rs`)

In both `execute` and `execute_stream`, after candidate selection but **before the
candidate walk / any provider call**:

1. No store, or no `request.auth`, or no matching tier constraints ⇒ return `Ok`
   (no enforcement).
2. Resolve the tier: `constraints.tiers[tier]`, else `constraints.default`.
3. Effective limits = tier-wide `quota` ∪ `per_capability[request.capability]`.
4. Read usage **once per distinct window** via `get_usage_since`, then for each
   limit check `used + this_call_estimate > limit`.
5. Over ⇒ `GatewayError::QuotaExceeded { unit, window, limit, used }`.

`this_call_estimate`: `Requests` → 1; `InputTokens`/`TotalTokens` → the request's
input-token estimate; `OutputTokens`/`CostUsdMillis` → 0 pre-call (see Notes).

`QuotaExceeded` is a **hard stop** — per-subject, so no other candidate helps. It is
**not** a `FallbackTrigger` (see `should_trigger_fallback`), and in `execute_stream`
it is a setup error returned before any stream. `stream_error_code` maps it to
`"quota_exceeded"`.

## 6. Recording & usage — `src/store.rs`

The engine persists each terminal call (success, terminal failure, streaming
`Done`) to the optional store, attributed via `InferenceCall.subject_id` / `.tier`
(pulled from `request.auth`). Recording is **best-effort**: a store error is
`warn`-logged and never fails the caller's inference.

`GatewayStore::get_usage_since(subject_id, since) -> UsageTotals` aggregates a
subject's usage in a window:

```rust
pub struct UsageTotals {
    pub requests: u64, pub input_tokens: u64, pub output_tokens: u64,
    pub total_tokens: u64, pub cost_usd_millis: u64,
}
```

`InMemoryStore` implements it (test/dev); a real consumer backs it with
`SUM(...) WHERE subject_id = $1 AND recorded_at >= $2`. Existing
`get_spend_since` / `get_spend_by_model_since` remain for USD reporting.

## 7. Attaching a store

```rust
let gw = Gateway::new(config, adapters, cb).with_store(Arc::new(my_store));
```

Without `with_store`, the gateway records nothing and enforces nothing.

## 8. Notes / quirks

- **Soft limits (TOCTOU).** Enforcement reads usage then checks; concurrent
  requests for one subject can overshoot by ~the in-flight count. Deliberate for v1
  (design D3). A hard/atomic `reserve`/`commit` path is a future add.
- **Rolling windows.** `Day`/`Week`/`Month` are `now − {1, 7, 30} days`, not
  calendar-aligned. Billing that needs calendar resets is future work (design D2).
- **Output/cost pre-call estimate = 0.** Output tokens and dollar cost aren't known
  before the call, so `OutputTokens` / `CostUsdMillis` caps engage once the
  *recorded* usage crosses the line, not on the call that would cross it.
- **Failed calls count as requests.** A terminal failure is recorded (status
  `Failed`, cost 0), so it advances the `Requests` counter but not token/cost ones.
- **Breaking trait change.** `GatewayStore` gained a required `get_usage_since`;
  consumer store impls must add it (batch with the `register_into` + `cargo update`
  migration).
- **No PII in logs.** `subject_id` is an opaque `Uuid`; the engine never logs the
  auth context or subject. Consumer store impls own data-retention for attributed
  rows.
```
