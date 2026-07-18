# Upgrade guide: gateway 0.2.x → 0.3.0

For consumers (sensei / strategos) re-pinning the `gateway` / `gateway-embedded`
git dependency from a `v0.2.x` tag to **`v0.3.0`**. 0.3.0 bundles the capability-trait
refactor, the Hugging Face adapter + model download, per-call cost + streaming, and
subscription/quota metering.

## TL;DR

| Area | Change | Action |
|---|---|---|
| `Gateway::execute` | **Unchanged** signature + `InferenceResponse` fields | none |
| JSON wire format | New fields are `serde(default)` | none — old ↔ new JSON round-trips |
| Adapter registration | `registry.register(adapter)` **kept** (now generic) | none for concrete `Arc<Adapter>` |
| Custom adapters | fat `InferenceAdapter` **removed** → capability traits | required *if you wrote any* |
| `GatewayStore` impls | new required `get_usage_since` | required *if you implement the trait* |
| `InferenceRequest {…}` literals | new `auth` field | add `auth: None` |
| `GatewayConfig {…}` literals | new `constraints` field | add `constraints: Default::default()` |
| Transitive CVEs | Cargo.lock is gitignored | `cargo update` (below) |

Everything else is **additive/opt-in** (see the last section). The main call path —
build a request, `gateway.execute(&req).await`, read `InferenceResponse` — compiles
and behaves exactly as before once the items above are addressed.

---

## 1. Re-pin the dependency

```toml
# Cargo.toml
gateway          = { git = "https://github.com/sensei-hq/gateway", tag = "v0.3.0" }
gateway-embedded = { git = "https://github.com/sensei-hq/gateway", tag = "v0.3.0" }
```

Then, because `Cargo.lock` is gitignored in the gateway repo, apply the security
updates locally (3 RUSTSEC advisories fixed during 0.3):

```sh
cargo update -p anyhow -p quinn-proto -p crossbeam-epoch
```

## 2. Adapter registration: `register()` is preserved

Under the hood the registry now has **one map per capability** (`ChatModel`,
`EmbedModel`, `SttModel`, `TtsModel`, `ImageModel`, `VideoModel`) instead of one fat
`InferenceAdapter` map. But `AdapterRegistry::register` is kept as the entry point —
it's now generic and delegates to the adapter's `RegisterInto` impl — so the common
call is **unchanged**:

```rust
// 0.2.x AND 0.3.0 — same line:
registry.register(Arc::new(OpenAiAdapter::from_config(&cfg)?)).await;
```

A chat+embed adapter now lands in both capability maps automatically. The only thing
that breaks here is if you stored an adapter as the **fat trait object**
(`Arc<dyn InferenceAdapter>`) — that trait is gone; hold a concrete
`Arc<SomeAdapter>` (or `Arc<dyn ChatModel>` etc.) instead.

Need finer control? The per-capability methods are public too:
`registry.register_chat(a.clone()).await; registry.register_embed(a).await;`.

## 3. Custom adapters (only if you wrote your own)

The fat `InferenceAdapter` trait is removed. A custom adapter now implements
`Model` (for `id()`), the capability trait(s) it supports, and `RegisterInto`:

```rust
use gateway::adapters::{Model, ChatModel, RegisterInto, AdapterRegistry};

impl Model for MyAdapter { fn id(&self) -> &str { "my" } }

#[async_trait::async_trait]
impl ChatModel for MyAdapter {
    async fn chat(&self, cfg: &RouterConfig, req: &ChatRequest)
        -> Result<ChatResponse, GatewayError> { /* … */ }
    async fn chat_stream(&self, cfg: &RouterConfig, req: &ChatRequest)
        -> Result<ChunkStream, GatewayError> { /* … */ }
}

#[async_trait::async_trait]
impl RegisterInto for MyAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &AdapterRegistry) {
        reg.register_chat(self).await;               // + register_embed(...) etc.
    }
}
```

Adapters now take typed capability requests/responses (`ChatRequest`/`ChatResponse`,
`EmbedRequest`/`EmbedResponse`, …) instead of the old fat `InferenceRequest`. The
gateway translates at the boundary, so the **public** `execute` facade is unchanged.
See `docs/design/adapter-capability-traits.md`.

## 4. `GatewayStore` implementations: add `get_usage_since`

If you implement the `GatewayStore` trait (e.g. a Postgres-backed store), add the
one new required method — it aggregates a subject's usage over a window for quota
enforcement:

```rust
async fn get_usage_since(&self, subject_id: Uuid, since: DateTime<Utc>)
    -> Result<UsageTotals, GatewayError>;
```

`UsageTotals { requests, input_tokens, output_tokens, total_tokens, cost_usd_millis }`
(all `u64`; dollars are integer milli-USD). Backing SQL:

```sql
SELECT count(*)                        AS requests,
       coalesce(sum(input_tokens),0)   AS input_tokens,
       coalesce(sum(output_tokens),0)  AS output_tokens,
       coalesce(sum(input_tokens+output_tokens),0) AS total_tokens,
       coalesce(sum(round(cost_usd*1000)),0)        AS cost_usd_millis
FROM inference_calls
WHERE subject_id = $1 AND recorded_at >= $2;
```

`InferenceCall` also gained `subject_id: Option<Uuid>` and `tier: Option<String>`
(persist them if you store the row). If you only *call* the store, no change.

Don't need quotas yet? A correct stub is fine — return `UsageTotals::default()`;
enforcement never triggers unless the request carries `auth` **and** the config has
matching `constraints`.

## 5. Struct-literal fields

Two config/request structs gained a field. Both are `serde(default)`, so **anything
loaded from JSON/config is unaffected** — only hand-built struct literals need a fix.

```rust
// InferenceRequest { … } literals:
let req = InferenceRequest { /* …existing… */, budget: None, auth: None };

// GatewayConfig { … } literals:
let cfg = GatewayConfig { routers, models, chains, constraints: Default::default() };
```

## 6. What did NOT change

- `Gateway::execute(&InferenceRequest) -> Result<InferenceResponse>` — same signature.
- `InferenceResponse` — identical fields; reading responses is source-compatible.
- `RouterConfig` — unchanged.
- Serde wire format — new fields serialize only when set, so 0.2 ↔ 0.3 JSON is
  compatible in both directions.

## 7. What you gain in 0.3 (all opt-in, non-breaking)

- **Persistence + burn-rate:** `gateway.with_store(Arc::new(store))` — the engine now
  records every call; `get_spend_since` / `get_usage_since` have data.
- **Streaming:** `gateway.execute_stream(&req).await` → a stream of `StreamEvent`.
- **Hugging Face Inference adapter** (`huggingface`) — OpenAI-compatible router, bearer
  HF token; base URL overridable for Inference Endpoints.
- **HF model download** (`gateway-embedded`, opt-in `hf-download` feature) — pull GGUF/
  ONNX from the HF Hub into the managed store, with an in-`pull` RAM/disk fit guard.
- **Subscription / quota metering** — operator-configured tier limits in
  `GatewayConfig.constraints` + `AuthContext` on `request.auth`; enforced pre-flight
  as a hard stop. See `docs/features/subscription-quota.md`.
- **Accurate per-call cost**, structured failure traces, and hardened config
  (adapters honor `config.headers`/`config.url`; redacting `Debug` on `RouterConfig`).

## 8. Verify

```sh
cargo update -p anyhow -p quinn-proto -p crossbeam-epoch
cargo build
cargo test
```

A clean build + green tests means the migration is complete.
