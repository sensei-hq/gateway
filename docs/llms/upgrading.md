# Upgrading

Re-pin the gateway git dependency when moving between release tags. Sections are
**newest-first** — apply the one(s) that span your current tag → your target tag. The
routing call path (build a request, `gateway.execute(&req).await`, read
`InferenceResponse`) stays source-compatible across every step below; each section
lists only what you must touch.

## Unreleased

Small breaking change to the **`sensei-vault`** OAuth API (a maintainability refactor —
behaviour is unchanged). `store_oauth` now takes a struct instead of a long positional
list:

| Before | After |
|---|---|
| `Vault::store_oauth(tenant, router, access_token, refresh_token, expires_at_ms, scopes, client_id, actor)` | `Vault::store_oauth(tenant, router, &OAuthCredential { access_token, refresh_token, expires_at_ms, scopes, client_id }, actor)` |
| `TenantKeyCache::store_oauth(…same…)` | `TenantKeyCache::store_oauth(tenant, router, &OAuthCredential { … }, actor)` |
| `VaultStore::store_oauth(tenant, router, sealed, expires_at_ms, scopes, client_id, actor)` | `VaultStore::store_oauth(tenant, router, SealedOAuth { sealed, expires_at_ms, scopes, client_id }, actor)` |

`OAuthCredential` and `SealedOAuth` are re-exported from the crate root. **Action:** if you
implement `VaultStore` (e.g. sensei's own store adapter), update the `store_oauth`
signature; if you only call the vault, update the call sites. Nothing else in the vault
API changed.

## 0.3.x → 0.4.x

Re-pinning from a `v0.3.x` tag to **`v0.4.x`**. This is the release where the workspace
**re-layers**: the old two-crate layout (`gateway` + `gateway-embedded`) becomes five
single-responsibility crates, and the async **provisioning supervisor** + **chain
pruning** + a `ModelNotReady` degradation signal land on top. From **v0.4.1** you can
also collapse to a **single `sensei-gateway` dependency**.

### TL;DR

| Area | Change | Action |
|---|---|---|
| Crate layout | `gateway-embedded` **retired** → split into `local-providers` + `local-engine`; shared types → `kernel`; cloud adapters → `cloud-providers` (behind gateway's `cloud` feature) | re-pin deps + rename `gateway_embedded::…` imports |
| `gateway::…` routing paths | **preserved** via re-exports (`gateway::types`, `gateway::adapters`, `gateway::GatewayError`, `gateway::Gateway`, …) | none |
| `Gateway::execute` / `InferenceResponse` | **unchanged** signature + fields | none |
| `GatewayError` | new terminal variant `ModelNotReady { model, phase }` (enum is **not** `#[non_exhaustive]`) | add a match arm *if you match it exhaustively* |
| Model vocab (`ModelEntry`/`ModelResolver`/…) | moved to `kernel::registry` (also `gateway::registry`) | update import path *if used* |
| Local adapters / resolvers / pull | moved to `local-providers` / `local-engine` (or `gateway::local` in the single-dep form) | update import path *if used* |
| Async provisioning, pruning, facade | **new, opt-in** | adopt if you want them |
| Transitive CVEs | `Cargo.lock` is gitignored | `cargo update` as needed |

Everything on the routing call path compiles and behaves as before once the dependency
rename and the one new error variant are addressed.

### 1. Re-pin the dependency

**v0.3.x (before)** — two crates:
```toml
gateway          = { package = "sensei-gateway", git = "https://github.com/sensei-hq/gateway", tag = "v0.3.1" }
gateway-embedded = { package = "gateway-embedded", git = "https://github.com/sensei-hq/gateway", tag = "v0.3.1" }
```

**v0.4.x (after)** — `gateway-embedded` is gone; use the split crates you need:
```toml
gateway         = { package = "sensei-gateway",         git = "https://github.com/sensei-hq/gateway", tag = "v0.4.1" }
# only if you use local models:
local-engine    = { package = "sensei-local-engine",    git = "https://github.com/sensei-hq/gateway", tag = "v0.4.1", features = ["hf-download", "fastembed"] }
local-providers = { package = "sensei-local-providers", git = "https://github.com/sensei-hq/gateway", tag = "v0.4.1", features = ["fastembed"] }
```

**Or (v0.4.1+) collapse to one dependency** — gateway re-exports the whole local surface
under `gateway::local` and forwards the engine features:
```toml
gateway = { package = "sensei-gateway", git = "https://github.com/sensei-hq/gateway", tag = "v0.4.1",
            features = ["local", "local-fastembed"] }   # + local-llama-cpp / local-ort / local-hf-download
```
Cloud providers ride on the default `cloud` feature; a `--no-default-features` build sheds
the AWS SDK entirely (the kernel split made this possible).

### 2. Rename `gateway_embedded::…` imports

The routing-side `gateway::…` paths are preserved by re-exports, so most code is
untouched. Only the local-inference imports move:

| v0.3.x | v0.4.x (multi-crate) | v0.4.x (single-dep) |
|---|---|---|
| `gateway_embedded::adapters::{LlamaCppAdapter, EmbeddedLlamaAdapter, FastembedAdapter, OrtAdapter}` | `local_providers::adapters::…` | `gateway::local::…` |
| `gateway_embedded::math` | `local_providers::math` | `local_providers::math` |
| `gateway_embedded::registry::{ManagedResolver, OllamaResolver, ExternalResolver, ChainedResolver, HfHubPuller, PullSpec, PullingResolver, …}` | `local_engine::registry::…` | `gateway::local::…` |
| `ModelEntry` / `ModelSource` / `ModelFormat` / `ModelResolver` / `ResolveError` | `kernel::registry::…` | `gateway::registry::…` |

Cloud adapters keep their historical facade path: `gateway::adapters::<provider>` (feature `cloud`).

### 3. Handle the new `ModelNotReady` variant

`kernel::GatewayError` gained a terminal variant for a model that's still provisioning:
```rust
GatewayError::ModelNotReady { model: String, phase: ProvisionPhase }
```
The enum is **not** `#[non_exhaustive]`, so any exhaustive `match` on `GatewayError` must
add an arm. It never triggers fallback; treat it as a retry-later terminal:
```rust
GatewayError::ModelNotReady { model, phase } => {
    // e.g. surface "still provisioning" + the phase; retry once ready
}
```
If you only `?`/propagate errors, no change.

### 4. What's new (all opt-in)

- **Async provisioning supervisor** — `gateway::local::ProvisioningSupervisor` resolves /
  pulls / coldboots / registers local models in the background and streams
  `ProvisionEvent`s. Wire it in with `gateway.with_readiness(Arc::new(supervisor))`; chain
  exhaustion then degrades a still-provisioning model to `ModelNotReady` instead of the
  generic `AllAttemptsFailed`.
- **Chain pruning** — `gateway.prune_unavailable(judge)` drops permanently-unavailable
  chain candidates (disabled/unknown router, unknown model, or a caller `judge` verdict
  like "no API key") and returns a `Vec<ChainWarning>`; provisioning candidates are kept.
- **Facade builder** — `gateway::FacadeBuilder::new(config).plans(…).build().await` composes
  a gateway with cloud providers registered + the supervisor wired, over one shared
  `AdapterRegistry`.
- **`pull_with_progress`** — the HF pull now emits byte progress (bridged to
  `Downloading { done, total }`).

### 5. What did NOT change

- `Gateway::execute(&InferenceRequest) -> Result<InferenceResponse>` — same signature.
- `InferenceResponse`, `RouterConfig`, `GatewayConfig`, `GatewayStore` — unchanged.
- Serde wire format — additive; 0.3 ↔ 0.4 JSON round-trips.

### 6. Verify

```sh
cargo build
cargo test
```

A clean build + green tests means the migration is complete. If you adopted the supervisor,
also exercise the `wait=false` path (fire-and-forget provisioning → `ModelNotReady` while
the model comes up) and confirm your error handling surfaces it.

## 0.2.x → 0.3.x

Re-pinning from a `v0.2.x` tag to **`v0.3.x`**. 0.3.0 bundles the capability-trait refactor,
the Hugging Face adapter + model download, per-call cost + streaming, and
subscription/quota metering. Local inference still ships as the single `gateway-embedded`
crate throughout 0.3.x — the split into `local-providers` + `local-engine` (and the
extraction of shared types into `kernel`) landed in **v0.4.0** (see the section above).

### TL;DR

| Area | Change | Action |
|---|---|---|
| `Gateway::execute` | **Unchanged** signature + `InferenceResponse` fields | none |
| JSON wire format | New fields are `serde(default)` | none — old ↔ new JSON round-trips |
| Adapter registration | `registry.register(adapter)` **kept** (now generic) | none for concrete `Arc<Adapter>` |
| Custom adapters | fat `InferenceAdapter` **removed** → capability traits | required *if you wrote any* |
| `GatewayStore` impls | new required `get_usage_since` | required *if you implement the trait* |
| `InferenceRequest {…}` literals | new `auth` field | add `auth: None` |
| `GatewayConfig {…}` literals | new `constraints` field | add `constraints: Default::default()` |
| Local inference crate | still one `gateway-embedded` crate in 0.3.x (splits in v0.4.0) | none in 0.3.x |
| Transitive CVEs | Cargo.lock is gitignored | `cargo update` (below) |

Everything else is **additive/opt-in** (see the last subsection). The main call path —
build a request, `gateway.execute(&req).await`, read `InferenceResponse` — compiles and
behaves exactly as before once the items above are addressed.

### 1. Re-pin the dependency

```toml
# Cargo.toml
gateway          = { package = "sensei-gateway", git = "https://github.com/sensei-hq/gateway", tag = "v0.3.1" }
# Local inference (only if you use it) — still one crate in 0.3.x:
gateway-embedded = { package = "gateway-embedded", git = "https://github.com/sensei-hq/gateway", tag = "v0.3.1" }
```

Then, because `Cargo.lock` is gitignored in the gateway repo, apply the security updates
locally (3 RUSTSEC advisories fixed during 0.3):

```sh
cargo update -p anyhow -p quinn-proto -p crossbeam-epoch
```

### 2. Adapter registration: `register()` is preserved

Under the hood the registry now has **one map per capability** (`ChatModel`, `EmbedModel`,
`SttModel`, `TtsModel`, `ImageModel`, `VideoModel`) instead of one fat `InferenceAdapter`
map. But `AdapterRegistry::register` is kept as the entry point — it's now generic and
delegates to the adapter's `RegisterInto` impl — so the common call is **unchanged**:

```rust
// 0.2.x AND 0.3.0 — same line:
registry.register(Arc::new(OpenAiAdapter::from_config(&cfg)?)).await;
```

A chat+embed adapter now lands in both capability maps automatically. The only thing that
breaks here is if you stored an adapter as the **fat trait object**
(`Arc<dyn InferenceAdapter>`) — that trait is gone; hold a concrete `Arc<SomeAdapter>` (or
`Arc<dyn ChatModel>` etc.) instead.

Need finer control? The per-capability methods are public too:
`registry.register_chat(a.clone()).await; registry.register_embed(a).await;`.

### 3. Custom adapters (only if you wrote your own)

The fat `InferenceAdapter` trait is removed. A custom adapter now implements `Model` (for
`id()`), the capability trait(s) it supports, and `RegisterInto`:

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
`EmbedRequest`/`EmbedResponse`, …) instead of the old fat `InferenceRequest`. The gateway
translates at the boundary, so the **public** `execute` facade is unchanged. See
`docs/design/adapter-capability-traits.md`.

### 4. `GatewayStore` implementations: add `get_usage_since`

If you implement the `GatewayStore` trait (e.g. a Postgres-backed store), add the one new
required method — it aggregates a subject's usage over a window for quota enforcement:

```rust
async fn get_usage_since(&self, subject_id: Uuid, since: DateTime<Utc>)
    -> Result<UsageTotals, GatewayError>;
```

`UsageTotals { requests, input_tokens, output_tokens, total_tokens, cost_usd_millis }` (all
`u64`; dollars are integer milli-USD). Backing SQL:

```sql
SELECT count(*)                        AS requests,
       coalesce(sum(input_tokens),0)   AS input_tokens,
       coalesce(sum(output_tokens),0)  AS output_tokens,
       coalesce(sum(input_tokens+output_tokens),0) AS total_tokens,
       coalesce(sum(round(cost_usd*1000)),0)        AS cost_usd_millis
FROM inference_calls
WHERE subject_id = $1 AND recorded_at >= $2;
```

`InferenceCall` also gained `subject_id: Option<Uuid>` and `tier: Option<String>` (persist
them if you store the row). If you only *call* the store, no change.

Don't need quotas yet? A correct stub is fine — return `UsageTotals::default()`; enforcement
never triggers unless the request carries `auth` **and** the config has matching
`constraints`.

### 5. Struct-literal fields

Two config/request structs gained a field. Both are `serde(default)`, so **anything loaded
from JSON/config is unaffected** — only hand-built struct literals need a fix.

```rust
// InferenceRequest { … } literals:
let req = InferenceRequest { /* …existing… */, budget: None, auth: None };

// GatewayConfig { … } literals:
let cfg = GatewayConfig { routers, models, chains, constraints: Default::default() };
```

### 6. What did NOT change

- `Gateway::execute(&InferenceRequest) -> Result<InferenceResponse>` — same signature.
- `InferenceResponse` — identical fields; reading responses is source-compatible.
- `RouterConfig` — unchanged.
- Serde wire format — new fields serialize only when set, so 0.2 ↔ 0.3 JSON is compatible in
  both directions.

### 7. What you gain in 0.3 (all opt-in, non-breaking)

- **Persistence + burn-rate:** `gateway.with_store(Arc::new(store))` — the engine now records
  every call; `get_spend_since` / `get_usage_since` have data.
- **Streaming:** `gateway.execute_stream(&req).await` → a stream of `StreamEvent`.
- **Hugging Face Inference adapter** (`huggingface`) — OpenAI-compatible router, bearer HF
  token; base URL overridable for Inference Endpoints.
- **HF model download** (`local-engine`, opt-in `hf-download` feature) — pull GGUF/ONNX from
  the HF Hub into the managed store, with an in-`pull` RAM/disk fit guard.
- **Subscription / quota metering** — operator-configured tier limits in
  `GatewayConfig.constraints` + `AuthContext` on `request.auth`; enforced pre-flight as a
  hard stop. See `docs/features/subscription-quota.md`.
- **Accurate per-call cost**, structured failure traces, and hardened config (adapters honor
  `config.headers`/`config.url`; redacting `Debug` on `RouterConfig`).

### 8. Verify

```sh
cargo update -p anyhow -p quinn-proto -p crossbeam-epoch
cargo build
cargo test
```

A clean build + green tests means the migration is complete.
