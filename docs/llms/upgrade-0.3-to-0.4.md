# Upgrade guide: gateway 0.3.x → 0.4.x

For consumers (sensei / strategos) re-pinning the gateway git dependency from a
`v0.3.x` tag to **`v0.4.x`**. This is the release where the workspace **re-layers**:
the old two-crate layout (`gateway` + `gateway-embedded`) becomes five
single-responsibility crates, and the async **provisioning supervisor** + **chain
pruning** + a `ModelNotReady` degradation signal land on top. From **v0.4.1** you can
also collapse to a **single `sensei-gateway` dependency**.

## TL;DR

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

Everything on the routing call path — build a request, `gateway.execute(&req).await`,
read `InferenceResponse` — compiles and behaves as before once the dependency rename
and the one new error variant are addressed.

---

## 1. Re-pin the dependency

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

**Or (v0.4.1+) collapse to one dependency** — gateway re-exports the whole local
surface under `gateway::local` and forwards the engine features:
```toml
gateway = { package = "sensei-gateway", git = "https://github.com/sensei-hq/gateway", tag = "v0.4.1",
            features = ["local", "local-fastembed"] }   # + local-llama-cpp / local-ort / local-hf-download
```
Cloud providers ride on the default `cloud` feature; a `--no-default-features` build
sheds the AWS SDK entirely (the kernel split made this possible).

## 2. Rename `gateway_embedded::…` imports

The routing-side `gateway::…` paths are preserved by re-exports, so most code is
untouched. Only the local-inference imports move:

| v0.3.x | v0.4.x (multi-crate) | v0.4.x (single-dep) |
|---|---|---|
| `gateway_embedded::adapters::{LlamaCppAdapter, EmbeddedLlamaAdapter, FastembedAdapter, OrtAdapter}` | `local_providers::adapters::…` | `gateway::local::…` |
| `gateway_embedded::math` | `local_providers::math` | `local_providers::math` |
| `gateway_embedded::registry::{ManagedResolver, OllamaResolver, ExternalResolver, ChainedResolver, HfHubPuller, PullSpec, PullingResolver, …}` | `local_engine::registry::…` | `gateway::local::…` |
| `ModelEntry` / `ModelSource` / `ModelFormat` / `ModelResolver` / `ResolveError` | `kernel::registry::…` | `gateway::registry::…` |

Cloud adapters keep their historical facade path: `gateway::adapters::<provider>`
(feature `cloud`).

## 3. Handle the new `ModelNotReady` variant

`kernel::GatewayError` gained a terminal variant for a model that's still
provisioning:
```rust
GatewayError::ModelNotReady { model: String, phase: ProvisionPhase }
```
The enum is **not** `#[non_exhaustive]`, so any exhaustive `match` on `GatewayError`
must add an arm. It never triggers fallback; treat it as a retry-later terminal:
```rust
GatewayError::ModelNotReady { model, phase } => {
    // e.g. surface "still provisioning" + the phase; retry once ready
}
```
If you only `?`/propagate errors, no change.

## 4. What's new (all opt-in)

- **Async provisioning supervisor** — `gateway::local::ProvisioningSupervisor`
  resolves / pulls / coldboots / registers local models in the background and streams
  `ProvisionEvent`s. Wire it in with `gateway.with_readiness(Arc::new(supervisor))`;
  chain exhaustion then degrades a still-provisioning model to `ModelNotReady` instead
  of the generic `AllAttemptsFailed`.
- **Chain pruning** — `gateway.prune_unavailable(judge)` drops permanently-unavailable
  chain candidates (disabled/unknown router, unknown model, or a caller `judge` verdict
  like "no API key") and returns a `Vec<ChainWarning>`; provisioning candidates are kept.
- **Facade builder** — `gateway::FacadeBuilder::new(config).plans(…).build().await`
  composes a gateway with cloud providers registered + the supervisor wired, over one
  shared `AdapterRegistry`.
- **`pull_with_progress`** — the HF pull now emits byte progress (bridged to
  `Downloading { done, total }`).

## 5. What did NOT change

- `Gateway::execute(&InferenceRequest) -> Result<InferenceResponse>` — same signature.
- `InferenceResponse`, `RouterConfig`, `GatewayConfig`, `GatewayStore` — unchanged.
- Serde wire format — additive; 0.3 ↔ 0.4 JSON round-trips.

## 6. Verify

```sh
cargo build
cargo test
```

A clean build + green tests means the migration is complete. If you adopted the
supervisor, also exercise the `wait=false` path (fire-and-forget provisioning →
`ModelNotReady` while the model comes up) and confirm your error handling surfaces it.
