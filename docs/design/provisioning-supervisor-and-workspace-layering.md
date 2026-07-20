# Design: workspace layering + async provisioning supervisor + chain pruning

- **Status:** Proposed (2026-07-20)
- **Issue:** #4 — `feat(gateway): async ProvisioningSupervisor (streamed pull/coldboot) + chain viability pruning with warnings`
- **Crates (target):** `kernel` (new), `cloud-providers` (new), `local-providers` (new), `local-engine` (new), `gateway` (routing + budgeting + client entry point). Published to crates.io under a `sensei-` prefix; imported under the clean short names via Cargo `package =` aliases (see §2).
- **Depends on / builds on:** `docs/design/adapter-capability-traits.md`, `docs/design/hf-model-download.md`
- **Rollout:** staged across PRs (see §9); ships as new crate tags.

## 1. Goal

Provisioning and coldboot of local models are effectively **synchronous** from a
consumer's point of view today: pulling a GGUF, loading it into an embedded
backend, or booting an adapter all block whoever drives them, and the *first
inference request* eats the coldboot latency. A client should never have to wait
on a multi-GB pull or an in-process model load just to use the gateway.

This design delivers issue #4's two library additions:

1. A **provisioning supervisor** — a library-owned manager for local model
   **resolve / pull / coldboot / register** that runs work in the background and
   **exposes a stream of progress events**, so a consumer (e.g. the senseid
   daemon) can relay streamed output to a client or a progress display. Blocking
   is opt-in via an explicit `wait` flag.
2. **Chain viability pruning + a warning report** at/after gateway
   construction: drop chain candidates that are *permanently* unavailable (cloud
   router with no key, disabled router, unknown model) and return a structured
   list of what was dropped and why — while **keeping** candidates that are
   merely still provisioning.

It also captures a **workspace re-layering** the design discussion converged on:
splitting today's two crates into five single-responsibility crates so the local
side mirrors the cloud side and `gateway` becomes a lean, provider-agnostic
routing core behind a single client entry point.

Two facts about the current library make the feature cheap and safe (verified
against the code):

- **Late adapter registration already works.** `AdapterRegistry` is
  `#[derive(Clone)]` with every field an `Arc<RwLock<HashMap<..>>>`
  (`crates/gateway/src/adapters/mod.rs:40`), and `Gateway` holds that shared
  handle. `execute()` looks the adapter up **per request**
  (`crates/gateway/src/engine.rs:244`). So the supervisor can register a local
  adapter **after** the gateway is already serving and requests pick it up
  immediately — no restart.
- **Fallback already tolerates a missing adapter.** In the candidate walk, a
  router with no registered adapter records a failed attempt and continues to
  the next candidate (`engine.rs:300`); exhaustion returns the aggregate
  "all attempts failed" error (`engine.rs:482`). So "model not ready → try the
  next chain candidate (e.g. cloud)" rides on existing machinery.

## 2. Target crate architecture

Five single-responsibility crates. The local side (providers driven by an
engine) mirrors the cloud side; `kernel` holds the shared vocabulary they all
speak; `gateway` is the routing/budgeting core and the one entry point clients
depend on.

```
                                kernel                    (shared vocabulary + ports; no deps)
                 ▲         ▲          ▲          ▲          types · capability traits · AdapterRegistry
      ┌──────────┘         │          │          └───────┐  GatewayError · IO · ModelEntry/ModelResolver
cloud-providers    local-providers    │                  │  ReadinessProbe · ProvisionPhase
(anthropic, openai,  (llama.cpp,       │                  │
 bedrock, gemini,     ollama_embedded, │                  │
 ollama-http, …)      ort, fastembed)  │                  │
      ▲                     ▲      local-engine  ─────────┘
      │                     └────── resolvers + provisioning (pull) + supervisor
      │                             + coldboot + registration + readiness
      │                             impl kernel::ReadinessProbe   → kernel, local-providers
      └──────────────┬──────────────────────┘
                  gateway   ← the one crate clients depend on
                  routing + budgeting + selection + circuit breaker + store
                  + chain pruning + entry-point facade
                  → kernel; wires cloud-providers [feat: cloud] + local-engine [feat: local]
```

Dependency arrows all point **down** toward `kernel`; the graph is acyclic.
Neither providers wing depends on the other, and `gateway`'s routing core never
names a concrete provider — it dispatches through `AdapterRegistry` (kernel trait
objects), so it compiles against `kernel` alone. The routing engine and the
local engine meet only through the kernel's `ReadinessProbe` port (§5).

> **Crate naming & publishing.** crates.io has a single flat namespace — there is
> no `org::crate` scoping (RFC 3243 "packages as optional namespaces" is unshipped
> as of early 2026). For a public OSS release each crate is therefore **published
> under a `sensei-` prefix** (`sensei-kernel`, `sensei-cloud-providers`,
> `sensei-local-providers`, `sensei-local-engine`, `sensei-gateway`) to avoid
> collisions on common names like `gateway`/`kernel`. The **directory and import
> names stay clean** — a dependent aliases the package back to the short name, so
> code reads unchanged:
>
> ```toml
> # crates/gateway/Cargo.toml
> [package]
> name = "sensei-gateway"
> [dependencies]
> kernel = { package = "sensei-kernel", path = "../kernel" }   # → `use kernel::…`
> ```
>
> Downstream is identical: `gateway = { package = "sensei-gateway", git = "…", tag = "…" }`
> → `use gateway::…`. Org ownership via `cargo owner --add github:sensei-hq:<team>`.

### Responsibilities

| Crate | Owns | Depends on | Heavy deps |
| --- | --- | --- | --- |
| `kernel` | All shared types + traits: config (`GatewayConfig`, `RouterConfig`, `ModelConfig`, chains, constraints), `GatewayError`, IO types, `Capability`, capability traits (`ChatModel`…), `AdapterRegistry`/`RegisterInto`, model-management vocabulary (`ModelEntry`/`ModelSource`/`ModelFormat`/`ModelResolver` trait/`ResolveError`), and the new readiness ports (`ProvisionPhase`, `ProvisionEvent`, `ReadinessProbe`). | — | none (serde, thiserror, async-trait, futures) |
| `cloud-providers` | The ~15 HTTP provider adapters (anthropic, openai, bedrock, gemini, grok, huggingface, together, ollama-http, image/video/audio, `noop`, …). Each implements the kernel capability traits + `RegisterInto`. | kernel | AWS Bedrock SDK, reqwest |
| `local-providers` | Local inference adapters: `LlamaCppAdapter`, `EmbeddedLlamaAdapter`, `FastembedAdapter`, `OrtAdapter`, `math`. Each `load(&ModelEntry)`-s and implements the kernel traits + `RegisterInto`. | kernel | feature-gated engines (`llama-cpp`, `fastembed`, `ort`) |
| `local-engine` | Concrete resolvers (`Managed`/`Ollama`/`External`/`Chained`), provisioning (`HfHubPuller`, `check_fit`, `PullingResolver`, progress download — feature `hf-download`), and the **`ProvisioningSupervisor`** (readiness state machine, `watch` streaming, dedup, coldboot orchestration, adapter registration). `impl ReadinessProbe`. | kernel, local-providers | forwards engine + `hf-download` features |
| `gateway` | Routing engine (`execute`/`execute_stream`, `selection`, `circuit_breaker`, `dispatch`), **budgeting** (`budget`, `store`, quota/constraints), `GatewayBuilder`/`purpose`, **chain pruning**, and the **client entry point / facade** (curated re-exports + a batteries-included builder that registers cloud providers and wires the local engine). | kernel; `cloud-providers` (feat `cloud`); `local-engine` (feat `local`) | none in the core; provider deps arrive only via the enabled wing |

### Why this shape

- **`gateway` is the one crate clients depend on** and is now lean +
  provider-agnostic. It re-exports what clients need (`gateway::GatewayError`,
  `gateway::adapters::AdapterRegistry`, `gateway::Gateway`, …) so existing
  downstream import paths keep compiling.
- **Provider deps are opt-in per wing.** `--features cloud` pulls in
  `cloud-providers` (AWS SDK + reqwest); `--features local` pulls in
  `local-engine` → `local-providers` (engines). A **pure-local** build sheds the
  AWS SDK; a **pure-cloud** build sheds the engines. Today `gateway-embedded`
  transitively compiles the entire Bedrock SDK just to reach the traits — the
  kernel split ends that.
- **Symmetry.** `cloud-providers ∥ local-providers`; the routing engine
  (`gateway`) ∥ the local engine (`local-engine`); both speak `kernel`.

### (provider, model) addressing

The local engine addresses a local model as a **(provider, model)** pair, the
same way the routing engine resolves (router, model). The same underlying model
can be served by more than one provider:

- `ollama + gemma4` — the Ollama **daemon** over HTTP (`ollama-http` adapter).
- `ollama_embedded + gemma4` — the same GGUF blob out of Ollama's cache, loaded
  **in-process** via the embedded llama backend.
- `hf + <repo/file>` — a GGUF/ONNX **pulled** from the HF Hub into the managed
  store, then loaded in-process.

A model's **provisioning plan** (§3) names which provider stands it up and how
(probe a daemon vs. pull + coldboot). Readiness is keyed by the model id the
supervisor registers into the `AdapterRegistry`.

## 3. §A — `ProvisioningSupervisor` (in `local-engine`)

Owns readiness state; the `Gateway` stays a dumb executor.

- **State:** `Arc<RwLock<HashMap<String /*model id*/, ModelSlot>>>`, where
  `ModelSlot` holds a `tokio::sync::watch::Sender<ProvisionPhase>` and the join
  handle for the in-flight job. Model ids are plain `String` (matches
  `ModelEntry.id` / `ModelConfig.id`).
- **Phases (state machine):**

  ```rust
  // in kernel
  pub enum ProvisionPhase {
      Absent,
      Queued,
      Downloading { done: u64, total: Option<u64> },
      Verifying,
      Loading,
      Ready,
      Failed { error: String },
  }
  impl ProvisionPhase {
      /// true for Queued / Downloading / Verifying / Loading.
      pub fn is_in_flight(&self) -> bool;
  }
  pub struct ProvisionEvent { pub model: String, pub phase: ProvisionPhase }
  ```

- **Streaming = `tokio::sync::watch`.** One latest-value channel per model.
  `ProvisionHandle::events()` yields the phase sequence (via
  `tokio_stream::wrappers::WatchStream`); late subscribers immediately observe the
  current phase; memory is bounded. Very fast transitions may coalesce —
  acceptable for a progress display. This is the "daemon wraps and relays the
  streamed output" requirement.

  ```rust
  // in local-engine
  pub struct EnsureOpts { pub wait: bool }

  pub struct ProvisionHandle { /* watch::Receiver<ProvisionPhase> + model id */ }
  impl ProvisionHandle {
      pub fn events(&self) -> impl futures::Stream<Item = ProvisionEvent>; // consumer relays this
      pub async fn wait_ready(self) -> Result<(), ProvisionError>;         // wait=true path
      pub fn phase(&self) -> ProvisionPhase;                               // current snapshot
  }

  impl ProvisioningSupervisor {
      pub fn ensure(&self, model: &str, opts: EnsureOpts) -> ProvisionHandle;
      pub async fn status(&self, model: &str) -> ProvisionPhase;
      pub async fn status_all(&self) -> Vec<(String, ProvisionPhase)>;
  }
  ```

- **`ensure(model, EnsureOpts { wait })`** — idempotent:
  - `Ready` → return a handle already at `Ready`.
  - in-flight (`Queued`/`Downloading`/`Verifying`/`Loading`) →
    `wait ? await ready/failed : return current phase + subscribe`.
  - `Absent`/`Failed` → transition to `Queued`, spawn the job;
    `wait ? await : return pending + subscribe`.
- **Provisioning plans + jobs.** The supervisor is constructed with a map
  `HashMap<String /*model id*/, ProvisionPlan>` (mirrors the existing
  `PullingResolver { specs }` pattern in `registry/pull.rs`, which the
  hf-download design already says the daemon populates from operator config).
  Each plan names its provider and how it provisions; jobs are internal,
  feature-gated tasks (co-located, no cross-crate trait):
  - *Managed/HF GGUF* (`hf-download` + `llama-cpp`): `check_fit` → stream+verify
    to the managed dir → coldboot embedded backend → `registry.register(...)`.
  - *fastembed* (`fastembed`) / *ort* (`ort`): `load()` → `register_embed`.
  - *Ollama daemon* / *ollama_embedded*: probe/reuse the cached blob → register.
- **Concurrency:** a `tokio::Semaphore` caps simultaneous downloads; the
  phase-map slot is the dedup key (exactly one job per model id).
- **Progress plumbing.** `hf-hub`'s `repo.get` is monolithic with **no progress
  callback today** (`registry/pull.rs`). The pull path gains a progress-emitting
  variant — `pull_with_progress(spec, &mut dyn FnMut(u64, Option<u64>))` — that
  the supervisor bridges to `sender.send_replace(Downloading { done, total })`.
  `check_fit()` already ranged-probes the total size, so `total` is known up
  front. This progress-emitting download is the one genuinely new sub-feature
  (vs. wrapping existing code).
- **Registration** uses the confirmed `RegisterInto` / `register_*` path
  (`adapters/mod.rs:53`, `:80`); late registration is picked up per request
  (`engine.rs:244`), so a model becomes usable the instant its job registers.

## 4. §B — Chain viability pruning (in `gateway`)

A pruning pass over `GatewayConfig` (with an availability judgment supplied by
the caller) that returns the pruned config plus a warning report. Pure config
logic — no Keychain, no adapter lookups.

```rust
// in gateway
pub enum Availability { Available, Pending, Unavailable { reason: String } }
pub struct ChainWarning { pub chain: String, pub router: String, pub model: String, pub reason: String }

/// Core: mutate config in place, return the warnings.
pub fn prune_unavailable(
    config: &mut GatewayConfig,
    judge: impl Fn(&str /*router*/, &str /*model*/) -> Availability,
) -> Vec<ChainWarning>;

impl Gateway {
    /// Convenience: write-lock the config, apply, return warnings.
    pub async fn prune_unavailable(
        &self,
        judge: impl Fn(&str, &str) -> Availability,
    ) -> Vec<ChainWarning>;
}
```

- **Classify each chain candidate:**
  - **Permanently unavailable** → **drop the chain entry + warn.** The library
    itself treats pure-config signals — disabled router (`router.enabled == false`,
    cf. `selection.rs:148`), unknown router, unknown model — as `Unavailable`
    without consulting the judge. For everything else it calls the caller-supplied
    `judge` (which encodes "cloud router has no API key", etc., without the library
    ever touching a Keychain).
  - **Provisioning / transient** (`Pending`) → **keep.** A model mid-pull stays
    in the chain; it becomes available when the supervisor registers it, and the
    degradation path (§6) covers the interim.
- **Do not** key pruning off "is an adapter currently registered" — that would
  delete the very local models we're mid-pull on.
- **Shape:** prune removes the chain *entry* from `chain.models`. A chain that
  empties is **retained** (an honest `NoCandidates` at execute time) rather than
  deleted, so tier-3 capability resolution stays predictable.
- **Report:** `Vec<ChainWarning>`, e.g.
  `{ chain: "reasoning", router: "anthropic", model: "claude-…", reason: "no api key" }`.

## 5. §C — Readiness surface + the injection seam

The kernel owns the **port**; `local-engine` owns the **implementation**; the
`gateway` routing core consults the port through a trait object. This is what
lets the supervisor live in `local-engine` while `execute()` (in `gateway`) still
returns `ModelNotReady`, with no dependency between the two engines.

```rust
// in kernel
#[async_trait::async_trait]
pub trait ReadinessProbe: Send + Sync {
    async fn phase(&self, model: &str) -> ProvisionPhase;
    async fn status_all(&self) -> Vec<(String, ProvisionPhase)>;
}

// in local-engine
#[async_trait::async_trait]
impl ReadinessProbe for ProvisioningSupervisor { /* … */ }
```

`Gateway` gains an optional probe, mirroring the existing optional `store`
(`engine.rs:35`, `with_store` at `engine.rs:55`):

```rust
impl Gateway {
    pub fn with_readiness(mut self, probe: Arc<dyn ReadinessProbe>) -> Self;
}
```

- **Compile-time** dependency: `local-engine` → `kernel` (implements the trait).
  **Runtime** call: `gateway` → probe via `dyn` dispatch. Opposite directions, no
  cycle.
- **Composition root:** the `gateway` facade builder (or the daemon) constructs
  the supervisor and calls `with_readiness(...)`. `gateway`'s source never names
  `local-engine` outside the feature-gated wiring.
- Consumers map `status`/`status_all` + the pruning warnings onto their own
  status surface (e.g. senseid's `GET /api/gateway/status`). No HTTP in the
  library.

## 6. §D — Degradation semantics + `ModelNotReady`

No change to the `execute()` **selection algorithm**. Behavior falls out of
existing machinery plus the readiness signal.

- **`wait=true`** (consumer maps it): `supervisor.ensure(model, wait).wait_ready().await`
  → then `execute()`.
- **default (`wait=false`)**: fire `ensure(model, wait=false)` (kick off
  provisioning if `Absent`, non-blocking) → `execute()`. The chain routes to
  whatever is registered — the local provider if ready, else a cloud/other
  fallback candidate if the chain has one.
- **not-ready signal.** A new terminal error variant:

  ```rust
  // in kernel::GatewayError
  #[error("model '{model}' not ready: {phase:?}")]
  ModelNotReady { model: String, phase: ProvisionPhase },
  ```

  The candidate walk is untouched. Only at the terminal exhaustion point — where
  `execute()` today builds `AllAttemptsFailed` (`engine.rs:482`) — does it, **if a
  probe is attached**, consult the probe for the attempted candidates (in
  priority order) and return `ModelNotReady { model, phase }` for the first whose
  phase `is_in_flight()`. With no probe attached, behavior is byte-identical to
  today.
  - `ModelNotReady` never triggers fallback (`should_trigger_fallback` → false).
    The exhaustive `match` in `error.rs` (no `_` arm) forces this decision at
    compile time.
  - `execute_stream` maps it to a terminal `StreamEvent::Error { code:
    "model_not_ready", … }` for parity (`stream_error_code` gains an arm).

## 7. Testing strategy

- **`kernel`:** `ProvisionPhase::is_in_flight`, serde, and the `ReadinessProbe`
  trait via a fake impl.
- **Pruning (`gateway`):** table-driven — disabled router dropped, no-key cloud
  dropped, `Pending` local kept, empty-chain retention, warning contents. No
  network.
- **Supervisor (`local-engine`, no engine features):** `ensure` idempotency /
  dedup (one job per model under concurrent `ensure`), phase transitions over the
  `watch` channel, `wait_ready` on success + failure, `status_all`. Jobs are
  stubbed via a test `ProvisionPlan` that drives the phases without touching
  hf-hub or an engine.
- **Degradation (`gateway`):** with a fake `ReadinessProbe`, a chain whose only
  candidate is in-flight returns `ModelNotReady`; with a ready cloud fallback it
  succeeds; with no probe it returns `AllAttemptsFailed` exactly as today.
- **Real pull-with-progress:** a tiny public GGUF, `#[ignore]` (network), asserts
  monotonic `Downloading { done }` then `Ready`.
- **Gate:** default build unaffected; the `--features` matrix (`cloud`, `local`,
  each engine, `hf-download`) builds green; clippy `-D warnings` clean; existing
  `gateway::…` re-export paths still compile (a path-smoke test guards the shims).

## 8. Public API additions (summary)

- **`kernel`:** `ProvisionPhase` (+`is_in_flight`), `ProvisionEvent`,
  `ReadinessProbe`, `GatewayError::ModelNotReady`.
- **`local-engine`:** `ProvisioningSupervisor`, `EnsureOpts`, `ProvisionHandle`,
  `ProvisionError`, `ProvisionPlan`, `pull_with_progress`.
- **`gateway`:** `Availability`, `ChainWarning`, `prune_unavailable` (free fn +
  `Gateway` method), `Gateway::with_readiness`, and re-exports of the above so
  clients touch only `gateway`.

## 9. Build / PR sequence

Each PR is standalone and independently reviewable. Steps 1–3 are pure
relocations (no behavior change, guarded by re-export shims); step 4 is the
feature.

1. **Extract `kernel`.** Move `types/*`, `adapters/capability.rs`, and
   `adapters/mod.rs` (`AdapterRegistry`/`RegisterInto`) into `kernel`. `gateway`
   and `gateway-embedded` re-point at it; `gateway` re-exports the moved paths so
   downstream is unaffected. (Delivers the AWS-SDK-shedding win — see
   `docs/plans/2026-07-20-kernel-extraction.md`.) The `ModelEntry`/`ModelResolver`
   vocabulary stays in `gateway-embedded` for now and moves to `kernel` in step 3,
   when `local-providers` first needs it.
2. **Extract `cloud-providers`.** Move `gateway/adapters/*` (the concrete HTTP
   adapters) into `cloud-providers`; gate it behind `gateway`'s `cloud` feature;
   the AWS SDK + reqwest move with it.
3. **Split `gateway-embedded` → `local-providers` + `local-engine`.** Lift the
   `ModelEntry`/`ModelSource`/`ModelFormat`/`ModelResolver` vocabulary into
   `kernel`; adapters + `math` → `local-providers`; concrete resolvers + `pull.rs`
   → `local-engine`. Retire `gateway-embedded` (optionally keep the name a release
   as a re-export shim).
4. **Issue #4 proper.** Add the readiness ports + `ModelNotReady` (kernel), the
   `ProvisioningSupervisor` + streaming + `pull_with_progress` (local-engine),
   chain pruning + `with_readiness` + terminal `ModelNotReady` consultation
   (gateway), and the facade builder wiring.

Steps 2 and 3 are independent of #4 and could be reordered or deferred; step 1
is the prerequisite for all of them.

## 10. Open decisions (recommendations — confirm on review)

- **Provisioning is folded into `local-engine`** as a feature-gated module rather
  than its own crate (pull is only ever used to stand up a local model). Split
  later if a standalone consumer of pull appears.
- **`EmbeddedLlamaAdapter` placement.** Keep the lazy-resolving multi-model
  router in `local-providers` and inject an `Arc<dyn ModelResolver>` (supplied by
  `local-engine`) — vs. moving the router into `local-engine`. Recommended: keep
  it in providers (it *serves* inference), inject the resolver.
- **`ollama-http` placement.** The Ollama daemon (HTTP) adapter moves with the
  HTTP adapters into `cloud-providers`; `ollama_embedded` (in-process from the
  Ollama cache) is its local-provider counterpart. Reconcile the two later if
  desired.
- **`noop` adapter** ships in `cloud-providers` (used as a default/test
  placeholder) and is re-exported from `gateway`.

## 11. Non-goals (separate senseid follow-ups, per the issue)

- HTTP endpoints, SSE/websocket relay of the progress stream, CLI flags
  (`models pull --wait`).
- Daemon startup sequencing (bind + serve immediately, spawn the supervisor,
  Keychain key refresh via `spawn_blocking`).
- Computing the availability predicate (Keychain key presence, disabled routers,
  provisionable local models) that feeds `prune_unavailable`.
- No change to routing/selection, cloud adapters' behavior, or the circuit
  breaker; no new model formats.

## 12. Rollout

The library changes ship as new crate tags and, for the public OSS release,
publish to crates.io under the `sensei-` package names (§2); the git-tag path
remains available for pre-release pins. Downstream (senseid) bumps its
dependencies and wires the facade builder; once `gateway` re-exports the local
engine, senseid can collapse from `gateway` + `gateway-embedded` pins down to
just `sensei-gateway` (with the `cloud`/`local` features it needs). Tracked as
separate senseid issues.
