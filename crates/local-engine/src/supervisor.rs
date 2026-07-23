//! The [`ProvisioningSupervisor`] — a library-owned, background readiness state
//! machine for local models. It owns the "is this model ready?" question so the
//! routing engine can stay a dumb executor: `ensure` kicks provisioning off (if
//! needed) and hands back a [`ProvisionHandle`] that streams phase transitions;
//! `status`/`status_all` snapshot readiness; and (via [`kernel::ReadinessProbe`],
//! implemented here) the gateway consults readiness at request time.
//!
//! ## Shape
//! - One [`tokio::sync::watch`] channel per model — latest-value semantics, so a
//!   late subscriber immediately sees the current phase and memory is bounded.
//!   The [`ModelSlot`] holding that channel is the **dedup key**: exactly one
//!   job runs per model id.
//! - Jobs run in the background ([`tokio::spawn`]); a [`tokio::sync::Semaphore`]
//!   caps how many provision concurrently. `ensure` is synchronous and must be
//!   called from within a Tokio runtime (it spawns).
//! - The real pull/coldboot/register job bodies are feature-gated behind the
//!   engine wings; the default build carries only the [`ProvisionPlan::Scripted`]
//!   path, which drives an explicit phase sequence with no engine and no network.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::StreamExt;
use kernel::{ProvisionEvent, ProvisionPhase};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::WatchStream;

/// Options for [`ProvisioningSupervisor::ensure`].
#[derive(Debug, Clone, Copy, Default)]
pub struct EnsureOpts {
    /// Caller intent to block until the model is usable (the daemon's
    /// `models pull --wait`). `ensure` is **always non-blocking** and returns a
    /// handle immediately; a caller that sets this then awaits
    /// [`ProvisionHandle::wait_ready`] before dispatching (see the routing
    /// engine's degradation path). The flag rides on the options bag so that
    /// intent travels with the request rather than being reconstructed.
    pub wait: bool,
}

/// A provisioning job failed.
#[derive(Debug, thiserror::Error)]
pub enum ProvisionError {
    /// The job reached [`ProvisionPhase::Failed`] (or ended before becoming
    /// ready); the string is the actionable reason.
    #[error("provisioning failed: {0}")]
    Failed(String),
}

/// How a model is provisioned. [`Self::Scripted`] drives an explicit phase
/// sequence with no engine and no network — the state machine's test double and
/// a hook for callers that provision out-of-band. The feature-gated variants
/// (added with the engine wings) name a provider and carry its inputs.
///
/// `#[non_exhaustive]`: which variants exist depends on the enabled engine
/// features, so external `match`es must carry a wildcard arm.
#[derive(Clone)]
#[non_exhaustive]
pub enum ProvisionPlan {
    /// Apply a fixed phase sequence, in order.
    Scripted(ScriptedPlan),

    /// Pull a GGUF from the Hugging Face Hub, then coldboot it behind the
    /// embedded llama.cpp router and register that router. Readiness walks the
    /// pull progress → `Verifying` → `Loading` → `Ready`.
    #[cfg(all(feature = "hf-download", feature = "llama-cpp"))]
    HfGguf {
        /// What to pull (repo / files / id / format).
        spec: crate::registry::PullSpec,
    },

    /// Coldboot an already-resolvable GGUF (Managed / Ollama / External) behind
    /// the embedded llama.cpp router and register it — no download.
    #[cfg(feature = "llama-cpp")]
    EmbeddedGguf,

    /// Load an ONNX embedding model with fastembed and register it.
    #[cfg(feature = "fastembed")]
    Fastembed {
        config: local_providers::adapters::FastembedConfig,
    },

    /// Load an ONNX embedding model with ONNX Runtime and register it.
    #[cfg(feature = "ort")]
    Ort {
        config: local_providers::adapters::OrtConfig,
    },

    /// Load Kokoro-82M for TTS via ONNX Runtime and register it (gh#23).
    #[cfg(feature = "kokoro")]
    Kokoro {
        config: local_providers::adapters::KokoroConfig,
    },

    /// Pull Kokoro's model + voice files from HF, then coldboot + register the
    /// TTS adapter (gh#23). The lexicon is *not* on the model repo — supply it as
    /// a sibling via `config` (e.g. a relative `../us_gold.json`).
    #[cfg(all(feature = "hf-download", feature = "kokoro"))]
    HfKokoro {
        /// Model + voice files to pull (`files[0]` is the ONNX model).
        spec: crate::registry::PullSpec,
        config: local_providers::adapters::KokoroConfig,
    },
}

/// A [`ProvisionPlan`] that emits a predetermined phase sequence — no engine, no
/// network. `steps` are applied in order and end at whatever terminal phase they
/// specify (typically [`ProvisionPhase::Ready`] or [`ProvisionPhase::Failed`]).
#[derive(Clone)]
pub struct ScriptedPlan {
    steps: Arc<Vec<ProvisionPhase>>,
    /// Test-only run counter: incremented each time a job for this plan actually
    /// runs, so a test can assert dedup (exactly one job per model). `None` in
    /// normal use; set via the `#[cfg(test)]` builder.
    run_counter: Option<Arc<AtomicUsize>>,
}

impl ScriptedPlan {
    /// Drive these phases in order.
    pub fn new(steps: Vec<ProvisionPhase>) -> Self {
        Self {
            steps: Arc::new(steps),
            run_counter: None,
        }
    }

    /// Record that a job for this plan ran (no-op unless a test attached a
    /// counter).
    fn record_run(&self) {
        if let Some(c) = &self.run_counter {
            c.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[cfg(test)]
    fn with_run_counter(mut self, counter: Arc<AtomicUsize>) -> Self {
        self.run_counter = Some(counter);
        self
    }
}

/// A subscription to one model's provisioning. Cheap to hold; a consumer relays
/// [`Self::events`] to a progress UI and/or awaits [`Self::wait_ready`].
pub struct ProvisionHandle {
    model: String,
    rx: watch::Receiver<ProvisionPhase>,
}

impl ProvisionHandle {
    fn new(model: String, rx: watch::Receiver<ProvisionPhase>) -> Self {
        Self { model, rx }
    }

    /// The model id this handle tracks.
    pub fn model(&self) -> &str {
        &self.model
    }

    /// The current phase snapshot.
    pub fn phase(&self) -> ProvisionPhase {
        self.rx.borrow().clone()
    }

    /// Stream of phase transitions for this model. A late subscriber immediately
    /// observes the current phase, then each subsequent transition. Very fast
    /// transitions may coalesce (latest-value `watch` semantics) — acceptable
    /// for a progress display; the consumer relays these to its client.
    pub fn events(&self) -> impl futures::Stream<Item = ProvisionEvent> + 'static {
        let model = self.model.clone();
        WatchStream::new(self.rx.clone()).map(move |phase| ProvisionEvent {
            model: model.clone(),
            phase,
        })
    }

    /// Await a terminal phase: `Ok(())` on [`ProvisionPhase::Ready`], `Err` on
    /// [`ProvisionPhase::Failed`] (or if provisioning ended before readiness).
    pub async fn wait_ready(mut self) -> Result<(), ProvisionError> {
        loop {
            let phase = self.rx.borrow_and_update().clone();
            match phase {
                ProvisionPhase::Ready => return Ok(()),
                ProvisionPhase::Failed { error } => return Err(ProvisionError::Failed(error)),
                _ => {}
            }
            if self.rx.changed().await.is_err() {
                // Every sender dropped without reaching Ready — nothing more will
                // move this model forward.
                return Err(ProvisionError::Failed(
                    "provisioning ended before the model was ready".to_string(),
                ));
            }
        }
    }
}

/// One model's live provisioning state: the sender that drives its phase channel
/// plus the handle of the in-flight job. Presence in the supervisor's map is the
/// dedup key.
struct ModelSlot {
    tx: watch::Sender<ProvisionPhase>,
    job: JoinHandle<()>,
}

impl Drop for ModelSlot {
    fn drop(&mut self) {
        // When the slot goes away (supervisor drop, or a Failed slot being
        // replaced) abort the job so a background pull never outlives the
        // supervisor. Aborting a finished job is a no-op.
        self.job.abort();
    }
}

/// Library-owned readiness state machine for local models. See the module docs.
pub struct ProvisioningSupervisor {
    slots: Mutex<HashMap<String, ModelSlot>>,
    plans: HashMap<String, ProvisionPlan>,
    sem: Arc<Semaphore>,
    /// What coldboot jobs load into and resolve through. Present only when an
    /// engine wing is enabled; the default build carries no coldboot at all.
    #[cfg(feature = "coldboot")]
    ctx: ColdbootCtx,
}

/// The shared handles a coldboot job needs: the registry it registers the loaded
/// adapter into (late registration is picked up per-request by the gateway) and
/// the resolver that locates model bytes. HF GGUF plans also need a puller.
#[cfg(feature = "coldboot")]
#[derive(Clone)]
struct ColdbootCtx {
    registry: kernel::adapters::AdapterRegistry,
    resolver: Arc<dyn kernel::registry::ModelResolver>,
    #[cfg(feature = "hf-download")]
    puller: Option<Arc<crate::registry::HfHubPuller>>,
}

#[cfg(feature = "coldboot")]
impl ColdbootCtx {
    /// An empty context — no registered adapters, an empty resolver, no puller.
    /// The facade builder (or the daemon) replaces these with real handles.
    fn new() -> Self {
        Self {
            registry: kernel::adapters::AdapterRegistry::new(),
            resolver: Arc::new(crate::registry::ChainedResolver::new()),
            #[cfg(feature = "hf-download")]
            puller: None,
        }
    }
}

impl ProvisioningSupervisor {
    /// Build a supervisor over `plans` (keyed by model id), capping concurrent
    /// provisioning jobs at `max_concurrent` (clamped to at least 1).
    ///
    /// Must be driven from within a Tokio runtime — [`Self::ensure`] spawns
    /// background tasks.
    pub fn new(plans: HashMap<String, ProvisionPlan>, max_concurrent: usize) -> Self {
        Self {
            slots: Mutex::new(HashMap::new()),
            plans,
            sem: Arc::new(Semaphore::new(max_concurrent.max(1))),
            #[cfg(feature = "coldboot")]
            ctx: ColdbootCtx::new(),
        }
    }

    /// Ensure a job is standing this model up, returning a handle to its
    /// progress. Idempotent and non-blocking:
    /// - **Ready** or **in-flight** (`Queued`/`Downloading`/`Verifying`/`Loading`)
    ///   → subscribe to the existing job's channel; no new job.
    /// - **Absent** or **Failed** → transition to `Queued`, spawn the job, and
    ///   subscribe.
    /// - **no plan** for this id → a handle reporting `Absent` (nothing to do).
    ///
    /// The [`ModelSlot`] insert is guarded by the slots lock, so concurrent
    /// `ensure` calls for the same id share the one job.
    pub fn ensure(&self, model: &str, _opts: EnsureOpts) -> ProvisionHandle {
        let mut slots = self.slots.lock().expect("supervisor slots mutex poisoned");

        if let Some(slot) = slots.get(model) {
            let phase = slot.tx.borrow().clone();
            if matches!(phase, ProvisionPhase::Ready) || phase.is_in_flight() {
                return ProvisionHandle::new(model.to_string(), slot.tx.subscribe());
            }
            // Failed → fall through and restart provisioning (replacing the slot,
            // which aborts the old, already-finished job on drop).
        }

        match self.plans.get(model) {
            Some(plan) => {
                let (tx, _) = watch::channel(ProvisionPhase::Queued);
                let handle_rx = tx.subscribe();
                let job = self.spawn_job(model.to_string(), plan.clone(), tx.clone());
                slots.insert(model.to_string(), ModelSlot { tx, job });
                ProvisionHandle::new(model.to_string(), handle_rx)
            }
            None => {
                // No provisioning plan — nothing to stand up; report Absent.
                let (_tx, rx) = watch::channel(ProvisionPhase::Absent);
                ProvisionHandle::new(model.to_string(), rx)
            }
        }
    }

    /// The current phase of `model` — `Absent` if the supervisor has never been
    /// asked to provision it.
    pub async fn status(&self, model: &str) -> ProvisionPhase {
        self.phase_of(model)
    }

    /// A snapshot of every model the supervisor is (or has been) provisioning,
    /// with its current phase.
    pub async fn status_all(&self) -> Vec<(String, ProvisionPhase)> {
        self.all_phases()
    }

    /// Current phase of `model`, or `Absent` if untracked. Sync (locks only, no
    /// await held) — the shared body behind both the inherent `status` and the
    /// [`kernel::ReadinessProbe`] impl.
    fn phase_of(&self, model: &str) -> ProvisionPhase {
        let slots = self.slots.lock().expect("supervisor slots mutex poisoned");
        slots
            .get(model)
            .map(|s| s.tx.borrow().clone())
            .unwrap_or(ProvisionPhase::Absent)
    }

    /// Snapshot of every tracked model's phase. See [`Self::phase_of`].
    fn all_phases(&self) -> Vec<(String, ProvisionPhase)> {
        let slots = self.slots.lock().expect("supervisor slots mutex poisoned");
        slots
            .iter()
            .map(|(id, s)| (id.clone(), s.tx.borrow().clone()))
            .collect()
    }

    /// Spawn the background job that drives `plan`'s phases through `tx`.
    fn spawn_job(
        &self,
        model: String,
        plan: ProvisionPlan,
        tx: watch::Sender<ProvisionPhase>,
    ) -> JoinHandle<()> {
        let sem = self.sem.clone();
        #[cfg(feature = "coldboot")]
        let ctx = self.ctx.clone();
        tokio::spawn(run_job(
            model,
            plan,
            tx,
            sem,
            #[cfg(feature = "coldboot")]
            ctx,
        ))
    }
}

/// Coldboot wiring — present only with an engine wing. The composition root
/// (the `gateway` facade builder, or the daemon) sets these before serving.
#[cfg(feature = "coldboot")]
impl ProvisioningSupervisor {
    /// The registry coldbooted adapters register into. The gateway picks up a
    /// late registration on the next request, so a model becomes usable the
    /// instant its job registers.
    pub fn with_registry(mut self, registry: kernel::adapters::AdapterRegistry) -> Self {
        self.ctx.registry = registry;
        self
    }

    /// The resolver used to locate a model's bytes for coldboot (and, for the
    /// embedded llama router, at request time).
    pub fn with_resolver(mut self, resolver: Arc<dyn kernel::registry::ModelResolver>) -> Self {
        self.ctx.resolver = resolver;
        self
    }
}

/// HF pull wiring — present only with `hf-download` + an engine wing.
#[cfg(all(feature = "coldboot", feature = "hf-download"))]
impl ProvisioningSupervisor {
    /// The puller HF GGUF plans stream model files through.
    pub fn with_puller(mut self, puller: Arc<crate::registry::HfHubPuller>) -> Self {
        self.ctx.puller = Some(puller);
        self
    }
}

/// The routing engine (`gateway`) consults readiness through this port; the
/// supervisor answers from its live slot map. Compile-time this crate depends
/// only on `kernel` (the trait); the gateway calls back via `dyn` dispatch.
#[async_trait::async_trait]
impl kernel::ReadinessProbe for ProvisioningSupervisor {
    async fn phase(&self, model: &str) -> ProvisionPhase {
        self.phase_of(model)
    }

    async fn status_all(&self) -> Vec<(String, ProvisionPhase)> {
        self.all_phases()
    }
}

/// Run one provisioning job to completion, driving `tx` through the plan's
/// phases. Holds a semaphore permit for the job's lifetime so at most
/// `max_concurrent` run at once.
async fn run_job(
    model: String,
    plan: ProvisionPlan,
    tx: watch::Sender<ProvisionPhase>,
    sem: Arc<Semaphore>,
    #[cfg(feature = "coldboot")] ctx: ColdbootCtx,
) {
    // `acquire_owned` errs only if the semaphore is closed, which we never do;
    // the permit releases on return (and on panic).
    let _permit = sem.acquire_owned().await;
    tracing::debug!(model = %model, "provisioning job started");
    match plan {
        ProvisionPlan::Scripted(scripted) => run_scripted(scripted, &tx).await,
        #[cfg(all(feature = "hf-download", feature = "llama-cpp"))]
        ProvisionPlan::HfGguf { spec } => coldboot::run_hf_gguf(model, spec, &tx, &ctx).await,
        #[cfg(feature = "llama-cpp")]
        ProvisionPlan::EmbeddedGguf => coldboot::run_embedded_gguf(model, &tx, &ctx).await,
        #[cfg(feature = "fastembed")]
        ProvisionPlan::Fastembed { config } => {
            coldboot::run_fastembed(model, config, &tx, &ctx).await
        }
        #[cfg(feature = "ort")]
        ProvisionPlan::Ort { config } => coldboot::run_ort(model, config, &tx, &ctx).await,
        #[cfg(feature = "kokoro")]
        ProvisionPlan::Kokoro { config } => coldboot::run_kokoro(model, config, &tx, &ctx).await,
        #[cfg(all(feature = "hf-download", feature = "kokoro"))]
        ProvisionPlan::HfKokoro { spec, config } => {
            coldboot::run_hf_kokoro(model, spec, config, &tx, &ctx).await
        }
    }
}

/// Drive a [`ScriptedPlan`]'s phases through the channel, in order.
async fn run_scripted(plan: ScriptedPlan, tx: &watch::Sender<ProvisionPhase>) {
    plan.record_run();
    for phase in plan.steps.iter() {
        // Latest-value channel: overwrite with the next phase.
        let _ = tx.send_replace(phase.clone());
        // Give an actively-polling subscriber a chance to observe this phase
        // before the next overwrite coalesces it away.
        tokio::task::yield_now().await;
    }
}

/// The real pull / coldboot / register job bodies, one per provider. Compiled
/// only with an engine wing enabled (`coldboot`); the default build carries none
/// of this. Each drives the phase channel and, on failure, ends at `Failed`.
#[cfg(feature = "coldboot")]
mod coldboot {
    use super::*;
    use kernel::registry::ModelEntry;

    /// Overwrite the current phase (latest-value channel).
    fn emit(tx: &watch::Sender<ProvisionPhase>, phase: ProvisionPhase) {
        let _ = tx.send_replace(phase);
    }

    /// Resolve a model id to its on-disk entry via the shared resolver, mapping
    /// "not found" and backend errors to an actionable message.
    async fn resolve_entry(model: &str, ctx: &ColdbootCtx) -> Result<ModelEntry, String> {
        match ctx.resolver.resolve(model).await {
            Ok(Some(entry)) => Ok(entry),
            Ok(None) => Err(format!("model '{model}' is not known to any resolver")),
            Err(e) => Err(format!("resolving '{model}': {e}")),
        }
    }

    /// Register the single embedded llama.cpp router (idempotent). It serves
    /// every GGUF model lazily via the resolver, so one registration covers all
    /// GGUF plans.
    #[cfg(feature = "llama-cpp")]
    async fn register_embedded_llama(ctx: &ColdbootCtx) -> Result<(), String> {
        let adapter = local_providers::adapters::EmbeddedLlamaAdapter::with_shared_backend(
            "embedded-llama",
            ctx.resolver.clone(),
        )
        .map_err(|e| e.to_string())?;
        ctx.registry.register(Arc::new(adapter)).await;
        Ok(())
    }

    /// Pull a GGUF from HF (streaming progress), verify it resolves, then stand
    /// up + register the embedded llama router.
    #[cfg(all(feature = "hf-download", feature = "llama-cpp"))]
    pub(super) async fn run_hf_gguf(
        model: String,
        spec: crate::registry::PullSpec,
        tx: &watch::Sender<ProvisionPhase>,
        ctx: &ColdbootCtx,
    ) {
        let Some(puller) = ctx.puller.clone() else {
            emit(
                tx,
                ProvisionPhase::Failed {
                    error: "HF GGUF plan needs a puller; call `with_puller` on the supervisor"
                        .to_string(),
                },
            );
            return;
        };

        emit(
            tx,
            ProvisionPhase::Downloading {
                done: 0,
                total: None,
            },
        );
        let mut on_progress = {
            let tx = tx.clone();
            move |done, total| {
                let _ = tx.send_replace(ProvisionPhase::Downloading { done, total });
            }
        };
        if let Err(e) = puller.pull_with_progress(&spec, &mut on_progress).await {
            emit(
                tx,
                ProvisionPhase::Failed {
                    error: e.to_string(),
                },
            );
            return;
        }

        // The pull registered the bytes in the managed store; confirm the shared
        // resolver sees the model before we call it ready.
        emit(tx, ProvisionPhase::Verifying);
        if let Err(e) = resolve_entry(&model, ctx).await {
            emit(tx, ProvisionPhase::Failed { error: e });
            return;
        }

        emit(tx, ProvisionPhase::Loading);
        if let Err(e) = register_embedded_llama(ctx).await {
            emit(tx, ProvisionPhase::Failed { error: e });
            return;
        }
        emit(tx, ProvisionPhase::Ready);
    }

    /// Coldboot an already-resolvable GGUF (no download) behind the embedded
    /// llama router.
    #[cfg(feature = "llama-cpp")]
    pub(super) async fn run_embedded_gguf(
        model: String,
        tx: &watch::Sender<ProvisionPhase>,
        ctx: &ColdbootCtx,
    ) {
        emit(tx, ProvisionPhase::Verifying);
        if let Err(e) = resolve_entry(&model, ctx).await {
            emit(tx, ProvisionPhase::Failed { error: e });
            return;
        }
        emit(tx, ProvisionPhase::Loading);
        if let Err(e) = register_embedded_llama(ctx).await {
            emit(tx, ProvisionPhase::Failed { error: e });
            return;
        }
        emit(tx, ProvisionPhase::Ready);
    }

    /// Load an ONNX embedding model with fastembed and register it.
    #[cfg(feature = "fastembed")]
    pub(super) async fn run_fastembed(
        model: String,
        config: local_providers::adapters::FastembedConfig,
        tx: &watch::Sender<ProvisionPhase>,
        ctx: &ColdbootCtx,
    ) {
        emit(tx, ProvisionPhase::Verifying);
        let entry = match resolve_entry(&model, ctx).await {
            Ok(entry) => entry,
            Err(e) => {
                emit(tx, ProvisionPhase::Failed { error: e });
                return;
            }
        };

        emit(tx, ProvisionPhase::Loading);
        // Native load is blocking — keep it off the async worker threads.
        let loaded = tokio::task::spawn_blocking(move || {
            local_providers::adapters::FastembedAdapter::load(&entry, config)
        })
        .await;
        let adapter = match loaded {
            Ok(Ok(adapter)) => adapter,
            Ok(Err(e)) => {
                emit(
                    tx,
                    ProvisionPhase::Failed {
                        error: e.to_string(),
                    },
                );
                return;
            }
            Err(e) => {
                emit(
                    tx,
                    ProvisionPhase::Failed {
                        error: format!("load task failed: {e}"),
                    },
                );
                return;
            }
        };
        ctx.registry.register(Arc::new(adapter)).await;
        emit(tx, ProvisionPhase::Ready);
    }

    /// Load an ONNX embedding model with ONNX Runtime and register it.
    #[cfg(feature = "ort")]
    pub(super) async fn run_ort(
        model: String,
        config: local_providers::adapters::OrtConfig,
        tx: &watch::Sender<ProvisionPhase>,
        ctx: &ColdbootCtx,
    ) {
        emit(tx, ProvisionPhase::Verifying);
        let entry = match resolve_entry(&model, ctx).await {
            Ok(entry) => entry,
            Err(e) => {
                emit(tx, ProvisionPhase::Failed { error: e });
                return;
            }
        };

        emit(tx, ProvisionPhase::Loading);
        let loaded = tokio::task::spawn_blocking(move || {
            local_providers::adapters::OrtAdapter::load(&entry, config)
        })
        .await;
        let adapter = match loaded {
            Ok(Ok(adapter)) => adapter,
            Ok(Err(e)) => {
                emit(
                    tx,
                    ProvisionPhase::Failed {
                        error: e.to_string(),
                    },
                );
                return;
            }
            Err(e) => {
                emit(
                    tx,
                    ProvisionPhase::Failed {
                        error: format!("load task failed: {e}"),
                    },
                );
                return;
            }
        };
        ctx.registry.register(Arc::new(adapter)).await;
        emit(tx, ProvisionPhase::Ready);
    }

    /// Load Kokoro-82M for TTS and register it as a `TtsModel` (gh#23). Mirrors
    /// [`run_ort`]: resolve the model dir, load the adapter off the async worker
    /// threads (native ORT), and register.
    #[cfg(feature = "kokoro")]
    pub(super) async fn run_kokoro(
        model: String,
        config: local_providers::adapters::KokoroConfig,
        tx: &watch::Sender<ProvisionPhase>,
        ctx: &ColdbootCtx,
    ) {
        emit(tx, ProvisionPhase::Verifying);
        let entry = match resolve_entry(&model, ctx).await {
            Ok(entry) => entry,
            Err(e) => {
                emit(tx, ProvisionPhase::Failed { error: e });
                return;
            }
        };

        emit(tx, ProvisionPhase::Loading);
        let loaded = tokio::task::spawn_blocking(move || {
            local_providers::adapters::KokoroAdapter::load(&entry, config)
        })
        .await;
        let adapter = match loaded {
            Ok(Ok(adapter)) => adapter,
            Ok(Err(e)) => {
                emit(
                    tx,
                    ProvisionPhase::Failed {
                        error: e.to_string(),
                    },
                );
                return;
            }
            Err(e) => {
                emit(
                    tx,
                    ProvisionPhase::Failed {
                        error: format!("load task failed: {e}"),
                    },
                );
                return;
            }
        };
        ctx.registry.register(Arc::new(adapter)).await;
        emit(tx, ProvisionPhase::Ready);
    }

    /// Pull Kokoro's model + voice files from HF (streaming progress), then load
    /// the adapter and register it. Mirrors [`run_hf_gguf`] (pull) + [`run_kokoro`]
    /// (load). The lexicon isn't on the model repo — `config` points at it as a
    /// sibling the operator supplies.
    #[cfg(all(feature = "hf-download", feature = "kokoro"))]
    pub(super) async fn run_hf_kokoro(
        model: String,
        spec: crate::registry::PullSpec,
        config: local_providers::adapters::KokoroConfig,
        tx: &watch::Sender<ProvisionPhase>,
        ctx: &ColdbootCtx,
    ) {
        let Some(puller) = ctx.puller.clone() else {
            emit(
                tx,
                ProvisionPhase::Failed {
                    error: "HF Kokoro plan needs a puller; call `with_puller` on the supervisor"
                        .to_string(),
                },
            );
            return;
        };

        emit(
            tx,
            ProvisionPhase::Downloading {
                done: 0,
                total: None,
            },
        );
        let mut on_progress = {
            let tx = tx.clone();
            move |done, total| {
                let _ = tx.send_replace(ProvisionPhase::Downloading { done, total });
            }
        };
        if let Err(e) = puller.pull_with_progress(&spec, &mut on_progress).await {
            emit(
                tx,
                ProvisionPhase::Failed {
                    error: e.to_string(),
                },
            );
            return;
        }

        emit(tx, ProvisionPhase::Verifying);
        let entry = match resolve_entry(&model, ctx).await {
            Ok(entry) => entry,
            Err(e) => {
                emit(tx, ProvisionPhase::Failed { error: e });
                return;
            }
        };

        emit(tx, ProvisionPhase::Loading);
        let loaded = tokio::task::spawn_blocking(move || {
            local_providers::adapters::KokoroAdapter::load(&entry, config)
        })
        .await;
        let adapter = match loaded {
            Ok(Ok(adapter)) => adapter,
            Ok(Err(e)) => {
                emit(
                    tx,
                    ProvisionPhase::Failed {
                        error: e.to_string(),
                    },
                );
                return;
            }
            Err(e) => {
                emit(
                    tx,
                    ProvisionPhase::Failed {
                        error: format!("load task failed: {e}"),
                    },
                );
                return;
            }
        };
        ctx.registry.register(Arc::new(adapter)).await;
        emit(tx, ProvisionPhase::Ready);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    /// A supervisor with one scripted model whose job runs are counted.
    fn scripted_supervisor(
        model: &str,
        steps: Vec<ProvisionPhase>,
        counter: Arc<AtomicUsize>,
    ) -> ProvisioningSupervisor {
        let plan = ProvisionPlan::Scripted(ScriptedPlan::new(steps).with_run_counter(counter));
        let mut plans = HashMap::new();
        plans.insert(model.to_string(), plan);
        ProvisioningSupervisor::new(plans, 4)
    }

    #[tokio::test]
    async fn ensure_on_absent_queues_and_spawns_one_job() {
        let runs = Arc::new(AtomicUsize::new(0));
        let sup = scripted_supervisor(
            "m",
            vec![
                ProvisionPhase::Downloading {
                    done: 1,
                    total: Some(1),
                },
                ProvisionPhase::Ready,
            ],
            runs.clone(),
        );

        let handle = sup.ensure("m", EnsureOpts::default());
        // On a current-thread runtime the spawned job hasn't been polled yet (no
        // await since `ensure`), so the model is freshly Queued.
        assert_eq!(handle.phase(), ProvisionPhase::Queued);

        handle.wait_ready().await.unwrap();
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert_eq!(sup.status("m").await, ProvisionPhase::Ready);
    }

    #[tokio::test]
    async fn two_ensures_for_same_model_share_one_job() {
        let runs = Arc::new(AtomicUsize::new(0));
        let sup = scripted_supervisor("m", vec![ProvisionPhase::Ready], runs.clone());

        // The second `ensure` lands before the first job is scheduled (no await
        // between them), so it must find the in-flight slot and subscribe.
        let h1 = sup.ensure("m", EnsureOpts::default());
        let h2 = sup.ensure("m", EnsureOpts::default());
        h1.wait_ready().await.unwrap();
        h2.wait_ready().await.unwrap();

        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "dedup: exactly one job per model id"
        );
    }

    #[tokio::test]
    async fn status_and_status_all_reflect_current_phase() {
        let runs = Arc::new(AtomicUsize::new(0));
        let sup = scripted_supervisor("m", vec![ProvisionPhase::Ready], runs);

        // Untouched: no slot yet.
        assert!(sup.status_all().await.is_empty());
        assert_eq!(sup.status("m").await, ProvisionPhase::Absent);

        let h = sup.ensure("m", EnsureOpts::default());
        h.wait_ready().await.unwrap();

        assert_eq!(sup.status("m").await, ProvisionPhase::Ready);
        assert_eq!(
            sup.status_all().await,
            vec![("m".to_string(), ProvisionPhase::Ready)]
        );
    }

    #[tokio::test]
    async fn ensure_without_a_plan_reports_absent() {
        let sup = ProvisioningSupervisor::new(HashMap::new(), 2);
        let h = sup.ensure("unknown", EnsureOpts::default());
        assert_eq!(h.phase(), ProvisionPhase::Absent);
    }

    #[tokio::test]
    async fn events_stream_observes_the_ordered_phase_sequence() {
        let runs = Arc::new(AtomicUsize::new(0));
        // The channel starts at Queued (set by `ensure`); the plan drives the
        // rest, so the stream should walk Queued → … → Ready.
        let sup = scripted_supervisor(
            "m",
            vec![
                ProvisionPhase::Downloading {
                    done: 50,
                    total: Some(100),
                },
                ProvisionPhase::Verifying,
                ProvisionPhase::Loading,
                ProvisionPhase::Ready,
            ],
            runs,
        );

        let handle = sup.ensure("m", EnsureOpts::default());
        let mut stream = Box::pin(handle.events());

        let mut observed = Vec::new();
        while let Some(event) = stream.next().await {
            assert_eq!(event.model, "m");
            observed.push(event.phase.clone());
            if matches!(event.phase, ProvisionPhase::Ready) {
                break; // the channel stays open (slot holds a sender) past Ready.
            }
        }

        assert_eq!(
            observed,
            vec![
                ProvisionPhase::Queued,
                ProvisionPhase::Downloading {
                    done: 50,
                    total: Some(100),
                },
                ProvisionPhase::Verifying,
                ProvisionPhase::Loading,
                ProvisionPhase::Ready,
            ]
        );
    }

    #[tokio::test]
    async fn late_subscriber_immediately_sees_the_current_phase() {
        let runs = Arc::new(AtomicUsize::new(0));
        let sup = scripted_supervisor("m", vec![ProvisionPhase::Ready], runs.clone());

        // Drive to Ready first.
        sup.ensure("m", EnsureOpts::default())
            .wait_ready()
            .await
            .unwrap();

        // A handle acquired now subscribes to the finished slot and must see
        // Ready straight away — both as a snapshot and as the first event.
        let late = sup.ensure("m", EnsureOpts::default());
        assert_eq!(late.phase(), ProvisionPhase::Ready);
        let first = Box::pin(late.events()).next().await.unwrap();
        assert_eq!(first.phase, ProvisionPhase::Ready);

        // No second job ran — the late `ensure` only subscribed.
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn wait_ready_errors_on_failed() {
        let runs = Arc::new(AtomicUsize::new(0));
        let sup = scripted_supervisor(
            "m",
            vec![
                ProvisionPhase::Downloading {
                    done: 1,
                    total: Some(10),
                },
                ProvisionPhase::Failed {
                    error: "disk full".to_string(),
                },
            ],
            runs,
        );

        let err = sup
            .ensure("m", EnsureOpts::default())
            .wait_ready()
            .await
            .unwrap_err();
        let ProvisionError::Failed(msg) = err;
        assert!(msg.contains("disk full"), "got: {msg}");
        assert_eq!(
            sup.status("m").await,
            ProvisionPhase::Failed {
                error: "disk full".to_string()
            }
        );
    }

    #[tokio::test]
    async fn usable_as_a_dyn_readiness_probe() {
        use kernel::ReadinessProbe;

        let runs = Arc::new(AtomicUsize::new(0));
        let sup = scripted_supervisor("m", vec![ProvisionPhase::Ready], runs);
        sup.ensure("m", EnsureOpts::default())
            .wait_ready()
            .await
            .unwrap();

        let probe: Arc<dyn ReadinessProbe> = Arc::new(sup);
        assert_eq!(probe.phase("m").await, ProvisionPhase::Ready);
        assert_eq!(probe.phase("never-asked").await, ProvisionPhase::Absent);
        assert_eq!(
            probe.status_all().await,
            vec![("m".to_string(), ProvisionPhase::Ready)]
        );
    }

    /// Real end-to-end: pull a tiny public GGUF from the HF hub through an
    /// `HfGguf` plan and drive it to `Ready`, asserting the streamed
    /// `Downloading { done }` ticks never go backwards. Ignored — needs network
    /// and initialises the llama.cpp backend.
    ///
    /// Run: `cargo test -p sensei-local-engine --features hf-download,llama-cpp -- --ignored`
    #[cfg(all(feature = "hf-download", feature = "llama-cpp"))]
    #[tokio::test]
    #[ignore = "network: downloads a tiny public GGUF from the HF hub"]
    async fn e2e_hf_gguf_plan_streams_progress_to_ready() {
        use crate::registry::{ChainedResolver, HfHubPuller, ManagedResolver, PullSpec};
        use kernel::registry::ModelFormat;

        let dir = tempfile::TempDir::new().unwrap();
        let root = dir.path().to_path_buf();

        // The puller writes into the managed store; the resolver reads the same
        // store, so the pulled model resolves for the coldboot verify step.
        let puller = Arc::new(HfHubPuller::new(ManagedResolver::new(&root), None));
        let resolver: Arc<dyn kernel::registry::ModelResolver> =
            Arc::new(ChainedResolver::new().push(Arc::new(ManagedResolver::new(&root))));

        let spec = PullSpec {
            repo: "ggml-org/models".into(),
            revision: None,
            id: "tinyllamas-stories260k".into(),
            name: Some("TinyLlamas stories260K".into()),
            format: ModelFormat::Gguf,
            files: vec!["tinyllamas/stories260K.gguf".into()],
        };

        let mut plans = HashMap::new();
        plans.insert(
            "tinyllamas-stories260k".to_string(),
            ProvisionPlan::HfGguf { spec },
        );
        let sup = ProvisioningSupervisor::new(plans, 2)
            .with_registry(kernel::adapters::AdapterRegistry::new())
            .with_resolver(resolver)
            .with_puller(puller);

        let handle = sup.ensure("tinyllamas-stories260k", EnsureOpts::default());
        let mut stream = Box::pin(handle.events());

        let mut last_done = 0u64;
        let mut saw_downloading = false;
        let mut reached_ready = false;
        while let Some(ev) = stream.next().await {
            match ev.phase {
                ProvisionPhase::Downloading { done, .. } => {
                    assert!(
                        done >= last_done,
                        "download progress went backwards: {done} < {last_done}"
                    );
                    last_done = done;
                    saw_downloading = true;
                }
                ProvisionPhase::Ready => {
                    reached_ready = true;
                    break;
                }
                ProvisionPhase::Failed { error } => panic!("provisioning failed: {error}"),
                _ => {}
            }
        }
        assert!(saw_downloading, "expected at least one Downloading tick");
        assert!(reached_ready, "expected to reach Ready");
    }
}
