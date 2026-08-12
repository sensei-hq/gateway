---
title: SP-2 slice 5 — registry hot-reload
doctype: design
module: orchestrator
spec: SP-2
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§F registry, D24 config_versions, SP-2); ./2026-08-11-sp2-config-source-design.md (slice 1, D5 config-version deferred to hot-reload)
date: 2026-08-12
---

# SP-2 slice 5 — registry hot-reload

## 1. Goal

Reload the agent/skill/tool config from a `ConfigSource` at runtime — without
restarting the process — and atomically swap in a new `Arc<Registry>`, bumping a
**config generation** so the change is observable and safely fenced. This is
SP-2's final slice; it makes the registry a live, operator-reloadable component.
`ConfigSource` (slice 1) is the seam; `Registry::from_config` (slice 1) is the
validated assembly; this slice adds the swap + version.

## 2. SP-2 slicing (context)

1. `ConfigSource` adapter + `FilesystemConfigSource` (slice 1 — done).
2. role/kind → chain resolution (slice 2 — done).
3. tool permission declarations (slice 3 — done).
4. skill/tool activation policy, Q4 (slice 4 — done).
5. **This slice** — registry hot-reload (reload + swap + version bump). **Closes SP-2.**

## 3. Background & impact review

- **Executor holds** `registry: Arc<Registry>` (immutable, injected via
  `with_registry`) and `version: String` (injected at `new`). `run` journals
  `RunStarted { version: self.version }`; `start` (resume) **already** refuses a
  resume whose recorded `RunStarted.version` differs, with
  `OrchestratorError::VersionFenceMismatch { recorded, current }`
  (`executor/mod.rs:262-272`). **This existing fence is the exact safety machinery
  hot-reload needs** — no new fence.
- **`Registry::from_config`** (slice 1) already loads-and-validates (dup names,
  dangling skill/tool refs, chain routability, permission grant⊇need). A reload
  reuses it, so a bad config edit fails the reload — never a run.
- **Determinism (D5, slice 2):** config *content* is fenced per-node by
  `agent_input_hash`. Slice-1 D5 deferred "a config-version + reload" to this slice.
- **Impact: additive.** New `RegistryHandle` type in `orchestrator-core` (pure —
  std `RwLock`/`Arc`, no I/O; the I/O is the injected `ConfigSource`); the executor
  gains one optional field + one builder + `#[derive(Clone)]` + a per-run pin at the
  two entry points. **No handle wired ⇒ byte-identical** to today (the
  `with_registry` path and `version` string are unchanged).

## 4. Design

### 4.1 `RegistryHandle` (`orchestrator-core`)

```rust
/// A cheaply-clonable handle to a live, swappable `Registry` + a monotonic config
/// generation. Clones share one `Arc<RwLock<…>>`, so an operator's clone and the
/// executor's clone observe the same swaps.
#[derive(Clone)]
pub struct RegistryHandle {
    inner: Arc<RwLock<(Arc<Registry>, u64)>>, // (current registry, generation)
}

impl RegistryHandle {
    /// A handle over `registry` at generation 0.
    pub fn new(registry: Registry) -> Self;

    /// The current registry (read-lock, clones the `Arc`).
    pub fn current(&self) -> Arc<Registry>;

    /// The current config generation.
    pub fn generation(&self) -> u64;

    /// The atomic `(registry, generation)` pair — read together under one lock so
    /// a run pins a consistent snapshot.
    pub fn snapshot(&self) -> (Arc<Registry>, u64);

    /// Reload from `source`, atomically swapping in the new registry and bumping
    /// the generation (returns the new generation). **Validated + last-good:**
    /// `source.load().await` then `Registry::from_config` (which validates) run
    /// FIRST; only on success is the write-lock taken to swap + increment. A failed
    /// load/validate returns `Err` with the old registry still live. The `.await`
    /// happens OUTSIDE the lock (never held across it).
    pub async fn reload(&self, source: &dyn ConfigSource) -> Result<u64, OrchestratorError>;
}
```

Reload body (shape):
```rust
let cfg = source.load().await?;                 // I/O, no lock held
let next = Registry::from_config(cfg)?;         // validate; err → return, no swap
let mut w = self.inner.write().unwrap_or_else(|e| e.into_inner());
w.0 = Arc::new(next);
w.1 += 1;
Ok(w.1)
```

### 4.2 Executor wiring

- New field `handle: Option<RegistryHandle>` + builder `with_registry_handle(self,
  handle) -> Self`. `Executor` derives `Clone` (all fields are `Arc`/`Option<Arc>`/
  `String`/`usize`/`RegistryHandle`, all `Clone`).
- **Per-run pin** at the top of BOTH `run` and `start`:
  ```rust
  if let Some(h) = &self.handle {
      let (registry, generation) = h.snapshot();
      let pinned = self.clone().pinned(registry, generation); // sets registry+version, clears handle
      return pinned.run(run, graph).await;   // (or .start) — pinned.handle is None ⇒ falls through
  }
  ```
  where `pinned(registry, gen)` sets `self.registry = registry`,
  `self.version = format!("{}#cfg{}", self.version, gen)`, `self.handle = None`. The
  recursion terminates (the pinned clone has no handle). Every existing internal use
  of `self.registry`/`self.version` then reads the pinned snapshot, **fixed for the
  whole run**. Cloning is cheap (Arc clones) and happens only when a handle is wired.
- `start`'s internal fresh-run delegation (`self.run(...)`) runs on the pinned copy,
  so it stamps the pinned version consistently.

### 4.3 Data flow + fence

```
operator:  handle.reload(&source).await?           // gen 3 → 4 (validated, atomic, last-good)
new run:   exec.with_registry_handle(h).run(...)    // pins (reg@4, "v1#cfg4") → RunStarted{"v1#cfg4"}
resume A (started @cfg3) on the reloaded exec:      // recorded "v1#cfg3" ≠ current "v1#cfg4"
           → VersionFenceMismatch (loud, safe: one config generation per run)
```

### 4.4 Decisions

- **D1 — one config generation per run** (approved). The config generation is folded
  into the run's fence version (`"{base}#cfg{gen}"`), so a run uses exactly one
  generation and a post-reload resume of an in-flight run is refused loud via the
  existing `VersionFenceMismatch`. Never a mixed-config run.
- **D2 — version format `"{base}#cfg{gen}"`** (approved (a)). A no-handle executor
  keeps its bare `version` (e.g. `"v1"`) → existing behavior/tests byte-identical.
- **D3 — std `RwLock<(Arc<Registry>, u64)>`** (approved (b)); no `arc-swap` dep.
  Reload is a rare operator action, not a hot path; the `.await` is outside the lock.
- **D4 — atomic, validated, last-good reload.** Load + `from_config` validate BEFORE
  the swap; a bad reload returns `Err` and keeps the last-good registry live.
- **D5 — reuse the existing version fence** — no new determinism machinery; the
  `agent_input_hash` fence stays the finer second line.
- **D6 — per-run pin** (snapshot once at entry) so a mid-run reload can never change
  config within a run.

## 5. Deferred (stated)

- **Version-pinned resume** — resuming an in-flight run against its ORIGINAL config
  generation after a reload (today it's refused loud; the operator would rebuild a
  handle/executor pinned to that generation). A convenience for this lands later.
- **`on_config_reloaded(generation)` hook** — reload is on the handle (no hooks
  wiring); observability is the returned generation + `generation()` getter for now.
- **File-watch / auto-reload** — reload stays an explicit operator call; a watcher is
  out of scope.
- **Persistent config version** — SP-DATA's `config_versions` / `bump_config_version`
  (a durable, cross-process generation) layers on later; this slice's generation is
  in-process.

## 6. Acceptance criteria (TDD)

1. **Handle basics.** `RegistryHandle::new(reg)` → `generation() == 0`, `current()`
   resolves `reg`'s agents/skills/tools; `snapshot()` returns the `(registry, gen)`
   pair.
2. **Reload swaps + bumps.** `reload(&source)` over an `InMemoryConfigSource`/
   `FilesystemConfigSource` whose config adds an agent/skill absent from the initial
   registry → returns generation 1, and `current()` now resolves the new agent/skill
   (the old one's presence per the new config).
3. **Validated, last-good.** `reload` with a config that fails `from_config`
   (a dangling tool ref, or a dup name) → `Err`, and `current()` + `generation()` are
   UNCHANGED (the old registry stays live — never swaps in broken config).
4. **Executor uses the live config.** An `Executor::with_registry_handle(h)` whose
   run references an agent that only exists AFTER a reload: before reload the run
   fails (`UnknownAgent`); after `h.reload(...)`, a NEW run resolves and drives it.
5. **Version fence on reload.** Start run A on a handle-wired executor at gen 0 (the
   journal records `"{base}#cfg0"`); `h.reload(...)` → gen 1; `start(A)` on the same
   executor → `VersionFenceMismatch { recorded: "…#cfg0", current: "…#cfg1" }` (loud;
   the in-flight run is not silently resumed under new config).
6. **Per-run pin (each run stamps the generation live at ITS start).** Drive run A
   on a handle-wired executor (pins gen 0 → journals `"{base}#cfg0"`); `h.reload(...)`
   → gen 1; drive a DIFFERENT run B on the same executor (pins gen 1 → journals
   `"{base}#cfg1"`). Assert each run's recorded `RunStarted.version` reflects the
   generation live at its own start — the snapshot is taken once per run at entry, so
   a later reload never retroactively changes a prior run's pinned config.
7. **No-handle byte-identical.** With no handle wired, `run`/`start`/`version` behave
   exactly as before (existing tests unchanged; `version` stays the bare string, no
   `#cfg` suffix).
