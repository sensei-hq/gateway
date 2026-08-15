---
title: SP-4 slice 5 — Exactly-once hardening (idempotency-key core)
doctype: design
module: orchestrator
spec: SP-4
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§7.3 two-phase + in_doubt→reconcile, §13 enforcement, R3 reconciliation providers); ./2026-08-10-sp1-slice4-observation-mutation-design.md (the two-phase Mutation + `ReconcileProvider` this hardens); ./2026-08-14-sp4-permission-enforcement-design.md + ./2026-08-14-sp4-secret-redaction-design.md (the SP-4 slices this follows; the `Tool::required`/additive-default-method pattern)
date: 2026-08-15
---

# SP-4 slice 5 — Exactly-once (idempotency-key core)

## 1. Goal

Make the SP-1-slice-4 two-phase Mutation mechanism deliver **real, provider-side exactly-once**
by closing the one structural gap: the `idempotency_key` is computed and journaled internally
but **never reaches the tool**, so a real tool cannot pass it to an external API for that API to
dedupe on. This slice threads the key to the tool's execution (`call_ctx` + `ToolContext`),
lets a tool **supply/override** the key (`Tool::idempotency_key(args)`, pure), and makes the
in-doubt reconcile path **read the journaled key** (not recompute) so it queries the external
system by the exact key that was used — the R3 "provider-specific idempotency/status API"
pattern, proven with a demo status-query reconciler.

**Scope (user-chosen): the idempotency-key core only.** Saga/compensation, retry-under-key, and
real provider API integrations are **deferred** to later slices (§6). This slice is
self-contained (no new infrastructure), builds on the shipped two-phase machinery, and is
byte-identical when no tool opts in.

## 2. Background & impact review

- **Shipped (SP-1 s4):** `NodeKind`/tool Mutation effects are two-phase — `mutation_tool_effect`
  journals `EffectIntent{node, effect_id, idempotency_key, args_hash, seq}` **before** the side
  effect, then `record_tool_effect` runs the tool + journals `EffectRecorded`. On resume, an
  Intent without a Recorded is **in-doubt**: `reconcile_in_doubt` asks a per-tool
  `ReconcileProvider::reconcile(idempotency_key: &str, args: &Value) -> {Confirmed(Value),
  NotApplied, Indeterminate}` (absent ⇒ `Indeterminate` ⇒ durable `RunPaused`, never guess).
  `idempotency_key(effect_id, args_hash) = sha256(effect_id | args_hash)` — **structural**.
- **The gap:** `Tool::call(&self, args)` receives only `args`. The idempotency key is computed
  in `mutation_tool_effect` and journaled, but the tool never sees it — so a real money-moving
  tool cannot send it to its provider (Stripe `Idempotency-Key`, a booking API's dedup token,
  …). Provider-side exactly-once is therefore impossible today; the mechanism only guarantees
  **resume-internal** exactly-once (a completed Mutation replays from the memo; an in-doubt one
  reconciles). `reconcile_in_doubt` also **recomputes** the structural key rather than reading
  what was journaled — fine for the structural key, but unsafe once keys can be author-supplied.
- **Impact:** additive — two default trait methods on `Tool` (existing impls unchanged) + a
  `ToolContext`; the executor computes the effective key, threads it via `call_ctx`, and folds
  the journaled key so reconcile reads it. Default behavior (no `idempotency_key`/`call_ctx`
  override) is **byte-identical** (the structural key is already journaled today). The one
  non-additive-shaped change is the `fold.intents` set→map migration (§4.3), behavior-preserving.

## 4. Design

### 4.1 Types (`orchestrator-core`)

```rust
/// Per-call execution context for a tool (SP-4 s5). Carries the idempotency key the
/// executor journaled in the `EffectIntent`, so a tool can pass it to an external API
/// for provider-side dedup, and the effect id for correlation.
pub struct ToolContext {
    pub idempotency_key: String,
    pub effect_id: EffectId,
}

pub trait Tool: Send + Sync {
    fn spec(&self) -> ToolSpec;
    fn call(&self, args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError>;

    /// Execute with the per-call context (SP-4 s5). Default ignores `ctx` and delegates
    /// to `call` ⇒ existing tools are byte-identical. A tool that does provider-side
    /// idempotency overrides this to send `ctx.idempotency_key` to its external API.
    fn call_ctx(
        &self,
        args: serde_json::Value,
        _ctx: &ToolContext,
    ) -> Result<serde_json::Value, OrchestratorError> {
        self.call(args)
    }

    /// Author-supplied idempotency key for THIS call — MUST be pure over `args` (so it is
    /// stable across resume). Default `None` ⇒ the executor uses the structural key
    /// `sha256(effect_id | args_hash)`. Override to use a domain key (a booking ref, a
    /// payment token derived from args).
    fn idempotency_key(&self, _args: &serde_json::Value) -> Option<String> {
        None
    }
}
```
(`Tool::required` from s1 and `call`/`spec` are unchanged. `ReconcileProvider` is unchanged —
it already takes the key.)

### 4.2 The effective key + threading (`executor/`)

- **Effective key** (in `mutation_tool_effect`, before the Intent): `let key =
  self.tools.idempotency_key_of(&call.name, &args).unwrap_or_else(|| idempotency_key(teid,
  tih));`. This exact `key` is journaled in `EffectIntent.idempotency_key` (unchanged two-phase
  order) **and** placed in the `ToolContext`.
- **Threading:** `record_tool_effect` builds `ToolContext { idempotency_key, effect_id: teid }`
  and calls `self.tools.execute_ctx(&call.name, args, &ctx)` → `tool.call_ctx(args, &ctx)`
  (replacing the current `execute` → `call`). So the tool receives **the same key that was
  journaled**, to send to its external API. `record_tool_effect` is shared by all effect
  classes; Pure/Observation calls (no Intent) pass the structural key in the context, which a
  non-idempotent tool ignores via the default `call_ctx`.
- `ToolRegistry` gains `idempotency_key_of(name, args) -> Option<String>` (mirrors
  `required_of`/`spec_of`; unknown tool → `None`) and `execute_ctx(name, args, &ctx)`.

### 4.3 Reconcile reads the journaled key (fold `intents` set→map)

Today `reconcile_in_doubt` **recomputes** `idempotency_key(teid, tih)`. Once keys can be
author-supplied, recompute could drift from what was actually sent to the provider (e.g. tool
code changed between runs). Fix: **fold the journaled key** so reconcile reads it.
- `Fold.intents` changes from `HashSet<EffectId>` to `HashMap<EffectId, String>` (teid →
  journaled `idempotency_key`). The fold populates it from `EffectIntent`; an effect with a
  matching `EffectRecorded` is removed (no longer in-doubt), exactly as today.
- `reconcile_in_doubt` reads `let key = fold.intents.get(teid)` (guaranteed present on the
  in-doubt path) and passes it to `provider.reconcile(key, &args)`. So reconcile
  queries the external system by the **exact key used at execution** — the exactly-once
  correctness crux.
- `mutation_tool_effect`'s in-doubt check (`teid ∈ fold.intents`) is a map `contains_key` —
  behavior-preserving.

### 4.4 Exactly-once semantics + the status-query reconciler (demo, proving the pattern)

The mechanism delivers provider-side exactly-once: the tool sends `ctx.idempotency_key` to the
external API (which dedupes), and on an in-doubt resume the reconcile provider **queries status
by that key**. Since no real external provider exists yet, this slice ships a **demo** proving
the pattern end-to-end:
- A keyed "external system" store (`Arc<Mutex<HashMap<String, Value>>>`, key → recorded output).
- A demo Mutation tool whose `call_ctx` writes `store[ctx.idempotency_key] = output`
  **idempotently** — re-applying the same key returns the recorded output without a second
  effect (provider-side dedup).
- A `StatusQueryReconciler` (a `ReconcileProvider`) whose `reconcile(_, key, _)` does
  `store.get(key)` → `Some(output) ⇒ Confirmed(output)` (already applied — record, don't re-run)
  / `None ⇒ NotApplied` (run once under the standing Intent). This is the R3 provider-status-API
  pattern; a real provider swaps the store query for the vendor's status endpoint.

### 4.5 Determinism & additive

- The effective key is a **pure** function of `(effect_id, args)` (structural) or `args`
  (author), both deterministic across resume ⇒ the key is **stable**, so threading it to
  `call_ctx` changes no memoized output; a completed Mutation still replays its journaled output
  from the memo (no re-execution, no re-send). A tool whose `idempotency_key` is impure would
  break resume — stated as a hard contract on the method.
- **Additive:** default `call_ctx = call` and default `idempotency_key = None` ⇒ the executor
  uses the structural key it already journals today ⇒ **byte-identical** output/journal for every
  existing tool. The `intents` set→map fold is behavior-preserving (same membership; the value is
  additive), so the existing in-doubt/reconcile tests stay green.

## 5. Decisions

- **D1 — additive `call_ctx` default method + `ToolContext`** [approved]: existing tools
  byte-identical; only a provider-side-idempotent tool overrides. Rejected: changing `call`'s
  signature (breaking migration of every impl); injecting the key into `args` (pollutes the
  arg space, perturbs `args_hash`).
- **D2 — `Tool::idempotency_key(args) -> Option<String>`, pure, default structural** [approved]:
  author-supplied domain keys where they map better than the structural key; pure ⇒ replay-stable.
- **D3 — reconcile reads the JOURNALED key (fold `intents` set→map)** [approved]: query the
  external system by the exact key used at execution; robust for author keys (no recompute drift).
- **D4 — demo keyed store + `StatusQueryReconciler` prove the pattern** [approved]: no real
  provider API exists; the demo is the R3 status-query shape a real provider drops into.
- **D5 — scope = idempotency-key core only** [approved]: saga/compensation, retry-under-key, real
  provider integrations are separate later slices.

## 6. Deferred (stated)

- **Saga / compensation** — `(action, compensation)` pairs; a failed multi-step Mutation sequence
  runs compensating undos in reverse. Its own (bigger, structural) slice.
- **Retry-under-key** — bounded retry of a failed Mutation under the same idempotency key (safe
  because the provider dedupes).
- **Real provider integrations** (Stripe/booking/etc. status+idempotency APIs) — the demo proves
  the shape; a real `ReconcileProvider` + tool swap the store for the vendor API.
- **`ToolContext` for non-Mutation effects** — Pure/Observation have no Intent/key; the context
  carries the structural key but they ignore it. Richer context (deadline, cancellation) is future.
- **Author-key validation** — the executor trusts `Tool::idempotency_key` to be pure; a
  version-fence on the key derivation is future.

## 7. Acceptance criteria (TDD)

1. **`idempotency_key` default vs override.** A tool with no override → the executor journals the
   structural `idempotency_key(effect_id, args_hash)` in the `EffectIntent` (byte-identical to
   today). A tool that overrides → the executor journals the **author key** derived from `args`.
2. **`call_ctx` default delegates.** An existing tool (no `call_ctx` override) runs byte-identical
   (its output/journal unchanged). A tool that overrides `call_ctx` receives a `ToolContext` whose
   `idempotency_key` equals the journaled `EffectIntent.idempotency_key` and whose `effect_id`
   equals the effect id.
3. **The tool gets the journaled key.** For a Mutation, the key the tool receives in `call_ctx` ==
   the key journaled in the `EffectIntent` == (on in-doubt resume) the key passed to
   `provider.reconcile` — one key, end to end. (Reconcile reads it from `fold.intents`.)
4. **Fold `intents` map.** `EffectIntent.idempotency_key` is folded into `fold.intents`
   (teid→key); a matching `EffectRecorded` removes the entry (no longer in-doubt), exactly as the
   old set did. Existing in-doubt/reconcile tests pass unchanged.
5. **Exactly-once via status-query (the headline).** A demo keyed-dedup Mutation tool +
   `StatusQueryReconciler`: a run applies the effect (store[key]=output), crashes in-doubt (Intent
   journaled, no Recorded), resumes → the reconciler `store.get(key)` → **Confirmed** → records the
   output **without re-running the tool** (the external store shows the op applied **exactly
   once**). A separate case where the effect did **not** apply before the crash → `NotApplied` →
   runs once under the standing Intent (still exactly once).
6. **Determinism / additive.** The effective key is stable across resume (author key pure over
   args → mutation-verified); default tools + the existing reconcile suite are byte-identical.
7. **Absent provider still pauses (R3, preserved).** A Mutation with an author key but no
   registered `ReconcileProvider`, in-doubt on resume → `Indeterminate` → durable `RunPaused`
   (unchanged mandatory-human-reconciliation behavior).
