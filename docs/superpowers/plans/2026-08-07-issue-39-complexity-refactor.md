# Issue #39 — Complexity Refactor (engine split · llama_cpp streaming · adapter wire-mapping) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce the 11 `qlty` complexity smells (advisory/non-gating) in the hottest paths — behavior-preserving, one target per task, all behind the existing suite. Unblocks SP-0 plans (b)/(e) by splitting `engine.rs` and extracting `execute`'s per-attempt step into a seam.

**Architecture:** This is a **refactor, not a feature** — the existing tests are the contract; no behavior changes, no new deps, no macro obfuscation. Accept intentional complexity (`dispatch_capability` exhaustiveness) with `#[allow]` + rationale. Split the 4340-line `engine.rs` into an `engine/` folder by responsibility (mirroring the sibling `panel.rs`/`consensus.rs`/`purpose.rs`); tidy `llama_cpp::run_streaming_generation`; extract cohesive wire-mapping helpers in three adapters.

**Tech Stack:** Rust workspace. Per-task verification: `make check` (fmt + clippy `-D warnings` + `cargo build/test --workspace`) green; gateway line coverage **≥ 80%** (`cargo llvm-cov -p sensei-gateway --summary-only`); `qlty smells` count drops for the touched target with no new duplication/param findings. The pre-commit hook runs fmt+clippy on every commit.

**Rust note (engine split):** methods on `Gateway` can live in multiple `impl super::Gateway` blocks across sibling files under `engine/`. Child modules can read `Gateway`'s private fields. BUT a private method defined in one child module (e.g. `engine::dispatch`) is **not** visible to a sibling (e.g. `engine::execute`) — so any internal `Gateway` method called across the new files must be raised to **`pub(super)`** (visible within the `engine` module, still crate-private externally). The compiler will flag every such case; raise visibility, don't restructure call sites.

---

## File Structure (target)

```
crates/gateway/src/engine/
├── mod.rs        Gateway struct + new/try_new/with_store/with_readiness + thin ops
│                 (update_config, try_update_config, list_adapters, list_models,
│                  list_models_for_router, is_configured, refresh_router_keys,
│                  prune_unavailable) + `mod` declarations + re-exports.
├── util.rs       free fns: stream_error_code, window_start, read_usage,
│                  this_call_contribution, estimate_input_tokens, extract_prompt_text.
├── execute.rs    execute() + extracted per-attempt helper + record_call + check_quota.
├── stream.rs     execute_stream().
├── panel.rs      execute_panel() + execute_panel_addressed().
├── consensus.rs  execute_consensus() + execute_consensus_addressed().
├── dispatch.rs   dispatch_capability() (kept exhaustive).
└── tests.rs      the existing #[cfg(test)] mod (moved wholesale first; may split later).
```
Adapters (P3): `crates/local-providers/src/adapters/llama_cpp.rs`, `crates/cloud-providers/src/{bedrock,anthropic,openai_compat}.rs` (+ optional `bedrock/convert.rs` etc.).

**Global invariant for every task:** run `cargo test --workspace` (or at least `-p sensei-gateway`) and confirm the count is unchanged and all green; `make check` clean. A refactor that changes a test outcome is wrong — do not edit tests to make them pass (except P2, which *adds* streaming tests first).

---

## P1 — Split `engine.rs` (the biggest lever; unblocks SP-0)

### Task 1: `engine.rs` → `engine/mod.rs` (establish the folder, no content change)
**Files:** `git mv crates/gateway/src/engine.rs crates/gateway/src/engine/mod.rs`
- [ ] **Step 1:** `mkdir crates/gateway/src/engine && git mv crates/gateway/src/engine.rs crates/gateway/src/engine/mod.rs`. (`lib.rs` already says `pub mod engine;` — a folder module resolves the same.)
- [ ] **Step 2:** `cargo build -p sensei-gateway && cargo test -p sensei-gateway` → identical pass count, green. `cargo fmt --all --check` clean.
- [ ] **Step 3:** Commit: `refactor(gateway): engine.rs -> engine/mod.rs (folder module, no content change)`.

### Task 2: Extract free helper fns → `engine/util.rs`
**Files:** create `engine/util.rs`; modify `engine/mod.rs`.
- [ ] **Step 1:** Move the six free functions (`stream_error_code`, `window_start`, `read_usage`, `this_call_contribution`, `estimate_input_tokens`, `extract_prompt_text`) verbatim into `engine/util.rs`, preserving each fn's exact signature/visibility (mark `pub(super)` if used only within `engine`, `pub(crate)` if used outside). Add `mod util;` + `use util::*;` (or explicit `use`s) in `mod.rs`. Move any unit tests that target these fns (e.g. `estimate_input_tokens_stt`) into `util.rs`'s test module.
- [ ] **Step 2:** `cargo test -p sensei-gateway` green, same count; clippy/fmt clean.
- [ ] **Step 3:** Commit: `refactor(gateway): extract engine free-fn helpers into engine/util.rs`.

### Task 3: Move `execute_panel` (+ `_addressed`) → `engine/panel.rs`
**Files:** create `engine/panel.rs`; modify `engine/mod.rs`.
- [ ] **Step 1:** Move both fns into `engine/panel.rs` as `impl super::Gateway { pub async fn execute_panel(...){...} pub async fn execute_panel_addressed(...){...} }` with `use super::*;` (or precise imports). Add `mod panel;` to `mod.rs`. Raise any now-cross-module private `Gateway` method these call to `pub(super)`.
- [ ] **Step 2:** `cargo test -p sensei-gateway` green, same count; clippy/fmt clean.
- [ ] **Step 3:** Commit: `refactor(gateway): move panel orchestration into engine/panel.rs`.

### Task 4: Move `execute_consensus` (+ `_addressed`) → `engine/consensus.rs`
Same shape as Task 3. Commit: `refactor(gateway): move consensus orchestration into engine/consensus.rs`.

### Task 5: Move `dispatch_capability` → `engine/dispatch.rs` (keep exhaustive)
**Files:** create `engine/dispatch.rs`; modify `engine/mod.rs`.
- [ ] **Step 1:** Move `dispatch_capability` into `engine/dispatch.rs` (`impl super::Gateway`, `pub(super)` since `execute`/`execute_stream` call it). **Keep the exhaustive 6-way match (no `_` arm)** — its purpose is a compile error on a new `Capability`. Add `#[allow(clippy::too_many_lines)]` (or the specific lint) with a one-line rationale comment referencing #39, rather than splitting, UNLESS extracting one-liner `dispatch_chat`/`dispatch_embed`/… called from a still-exhaustive match reads clearly (implementer's judgment — prefer the `#[allow]`).
- [ ] **Step 2:** `cargo test -p sensei-gateway` green, same count; clippy `-D warnings` clean (the `#[allow]` must be scoped + justified). 
- [ ] **Step 3:** Commit: `refactor(gateway): move dispatch_capability into engine/dispatch.rs (exhaustiveness preserved)`.

### Task 6: Move `execute_stream` → `engine/stream.rs`
Same shape as Task 3 (it's ~260 lines). Raise cross-module private calls to `pub(super)`. Commit: `refactor(gateway): move execute_stream into engine/stream.rs`.

### Task 7: Move `execute` + extract the per-attempt helper → `engine/execute.rs` (SP-0 seam)
**Files:** create `engine/execute.rs`; modify `engine/mod.rs`.
- [ ] **Step 1:** Move `execute` (+ `record_call`, `check_quota`) into `engine/execute.rs`. **Extract the per-attempt step** — *select resolved candidate → dispatch → record the `Attempt` → decide fall-back* — into a private `pub(super) fn` (e.g. `attempt_candidate(...)` returning an enum like `AttemptStep::{Succeeded(resp), FellBack(attempt), Stop(err)}`), so the top-level loop reads as "walk the chain." **This helper is the seam SP-0 plan (b) will extend** (the outcome-recording site) — keep the record-`Attempt` and circuit-breaker `record_success`/`record_failure` calls localized inside it.
- [ ] **Step 2:** Behavior-preserving: `cargo test -p sensei-gateway` green, same count — pay attention to `execute_fallback_on_provider_error`, `execute_records_attempts`, `exhaustion_*`, `no_fallback_when_disabled_stops_at_primary`, quota tests. clippy/fmt clean. `execute`'s per-fn complexity should drop.
- [ ] **Step 3:** Commit: `refactor(gateway): move execute + extract per-attempt helper into engine/execute.rs`.

### Task 8: Move the test module → `engine/tests.rs`
**Files:** create `engine/tests.rs`; modify `engine/mod.rs`.
- [ ] **Step 1:** Move the remaining `#[cfg(test)] mod tests { ... }` block (fixtures + ~50 test fns) into `engine/tests.rs` as `#[cfg(test)] mod tests;` declared in `mod.rs`; fix `use super::*;`/`use crate::...` paths. (Optionally split by orchestrator later — out of scope here.)
- [ ] **Step 2:** `cargo test -p sensei-gateway` green, same count; clippy/fmt clean. Confirm `engine/mod.rs` is now a thin surface.
- [ ] **Step 3:** Commit: `refactor(gateway): move engine tests into engine/tests.rs`.
- [ ] **Step 4 (P1 gate):** `cargo llvm-cov -p sensei-gateway --summary-only` ≥ 80%; `qlty smells crates/gateway/src/engine` shows the file-complexity finding cleared/reduced and no new smells.

## P2 — `llama_cpp::run_streaming_generation`

### Task 9: Tidy the streaming decode loop (behind added streaming tests)
**Files:** `crates/local-providers/src/adapters/llama_cpp.rs`.
- [ ] **Step 1 (test first):** If not already covered, add a test that drives `run_streaming_generation` (or the streaming `chat_stream` path) and **captures the emitted `StreamEvent` sequence** (chunks then terminal), including an injected-error path. This pins behavior before the refactor. Run it green.
- [ ] **Step 2:** Refactor: introduce a small helper/closure `emit_err(&tx, e)` (send `Err`, return the sentinel) so the **seven** `return`s collapse to early-exits through one path; extract the token-decode body into a `decode_step(...)` helper so the loop reads `setup → loop { decode_step } → finish`. Do NOT change the emitted event order/content.
- [ ] **Step 3:** Re-run the streaming test(s) and **diff the `StreamEvent` sequence before/after** (must be identical). `cargo test -p sensei-local-providers` green (features enabling llama_cpp; if the feature isn't built in CI, run `cargo build -p sensei-local-providers --features local-llama-cpp` locally and note it). clippy/fmt clean; the fn-complexity + return-statement smells drop.
- [ ] **Step 4:** Commit: `refactor(local-providers): simplify llama_cpp streaming decode loop (emit_err + decode_step)`.

## P3 — adapter wire-mappings (lowest priority; split only where a helper clarifies)

### Task 10: Bedrock content-block ↔ SDK conversion
**Files:** `crates/cloud-providers/src/bedrock.rs` (+ optional `bedrock/convert.rs`).
- [ ] **Step 1:** Extract the pure content-block ↔ SDK-type mapping into helper fns (or a `bedrock/convert.rs` submodule). Keep the adapter's `impl ChatModel` thin. No behavior change.
- [ ] **Step 2:** `cargo test -p sensei-cloud-providers` green; clippy/fmt clean; bedrock file-complexity drops.
- [ ] **Step 3:** Commit: `refactor(cloud-providers): extract bedrock wire-mapping helpers`.

### Task 11: Anthropic content-block build + SSE parse
Same shape; extract content-block build and SSE-parse helpers. Commit: `refactor(cloud-providers): extract anthropic content-build + SSE-parse helpers`.

### Task 12: `openai_compat` streaming-with-tools vs non-streaming split
Same shape; separate the streaming-with-tools assembly from the non-streaming path into helpers. Commit: `refactor(cloud-providers): split openai_compat streaming-with-tools from non-streaming`.

---

## Self-Review

- **Spec coverage:** P1 (engine split + per-attempt seam + dispatch exhaustiveness decision) = Tasks 1–8; P2 (llama_cpp) = Task 9; P3 (3 adapters) = Tasks 10–12. Every #39 target has a task.
- **Behavior preservation:** each task's contract is "same test count, all green, `make check` clean" — no test assertions changed (except P2 *adds* streaming tests first). The per-attempt helper (Task 7) preserves the exact record/fall-back semantics and is the seam SP-0 (b) extends.
- **No metric chase:** `dispatch_capability` stays exhaustive with `#[allow]` + rationale; P3 splits only where a helper genuinely clarifies. No new deps, no macros.
- **Sequencing:** Tasks 1→8 are strictly ordered (each moves code out of a shrinking `mod.rs`); P2/P3 are independent of P1 and of each other (can be parallelized across sessions, but subagent-driven runs them sequentially). Coverage/qlty gate checked at the P1 boundary (Task 8) and per adapter.
- **Cross-module visibility:** the recurring risk is a private `Gateway` method becoming cross-module — resolved by raising to `pub(super)`; the compiler enforces completeness.

## Execution Handoff

**Two options:**
1. **Subagent-Driven (recommended)** — fresh subagent per task, spec+quality review between tasks, in an isolated worktree (same flow that landed the SP-0 foundation).
2. **Inline Execution** — execute here with checkpoints.

After all tasks: final whole-branch review, then `superpowers:finishing-a-development-branch` (merge to `develop`), then return to **SP-0 plan (b)** (`HealthRecorder` write-side) on the clean post-#39 engine layout — the per-attempt helper from Task 7 is its extension point.
