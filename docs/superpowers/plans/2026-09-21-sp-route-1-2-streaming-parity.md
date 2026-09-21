# SP-ROUTE-1.2 — Streaming Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the streaming path the metering and explanation the unary path already has — a streamed request must always be billed, its persisted duration must mean the same thing, and its caller must be able to ask why a provider was chosen.

**Architecture:** Three fixes in one function, `Gateway::execute_stream`. Move the metering write above `yield StreamEvent::Done`; measure its duration from `attempt_start`; carry the `RoutingDecision` on `Done`.

**Tech Stack:** Rust 2024. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-09-19-sp-route-1-2-streaming-parity-design.md`

---

## Orientation — read before Task 1

**Baseline: 1918 passed, 0 failed, 60 ignored, real exit 0.** Repo root `/Users/Jerry/Developer/gateway`.

### The one thing that makes this slice testable

`engine/tests.rs:6309` — `a_consumer_that_stops_at_done_still_sees_its_verdict_recorded` — is the **only** streaming test that behaves like a real SSE consumer. Every other one uses `collect_stream`, which drains to `None` and therefore cannot observe a defect that only bites when the caller stops polling:

```rust
loop {
    match stream.next().await {
        Some(StreamEvent::Done { .. }) => break,
        Some(_) => continue,
        None => panic!("stream ended without a terminal Done event"),
    }
}
drop(stream); // stop exactly where a real consumer stops — no further polling
```

**Copy this shape for Task 1. Do NOT write AC1 against `collect_stream`** — it passes either way, which is precisely how the metering defect survived SP-ROUTE-1 Task 5's review while the sibling defect beside it was caught.

### The irony to preserve, not delete

`stream.rs:356-361` already explains why the performance dispatch must precede the `yield`. Task 1 moves the metering write for the *same* reason. Reference that comment rather than restating it — and do not remove it.

### Repo conventions

- **TDD, strictly red-first.** Write the failing test, RUN it, read the actual failure, then implement.
- **`cargo fmt --all` before committing.** Pre-commit runs fmt-check + `cargo clippy --workspace --all-targets -- -D warnings`, and **no tests**.
- **Verify real exit codes.** `cargo test` exits 101 on a compile error too. Redirect to a file, read `$?` — zsh, so `${PIPESTATUS[0]}` does NOT work. Grep for `panicked at`.
- Clippy under **both** toolchains. Bare `cargo clippy` is Homebrew 0.1.97; **`rustup run stable` is NOT enough** — it still uses Homebrew's `clippy-driver`. Only `PATH="$HOME/.rustup/toolchains/stable-aarch64-apple-darwin/bin:$PATH"` gives you 0.1.98, which is what CI runs.
- Mutation-check every claim: apply the one-line mutation, confirm a real panic, restore, quote the panic line.

---

## File Structure

**Modified:**
- `crates/gateway/src/engine/stream.rs` — all three fixes
- `crates/kernel/src/types/request.rs` — `StreamEvent::Done.routing`
- `crates/gateway/src/engine/tests.rs` — the tests, plus one pattern fix
- `docs/llms/upgrading.md`, `docs/features/observability/tracing-and-attempts.md`, `docs/features/routing/provider-preferences.md` — Task 4

No new files, no new dependencies.

---

## Task 1: A streamed request is always metered

**Files:** Modify `crates/gateway/src/engine/stream.rs`; test in `crates/gateway/src/engine/tests.rs`

- [ ] **Step 1: Write the failing test**

Model it on `a_consumer_that_stops_at_done_still_sees_its_verdict_recorded`, but assert the **store** rather than the perf stats. You need a `Gateway` with an `InMemoryStore` wired — find how via `rg -n 'with_store|InMemoryStore' crates/gateway/src`.

```rust
/// A consumer that stops polling at `Done` — the normal shape for an SSE
/// handler — must still be BILLED. `insert_inference_call` sat AFTER the
/// `yield`, and in an `async_stream` generator code after a yield runs only on
/// the NEXT poll, so a streamed request was metered only if its caller happened
/// to over-poll. A non-streaming request is always metered.
///
/// Deliberately NOT written against `collect_stream`: that drains to `None` and
/// passes whether the write is above or below the yield, which is exactly how
/// this survived the review that caught the sibling defect beside it.
#[tokio::test]
async fn a_consumer_that_stops_at_done_is_still_metered() {
    // …gateway with an InMemoryStore, FakeStreamer…
    let mut stream = gw.execute_stream(&request).await.expect("stream should start");
    loop {
        match stream.next().await {
            Some(StreamEvent::Done { .. }) => break,
            Some(_) => continue,
            None => panic!("stream ended without a terminal Done event"),
        }
    }
    drop(stream); // stop exactly where a real consumer stops

    let calls = store.get_inference_calls_by_session(session_id).await.unwrap();
    assert_eq!(calls.len(), 1, "the metering row must exist once Done is observed");
}
```

If `get_inference_calls_by_session` needs a session id the request does not carry, use whichever `GatewayStore` read the in-memory impl exposes — check `crates/gateway/src/store.rs`. **Report which you used and why.**

- [ ] **Step 2: Run it, confirm it fails, read the actual failure**

Expect `calls.len() == 0`. Quote it.

- [ ] **Step 3: Move the write above the yield**

In `stream.rs`, relocate the `if let Some(store) = &store && let Some(call) = call && let Err(e) = store.insert_inference_call(&call).await { … }` block to **before** `yield StreamEvent::Done { … }`.

Add a comment pointing at the existing one rather than restating it:

```rust
                    // Before the `yield`, for the reason spelled out above the
                    // completion dispatch: a consumer that stops polling at the
                    // terminal `Done` never resumes this generator, so anything
                    // placed after the yield silently never runs. That cost the
                    // verdict once (Task 5 review, Minor 2); here it costs the
                    // BILLING ROW.
```

**Watch the borrow:** `tokens` is moved into the yielded `Done`. The `InferenceCall` is already built before the yield (into `call`), so this should be a clean move — but if the borrow checker objects, hoist what you need rather than cloning blindly, and say what you did.

- [ ] **Step 4: Confirm green, then mutation-check**

Move the write back below the yield. `a_consumer_that_stops_at_done_is_still_metered` **must** fail. Quote the panic. Also confirm an existing `collect_stream`-based metering test still passes under that mutation — that contrast is the point of AC2.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "fix(gateway): meter a streamed call before yielding Done (SP-ROUTE-1.2 Task 1, AC1-AC2)"
```

---

## Task 2: The persisted duration means the same thing on both paths

**Files:** Modify `crates/gateway/src/engine/stream.rs`; test in `crates/gateway/src/engine/tests.rs`

- [ ] **Step 1: Write the failing test**

`InferenceCall.duration_ms` uses `stream_start.elapsed()` — generation time only. `execute` writes the same column with the whole call's wall time, so `inference_calls` mixes two quantities under one name.

Use a fixture with a distinguishable pre-first-byte delay. **`FakeStreamerWithRealDelay` does NOT exist — that name in an earlier draft was wrong.** The real one is `SplitDelayStreamer` (`tests.rs:2675`): it sleeps `SETUP_DELAY` (60ms) *inside* `chat_stream` — pre-first-byte — and `GENERATION_DELAY` (60ms) inside the stream, which is exactly the split this needs.

Also: **no `GatewayStore` read exposes `duration_ms`**, so reading it back needs a test-local recording store. Give its unused reads `unimplemented!()` so a future test leaning on one fails loudly rather than silently taking a default.

Assert the persisted `duration_ms` **includes** the acquisition span:

```rust
/// The persisted duration must be the same quantity `execute` records — total
/// attempt wall time — or `inference_calls` mixes two meanings under one column
/// and any analytics over it is wrong with nothing surfacing the fact.
///
/// This is the same argument the completion dispatch above it already makes for
/// `tokens_per_sec`; the metering row was simply never brought along.
#[tokio::test]
async fn a_streamed_calls_persisted_duration_includes_acquisition() { /* … */ }
```

Make the assertion **relational**, not an absolute bound — a loaded CI runner must not flake it. Compare against the known pre-first-byte delay rather than a fixed millisecond ceiling.

- [ ] **Step 2: Run it, confirm it fails**

- [ ] **Step 3: Change `stream_start.elapsed()` to `attempt_start.elapsed()`** on the `InferenceCall`, with a comment naming the parity it restores.

- [ ] **Step 4: Confirm green, mutation-check (revert to `stream_start`), quote the panic**

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "fix(gateway): persist the total attempt span for streamed calls (SP-ROUTE-1.2 Task 2, AC3)"
```

---

## Task 3: `StreamEvent::Done` carries the routing decision

**Files:** Modify `crates/kernel/src/types/request.rs`, `crates/gateway/src/engine/stream.rs`; tests in both

**The breakage surface, measured not estimated.** `rg -n 'StreamEvent::Done'` across the workspace: most sites use `{ .. }` or `{ model, .. }` and survive. **Exactly two break**, plus the production construction:
- `crates/gateway/src/engine/tests.rs:2975` — an exhaustive destructuring `{ model, tokens, cost }`; add `..`
- `crates/kernel/src/types/request.rs:1760` — a `Done` literal; add the field
- `crates/gateway/src/engine/stream.rs:408` — the production site, which is the change itself

- [ ] **Step 1: Write the failing tests**

```rust
/// AC4 — a streaming caller can ask why its provider was chosen. Preferences
/// already APPLY on this path; only the explanation was missing.
#[tokio::test]
async fn a_streamed_request_carries_its_routing_decision() { /* assert Done.routing is Some, strategy named */ }

/// AC5 — and it is the decision the selection actually produced, not a
/// re-derivation. Same request and chain through `execute` must report the
/// same strategy and the same candidate order.
#[tokio::test]
async fn the_streamed_decision_matches_what_execute_reports() { /* … */ }
```

AC5 is the one that matters: SP-ROUTE-1 Task 11 shipped a re-derivation at the attachment site that survived its whole suite, so "it is the one selection produced" needs asserting, not assuming.

- [ ] **Step 2: Run them, confirm they fail**

- [ ] **Step 3: Add the field and thread it**

In `crates/kernel/src/types/request.rs`, add to `StreamEvent::Done`:

```rust
        /// Why these candidates came out in this order — the same
        /// [`RoutingDecision`] `execute` puts on [`InferenceResponse::routing`].
        /// `None` when no strategy ordered anything (a tier-1 direct request).
        routing: Option<RoutingDecision>,
```

In `stream.rs`, pull the decision out beside the other owned pieces **before** the `async_stream::stream!` block — `result` is already destructured that way (`skipped_owned`, `candidates`, `fallback_triggers`), so follow the pattern:

```rust
        let decision = result.decision;
```

then move it into the generator and attach it on the `Done` yield. **Do not re-derive it** — that is the Task 11 defect.

- [ ] **Step 4: Fix the two break sites**, then confirm green

- [ ] **Step 5: Mutation-check**

- Attach a freshly-built `RoutingDecision` instead of `decision` — AC5 must fail.
- Attach `None` — AC4 must fail.

Quote both panics.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(gateway): carry the routing decision on StreamEvent::Done (SP-ROUTE-1.2 Task 3, AC4-AC5)"
```

---

## Task 4: Docs and final verification

**Files:** `docs/llms/upgrading.md`, `docs/features/observability/tracing-and-attempts.md`, `docs/features/routing/provider-preferences.md`, the SP-ROUTE-1 plan's carry-forwards, `docs/CHECKPOINT.md`

- [ ] **Step 1: Find every surface**

```
rg --no-ignore -g '!target' -g '!site/node_modules' -l 'StreamEvent|execute_stream|streaming' docs/ README.md
```

Report the full list and which you changed.

- [ ] **Step 2: Document**

- **`StreamEvent::Done` gained a field** — a public enum change; exhaustive matches break. `upgrading.md`.
- **The `duration_ms` discontinuity** — pre-slice streamed rows carry generation-only durations, post-slice rows carry total. Analytics spanning the boundary sees a step change. This is deliberate: it ends an ongoing wrongness at the cost of a one-time one. `upgrading.md`.
- **Streaming now carries a `RoutingDecision`** — update the observability doc, which currently states the absence as a known gap.
- Close SP-ROUTE-1 carry-forwards **3, 4 and 5** in `docs/superpowers/plans/2026-09-17-sp-route-1-provider-routing-preferences.md`, naming this slice. One remains: consensus legs.

- [ ] **Step 3: Verify**

Each with its REAL unpiped exit code:

```
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings     # BOTH toolchains, see Orientation
cargo fmt --all --check
cargo test -p sensei-gateway --features local --locked
cargo doc --workspace --no-deps
```

Grep the test log for `panicked at`; report the count and `cargo doc`'s unresolved-link count.

- [ ] **Step 4: Confirm each AC has a named passing test**

AC1–AC7 from the spec. Name the test for each, run it individually, confirm it passes. Any AC without a green named test is unfinished — report it.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "docs: SP-ROUTE-1.2 streaming parity (Task 4, AC7)"
```

---

## Self-review notes

**Spec coverage:** §2.1 → Task 1 (AC1, AC2, AC6). §2.2 → Task 2 (AC3). §2.3 → Task 3 (AC4, AC5). §5 AC7 → Task 4.

**The test that must not be waved through is AC1**, and the reason is specific: it is the only test in this slice that `collect_stream` cannot substitute for. Written against `collect_stream` it passes whether the write is above or below the yield — which is not hypothetical, it is how this exact defect survived a review that caught the dispatch beside it. Its mutation check (move the write back below the yield) is mandatory.

**AC5 is second.** SP-ROUTE-1 Task 11 shipped a re-derivation at an attachment site that survived the whole suite until a reviewer mutated it. The same shape is available here.
