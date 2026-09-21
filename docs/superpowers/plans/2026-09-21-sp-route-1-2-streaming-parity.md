# SP-ROUTE-1.2 — Streaming Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the streaming path the metering and explanation the unary path already has — a streamed request must always be billed, its persisted duration must mean the same thing, and its caller must be able to ask why a provider was chosen.

**Architecture:** Three fixes in one function, `Gateway::execute_stream`. Move the metering write above `yield StreamEvent::Done`; measure its duration from `attempt_start`; carry the `RoutingDecision` on `Done`.

**Tech Stack:** Rust 2024. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-09-19-sp-route-1-2-streaming-parity-design.md`

---

## Progress

| Task | Commits | State |
|---|---|---|
| 1 — a streamed request is always metered | `2fb6b86` | ✅ done (AC1–AC2, AC6); the write moved above the `yield`, mutation confirmed red at `requests == 0` |
| 2 — the persisted duration means the same thing | `ab4e6ee` · `f40d8f8` (plan correction) | ✅ done (AC3); `attempt_start` on the store row, both assertions relational against the fixture's own delays |
| 3 — `Done` carries the routing decision | `7e89dc3` | ✅ done (AC4–AC5); `result.decision` moved onto the event, not re-derived; both mutations confirmed red |
| 4 — docs + final verification | `4aee074` | ✅ done (AC7) |
| **Whole-slice review** | *(this commit)* | ✅ **COMPLETE** — 2 Critical, 1 Important, 1 Minor, all fixed red-first; see below |

### Whole-slice adversarial review — complete

The review found that **Task 1 had fixed one third of the metering defect and
the docs claimed all of it**, and that the fix itself introduced a liveness
regression. Both Criticals were reproduced with probes before anything was
changed, and every fix was driven red-first.

**C2 — the terminal event became hostage to an unbounded store write.** Task 1's
comment justified putting `insert_inference_call` ahead of the `yield` as "the
same price the dispatch above already charges". That was **false, and I wrote
it**: `dispatch_outcome` is a *synchronous* fold over in-memory recorders and
awaits nothing, while `insert_inference_call` is an `async` call into a
consumer-supplied store with no timeout anywhere in the crate. Measured: a 400ms
store delayed `Done` by 403ms, and a consumer with an ordinary 100ms per-event
timeout received the full content and then **no terminal event at all** — no
tokens, no cost, none of Task 3's routing — with the in-flight write cancelled
so no row landed either. Task 1 had converted *delivered but unbilled* into *not
delivered and unbilled*, and an unreachable database meant a stream that never
terminates. Fixed with a 2s `METERING_WRITE_BUDGET` and a `record_call_bounded`
helper; the false comment is corrected at the source and in `upgrading.md`.

**C1 — streamed failures were never metered, and Task 4's docs said they were.**
Task 1 fixed the success path only. Both failure `return` sites — the mid-stream
`Err` arm and setup exhaustion — returned without any write, so a stream that
died after generating real tokens recorded **nothing**, even when fully drained
with `collect_stream`. The absence was structural, not a polling artefact.
Meanwhile `execute` writes a `CallStatus::Failed` row for the analogous
exhaustion: unary one row, streamed zero, same adapter. Both sites now write
one, ahead of their terminal `Error`, under the same budget.

**This is the finding to remember, and it is about the docs, not the code.**
Task 4 asserted "a streamed call is always metered… both paths bill alike" and
told operators that post-upgrade numbers "should agree" with a provider invoice
— advice to trust a reconciliation that still under-counted every stream that
died after first byte. A doc written from the *intent* of a fix rather than from
its *reach* is worse than no doc: it converts a known gap into a false
assurance. The prose was not corrected to match the code; the code was finished
so the prose became true.

**I1 — AC3's parity claim was unguarded on the only divergent branch.**
`a_streamed_calls_persisted_duration_includes_acquisition` uses a
single-candidate, first-candidate-success fixture and asserts two lower bounds,
but AC3 requires the *winning candidate's* span, not the request's — and a lower
bound cannot separate those without a fallback. Behaviour was already correct;
hoisting `attempt_start` above the candidate loop is a one-line edit that left
all 442 shipped tests green while making the streamed row read 252ms against the
unary row's ~0ms. Now pinned by
`the_persisted_duration_excludes_a_failed_candidates_span`.

**M1 — `Done.routing`'s documented `None` case was unasserted**, so a
"helpful" `decision.or_else(|| Some(…))` would have made every direct request
report a fabricated empty decision. Pinned by
`a_direct_streamed_request_carries_no_routing_decision`.

**Independently confirmed by the reviewer:** my Task 4 AC2 measurement (exactly
1 of 442) reproduces, and Task 3's move-without-clone is compiler-enforced —
deleting the `return;` yields `E0382: value moved here, in previous iteration of
loop`.

**Five mutations, five real panics** (`stream.rs` restored byte-identical after,
verified by `diff` against a pre-mutation backup rather than `git checkout`,
which would have reverted the uncommitted fix):

| Mutation | Test that went red | Panic |
|---|---|---|
| Remove the timeout | `a_stalled_store_does_not_hold_the_terminal_event` | `waited 30s of the store's 30s stall` |
| Delete the mid-stream write | `a_stream_that_dies_mid_generation_is_still_metered` | `left: 0, right: 1` |
| Delete the exhaustion write | `a_stream_that_exhausts_its_chain_is_still_metered` | `left: 0, right: 1` |
| Hoist `attempt_start` | `the_persisted_duration_excludes_a_failed_candidates_span` | `must not absorb the failed candidate's 250ms; got 252ms` |
| Fabricate an empty decision | `a_direct_streamed_request_carries_no_routing_decision` | `is_none()` assertion |

**One decision the review's suggested fix got wrong, and I changed.** It
specified `output_tokens: usage_acc.map(…)` for *both* failure sites. At setup
exhaustion there is no `usage_acc` and the honest value is not `Some(0)` either:
checking `execute.rs` showed it writes `tokens: None, cost: None` for a failed
attempt, and a setup failure does not prove the provider generated nothing —
only that no usage was reported. That site now mirrors `execute` field-for-field
with `None`. The mid-stream site *does* carry its usage and costs it, diverging
from `execute` only because `execute` has no partial-success concept and never
holds usage for a failed call; discarding tokens the provider bills for would
knowingly under-count spend. The first draft of the test asserted `Some(0)` and
was corrected to `None` once `execute.rs` was actually read.

**Final state:** suite **1922 passed / 0 failed / 60 ignored**, real exit 0, zero
`panicked at`. Clippy clean under **both** toolchains (Homebrew 0.1.97 and rustup
stable 0.1.98), `fmt --all --check` clean, `cargo test -p sensei-gateway --features
local --locked` green, `cargo doc --workspace --no-deps` clean with zero unresolved
links. Closes SP-ROUTE-1 carry-forwards **1** and **3**, plus the unnumbered
persisted-duration mismatch. **5** (consensus legs) and **2** (`ExecutionTrace`
forward provision) remain open by design — see the ledger in the SP-ROUTE-1 plan.

### The two things this slice proves that a green suite does not

**AC1 is the one that must not be waved through.** It is the only test here that
`collect_stream` cannot substitute for: written against `collect_stream` it passes
whether the write is above or below the `yield`, which is not a hypothetical — it is
how this exact defect survived the SP-ROUTE-1 Task 5 review that caught the sibling
dispatch defect fifteen lines away.

Re-measured in Task 4 against the finished slice, not carried over from Task 1's
smaller suite. Moving the write back below the `yield` and running
`cargo test -p sensei-gateway --lib`:

```text
test result: FAILED. 441 passed; 1 failed; 0 ignored
    engine::tests::a_consumer_that_stops_at_done_is_still_metered
assertion `left == right` failed: the metering row must exist once Done is observed…
  left: 0
 right: 1
```

**Exactly 1 of 442 fails, and it is this one** (Task 1 measured 1 of 439; the suite
has since gained the three tests Tasks 2–3 added). That is AC2. **AC6 is the same run
read the other way:** every `collect_stream`-based streaming test stays green under
that mutation — `execute_stream_yields_chunks_then_done_with_cost`,
`a_consumer_that_stops_at_done_still_sees_its_verdict_recorded`, and most pointedly
`a_streamed_calls_persisted_duration_includes_acquisition`, which *asserts a metering
row exists* and still finds one, because draining to `None` over-polls the generator
and runs the write anyway. A `collect_stream` fixture cannot see this defect. The
source file was restored byte-identical afterwards (`git diff crates/` empty).

**AC5 is second.** SP-ROUTE-1 Task 11 shipped a re-derivation at an attachment site
that survived its whole suite until a reviewer mutated it. The same shape was
available here, so `the_streamed_decision_matches_what_execute_reports` asserts the
decision on `Done` is the one the *selection* produced — same strategy, same candidate
order as `execute` reports for the same request and chain — rather than merely
non-`None`.

### The process gap this slice closed by hitting it

This plan shipped with **no whole-slice review step** — SP-ROUTE-1's Task 12 had one,
Task 4 here ended at the commit. That gap was flagged in Task 4 rather than left
implicit, and the review was then run. It found two Criticals in ~120 lines of
already-"verified" code, one of them a regression introduced by the fix itself and one
a false claim in the verification commit's own documentation.

**The lesson is the SP-6 one, earned again: per-task mutation checks do not compose
into slice-level correctness.** Every task here was individually red-first, mutation-
checked, and green. The defects lived in what no single task owned — Task 1's fix
measured against Task 1's test, with no one asking what it cost the *other* terminal
event or what it left undone on the *other* return path. A slice-level pass is not a
formality after per-task rigour; it is the only phase that sees the seams.

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

- [x] **Step 1: Write the failing test**

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

- [x] **Step 2: Run it, confirm it fails, read the actual failure**

Expect `calls.len() == 0`. Quote it.

- [x] **Step 3: Move the write above the yield**

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

- [x] **Step 4: Confirm green, then mutation-check**

Move the write back below the yield. `a_consumer_that_stops_at_done_is_still_metered` **must** fail. Quote the panic. Also confirm an existing `collect_stream`-based metering test still passes under that mutation — that contrast is the point of AC2.

- [x] **Step 5: Commit**

```bash
git add -A
git commit -m "fix(gateway): meter a streamed call before yielding Done (SP-ROUTE-1.2 Task 1, AC1-AC2)"
```

---

## Task 2: The persisted duration means the same thing on both paths

**Files:** Modify `crates/gateway/src/engine/stream.rs`; test in `crates/gateway/src/engine/tests.rs`

- [x] **Step 1: Write the failing test**

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

- [x] **Step 2: Run it, confirm it fails**

- [x] **Step 3: Change `stream_start.elapsed()` to `attempt_start.elapsed()`** on the `InferenceCall`, with a comment naming the parity it restores.

- [x] **Step 4: Confirm green, mutation-check (revert to `stream_start`), quote the panic**

- [x] **Step 5: Commit**

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

- [x] **Step 1: Write the failing tests**

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

- [x] **Step 2: Run them, confirm they fail**

- [x] **Step 3: Add the field and thread it**

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

- [x] **Step 4: Fix the two break sites**, then confirm green

- [x] **Step 5: Mutation-check**

- Attach a freshly-built `RoutingDecision` instead of `decision` — AC5 must fail.
- Attach `None` — AC4 must fail.

Quote both panics.

- [x] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(gateway): carry the routing decision on StreamEvent::Done (SP-ROUTE-1.2 Task 3, AC4-AC5)"
```

---

## Task 4: Docs and final verification

**Files:** `docs/llms/upgrading.md`, `docs/features/observability/tracing-and-attempts.md`, `docs/features/routing/provider-preferences.md`, the SP-ROUTE-1 plan's carry-forwards, `docs/CHECKPOINT.md`

- [x] **Step 1: Find every surface**

```
rg --no-ignore -g '!target' -g '!site/node_modules' -l 'StreamEvent|execute_stream|streaming' docs/ README.md
```

Report the full list and which you changed.

**Done.** The sweep (widened with `|duration_ms`) returned **58 files of the 180 markdown
files under `docs/`**. Most match on an unrelated "streaming" in another slice's plan or
spec, or on an `AttemptOutcome::duration_ms` that this slice does not touch. **Seven
user-facing surfaces carried a claim this slice falsifies, and all seven changed:**

| Surface | Change |
|---|---|
| `docs/llms/upgrading.md` | three new table rows + three new subsections: the `Done` migration (E0027/E0063), the always-metered billing fix, the `duration_ms` discontinuity |
| `docs/llms/recipes.md` | the "no routing explanation when streaming" note **inverted**; the `match` sample destructured `Done { model, tokens, cost }` **exhaustively** and no longer compiled — now reads `routing` |
| `docs/features/observability/tracing-and-attempts.md` | gap bullet → "the streaming path reports it too"; the `StreamEvent` enum block, variant table, and lead-in sentence refreshed |
| `docs/features/observability/persistence-store.md` | `duration_ms` row now states the span it measures; a new subsection on always-metered + the discontinuity + best-effort |
| `docs/features/routing/provider-preferences.md` | "Two known gaps" → "One known gap", with the streaming decision documented as delivered |
| `docs/skills/using-gateway/SKILL.md` | §8's "`execute_stream` applies preferences but returns no explanation" corrected |
| `docs/CHECKPOINT.md` | rewritten for this slice |

Plus four slice-record surfaces: the SP-ROUTE-1 plan (carry-forward ledger), this plan,
and the SP-ROUTE-1.1 / SP-ROUTE-1.2 spec frontmatter (mis-numbered `closes:` lines).

**Two surfaces checked and deliberately left alone.** `site/src/lib/content/docs/` is
generated **and gitignored** (`site/.gitignore:5` — `/src/lib/content`), so `docs/llms/`
is the source of truth. `site/src/lib/data.ts` **is** committed and published, and was
read: its streaming copy is "Stream tokens as they arrive, read an attempt-by-attempt
trace, and plug in your own GatewayStore" — marketing prose carrying no field list and no
claim about a routing explanation, so nothing in it is falsified. `README.md` matched
none of the four terms at all.

**Three surfaces state the old behaviour and were kept as written**, because they are
historical records of what a past slice knew:
`docs/reviews/2026-07-17-production-readiness-review.md`,
`docs/superpowers/plans/2026-08-07-sp0-e-resume-after-allgated.md`, and SP-ROUTE-1's own
Task 12 doc-surface table (which says it documented "BOTH gaps"). Rewriting those would
falsify the record rather than correct it.

- [x] **Step 2: Document**

- **`StreamEvent::Done` gained a field** — a public enum change; exhaustive matches break. `upgrading.md`.
- **The `duration_ms` discontinuity** — pre-slice streamed rows carry generation-only durations, post-slice rows carry total. Analytics spanning the boundary sees a step change. This is deliberate: it ends an ongoing wrongness at the cost of a one-time one. `upgrading.md`.
- **Streaming now carries a `RoutingDecision`** — update the observability doc, which currently states the absence as a known gap.
- Close SP-ROUTE-1 carry-forwards **3, 4 and 5** in `docs/superpowers/plans/2026-09-17-sp-route-1-provider-routing-preferences.md`, naming this slice. One remains: consensus legs.

> **The numbers in that last bullet are wrong, and finding out why was part of Task 4.**
> There is no list on which "3, 4, 5" are this slice's three items. The only enumerated
> list SP-ROUTE-1 ever produced is the one in `76a3e2a`'s `docs/CHECKPOINT.md`, and on it
> this slice closes **1** (streaming carries no `RoutingDecision`) and **3** (the metering
> row after `yield Done`). **4** was already closed by SP-ROUTE-1.1. **5** *is* the
> consensus item the same bullet says must stay open — so taken literally the instruction
> contradicts itself. The third thing this slice fixes, the persisted `duration_ms` span,
> was never numbered at all.
>
> Resolved by intent, not by invention: the three streaming defects named in spec §2 are
> unambiguous, and consensus legs stay open. The fix is upstream of the numbering — the
> SP-ROUTE-1 plan now carries a **canonical carry-forward ledger** with all five items and
> their states, and both mis-numbered `closes:` frontmatter lines (SP-ROUTE-1.1's "2",
> SP-ROUTE-1.2's "3, 4, 5") now cite by name. **A cross-slice reference by bare ordinal
> needs one canonical list, or it quietly means something different in every file.**

- [x] **Step 3: Verify**

Each with its REAL unpiped exit code:

```
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings     # BOTH toolchains, see Orientation
cargo fmt --all --check
cargo test -p sensei-gateway --features local --locked
cargo doc --workspace --no-deps
```

Grep the test log for `panicked at`; report the count and `cargo doc`'s unresolved-link count.

- [x] **Step 4: Confirm each AC has a named passing test**

AC1–AC7 from the spec. Name the test for each, run it individually, confirm it passes. Any AC without a green named test is unfinished — report it.

**Done.** Every named test was run **individually** (`--exact`) and passed 1/0, real
exit 0. All live in `crates/gateway/src/engine/tests.rs`.

| AC | Test (module path) | Individually |
|---|---|---|
| AC1 | `engine::tests::a_consumer_that_stops_at_done_is_still_metered` | ✅ 1/0 |
| AC2 | *(mutation, not a test — see below)* | ✅ 1 of 442 red |
| AC3 | `engine::tests::a_streamed_calls_persisted_duration_includes_acquisition` | ✅ 1/0 |
| AC4 | `engine::tests::a_streamed_request_carries_its_routing_decision` | ✅ 1/0 |
| AC5 | `engine::tests::the_streamed_decision_matches_what_execute_reports` | ✅ 1/0 |
| AC6 | `engine::tests::execute_stream_yields_chunks_then_done_with_cost` *(and the whole `collect_stream` cohort — see below)* | ✅ 1/0 |
| AC7 | Step 3 | ✅ |

**AC2 and AC6 are contrast claims, not standalone tests**, so both were *established*
rather than asserted — and re-established in Task 4 against the finished slice rather
than inherited from Task 1's smaller suite. One mutation run proves both: move the
`insert_inference_call` block back below `yield StreamEvent::Done` and run
`cargo test -p sensei-gateway --lib`.

- **AC2** — exactly one test goes red, and it is AC1's: `441 passed; 1 failed`, panicking
  at `tests.rs:6573` with `left: 0, right: 1` on `UsageTotals { requests: 0, … }`. The
  fixture has teeth.
- **AC6** — the other 441 stay green, including every `collect_stream`-based streaming
  test. The sharpest of them is `a_streamed_calls_persisted_duration_includes_acquisition`:
  it *asserts a metering row exists* (`streamed.len() == 1`) and still finds one under the
  mutation, because draining to `None` over-polls the generator and runs the write anyway.
  That is the whole reason AC1 could not be written against `collect_stream`, demonstrated
  rather than argued. `execute_stream_yields_chunks_then_done_with_cost` is named in the
  table as the AC6 representative — it is the one existing streaming test Task 3 had to
  touch at all, and only to add `..` to a pattern; its assertions are unchanged.

The source file was restored byte-identical after the mutation (`git diff crates/` empty,
verified before committing).

- [x] **Step 5: Commit**

```bash
git add -A
git commit -m "docs: SP-ROUTE-1.2 streaming parity (Task 4, AC7)"
```

---

## Self-review notes

**Spec coverage:** §2.1 → Task 1 (AC1, AC2, AC6). §2.2 → Task 2 (AC3). §2.3 → Task 3 (AC4, AC5). §5 AC7 → Task 4.

**The test that must not be waved through is AC1**, and the reason is specific: it is the only test in this slice that `collect_stream` cannot substitute for. Written against `collect_stream` it passes whether the write is above or below the yield — which is not hypothetical, it is how this exact defect survived a review that caught the dispatch beside it. Its mutation check (move the write back below the yield) is mandatory.

**AC5 is second.** SP-ROUTE-1 Task 11 shipped a re-derivation at an attachment site that survived the whole suite until a reviewer mutated it. The same shape is available here.
