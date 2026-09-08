# Checkpoint

**Slice: SP-DOC-1 — doc-truth pass. COMPLETE.** Plan
`docs/superpowers/plans/2026-09-08-sp-doc-1-doc-truth.md`, Tasks 1–6, 31 items + 2 coverage gaps, all
closed (`d5b88cd`). After SP-7b.1 (Tasks 1–5) and SP-7b, on `main` via PR #54 — `main` = `24d1868`.

## Done — `SP-DOC-1 Task N`, not any implementation slice's `Task N`

A 13-agent audit swept every superpowers spec/plan for claims about EXISTING CODE that the code
falsifies: 36 candidates → an adversarial verifier refuted 5 → **31 confirmed**, 3 re-derived by
hand. A second 13-agent pass applied them, each diff adversarially re-reviewed. The two biggest:

- **The executor is not a concurrent DAG scheduler.** `drive` is `for node in ready { .await }` —
  sequential. `Executor.concurrency` caps `Map` CHILDREN; the only `Semaphore` is in `fanout.rs`.
- **Snapshot-seeded resume was never wired.** `start_inner` folds the WHOLE journal, `latest_snapshot`
  has no non-test caller, and the spec's `SnapshotWritten` variant never existed in `crates/`.

Security-relevant: the Linux-sandbox plan prescribed `ABI::V1` (leaves `truncate(2)` unmediated;
shipped is V5), the subprocess spec claimed a portable mem cap macOS fails closed on, and the
broker docs recorded redact-then-scrub when the shipped order is scrub-then-redact.

**The re-review earned the pass its trust:** 13 defects caught, 11 fixed, overwhelmingly NEW
falsehoods the corrections introduced (an invented `torii config status`; an amendment negating a
halt that still exists as `pause_context_floor`). Without it: one set of false claims for another.

## Verified

`cargo test --workspace` **1760 / 0 failed, real exit 0** — UNCHANGED from baseline, the required
property for a docs-only pass, not merely green · clippy `-D warnings` 0 · `fmt --check` 0 · **zero
files under `crates/` in the diff** (30 files, +947/−17, all docs).

## Next

1. **Open the develop→main PR.** `develop` is 12 ahead, clean, pushed; it contains main's merge
   commits so it will not sit BEHIND. The natural batch boundary.
2. **SP-7c** — no spec, named nowhere; begins at `/sensei:design`. Two verified gaps for it:
   `Message::attachments` uncounted by the estimator (so the gate can admit what a call's images
   push over the window), and §4.5's wall-clock input drift.

**Sensei daemon NOT running — this file is the record.**
