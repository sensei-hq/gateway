# SP-7c — multimodal window correctness Implementation Plan

> Steps use checkbox (`- [ ]`) syntax. Task ids here are `SP-7c Task N` — not `SP-DOC-1 Task N`
> (complete) and not `SP-7b.1 Task N` (complete).

**Spec:** `docs/superpowers/specs/2026-09-08-sp-7c-multimodal-window-design.md` (7 ACs).

**Goal:** charge each `Message::attachments` entry a declared per-image token ceiling inside
`estimate_input_tokens_pessimistic`, so the per-candidate `ContextWindowGate` stops admitting
candidates whose window a multimodal request would exceed.

**Baseline that must not regress:** `cargo test --workspace` = **1760 passed / 0 failed** at
`6ec0b85`; `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --all --check`
both exit 0. AC7 makes the *executor* half of that stricter than "green": no orchestrator path
constructs an attachment, so every existing test must pass unchanged.

**Method:** red-first, one task at a time. Every AC is mutation-verified — the mutation is named in
the task and must redden that test and no other.

**Files:** `crates/gateway/src/engine/util.rs` (the estimator + its `mod tests`),
`crates/gateway/src/gates/context_window.rs` (the gate test only — the gate itself does not change).

---

## Task 1: The constant and the per-attachment term (AC1, AC2, AC3, AC6)

- [x] Red: `an_attachment_is_charged_the_per_image_ceiling` — one image ⇒ text estimate +
      `MAX_TOKENS_PER_ATTACHMENT` (AC1).
- [x] Red: `attachments_are_charged_per_entry_across_messages` — N images over M messages ⇒
      `N × MAX_TOKENS_PER_ATTACHMENT` (AC2).
- [x] Red: `a_url_and_a_base64_attachment_cost_the_same` — a long base64 blob and a short URL
      price identically (AC3). This is the failure mode being removed, so it is pinned directly.
- [x] Add `MAX_TOKENS_PER_ATTACHMENT: u32 = 4784` with the tier table from spec §D2 beside it.
- [x] Add the term as an **exhaustive match** on `MediaAttachment` (spec §D4), summed with
      `saturating_add`, added to the token total **after** the `/3` divide (spec §D3).
- [x] Red: `the_attachment_charge_saturates_rather_than_wrapping` (AC6).
- [x] Mutations, each reddening exactly one test: charge `0`; charge per-message instead of
      per-attachment; add the term to `chars` before the divide; `+` instead of `saturating_add`.

## Task 2: The gate refuses what it used to admit (AC5)

- [x] Red: a candidate whose window the TEXT fits but text+images does not is SKIPPED. Asserted
      through `ContextWindowGate`, not the estimator — admitting an over-window candidate is the
      defect; the estimator is only its cause.
- [x] The gate itself does not change. If this test passes without a gate edit, that is the
      expected result and the task is the test.
- [x] Mutation: charge `0` per attachment ⇒ this test reddens (the candidate is admitted again).

## Task 3: Additivity and the doc (AC4, AC7)

- [x] Red: `a_payload_with_no_attachments_is_unchanged` — byte-identical to today's return (AC4).
- [x] `cargo test --workspace` unchanged in count and result vs. the baseline (AC7).
- [x] Replace the "What it does NOT count" paragraph: quote the superseded text, state what is
      counted now and why 4784, per the amendment convention. Correct the sentence that says no
      producer exists **only if** it has become false — re-check, do not assume.

---

## Final verification

- [x] `cargo test --workspace` — real exit code, unpiped; count = baseline + the tests added here.
- [x] `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --all --check` exit 0.
- [x] `git diff --stat` touches only the two files above plus docs.
- [x] Every mutation in Tasks 1–2 re-run and confirmed to redden its own test alone.
