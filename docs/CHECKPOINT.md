# Checkpoint

**Slice: SP-7c — multimodal window correctness. BUILD COMPLETE, review not yet run.**
Spec `2026-09-08-sp-7c-multimodal-window-design.md` (7 ACs) + plan, both ticked. `3918784`.
`main` = `c9d4b06` (PR #55: SP-7b.1, the clamp flake, SP-DOC-1).

## Done

The estimator did not count `Message::attachments` at all, so every per-candidate
`ContextWindowGate` decision on a multimodal request was made on the request's TEXT alone — a
request whose images pushed it past a window was declared to fit and the provider rejected it.
Each attachment is now charged `MAX_TOKENS_PER_ATTACHMENT` = 4784.

- **A declared ceiling, not a measurement.** The doc had already ruled out both string shapes
  (base64 over-counts by 2–3 orders of magnitude; a URL's length means nothing). 4784 = the largest
  published per-image figure (high-res 2576px tier; ~1600 for 1568px) — the bound must not
  under-count for ANY candidate in a mixed-tier chain. Charged in TOKENS after the `/3` divide, as
  the superseded doc prescribed; exhaustive match so a future variant fails to compile.
- **Premise recorded, and it scopes the slice:** the hole is REAL but LATENT — all 20
  `with_attachment` sites are tests and the executor passes `Vec::new()` everywhere. It justifies
  fixing the estimator+gate; it does not justify a producer, and none was built.

## Verified

`cargo test --workspace` **1766 / 0 failed, real exit 0** (baseline 1760 + 6) · clippy 0 · fmt 0 ·
diff = 2 code files. **Six mutations run:** charge-0, per-message, before-the-divide and
`wrapping_add` each redden their own test; AC3 survived all four so it was verified separately
against price-base64-by-length (reddens alone); AC4 against charge-every-message. AC5 asserts the
real composition (payload → estimator → gate), text-only twin still admitted.

## Next

1. **`/sensei:review`** — the whole-slice adversarial review has NOT run. Every prior slice found
   something; this project's record is that fixes introduce defects.
2. Then the develop→main PR (`develop` is 3 ahead).

## Open

Deferred in spec §8, none blocking: a real per-image cost model (needs decoding or a provider count
endpoint); tier-aware ceilings (plumbing exists, not worth a model-tracking table yet); an
orchestrator producer (its own slice — prompt assembler, `agent_input_hash`, journal, redaction).

**Sensei daemon NOT running — this file is the record.**
