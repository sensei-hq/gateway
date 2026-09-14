# Checkpoint

**Slice: SP-7a.1 — multimodal window correctness. Build DONE; whole-slice review DONE, fixes
landed; not yet merged to main.** Spec+plan: `docs/superpowers/*/2026-09-08-sp-7a-1-*`.
`main` = `2b6351f` (PR #57 CodeQL, #58 anchor-id). `develop` synced with main at `5c7d5d0`.

## Done

The estimator ignored `Message::attachments`, so every `ContextWindowGate` decision on a
multimodal request was made on its TEXT alone. Each attachment now costs 4784 tokens, charged
after the `/3` divide. Real but LATENT: all 18 `with_attachment` invocations are tests.

Five adversarial reviewers (security + data-correctness clean); 11 findings, each hand-verified
before acting. Fixes: `760b2f9` rustdoc placement — the constant was inserted mid-doc-block,
silently reassigning the estimator's 194-line doc to it (public page 4258 → 21509 bytes), same
mistake orphaned a gate test's doc. `bfd5216` test strength — 4784 was asserted against itself
(halving it left 322 tests green), AC3 was green under charge-zero so its plan `Red:` tick was
false. This commit — doc truth on six surfaces: the false "ten images fit every window" (shipped
preset is 8192, so the SECOND image overruns), four stale specs, the orchestrator's now-false
`× 3` identity, miscounted citations (18 sites not 20; no `dispatch.rs` `Vec::new`).

## Verified

`cargo test --workspace` 1767 / 0, real exit 0 · clippy -D warnings 0 · fmt 0 · rustdoc 0 warn.
Mutations: halved ceiling → exit 101, 1 red; charge-zero → exit 101, 5 red.

## Next

1. Rename slice id SP-7c → **SP-7a.1** (spec/plan filenames + in-code markers) — `SP-7c` is
   already bound to "semantic / retrieval-ranked activation" on six surfaces. User-decided.
2. develop→main PR.
3. **Issue #56**, its own slice: commit `Cargo.lock` + `cargo audit`; CI clippy/fmt/audit gates;
   a `site/` job; bump undici 7.28.0 (HIGH) / dompurify / vitest / devalue.

## Open

Deferred, none blocking: tier-aware ceilings (would remove the 8192 over-refusal); a per-image
cost model; an orchestrator producer. Dependabot does NOT parse `site/bun.lock` and sees only
direct Rust crates — feeds #56. **Sensei daemon NOT running; this file is the record.**
