# Checkpoint

**Current slice: SP-DEC-3 — local open-weight System One via Laya-CoreML** (`laya` router id,
`laya-coreml-serve` over HTTP). Branch `sp-dec-3-laya` (worktree `../gateway-sp-dec-3`), **commit gate
PASSED, not yet merged to develop** (merge was permission-blocked — needs the user). **Breaking → 0.12.0.**
Spec `docs/superpowers/specs/2026-10-20-sp-dec-3-laya-local-system-one-design.md` (D1–D7, AC1–AC10).

## Done

- T1–T8 + whole-slice review fixes R1–R4 + final-review groups A–C, every task spec- and quality-reviewed.
- D5 `GatewayError::InputRejected` (falls back always, never retried, breaker/health-neutral, neutral to
  AllGated); D6 Laya per-question head budget (conservative estimate, +2/option +1/head, fixture-measured);
  D7 re-routed checkpoint (alias-canonicalised, runs first). Docs + 0.12.0 upgrading notes on every surface.
- Gate: `cargo test --workspace` 1869 passed · stable clippy · fmt · live english/multilingual/engine ·
  docs-sync · site copies identical.

## Todo

- [ ] Merge `sp-dec-3-laya` → develop (`git merge --no-ff`), re-run `cargo test --workspace`, push.
- [ ] Release 0.12.0 later (release checklist) + torii re-pin (exhaustive `GatewayError` matches).
- [ ] SP-DEC-4: System One benchmark harness + per-scenario ranking report (user chose option 1).
- [ ] torii#45 Phase 2 wave B (#46 #49 #55), then wave C #47.

## Next

User merges (or grants permission), then: `cd ../gateway && git merge --no-ff sp-dec-3-laya`.

## Open questions

- Bundle id `convaiinnovations/laya` skips D7 with budget 192 (kept; one-line change to refuse instead).
- Upstream: ask Laya to report head truncation (external contact — user's call).

## Known-broken / carry-forwards

- Laya 413/422 still `ProviderError` (breaker-counted); no per-model `head_max_len` override for
  `LAYA_EXTRA_MODELS`; exact tokenizer counts; unmetered truncated hosted calls; image guard keyed on id `laya`.
- Estimate errs high (~1.7× English, 2–7× non-Latin). Docs-sync id list is hand-kept (LOW).
- sensei daemon `update_phase` 404 (2026-10-20). Scratch: `/tmp/laya-bench` (venv), `/tmp/laya-src`,
  `/tmp/clm-src`, `/tmp/r3-scripts`, `/tmp/rev-r3` — delete after merge.
