# Checkpoint

**Last slice: SP-DEC-3 — DONE, merged to develop (`4955595`, pushed 2026-10-20).** Local open-weight
System One via Laya-CoreML (`laya` router) + `GatewayError::InputRejected`. **Breaking → next release
0.12.0** (torii re-pin: exhaustive `GatewayError` matches). Spec
`docs/superpowers/specs/2026-10-20-sp-dec-3-laya-local-system-one-design.md` (D1–D7, AC1–AC10).

## Done

- SP-DEC-3: D5 InputRejected (falls back always, never retried, breaker/health-neutral, neutral to AllGated);
  D6 Laya per-question head budget (conservative estimate, fixture-measured); D7 re-routed checkpoint.
  Gate on merged develop: `cargo test --workspace` 1869 passed, stable clippy, fmt, live, docs-sync.
- torii#45: Phase 1 → v0.11.0; Phase 2 wave A → torii #57.

## Todo

- [ ] SP-DEC-4: System One benchmark harness + per-scenario ranking report (option 1; no routing by score).
- [ ] Release 0.12.0 (release checklist) + torii re-pin — when the user calls it.
- [ ] torii#45 Phase 2 wave B (#46 #49 #55), then wave C #47.

## Next

Brainstorm SP-DEC-4 (`superpowers:brainstorming`). Laya venv kept at `/tmp/laya-bench/env`
(`LAYA_HOST=127.0.0.1 LAYA_PORT=8766 LAYA_MODELS=english,multilingual …/laya-coreml-serve`; 8765 is taken).

## Open questions

- Upstream: ask Laya to report head truncation (external contact — user's call).
- Bundle id `convaiinnovations/laya` skips D7 with budget 192 (kept; one-line change to refuse).

## Known-broken / carry-forwards

- SP-DEC-3: Laya 413/422 still `ProviderError`; no per-model `head_max_len` override; exact tokenizer
  counts; unmetered truncated hosted calls; image guard keyed on id `laya`; estimate errs ~1.7× high.
- Flaky: `orchestrator agent::sandbox::tests::a_backgrounded_straggler_is_reaped_on_clean_exit` timed out
  (8.0s > 5s) once under full-workspace load on a cold build; passed alone 3/3 and in the next full run.
- sensei daemon `update_phase` 404 (2026-10-20). torii DB-gated tests don't run in CI (#55).
