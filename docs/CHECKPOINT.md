# Checkpoint

**Last release: v0.12.0 (2026-10-20)** — PR #92 → tag on main `6aa26e0`, GitHub release published
(v0.11.0's backfilled too). SP-DEC-3: Laya local System One (`laya` router) + `GatewayError::InputRejected`
[breaking]; plus quinn-proto 0.11.19 / rustls 0.23.46 for new RUSTSEC advisories. **Current slice: SP-DEC-4.**

## Done

- v0.12.0: gates green (1869 tests, stable clippy, fmt, cargo audit, local `qlty check` clean); consumer crate
  pinned to `tag = "v0.12.0"` builds and uses `InputRejected` + the `laya` adapter.
- torii#45: Phase 1 → v0.11.0; Phase 2 wave A → torii #57.

## Todo

- [ ] SP-DEC-4: System One benchmark harness + per-scenario ranking report (no routing by score).
      Data: MIXED — public labelled datasets fetched at run time + hand-curated gate sets the user signs off.
- [ ] torii re-pin to v0.12.0 (add the `InputRejected` arm to exhaustive `GatewayError` matches).
- [ ] torii#45 Phase 2 wave B (#46 #49 #55), then wave C #47.

## Next

SP-DEC-4 brainstorming: next clarifying question after the data source. Laya venv at `/tmp/laya-bench/env`
(`LAYA_HOST=127.0.0.1 LAYA_PORT=8766 LAYA_MODELS=english,multilingual …/laya-coreml-serve`; 8765 is taken).

## Open questions

- Upstream: ask Laya to report head truncation (external contact — user's call).
- Bundle id `convaiinnovations/laya` skips D7 with budget 192 (kept; one-line change to refuse).

## Known-broken / carry-forwards

- SP-DEC-3: Laya 413/422 still `ProviderError`; no per-model `head_max_len` override; exact tokenizer
  counts; unmetered truncated hosted calls; image guard keyed on id `laya`; estimate errs ~1.7× high.
- Qlty Cloud statuses fail on billing (qlty.sh workspace) — local `qlty check` is the accepted substitute.
- Flaky: `orchestrator agent::sandbox::tests::a_backgrounded_straggler_is_reaped_on_clean_exit` (timing).
- sensei daemon `update_phase` 404 (2026-10-20). torii DB-gated tests don't run in CI (#55).
