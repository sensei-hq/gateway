# Checkpoint

**Current slice: SP-DEC-3 — local open-weight System One via Laya-CoreML** (`laya` router id,
`laya-coreml-serve` over HTTP). Spec: `docs/superpowers/specs/2026-10-20-sp-dec-3-laya-local-system-one-design.md`
(`e849c7e`). Paused behind it: epic [torii#45](https://github.com/sensei-hq/torii/issues/45) Phase 2 wave B.

## Done

- SP-DEC-3 design approved (D1 `laya` facade id · D2 refuse images pre-send · D3 `usage.truncated` →
  ProviderError · D4 additive only); spec committed. HTTP cost measured: +0.35 ms/decision on ANE.
- torii#45: Phase 1 → v0.11.0; torii #54 #56; Phase 2 wave A → torii #57 (`2724a1d`).

## Todo

- [ ] SP-DEC-3: user reviews spec → plan (`docs/superpowers/plans/2026-10-20-sp-dec-3-*.md`) → build
  red-first (AC1–AC7) → live test vs `laya-coreml-serve` → whole-slice review → docs/site sync.
- [ ] torii#45 Phase 2 wave B: #46 config init · #49 budgets from caps · #55 follow-ups; then wave C #47.

## Next

User review of the SP-DEC-3 spec, then `superpowers:writing-plans`. Live server for AC6:
`cd /tmp/laya-bench/scripts && LAYA_HOST=127.0.0.1 LAYA_PORT=8765 LAYA_MODELS=multilingual ../env/.venv/bin/laya-coreml-serve`

## Open questions

- None for SP-DEC-3. torii#55: may an operator correct a tool-confirmation decision before resume?

## Known-broken / carry-forwards

- SP-DEC-3 out of scope: in-process CoreML Laya; CLM-8B (needs vLLM/CUDA — revisit if ported to Metal);
  `/v1/systemone/batch`. Scratch `/tmp/laya-bench` (134 MB) to delete at slice end.
- sensei daemon `update_phase` / `get_workflow_state` return HTTP 404 (2026-10-20); checkpoint lives here.
- Process crash between a paid response and its `EffectRecorded` re-buys one call (documented).
- torii DB-gated tests don't run in CI (#55). Gateway engine follow-ons: gateway#88.
