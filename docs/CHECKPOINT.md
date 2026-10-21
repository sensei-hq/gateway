# Checkpoint

**Last release: v0.12.0 (2026-10-20)** — PR #92 → tag on main `6aa26e0`. **SP-DEC-4 on main
2026-10-21 (PR #93, `60de4b5`)**; unreleased (no tag).

## Done

- 2026-10-21: gateway PR #93 merged to main (`60de4b5`, SP-DEC-4 + RUSTSEC-2026-0382/0383); develop fast-forwarded.
  torii PR #58 merged (`913500f`): gateway re-pinned to v0.12.0 + the same advisory fix.
- S4 labels signed off by the user 2026-10-20 (`3f919f1`); report re-rendered, no "unreviewed" banner.
- SP-DEC-4 (System One benchmark): `crates/decision-bench` (check / fetch / run / report), S1–S4, first
  local report `docs/benchmarks/2026-10-20-system-one-local.md` (5 models, laya-ane NOT RUN: no input fits
  96-token window). Whole-slice review (5 reviewers) + re-review: 1 HIGH + 13 MEDIUM + cheap LOWs fixed red-first.
  Gates: 2101 tests, stable clippy, fmt, local qlty clean.
- v0.12.0 released; torii#45 Phase 1 → v0.11.0; Phase 2 wave A → torii #57.

## Todo

- [ ] torii#45 Phase 2 wave B (#46 #49 #55), then wave C #47.
- [ ] torii: 4 Dependabot alerts on its default branch (1 high, 3 moderate), not yet looked at.

## Next

torii#45 Phase 2 wave B (#46 #49 #55; worktrees torii-p2-46/-49/-55, integrate/phase2-wave-b) — rebase onto torii develop `913500f` first.

## Open questions

- Upstream: ask Laya to report head truncation (external contact — user's call).
- Kernel could keep each attempt's error kind (connect / timeout / pre-send): today the bench infers
  "reached the server" from timing, and a connect that hangs to the timeout reads as reached.

## Known-broken / carry-forwards

- SP-DEC-4: hosted models, routing by score, calibration fitting, multi-question cases, bench CI job;
  laya-english S4 0% coverage is the D6 estimate over-counting (~1.7× high), not a model failure.
- SP-DEC-3: Laya 413/422 still `ProviderError`; no per-model `head_max_len` override; exact tokenizer counts.
- Qlty Cloud statuses fail on billing — local `qlty check` is the accepted substitute.
- CI rustdoc (`RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps --locked`) is not in the pre-commit hook — run it before a PR.
- Flaky: `orchestrator agent::sandbox::tests::a_backgrounded_straggler_is_reaped_on_clean_exit` (timing).
- sensei daemon `update_phase` 404 (2026-10-20). torii DB-gated tests don't run in CI (#55).
