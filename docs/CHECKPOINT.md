# Checkpoint

**Last release: v0.12.0 (2026-10-20)** — PR #92 → tag on main `6aa26e0`. **SP-DEC-4 on main
2026-10-21 (PR #93, `60de4b5`)**; unreleased (no tag).

## Done

- 2026-10-21: gateway PR #93 merged to main (`60de4b5`, SP-DEC-4 + RUSTSEC-2026-0382/0383); develop fast-forwarded.
  torii PR #58 merged (`913500f`): gateway re-pinned to v0.12.0 + the same advisory fix.
- SP-DEC-4 (System One benchmark): `crates/decision-bench` (check / fetch / run / report), S1–S4, first
  local report `docs/benchmarks/2026-10-20-system-one-local.md` (5 models, laya-ane NOT RUN: no input fits
  96-token window). Whole-slice review (5 reviewers) + re-review: 1 HIGH + 13 MEDIUM + cheap LOWs fixed red-first.
  Gates: 2101 tests, stable clippy, fmt, local qlty clean.

## Todo

- [ ] Review + merge torii PR #60 (Phase 2 wave B: #46 #49 #55; CI green). Then wave C #47.
- [ ] Review + merge torii PR #59 (jsonwebtoken >= 10.3, Dependabot #3).

## Next

torii PR #60 (wave B) awaits review; after merge remove worktrees torii-p2-46/-49/-55/-p2b and start wave C #47 (gate authorization; also binds TORII_USER to an authenticated principal).

## Open questions

- Upstream: ask Laya to report head truncation (external contact — user's call).
- Kernel could keep each attempt's error kind (connect / timeout / pre-send): today the bench infers
  "reached the server" from timing, and a connect that hangs to the timeout reads as reached.

## Known-broken / carry-forwards

- torii wave B: a DB that already applied an older `runs.run_charges` needs it rebuilt (two columns added); orphan charges are not swept; dbd 0.27 rejects `model_capabilities.jsonl` record 26 (CI pins 0.24.1); glib alert waits on Tauri.

- SP-DEC-4: hosted models, routing by score, calibration fitting, multi-question cases, bench CI job;
  laya-english S4 0% coverage is the D6 estimate over-counting (~1.7× high), not a model failure.
- SP-DEC-3: Laya 413/422 still `ProviderError`; no per-model `head_max_len` override; exact tokenizer counts.
- Qlty Cloud statuses fail on billing — local `qlty check` is the accepted substitute.
- CI rustdoc (`RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps --locked`) is not in the pre-commit hook — run it before a PR.
- Flaky: `orchestrator agent::sandbox::tests::a_backgrounded_straggler_is_reaped_on_clean_exit` (timing).
- sensei daemon `update_phase` 404 (2026-10-20). torii DB-gated tests don't run in CI (#55).
