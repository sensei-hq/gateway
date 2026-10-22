# Checkpoint

**Last release: v0.12.0 (2026-10-20)** — PR #92 → tag on main `6aa26e0`. **SP-DEC-4 on main
2026-10-21 (PR #93, `60de4b5`)**; unreleased (no tag).

## Done

- 2026-10-22: torii PR #60 merged (`f11728f`, Phase 2 wave B #46 #49 #55 closed; dbd 0.27.5; CI
  Postgres image from Docker Hub by digest) + PR #59 (`dddc38e`, jsonwebtoken >= 10.3).
- 2026-10-21: gateway PR #93 → main (`60de4b5`, SP-DEC-4); torii #58 re-pinned v0.12.0 (`913500f`).
- SP-DEC-4 (System One benchmark): `crates/decision-bench` (check / fetch / run / report), S1–S4, first
  local report `docs/benchmarks/2026-10-20-system-one-local.md` (5 models, laya-ane NOT RUN: no input fits
  96-token window). Whole-slice review (5 reviewers) + re-review: 1 HIGH + 13 MEDIUM + cheap LOWs fixed red-first.
  Gates: 2101 tests, stable clippy, fmt, local qlty clean.

## Todo

- [ ] Review + merge torii PR #61 (pin bun 1.4.3: bun 1.4.4 broke the js gate on develop). Then wave C #47.

## Next

torii wave C #47 (gate authorization; also binds TORII_USER to an authenticated principal), after PR #61 lands.

## Open questions

- Upstream: ask Laya to report head truncation (external contact — user's call).
- Kernel could keep each attempt's error kind (connect / timeout / pre-send): today the bench infers
  "reached the server" from timing, and a connect that hangs to the timeout reads as reached.

## Known-broken / carry-forwards

- torii: update an existing DB with `dbd -e dev reconcile` then `dbd -e dev deploy` (database/README.md); orphan charges are not swept; `packageManager: bun@1.3.14` ≠ CI's 1.4.3; glib alert waits on Tauri.

- SP-DEC-4: hosted models, routing by score, calibration fitting, multi-question cases, bench CI job;
  laya-english S4 0% coverage is the D6 estimate over-counting (~1.7× high), not a model failure.
- SP-DEC-3: Laya 413/422 still `ProviderError`; no per-model `head_max_len` override; exact tokenizer counts.
- Qlty Cloud statuses fail on billing — local `qlty check` is the accepted substitute.
- CI rustdoc (`RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps --locked`) is not in the pre-commit hook — run it before a PR.
- Flaky: `orchestrator agent::sandbox::tests::a_backgrounded_straggler_is_reaped_on_clean_exit` (timing).
- sensei daemon `update_phase` 404 (2026-10-20). torii DB-gated tests don't run in CI (#55).
