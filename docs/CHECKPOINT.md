# Checkpoint

**SP-DEC-1 — Decision (System One) capability, gh#72: T1–T7 DONE on `develop`
(`1ee7a2d`, pushed). NOT released.** Suite **1964 passed / 0 failed / 61
ignored**, real exit 0; fmt + clippy `-D warnings` clean on Homebrew 1.98,
rustup stable 1.99 and `--features local --locked`. Plan:
`docs/superpowers/plans/2026-10-03-sp-dec-1-decision-capability.md` (Progress +
review rows — read first).

Also this session: **gh#73 fixed and released as v0.6.1** (llama-cpp-2 → 0.1.158
vocab API, floor raised); PR #74 develop→main awaits human review.

## Done

T1–T2 types/trait (`e21ade4`) · T3 engine (`8f80053`) · T4 Ollama + generic
SystemOneAdapter + probe (`7bcd6a0`) · T5 facade openrouter/typesafe (`50cafb9`) ·
T6 docs (`ccb0458`) + `adapters::systemone` re-export (`247745e`) · T7 whole-slice
review: 12 must-fix (1 CRITICAL, 4 HIGH, 7 MEDIUM) fixed red-first in `8f41fed`,
`07e780f`, `8a2bc5e`, `bf47f87`, `1ee7a2d`.

sensei seed (separate repo): branch `feat/decision-seed` in worktree
`~/Developer/sensei-hq/sensei-decision-seed` — `25fd9352` test, `e9192043` seed.
**Local only, not pushed.**

## Next

1. Cut gateway **v0.7.0**: `make bump v=minor` (awaiting the user's go-ahead).
2. sensei: bump the gateway pins to v0.7.0 (sensei#202), add
   `map_capability("decision") => Capability::Decision` and the
   `gateway_routers` entries, then push `feat/decision-seed` / open a PR.

## Open questions

Release v0.7.0 now, or batch with more work? Push the sensei branch?

## Known-broken (pre-existing, not this slice)

- sensei `dbd reconcile` fails on `sensei.libraries` (column used by a view) even
  on pristine `origin/develop`.
- `OllamaAdapter::from_config` / `OpenAIAdapter::from_config`: no default timeout
  when `timeout_ms` is unset (chat path).
- LOW carry: estimator test doesn't pin question names/option keys; llms
  README's upgrading row still says "0.3→0.4" first.
