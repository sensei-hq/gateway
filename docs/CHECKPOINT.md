# Checkpoint

**SP-7a (window-aware selection) — COMPLETE. All seven tasks done, on `develop`, unpushed.**

## What shipped

`ContextWindowGate` — the sixth `AdmissionGate`, registered LAST — asks the window question PER
CANDIDATE, so `[128k, 8k]` serves a 20k prompt instead of refusing it against the chain minimum. Over
EVERY window is an `AllGated` naming each candidate's window, the estimate and
`HumanAction::UseLargerContextWindow`: refused, never truncated. `PromptOverBudget`, `over_budget`,
`est_prompt_tokens`, `est_tokens` gone; when BUDGETED the clamp still refuses before selection.

**Task 7 = the gate + the sweep.** `orchestrator-overview.md` now records SP-7a shipped and SP-7 as
three slices (a selection / b context budgeting / c semantic activation), split because 7a changes
WHICH MODEL serves a prompt and not one byte of it while `agent_input_hash` hashes
`{chain, system, messages, tools}` — 7b's truncation moves that key, 7a does not. Plus 3 slice-table
rows and 2 stale claims (`README.md`'s "per-turn window budget"; the Gherkin "smallest model").

## Verified — real exit codes

`cargo test --workspace` **1714 passed / 0 failed / 56 ignored, exit 0** (35 suites) · `clippy
--workspace --all-targets -- -D warnings` **0** · `fmt --all --check` **0** · `cargo doc` unresolved
links **16 = baseline**. No container started; `$DATABASE_URL` never read.

## Known-broken — one PRE-EXISTING flake, not this slice

`executor::tests::both_clamp_signals_fire_when_the_clamp_bit_and_the_estimate_was_low` failed 1 of 4
full-workspace runs (`under-estimated` warn uncaptured; the `clamp bit` info beside it captured).
Only a doc comment changed in Rust this round. ~17 tests hit that `tracing::warn!` callsite with no
subscriber on their thread and one asserts it ⇒ thread-local `set_default` racing the global
callsite-`Interest` cache. Green 6/6 isolated, 20/20 loaded, 1-threaded — no red-first repro, so
reported not patched; likely fix `rebuild_interest_cache()` after `set_default`.

## Next command

`git push origin develop` (the coordinator pushes), then the `develop` → `main` PR — merge
`origin/main` into `develop` FIRST or the strict ruleset leaves it BEHIND. Deferred (spec §8): bound
the clamp by the SELECTED candidate's window (AC1 when budgeted); TTS/image/video input bounds; an
`Embed` AGGREGATE limit. **Sensei daemon is down, so this file is the only durable record.**
