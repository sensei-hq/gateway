# Checkpoint

**SP-7a (window-aware selection) — Tasks 1-6 shipped; the whole-slice review round is now applied.**
Task 7 (release gate + checkpoint) is the only step left.

## Done

The window question lives in selection: `ContextWindowGate` asks it PER CANDIDATE, so `[128k, 8k]`
serves a 20k prompt instead of refusing it against the chain minimum. The orchestrator's
`over_budget` / `PromptOverBudget` halt, `est_prompt_tokens` and (this round) the dead `est_tokens`
are gone.

## The review round — 21 findings, all addressed

**Behaviour fixed, red-first:** `Embed` is gated on the LARGEST text, not the batch sum (a Critical:
100 RAG chunks of 300 bytes were terminally refused by an 8k model) · `Tts`/`ImageGenerate`/
`VideoGenerate` are no longer gated at all (`context_window` is a chat-model field they do not
publish) · new config Rule 6 rejects `context_window == 0` · `all_gated_error` keeps `human_action`
beside a `resume_after`, `AllGated`'s `Display` renders both, and `classify_gateway_error`'s PAUSE
arm now uses `err.to_string()` so the diagnosis reaches the journaled `RunPaused` · the SP-DATA-5
clamp's `BelowFloor` refusal says `"context window: …"` and offers no cap raise when the WINDOW is
the binding term (measured identical at caps of 1e6 and `u64::MAX`).

**Untested properties now guarded** (each mutation-proven): the gate's registration position, both
halves — health-before-window (pause vs terminal) and budget-before-window · AC1's success path at
the EXECUTOR boundary via a new `two_window_chain_config` · the over-window node's journal shape
(`NodeStarted` now present) and its `on_agent_turn` hook · AC9's "selection may differ" made real
(run 2 resolves the same chain name to a different model).

**False comments rewritten:** `min_context_window`'s doc, three `dispatch.rs` paragraphs naming
deleted functions, the AC1 claims in `agent.rs`/`prompt.rs` (unqualified for a BUDGETED run), the
"unknown window → `no_estimate_admits`" mis-mapping, and four feature-doc sites.

## Verified — real exit codes

`cargo test --workspace` **1714 passed / 0 failed / 56 ignored, exit 0** · `clippy --workspace
--all-targets -- -D warnings` **exit 0** · `fmt --all --check` **exit 0** · `cargo doc` unresolved
links **16** (baseline). No database started; `$DATABASE_URL` never read.

## Next command

Task 7: `rg -n 'PromptOverBudget|min_context_window|over_budget' --no-ignore -g '!target' crates/
docs/` for any last surface, then the `develop` → `main` PR.

## Known-broken

Nothing. Deferred and written into the spec's §8: a real per-capability input bound for TTS/image/
video, an `Embed` per-request AGGREGATE limit, batch splitting, and bounding the SP-DATA-5 clamp by
the SELECTED candidate's window (which is what would make AC1 hold on a budgeted run).
**The sensei daemon is not running, so this file is the only durable record.**
