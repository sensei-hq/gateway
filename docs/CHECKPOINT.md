# Checkpoint

**Slice:** SP-6 s1 — `AwaitSignal`, the HITL primitive. **COMPLETE and on `main`.**

## Done / remaining

- **SP-6 s1 ✅** — `AwaitSignal` node, `SignalAwaited`/`SignalReceived` events (FORMAT_VERSION stays 1),
  `torii run signal`, `run list-paused` awaiting-node reporting. Merged to `main` via PR #46.
- **SP-DATA 1–5 ✅**, SP-3 ✅, SP-4 ✅ — all on `main` via the same PR.
- **Whole-slice review ✅** — 5 parallel reviewers, 13 findings deduped, **12 fixed red-first**, 1 LOW carried.
  Fixes: scheduler poison-pill (one unloadable journal stalled the whole paused fleet); the payload cap
  measured `serde_json` bytes while the durable column is `jsonb` (4088 accepted → **181,320 stored**,
  measured on postgres:16); `--payload-file` (the flag was argv-only, leaking a pasted secret to
  `ps`/history/CI); `redact_keyed` missed arrays (`{"tokens":["<secret>"]}` journaled in the clear);
  six doc/spec conformance corrections.
- **SP-6 s2 `HumanGate` ⬜** — not started. **No spec, no plan on disk.**

## Next command

Nothing is queued — SP-6 s2 needs design before code:

```
/sensei:brainstorm SP-6 s2 HumanGate — typed approve/reject/choose over AwaitSignal
```

## Open questions

- **LOW finding, unfixed by design.** `adding_the_signal_events_does_not_break_old_event_loading`
  (`crates/orchestrator-core/src/journal.rs:465`) only deserialises `RunStarted`, so no mutation confined
  to the two new variants can break it. Rename it, or make it fold a mixed journal.
- 6 pre-existing unresolved rustdoc links (`InMemoryJournal` ×2, `ContentRef`, +3). None from this slice.
- Deferred from s1 §8: business-level signal key, non-CLI delivery (needs an auth model), N-of-M approval,
  no `OrchestratorHooks` callback for either signal event.

## Known-broken

None. **1427 passed / 0 failed / 7 ignored, exit 0.** `clippy -D warnings` clean, `fmt` clean, tree clean.

## Repo state

All history re-authored to `Sensei-HQ <hi@sensei-hq.com>` — 664 commits, all branches + 28 tags,
force-pushed. `main` protection (ruleset 20638300) **active**, 4 rules, 0 bypass actors, verified via
`rules/branches/main`. Backups: `~/Developer/gateway-backups/` (pre-rewrite bundle + ruleset JSON).
**Every SHA changed — other clones must `reset --hard`, not `pull`.**
