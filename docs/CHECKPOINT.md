# Checkpoint

**Slice:** SP-6 s2 `HumanGate` — the typed menu over `AwaitSignal`. **MERGED TO `main`** via PR #47
(`5d9b9d1`). Spec+plan: `docs/superpowers/{specs,plans}/2026-08-27-sp-6-s2-human-gate*`.

## Done / remaining

- **SP-6 s2 ✅ tasks 1–8** — the node kind, its two journal events, the shared waiting arms, the
  fold, `validate_dag`, `run gate approve|reject|decide`, `list-paused` menus, the cross-process
  Postgres e2e. Full detail: `docs/superpowers/orchestrator-overview.md` §3.
- **Reviews ✅** — whole-slice: 1 Critical, 1 HIGH, 5 Medium, 11 doc. Re-review OF those fixes: 7
  more in three commits — 3 HIGH in `gate decide`'s post-append classification (honoured `reject`
  reported "not read"; both "not read" arms untested; a journal fault echoed raw) · 2 MED · 2 doc.
- **SP-6 s1 · SP-DATA 1–5 · SP-3 · SP-4 ✅** on `main`. **SP-6 s3 human-as-Agent ⬜ — the LAST slice
  of SP-6.** No spec, no plan on disk.

## Next command

s3 needs design before code (a human-backed agent whose execution is a pause-for-input):

```
/sensei:brainstorm SP-6 s3 human-as-Agent
```

## Open questions

- **s3's shape is genuinely open:** does a human-backed agent reuse the existing waiting events, or
  does an `Agent` node gain a human-backed `AgentRef`? That decides whether s3 is a thin wrapper or
  a registry change.
- **`AwaitSignal` and `HumanGate` differ in TWO places, not one** (spec §6.1): where the answer-read
  sits, AND whether the clock is re-read after journaling a fresh deadline (only s1 does, so a gate
  given a nanosecond pauses once on a past instant). s3 is the third waiting kind; it must pick
  deliberately rather than inherit by accident.
- Deferred (§10): authorization (`actor` is attribution, not authentication — never branch on it),
  `RunStatus::Rejected`, non-CLI delivery, N-of-M approval, no hook callback for a gate event.

## Known-broken

None. **1505 passed / 0 failed / 7 ignored, exit 0** — verified BOTH without a database and against
live `postgres:16`, where the 6 `e2e_pg` tests ran for real (0 skips; `GateAwaited` + `GateDecided`
rows confirmed in `journal_events`). clippy `-D warnings` + fmt clean; `cargo doc` back to the 8
warnings that pre-date this slice. `target/` cleaned.
