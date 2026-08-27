# Checkpoint

**Slice:** SP-6 s2 `HumanGate` — the typed menu over `AwaitSignal`. **Reviewed, re-reviewed, MERGED
to `develop`** (`a891442`; `merge-base --is-ancestor` exits 0). Spec+plan:
`docs/superpowers/{specs,plans}/2026-08-27-sp-6-s2-human-gate*`.

## Done / remaining

- **SP-6 s2 ✅ tasks 1–8** — the node kind, its two journal events, the shared waiting arms, the
  fold, `validate_dag`, `run gate approve|reject|decide`, `list-paused` menus, the cross-process
  Postgres e2e. Full detail: `docs/superpowers/orchestrator-overview.md` §3.
- **Reviews ✅** — whole-slice: 1 Critical, 1 HIGH, 5 Medium, 11 doc. Re-review OF those fixes: 7
  more in three commits — 3 HIGH in `gate decide`'s post-append classification (honoured `reject`
  reported "not read"; both "not read" arms untested; a journal fault echoed raw) · 2 MED · 2 doc.
- **SP-6 s1 · SP-DATA 1–5 · SP-3 · SP-4 ✅** on `main` (PR #46). **SP-6 s3 human-as-Agent ⬜** — last
  SP-6 slice, no spec or plan on disk.

## Next command

`main` is protected (strict, no bypass): CI `build · test` + `coverage (gateway >= 80%)` AND a human review — and **develop must contain main's merge commits or the PR sits BEHIND**, hence the merge.

```
git push origin develop && git fetch origin && git merge origin/main && gh pr create --base main --head develop
```

## Open questions

- **`AwaitSignal` and `HumanGate` differ in TWO places, not one** (spec §6.1): where the answer-read
  sits, AND whether the clock is re-read after journaling a fresh deadline (only s1 does, so a gate
  given a nanosecond pauses once on a past instant). s3 is the third waiting kind; it inherits both.
- **AC12 passes without running** — `DATABASE_URL`-guarded, returns early, counted green having
  exercised nothing; a raw-stderr `SKIP` is the only signal (§9).
- Deferred (§10): authorization (`actor` is attribution, not authentication), `RunStatus::Rejected`,
  non-CLI delivery, N-of-M approval, no hook callback for a gate event.

## Known-broken

None. **1505 passed / 0 failed / 7 ignored, exit 0** (`env -u DATABASE_URL cargo test --workspace`).
clippy `-D warnings` + fmt clean; `cargo doc` back to the 8 warnings that pre-date this slice.
