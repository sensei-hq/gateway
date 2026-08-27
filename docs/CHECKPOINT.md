# Checkpoint

**Slice:** SP-6 s2 — `HumanGate`, the typed menu over `AwaitSignal`. **Reviewed, re-reviewed, and
MERGED to `develop`** (merge commit `a891442`; `git merge-base --is-ancestor
feat/sp-6-s2-human-gate develop` exits 0). Spec + plan under
`docs/superpowers/{specs,plans}/2026-08-27-sp-6-s2-human-gate*`.

## Done / remaining

- **SP-6 s2 ✅ tasks 1–8** — `GateOption`/`GateOutcome` + `GateAwaited`/`GateDecided` (new variants
  ⇒ `FORMAT_VERSION` stays 1) · `NodeKind::HumanGate` + `validate_dag` rules · s1's waiting node
  split into the shared `gate_precheck`/`wait_or_expire`/`pause_awaiting` · the gate fold (decisions
  last-wins, menus first-wins) · `run_human_gate` · conditional `Branch` exhaustiveness ·
  `torii run gate approve|reject|decide` · `list-paused` menus + the cross-process Postgres e2e.
- **Whole-slice review ✅** — 1 Critical, 1 HIGH, 5 Medium, then 11 doc/spec findings.
- **Re-review OF the review fixes ✅** — 7 findings, three commits on `develop`: 3 HIGH in
  `gate decide`'s post-append classification (a honoured `reject` reported "not read"; both
  "not read" arms asserted by nothing — the collapse left 192 green; a journal fault echoed raw,
  fixed in `cmd::run::signal` too) · 2 MED bounds (`--as`, the success line) · 2 MED docs.
- **SP-6 s1 ✅ · SP-DATA 1–5 ✅ · SP-3 ✅ · SP-4 ✅** — all on `main` via PR #46.
- **SP-6 s3 human-as-Agent ⬜** — the last SP-6 slice. No spec, no plan on disk.

## Next command

`main` is protected (strict, no bypass): CI `build · test` + `coverage (gateway >= 80%)` AND a human
review, and **develop must already contain main's merge commits or the PR sits BEHIND**.

```
git push origin develop && git fetch origin && git merge origin/main && gh pr create --base main --head develop
```

## Open questions

- **`AwaitSignal` and `HumanGate` differ in TWO places, not one** (spec §6.1): where the answer-read
  sits, AND whether the clock is re-read after journaling a fresh deadline (only s1 does, so a gate
  given a nanosecond pauses once on a past instant). s3 is the third waiting kind — it inherits both.
- **AC12 passes without running** — `DATABASE_URL`-guarded, returns early, counted green having
  exercised nothing; a raw-stderr `SKIP` line is the only signal (spec §9).
- Deferred (§10): authorization (`actor` is attribution, not authentication), `RunStatus::Rejected`,
  non-CLI delivery, N-of-M approval, no hook callback for either gate event.

## Known-broken

None. **1505 passed / 0 failed / 7 ignored, exit 0** (`env -u DATABASE_URL cargo test --workspace`).
`clippy --workspace --all-targets -D warnings` + `fmt --all` clean; `cargo doc` back to the 8
warnings that pre-date this slice.
