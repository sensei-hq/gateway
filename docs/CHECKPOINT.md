# Checkpoint

**Slice:** SP-6 s2 — `HumanGate`, the typed menu over `AwaitSignal`. **Code-complete, reviewed, on
`feat/sp-6-s2-human-gate`.** Spec + plan on disk under `docs/superpowers/{specs,plans}/2026-08-27-sp-6-s2-human-gate*`.

## Done / remaining

- **SP-6 s2 ✅ tasks 1–8** — `GateOption`/`GateOutcome` + `GateAwaited`/`GateDecided`
  (new variants ⇒ `FORMAT_VERSION` stays 1) · `NodeKind::HumanGate` + its `validate_dag` rules ·
  s1's waiting node split into the shared `gate_precheck`/`wait_or_expire`/`pause_awaiting` ·
  the gate fold (decisions last-wins, menus first-wins) · `run_human_gate` · conditional
  exhaustiveness for a `Branch` on a gate · `torii run gate approve|reject|decide` ·
  `run list-paused` showing the menu + the cross-process Postgres e2e.
- **Whole-slice review ✅** — 1 Critical (`decide` checked the RUN, not the NODE), 1 HIGH
  (two untested properties), 5 Medium, all fixed; then 11 doc/spec conformance findings fixed.
- **SP-6 s1 ✅ · SP-DATA 1–5 ✅ · SP-3 ✅ · SP-4 ✅** — all on `main` via PR #46.
- **SP-6 s3 human-as-Agent ⬜** — the last SP-6 slice. No spec, no plan on disk.

## Next command

Merge the finished slice to `develop`, then push (no PR needed for develop):

```
git checkout develop && git merge --no-ff feat/sp-6-s2-human-gate && git push origin develop
```

## Open questions

- **`AwaitSignal` and `HumanGate` order their answer-read differently** (spec §6.1): a gate is
  expired BEFORE its decision is read, an `AwaitSignal` completes on a folded signal even past its
  deadline (unless a drive already expired it). Deliberate for s2; s1 was left as-is. Revisit in s3.
- **AC12 passes without running** — it is `DATABASE_URL`-guarded and returns early, so it is counted
  green having exercised nothing; the raw-stderr `SKIP` line is the only signal (spec §9).
- Deferred from §10: authorization (`actor` is attribution, not authentication), `RunStatus::Rejected`,
  non-CLI delivery, N-of-M approval, no `OrchestratorHooks` callback for either gate event.

## Known-broken

None. **1497 passed / 0 failed / 7 ignored, exit 0** (`env -u DATABASE_URL cargo test --workspace`).
`cargo clippy --workspace --all-targets -- -D warnings` clean, `cargo fmt --all` clean.
