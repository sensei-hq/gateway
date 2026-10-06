# Checkpoint

**Agentic completion — epic [torii#45](https://github.com/sensei-hq/torii/issues/45): complete the
agentic runtime and incorporate it into torii.** Follows epic #76 (done 2026-10-06: the gateway is a
library, torii owns persistence, the stores and the `torii` CLI; gateway v0.10.0, torii pins it).
Last release: v0.10.0 (on main). torii `develop` now carries DECISIONS §11 (torii PR #32).

## Todo (the epic's actionable checklist, in order)

- [ ] **AG-1** gateway#85 — wire the planner discovery tools (SP-REG-2) per run in `Executor::pinned`
- [ ] **AG-2** gateway#86 — `OrchestratorHooks` for HITL events (signal, gate, agent answer, loop gate)
- [ ] **AG-3** gateway#87 — scheduler wake backoff / jitter / `max_attempts` (+ testkit; torii store)
- [ ] gateway release carrying AG-1..AG-3; torii re-pins all 13 sensei-* deps (`one_gateway_ref`)
- [ ] **AG-5** torii#34 — boot seams: hooks, transient retry, concurrency, lease, registry reload
- [ ] **AG-4** torii#33 — `torii run results <id>`
- [ ] **AG-6** torii#35 — `torii config show` / `config pull`
- [ ] **AG-7** torii#36 — refuse UUID-shaped org slugs at write time; fix `authz.sql` declassify

## Next

AG-1 (gateway#85): read `docs/superpowers/specs/2026-09-15-sp-reg-programme-design.md` §SP-REG-2,
then red-first in `crates/orchestrator` (in-memory). Work on a feature branch; `make clean` after
any release; torii work in a torii worktree (the main checkout has the user's uncommitted changes).

## Open questions — decisions the user owns (gate the API, UI and deployment work)

torii#37 C1 agent runtime v1 + UI? · #38 C2 one app or two · #39 C3 shipped registry content ·
#40 C4 mockup-vs-engine contradictions · #41 C5 spend model · #42 C6 who may answer a gate ·
#43 C7 worker deployment/tenancy · #44 C8 X2 schema vs `registry.*`/`runs.*`.

## Known-broken / carry-forwards

- Lower-priority engine follow-ons (SP-7c, HITL/coordinator/budget/sandbox/perf, routing nits,
  the flaky sandbox-straggler test, the `seq: 0` fixture) are tracked in gateway#88.
- torii `authz.sql` declassify case fails on unmodified develop — AG-7 (torii#36).
