# Checkpoint

**Torii move — epic [#76](https://github.com/sensei-hq/gateway/issues/76): "Gateway is a library; torii
owns persistence"** (torii `docs/DECISIONS.md` §11, ratified 2026-09-17). Move `crates/torii`, the
orchestrator Postgres store adapters and the `orchestrator` schema to `sensei-hq/torii`; the gateway
keeps the engine, the persistence traits and in-memory stores. Last release: v0.8.0 (on main).

## Todo (the epic's checklist, in order)

- [x] **TM-1** #77 — redirect data-tier docs to §11 (the extraction into the gateway is cancelled)
- [x] **TM-2** #78 — config WRITE path behind a trait (`ConfigStore`)
- [x] **TM-3** #79 — exported store conformance suite
- [x] **TM-4** #80 — `GatewayConfig` source seam + always-on registry↔chain cross-check
- [x] **TM-5** #81 — backend-selectable boot (memory | postgres)
- [x] **TM-6** torii#24 — tenant-scoped orchestrator schema + RLS + per-tenant config versions (torii PR #27, `a5f6b4d`)
- [x] **TM-7** torii#25 — store traits over that schema (passes TM-3) (torii PR #28, `e9dbe59`)
- [ ] **TM-8** torii#26 — move the CLI/worker into torii
- [ ] **TM-9** #82 — delete `crates/torii`, Postgres adapters, `database/`; release (breaking)

## Next

TM-8 (torii#26), decided: FIRST bump torii `services/gateway` to the current gateway API (7 compile
errors: new `routing`/`catalog` fields) and extract `config_loader` into a lib crate shared by
`services/gateway` and the CLI — own PR. Then move `crates/torii` onto `torii-orchestrator-store`
(tenant flag/env) + that loader as its `GatewayConfigSource`; keep the memory backend; port tests
(e2e_pg.rs → per-tenant). torii pins gateway crates by `branch = "develop"` until a release.

## Open questions

Cut gateway v0.9.0 (TM-2..TM-5 + testkit) so torii can pin ONE tag? (asked 2026-10-06)
Torii's main checkout has others' uncommitted changes — always work in a torii worktree.

## Known-broken / carry-forwards

- torii `database/tests/authz.sql` declassify case fails on unmodified develop (pre-existing);
  `run.sh` stops there, so later suites only run individually.
- Out of the epic's scope (agentic execution): SP-REG-2 discovery tools, a results command, SP-REG-4
  shipped content (needs a product decision).
- From SP-DEC-2: Cloudflare never run live; engine counts caller-caused errors against the breaker;
  LOW test gaps. Pre-existing: `OllamaAdapter`/`OpenAIAdapter::from_config` no default timeout.
