# Checkpoint

**Torii move — epic [#76](https://github.com/sensei-hq/gateway/issues/76): "Gateway is a library; torii
owns persistence"** (torii `docs/DECISIONS.md` §11, ratified 2026-09-17). Move `crates/torii`, the
orchestrator Postgres store adapters and the `orchestrator` schema to `sensei-hq/torii`; the gateway
keeps the engine, the persistence traits and in-memory stores. Last release: v0.10.0 (on main, 2026-10-06). **Epic #76 COMPLETE.**

## Todo (the epic's checklist, in order)

- [x] **TM-1** #77 — redirect data-tier docs to §11 (the extraction into the gateway is cancelled)
- [x] **TM-2** #78 — config WRITE path behind a trait (`ConfigStore`)
- [x] **TM-3** #79 — exported store conformance suite
- [x] **TM-4** #80 — `GatewayConfig` source seam + always-on registry↔chain cross-check
- [x] **TM-5** #81 — backend-selectable boot (memory | postgres)
- [x] **TM-6** torii#24 — tenant-scoped orchestrator schema + RLS + per-tenant config versions (torii PR #27, `a5f6b4d`)
- [x] **TM-7** torii#25 — store traits over that schema (passes TM-3) (torii PR #28, `e9dbe59`)
- [x] **TM-8** torii#26 — move the CLI/worker into torii (torii PR #30, `73a5a0e`; v0.9.0 released)
- [x] **TM-9** #82 — delete `crates/torii`, Postgres adapters, `database/`; release (v0.10.0, PR #84)

## Next

Epic #76 is done: torii owns persistence; torii pins v0.10.0 (torii#31). One verification left:
the DB-gated torii suites against the PUBLISHED v0.10.0 tag (Docker was down — a privileged
helper prompt) — CI-shape without a DB passed 427/0; pre-tag with a DB 486/0. Follow-ups:
torii API should refuse UUID-shaped org slugs at write time; LOW: EffectIntent fixture seq=0.

## Open questions

None. (Decided 2026-10-06: cut v0.9.0; shared torii crate for CLI + API.)
Torii's main checkout has others' uncommitted changes — always work in a torii worktree.

## Known-broken / carry-forwards

- `orchestrator` sandbox straggler test's 5s bound trips under full-suite load (passes alone).
- torii `database/tests/authz.sql` declassify case fails on unmodified develop (pre-existing);
  `run.sh` stops there, so later suites only run individually.
- Out of the epic's scope: SP-REG-2 discovery tools, a results command, SP-REG-4 content.
- From SP-DEC-2: Cloudflare never run live; engine counts caller-caused errors against the breaker;
  LOW test gaps. Pre-existing: `OllamaAdapter`/`OpenAIAdapter::from_config` no default timeout.
