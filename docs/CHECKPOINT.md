# Checkpoint

**Slice:** SP-6 s3 `human-as-Agent` — the LAST slice of SP-6. **In progress on
`feat/sp-6-s3-human-as-agent`** (branched from `develop` at `771af25`). Task 1 of 7 done.

## Done / remaining

- **SP-6 s2 ✅ MERGED TO `main`** via PR #47 (`5d9b9d1`). s1 · SP-DATA 1–5 · SP-3 · SP-4 also on main.
- **s3 spec ✅ approved** after a depth review that found 5 blockers — four of them places the spec
  asserted something false about the codebase. `specs/2026-08-27-sp-6-s3-human-as-agent-design.md`.
- **s3 plan ✅** — 7 tasks, 42 steps, written against verified signatures.
  `plans/2026-08-27-sp-6-s3-human-as-agent.md`.
- **Task 1 ✅** (`fa070dd`) — `AgentBacking::{Model,Human{timeout}}` + the four `validate()` rules.
  **1510 passed / 0 failed / 7 ignored**, exit 0. Used `RegistryLoad` (no `InvalidConfig` variant
  exists). 27 literal sites across 7 files needed the new field. **Not yet reviewed** — the
  two-stage spec+quality review for Task 1 was not run.
- **Tasks 2–7 ⬜** — journal events · fold · `run_human_agent` · CLI · `list-paused` · e2e.

## Next command

Run Task 1's two-stage review first (it was skipped), then Task 2:

```
# review Task 1 (fa070dd), then:
sed -n '/^## Task 2/,/^## Task 3/p' docs/superpowers/plans/2026-08-27-sp-6-s3-human-as-agent.md
```

## Open questions

- **Task 4 is the delicate one** — 8 ACs, and the slice's one deliberate divergence: the answer is
  read BEFORE expiry, unlike `HumanGate`. Its mutation list targets that ordering specifically.
- `drive_agent` takes `&NodeId` but `gate_precheck`/`wait_or_expire` take `&Node` — Task 4 adds thin
  `_by_id` variants with the existing ones delegating. Do not duplicate either body.
- The non-top-level rejection (§5.5) is a **runtime** check, not load-time: `validate_dag` cannot see
  the registry. Stated limitation, not an oversight.

## Known-broken

None. **1510 passed / 0 failed / 7 ignored, exit 0**; clippy `-D warnings` and fmt clean.
`$DATABASE_URL` is a REMOTE Supabase instance — never run the DB suite against it; use a throwaway
container (`env -u DATABASE_URL` otherwise).
