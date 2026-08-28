# Checkpoint

**Slice:** SP-6 s3 `human-as-Agent` — the LAST slice of SP-6. **In progress on
`feat/sp-6-s3-human-as-agent`** (branched from `develop` at `771af25`). Task 2 of 7 done.

## Done / remaining

- **SP-6 s2 ✅ MERGED TO `main`** via PR #47 (`5d9b9d1`). s1 · SP-DATA 1–5 · SP-3 · SP-4 also on main.
- **s3 spec ✅ approved** after a depth review that found 5 blockers — four of them places the spec
  asserted something false about the codebase. `specs/2026-08-27-sp-6-s3-human-as-agent-design.md`.
- **s3 plan ✅** — 7 tasks, 42 steps, written against verified signatures.
  `plans/2026-08-27-sp-6-s3-human-as-agent.md`.
- **Task 1 ✅** (`fa070dd`) — `AgentBacking::{Model,Human{timeout}}` + the four `validate()` rules.
  Used `RegistryLoad` (no `InvalidConfig` variant exists). 27 literal sites across 7 files needed
  the new field.
- **Task 1 review ✅ REVIEWED + all 7 findings fixed** (`c2fef43` · `4f25b8b` · `c0ae114` ·
  `d33465d`). The HIGH: the no-tools rule was untested — `UnknownToolRef` fired first and its
  message satisfied both assertions, so the rule deleted still passed. The MEDIUM that changed
  behaviour: `from_frontmatter` hardcoded `Model`, so a human agent could not be AUTHORED and
  `config push` silently downgraded a `Human` row; there is now a `backed_by:` / `timeout: 48h`
  frontmatter surface (both loud on typos), recorded in spec §4. Also: reject grants on a human
  agent, `validate`'s doc rewritten, one match not two.
  **1521 passed / 0 failed / 7 ignored**, exit 0.
- **Task 2 ✅** (`864cb1f`) — `JournalEvent::{AgentAwaited,AgentAnswered}` + `MAX_HUMAN_TEXT_BYTES`
  (in `journal.rs` beside `FORMAT_VERSION`, re-exported from `lib.rs`). New VARIANTS, so
  `FORMAT_VERSION` stays 1. Red first: `no variant named AgentAwaited` (E0599 x4). The one
  non-exhaustive `match` the plan predicted was the only one: `label()` in `executor/tests.rs`, given
  two explicit arms, never a wildcard. Two plan doc claims were corrected against the code before
  being written down: `MAX_PAYLOAD_BYTES` is `pub` (only `check_payload_size` is `pub(crate)`) — the
  real reason reuse is impossible is the dependency CYCLE — and the journaled prompt is
  `assemble_prompt`'s SYSTEM string, not "exactly what the model would have received" (the model also
  gets the rendered input as a user message plus tool schemas).
  **1522 passed / 0 failed / 7 ignored**, exit 0.
- **Tasks 3–7 ⬜** — fold · `run_human_agent` · CLI · `list-paused` · e2e.

## Next command

Task 2 is done; start Task 3 (fold the two events):

```
sed -n '/^## Task 3/,/^## Task 4/p' docs/superpowers/plans/2026-08-27-sp-6-s3-human-as-agent.md
```

## Open questions

- **Task 4 is the delicate one** — 8 ACs, and the slice's one deliberate divergence: the answer is
  read BEFORE expiry, unlike `HumanGate`. Its mutation list targets that ordering specifically.
- `drive_agent` takes `&NodeId` but `gate_precheck`/`wait_or_expire` take `&Node` — Task 4 adds thin
  `_by_id` variants with the existing ones delegating. Do not duplicate either body.
- The non-top-level rejection (§5.5) is a **runtime** check, not load-time: `validate_dag` cannot see
  the registry. Stated limitation, not an oversight.

## Known-broken

None. **1522 passed / 0 failed / 7 ignored, exit 0**; clippy `-D warnings` and fmt clean.
`$DATABASE_URL` is a REMOTE Supabase instance — never run the DB suite against it; use a throwaway
container (`env -u DATABASE_URL` otherwise).
