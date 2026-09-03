# Checkpoint

**SP-6 s4 (the human loop gate) is COMPLETE and ON `main`** — PR #50 merged 2026-09-03 as
`a9e10a2` (59 commits, 29 files, +14163/−509). `develop` and `main` are identical (0/0).
No slice is in flight; the next one is unchosen.

## Shipped — `GateSpec::Human { agent, menu }`, the FOURTH waiting kind

A `Loop` whose stop decision a person makes, from a graph-level menu, once per iteration, at the
reserved `"{loop}/{i}/__gate__"` path. Three new journal variants (`LoopGateAwaited` FIRST-wins,
`LoopGateDecided` LAST-wins, `LoopGateSettled` FIRST-wins) ⇒ `FORMAT_VERSION` stays 1.
`validate_dag` rejects a menu that can never converge and bans a bare
`__plan__`/`__gate__`/`__select__` node id at every depth. Operator surface: `torii run gate
decide --node "{loop}/{i}/__gate__" --option <name>`; `run signal`/`run agent answer` refuse it.

## Verified at the gate — real exit codes

DB-free workspace **1658/0/56 exit 0**; clippy `-D warnings` 0; fmt 0; `cargo doc` **16**
unresolved links on a forced full re-document (the baseline). Against a throwaway `postgres:16`:
workspace **1707/0/7** (the 7 are live-provider, no DB test) · orchestrator `postgres-tests`
**406/0/0** · store **69/0/0** · torii `e2e_pg` **8/0/0** — **0 ignored in every DB suite**.
**CI re-ran the PG suites on the PR**: orchestrator 410/0, store 69/0.

**The Critical the slice found in its own work.** The design was wrong by omission — §5.2 gave
the order of operations for a *fresh* drive and never said what a *replay* does. `run_loop`
re-enters from iteration 0 every wake, so expiry was re-evaluated against long-decided gates: an
operator answering each question inside its own SLA still got the run killed, and an
already-converged loop was killed retroactively a day later. Fixed by a settlement fence (a
settled gate replays; it does not re-expire). Also closed: a secret reaching two durable fields
while scrubbed in a third on the same drive, and a **pre-existing** hole where
`plan::check_agent_refs` never resolved a `Loop` gate's `AgentRef`.

## Remaining

Nothing open on s4. Carry-forwards in spec §9: free-text reasoning alongside the pick; asking
once for the whole loop; authorization / N-of-M / non-CLI delivery; a hook for the new events.

**Two process lessons.** (1) A session limit can kill a workflow's reviewers *after* its
implementation lands — `6a20fea` reached `develop` unreviewed, and the re-launched review is what
found the Critical. Review must not be the last phase. (2) The recurring defect class here is a
doc comment asserting a property is guarded by a test that does not exist; mutation-test every
"guarded by X" claim.

## Next command

Pick the next slice: **SP-7 prompt budgeting** (needs its own design pass) or the **SP-DATA-5
carry-forwards** (smaller; spec §8 — the dormant `Snapshot`-spend landmine is the sharpest).

## Known-broken

Nothing. `$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it. The **sensei
daemon is not running**, so this file is the only durable record; `sensei start` restores it.
