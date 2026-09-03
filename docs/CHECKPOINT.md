# Checkpoint

**Slice: SP-6 s4 — the human loop gate. Tasks 1–14 DONE; the whole-slice review's 24 findings
are CLOSED.** Spec (`1633a96`), plan (`f7641c1`). Everything through SP-6 s3 is on `main`
(PR #49, `78c5138`); `develop` is ahead by this slice and is **unpushed — the coordinator
pushes**. `$DATABASE_URL` is REMOTE Supabase: never run a suite against it.

## Done — the review round (6 commits on top of `5cd1ffe`)

- **`e3bec3e` — the reserved gate SEGMENT.** s1's `/` ban reserves the SEPARATOR, so a bare
  `__gate__` inside a `Loop`'s `Subgraph` body namespaced onto the gate's own path. Where the
  colliding kind COMPLETES, the gate asked at an id carrying `NodeCompleted`, torii folded it
  terminal, and the run paused on a question `list-paused` omitted and `gate decide` refused.
  `plan::feasible` never recursed either, though four places said it did. Fixed in
  `validate_dag` block 1c + a recursive `check_reserved_ids` + an arm-level refusal to ask at an
  occupied path (the mid-run `GateSpec::Agent`→`Human` edit no validator can see).
- **`1d753c5` — five properties nothing could redden**, each mutation-proved: the settlement
  fence from BOTH sides (decision re-read; `LoopGateSettled` LAST-wins), `fail_loop_gate`'s
  redaction chokepoint, `gate_ask`'s stop/continue annotation (pinned only by a
  `DATABASE_URL`-gated e2e that skips), and the INDEFINITE SLA — which no executor test drove.
- **`aa5245a` — `fail_loop` kept the first cause forever** (presence-keyed). Now keyed on the
  message via a new `Fold::failure_messages` SET: equality against FIRST-wins `Fold::failed` was
  my own first fix and appends on every wake — measured, not reasoned about. Plus
  `not_delivered`'s Completed clause per kind, `run_loop`'s doc naming `LoopGateSettled`, and
  `awaiting_nodes`' menu-scrub claim corrected (the executor owes it; no redactor ⇒ as stored).
- **`7360d23` / `8df498c` / `4bf30d6` — docs.** Four shipped surfaces still called s4 unbuilt;
  `durable-journal.md` has its s4 section (all THREE variants); §5.2 numbers the SLA read §5.5
  argues about; §2's additivity now points at §5.8's four shared-path changes.

## Remaining

**None for this slice.** Next: the coordinator pushes `develop`, then develop→main.

**Green (verified, real exit codes).** DB-free `cargo test --workspace` = **1658 passed, 0
failed, 56 ignored**; clippy `-D warnings` + `fmt --check` exit 0. Against a throwaway
`postgres:16` on 55432 (removed afterwards): workspace **1707/0/7**, `orchestrator --features
postgres-tests` **406/0/0**, `store --features postgres` **69/0/0**, `e2e_pg` **8/0/0** —
including `a_loop_gate_decided_in_another_process_resumes_and_converges`. **Broken:** none.
