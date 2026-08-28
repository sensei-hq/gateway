# Checkpoint

**Slice:** SP-6 s3 `human-as-Agent` — merged to `develop` at `4026a90`. The four
carried-forward MEDIUM/rustdoc findings from the re-review are now CLOSED.

## Done

- `56862ab` test(torii) — pins the `--as` post-redaction ordering
  (`an_actor_that_only_exceeds_the_cap_after_redaction_is_rejected`).
  **Mutation-verified in a scratch copy**: reversing redact/check turned it RED
  (`Outcome{text:"answered: reviewer…",code:0}`) with the other 27 `cmd::human` tests green.
- `e288143` docs(torii) — the 7 new rustdoc "links to private item" warnings, fixed as code
  spans, not by widening visibility. `sensei-torii` 15 → **8** warnings; workspace
  "links to private item" 31 → **24**; zero residue for the seven names.
- `0c3b033` docs(sp-6 s3) — spec §5.5's swapped `fanout.rs:183,269`, plus two SIBLING
  references verified FALSE while there (`mod.rs:166`, `agent.rs:97`) and one more
  (`registry.rs:450-475`). Sites are now named by FUNCTION. `scheduler.rs:52` checked and
  left: it is correct.
- `d7d0537` docs(features) — `agents-skills-tools.md` + `durable-journal.md` gained their s3
  paragraphs; three `SP-6-2` markers bumped to `SP-6-3`; plus `durable-journal.md`'s stale
  "Planned (SP-1)" header, the README agents row, and "all four → all six" variants.

**Measured:** `cargo test --workspace` **1593 passed / 0 failed / 7 ignored, exit 0**
(1592 + the new guard). `clippy -D warnings` exit 0. `fmt --check` exit 0. `cargo doc` exit 0.

## Remaining

The re-review's **7 LOW** only: stale doc sentences asserting the old single-4096 bound;
`truncate_prompt_to_bound` overruns for a tiny `max`; `assemble_prompt` has zero production
callers.

## Next command

Pick up the 7 LOW, or open the develop→main PR. `main` is strict — merge `origin/main` into
`develop` first or the PR sits BEHIND.

## Known-broken

Nothing. `$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it; use a
throwaway container. The Postgres e2e was last re-run at `4026a90` (7 passed, zero skips);
these four commits touch no PG path.
