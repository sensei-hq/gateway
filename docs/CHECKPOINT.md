# Checkpoint

**Slice: SP-DATA-5 follow-on — the budget clamp.** On `develop`, unpushed. Tasks 1–8 done; Task 9
is down to Steps 1–2 and 5–6 (its doc steps 3–4 landed in review round 2).

## Done

Tasks 1–4 at `c301901`; round-1 raised 20 findings and Tasks 5, 6 and most of 8 landed with the
fixes. Then Task 7 (`cd992df`), Task 8 (`f4f97b0`), the ledger (`bc902c4`).

**Review round 2 — 18 findings, all fixed.** One real bug: `BelowFloor`'s recommended raise was
derived from the SATURATED allowance, so on the `est ≥ remaining` branch it understated the answer
("at least 257" where 271 was needed) and each `BudgetRaised` + `force_wake` bought only another
256 tokens. Now `spent + est_input + floor`, with the wrap test re-driving at the cap it names.
Three UNTESTED properties got red-first guards: the AGENT determinism call site (a budgeted agent
paused mid-loop, raised, replaying turn 0 — folding the remaining budget in there was green across
the whole suite), AC10's clamp-bit signal keyed on `emitted` not `allowance`, and the
`--budget-tokens` whitespace trim the floor rescale deleted. `max_output_tokens: 0` is now a
config-validation error (`ceiling = Some(0)` ⇒ `max_tokens: Some(0)`), and "neither signal fires"
gained a positive control so a dead thread-local capture cannot pass it. The rest were false doc
claims: the five-family adapter survey was wrong for gemini and the local engine in four places,
three stale mutation counts are now properties, and the pre-clamp contract still read as current
in the overview and the older SP-DATA-5 spec.

## Verified at the gate — real exit codes

`cargo test --workspace` **1682 passed / 0 failed / 56 ignored, exit 0** · `clippy --workspace
--all-targets -- -D warnings` exit 0 · `fmt --all --check` exit 0 · `cargo doc` unresolved links
**16**, the baseline. Every round-2 mutation is tabulated with its failure in the plan's ledger.

## Remaining, the next command, and known-broken

Task 9 Steps 5–6, re-running Steps 1–2 at the tip; `develop` → `main` is still the batched PR. Run
`cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`. Broken:
nothing. `$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it. The Postgres AC6
budget e2e is `ignore`d without `have_database_url`, so a local `--workspace` run misses it, and
the **sensei daemon is not running**, so this file is the only durable record.
