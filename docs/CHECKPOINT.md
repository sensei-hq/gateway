# Checkpoint

**`main` = `15688a5`. `develop` = `570dc7b`. PR #66 OPEN (SP-REG-3, 5 commits, awaiting review).**
Merged: #59–#65. Issue #56 CLOSED. SP-REG-1 is a programme — spec
`docs/superpowers/specs/2026-09-15-sp-reg-programme-design.md`.

## Done

**#63 SP-REG-0** — `PlannerRef::Select` was dead in every shipped binary: `expand.rs` refuses
twice and `boot::heavy` wired eight builders but not the selector. **#64** — torii's first README
(the crate had zero `.md`) plus the four orchestrator crates in the root README. **#65 SP-REG-5** —
`config push --gateway-config` refuses a registry whose chain ids the gateway catalog does not
define, across all three reference surfaces.

**PR #66 SP-REG-3** — a `default_planner` marker; `planner_candidates` orders it first, read from
the PINNED registry, not a boot snapshot (an earlier design was rejected for exactly that). Built
by a 9-agent workflow; 6 findings, all verified, none refuted; 3 MEDIUM fixed, **3 LOW open**.

**Docs** — `crates/torii/docs/features/agentic-execution-surface.md`: the seiki (administer) and
torii (operate) surface. 12 screens with goals + props, the goal→planner→plan→execute journey, the
invariants a naive UI gets wrong, §7's honest gaps. `docs/mockups/` is the MARKETING site and
covers none of it.

## Next

**The capability survey** — what an embedder can do today vs what production agentic execution
needs. NOT yet run; it is the real answer to "what will it take to build on top".

Then by value: **wire the five discovery tools** (a planner currently plans blind; spec §3 —
compose in `pinned` ONLY, never the no-handle path) · **default registry content** (product call;
without it a fresh install cannot plan) · **`config pull`** (small — `load_versioned()` exists and
`push` already calls it).

## Verified

`cargo test --workspace --locked` **1831 / 0** real exit 0 with a live Postgres · clippy **0.1.98**
`-D warnings` 0 · fmt 0 · rustdoc `-D warnings` 0 · `cargo audit` 0.
Homebrew `rustc` 1.97 SHADOWS rustup's 1.98 — use `~/.rustup/toolchains/*/bin`. Postgres on 5432,
schema applied. **Ripgrep counts here have been wrong 4/4** — see the memory notes.
