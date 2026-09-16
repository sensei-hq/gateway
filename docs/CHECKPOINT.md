# Checkpoint

**`main` = `e6658d6`. All of SP-REG-0/3/5 and the torii docs are merged.**
Merged: #59–#66. Issue #56 CLOSED. SP-REG-1 is a programme — spec
`docs/superpowers/specs/2026-09-15-sp-reg-programme-design.md`.

## Done

**#63 SP-REG-0** — `PlannerRef::Select` was dead in every shipped binary: `expand.rs` refuses
twice and `boot::heavy` wired eight builders but not the selector. **#64** — torii's first README
plus the four orchestrator crates in the root README. **#65 SP-REG-5** — `config push
--gateway-config` refuses a registry whose chain ids the gateway catalog does not define.
**#66 SP-REG-3** — a `default_planner` marker; `planner_candidates` orders it first, read from the
PINNED registry, not a boot snapshot. Built by a 9-agent workflow; 6 findings, all verified, none
refuted; 3 MEDIUM fixed.

**Docs** — `crates/torii/docs/features/agentic-execution-surface.md`: the seiki/torii surface —
12 screens with goals + props, the goal→planner→plan→execute journey, the invariants a naive UI
gets wrong, §7's honest gaps. `docs/mockups/` is the MARKETING site, and covers none of it.

## Open — 3 LOW from SP-REG-3, on main unfixed

1. `validate`'s comment claims `with_agent` coverage; `from_config` is its only non-test caller.
2. The name tie-break in `planner_candidates` is only probabilistically guarded — dropping it
   escaped its two ordering tests 6 runs in 24.
3. The `default_planner`-outside-`planning` check returns on the first offender in `HashMap`
   order, so 2+ offenders name an arbitrary one, varying per load.

## Next

**The capability survey** — what an embedder can do today vs what production agentic execution
needs; NOT yet run. Then: **wire the five discovery tools** (a planner plans blind; spec §3 —
compose in `pinned` ONLY) · **default content** (product call) · **`config pull`** (small).

## Verified

`cargo test --workspace --locked` **1831 / 0** real exit 0 with a live Postgres · clippy **0.1.98**
`-D warnings` 0 · fmt 0 · rustdoc 0 · `cargo audit` 0. Homebrew `rustc` 1.97 SHADOWS rustup's 1.98
— use `~/.rustup/toolchains/*/bin`. Postgres on 5432. **Ripgrep counts here: wrong 4/4.**
