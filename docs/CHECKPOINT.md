# Checkpoint

**No slice in flight. `main` = `52c8a02`. SP-REG-0 + torii docs shipped; SP-REG-1 is a programme.**
Merged 2026-09-14/15: #59, #60, #61, #62, #63, #64. Issue #56 CLOSED.

## Done

**#59** SP-7a.1 multimodal window + its whole-slice review (11 findings). **#60** issue #56 —
`Cargo.lock` (560 crates auditable vs 47), `h2` → 0.4.19, `undici` → 7.29.1 (HIGH TLS bypass),
`lint`/`audit`/`site` CI jobs. **#61** `rustls` → 0.23.45. **#62** 32 intra-doc links + a rustdoc
gate. **#64** torii's first README (the crate had ZERO `.md`) + the four orchestrator crates in the
root README; every claim verified against the built binary.

**#63 SP-REG-0 — `PlannerRef::Select` was dead in every shipped `torii`.** `expand.rs` refuses
twice and `boot::heavy` wired eight builders but not the selector; all `with_planner_selector`
calls were in tests, so a whole SP-3 slice-4B feature could not run while the suite stayed green.
Now wires `RulePlannerSelector::new(None)`. Two `#[cfg(test-support)]` seams were needed because
the defect is unobservable without a model backend CI lacks — which is how it shipped.

## SP-REG-1 — split, NOT done

Four rounds produced **10 → 12 → 12** findings (not converging); round 4 falsified two §2
foundation claims, one citing a `#[cfg(test)]` fixture as production capability — the trap that
document's own earlier correction disqualifies. It had accreted into ~8 workstreams.

**Remaining (table in §7.1 of `docs/analysis/2026-09-14-sp-reg-1-registry-content.md`):** content
list · `config init` · embedded defaults · gateway chain alignment · tool specs + discovery tools
(**§7.2 — NOT boot-wiring: a boot snapshot goes stale per-run and is a replay hazard**) ·
default-planner designation · distribution.

Re-scope from §7.1, not §4's framing; §2 is verified only as of round 4, and the daemon was down so
those absolutes are ripgrep-derived — re-check with the code graph.

## Verified

`cargo test --workspace --locked` **1817 / 0** with a live Postgres (1767 without) · clippy
**0.1.98** `-D warnings` 0 · fmt 0 · rustdoc `-D warnings` 0 · `cargo audit` 0 · both PG suites 0.
Local `rustc` is Homebrew 1.97 and SHADOWS rustup's 1.98 — verify via `~/.rustup/toolchains/*/bin`.
Postgres is live on 5432 with `database/_apply_all.sql` applied.
