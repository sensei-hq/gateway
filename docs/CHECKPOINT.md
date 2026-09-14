# Checkpoint

**No slice in flight. No open tasks. `main` = `cece751`, CI green 5/5.**
Merged 2026-09-14: #59, #60, #61, #62. Issue #56 CLOSED.

## Done

**#59 — SP-7a.1 multimodal window correctness + whole-slice review.** The estimator ignored
`Message::attachments`, so every `ContextWindowGate` decision on a multimodal request was made
on TEXT alone; each now costs 4784 tokens after the `/3` divide. LATENT — all 18
`with_attachment` sites are tests. Five reviewers, 11 findings, all fixed and hand-verified.

**#60 — issue #56, and #61 behind it.** `Cargo.lock` committed (560 crates auditable vs 47);
`h2` → 0.4.19; `rsa` documented as uncompiled in `.cargo/audit.toml`; `undici` → 7.29.1 (HIGH
TLS bypass) plus dompurify/devalue/vitest; new `lint`, `cargo audit`, `site` CI jobs. #61 then
took `rustls` → 0.23.45 (RUSTSEC-2026-0285), published hours after #60's audit ran green.

**#62 — 32 broken intra-doc links + the missing gate.** Rustdoc warnings are invisible to
build/test/clippy/fmt, which is how SP-7a.1 rebound a public API's 194-line doc onto a private
constant and shipped. `RUSTDOCFLAGS="-D warnings" cargo doc` now runs in `lint`,
mutation-verified to exit 101 on one reintroduced link. Also dropped `stash@{0}` (`c76a3fe`),
the `loop_gate_settled_with` ordering mutation a named test already guards.

## Verified

`cargo test --workspace --locked` 1767 / 0 real exit 0 · clippy **0.1.98** `-D warnings` 0 · fmt
0 · rustdoc `-D warnings` 0 · `cargo audit` 0 · site 657 files, 46 tests. Local `rustc` is
Homebrew 1.97 and SHADOWS rustup's 1.98 — verify via `~/.rustup/toolchains/*/bin`.

## Next

Nothing committed. Candidates: **SP-7c semantic activation** (id released by #59, unspecced —
starts at `/sensei:design`) · tier-aware ceilings, removing the 8192 over-refusal (SP-7a.1 §8).

## Open

`cargo audit` reads a run-time database, so it can redden an unchanged `main` — it did (#61); if
disruptive, make it a scheduled workflow that files an issue. Dependabot does NOT parse
`site/bun.lock`; the `site` job watches it. `cookie` un-overridden. **Daemon NOT running.**
