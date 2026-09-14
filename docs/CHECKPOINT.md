# Checkpoint

**No slice in flight. `main` = `b903a32`, CI green 5/5. `develop` == `main`.**
Merged 2026-09-14: #59, #60, #61.

## Done

**#59 — SP-7a.1 multimodal window correctness + whole-slice review.** The estimator ignored
`Message::attachments`, so every `ContextWindowGate` decision on a multimodal request was made
on TEXT alone; each now costs 4784 tokens after the `/3` divide. LATENT — all 18
`with_attachment` sites are tests. Five reviewers, 11 findings, all fixed and hand-verified:
the constant was inserted mid-doc-block, silently rebinding the estimator's 194-line doc to
itself (public page 4258 → 22253 bytes); 4784 was asserted against itself, so halving it left
322 tests green; "ten images fit every window" was false — the shipped preset is 8192, so the
SECOND image overruns. Renumbered SP-7c → SP-7a.1 (that id was already taken).

**#60 — issue #56, CLOSED.** `Cargo.lock` committed (560 crates auditable vs 47); `h2` →
0.4.19; `rsa` documented as uncompiled in `.cargo/audit.toml`; `undici` → 7.29.1 (HIGH TLS
bypass) plus dompurify/devalue/vitest; new `lint`, `cargo audit`, `site` CI jobs.

**#61 — `rustls` → 0.23.45** (RUSTSEC-2026-0285), published hours after #60's audit ran green.

## Verified

`cargo test --workspace --locked` 1767 / 0 real exit 0 · clippy **0.1.98** `-D warnings` 0 · fmt
0 · `cargo audit` 0 · site 657 files 0 errors, 46 tests. Local `rustc` is Homebrew 1.97 and
SHADOWS rustup's 1.98 — verify via `~/.rustup/toolchains/*/bin` or CI will disagree.

## Next

None committed. Candidates: **SP-7c semantic activation** (id free, unspecced) · tier-aware
ceilings, removing the 8192 over-refusal (SP-7a.1 §8) · 11 orchestrator rustdoc link warnings.

## Open

`cargo audit` reads a run-time database, so it can redden an unchanged `main` — it did, within
hours. Dependabot does NOT parse `site/bun.lock`; the `site` job watches it. `cookie`
deliberately un-overridden (bun ignores nested overrides; kit and youch need incompatible
majors). **Sensei daemon NOT running; this file is the record.**
