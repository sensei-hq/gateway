# Checkpoint

**v0.8.0 RELEASED and MERGED to main** (tag `0d59086`, PR #75 → `0982046`, GitHub Release
published, `develop` == `main`). SP-DEC-2 — more System One routes: Cloudflare Workers AI
(`clef`, `clef-flash`) + self-hosted llama.cpp / SGLang. Plan:
`docs/superpowers/plans/2026-10-03-sp-dec-2-system-one-routes.md` (Contract, Progress, carry-forwards).

## Done

- v0.8.0 = SP-DEC-2 T1–T6 + review fixes (MEDIUM: Workers AI model → URL-path injection, now
  validated). Gate: 1980 passed / 0 failed, clippy 0.1.98 + 0.1.99, PR #75 CI 22/22 green incl.
  CodeQL.
- Shipped artifact verified: a fresh consumer on `tag = "v0.8.0"` → facade → `llamacpp` → live
  llama-server b11381 + Nimble v3 answered (p(refund)=0.99). Server stopped afterwards.
- Earlier this run: v0.6.1 (#73), v0.7.0 (#72, SP-DEC-1), both on main with Release pages.

Live rig (outside the repo, for the ignored tests): `~/opt/llamacpp-systemone/bin-b11381/llama-b11381/llama-server
-m ~/opt/llamacpp-systemone/models/Bespoke-Nimble-9B-v3-Q4_K_M.gguf --alias nimble-v3 --port 8091 -c 8192`;
`LLAMACPP_URL=http://localhost:8091 cargo test … -- --ignored`.

## Next

Nothing in flight. Candidates: the carry-forwards below; sensei re-pin to v0.8.0 is the other
session's call.

## Open questions

None.

## Known-broken / carry-forwards

- Never run live: Cloudflare (needs `CLOUDFLARE_ACCOUNT_ID` + `CLOUDFLARE_API_TOKEN`).
- Engine policy: any caller-caused error (a client-side image error or a provider 4xx) trips the
  circuit breaker and is `retryable` — fix at the engine level.
- LOW test gaps: Cloudflare error reasons, extraction order, Cloudflare image limits (GIF / >4
  images go out and fall back on Cloudflare's 400).
- Pre-existing: `OllamaAdapter::from_config` / `OpenAIAdapter::from_config` no default timeout;
  `providers.md` `together` base URL nit.
