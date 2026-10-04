# Checkpoint

**SP-DEC-2 — more System One routes (Cloudflare Workers AI + self-hosted llama.cpp/SGLang):
T1–T6 DONE on `develop`, review fixed, gate PASS. NOT released.** Plan:
`docs/superpowers/plans/2026-10-03-sp-dec-2-system-one-routes.md` (Contract + Progress — read first).
Previous: v0.7.0 (SP-DEC-1) released + merged to main (`51cf9bc`, PR #74); GitHub Release page
published.

Gate: workspace **1980 passed / 0 failed**, `--features local` ok, fmt ok, clippy **0.1.98 and
0.1.99** (real rustup driver on PATH) ok; live llama.cpp adapter + engine tests ok.

## Done

T1 error extraction (`4924875`) · T2 per-host image encoding (`0647e80`) · T3 `Dialect::WorkersAi` +
`CloudflareAdapter` (`9c6c6d3`) · T4 self-hosted + live llama.cpp (`a3435d5`) · T5 facade ids
(`71e8f90`), live engine e2e (`2d2eaf2`), docs (`92d85af`) · T6 review `wf_77336922-196`: MEDIUM #1
model→URL-path injection fixed (`7bca090`), MEDIUM #2 facade test (`437a896`), LOW #4 (`a2d80c6`),
LOW #5 (`ba4d7a9`).

Live test rig (outside the repo): `~/opt/llamacpp-systemone/bin-b11381/llama-b11381/llama-server -m
~/opt/llamacpp-systemone/models/Bespoke-Nimble-9B-v3-Q4_K_M.gguf --alias nimble-v3 --port 8091 -c 8192`,
then `LLAMACPP_URL=http://localhost:8091 cargo test … -- --ignored`. Server stopped.

## Next

1. Ask the user: release (additive + two runtime behaviour changes — see upgrading.md "0.7.x → next";
   likely v0.8.0) and a develop→main PR.
2. Cloudflare live test needs `CLOUDFLARE_ACCOUNT_ID` + `CLOUDFLARE_API_TOKEN` (never run — no creds).

## Open questions

Release version for SP-DEC-2?

## Known-broken / carry-forwards

- LOW #3/#7/#8 test gaps; #6 engine policy: any caller-caused error (client-side image error or a
  provider 4xx) trips the circuit breaker and is `retryable` — fix at the engine.
- Pre-existing: `OllamaAdapter::from_config` / `OpenAIAdapter::from_config` no default timeout;
  `providers.md` `together` base URL nit; sensei `dbd reconcile` fails on `sensei.libraries`.
