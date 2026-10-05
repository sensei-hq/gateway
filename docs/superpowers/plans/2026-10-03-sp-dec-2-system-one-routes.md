# SP-DEC-2 — more System One routes: Cloudflare Workers AI + self-hosted servers

Follows SP-DEC-1 (gh#72, v0.7.0), whose plan named Cloudflare a carry-forward (D4).
Scope chosen by the user 2026-10-03: **Cloudflare Workers AI** (`clef`, `clef-flash`)
and **self-hosted servers** (llama.cpp, SGLang; vLLM documented only). Not in scope:
Liquid AI, logprob emulation on chat models.

## Contract (verified 2026-10-03 — research workflow `wf_44d06905-6ac`, every load-bearing claim re-checked by an independent skeptic)

**Cloudflare Workers AI** — only `POST https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/@cf/cloudflare/{clef|clef-flash}`
(no `/v1/systemone`; the `/ai/v1` OpenAI-compat base covers chat/embeddings/responses only). Bearer API token.
Body `model` must be the SHORT name `clef`/`clef-flash` (schema `^\s*(clef|clef-flash)\s*$`); the `@cf/…` id goes in the path.
Body defines only `model, state, questions, images`; `images` ≤ 4, each a data URL (`^data:`) or `{content_type, base64}` with
`image/png|jpeg|webp` — a bare base64 string matches neither. Choice 2–255, score 2–10 levels, questions 1–64, instructions
required for every type. Long state is truncated **silently**. REST responses are wrapped `{result:{model,answers,usage},
success, errors, messages}`; errors use the v4 envelope `{result:null, success:false, errors:[{code,message}]}` (live:
401 code 10000, 404 code 7003 for a malformed account id). 65,536-token window; input-only pricing.

**llama.cpp server** (`b11361`+; nimble `b11364`+; clef `b11371`+; Homebrew is too old) — `POST /v1/systemone`, always
registered; **501** `not a decision model` unless the GGUF carries `<arch>.decision.type` + a `systemone` template (the
Ollama `nimble` blob does not — use `ggml-org/Bespoke-Nimble-9B-v3-GGUF`). Images must be **data URLs** (bare base64 → 400),
≤ 8. Errors `{"error":{"code","message","type"}}`, status = code (400 / 501 / 500 / 401). `model`/`keep_alive` ignored in
single-model mode; response `model` = the server alias; `usage.output_tokens` always 0.

**SGLang** — `/v1/systemone` in release v0.5.21 (#41208); images + decision checkpoints only on main (#42183) — **v0.5.21
silently drops `images`** (200, text-only answers). Accepts bare base64 or data URLs. Errors: 400/500 flat
`{object:"error", message, type, param, code}`, 422 FastAPI `{detail:[…]}` (list). A model name containing `:` is parsed as a
LoRA adapter → 400. Optional `--api-key` bearer.

**vLLM** — no upstream `/v1/systemone` (PR #59299 open; only an example proxy). Documented as unsupported.

## Decisions

- **D1 Image encoding is per host.** Ollama keeps bare base64 (it rejects data URLs). Every other System One host that
  takes images accepts a data URL (Cloudflare, llama.cpp, SGLang, vLLM example), and OpenRouter/TypeSafe take none — so
  the generic `SystemOneAdapter` and Cloudflare send `data:<mime>;base64,…`. The mime is sniffed from the decoded magic
  bytes (PNG/JPEG/WebP/GIF); an unrecognised image is a `ProviderError` before any request (falls back to a host that may
  take it), never a guessed type.
- **D2 Cloudflare is a wire dialect of the same core, not a copy.** `systemone::decide` gains a `Dialect`
  (`SystemOne` | `WorkersAi`) that varies exactly: URL (`{base}/v1/systemone` vs `{base}/run/@cf/cloudflare/{short}`),
  body `model` (short name), `keep_alive` (dropped), response (unwrap `result`, `success:false` → error). Transport, auth,
  timeout, header forwarding, error mapping and the missing-answer check stay shared.
- **D3 No breaking change.** The account id rides in `url` (`https://api.cloudflare.com/client/v4/accounts/<id>/ai`);
  `RouterConfig`/`DecisionRequest` are untouched. New public surface is additive: `cloud_providers::cloudflare::CloudflareAdapter`.
- **D4 Error extraction learns three more shapes** in the shared `base::extract_error_message`: the v4 `errors[]` array
  (Cloudflare), a flat top-level `message` (SGLang/OpenAI-legacy), and a FastAPI `detail` list (SGLang 422). Additive: a
  body that matched before yields the same message.
- **D5 Silent truncation is a routing problem, not an adapter one.** Seed Cloudflare models with their real 65,536 window
  so the existing `ContextWindowGate` (state + every question + framing + images) never sends a request Cloudflare would
  truncate. Documented at the point of use.
- **D6 Facade ids.** `cloudflare` → `CloudflareAdapter`; `llamacpp` and `sglang` → `SystemOneAdapter` (decision-only; a
  caller's own chat adapter under the same id is untouched — separate registry map). No `vllm` id: there is no upstream route.

## Tasks

| # | Task |
|---|---|
| T1 | `extract_error_message`: Cloudflare `errors[]`, flat `message`, FastAPI `detail` list |
| T2 | image encoding: mime sniff + data URL for the generic adapter; Ollama stays bare |
| T3 | `Dialect::WorkersAi` in the core + `CloudflareAdapter`; wiremock against the verified envelope; ignored live test (`CLOUDFLARE_ACCOUNT_ID`/`CLOUDFLARE_API_TOKEN`) |
| T4 | self-hosted: llama.cpp + SGLang error bodies through the generic adapter (501, 400, 422 list); live test against a real `llama-server` ≥ b11371 + Nimble v3 |
| T5 | facade `cloudflare`/`llamacpp`/`sglang`; `docs_sync_capabilities` row names; docs (providers, capabilities matrices, recipes, upgrading) |
| T6 | whole-slice adversarial review (workflow), gate |

## Progress

| # | Commit | Notes |
|---|---|---|
| T1 | `6825012` red → `4924875` | error extraction: Cloudflare `errors[]`, flat `message`, FastAPI `detail` list; 3/3 mutations |
| T2 | red → `0647e80` | per-host image encoding; mime sniff; 6/6 mutations (GIF87a pinned) |
| T3 | `8907f30` red → `9c6c6d3`, `ea53944` | `Dialect::WorkersAi` + `CloudflareAdapter`; 6/6 mutations (success flag pinned). `9c6c6d3` landed with the docs_sync guard red (a `;`-chained commit) — fixed next commit |
| T4 | `a3435d5` | llama.cpp/SGLang error bodies; LIVE vs llama-server b11381 + Nimble v3 — and with bare images the real server returns its 400 |
| T5 | `c25adfc` red → `71e8f90`, `2d2eaf2`, `92d85af` | facade ids; live engine e2e (facade → decide chain → llama.cpp, 1 attempt); docs (recipes compile) |
| T6 | review `wf_77336922-196` | 5 reviewers + 3-skeptic majority: 8 survive, 1 refuted. Fixed red-first: **#1 MEDIUM** Workers AI model spliced into the URL path → validated (`7bca090`); **#2 MEDIUM** facade test now proves which adapter (`437a896`); #4 LOW wrapped base64 in data URLs (`a2d80c6`); #5 LOW upgrading accuracy (`ba4d7a9`). Gate: 1980 passed, clippy 0.1.98 + 0.1.99, live adapter + engine |

Carry-forwards (LOW, reported not fixed): #3 test pins Cloudflare's error reasons; #7 test pins extraction order; #8 Cloudflare image limits
(GIF / >4 images are sent and fall back on Cloudflare's 400). #6 — a client-side image error trips the breaker and is `retryable` —
is the engine's existing policy for ANY caller-caused 4xx; fix belongs at the engine (exclude caller-fault errors from breaker votes
and HardFailure), not here. Pre-existing doc nit: `providers.md` `together` base shows `/v1`, code is `https://api.together.xyz`.
