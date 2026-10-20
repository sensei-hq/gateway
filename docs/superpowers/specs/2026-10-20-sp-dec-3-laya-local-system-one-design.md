# SP-DEC-3 — local open-weight System One: Laya-CoreML

Follows SP-DEC-2 (self-hosted `llamacpp` / `sglang` routes). Scope chosen by the user 2026-10-20:
**Laya-CoreML over its HTTP server** — no cloud key, runs on Apple Silicon (CoreML: CPU+GPU or ANE).

Not in scope (carry-forwards):
- **In-process Laya** (load the CoreML bundle from Rust, no server) — a future slice; the measured HTTP cost
  below does not justify it yet.
- **CLM-8B** (Stanford/NVIDIA, `Contrastive-LM/CLM`) — wire-compatible, but its encoder is vLLM + Qwen3-8B
  (CUDA/CPU). Reconsider if it is ported to Metal.
- `POST /v1/systemone/batch`, Laya's `max_len` / `task` / `lang` / `min_confidence` body controls.
- **Truncation is input-determined but classified like any provider fault** (found in T2 review). The same
  over-window request truncates on every retry: with no other candidate it returns `AllAttemptsFailed
  { retryable: true }`; an orchestrator node (ModelCall or Agent) retries the identical request only if the
  executor was built with `with_max_transient_attempts(n > 1)` (default 1 = no retry), and each repeat fails
  the same way. Every truncation counts
  toward the router's circuit breaker (a burst can open it for valid short requests). Mitigation today:
  set `context_window` so the gate refuses first, and chain a second decision router. A typed
  non-retryable, breaker-neutral marker is the follow-up.
- **The image guard keys on the exact router id `laya`** (T1 review): a Laya server under another id gets
  data URLs it silently ignores. A capability-level "no images" router setting is the follow-up.

## Contract (verified 2026-10-20 against `laya-coreml` 0.5.1 from PyPI, live on an M4 Max)

- `laya-coreml-serve` (extra `serve`) — `POST /v1/systemone`, TypeSafe's wire. Port `LAYA_PORT` (default 8000).
  Bearer only if `LAYA_API_KEY` is set (`Authorization: Bearer <key>`); public Hub weights, offline after first
  download. No torch installed; `/health` reports `device: coreml:cpu_gpu`.
- Body: `model` names a checkpoint — `english` (512 tokens), `multilingual` (1024), `typed-decisions` (1024), or
  an operator-registered one (`LAYA_EXTRA_MODELS='{"ane":"aac6fef/laya-multilingual-coreml-ane"}'`, 96 tokens).
  Unknown body keys (`images`, `keep_alive`) are **ignored silently**.
- Response: `{model, answers, usage, routing}`. Answers are `type`-tagged like ours; the default (non-strict)
  payload adds `confidence`/`answer_confidence`/`action` on answers, `x_jev_confidence` on choice/score, a root
  `routing`, and in `usage` the extra keys `state_tokens`, `state_tokens_dropped`, `truncated`, `truncated_questions`.
  The response `model` is `laya-rl-agent`, not the requested checkpoint. `usage.output_tokens` is 0.
- A state over the checkpoint's window is **truncated and answered 200**; the only signal is `usage.truncated`.
- Errors: FastAPI `{"detail": "<string>"}` — 400 (missing state, questions not an object), 413 (too many
  questions/options), 422 (bad controls), 401 (bad key), fixed 500 on inference failure, 503 over
  `LAYA_MAX_CONCURRENT`.

**Measured HTTP cost** (median of 200 warm calls, one noul question, keep-alive): multilingual CPU+GPU 50.1 ms
in-process vs 46.9 ms HTTP (noise); ANE 4.12 ms vs 4.47 ms (+0.35 ms ≈ 8%); bare `/health` round trip 0.35 ms.

## Decisions

- **D1 Facade id `laya`.** The facade auto-registers `laya` as a decision-only `SystemOneAdapter` — one more arm
  beside `llamacpp | sglang` (`facade.rs`). No default `url` and no default model (consistent with the other
  self-hosted ids): the operator sets `url` and pins `api_model_id`. A chat adapter registered under `laya` is left
  untouched (separate registry map).
- **D2 Images are refused before any request.** `ImageEncoding` gains `Unsupported`; `encode_images` returns an
  error for a non-empty `images` under it, which `decide` already maps to `ProviderError` (status `None`) before
  sending — fallback-eligible, the same rule SP-DEC-2 D1 applies to an unrecognisable image. The adapter selects
  the encoding by id: `laya` → `Unsupported`, every other id unchanged (`DataUrl`). The message names the router
  and says it does not read images.
- **D3 A truncated answer is a provider fault, not a success.** `WireUsage` reads an optional `truncated: bool`
  (absent → `false`, so every other host is unaffected). `true` → `ProviderError` (status `Some(200)`, as for the existing missing-answer check) naming the router
  and the dropped token count when present, so the call falls back instead of returning answers about a state the
  model never fully read. Primary defence remains the existing `ContextWindowGate`: the docs tell the operator to
  set the model's `context_window` to the checkpoint's window, so oversize requests are refused before they are sent
  (SP-DEC-2 D5). D3 catches what the gate's character estimate misses (e.g. non-English text tokenising denser).
  Requires Laya's default payload — `LAYA_JEV_STRICT` strips `truncated`; documented.
- **D4 (superseded by D5, 2026-10-20 whole-slice review).** Originally "no breaking change". D5 adds a
  `GatewayError` variant; `GatewayError` is not `#[non_exhaustive]`, so downstream exhaustive matches (torii) break
  → the next release is a minor bump (0.12.0) with a torii re-pin. Two behaviour changes are also user-visible:
  the facade's `laya` id replaces a caller's own decision adapter under that id, and a present-but-non-bool
  `usage.truncated` fails the parse. See `docs/llms/upgrading.md`.
- **D5 `GatewayError::InputRejected { adapter, message }` — "this candidate cannot serve this request's
  input; the endpoint is healthy"** (user decision 2026-10-20, review option 2). Whole-slice review found that
  every adapter error counts toward the circuit breaker and marks exhaustion retryable, so an input that can
  never succeed (an image to `laya`, an over-window state) opens the breaker for valid requests and is retried
  forever by an executor with `max_transient_attempts > 1`. Semantics:
  - **Fallback:** `should_trigger_fallback` is `true` for it whatever the trigger set (including empty) — an
    ineligible candidate is a skip, not a provider-fault policy question. `allow_fallback: false` still stops the
    walk after one attempt.
  - **Not retryable:** `is_retryable` is `false`; it contributes a new `GateContribution::InputRejected`, which
    makes exhaustion `retryable: false` unless another attempt hard-failed, and which is neutral to the
    all-gated decision, like an over-window skip: if every other candidate was gated, the exhaustion is
    `AllGated` on those gates (waiting or a human fixes THEM, and the walk then passes the cheap rejection);
    with no gates it is `AllAttemptsFailed { retryable: false }`.
  - **No health signal:** the engine's one recorder fan-out (`dispatch_outcome`) drops it — the breaker, lockout, cooldown and
    performance recorders never see it. The `Attempt` is still recorded (`status: Failed`, `error`,
    `fallback_triggered: true`).
  - **Producers:** D2 (image to `laya`), SP-DEC-2's unrecognisable-image refusal, D3 (`usage.truncated`), D6.
    Stream code `"input_rejected"`.
- **D6 Laya's per-question budget is enforced before sending** (found live in the whole-slice review). Laya's
  `build_head` silently cuts each question's head (`<type> question: <instructions>` plus its options) to
  `head_max_len` tokens and reports nothing — `truncated` covers only the state. On `english` an 808-character
  instruction lost the question at its end and the answer flipped from 0.90 to 0.15 with `truncated: false`.
  For router id `laya` the adapter estimates each question's head as `ceil(chars / 3)` over
  `"<type> question: <instructions as text>"` plus, per option, `1 + min(48, ceil(chars / 3))` (Laya caps each
  option at 48 tokens), and refuses with `InputRejected` if it exceeds the checkpoint's budget: `english` 192,
  `multilingual` / `typed-decisions` 256 (from each checkpoint's `rl_agent_config.json`), any other name 192.
  `chars / 3` over-counts English (≈ 4 chars/token), so an instruction just under the real limit may be refused
  and fall back — the safe direction.
- **D7 A re-routed checkpoint is a failed attempt.** Laya treats an unknown `model` name as "no explicit
  checkpoint" and auto-routes it (200, `routing.reason` explains), so a misspelled `api_model_id` silently
  lands on a checkpoint with a different window. When a System One response carries `routing.model` and it
  differs from the requested model, the adapter returns `ProviderError` (status `Some(200)`) naming both — a
  configuration fault, so it counts toward the breaker like any endpoint-wide fault. Only Laya sends `routing`.

## Acceptance criteria

- **AC1** A gateway with a `laya` router (url only) can make a decision: the facade registers a
  `SystemOneAdapter` with id `laya` for decisions and no chat adapter.
- **AC2** A `laya` decision with ≥ 1 image returns `InputRejected` (D5) and **no HTTP request is sent** (wiremock
  expects 0 calls); the same request on `llamacpp` still sends data-URL images.
- **AC3** Laya's real default response (captured live, including `routing`, `x_jev_confidence`, noul `confidence`,
  extended `usage`) decodes into the right `DecisionAnswers`, `model`, and usage tokens.
- **AC4** A 200 with `usage.truncated: true` is an `InputRejected` (D5) that falls back; `truncated: false` (even
  with `state_tokens_dropped > 0`) or absent is a success; `truncated: null` fails the parse (`ProviderError`).
- **AC5** Laya's errors map: 401 → `Authentication`; 400 / 413 / 422 / 500 / 503 → `ProviderError` with the
  `detail` string as the message.
- **AC6** Ignored live tests against a real `laya-coreml-serve` (`LAYA_URL`; `LAYA_MODEL`, default `english`):
  the adapter answers a choice, a noul and a score with a well-formed distribution, a state many times the
  checkpoint's window (~6000 tokens, under Laya's 50 000-char cap) comes back as the D3 `InputRejected`, and an image is refused before sending; a
  `decide` chain through the facade and engine is served by `laya` in one attempt.
- **AC7** Docs list `laya` everywhere `llamacpp`/`sglang` are listed (`docs/llms/local.md`, `recipes.md`,
  `docs/features/inference/providers.md`, `capabilities-and-adapters.md`, `docs/features/README.md`, both
  `SKILL.md` copies, the release notes in `docs/llms/upgrading.md` (this repo has no CHANGELOG) and their generated
  site copies), and `docs_sync_capabilities` passes.

- **AC8 (D5)** An `InputRejected` attempt falls back even with an empty trigger set, never records a breaker /
  lockout / cooldown / performance outcome (N rejected attempts leave the endpoint `Closed` and dispatchable), and
  an exhaustion made only of input rejections is `AllAttemptsFailed { retryable: false }`; beside a gated candidate
  it is `AllGated` on that gate; beside a hard failure it is retryable.
- **AC9 (D6)** A `laya` question whose estimated head exceeds the checkpoint's budget is refused with
  `InputRejected` before any request (wiremock `expect(0)`); a head under it is sent; other ids are never
  budget-checked. Live: the 808-character instruction on `english` is refused, the 288-character one is answered.
- **AC10 (D7)** A 200 whose `routing.model` differs from the requested model is a `ProviderError` naming both;
  an equal or absent `routing` is a success. Live: `model: "englsh"` is refused.

## Testing

Red-first per task. Unit: `encode_images` under `Unsupported`; adapter picks the encoding by id. Wiremock
(`tests/systemone_mock.rs`): AC2–AC4 against bodies captured from the live server; AC5 against live 401/400/413 bodies and
representative 422/500/503 `detail` strings. Facade: AC1 alongside the
SP-DEC-2 "which adapter each id gets" test. Engine fallback: AC4 with a second decision router. Live: AC6,
run on this machine before merge. Mutation-check the image guard and the truncation check.
