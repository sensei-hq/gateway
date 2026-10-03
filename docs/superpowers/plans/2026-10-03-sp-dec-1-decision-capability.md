# SP-DEC-1 — Decision (System One) capability

Closes gh#72. Spec and plan in one file: the issue's "Done when" list is the
acceptance contract, so this file maps it onto tasks rather than restating it.

## Contract (verified 2026-10-03)

- Wire: `POST {base}/v1/systemone` — `{model, state, questions{name: choice|noul|score}, images?, keep_alive?}`
  → `{model, answers{name: …}, usage{input_tokens, output_tokens}}`. Origin is TypeSafe's Jev API;
  Ollama 0.35 implements it (docs.ollama.com/api/systemone), OpenRouter serves it at
  `https://openrouter.ai/api/v1/systemone`, TypeSafe at `https://api.typesafe.ai/v1/systemone`.
- Captured live against local Ollama 0.35.0 + `nimble`: a mixed choice/noul/score request → 200;
  non-decision model → `400 {"error":"model \"llama3.2\" is not supported by System One; …"}`;
  missing model → `404 {"error":"model \"nosuch\" not found, try pulling it first"}`; empty questions →
  `400`; an endpoint Ollama doesn't have → `404` **plain text** `404 page not found` (what a pre-0.35
  server returns for `/v1/systemone`). `/api/show` lists `"decision"` in `capabilities` for decision models.
- Order is semantic: choice ties follow option order, score criteria are ordered. Maps are `IndexMap`.

## Decisions

- **D1** `Capability::Decision` (serde `decision`) — matches Ollama's capability string and OpenRouter's
  `decisions` output modality.
- **D2** `GatewayError::InvalidRequest { message }` — a hard stop raised before selection for structural
  violations (1–64 questions, ≥2 choice/score criteria, non-blank names/state). Not a fallback trigger:
  every candidate would reject it identically. Per-provider *upper* bounds (Ollama 26 options, TypeSafe
  255/10) stay with the provider's 400, which does fall back.
- **D3** Short context is enforced by the existing `ContextWindowGate`: the pessimistic estimate for a
  decision payload counts state + every question (Tev1 scores each question with the whole set in its
  prompt), and the consumer seeds a model's real `context_window`.
- **D4** Routers: Ollama (native, keyless) + a generic id-configurable `SystemOneAdapter` (bearer auth) the
  facade auto-registers for `openrouter` and `typesafe`. Cloudflare Workers AI (different image shape,
  truncates input) and Liquid (non-standard path, usable via `url`) are carry-forwards.
- **D5** No streaming: `execute_stream` already refuses every non-chat capability.
- **D6** Breaking public-API additions (new enum variants, response field) → release **v0.7.0**.

## Tasks

| # | Task | AC (gh#72) |
|---|---|---|
| T1 | kernel types: `Capability::Decision`, `types::decision` (questions, answers, `validate`), `Payload::Decision`, `InferenceResponse.decisions`, io `DecisionRequest/Response`, `GatewayError::InvalidRequest`; wire round-trips against the doc + live fixtures | 1, 3, 4 |
| T2 | `DecisionModel` trait, registry `decision` map + `list`, re-exports, `NoopAdapter` | 1 |
| T3 | engine: dispatch arm, boundary `to_/from_decision_*`, three exhaustive estimators, upfront validation in `execute` | 3 |
| T4 | cloud-providers `systemone` core + `SystemOneAdapter`; Ollama `DecisionModel` + error interpretation + `probe_decision_model`; wiremock + ignored live test | 2, 5 |
| T5 | facade auto-registration: `openrouter`, `typesafe` | — |
| T6 | docs: capability tables/matrices, providers, llms, SKILL.md; `confidence` = concentration at point of use | 4 |
| T7 | whole-slice review, then release v0.7.0 | — |

Downstream (sensei, after v0.7.0): `model_capability` gains `decision`, seed decision models
(nimble, tev1, clef, clef-flash on ollama; jev/tev1 on openrouter) and recent prominent models,
`map_capability("decision")`. Needs sensei's gateway pin past v0.5.1 (sensei#202).

## Progress

| # | Commit | Notes |
|---|---|---|
| T1–T2 | `e21ade4` | types+trait+registry in one commit (a test that cannot compile without the type cannot be committed red under the clippy hook — SP-ROUTE-1 T1 precedent); validate_decision 4/4 mutations caught |
| T3 | `49c9000` red → `8f80053` | 7 engine tests; 8/8 mutations caught (criteria counting initially survived — test tightened) |
| T4 | `04c9e2e` red → `7bcd6a0` | 9 wiremock + 1 live (Ollama 0.35.0 + nimble, passes); 8/8 mutations caught |
| T5 | red → `50cafb9` | facade: openrouter (chat + decision), typesafe |
| T6 | docs commit + re-export fix | docs sync found `systemone` missing from `gateway::adapters` re-exports — fixed, pinned by `reexport_paths`; upgrading.md 0.6.x → 0.7.0 |
| T7 | — | whole-slice review, then v0.7.0 |

Carry-forwards: Cloudflare Workers AI (`clef`, different image shape, truncates); Liquid (non-standard path);
emulating decisions over logprobs on chat endpoints; sensei seed + `map_capability` (needs sensei#202).
