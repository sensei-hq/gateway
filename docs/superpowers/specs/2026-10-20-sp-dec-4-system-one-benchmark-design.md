# SP-DEC-4 — System One benchmark harness + per-scenario ranking report

Follows SP-DEC-3 (v0.12.0). The user's choices, 2026-10-20:
- Deliverable: harness + report + JSON results only. Routing by score is NOT in scope; the JSON stays shaped so a later
  slice could consume it.
- Data: mixed — public labelled datasets fetched at run time, plus hand-curated coding-gate sets the user signs off.
- Form: a workspace binary crate driven through the gateway facade.
- First run: local models only.
- Scenarios S1–S4 as below.

Motivation: the 2026-10-20 zero-shot gate experiment (memory `laya-zero-shot-gates-unreliable`) showed System One
accuracy varies by scenario and model, and that confidence alone misleads (1.00-confident wrong answers). Users need
evidence for "which model for which scenario".

## Roster (first run — local, free)

| Model id | Router | Served by | Window |
|---|---|---|---|
| `laya-english` | `laya` | `laya-coreml-serve`, checkpoint `english` | 512 |
| `laya-multilingual` | `laya` | checkpoint `multilingual` | 1024 |
| `laya-typed` | `laya` | checkpoint `typed-decisions` | 1024 |
| `laya-ane` | `laya` | `LAYA_EXTRA_MODELS={"ane":"aac6fef/laya-multilingual-coreml-ane"}` | 96 |
| `nimble` | `ollama` | Ollama 0.40 `nimble:latest` | 8192 |
| `nimble-v3` | `llamacpp` | `llama-server` b11381, `Bespoke-Nimble-9B-v3-Q4_K_M.gguf` | 8192 |

Hosted routers (TypeSafe, OpenRouter, Cloudflare) are config-only. They are not run in this slice: they need keys and cost money.

## Scenarios

One question per case.

| Id | Scenario | Type | Source | Licence |
|---|---|---|---|---|
| S1a | Intent routing (10 domains + out-of-scope) | choice | `clinc/clinc_oos` config `plus` (HF datasets-server) | CC-BY-3.0 |
| S1b | Tool/intent routing, 8 candidates per case (gold + 7 seeded distractors from 77 intents) | choice | `legacy-datasets/banking77` | CC-BY-4.0 |
| S2 | Ordered triage score (negative / neutral / positive as a 3-level score) | score | `Sp1786/multiclass-sentiment-analysis-dataset` | Apache-2.0 |
| S3 | Does B follow from A? (entailment → yes, contradiction → no; neutral excluded) | noul | `stanfordnlp/snli` | CC-BY-SA-4.0 |
| S4 | Coding-pipeline gates: test output → continue / fix_code / retry / fix_environment; review finding → block / follow_up / dismiss; "does the change fix the finding?" | choice + noul | hand-curated, committed | repo |

Dataset notes:
- Verified 2026-10-20: each id exists, is ungated, and has its splits served by `datasets-server.huggingface.co`.
- `PolyAI/banking77` is not served, so its `legacy-datasets/` copy is used.
- `SetFit/sst5` was rejected (no licence), and so were Yelp (licence "other") and MultiNLI (mixed licence).
- Public data is **fetched, never committed**.
- The report names each dataset, its licence and the revision sha that was used.

S4 lives in `crates/decision-bench/scenarios/s4-*.jsonl`, 50–100 cases per gate, built from realistic `cargo test` output and review findings. Each file carries a `labels_reviewed_by` header. Until the user fills it in, the report marks S4 **labels unreviewed**.

## Decisions

- **D1 Crate.** New `crates/decision-bench`.
  - `publish = false`, with one binary, `decision-bench`.
  - It depends on `gateway` (facade only), `tokio`, `serde`/`serde_json`, `reqwest`, `clap`, `toml`, and a seeded RNG (`rand` with a fixed `StdRng` seed).
  - It adds nothing to `gateway`'s public API and makes no gateway code changes.
- **D2 Through the facade.** Each model gets a single-entry decision chain with **no fallback**, so a result belongs to exactly one model.
  - Requests go through `FacadeBuilder` → `Gateway::execute`, so results include alias handling, the D6 budget refusals and `InputRejected`.
  - An `InputRejected` counts as outcome `refused`. Any other error counts as outcome `error`. Neither is retried or dropped.
- **D3 Config.** `bench.toml` (example committed, local copy gitignored) lists models: id, router, url, `api_model_id`, `context_window`.
- **D4 Commands.**
  - `check`: probes every configured router. An unreachable one is **NOT RUN** and is named at the top of the report, never silently skipped.
  - `fetch [--per-scenario N=200] [--seed 2026]`: pulls rows from datasets-server `/rows` (JSON, paginated) into `~/.cache/decision-bench/<dataset>@<sha>/<split>.jsonl`. It then builds the seeded case files. The cache is outside the repo.
  - `run [--scenarios …] [--models …]`: runs cases sequentially, after one warm-up call per model. It writes `results.json`, one row per (case, model): answer, probabilities, correct, latency_ms, outcome.
  - `report`: renders Markdown from `results.json`.
- **D5 Metrics, per (scenario, model):**
  - Accuracy two ways: over answered cases, and over all cases (refused or error counts as wrong).
  - Refusal rate and error rate.
  - Bootstrap 95% CI for accuracy over all cases: 1000 resamples, seeded.
  - Expected calibration error on the top probability, in 10 equal-width bins.
  - Confident-wrong rate: wrong with top probability ≥ 0.8.
  - For score questions, also exact-level accuracy (argmax) and mean absolute error of the expected level.
  - Noul answers are decided at 0.5.
  - Latency p50/p95.
- **D6 Ranking.**
  - Within a scenario, models are ordered by all-cases accuracy, then lower confident-wrong rate, then lower p50 latency.
  - A model is called the **winner** only if its CI lower bound exceeds the runner-up's CI upper bound. Otherwise the top models are reported as **tied**.
  - The report also has a scenario × model summary table and a "use X for scenario Y" line per scenario (or "tie: X, Y").
- **D7 Outputs.**
  - `results.json` is gitignored and machine-readable, with stable field names so a later routing slice can read it.
  - The first local report is committed to `docs/benchmarks/2026-10-<dd>-system-one-local.md`, with roster, datasets, licences, revisions, seeds and NOT RUN models.

## Acceptance criteria

- **AC1** `cargo run -p decision-bench -- check` reports every configured router as reachable or NOT RUN; an unreachable one never aborts the run silently.
- **AC2** `fetch` is reproducible: the same seed and revision give byte-identical case files. It parses datasets-server JSON. This is tested against a recorded fixture: a JSON file captured from the API, not real dataset rows. The fetched data is never written inside the repo.
- **AC3** `run` records `ok` / `refused` / `error` per case. An `InputRejected` is `refused`, and a refused case counts as wrong in all-cases accuracy. A refusal is never dropped.
- **AC4** Every metric in D5 has a unit test against hand-computed values: accuracy both ways, bootstrap CI with a seed, ECE, confident-wrong, score MAE, and p50/p95.
- **AC5** The D6 ranking and tie rule are unit-tested. A test shows overlapping CIs give "tied" and separated CIs give "winner".
- **AC6** A wiremock end-to-end test runs `run` + `report` over two fake routers, one of which refuses some cases. The report shows the expected ranking, refusal rate and NOT RUN handling.
- **AC7** The first live local run over S1–S4 against the six-model roster produces the committed report. S4 is marked unreviewed until the user signs off. The report states any model that was NOT RUN.
- **AC8** Docs: a `docs/llms` page (or a section in `local.md`) explains how to run the bench and read the report. The crate README documents config, commands and dataset licences.

## Testing

Red-first per task.
- Unit tests cover metrics, ranking, sampling and parsing.
- Wiremock covers the end-to-end run.
- The live run is the acceptance test.
- Every guard is mutation-checked, e.g. the tie rule, refusal counting and seeded sampling.

## Not in scope (carry-forwards)

- Hosted models: config is ready, but they need keys and a spend cap.
- Routing by benchmark score in the engine.
- Calibration fitting.
- Multi-question cases.
- A CI job for the bench: it needs model servers.
