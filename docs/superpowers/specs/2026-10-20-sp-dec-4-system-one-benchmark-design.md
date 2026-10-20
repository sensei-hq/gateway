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
| S1a | Intent routing with an out-of-scope option: 9 options per case: `oos` plus 8 intents (gold + 7 seeded distractors, or 8 distractors when gold is `oos`), so every case has the same 1-in-9 chance baseline | choice | `clinc/clinc_oos` config `plus` (HF datasets-server) | CC-BY-3.0 |
| S1b | Tool/intent routing, 8 candidates per case (gold + 7 seeded distractors from 77 intents) | choice | `legacy-datasets/banking77` | CC-BY-4.0 |
| S2 | Ordered triage score (negative / neutral / positive as a 3-level score) | score | `Sp1786/multiclass-sentiment-analysis-dataset` | Apache-2.0 |
| S3 | Does B follow from A? (entailment → yes, contradiction → no; neutral excluded) | noul | `stanfordnlp/snli` | CC-BY-SA-4.0 |
| S4 | Coding-pipeline gates: test output → continue / fix_code / retry / fix_environment; review finding → block / follow_up / dismiss; "does the change fix the finding?" | choice + noul | hand-curated, committed | repo |

Dataset notes:
- Verified 2026-10-20: each id exists, is ungated, and has its splits served by `datasets-server.huggingface.co`. `/rows` pages are at most 100 rows long. CLINC `plus` has 151 labels, `oos` among them (looked up by name, never by index). The sentiment set is 0 negative, 1 neutral, 2 positive. datasets-server serves only the current revision, so `fetch` records the hub sha and refuses to reuse a cache built from a different one. Limitation: datasets-server can lag a hub push, so for a short window it may serve rows of the previous revision under the new sha. The rows sha256 in the manifest makes any such drift visible on re-fetch.
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
  - Requests go through `FacadeBuilder` → `Gateway::execute`, so results include alias handling and the gateway's own fit rules. No gateway code changes (user decision 2026-10-20, option 2).
- **D2a A case that does not fit a model is `not_applicable`, not wrong** (user decision 2026-10-20). The outcomes are:
  - `ok`: answered.
  - `not_applicable`: the input does not fit this model. This covers two cases.
    - The gateway's context-window refusal. It is typed: `AllGated { human_action: Some(UseLargerContextWindow) }`.
    - Any `InputRejected`: Laya head budget, images, server-side truncation, or collapsed options.
  - `error`: anything else, including a D7 re-routed checkpoint, network errors and 4xx/5xx. It counts as wrong.
  - Nothing is retried or dropped.
- **Detecting `InputRejected`.** It is found by the kernel's own `InputRejected` Display prefix (`adapter '…' cannot serve this input:`) on the single attempt's error string.
  - A bench unit test builds a real `InputRejected` and asserts that prefix. If the kernel format changes, the bench's tests fail instead of silently mis-classifying.
- **Which rows reached the server** (`reached_server` on each row; the report lists the rest under "Rows that never reached the server"). The chain has one candidate, and the kernel keeps only each attempt's error text.
  - Not reached: a gateway gate refused the candidate before dispatch (it is listed in `AllGated.skipped`: context window, cooldown, lockout, breaker), any other pre-dispatch error, and an attempt that failed with the kernel's network-send error text (`network error: error sending request`) before the router timeout ran out: a refused or dropped connection, or a DNS failure (review A1).
  - Reached: an answer; an attempt that got any reply; an attempt that lasted the whole router timeout (the same network-send text, read as a timeout); and an `AllGated` with an empty `skipped` (the engine gated the chain after an attempt, e.g. a 429 lockout). A server-side truncation is reached, because it is only known after the call.
  - Caveat: an adapter's own pre-send `InputRejected` (Laya head budget, images) is a recorded attempt, so it reads as reached although nothing was sent; its error text cannot be told apart from a server-side truncation.
  - Caveat: a connect that hangs for the whole router timeout (a blackholed host) reads as reached, because its text is the same as a timeout's.
- **D3 Config.** `bench.toml` (example committed, local copy gitignored) lists models: id, router, url, `api_model_id`, `context_window`.
- **D4 Commands.**
  - `check`: probes every configured router with one tiny decision. A model whose probe gets no answer (unreachable, an error, or no reply in time), or that no input fits its context window (e.g. `laya-ane`), is **NOT RUN** and is named at the top of the report, never silently skipped.
  - `fetch [--per-scenario N=200] [--seed 2026] [--out cases] [--cache-dir …]`: pulls rows from datasets-server `/rows` (JSON, paginated) into `<user cache dir>/decision-bench/<id with / → __>@<sha>/<config>-<split>.jsonl`, beside a `meta.json` written last. It then builds the seeded case files. The cache is outside the repo.
  - `run [--config …] [--cases cases] [--scenarios …] [--models …] [--out results.json]`: runs cases sequentially, after one warm-up call per model. It writes `results.json`, one row per (case, model) with the fields `case_id`, `scenario`, `model`, `outcome`, `correct`, `top_probability`, `score_value`, `expected_level`, `answer`, `probabilities`, `latency_ms`, `reached_server` and `detail`.
  - `report`: renders Markdown from `results.json`.
- **D5 Metrics, per (scenario, model):**
  - Coverage: the share of the scenario's cases that are applicable, i.e. not `not_applicable`.
  - Accuracy over the applicable cases, where `error` counts as wrong. The error rate is reported too.
  - Bootstrap 95% CI for accuracy, using 1000 seeded resamples.
  - Expected calibration error on the top probability, in 10 equal-width bins.
  - Confident-wrong rate: wrong with top probability ≥ 0.8.
  - For score questions, also exact-level accuracy (argmax) and mean absolute error of the expected level.
  - Noul answers are decided at 0.5.
  - Latency p50/p95.
- **D6 Ranking.**
  - **Head-to-head on a shared subset.** The ranking within a scenario uses only the cases that are applicable to every model being ranked, so no model is graded on an easier subset.
  - **Coverage floor.** A model with coverage below 50% is left out of the head-to-head and gets its own table over its own applicable cases. Example: "ANE: 30% coverage, 0.91 accuracy on short checks".
  - **Ordering.** Inside the head-to-head, models are ordered by accuracy, then lower confident-wrong rate, then lower p50 latency.
  - **Insufficient.** Fewer than 2 head-to-head models, or fewer than 20 shared cases (`MIN_SHARED_CASES`), is **Insufficient**: no verdict, with the reason.
  - Otherwise a model is called the **winner** only if its CI lower bound exceeds the CI upper bound of **every** other ranked model, not just the runner-up. This means the verdict cannot depend on an accuracy or latency tiebreak (T6 review). Otherwise the top models are reported as **tied**: the tie lists the top model and every ranked model whose CI overlaps the top model's, i.e. `hi_other ≥ lo_top` (with the every-model winner rule, Winner and Tie are exact complements).
  - The report also has a scenario × model summary table and a "use X for scenario Y" line per scenario (or "tie: X, Y").
- **D7 Outputs.**
  - `results.json` is gitignored and machine-readable, with stable field names so a later routing slice can read it.
  - The first local report is committed to `docs/benchmarks/2026-10-<dd>-system-one-local.md`, with roster, datasets, licences, revisions, seeds and NOT RUN models.

## Acceptance criteria

- **AC1** `cargo run -p decision-bench -- check` reports every configured router as reachable or NOT RUN. NOT RUN covers an unreachable model and one that no input fits its context window; neither aborts the run silently.
- **AC2** `fetch` is reproducible: the same seed, revision and row content give byte-identical case files. Each dataset's rows sha256 is recorded in the manifest so this is checkable. `fetch` parses datasets-server JSON, tested against recorded fixtures (API responses, not datasets). Fetched data is never committed: the case dir and cache are gitignored or outside the repo. Every case's `source` carries the seed and rand version.
- **AC3** `run` records `ok` / `not_applicable` / `error` per case.
  - A context-window refusal is `not_applicable`, and so is any `InputRejected`. The prefix used to detect `InputRejected` is pinned by a unit test against the real kernel type.
  - A D7 re-route and every other error are `error`, and count as wrong.
  - Nothing is dropped, and coverage is reported per (scenario, model).
- **AC4** Every metric in D5 has a unit test against hand-computed values: accuracy both ways, bootstrap CI with a seed, ECE, confident-wrong, score MAE, and p50/p95.
- **AC5** The D6 rules are unit-tested:
  - the shared-subset rule: a case that is not applicable to any one compared model is excluded from that scenario's ranking;
  - the 50% coverage floor;
  - the tie rule: overlapping CIs give "tied", separated CIs give "winner".
- **AC6** A wiremock end-to-end test runs `run` + `report` over two fake routers, one of which refuses some cases with `InputRejected`. The report shows:
  - the expected coverage;
  - the shared-subset ranking;
  - NOT RUN handling.
- **AC7** The first live local run over S1–S4 against the six-model roster produces the committed report. S4 is marked unreviewed until the user signs off. The report states any model that was NOT RUN.
- **AC8** Docs: a `docs/llms` page (or a section in `local.md`) explains how to run the bench and read the report. The crate README documents config, commands and dataset licences.

## Testing

Red-first per task.
- Unit tests cover metrics, ranking, sampling and parsing.
- Wiremock covers the end-to-end run.
- The live run is the acceptance test.
- Every guard is mutation-checked, e.g. the tie rule, `not_applicable` classification, the shared subset and seeded sampling.

## Not in scope (carry-forwards)

- **Tiny-window decision models can't be benchmarked through the gateway** (user decision 2026-10-20, option 1). The gateway's decision-size estimate puts even the minimal decision at about 182 input tokens, so a 96-token checkpoint such as Laya ANE is refused before sending. The bench reports it as NOT RUN, with that estimate in the reason. Follow-up (gateway): make the decision prompt-overhead estimate model-aware, so small-window decision models are usable.

- **A transport failure is told from a timeout by duration only** (review A1). The kernel keeps only each attempt's error text, and a refused connection, a DNS failure, a dropped connection and a timeout all read `network error: error sending request`. The bench calls an attempt that failed so before the router timeout "not reached", and one that lasted the whole timeout "reached", so a connect that hangs to the timeout reads as reached. An adapter's pre-send `InputRejected` also reads as reached. Follow-up (gateway kernel): preserve the error's source or kind per attempt (connect vs. timeout vs. pre-send), so `reached_server` need not infer it.

- Hosted models: config is ready, but they need keys and a spend cap.
- Routing by benchmark score in the engine.
- Calibration fitting.
- Multi-question cases.
- A CI job for the bench: it needs model servers.
