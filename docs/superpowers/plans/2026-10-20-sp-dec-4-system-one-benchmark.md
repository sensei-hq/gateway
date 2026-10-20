# SP-DEC-4 — System One benchmark harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `decision-bench` binary that runs every configured System One router through the gateway facade on four labelled scenarios. It writes `results.json` and a Markdown per-scenario ranking.

**Architecture:**
- **Crate:** a new workspace crate, `crates/decision-bench` (`publish = false`).
- **Data flow:** fetch → cases → run → results → report.
- **Fetching:** public datasets come from the HF datasets-server JSON API into a cache outside the repo. Hand-curated S4 cases are committed JSONL.
- **Runner:** each model gets a single-entry chain with no fallback. Every case is classified `ok` / `not_applicable` / `error`, so metrics and ranking work on the applicable subset.

**Tech Stack:** Rust 2021, `gateway` (facade), `tokio`, `serde`/`serde_json`, `reqwest` (rustls), `clap` (derive), `toml`, `rand` (`StdRng` seeded), `wiremock` (tests).

**Spec:** `docs/superpowers/specs/2026-10-20-sp-dec-4-system-one-benchmark-design.md` (D1–D7, AC1–AC8).

---

## Ground rules

- **Branch.** `git switch -c sp-dec-4-bench` in a worktree `../gateway-sp-dec-4`, with its own target dir (the default `target/` inside the worktree).
- **Red first, every task.** A red run shows `panicked at` (or a named compile error). Exit 101 alone proves nothing.
- **Real exit codes.** Run `cmd > /tmp/sp4.log 2>&1; echo "exit $?"`, then `rg -n "error\[|panicked at|test result" /tmp/sp4.log`. Never pipe to `tail`, and make sure the test you meant actually ran (`... ok`).
- **Before every commit.** Run `cargo fmt --all`; the pre-commit hook runs fmt-check and clippy, but no tests. Stage only the named files and check `git diff --cached --stat`.
- **Mutations.** Keep a `/tmp` backup; never use `git checkout`/`restore`/`reset --hard`.
- **No gateway changes.** If a needed API is not public, STOP and report it rather than editing `crates/gateway`.

## Verified facts (2026-10-20)

- **datasets-server `GET /rows?dataset=&config=&split=&offset=&length=`.**
  - `length` must be ≤ 100.
  - The response is `{features:[{name,type}], rows:[{row_idx,row:{…},truncated_cells}], num_rows_total, num_rows_per_page, partial}`.
  - A `ClassLabel` feature's `type` is `{"names":[…], "_type":"ClassLabel"}`.
- **Hub sha.** `GET https://huggingface.co/api/datasets/<id>` returns `{sha, cardData:{license}}`.
- **Datasets:**

  | Dataset id | config / split | fields |
  |---|---|---|
  | `clinc/clinc_oos` | `plus` / `test` (5500 rows) | `text`, `intent` (ClassLabel, 151 names, `oos` among them) |
  | `legacy-datasets/banking77` | `default` / `test` (3080) | `text`, `label` (ClassLabel, 77 names) |
  | `Sp1786/multiclass-sentiment-analysis-dataset` | `default` / `test` (5206) | `text`, `label` (int: 0 negative, 1 neutral, 2 positive), `sentiment` |
  | `stanfordnlp/snli` | `plain_text` / `test` (10000) | `premise`, `hypothesis`, `label` (ClassLabel `["entailment","neutral","contradiction"]`; −1 means no gold) |

- **Gateway public API.**
  - `gateway::{FacadeBuilder, Gateway, InferenceRequest, InferenceResponse, GatewayError, Capability}`
  - `gateway::types::config::{GatewayConfig, RouterConfig, ModelConfig, FallbackChainConfig, ChainEntry, FallbackTrigger}` (`ChainEntry.priority: u8`)
  - `gateway::types::decision::{DecisionQuestion, DecisionQuestions, DecisionAnswer}`
  - `gateway::types::request::Payload::Decision { state, questions, images, keep_alive }`
  - `gateway::types::error::HumanAction::UseLargerContextWindow`
  - The response carries `decisions: Option<DecisionAnswers>` and `attempts: Vec<Attempt>`; `Attempt.error` is `Option<String>`.
- **Errors for a single-candidate chain.**
  - A context-window refusal is `Err(AllGated { human_action: Some(UseLargerContextWindow), .. })`.
  - An `InputRejected` is `Err(AllAttemptsFailed { attempts_detail, .. })`, where `attempts_detail[0].error` starts with the kernel Display `"adapter '<id>' cannot serve this input: "`.

## File map

| File | Responsibility |
|---|---|
| `Cargo.toml` (workspace) | add `crates/decision-bench` to `members` |
| `crates/decision-bench/Cargo.toml` | crate manifest, `publish = false`, `[[bin]] name = "decision-bench"` |
| `crates/decision-bench/src/main.rs` | clap CLI: `check`, `fetch`, `run`, `report` |
| `crates/decision-bench/src/lib.rs` | `pub mod` for the modules below (so tests can import them) |
| `src/config.rs` | `BenchConfig` / `ModelSpec` from `bench.toml`; build a `GatewayConfig` per model |
| `src/case.rs` | `Case`, `Expected`, `Scenario`; JSONL read/write |
| `src/fetch.rs` | datasets-server + hub client, cache layout, sha guard |
| `src/scenarios.rs` | build S1a/S1b/S2/S3 cases from raw rows (seeded); load S4 |
| `src/run.rs` | execute cases through the facade; `Outcome` classification; `ResultRow` |
| `src/metrics.rs` | coverage, accuracy, bootstrap CI, ECE, confident-wrong, MAE, percentiles |
| `src/rank.rs` | shared-subset head-to-head, coverage floor, tie rule |
| `src/report.rs` | Markdown rendering |
| `crates/decision-bench/scenarios/s4-*.jsonl` | hand-curated gate cases with a `labels_reviewed_by` header line |
| `crates/decision-bench/bench.example.toml` | the six-model local roster |
| `crates/decision-bench/README.md` | config, commands, dataset licences |
| `crates/decision-bench/tests/e2e.rs` | wiremock end-to-end (AC6) |
| `crates/decision-bench/tests/fixtures/rows_*.json` | recorded datasets-server responses (3 rows each, captured live) |
| `.gitignore` | `crates/decision-bench/bench.toml`, `crates/decision-bench/results*.json` |
| `docs/benchmarks/2026-10-<dd>-system-one-local.md` | first live report (T10) |
| `docs/llms/local.md` | "Benchmarking System One models" section (T11) |

---

### Task 1: crate skeleton + config

**Files:** workspace `Cargo.toml`, `crates/decision-bench/{Cargo.toml,src/lib.rs,src/main.rs,src/config.rs,bench.example.toml}`, `.gitignore`.

- [ ] **Step 1 — Manifest.**

```toml
[package]
name = "decision-bench"
version = "0.12.0"
edition = "2021"
publish = false
description = "Benchmark System One decision models through the sensei gateway"

[[bin]]
name = "decision-bench"
path = "src/main.rs"

[dependencies]
gateway = { package = "sensei-gateway", path = "../gateway" }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
clap = { version = "4", features = ["derive"] }
toml = "0.8"
rand = "0.8"
anyhow = "1"
dirs = "5"

[dev-dependencies]
wiremock = "0.6"
tempfile = "3"
```

**As built (T1 review):** edition 2024, `reqwest` 0.13 with feature `rustls`, and `dirs` 6, which are the workspace's own versions. The snippet above predates that check. `gateway_config_for` returns `anyhow::Result<GatewayConfig>`, built through the public `GatewayBuilder`, and rejects routers the facade does not register.

Before choosing versions, check each dependency's workspace pin (`rg -n '^(reqwest|tokio|serde|wiremock|rand|toml|clap|anyhow|dirs|tempfile) ' crates/*/Cargo.toml`). Reuse the workspace's version and features wherever one already exists, so the lockfile gains no second major version.

- [ ] **Step 2 — Red test.** Put this in `config.rs` under `#[cfg(test)]`:

```rust
#[test]
fn the_example_config_parses_into_six_models_with_single_entry_chains() {
    let cfg = BenchConfig::from_toml(include_str!("../bench.example.toml")).unwrap();
    assert_eq!(cfg.models.len(), 6);
    let m = &cfg.models[0];
    let g = cfg.gateway_config_for(m);
    let chain = &g.chains[&chain_id(m)];
    assert_eq!(chain.models.len(), 1, "no fallback: one model per chain");
    assert!(chain.fallback_triggers.is_empty());
    assert_eq!(g.routers.len(), 1, "only this model's router");
}
```

- [ ] **Step 3 — Implement `config.rs`.**
  - `#[derive(Deserialize)] pub struct BenchConfig { pub models: Vec<ModelSpec> }`
  - `#[derive(Deserialize, Clone)] pub struct ModelSpec { pub id: String, pub router: String, pub url: String, pub api_model_id: String, pub context_window: u32, #[serde(default)] pub api_key_env: Option<String> }`
  - `BenchConfig::from_toml(&str) -> anyhow::Result<Self>`
  - `pub fn chain_id(m: &ModelSpec) -> String { format!("bench-{}", m.id) }`
  - `gateway_config_for(&self, m)` builds a `GatewayConfig` with exactly one router (`m.router`, `timeout_ms: Some(120_000)`), one `ModelConfig` (`capabilities: vec![Capability::Decision]`, `context_window: m.context_window`, `max_output_tokens: 1`), and one chain with that single entry and `fallback_triggers: vec![]`.
- [ ] **Step 4 — Example config.** Write `bench.example.toml` with the six models from spec §Roster:
  - Laya on `http://127.0.0.1:8766`, with `api_model_id` set to `english` / `multilingual` / `typed-decisions` / `ane`.
  - Ollama on `http://127.0.0.1:11434`, `nimble`.
  - llamacpp on `http://127.0.0.1:8091`, `nimble-v3`.
- [ ] **Step 5 — Main stub.** `main.rs` has a clap enum with `Check`, `Fetch`, `Run` and `Report`, each printing "not implemented yet" and exiting 2. That keeps the binary compiling until the later tasks fill them in. Add the crate to the workspace `members`.
- [ ] **Step 6 — Green.** Run `cargo test -p decision-bench` and `cargo build -p decision-bench`.
- [ ] **Step 7 — Mutation.** Set `fallback_triggers` to `vec![FallbackTrigger::ProviderError]` and confirm the test goes red, then restore it.
- [ ] **Step 8 — Commit.** Add `.gitignore` lines for `crates/decision-bench/bench.toml` and `crates/decision-bench/results*.json`. Commit as `feat(decision-bench): crate skeleton + bench.toml → one single-entry chain per model (SP-DEC-4 T1)`.

### Task 2: cases + JSONL

**Files:** `src/case.rs`, `src/lib.rs`.

- [ ] **Step 1 — Types.**

```rust
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Scenario { S1aIntentOos, S1bBanking77, S2Sentiment, S3Entailment, S4TestOutput, S4ReviewFinding, S4FixCheck }

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Expected { Choice { option: String }, Noul { value: bool }, Score { level: usize } }

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Case {
    pub id: String,
    pub scenario: Scenario,
    pub state: serde_json::Value,
    pub question: gateway::types::decision::DecisionQuestion,
    pub expected: Expected,
    pub source: String, // e.g. "clinc/clinc_oos@155b9c7104:test#61" or "hand-curated"
}
```

- [ ] **Step 2 — Red tests.**
  - `case_round_trips_through_jsonl`: write two cases with `write_jsonl` and read them back with `read_jsonl`; they must be equal.
  - `a_header_line_is_returned_separately`: the first line of an S4 file is `{"labels_reviewed_by": null, "notes": "..."}`; `read_case_file` returns `(Header, Vec<Case>)` as built (`Header { labels_reviewed_by, notes }`); the original text said `(Option<String>, Vec<Case>)`.
  - `expected_must_match_the_question_type`: a `Choice` expectation whose option is not among the question's criteria is rejected with an error naming the case id. Do the same for a `Score` level out of range.
- [ ] **Step 3 — Implement.** Write `read_jsonl`, `write_jsonl`, `read_case_file` and `Case::validate()`. `read_case_file` calls `validate` on every case.
- [ ] **Step 4 — Green, mutation, commit.** For the mutation, remove the validate call and confirm the test goes red. Commit as `feat(decision-bench): Case/Expected types, JSONL io, expectation validation (SP-DEC-4 T2)`.

### Task 3: fetch + scenario builders (AC2)

**Files:** `src/fetch.rs`, `src/scenarios.rs`, `tests/fixtures/rows_{clinc,banking77,sentiment,snli}.json`.

- [ ] **Step 1 — Capture fixtures live.** Save the real response of `GET /rows?...&offset=0&length=3` for each of the four datasets (curl, live) into `tests/fixtures/`. These are API responses, not datasets: three rows each. Do not edit them.
- [ ] **Step 2 — Red tests.**
  - `fetch::parse_rows_page`: parse each fixture into `RowsPage { features, rows: Vec<serde_json::Value>, num_rows_total }` and resolve ClassLabel names. For CLINC, `names[61]` is the intent of row 0.
  - `scenarios::s1a_case_offers_gold_plus_seven_distractors_plus_oos`:
    - Build from a synthetic row set.
    - Assert there are 9 options when the gold label is not `oos` (gold + 7 distractors + `oos`), or 8 when gold is `oos`.
    - Assert no option is duplicated.
    - Assert the options are shuffled with the seed, so gold is not always first.
    - Assert the expected value is the gold label name.
  - `s1b_has_eight_options`: same shape as S1a, without the `oos` rule.
  - `s2_maps_0_1_2_to_score_levels`: criteria are `["negative","neutral","positive"]`, and expected `level == label`.
  - `s3_maps_entailment_yes_contradiction_no_and_drops_neutral_and_unlabelled`: label 0 becomes `Noul{true}`, 2 becomes `Noul{false}`, and 1 and −1 produce no case. The state is `{"premise":…, "hypothesis":…}`.
  - `sampling_is_reproducible`: the same seed and rows give byte-identical JSONL output, and a different seed gives different output.
  - `the_sha_guard_refuses_a_stale_cache`: a cache directory recorded under sha `a` combined with a live sha `b` is an error that names both.
- [ ] **Step 3 — Implement `fetch.rs`.**
  - `hub_sha(client, base, id)` → `GET {hub}/api/datasets/{id}`, returning `.sha`.
  - `fetch_rows(client, base, id, config, split, offsets)` pages with `length=100`. Respect `num_rows_total`; on HTTP 429, retry once after 2 s.
  - The cache is `dirs::cache_dir()/decision-bench/<id with / → __>@<sha>/<config>-<split>.jsonl`.
  - Both bases come from parameters (`datasets-server.huggingface.co` and `huggingface.co`) so wiremock can stand in for them in tests.
- [ ] **Step 4 — Implement `scenarios.rs`.**
  - Builders take `(rows, label_names, seed, n)` and return `Vec<Case>`.
  - Sampling: draw `n` row indices without replacement using `StdRng::seed_from_u64(seed ^ scenario_tag)`, then sort them so the output is stable.
  - Questions:
    - S1a/S1b: `DecisionQuestion::Choice { instructions: json!("Which intent does this request express?"), criteria: options→None }`.
    - S2: `Score { instructions: json!("What is the sentiment of this text?"), criteria: vec!["negative","neutral","positive"] }`.
    - S3: `Noul { instructions: json!("Does the hypothesis follow from the premise?"), criteria: None }`.
  - For S1a and S1b, the state is the text as a JSON string.
- [ ] **Record versions.** Record the `rand` crate version next to the seed, in each case's `source` metadata and in `results.json`, because `StdRng`'s algorithm is not stable across rand majors. That way a report states exactly how to reproduce its sample.
- [ ] **Step 5 — `fetch` command.** `main.rs fetch --per-scenario N --seed S --out <dir, default crates/decision-bench/cases>` writes `s1a.jsonl`, `s1b.jsonl`, `s2.jsonl` and `s3.jsonl`. Add `crates/decision-bench/cases/` to `.gitignore`, because public data is never committed.
- [ ] **Step 6 — Green.** Mutate the seed handling (use a constant seed) and the oos rule, and confirm the tests go red. Commit as `feat(decision-bench): datasets-server fetch with sha guard + seeded S1a/S1b/S2/S3 builders (SP-DEC-4 T3)`.

### Task 4: runner + outcome classification (AC3)

**Files:** `src/run.rs`.

- [ ] **Step 1 — Types.**

```rust
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Outcome { Ok, NotApplicable, Error }

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ResultRow {
    pub case_id: String,
    pub scenario: Scenario,
    pub model: String,
    pub outcome: Outcome,
    pub correct: Option<bool>,          // None unless Ok
    pub top_probability: Option<f64>,   // the answered option's probability (noul: max(p, 1-p))
    pub score_value: Option<f64>,       // Score answers: the expected-level value
    pub latency_ms: u64,
    pub detail: Option<String>,         // error / not_applicable reason
}

/// The kernel's `InputRejected` Display prefix — pinned by `the_input_rejected_prefix_matches_the_kernel`.
pub const INPUT_REJECTED_MARKER: &str = "cannot serve this input: ";
```

- [ ] **Step 2 — Red tests.**
  - `the_input_rejected_prefix_matches_the_kernel`: `GatewayError::InputRejected { adapter: "laya".into(), message: "m".into() }.to_string()` contains `INPUT_REJECTED_MARKER` and starts with `"adapter 'laya' "`.
  - `classify`:
    - `AllGated { human_action: Some(UseLargerContextWindow), .. }` → `NotApplicable`.
    - `AllAttemptsFailed` whose single attempt error contains the marker → `NotApplicable`.
    - `AllAttemptsFailed` with a `ProviderError` text → `Error`.
    - A network error → `Error`.
  - `score`:
    - A Choice answer equal to the expected option is correct, and `top_probability = probabilities[choice]`.
    - Noul at 0.5: `p ≥ 0.5` means `true`.
    - Score: `correct = argmax level == expected level`, and `score_value` is recorded.
  - `run_case_uses_the_models_chain`: unit-tested through the T8 e2e test, not here.
- [ ] **Step 3 — Implement.**
  - `pub fn classify(err: &GatewayError) -> Outcome`
  - `pub fn score(case: &Case, answer: &DecisionAnswer) -> (bool, f64, Option<f64>)`
  - `pub async fn run(cfg: &BenchConfig, cases: &[Case], models: &[String]) -> Vec<ResultRow>`. For each model:
    - build `FacadeBuilder::new(cfg.gateway_config_for(m)?).circuit_breaker(CircuitBreakerConfig { threshold: usize::MAX, timeout: Duration::from_secs(1), half_open_max_requests: 1 }).build().await.gateway`.
      - **The breaker must be off for bench runs** (T1 review). With the default threshold of 5, five real server failures in a row gate every later case into a ~0 ms skip that never reaches the server, so cases are dropped silently.
      - Build one facade per model, and pin `chain: Some(chain_id(m))` on every request.
      - T8 proves this: a mock returns 500 six times, and the sixth case still reaches the server.
    - send one warm-up call (the first case, result discarded);
    - then for each case, build an `InferenceRequest` (`Capability::Decision`, `chain: Some(chain_id(m))`, `payload: Payload::Decision { state, questions: {"q": question}, images: vec![], keep_alive: None }`, `allow_fallback: false`, the other fields as in `crates/gateway/tests/decision_laya.rs`), time `gw.execute`, and classify the result.
  - A result whose `decisions` is missing `"q"` is `Error` with `detail: "no answer"`.
- [ ] **Step 4 — Green.** Mutate `classify` so that any `AllAttemptsFailed` is `NotApplicable`; the ProviderError case must go red. Commit as `feat(decision-bench): runner through the facade; ok / not_applicable / error classification (SP-DEC-4 T4)`.

### Task 5: metrics (AC4)

**Files:** `src/metrics.rs`.

- [ ] **Step 1 — Red tests, each against hand-computed values.**
  - `coverage`: 10 rows, of which 3 are NotApplicable, gives 0.7.
  - `accuracy over applicable`: among 7 applicable rows, 5 correct and 1 Error give 5/7.
  - `bootstrap_ci_is_seeded_and_brackets_the_mean`: with seed 7 and 1000 resamples over `[1,1,1,0,1,0,1,1,1,1]`, the CI is identical across two calls and `lo ≤ 0.8 ≤ hi`. Also assert the exact `(lo, hi)` the implementation produces with seed 7, computed once and pinned in the test with a comment.
  - `ece`: bins `[(p=0.95, correct), (0.95, wrong), (0.55, correct), (0.55, correct)]` give hand-computed ECE = |0.5−0.95|·0.5 + |1.0−0.55|·0.5 = 0.45.
  - `confident_wrong_rate`: wrong with top probability ≥ 0.8, divided by applicable rows.
  - `mae`: expected levels `[0,1,2]` against values `[0.5,1.0,1.0]` give 0.5.
  - `percentile`: p50 and p95 of 1..=100 ms are 50 and 95 (nearest-rank).
- [ ] **Step 2 — Implement** `pub struct Metrics { n_total, n_applicable, coverage, accuracy, ci_lo, ci_hi, ece, confident_wrong, error_rate, mae: Option<f64>, p50_ms, p95_ms }` with `pub fn compute(rows: &[ResultRow], seed: u64) -> Metrics`.
- [ ] **Step 3 — Green.** Mutate the confident threshold (0.8 → 0.9) and the ECE bin count, and confirm red. Commit as `feat(decision-bench): metrics — coverage, accuracy, seeded bootstrap CI, ECE, confident-wrong, MAE, p50/p95 (SP-DEC-4 T5)`.

### Task 6: ranking (AC5)

**Files:** `src/rank.rs`.

- [ ] **Step 1 — Red tests.**
  - `the_head_to_head_uses_only_cases_applicable_to_every_ranked_model`: model A answers every case. Model B is NotApplicable on case 3, so case 3 is excluded from both A's and B's head-to-head metrics.
  - `a_model_under_50_percent_coverage_is_reported_separately`: it is absent from the head-to-head and present in `low_coverage` with its own metrics.
  - `separated_cis_name_a_winner_and_overlapping_cis_tie`.
  - `ordering_is_accuracy_then_confident_wrong_then_p50`.
- [ ] **Step 2 — Implement** `pub struct ScenarioRanking { scenario, shared_cases: usize, ranked: Vec<(String, Metrics)>, verdict: Verdict, low_coverage: Vec<(String, Metrics)> }` and `pub enum Verdict { Winner(String), Tie(Vec<String>), Insufficient }`. Use `Insufficient` when fewer than 2 models are ranked or `shared_cases < 20`.
- [ ] **Step 3 — Green.** Mutate the shared-subset filter, the 0.5 floor, and the tie comparison (`>` → `>=`), and confirm red. Commit as `feat(decision-bench): shared-subset head-to-head, coverage floor, CI tie rule (SP-DEC-4 T6)`.

### Task 7: report + `check` + CLI wiring (AC1)

**Files:** `src/report.rs`, `src/main.rs`.

- [ ] **Step 1 — Red tests.**
  - `report` (snapshot-free, structural):
    - The rendered Markdown contains one section per scenario, a summary table with one column per model, the coverage column, the verdict line ("use X for …" or "tie: …"), and the dataset/licence/sha lines.
    - A `NOT RUN` model is listed at the top.
    - S4 is flagged "labels unreviewed" when the header's `labels_reviewed_by` is null.
  - `check`: given an unreachable URL, the router is reported `NotRun`, and `check` returns normally (no panic, no early exit).
- [ ] **Step 2 — Implement.**
  - `check` probes each model with one tiny decision (a noul on "x"). Its outcome is `Ok`, or `Error`/`NotApplicable` → `NotRun { reason }`.
  - `run` loads `bench.toml` (`--config`) and the cases (`--cases` dir and `scenarios/`), writes `results.json` (`--out`), and records models that are NOT RUN in a top-level `not_run` field.
  - `report --results results.json --out report.md`.
  - `results.json` is `{ "generated_at", "seed", "datasets": [{id, sha, license, split}], "not_run": [...], "rows": [...] }`.
- [ ] **Step 3 — Green, mutation, commit.** Mutation: drop the NOT RUN list from the report and confirm red. Commit as `feat(decision-bench): check / run / report commands and Markdown report (SP-DEC-4 T7)`.

### Task 8: wiremock end-to-end (AC6)

**Files:** `tests/e2e.rs`.

- [ ] **Step 1 — Test.**
  - Two wiremock routers, both registered under id `llamacpp` but configured as two models (`good`, `picky`) at different URLs:
    - `good` answers every case correctly.
    - `picky` returns a 200 with `usage.truncated: true` on cases whose state contains "LONG", which the gateway turns into `InputRejected`, hence `NotApplicable`.
  - A third model points at `http://127.0.0.1:9`, so it is `NOT RUN`.
  - 30 synthetic S3 cases, 5 of them "LONG". Run `run` and `report` in-process.
  - Assert:
    - `picky` coverage is 25/30;
    - the head-to-head has 25 shared cases;
    - the NOT RUN model is named;
    - the report has the expected verdict;
    - `results.json` parses with the documented fields.
- [ ] **Step 2 — Prove it.** Mutate `classify`, sending NotApplicable to Error; the coverage assertion must go red. Commit as `test(decision-bench): wiremock end-to-end run + report (SP-DEC-4 T8, AC6)`.

### Task 9: hand-curated S4 gate sets

**Files:** `crates/decision-bench/scenarios/s4-test-output.jsonl`, `s4-review-finding.jsonl`, `s4-fix-check.jsonl`.

- [ ] **Step 1 — Write the cases.** 60 cases per gate, balanced across labels, as realistic snippets: real `cargo test` / `pytest` / `jest` output shapes, compile errors, flaky timeouts, environment failures; review findings at blocking, follow-up and dismissible severity; finding+change pairs that do or don't fix the finding.
  - Keep each `state` under about 600 chars, so most models' windows apply and coverage stays meaningful.
  - Line 1 of each file is the header `{"labels_reviewed_by": null, "notes": "hand-curated 2026-10-<dd>; labels by the author; awaiting user review"}`.
- [ ] **Step 2 — Check.** A unit test loads all three files: every case validates, ids are unique, and each gate's label distribution is within ±20% of uniform.
- [ ] **Step 3 — Commit.** `feat(decision-bench): hand-curated S4 coding-gate cases (labels unreviewed) (SP-DEC-4 T9)`.

### Task 10: live run + committed report (AC7)

- [ ] **Step 1 — Preflight.** Check `df -h`, check `lsof -i :8766 -i :11434 -i :8091`, and confirm 8765 is not used.
- [ ] **Step 2 — Start the servers.**
  - Laya: `LAYA_HOST=127.0.0.1 LAYA_PORT=8766 LAYA_MODELS=english,multilingual,typed-decisions LAYA_EXTRA_MODELS='{"ane":"aac6fef/laya-multilingual-coreml-ane"}'` from `/tmp/laya-bench/env`.
  - llama.cpp: `~/opt/llamacpp-systemone/bin-b11381/llama-b11381/llama-server -m ~/opt/llamacpp-systemone/models/Bespoke-Nimble-9B-v3-Q4_K_M.gguf --alias nimble-v3 --port 8091 -c 8192`.
  - Ollama: already running.
  - Wait for each health check.
- [ ] **Step 3 — Run.** `cargo run -p decision-bench --release -- check`, then `fetch --per-scenario 200 --seed 2026`, `run`, and `report --out docs/benchmarks/2026-10-<dd>-system-one-local.md`. Record wall time.
- [ ] **Step 4 — Read the report critically.** If any metric looks implausible (e.g. 0% or 100% everywhere, or coverage 0), investigate before committing.
- [ ] **Step 5 — Stop the servers you started, then commit the report.** Commit as `docs(benchmarks): first local System One ranking (SP-DEC-4 T10, AC7)`.

### Task 11: docs + review + land (AC8)

- [ ] **Docs.** Write the crate README (config, commands, datasets and licences, how to read the report, S4 sign-off). Add a "Benchmarking System One models" section to `docs/llms/local.md`, then run `cd site && bun run check` and confirm the site copies are identical.
- [ ] **Verify.** Run, with real exit codes:
  - `cargo test --workspace`
  - stable clippy with `-D warnings`
  - fmt
  - `qlty check --upstream develop`, the local substitute for Qlty Cloud
- [ ] **Review.** Run the whole-slice review (`/sensei:review`), fix every finding red-first, and re-review the fixes.
- [ ] **Land.** Merge into develop, re-run the suite, push, checkpoint, and remove the worktree and target dir.
