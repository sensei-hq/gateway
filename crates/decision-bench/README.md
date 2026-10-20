# decision-bench

Benchmarks System One decision models (Laya checkpoints, Bespoke-Nimble builds) through the
sensei gateway, scenario by scenario, and renders a per-scenario ranking report. It answers
"which model for which kind of decision?" with evidence: accuracy with a confidence interval,
calibration, and how often a model is confidently wrong.

It is a measurement tool only. It does **not** route by score: nothing here changes how the
gateway picks a model. `results.json` keeps stable field names so a later slice could consume it.

`publish = false`; one binary, `decision-bench`. It goes through the gateway facade
(`FacadeBuilder` then `Gateway::execute`), with a single-entry decision chain and no fallback per
model, so every result belongs to exactly one model and includes the gateway's own alias and
fit rules.

## Roster

First run: local, free models. Hosted routers (TypeSafe, OpenRouter, Cloudflare) are
config-only and not run: they need keys and cost money.

| Model id | Router | Served by | Window |
|---|---|---|---|
| `laya-english` | `laya` | `laya-coreml-serve`, checkpoint `english` | 512 |
| `laya-multilingual` | `laya` | checkpoint `multilingual` | 1024 |
| `laya-typed` | `laya` | checkpoint `typed-decisions` | 1024 |
| `laya-ane` | `laya` | `LAYA_EXTRA_MODELS={"ane":"aac6fef/laya-multilingual-coreml-ane"}` | 96 (NOT RUN, see Known limitations) |
| `nimble` | `ollama` | Ollama 0.40, `nimble` | 8192 |
| `nimble-v3` | `llamacpp` | `llama-server` b11381, `Bespoke-Nimble-9B-v3-Q4_K_M.gguf` | 8192 |

### Starting each server

Verified on the author's machine. Adjust paths to yours.

**Laya**: one server for `english`, `multilingual`, `typed-decisions` and `ane`. The venv at
`/tmp/laya-bench/env` is a scratch venv that a reboot wipes. Recreate it:

```sh
uv venv -p 3.12 /tmp/laya-bench/env/.venv && \
VIRTUAL_ENV=/tmp/laya-bench/env/.venv uv pip install "laya-coreml[serve]==0.5.1"
```

Port 8765 is taken on that machine, so 8766:

```sh
LAYA_HOST=127.0.0.1 LAYA_PORT=8766 \
LAYA_MODELS=english,multilingual,typed-decisions \
LAYA_EXTRA_MODELS='{"ane":"aac6fef/laya-multilingual-coreml-ane"}' \
/tmp/laya-bench/env/.venv/bin/laya-coreml-serve
```

**Ollama** (verified on 0.40). Pull the model once with `ollama pull nimble`, then:

```sh
ollama serve
```

`ollama show nimble` reports Bespoke-Nimble-9B **Q8_0** (capability `decision`). That is NOT the
same build as `nimble-v3` (llama.cpp, Q4_K_M, v3), so the roster compares two quantisations of a
similar 9B model, not one model on two servers.

**llama.cpp** b11381:

```sh
~/opt/llamacpp-systemone/bin-b11381/llama-b11381/llama-server \
  -m ~/opt/llamacpp-systemone/models/Bespoke-Nimble-9B-v3-Q4_K_M.gguf \
  --alias nimble-v3 --port 8091 -c 8192
```

## Config

`bench.example.toml` is committed and holds the roster above (`[[models]]` with `id`, `router`,
`url`, `api_model_id`, `context_window`). Copy it to `bench.toml` to change URLs, ports or the
roster; `bench.toml` is gitignored. With no `--config`, the bench reads `bench.toml` and falls
back to `bench.example.toml`.

## Commands

Run from the repo root as `cargo run --release -p decision-bench -- <command>`. A relative
path argument resolves under `crates/decision-bench/`, not the current directory, so the same
command works from anywhere.

| Command | What it does | Flags (defaults) |
|---|---|---|
| `check` | Probes every selected model with one tiny decision. A model that does not answer, or that no input fits its context window, is **NOT RUN**; `check` still exits 0. | `--config <path>`, `--models <a,b>` |
| `fetch` | Downloads the public datasets and builds the seeded S1a/S1b/S2/S3 case files plus `manifest.json`. | `--per-scenario <n>` (200), `--seed <n>` (2026), `--out <dir>` (`cases`), `--cache-dir <dir>` (user cache dir, `decision-bench/`) |
| `run` | Runs `check`, then every reachable model over the cases, sequentially after one warm-up call per model. Rewrites the results file atomically after each model. | `--config`, `--models`, `--cases <dir>` (`cases`), `--scenarios <list>` (all found), `--out <file>` (`results.json`) |
| `report` | Renders the Markdown ranking report from a results file. | `--results <file>` (`results.json`), `--out <file>` (`report.md`), `--seed <n>` (the manifest's sampling seed, else 2026) |

`--scenarios` takes a comma-separated list of `s1a`, `s1b`, `s2`, `s3`, `s4` (every S4 gate),
`s4-test-output`, `s4-review-finding`, `s4-fix-check`. `--models` takes model ids from the
config. The report ends with a **Reproduce** section whose commands replay the run, quoted
so they can be pasted into a shell.

Typical session:

```sh
cargo run --release -p decision-bench -- check
cargo run --release -p decision-bench -- fetch --per-scenario 40 --seed 2026
cargo run --release -p decision-bench -- run
cargo run --release -p decision-bench -- report
```

## Where things live

- **Cases**: `crates/decision-bench/cases/` (`s1a.jsonl`, `s1b.jsonl`, `s2.jsonl`, `s3.jsonl`,
  `manifest.json`). `fetch` replaces the directory as a whole.
- **Raw-row cache**: `<user cache dir>/decision-bench/<dataset>@<sha>/`, outside the repo (override
  with `--cache-dir`). `fetch` records the hub revision sha and refuses to reuse a cache built
  from a different one.
- **Results**: `crates/decision-bench/results.json`; **report**: `report.md` by default.
- **S4 cases**: `crates/decision-bench/scenarios/s4-*.jsonl`, hand-curated and committed.

Everything under `crates/decision-bench/` except `src/`, `tests/` and `scenarios/` is
gitignored (plus `bench.toml`, `results*.json` and `report*.md`), so fetched public data cannot
be committed by accident. A report worth keeping is committed by passing `--out` under
`docs/benchmarks/`.

## Datasets

Public data is fetched at run time and never committed. The report names each dataset, its
licence and the revision sha used; `fetch` fails if the hub card's licence is not the one
expected below.

| Scenario | What is asked | Dataset (HF id) | Licence |
|---|---|---|---|
| S1a | Intent routing, 9 options per case (`oos` plus 8 intents), 1-in-9 baseline | `clinc/clinc_oos` (config `plus`) | CC-BY-3.0 |
| S1b | Tool/intent routing, 8 candidates per case from 77 intents | `legacy-datasets/banking77` (the `PolyAI/` copy is not served) | CC-BY-4.0 |
| S2 | Ordered triage score: negative / neutral / positive | `Sp1786/multiclass-sentiment-analysis-dataset` | Apache-2.0 |
| S3 | Does B follow from A? (entailment is yes, contradiction is no; neutral excluded) | `stanfordnlp/snli` | CC-BY-SA-4.0 |
| S4 | Coding-pipeline gates: test output (continue / fix_code / retry / fix_environment), review finding (block / follow_up / dismiss), "does the change fix the finding?" | hand-curated | this repo |

Rows come from the `datasets-server.huggingface.co` `/rows` endpoint. Sampling is seeded, and
each dataset's rows sha256 goes in `manifest.json`, so `fetch` rebuilds byte-identical cases only
while each dataset is still at the recorded revision sha. Limitation: datasets-server can lag a
hub push, so for a short window it may serve rows of the previous revision under the new sha; the
rows sha256 makes that drift visible on re-fetch.

## S4 label sign-off

Each `scenarios/s4-*.jsonl` file starts with a header line:

```json
{"labels_reviewed_by": null, "notes": "..."}
```

The labels are the author's draft. Until a human reviews them, `labels_reviewed_by` is `null`,
and the report marks that scenario **labels unreviewed** and its results provisional. To sign
off: review every case in the file, then set `labels_reviewed_by` to your name in that file's
header line. Re-run `run` (or at least `report` after a `run`) so the results file picks it up;
the report's "Label review" section then says "reviewed by <name>". Each S4 file is signed off
separately.

## Reading the report

The report has these sections: Roster, NOT RUN (when any), Datasets, Summary, one ranked section
per scenario, Label review (S4 only), Rows that never reached the server (when any), and
Reproduce.

- **Outcome per case**: `ok` (answered), `not_applicable` (the input does not fit this model),
  or `error` (anything else, including network errors, 4xx/5xx and a re-routed checkpoint).
  Nothing is retried or dropped.
- **Coverage**: the share of a scenario's cases that are applicable, i.e. not `not_applicable`.
- **`not_applicable`**: not wrong, just not gradable for that model. It covers the gateway's
  context-window refusal and any `InputRejected` (Laya head budget, images, server-side
  truncation, collapsed options). Under each table, every model with `not_applicable` rows is
  listed with its most common reason and how many rows have it.
- **Accuracy**: over the applicable cases, with `error` counted as wrong (the error rate is
  shown too). The 95% CI is a bootstrap over 1000 seeded resamples; noul answers are decided at 0.5.
- **The shared subset**: the head-to-head ranking uses only the cases applicable to every ranked
  model, so no model is graded on an easier subset. If no case is shared, the section says so
  instead of printing a table.
- **The 50% coverage floor**: a model with coverage below 50% is left out of the head-to-head and
  gets its own table over its own applicable cases. The Summary marks these `(own cases)`.
- **Winner vs Tie**: models are ordered by accuracy, then lower confident-wrong rate, then lower
  p50 latency, but the verdict does not depend on that tiebreak. A **Winner** ("Use X for
  Sn") must be separated from every head-to-head model: its CI lower bound exceeds every other
  ranked model's CI upper bound. Otherwise the report says **Tie** and lists the top model and
  each model whose CI overlaps it. Winner and Tie are exact complements.
- **Confident-wrong**: the share of answers that were wrong with a top probability of at least
  0.8. High accuracy with a high confident-wrong rate means confidence cannot be trusted.
- **ECE**: expected calibration error of the top probability, in 10 equal-width bins. Lower is
  better calibrated.
- **Score scenarios (S2)** also report exact-level accuracy (argmax) and the mean absolute error
  of the expected level. **Latency** is p50/p95 in ms.
- **"Rows that never reached the server"**: per model and scenario, rows refused before any
  request was sent (a gateway fit refusal or an error before the call) and rows whose transport
  failed before any reply (a refused or dropped connection, a DNS failure), with their outcomes.
  A server-side truncation, and a case that ran out the router timeout, do reach the server and
  are not listed here.
- **INCOMPLETE RUN**: a banner naming roster models that neither finished nor were NOT RUN, i.e.
  the run stopped early. Their rows are partial, so this is not a complete ranking.
- **NOT RUN**: models `check` could not run (unreachable, or no input fits their context
  window), with the reason. They have no results; they are named at the top of the report,
  never silently skipped.

## Known limitations

- **`laya-ane` is NOT RUN.** The gateway's decision-size estimate puts even the minimal decision
  at about 182 input tokens, which exceeds that checkpoint's 96-token window, so the request is
  refused before sending and nothing can ever be graded. It stays in the roster so the report
  says so. Follow-up (gateway): make the decision prompt-overhead estimate model-aware.
- Hosted models (config-ready, but they need keys and a spend cap), routing by benchmark score,
  calibration fitting, multi-question cases and a CI job for the bench (it needs model servers)
  are out of scope.
- A case is judged by one question; there is no multi-turn or multi-question scoring.
- `InputRejected` is detected by the kernel's Display prefix (`adapter '...' cannot serve this
  input:`) on the attempt's error string. A unit test pins it against the real type, so a format
  change fails tests instead of silently misclassifying.
- The S4 sets are 50 to 100 cases per gate; their intervals are wide, and until signed off they
  are provisional.
