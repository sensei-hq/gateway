# System One benchmark

Generated 2026-10-20 23:53:28 UTC.

## Roster

| Model | Router | API model id | Context window |
|---|---|---|---|
| laya-english | laya | english | 512 |
| laya-multilingual | laya | multilingual | 1024 |
| laya-typed | laya | typed-decisions | 1024 |
| laya-ane | laya | ane | 96 |
| nimble | ollama | nimble | 8192 |
| nimble-v3 | llamacpp | nimble-v3 | 8192 |

## NOT RUN

`check` could not run these models (unreachable, or no input fits their context window), so they have no results below:

- **laya-ane**: no input fits: the gateway estimates the minimal decision at 182 input tokens > the model's 96-token context window

## Datasets

| Dataset | Config / split | Licence | Revision sha | Rows sha256 |
|---|---|---|---|---|
| clinc/clinc_oos | plus / test | cc-by-3.0 | `155b9c710419136e17307b80d0a13e68cd46b4ec` | `b514ec6da101cb8a5beaa5ef036a018d4a5a9d08ea761b3c984bae3c4eb967f4` |
| legacy-datasets/banking77 | default / test | cc-by-4.0 | `f54121560de48f2852f90be299010d1d6dc612ec` | `39452c50042155115792548a48604d3d9bf8a21da8bcc6385be3d81da94b1edc` |
| Sp1786/multiclass-sentiment-analysis-dataset | default / test | apache-2.0 | `a7ede749d4b3e7ef516603cad80bd6b39b0a1fca` | `f62682c5c7eb95ee042140776c6b0a79997447b055b1851bc76416fc7fc00253` |
| stanfordnlp/snli | plain_text / test | cc-by-sa-4.0 | `cdb5c3d5eed6ead6e5a341c8e56e669bb666725b` | `ed6d975701a7ab3cd88d39f3efe70f78578a1d81a97ca44dd75b2ad93df9a312` |

Sampling: seed 2026, rand 0.8.7, 200 cases per scenario.

S4 cases are hand-curated: `crates/decision-bench/scenarios/s4-*.jsonl`.

Bootstrap CIs: 1000 resamples, seed 2026.

## Summary

Head-to-head accuracy on each scenario's shared cases, with the model's own coverage. `(own cases)`: below the 50% coverage floor, so accuracy is over its own applicable cases and it is not ranked. —: no rows.

| Scenario | laya-english | laya-multilingual | laya-typed | nimble | nimble-v3 |
|---|---|---|---|---|---|
| S1a | 0.920 (cov 100%) | 0.900 (cov 100%) | 0.910 (cov 100%) | 0.970 (cov 100%) | 0.935 (cov 100%) |
| S1b | 0.863 (cov 98%) | 0.822 (cov 100%) | 0.853 (cov 100%) | 0.949 (cov 100%) | 0.964 (cov 100%) |
| S2 | 0.550 (cov 100%) | 0.485 (cov 100%) | 0.620 (cov 100%) | 0.615 (cov 100%) | 0.630 (cov 100%) |
| S3 | 0.490 (cov 100%) | 0.535 (cov 100%) | 0.480 (cov 100%) | 0.985 (cov 100%) | 0.970 (cov 100%) |
| S4 test-output | — (cov 0%, own cases) | 0.483 (cov 100%) | 0.467 (cov 100%) | 0.983 (cov 100%) | 0.983 (cov 100%) |
| S4 review-finding | — (cov 0%, own cases) | 0.300 (cov 100%) | 0.517 (cov 100%) | 1.000 (cov 100%) | 0.950 (cov 100%) |
| S4 fix-check | 0.533 (cov 100%) | 0.450 (cov 100%) | 0.533 (cov 100%) | 0.717 (cov 100%) | 0.817 (cov 100%) |

## S1a — intent routing with an out-of-scope option (choice of 9)

Shared cases: 200 (applicable to every ranked model). Metrics are over the shared cases; coverage and not-reached are over the model's own cases.

| Model | Coverage | Accuracy | 95% CI | ECE | Confident-wrong | Error rate | Not reached | MAE | p50 ms | p95 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| nimble | 100% | 0.970 | [0.945, 0.990] | 0.01 | 0.01 | 0.00 | 0 | — | 462 | 511 |
| nimble-v3 | 100% | 0.935 | [0.900, 0.965] | 0.03 | 0.03 | 0.00 | 0 | — | 423 | 530 |
| laya-english | 100% | 0.920 | [0.880, 0.955] | 0.06 | 0.03 | 0.00 | 0 | — | 144 | 257 |
| laya-typed | 100% | 0.910 | [0.870, 0.950] | 0.16 | 0.01 | 0.00 | 0 | — | 147 | 210 |
| laya-multilingual | 100% | 0.900 | [0.855, 0.940] | 0.08 | 0.04 | 0.00 | 0 | — | 53 | 85 |

**Tie: nimble, nimble-v3, laya-english, laya-typed**: not statistically separable (their 95% CIs reach the top model's).

## S1b — tool / intent routing (choice of 8)

Shared cases: 197 (applicable to every ranked model). Metrics are over the shared cases; coverage and not-reached are over the model's own cases.

| Model | Coverage | Accuracy | 95% CI | ECE | Confident-wrong | Error rate | Not reached | MAE | p50 ms | p95 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| nimble-v3 | 100% | 0.964 | [0.939, 0.990] | 0.04 | 0.02 | 0.00 | 0 | — | 419 | 487 |
| nimble | 100% | 0.949 | [0.919, 0.980] | 0.03 | 0.03 | 0.00 | 0 | — | 532 | 618 |
| laya-english | 98% | 0.863 | [0.812, 0.909] | 0.07 | 0.06 | 0.00 | 0 | — | 199 | 290 |
| laya-typed | 100% | 0.853 | [0.802, 0.904] | 0.16 | 0.00 | 0.00 | 0 | — | 204 | 308 |
| laya-multilingual | 100% | 0.822 | [0.761, 0.878] | 0.10 | 0.08 | 0.00 | 0 | — | 79 | 105 |

Most common not-applicable reason:

- laya-english: 1 of 3 not applicable: adapter 'laya' cannot serve this input: question 'q' needs ≈193 tokens of head but checkpoint 'english' allows 192 — Laya would silently cut it; shorten the ins…

**Tie: nimble-v3, nimble**: not statistically separable (their 95% CIs reach the top model's).

## S2 — ordered triage score (3 levels)

Shared cases: 200 (applicable to every ranked model). Metrics are over the shared cases; coverage and not-reached are over the model's own cases.

| Model | Coverage | Accuracy | 95% CI | ECE | Confident-wrong | Error rate | Not reached | MAE | p50 ms | p95 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| nimble-v3 | 100% | 0.630 | [0.560, 0.700] | 0.19 | 0.17 | 0.00 | 0 | 0.43 | 338 | 439 |
| laya-typed | 100% | 0.620 | [0.550, 0.690] | 0.09 | 0.05 | 0.00 | 0 | 0.52 | 174 | 427 |
| nimble | 100% | 0.615 | [0.545, 0.680] | 0.27 | 0.24 | 0.00 | 0 | 0.45 | 444 | 571 |
| laya-english | 100% | 0.550 | [0.480, 0.615] | 0.19 | 0.15 | 0.00 | 0 | 0.56 | 164 | 406 |
| laya-multilingual | 100% | 0.485 | [0.410, 0.550] | 0.31 | 0.25 | 0.00 | 0 | 0.60 | 54 | 108 |

**Tie: nimble-v3, laya-typed, nimble, laya-english**: not statistically separable (their 95% CIs reach the top model's).

## S3 — does B follow from A? (yes / no)

Shared cases: 200 (applicable to every ranked model). Metrics are over the shared cases; coverage and not-reached are over the model's own cases.

| Model | Coverage | Accuracy | 95% CI | ECE | Confident-wrong | Error rate | Not reached | MAE | p50 ms | p95 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| nimble | 100% | 0.985 | [0.965, 1.000] | 0.02 | 0.01 | 0.00 | 0 | — | 383 | 490 |
| nimble-v3 | 100% | 0.970 | [0.945, 0.990] | 0.02 | 0.02 | 0.00 | 0 | — | 345 | 454 |
| laya-multilingual | 100% | 0.535 | [0.465, 0.605] | 0.37 | 0.38 | 0.00 | 0 | — | 75 | 86 |
| laya-english | 100% | 0.490 | [0.425, 0.560] | 0.45 | 0.47 | 0.00 | 0 | — | 207 | 507 |
| laya-typed | 100% | 0.480 | [0.415, 0.550] | 0.36 | 0.38 | 0.00 | 0 | — | 222 | 288 |

**Tie: nimble, nimble-v3**: not statistically separable (their 95% CIs reach the top model's).

## S4 test-output — test output → continue / fix_code / retry / fix_environment

> **labels unreviewed**: nobody has signed off these hand-curated labels yet, so the results below are provisional.

Shared cases: 60 (applicable to every ranked model). Metrics are over the shared cases; coverage and not-reached are over the model's own cases.

| Model | Coverage | Accuracy | 95% CI | ECE | Confident-wrong | Error rate | Not reached | MAE | p50 ms | p95 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| nimble-v3 | 100% | 0.983 | [0.950, 1.000] | 0.02 | 0.00 | 0.00 | 0 | — | 611 | 865 |
| nimble | 100% | 0.983 | [0.950, 1.000] | 0.05 | 0.00 | 0.00 | 0 | — | 679 | 862 |
| laya-multilingual | 100% | 0.483 | [0.350, 0.600] | 0.17 | 0.08 | 0.00 | 0 | — | 225 | 359 |
| laya-typed | 100% | 0.467 | [0.333, 0.583] | 0.13 | 0.00 | 0.00 | 0 | — | 665 | 1910 |

**Tie: nimble-v3, nimble**: not statistically separable (their 95% CIs reach the top model's).

### Below the 50% coverage floor (own cases)

| Model | Coverage | Accuracy | 95% CI | ECE | Confident-wrong | Error rate | Not reached | MAE | p50 ms | p95 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| laya-english | 0% | — | — | — | — | — | 0 | — | — | — |

Most common not-applicable reason:

- laya-english: 60 not applicable: adapter 'laya' cannot serve this input: the options of question 'q' need ≈182 tokens but checkpoint 'english' leaves options 176 of its 192 — the options would …

## S4 review-finding — review finding → block / follow_up / dismiss

> **labels unreviewed**: nobody has signed off these hand-curated labels yet, so the results below are provisional.

Shared cases: 60 (applicable to every ranked model). Metrics are over the shared cases; coverage and not-reached are over the model's own cases.

| Model | Coverage | Accuracy | 95% CI | ECE | Confident-wrong | Error rate | Not reached | MAE | p50 ms | p95 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| nimble | 100% | 1.000 | [1.000, 1.000] | 0.04 | 0.00 | 0.00 | 0 | — | 517 | 564 |
| nimble-v3 | 100% | 0.950 | [0.883, 1.000] | 0.05 | 0.00 | 0.00 | 0 | — | 451 | 498 |
| laya-typed | 100% | 0.517 | [0.383, 0.633] | 0.06 | 0.00 | 0.00 | 0 | — | 390 | 476 |
| laya-multilingual | 100% | 0.300 | [0.183, 0.417] | 0.26 | 0.05 | 0.00 | 0 | — | 143 | 160 |

**Tie: nimble, nimble-v3**: not statistically separable (their 95% CIs reach the top model's).

### Below the 50% coverage floor (own cases)

| Model | Coverage | Accuracy | 95% CI | ECE | Confident-wrong | Error rate | Not reached | MAE | p50 ms | p95 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| laya-english | 0% | — | — | — | — | — | 0 | — | — | — |

Most common not-applicable reason:

- laya-english: 60 not applicable: adapter 'laya' cannot serve this input: question 'q' needs ≈207 tokens of head but checkpoint 'english' allows 192 — Laya would silently cut it; shorten the ins…

## S4 fix-check — does the change fix the finding? (yes / no)

> **labels unreviewed**: nobody has signed off these hand-curated labels yet, so the results below are provisional.

Shared cases: 60 (applicable to every ranked model). Metrics are over the shared cases; coverage and not-reached are over the model's own cases.

| Model | Coverage | Accuracy | 95% CI | ECE | Confident-wrong | Error rate | Not reached | MAE | p50 ms | p95 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| nimble-v3 | 100% | 0.817 | [0.717, 0.917] | 0.10 | 0.13 | 0.00 | 0 | — | 366 | 412 |
| nimble | 100% | 0.717 | [0.600, 0.833] | 0.22 | 0.22 | 0.00 | 0 | — | 520 | 705 |
| laya-typed | 100% | 0.533 | [0.400, 0.667] | 0.09 | 0.02 | 0.00 | 0 | — | 369 | 573 |
| laya-english | 100% | 0.533 | [0.400, 0.667] | 0.21 | 0.13 | 0.00 | 0 | — | 402 | 513 |
| laya-multilingual | 100% | 0.450 | [0.333, 0.567] | 0.37 | 0.27 | 0.00 | 0 | — | 132 | 140 |

**Tie: nimble-v3, nimble**: not statistically separable (their 95% CIs reach the top model's).

## Label review

- S4 test-output: **labels unreviewed**; results are provisional until `labels_reviewed_by` is set in the file's header line.
- S4 review-finding: **labels unreviewed**; results are provisional until `labels_reviewed_by` is set in the file's header line.
- S4 fix-check: **labels unreviewed**; results are provisional until `labels_reviewed_by` is set in the file's header line.

## Reproduce

From the repo root (relative paths resolve under `crates/decision-bench/`):

```sh
cargo run --release -p decision-bench -- check --config bench.example.toml --models laya-english,laya-multilingual,laya-typed,laya-ane,nimble,nimble-v3
cargo run --release -p decision-bench -- fetch --per-scenario 200 --seed 2026 --out cases
cargo run --release -p decision-bench -- run --config bench.example.toml --cases cases --scenarios s1a,s1b,s2,s3,s4-test-output,s4-review-finding,s4-fix-check --models laya-english,laya-multilingual,laya-typed,laya-ane,nimble,nimble-v3 --out results.json
cargo run --release -p decision-bench -- report --results results.json --out ../../docs/benchmarks/2026-10-20-system-one-local.md --seed 2026
```

`fetch` rebuilds byte-identical cases only while each dataset is still at the revision sha above; compare the rows sha256 in the cases' `manifest.json`.
