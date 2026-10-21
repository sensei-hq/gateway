use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use decision_bench::check::{CheckResult, CheckStatus, check};
use decision_bench::config::{BenchConfig, resolve_under};
use decision_bench::fetch::{Hf, default_cache_root};
use decision_bench::load::{load, parse_scenarios};
use decision_bench::report::{cli_name, render};
use decision_bench::run::{NotRun, Results, RosterEntry, RunPaths, run_saving};
use decision_bench::scenarios::{FetchOptions, fetch_cases};

#[derive(Parser)]
#[command(
    name = "decision-bench",
    about = "Benchmark System One decision models"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Probe every selected model with one tiny decision. A model that does
    /// not answer, or that no input fits its context window, is NOT RUN;
    /// this still exits 0.
    #[command(after_long_help = CHECK_NOTE)]
    Check {
        #[command(flatten)]
        select: Select,
    },
    /// Fetch the public datasets and build the seeded S1a/S1b/S2/S3 case files.
    #[command(after_long_help = PATH_NOTE)]
    Fetch {
        /// Cases sampled per scenario.
        #[arg(long, default_value_t = 200)]
        per_scenario: usize,
        /// Sampling seed.
        #[arg(long, default_value_t = 2026)]
        seed: u64,
        /// Output directory for s1a/s1b/s2/s3.jsonl and manifest.json, replaced
        /// as a whole (relative: see below).
        #[arg(long, default_value = "cases")]
        out: PathBuf,
        /// Raw-row cache (default: <user cache dir>/decision-bench; relative:
        /// see below).
        #[arg(long)]
        cache_dir: Option<PathBuf>,
    },
    /// Run the benchmark: `check`, then every reachable model over the cases,
    /// rewriting the results file after each model.
    #[command(after_long_help = RUN_NOTE)]
    Run {
        #[command(flatten)]
        select: Select,
        /// Directory of fetched case files (`*.jsonl` + `manifest.json`).
        #[arg(long, default_value = "cases")]
        cases: PathBuf,
        /// Comma-separated scenarios: s1a, s1b, s2, s3, s4 (every gate),
        /// s4-test-output, s4-review-finding, s4-fix-check. Default: all found.
        #[arg(long)]
        scenarios: Option<String>,
        /// Results file, rewritten atomically after each model.
        #[arg(long, default_value = "results.json")]
        out: PathBuf,
    },
    /// Render the per-scenario ranking report from a results file.
    #[command(after_long_help = REPORT_NOTE)]
    Report {
        #[arg(long, default_value = "results.json")]
        results: PathBuf,
        #[arg(long, default_value = "report.md")]
        out: PathBuf,
        /// Bootstrap seed for the CIs (default: the manifest's sampling seed,
        /// else 2026).
        #[arg(long)]
        seed: Option<u64>,
    },
}

/// Which config, and which of its models.
#[derive(clap::Args, Debug, Clone, PartialEq)]
struct Select {
    /// Bench config (default: bench.toml, else bench.example.toml).
    #[arg(long)]
    config: Option<PathBuf>,
    /// Comma-separated model ids (default: every configured model).
    #[arg(long)]
    models: Option<String>,
}

/// Where a relative path is anchored, so `fetch` writes to the same place
/// whatever directory it is run from.
const PATH_NOTE: &str = concat!(
    "Paths: relative paths resolve under the decision-bench crate directory, ",
    env!("CARGO_MANIFEST_DIR"),
    ", not the current directory; so the default --out is ",
    env!("CARGO_MANIFEST_DIR"),
    "/cases. Every subdirectory there except src/, tests/ and scenarios/ is \
     gitignored, so fetched data cannot be committed by accident."
);

/// Path anchoring for `check`: only `--config` is a path.
const CHECK_NOTE: &str = concat!(
    "Paths: a relative --config path resolves under the decision-bench crate directory, ",
    env!("CARGO_MANIFEST_DIR"),
    ", not the current directory."
);

/// Path anchoring for `run`.
const RUN_NOTE: &str = concat!(
    "Paths: relative --config, --cases and --out paths resolve under the decision-bench \
     crate directory, ",
    env!("CARGO_MANIFEST_DIR"),
    ", not the current directory. Hand-curated S4 cases are read from its scenarios/ \
     subdirectory (s4-*.jsonl; none is fine)."
);

/// Path anchoring for `report`.
const REPORT_NOTE: &str = concat!(
    "Paths: relative --results and --out paths resolve under the decision-bench crate \
     directory, ",
    env!("CARGO_MANIFEST_DIR"),
    ", not the current directory; so a report to commit takes \
     --out ../../docs/benchmarks/<date>-system-one-local.md (or an absolute path)."
);

/// `p` if absolute, else `p` under the crate directory (see [`PATH_NOTE`]).
fn resolve(p: &Path) -> PathBuf {
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        Path::new(env!("CARGO_MANIFEST_DIR")).join(p)
    }
}

async fn fetch(
    per_scenario: usize,
    seed: u64,
    out: PathBuf,
    cache_dir: Option<PathBuf>,
) -> anyhow::Result<()> {
    let cache_root = match cache_dir {
        Some(d) => resolve(&d),
        None => default_cache_root()?,
    };
    let out = resolve(&out);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .user_agent(concat!("decision-bench/", env!("CARGO_PKG_VERSION")))
        .build()?;
    let manifest = fetch_cases(
        &Hf::live(client),
        &FetchOptions {
            cache_root: &cache_root,
            out: &out,
            seed,
            per_scenario,
        },
    )
    .await?;
    for d in &manifest.datasets {
        println!(
            "{} {}/{}  sha {}  licence {}  rows {}",
            d.id, d.config, d.split, d.sha, d.license, d.num_rows
        );
    }
    for f in &manifest.files {
        println!("{}  {} cases", out.join(&f.file).display(), f.cases);
    }
    println!(
        "seed {}  rand {}  cache {}",
        manifest.seed,
        manifest.rand_version,
        cache_root.display()
    );
    Ok(())
}

/// The ids named by `--models`, in config order; every configured model when
/// `None`. An id not in the config is an error.
fn select_models(cfg: &BenchConfig, models: Option<&str>) -> anyhow::Result<Vec<String>> {
    let Some(list) = models else {
        return Ok(cfg.models.iter().map(|m| m.id.clone()).collect());
    };
    let named: Vec<&str> = list
        .split(',')
        .map(str::trim)
        .filter(|n| !n.is_empty())
        .collect();
    if named.is_empty() {
        anyhow::bail!("--models names no model");
    }
    if let Some(unknown) = named
        .iter()
        .find(|n| !cfg.models.iter().any(|m| m.id == **n))
    {
        anyhow::bail!(
            "unknown model '{unknown}': not in the bench config (configured: {})",
            cfg.models
                .iter()
                .map(|m| m.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Ok(cfg
        .models
        .iter()
        .filter(|m| named.contains(&m.id.as_str()))
        .map(|m| m.id.clone())
        .collect())
}

/// `p` relative to `crate_dir` when under it, else as given.
fn rel(p: &Path, crate_dir: &Path) -> String {
    p.strip_prefix(crate_dir).unwrap_or(p).display().to_string()
}

/// Load the config named by `select` (printing a note when the example is
/// used) and the selected model ids.
fn load_config(
    select: &Select,
    crate_dir: &Path,
) -> anyhow::Result<(BenchConfig, Vec<String>, PathBuf)> {
    let loaded = BenchConfig::load(select.config.as_deref(), crate_dir)?;
    if loaded.is_example {
        eprintln!(
            "decision-bench: no bench.toml; using {} (copy it to bench.toml to edit)",
            loaded.path.display()
        );
    }
    let ids = select_models(&loaded.config, select.models.as_deref())?;
    Ok((loaded.config, ids, loaded.path))
}

/// The `check` table: one line per model.
fn check_table(cfg: &BenchConfig, results: &[CheckResult]) -> String {
    let router = |id: &str| {
        cfg.models
            .iter()
            .find(|m| m.id == id)
            .map_or("?", |m| m.router.as_str())
    };
    let width = results
        .iter()
        .map(|r| r.model.len())
        .chain([5])
        .max()
        .unwrap_or(5);
    let mut out = format!("{:width$}  {:10}  STATUS\n", "MODEL", "ROUTER");
    for r in results {
        let status = match &r.status {
            CheckStatus::Reachable => "reachable".to_string(),
            CheckStatus::NotRun { reason } => format!("NOT RUN: {}", reason.replace('\n', " ")),
        };
        out += &format!("{:width$}  {:10}  {status}\n", r.model, router(&r.model));
    }
    out
}

/// `check`: probe and print. A NOT RUN model is reported, never an error.
async fn check_command(select: &Select, crate_dir: &Path) -> anyhow::Result<Vec<CheckResult>> {
    let (cfg, ids, _) = load_config(select, crate_dir)?;
    let results = check(&cfg, &ids).await;
    print!("{}", check_table(&cfg, &results));
    Ok(results)
}

/// `run`: load cases, `check`, then run every reachable model, rewriting `out`
/// after each.
async fn run_command(
    select: &Select,
    cases: &Path,
    scenarios: Option<&str>,
    out: &Path,
    crate_dir: &Path,
) -> anyhow::Result<Results> {
    let (cfg, ids, config_path) = load_config(select, crate_dir)?;
    let only = scenarios.map(parse_scenarios).transpose()?;
    let cases_dir = resolve_under(cases, crate_dir);
    let loaded = load(&cases_dir, &crate_dir.join("scenarios"), only.as_deref())?;
    for s in &loaded.missing {
        eprintln!("decision-bench: no cases for {}; skipped", cli_name(*s));
    }
    if loaded.manifest.is_none() {
        eprintln!(
            "decision-bench: no manifest.json in {}; the report will not name dataset revisions",
            cases_dir.display()
        );
    }
    let checked = check(&cfg, &ids).await;
    eprint!("{}", check_table(&cfg, &checked));
    let not_run: Vec<NotRun> = checked
        .iter()
        .filter_map(|c| match &c.status {
            CheckStatus::NotRun { reason } => Some(NotRun {
                model: c.model.clone(),
                reason: reason.clone(),
            }),
            CheckStatus::Reachable => None,
        })
        .collect();
    let reachable: Vec<String> = ids
        .iter()
        .filter(|id| !not_run.iter().any(|n| &n.model == *id))
        .cloned()
        .collect();
    let results = Results {
        generated_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs()),
        manifest: loaded.manifest,
        not_run,
        roster: cfg
            .models
            .iter()
            .filter(|m| ids.contains(&m.id))
            .map(RosterEntry::from)
            .collect(),
        labels_reviewed: loaded.labels_reviewed,
        completed: vec![],
        paths: Some(RunPaths {
            config: Some(rel(&config_path, crate_dir)),
            cases: rel(&cases_dir, crate_dir),
            results: rel(&resolve_under(out, crate_dir), crate_dir),
            report: None,
        }),
        rows: vec![],
    };
    let out = resolve_under(out, crate_dir);
    let results = run_saving(&cfg, &loaded.cases, &reachable, results, &out).await?;
    eprintln!(
        "decision-bench: wrote {} ({} rows); render it with `decision-bench report`",
        out.display(),
        results.rows.len()
    );
    Ok(results)
}

/// `report`: render `results` to `out`; returns the path written.
fn report_command(
    results: &Path,
    out: &Path,
    seed: Option<u64>,
    crate_dir: &Path,
) -> anyhow::Result<PathBuf> {
    let path = resolve_under(results, crate_dir);
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("reading results {}", path.display()))?;
    let mut results: Results =
        serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    let seed = seed
        .or(results.manifest.as_ref().map(|m| m.seed))
        .unwrap_or(DEFAULT_SEED);
    let out = resolve_under(out, crate_dir);
    if let Some(p) = results.paths.as_mut() {
        p.report = Some(rel(&out, crate_dir));
    }
    decision_bench::fetch::write_atomic(&out, render(&results, seed).as_bytes())
        .with_context(|| format!("writing {}", out.display()))?;
    eprintln!("decision-bench: wrote {}", out.display());
    Ok(out)
}

/// The bootstrap seed when neither `--seed` nor a manifest gives one.
const DEFAULT_SEED: u64 = 2026;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    match Cli::parse().command {
        Command::Fetch {
            per_scenario,
            seed,
            out,
            cache_dir,
        } => fetch(per_scenario, seed, out, cache_dir).await,
        // NOT RUN models are a result, not a failure: exit 0.
        Command::Check { select } => check_command(&select, crate_dir).await.map(|_| ()),
        Command::Run {
            select,
            cases,
            scenarios,
            out,
        } => run_command(&select, &cases, scenarios.as_deref(), &out, crate_dir)
            .await
            .map(|_| ()),
        Command::Report { results, out, seed } => {
            report_command(&results, &out, seed, crate_dir).map(|_| ())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use decision_bench::config::ModelSpec;

    const CRATE: &str = env!("CARGO_MANIFEST_DIR");

    /// Split like a POSIX shell for the one quoting form the footer emits:
    /// whitespace separates words, `'...'` is literal, `\'` is a bare quote.
    fn shell_split(line: &str) -> Vec<String> {
        let (mut words, mut cur, mut in_word, mut quoted) =
            (Vec::new(), String::new(), false, false);
        let mut chars = line.chars();
        while let Some(c) = chars.next() {
            match (quoted, c) {
                (true, '\'') => quoted = false,
                (true, c) => cur.push(c),
                (false, '\'') => {
                    quoted = true;
                    in_word = true;
                }
                (false, '\\') => {
                    cur.push(chars.next().expect("dangling backslash"));
                    in_word = true;
                }
                (false, c) if c.is_whitespace() => {
                    if in_word {
                        words.push(std::mem::take(&mut cur));
                        in_word = false;
                    }
                }
                (false, c) => {
                    cur.push(c);
                    in_word = true;
                }
            }
        }
        assert!(!quoted, "unterminated quote in {line}");
        if in_word {
            words.push(cur);
        }
        words
    }

    /// SP-DEC-4 T7 nit: a path with a space or a quote must survive copy-paste.
    #[test]
    fn every_reproduce_line_parses_when_a_path_has_a_space_or_a_quote() {
        use decision_bench::run::{Results, RunPaths};
        let results = Results {
            generated_at: 0,
            manifest: None,
            not_run: vec![],
            roster: vec![],
            labels_reviewed: vec![],
            completed: vec![],
            paths: Some(RunPaths {
                config: Some("my bench.toml".into()),
                cases: "my cases".into(),
                results: "it's out/r.json".into(),
                report: Some("my report.md".into()),
            }),
            rows: vec![],
        };
        let md = render(&results, 7);
        let block = md
            .split("```sh\n")
            .nth(1)
            .and_then(|b| b.split("```").next())
            .expect("a sh block");
        let mut seen = Vec::new();
        for line in block.lines().filter(|l| !l.is_empty()) {
            let words = shell_split(line);
            let prefix = ["cargo", "run", "--release", "-p", "decision-bench", "--"];
            assert_eq!(&words[..6], prefix, "{line}");
            Cli::try_parse_from(
                std::iter::once("decision-bench".to_string()).chain(words[6..].iter().cloned()),
            )
            .unwrap_or_else(|e| panic!("{line}: {e}"));
            seen.extend(words);
        }
        for want in [
            "my cases",
            "my bench.toml",
            "it's out/r.json",
            "my report.md",
        ] {
            assert!(seen.iter().any(|w| w == want), "{want} not a single word");
        }
    }

    #[test]
    fn relative_paths_resolve_against_the_crate_dir_and_absolute_ones_are_kept() {
        assert_eq!(resolve(Path::new("cases")), Path::new(CRATE).join("cases"));
        assert_eq!(resolve(Path::new("a/b")), Path::new(CRATE).join("a/b"));
        assert_eq!(resolve(Path::new("/tmp/x")), Path::new("/tmp/x"));
    }

    #[test]
    fn the_default_out_is_the_crates_cases_dir_wherever_it_runs_from() {
        let Command::Fetch { out, .. } = Cli::parse_from(["decision-bench", "fetch"]).command
        else {
            panic!("not fetch")
        };
        assert_eq!(resolve(&out), Path::new(CRATE).join("cases"));
    }

    #[test]
    fn run_and_report_defaults_resolve_under_the_crate_dir() {
        let Command::Run {
            select,
            cases,
            scenarios,
            out,
        } = Cli::parse_from(["decision-bench", "run"]).command
        else {
            panic!("not run")
        };
        assert_eq!(
            select,
            Select {
                config: None,
                models: None
            }
        );
        assert_eq!(resolve(&cases), Path::new(CRATE).join("cases"));
        assert_eq!(resolve(&out), Path::new(CRATE).join("results.json"));
        assert_eq!(scenarios, None);
        let Command::Report { results, out, seed } =
            Cli::parse_from(["decision-bench", "report"]).command
        else {
            panic!("not report")
        };
        assert_eq!(resolve(&results), Path::new(CRATE).join("results.json"));
        assert_eq!(resolve(&out), Path::new(CRATE).join("report.md"));
        assert_eq!(seed, None);
        let Command::Check { select } = Cli::parse_from([
            "decision-bench",
            "check",
            "--config",
            "x.toml",
            "--models",
            "a,b",
        ])
        .command
        else {
            panic!("not check")
        };
        assert_eq!(select.config, Some(PathBuf::from("x.toml")));
        assert_eq!(select.models.as_deref(), Some("a,b"));
    }

    fn spec(id: &str, url: &str) -> ModelSpec {
        ModelSpec {
            id: id.into(),
            router: "llamacpp".into(),
            url: url.into(),
            api_model_id: format!("{id}-api"),
            context_window: 8192,
            api_key_env: None,
        }
    }

    #[test]
    fn models_are_selected_in_config_order_and_an_unknown_one_is_an_error() {
        let cfg = BenchConfig {
            models: vec![spec("a", "http://x"), spec("b", "http://x")],
        };
        assert_eq!(select_models(&cfg, None).unwrap(), ["a", "b"]);
        assert_eq!(select_models(&cfg, Some("b, a")).unwrap(), ["a", "b"]);
        let err = select_models(&cfg, Some("a,ghost"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("ghost"), "{err}");
        assert!(select_models(&cfg, Some(" , ")).is_err());
    }

    #[test]
    fn the_check_table_names_each_model_router_and_status() {
        let cfg = BenchConfig {
            models: vec![spec("up", "http://x"), spec("down", "http://y")],
        };
        let table = check_table(
            &cfg,
            &[
                CheckResult {
                    model: "up".into(),
                    status: CheckStatus::Reachable,
                },
                CheckResult {
                    model: "down".into(),
                    status: CheckStatus::NotRun {
                        reason: "connection refused".into(),
                    },
                },
            ],
        );
        let up = table.lines().find(|l| l.starts_with("up ")).unwrap();
        assert!(
            up.contains("llamacpp") && up.contains("reachable"),
            "{table}"
        );
        let down = table.lines().find(|l| l.starts_with("down ")).unwrap();
        assert!(
            down.contains("NOT RUN") && down.contains("connection refused"),
            "{table}"
        );
    }

    /// A crate dir with a bench.toml of `models`, `cases/s3.jsonl` (+ manifest)
    /// and `scenarios/s4-fix-check.jsonl` with an unreviewed header.
    fn crate_dir(models: &[ModelSpec]) -> tempfile::TempDir {
        use decision_bench::case::{Case, Expected, Scenario, write_jsonl};
        use gateway::types::decision::DecisionQuestion;
        let dir = tempfile::tempdir().unwrap();
        let mut toml = String::new();
        for m in models {
            toml += &format!(
                "[[models]]\nid = \"{}\"\nrouter = \"{}\"\nurl = \"{}\"\napi_model_id = \"{}\"\ncontext_window = {}\n\n",
                m.id, m.router, m.url, m.api_model_id, m.context_window
            );
        }
        std::fs::write(dir.path().join("bench.toml"), toml).unwrap();
        let case = |id: usize, scenario: Scenario| Case {
            id: format!("c{id}"),
            scenario,
            state: serde_json::json!("s"),
            question: DecisionQuestion::Noul {
                instructions: serde_json::json!("holds?"),
                criteria: None,
            },
            expected: Expected::Noul { value: true },
            source: "test".into(),
        };
        let cases = dir.path().join("cases");
        std::fs::create_dir_all(&cases).unwrap();
        let s3: Vec<Case> = (1..=3).map(|i| case(i, Scenario::S3Entailment)).collect();
        write_jsonl(&cases.join("s3.jsonl"), &s3).unwrap();
        std::fs::write(
            cases.join("manifest.json"),
            serde_json::json!({
                "seed": 2026, "per_scenario": 3, "rand_version": "0.8.5",
                "datasets": [], "files": []
            })
            .to_string(),
        )
        .unwrap();
        let s4 = dir.path().join("scenarios");
        std::fs::create_dir_all(&s4).unwrap();
        let mut text = "{\"labels_reviewed_by\": null, \"notes\": \"t\"}\n".to_string();
        for i in 1..=2 {
            text += &(serde_json::to_string(&case(i, Scenario::S4FixCheck)).unwrap() + "\n");
        }
        std::fs::write(s4.join("s4-fix-check.jsonl"), text).unwrap();
        dir
    }

    #[tokio::test]
    async fn run_checks_first_skips_not_run_models_and_report_renders_the_results() {
        use decision_bench::case::Scenario;
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "good-api",
                "answers": {"q": {"type": "noul", "noul": 0.9}},
                "usage": {"input_tokens": 30, "output_tokens": 0}
            })))
            .mount(&server)
            .await;
        let dir = crate_dir(&[
            spec("good", &server.uri()),
            spec("down", "http://127.0.0.1:9"),
        ]);
        let select = Select {
            config: None,
            models: None,
        };
        let results = run_command(
            &select,
            Path::new("cases"),
            None,
            Path::new("results.json"),
            dir.path(),
        )
        .await
        .unwrap();
        // 1 probe + 1 warm-up + 5 cases (3 S3 + 2 S4) for `good`.
        assert_eq!(server.received_requests().await.unwrap().len(), 7);
        assert_eq!(results.not_run.len(), 1);
        assert_eq!(results.not_run[0].model, "down");
        let roster: Vec<_> = results.roster.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(roster, ["good", "down"]);
        assert_eq!(
            results.labels_reviewed,
            vec![decision_bench::run::LabelReview {
                scenario: Scenario::S4FixCheck,
                reviewed_by: None
            }]
        );
        assert_eq!(results.manifest.as_ref().unwrap().seed, 2026);
        assert_eq!(results.rows.len(), 5);
        assert!(results.rows.iter().all(|r| r.model == "good"));
        let on_disk: Results = serde_json::from_str(
            &std::fs::read_to_string(dir.path().join("results.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(on_disk, results);

        let only_s4 = run_command(
            &Select {
                config: None,
                models: Some("good".into()),
            },
            Path::new("cases"),
            Some("s4"),
            Path::new("s4.json"),
            dir.path(),
        )
        .await
        .unwrap();
        assert_eq!(only_s4.rows.len(), 2);
        assert!(only_s4.not_run.is_empty());

        let report = report_command(
            Path::new("results.json"),
            Path::new("r.md"),
            None,
            dir.path(),
        )
        .unwrap();
        assert_eq!(report, dir.path().join("r.md"));
        let md = std::fs::read_to_string(report).unwrap();
        assert!(md.contains("## NOT RUN") && md.contains("**down**"), "{md}");
        assert!(md.contains("**labels unreviewed**"), "{md}");
        assert!(
            md.contains("report --results results.json --out r.md --seed 2026"),
            "default seed is the manifest's: {md}"
        );
        // T7 review M2: the run recorded its paths, relative to the crate dir.
        let paths = results.paths.as_ref().unwrap();
        assert_eq!(paths.cases, "cases");
        assert_eq!(paths.results, "results.json");
        assert_eq!(paths.config.as_deref(), Some("bench.toml"));
        // Every command the footer gives parses as this CLI.
        let block = md
            .split("```sh\n")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap();
        let mut n = 0;
        for line in block.lines() {
            let args = line.split(" -- ").nth(1).unwrap();
            let argv = std::iter::once("decision-bench").chain(args.split_whitespace());
            Cli::try_parse_from(argv).unwrap_or_else(|e| panic!("{line}: {e}"));
            n += 1;
        }
        assert_eq!(n, 4, "{block}");
    }

    /// T7 review M3: an invalid model entry is a NOT RUN row, not an abort.
    #[tokio::test]
    async fn an_invalid_model_entry_is_not_run_and_the_rest_still_run() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "good-api",
                "answers": {"q": {"type": "noul", "noul": 0.9}},
                "usage": {"input_tokens": 30, "output_tokens": 0}
            })))
            .mount(&server)
            .await;
        let mut bad = spec("bad", &server.uri());
        bad.router = "nope".into();
        let dir = crate_dir(&[bad, spec("good", &server.uri())]);
        let select = Select {
            config: None,
            models: None,
        };
        let results = run_command(
            &select,
            Path::new("cases"),
            Some("s3"),
            Path::new("results.json"),
            dir.path(),
        )
        .await
        .unwrap();
        assert_eq!(results.not_run.len(), 1, "{:?}", results.not_run);
        assert_eq!(results.not_run[0].model, "bad");
        assert!(
            results.not_run[0].reason.contains("nope"),
            "{:?}",
            results.not_run
        );
        assert_eq!(results.completed, ["good"]);
    }

    #[tokio::test]
    async fn check_reports_not_run_and_returns_normally_but_a_bad_config_is_an_error() {
        let dir = crate_dir(&[spec("down", "http://127.0.0.1:9")]);
        let select = Select {
            config: None,
            models: None,
        };
        let got = check_command(&select, dir.path()).await.unwrap();
        assert!(
            matches!(got[0].status, CheckStatus::NotRun { .. }),
            "{got:?}"
        );
        let bad = Select {
            config: Some("missing.toml".into()),
            models: None,
        };
        assert!(check_command(&bad, dir.path()).await.is_err());
        let ghost = Select {
            config: None,
            models: Some("ghost".into()),
        };
        assert!(check_command(&ghost, dir.path()).await.is_err());
    }

    #[test]
    fn check_run_and_report_help_say_where_relative_paths_resolve() {
        let mut cmd = Cli::command();
        for sub in ["check", "run", "report"] {
            let help = cmd
                .find_subcommand_mut(sub)
                .unwrap()
                .render_long_help()
                .to_string();
            assert!(help.contains("Paths: "), "{sub}: {help}");
            assert!(help.contains("not the current directory"), "{sub}: {help}");
            assert!(help.contains(CRATE), "{sub}: {help}");
            assert!(!help.contains("default --out is"), "{sub}: {help}");
        }
    }

    /// SP-DEC-4 review C7: each subcommand's path note names only flags that
    /// subcommand has.
    #[test]
    fn each_path_note_names_only_its_own_flags() {
        let cmd = Cli::command();
        for sub in cmd.get_subcommands() {
            let note = sub
                .get_after_long_help()
                .map(|h| h.to_string())
                .unwrap_or_default();
            let own: Vec<String> = sub
                .get_arguments()
                .filter_map(|a| a.get_long())
                .map(|l| format!("--{l}"))
                .collect();
            let named: Vec<&str> = note
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '-'))
                .filter(|w| w.starts_with("--") && w.len() > 2)
                .collect();
            for flag in named {
                assert!(
                    own.iter().any(|o| o == flag),
                    "{}: help names {flag}, which it does not take ({own:?})",
                    sub.get_name()
                );
            }
        }
    }

    /// SP-DEC-4 review A7: with no `--seed`, `report` seeds the CIs with the
    /// manifest's sampling seed, else 2026.
    #[test]
    fn report_defaults_seed_to_manifest_seed_else_2026() {
        let dir = tempfile::tempdir().unwrap();
        let results = |manifest: serde_json::Value| {
            serde_json::json!({
                "generated_at": 0, "manifest": manifest, "not_run": [], "rows": []
            })
            .to_string()
        };
        let seeded = results(serde_json::json!({
            "seed": 4242, "per_scenario": 3, "rand_version": "0.8.5",
            "datasets": [], "files": []
        }));
        std::fs::write(dir.path().join("seeded.json"), seeded).unwrap();
        std::fs::write(
            dir.path().join("bare.json"),
            results(serde_json::Value::Null),
        )
        .unwrap();
        let render_of = |file: &str, seed: Option<u64>| {
            let out = report_command(Path::new(file), Path::new("r.md"), seed, dir.path()).unwrap();
            std::fs::read_to_string(out).unwrap()
        };
        let md = render_of("seeded.json", None);
        assert!(
            md.contains("Bootstrap CIs: 1000 resamples, seed 4242."),
            "{md}"
        );
        assert!(md.contains("report --seed 4242"), "{md}");
        let md = render_of("bare.json", None);
        assert!(
            md.contains("Bootstrap CIs: 1000 resamples, seed 2026."),
            "{md}"
        );
        assert!(md.contains("report --seed 2026"), "{md}");
        let md = render_of("seeded.json", Some(9));
        assert!(md.contains("report --seed 9"), "--seed wins: {md}");
    }

    /// SP-DEC-4 review A4: NOT RUN is also a reachable model no input fits.
    #[test]
    fn check_help_says_a_model_no_input_fits_is_not_run_too() {
        let mut cmd = Cli::command();
        let help = cmd
            .find_subcommand_mut("check")
            .unwrap()
            .render_long_help()
            .to_string();
        assert!(help.contains("no input fits its context window"), "{help}");
    }

    #[test]
    fn fetch_help_says_relative_paths_are_resolved_against_the_crate_dir() {
        let mut cmd = Cli::command();
        let help = cmd
            .find_subcommand_mut("fetch")
            .unwrap()
            .render_long_help()
            .to_string();
        assert!(help.contains("relative paths resolve under"), "{help}");
        assert!(help.contains("gitignored"), "{help}");
        assert!(help.contains(CRATE), "{help}");
    }
}
