//! Seeded builders turning datasets-server rows into benchmark cases
//! (S1a/S1b/S2/S3), and the fetch manifest.

use anyhow::{Context, bail};
use gateway::types::decision::DecisionQuestion;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::case::{Case, Expected, Scenario};
use crate::fetch::Row;

/// Where a row set came from; feeds each case's `source`.
#[derive(Debug, Clone, Copy)]
pub struct Source<'a> {
    pub dataset: &'a str,
    pub sha: &'a str,
    pub split: &'a str,
}

/// The `rand` release the sample was drawn with.
pub const RAND_VERSION: &str = "0.8.7";

pub const SENTIMENT_LEVELS: [&str; 3] = ["negative", "neutral", "positive"];

/// Distractors offered beside the gold label in S1a/S1b.
pub const DISTRACTORS: usize = 7;

const S1_INSTRUCTIONS: &str = "Which intent does this request express?";
const S2_INSTRUCTIONS: &str = "What is the sentiment of this text?";
const S3_INSTRUCTIONS: &str = "Does the hypothesis follow from the premise?";

fn scenario_name(s: Scenario) -> String {
    serde_json::to_value(s)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_else(|| format!("{s:?}"))
}

fn case_id(s: Scenario, r: &Row) -> String {
    format!("{}-{}", scenario_name(s), r.row_idx)
}

/// `<dataset>@<sha10>:<split>#<row_idx> seed=<seed> rand=<RAND_VERSION>`: enough
/// to redraw the case (`StdRng` is not stable across rand releases).
fn source(src: Source<'_>, r: &Row, seed: u64) -> String {
    let sha10 = src.sha.get(..10).unwrap_or(src.sha);
    format!(
        "{}@{sha10}:{}#{} seed={seed} rand={RAND_VERSION}",
        src.dataset, src.split, r.row_idx
    )
}

/// A row's text field: `None` when it is null or blank (the row is skipped;
/// Sp1786 test row 3751 has `"text": null`), an error when it is missing or
/// not a string.
fn text<'r>(r: &'r Row, field: &str) -> anyhow::Result<Option<&'r str>> {
    match r.row.get(field) {
        Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) => Ok(Some(s.as_str()).filter(|s| !s.trim().is_empty())),
        _ => bail!(
            "row {}: field {field:?} is missing or not a string",
            r.row_idx
        ),
    }
}

fn label(r: &Row, field: &str) -> anyhow::Result<i64> {
    r.row.get(field).and_then(|v| v.as_i64()).with_context(|| {
        format!(
            "row {}: field {field:?} is missing or not an integer",
            r.row_idx
        )
    })
}

/// The label index of `r`, which must name one of `names`.
fn class(r: &Row, field: &str, names: &[String]) -> anyhow::Result<usize> {
    let l = label(r, field)?;
    usize::try_from(l)
        .ok()
        .filter(|&i| i < names.len())
        .with_context(|| {
            format!(
                "row {}: label {l} is outside the {} label names",
                r.row_idx,
                names.len()
            )
        })
}

/// Draw `n` of `eligible` indices without replacement from a generator seeded
/// with `seed ^ scenario.tag()`, sorted so the output follows row order. The
/// generator is returned so per-case draws continue the same stream.
fn sample(
    scenario: Scenario,
    seed: u64,
    eligible: usize,
    n: usize,
) -> anyhow::Result<(StdRng, Vec<usize>)> {
    if n > eligible {
        bail!(
            "{}: asked for {n} cases but only {eligible} rows are eligible",
            scenario_name(scenario)
        );
    }
    let mut rng = StdRng::seed_from_u64(seed ^ scenario.tag());
    let mut picked = rand::seq::index::sample(&mut rng, eligible, n).into_vec();
    picked.sort_unstable();
    Ok((rng, picked))
}

fn unique_names(names: &[String]) -> anyhow::Result<()> {
    let mut seen = std::collections::HashSet::new();
    if let Some(dup) = names.iter().find(|n| !seen.insert(n.as_str())) {
        bail!("label name {dup:?} appears twice");
    }
    Ok(())
}

/// The label indices offered for one case, shuffled: `gold`, distractors
/// drawn without replacement from the other labels (never `oos`), and `oos`
/// when given. With `oos`, every case offers 9 options: when gold IS `oos` an
/// eighth distractor takes its slot, so the chance baseline is the same for
/// every case.
fn draw_options(rng: &mut StdRng, gold: usize, n_labels: usize, oos: Option<usize>) -> Vec<usize> {
    let pool: Vec<usize> = (0..n_labels)
        .filter(|&l| l != gold && Some(l) != oos)
        .collect();
    let distractors = DISTRACTORS + usize::from(oos == Some(gold));
    let mut options: Vec<usize> = vec![gold];
    options.extend(
        rand::seq::index::sample(rng, pool.len(), distractors)
            .into_iter()
            .map(|k| pool[k]),
    );
    if let Some(o) = oos
        && o != gold
    {
        options.push(o);
    }
    options.shuffle(rng);
    options
}

#[allow(clippy::too_many_arguments)]
fn build_choice(
    scenario: Scenario,
    src: Source<'_>,
    rows: &[Row],
    field: &str,
    names: &[String],
    with_oos: bool,
    seed: u64,
    n: usize,
) -> anyhow::Result<Vec<Case>> {
    unique_names(names)?;
    let oos = if with_oos {
        Some(
            names
                .iter()
                .position(|l| l == "oos")
                .context("S1a needs an `oos` label among the label names")?,
        )
    } else {
        None
    };
    let pool_size = names.len() - 1 - usize::from(oos.is_some());
    if pool_size < DISTRACTORS {
        bail!(
            "{}: {} labels leave {pool_size} distractor candidates; need {DISTRACTORS}",
            scenario_name(scenario),
            names.len()
        );
    }
    let mut eligible = Vec::new();
    for r in rows {
        let t = text(r, "text")?;
        let gold = class(r, field, names)?;
        if let Some(t) = t {
            eligible.push((r, t, gold));
        }
    }
    let (mut rng, picked) = sample(scenario, seed, eligible.len(), n)?;
    let mut out = Vec::with_capacity(picked.len());
    for i in picked {
        let (r, t, gold) = eligible[i];
        let options = draw_options(&mut rng, gold, names.len(), oos);
        let question = DecisionQuestion::Choice {
            instructions: json!(S1_INSTRUCTIONS),
            criteria: options.iter().map(|&l| (names[l].clone(), None)).collect(),
        };
        if let DecisionQuestion::Choice { criteria, .. } = &question {
            // Label names are unique (checked above), so no option collapses.
            debug_assert_eq!(criteria.len(), options.len());
        }
        out.push(Case {
            id: case_id(scenario, r),
            scenario,
            state: serde_json::Value::String(t.to_string()),
            question,
            expected: Expected::Choice {
                option: names[gold].clone(),
            },
            source: source(src, r, seed),
        });
    }
    Ok(out)
}

/// S1a (CLINC `plus`): 9 options, shuffled: gold + 7 distractors + `oos`, or
/// `oos` + 8 distractors when gold is `oos` itself.
pub fn build_s1a(
    src: Source<'_>,
    rows: &[Row],
    label_names: &[String],
    seed: u64,
    n: usize,
) -> anyhow::Result<Vec<Case>> {
    build_choice(
        Scenario::S1aIntentOos,
        src,
        rows,
        "intent",
        label_names,
        true,
        seed,
        n,
    )
}

/// S1b (banking77): gold + 7 distractors, shuffled.
pub fn build_s1b(
    src: Source<'_>,
    rows: &[Row],
    label_names: &[String],
    seed: u64,
    n: usize,
) -> anyhow::Result<Vec<Case>> {
    build_choice(
        Scenario::S1bBanking77,
        src,
        rows,
        "label",
        label_names,
        false,
        seed,
        n,
    )
}

/// S2 (sentiment): `label` 0/1/2 is the score level over `label_names`
/// (normally [`SENTIMENT_LEVELS`]). A row whose `sentiment` string disagrees
/// with its label is an error.
pub fn build_s2(
    src: Source<'_>,
    rows: &[Row],
    label_names: &[String],
    seed: u64,
    n: usize,
) -> anyhow::Result<Vec<Case>> {
    let scenario = Scenario::S2Sentiment;
    let mut eligible = Vec::new();
    for r in rows {
        let t = text(r, "text")?;
        let level = class(r, "label", label_names)?;
        if let Some(s) = r.row.get("sentiment")
            && s.as_str() != Some(label_names[level].as_str())
        {
            bail!(
                "row {}: sentiment {s} disagrees with label {level} ({:?})",
                r.row_idx,
                label_names[level]
            );
        }
        if let Some(t) = t {
            eligible.push((r, t, level));
        }
    }
    let (_, picked) = sample(scenario, seed, eligible.len(), n)?;
    Ok(picked
        .into_iter()
        .map(|i| {
            let (r, t, level) = eligible[i];
            Case {
                id: case_id(scenario, r),
                scenario,
                state: serde_json::Value::String(t.to_string()),
                question: DecisionQuestion::Score {
                    instructions: json!(S2_INSTRUCTIONS),
                    criteria: label_names.to_vec(),
                },
                expected: Expected::Score { level },
                source: source(src, r, seed),
            }
        })
        .collect())
}

/// S3 (SNLI): entailment → yes, contradiction → no; neutral and unlabelled
/// (−1) rows are dropped before sampling.
pub fn build_s3(
    src: Source<'_>,
    rows: &[Row],
    label_names: &[String],
    seed: u64,
    n: usize,
) -> anyhow::Result<Vec<Case>> {
    let scenario = Scenario::S3Entailment;
    let find = |name: &str| {
        label_names
            .iter()
            .position(|l| l == name)
            .with_context(|| format!("S3 needs a {name:?} label, got {label_names:?}"))
    };
    let (yes, no) = (find("entailment")? as i64, find("contradiction")? as i64);
    let mut eligible = Vec::new();
    for r in rows {
        let l = label(r, "label")?;
        let value = if l == yes {
            true
        } else if l == no {
            false
        } else {
            continue;
        };
        let (p, h) = (text(r, "premise")?, text(r, "hypothesis")?);
        if let (Some(p), Some(h)) = (p, h) {
            eligible.push((r, p, h, value));
        }
    }
    let (_, picked) = sample(scenario, seed, eligible.len(), n)?;
    Ok(picked
        .into_iter()
        .map(|i| {
            let (r, p, h, value) = eligible[i];
            Case {
                id: case_id(scenario, r),
                scenario,
                state: json!({"premise": p, "hypothesis": h}),
                question: DecisionQuestion::Noul {
                    instructions: json!(S3_INSTRUCTIONS),
                    criteria: None,
                },
                expected: Expected::Noul { value },
                source: source(src, r, seed),
            }
        })
        .collect())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestDataset {
    pub id: String,
    pub sha: String,
    pub license: String,
    pub config: String,
    pub split: String,
    pub num_rows: u64,
    /// sha256 of the cached rows file the cases were built from.
    pub rows_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestFile {
    pub file: String,
    pub scenario: Scenario,
    pub dataset: String,
    pub cases: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub seed: u64,
    pub per_scenario: usize,
    pub rand_version: String,
    pub datasets: Vec<ManifestDataset>,
    pub files: Vec<ManifestFile>,
}

/// One public dataset and the scenario built from it.
#[derive(Debug, Clone, Copy)]
pub struct DatasetSpec {
    pub scenario: Scenario,
    pub file: &'static str,
    pub id: &'static str,
    pub config: &'static str,
    pub split: &'static str,
    /// The licence the hub card must report; anything else fails the fetch.
    pub license: &'static str,
}

pub const DATASETS: [DatasetSpec; 4] = [
    DatasetSpec {
        scenario: Scenario::S1aIntentOos,
        file: "s1a.jsonl",
        id: "clinc/clinc_oos",
        config: "plus",
        split: "test",
        license: "cc-by-3.0",
    },
    DatasetSpec {
        scenario: Scenario::S1bBanking77,
        file: "s1b.jsonl",
        id: "legacy-datasets/banking77",
        config: "default",
        split: "test",
        license: "cc-by-4.0",
    },
    DatasetSpec {
        scenario: Scenario::S2Sentiment,
        file: "s2.jsonl",
        id: "Sp1786/multiclass-sentiment-analysis-dataset",
        config: "default",
        split: "test",
        license: "apache-2.0",
    },
    DatasetSpec {
        scenario: Scenario::S3Entailment,
        file: "s3.jsonl",
        id: "stanfordnlp/snli",
        config: "plain_text",
        split: "test",
        license: "cc-by-sa-4.0",
    },
];

/// Where `fetch_cases` reads from and writes to.
pub struct FetchOptions<'a> {
    pub cache_root: &'a std::path::Path,
    pub out: &'a std::path::Path,
    pub seed: u64,
    pub per_scenario: usize,
}

/// Fetch (or load from cache) every dataset in [`DATASETS`], check its
/// licence, build its scenario's cases, validate them, and write
/// `<out>/s1a.jsonl` … `s3.jsonl` plus `manifest.json` as one set (see
/// `write_case_set`). Nothing is written unless every dataset built.
pub async fn fetch_cases(
    hf: &crate::fetch::Hf,
    opts: &FetchOptions<'_>,
) -> anyhow::Result<Manifest> {
    let mut datasets = Vec::new();
    let mut built = Vec::new();
    for spec in DATASETS {
        let ds = hf
            .load_or_fetch(opts.cache_root, spec.id, spec.config, spec.split)
            .await?;
        if ds.meta.license != spec.license {
            bail!(
                "{}: the hub card's licence is {:?}, expected {:?}; check the new terms before \
                 using it",
                spec.id,
                ds.meta.license,
                spec.license
            );
        }
        let src = Source {
            dataset: spec.id,
            sha: &ds.meta.sha,
            split: spec.split,
        };
        let (seed, n) = (opts.seed, opts.per_scenario);
        let class = |field| crate::fetch::class_names(&ds.meta.features, field);
        let cases = match spec.scenario {
            Scenario::S1aIntentOos => build_s1a(src, &ds.rows, &class("intent")?, seed, n),
            Scenario::S1bBanking77 => build_s1b(src, &ds.rows, &class("label")?, seed, n),
            Scenario::S2Sentiment => {
                let levels: Vec<String> = SENTIMENT_LEVELS.iter().map(|s| s.to_string()).collect();
                build_s2(src, &ds.rows, &levels, seed, n)
            }
            Scenario::S3Entailment => build_s3(src, &ds.rows, &class("label")?, seed, n),
            other => bail!("{other:?} is not built from a public dataset"),
        }
        .with_context(|| format!("building {} from {}", spec.file, spec.id))?;
        for c in &cases {
            c.validate()?;
        }
        datasets.push(ManifestDataset {
            id: ds.meta.id.clone(),
            sha: ds.meta.sha.clone(),
            license: ds.meta.license.clone(),
            config: ds.meta.config.clone(),
            split: ds.meta.split.clone(),
            num_rows: ds.meta.num_rows,
            rows_sha256: ds
                .meta
                .rows_sha256
                .clone()
                .context("load_or_fetch returned no rows sha256")?,
        });
        built.push((spec, cases));
    }
    let files = built
        .iter()
        .map(|(spec, cases)| ManifestFile {
            file: spec.file.to_string(),
            scenario: spec.scenario,
            dataset: spec.id.to_string(),
            cases: cases.len(),
        })
        .collect();
    let manifest = Manifest {
        seed: opts.seed,
        per_scenario: opts.per_scenario,
        rand_version: RAND_VERSION.to_string(),
        datasets,
        files,
    };
    for warning in write_case_set(opts.out, &built, &manifest)? {
        eprintln!("warning: {warning}");
    }
    Ok(manifest)
}

fn sibling(dir: &std::path::Path, suffix: &str) -> std::path::PathBuf {
    let mut s = dir.as_os_str().to_owned();
    s.push(suffix);
    std::path::PathBuf::from(s)
}

/// The filesystem calls the case-set swap makes, injectable so tests can fail
/// them.
struct SwapFs {
    rename: fn(&std::path::Path, &std::path::Path) -> std::io::Result<()>,
    remove_dir_all: fn(&std::path::Path) -> std::io::Result<()>,
}

fn real_rename(a: &std::path::Path, b: &std::path::Path) -> std::io::Result<()> {
    std::fs::rename(a, b)
}

fn real_remove_dir_all(p: &std::path::Path) -> std::io::Result<()> {
    std::fs::remove_dir_all(p)
}

impl SwapFs {
    const REAL: SwapFs = SwapFs {
        rename: real_rename,
        remove_dir_all: real_remove_dir_all,
    };
}

/// A directory this tool may replace or delete: empty, or holding a
/// `manifest.json` (a case set, or our own staging dir, which gets its
/// manifest first).
fn is_ours(dir: &std::path::Path) -> anyhow::Result<bool> {
    Ok(dir.join("manifest.json").exists()
        || std::fs::read_dir(dir)
            .with_context(|| format!("reading {}", dir.display()))?
            .next()
            .is_none())
}

/// Write every case file plus `manifest.json` into `<out>.tmp/`, then swap it
/// in for `out` (`out` → `<out>.old`, `<out>.tmp` → `out`, delete `.old`), so
/// the files in `out` always come from one run. Returns warnings that do not
/// make the run a failure.
///
/// Safety rules:
/// - a crash between the two renames leaves no `out` and the previous set in
///   `<out>.old`; the next run renames it back before anything else;
/// - an existing `out`, `.tmp` or `.old` is replaced or deleted only if
///   [`is_ours`]; anything else is an error naming it.
fn write_case_set(
    out: &std::path::Path,
    built: &[(DatasetSpec, Vec<Case>)],
    manifest: &Manifest,
) -> anyhow::Result<Vec<String>> {
    write_case_set_with(&SwapFs::REAL, out, built, manifest)
}

fn write_case_set_with(
    fs: &SwapFs,
    out: &std::path::Path,
    built: &[(DatasetSpec, Vec<Case>)],
    manifest: &Manifest,
) -> anyhow::Result<Vec<String>> {
    let mut warnings = Vec::new();
    let tmp = sibling(out, ".tmp");
    let old = sibling(out, ".old");
    if !out.exists() && old.join("manifest.json").exists() {
        (fs.rename)(&old, out).with_context(|| {
            format!(
                "restoring the previous case set from {} to {}",
                old.display(),
                out.display()
            )
        })?;
        warnings.push(format!(
            "restored the previous case set from {} (an earlier run stopped mid-swap)",
            old.display()
        ));
    }
    let replaces = out.exists();
    if replaces && !is_ours(out)? {
        bail!(
            "refusing to replace {}: it is not empty and has no manifest.json, so it is not \
             a decision-bench case set",
            out.display()
        );
    }
    if let Some(parent) = out.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    for leftover in [&tmp, &old] {
        if !leftover.exists() {
            continue;
        }
        if !is_ours(leftover)? {
            bail!(
                "refusing to delete {}: it is not empty and has no manifest.json, so it is not \
                 a leftover of this tool; move it away",
                leftover.display()
            );
        }
        (fs.remove_dir_all)(leftover)
            .with_context(|| format!("removing leftover {}", leftover.display()))?;
    }
    let staged = (|| -> anyhow::Result<()> {
        std::fs::create_dir(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
        // The manifest goes in first, so a staging dir left by a crash is
        // recognisably ours (see `is_ours`).
        let mut bytes = serde_json::to_vec_pretty(manifest)?;
        bytes.push(b'\n');
        crate::fetch::write_atomic(&tmp.join("manifest.json"), &bytes)?;
        for (spec, cases) in built {
            crate::case::write_jsonl(&tmp.join(spec.file), cases)?;
        }
        Ok(())
    })();
    if let Err(e) = staged {
        let _ = (fs.remove_dir_all)(&tmp);
        return Err(e);
    }
    // Two renames: no portable call swaps two directories atomically.
    if replaces {
        (fs.rename)(out, &old)
            .with_context(|| format!("moving {} aside to {}", out.display(), old.display()))?;
    }
    if let Err(e) = (fs.rename)(&tmp, out) {
        let swap = format!("renaming {} to {}: {e}", tmp.display(), out.display());
        if replaces {
            if let Err(back) = (fs.rename)(&old, out) {
                bail!(
                    "{swap}; restoring the previous set from {} also failed: {back}. The \
                     previous set is still in {}, and the new one in {}",
                    old.display(),
                    old.display(),
                    tmp.display()
                );
            }
            bail!("{swap}; the previous set was put back");
        }
        bail!("{swap}");
    }
    if replaces && let Err(e) = (fs.remove_dir_all)(&old) {
        warnings.push(format!(
            "the new case set is in place, but removing the previous one at {} failed: {e}; \
             delete it by hand",
            old.display()
        ));
    }
    Ok(warnings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::case::write_jsonl;

    const SRC: Source<'static> = Source {
        dataset: "org/ds",
        sha: "0123456789abcdef",
        split: "test",
    };

    fn row(idx: u64, fields: serde_json::Value) -> Row {
        Row {
            row_idx: idx,
            row: fields.as_object().unwrap().clone(),
        }
    }

    /// 20 intents with `oos` at index 7; 40 rows cycling through them.
    fn intent_set(field: &str, with_oos: bool) -> (Vec<Row>, Vec<String>) {
        let names: Vec<String> = (0..20)
            .map(|i| {
                if with_oos && i == 7 {
                    "oos".to_string()
                } else {
                    format!("intent_{i}")
                }
            })
            .collect();
        let rows = (0..40)
            .map(|i| {
                row(
                    i,
                    json!({"text": format!("request number {i}"), field: i % 20}),
                )
            })
            .collect();
        (rows, names)
    }

    fn options(c: &Case) -> Vec<String> {
        let DecisionQuestion::Choice {
            instructions,
            criteria,
        } = &c.question
        else {
            panic!("{}: not a choice", c.id)
        };
        assert_eq!(
            instructions,
            &json!("Which intent does this request express?")
        );
        assert!(criteria.values().all(Option::is_none));
        criteria.keys().cloned().collect()
    }

    fn gold(c: &Case) -> String {
        let Expected::Choice { option } = &c.expected else {
            panic!("{}: not a choice expectation", c.id)
        };
        option.clone()
    }

    #[test]
    fn s1a_case_offers_gold_plus_seven_distractors_plus_oos() {
        let (rows, names) = intent_set("intent", true);
        let mut gold_positions = std::collections::BTreeSet::new();
        let mut saw_oos_gold = false;
        for seed in 0..8 {
            let cases = build_s1a(SRC, &rows, &names, seed, 40).unwrap();
            assert_eq!(cases.len(), 40);
            for c in &cases {
                assert_eq!(c.scenario, Scenario::S1aIntentOos);
                let idx: u64 = c.id.rsplit('-').next().unwrap().parse().unwrap();
                let want = &names[(idx % 20) as usize];
                assert_eq!(&gold(c), want, "{}", c.id);
                assert_eq!(c.state, json!(format!("request number {idx}")));
                // Distinctness is asserted on the drawn indices
                // (`draw_options_are_distinct_label_indices`): these keys come
                // out of an IndexMap, which would already have collapsed a
                // duplicate.
                let opts = options(c);
                assert!(opts.contains(want));
                assert!(opts.iter().any(|o| o == "oos"), "{}: no oos {opts:?}", c.id);
                saw_oos_gold |= want == "oos";
                assert_eq!(opts.len(), 9, "{}: {opts:?}", c.id);
                gold_positions.insert(opts.iter().position(|o| o == want).unwrap());
            }
        }
        assert!(saw_oos_gold);
        assert!(
            gold_positions.len() > 3,
            "gold is not shuffled: positions {gold_positions:?}"
        );
    }

    #[test]
    fn s1b_has_eight_options() {
        let (rows, names) = intent_set("label", false);
        let mut gold_positions = std::collections::BTreeSet::new();
        for seed in 0..8 {
            let cases = build_s1b(SRC, &rows, &names, seed, 40).unwrap();
            assert_eq!(cases.len(), 40);
            for c in &cases {
                assert_eq!(c.scenario, Scenario::S1bBanking77);
                let idx: u64 = c.id.rsplit('-').next().unwrap().parse().unwrap();
                let want = &names[(idx % 20) as usize];
                assert_eq!(&gold(c), want);
                assert_eq!(c.state, json!(format!("request number {idx}")));
                let opts = options(c);
                assert_eq!(opts.len(), 8, "{}: {opts:?}", c.id);
                assert!(opts.contains(want));
                gold_positions.insert(opts.iter().position(|o| o == want).unwrap());
            }
        }
        assert!(gold_positions.len() > 3, "{gold_positions:?}");
    }

    #[test]
    fn s1a_rejects_a_label_set_without_oos_and_s1b_one_with_too_few_labels() {
        let (rows, names) = intent_set("intent", false);
        let err = build_s1a(SRC, &rows, &names, 1, 5).unwrap_err().to_string();
        assert!(err.contains("oos"), "{err}");
        let few: Vec<String> = names[..7].to_vec();
        let rows: Vec<Row> = (0..7)
            .map(|i| row(i, json!({"text": "x", "label": i})))
            .collect();
        let err = build_s1b(SRC, &rows, &few, 1, 5).unwrap_err().to_string();
        assert!(err.contains("distractor"), "{err}");
    }

    fn sentiment_rows() -> Vec<Row> {
        (0..30)
            .map(|i| {
                let label = i % 3;
                row(
                    i,
                    json!({"id": 1000 + i, "text": format!("feeling {i}"), "label": label,
                           "sentiment": SENTIMENT_LEVELS[label as usize]}),
                )
            })
            .collect()
    }

    fn levels() -> Vec<String> {
        SENTIMENT_LEVELS.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn s2_maps_0_1_2_to_score_levels() {
        let cases = build_s2(SRC, &sentiment_rows(), &levels(), 7, 30).unwrap();
        assert_eq!(cases.len(), 30);
        for c in &cases {
            let idx: u64 = c.id.rsplit('-').next().unwrap().parse().unwrap();
            assert_eq!(c.scenario, Scenario::S2Sentiment);
            assert_eq!(c.state, json!(format!("feeling {idx}")));
            assert_eq!(
                c.question,
                DecisionQuestion::Score {
                    instructions: json!("What is the sentiment of this text?"),
                    criteria: levels(),
                }
            );
            assert_eq!(
                c.expected,
                Expected::Score {
                    level: (idx % 3) as usize
                }
            );
        }
        // `sentiment` disagreeing with `label` is a data error, not a guess.
        let mut rows = sentiment_rows();
        rows[4].row.insert("sentiment".into(), json!("positive"));
        let err = build_s2(SRC, &rows, &levels(), 7, 30)
            .unwrap_err()
            .to_string();
        assert!(err.contains("row 4"), "{err}");
    }

    fn snli_names() -> Vec<String> {
        ["entailment", "neutral", "contradiction"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    fn snli_rows() -> Vec<Row> {
        // labels cycle 0, 1, 2, -1
        (0..40)
            .map(|i| {
                let label = [0, 1, 2, -1][(i % 4) as usize];
                row(
                    i,
                    json!({"premise": format!("p{i}"), "hypothesis": format!("h{i}"), "label": label}),
                )
            })
            .collect()
    }

    #[test]
    fn s3_maps_entailment_yes_contradiction_no_and_drops_neutral_and_unlabelled() {
        // 20 of the 40 rows are entailment/contradiction; asking for all 20.
        let cases = build_s3(SRC, &snli_rows(), &snli_names(), 3, 20).unwrap();
        assert_eq!(cases.len(), 20);
        for c in &cases {
            let idx: u64 = c.id.rsplit('-').next().unwrap().parse().unwrap();
            assert_eq!(c.scenario, Scenario::S3Entailment);
            assert_eq!(
                c.state,
                json!({"premise": format!("p{idx}"), "hypothesis": format!("h{idx}")})
            );
            assert_eq!(
                c.question,
                DecisionQuestion::Noul {
                    instructions: json!("Does the hypothesis follow from the premise?"),
                    criteria: None,
                }
            );
            match idx % 4 {
                0 => assert_eq!(c.expected, Expected::Noul { value: true }),
                2 => assert_eq!(c.expected, Expected::Noul { value: false }),
                other => panic!("{}: label class {other} should have been dropped", c.id),
            }
        }
        // Asking for more than the eligible rows is an error, not a silent shortfall.
        let err = build_s3(SRC, &snli_rows(), &snli_names(), 3, 21)
            .unwrap_err()
            .to_string();
        assert!(err.contains("21") && err.contains("20"), "{err}");
    }

    #[test]
    fn case_ids_and_sources_name_the_scenario_dataset_sha_and_row() {
        let (rows, names) = intent_set("intent", true);
        let cases = build_s1a(SRC, &rows, &names, 1, 40).unwrap();
        let c = cases.iter().find(|c| c.id.ends_with("-13")).unwrap();
        assert_eq!(c.id, "s1a_intent_oos-13");
        assert_eq!(c.source, "org/ds@0123456789:test#13 seed=1 rand=0.8.7");
    }

    fn jsonl_bytes(cases: &[Case]) -> Vec<u8> {
        let f = tempfile::NamedTempFile::new().unwrap();
        write_jsonl(f.path(), cases).unwrap();
        std::fs::read(f.path()).unwrap()
    }

    fn all_builders(seed: u64, n: usize) -> Vec<Vec<Case>> {
        let (r1a, n1a) = intent_set("intent", true);
        let (r1b, n1b) = intent_set("label", false);
        vec![
            build_s1a(SRC, &r1a, &n1a, seed, n).unwrap(),
            build_s1b(SRC, &r1b, &n1b, seed, n).unwrap(),
            build_s2(SRC, &sentiment_rows(), &levels(), seed, n).unwrap(),
            build_s3(SRC, &snli_rows(), &snli_names(), seed, n).unwrap(),
        ]
    }

    #[test]
    fn sampling_is_reproducible() {
        let a = all_builders(2026, 10);
        let b = all_builders(2026, 10);
        let c = all_builders(2027, 10);
        for i in 0..4 {
            assert_eq!(a[i].len(), 10);
            assert_eq!(jsonl_bytes(&a[i]), jsonl_bytes(&b[i]), "builder {i}");
            assert_ne!(jsonl_bytes(&a[i]), jsonl_bytes(&c[i]), "builder {i}");
        }
    }

    #[test]
    fn cases_come_out_in_row_order() {
        for seed in [0, 5, 2026] {
            for cases in all_builders(seed, 10) {
                let idx: Vec<u64> = cases
                    .iter()
                    .map(|c| c.id.rsplit('-').next().unwrap().parse().unwrap())
                    .collect();
                assert!(idx.windows(2).all(|w| w[0] < w[1]), "seed {seed}: {idx:?}");
            }
        }
    }

    #[test]
    fn every_built_case_validates() {
        for seed in [0, 1, 2026] {
            for cases in all_builders(seed, 15) {
                assert!(!cases.is_empty());
                for c in &cases {
                    c.validate().unwrap();
                }
            }
        }
    }

    #[test]
    fn rows_with_blank_text_are_never_sampled() {
        let (mut rows, names) = intent_set("label", false);
        for r in rows.iter_mut().filter(|r| r.row_idx % 2 == 0) {
            r.row.insert("text".into(), json!("   "));
        }
        let cases = build_s1b(SRC, &rows, &names, 9, 20).unwrap();
        assert!(
            cases
                .iter()
                .all(|c| c.id.ends_with(|ch: char| "13579".contains(ch)))
        );
        assert!(build_s1b(SRC, &rows, &names, 9, 21).is_err());
    }

    #[test]
    fn a_null_text_is_skipped_like_a_blank_one_but_a_missing_or_numeric_one_is_an_error() {
        // Seen live: Sp1786 test row 3751 has `"text": null`.
        let mut rows = sentiment_rows();
        rows[3].row.insert("text".into(), json!(null));
        let cases = build_s2(SRC, &rows, &levels(), 1, 29).unwrap();
        assert!(cases.iter().all(|c| c.id != "s2_sentiment-3"));
        assert!(build_s2(SRC, &rows, &levels(), 1, 30).is_err());

        let (mut rows, names) = intent_set("label", false);
        rows[3].row.insert("text".into(), json!(null));
        assert_eq!(build_s1b(SRC, &rows, &names, 1, 39).unwrap().len(), 39);

        for bad in [json!(7), json!(["x"])] {
            let mut rows = sentiment_rows();
            rows[3].row.insert("text".into(), bad);
            let err = build_s2(SRC, &rows, &levels(), 1, 5)
                .unwrap_err()
                .to_string();
            assert!(err.contains("row 3") && err.contains("text"), "{err}");
        }
        let mut rows = sentiment_rows();
        rows[3].row.remove("text");
        assert!(build_s2(SRC, &rows, &levels(), 1, 5).is_err());
    }

    #[test]
    fn every_s1a_case_has_nine_options_even_when_gold_is_oos() {
        // 60 rows over 20 intents, `oos` at index 7: 3 of them are oos-gold.
        let names: Vec<String> = (0..20)
            .map(|i| {
                if i == 7 {
                    "oos".into()
                } else {
                    format!("intent_{i}")
                }
            })
            .collect();
        let rows: Vec<Row> = (0..60)
            .map(|i| row(i, json!({"text": format!("r{i}"), "intent": i % 20})))
            .collect();
        let cases = build_s1a(SRC, &rows, &names, 11, 50).unwrap();
        assert_eq!(cases.len(), 50);
        assert!(cases.iter().any(|c| gold(c) == "oos"));
        for c in &cases {
            assert_eq!(options(c).len(), 9, "{}: {:?}", c.id, options(c));
        }
    }

    #[test]
    fn draw_options_are_distinct_label_indices() {
        let mut rng = StdRng::seed_from_u64(3);
        for _ in 0..200 {
            for gold in 0..20 {
                for oos in [Some(7), None] {
                    let opts = draw_options(&mut rng, gold, 20, oos);
                    let distinct: std::collections::BTreeSet<usize> =
                        opts.iter().copied().collect();
                    assert_eq!(distinct.len(), opts.len(), "duplicate in {opts:?}");
                    assert!(opts.iter().all(|&l| l < 20), "{opts:?}");
                    assert!(opts.contains(&gold));
                    match oos {
                        Some(o) => {
                            assert_eq!(opts.len(), 9, "{opts:?}");
                            assert!(opts.contains(&o));
                        }
                        None => assert_eq!(opts.len(), 8, "{opts:?}"),
                    }
                }
            }
        }
    }

    #[test]
    fn one_seed_samples_different_rows_for_each_scenario() {
        // S2 and S3 over the same number of eligible rows: without the
        // scenario tag mixed into the seed they would pick the same indices.
        let s2_rows: Vec<Row> = (0..30)
            .map(|i| row(i, json!({"text": format!("t{i}"), "label": i % 3})))
            .collect();
        let s3_rows: Vec<Row> = (0..30)
            .map(|i| {
                row(
                    i,
                    json!({"premise": "p", "hypothesis": format!("h{i}"), "label": ([0, 2])[(i % 2) as usize]}),
                )
            })
            .collect();
        let ids = |cases: Vec<Case>| -> Vec<u64> {
            cases
                .iter()
                .map(|c| c.id.rsplit('-').next().unwrap().parse().unwrap())
                .collect()
        };
        let a = ids(build_s2(SRC, &s2_rows, &levels(), 2026, 5).unwrap());
        let b = ids(build_s3(SRC, &s3_rows, &snli_names(), 2026, 5).unwrap());
        assert_ne!(a, b);
    }

    #[test]
    fn rand_version_matches_the_lockfile() {
        let lock =
            std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../Cargo.lock"))
                .unwrap();
        let block = lock
            .split("[[package]]")
            .find(|b| b.contains("name = \"decision-bench\""))
            .expect("decision-bench in Cargo.lock");
        assert!(
            block.contains(&format!("\"rand {RAND_VERSION}\"")),
            "RAND_VERSION {RAND_VERSION} is stale:\n{block}"
        );
    }

    struct FakeDs;
    impl wiremock::Respond for FakeDs {
        fn respond(&self, req: &wiremock::Request) -> wiremock::ResponseTemplate {
            let q: std::collections::HashMap<_, _> = req.url.query_pairs().into_owned().collect();
            let (features, rows): (serde_json::Value, Vec<serde_json::Value>) =
                match q["dataset"].as_str() {
                    "clinc/clinc_oos" => {
                        let mut names: Vec<String> = (0..19).map(|i| format!("i{i}")).collect();
                        names.push("oos".into());
                        (
                            json!([{"name":"text","type":{"_type":"Value"}},{"name":"intent","type":{"names":names,"_type":"ClassLabel"}}]),
                            (0..30).map(|i| json!({"text": format!("c{i}"), "intent": i % 20})).collect(),
                        )
                    }
                    "legacy-datasets/banking77" => {
                        let names: Vec<String> = (0..12).map(|i| format!("b{i}")).collect();
                        (
                            json!([{"name":"text","type":{"_type":"Value"}},{"name":"label","type":{"names":names,"_type":"ClassLabel"}}]),
                            (0..30).map(|i| json!({"text": format!("b{i}"), "label": i % 12})).collect(),
                        )
                    }
                    "Sp1786/multiclass-sentiment-analysis-dataset" => (
                        json!([{"name":"text","type":{"_type":"Value"}},{"name":"label","type":{"dtype":"int64","_type":"Value"}}]),
                        (0..30).map(|i| json!({"text": format!("s{i}"), "label": i % 3, "sentiment": SENTIMENT_LEVELS[i % 3]})).collect(),
                    ),
                    "stanfordnlp/snli" => (
                        json!([{"name":"premise","type":{"_type":"Value"}},{"name":"hypothesis","type":{"_type":"Value"}},{"name":"label","type":{"names":["entailment","neutral","contradiction"],"_type":"ClassLabel"}}]),
                        (0..30).map(|i| json!({"premise": format!("p{i}"), "hypothesis": format!("h{i}"), "label": ([0, 1, 2, -1])[i % 4]})).collect(),
                    ),
                    other => panic!("unexpected dataset {other}"),
                };
            let offset: usize = q["offset"].parse().unwrap();
            let length: usize = q["length"].parse().unwrap();
            let page: Vec<_> = rows
                .iter()
                .enumerate()
                .skip(offset)
                .take(length)
                .map(|(i, r)| json!({"row_idx": i, "row": r, "truncated_cells": []}))
                .collect();
            wiremock::ResponseTemplate::new(200).set_body_json(json!({
                "features": features, "rows": page, "num_rows_total": rows.len(),
                "num_rows_per_page": 100, "partial": false
            }))
        }
    }

    /// A fake datasets-server and hub serving every entry of [`DATASETS`]
    /// at sha `feedfacecafe0001` + 24 zeros, each with its expected licence unless
    /// `licence_for` overrides one.
    async fn fake_hf(
        licence_for: Option<(&str, &str)>,
    ) -> (crate::fetch::Hf, [wiremock::MockServer; 2]) {
        use wiremock::matchers::path;
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let ds = MockServer::start().await;
        Mock::given(path("/rows"))
            .respond_with(FakeDs)
            .mount(&ds)
            .await;
        let hub = MockServer::start().await;
        for spec in DATASETS {
            let license = match licence_for {
                Some((id, l)) if id == spec.id => l,
                _ => spec.license,
            };
            Mock::given(path(format!("/api/datasets/{}", spec.id)))
                .respond_with(ResponseTemplate::new(200).set_body_json(
                    json!({"sha": "feedfacecafe0001000000000000000000000000", "cardData": {"license": [license]}}),
                ))
                .mount(&hub)
                .await;
        }
        let hf = crate::fetch::Hf {
            client: reqwest::Client::new(),
            ds_base: ds.uri(),
            hub_base: hub.uri(),
            pacing: crate::fetch::Pacing::ZERO,
        };
        (hf, [ds, hub])
    }

    async fn fetch_into(
        hf: &crate::fetch::Hf,
        cache: &std::path::Path,
        out: &std::path::Path,
        seed: u64,
    ) -> anyhow::Result<Manifest> {
        let opts = FetchOptions {
            cache_root: cache,
            out,
            seed,
            per_scenario: 5,
        };
        fetch_cases(hf, &opts).await
    }

    const CASE_FILES: [&str; 5] = [
        "s1a.jsonl",
        "s1b.jsonl",
        "s2.jsonl",
        "s3.jsonl",
        "manifest.json",
    ];

    #[tokio::test]
    async fn fetch_cases_writes_four_case_files_and_a_reproducible_manifest() {
        let (hf, _servers) = fake_hf(None).await;
        let cache = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let (a, b, c) = (
            root.path().join("a"),
            root.path().join("b"),
            root.path().join("c"),
        );
        let m = fetch_into(&hf, cache.path(), &a, 2026).await.unwrap();
        fetch_into(&hf, cache.path(), &b, 2026).await.unwrap();
        fetch_into(&hf, cache.path(), &c, 1).await.unwrap();

        assert_eq!(m.seed, 2026);
        assert_eq!(m.per_scenario, 5);
        assert_eq!(m.rand_version, RAND_VERSION);
        assert_eq!(m.datasets.len(), 4);
        for (d, spec) in m.datasets.iter().zip(DATASETS) {
            assert_eq!(
                (d.id.as_str(), d.sha.as_str()),
                (spec.id, "feedfacecafe0001000000000000000000000000")
            );
            assert_eq!(d.license, spec.license);
            let rows_file = crate::fetch::cache_dir(cache.path(), spec.id, &d.sha)
                .join(format!("{}-{}.jsonl", spec.config, spec.split));
            let bytes = std::fs::read(&rows_file).unwrap();
            assert_eq!(d.rows_sha256, crate::fetch::sha256_hex(&bytes), "{}", d.id);
        }
        assert_eq!(
            m.files
                .iter()
                .map(|f| (f.file.as_str(), f.cases))
                .collect::<Vec<_>>(),
            [
                ("s1a.jsonl", 5),
                ("s1b.jsonl", 5),
                ("s2.jsonl", 5),
                ("s3.jsonl", 5)
            ]
        );
        for f in CASE_FILES {
            let bytes = std::fs::read(a.join(f)).unwrap();
            assert_eq!(bytes, std::fs::read(b.join(f)).unwrap(), "{f}");
            assert_ne!(bytes, std::fs::read(c.join(f)).unwrap(), "{f}");
            if f != "manifest.json" {
                let (_, cases) = crate::case::read_case_file(&a.join(f)).unwrap();
                assert_eq!(cases.len(), 5);
                let id = DATASETS.iter().find(|d| d.file == f).unwrap().id;
                for case in &cases {
                    assert!(case.source.starts_with(&format!("{id}@feedfaceca:test#")));
                    assert!(
                        case.source
                            .ends_with(&format!(" seed=2026 rand={RAND_VERSION}")),
                        "{}",
                        case.source
                    );
                }
            }
        }
        let on_disk: Manifest =
            serde_json::from_slice(&std::fs::read(a.join("manifest.json")).unwrap()).unwrap();
        assert_eq!(on_disk, m);
    }

    #[tokio::test]
    async fn a_licence_other_than_the_expected_one_fails_the_fetch() {
        let (hf, _servers) = fake_hf(Some(("clinc/clinc_oos", "mit"))).await;
        let cache = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        let err = fetch_into(&hf, cache.path(), &out.path().join("cases"), 1)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("clinc/clinc_oos") && err.contains("mit") && err.contains("cc-by-3.0"),
            "{err}"
        );
    }

    fn listing(dir: &std::path::Path) -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        v.sort();
        v
    }

    #[tokio::test]
    async fn the_case_set_is_replaced_whole_and_a_failed_fetch_keeps_the_old_one() {
        let (hf, _servers) = fake_hf(None).await;
        let cache = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let out = root.path().join("cases");

        fetch_into(&hf, cache.path(), &out, 1).await.unwrap();
        // A file from some older run must not survive the next one.
        std::fs::write(out.join("s4-stale.jsonl"), "x").unwrap();
        let first = std::fs::read(out.join("s1a.jsonl")).unwrap();
        fetch_into(&hf, cache.path(), &out, 2).await.unwrap();
        let mut want: Vec<String> = CASE_FILES.iter().map(|s| s.to_string()).collect();
        want.sort();
        assert_eq!(listing(&out), want);
        assert_ne!(std::fs::read(out.join("s1a.jsonl")).unwrap(), first);
        assert_eq!(listing(root.path()), ["cases"], "scratch dirs left behind");

        // A failing fetch leaves the previous set untouched.
        let before: Vec<Vec<u8>> = CASE_FILES
            .iter()
            .map(|f| std::fs::read(out.join(f)).unwrap())
            .collect();
        let (bad, _bad_servers) = fake_hf(Some(("stanfordnlp/snli", "mit"))).await;
        let empty_cache = tempfile::tempdir().unwrap();
        assert!(fetch_into(&bad, empty_cache.path(), &out, 3).await.is_err());
        let after: Vec<Vec<u8>> = CASE_FILES
            .iter()
            .map(|f| std::fs::read(out.join(f)).unwrap())
            .collect();
        assert_eq!(before, after);
        assert_eq!(listing(root.path()), ["cases"], "scratch dirs left behind");
    }

    #[tokio::test]
    async fn fetch_refuses_to_replace_a_directory_that_is_not_a_case_set() {
        let (hf, _servers) = fake_hf(None).await;
        let cache = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let out = root.path().join("mine");
        std::fs::create_dir_all(&out).unwrap();
        std::fs::write(out.join("notes.txt"), "keep me").unwrap();
        let err = fetch_into(&hf, cache.path(), &out, 1)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("manifest.json") && err.contains("mine"),
            "{err}"
        );
        assert_eq!(
            std::fs::read_to_string(out.join("notes.txt")).unwrap(),
            "keep me"
        );

        // An empty directory is fine.
        let empty = root.path().join("empty");
        std::fs::create_dir_all(&empty).unwrap();
        fetch_into(&hf, cache.path(), &empty, 1).await.unwrap();
    }

    // --- write_case_set: crash recovery and honest failure paths ---

    fn tiny_set() -> (Vec<(DatasetSpec, Vec<Case>)>, Manifest) {
        let (rows, names) = intent_set("label", false);
        let cases = build_s1b(SRC, &rows, &names, 1, 2).unwrap();
        let manifest = Manifest {
            seed: 1,
            per_scenario: 2,
            rand_version: RAND_VERSION.into(),
            datasets: vec![],
            files: vec![],
        };
        (vec![(DATASETS[1], cases)], manifest)
    }

    /// A previous case set: `manifest.json` + `s1b.jsonl` holding `marker`.
    fn old_set(dir: &std::path::Path, marker: &str) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("manifest.json"), "{}").unwrap();
        std::fs::write(dir.join("s1b.jsonl"), marker).unwrap();
    }

    fn injected() -> std::io::Error {
        std::io::Error::other("injected failure")
    }
    fn ends(p: &std::path::Path, suffix: &str) -> bool {
        p.to_string_lossy().ends_with(suffix)
    }
    fn rename_fails_from_tmp(a: &std::path::Path, b: &std::path::Path) -> std::io::Result<()> {
        if ends(a, ".tmp") {
            return Err(injected());
        }
        std::fs::rename(a, b)
    }
    fn rename_fails_from_tmp_and_old(
        a: &std::path::Path,
        b: &std::path::Path,
    ) -> std::io::Result<()> {
        if ends(a, ".tmp") || ends(a, ".old") {
            return Err(injected());
        }
        std::fs::rename(a, b)
    }
    fn remove_fails_on_old(p: &std::path::Path) -> std::io::Result<()> {
        if ends(p, ".old") {
            return Err(injected());
        }
        std::fs::remove_dir_all(p)
    }

    #[test]
    fn a_set_stranded_in_old_by_a_crash_is_restored_not_deleted() {
        // Crash between `rename(out, old)` and `rename(tmp, out)`: no `out`,
        // the only copy of the previous set in `<out>.old`. Make this run's
        // swap fail too: the previous set must come back as `out`.
        let root = tempfile::tempdir().unwrap();
        let out = root.path().join("cases");
        old_set(&root.path().join("cases.old"), "previous");
        let (built, m) = tiny_set();
        let fs = SwapFs {
            rename: rename_fails_from_tmp,
            ..SwapFs::REAL
        };
        assert!(write_case_set_with(&fs, &out, &built, &m).is_err());
        assert_eq!(
            std::fs::read_to_string(out.join("s1b.jsonl")).unwrap(),
            "previous"
        );

        // And a normal run after the crash replaces it, leaving no scratch.
        let root = tempfile::tempdir().unwrap();
        let out = root.path().join("cases");
        old_set(&root.path().join("cases.old"), "previous");
        let warnings = write_case_set(&out, &built, &m).unwrap();
        assert!(
            warnings.iter().any(|w| w.contains("restored")),
            "{warnings:?}"
        );
        assert_ne!(
            std::fs::read_to_string(out.join("s1b.jsonl")).unwrap(),
            "previous"
        );
        assert_eq!(listing(root.path()), ["cases"]);
    }

    #[test]
    fn a_leftover_tmp_or_old_that_is_not_ours_is_never_deleted() {
        for suffix in [".old", ".tmp"] {
            let root = tempfile::tempdir().unwrap();
            let out = root.path().join("cases");
            old_set(&out, "current");
            let theirs = root.path().join(format!("cases{suffix}"));
            std::fs::create_dir_all(&theirs).unwrap();
            std::fs::write(theirs.join("notes.txt"), "keep me").unwrap();
            let (built, m) = tiny_set();
            let err = write_case_set(&out, &built, &m).unwrap_err().to_string();
            assert!(
                err.contains(&theirs.display().to_string()) && err.contains("manifest.json"),
                "{err}"
            );
            assert_eq!(
                std::fs::read_to_string(theirs.join("notes.txt")).unwrap(),
                "keep me"
            );
            assert_eq!(
                std::fs::read_to_string(out.join("s1b.jsonl")).unwrap(),
                "current"
            );
        }
        // Our own leftovers (empty, or holding a manifest) are cleared.
        let root = tempfile::tempdir().unwrap();
        let out = root.path().join("cases");
        old_set(&out, "current");
        old_set(&root.path().join("cases.old"), "stale");
        std::fs::create_dir_all(root.path().join("cases.tmp")).unwrap();
        let (built, m) = tiny_set();
        write_case_set(&out, &built, &m).unwrap();
        assert_eq!(listing(root.path()), ["cases"]);
    }

    #[test]
    fn a_failed_rollback_reports_both_errors_and_where_the_old_set_is() {
        let root = tempfile::tempdir().unwrap();
        let out = root.path().join("cases");
        old_set(&out, "current");
        let (built, m) = tiny_set();
        let fs = SwapFs {
            rename: rename_fails_from_tmp_and_old,
            ..SwapFs::REAL
        };
        // The first rename (out -> old) must succeed: it is not from .tmp/.old.
        let err = format!(
            "{:#}",
            write_case_set_with(&fs, &out, &built, &m).unwrap_err()
        );
        let old = root.path().join("cases.old");
        assert!(
            err.matches("injected failure").count() == 2
                && err.contains(&old.display().to_string())
                && err.contains("restoring"),
            "{err}"
        );
        assert_eq!(
            std::fs::read_to_string(old.join("s1b.jsonl")).unwrap(),
            "current"
        );
    }

    #[test]
    fn a_failed_cleanup_after_a_successful_swap_is_a_warning_not_an_error() {
        let root = tempfile::tempdir().unwrap();
        let out = root.path().join("cases");
        old_set(&out, "current");
        let (built, m) = tiny_set();
        let fs = SwapFs {
            remove_dir_all: remove_fails_on_old,
            ..SwapFs::REAL
        };
        let warnings = write_case_set_with(&fs, &out, &built, &m).unwrap();
        let old = root.path().join("cases.old");
        assert!(
            warnings
                .iter()
                .any(|w| w.contains(&old.display().to_string()) && w.contains("injected failure")),
            "{warnings:?}"
        );
        assert!(out.join("manifest.json").exists());
        assert_ne!(
            std::fs::read_to_string(out.join("s1b.jsonl")).unwrap(),
            "current"
        );
    }
}
