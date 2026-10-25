//! `corpus.lock.json`: the committed record of *which* corpus a ranking was
//! measured on.
//!
//! `fetch` writes the fetched manifest to gitignored directories only, so a
//! ranking built from it could not be rebuilt from the repo. The lock is the
//! part that is committed: dataset revisions, row hashes, the sampling seed and
//! the per-file case counts. `fetch` refuses to overwrite a lock it disagrees
//! with; `run` refuses to run cases that disagree with the lock.

use std::path::Path;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};

use crate::fetch::write_atomic;
use crate::scenarios::{Manifest, ManifestDataset, ManifestFile};

/// Bumped when the lock's shape changes, never for a new optional field.
pub const SCHEMA: u32 = 1;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CorpusLock {
    pub schema: u32,
    pub seed: u64,
    pub per_scenario: usize,
    pub rand_version: String,
    pub datasets: Vec<ManifestDataset>,
    pub files: Vec<ManifestFile>,
}

impl CorpusLock {
    pub fn from_manifest(m: &Manifest) -> CorpusLock {
        CorpusLock {
            schema: SCHEMA,
            seed: m.seed,
            per_scenario: m.per_scenario,
            rand_version: m.rand_version.clone(),
            datasets: m.datasets.clone(),
            files: m.files.clone(),
        }
    }

    pub fn to_manifest(&self) -> Manifest {
        Manifest {
            seed: self.seed,
            per_scenario: self.per_scenario,
            rand_version: self.rand_version.clone(),
            datasets: self.datasets.clone(),
            files: self.files.clone(),
        }
    }

    /// Pretty JSON with a trailing newline, so a diff is readable.
    pub fn to_json(&self) -> String {
        format!(
            "{}\n",
            serde_json::to_string_pretty(self).expect("lock serializes")
        )
    }

    pub fn read(path: &Path) -> anyhow::Result<CorpusLock> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading corpus lock {}", path.display()))?;
        let lock: CorpusLock = serde_json::from_str(&text)
            .with_context(|| format!("parsing corpus lock {}", path.display()))?;
        if lock.schema != SCHEMA {
            bail!(
                "{}: corpus lock schema {} is not {SCHEMA}",
                path.display(),
                lock.schema
            );
        }
        Ok(lock)
    }

    /// Write atomically, creating the lock's directory if needed.
    pub fn write(path: &Path, manifest: &Manifest) -> anyhow::Result<()> {
        if let Some(dir) = path.parent()
            && !dir.as_os_str().is_empty()
        {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        }
        write_atomic(
            path,
            CorpusLock::from_manifest(manifest).to_json().as_bytes(),
        )
        .with_context(|| format!("writing corpus lock {}", path.display()))
    }
}

/// Every way `manifest` disagrees with `lock`, as one line each. Empty means
/// they match.
pub fn differences(manifest: &Manifest, lock: &CorpusLock) -> Vec<String> {
    let mut out = Vec::new();
    if manifest.seed != lock.seed {
        out.push(format!(
            "sampling seed: fetched {}, lock {}",
            manifest.seed, lock.seed
        ));
    }
    if manifest.per_scenario != lock.per_scenario {
        out.push(format!(
            "cases per scenario: fetched {}, lock {}",
            manifest.per_scenario, lock.per_scenario
        ));
    }
    if manifest.rand_version != lock.rand_version {
        out.push(format!(
            "rand version: fetched {}, lock {}",
            manifest.rand_version, lock.rand_version
        ));
    }
    for locked in &lock.datasets {
        match manifest.datasets.iter().find(|d| d.id == locked.id) {
            None => out.push(format!("dataset {}: missing from the fetch", locked.id)),
            Some(d) => {
                if d.sha != locked.sha {
                    out.push(format!(
                        "dataset {}: revision {} → {}",
                        d.id, locked.sha, d.sha
                    ));
                }
                if d.rows_sha256 != locked.rows_sha256 {
                    out.push(format!(
                        "dataset {}: rows sha256 {} → {}",
                        d.id, locked.rows_sha256, d.rows_sha256
                    ));
                }
                if d.num_rows != locked.num_rows {
                    out.push(format!(
                        "dataset {}: {} rows → {}",
                        d.id, locked.num_rows, d.num_rows
                    ));
                }
            }
        }
    }
    for d in &manifest.datasets {
        if !lock.datasets.iter().any(|l| l.id == d.id) {
            out.push(format!("dataset {}: not in the lock", d.id));
        }
    }
    for locked in &lock.files {
        match manifest.files.iter().find(|f| f.file == locked.file) {
            None => out.push(format!("{}: missing from the fetch", locked.file)),
            Some(f) if f.cases != locked.cases => {
                out.push(format!("{}: {} cases → {}", f.file, locked.cases, f.cases))
            }
            Some(_) => {}
        }
    }
    for f in &manifest.files {
        if !lock.files.iter().any(|l| l.file == f.file) {
            out.push(format!("{}: not in the lock", f.file));
        }
    }
    out
}

/// Fail unless `manifest` is exactly the corpus the lock names.
pub fn verify(manifest: &Manifest, lock: &CorpusLock, lock_path: &Path) -> anyhow::Result<()> {
    let diff = differences(manifest, lock);
    if diff.is_empty() {
        return Ok(());
    }
    bail!(
        "the fetched cases do not match the corpus lock {}:\n  {}\n\
         A ranking names the corpus it was measured on; rebuild the cases from the locked \
         revisions, or — only when the change is intended — re-run `decision-bench fetch \
         --rewrite-lock`.",
        lock_path.display(),
        diff.join("\n  ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> Manifest {
        Manifest {
            seed: 2026,
            per_scenario: 200,
            rand_version: "0.8.5".into(),
            datasets: vec![ManifestDataset {
                id: "legacy-datasets/banking77".into(),
                sha: "aaa".into(),
                license: "cc-by-4.0".into(),
                config: "default".into(),
                split: "test".into(),
                num_rows: 3080,
                rows_sha256: "bbb".into(),
            }],
            files: vec![ManifestFile {
                file: "s1b.jsonl".into(),
                scenario: crate::case::Scenario::S1bBanking77,
                dataset: "legacy-datasets/banking77".into(),
                cases: 200,
            }],
        }
    }

    #[test]
    fn a_matching_manifest_verifies_and_round_trips() {
        let m = manifest();
        let lock = CorpusLock::from_manifest(&m);
        assert_eq!(lock.to_manifest(), m);
        assert!(differences(&m, &lock).is_empty());
        assert!(verify(&m, &lock, Path::new("corpus.lock.json")).is_ok());
        assert!(lock.to_json().ends_with("}\n"));
        assert_eq!(
            serde_json::from_str::<CorpusLock>(&lock.to_json()).unwrap(),
            lock
        );
    }

    #[test]
    fn a_changed_revision_case_count_or_seed_is_reported_by_name() {
        let lock = CorpusLock::from_manifest(&manifest());
        let mut fetched = manifest();
        fetched.datasets[0].sha = "ccc".into();
        fetched.files[0].cases = 199;
        fetched.seed = 7;
        let diff = differences(&fetched, &lock);
        assert_eq!(diff.len(), 3, "{diff:?}");
        assert!(
            diff.iter().any(|d| d.contains("revision aaa → ccc")),
            "{diff:?}"
        );
        assert!(
            diff.iter()
                .any(|d| d.contains("s1b.jsonl: 200 cases → 199")),
            "{diff:?}"
        );
        assert!(diff.iter().any(|d| d.contains("sampling seed")), "{diff:?}");
        let err = format!(
            "{:#}",
            verify(&fetched, &lock, Path::new("lock.json")).unwrap_err()
        );
        assert!(
            err.contains("lock.json") && err.contains("--rewrite-lock"),
            "{err}"
        );
    }

    #[test]
    fn a_dataset_or_file_only_one_side_has_is_reported() {
        let lock = CorpusLock::from_manifest(&manifest());
        let mut fetched = manifest();
        fetched.datasets.clear();
        fetched.files.clear();
        let diff = differences(&fetched, &lock);
        assert!(
            diff.iter().any(|d| d.contains("missing from the fetch")),
            "{diff:?}"
        );
        let added = manifest();
        let mut lock = lock;
        lock.datasets.clear();
        lock.files.clear();
        let diff = differences(&added, &lock);
        assert!(
            diff.iter().any(|d| d.contains("not in the lock")),
            "{diff:?}"
        );
    }
}
