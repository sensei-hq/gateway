//! Hugging Face datasets-server + hub client, the on-disk cache, and the sha
//! guard.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, anyhow, bail};

use serde::{Deserialize, Serialize};

pub const DATASETS_SERVER: &str = "https://datasets-server.huggingface.co";
pub const HUB: &str = "https://huggingface.co";

/// The waits a fetch makes: between page requests, and before the single
/// retry of a transient status. Defaults to [`PAGE_GAP`] / [`RETRY_AFTER`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pacing {
    pub page_gap: Duration,
    pub retry_after: Duration,
}

impl Default for Pacing {
    fn default() -> Self {
        Pacing {
            page_gap: PAGE_GAP,
            retry_after: RETRY_AFTER,
        }
    }
}

impl Pacing {
    /// No waits (tests).
    pub const ZERO: Pacing = Pacing {
        page_gap: Duration::ZERO,
        retry_after: Duration::ZERO,
    };
}

/// A Hugging Face endpoint pair (datasets-server + hub) and how to pace it.
#[derive(Debug, Clone)]
pub struct Hf {
    pub client: reqwest::Client,
    pub ds_base: String,
    pub hub_base: String,
    pub pacing: Pacing,
}

impl Hf {
    /// The real endpoints with the default pacing.
    pub fn live(client: reqwest::Client) -> Self {
        Hf {
            client,
            ds_base: DATASETS_SERVER.to_string(),
            hub_base: HUB.to_string(),
            pacing: Pacing::default(),
        }
    }

    pub async fn hub_info(&self, id: &str) -> anyhow::Result<HubInfo> {
        hub_info(self, id).await
    }

    pub async fn fetch_rows(
        &self,
        id: &str,
        config: &str,
        split: &str,
    ) -> anyhow::Result<RowsPage> {
        fetch_rows_saving(self, id, config, split, None).await
    }

    pub async fn load_or_fetch(
        &self,
        cache_root: &Path,
        id: &str,
        config: &str,
        split: &str,
    ) -> anyhow::Result<Dataset> {
        load_or_fetch(self, cache_root, id, config, split).await
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Feature {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Row {
    pub row_idx: u64,
    pub row: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RowsPage {
    pub features: Vec<Feature>,
    pub rows: Vec<Row>,
    pub num_rows_total: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HubInfo {
    pub sha: String,
    pub license: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Meta {
    pub id: String,
    pub sha: String,
    pub license: String,
    pub config: String,
    pub split: String,
    pub fetched_at: u64,
    pub num_rows: u64,
    pub features: Vec<Feature>,
    /// sha256 (hex) of the cached `<config>-<split>.jsonl`. Absent in a cache
    /// written before it was recorded; backfilled on the next load.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Dataset {
    pub meta: Meta,
    pub rows: Vec<Row>,
}

#[derive(Deserialize)]
struct RawPage {
    #[serde(default)]
    partial: bool,
    features: Vec<Feature>,
    rows: Vec<RawRow>,
    num_rows_total: u64,
}

#[derive(Deserialize)]
struct RawRow {
    row_idx: u64,
    row: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    truncated_cells: Vec<String>,
}

/// Parse one datasets-server `/rows` response. A row with truncated cells is
/// an error: a cut-off text would silently become a different case.
pub fn parse_rows_page(json: &str) -> anyhow::Result<RowsPage> {
    let raw: RawPage =
        serde_json::from_str(json).context("parsing a datasets-server /rows page")?;
    if raw.partial {
        bail!("datasets-server marked the page partial (an incomplete conversion); refusing it");
    }
    let mut rows = Vec::with_capacity(raw.rows.len());
    for r in raw.rows {
        if !r.truncated_cells.is_empty() {
            bail!(
                "row {} has truncated cells {:?}; refusing partial data",
                r.row_idx,
                r.truncated_cells
            );
        }
        rows.push(Row {
            row_idx: r.row_idx,
            row: r.row,
        });
    }
    Ok(RowsPage {
        features: raw.features,
        rows,
        num_rows_total: raw.num_rows_total,
    })
}

/// The label names of a `ClassLabel` feature, in index order.
pub fn class_names(features: &[Feature], field: &str) -> anyhow::Result<Vec<String>> {
    let f = features
        .iter()
        .find(|f| f.name == field)
        .with_context(|| format!("no feature named {field:?}"))?;
    if f.ty.get("_type").and_then(|t| t.as_str()) != Some("ClassLabel") {
        bail!("feature {field:?} is not a ClassLabel: {}", f.ty);
    }
    serde_json::from_value(f.ty.get("names").cloned().unwrap_or_default())
        .with_context(|| format!("feature {field:?}: ClassLabel names"))
}

/// The first 300 chars of an error body, noting the full size when cut.
fn clip(body: &str) -> String {
    const MAX: usize = 300;
    if body.chars().count() <= MAX {
        return body.to_string();
    }
    let head: String = body.chars().take(MAX).collect();
    format!("{head}… ({} bytes)", body.len())
}

/// Delay before the single retry of a transient status (see [`is_transient`]).
const RETRY_AFTER: Duration = Duration::from_secs(2);

/// HTTP 429, or a 502/503/504 from the datasets-server proxy (seen live
/// mid-fetch, 2026-10-20).
fn is_transient(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 429 | 502 | 503 | 504)
}

/// Send `req`, retrying once after `retry_after` on a transient status. Any other
/// non-success status, or a second transient one, is an error carrying status
/// and body.
async fn get_text(
    req: reqwest::RequestBuilder,
    what: &str,
    retry_after: Duration,
) -> anyhow::Result<String> {
    let mut retried = false;
    loop {
        let attempt = req
            .try_clone()
            .context("request cannot be retried")?
            .send()
            .await
            .with_context(|| format!("{what}: request failed"))?;
        let status = attempt.status();
        if is_transient(status) && !retried {
            retried = true;
            tokio::time::sleep(retry_after).await;
            continue;
        }
        let body = attempt
            .text()
            .await
            .with_context(|| format!("{what}: reading the body"))?;
        if !status.is_success() {
            let again = if retried { " (after one retry)" } else { "" };
            bail!("{what}: HTTP {status}{again}: {}", clip(&body));
        }
        return Ok(body);
    }
}

#[derive(Deserialize)]
struct RawHub {
    sha: String,
    #[serde(rename = "cardData", default)]
    card_data: Option<serde_json::Value>,
}

/// The dataset's current revision sha and its card licence (a list is joined
/// with ", ").
async fn hub_info(hf: &Hf, id: &str) -> anyhow::Result<HubInfo> {
    let what = format!("hub info for {id}");
    let req = hf.client.get(format!("{}/api/datasets/{id}", hf.hub_base));
    let body = get_text(req, &what, hf.pacing.retry_after).await?;
    let raw: RawHub = serde_json::from_str(&body).with_context(|| format!("{what}: parsing"))?;
    // The sha names a cache directory (`cache_dir`), so it must be a plain
    // revision hash, never a path.
    let hex = |c: char| c.is_ascii_digit() || ('a'..='f').contains(&c);
    if !matches!(raw.sha.len(), 40 | 64) || !raw.sha.chars().all(hex) {
        bail!(
            "{what}: sha {:?} is not a hex revision (40 or 64 lowercase hex chars)",
            raw.sha
        );
    }
    let license = match raw.card_data.as_ref().and_then(|c| c.get("license")) {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Array(a)) if !a.is_empty() => a
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .with_context(|| format!("{what}: licence {v} is not a string"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .join(", "),
        other => bail!("{what}: no cardData.license (got {other:?})"),
    };
    Ok(HubInfo {
        sha: raw.sha,
        license,
    })
}

/// Datasets-server's maximum page size.
pub const PAGE_LEN: u64 = 100;

/// Pause between consecutive page requests. Measured 2026-10-20: unpaced,
/// datasets-server (CloudFront) answered 429 after ~85 requests in ~55 s and
/// stayed blocked ~1.5 min with no `Retry-After`, which one 2 s retry cannot
/// ride out.
pub const PAGE_GAP: Duration = Duration::from_secs(1);

/// Every row of `config`/`split`, paged `PAGE_LEN` at a time until the first
/// page's `num_rows_total` is reached, `hf.pacing.page_gap` apart. With
/// `pages`, each page body is saved verbatim as `<pages>/<offset>.json` and an
/// already-saved page is read instead of requested again, so a run stopped by
/// a rate limit resumes where it failed.
async fn fetch_rows_saving(
    hf: &Hf,
    id: &str,
    config: &str,
    split: &str,
    pages: Option<&Path>,
) -> anyhow::Result<RowsPage> {
    if let Some(dir) = pages {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let mut requested = false;
    // Whether the first page, which every later page is compared against for
    // `num_rows_total` and features, was read from a saved file.
    let mut baseline_resumed = false;
    let mut features: Option<Vec<Feature>> = None;
    let mut total: Option<u64> = None;
    let mut rows: Vec<Row> = Vec::new();
    let mut offset = 0u64;
    while total.is_none_or(|t| offset < t) {
        let length = total.map_or(PAGE_LEN, |t| PAGE_LEN.min(t - offset));
        let what = format!("{id} {config}/{split} rows at offset {offset}");
        let mut url = reqwest::Url::parse(&format!("{}/rows", hf.ds_base))
            .with_context(|| format!("bad datasets-server base {:?}", hf.ds_base))?;
        url.query_pairs_mut()
            .append_pair("dataset", id)
            .append_pair("config", config)
            .append_pair("split", split)
            .append_pair("offset", &offset.to_string())
            .append_pair("length", &length.to_string());
        let saved = pages.map(|d| d.join(format!("{offset}.json")));
        let resumed = saved.as_deref().filter(|p| p.exists());
        let body = match resumed {
            Some(p) => {
                std::fs::read_to_string(p).with_context(|| format!("reading {}", p.display()))?
            }
            None => {
                if requested {
                    tokio::time::sleep(hf.pacing.page_gap).await;
                }
                requested = true;
                let body = get_text(hf.client.get(url), &what, hf.pacing.retry_after).await?;
                if let Some(p) = &saved {
                    // Validate before saving, so a bad page is never resumed from.
                    parse_rows_page(&body).with_context(|| what.clone())?;
                    write_atomic(p, body.as_bytes())?;
                }
                body
            }
        };
        let page = parse_rows_page(&body).with_context(|| match resumed {
            Some(p) => format!(
                "{what}: saved page {} is corrupt; delete it to re-fetch",
                p.display()
            ),
            None => what.clone(),
        })?;
        if total.is_none() {
            baseline_resumed = resumed.is_some();
        }
        if let Err(fault) = check_page(&what, offset, total, features.as_deref(), &page) {
            // Blame the saved pages only when the failing comparison involves
            // one: the failing page was resumed, or it disagrees with a resumed
            // baseline. Kept, such a page would fail every later fetch
            // identically. A failure within a live page keeps the saved pages.
            let saved_involved = resumed.is_some() || (fault.vs_baseline && baseline_resumed);
            let e = fault.error;
            if let (true, Some(dir)) = (saved_involved, pages) {
                let cleanup = discard_pages(dir);
                return Err(e.context(format!(
                    "saved pages {} from an earlier run disagree with the live dataset; \
                     discarded them{cleanup}, re-run to fetch afresh",
                    dir.display()
                )));
            }
            return Err(e);
        }
        total.get_or_insert(page.num_rows_total);
        features.get_or_insert(page.features);
        offset += page.rows.len() as u64;
        rows.extend(page.rows);
    }
    Ok(RowsPage {
        features: features.unwrap_or_default(),
        rows,
        num_rows_total: total.unwrap_or(0),
    })
}

/// Why a page failed `check_page`.
struct PageFault {
    error: anyhow::Error,
    /// The page disagrees with the first page's `num_rows_total` or
    /// features, so either side may be the stale one; otherwise the fault is
    /// within the page itself.
    vs_baseline: bool,
}

/// `page` (at `offset`) agrees with the pages before it: the same
/// `num_rows_total` and features, rows before the total, and contiguous
/// `row_idx`.
fn check_page(
    what: &str,
    offset: u64,
    total: Option<u64>,
    features: Option<&[Feature]>,
    page: &RowsPage,
) -> Result<(), PageFault> {
    let fault = |vs_baseline, error| Err(PageFault { error, vs_baseline });
    if let Some(t) = total
        && t != page.num_rows_total
    {
        return fault(
            true,
            anyhow!(
                "{what}: num_rows_total changed from {t} to {} mid-fetch",
                page.num_rows_total
            ),
        );
    }
    if let Some(f) = features
        && *f != page.features[..]
    {
        return fault(true, anyhow!("{what}: features changed mid-fetch"));
    }
    if page.rows.is_empty() && offset < page.num_rows_total {
        return fault(
            false,
            anyhow!(
                "{what}: no rows returned before num_rows_total {}",
                page.num_rows_total
            ),
        );
    }
    for (i, r) in page.rows.iter().enumerate() {
        let want = offset + i as u64;
        if r.row_idx != want {
            return fault(
                false,
                anyhow!("{what}: expected row_idx {want}, got {}", r.row_idx),
            );
        }
    }
    Ok(())
}

/// Remove the saved pages `dir`; empty on success, otherwise a note naming
/// the failure for the caller's error.
fn discard_pages(dir: &Path) -> String {
    std::fs::remove_dir_all(dir)
        .err()
        .map(|e| format!(" (and removing {} failed: {e})", dir.display()))
        .unwrap_or_default()
}

/// `<user cache dir>/decision-bench`.
pub fn default_cache_root() -> anyhow::Result<PathBuf> {
    Ok(dirs::cache_dir()
        .context("no user cache directory on this platform")?
        .join("decision-bench"))
}

/// `<root>/<id with "/" → "__">@<sha>`.
pub fn cache_dir(root: &Path, id: &str, sha: &str) -> PathBuf {
    root.join(format!("{}@{sha}", id.replace('/', "__")))
}

/// The sha guard: a cache directory's `meta.json` must record the live hub
/// sha. `Ok(None)` when there is no `meta.json` (nothing cached yet).
pub fn check_cached_sha(dir: &Path, live_sha: &str) -> anyhow::Result<Option<Meta>> {
    let path = dir.join("meta.json");
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let meta: Meta =
        serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    if meta.sha != live_sha {
        bail!(
            "stale cache {}: recorded under sha {} but the hub now reports {live_sha}; \
             datasets-server serves only the current revision, so delete the directory to re-fetch",
            dir.display(),
            meta.sha
        );
    }
    Ok(Some(meta))
}

/// Write `bytes` to a sibling temp file unique to this process and moment
/// (`<path>.<pid>.<nanos>.tmp`), `sync_all` it, then rename over `path`, so a
/// crash leaves either the old file or the new one, and two writers never
/// share a temp file. A failed write removes its temp file.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    use std::io::Write;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(format!(".{}.{nanos}.tmp", std::process::id()));
    let tmp = PathBuf::from(tmp);
    let result = (|| -> anyhow::Result<()> {
        let mut f =
            std::fs::File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
        f.write_all(bytes)
            .with_context(|| format!("writing {}", tmp.display()))?;
        f.sync_all()
            .with_context(|| format!("syncing {}", tmp.display()))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("renaming {} to {}", tmp.display(), path.display()))
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// sha256 of `bytes`, lowercase hex.
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn write_meta(dir: &Path, meta: &Meta) -> anyhow::Result<()> {
    write_atomic(&dir.join("meta.json"), &serde_json::to_vec_pretty(meta)?)
}

/// Load `id` `config`/`split` from the cache for the hub's current sha, or
/// fetch it from datasets-server and cache it. `meta.json` is written last, so
/// its presence marks a complete cache; it records the sha256 of the rows
/// file, which is checked on every load.
///
/// The hub sha is read before and after a fetch; if it moved, the rows may mix
/// revisions, so nothing (not even the saved pages) is kept.
///
/// Blind spot: datasets-server converts a revision some time after it is
/// pushed to the hub, and serves only its latest conversion. In that window
/// the hub already reports the new sha while datasets-server still serves the
/// old rows, and neither check can tell: the rows get cached under the new
/// sha. Nothing in either API ties a `/rows` response to a revision.
async fn load_or_fetch(
    hf: &Hf,
    cache_root: &Path,
    id: &str,
    config: &str,
    split: &str,
) -> anyhow::Result<Dataset> {
    let hub = hub_info(hf, id).await?;
    let dir = cache_dir(cache_root, id, &hub.sha);
    let rows_path = dir.join(format!("{config}-{split}.jsonl"));
    if let Some(mut meta) = check_cached_sha(&dir, &hub.sha)? {
        if meta.id != id || meta.config != config || meta.split != split {
            bail!(
                "{}: meta.json records {} {}/{}, not {id} {config}/{split}",
                dir.display(),
                meta.id,
                meta.config,
                meta.split
            );
        }
        let bytes = std::fs::read(&rows_path)
            .with_context(|| format!("reading {}", rows_path.display()))?;
        let actual = sha256_hex(&bytes);
        match &meta.rows_sha256 {
            Some(recorded) if *recorded != actual => bail!(
                "{}: sha256 {actual} but meta.json records {recorded}; the cache was modified, \
                 delete {} to re-fetch",
                rows_path.display(),
                dir.display()
            ),
            Some(_) => {}
            None => {
                // A cache written before the hash was recorded: record it now.
                meta.rows_sha256 = Some(actual);
                write_meta(&dir, &meta)?;
            }
        }
        let rows: Vec<Row> = crate::case::read_jsonl(&rows_path)?;
        if rows.len() as u64 != meta.num_rows {
            bail!(
                "{}: {} rows but meta.json records {}",
                rows_path.display(),
                rows.len(),
                meta.num_rows
            );
        }
        return Ok(Dataset { meta, rows });
    }
    let pages_dir = dir.join(format!("{config}-{split}.pages"));
    let page = fetch_rows_saving(hf, id, config, split, Some(&pages_dir)).await?;
    let after = hub_info(hf, id).await?;
    if after.sha != hub.sha {
        let cleanup = discard_pages(&pages_dir);
        bail!(
            "{id}: the hub revision moved from {} to {} during the fetch; re-run{cleanup}",
            hub.sha,
            after.sha
        );
    }
    let fetched_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    crate::case::write_jsonl(&rows_path, &page.rows)?;
    let bytes =
        std::fs::read(&rows_path).with_context(|| format!("reading {}", rows_path.display()))?;
    let meta = Meta {
        id: id.to_string(),
        sha: hub.sha,
        license: hub.license,
        config: config.to_string(),
        split: split.to_string(),
        fetched_at,
        num_rows: page.rows.len() as u64,
        features: page.features,
        rows_sha256: Some(sha256_hex(&bytes)),
    };
    write_meta(&dir, &meta)?;
    std::fs::remove_dir_all(&pages_dir)
        .with_context(|| format!("removing {}", pages_dir.display()))?;
    Ok(Dataset {
        meta,
        rows: page.rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sha2::Digest;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    /// Hex revisions for the mocked hub (a sha must look like one).
    const S1: &str = "5151515151515151515151515151515151515151";
    const SHA_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const SHA_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const SHA_OLD: &str = "0000000000000000000000000000000000000001";
    const SHA_NEW: &str = "0000000000000000000000000000000000000002";

    const CLINC: &str = include_str!("../tests/fixtures/rows_clinc.json");
    const BANKING: &str = include_str!("../tests/fixtures/rows_banking77.json");
    const SENTIMENT: &str = include_str!("../tests/fixtures/rows_sentiment.json");
    const SNLI: &str = include_str!("../tests/fixtures/rows_snli.json");
    const HUB_CLINC_REDUCED: &str = include_str!("../tests/fixtures/hub_clinc.reduced.json");
    const HUB_SENTIMENT_REDUCED: &str =
        include_str!("../tests/fixtures/hub_sentiment.reduced.json");
    const HUB_CLINC_FULL: &str = include_str!("../tests/fixtures/hub_clinc.full.json");
    const HUB_SNLI_FULL: &str = include_str!("../tests/fixtures/hub_snli.full.json");

    /// An `Hf` pointed at mocks, with no waits.
    fn hf(ds: &str, hub: &str) -> Hf {
        Hf {
            client: reqwest::Client::new(),
            ds_base: ds.to_string(),
            hub_base: hub.to_string(),
            pacing: Pacing::ZERO,
        }
    }

    /// Independent of the module's `sha256_hex`.
    fn sha256_ref(bytes: &[u8]) -> String {
        sha2::Sha256::digest(bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    #[test]
    fn parse_rows_page_reads_every_fixture_and_resolves_class_labels() {
        let p = parse_rows_page(CLINC).unwrap();
        assert_eq!(p.num_rows_total, 5500);
        assert_eq!(p.rows.len(), 3);
        assert_eq!(p.rows[0].row_idx, 0);
        let names = class_names(&p.features, "intent").unwrap();
        assert_eq!(names.len(), 151);
        let intent = p.rows[0].row["intent"].as_u64().unwrap();
        assert_eq!(intent, 61);
        assert_eq!(names[intent as usize], "translate");
        assert!(names.iter().any(|n| n == "oos"));
        assert_eq!(
            p.rows[0].row["text"],
            json!("how would you say fly in italian")
        );

        let p = parse_rows_page(BANKING).unwrap();
        assert_eq!(p.num_rows_total, 3080);
        let names = class_names(&p.features, "label").unwrap();
        assert_eq!(names.len(), 77);
        assert_eq!(
            names[p.rows[0].row["label"].as_u64().unwrap() as usize],
            "card_arrival"
        );

        let p = parse_rows_page(SENTIMENT).unwrap();
        assert_eq!(p.num_rows_total, 5206);
        assert_eq!(p.rows[0].row["label"], json!(1));
        assert_eq!(p.rows[0].row["sentiment"], json!("neutral"));
        // `label` here is a plain int, not a ClassLabel.
        let err = class_names(&p.features, "label").unwrap_err().to_string();
        assert!(err.contains("label") && err.contains("ClassLabel"), "{err}");

        let p = parse_rows_page(SNLI).unwrap();
        assert_eq!(p.num_rows_total, 10000);
        assert_eq!(
            class_names(&p.features, "label").unwrap(),
            ["entailment", "neutral", "contradiction"]
        );
        let labels: Vec<_> = p.rows.iter().map(|r| r.row["label"].clone()).collect();
        assert_eq!(labels, [json!(1), json!(0), json!(2)]);

        let err = class_names(&p.features, "nope").unwrap_err().to_string();
        assert!(err.contains("nope"), "{err}");
    }

    #[test]
    fn a_page_with_truncated_cells_is_rejected() {
        let mut v: serde_json::Value = serde_json::from_str(SNLI).unwrap();
        v["rows"][1]["truncated_cells"] = json!(["premise"]);
        let err = parse_rows_page(&v.to_string()).unwrap_err().to_string();
        assert!(err.contains("truncated") && err.contains("row 1"), "{err}");
    }

    #[test]
    fn a_partial_page_is_rejected() {
        let mut v: serde_json::Value = serde_json::from_str(SNLI).unwrap();
        assert_eq!(v["partial"], json!(false));
        v["partial"] = json!(true);
        let err = parse_rows_page(&v.to_string()).unwrap_err().to_string();
        assert!(err.contains("partial"), "{err}");
    }

    fn features(names: &[&str]) -> serde_json::Value {
        json!([
            {"feature_idx": 0, "name": "text", "type": {"dtype": "string", "_type": "Value"}},
            {"feature_idx": 1, "name": "label", "type": {"names": names, "_type": "ClassLabel"}}
        ])
    }

    fn page(total: u64, offset: u64, count: u64) -> serde_json::Value {
        let rows: Vec<_> = (offset..offset + count)
            .map(|i| json!({"row_idx": i, "row": {"text": format!("t{i}"), "label": i % 3}, "truncated_cells": []}))
            .collect();
        json!({
            "features": features(&["a", "b", "c"]),
            "rows": rows,
            "num_rows_total": total,
            "num_rows_per_page": 100,
            "partial": false
        })
    }

    fn offset_length(req: &Request) -> (u64, u64) {
        let q: std::collections::HashMap<_, _> = req.url.query_pairs().into_owned().collect();
        (q["offset"].parse().unwrap(), q["length"].parse().unwrap())
    }

    /// A well-behaved datasets-server over `self.0` rows.
    struct Pager(u64);
    impl wiremock::Respond for Pager {
        fn respond(&self, req: &Request) -> ResponseTemplate {
            let (offset, length) = offset_length(req);
            if length > 100 {
                return ResponseTemplate::new(422).set_body_json(
                    json!({"error": "Parameter 'length' must not be greater than 100"}),
                );
            }
            let count = length.min(self.0.saturating_sub(offset));
            ResponseTemplate::new(200).set_body_json(page(self.0, offset, count))
        }
    }

    /// A 250-row pager whose page at offset 100 is replaced by `f(page)`.
    struct Doctored<F>(F);
    impl<F> wiremock::Respond for Doctored<F>
    where
        F: Fn(serde_json::Value) -> serde_json::Value + Send + Sync + 'static,
    {
        fn respond(&self, req: &Request) -> ResponseTemplate {
            let (offset, length) = offset_length(req);
            let p = page(250, offset, length.min(250 - offset));
            let p = if offset == 100 { (self.0)(p) } else { p };
            ResponseTemplate::new(200).set_body_json(p)
        }
    }

    async fn doctored_err<F>(f: F) -> String
    where
        F: Fn(serde_json::Value) -> serde_json::Value + Send + Sync + 'static,
    {
        let ds = MockServer::start().await;
        Mock::given(path("/rows"))
            .respond_with(Doctored(f))
            .mount(&ds)
            .await;
        format!(
            "{:#}",
            hf(&ds.uri(), "")
                .fetch_rows("o/d", "c", "s")
                .await
                .unwrap_err()
        )
    }

    fn requests_offsets_and_lengths(reqs: &[Request]) -> Vec<(u64, u64)> {
        reqs.iter().map(offset_length).collect()
    }

    #[tokio::test]
    async fn fetch_pages_with_length_at_most_100_and_respects_num_rows_total() {
        let ds = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/rows"))
            .and(query_param("dataset", "org/ds"))
            .and(query_param("config", "plain"))
            .and(query_param("split", "test"))
            .respond_with(Pager(250))
            .mount(&ds)
            .await;
        let got = hf(&ds.uri(), "")
            .fetch_rows("org/ds", "plain", "test")
            .await
            .unwrap();
        assert_eq!(got.num_rows_total, 250);
        assert_eq!(got.rows.len(), 250);
        let idx: Vec<u64> = got.rows.iter().map(|r| r.row_idx).collect();
        assert_eq!(idx, (0..250).collect::<Vec<_>>());
        assert_eq!(
            class_names(&got.features, "label").unwrap(),
            ["a", "b", "c"]
        );

        let reqs = ds.received_requests().await.unwrap();
        let ol = requests_offsets_and_lengths(&reqs);
        assert_eq!(
            ol.iter().map(|(o, _)| *o).collect::<Vec<_>>(),
            [0, 100, 200],
            "{ol:?}"
        );
        assert!(ol.iter().all(|(_, l)| *l <= 100 && *l > 0), "{ol:?}");
    }

    #[test]
    fn default_pacing_is_the_measured_constants() {
        assert_eq!(
            Pacing::default(),
            Pacing {
                page_gap: Duration::from_secs(1),
                retry_after: Duration::from_secs(2)
            }
        );
        assert_eq!(Hf::live(reqwest::Client::new()).pacing, Pacing::default());
    }

    #[tokio::test]
    async fn zero_pacing_does_not_wait() {
        let ds = MockServer::start().await;
        Mock::given(path("/rows"))
            .respond_with(ResponseTemplate::new(429))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&ds)
            .await;
        Mock::given(path("/rows"))
            .respond_with(Pager(250))
            .mount(&ds)
            .await;
        let started = std::time::Instant::now();
        hf(&ds.uri(), "").fetch_rows("o/d", "c", "s").await.unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(900),
            "{:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn the_page_gap_and_retry_delay_come_from_the_pacing() {
        // Real (small) waits: under `start_paused` the clock auto-advances
        // while the mock's socket IO is pending, so virtual time cannot pin them.
        let ds = MockServer::start().await;
        Mock::given(path("/rows"))
            .respond_with(ResponseTemplate::new(429))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&ds)
            .await;
        Mock::given(path("/rows"))
            .respond_with(Pager(250))
            .mount(&ds)
            .await;
        let mut h = hf(&ds.uri(), "");
        h.pacing = Pacing {
            page_gap: Duration::from_millis(150),
            retry_after: Duration::from_millis(250),
        };
        let started = std::time::Instant::now();
        h.fetch_rows("o/d", "c", "s").await.unwrap();
        // One 250 ms retry, then three pages with two 150 ms gaps.
        let waited = started.elapsed();
        assert!(waited >= Duration::from_millis(550), "{waited:?}");
        assert!(waited < Duration::from_secs(2), "{waited:?}");
    }

    #[tokio::test]
    async fn an_error_body_is_truncated() {
        let ds = MockServer::start().await;
        Mock::given(path("/rows"))
            .respond_with(ResponseTemplate::new(400).set_body_string("x".repeat(5000)))
            .mount(&ds)
            .await;
        let err = hf(&ds.uri(), "")
            .fetch_rows("o/d", "c", "s")
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.len() < 600 && err.contains("5000 bytes"),
            "{}",
            err.len()
        );
    }

    #[tokio::test]
    async fn a_short_page_before_num_rows_total_is_an_error_not_a_hang() {
        let err = doctored_err(|mut p| {
            p["rows"] = json!([]);
            p
        })
        .await;
        assert!(
            err.contains("no rows") && err.contains("offset 100"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn a_gap_in_row_idx_is_an_error() {
        let err = doctored_err(|mut p| {
            p["rows"][5]["row_idx"] = json!(999);
            p
        })
        .await;
        assert!(
            err.contains("expected row_idx 105") && err.contains("999"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn num_rows_total_drift_is_an_error() {
        let err = doctored_err(|mut p| {
            p["num_rows_total"] = json!(300);
            p
        })
        .await;
        assert!(
            err.contains("num_rows_total changed from 250 to 300"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn features_drift_is_an_error() {
        let err = doctored_err(|mut p| {
            p["features"] = features(&["a", "b", "zzz"]);
            p
        })
        .await;
        assert!(err.contains("features changed"), "{err}");
    }

    #[tokio::test]
    async fn a_429_is_retried_once_then_fails_loudly() {
        // Always 429: exactly two requests, then an error naming the status.
        let ds = MockServer::start().await;
        Mock::given(path("/rows"))
            .respond_with(ResponseTemplate::new(429).set_body_string("slow down"))
            .expect(2)
            .mount(&ds)
            .await;
        let err = format!(
            "{:#}",
            hf(&ds.uri(), "")
                .fetch_rows("o/d", "c", "s")
                .await
                .unwrap_err()
        );
        assert!(
            err.contains("429") && err.contains("offset 0") && err.contains("after one retry"),
            "{err}"
        );
        ds.verify().await;

        // One 429, then success: the retry recovers.
        let ds = MockServer::start().await;
        Mock::given(path("/rows"))
            .respond_with(ResponseTemplate::new(429))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&ds)
            .await;
        Mock::given(path("/rows"))
            .respond_with(Pager(5))
            .mount(&ds)
            .await;
        let got = hf(&ds.uri(), "").fetch_rows("o/d", "c", "s").await.unwrap();
        assert_eq!(got.rows.len(), 5);
        assert_eq!(ds.received_requests().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_transient_gateway_error_is_retried_once_too() {
        for status in [502, 503, 504] {
            let ds = MockServer::start().await;
            Mock::given(path("/rows"))
                .respond_with(ResponseTemplate::new(status))
                .up_to_n_times(1)
                .with_priority(1)
                .mount(&ds)
                .await;
            Mock::given(path("/rows"))
                .respond_with(Pager(3))
                .mount(&ds)
                .await;
            let got = hf(&ds.uri(), "").fetch_rows("o/d", "c", "s").await.unwrap();
            assert_eq!(got.rows.len(), 3, "{status}");
        }
    }

    #[tokio::test]
    async fn a_non_429_error_status_is_not_retried_and_names_the_body() {
        let ds = MockServer::start().await;
        Mock::given(path("/rows"))
            .respond_with(ResponseTemplate::new(404).set_body_string("no such dataset"))
            .expect(1)
            .mount(&ds)
            .await;
        let err = format!(
            "{:#}",
            hf(&ds.uri(), "")
                .fetch_rows("o/d", "c", "s")
                .await
                .unwrap_err()
        );
        assert!(
            err.contains("404") && err.contains("no such dataset"),
            "{err}"
        );
        ds.verify().await;
    }

    async fn hub_serving(id: &str, body: &str) -> MockServer {
        let hub = MockServer::start().await;
        Mock::given(path(format!("/api/datasets/{id}")))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&hub)
            .await;
        hub
    }

    #[tokio::test]
    async fn hub_info_reads_sha_and_license_as_string_or_list() {
        let hub = hub_serving("clinc/clinc_oos", HUB_CLINC_REDUCED).await;
        let a = hf("", &hub.uri())
            .hub_info("clinc/clinc_oos")
            .await
            .unwrap();
        assert_eq!(
            a,
            HubInfo {
                sha: "155b9c710419136e17307b80d0a13e68cd46b4ec".into(),
                license: "cc-by-3.0".into()
            }
        );
        let id = "Sp1786/multiclass-sentiment-analysis-dataset";
        let hub = hub_serving(id, HUB_SENTIMENT_REDUCED).await;
        let b = hf("", &hub.uri()).hub_info(id).await.unwrap();
        assert_eq!(b.license, "apache-2.0");
        assert_eq!(b.sha, "a7ede749d4b3e7ef516603cad80bd6b39b0a1fca");
    }

    #[tokio::test]
    async fn hub_info_parses_the_full_recorded_hub_responses() {
        for (id, body, sha, license) in [
            (
                "clinc/clinc_oos",
                HUB_CLINC_FULL,
                "155b9c710419136e17307b80d0a13e68cd46b4ec",
                "cc-by-3.0",
            ),
            (
                "stanfordnlp/snli",
                HUB_SNLI_FULL,
                "cdb5c3d5eed6ead6e5a341c8e56e669bb666725b",
                "cc-by-sa-4.0",
            ),
        ] {
            let hub = hub_serving(id, body).await;
            let got = hf("", &hub.uri()).hub_info(id).await.unwrap();
            assert_eq!(
                got,
                HubInfo {
                    sha: sha.into(),
                    license: license.into()
                }
            );
        }
    }

    #[tokio::test]
    async fn hub_info_without_a_usable_licence_is_an_error() {
        for body in [
            json!({"sha": S1}),
            json!({"sha": S1, "cardData": {}}),
            json!({"sha": S1, "cardData": {"license": []}}),
        ] {
            let hub = hub_serving("o/d", &body.to_string()).await;
            let err = hf("", &hub.uri())
                .hub_info("o/d")
                .await
                .unwrap_err()
                .to_string();
            assert!(err.contains("no cardData.license"), "{body}: {err}");
        }
        let hub = hub_serving(
            "o/d",
            &json!({"sha": S1, "cardData": {"license": ["mit", 3]}}).to_string(),
        )
        .await;
        let err = hf("", &hub.uri())
            .hub_info("o/d")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("licence 3 is not a string"), "{err}");
    }

    /// SP-DEC-4 review B4: the hub sha names a cache directory, so anything
    /// but a 40- or 64-char lowercase hex revision is refused before use.
    #[tokio::test]
    async fn a_hub_sha_that_is_not_a_hex_revision_is_refused() {
        for bad in [
            "../../x",
            "s1",
            "ABCDEF0123456789ABCDEF0123456789ABCDEF01",
            &"a".repeat(41),
            &format!("{}/", "a".repeat(39)),
        ] {
            let hub = hub_serving(
                "org/ds",
                &json!({"sha": bad, "cardData": {"license": "mit"}}).to_string(),
            )
            .await;
            let root = tempfile::tempdir().unwrap();
            let inner = root.path().join("a").join("b");
            let err = format!(
                "{:#}",
                hf("http://127.0.0.1:9", &hub.uri())
                    .load_or_fetch(&inner, "org/ds", "plain", "test")
                    .await
                    .unwrap_err()
            );
            assert!(err.contains("not a hex revision"), "{bad}: {err}");
            assert!(!root.path().join("x").exists(), "{bad}");
        }
        for good in ["0123456789abcdef0123456789abcdef01234567", &"f".repeat(64)] {
            let hub = hub_serving(
                "o/d",
                &json!({"sha": good, "cardData": {"license": "mit"}}).to_string(),
            )
            .await;
            assert_eq!(hf("", &hub.uri()).hub_info("o/d").await.unwrap().sha, good);
        }
    }

    #[test]
    fn cache_dir_flattens_the_id_and_pins_the_sha() {
        assert_eq!(
            cache_dir(Path::new("/c"), "clinc/clinc_oos", "abc"),
            PathBuf::from("/c/clinc__clinc_oos@abc")
        );
        let root = default_cache_root().unwrap();
        assert!(root.ends_with("decision-bench"), "{}", root.display());
    }

    fn meta(sha: &str) -> Meta {
        Meta {
            id: "org/ds".into(),
            sha: sha.into(),
            license: "mit".into(),
            config: "plain".into(),
            split: "test".into(),
            fetched_at: 1,
            num_rows: 0,
            features: vec![],
            rows_sha256: None,
        }
    }

    #[test]
    fn the_sha_guard_refuses_a_stale_cache() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(check_cached_sha(dir.path(), SHA_B).unwrap(), None);
        std::fs::write(
            dir.path().join("meta.json"),
            serde_json::to_string(&meta(SHA_A)).unwrap(),
        )
        .unwrap();
        let err = check_cached_sha(dir.path(), SHA_B).unwrap_err().to_string();
        assert!(err.contains(SHA_A) && err.contains(SHA_B), "{err}");
        assert_eq!(
            check_cached_sha(dir.path(), SHA_A).unwrap(),
            Some(meta(SHA_A))
        );
    }

    async fn hub_with_sha(sha: &str) -> MockServer {
        let hub = MockServer::start().await;
        Mock::given(path("/api/datasets/org/ds"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"sha": sha, "cardData": {"license": "mit"}})),
            )
            .mount(&hub)
            .await;
        hub
    }

    #[tokio::test]
    async fn load_or_fetch_writes_the_cache_once_and_then_reads_it() {
        let hub = hub_with_sha(S1).await;
        let ds = MockServer::start().await;
        Mock::given(path("/rows"))
            .respond_with(Pager(150))
            .expect(2)
            .mount(&ds)
            .await;
        let root = tempfile::tempdir().unwrap();
        let h = hf(&ds.uri(), &hub.uri());
        let a = h
            .load_or_fetch(root.path(), "org/ds", "plain", "test")
            .await
            .unwrap();
        assert_eq!(a.rows.len(), 150);
        assert_eq!(a.meta.sha, S1);
        assert_eq!(a.meta.num_rows, 150);
        assert_eq!(a.meta.license, "mit");
        let dir = root.path().join(format!("org__ds@{S1}"));
        assert!(dir.join("meta.json").exists());
        let rows_file = std::fs::read(dir.join("plain-test.jsonl")).unwrap();
        assert_eq!(a.meta.rows_sha256, Some(sha256_ref(&rows_file)));
        let on_disk: Meta =
            serde_json::from_slice(&std::fs::read(dir.join("meta.json")).unwrap()).unwrap();
        assert_eq!(on_disk, a.meta);

        // Second load: served from the cache, no further datasets-server calls.
        let b = h
            .load_or_fetch(root.path(), "org/ds", "plain", "test")
            .await
            .unwrap();
        assert_eq!(a.rows, b.rows);
        assert_eq!(a.meta, b.meta);
        ds.verify().await;
    }

    /// A complete cache for org/ds@s1 holding `rows` 3-row-file rows, with
    /// `meta` edited by `edit`.
    fn seed_cache(root: &Path, edit: impl FnOnce(&mut Meta)) -> PathBuf {
        let dir = root.join(format!("org__ds@{S1}"));
        std::fs::create_dir_all(&dir).unwrap();
        let rows: Vec<Row> = parse_rows_page(&page(3, 0, 3).to_string()).unwrap().rows;
        crate::case::write_jsonl(&dir.join("plain-test.jsonl"), &rows).unwrap();
        let mut m = meta(S1);
        m.num_rows = 3;
        m.rows_sha256 = Some(sha256_ref(
            &std::fs::read(dir.join("plain-test.jsonl")).unwrap(),
        ));
        edit(&mut m);
        std::fs::write(dir.join("meta.json"), serde_json::to_vec(&m).unwrap()).unwrap();
        dir
    }

    async fn load_cached(root: &Path) -> anyhow::Result<Dataset> {
        let hub = hub_with_sha(S1).await;
        // Any datasets-server call would fail: the cache must answer.
        hf("http://127.0.0.1:9", &hub.uri())
            .load_or_fetch(root, "org/ds", "plain", "test")
            .await
    }

    #[tokio::test]
    async fn a_cache_whose_meta_names_another_dataset_is_refused() {
        for edit in [
            (|m: &mut Meta| m.id = "other/ds".into()) as fn(&mut Meta),
            |m: &mut Meta| m.config = "other".into(),
            |m: &mut Meta| m.split = "train".into(),
        ] {
            let root = tempfile::tempdir().unwrap();
            seed_cache(root.path(), edit);
            let err = load_cached(root.path()).await.unwrap_err().to_string();
            assert!(err.contains("meta.json records"), "{err}");
        }
    }

    #[tokio::test]
    async fn a_cache_with_the_wrong_row_count_is_refused() {
        let root = tempfile::tempdir().unwrap();
        seed_cache(root.path(), |m| m.num_rows = 5);
        let err = load_cached(root.path()).await.unwrap_err().to_string();
        assert!(err.contains("3 rows but meta.json records 5"), "{err}");
    }

    #[tokio::test]
    async fn a_cache_whose_rows_file_changed_is_refused_and_an_old_one_is_backfilled() {
        let root = tempfile::tempdir().unwrap();
        let dir = seed_cache(root.path(), |m| m.rows_sha256 = Some("00".repeat(32)));
        let err = load_cached(root.path()).await.unwrap_err().to_string();
        assert!(
            err.contains("sha256") && err.contains(&"00".repeat(32)),
            "{err}"
        );

        // A cache from before rows_sha256 existed: accepted, and the hash is recorded.
        let root = tempfile::tempdir().unwrap();
        let dir2 = seed_cache(root.path(), |m| m.rows_sha256 = None);
        let got = load_cached(root.path()).await.unwrap();
        let want = sha256_ref(&std::fs::read(dir2.join("plain-test.jsonl")).unwrap());
        assert_eq!(got.meta.rows_sha256.as_deref(), Some(want.as_str()));
        let on_disk: Meta =
            serde_json::from_slice(&std::fs::read(dir2.join("meta.json")).unwrap()).unwrap();
        assert_eq!(on_disk.rows_sha256, Some(want));
        drop(dir);
    }

    #[tokio::test]
    async fn an_interrupted_fetch_resumes_from_the_saved_pages() {
        let hub = hub_with_sha(S1).await;
        let ds = MockServer::start().await;
        Mock::given(path("/rows"))
            .and(query_param("offset", "100"))
            .respond_with(ResponseTemplate::new(404))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&ds)
            .await;
        Mock::given(path("/rows"))
            .respond_with(Pager(250))
            .mount(&ds)
            .await;
        let root = tempfile::tempdir().unwrap();
        let h = hf(&ds.uri(), &hub.uri());
        let err = h
            .load_or_fetch(root.path(), "org/ds", "plain", "test")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("offset 100"), "{err}");
        let got = h
            .load_or_fetch(root.path(), "org/ds", "plain", "test")
            .await
            .unwrap();
        assert_eq!(got.rows.len(), 250);
        let idx: Vec<u64> = got.rows.iter().map(|r| r.row_idx).collect();
        assert_eq!(idx, (0..250).collect::<Vec<_>>());
        let offsets: Vec<u64> =
            requests_offsets_and_lengths(&ds.received_requests().await.unwrap())
                .into_iter()
                .map(|(o, _)| o)
                .collect();
        // Run 1: 0 ok, 100 fails. Run 2 resumes at 100; offset 0 is never re-fetched.
        assert_eq!(offsets, [0, 100, 100, 200]);
        let dir = root.path().join(format!("org__ds@{S1}"));
        assert!(
            !dir.join("plain-test.pages").exists(),
            "page files not cleaned up"
        );
        assert!(dir.join("meta.json").exists());
    }

    #[tokio::test]
    async fn an_invalid_page_is_never_saved() {
        let hub = hub_with_sha(S1).await;
        let ds = MockServer::start().await;
        Mock::given(path("/rows"))
            .and(query_param("offset", "100"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"bogus": 1})))
            .with_priority(1)
            .mount(&ds)
            .await;
        Mock::given(path("/rows"))
            .respond_with(Pager(250))
            .mount(&ds)
            .await;
        let root = tempfile::tempdir().unwrap();
        let err = hf(&ds.uri(), &hub.uri())
            .load_or_fetch(root.path(), "org/ds", "plain", "test")
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("offset 100"), "{err:#}");
        let pages = root
            .path()
            .join(format!("org__ds@{S1}"))
            .join("plain-test.pages");
        assert!(pages.join("0.json").exists());
        assert!(!pages.join("100.json").exists());
    }

    #[tokio::test]
    async fn a_corrupt_saved_page_names_its_file_and_how_to_recover() {
        let hub = hub_with_sha(S1).await;
        let root = tempfile::tempdir().unwrap();
        let pages = root
            .path()
            .join(format!("org__ds@{S1}"))
            .join("plain-test.pages");
        std::fs::create_dir_all(&pages).unwrap();
        std::fs::write(pages.join("0.json"), "{not json").unwrap();
        let err = format!(
            "{:#}",
            hf("http://127.0.0.1:9", &hub.uri())
                .load_or_fetch(root.path(), "org/ds", "plain", "test")
                .await
                .unwrap_err()
        );
        assert!(
            err.contains(&pages.join("0.json").display().to_string())
                && err.contains("delete it to re-fetch"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn a_stale_resumed_page_is_discarded_so_a_retry_succeeds() {
        let hub = hub_with_sha(S1).await;
        let ds = MockServer::start().await;
        Mock::given(path("/rows"))
            .respond_with(Pager(300))
            .mount(&ds)
            .await;
        let root = tempfile::tempdir().unwrap();
        let pages = root
            .path()
            .join(format!("org__ds@{S1}"))
            .join("plain-test.pages");
        std::fs::create_dir_all(&pages).unwrap();
        // Saved by an earlier run when the split had 250 rows.
        std::fs::write(pages.join("0.json"), page(250, 0, 100).to_string()).unwrap();
        let h = hf(&ds.uri(), &hub.uri());
        let err = format!(
            "{:#}",
            h.load_or_fetch(root.path(), "org/ds", "plain", "test")
                .await
                .unwrap_err()
        );
        assert!(
            err.contains("num_rows_total changed from 250 to 300")
                && err.contains(&pages.display().to_string())
                && err.contains("discarded"),
            "{err}"
        );
        assert!(!pages.exists(), "the stale pages were kept");
        let got = h
            .load_or_fetch(root.path(), "org/ds", "plain", "test")
            .await
            .unwrap();
        assert_eq!(got.rows.len(), 300);
    }

    #[tokio::test]
    async fn a_live_page_failure_keeps_the_saved_pages_and_blames_the_live_page() {
        let hub = hub_with_sha(S1).await;
        let ds = MockServer::start().await;
        Mock::given(path("/rows"))
            .and(query_param("offset", "200"))
            .respond_with(ResponseTemplate::new(200).set_body_json(page(300, 200, 0)))
            .with_priority(1)
            .mount(&ds)
            .await;
        Mock::given(path("/rows"))
            .respond_with(Pager(300))
            .mount(&ds)
            .await;
        let root = tempfile::tempdir().unwrap();
        let pages = root
            .path()
            .join(format!("org__ds@{S1}"))
            .join("plain-test.pages");
        std::fs::create_dir_all(&pages).unwrap();
        // Saved by an earlier run, and consistent with the live dataset.
        std::fs::write(pages.join("0.json"), page(300, 0, 100).to_string()).unwrap();
        let err = format!(
            "{:#}",
            hf(&ds.uri(), &hub.uri())
                .load_or_fetch(root.path(), "org/ds", "plain", "test")
                .await
                .unwrap_err()
        );
        assert!(
            err.contains("offset 200") && err.contains("no rows returned"),
            "{err}"
        );
        assert!(
            !err.contains("earlier run"),
            "blamed the saved pages: {err}"
        );
        assert!(
            pages.join("0.json").exists() && pages.join("100.json").exists(),
            "good saved pages were discarded"
        );
    }

    #[tokio::test]
    async fn load_or_fetch_refuses_a_cache_whose_meta_disagrees_with_the_hub() {
        let hub = hub_with_sha(SHA_B).await;
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join(format!("org__ds@{SHA_B}"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("meta.json"),
            serde_json::to_string(&meta(SHA_A)).unwrap(),
        )
        .unwrap();
        let err = hf("http://127.0.0.1:9", &hub.uri())
            .load_or_fetch(root.path(), "org/ds", "plain", "test")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains(SHA_A) && err.contains(SHA_B), "{err}");
    }

    #[tokio::test]
    async fn a_revision_change_during_the_fetch_is_an_error_and_keeps_nothing() {
        let hub = MockServer::start().await;
        let body = |sha: &str| {
            ResponseTemplate::new(200)
                .set_body_json(json!({"sha": sha, "cardData": {"license": "mit"}}))
        };
        Mock::given(path("/api/datasets/org/ds"))
            .respond_with(body(SHA_OLD))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&hub)
            .await;
        Mock::given(path("/api/datasets/org/ds"))
            .respond_with(body(SHA_NEW))
            .mount(&hub)
            .await;
        let ds = MockServer::start().await;
        Mock::given(path("/rows"))
            .respond_with(Pager(3))
            .mount(&ds)
            .await;
        let root = tempfile::tempdir().unwrap();
        let err = hf(&ds.uri(), &hub.uri())
            .load_or_fetch(root.path(), "org/ds", "plain", "test")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains(SHA_OLD) && err.contains(SHA_NEW), "{err}");
        let dir = root.path().join(format!("org__ds@{SHA_OLD}"));
        assert!(!dir.join("meta.json").exists());
        assert!(
            !dir.join("plain-test.pages").exists(),
            "pages of a mixed revision were kept"
        );
    }
}
