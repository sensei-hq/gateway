//! Hugging Face datasets-server + hub client, the on-disk cache, and the sha
//! guard.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, bail};

use serde::{Deserialize, Serialize};

pub const DATASETS_SERVER: &str = "https://datasets-server.huggingface.co";
pub const HUB: &str = "https://huggingface.co";

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
}

#[derive(Debug, Clone, PartialEq)]
pub struct Dataset {
    pub meta: Meta,
    pub rows: Vec<Row>,
}

#[derive(Deserialize)]
struct RawPage {
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

/// Send `req`, retrying once after [`RETRY_AFTER`] on a transient status. Any other
/// non-success status, or a second transient one, is an error carrying status
/// and body.
async fn get_text(req: reqwest::RequestBuilder, what: &str) -> anyhow::Result<String> {
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
            tokio::time::sleep(RETRY_AFTER).await;
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
pub async fn hub_info(
    client: &reqwest::Client,
    hub_base: &str,
    id: &str,
) -> anyhow::Result<HubInfo> {
    let what = format!("hub info for {id}");
    let body = get_text(client.get(format!("{hub_base}/api/datasets/{id}")), &what).await?;
    let raw: RawHub = serde_json::from_str(&body).with_context(|| format!("{what}: parsing"))?;
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
/// page's `num_rows_total` is reached.
pub async fn fetch_rows(
    client: &reqwest::Client,
    ds_base: &str,
    id: &str,
    config: &str,
    split: &str,
) -> anyhow::Result<RowsPage> {
    fetch_rows_saving(client, ds_base, id, config, split, None).await
}

/// [`fetch_rows`], saving each page body verbatim as `<pages>/<offset>.json`
/// and reading an already-saved page instead of requesting it again, so a
/// run stopped by a rate limit resumes where it failed.
pub async fn fetch_rows_saving(
    client: &reqwest::Client,
    ds_base: &str,
    id: &str,
    config: &str,
    split: &str,
    pages: Option<&Path>,
) -> anyhow::Result<RowsPage> {
    if let Some(dir) = pages {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let mut requested = false;
    let mut features: Option<Vec<Feature>> = None;
    let mut total: Option<u64> = None;
    let mut rows: Vec<Row> = Vec::new();
    let mut offset = 0u64;
    while total.is_none_or(|t| offset < t) {
        let length = total.map_or(PAGE_LEN, |t| PAGE_LEN.min(t - offset));
        let what = format!("{id} {config}/{split} rows at offset {offset}");
        let mut url = reqwest::Url::parse(&format!("{ds_base}/rows"))
            .with_context(|| format!("bad datasets-server base {ds_base:?}"))?;
        url.query_pairs_mut()
            .append_pair("dataset", id)
            .append_pair("config", config)
            .append_pair("split", split)
            .append_pair("offset", &offset.to_string())
            .append_pair("length", &length.to_string());
        let saved = pages.map(|d| d.join(format!("{offset}.json")));
        let body = match saved.as_deref().filter(|p| p.exists()) {
            Some(p) => {
                std::fs::read_to_string(p).with_context(|| format!("reading {}", p.display()))?
            }
            None => {
                if requested {
                    tokio::time::sleep(PAGE_GAP).await;
                }
                requested = true;
                let body = get_text(client.get(url), &what).await?;
                if let Some(p) = &saved {
                    // Validate before saving, so a bad page is never resumed from.
                    parse_rows_page(&body).with_context(|| what.clone())?;
                    write_atomic(p, body.as_bytes())?;
                }
                body
            }
        };
        let page = parse_rows_page(&body).with_context(|| what.clone())?;
        match total {
            None => total = Some(page.num_rows_total),
            Some(t) if t != page.num_rows_total => bail!(
                "{what}: num_rows_total changed from {t} to {} mid-fetch",
                page.num_rows_total
            ),
            Some(_) => {}
        }
        match &features {
            None => features = Some(page.features),
            Some(f) if *f != page.features => bail!("{what}: features changed mid-fetch"),
            Some(_) => {}
        }
        if page.rows.is_empty() && offset < page.num_rows_total {
            bail!(
                "{what}: no rows returned before num_rows_total {}",
                page.num_rows_total
            );
        }
        for (i, r) in page.rows.iter().enumerate() {
            let want = offset + i as u64;
            if r.row_idx != want {
                bail!("{what}: expected row_idx {want}, got {}", r.row_idx);
            }
        }
        offset += page.rows.len() as u64;
        rows.extend(page.rows);
    }
    Ok(RowsPage {
        features: features.unwrap_or_default(),
        rows,
        num_rows_total: total.unwrap_or(0),
    })
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

/// Write `bytes` to `<path>.tmp`, then rename over `path`.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    std::fs::write(&tmp, bytes).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("renaming {} to {}", tmp.display(), path.display()))
}

/// Load `id` `config`/`split` from the cache for the hub's current sha, or
/// fetch it from datasets-server and cache it. The hub sha is read before and
/// after a fetch; if it moved, the rows may mix revisions and nothing is
/// cached. `meta.json` is written last, so its presence marks a complete
/// cache.
pub async fn load_or_fetch(
    client: &reqwest::Client,
    ds_base: &str,
    hub_base: &str,
    cache_root: &Path,
    id: &str,
    config: &str,
    split: &str,
) -> anyhow::Result<Dataset> {
    let hub = hub_info(client, hub_base, id).await?;
    let dir = cache_dir(cache_root, id, &hub.sha);
    let rows_path = dir.join(format!("{config}-{split}.jsonl"));
    if let Some(meta) = check_cached_sha(&dir, &hub.sha)? {
        if meta.id != id || meta.config != config || meta.split != split {
            bail!(
                "{}: meta.json records {} {}/{}, not {id} {config}/{split}",
                dir.display(),
                meta.id,
                meta.config,
                meta.split
            );
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
    let page = fetch_rows_saving(client, ds_base, id, config, split, Some(&pages_dir)).await?;
    let after = hub_info(client, hub_base, id).await?;
    if after.sha != hub.sha {
        bail!(
            "{id}: the hub revision moved from {} to {} during the fetch; re-run",
            hub.sha,
            after.sha
        );
    }
    let fetched_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let meta = Meta {
        id: id.to_string(),
        sha: hub.sha,
        license: hub.license,
        config: config.to_string(),
        split: split.to_string(),
        fetched_at,
        num_rows: page.rows.len() as u64,
        features: page.features,
    };
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    crate::case::write_jsonl(&rows_path, &page.rows)?;
    write_atomic(&dir.join("meta.json"), &serde_json::to_vec_pretty(&meta)?)?;
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
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    const CLINC: &str = include_str!("../tests/fixtures/rows_clinc.json");
    const BANKING: &str = include_str!("../tests/fixtures/rows_banking77.json");
    const SENTIMENT: &str = include_str!("../tests/fixtures/rows_sentiment.json");
    const SNLI: &str = include_str!("../tests/fixtures/rows_snli.json");
    const HUB_CLINC: &str = include_str!("../tests/fixtures/hub_clinc.json");
    const HUB_SENTIMENT: &str = include_str!("../tests/fixtures/hub_sentiment.json");

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

    fn page(total: u64, offset: u64, count: u64) -> serde_json::Value {
        let rows: Vec<_> = (offset..offset + count)
            .map(|i| json!({"row_idx": i, "row": {"text": format!("t{i}"), "label": i % 3}, "truncated_cells": []}))
            .collect();
        json!({
            "features": [
                {"feature_idx": 0, "name": "text", "type": {"dtype": "string", "_type": "Value"}},
                {"feature_idx": 1, "name": "label", "type": {"names": ["a", "b", "c"], "_type": "ClassLabel"}}
            ],
            "rows": rows,
            "num_rows_total": total,
            "num_rows_per_page": 100,
            "partial": false
        })
    }

    struct Pager(u64);
    impl wiremock::Respond for Pager {
        fn respond(&self, req: &Request) -> ResponseTemplate {
            let q: std::collections::HashMap<_, _> = req.url.query_pairs().into_owned().collect();
            let offset: u64 = q["offset"].parse().unwrap();
            let length: u64 = q["length"].parse().unwrap();
            if length > 100 {
                return ResponseTemplate::new(422).set_body_json(
                    json!({"error": "Parameter 'length' must not be greater than 100"}),
                );
            }
            let count = length.min(self.0.saturating_sub(offset));
            ResponseTemplate::new(200).set_body_json(page(self.0, offset, count))
        }
    }

    fn requests_offsets_and_lengths(reqs: &[Request]) -> Vec<(u64, u64)> {
        reqs.iter()
            .map(|r| {
                let q: std::collections::HashMap<_, _> = r.url.query_pairs().into_owned().collect();
                (q["offset"].parse().unwrap(), q["length"].parse().unwrap())
            })
            .collect()
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
        let client = reqwest::Client::new();
        let got = fetch_rows(&client, &ds.uri(), "org/ds", "plain", "test")
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

    #[tokio::test]
    async fn pages_after_the_first_are_spaced_by_the_page_gap() {
        let ds = MockServer::start().await;
        Mock::given(path("/rows"))
            .respond_with(Pager(250))
            .mount(&ds)
            .await;
        let started = std::time::Instant::now();
        fetch_rows(&reqwest::Client::new(), &ds.uri(), "o/d", "c", "s")
            .await
            .unwrap();
        // Three pages, two gaps of at least a second each.
        assert!(
            started.elapsed() >= std::time::Duration::from_secs(2),
            "{:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn an_error_body_is_truncated() {
        let ds = MockServer::start().await;
        Mock::given(path("/rows"))
            .respond_with(ResponseTemplate::new(400).set_body_string("x".repeat(5000)))
            .mount(&ds)
            .await;
        let err = fetch_rows(&reqwest::Client::new(), &ds.uri(), "o/d", "c", "s")
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
        struct Stingy;
        impl wiremock::Respond for Stingy {
            fn respond(&self, _req: &Request) -> ResponseTemplate {
                ResponseTemplate::new(200).set_body_json(page(250, 0, 0))
            }
        }
        let ds = MockServer::start().await;
        Mock::given(path("/rows"))
            .respond_with(Stingy)
            .mount(&ds)
            .await;
        let err = fetch_rows(&reqwest::Client::new(), &ds.uri(), "o/d", "c", "s")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("no rows") && err.contains("offset 0"), "{err}");
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
        let client = reqwest::Client::new();
        let started = std::time::Instant::now();
        let err = format!(
            "{:#}",
            fetch_rows(&client, &ds.uri(), "o/d", "c", "s")
                .await
                .unwrap_err()
        );
        assert!(err.contains("429") && err.contains("offset 0"), "{err}");
        assert!(started.elapsed() >= std::time::Duration::from_secs(2));
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
        let got = fetch_rows(&client, &ds.uri(), "o/d", "c", "s")
            .await
            .unwrap();
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
            let got = fetch_rows(&reqwest::Client::new(), &ds.uri(), "o/d", "c", "s")
                .await
                .unwrap();
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
            fetch_rows(&reqwest::Client::new(), &ds.uri(), "o/d", "c", "s")
                .await
                .unwrap_err()
        );
        assert!(
            err.contains("404") && err.contains("no such dataset"),
            "{err}"
        );
        ds.verify().await;
    }

    #[tokio::test]
    async fn hub_info_reads_sha_and_license_as_string_or_list() {
        let hub = MockServer::start().await;
        Mock::given(path("/api/datasets/clinc/clinc_oos"))
            .respond_with(ResponseTemplate::new(200).set_body_string(HUB_CLINC))
            .mount(&hub)
            .await;
        Mock::given(path(
            "/api/datasets/Sp1786/multiclass-sentiment-analysis-dataset",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_string(HUB_SENTIMENT))
        .mount(&hub)
        .await;
        let client = reqwest::Client::new();
        let a = hub_info(&client, &hub.uri(), "clinc/clinc_oos")
            .await
            .unwrap();
        assert_eq!(
            a,
            HubInfo {
                sha: "155b9c710419136e17307b80d0a13e68cd46b4ec".into(),
                license: "cc-by-3.0".into()
            }
        );
        let b = hub_info(
            &client,
            &hub.uri(),
            "Sp1786/multiclass-sentiment-analysis-dataset",
        )
        .await
        .unwrap();
        assert_eq!(b.license, "apache-2.0");
        assert_eq!(b.sha, "a7ede749d4b3e7ef516603cad80bd6b39b0a1fca");
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
        }
    }

    #[test]
    fn the_sha_guard_refuses_a_stale_cache() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(check_cached_sha(dir.path(), "bbb").unwrap(), None);
        std::fs::write(
            dir.path().join("meta.json"),
            serde_json::to_string(&meta("aaa")).unwrap(),
        )
        .unwrap();
        let err = check_cached_sha(dir.path(), "bbb").unwrap_err().to_string();
        assert!(err.contains("aaa") && err.contains("bbb"), "{err}");
        assert_eq!(
            check_cached_sha(dir.path(), "aaa").unwrap(),
            Some(meta("aaa"))
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
        let hub = hub_with_sha("s1").await;
        let ds = MockServer::start().await;
        Mock::given(path("/rows"))
            .respond_with(Pager(150))
            .expect(2)
            .mount(&ds)
            .await;
        let root = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let a = load_or_fetch(
            &client,
            &ds.uri(),
            &hub.uri(),
            root.path(),
            "org/ds",
            "plain",
            "test",
        )
        .await
        .unwrap();
        assert_eq!(a.rows.len(), 150);
        assert_eq!(a.meta.sha, "s1");
        assert_eq!(a.meta.num_rows, 150);
        assert_eq!(a.meta.license, "mit");
        let dir = root.path().join("org__ds@s1");
        assert!(dir.join("meta.json").exists());
        assert!(dir.join("plain-test.jsonl").exists());

        // Second load: served from the cache, no further datasets-server calls.
        let b = load_or_fetch(
            &client,
            &ds.uri(),
            &hub.uri(),
            root.path(),
            "org/ds",
            "plain",
            "test",
        )
        .await
        .unwrap();
        assert_eq!(a.rows, b.rows);
        assert_eq!(a.meta, b.meta);
        ds.verify().await;
    }

    #[tokio::test]
    async fn an_interrupted_fetch_resumes_from_the_saved_pages() {
        let hub = hub_with_sha("s1").await;
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
        let client = reqwest::Client::new();
        let (ds_uri, hub_uri) = (ds.uri(), hub.uri());
        let load = || {
            load_or_fetch(
                &client,
                &ds_uri,
                &hub_uri,
                root.path(),
                "org/ds",
                "plain",
                "test",
            )
        };
        let err = load().await.unwrap_err().to_string();
        assert!(err.contains("offset 100"), "{err}");
        let got = load().await.unwrap();
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
        let dir = root.path().join("org__ds@s1");
        assert!(
            !dir.join("plain-test.pages").exists(),
            "page files not cleaned up"
        );
        assert!(dir.join("meta.json").exists());
    }

    #[tokio::test]
    async fn load_or_fetch_refuses_a_cache_whose_meta_disagrees_with_the_hub() {
        let hub = hub_with_sha("bbb").await;
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("org__ds@bbb");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("meta.json"),
            serde_json::to_string(&meta("aaa")).unwrap(),
        )
        .unwrap();
        let err = load_or_fetch(
            &reqwest::Client::new(),
            "http://127.0.0.1:9",
            &hub.uri(),
            root.path(),
            "org/ds",
            "plain",
            "test",
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("aaa") && err.contains("bbb"), "{err}");
    }

    #[tokio::test]
    async fn a_revision_change_during_the_fetch_is_an_error_and_writes_no_cache() {
        let hub = MockServer::start().await;
        let body = |sha: &str| {
            ResponseTemplate::new(200)
                .set_body_json(json!({"sha": sha, "cardData": {"license": "mit"}}))
        };
        Mock::given(path("/api/datasets/org/ds"))
            .respond_with(body("old"))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&hub)
            .await;
        Mock::given(path("/api/datasets/org/ds"))
            .respond_with(body("new"))
            .mount(&hub)
            .await;
        let ds = MockServer::start().await;
        Mock::given(path("/rows"))
            .respond_with(Pager(3))
            .mount(&ds)
            .await;
        let root = tempfile::tempdir().unwrap();
        let err = load_or_fetch(
            &reqwest::Client::new(),
            &ds.uri(),
            &hub.uri(),
            root.path(),
            "org/ds",
            "plain",
            "test",
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("old") && err.contains("new"), "{err}");
        assert!(!root.path().join("org__ds@old").join("meta.json").exists());
    }
}
