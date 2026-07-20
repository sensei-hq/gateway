//! Hugging Face model download (HF-A), gated behind the `hf-download` feature.
//!
//! Fetches a model file (plus any siblings, e.g. `tokenizer.json` for ONNX)
//! from the HF Hub, places it in the managed store, and registers a
//! [`ModelEntry`] so the embedded engines can run it — no Ollama required.
//!
//! ## Resource pre-flight (mandatory)
//! A model must not be downloaded if it can't run on the machine. [`HfHubPuller::pull`]
//! calls [`HfHubPuller::check_fit`] **first**, before fetching any file bytes, and
//! returns [`PullError::WontFit`] with an actionable message if the model won't fit —
//! so we never download a 30 GB file that can't load on 8 GB of RAM. The size is read
//! from HF with a one-byte ranged `GET` (no body download); machine RAM + free disk
//! come from `sysinfo`. The fit rule is a deliberately gross guard (see [`evaluate_fit`]).
//!
//! ## Pull-on-missing
//! [`PullingResolver`] wraps a [`ManagedResolver`] with a table of [`PullSpec`]s keyed
//! by model id: a configured-but-absent model is fetched (and resource-checked) the
//! first time an engine resolves it.

use super::{ManagedResolver, ModelEntry, ModelFormat, ModelResolver, ModelSource, ResolveError};
use async_trait::async_trait;
use hf_hub::api::tokio::ApiBuilder;
use hf_hub::{Repo, RepoType};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use sysinfo::{Disks, System};

/// A model file (plus any siblings) to fetch and register.
#[derive(Debug, Clone)]
pub struct PullSpec {
    /// HF repo id, e.g. `"bartowski/Llama-3.2-3B-Instruct-GGUF"`.
    pub repo: String,
    /// Git revision to pin; defaults to `"main"` when `None`.
    pub revision: Option<String>,
    /// Stable registry id to register the model under.
    pub id: String,
    /// Display name; defaults to `id` when `None`.
    pub name: Option<String>,
    /// On-disk format of `files[0]`.
    pub format: ModelFormat,
    /// Files to download. `files[0]` is the model file registered as the
    /// source path; the rest are siblings (e.g. `tokenizer.json` for ONNX).
    pub files: Vec<String>,
}

/// Failure modes of a model pull.
#[derive(Debug, thiserror::Error)]
pub enum PullError {
    /// The HF Hub client or a size probe failed.
    #[error("hugging face hub: {0}")]
    Hub(String),
    /// A local filesystem operation failed while staging the files.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Registering the downloaded entry in the managed store failed.
    #[error("registry: {0}")]
    Registry(#[from] ResolveError),
    /// The spec listed no files to download.
    #[error("pull spec has no files")]
    EmptySpec,
    /// The model can't run on this machine (disk or RAM); message is actionable.
    #[error("{0}")]
    WontFit(String),
}

/// Result of the resource pre-flight: the model's on-HF size versus the
/// machine's disk and RAM, with a verdict and (on failure) a human message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FitReport {
    /// Total size of all `files` as reported by HF, in bytes.
    pub model_bytes: u64,
    /// Free disk on the filesystem holding the managed root, in bytes.
    pub disk_available: u64,
    /// Total system RAM, in bytes.
    pub ram_total: u64,
    /// Currently-available system RAM, in bytes.
    pub ram_available: u64,
    /// Whether the model clears the (gross) disk + RAM guard.
    pub fits: bool,
    /// Actionable reason when `!fits`.
    pub reason: Option<String>,
}

/// Downloads model files into managed storage and registers them.
#[async_trait]
pub trait ModelPuller: Send + Sync {
    /// Resource-check (mandatory) then download the spec's files into managed
    /// storage and register the entry. Returns the registered [`ModelEntry`].
    async fn pull(&self, spec: &PullSpec) -> Result<ModelEntry, PullError>;

    /// Pre-flight only: report whether the model would fit, without downloading
    /// any file bytes. Public so a UI can check before offering a pull.
    async fn check_fit(&self, spec: &PullSpec) -> Result<FitReport, PullError>;
}

/// Pulls from the Hugging Face Hub into a [`ManagedResolver`]-owned store.
pub struct HfHubPuller {
    managed: ManagedResolver,
    token: Option<String>,
}

impl HfHubPuller {
    /// Create a puller over the given managed store, with an optional HF token
    /// for gated/private repos.
    pub fn new(managed: ManagedResolver, token: Option<String>) -> Self {
        Self { managed, token }
    }

    /// Record already-staged files as a managed [`ModelEntry`] (`staged[0]` is
    /// the model file). Split out from [`Self::pull`] so the registration step
    /// is unit-testable with local temp files (no network).
    pub(crate) async fn register_local(
        &self,
        id: &str,
        name: Option<String>,
        format: ModelFormat,
        staged: &[PathBuf],
    ) -> Result<ModelEntry, PullError> {
        let model_path = staged.first().ok_or(PullError::EmptySpec)?;
        let size_bytes = tokio::fs::metadata(model_path).await?.len();
        let entry = ModelEntry {
            id: id.to_string(),
            name: name.unwrap_or_else(|| id.to_string()),
            format,
            source: ModelSource::Managed {
                path: model_path.clone(),
            },
            sha256: None,
            size_bytes: Some(size_bytes),
        };
        self.managed.add(entry.clone()).await?;
        Ok(entry)
    }
}

#[async_trait]
impl ModelPuller for HfHubPuller {
    async fn pull(&self, spec: &PullSpec) -> Result<ModelEntry, PullError> {
        if spec.files.is_empty() {
            return Err(PullError::EmptySpec);
        }

        // Validate every destination up front, before any network I/O: the id
        // and filenames are operator-supplied and must not escape the managed
        // root via `..`, an absolute path, or a root/prefix component.
        let dest_dir = safe_join(self.managed.root(), &spec.id)?;
        let dests = spec
            .files
            .iter()
            .map(|f| safe_join(&dest_dir, f))
            .collect::<std::io::Result<Vec<_>>>()?;

        // Resource guard FIRST — never fetch file bytes for a model that can't
        // run here. `check_fit` only reads sizes (one-byte ranged probes), not
        // bodies; if it says the model won't fit we bail before any download.
        let fit = self.check_fit(spec).await?;
        if !fit.fits {
            let reason = fit
                .reason
                .unwrap_or_else(|| "does not fit on this machine".to_string());
            return Err(PullError::WontFit(format!("model '{}' {reason}", spec.id)));
        }

        let revision = spec.revision.as_deref().unwrap_or("main").to_string();
        let api = ApiBuilder::new()
            .with_token(self.token.clone())
            .build()
            .map_err(|e| PullError::Hub(e.to_string()))?;
        let repo = api.repo(Repo::with_revision(
            spec.repo.clone(),
            RepoType::Model,
            revision,
        ));

        tokio::fs::create_dir_all(&dest_dir).await?;

        let mut staged = Vec::with_capacity(spec.files.len());
        for (file, dest) in spec.files.iter().zip(dests) {
            let cached = repo
                .get(file)
                .await
                .map_err(|e| PullError::Hub(format!("download of '{file}' failed: {e}")))?;
            if let Some(parent) = dest.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            stage_file(&cached, &dest).await?;
            staged.push(dest);
        }

        self.register_local(&spec.id, spec.name.clone(), spec.format, &staged)
            .await
    }

    async fn check_fit(&self, spec: &PullSpec) -> Result<FitReport, PullError> {
        if spec.files.is_empty() {
            return Err(PullError::EmptySpec);
        }
        let revision = spec.revision.as_deref().unwrap_or("main");
        let endpoint = hf_endpoint();
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(SIZE_PROBE_TIMEOUT_SECS))
            .build()
            .map_err(|e| PullError::Hub(e.to_string()))?;

        let mut model_bytes: u64 = 0;
        for file in &spec.files {
            let url = format!("{endpoint}/{}/resolve/{}/{}", spec.repo, revision, file);
            let size = remote_file_size(&client, &url, self.token.as_deref()).await?;
            model_bytes = model_bytes.saturating_add(size);
        }

        let (disk_available, ram_total, ram_available) = machine_resources(self.managed.root());
        Ok(evaluate_fit(
            model_bytes,
            disk_available,
            ram_total,
            ram_available,
        ))
    }
}

/// A [`ModelResolver`] that fetches a configured model the first time it's
/// asked for. Reads go through `inner`; a miss with a registered [`PullSpec`]
/// triggers a guarded `pull` (which registers into the same managed store).
pub struct PullingResolver {
    inner: ManagedResolver,
    puller: HfHubPuller,
    specs: HashMap<String, PullSpec>,
}

impl PullingResolver {
    /// Build a resolver over the puller's managed store, driven by `specs`
    /// (keyed by model id). The read-side view is derived from the puller's
    /// managed root so both see the same on-disk index.
    pub fn new(puller: HfHubPuller, specs: HashMap<String, PullSpec>) -> Self {
        let inner = ManagedResolver::new(puller.managed.root().to_path_buf());
        Self {
            inner,
            puller,
            specs,
        }
    }

    /// Register (or replace) a pull spec, keyed by its `id`. Builder-style.
    pub fn with_spec(mut self, spec: PullSpec) -> Self {
        self.specs.insert(spec.id.clone(), spec);
        self
    }
}

#[async_trait]
impl ModelResolver for PullingResolver {
    async fn resolve(&self, id: &str) -> Result<Option<ModelEntry>, ResolveError> {
        if let Some(entry) = self.inner.resolve(id).await? {
            return Ok(Some(entry));
        }
        match self.specs.get(id) {
            Some(spec) => match self.puller.pull(spec).await {
                Ok(entry) => Ok(Some(entry)),
                // Surface WontFit/download errors as a resolve error so the
                // caller gets the actionable message, not a silent miss.
                Err(err) => Err(ResolveError::Pull(err.to_string())),
            },
            None => Ok(None),
        }
    }

    async fn list(&self) -> Result<Vec<ModelEntry>, ResolveError> {
        self.inner.list().await
    }
}

/// Pure, unit-testable fit heuristic. `need_disk = size * 1.05`; `need_ram =
/// size * 1.2` (GGUF loads ≈ file size resident, plus a KV/context margin).
/// This is a *gross* guard for the "won't remotely fit" cases, not a precise
/// budget — real RAM need depends on quant and context length.
fn evaluate_fit(
    model_bytes: u64,
    disk_available: u64,
    ram_total: u64,
    ram_available: u64,
) -> FitReport {
    let need_disk = model_bytes.saturating_mul(105) / 100;
    let need_ram = model_bytes.saturating_mul(12) / 10;

    let mut fits = true;
    let mut reason = None;

    if need_disk > disk_available {
        fits = false;
        reason = Some(format!(
            "needs ~{} disk but only {} is free",
            fmt_bytes(need_disk),
            fmt_bytes(disk_available),
        ));
    }
    if need_ram > ram_total {
        fits = false;
        reason = Some(format!(
            "is ~{} and needs ~{} RAM; this machine has {} — not usable",
            fmt_bytes(model_bytes),
            fmt_bytes(need_ram),
            fmt_bytes(ram_total),
        ));
    }

    FitReport {
        model_bytes,
        disk_available,
        ram_total,
        ram_available,
        fits,
        reason,
    }
}

/// Format a byte count as GB (>= 1 GiB) or MB, e.g. `18.0 GB`.
fn fmt_bytes(n: u64) -> String {
    const MB: f64 = 1024.0 * 1024.0;
    const GB: f64 = MB * 1024.0;
    let f = n as f64;
    if f >= GB {
        format!("{:.1} GB", f / GB)
    } else {
        format!("{:.1} MB", f / MB)
    }
}

/// Per-file size-probe timeout. The probe reads only one byte, so a wedged or
/// slow HF endpoint must not be allowed to hang the whole `pull` — bound it.
const SIZE_PROBE_TIMEOUT_SECS: u64 = 30;

/// Base HF Hub endpoint for the size probe. Honours the `HF_ENDPOINT` env var —
/// the *same* override hf-hub applies to the download — so a self-hosted mirror
/// is size-checked and fetched from one host rather than split across two. The
/// public hub is a fallback default only, never a baked-in operational endpoint.
fn hf_endpoint() -> String {
    match std::env::var("HF_ENDPOINT") {
        Ok(e) if !e.trim_end_matches('/').is_empty() => e.trim_end_matches('/').to_string(),
        _ => "https://huggingface.co".to_string(),
    }
}

/// Probe a file's size on HF **without downloading the body**: a ranged
/// `GET` with `Range: bytes=0-0` yields a `206` whose `Content-Range:
/// bytes 0-0/<TOTAL>` carries the full length. Falls back to `Content-Length`
/// if the server ignored the range (`200`).
async fn remote_file_size(
    client: &reqwest::Client,
    url: &str,
    token: Option<&str>,
) -> Result<u64, PullError> {
    let mut req = client.get(url).header(reqwest::header::RANGE, "bytes=0-0");
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| PullError::Hub(format!("size probe failed for {url}: {e}")))?
        .error_for_status()
        .map_err(|e| PullError::Hub(format!("size probe status for {url}: {e}")))?;

    // Prefer the total from Content-Range (authoritative on a 206; content_length
    // there would be 1, the single ranged byte).
    if let Some(total) = resp
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.rsplit('/').next())
        .map(str::trim)
        .and_then(|s| s.parse::<u64>().ok())
    {
        return Ok(total);
    }
    if let Some(len) = resp.content_length() {
        return Ok(len);
    }
    Err(PullError::Hub(format!(
        "could not determine size for {url}"
    )))
}

/// Query `(disk_available, ram_total, ram_available)` for the filesystem
/// holding `root` and the whole machine, in bytes.
fn machine_resources(root: &Path) -> (u64, u64, u64) {
    let mut sys = System::new();
    sys.refresh_memory();
    let ram_total = sys.total_memory();
    let ram_available = sys.available_memory();
    let disk_available = free_disk_for(root);
    (disk_available, ram_total, ram_available)
}

/// Free space on the filesystem that will hold `root` — the disk whose mount
/// point is the longest prefix of the nearest existing ancestor of `root`.
fn free_disk_for(root: &Path) -> u64 {
    let canon = nearest_existing(root);
    let disks = Disks::new_with_refreshed_list();
    disks
        .list()
        .iter()
        .filter(|d| canon.starts_with(d.mount_point()))
        .max_by_key(|d| d.mount_point().as_os_str().len())
        .map(|d| d.available_space())
        .unwrap_or(0)
}

/// Canonicalize the nearest existing ancestor of `path` (the managed root may
/// not exist yet, but its parent filesystem does).
fn nearest_existing(path: &Path) -> PathBuf {
    let mut probe = path;
    loop {
        if let Ok(c) = probe.canonicalize() {
            return c;
        }
        match probe.parent() {
            Some(p) => probe = p,
            None => return path.to_path_buf(),
        }
    }
}

/// Join an operator-supplied relative path onto `base`, rejecting anything that
/// would escape it — an absolute path, a root/prefix component, or `..`. This is
/// the traversal guard for `<managed root>/<id>/<file>`, all of which come from
/// operator config; only `Normal` and `.` components are allowed.
fn safe_join(base: &Path, rel: &str) -> std::io::Result<PathBuf> {
    use std::path::Component;
    // This function IS the traversal guard: `rel` is accepted only if every
    // component is Normal/CurDir (checked just below), so it can't escape `base`.
    let rel_path = Path::new(rel); // nosemgrep: rust.actix.path-traversal.tainted-path.tainted-path
    let safe = !rel.is_empty()
        && rel_path
            .components()
            .all(|c| matches!(c, Component::Normal(_) | Component::CurDir));
    if !safe {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("unsafe path in pull spec (would escape the managed root): {rel:?}"),
        ));
    }
    Ok(base.join(rel_path))
}

/// Stage a cached file into the managed store: hard-link (no extra bytes) with
/// a copy fallback for cross-device links; skip if a same-size target exists.
///
/// `dest` is always a validated path from [`safe_join`] (the sole caller checks
/// the operator-supplied id/filenames can't escape the managed root), and `src`
/// is hf-hub's own cache path — so the fs sinks below are not attacker-directed.
async fn stage_file(src: &Path, dest: &Path) -> std::io::Result<()> {
    // hf-hub's cache path is usually a symlink into its blob store; resolve it
    // so the hard link points at the real file.
    let real_src = tokio::fs::canonicalize(src)
        .await
        .unwrap_or_else(|_| src.to_path_buf());
    let src_len = tokio::fs::metadata(&real_src).await?.len();

    if let Ok(meta) = tokio::fs::metadata(dest).await {
        if meta.len() == src_len {
            return Ok(()); // idempotent: already staged
        }
        tokio::fs::remove_file(dest).await?;
    }

    // `dest` is validated by `safe_join` (sole caller) and `src` is hf-hub's own
    // cache path; neither is attacker-directed. Directives silence the taint rule.
    // Prefer a hard link (no extra bytes); fall back to a copy across devices.
    let linked = tokio::fs::hard_link(&real_src, dest).await; // nosemgrep: rust.actix.path-traversal.tainted-path.tainted-path
    if linked.is_err() {
        tokio::fs::copy(&real_src, dest).await?; // nosemgrep: rust.actix.path-traversal.tainted-path.tainted-path
    }
    Ok(())
}

#[cfg(all(test, feature = "hf-download"))]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const GB: u64 = 1024 * 1024 * 1024;
    const MB: u64 = 1024 * 1024;

    #[test]
    fn evaluate_fit_rejects_large_model_on_small_ram() {
        // 30 GB model, ample disk, only 8 GB RAM.
        let report = evaluate_fit(30 * GB, 500 * GB, 8 * GB, 6 * GB);
        assert!(!report.fits);
        let reason = report.reason.expect("a reason");
        assert!(
            reason.to_lowercase().contains("ram"),
            "expected a RAM reason, got: {reason}"
        );
    }

    #[test]
    fn evaluate_fit_accepts_tiny_model_with_ample_resources() {
        let report = evaluate_fit(50 * MB, 500 * GB, 32 * GB, 24 * GB);
        assert!(report.fits);
        assert!(report.reason.is_none());
    }

    #[test]
    fn evaluate_fit_rejects_model_larger_than_disk() {
        // 100 GB model, only 10 GB free disk, but plenty of RAM.
        let report = evaluate_fit(100 * GB, 10 * GB, 256 * GB, 200 * GB);
        assert!(!report.fits);
        let reason = report.reason.expect("a reason");
        assert!(
            reason.to_lowercase().contains("disk"),
            "expected a disk reason, got: {reason}"
        );
    }

    #[test]
    fn fmt_bytes_uses_gb_then_mb() {
        assert_eq!(fmt_bytes(18 * GB), "18.0 GB");
        assert_eq!(fmt_bytes(512 * MB), "512.0 MB");
    }

    #[test]
    fn safe_join_allows_plain_names_and_rejects_escapes() {
        let base = Path::new("/managed/id");
        assert!(safe_join(base, "model.gguf").is_ok());
        assert!(safe_join(base, "sub/tokenizer.json").is_ok());
        assert!(safe_join(base, "../escape").is_err());
        assert!(safe_join(base, "a/../../escape").is_err());
        assert!(safe_join(base, "/etc/passwd").is_err());
        assert!(safe_join(base, "").is_err());
    }

    #[tokio::test]
    async fn pull_rejects_path_traversal_in_filename_before_any_network() {
        // A `..` filename must be rejected up front (offline) — no size probe,
        // no download, nothing written outside the managed root.
        let dir = TempDir::new().unwrap();
        let puller = HfHubPuller::new(ManagedResolver::new(dir.path()), None);
        let spec = PullSpec {
            repo: "org/repo".into(),
            revision: None,
            id: "safe-id".into(),
            name: None,
            format: ModelFormat::Gguf,
            files: vec!["../../etc/evil".into()],
        };
        match puller.pull(&spec).await {
            Err(PullError::Io(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
            }
            other => panic!("expected Io(InvalidInput) for traversal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pull_rejects_empty_files() {
        let dir = TempDir::new().unwrap();
        let puller = HfHubPuller::new(ManagedResolver::new(dir.path()), None);
        let spec = PullSpec {
            repo: "org/repo".into(),
            revision: None,
            id: "x".into(),
            name: None,
            format: ModelFormat::Gguf,
            files: vec![],
        };
        match puller.pull(&spec).await {
            Err(PullError::EmptySpec) => {}
            other => panic!("expected EmptySpec, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn register_local_places_and_registers_the_entry() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("managed");
        tokio::fs::create_dir_all(&root).await.unwrap();
        let model = root.join("m.gguf");
        tokio::fs::write(&model, b"hello-gguf").await.unwrap(); // 10 bytes

        let puller = HfHubPuller::new(ManagedResolver::new(&root), None);
        let entry = puller
            .register_local(
                "m1",
                Some("My Model".into()),
                ModelFormat::Gguf,
                std::slice::from_ref(&model),
            )
            .await
            .unwrap();

        assert_eq!(entry.id, "m1");
        assert_eq!(entry.name, "My Model");
        assert_eq!(entry.format, ModelFormat::Gguf);
        assert_eq!(entry.size_bytes, Some(10));
        assert_eq!(entry.source.path(), model.as_path());

        // Registered in the managed index and resolvable through a fresh instance.
        let managed = ManagedResolver::new(&root);
        let got = managed.resolve("m1").await.unwrap().expect("registered");
        assert_eq!(got.source.path(), model.as_path());
    }

    /// End-to-end pull of a tiny public GGUF. Documents real usage; ignored so
    /// the default test run stays offline. Run with `--ignored` and network.
    #[tokio::test]
    #[ignore = "network: downloads a tiny public GGUF from the HF hub"]
    async fn e2e_pull_tiny_public_gguf() {
        let dir = TempDir::new().unwrap();
        let puller = HfHubPuller::new(ManagedResolver::new(dir.path()), None);
        let spec = PullSpec {
            repo: "ggml-org/models".into(),
            revision: None,
            id: "tinyllamas-stories260k".into(),
            name: Some("TinyLlamas stories260K".into()),
            format: ModelFormat::Gguf,
            files: vec!["tinyllamas/stories260K.gguf".into()],
        };
        let entry = puller.pull(&spec).await.expect("pull succeeds");
        assert_eq!(entry.id, "tinyllamas-stories260k");
        assert!(entry.size_bytes.unwrap() > 0);
        assert!(entry.source.path().exists());
    }
}
