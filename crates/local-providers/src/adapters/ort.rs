//! Direct ONNX Runtime adapter — the lowest-level of the three embedded
//! engines.
//!
//! `FastembedAdapter` already runs through ORT under the hood, so why a
//! second adapter? Three reasons:
//!
//! 1. **Custom pooling.** Fastembed pulls pooling from its bundled
//!    `Pooling` enum; this adapter exposes the full lever set —
//!    sentence-transformers Mean pooling is the default, CLS-token
//!    pooling is available, and a Last-token strategy can be added
//!    without a third-party API. Useful for models whose export
//!    didn't bake pooling into the graph.
//! 2. **Arbitrary BERT-class ONNX exports.** Fastembed gates the
//!    embedding-model surface behind its own enum / known repos. This
//!    adapter loads any ONNX whose tokenizer files sit alongside it
//!    (the same layout `optimum-cli export onnx` produces).
//! 3. **Execution-provider tuning.** The ORT `SessionBuilder` exposes
//!    intra/inter-op threads, optimisation level, and execution
//!    providers (CoreML / CUDA / TensorRT). Sensei builds opt into
//!    the platform-specific ones via the `ort` crate's feature flags;
//!    this adapter just forwards thread counts today and leaves the
//!    EP plumbing for a follow-up.
//!
//! Performance: `minilm-bench` measured ORT (fp32) at 1.10 ms p50 for
//! single short-text queries on Apple M4 Max — slightly faster than
//! fastembed (1.71 ms) at batch=1, both saturating around the same
//! throughput at batch=32. The tradeoff is "less hand-holding": you
//! pick the pooling strategy, you supply the tokenizer files, you
//! own the ONNX export.
//!
//! Model resolution: identical contract to [`super::FastembedAdapter`]
//! — `entry.source.path()` points at the ONNX file, sibling
//! `tokenizer.json` is required.

use ::ort::{
    session::{Session, builder::GraphOptimizationLevel},
    value::Tensor,
};
use async_trait::async_trait;
use kernel::types::config::RouterConfig;
use kernel::types::error::GatewayError;
use kernel::types::io::{EmbedRequest, EmbedResponse};
use std::path::Path;
use tokenizers::{PaddingDirection, PaddingParams, PaddingStrategy, Tokenizer};

use crate::math::l2_normalize_in_place;
use kernel::registry::ModelEntry;

/// Pooling strategy applied to the model's per-token hidden states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrtPoolingStrategy {
    /// Average over the last hidden state masked by the attention
    /// mask. The sentence-transformers default for BERT-class models.
    Mean,
    /// Take the first token's hidden state (the [CLS] embedding).
    /// Matches the original BERT pre-training target.
    Cls,
}

/// Construction-time configuration for [`OrtAdapter`].
#[derive(Debug, Clone)]
pub struct OrtConfig {
    pub adapter_id: String,
    pub model_id: String,
    /// Tokenizer truncation length. 256 matches the
    /// sentence-transformers default for MiniLM / BGE.
    pub max_length: usize,
    /// Pooling strategy applied after the forward pass.
    pub pooling: OrtPoolingStrategy,
    /// Intra-op thread count for ORT. The bench showed CPU-bound
    /// embedding workloads scale ~2x from 1 → 8 threads on Apple
    /// Silicon; default 1 keeps adapter-construction cheap and lets
    /// the caller opt into more if they want throughput.
    pub threads: usize,
}

impl Default for OrtConfig {
    fn default() -> Self {
        Self {
            adapter_id: "ort".into(),
            model_id: "default".into(),
            max_length: 256,
            pooling: OrtPoolingStrategy::Mean,
            threads: 1,
        }
    }
}

impl OrtConfig {
    /// Sensible defaults for sentence-transformers BERT-class models
    /// (MiniLM, BGE, nomic-embed): mean pooling, 256 max tokens.
    pub fn bert(model_id: impl Into<String>) -> Self {
        Self {
            model_id: model_id.into(),
            ..Default::default()
        }
    }

    /// Same as [`Self::bert`] but using CLS-token pooling for models
    /// that didn't have pooling baked into the ONNX graph.
    pub fn bert_cls(model_id: impl Into<String>) -> Self {
        Self {
            model_id: model_id.into(),
            pooling: OrtPoolingStrategy::Cls,
            ..Default::default()
        }
    }
}

/// In-process embedding adapter calling ORT directly.
///
/// `Session::run` requires `&mut self`, so concurrent `execute()`
/// calls serialise on a single `std::sync::Mutex`. The work itself
/// is fully blocking on ORT's native side anyway — same approach as
/// [`super::FastembedAdapter`] / [`super::LlamaCppAdapter`].
pub struct OrtAdapter {
    config: OrtConfig,
    session: std::sync::Mutex<Session>,
    tokenizer: Tokenizer,
}

impl OrtAdapter {
    /// Load an ONNX embedding model and its tokenizer. `entry`'s source
    /// path points at the ONNX file; `tokenizer.json` is read from the
    /// same parent directory.
    pub fn load(entry: &ModelEntry, config: OrtConfig) -> Result<Self, GatewayError> {
        let onnx_path = entry.source.path();
        let dir = onnx_path
            .parent()
            .ok_or_else(|| Self::err(&config, "model path has no parent directory"))?;
        let tokenizer_path = dir.join("tokenizer.json");

        let session = Session::builder()
            .map_err(|e| Self::err(&config, format!("session builder: {e}")))?
            .with_optimization_level(GraphOptimizationLevel::Level3)
            .map_err(|e| Self::err(&config, format!("set optimization level: {e}")))?
            .with_intra_threads(config.threads)
            .map_err(|e| Self::err(&config, format!("set intra threads: {e}")))?
            .commit_from_file(onnx_path)
            .map_err(|e| Self::err(&config, format!("load model {}: {e}", onnx_path.display())))?;

        let mut tokenizer = load_tokenizer(&tokenizer_path, &config)?;
        configure_tokenizer(&mut tokenizer, &config)?;

        Ok(Self {
            config,
            session: std::sync::Mutex::new(session),
            tokenizer,
        })
    }

    /// Public, trait-free embedding entry point. Same shape as
    /// [`super::FastembedAdapter::embed`] for symmetry.
    pub fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, GatewayError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let encs = self
            .tokenizer
            .encode_batch(texts.to_vec(), true)
            .map_err(|e| self.adapter_err(format!("tokenize: {e}")))?;

        let batch_size = encs.len();
        let seq_len = encs.first().map(|e| e.get_ids().len()).unwrap_or(0);

        let mut ids: Vec<i64> = Vec::with_capacity(batch_size * seq_len);
        let mut mask: Vec<i64> = Vec::with_capacity(batch_size * seq_len);
        for enc in &encs {
            ids.extend(enc.get_ids().iter().map(|&x| x as i64));
            mask.extend(enc.get_attention_mask().iter().map(|&x| x as i64));
        }
        let tt: Vec<i64> = vec![0; batch_size * seq_len];

        let ids_t = Tensor::<i64>::from_array(([batch_size, seq_len], ids))
            .map_err(|e| self.adapter_err(format!("input_ids tensor: {e}")))?;
        let mask_t = Tensor::<i64>::from_array(([batch_size, seq_len], mask.clone()))
            .map_err(|e| self.adapter_err(format!("attention_mask tensor: {e}")))?;
        let tt_t = Tensor::<i64>::from_array(([batch_size, seq_len], tt))
            .map_err(|e| self.adapter_err(format!("token_type_ids tensor: {e}")))?;

        let mut session = self
            .session
            .lock()
            .map_err(|_| self.adapter_err("session mutex poisoned"))?;
        let outputs = session
            .run(::ort::inputs![
                "input_ids" => ids_t,
                "attention_mask" => mask_t,
                "token_type_ids" => tt_t,
            ])
            .map_err(|e| self.adapter_err(format!("session.run: {e}")))?;

        let (shape, data) = outputs[0]
            .try_extract_tensor::<f32>()
            .map_err(|e| self.adapter_err(format!("extract hidden states: {e}")))?;
        if shape.len() < 3 {
            return Err(self.adapter_err(format!(
                "expected rank-3 output [batch, seq, hidden], got {:?}",
                &shape[..]
            )));
        }
        let seq_out = shape[shape.len() - 2] as usize;
        let hidden = shape[shape.len() - 1] as usize;

        let mut results = Vec::with_capacity(batch_size);
        for b in 0..batch_size {
            let pooled = match self.config.pooling {
                OrtPoolingStrategy::Mean => mean_pool(data, &mask, b, seq_out, hidden),
                OrtPoolingStrategy::Cls => cls_pool(data, b, seq_out, hidden),
            };
            let mut owned = pooled;
            l2_normalize_in_place(&mut owned);
            results.push(owned);
        }
        Ok(results)
    }

    fn err(config: &OrtConfig, message: impl Into<String>) -> GatewayError {
        GatewayError::ProviderError {
            adapter: config.adapter_id.clone(),
            message: message.into(),
            status: None,
        }
    }

    fn adapter_err(&self, message: impl Into<String>) -> GatewayError {
        Self::err(&self.config, message)
    }
}

// ---------------------------------------------------------------------------
// Pure helpers — unit-testable without an ORT session
// ---------------------------------------------------------------------------

/// Mean-pool the per-token hidden states for one batch row, weighted
/// by its attention mask. Matches what sentence-transformers'
/// `Pooling(MEAN)` does in Python.
pub(crate) fn mean_pool(
    data: &[f32],
    mask: &[i64],
    batch_index: usize,
    seq_len: usize,
    hidden: usize,
) -> Vec<f32> {
    let mut pooled = vec![0f32; hidden];
    let mut count = 0f32;
    let mask_offset = batch_index * seq_len;
    let data_offset = batch_index * seq_len * hidden;
    for t in 0..seq_len {
        if mask[mask_offset + t] == 1 {
            let off = data_offset + t * hidden;
            for d in 0..hidden {
                pooled[d] += data[off + d];
            }
            count += 1.0;
        }
    }
    if count > 0.0 {
        for v in pooled.iter_mut() {
            *v /= count;
        }
    }
    pooled
}

/// CLS-token pool: take the very first token's hidden state.
pub(crate) fn cls_pool(
    data: &[f32],
    batch_index: usize,
    seq_len: usize,
    hidden: usize,
) -> Vec<f32> {
    let start = batch_index * seq_len * hidden;
    data[start..start + hidden].to_vec()
}

fn load_tokenizer(path: &Path, config: &OrtConfig) -> Result<Tokenizer, GatewayError> {
    Tokenizer::from_file(path)
        .map_err(|e| OrtAdapter::err(config, format!("read tokenizer {}: {e}", path.display())))
}

fn configure_tokenizer(t: &mut Tokenizer, config: &OrtConfig) -> Result<(), GatewayError> {
    t.with_padding(Some(PaddingParams {
        strategy: PaddingStrategy::BatchLongest,
        direction: PaddingDirection::Right,
        pad_to_multiple_of: None,
        pad_id: 0,
        pad_type_id: 0,
        pad_token: "[PAD]".to_string(),
    }));
    t.with_truncation(Some(tokenizers::TruncationParams {
        max_length: config.max_length,
        strategy: tokenizers::TruncationStrategy::LongestFirst,
        stride: 0,
        direction: tokenizers::TruncationDirection::Right,
    }))
    .map_err(|e| OrtAdapter::err(config, format!("set truncation: {e}")))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Capability traits (see docs/design/adapter-capability-traits.md).
// `Model` / `EmbedModel` / `RegisterInto` are referenced by full path.
// ORT is embed-only.
// ---------------------------------------------------------------------------

impl kernel::adapters::capability::Model for OrtAdapter {
    fn id(&self) -> &str {
        &self.config.adapter_id
    }
}

#[async_trait]
impl kernel::adapters::capability::EmbedModel for OrtAdapter {
    async fn embed(
        &self,
        _config: &RouterConfig,
        req: &EmbedRequest,
    ) -> Result<EmbedResponse, GatewayError> {
        // Model resolution mirrors the `Payload::Embed` arm of `execute`:
        // an explicit, non-matching model name is rejected up front.
        if let Some(requested) = &req.model
            && requested != &self.config.model_id
        {
            return Err(GatewayError::ModelUnavailable {
                adapter: self.config.adapter_id.clone(),
                model: requested.clone(),
            });
        }

        // Inherent `OrtAdapter::embed(&[String])` wins method resolution over
        // this trait method, preserving the ONNX session + tokenizer path.
        let embeddings = self.embed(&req.texts)?;
        Ok(EmbedResponse {
            embeddings,
            usage: None,
            degraded: false,
        })
    }
}

#[async_trait]
impl kernel::adapters::RegisterInto for OrtAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &kernel::adapters::AdapterRegistry) {
        reg.register_embed(self).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kernel::registry::{ModelFormat, ModelSource};
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn external_entry(path: impl Into<PathBuf>) -> ModelEntry {
        let path = path.into();
        ModelEntry {
            id: "test".into(),
            name: "test".into(),
            format: ModelFormat::Onnx,
            source: ModelSource::External { path },
            sha256: None,
            size_bytes: None,
        }
    }

    #[test]
    fn config_bert_builder_sets_sensei_defaults() {
        let cfg = OrtConfig::bert("ort-minilm");
        assert_eq!(cfg.model_id, "ort-minilm");
        assert_eq!(cfg.adapter_id, "ort");
        assert_eq!(cfg.max_length, 256);
        assert_eq!(cfg.pooling, OrtPoolingStrategy::Mean);
        assert_eq!(cfg.threads, 1);
    }

    #[test]
    fn config_bert_cls_swaps_pooling_only() {
        let cfg = OrtConfig::bert_cls("ort-bert");
        assert_eq!(cfg.pooling, OrtPoolingStrategy::Cls);
        // everything else stays at the bert defaults
        assert_eq!(cfg.max_length, 256);
        assert_eq!(cfg.adapter_id, "ort");
    }

    #[test]
    fn load_rejects_missing_onnx_file_with_provider_error() {
        let entry = external_entry("/definitely/does/not/exist/model.onnx");
        let err = match OrtAdapter::load(&entry, OrtConfig::bert("x")) {
            Ok(_) => panic!("expected load to fail"),
            Err(e) => e,
        };
        match err {
            GatewayError::ProviderError { message, .. } => {
                assert!(
                    message.contains("load model") || message.contains("read tokenizer"),
                    "got: {message}"
                );
            }
            other => panic!("expected ProviderError, got {other:?}"),
        }
    }

    #[test]
    fn load_rejects_directory_missing_tokenizer_with_provider_error() {
        let dir = TempDir::new().unwrap();
        // Create a placeholder ONNX file but no tokenizer.json sibling.
        let onnx = dir.path().join("model.onnx");
        std::fs::write(&onnx, b"\x08\x01").unwrap();
        let entry = external_entry(&onnx);
        let err = match OrtAdapter::load(&entry, OrtConfig::bert("x")) {
            Ok(_) => panic!("expected load to fail"),
            Err(e) => e,
        };
        match err {
            GatewayError::ProviderError { message, .. } => {
                // The error comes either at session load (bad ONNX bytes)
                // or at tokenizer load — both are acceptable. The
                // important part is we got a ProviderError, not a panic
                // or a silent default.
                assert!(
                    message.contains("load model")
                        || message.contains("read tokenizer")
                        || message.contains("session"),
                    "got: {message}"
                );
            }
            other => panic!("expected ProviderError, got {other:?}"),
        }
    }

    #[test]
    fn mean_pool_averages_unmasked_tokens_only() {
        // 1 batch, 4 tokens, 2 hidden dims. Mask token at index 3 out.
        // Sequence is [1.0,1.0], [2.0,2.0], [3.0,3.0], [99.0,99.0]
        // with mask [1,1,1,0]; mean of first three rows is [2.0, 2.0].
        let data = vec![
            1.0, 1.0, // tok 0
            2.0, 2.0, // tok 1
            3.0, 3.0, // tok 2
            99.0, 99.0, // tok 3 (masked out)
        ];
        let mask = vec![1i64, 1, 1, 0];
        let pooled = mean_pool(&data, &mask, 0, 4, 2);
        assert_eq!(pooled.len(), 2);
        assert!((pooled[0] - 2.0).abs() < 1e-6);
        assert!((pooled[1] - 2.0).abs() < 1e-6);
    }

    #[test]
    fn mean_pool_handles_all_masked_rows_without_dividing_by_zero() {
        let data = vec![5.0, 5.0, 5.0, 5.0];
        let mask = vec![0i64, 0];
        let pooled = mean_pool(&data, &mask, 0, 2, 2);
        // No unmasked tokens — return the zero vector rather than NaN.
        assert_eq!(pooled, vec![0.0, 0.0]);
    }

    #[test]
    fn cls_pool_returns_first_token_hidden_state() {
        // 2 batches, 3 tokens, 2 hidden dims. CLS for batch 1 is at
        // offset (1 * 3 * 2) = 6 and lasts hidden=2 elements.
        let data = vec![
            // batch 0 CLS = [10, 20]
            10.0, 20.0, 0.0, 0.0, 0.0, 0.0, // batch 1 CLS = [30, 40]
            30.0, 40.0, 0.0, 0.0, 0.0, 0.0,
        ];
        assert_eq!(cls_pool(&data, 0, 3, 2), vec![10.0, 20.0]);
        assert_eq!(cls_pool(&data, 1, 3, 2), vec![30.0, 40.0]);
    }

    #[test]
    fn mean_pool_offsets_correctly_across_batch_rows() {
        // 2 batches, 2 tokens, 2 hidden dims. Mask shape (2, 2).
        // batch 0: [[1,1], [3,3]] mask [1,1] → mean [2,2]
        // batch 1: [[10,10], [20,20]] mask [1,0] → mean [10,10]
        let data = vec![
            1.0, 1.0, 3.0, 3.0, // batch 0
            10.0, 10.0, 20.0, 20.0, // batch 1
        ];
        let mask = vec![1i64, 1, 1, 0];
        assert_eq!(mean_pool(&data, &mask, 0, 2, 2), vec![2.0, 2.0]);
        assert_eq!(mean_pool(&data, &mask, 1, 2, 2), vec![10.0, 10.0]);
    }

    /// End-to-end embedding against a real ONNX directory (model.onnx
    /// + tokenizer.json + the rest). The `minilm-bench` Qdrant cache
    /// has exactly this layout. Run with:
    ///
    ///     ORT_TEST_DIR=$HOME/.../models--Qdrant--all-MiniLM-L6-v2-onnx/snapshots/<id> \
    ///       cargo test -p sensei-local-providers --features ort -- --ignored
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires ORT_TEST_DIR env var pointing at an ONNX embedding model directory"]
    async fn embed_against_real_onnx_returns_unit_length_vectors() {
        let dir = std::env::var("ORT_TEST_DIR")
            .expect("ORT_TEST_DIR must point at an ONNX embedding model directory");
        let entry = external_entry(PathBuf::from(&dir).join("model.onnx"));

        let adapter = OrtAdapter::load(&entry, OrtConfig::bert("test-ort")).expect("load");

        let embeddings = adapter
            .embed(&[
                "hello world".to_string(),
                "the quick brown fox jumps over the lazy dog".to_string(),
            ])
            .expect("embed");

        assert_eq!(embeddings.len(), 2);
        for (i, v) in embeddings.iter().enumerate() {
            assert!(!v.is_empty(), "embedding[{i}] dim should be > 0");
            let mag: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            assert!(
                (mag - 1.0).abs() < 1e-3,
                "embedding[{i}]: expected ~unit length, got |v|={mag}",
            );
        }
    }

    /// The capability-trait `Model::id` must return the same id the legacy
    /// `Model::id` returns (the config's `adapter_id`). Constructing a
    /// real `OrtAdapter` needs a live ONNX session, so this is gated on the
    /// same `ORT_TEST_DIR` as the end-to-end embed test above; it still
    /// compile-checks the full-path `Model::id` wiring on every build.
    #[test]
    #[ignore = "requires ORT_TEST_DIR env var pointing at an ONNX embedding model directory"]
    fn capability_model_id_matches_config_adapter_id() {
        let dir = std::env::var("ORT_TEST_DIR")
            .expect("ORT_TEST_DIR must point at an ONNX embedding model directory");
        let entry = external_entry(PathBuf::from(&dir).join("model.onnx"));
        let adapter = OrtAdapter::load(&entry, OrtConfig::bert("test-ort")).expect("load");
        assert_eq!(kernel::adapters::capability::Model::id(&adapter), "ort");
    }
}
