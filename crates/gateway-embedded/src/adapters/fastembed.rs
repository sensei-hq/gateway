//! In-process embedding adapter built on the `fastembed` crate.
//!
//! Why this exists alongside [`super::LlamaCppAdapter`]: fastembed wraps
//! ONNX Runtime and ships hand-tuned, pre-quantised ONNX exports for the
//! popular BERT-class embedding models (MiniLM, BGE, nomic-embed). For
//! batched embedding workloads it consistently matches or beats raw ort
//! on the rust-embedding-bench harness; for sensei the value is "one
//! library that loads any of the standard embedding models with the
//! correct pooling and tokenizer defaults already wired."
//!
//! Model resolution: this adapter takes a [`ModelEntry`] whose
//! `source.path()` points at the **ONNX file** of an embedding model.
//! The companion tokenizer files (`tokenizer.json`, `config.json`,
//! `special_tokens_map.json`, `tokenizer_config.json`) are expected to
//! live in the same directory — that's the layout produced by both
//! Qdrant's pre-optimised exports (which fastembed itself uses) and
//! optimum's `optimum-cli export onnx` output, so the same files we
//! grab in `minilm-bench/reference/download_qdrant.py` work as-is.
//!
//! Note on `ModelSource::Ollama` entries: those resolve to GGUF blobs,
//! not ONNX. Loading an Ollama-sourced entry through this adapter will
//! fail at fastembed's ONNX parse step with a clear error.

use crate::registry::ModelEntry;
use async_trait::async_trait;
use fastembed::{
    InitOptionsUserDefined, Pooling, QuantizationMode, TextEmbedding, TokenizerFiles,
    UserDefinedEmbeddingModel,
};
use kernel::types::config::RouterConfig;
use kernel::types::error::GatewayError;
use kernel::types::io::{EmbedRequest, EmbedResponse};
use std::path::Path;
use std::sync::Mutex;

/// Construction-time configuration for [`FastembedAdapter`].
#[derive(Debug, Clone)]
pub struct FastembedConfig {
    /// Adapter id surfaced to the gateway. Defaults to `"fastembed"`.
    pub adapter_id: String,
    /// Stable sensei model id this adapter serves. Requests whose `model`
    /// field disagrees return [`GatewayError::ModelUnavailable`].
    pub model_id: String,
    /// Max input length passed to fastembed's tokenizer / ORT session.
    /// Sequence-transformers BERT defaults expect 256.
    pub max_length: usize,
    /// Pooling strategy. Mean pooling matches sentence-transformers
    /// defaults; CLS matches the original BERT pretraining target. The
    /// model export determines which one is correct — Qdrant's MiniLM
    /// uses Mean.
    pub pooling: Pooling,
    /// Quantisation mode of the on-disk ONNX. Most public exports are
    /// already quantised; fastembed needs to know the layout so it
    /// dequantises (or doesn't) at the right step.
    pub quantization: QuantizationMode,
}

impl Default for FastembedConfig {
    fn default() -> Self {
        Self {
            adapter_id: "fastembed".into(),
            model_id: "default".into(),
            max_length: 256,
            pooling: Pooling::Mean,
            quantization: QuantizationMode::None,
        }
    }
}

impl FastembedConfig {
    /// Sensible defaults for a sentence-transformers BERT-class model
    /// (MiniLM / BGE / nomic-embed) — mean pooling, fp32, 256 max tokens.
    pub fn bert(model_id: impl Into<String>) -> Self {
        Self {
            model_id: model_id.into(),
            ..Default::default()
        }
    }
}

/// In-process embedding adapter wrapping [`fastembed::TextEmbedding`].
pub struct FastembedAdapter {
    config: FastembedConfig,
    /// `TextEmbedding::embed` takes `&mut self`, so concurrent
    /// `execute()` calls serialise on this mutex. Mirrors how
    /// [`super::LlamaCppAdapter`] handles its context.
    inner: Mutex<TextEmbedding>,
}

impl FastembedAdapter {
    /// Load an ONNX embedding model and its tokenizer files from disk.
    ///
    /// `entry.source.path()` must point at the ONNX file; the four
    /// tokenizer files are read from the same parent directory.
    pub fn load(entry: &ModelEntry, config: FastembedConfig) -> Result<Self, GatewayError> {
        let onnx_path = entry.source.path();
        let dir = onnx_path
            .parent()
            .ok_or_else(|| Self::err(&config, "model path has no parent directory"))?;

        let onnx_bytes = read_required(onnx_path, &config)?;
        let tokenizer_files = TokenizerFiles {
            tokenizer_file: read_required(&dir.join("tokenizer.json"), &config)?,
            config_file: read_required(&dir.join("config.json"), &config)?,
            special_tokens_map_file: read_required(&dir.join("special_tokens_map.json"), &config)?,
            tokenizer_config_file: read_required(&dir.join("tokenizer_config.json"), &config)?,
        };

        let user_model = UserDefinedEmbeddingModel::new(onnx_bytes, tokenizer_files)
            .with_pooling(config.pooling.clone())
            .with_quantization(config.quantization);
        let init = InitOptionsUserDefined::new().with_max_length(config.max_length);

        let embedding = TextEmbedding::try_new_from_user_defined(user_model, init)
            .map_err(|e| Self::err(&config, format!("TextEmbedding::try_new: {e}")))?;

        Ok(Self {
            config,
            inner: Mutex::new(embedding),
        })
    }

    /// Public, trait-free embedding entry point. Mirrors
    /// [`super::LlamaCppAdapter::embed`] for symmetry — sensei-internal
    /// callers that already hold the adapter can skip the trait envelope.
    pub fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, GatewayError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let mut model = self
            .inner
            .lock()
            .map_err(|_| self.adapter_err("inner mutex poisoned"))?;
        let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
        model
            .embed(refs, None)
            .map_err(|e| self.adapter_err(format!("fastembed: {e}")))
    }

    fn err(config: &FastembedConfig, message: impl Into<String>) -> GatewayError {
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

fn read_required(path: &Path, config: &FastembedConfig) -> Result<Vec<u8>, GatewayError> {
    std::fs::read(path)
        .map_err(|e| FastembedAdapter::err(config, format!("read {}: {e}", path.display())))
}

// ---------------------------------------------------------------------------
// Capability traits (target model). Traits + RegisterInto referenced by full path.
// Embed-only: fastembed serves the TextEmbed capability and nothing else.
// ---------------------------------------------------------------------------

impl kernel::adapters::capability::Model for FastembedAdapter {
    fn id(&self) -> &str {
        &self.config.adapter_id
    }
}

#[async_trait]
impl kernel::adapters::capability::EmbedModel for FastembedAdapter {
    async fn embed(
        &self,
        _cfg: &RouterConfig,
        req: &EmbedRequest,
    ) -> Result<EmbedResponse, GatewayError> {
        // Preserve the legacy `execute` model resolution: reject a request
        // pinned to a model this adapter does not serve.
        if let Some(requested) = &req.model
            && requested != &self.config.model_id
        {
            return Err(GatewayError::ModelUnavailable {
                adapter: self.config.adapter_id.clone(),
                model: requested.clone(),
            });
        }
        // Inherent `FastembedAdapter::embed(&[String])` does the fastembed
        // model loading/pooling; fastembed reports no token usage.
        let embeddings = self.embed(&req.texts)?;
        Ok(EmbedResponse {
            embeddings,
            usage: None,
            degraded: false,
        })
    }
}

#[async_trait]
impl kernel::adapters::RegisterInto for FastembedAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &kernel::adapters::AdapterRegistry) {
        reg.register_embed(self).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{ModelFormat, ModelSource};
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
        let cfg = FastembedConfig::bert("all-minilm-qdrant");
        assert_eq!(cfg.model_id, "all-minilm-qdrant");
        assert_eq!(cfg.adapter_id, "fastembed");
        assert_eq!(cfg.max_length, 256);
        assert!(matches!(cfg.pooling, Pooling::Mean));
    }

    #[test]
    fn load_rejects_missing_onnx_file_with_provider_error() {
        let entry = external_entry("/definitely/does/not/exist/model.onnx");
        let err = match FastembedAdapter::load(&entry, FastembedConfig::bert("x")) {
            Ok(_) => panic!("expected load to fail"),
            Err(e) => e,
        };
        match err {
            GatewayError::ProviderError { message, .. } => {
                assert!(message.contains("read"), "got: {message}");
            }
            other => panic!("expected ProviderError, got {other:?}"),
        }
    }

    #[test]
    fn load_rejects_directory_missing_tokenizer_with_provider_error() {
        let dir = TempDir::new().unwrap();
        // Create a fake ONNX file but no tokenizer siblings.
        let onnx = dir.path().join("model.onnx");
        std::fs::write(&onnx, b"\x08\x01").unwrap();
        let entry = external_entry(&onnx);
        let err = match FastembedAdapter::load(&entry, FastembedConfig::bert("x")) {
            Ok(_) => panic!("expected load to fail"),
            Err(e) => e,
        };
        match err {
            GatewayError::ProviderError { message, .. } => {
                assert!(message.contains("tokenizer.json"), "got: {message}");
            }
            other => panic!("expected ProviderError, got {other:?}"),
        }
    }

    /// End-to-end embedding against a real fastembed-compatible ONNX
    /// directory (model.onnx + tokenizer.json + config.json +
    /// special_tokens_map.json + tokenizer_config.json). The
    /// `minilm-bench` harness produces exactly this layout under
    /// `models/all-MiniLM-L6-v2-qdrant/`. Run with:
    ///
    ///     FASTEMBED_TEST_DIR=$HOME/...rust-embedding-bench/models/all-MiniLM-L6-v2-qdrant \
    ///       cargo test -p gateway-embedded --features fastembed -- --ignored
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires FASTEMBED_TEST_DIR env var pointing at a directory with the standard Qdrant ONNX layout"]
    async fn embed_against_real_qdrant_layout_returns_dense_vectors() {
        let dir = std::env::var("FASTEMBED_TEST_DIR")
            .expect("FASTEMBED_TEST_DIR must point at an ONNX embedding model directory");
        let entry = external_entry(PathBuf::from(&dir).join("model.onnx"));

        let adapter =
            FastembedAdapter::load(&entry, FastembedConfig::bert("test-fastembed")).expect("load");

        let embeddings = adapter
            .embed(&[
                "hello world".to_string(),
                "the quick brown fox jumps over the lazy dog".to_string(),
            ])
            .expect("embed");

        assert_eq!(embeddings.len(), 2);
        for (i, v) in embeddings.iter().enumerate() {
            assert!(!v.is_empty(), "embedding[{i}] dim should be > 0");
            // fastembed L2-normalises by default for BERT-class models, so
            // the magnitudes should be unit length.
            let mag: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            assert!(
                (mag - 1.0).abs() < 1e-3,
                "embedding[{i}]: expected ~unit length, got |v|={mag}",
            );
        }
    }

    /// The capability-trait `Model::id` reports the same id as the legacy
    /// `Model::id` (reads `config.adapter_id`). Constructing a
    /// `FastembedAdapter` needs a real ONNX model, so this shares the
    /// `FASTEMBED_TEST_DIR` guard of the e2e test above.
    #[test]
    #[ignore = "requires FASTEMBED_TEST_DIR env var pointing at a directory with the standard Qdrant ONNX layout"]
    fn capability_model_id_matches_adapter_id() {
        let dir = std::env::var("FASTEMBED_TEST_DIR")
            .expect("FASTEMBED_TEST_DIR must point at an ONNX embedding model directory");
        let entry = external_entry(PathBuf::from(&dir).join("model.onnx"));
        let adapter =
            FastembedAdapter::load(&entry, FastembedConfig::bert("test-fastembed")).expect("load");

        // Full-path call to `Model::id`.
        assert_eq!(
            kernel::adapters::capability::Model::id(&adapter),
            "fastembed"
        );
    }
}
