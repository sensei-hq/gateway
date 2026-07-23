//! Local Kokoro-82M text-to-speech adapter (gh#23).
//!
//! Wraps the `sensei-kokoro` engine ([`::kokoro::KokoroTts`]) as a
//! [`TtsModel`](kernel::adapters::capability::TtsModel) — the first *local* TTS
//! provider (chat/embeddings aside, TTS was cloud-only before this).
//!
//! Model resolution mirrors [`super::OrtAdapter`]: `entry.source.path()` points
//! at the ONNX file, and the voice pack (`voices.bin`) + misaki lexicon
//! (`us_gold.json`) are read from siblings in the same directory — so an
//! `hf-download`-managed model dir and an external user path both work. English
//! only for now; the language picks the lexicon + phonemizer variant.

use async_trait::async_trait;
use kernel::registry::ModelEntry;
use kernel::types::config::RouterConfig;
use kernel::types::error::GatewayError;
use kernel::types::io::{TtsRequest, TtsResponse};

use ::kokoro::{EnglishG2p, KokoroModel, KokoroTts, Lexicon, Voices};

/// The English variant to synthesize — selects the phonemizer + lexicon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KokoroLang {
    /// American English (`us_*` lexicon; `af_` / `am_` voices).
    American,
    /// British English (`gb_*` lexicon; `bf_` / `bm_` voices).
    British,
}

/// Construction-time configuration for [`KokoroAdapter`].
#[derive(Debug, Clone)]
pub struct KokoroConfig {
    /// Adapter id (the router name it registers under).
    pub adapter_id: String,
    /// Model id a request may pin; a mismatch is rejected.
    pub model_id: String,
    /// English variant to phonemize with.
    pub lang: KokoroLang,
    /// Voice-pack filename, resolved as a sibling of the ONNX model.
    pub voices_file: String,
    /// misaki lexicon (JSON) filename, resolved as a sibling of the ONNX model.
    pub lexicon_file: String,
}

impl Default for KokoroConfig {
    fn default() -> Self {
        Self {
            adapter_id: "kokoro".into(),
            model_id: "kokoro".into(),
            lang: KokoroLang::American,
            voices_file: "voices.bin".into(),
            lexicon_file: "us_gold.json".into(),
        }
    }
}

impl KokoroConfig {
    /// Config for the directory layout the HF pull (`HfKokoro` plan) produces —
    /// the model under `onnx/`, voices under `voices/`. Since `voices_file` /
    /// `lexicon_file` resolve relative to the model's parent (`onnx/`), both use
    /// `../` paths. `voice` is a voice id such as `"af_heart"`; the lexicon
    /// (`us_gold.json`) is the operator-supplied sibling at the dir root.
    pub fn hf_layout(voice: &str) -> Self {
        Self {
            voices_file: format!("../voices/{voice}.bin"),
            lexicon_file: "../us_gold.json".into(),
            ..Self::default()
        }
    }
}

/// In-process TTS adapter backed by Kokoro-82M via ONNX Runtime.
pub struct KokoroAdapter {
    config: KokoroConfig,
    tts: KokoroTts,
}

impl KokoroAdapter {
    /// Load the ONNX model plus its sibling voice pack + lexicon from a resolved
    /// [`ModelEntry`]. `entry.source.path()` is the ONNX file; the voice pack and
    /// lexicon filenames come from [`KokoroConfig`] and are read from the same
    /// directory.
    pub fn load(entry: &ModelEntry, config: KokoroConfig) -> Result<Self, GatewayError> {
        let onnx_path = entry.source.path();
        let dir = onnx_path
            .parent()
            .ok_or_else(|| Self::err(&config, "model path has no parent directory"))?;

        let model = KokoroModel::from_path(onnx_path)
            .map_err(|e| Self::err(&config, format!("load model {}: {e}", onnx_path.display())))?;

        let voices_path = dir.join(&config.voices_file);
        let voices_bytes = std::fs::read(&voices_path).map_err(|e| {
            Self::err(
                &config,
                format!("read voices {}: {e}", voices_path.display()),
            )
        })?;
        let voices = Voices::from_bytes(&voices_bytes)
            .map_err(|e| Self::err(&config, format!("parse voices: {e}")))?;

        let lexicon_path = dir.join(&config.lexicon_file);
        let lexicon_json = std::fs::read_to_string(&lexicon_path).map_err(|e| {
            Self::err(
                &config,
                format!("read lexicon {}: {e}", lexicon_path.display()),
            )
        })?;
        let lexicon = Lexicon::from_misaki_json(&lexicon_json)
            .map_err(|e| Self::err(&config, format!("parse lexicon: {e}")))?;

        let g2p = match config.lang {
            KokoroLang::American => EnglishG2p::american(lexicon),
            KokoroLang::British => EnglishG2p::british(lexicon),
        };
        let tts = KokoroTts::new(Box::new(g2p), voices, model);
        Ok(Self { config, tts })
    }

    fn err(config: &KokoroConfig, message: impl Into<String>) -> GatewayError {
        GatewayError::ProviderError {
            adapter: config.adapter_id.clone(),
            message: message.into(),
            status: None,
        }
    }
}

impl kernel::adapters::capability::Model for KokoroAdapter {
    fn id(&self) -> &str {
        &self.config.adapter_id
    }
}

#[async_trait]
impl kernel::adapters::capability::TtsModel for KokoroAdapter {
    async fn speak(
        &self,
        _cfg: &RouterConfig,
        req: &TtsRequest,
    ) -> Result<TtsResponse, GatewayError> {
        // An explicit, non-matching model name is rejected up front (mirrors the
        // other adapters' model-resolution contract).
        if let Some(requested) = &req.model
            && requested != &self.config.model_id
        {
            return Err(GatewayError::ModelUnavailable {
                adapter: self.config.adapter_id.clone(),
                model: requested.clone(),
            });
        }

        let speed = req.speed.unwrap_or(1.0);
        // Kokoro emits 24 kHz WAV; honoring `req.output_format` (e.g. MP3) needs
        // a transcode step — a follow-up.
        let audio = self
            .tts
            .synthesize_wav(&req.text, speed)
            .map_err(|e| Self::err(&self.config, format!("synthesize: {e}")))?;

        Ok(TtsResponse {
            audio,
            degraded: false,
        })
    }
}

#[async_trait]
impl kernel::adapters::RegisterInto for KokoroAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &kernel::adapters::AdapterRegistry) {
        reg.register_tts(self).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kernel::registry::{ModelFormat, ModelSource};
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn external_entry(path: impl Into<PathBuf>) -> ModelEntry {
        ModelEntry {
            id: "test".into(),
            name: "test".into(),
            format: ModelFormat::Onnx,
            source: ModelSource::External { path: path.into() },
            sha256: None,
            size_bytes: None,
        }
    }

    #[test]
    fn config_defaults_are_american_with_sibling_filenames() {
        let cfg = KokoroConfig::default();
        assert_eq!(cfg.adapter_id, "kokoro");
        assert_eq!(cfg.lang, KokoroLang::American);
        assert_eq!(cfg.voices_file, "voices.bin");
        assert_eq!(cfg.lexicon_file, "us_gold.json");
    }

    #[test]
    fn hf_layout_uses_relative_sibling_paths() {
        let cfg = KokoroConfig::hf_layout("af_heart");
        assert_eq!(cfg.voices_file, "../voices/af_heart.bin");
        assert_eq!(cfg.lexicon_file, "../us_gold.json");
        assert_eq!(cfg.lang, KokoroLang::American);
    }

    #[test]
    fn load_rejects_missing_model_file_with_provider_error() {
        let entry = external_entry("/definitely/not/here/model.onnx");
        let err = match KokoroAdapter::load(&entry, KokoroConfig::default()) {
            Ok(_) => panic!("expected load to fail"),
            Err(e) => e,
        };
        match err {
            GatewayError::ProviderError { message, .. } => {
                assert!(message.contains("load model"), "got: {message}");
            }
            other => panic!("expected ProviderError, got {other:?}"),
        }
    }

    #[test]
    fn load_rejects_invalid_onnx_bytes_with_provider_error() {
        // A file that exists but isn't a valid ONNX graph fails at model load,
        // before the voices/lexicon siblings are read.
        let dir = TempDir::new().unwrap();
        let onnx = dir.path().join("model.onnx");
        std::fs::write(&onnx, b"not really onnx").unwrap();
        let entry = external_entry(&onnx);
        let err = match KokoroAdapter::load(&entry, KokoroConfig::default()) {
            Ok(_) => panic!("expected load to fail"),
            Err(e) => e,
        };
        assert!(
            matches!(err, GatewayError::ProviderError { ref message, .. } if message.contains("load model")),
            "got {err:?}"
        );
    }

    /// End-to-end TTS against a real Kokoro model directory (model.onnx +
    /// voices.bin + us_gold.json). Run with:
    ///
    ///     KOKORO_DIR=/path/to/kokoro cargo test -p sensei-local-providers \
    ///       --features kokoro -- --ignored
    #[tokio::test]
    #[ignore = "requires KOKORO_DIR with model.onnx + voices.bin + us_gold.json"]
    async fn speak_against_a_real_model_returns_wav() {
        use kernel::adapters::capability::TtsModel;
        use kernel::types::request::AudioFormat;

        let dir = std::env::var("KOKORO_DIR").expect("KOKORO_DIR must be set");
        let entry = external_entry(PathBuf::from(&dir).join("model.onnx"));
        let adapter = KokoroAdapter::load(&entry, KokoroConfig::default()).expect("load");

        let req = TtsRequest {
            model: None,
            text: "Hello, world.".into(),
            voice: None,
            speed: None,
            output_format: AudioFormat::Wav,
        };
        let router = RouterConfig {
            url: String::new(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: std::collections::HashMap::new(),
        };
        let resp = adapter.speak(&router, &req).await.expect("speak");
        assert!(!resp.audio.is_empty(), "expected non-empty WAV audio");
        assert_eq!(&resp.audio[0..4], b"RIFF");
    }
}
