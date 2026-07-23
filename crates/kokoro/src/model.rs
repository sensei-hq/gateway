//! ONNX inference for Kokoro via ONNX Runtime (`ort`) — behind the `onnx`
//! feature.
//!
//! The model takes three inputs and returns audio:
//! - `input_ids`: `i64`, shape `(1, ≤512)` — the padded phoneme token ids.
//! - `style`: `f32`, shape `(1, 256)` — the voice's `ref_s` style vector.
//! - `speed`: `f32`, shape `(1,)` — playback speed (`1.0` = normal).
//! - output: `f32`, shape `(1, samples)` — 24 kHz mono PCM.
//!
//! `Session::run` needs `&mut self`, so a single [`std::sync::Mutex`] serialises
//! calls — the work is blocking on ORT's native side anyway (same pattern as
//! `local-providers`' ORT adapter).

use std::path::Path;
use std::sync::Mutex;

use ort::session::{Session, builder::GraphOptimizationLevel};
use ort::value::Tensor;

use crate::error::KokoroError;
use crate::voices::Voices;

/// A loaded Kokoro ONNX model.
pub struct KokoroModel {
    session: Mutex<Session>,
}

impl KokoroModel {
    /// Load a Kokoro ONNX model from a file (e.g. `model_q8f16.onnx`).
    pub fn from_path(path: &Path) -> Result<Self, KokoroError> {
        let session = Session::builder()
            .map_err(|e| KokoroError::Model(format!("session builder: {e}")))?
            .with_optimization_level(GraphOptimizationLevel::Level3)
            .map_err(|e| KokoroError::Model(format!("set optimization level: {e}")))?
            .commit_from_file(path)
            .map_err(|e| KokoroError::Model(format!("load {}: {e}", path.display())))?;
        Ok(Self {
            session: Mutex::new(session),
        })
    }

    /// Run inference: padded `input_ids` + a [`Voices::DIM`]-length `style`
    /// vector + `speed` → mono `f32` PCM at [`crate::audio::SAMPLE_RATE`].
    pub fn synthesize(
        &self,
        input_ids: &[i64],
        style: &[f32],
        speed: f32,
    ) -> Result<Vec<f32>, KokoroError> {
        check_style_len(style.len())?;

        let ids = Tensor::<i64>::from_array(([1, input_ids.len()], input_ids.to_vec()))
            .map_err(|e| KokoroError::Model(format!("input_ids tensor: {e}")))?;
        let style = Tensor::<f32>::from_array(([1, style.len()], style.to_vec()))
            .map_err(|e| KokoroError::Model(format!("style tensor: {e}")))?;
        let speed = Tensor::<f32>::from_array(([1], vec![speed]))
            .map_err(|e| KokoroError::Model(format!("speed tensor: {e}")))?;

        let mut session = self
            .session
            .lock()
            .map_err(|_| KokoroError::Model("session mutex poisoned".into()))?;
        let outputs = session
            .run(ort::inputs![
                "input_ids" => ids,
                "style" => style,
                "speed" => speed,
            ])
            .map_err(|e| KokoroError::Model(format!("session.run: {e}")))?;

        let (_shape, data) = outputs[0]
            .try_extract_tensor::<f32>()
            .map_err(|e| KokoroError::Model(format!("extract audio: {e}")))?;
        Ok(data.to_vec())
    }
}

/// Validate the style vector's length against the model's expected `(1, 256)`.
fn check_style_len(len: usize) -> Result<(), KokoroError> {
    if len != Voices::DIM {
        return Err(KokoroError::Model(format!(
            "style vector must be {} floats, got {len}",
            Voices::DIM
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn style_length_is_validated() {
        assert!(check_style_len(Voices::DIM).is_ok());
        let err = check_style_len(10).unwrap_err();
        assert!(
            matches!(err, KokoroError::Model(ref m) if m.contains("style vector")),
            "got {err:?}"
        );
    }

    /// End-to-end inference against a real Kokoro ONNX file. Run with:
    ///
    ///     KOKORO_MODEL=/path/to/model_q8f16.onnx \
    ///       cargo test -p sensei-kokoro --features onnx -- --ignored
    #[test]
    #[ignore = "requires KOKORO_MODEL env var pointing at a Kokoro ONNX file"]
    fn synthesizes_audio_from_a_real_model() {
        let path = std::env::var("KOKORO_MODEL").expect("KOKORO_MODEL must be set");
        let model = KokoroModel::from_path(Path::new(&path)).expect("load model");
        // [PAD, a few phoneme ids, PAD] + a zero style vector: audio content is
        // meaningless, but the run must succeed and emit samples.
        let input_ids = vec![0i64, 43, 44, 43, 0];
        let style = vec![0.0f32; Voices::DIM];
        let pcm = model
            .synthesize(&input_ids, &style, 1.0)
            .expect("synthesize");
        assert!(!pcm.is_empty(), "model should emit audio samples");
    }
}
