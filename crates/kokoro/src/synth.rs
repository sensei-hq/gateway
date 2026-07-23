//! End-to-end Kokoro synthesizer — behind the `onnx` feature.
//!
//! Composes the modular pieces into `text → WAV`: a per-language [`G2p`]
//! (dependency-injected) → the shared [`tokenize`](crate::tokenizer::tokenize) →
//! [`Voices::style_for`] → the [`KokoroModel`] → [`pcm_f32_to_wav`]. Adding a
//! language means passing a different `Box<dyn G2p>`; nothing else changes.

use crate::audio::pcm_f32_to_wav;
use crate::error::KokoroError;
use crate::g2p::G2p;
use crate::model::KokoroModel;
use crate::tokenizer::tokenize;
use crate::voices::Voices;

/// A ready-to-use Kokoro text-to-speech pipeline: an injected G2P frontend plus
/// the shared voices + model.
pub struct KokoroTts {
    g2p: Box<dyn G2p>,
    voices: Voices,
    model: KokoroModel,
}

impl KokoroTts {
    /// Assemble a synthesizer from a language G2P, a voice pack, and a model.
    pub fn new(g2p: Box<dyn G2p>, voices: Voices, model: KokoroModel) -> Self {
        Self { g2p, voices, model }
    }

    /// Synthesize `text` to a 24 kHz, 16-bit mono WAV byte buffer.
    /// `speed` is the playback rate (`1.0` = normal).
    pub fn synthesize_wav(&self, text: &str, speed: f32) -> Result<Vec<u8>, KokoroError> {
        Ok(pcm_f32_to_wav(&self.synthesize_pcm(text, speed)?))
    }

    /// Synthesize `text` to raw mono `f32` PCM at [`crate::audio::SAMPLE_RATE`].
    pub fn synthesize_pcm(&self, text: &str, speed: f32) -> Result<Vec<f32>, KokoroError> {
        let phonemes = self.g2p.phonemize(text);
        let tokenized = tokenize(&phonemes);
        let style = self.voices.style_for(tokenized.phoneme_count);
        self.model.synthesize(&tokenized.input_ids, style, speed)
    }
}
