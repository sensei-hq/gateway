//! Local **Kokoro-82M** text-to-speech engine building blocks (gh#23).
//!
//! Kokoro is an Apache-2.0 StyleTTS2 model (`hexgrad/Kokoro-82M`; ONNX export at
//! `onnx-community/Kokoro-82M-v1.0-ONNX`). This crate holds the in-process,
//! dependency-light pieces of its inference pipeline, split so the one axis that
//! varies by language — the grapheme-to-phoneme frontend — is a pluggable trait
//! rather than a monolith:
//!
//! - [`lang`] — the [`Lang`] enum + inference from a voice id.
//! - [`vocab`] — the **shared** IPA phoneme → token-id table (one set for all
//!   languages; it is baked into the model's embedding).
//! - [`tokenizer`] — a phoneme string → padded `input_ids` (shared).
//! - [`g2p`] — the [`G2p`] **strategy trait** + implementations per language
//!   ([`g2p::en`] today; `ja` / `zh` / … later). *This* is where languages differ.
//! - [`voices`] — voice-pack parsing + style-vector selection by phoneme count.
//! - [`audio`] — 24 kHz `f32` PCM → WAV bytes (shared).
//!
//! The English G2P ([`EnglishG2p`]) is lexicon-driven — load misaki's Apache-2.0
//! dictionary with [`Lexicon::from_misaki_json`]. ONNX inference (`input_ids` +
//! `style` + `speed` → audio, behind an `onnx` feature) lands in a follow-up PR;
//! see gh#23.
//!
//! # Pipeline
//! ```text
//! text ──[G2p (per language)]──▶ phonemes ──[tokenize]──▶ input_ids ─┐
//!                                     │                               ├─▶ model ─▶ f32 PCM ─▶ [pcm_f32_to_wav]
//!               voice id ─[Voices::style_for(phoneme_count)]─▶ ref_s ─┘
//! ```

pub mod audio;
pub mod error;
pub mod g2p;
pub mod lang;
pub mod tokenizer;
pub mod vocab;
pub mod voices;

pub use error::KokoroError;
pub use g2p::G2p;
pub use g2p::en::{EnglishG2p, Lexicon};
pub use lang::Lang;
pub use tokenizer::{Tokenized, tokenize};
pub use voices::Voices;

/// The pre-inference step: tokenize a phoneme string and pick the matching voice
/// style vector, ready to feed the model (`input_ids`, `style`).
///
/// This composes [`tokenize`] with [`Voices::style_for`] using the tokenized
/// phoneme count as the style index — the two inputs the model needs alongside a
/// fixed `speed`. Language selection happens upstream, where the phonemes are
/// produced by a [`G2p`] implementation.
pub fn prepare_inputs<'v>(phonemes: &str, voices: &'v Voices) -> (Tokenized, &'v [f32]) {
    let tokenized = tokenize(phonemes);
    let style = voices.style_for(tokenized.phoneme_count);
    (tokenized, style)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepare_inputs_pairs_tokens_with_the_matching_style_row() {
        // 4-row pack; "ba" → 2 phonemes → style row index 2.
        let mut bytes = Vec::new();
        for i in 0..4u32 {
            for _ in 0..Voices::DIM {
                bytes.extend_from_slice(&(i as f32).to_le_bytes());
            }
        }
        let voices = Voices::from_bytes(&bytes).unwrap();

        let (tok, style) = prepare_inputs("ba", &voices);
        assert_eq!(tok.phoneme_count, 2);
        assert_eq!(tok.input_ids, vec![0, 44, 43, 0]);
        assert_eq!(style, [2.0; Voices::DIM]);
    }
}
