//! Grapheme-to-phoneme (G2P): the per-language frontend that turns text into the
//! Kokoro IPA phoneme string the shared [`tokenizer`](crate::tokenizer) expects.
//!
//! This is the **pluggable axis** of the pipeline. The phoneme *vocabulary* is
//! shared across languages (one IPA set baked into the model), so only the G2P
//! strategy and the voice packs vary by [`Lang`](crate::lang::Lang). Implement
//! [`G2p`] once per language — e.g. `en` (a misaki-en port, landing in follow-up
//! work) — and inject it into the synthesizer; nothing else in the pipeline
//! changes. See gh#23.

use crate::lang::Lang;

pub mod en;

/// A per-language grapheme-to-phoneme converter (strategy).
///
/// Implementations map input text to a Kokoro IPA phoneme string built from the
/// symbols in the shared [`crate::vocab`]; out-of-vocabulary handling is the
/// implementation's concern. `Send + Sync` so a `Box<dyn G2p>` can back a shared
/// synthesizer.
pub trait G2p: Send + Sync {
    /// The language this converter handles.
    fn lang(&self) -> Lang;

    /// Convert `text` into a Kokoro IPA phoneme string.
    fn phonemize(&self, text: &str) -> String;
}
