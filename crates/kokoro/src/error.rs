//! Error type for the Kokoro engine.

/// Errors produced by the Kokoro building blocks. Grows as inference and the
/// grapheme-to-phoneme frontend land (see gh#23).
#[derive(Debug, thiserror::Error)]
pub enum KokoroError {
    /// A voice-pack blob had an invalid length / shape.
    #[error("invalid voice pack: {0}")]
    VoicePack(String),
    /// ONNX model load or inference failure (only under the `onnx` feature).
    #[error("kokoro model: {0}")]
    Model(String),
}
