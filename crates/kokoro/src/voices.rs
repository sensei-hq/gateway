//! Voice packs: the per-voice style vectors (`ref_s`) the model conditions on.
//!
//! A pack is a little-endian `f32` blob of shape `(rows, 1, 256)`; Kokoro picks
//! the row by phoneme count (`ref_s = voices[len(phonemes)]`). Parsing/selection
//! is language-agnostic — a voice's language is inferred from its id
//! ([`Lang::from_voice_id`](crate::lang::Lang::from_voice_id)), not the bytes.

use crate::error::KokoroError;

/// A Kokoro voice pack: `rows` style vectors of dimension [`Voices::DIM`],
/// indexed by phoneme count.
#[derive(Debug, Clone)]
pub struct Voices {
    data: Vec<f32>,
    rows: usize,
}

impl Voices {
    /// Style-vector dimension — the model's `style` input is `(1, 256)`.
    pub const DIM: usize = 256;

    /// Parse a voice pack from a little-endian `f32` blob of shape
    /// `(rows, 1, 256)`.
    ///
    /// Errors with [`KokoroError::VoicePack`] if the blob is empty or its length
    /// is not a multiple of `256 × 4` bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, KokoroError> {
        const ROW_BYTES: usize = Voices::DIM * 4;
        if bytes.is_empty() || !bytes.len().is_multiple_of(ROW_BYTES) {
            return Err(KokoroError::VoicePack(format!(
                "{} bytes is not a non-zero multiple of {ROW_BYTES} (rows × 256 × f32)",
                bytes.len()
            )));
        }
        let data = bytes
            .chunks_exact(4)
            .map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
            .collect::<Vec<f32>>();
        let rows = data.len() / Self::DIM;
        Ok(Self { data, rows })
    }

    /// Number of style vectors in the pack.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// The `(256,)` style vector for a given phoneme count.
    ///
    /// Kokoro selects `ref_s = voices[len(phonemes)]`; the index is clamped to
    /// the pack's last row, so an out-of-range count still yields a valid vector.
    pub fn style_for(&self, phoneme_count: usize) -> &[f32] {
        let idx = phoneme_count.min(self.rows - 1);
        let start = idx * Self::DIM;
        &self.data[start..start + Self::DIM]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a `rows`-row pack where row `i` is filled with the value `i as f32`.
    fn pack(rows: usize) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(rows * Voices::DIM * 4);
        for i in 0..rows {
            for _ in 0..Voices::DIM {
                bytes.extend_from_slice(&(i as f32).to_le_bytes());
            }
        }
        bytes
    }

    #[test]
    fn parses_rows_and_selects_by_count() {
        let v = Voices::from_bytes(&pack(3)).unwrap();
        assert_eq!(v.rows(), 3);
        assert_eq!(v.style_for(0), [0.0; Voices::DIM]);
        assert_eq!(v.style_for(2), [2.0; Voices::DIM]);
    }

    #[test]
    fn out_of_range_count_clamps_to_last_row() {
        let v = Voices::from_bytes(&pack(3)).unwrap();
        assert_eq!(v.style_for(99), [2.0; Voices::DIM]);
    }

    #[test]
    fn rejects_empty_or_misshaped_blobs() {
        assert!(Voices::from_bytes(&[]).is_err());
        assert!(Voices::from_bytes(&[0u8; 100]).is_err()); // not a multiple of 1024
    }
}
