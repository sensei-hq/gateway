//! Turn a Kokoro IPA phoneme string into the model's padded `input_ids`.
//!
//! Language-agnostic: it consumes the shared [`vocab`](crate::vocab), so it is
//! identical for every language — only the upstream [`g2p`](crate::g2p) differs.

use crate::vocab::{PAD_ID, token_id};

/// Max ids the model accepts in one call, including the leading + trailing pad.
pub const MAX_INPUT_IDS: usize = 512;

/// Max phoneme tokens per call — the input budget minus the two pad tokens.
pub const MAX_PHONEMES: usize = MAX_INPUT_IDS - 2;

/// The result of tokenizing a phoneme string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tokenized {
    /// `[PAD_ID, id…, PAD_ID]` — ready for the model's `input_ids` input.
    pub input_ids: Vec<i64>,
    /// Number of phoneme tokens (excludes the two pads). This is the index used
    /// to pick the voice's style vector — see [`Voices::style_for`](crate::voices::Voices::style_for).
    pub phoneme_count: usize,
    /// Phoneme chars dropped because they are not in the [`vocab`](crate::vocab).
    pub skipped: usize,
    /// True when the input exceeded [`MAX_PHONEMES`] and was truncated.
    pub truncated: bool,
}

/// Map a Kokoro phoneme string to padded `input_ids`.
///
/// Each recognized phoneme char becomes its vocab id; chars absent from the
/// vocab are skipped (counted in [`Tokenized::skipped`]). The phoneme run is
/// capped at [`MAX_PHONEMES`] and wrapped with [`PAD_ID`] on both ends.
pub fn tokenize(phonemes: &str) -> Tokenized {
    let mut input_ids = Vec::with_capacity(phonemes.len().min(MAX_PHONEMES) + 2);
    input_ids.push(PAD_ID);

    let mut phoneme_count = 0;
    let mut skipped = 0;
    let mut truncated = false;

    for c in phonemes.chars() {
        match token_id(c) {
            Some(id) => {
                if phoneme_count == MAX_PHONEMES {
                    truncated = true;
                    break;
                }
                input_ids.push(id);
                phoneme_count += 1;
            }
            None => skipped += 1,
        }
    }

    input_ids.push(PAD_ID);
    Tokenized {
        input_ids,
        phoneme_count,
        skipped,
        truncated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_is_two_pads() {
        let t = tokenize("");
        assert_eq!(t.input_ids, vec![PAD_ID, PAD_ID]);
        assert_eq!(t.phoneme_count, 0);
        assert_eq!(t.skipped, 0);
        assert!(!t.truncated);
    }

    #[test]
    fn maps_and_pads_known_phonemes() {
        // "ba" → b(44) a(43), wrapped in pads.
        let t = tokenize("ba");
        assert_eq!(t.input_ids, vec![PAD_ID, 44, 43, PAD_ID]);
        assert_eq!(t.phoneme_count, 2);
        assert_eq!(t.skipped, 0);
    }

    #[test]
    fn drops_unknown_chars_but_keeps_known() {
        // 'g' (ASCII) and '日' are not in the vocab; 'a' is.
        let t = tokenize("ga日");
        assert_eq!(t.input_ids, vec![PAD_ID, 43, PAD_ID]);
        assert_eq!(t.phoneme_count, 1);
        assert_eq!(t.skipped, 2);
    }

    #[test]
    fn truncates_beyond_the_phoneme_budget() {
        let long = "a".repeat(MAX_PHONEMES + 5);
        let t = tokenize(&long);
        assert!(t.truncated);
        assert_eq!(t.phoneme_count, MAX_PHONEMES);
        assert_eq!(t.input_ids.len(), MAX_INPUT_IDS);
        assert_eq!(t.input_ids.first(), Some(&PAD_ID));
        assert_eq!(t.input_ids.last(), Some(&PAD_ID));
    }
}
