//! The Kokoro phoneme vocabulary — a single IPA symbol → token-id table shared
//! by **every** language (it is baked into the model's embedding layer).
//!
//! Transcribed verbatim from the model's `config.json` (`hexgrad/Kokoro-82M`,
//! `n_token = 178`). Ids are sparse — some values in `0..178` are unused. Id `0`
//! is the pad / boundary token (`$`), handled by the [`tokenizer`](crate::tokenizer).
//!
//! Note there is **no ASCII `g`** (id 49 is unused); the velar plosive is the
//! IPA script-g `\u{0261}` (ɡ) at 92. Non-ASCII entries use `\u{…}` escapes with
//! the glyph in a trailing comment so the table is unambiguous to review.

use std::collections::HashMap;
use std::sync::LazyLock;

/// Pad / boundary token id (`$`). Prepended and appended to every phoneme
/// sequence before inference.
pub const PAD_ID: i64 = 0;

/// The model's `n_token` — the id space size (`0..N_TOKEN`), sparsely populated.
pub const N_TOKEN: usize = 178;

/// Kokoro phoneme → token-id pairs, verbatim from the model config.
const VOCAB: &[(char, i64)] = &[
    (';', 1),
    (':', 2),
    (',', 3),
    ('.', 4),
    ('!', 5),
    ('?', 6),
    ('\u{2014}', 9),  // —
    ('\u{2026}', 10), // …
    ('"', 11),
    ('(', 12),
    (')', 13),
    ('\u{201C}', 14), // “
    ('\u{201D}', 15), // ”
    (' ', 16),
    ('\u{0303}', 17), // ◌̃  combining tilde
    ('\u{02A3}', 18), // ʣ
    ('\u{02A5}', 19), // ʥ
    ('\u{02A6}', 20), // ʦ
    ('\u{02A8}', 21), // ʨ
    ('\u{1D5D}', 22), // ᵝ
    ('\u{AB67}', 23), // ꭧ
    ('A', 24),
    ('I', 25),
    ('O', 31),
    ('Q', 33),
    ('S', 35),
    ('T', 36),
    ('W', 39),
    ('Y', 41),
    ('\u{1D4A}', 42), // ᵊ
    ('a', 43),
    ('b', 44),
    ('c', 45),
    ('d', 46),
    ('e', 47),
    ('f', 48),
    ('h', 50),
    ('i', 51),
    ('j', 52),
    ('k', 53),
    ('l', 54),
    ('m', 55),
    ('n', 56),
    ('o', 57),
    ('p', 58),
    ('q', 59),
    ('r', 60),
    ('s', 61),
    ('t', 62),
    ('u', 63),
    ('v', 64),
    ('w', 65),
    ('x', 66),
    ('y', 67),
    ('z', 68),
    ('\u{0251}', 69),  // ɑ
    ('\u{0250}', 70),  // ɐ
    ('\u{0252}', 71),  // ɒ
    ('\u{00E6}', 72),  // æ
    ('\u{03B2}', 75),  // β
    ('\u{0254}', 76),  // ɔ
    ('\u{0255}', 77),  // ɕ
    ('\u{00E7}', 78),  // ç
    ('\u{0256}', 80),  // ɖ
    ('\u{00F0}', 81),  // ð
    ('\u{02A4}', 82),  // ʤ
    ('\u{0259}', 83),  // ə
    ('\u{025A}', 85),  // ɚ
    ('\u{025B}', 86),  // ɛ
    ('\u{025C}', 87),  // ɜ
    ('\u{025F}', 90),  // ɟ
    ('\u{0261}', 92),  // ɡ (script g — not ASCII 'g')
    ('\u{0265}', 99),  // ɥ
    ('\u{0268}', 101), // ɨ
    ('\u{026A}', 102), // ɪ
    ('\u{029D}', 103), // ʝ
    ('\u{026F}', 110), // ɯ
    ('\u{0270}', 111), // ɰ
    ('\u{014B}', 112), // ŋ
    ('\u{0273}', 113), // ɳ
    ('\u{0272}', 114), // ɲ
    ('\u{0274}', 115), // ɴ
    ('\u{00F8}', 116), // ø
    ('\u{0278}', 118), // ɸ
    ('\u{03B8}', 119), // θ
    ('\u{0153}', 120), // œ
    ('\u{0279}', 123), // ɹ
    ('\u{027E}', 125), // ɾ
    ('\u{027B}', 126), // ɻ
    ('\u{0281}', 128), // ʁ
    ('\u{027D}', 129), // ɽ
    ('\u{0282}', 130), // ʂ
    ('\u{0283}', 131), // ʃ
    ('\u{0288}', 132), // ʈ
    ('\u{02A7}', 133), // ʧ
    ('\u{028A}', 135), // ʊ
    ('\u{028B}', 136), // ʋ
    ('\u{028C}', 138), // ʌ
    ('\u{0263}', 139), // ɣ
    ('\u{0264}', 140), // ɤ
    ('\u{03C7}', 142), // χ
    ('\u{028E}', 143), // ʎ
    ('\u{0292}', 147), // ʒ
    ('\u{0294}', 148), // ʔ
    ('\u{02C8}', 156), // ˈ  primary stress
    ('\u{02CC}', 157), // ˌ  secondary stress
    ('\u{02D0}', 158), // ː  length mark
    ('\u{02B0}', 162), // ʰ
    ('\u{02B2}', 164), // ʲ
    ('\u{2193}', 169), // ↓
    ('\u{2192}', 171), // →
    ('\u{2197}', 172), // ↗
    ('\u{2198}', 173), // ↘
    ('\u{1D7B}', 177), // ᵻ
];

static LOOKUP: LazyLock<HashMap<char, i64>> = LazyLock::new(|| VOCAB.iter().copied().collect());

/// The token id for a phoneme char, or `None` when it is not in the vocab.
pub fn token_id(phoneme: char) -> Option<i64> {
    LOOKUP.get(&phoneme).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_symbols_map_to_expected_ids() {
        assert_eq!(token_id(' '), Some(16));
        assert_eq!(token_id('a'), Some(43));
        assert_eq!(token_id('\u{0251}'), Some(69)); // ɑ
        assert_eq!(token_id('\u{02C8}'), Some(156)); // ˈ primary stress
        assert_eq!(token_id('\u{0261}'), Some(92)); // ɡ script-g
    }

    #[test]
    fn ascii_g_is_not_in_the_vocab() {
        // The velar plosive is the IPA script-g (92), never ASCII 'g'.
        assert_eq!(token_id('g'), None);
    }

    #[test]
    fn unknown_symbol_is_none() {
        assert_eq!(token_id('%'), None);
        assert_eq!(token_id('日'), None);
    }

    #[test]
    fn table_is_unique_and_in_range() {
        let mut seen_chars = std::collections::HashSet::new();
        let mut seen_ids = std::collections::HashSet::new();
        for &(c, id) in VOCAB {
            assert!(seen_chars.insert(c), "duplicate char {c:?}");
            assert!(seen_ids.insert(id), "duplicate id {id}");
            assert!(id > 0 && (id as usize) < N_TOKEN, "id {id} out of range");
        }
    }
}
