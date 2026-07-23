//! English grapheme-to-phoneme: a lexicon-driven [`G2p`] implementation.
//!
//! The *logic* (tokenize → lexicon lookup → punctuation mapping → join) lives
//! here; the pronunciation *data* is **injected** as a [`Lexicon`], so the crate
//! stays lean and the pipeline is testable without bundling megabytes. The
//! production lexicon is misaki's Apache-2.0 English dictionaries
//! (`us_gold` / `gb_gold`), loaded via [`Lexicon::from_misaki_json`] and
//! provisioned alongside the model.
//!
//! Deliberately *not* handled yet (follow-ups): number / currency expansion,
//! homograph POS disambiguation (the loader takes each word's default
//! pronunciation), and an out-of-vocabulary letter-to-sound fallback — OOV words
//! are currently dropped.

use std::collections::HashMap;

use crate::g2p::G2p;
use crate::lang::Lang;

/// A word → IPA-phoneme pronunciation dictionary (keys lowercased).
#[derive(Debug, Clone, Default)]
pub struct Lexicon {
    entries: HashMap<String, String>,
}

impl Lexicon {
    /// Build a lexicon from `(word, phonemes)` pairs; words are lowercased.
    pub fn from_entries<I, W, P>(entries: I) -> Self
    where
        I: IntoIterator<Item = (W, P)>,
        W: Into<String>,
        P: Into<String>,
    {
        Self {
            entries: entries
                .into_iter()
                .map(|(w, p)| (w.into().to_lowercase(), p.into()))
                .collect(),
        }
    }

    /// Parse a misaki-format English dictionary: a JSON object whose values are
    /// either a phoneme string or a `{ POS: phonemes }` object (the `"DEFAULT"`
    /// entry, else any entry, is taken). Words are lowercased.
    pub fn from_misaki_json(json: &str) -> Result<Self, serde_json::Error> {
        let raw: HashMap<String, serde_json::Value> = serde_json::from_str(json)?;
        let entries = raw
            .into_iter()
            .filter_map(|(word, value)| {
                default_pronunciation(&value).map(|ph| (word.to_lowercase(), ph))
            })
            .collect();
        Ok(Self { entries })
    }

    /// The phoneme string for a word (case-insensitive), if present.
    pub fn get(&self, word: &str) -> Option<&str> {
        self.entries.get(&word.to_lowercase()).map(String::as_str)
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the lexicon is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// A word's default pronunciation from a misaki JSON value: a bare string, or
/// the `"DEFAULT"` (else first) value of a `{ POS: phonemes }` object.
fn default_pronunciation(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Object(map) => map
            .get("DEFAULT")
            .or_else(|| map.values().next())
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        _ => None,
    }
}

/// English G2P over an injected [`Lexicon`]. Out-of-vocabulary words are dropped
/// (a letter-to-sound fallback is a follow-up).
#[derive(Debug, Clone)]
pub struct EnglishG2p {
    lang: Lang,
    lexicon: Lexicon,
}

impl EnglishG2p {
    /// American-English G2P (`us_*` lexicon).
    pub fn american(lexicon: Lexicon) -> Self {
        Self {
            lang: Lang::AmericanEnglish,
            lexicon,
        }
    }

    /// British-English G2P (`gb_*` lexicon).
    pub fn british(lexicon: Lexicon) -> Self {
        Self {
            lang: Lang::BritishEnglish,
            lexicon,
        }
    }

    /// The backing lexicon.
    pub fn lexicon(&self) -> &Lexicon {
        &self.lexicon
    }
}

impl G2p for EnglishG2p {
    fn lang(&self) -> Lang {
        self.lang
    }

    fn phonemize(&self, text: &str) -> String {
        let mut out = String::new();
        let mut word = String::new();
        // A space is owed before the next word once we've emitted a word or a
        // punctuation mark, so words are space-separated but punctuation attaches
        // to the preceding token.
        let mut pending_space = false;

        for c in text.chars() {
            if is_word_char(c) {
                word.push(c);
                continue;
            }
            flush_word(&self.lexicon, &mut word, &mut out, &mut pending_space);
            if c.is_whitespace() {
                pending_space = true;
            } else if let Some(p) = punctuation(c) {
                out.push(p);
                pending_space = true;
            }
            // Any other char is dropped.
        }
        flush_word(&self.lexicon, &mut word, &mut out, &mut pending_space);
        out
    }
}

/// Look a completed word up in the lexicon and append its phonemes (space-
/// separated from prior output). Unknown words are dropped. Clears `word`.
fn flush_word(lex: &Lexicon, word: &mut String, out: &mut String, pending_space: &mut bool) {
    if word.is_empty() {
        return;
    }
    if let Some(phonemes) = lex.get(word) {
        if *pending_space && !out.is_empty() {
            out.push(' ');
        }
        out.push_str(phonemes);
        *pending_space = false;
    }
    word.clear();
}

/// Whether `c` is part of a word — letters plus intra-word apostrophes (`don't`).
fn is_word_char(c: char) -> bool {
    c.is_alphabetic() || c == '\'' || c == '\u{2019}'
}

/// Map an input punctuation char to its Kokoro vocab phoneme, if any.
fn punctuation(c: char) -> Option<char> {
    match c {
        '.' | ',' | '!' | '?' | ';' | ':' | '(' | ')' | '"' => Some(c),
        '\u{2014}' => Some('\u{2014}'), // —
        '\u{2026}' => Some('\u{2026}'), // …
        '\u{201C}' => Some('\u{201C}'), // “
        '\u{201D}' => Some('\u{201D}'), // ”
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Placeholder phonemes built from valid vocab chars — these tests exercise
    // the pipeline mechanics, not IPA correctness (that rides on the real dict).
    fn lex() -> Lexicon {
        Lexicon::from_entries([
            ("hello", "h\u{25B}l\u{2C8}o\u{28A}"),
            ("world", "w\u{25C}\u{279}ld"),
        ])
    }

    #[test]
    fn phonemizes_words_with_spaces_and_punctuation() {
        let g = EnglishG2p::american(lex());
        assert_eq!(
            g.phonemize("Hello, world!"),
            "h\u{25B}l\u{2C8}o\u{28A}, w\u{25C}\u{279}ld!"
        );
    }

    #[test]
    fn lookup_is_case_insensitive() {
        let g = EnglishG2p::american(lex());
        assert_eq!(g.phonemize("HELLO"), "h\u{25B}l\u{2C8}o\u{28A}");
    }

    #[test]
    fn out_of_vocabulary_words_are_dropped_without_stray_spaces() {
        let g = EnglishG2p::american(lex());
        // "foo" is unknown; "world" is known — no leading space, no gap.
        assert_eq!(g.phonemize("foo world"), "w\u{25C}\u{279}ld");
    }

    #[test]
    fn lang_reflects_the_constructor() {
        assert_eq!(
            EnglishG2p::american(Lexicon::default()).lang(),
            Lang::AmericanEnglish
        );
        assert_eq!(
            EnglishG2p::british(Lexicon::default()).lang(),
            Lang::BritishEnglish
        );
    }

    #[test]
    fn from_misaki_json_takes_string_or_default_pos() {
        let json = r#"{"cat":"kˈæt","read":{"DEFAULT":"ɹˈɛd","VERB":"ɹˈid"}}"#;
        let lex = Lexicon::from_misaki_json(json).unwrap();
        assert_eq!(lex.get("cat"), Some("k\u{2c8}\u{e6}t"));
        assert_eq!(lex.get("read"), Some("\u{279}\u{2c8}\u{25b}d")); // DEFAULT chosen
        assert_eq!(lex.len(), 2);
    }
}
