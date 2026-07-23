//! Languages Kokoro supports.
//!
//! The model shares one IPA phoneme [`vocab`](crate::vocab) across every
//! language — what differs per language is the grapheme-to-phoneme frontend
//! ([`crate::g2p`]) and the voice packs. `Lang` is the axis those two vary on.

/// A spoken language Kokoro supports, identified by the one-letter prefix of its
/// voice ids (e.g. `af_bella` → [`Lang::AmericanEnglish`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lang {
    /// American English (voice prefixes `af_` / `am_`).
    AmericanEnglish,
    /// British English (`bf_` / `bm_`).
    BritishEnglish,
    /// Spanish (`ef_` / `em_`).
    Spanish,
    /// French (`ff_` / `fm_`).
    French,
    /// Hindi (`hf_` / `hm_`).
    Hindi,
    /// Italian (`if_` / `im_`).
    Italian,
    /// Brazilian Portuguese (`pf_` / `pm_`).
    Portuguese,
    /// Japanese (`jf_` / `jm_`).
    Japanese,
    /// Mandarin Chinese (`zf_` / `zm_`).
    Chinese,
}

impl Lang {
    /// Infer the language from a Kokoro voice id via its one-letter prefix
    /// (`"af_bella"` → American English). `None` for an unknown prefix.
    pub fn from_voice_id(voice: &str) -> Option<Self> {
        match voice.bytes().next()? {
            b'a' => Some(Self::AmericanEnglish),
            b'b' => Some(Self::BritishEnglish),
            b'e' => Some(Self::Spanish),
            b'f' => Some(Self::French),
            b'h' => Some(Self::Hindi),
            b'i' => Some(Self::Italian),
            b'p' => Some(Self::Portuguese),
            b'j' => Some(Self::Japanese),
            b'z' => Some(Self::Chinese),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infers_language_from_voice_prefix() {
        assert_eq!(Lang::from_voice_id("af_bella"), Some(Lang::AmericanEnglish));
        assert_eq!(Lang::from_voice_id("am_adam"), Some(Lang::AmericanEnglish));
        assert_eq!(Lang::from_voice_id("bm_george"), Some(Lang::BritishEnglish));
        assert_eq!(Lang::from_voice_id("jf_alpha"), Some(Lang::Japanese));
        assert_eq!(Lang::from_voice_id("zf_xiaobei"), Some(Lang::Chinese));
    }

    #[test]
    fn unknown_prefix_and_empty_are_none() {
        assert_eq!(Lang::from_voice_id("qx_nope"), None);
        assert_eq!(Lang::from_voice_id(""), None);
    }
}
