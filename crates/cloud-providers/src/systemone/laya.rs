//! Laya's per-question head budget (SP-DEC-3 D6): Laya's `build_head`
//! silently cuts each question's head to the checkpoint's `head_max_len` and
//! reports nothing, so the generic adapter estimates the head and refuses an
//! over-budget question before sending.

use kernel::types::decision::{DecisionContent, DecisionQuestion, DecisionQuestions};
use kernel::types::error::GatewayError;

/// Laya's per-question head budget for a checkpoint (SP-DEC-3 D6): its
/// `head_max_len` from `rl_agent_config.json` — `english` 192, `multilingual` /
/// `typed-decisions` 256. Any other name (an operator-registered checkpoint, a
/// typo) gets the smallest, never room it may not have.
fn laya_head_budget(model: &str) -> usize {
    match laya_canonical_model(model).as_deref() {
        Some("multilingual" | "typed-decisions") => 256,
        // Unknown names, and the bundle id (Laya picks): the smallest budget.
        _ => 192,
    }
}

/// The checkpoint Laya serves for a requested `model`, as Laya 0.5.1 resolves
/// it — `None` for the bundle id, which asks Laya to choose. Copied from the
/// installed package:
/// - `laya_coreml/serve.py` `_resolve_model`: a published Hub id (inverted
///   `router.py` `STANDALONE_MODELS`, minus the bundle) pins its checkpoint,
///   matched trimmed and lowercased; `convaiinnovations/laya` (`BUNDLE_REPO`)
///   means auto-route;
/// - `laya_coreml/router.py` `canonical_name`: trim, lowercase, then `_ALIASES`.
///
/// Any other name passes through canonicalised (trimmed, lowercased): a typo
/// stays a typo, so D7 still refuses what Laya auto-routed.
pub(super) fn laya_canonical_model(model: &str) -> Option<String> {
    let key = model.trim().to_lowercase();
    let canonical = match key.as_str() {
        // serve.py: `STANDALONE_MODELS` inverted, the bundle excluded.
        "convaiinnovations/laya-multilingual" => "multilingual",
        "convaiinnovations/laya-typed-decisions" => "typed-decisions",
        // serve.py: the bundle id is "let the Router choose".
        "convaiinnovations/laya" => return None,
        // router.py `_ALIASES`.
        "en" | "laya" | "default" => "english",
        "multi" | "ml" | "laya-multilingual" => "multilingual",
        "typed" | "typed_decisions" | "laya-typed-decisions" | "decisions" => "typed-decisions",
        _ => return Some(key),
    };
    Some(canonical.to_string())
}

/// The whole estimated head of [`estimated_head_parts`] (tests).
#[cfg(test)]
fn estimated_head_tokens(q: &DecisionQuestion) -> usize {
    let (instruction, options) = estimated_head_parts(q);
    instruction + options
}

/// One rendered option: its `[MASK]` plus `" " + text` — Laya encodes the
/// leading space, which the multilingual tokenizer makes a token of its own
/// before a digit, symbol or CJK char — capped at Laya's 48, plus
/// [`OPTION_MARGIN`].
fn laya_option_tokens(text: &str) -> usize {
    1 + laya_text_tokens(&format!(" {text}")).min(48) + OPTION_MARGIN
}

/// Safety margin on every option. [`laya_text_tokens`] can be under on a
/// short random letter string, and that happens once PER OPTION, so it adds
/// up: 32 random 5–8 letter keys each one under on `english` sum to 32, enough
/// for Laya to cut every option while the estimate says they fit. Measured by
/// `tests/fixtures/laya_head_tokens.py stress-options` over 900 000 random
/// 1–12 char keys (lowercase, alphanumeric, printable; seeds 1, 7, 42): with a
/// margin of 1, 15 options were still 1 under (worst −2 before any margin);
/// with 2, none.
const OPTION_MARGIN: usize = 2;

/// Safety margin on the instruction head, encoded once per question: the
/// per-string under-count is at most 1 there (6 of 21 000 short random strings
/// by 1, `stress 99`), so one token covers it.
const INSTRUCTION_MARGIN: usize = 1;

/// A conservative estimate of the tokens Laya's `build_head` spends on one
/// question (SP-DEC-3 D6), as `(instruction part, all options)`. Laya renders
/// `"<type> question: <instructions>"` and one `[MASK]`-prefixed span per
/// option (capped at 48 tokens each), then cuts the instruction SILENTLY so the
/// whole head fits `head_max_len` — `truncated` never reports it.
///
/// It counts the same text Laya renders (`render_options` in
/// `laya_coreml/common.py`) with [`laya_text_tokens`]: a choice option is
/// `"key: description"` or the bare key; a score level is
/// `"level i: description"`; a noul side is `"false: …"` / `"true: …"` with
/// Laya's default text when undescribed. Each option is counted as Laya
/// encodes it, `" " + text` (see [`laya_option_tokens`]). Non-string
/// instructions are Laya's `json.dumps` text, separators included.
///
/// The estimate errs high; it is not a proof. Measured 2026-10-20 by
/// `tests/fixtures/laya_head_tokens.py` against both real tokenizers
/// (`english`, which typed-decisions shares, and `multilingual`, which -ane
/// shares) on 30 head samples and 52 option samples — English, JSON (compact,
/// Python, pretty), Japanese, Chinese, Korean, Cyrillic, Arabic, Hindi, Thai,
/// German, emoji, astral CJK, code, URLs, numbers, dates, money, hex, base64,
/// gibberish, mixed case, ALLCAPS, camelCase, symbols, whitespace, long words,
/// digit and CJK option keys: the text count alone is never below the real
/// count, tightest at +0 (digit-only options on multilingual, astral CJK).
/// Known text under-count: 6 of 21 000 short random strings by 1 token
/// (`stress 99`), and short random option keys by up to 2 — once per option,
/// which adds up. [`INSTRUCTION_MARGIN`] and [`OPTION_MARGIN`] cover those:
/// with them no option of 900 000 random keys (`stress-options`) and no whole
/// head has been observed under. The price is over-counting: about 1.7× on
/// English (the 288-char D6 instruction: 185 estimated, 109 real on `english`;
/// the 548-char one that really fits, 305 vs 189, refused) and 2–7× on non-Latin
/// scripts, whose every char is counted at its UTF-8 bytes. Refusing a head
/// that would fit costs a fallback; sending one that does not returns a
/// confident answer to a question the model never read.
fn estimated_head_parts(q: &DecisionQuestion) -> (usize, usize) {
    let tokens = laya_text_tokens;
    let option = laya_option_tokens;
    let (kind, instructions) = match q {
        DecisionQuestion::Choice { instructions, .. } => ("choice", instructions),
        DecisionQuestion::Noul { instructions, .. } => ("noul", instructions),
        DecisionQuestion::Score { instructions, .. } => ("score", instructions),
    };
    let head = tokens(&format!(
        "{kind} question: {}",
        laya_instruction_text(instructions)
    )) + INSTRUCTION_MARGIN;
    let options: usize = match q {
        DecisionQuestion::Choice { criteria, .. } => criteria
            .iter()
            .map(|(key, desc)| match desc.as_deref() {
                None | Some("") => option(key),
                Some(desc) => option(&format!("{key}: {desc}")),
            })
            .sum(),
        DecisionQuestion::Score { criteria, .. } => criteria
            .iter()
            .enumerate()
            .map(|(i, desc)| option(&format!("level {i}: {desc}")))
            .sum(),
        DecisionQuestion::Noul { criteria, .. } => {
            let side = |label: &str, desc: Option<&str>, default: &str| {
                option(&format!(
                    "{label}: {}",
                    desc.filter(|d| !d.is_empty()).unwrap_or(default)
                ))
            };
            let c = criteria.as_ref();
            side(
                "false",
                c.and_then(|c| c.r#false.as_deref()),
                "no, the statement does not hold",
            ) + side(
                "true",
                c.and_then(|c| c.r#true.as_deref()),
                "yes, the statement holds",
            )
        }
    };
    (head, options)
}

/// An upper estimate of the tokens one piece of Laya head text costs, on
/// either shipped tokenizer (SP-DEC-3 D6). Each rule is a bound or errs high:
/// - a non-ASCII char costs its UTF-8 byte count — both tokenizers fall back
///   to single bytes for a char they do not know (measured: `𠜎` is 4 tokens on
///   multilingual), so no char can cost more;
/// - an ASCII digit, punctuation, symbol or non-space whitespace char costs 1
///   (multilingual splits digits one per token and gives each extra space its
///   own token);
/// - a space costs nothing before an ASCII letter (it merges into the word),
///   else 1;
/// - an ASCII word costs [`laya_word_tokens`].
fn laya_text_tokens(text: &str) -> usize {
    let chars: Vec<char> = text.chars().collect();
    let (mut n, mut i) = (0, 0);
    while i < chars.len() {
        let c = chars[i];
        if !c.is_ascii() {
            n += c.len_utf8();
            i += 1;
        } else if c.is_ascii_alphabetic() {
            let start = i;
            while i < chars.len() && chars[i].is_ascii_alphabetic() {
                i += 1;
            }
            n += laya_word_tokens(&chars[start..i]);
        } else {
            let merges = c == ' ' && chars.get(i + 1).is_some_and(char::is_ascii_alphabetic);
            n += usize::from(!merges);
            i += 1;
        }
    }
    n
}

/// One run of ASCII letters. A word with an uppercase letter after its first
/// (camelCase, ALLCAPS, base64) costs one token per letter — those split
/// almost letter by letter. Any other word costs `max(ceil(len / 2), vowel
/// groups, min(len, 2))` plus one per three consecutive vowels or consonants
/// and one per `j q x z v k`: letter strings that are not words (ids, hashes,
/// typos) split at rare letters and long consonant runs, and English words
/// rarely have either.
fn laya_word_tokens(word: &[char]) -> usize {
    if word.iter().skip(1).any(char::is_ascii_uppercase) {
        return word.len();
    }
    let (mut groups, mut runs, mut run, mut prev) = (0, 0, 0, None);
    for c in word.iter().map(char::to_ascii_lowercase) {
        let vowel = matches!(c, 'a' | 'e' | 'i' | 'o' | 'u' | 'y');
        if vowel && prev != Some(true) {
            groups += 1;
        }
        if prev != Some(vowel) {
            runs += run / 3;
            run = 0;
        }
        run += 1;
        prev = Some(vowel);
    }
    runs += run / 3;
    let rare = word
        .iter()
        .filter(|c| matches!(c.to_ascii_lowercase(), 'j' | 'q' | 'x' | 'z' | 'v' | 'k'))
        .count();
    word.len().div_ceil(2).max(groups).max(word.len().min(2)) + runs + rare
}

/// Instructions as Laya reads them: a string as-is, anything else as Python's
/// `json.dumps(…, ensure_ascii=False)` — whose `", "` / `": "` separators are
/// longer than serde_json's compact form, so they are reproduced here.
fn laya_instruction_text(instructions: &DecisionContent) -> String {
    struct PythonJson;
    impl serde_json::ser::Formatter for PythonJson {
        fn begin_array_value<W: ?Sized + std::io::Write>(
            &mut self,
            w: &mut W,
            first: bool,
        ) -> std::io::Result<()> {
            if first { Ok(()) } else { w.write_all(b", ") }
        }
        fn begin_object_key<W: ?Sized + std::io::Write>(
            &mut self,
            w: &mut W,
            first: bool,
        ) -> std::io::Result<()> {
            if first { Ok(()) } else { w.write_all(b", ") }
        }
        fn begin_object_value<W: ?Sized + std::io::Write>(
            &mut self,
            w: &mut W,
        ) -> std::io::Result<()> {
            w.write_all(b": ")
        }
    }
    match instructions {
        serde_json::Value::String(s) => s.clone(),
        other => {
            let mut out = Vec::new();
            let mut ser = serde_json::Serializer::with_formatter(&mut out, PythonJson);
            match serde::Serialize::serialize(other, &mut ser) {
                Ok(()) => String::from_utf8(out).unwrap_or_else(|_| other.to_string()),
                Err(_) => other.to_string(),
            }
        }
    }
}

/// The first question whose estimated head exceeds Laya's budget for
/// `model`, refused before anything is sent (SP-DEC-3 D6).
pub(super) fn check_laya_head_budget(
    adapter: &str,
    model: &str,
    questions: &DecisionQuestions,
) -> Result<(), GatewayError> {
    let budget = laya_head_budget(model);
    // "'ml' (multilingual)": the name as configured and, when Laya resolves it
    // to another, the checkpoint it will serve.
    let checkpoint = match laya_canonical_model(model) {
        Some(canonical) if canonical != model => format!("'{model}' ({canonical})"),
        _ => format!("'{model}'"),
    };
    // Laya's `build_head` cuts EVERY option to `max(4, (head_max_len − 16) / n)`
    // tokens once `head_max_len − Σ options < 16` — options that shared a prefix
    // then collapse into the same tokens and cannot be told apart.
    let option_room = budget.saturating_sub(16);
    for (name, q) in questions {
        let (instruction, options) = estimated_head_parts(q);
        if options > option_room {
            return Err(GatewayError::InputRejected {
                adapter: adapter.into(),
                message: format!(
                    "the options of question '{name}' need ≈{options} tokens but checkpoint \
                     {checkpoint} leaves options {option_room} of its {budget} — the options \
                     would be cut (Laya trims every option to fit, until some cannot be told \
                     apart); use fewer or shorter options"
                ),
            });
        }
        let estimate = instruction + options;
        if estimate > budget {
            return Err(GatewayError::InputRejected {
                adapter: adapter.into(),
                message: format!(
                    "question '{name}' needs ≈{estimate} tokens of head but checkpoint \
                     {checkpoint} allows {budget} — Laya would silently cut it; shorten the \
                     instructions/options"
                ),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// SP-DEC-3 D6: each checkpoint's `head_max_len` (its `rl_agent_config.json`);
    /// an unknown name gets the smallest, so a misspelled or operator-registered
    /// checkpoint is never assumed to have room it may not have.
    #[test]
    fn laya_head_budget_is_the_checkpoints_head_max_len() {
        assert_eq!(laya_head_budget("english"), 192);
        assert_eq!(laya_head_budget("multilingual"), 256);
        assert_eq!(laya_head_budget("typed-decisions"), 256);
        for unknown in ["ane", "englsh", "", "convaiinnovations/laya"] {
            assert_eq!(laya_head_budget(unknown), 192, "{unknown:?}");
        }
        // Laya's own spellings of the same checkpoints (aliases, case, spaces,
        // published Hub ids) get the same budget.
        for name in [
            "ml",
            "multi",
            " Multilingual ",
            "MULTILINGUAL",
            "laya-multilingual",
            "convaiinnovations/laya-multilingual",
            "typed",
            "typed_decisions",
            "decisions",
            "convaiinnovations/laya-typed-decisions",
        ] {
            assert_eq!(laya_head_budget(name), 256, "{name:?}");
        }
    }

    /// Laya 0.5.1's name resolution, copied: `router.py` `canonical_name` (trim,
    /// lowercase, `_ALIASES`) and `serve.py` `_resolve_model` (published Hub
    /// ids pin; the bundle id `convaiinnovations/laya` means auto-route).
    #[test]
    fn laya_model_names_canonicalise_as_laya_resolves_them() {
        let c = laya_canonical_model;
        for (name, want) in [
            ("english", Some("english")),
            ("English", Some("english")),
            ("  EN ", Some("english")),
            ("laya", Some("english")),
            ("default", Some("english")),
            ("multi", Some("multilingual")),
            ("ml", Some("multilingual")),
            ("laya-multilingual", Some("multilingual")),
            ("typed", Some("typed-decisions")),
            ("typed_decisions", Some("typed-decisions")),
            ("laya-typed-decisions", Some("typed-decisions")),
            ("decisions", Some("typed-decisions")),
            ("convaiinnovations/laya-multilingual", Some("multilingual")),
            (
                "Convaiinnovations/Laya-Typed-Decisions",
                Some("typed-decisions"),
            ),
            ("convaiinnovations/laya", None),
            ("englsh", Some("englsh")),
            ("ane", Some("ane")),
        ] {
            assert_eq!(c(name).as_deref(), want, "{name:?}");
        }
    }

    /// The estimate counts the text Laya renders (`build_head` / `render_options`
    /// in laya_coreml/common.py) — each piece through `laya_text_tokens` — plus
    /// one `[MASK]` per option and Laya's 48-token option cap.
    #[test]
    fn estimated_head_tokens_counts_what_laya_renders() {
        use kernel::types::decision::{DecisionQuestion, NoulCriteria};
        let t = laya_text_tokens;
        let o = laya_option_tokens;
        let (no, yes) = (
            o("false: no, the statement does not hold"),
            o("true: yes, the statement holds"),
        );
        // Laya's default noul options when a side is undescribed.
        let noul = DecisionQuestion::Noul {
            instructions: serde_json::json!("abcdef"),
            criteria: None,
        };
        assert_eq!(
            estimated_head_tokens(&noul),
            t("noul question: abcdef") + 1 + no + yes
        );
        // A described side replaces the default; an empty description is no
        // description (Laya: `not in (None, "")`).
        let noul = DecisionQuestion::Noul {
            instructions: serde_json::json!("abcdef"),
            criteria: Some(NoulCriteria {
                r#false: Some(String::new()),
                r#true: Some("x".into()),
            }),
        };
        assert_eq!(
            estimated_head_tokens(&noul),
            t("noul question: abcdef") + 1 + no + o("true: x")
        );
        // "key: description", or the bare key.
        let choice = DecisionQuestion::Choice {
            instructions: serde_json::json!("Pick"),
            criteria: [
                ("a".to_string(), Some("bb".to_string())),
                ("c".to_string(), None),
            ]
            .into_iter()
            .collect(),
        };
        assert_eq!(
            estimated_head_tokens(&choice),
            t("choice question: Pick") + 1 + o("a: bb") + o("c")
        );
        // Non-string instructions are Laya's `json.dumps` text, WITH its default
        // ", " / ": " separators; score levels are "level i: description".
        let score = DecisionQuestion::Score {
            instructions: serde_json::json!({"k": 1, "j": [1, 2]}),
            criteria: vec!["lo".into(), "hi".into()],
        };
        let json_text = laya_instruction_text(&serde_json::json!({"k": 1, "j": [1, 2]}));
        assert!(
            json_text == r#"{"j": [1, 2], "k": 1}"# || json_text == r#"{"k": 1, "j": [1, 2]}"#,
            "{json_text}"
        );
        assert_eq!(
            estimated_head_tokens(&score),
            t(&format!("score question: {json_text}")) + 1 + o("level 0: lo") + o("level 1: hi")
        );
        // An option is capped at 48 tokens (+1 [MASK], +2 margin) however long.
        let long = DecisionQuestion::Choice {
            instructions: serde_json::json!("Pick"),
            criteria: [("k".to_string(), Some("d".repeat(300)))]
                .into_iter()
                .collect(),
        };
        assert_eq!(
            estimated_head_tokens(&long),
            t("choice question: Pick") + 1 + 51
        );
    }

    /// The counting rule itself, worked by hand.
    #[test]
    fn laya_text_tokens_follows_its_rules() {
        let t = laya_text_tokens;
        assert_eq!(t(""), 0);
        // noul: max(⌈4/2⌉, 1 vowel group, 2) = 2; the space before a letter is
        // free; question: max(4, 2, 2) + 1 (q) = 5; ':' 1; abcdef: max(3, 2, 2)
        // + 1 (the "bcd" consonant run) = 4.
        assert_eq!(t("noul question: abcdef"), 2 + 5 + 1 + 4);
        // A word with a later capital costs a token per letter; a digit is 1.
        assert_eq!(t("XK9"), 2 + 1);
        assert_eq!(t("getUserById"), 11);
        // Non-ASCII costs its UTF-8 bytes: 3 each for 日本, 4 for 𠜎.
        assert_eq!(t("日本"), 6);
        assert_eq!(t("𠜎"), 4);
        // A space not before a letter costs 1: 'a' 1, ' ' 1, ' '+'b' 0, 'b' 1.
        assert_eq!(t("a  b"), 3);
        // Rare letters and long runs: "zzkk" = max(2, 0, 2) + 1 (run of 4) + 4.
        assert_eq!(t("zzkk"), 2 + 1 + 4);
        // An odd-length word whose only floor is ⌈len/2⌉: "lemon" = ⌈5/2⌉ = 3
        // (2 vowel groups, no run of 3, no rare letter).
        assert_eq!(t("lemon"), 3);
    }

    /// SP-DEC-3 D6: the estimate must never be BELOW what Laya really spends —
    /// an under-count is a silent cut. Real counts measured 2026-10-20 with each
    /// checkpoint's own `tokenizer.json` (`english` = laya-coreml, also used by
    /// typed-decisions; `multilingual` = laya-multilingual-coreml, also -ane) by
    /// `tests/fixtures/laya_head_tokens.py rust head` (the `tokenizers` package,
    /// `add_special_tokens=False`, exactly as `build_head` encodes the head).
    #[test]
    fn the_text_estimate_is_at_least_the_real_count_on_both_tokenizers() {
        // (head text, real english tokens, real multilingual tokens)
        let cases: [(&str, usize, usize); _] = [
            (
                "noul question: Does the customer want a refund for the duplicate charge on their invoice?",
                18,
                18,
            ), // english
            (
                "noul question: {\"rubric\":\"refund\",\"weight\":3,\"tags\":[\"billing\",\"urgent\"],\"nested\":{\"a\":1,\"b\":[1,2,3]}}",
                39,
                36,
            ), // json_compact
            (
                "noul question: {\"rubric\": \"refund\", \"weight\": 3, \"tags\": [\"billing\", \"urgent\"], \"nested\": {\"a\": 1, \"b\": [1, 2, 3]}}",
                47,
                48,
            ), // json_python
            (
                "noul question: {\n  \"rubric\": \"refund\",\n  \"weight\": 3,\n  \"tags\": [\n    \"billing\",\n    \"urgent\"\n  ],\n  \"nested\": {\n    \"a\": 1,\n    \"b\": [\n      1,\n      2,\n      3\n    ]\n  }\n}",
                81,
                105,
            ), // json_pretty
            (
                "noul question: 顧客は返金を求めていますか？請求書が二重に発行されました。",
                31,
                22,
            ), // japanese
            (
                "noul question: 客户是否要求退款？发票被重复开具了。",
                30,
                17,
            ), // chinese
            (
                "noul question: 고객이 환불을 원합니까? 송장이 두 번 발행되었습니다.",
                53,
                24,
            ), // korean
            (
                "noul question: Хочет ли клиент вернуть деньги за двойное списание?",
                32,
                16,
            ), // cyrillic
            (
                "noul question: هل يريد العميل استرداد المبلغ بسبب الخصم المزدوج؟",
                40,
                21,
            ), // arabic
            ("noul question: क्या ग्राहक धनवापसी चाहता है?", 32, 13), // hindi
            ("noul question: ลูกค้าต้องการขอคืนเงินหรือไม่", 38, 12),     // thai
            (
                "noul question: Möchte der Kunde eine Rückerstattung für die doppelte Abbuchung? Straße",
                29,
                21,
            ), // german
            (
                "noul question: Is the user happy 😀😃🎉🔥👍🏽👨‍👩‍👧 or angry 😡🤬?",
                46,
                25,
            ), // emoji
            ("noul question: 𠜎𠜱𡃁 𓀀𓀁𓀂", 30, 30),                 // rare_cjk
            ("noul question: fn f(x) -> u8 { x + 1 }", 17, 18),    // code
            (
                "noul question: See https://example.com/api/v1/refunds?id=12345&sort=desc#top for details",
                30,
                34,
            ), // url
            (
                "noul question: Order 1234567890 cost 98765.4321 on 2026-10-20 at 23:59:59, ref 000111222333",
                36,
                65,
            ), // numbers
            (
                "noul question: deadbeef0123456789abcdefABCDEF 3f2a9c7e1b",
                24,
                29,
            ), // hex
            (
                "noul question: aGVsbG8gd29ybGQgdGhpcyBpcyBiYXNlNjQ=",
                31,
                26,
            ), // base64
            (
                "noul question: qxzvbnmkjhgfdsawrtyp zzkkqqxxvv bcdfghjklmnpqrstvwxz",
                33,
                30,
            ), // gibberish
            (
                "noul question: aBcDeFgHiJkLmNoPqRsTuVwXyZ qWeRtYuIoP",
                32,
                25,
            ), // mixed_case
            ("noul question: URGENT REFUND REQUEST XK9QZT7WPL", 21, 16), // all_caps
            (
                "noul question: HTTPServerError getUserById snake_case_identifier_name",
                18,
                15,
            ), // camel
            (
                "noul question: !!??..,,;;::--__==++**&&||^^%%$$##@@~~``''\"\"<<>>//\\\\",
                30,
                30,
            ), // symbols
            ("noul question: a  b   c\n\n\td    e\t\t\tf", 17, 20), // whitespace
            (
                "noul question: Pneumonoultramicroscopicsilicovolcanoconiosis antidisestablishmentarianism supercalifragilisticexpialidocious",
                38,
                31,
            ), // longword
            ("noul question: aeiou aeiouaeiou ooooo eeeee iaiaia", 21, 19), // vowelly
        ];
        for (text, english, multilingual) in cases {
            let estimate = laya_text_tokens(text);
            assert!(
                estimate >= english.max(multilingual),
                "{text:?}: estimate {estimate} < real (english {english}, multilingual {multilingual})"
            );
        }
    }

    /// The budget is inclusive: Laya cuts only past `head_max_len`, so a head
    /// estimated at exactly the budget is sent and one token more is refused.
    #[test]
    fn a_head_estimated_at_exactly_the_budget_is_sent() {
        let noul = |n: usize| DecisionQuestion::Noul {
            instructions: serde_json::json!(vec!["a"; n].join(" ")),
            criteria: None,
        };
        // Each further " a" costs exactly one token (a free space, a 1-token word).
        let n = (1..200)
            .find(|&n| estimated_head_tokens(&noul(n)) >= 192)
            .unwrap();
        assert_eq!(estimated_head_tokens(&noul(n)), 192);
        let (_, options) = estimated_head_parts(&noul(n));
        assert!(options <= 192 - 16, "only the whole-head rule is in play");
        let questions = |n| DecisionQuestions::from([("q".to_string(), noul(n))]);
        assert!(check_laya_head_budget("laya", "english", &questions(n)).is_ok());
        assert!(matches!(
            check_laya_head_budget("laya", "english", &questions(n + 1)),
            Err(GatewayError::InputRejected { .. })
        ));
    }

    /// The option rule is inclusive too: options estimated at exactly
    /// `budget − 16` leave Laya its 16 tokens of slack and are sent; one more
    /// token triggers its per-option cut and is refused.
    #[test]
    fn options_estimated_at_exactly_the_option_room_are_sent() {
        // 14 eight-digit keys at 12 each ([MASK], the leading space, 8 digits,
        // the 2-token margin), plus one last key: "1234" (8) makes 176,
        // "12345" (9) makes 177.
        let choice = |last: &str| {
            let mut criteria: Vec<(String, Option<String>)> = (0..14)
                .map(|i| (format!("{}", 10_000_000 + i), None))
                .collect();
            criteria.push((last.to_string(), None));
            DecisionQuestion::Choice {
                instructions: serde_json::json!("a"),
                criteria: criteria.into_iter().collect(),
            }
        };
        let (instruction, at_room) = estimated_head_parts(&choice("1234"));
        assert_eq!(at_room, 192 - 16);
        let (_, over) = estimated_head_parts(&choice("12345"));
        assert_eq!(over, 192 - 16 + 1);
        assert!(
            instruction + over <= 192,
            "the whole head fits: only the option rule is in play"
        );
        let questions = |last| DecisionQuestions::from([("q".to_string(), choice(last))]);
        assert!(check_laya_head_budget("laya", "english", &questions("1234")).is_ok());
        assert!(matches!(
            check_laya_head_budget("laya", "english", &questions("12345")),
            Err(GatewayError::InputRejected { message, .. }) if message.contains("options would be cut")
        ));
    }

    /// The ≤ 1-token under-count happens once PER OPTION, so it adds up: each of
    /// these 32 random keys has `" " + key` estimated 4 by the word rule but
    /// really 5 on `english` (`tests/fixtures/laya_head_tokens.py`, seed 2026),
    /// so the options really cost 32 × 6 = 192 > 176 and Laya cuts every
    /// option — silently: all stay distinct, no `usage.options`, `truncated:
    /// false`. The per-option margin must refuse it.
    #[test]
    fn per_option_under_counts_do_not_add_up_to_a_silent_cut() {
        let keys = [
            "yysohatc", "mzuegw", "cudfyihh", "nluhmimb", "fcaygoo", "ysjifu", "ilyttagy",
            "fynwyrur", "gdaaloiw", "hsehyetu", "aomnaboe", "aeswywec", "tdeirbuu", "iwybmyst",
            "dymcoosg", "dseemboo", "gwauhle", "yghowpao", "ylhojw", "hnacwuds", "lgupuyp",
            "gacmywou", "hbyinhio", "ihoegcic", "fwysuah", "rtohbacb", "lnuwfotg", "ifsytufb",
            "tgacsoh", "ciihunm", "bmylimt", "ymsyydpe",
        ];
        for key in keys {
            assert_eq!(laya_text_tokens(&format!(" {key}")), 4, "{key}");
        }
        let q = DecisionQuestion::Choice {
            instructions: serde_json::json!("Pick"),
            criteria: keys.iter().map(|k| (k.to_string(), None)).collect(),
        };
        let questions = DecisionQuestions::from([("q".to_string(), q)]);
        match check_laya_head_budget("laya", "english", &questions) {
            Err(GatewayError::InputRejected { message, .. }) => {
                assert!(message.contains("options would be cut"), "{message}")
            }
            other => panic!("32 options really costing 192 > 176 must be refused, got {other:?}"),
        }
    }

    /// Refusals name the checkpoint as requested and, when Laya resolves it to
    /// another name, as Laya will serve it.
    #[test]
    fn refusals_name_the_canonical_checkpoint() {
        let long = |n: usize| DecisionQuestion::Noul {
            instructions: serde_json::json!(vec!["a"; n].join(" ")),
            criteria: None,
        };
        let message = |model: &str, q: DecisionQuestion| match check_laya_head_budget(
            "laya",
            model,
            &DecisionQuestions::from([("q".to_string(), q)]),
        ) {
            Err(GatewayError::InputRejected { message, .. }) => message,
            other => panic!("{model}: expected a refusal, got {other:?}"),
        };
        let m = message("ml", long(300));
        assert!(
            m.contains("checkpoint 'ml' (multilingual) allows 256"),
            "{m}"
        );
        let m = message("english", long(300));
        assert!(m.contains("checkpoint 'english' allows 192"), "{m}");
        assert!(!m.contains("(english)"), "shown once when equal: {m}");
        let many = DecisionQuestion::Choice {
            instructions: serde_json::json!("Pick"),
            criteria: (0..25)
                .map(|i| (format!("{}", 10_000_000 + i), None))
                .collect(),
        };
        let m = message("ml", many);
        assert!(
            m.contains("the options of question 'q' need")
                && m.contains("checkpoint 'ml' (multilingual) leaves options 240 of its 256"),
            "{m}"
        );
    }

    /// Laya renders an empty description as none (`v is None or v == ""`): the
    /// bare key.
    #[test]
    fn an_empty_choice_description_costs_the_same_as_none() {
        let choice = |desc: Option<&str>| DecisionQuestion::Choice {
            instructions: serde_json::json!("Pick"),
            criteria: [
                ("a".to_string(), desc.map(str::to_string)),
                ("b".to_string(), None),
            ]
            .into_iter()
            .collect(),
        };
        assert_eq!(
            estimated_head_tokens(&choice(Some(""))),
            estimated_head_tokens(&choice(None))
        );
        assert!(
            estimated_head_tokens(&choice(Some("a long description of option a")))
                > estimated_head_tokens(&choice(None))
        );
    }

    /// Laya encodes each option as `" " + text` (`build_head`), and on the
    /// multilingual tokenizer a space before a digit, symbol or CJK char is a
    /// token of its own (`"▁", "1", "0", …`). Real counts of `" " + option`, by
    /// `tests/fixtures/laya_head_tokens.py rust option`; each option also costs
    /// its `[MASK]` and is capped at 48.
    #[test]
    fn the_option_estimate_is_at_least_the_real_count_on_both_tokenizers() {
        // (rendered option, real english tokens, real multilingual tokens)
        let cases: [(&str, usize, usize); _] = [
            ("10000000", 2, 9),                               // digit_key
            ("1234567890", 4, 11),                            // digit_key_10
            ("7", 1, 2),                                      // one_digit
            ("level 0: Routine", 5, 5),                       // level
            ("false: no, the statement does not hold", 9, 9), // noul_false
            ("true: yes, the statement holds", 7, 7),         // noul_true
            ("billing: Payments and refunds", 7, 5),          // choice_desc
            ("bug", 1, 1),                                    // bare_key
            ("退款", 5, 2),                                   // cjk_key
            ("退款: 客户要求退款", 16, 6),                    // cjk_desc
            ("<=5", 2, 2),                                    // symbol_key
            ("$100.00", 4, 7),                                // money
            ("50%", 2, 4),                                    // percent
            ("#1", 2, 2),                                     // hash
            ("😀", 2, 1),                                     // emoji_key
            ("-", 1, 1),                                      // dash
            ("a: {\"min\": 1, \"max\": 5}", 12, 14),          // json_desc
            ("2026-10-20", 6, 11),                            // date
            ("Возврат", 4, 2),                                // cyrillic_key
            ("استرداد", 6, 3),                                // arabic_key
            ("P1", 2, 2),                                     // upper_key
            ("needs_human_review", 5, 5),                     // snake_key
            (
                "Does the customer want a refund for the duplicate charge on their invoice?",
                14,
                14,
            ), // english
            (
                "{\"rubric\":\"refund\",\"weight\":3,\"tags\":[\"billing\",\"urgent\"],\"nested\":{\"a\":1,\"b\":[1,2,3]}}",
                35,
                32,
            ), // json_compact
            (
                "{\"rubric\": \"refund\", \"weight\": 3, \"tags\": [\"billing\", \"urgent\"], \"nested\": {\"a\": 1, \"b\": [1, 2, 3]}}",
                43,
                44,
            ), // json_python
            (
                "{\n  \"rubric\": \"refund\",\n  \"weight\": 3,\n  \"tags\": [\n    \"billing\",\n    \"urgent\"\n  ],\n  \"nested\": {\n    \"a\": 1,\n    \"b\": [\n      1,\n      2,\n      3\n    ]\n  }\n}",
                77,
                101,
            ), // json_pretty
            (
                "顧客は返金を求めていますか？請求書が二重に発行されました。",
                27,
                18,
            ), // japanese
            ("客户是否要求退款？发票被重复开具了。", 26, 13), // chinese
            (
                "고객이 환불을 원합니까? 송장이 두 번 발행되었습니다.",
                49,
                20,
            ), // korean
            (
                "Хочет ли клиент вернуть деньги за двойное списание?",
                28,
                12,
            ), // cyrillic
            ("هل يريد العميل استرداد المبلغ بسبب الخصم المزدوج؟", 36, 17), // arabic
            ("क्या ग्राहक धनवापसी चाहता है?", 28, 9),            // hindi
            ("ลูกค้าต้องการขอคืนเงินหรือไม่", 34, 8),                // thai
            (
                "Möchte der Kunde eine Rückerstattung für die doppelte Abbuchung? Straße",
                25,
                17,
            ), // german
            ("Is the user happy 😀😃🎉🔥👍🏽👨‍👩‍👧 or angry 😡🤬?", 42, 21), // emoji
            ("𠜎𠜱𡃁 𓀀𓀁𓀂", 26, 26),                           // rare_cjk
            ("fn f(x) -> u8 { x + 1 }", 13, 14),              // code
            (
                "See https://example.com/api/v1/refunds?id=12345&sort=desc#top for details",
                26,
                30,
            ), // url
            (
                "Order 1234567890 cost 98765.4321 on 2026-10-20 at 23:59:59, ref 000111222333",
                32,
                61,
            ), // numbers
            ("deadbeef0123456789abcdefABCDEF 3f2a9c7e1b", 20, 25), // hex
            ("aGVsbG8gd29ybGQgdGhpcyBpcyBiYXNlNjQ=", 27, 22), // base64
            (
                "qxzvbnmkjhgfdsawrtyp zzkkqqxxvv bcdfghjklmnpqrstvwxz",
                29,
                26,
            ), // gibberish
            ("aBcDeFgHiJkLmNoPqRsTuVwXyZ qWeRtYuIoP", 28, 21), // mixed_case
            ("URGENT REFUND REQUEST XK9QZT7WPL", 17, 12),     // all_caps
            (
                "HTTPServerError getUserById snake_case_identifier_name",
                14,
                11,
            ), // camel
            (
                "!!??..,,;;::--__==++**&&||^^%%$$##@@~~``''\"\"<<>>//\\\\",
                26,
                26,
            ), // symbols
            ("a  b   c\n\n\td    e\t\t\tf", 13, 16),          // whitespace
            (
                "Pneumonoultramicroscopicsilicovolcanoconiosis antidisestablishmentarianism supercalifragilisticexpialidocious",
                34,
                27,
            ), // longword
            ("aeiou aeiouaeiou ooooo eeeee iaiaia", 17, 15),  // vowelly
            // `stress-options` (seeds 1, 7): each 2 tokens under before the margin.
            ("uyrmuawr", 6, 5),   // random key, 2 under before the margin
            ("fwytiwee", 6, 4),   // random key, 2 under before the margin
            ("sdyiwlufyb", 7, 6), // random key, 2 under before the margin
            ("iafupm", 5, 4),     // random key, 2 under before the margin
            ("iybsuimilb", 7, 6), // random key, 2 under before the margin
            ("hnohpudb", 6, 5),   // random key, 2 under before the margin
            ("iycehriy", 6, 4),   // random key, 2 under before the margin
            ("yynoowyh", 6, 4),   // random key, 2 under before the margin
        ];
        for (option, english, multilingual) in cases {
            let estimate = laya_option_tokens(option);
            let real = 1 + english.max(multilingual).min(48);
            assert!(
                estimate >= real,
                "{option:?}: estimate {estimate} < real {real} (english {english}, multilingual {multilingual})"
            );
        }
    }

    /// The live Critical: 25 eight-digit keys on `multilingual` came back with
    /// `usage.options: {"q": {"total": 25, "distinct": 3, "tokens_per_option": 9}}`
    /// and `truncated: false`. Each really costs 10 (`[MASK]`, `▁`, 8 digits).
    #[test]
    fn twenty_five_eight_digit_keys_are_over_the_multilingual_budget() {
        let q = DecisionQuestion::Choice {
            instructions: serde_json::json!("Pick"),
            criteria: (0..25)
                .map(|i| (format!("{}", 10_000_000 + i), None))
                .collect(),
        };
        let estimate = estimated_head_tokens(&q);
        assert!(estimate > 256, "estimate {estimate}");
        let questions = DecisionQuestions::from([("q".to_string(), q)]);
        assert!(matches!(
            check_laya_head_budget("laya", "multilingual", &questions),
            Err(GatewayError::InputRejected { .. })
        ));
    }

    /// Laya cuts EVERY option to `max(4, (head_max_len − 16) / n)` tokens when
    /// `head_max_len − Σ options < 16` (`build_head`), even if the whole head
    /// would fit — so options over `budget − 16` are refused on their own.
    #[test]
    fn options_that_would_trigger_the_per_option_cut_are_refused() {
        // 14 eight-digit keys at 12 and one nine-digit key at 13: 181 = 192 − 11;
        // with the 11-token head of instruction "a" the whole head is 192.
        let mut criteria: Vec<(String, Option<String>)> = (0..14)
            .map(|i| (format!("{}", 10_000_000 + i), None))
            .collect();
        criteria.push(("123456789".into(), None));
        let q = DecisionQuestion::Choice {
            instructions: serde_json::json!("a"),
            criteria: criteria.into_iter().collect(),
        };
        let (instruction, options) = estimated_head_parts(&q);
        assert_eq!(options, 192 - 11);
        assert!(
            instruction + options <= 192,
            "the whole head fits ({instruction} + {options}): only the option rule refuses it"
        );
        let questions = DecisionQuestions::from([("q".to_string(), q)]);
        match check_laya_head_budget("laya", "english", &questions) {
            Err(GatewayError::InputRejected { message, .. }) => {
                assert!(message.contains("question 'q'"), "{message}");
                assert!(message.contains("options would be cut"), "{message}");
            }
            other => panic!("expected the per-option refusal, got {other:?}"),
        }
    }

    /// The whole head — instruction plus Laya's default noul options — for the
    /// Japanese sample: measured 49 real tokens on `english`, 40 on `multilingual`
    /// (`tests/fixtures/laya_head_tokens.py heads`).
    #[test]
    fn a_japanese_question_is_not_under_counted() {
        let q = DecisionQuestion::Noul {
            instructions: serde_json::json!(
                "顧客は返金を求めていますか？請求書が二重に発行されました。"
            ),
            criteria: None,
        };
        let estimate = estimated_head_tokens(&q);
        assert!(estimate >= 49, "estimate {estimate} < 49 real tokens");
    }
}
