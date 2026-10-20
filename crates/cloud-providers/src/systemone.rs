//! System One decision calls (gh#72) — `POST {base}/v1/systemone`.
//!
//! The wire format is TypeSafe's System One API, served verbatim by Ollama
//! (≥ 0.35), OpenRouter (`https://openrouter.ai/api`) and TypeSafe
//! (`https://api.typesafe.ai`). This module holds the shared call and
//! [`SystemOneAdapter`], an id-configurable adapter for any bearer-auth host
//! of the endpoint; Ollama's adapter delegates to the same core and adds its
//! own reading of the 404s it returns.

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::base::{
    base_url_or, extract_error_message, map_status_error, parse_retry_after_ms, resolve_api_key,
};
use kernel::types::config::RouterConfig;
use kernel::types::cost::TokenUsage;
use kernel::types::decision::{
    DecisionAnswers, DecisionContent, DecisionQuestion, DecisionQuestions,
};
use kernel::types::error::GatewayError;
use kernel::types::io::{DecisionRequest, DecisionResponse};

/// OpenRouter's API base; `/v1/systemone` is appended.
pub const OPENROUTER_BASE_URL: &str = "https://openrouter.ai/api";
/// TypeSafe's API base (the reference implementation of System One).
pub const TYPESAFE_BASE_URL: &str = "https://api.typesafe.ai";

const PATH: &str = "/v1/systemone";

/// Which wire a System One host speaks. Everything else — transport, auth,
/// timeout, header forwarding, error mapping, the missing-answer check — is
/// shared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Dialect {
    /// TypeSafe's API verbatim: `POST {base}/v1/systemone`, the body `model`
    /// as given, the response body is the result. Ollama, OpenRouter,
    /// TypeSafe, llama.cpp, SGLang, Laya.
    SystemOne,
    /// Cloudflare Workers AI REST (verified 2026-10-03): `POST
    /// {base}/run/@cf/cloudflare/{model}` with `base = .../accounts/<id>/ai`;
    /// the body `model` is the SHORT name; no `keep_alive` (not in the
    /// schema); the result is wrapped in `{result, success, errors, messages}`.
    WorkersAi,
}

/// Who is being called and how: the adapter id errors are tagged with, the
/// wire dialect, and the image encoding the host accepts.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Host<'a> {
    pub adapter: &'a str,
    pub dialect: Dialect,
    pub images: ImageEncoding,
}

/// `(path id, body model)` for Workers AI: the catalog id `@cf/<org>/<name>`
/// goes in the path and only `<name>` in the body (the schema pins the body to
/// `^\s*(clef|clef-flash)\s*$`). A bare name is a Cloudflare-published model.
///
/// The model becomes part of the URL PATH and a caller can pin it, so it is
/// validated, never spliced: each segment must be a plain name
/// (`[A-Za-z0-9][A-Za-z0-9._-]*`), which rules out `..`, `?`, `#`, `%` and
/// extra `/` — anything that could steer the request (and the operator's
/// token) off `/ai/run/` to another Cloudflare API path.
fn workers_ai_model(model: &str) -> Result<(String, &str), String> {
    fn plain(segment: &str) -> bool {
        let mut chars = segment.chars();
        chars.next().is_some_and(|c| c.is_ascii_alphanumeric())
            && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    }
    let model = model.trim();
    let invalid = || {
        format!(
            "'{model}' is not a Workers AI model name (expected e.g. clef or @cf/cloudflare/clef)"
        )
    };
    match model.strip_prefix("@cf/") {
        Some(rest) => match rest.split('/').collect::<Vec<_>>().as_slice() {
            [org, name] if plain(org) && plain(name) => Ok((model.to_string(), *name)),
            _ => Err(invalid()),
        },
        None if plain(model) => Ok((format!("@cf/cloudflare/{model}"), model)),
        None => Err(invalid()),
    }
}

/// How a host wants `DecisionRequest.images` (bare base64 from the caller).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImageEncoding {
    /// Sent verbatim. Ollama REQUIRES this ("URLs and data URLs are not
    /// supported").
    Bare,
    /// `data:<sniffed mime>;base64,<b64>` — what llama.cpp (bare → 400),
    /// Cloudflare, vLLM's example and SGLang all accept.
    DataUrl,
    /// The host does not read images and IGNORES the field (Laya: 200 with
    /// text-only answers), so any image is refused before sending (SP-DEC-3 D2).
    Unsupported,
}

/// Encode the caller's images for a host. A value already in data-URL form
/// passes through. For `DataUrl`, the mime type is read from the decoded magic
/// bytes; an image that is no known format cannot be labelled truthfully, so
/// it is an error (before anything is sent) rather than a guessed type.
fn encode_images(images: &[String], encoding: ImageEncoding) -> Result<Vec<String>, String> {
    match encoding {
        ImageEncoding::Unsupported if images.is_empty() => Ok(vec![]),
        ImageEncoding::Unsupported => Err(format!(
            "this host does not read images ({} sent) — it would answer as if they were \
             absent; route image decisions to a vision-capable System One host",
            images.len()
        )),
        ImageEncoding::Bare => Ok(images.to_vec()),
        ImageEncoding::DataUrl => images
            .iter()
            .enumerate()
            .map(|(i, img)| {
                if img.starts_with("data:") {
                    return Ok(img.clone());
                }
                let mime = sniff_image_mime(img).ok_or_else(|| {
                    format!(
                        "image {} is not PNG, JPEG, WebP or GIF base64 — cannot build a data URL for it",
                        i + 1
                    )
                })?;
                // The sniffer reads past line breaks (MIME/PEM-wrapped base64);
                // the URL must not carry them into the request.
                let b64: String = img.chars().filter(|c| !c.is_whitespace()).collect();
                Ok(format!("data:{mime};base64,{b64}"))
            })
            .collect()
    }
}

/// The image encoding the generic adapter uses for a router id: `laya`
/// ignores images (SP-DEC-3 D2); every other `/v1/systemone` host that takes
/// images accepts data URLs (SP-DEC-2 D1).
fn image_encoding(id: &str) -> ImageEncoding {
    match id {
        "laya" => ImageEncoding::Unsupported,
        _ => ImageEncoding::DataUrl,
    }
}

/// Laya's per-question head budget for a checkpoint (SP-DEC-3 D6): its
/// `head_max_len` from `rl_agent_config.json` — `english` 192, `multilingual` /
/// `typed-decisions` 256. Any other name (an operator-registered checkpoint, a
/// typo) gets the smallest, never room it may not have.
fn laya_head_budget(model: &str) -> usize {
    match model {
        "multilingual" | "typed-decisions" => 256,
        _ => 192,
    }
}

/// A conservative estimate of the tokens Laya's `build_head` spends on one
/// question (SP-DEC-3 D6). Laya renders `"<type> question: <instructions>"` and
/// one `[MASK]`-prefixed span per option (capped at 48 tokens each), then cuts
/// the instruction SILENTLY so the whole head fits `head_max_len` — `truncated`
/// never reports it.
///
/// It counts the same text Laya renders (`render_options` in
/// `laya_coreml/common.py`) with [`laya_text_tokens`]: a choice option is
/// `"key: description"` or the bare key; a score level is
/// `"level i: description"`; a noul side is `"false: …"` / `"true: …"` with
/// Laya's default text when undescribed. Non-string instructions are Laya's
/// `json.dumps` text, separators included.
///
/// Measured 2026-10-20 against both real tokenizers (`english`, which
/// typed-decisions shares, and `multilingual`, which -ane shares) on 27 varied
/// samples — English, JSON (compact, Python, pretty), Japanese, Chinese,
/// Korean, Cyrillic, Arabic, Hindi, Thai, German, emoji, astral CJK, code,
/// URLs, numbers, hex, base64, gibberish, mixed case, ALLCAPS, camelCase,
/// symbols, whitespace, long words: the estimate is never below the real
/// count; the tightest margins are +0 (astral CJK as an option: a byte bound)
/// and +2 (whitespace, numbers). On 21 000 seeded random strings, 6 short ones
/// were under by 1 token. The price is over-counting: about 1.6× on English
/// (the 288-char D6 instruction: 180 estimated, 109 real on `english`; the
/// 548-char one that really fits, 300 vs 189, refused) and 2–7× on non-Latin
/// scripts, whose every char is counted at its UTF-8 bytes. Refusing a head
/// that would fit costs a fallback; sending one that does not returns a
/// confident answer to a question the model never read.
fn estimated_head_tokens(q: &DecisionQuestion) -> usize {
    let tokens = laya_text_tokens;
    fn option(text: &str) -> usize {
        1 + laya_text_tokens(text).min(48)
    }
    let (kind, instructions) = match q {
        DecisionQuestion::Choice { instructions, .. } => ("choice", instructions),
        DecisionQuestion::Noul { instructions, .. } => ("noul", instructions),
        DecisionQuestion::Score { instructions, .. } => ("score", instructions),
    };
    let head = tokens(&format!(
        "{kind} question: {}",
        laya_instruction_text(instructions)
    ));
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
    head + options
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
fn check_laya_head_budget(
    adapter: &str,
    model: &str,
    questions: &DecisionQuestions,
) -> Result<(), GatewayError> {
    let budget = laya_head_budget(model);
    for (name, q) in questions {
        let estimate = estimated_head_tokens(q);
        if estimate > budget {
            return Err(GatewayError::InputRejected {
                adapter: adapter.into(),
                message: format!(
                    "question '{name}' needs ≈{estimate} tokens of head but checkpoint \
                     '{model}' allows {budget} — Laya would silently cut it; shorten the \
                     instructions/options"
                ),
            });
        }
    }
    Ok(())
}

/// The image type from the first decoded bytes of a base64 string.
fn sniff_image_mime(b64: &str) -> Option<&'static str> {
    use base64::Engine as _;
    // 16 base64 chars = 12 bytes: enough for every signature below, and a
    // multiple of 4 so a prefix decodes without padding.
    let prefix: String = b64
        .chars()
        .filter(|c| !c.is_whitespace())
        .take(16)
        .collect();
    let head = base64::engine::general_purpose::STANDARD
        .decode(prefix)
        .ok()?;
    match head.as_slice() {
        [0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, ..] => Some("image/png"),
        [0xFF, 0xD8, 0xFF, ..] => Some("image/jpeg"),
        [
            b'R',
            b'I',
            b'F',
            b'F',
            _,
            _,
            _,
            _,
            b'W',
            b'E',
            b'B',
            b'P',
            ..,
        ] => Some("image/webp"),
        [b'G', b'I', b'F', b'8', b'7' | b'9', b'a', ..] => Some("image/gif"),
        _ => None,
    }
}

/// Bound on one decision call when the router sets no `timeout_ms`. A bare
/// `reqwest::Client` has NO timeout, so a host that accepts the connection and
/// never answers would hang the caller forever (the defect `OllamaAdapter::new`
/// already fixed for chat with the same 120s).
const DEFAULT_TIMEOUT_SECS: u64 = 120;

/// The deadline applied to every decision request: the router's `timeout_ms`,
/// else [`DEFAULT_TIMEOUT_SECS`]. Applied per request rather than per client so
/// it holds however the adapter was built (`with_id` has no config to read).
pub(crate) fn request_timeout(cfg: &RouterConfig) -> std::time::Duration {
    cfg.timeout_ms
        .map(std::time::Duration::from_millis)
        .unwrap_or(std::time::Duration::from_secs(DEFAULT_TIMEOUT_SECS))
}

/// Whether a local Ollama can serve a decision model right now — the
/// distinctions gh#72 asks probing to make.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecisionModelStatus {
    /// The model is pulled, advertises the `decision` capability, and the
    /// server has the endpoint.
    Ready,
    /// The server predates System One (`/v1/systemone` arrived in Ollama 0.35).
    ServerTooOld { version: String },
    /// The server is new enough but the model is not pulled.
    NotPulled,
    /// The model is pulled but is not a decision model.
    NotADecisionModel,
}

#[derive(Serialize)]
struct WireRequest<'a> {
    model: &'a str,
    state: &'a DecisionContent,
    questions: &'a DecisionQuestions,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    images: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    keep_alive: Option<&'a serde_json::Value>,
}

/// Workers AI's REST envelope around a result. Unknown fields are ignored.
#[derive(Deserialize)]
struct WorkersAiEnvelope {
    #[serde(default)]
    success: bool,
    #[serde(default)]
    result: Option<WireResponse>,
    #[serde(default)]
    errors: Vec<WorkersAiError>,
}

#[derive(Deserialize)]
struct WorkersAiError {
    #[serde(default)]
    message: String,
}

/// Unknown fields (OpenRouter's `id`, `provider`, `usage.cost`) are ignored.
#[derive(Deserialize)]
struct WireResponse {
    #[serde(default)]
    model: Option<String>,
    answers: DecisionAnswers,
    #[serde(default)]
    usage: Option<WireUsage>,
    /// Laya's report of the checkpoint that actually served the call (SP-DEC-3
    /// D7). Absent on every other host.
    #[serde(default)]
    routing: Option<WireRouting>,
}

/// Laya auto-routes a `model` it does not know (200, no error), so the served
/// checkpoint is the only sign of a misspelled `api_model_id`.
#[derive(Deserialize)]
struct WireRouting {
    #[serde(default)]
    model: Option<String>,
}

#[derive(Deserialize)]
struct WireUsage {
    #[serde(default)]
    input_tokens: u32,
    #[serde(default)]
    output_tokens: u32,
    /// Laya's report that it cut the input to fit its window (SP-DEC-3 D3).
    /// Absent on every other host, so it defaults to "not truncated". It is
    /// the authority: `false` with `state_tokens_dropped > 0` is a success. A
    /// `null` or non-bool fails the parse on purpose — we cannot tell the
    /// answer was not truncated, so it fails closed (ProviderError, fallback);
    /// `true` is an InputRejected (D5).
    #[serde(default)]
    truncated: bool,
    /// Diagnostics only, shown in the error: a `null` here never vetoes an answer.
    #[serde(default)]
    state_tokens_dropped: Option<u32>,
    #[serde(default)]
    truncated_questions: Option<Vec<String>>,
}

/// A non-success response, before it becomes a [`GatewayError`] — kept apart
/// so a caller can reword a status it knows more about (Ollama's two 404s).
pub(crate) struct Rejection {
    pub status: u16,
    pub body: String,
    retry_after_ms: Option<u64>,
}

impl Rejection {
    /// The provider's own message: the JSON `error` when present, else the raw body.
    pub fn message(&self) -> String {
        extract_error_message(&self.body).unwrap_or_else(|| self.body.clone())
    }

    /// Whether the body is a JSON error envelope (as opposed to a bare router
    /// 404 page from a server that has no such endpoint at all).
    pub fn is_json_error(&self) -> bool {
        extract_error_message(&self.body).is_some()
    }

    pub fn into_error(self, adapter: &str, message: String) -> GatewayError {
        map_status_error(adapter, self.status, message, self.retry_after_ms)
    }
}

/// POST one decision call to `{base_url}/v1/systemone`. A bearer is sent only
/// when [`resolve_api_key`] yields one (local Ollama is keyless).
pub(crate) async fn decide(
    client: &Client,
    base_url: &str,
    host: Host<'_>,
    model: &str,
    cfg: &RouterConfig,
    req: &DecisionRequest,
) -> Result<Result<DecisionResponse, Rejection>, GatewayError> {
    let adapter = host.adapter;
    // An image this host cannot take (D2) or cannot be labelled (SP-DEC-2) is
    // the request's fault, not the endpoint's: InputRejected (SP-DEC-3 D5) —
    // falls back whatever the triggers, not retried, no breaker signal.
    let images =
        encode_images(&req.images, host.images).map_err(|message| GatewayError::InputRejected {
            adapter: adapter.into(),
            message,
        })?;
    let base = base_url.trim_end_matches('/');
    let (url, body_model, keep_alive) = match host.dialect {
        Dialect::SystemOne => (format!("{base}{PATH}"), model, req.keep_alive.as_ref()),
        Dialect::WorkersAi => {
            let (path_id, short) = workers_ai_model(model)
                .map_err(|message| GatewayError::InvalidRequest { message })?;
            (format!("{base}/run/{path_id}"), short, None)
        }
    };
    let body = WireRequest {
        model: body_model,
        state: &req.state,
        questions: &req.questions,
        images,
        keep_alive,
    };
    let mut http = client.post(url).timeout(request_timeout(cfg)).json(&body);
    if let Some(key) = resolve_api_key(cfg) {
        http = http.bearer_auth(key);
    }
    for (k, v) in &cfg.headers {
        http = http.header(k.as_str(), v.as_str());
    }

    let response = http.send().await?;
    let status = response.status();
    if !status.is_success() {
        let retry_after_ms = parse_retry_after_ms(
            response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
        );
        return Ok(Err(Rejection {
            status: status.as_u16(),
            body: response.text().await.unwrap_or_default(),
            retry_after_ms,
        }));
    }

    let parse_err = |e: reqwest::Error| GatewayError::ProviderError {
        adapter: adapter.into(),
        message: format!("failed to parse System One response: {e}"),
        status: Some(status.as_u16()),
    };
    let wire: WireResponse = match host.dialect {
        Dialect::SystemOne => response.json().await.map_err(parse_err)?,
        Dialect::WorkersAi => {
            let envelope: WorkersAiEnvelope = response.json().await.map_err(parse_err)?;
            match envelope.result {
                Some(result) if envelope.success => result,
                // A 2xx that is not a success — or carries no result — did
                // not answer anything; never read it as an empty success.
                _ => {
                    let reasons: Vec<String> = envelope
                        .errors
                        .into_iter()
                        .map(|e| e.message)
                        .filter(|m| !m.is_empty())
                        .collect();
                    return Err(GatewayError::ProviderError {
                        adapter: adapter.into(),
                        message: if reasons.is_empty() {
                            "Workers AI returned no result".into()
                        } else {
                            format!("Workers AI returned no result: {}", reasons.join("; "))
                        },
                        status: Some(status.as_u16()),
                    });
                }
            }
        }
    };
    // SP-DEC-3 D3: a host that truncated the input still answers 200 — about a
    // state it never fully read. The same input truncates on every retry and
    // the endpoint is healthy, so it is an InputRejected (D5): the chain falls
    // back, nothing retries it, and the breaker never counts it.
    if let Some(u) = wire.usage.as_ref().filter(|u| u.truncated) {
        let mut cut = Vec::new();
        let dropped = u.state_tokens_dropped.unwrap_or_default();
        if dropped > 0 {
            cut.push(format!("{dropped} state tokens dropped"));
        }
        if let Some(qs) = u.truncated_questions.as_deref().filter(|q| !q.is_empty()) {
            cut.push(format!("questions cut: {qs:?}"));
        }
        return Err(GatewayError::InputRejected {
            adapter: adapter.into(),
            message: format!(
                "the host truncated the input to fit its window ({}); its answers do not cover \
                 the whole request — set the model's context_window to its real token window \
                 (or lower it if it already is)",
                if cut.is_empty() {
                    "no detail".to_string()
                } else {
                    cut.join("; ")
                }
            ),
        });
    }

    // A 2xx with holes is not a success: report it as a failed attempt so the
    // engine records it and falls back, rather than `success: true` over
    // questions nobody answered.
    let missing: Vec<&str> = req
        .questions
        .keys()
        .filter(|name| !wire.answers.contains_key(*name))
        .map(String::as_str)
        .collect();
    if !missing.is_empty() {
        return Err(GatewayError::ProviderError {
            adapter: adapter.into(),
            message: format!("System One response has no answer for {missing:?}"),
            status: Some(status.as_u16()),
        });
    }

    // SP-DEC-3 D7: the host served another checkpoint than the one asked for
    // (Laya auto-routes an unknown name) — answers from a model with another
    // window. A configuration fault, so a ProviderError like any endpoint-wide
    // fault. Compared with the BODY model: Workers AI's is the short name.
    if let Some(served) = wire.routing.as_ref().and_then(|r| r.model.as_deref())
        && served != body_model
    {
        return Err(GatewayError::ProviderError {
            adapter: adapter.into(),
            message: format!(
                "the host served checkpoint '{served}' instead of the requested '{body_model}' \
                 — check the model's api_model_id against the server's loaded checkpoints"
            ),
            status: Some(status.as_u16()),
        });
    }

    Ok(Ok(DecisionResponse {
        answers: wire.answers,
        usage: wire.usage.map(|u| TokenUsage {
            input_tokens: u.input_tokens,
            output_tokens: u.output_tokens,
            total_tokens: u.input_tokens.saturating_add(u.output_tokens),
        }),
        model: wire.model.or_else(|| Some(model.to_string())),
        degraded: false,
    }))
}

/// Generic System One adapter for any bearer-auth host of the endpoint —
/// OpenRouter, TypeSafe, or a self-hosted server (llama.cpp, SGLang, Laya — `laya`, which refuses images) via `url`.
/// Decision-only: register a chat adapter under the same id separately.
pub struct SystemOneAdapter {
    client: Client,
    id: String,
}

impl SystemOneAdapter {
    /// Build an adapter registered under `id`. The id should match the
    /// router key in `GatewayConfig::routers`, since the engine dispatches by
    /// router id.
    pub fn with_id(id: impl Into<String>) -> Result<Self, GatewayError> {
        Ok(Self {
            client: Client::new(),
            id: id.into(),
        })
    }

    /// Same as [`Self::with_id`] with the router's timeout applied.
    pub fn from_config_with_id(
        id: impl Into<String>,
        config: &RouterConfig,
    ) -> Result<Self, GatewayError> {
        Ok(Self {
            client: crate::base::build_client(config)?,
            id: id.into(),
        })
    }

    /// `config.url`, else the well-known base for `openrouter` / `typesafe`.
    fn base_url<'a>(&self, config: &'a RouterConfig) -> Result<&'a str, GatewayError> {
        let default = match self.id.as_str() {
            "openrouter" => OPENROUTER_BASE_URL,
            "typesafe" => TYPESAFE_BASE_URL,
            _ => "",
        };
        match base_url_or(config, default) {
            "" => Err(GatewayError::InvalidConfig(format!(
                "router '{}' needs a `url` for its System One endpoint",
                self.id
            ))),
            url => Ok(url),
        }
    }
}

impl kernel::adapters::capability::Model for SystemOneAdapter {
    fn id(&self) -> &str {
        &self.id
    }
}

#[async_trait]
impl kernel::adapters::capability::DecisionModel for SystemOneAdapter {
    async fn decide(
        &self,
        config: &RouterConfig,
        req: &DecisionRequest,
    ) -> Result<DecisionResponse, GatewayError> {
        // A hosted router has no sensible default decision model; guessing one
        // would bill the caller for a model they never chose.
        let Some(model) = req.model.as_deref() else {
            return Err(GatewayError::InvalidRequest {
                message: format!("a decision call to '{}' needs a model", self.id),
            });
        };
        let base_url = self.base_url(config)?;
        if self.id == "laya" {
            check_laya_head_budget(&self.id, model, &req.questions)?;
        }
        let host = Host {
            adapter: &self.id,
            dialect: Dialect::SystemOne,
            images: image_encoding(&self.id),
        };
        decide(&self.client, base_url, host, model, config, req)
            .await?
            .map_err(|r| {
                let message = r.message();
                r.into_error(&self.id, message)
            })
    }
}

#[async_trait]
impl kernel::adapters::RegisterInto for SystemOneAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &kernel::adapters::AdapterRegistry) {
        reg.register_decision(self).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn cfg(url: &str) -> RouterConfig {
        RouterConfig {
            url: url.into(),
            api_key: None,
            api_key_env: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        }
    }

    /// SP-DEC-3 D2: a host that ignores `images` must never receive them —
    /// the answers would silently describe a text-only request.
    #[test]
    fn an_unsupported_host_refuses_images_and_passes_text_only_calls() {
        assert_eq!(encode_images(&[], ImageEncoding::Unsupported), Ok(vec![]));
        let err = encode_images(
            &["iVBORw0KGgo=".to_string(), "iVBORw0KGgo=".to_string()],
            ImageEncoding::Unsupported,
        )
        .unwrap_err();
        assert!(err.contains("does not read images"), "{err}");
        assert!(err.contains('2'), "says how many were refused: {err}");
    }

    #[test]
    fn laya_is_the_only_generic_id_that_refuses_images() {
        assert_eq!(image_encoding("laya"), ImageEncoding::Unsupported);
        for id in ["openrouter", "typesafe", "llamacpp", "sglang", "custom"] {
            assert_eq!(image_encoding(id), ImageEncoding::DataUrl, "{id}");
        }
    }

    /// SP-DEC-3 D6: each checkpoint's `head_max_len` (its `rl_agent_config.json`);
    /// an unknown name gets the smallest, so a misspelled or operator-registered
    /// checkpoint is never assumed to have room it may not have.
    #[test]
    fn laya_head_budget_is_the_checkpoints_head_max_len() {
        assert_eq!(laya_head_budget("english"), 192);
        assert_eq!(laya_head_budget("multilingual"), 256);
        assert_eq!(laya_head_budget("typed-decisions"), 256);
        for unknown in ["ane", "englsh", "", "MULTILINGUAL"] {
            assert_eq!(laya_head_budget(unknown), 192, "{unknown:?}");
        }
    }

    /// The estimate counts the text Laya renders (`build_head` / `render_options`
    /// in laya_coreml/common.py) — each piece through `laya_text_tokens` — plus
    /// one `[MASK]` per option and Laya's 48-token option cap.
    #[test]
    fn estimated_head_tokens_counts_what_laya_renders() {
        use kernel::types::decision::{DecisionQuestion, NoulCriteria};
        let t = laya_text_tokens;
        let (no, yes) = (
            1 + t("false: no, the statement does not hold"),
            1 + t("true: yes, the statement holds"),
        );
        // Laya's default noul options when a side is undescribed.
        let noul = DecisionQuestion::Noul {
            instructions: serde_json::json!("abcdef"),
            criteria: None,
        };
        assert_eq!(
            estimated_head_tokens(&noul),
            t("noul question: abcdef") + no + yes
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
            t("noul question: abcdef") + no + 1 + t("true: x")
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
            t("choice question: Pick") + 1 + t("a: bb") + 1 + t("c")
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
            t(&format!("score question: {json_text}"))
                + 1
                + t("level 0: lo")
                + 1
                + t("level 1: hi")
        );
        // An option is capped at 48 tokens (+1 [MASK]) however long it is.
        let long = DecisionQuestion::Choice {
            instructions: serde_json::json!("Pick"),
            criteria: [("k".to_string(), Some("d".repeat(300)))]
                .into_iter()
                .collect(),
        };
        assert_eq!(
            estimated_head_tokens(&long),
            t("choice question: Pick") + 49
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
    }

    /// SP-DEC-3 D6: the estimate must never be BELOW what Laya really spends —
    /// an under-count is a silent cut. Real counts measured 2026-10-20 with each
    /// checkpoint's own `tokenizer.json` (`english` = laya-coreml, also used by
    /// typed-decisions; `multilingual` = laya-multilingual-coreml, also -ane) by
    /// `/tmp/r3-scripts/measure.py` / `rust_cases.py` (the `tokenizers` package,
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

    /// The whole head — instruction plus Laya's default noul options — for the
    /// Japanese sample: measured 49 real tokens on `english`, 40 on `multilingual`
    /// (`/tmp/r3-scripts/heads.py`).
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

    /// Line-wrapped base64 (MIME/PEM style) sniffs fine — and the data URL
    /// built from it must not carry the line breaks into the request.
    #[test]
    fn a_wrapped_image_becomes_a_single_line_data_url() {
        let wrapped = "iVBORw0KGgoAAAANSUhE\nUgAAAAEAAAABCAYAAAAf\r\nFcSJ".to_string();
        let encoded = encode_images(&[wrapped], ImageEncoding::DataUrl).unwrap();
        assert_eq!(
            encoded,
            ["data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJ"]
        );
        // Bare (Ollama) still sends exactly what the caller gave.
        let wrapped = "iVBORw0KGgo\nAAAANSUhE".to_string();
        assert_eq!(
            encode_images(std::slice::from_ref(&wrapped), ImageEncoding::Bare).unwrap(),
            [wrapped]
        );
    }

    /// Every signature the sniffer claims, from real file headers; anything
    /// else — text, truncated input, non-base64 — is no type at all.
    #[test]
    fn image_mime_is_read_from_the_magic_bytes() {
        use base64::Engine as _;
        let b64 = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
        let cases: [(&[u8], Option<&str>); 9] = [
            (b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR", Some("image/png")),
            (b"\xff\xd8\xff\xe0\0\x10JFIF\0\x01", Some("image/jpeg")),
            (b"RIFF$\0\0\0WEBPVP8 ", Some("image/webp")),
            (b"GIF87a\x01\0\x01\0\0\0", Some("image/gif")),
            (b"GIF89a\x01\0\x01\0\0\0", Some("image/gif")),
            (b"RIFF$\0\0\0WAVEfmt ", None), // RIFF, but audio
            (b"GIF86a\x01\0\x01\0\0\0", None),
            (b"hello world!", None),
            (b"\x89PN", None), // too short to be a PNG signature
        ];
        for (bytes, want) in cases {
            assert_eq!(sniff_image_mime(&b64(bytes)), want, "{bytes:?}");
        }
        assert_eq!(sniff_image_mime("not base64 at all!!"), None);
        // Line-wrapped base64 (whitespace inside) still sniffs.
        let wrapped: String = b64(b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR")
            .chars()
            .flat_map(|c| [c, '\n'])
            .collect();
        assert_eq!(sniff_image_mime(&wrapped), Some("image/png"));
    }

    /// A router with no `timeout_ms` still gets a bound: a wedged host must
    /// not hang `Gateway::execute` (the defect `OllamaAdapter::new` fixed).
    #[test]
    fn requests_are_bounded_even_without_a_configured_timeout() {
        assert_eq!(
            request_timeout(&cfg("http://h")),
            std::time::Duration::from_secs(120)
        );
        let mut c = cfg("http://h");
        c.timeout_ms = Some(2500);
        assert_eq!(request_timeout(&c), std::time::Duration::from_millis(2500));
    }

    #[test]
    fn well_known_ids_default_their_base_url_and_others_require_one() {
        let base = |id: &str, url: &str| {
            SystemOneAdapter::with_id(id)
                .unwrap()
                .base_url(&cfg(url))
                .map(str::to_string)
        };
        assert_eq!(base("openrouter", "").unwrap(), OPENROUTER_BASE_URL);
        assert_eq!(base("typesafe", "").unwrap(), TYPESAFE_BASE_URL);
        assert_eq!(
            base("openrouter", "https://proxy.example/api/").unwrap(),
            "https://proxy.example/api"
        );
        assert!(matches!(
            base("llamacpp", ""),
            Err(GatewayError::InvalidConfig(m)) if m.contains("llamacpp")
        ));
    }
}
