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
use kernel::types::decision::{DecisionAnswers, DecisionContent, DecisionQuestions};
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
    /// TypeSafe, llama.cpp, SGLang.
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
}

/// Encode the caller's images for a host. A value already in data-URL form
/// passes through. For `DataUrl`, the mime type is read from the decoded magic
/// bytes; an image that is no known format cannot be labelled truthfully, so
/// it is an error (before anything is sent) rather than a guessed type.
fn encode_images(images: &[String], encoding: ImageEncoding) -> Result<Vec<String>, String> {
    if encoding == ImageEncoding::Bare {
        return Ok(images.to_vec());
    }
    images
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
}

#[derive(Deserialize)]
struct WireUsage {
    #[serde(default)]
    input_tokens: u32,
    #[serde(default)]
    output_tokens: u32,
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
    let images =
        encode_images(&req.images, host.images).map_err(|message| GatewayError::ProviderError {
            adapter: adapter.into(),
            message,
            status: None,
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
/// OpenRouter, TypeSafe, or a self-hosted server (llama.cpp, SGLang) via `url`.
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
        let host = Host {
            adapter: &self.id,
            dialect: Dialect::SystemOne,
            images: ImageEncoding::DataUrl,
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
