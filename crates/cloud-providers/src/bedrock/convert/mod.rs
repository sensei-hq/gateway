//! Pure wire-format conversion between gateway types and the Bedrock
//! Converse SDK types.
//!
//! Everything here is a total function of its inputs — content-block ↔
//! SDK-type mapping (request messages in, `ConverseResponse` / stream
//! events out), plus SDK-error → [`GatewayError`] mapping. None of it
//! touches HTTP, auth, or adapter state (the `Client`, credentials, and
//! `RouterConfig` plumbing all stay in `super`), which is what makes it
//! unit-testable without an SDK client.
//!
//! Split across two child modules by direction of travel:
//! - [`request`] — gateway messages/tools → Bedrock request types.
//! - [`response`] — Bedrock `ConverseResponse` / stream events / SDK
//!   errors → gateway types.
//!
//! The [`Document`] ↔ [`serde_json::Value`] bridge ([`json_to_document`]
//! / [`document_to_json`]) is shared by both directions, so it stays here
//! and is reached from the child modules as an ancestor-private.

use aws_smithy_types::{Document, Number};

mod request;
mod response;

// Re-exported so the parent `bedrock` module keeps importing these from
// `convert::…` exactly as before the split.
pub(super) use request::{build_messages, build_system, build_tool_config};
pub(super) use response::{chunk_from_event, extract_text, extract_tool_calls, map_sdk_error};

/// Recursively convert a [`serde_json::Value`] into an
/// [`aws_smithy_types::Document`]. Both have the same JSON-shaped
/// tree — only the number wrapping differs.
fn json_to_document(v: serde_json::Value) -> Document {
    match v {
        serde_json::Value::Null => Document::Null,
        serde_json::Value::Bool(b) => Document::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                Document::Number(Number::PosInt(u))
            } else if let Some(i) = n.as_i64() {
                Document::Number(Number::NegInt(i))
            } else if let Some(f) = n.as_f64() {
                Document::Number(Number::Float(f))
            } else {
                Document::Null
            }
        }
        serde_json::Value::String(s) => Document::String(s),
        serde_json::Value::Array(arr) => {
            Document::Array(arr.into_iter().map(json_to_document).collect())
        }
        serde_json::Value::Object(map) => Document::Object(
            map.into_iter()
                .map(|(k, v)| (k, json_to_document(v)))
                .collect(),
        ),
    }
}

/// Inverse of [`json_to_document`]. Floats that don't fit a JSON
/// number (NaN / infinity) degrade to Null, mirroring serde_json's
/// own behaviour.
fn document_to_json(d: &Document) -> serde_json::Value {
    match d {
        Document::Null => serde_json::Value::Null,
        Document::Bool(b) => serde_json::Value::Bool(*b),
        Document::Number(n) => match n {
            Number::PosInt(u) => serde_json::Value::Number((*u).into()),
            Number::NegInt(i) => serde_json::Value::Number((*i).into()),
            Number::Float(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
        },
        Document::String(s) => serde_json::Value::String(s.clone()),
        Document::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(document_to_json).collect())
        }
        Document::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), document_to_json(v)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_to_document_round_trips_through_document_to_json() {
        let original = serde_json::json!({
            "city": "Berlin",
            "limit": 5,
            "negative": -3,
            "ratio": 0.75,
            "flag": true,
            "tags": ["a", "b"],
            "nested": {"deep": null},
        });
        let doc = json_to_document(original.clone());
        let back = document_to_json(&doc);
        assert_eq!(back, original);
    }
}
