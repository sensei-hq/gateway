use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::capability::Capability;
use super::cost::{Cost, CostEstimate, TokenUsage};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceStatus {
    Success,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    Success,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attempt {
    pub sequence: u8,
    pub adapter: String,
    pub model: String,
    pub api_model_id: String,
    pub status: AttemptStatus,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokens: Option<TokenUsage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub fallback_triggered: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandidateInfo {
    pub model: String,
    pub router: String,
    pub priority: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkippedInfo {
    pub model: String,
    pub router: String,
    pub reason: String,
}

/// Why the candidates came out in the order they did (SP-ROUTE-1).
///
/// Recorded because the default strategy is a WEIGHTED DRAW: without the
/// weights and their inputs, "why did it pick the expensive one" has no answer
/// in a bug report, and a weighted router is otherwise unfalsifiable in
/// production — two identical requests may legitimately route differently, so
/// there is nothing to re-run and compare against.
///
/// Delivered on [`InferenceResponse::routing`], which is the artefact a caller
/// actually holds. Two gaps to know before going looking for it:
///
/// - **The streaming path carries none.** `Gateway::execute_stream` selects with
///   the full preferences — filtering and ordering both apply — but returns a
///   stream of `StreamEvent`s rather than an `InferenceResponse`, so it has
///   nowhere to put the explanation. A streamed request routes correctly and
///   cannot currently report why. Recorded as a known gap rather than papered
///   over.
/// - **[`ExecutionTrace::routing`] is forward provision.** See that field.
///
/// [`InferenceResponse::routing`]: super::request::InferenceResponse::routing
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RoutingDecision {
    /// Read from `RoutingStrategy::name()` — the strategy that actually ran,
    /// not a re-derivation from the request. One of `grouped_weighted`,
    /// `price`, `latency`, `throughput` (or `priority` for the retained
    /// baseline).
    pub strategy: String,
    /// Whether a metric sort found too few samples to reorder anything, so it
    /// degraded to priority order.
    ///
    /// The single most important thing this record can say out loud: a
    /// `sort: latency` that silently returns priority order looks, from the
    /// outside, exactly like a `sort: latency` that was ignored.
    pub degraded: bool,
    /// The candidates in the order the engine would try them — i.e. AFTER any
    /// `order` re-rank, not merely as the strategy left them.
    ///
    /// "Would", not "will": this is the order selection handed the walk, and
    /// the walk may end before the tail is reached. `allow_fallback: false`
    /// attempts only the first entry, and a terminal error (a 401, or exhausted
    /// credits) stops the walk where it stands. Read it as the ranking, not as
    /// a record of what was attempted — `attempts` is that record.
    pub order: Vec<RoutedCandidate>,
}

/// One candidate's place in a [`RoutingDecision`], and the inputs that put it
/// there.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RoutedCandidate {
    /// `"{router}:{model}"`.
    pub endpoint: String,
    pub priority: u8,
    /// Estimated cost; `None` for an unpriced (free) candidate.
    pub cost: Option<f64>,
    /// Windowed success rate. `None` when unmeasured — which is NOT the same as
    /// `Some(0.0)`, and conflating them is how a healthy fleet gets routed as
    /// though every provider were dead. `None` also when the strategy that ran
    /// does not consult reliability at all (every sort but the default).
    pub reliability: Option<f64>,
    /// The draw weight, when the weighted default ran.
    ///
    /// `None` when the candidate never entered a draw: no weighted strategy
    /// ran, or it was free (an unpriced candidate, or one so cheap that
    /// `1/cost²` overflows) and so leads its group outright. `Some(0.0)` is
    /// distinct and meaningful — a candidate with a real weight of zero, which
    /// can never be drawn and therefore goes last.
    pub weight: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionTrace {
    pub request_id: String,
    pub capability: Capability,
    pub status: TraceStatus,
    pub duration_ms: u64,
    pub candidates: Vec<CandidateInfo>,
    pub skipped: Vec<SkippedInfo>,
    pub attempts: Vec<Attempt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_cost: Option<CostEstimate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual_cost: Option<Cost>,
    /// Why the candidates came out in this order (SP-ROUTE-1 AC10).
    ///
    /// **FORWARD PROVISION, not a delivered surface.** The field exists and is
    /// guarded by persistence round-trip tests, but **nothing in this workspace
    /// builds an `ExecutionTrace` in production** — the type is constructed in
    /// exactly one test helper, and `GatewayStore::insert_execution_trace` has no
    /// production caller at all. Every field of this struct is equally unfilled.
    /// Do not write "the trace records the routing decision" without that
    /// qualifier, or a reader goes hunting for a writer that does not exist.
    /// [`InferenceResponse::routing`] is the delivered surface.
    ///
    /// `default` is belt-and-braces, not load-bearing: serde already resolves a
    /// missing field for an `Option<T>` to `None` via `missing_field`. Stated
    /// because the opposite claim is an easy one to make and would mislead the
    /// next person to add an optional field here.
    ///
    /// [`InferenceResponse::routing`]: super::request::InferenceResponse::routing
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing: Option<RoutingDecision>,
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attempt_serde_roundtrip() {
        let attempt = Attempt {
            sequence: 1,
            adapter: "anthropic".to_string(),
            model: "claude-sonnet".to_string(),
            api_model_id: "claude-3-5-sonnet-20241022".to_string(),
            status: AttemptStatus::Success,
            duration_ms: 1500,
            tokens: Some(TokenUsage {
                input_tokens: 100,
                output_tokens: 50,
                total_tokens: 150,
            }),
            cost: Some(0.003),
            error: None,
            fallback_triggered: false,
        };

        let json = serde_json::to_string(&attempt).unwrap();
        let deserialized: Attempt = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.sequence, 1);
        assert_eq!(deserialized.adapter, "anthropic");
        assert_eq!(deserialized.model, "claude-sonnet");
        assert_eq!(deserialized.status, AttemptStatus::Success);
        assert_eq!(deserialized.duration_ms, 1500);
        assert!(deserialized.tokens.is_some());
        assert!(!deserialized.fallback_triggered);
    }

    #[test]
    fn trace_status_serde() {
        let status = TraceStatus::Failed;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, r#""failed""#);

        let status = TraceStatus::Success;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, r#""success""#);
    }
}
