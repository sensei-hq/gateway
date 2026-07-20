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
