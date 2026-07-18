use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::types::capability::Capability;
use crate::types::error::GatewayError;
use crate::types::trace::ExecutionTrace;

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallStatus {
    Success,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceCall {
    pub id: Uuid,
    pub session_id: Option<Uuid>,
    pub project_id: Option<Uuid>,
    pub capability: Capability,
    pub chain_id: Option<String>,
    pub adapter: String,
    pub model: String,
    pub api_model_id: Option<String>,
    pub input_tokens: Option<u32>,
    pub output_tokens: Option<u32>,
    pub cost_usd: f64,
    pub duration_ms: u64,
    pub status: CallStatus,
    pub error_type: Option<String>,
    pub fallback_sequence: u8,
    pub recorded_at: DateTime<Utc>,
    /// Subject (team/tenant) the call is metered against — populated from the
    /// request's `AuthContext` (AUTH track). `None` when unauthenticated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_id: Option<Uuid>,
    /// Subscription tier label at call time, for attribution/reporting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTrace {
    pub id: Uuid,
    pub inference_call_id: Option<Uuid>,
    pub trace: ExecutionTrace,
    pub created_at: DateTime<Utc>,
}

/// A subject's aggregated usage over a window, in the units the AUTH quota
/// enforcer checks. Dollars are integer milli-USD (`cost_usd × 1000`, rounded)
/// so quota counters stay integer while the f64 `Cost` USD path is untouched.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageTotals {
    pub requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    pub cost_usd_millis: u64,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

#[async_trait]
pub trait GatewayStore: Send + Sync {
    async fn insert_inference_call(&self, call: &InferenceCall) -> Result<Uuid, GatewayError>;
    async fn get_inference_calls_by_session(
        &self,
        session_id: Uuid,
    ) -> Result<Vec<InferenceCall>, GatewayError>;
    async fn get_spend_since(&self, since: DateTime<Utc>) -> Result<f64, GatewayError>;
    async fn get_spend_by_model_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<(String, f64)>, GatewayError>;

    /// Aggregate a subject's usage since `since` (a rolling-window start), for
    /// AUTH quota enforcement. Counts only calls attributed to `subject_id`
    /// (`InferenceCall::subject_id`); an unknown subject yields zeroes.
    async fn get_usage_since(
        &self,
        subject_id: Uuid,
        since: DateTime<Utc>,
    ) -> Result<UsageTotals, GatewayError>;

    async fn insert_execution_trace(&self, trace: &StoredTrace) -> Result<Uuid, GatewayError>;
    async fn get_execution_trace(&self, id: Uuid) -> Result<Option<StoredTrace>, GatewayError>;
    async fn get_traces_by_call(
        &self,
        inference_call_id: Uuid,
    ) -> Result<Vec<StoredTrace>, GatewayError>;
}

// ---------------------------------------------------------------------------
// In-memory implementation (for testing)
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct InMemoryStore {
    calls: Mutex<Vec<InferenceCall>>,
    traces: Mutex<Vec<StoredTrace>>,
}

#[async_trait]
impl GatewayStore for InMemoryStore {
    async fn insert_inference_call(&self, call: &InferenceCall) -> Result<Uuid, GatewayError> {
        let id = call.id;
        self.calls.lock().unwrap().push(call.clone());
        Ok(id)
    }

    async fn get_inference_calls_by_session(
        &self,
        session_id: Uuid,
    ) -> Result<Vec<InferenceCall>, GatewayError> {
        let calls = self.calls.lock().unwrap();
        Ok(calls
            .iter()
            .filter(|c| c.session_id == Some(session_id))
            .cloned()
            .collect())
    }

    async fn get_spend_since(&self, since: DateTime<Utc>) -> Result<f64, GatewayError> {
        let calls = self.calls.lock().unwrap();
        Ok(calls
            .iter()
            .filter(|c| c.recorded_at >= since)
            .map(|c| c.cost_usd)
            .sum())
    }

    async fn get_spend_by_model_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<(String, f64)>, GatewayError> {
        let calls = self.calls.lock().unwrap();
        let mut map: HashMap<String, f64> = HashMap::new();
        for call in calls.iter().filter(|c| c.recorded_at >= since) {
            *map.entry(call.model.clone()).or_default() += call.cost_usd;
        }
        let mut result: Vec<(String, f64)> = map.into_iter().collect();
        result.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(result)
    }

    async fn get_usage_since(
        &self,
        subject_id: Uuid,
        since: DateTime<Utc>,
    ) -> Result<UsageTotals, GatewayError> {
        let calls = self.calls.lock().unwrap();
        let mut totals = UsageTotals::default();
        for c in calls
            .iter()
            .filter(|c| c.subject_id == Some(subject_id) && c.recorded_at >= since)
        {
            let input = c.input_tokens.unwrap_or(0) as u64;
            let output = c.output_tokens.unwrap_or(0) as u64;
            totals.requests += 1;
            totals.input_tokens += input;
            totals.output_tokens += output;
            totals.total_tokens += input + output;
            totals.cost_usd_millis += (c.cost_usd.max(0.0) * 1000.0).round() as u64;
        }
        Ok(totals)
    }

    async fn insert_execution_trace(&self, trace: &StoredTrace) -> Result<Uuid, GatewayError> {
        let id = trace.id;
        self.traces.lock().unwrap().push(trace.clone());
        Ok(id)
    }

    async fn get_execution_trace(&self, id: Uuid) -> Result<Option<StoredTrace>, GatewayError> {
        let traces = self.traces.lock().unwrap();
        Ok(traces.iter().find(|t| t.id == id).cloned())
    }

    async fn get_traces_by_call(
        &self,
        inference_call_id: Uuid,
    ) -> Result<Vec<StoredTrace>, GatewayError> {
        let traces = self.traces.lock().unwrap();
        Ok(traces
            .iter()
            .filter(|t| t.inference_call_id == Some(inference_call_id))
            .cloned()
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use super::*;
    use crate::types::cost::CostEstimate;
    use crate::types::trace::{Attempt, AttemptStatus, CandidateInfo, SkippedInfo, TraceStatus};

    fn make_call(
        session_id: Option<Uuid>,
        model: &str,
        cost: f64,
        recorded_at: DateTime<Utc>,
    ) -> InferenceCall {
        InferenceCall {
            id: Uuid::new_v4(),
            session_id,
            project_id: None,
            capability: Capability::TextChat,
            chain_id: None,
            adapter: "anthropic".to_string(),
            model: model.to_string(),
            api_model_id: Some("claude-3-5-sonnet-20241022".to_string()),
            input_tokens: Some(100),
            output_tokens: Some(50),
            cost_usd: cost,
            duration_ms: 1200,
            status: CallStatus::Success,
            error_type: None,
            fallback_sequence: 0,
            recorded_at,
            subject_id: None,
            tier: None,
        }
    }

    fn make_trace(call_id: Option<Uuid>) -> StoredTrace {
        StoredTrace {
            id: Uuid::new_v4(),
            inference_call_id: call_id,
            trace: ExecutionTrace {
                request_id: Uuid::new_v4().to_string(),
                capability: Capability::TextChat,
                status: TraceStatus::Success,
                duration_ms: 1500,
                candidates: vec![CandidateInfo {
                    model: "claude-sonnet".to_string(),
                    router: "priority".to_string(),
                    priority: 1,
                }],
                skipped: vec![SkippedInfo {
                    model: "gpt-4o".to_string(),
                    router: "priority".to_string(),
                    reason: "circuit breaker open".to_string(),
                }],
                attempts: vec![Attempt {
                    sequence: 0,
                    adapter: "anthropic".to_string(),
                    model: "claude-sonnet".to_string(),
                    api_model_id: "claude-3-5-sonnet-20241022".to_string(),
                    status: AttemptStatus::Success,
                    duration_ms: 1400,
                    tokens: None,
                    cost: Some(0.003),
                    error: None,
                    fallback_triggered: false,
                }],
                estimated_cost: Some(CostEstimate {
                    estimated: 0.003,
                    minimum: 0.001,
                    maximum: 0.01,
                    currency: "USD".to_string(),
                    model: "claude-sonnet".to_string(),
                }),
                actual_cost: None,
                created_at: Utc::now(),
            },
            created_at: Utc::now(),
        }
    }

    // 1. InferenceCall serde roundtrip
    #[test]
    fn inference_call_serde_roundtrip() {
        let call = make_call(Some(Uuid::new_v4()), "claude-sonnet", 0.003, Utc::now());
        let json = serde_json::to_string(&call).unwrap();
        let deserialized: InferenceCall = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.id, call.id);
        assert_eq!(deserialized.session_id, call.session_id);
        assert_eq!(deserialized.model, "claude-sonnet");
        assert!((deserialized.cost_usd - 0.003).abs() < f64::EPSILON);
        assert_eq!(deserialized.status, CallStatus::Success);
    }

    // 2. StoredTrace serde roundtrip
    #[test]
    fn stored_trace_serde_roundtrip() {
        let trace = make_trace(Some(Uuid::new_v4()));
        let json = serde_json::to_string(&trace).unwrap();
        let deserialized: StoredTrace = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.id, trace.id);
        assert_eq!(deserialized.inference_call_id, trace.inference_call_id);
        assert_eq!(deserialized.trace.request_id, trace.trace.request_id);
        assert_eq!(deserialized.trace.candidates.len(), 1);
        assert_eq!(deserialized.trace.attempts.len(), 1);
    }

    // 3. CallStatus serde
    #[test]
    fn call_status_serde() {
        let success = CallStatus::Success;
        let json = serde_json::to_string(&success).unwrap();
        assert_eq!(json, r#""success""#);

        let failed = CallStatus::Failed;
        let json = serde_json::to_string(&failed).unwrap();
        assert_eq!(json, r#""failed""#);

        let roundtrip: CallStatus = serde_json::from_str(r#""success""#).unwrap();
        assert_eq!(roundtrip, CallStatus::Success);

        let roundtrip: CallStatus = serde_json::from_str(r#""failed""#).unwrap();
        assert_eq!(roundtrip, CallStatus::Failed);
    }

    // 4. Insert and get call by session
    #[tokio::test]
    async fn in_memory_insert_and_get_call() {
        let store = InMemoryStore::default();
        let session_id = Uuid::new_v4();
        let call = make_call(Some(session_id), "claude-sonnet", 0.003, Utc::now());
        let expected_id = call.id;

        let id = store.insert_inference_call(&call).await.unwrap();
        assert_eq!(id, expected_id);

        let calls = store
            .get_inference_calls_by_session(session_id)
            .await
            .unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, expected_id);
        assert_eq!(calls[0].model, "claude-sonnet");
    }

    // 5. Unknown session returns empty vec
    #[tokio::test]
    async fn in_memory_get_calls_empty_session() {
        let store = InMemoryStore::default();
        let session_id = Uuid::new_v4();

        // Insert a call for a different session
        let other_session = Uuid::new_v4();
        let call = make_call(Some(other_session), "claude-sonnet", 0.003, Utc::now());
        store.insert_inference_call(&call).await.unwrap();

        let calls = store
            .get_inference_calls_by_session(session_id)
            .await
            .unwrap();
        assert!(calls.is_empty());
    }

    // 6. Spend since cutoff
    #[tokio::test]
    async fn in_memory_spend_since() {
        let store = InMemoryStore::default();
        let now = Utc::now();
        let one_hour_ago = now - Duration::hours(1);
        let two_hours_ago = now - Duration::hours(2);

        // Call from 2 hours ago (should be excluded when querying from 90 min ago)
        let call1 = make_call(None, "claude-sonnet", 0.01, two_hours_ago);
        // Call from 1 hour ago (included)
        let call2 = make_call(None, "claude-sonnet", 0.02, one_hour_ago);
        // Call from now (included)
        let call3 = make_call(None, "claude-sonnet", 0.03, now);

        store.insert_inference_call(&call1).await.unwrap();
        store.insert_inference_call(&call2).await.unwrap();
        store.insert_inference_call(&call3).await.unwrap();

        let cutoff = now - Duration::minutes(90);
        let spend = store.get_spend_since(cutoff).await.unwrap();
        assert!((spend - 0.05).abs() < f64::EPSILON);

        // All-time spend
        let ancient = now - Duration::days(365);
        let total = store.get_spend_since(ancient).await.unwrap();
        assert!((total - 0.06).abs() < f64::EPSILON);
    }

    // 7. Spend by model
    #[tokio::test]
    async fn in_memory_spend_by_model() {
        let store = InMemoryStore::default();
        let now = Utc::now();

        let call1 = make_call(None, "claude-sonnet", 0.01, now);
        let call2 = make_call(None, "claude-sonnet", 0.02, now);
        let call3 = make_call(None, "gpt-4o", 0.05, now);

        store.insert_inference_call(&call1).await.unwrap();
        store.insert_inference_call(&call2).await.unwrap();
        store.insert_inference_call(&call3).await.unwrap();

        let cutoff = now - Duration::hours(1);
        let by_model = store.get_spend_by_model_since(cutoff).await.unwrap();

        assert_eq!(by_model.len(), 2);
        // Sorted alphabetically by model name
        let sonnet = by_model.iter().find(|(m, _)| m == "claude-sonnet").unwrap();
        let gpt = by_model.iter().find(|(m, _)| m == "gpt-4o").unwrap();

        assert!((sonnet.1 - 0.03).abs() < f64::EPSILON);
        assert!((gpt.1 - 0.05).abs() < f64::EPSILON);
    }

    // 7b. Usage aggregation is scoped to the subject and the window.
    #[tokio::test]
    async fn in_memory_usage_since_filters_by_subject_and_window() {
        let store = InMemoryStore::default();
        let subject = Uuid::new_v4();
        let now = Utc::now();

        // make_call records 100 input + 50 output tokens each.
        let mut c1 = make_call(None, "m", 0.01, now);
        c1.subject_id = Some(subject);
        let mut c2 = make_call(None, "m", 0.02, now);
        c2.subject_id = Some(subject);
        // Different subject — excluded.
        let mut c3 = make_call(None, "m", 0.05, now);
        c3.subject_id = Some(Uuid::new_v4());
        // Same subject but before the window — excluded.
        let mut c4 = make_call(None, "m", 0.04, now - Duration::hours(2));
        c4.subject_id = Some(subject);

        for c in [&c1, &c2, &c3, &c4] {
            store.insert_inference_call(c).await.unwrap();
        }

        let usage = store
            .get_usage_since(subject, now - Duration::hours(1))
            .await
            .unwrap();
        assert_eq!(usage.requests, 2);
        assert_eq!(usage.input_tokens, 200);
        assert_eq!(usage.output_tokens, 100);
        assert_eq!(usage.total_tokens, 300);
        assert_eq!(usage.cost_usd_millis, 30); // round(0.01*1000)+round(0.02*1000)
    }

    // 7c. Unknown subject aggregates to zero.
    #[tokio::test]
    async fn in_memory_usage_since_unknown_subject_is_zero() {
        let store = InMemoryStore::default();
        let usage = store
            .get_usage_since(Uuid::new_v4(), Utc::now() - Duration::hours(1))
            .await
            .unwrap();
        assert_eq!(usage, UsageTotals::default());
    }

    // 8. Insert and get trace
    #[tokio::test]
    async fn in_memory_insert_and_get_trace() {
        let store = InMemoryStore::default();
        let trace = make_trace(Some(Uuid::new_v4()));
        let expected_id = trace.id;

        let id = store.insert_execution_trace(&trace).await.unwrap();
        assert_eq!(id, expected_id);

        let retrieved = store.get_execution_trace(expected_id).await.unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.id, expected_id);
        assert_eq!(retrieved.trace.request_id, trace.trace.request_id);
    }

    // 9. Get trace not found
    #[tokio::test]
    async fn in_memory_get_trace_not_found() {
        let store = InMemoryStore::default();
        let result = store.get_execution_trace(Uuid::new_v4()).await.unwrap();
        assert!(result.is_none());
    }

    // 10. Traces by call
    #[tokio::test]
    async fn in_memory_traces_by_call() {
        let store = InMemoryStore::default();
        let call_id = Uuid::new_v4();

        let trace1 = make_trace(Some(call_id));
        let trace2 = make_trace(Some(call_id));
        let trace3 = make_trace(Some(Uuid::new_v4())); // different call

        store.insert_execution_trace(&trace1).await.unwrap();
        store.insert_execution_trace(&trace2).await.unwrap();
        store.insert_execution_trace(&trace3).await.unwrap();

        let traces = store.get_traces_by_call(call_id).await.unwrap();
        assert_eq!(traces.len(), 2);
        assert!(traces.iter().all(|t| t.inference_call_id == Some(call_id)));
    }
}
