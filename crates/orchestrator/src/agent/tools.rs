//! Tool runtime. Slice 2 executed ONLY Pure (deterministic, memoize-forever)
//! tools; slice 4 lifts that restriction — the executor now runs any effect
//! class (Pure/Observation/Mutation), with the two-phase/TTL wrapping owned
//! by the executor, not this runtime.

use std::collections::HashMap;
use std::sync::Arc;

use orchestrator_core::{Activation, EffectClass, OrchestratorError, Permissions, ToolSpec};

/// An executable tool. `spec().effect_class` may be `Pure`, `Observation`, or
/// `Mutation` (slice 4) — the executor is responsible for wrapping
/// Observation/Mutation calls with TTL checks and two-phase intent/reconcile.
pub trait Tool: Send + Sync {
    fn spec(&self) -> ToolSpec;
    fn call(&self, args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError>;
}

/// Name→executor map. Prompt schemas come from the core `Registry`'s `ToolSpec`s;
/// this holds the executable side.
#[derive(Default, Clone)]
pub struct ToolRegistry {
    tools: HashMap<String, Arc<dyn Tool>>,
}

impl ToolRegistry {
    pub fn with_tool(mut self, tool: Arc<dyn Tool>) -> Self {
        self.tools.insert(tool.spec().name, tool);
        self
    }

    /// Execute a tool by name, any effect class (slice 4). Unknown → loud; a
    /// tool error is surfaced.
    pub fn execute(
        &self,
        name: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, OrchestratorError> {
        let tool = self
            .tools
            .get(name)
            .ok_or_else(|| OrchestratorError::UnknownTool(name.to_string()))?;
        tool.call(args)
    }

    /// The spec of a registered tool by name (for the executor to read its class/ttl).
    pub fn spec_of(&self, name: &str) -> Option<orchestrator_core::ToolSpec> {
        self.tools.get(name).map(|t| t.spec())
    }
}

/// Name → reconcile provider, queried when a Mutation is in-doubt on resume.
#[derive(Default, Clone)]
pub struct ReconcileRegistry {
    providers:
        std::collections::HashMap<String, std::sync::Arc<dyn orchestrator_core::ReconcileProvider>>,
}
impl ReconcileRegistry {
    pub fn with_provider(
        mut self,
        name: impl Into<String>,
        p: std::sync::Arc<dyn orchestrator_core::ReconcileProvider>,
    ) -> Self {
        self.providers.insert(name.into(), p);
        self
    }
    pub fn get(
        &self,
        name: &str,
    ) -> Option<&std::sync::Arc<dyn orchestrator_core::ReconcileProvider>> {
        self.providers.get(name)
    }
}

/// Demo Pure tool: deterministic arithmetic `{op: add|mul, a, b} → {result}`.
pub struct Calc;

impl Tool for Calc {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "calc".into(),
            description: Some("Deterministic arithmetic over two numbers".into()),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": { "op": {"type":"string"}, "a": {"type":"number"}, "b": {"type":"number"} },
                "required": ["op","a","b"]
            }),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
            permissions: Permissions::default(),
            activation: Activation::default(),
        }
    }

    fn call(&self, args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        let err = |m: &str| OrchestratorError::Tool {
            tool: "calc".into(),
            message: m.into(),
        };
        let a = args
            .get("a")
            .and_then(|v| v.as_f64())
            .ok_or_else(|| err("missing number 'a'"))?;
        let b = args
            .get("b")
            .and_then(|v| v.as_f64())
            .ok_or_else(|| err("missing number 'b'"))?;
        let result = match args.get("op").and_then(|v| v.as_str()) {
            Some("add") => a + b,
            Some("mul") => a * b,
            other => return Err(err(&format!("unknown op: {other:?}"))),
        };
        Ok(serde_json::json!({ "result": result }))
    }
}

/// Demo Observation tool: canned, deterministic "search" results keyed on the
/// query string. Memoized by the executor for `ttl_secs`; the shared counter
/// lets tests (and later acceptance tasks) observe how many times the real
/// call actually ran vs. was served from memo.
pub struct Search {
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl Search {
    pub fn new(counter: Arc<std::sync::atomic::AtomicUsize>) -> Self {
        Self { calls: counter }
    }
}

impl Tool for Search {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "search".into(),
            description: Some("Canned, deterministic search results for a query".into()),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": { "query": {"type": "string"} },
                "required": ["query"]
            }),
            effect_class: EffectClass::Observation,
            ttl_secs: Some(60),
            source: Some("search".into()),
            permissions: Permissions::default(),
            activation: Activation::default(),
        }
    }

    fn call(&self, args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        let q =
            args.get("query")
                .and_then(|v| v.as_str())
                .ok_or_else(|| OrchestratorError::Tool {
                    tool: "search".into(),
                    message: "missing 'query'".into(),
                })?;
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(serde_json::json!({
            "query": q,
            "results": [format!("result 1 for {q}"), format!("result 2 for {q}")]
        }))
    }
}

/// Demo Mutation tool: appends a note to a shared sink (the "world" being
/// mutated). The executor journals intent/record around this call (§7.1);
/// `NoteReconciler` below is the paired reconcile provider for resume.
pub struct RecordNote {
    sink: Arc<std::sync::Mutex<Vec<String>>>,
}

impl RecordNote {
    pub fn new(sink: Arc<std::sync::Mutex<Vec<String>>>) -> Self {
        Self { sink }
    }
}

impl Tool for RecordNote {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "record_note".into(),
            description: Some("Append a note to the shared notes sink".into()),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": { "note": {"type": "string"} },
                "required": ["note"]
            }),
            effect_class: EffectClass::Mutation,
            ttl_secs: None,
            source: None,
            permissions: Permissions::default(),
            activation: Activation::default(),
        }
    }

    fn call(&self, args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        let note =
            args.get("note")
                .and_then(|v| v.as_str())
                .ok_or_else(|| OrchestratorError::Tool {
                    tool: "record_note".into(),
                    message: "missing 'note'".into(),
                })?;
        self.sink.lock().unwrap().push(note.to_string());
        Ok(serde_json::json!({ "recorded": note }))
    }
}

/// Reconcile provider for `record_note`: on resume, an in-doubt Mutation is
/// confirmed by checking whether the note already made it into the sink —
/// never re-run and never guess.
pub struct NoteReconciler {
    sink: Arc<std::sync::Mutex<Vec<String>>>,
}

impl NoteReconciler {
    pub fn new(sink: Arc<std::sync::Mutex<Vec<String>>>) -> Self {
        Self { sink }
    }
}

#[async_trait::async_trait]
impl orchestrator_core::ReconcileProvider for NoteReconciler {
    async fn reconcile(
        &self,
        _idempotency_key: &str,
        args: &serde_json::Value,
    ) -> Result<orchestrator_core::ReconcileOutcome, OrchestratorError> {
        let note =
            args.get("note")
                .and_then(|v| v.as_str())
                .ok_or_else(|| OrchestratorError::Tool {
                    tool: "record_note".into(),
                    message: "missing 'note'".into(),
                })?;
        let sink = self.sink.lock().unwrap();
        if sink.iter().any(|n| n == note) {
            Ok(orchestrator_core::ReconcileOutcome::Confirmed(
                serde_json::json!({ "recorded": note }),
            ))
        } else {
            Ok(orchestrator_core::ReconcileOutcome::NotApplied)
        }
    }
}

/// Test reconcile provider that can never determine the world's state — the
/// executor must pause loud rather than guess. Used to exercise the
/// `in_doubt → pause` path.
pub struct AlwaysIndeterminate;

#[async_trait::async_trait]
impl orchestrator_core::ReconcileProvider for AlwaysIndeterminate {
    async fn reconcile(
        &self,
        _idempotency_key: &str,
        _args: &serde_json::Value,
    ) -> Result<orchestrator_core::ReconcileOutcome, OrchestratorError> {
        Ok(orchestrator_core::ReconcileOutcome::Indeterminate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{
        EffectClass, OrchestratorError, ReconcileOutcome, ReconcileProvider, ToolSpec,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[test]
    fn calc_adds_two_numbers() {
        let out = Calc
            .call(serde_json::json!({"op":"add","a":2,"b":3}))
            .expect("calc runs");
        assert_eq!(out, serde_json::json!({"result": 5.0}));
    }

    #[test]
    fn registry_executes_a_pure_tool_by_name() {
        let reg = ToolRegistry::default().with_tool(std::sync::Arc::new(Calc));
        let out = reg
            .execute("calc", serde_json::json!({"op":"mul","a":4,"b":5}))
            .expect("executes");
        assert_eq!(out, serde_json::json!({"result": 20.0}));
    }

    #[test]
    fn unknown_tool_is_a_loud_error() {
        let reg = ToolRegistry::default();
        assert!(matches!(
            reg.execute("nope", serde_json::json!({})),
            Err(OrchestratorError::UnknownTool(_))
        ));
    }

    #[test]
    fn tool_registry_executes_a_non_pure_tool() {
        struct Obs;
        impl Tool for Obs {
            fn spec(&self) -> ToolSpec {
                ToolSpec {
                    name: "obs".into(),
                    description: None,
                    input_schema: serde_json::json!({}),
                    effect_class: EffectClass::Observation,
                    ttl_secs: Some(60),
                    source: None,
                    permissions: Permissions::default(),
                    activation: Activation::default(),
                }
            }
            fn call(&self, _a: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
                Ok(serde_json::json!({"ok": true}))
            }
        }
        let reg = ToolRegistry::default().with_tool(std::sync::Arc::new(Obs));
        assert_eq!(
            reg.execute("obs", serde_json::json!({})).unwrap(),
            serde_json::json!({"ok": true})
        );
    }

    #[test]
    fn search_increments_counter_and_returns_query_results() {
        let counter = Arc::new(AtomicUsize::new(0));
        let search = Search::new(counter.clone());

        let out = search
            .call(serde_json::json!({"query": "rust orchestrators"}))
            .expect("search runs");

        assert_eq!(counter.load(Ordering::SeqCst), 1);
        assert_eq!(
            out,
            serde_json::json!({
                "query": "rust orchestrators",
                "results": [
                    "result 1 for rust orchestrators",
                    "result 2 for rust orchestrators"
                ]
            })
        );

        search
            .call(serde_json::json!({"query": "again"}))
            .expect("search runs again");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn record_note_appends_to_sink_and_returns_recorded() {
        let sink = Arc::new(Mutex::new(Vec::new()));
        let record_note = RecordNote::new(sink.clone());

        let out = record_note
            .call(serde_json::json!({"note": "buy milk"}))
            .expect("record_note runs");

        assert_eq!(out, serde_json::json!({"recorded": "buy milk"}));
        assert_eq!(*sink.lock().unwrap(), vec!["buy milk".to_string()]);
    }

    #[tokio::test]
    async fn note_reconciler_confirms_when_note_already_in_sink() {
        let sink = Arc::new(Mutex::new(vec!["already recorded".to_string()]));
        let reconciler = NoteReconciler::new(sink);

        let outcome = reconciler
            .reconcile("some-key", &serde_json::json!({"note": "already recorded"}))
            .await
            .expect("reconcile runs");

        assert_eq!(
            outcome,
            ReconcileOutcome::Confirmed(serde_json::json!({"recorded": "already recorded"}))
        );
    }

    #[tokio::test]
    async fn note_reconciler_not_applied_when_note_absent_from_sink() {
        let sink = Arc::new(Mutex::new(Vec::new()));
        let reconciler = NoteReconciler::new(sink);

        let outcome = reconciler
            .reconcile("some-key", &serde_json::json!({"note": "missing"}))
            .await
            .expect("reconcile runs");

        assert_eq!(outcome, ReconcileOutcome::NotApplied);
    }

    #[tokio::test]
    async fn always_indeterminate_returns_indeterminate() {
        let outcome = AlwaysIndeterminate
            .reconcile("some-key", &serde_json::json!({}))
            .await
            .expect("reconcile runs");

        assert_eq!(outcome, ReconcileOutcome::Indeterminate);
    }
}
