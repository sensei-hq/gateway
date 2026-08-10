//! Tool runtime. Slice 2 executed ONLY Pure (deterministic, memoize-forever)
//! tools; slice 4 lifts that restriction — the executor now runs any effect
//! class (Pure/Observation/Mutation), with the two-phase/TTL wrapping owned
//! by the executor, not this runtime.

use std::collections::HashMap;
use std::sync::Arc;

use orchestrator_core::{EffectClass, OrchestratorError, ToolSpec};

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

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{EffectClass, OrchestratorError, ToolSpec};

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
}
