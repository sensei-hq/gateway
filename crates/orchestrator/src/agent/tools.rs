//! Pure tool runtime. Slice 2 executes ONLY Pure (deterministic, memoize-forever)
//! tools in the orchestrator; Observation/Mutation are rejected loud (slice 4).

use std::collections::HashMap;
use std::sync::Arc;

use orchestrator_core::{EffectClass, OrchestratorError, ToolSpec};

/// An executable tool. `spec().effect_class` MUST be `Pure` in slice 2.
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

    /// Execute a Pure tool by name. Unknown → loud; non-Pure → `ToolEffectDeferred`
    /// (an honest slice-4 boundary, never a silent skip); a tool error is surfaced.
    pub fn execute(
        &self,
        name: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, OrchestratorError> {
        let tool = self
            .tools
            .get(name)
            .ok_or_else(|| OrchestratorError::UnknownTool(name.to_string()))?;
        let class = tool.spec().effect_class;
        if class != EffectClass::Pure {
            return Err(OrchestratorError::ToolEffectDeferred {
                tool: name.to_string(),
                class,
            });
        }
        tool.call(args)
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

    struct Reader;
    impl Tool for Reader {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "read".into(),
                description: None,
                input_schema: serde_json::json!({}),
                effect_class: EffectClass::Observation,
            }
        }
        fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
            Ok(serde_json::json!({}))
        }
    }

    #[test]
    fn non_pure_tool_is_rejected_before_execution() {
        let reg = ToolRegistry::default().with_tool(std::sync::Arc::new(Reader));
        assert!(matches!(
            reg.execute("read", serde_json::json!({})),
            Err(OrchestratorError::ToolEffectDeferred { .. })
        ));
    }
}
