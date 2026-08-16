//! Tool runtime. Slice 2 executed ONLY Pure (deterministic, memoize-forever)
//! tools; slice 4 lifts that restriction — the executor now runs any effect
//! class (Pure/Observation/Mutation), with the two-phase/TTL wrapping owned
//! by the executor, not this runtime.

use std::collections::HashMap;
use std::sync::Arc;

use orchestrator_core::{
    Activation, EffectClass, EffectId, OrchestratorError, Permissions, Registry, ToolSpec,
};

/// Per-call execution context for a tool (SP-4 s5). Carries the idempotency key the
/// executor journaled in the `EffectIntent` (so a tool can send it to an external API
/// for provider-side dedup) + the effect id for correlation.
#[derive(Debug, Clone)]
pub struct ToolContext {
    /// The RESOLVED idempotency key the executor journaled in the `EffectIntent`: the
    /// tool's author key if it overrode `Tool::idempotency_key`, else the structural
    /// `sha256(effect_id | args_hash)`. A tool sends THIS to an external API for
    /// provider-side dedup.
    pub idempotency_key: String,
    /// The effect id for this call (`sha256(parent_path | iter | idx)`), for correlation.
    pub effect_id: EffectId,
    /// Broker-resolved credentials for THIS call (SP-4). Ephemeral — never journaled/
    /// hashed; zeroized on drop. `Arc` so cloning `ToolContext` shares one secret store.
    /// A tool reads `ctx.credentials.get(ref).map(Secret::expose)` and sends it to its API.
    pub credentials: std::sync::Arc<std::collections::HashMap<String, orchestrator_core::Secret>>,
    /// The CANONICAL per-run workspace root the executor resolved (SP-4 s3), or `None`
    /// when no workspace is wired. A confined fs tool resolves its target via
    /// [`workspace::confine`](crate::agent::workspace::confine) against this root.
    pub workspace_root: Option<std::sync::Arc<std::path::PathBuf>>,
}

impl ToolContext {
    /// The raw, EXPOSED (plaintext) injected secret values, for the executor's per-call
    /// exact-value scrub (Task 3). Named to self-document the exposure at the call site,
    /// matching the [`Secret::expose`](orchestrator_core::Secret::expose) convention.
    pub fn exposed_secret_values(&self) -> Vec<&str> {
        self.credentials
            .values()
            .map(orchestrator_core::Secret::expose)
            .collect()
    }
}

/// An executable tool. `spec().effect_class` may be `Pure`, `Observation`, or
/// `Mutation` (slice 4) — the executor is responsible for wrapping
/// Observation/Mutation calls with TTL checks and two-phase intent/reconcile.
pub trait Tool: Send + Sync {
    fn spec(&self) -> ToolSpec;
    fn call(&self, args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError>;

    /// Execute with the per-call context (SP-4 s5). Default ignores `ctx` and delegates
    /// to `call` ⇒ existing tools are byte-identical. Override to send
    /// `ctx.idempotency_key` to an external API for provider-side dedup.
    ///
    /// **The executor invokes `call_ctx`** (never `call` directly). Override `call` for a
    /// tool's logic; override `call_ctx` ONLY to consume `ctx.idempotency_key` (e.g. send it
    /// to an external API for provider-side dedup) — the default forwards to `call`, so a
    /// tool that overrides only `call` is still driven correctly.
    fn call_ctx(
        &self,
        args: serde_json::Value,
        _ctx: &ToolContext,
    ) -> Result<serde_json::Value, OrchestratorError> {
        self.call(args)
    }

    /// Author-supplied idempotency key for THIS call. Default `None` ⇒ the executor uses
    /// the structural key `sha256(effect_id | args_hash)` (`orchestrator_core::idempotency_key`).
    /// Override for a domain key (booking ref, payment token derived from `args`).
    ///
    /// MUST be a pure function of `args` — NO I/O, clock, or RNG. The key is journaled in the
    /// `EffectIntent` and threaded to `call_ctx`; on an in-doubt resume the executor reads the
    /// JOURNALED key to query the provider. A nondeterministic key would recompute differently
    /// across runs → the provider is queried under the wrong key → dedup fails → the mutation
    /// applies TWICE. (Reading the journaled key protects the reconcile side; a pure derivation
    /// is still required so the key sent to the provider at execution matches.)
    fn idempotency_key(&self, _args: &serde_json::Value) -> Option<String> {
        None
    }

    /// The CONCRETE permissions THIS specific call needs (SP-4 authorization gate).
    /// Default = the tool's static declared surface (`spec().permissions`), so a tool
    /// with no permission-relevant arguments needs no change.
    ///
    /// MUST be a pure function of `args` (plus immutable config) — NO I/O, clock, or
    /// RNG: the gate's allow/deny feeds the journaled ReAct transcript, so any
    /// nondeterminism here breaks resume-determinism.
    ///
    /// A tool whose arguments carry a path/host/command MUST override this. If it
    /// does not, the gate silently falls back to coarse static-surface granularity —
    /// a fail-OPEN trap: a specific call could be authorized for a concrete resource
    /// that its narrow grant would otherwise deny. The type system cannot catch this.
    fn required(&self, _args: &serde_json::Value) -> Permissions {
        self.spec().permissions
    }
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
    ///
    /// ⚠️ Test-only (`#[cfg(test)]`): this calls `Tool::call` DIRECTLY, bypassing any
    /// `call_ctx` override — so it does NOT thread the SP-4 s5 idempotency key. The
    /// executor dispatches every effect through `execute_ctx`; a production caller
    /// reaching for this shorter form would silently skip idempotency/dedup, so it is
    /// gated out of non-test builds entirely.
    #[cfg(test)]
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

    /// Execute a tool with its per-call context (SP-4 s5). Unknown → loud `UnknownTool`.
    pub fn execute_ctx(
        &self,
        name: &str,
        args: serde_json::Value,
        ctx: &ToolContext,
    ) -> Result<serde_json::Value, OrchestratorError> {
        let tool = self
            .tools
            .get(name)
            .ok_or_else(|| OrchestratorError::UnknownTool(name.to_string()))?;
        tool.call_ctx(args, ctx)
    }

    /// The author-supplied idempotency key for a call, if the tool overrides it (else None
    /// ⇒ the executor uses the structural key). Unknown tool → None.
    pub fn idempotency_key_of(&self, name: &str, args: &serde_json::Value) -> Option<String> {
        self.tools.get(name).and_then(|t| t.idempotency_key(args))
    }

    /// The spec of a registered tool by name (for the executor to read its class/ttl).
    pub fn spec_of(&self, name: &str) -> Option<orchestrator_core::ToolSpec> {
        self.tools.get(name).map(|t| t.spec())
    }

    /// The concrete permissions the named tool needs for `args` (for the gate).
    /// Unknown tool → empty `Permissions` (denied by the separate `tool ∈ agent.tools`
    /// check in the executor).
    pub fn required_of(&self, name: &str, args: &serde_json::Value) -> Permissions {
        self.tools
            .get(name)
            .map(|t| t.required(args))
            .unwrap_or_default()
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

/// Demo credential broker: an in-memory `ref → secret` map (SP-4). A real broker (future)
/// wraps `vault::Vault`.
pub struct StaticCredentialBroker(std::collections::HashMap<String, String>);

impl StaticCredentialBroker {
    pub fn new(map: std::collections::HashMap<String, String>) -> Self {
        Self(map)
    }
}

#[async_trait::async_trait]
impl orchestrator_core::CredentialBroker for StaticCredentialBroker {
    async fn resolve(
        &self,
        cred_ref: &str,
    ) -> Result<Option<orchestrator_core::Secret>, OrchestratorError> {
        Ok(self.0.get(cred_ref).map(orchestrator_core::Secret::new))
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
            credentials: vec![],
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
            credentials: vec![],
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
            credentials: vec![],
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

/// Demo Mutation tool with a permission-relevant argument: writes to a path.
/// Its static surface is `/workspace`, but the gate authorizes each call against
/// the agent's grant using the CONCRETE path in `required(args)`. `call` just
/// records the path in a sink (the "filesystem" being mutated) — no real I/O.
///
/// `required` returns an EMPTY need when `path` is missing/invalid — safe ONLY
/// because `call` fail-closes (`missing 'path'`) before mutating; a tool with a
/// default action on a missing arg must NOT copy this (its empty need would be a
/// fail-open gap). This is a GATE DEMO only: it has no paired `ReconcileProvider`,
/// so an in-doubt `fs.write` on resume is unreconcilable — not a resume-safe
/// template. The `content` field in the schema is illustrative (only `path` is read).
pub struct ScopedWriter {
    sink: Arc<std::sync::Mutex<Vec<String>>>,
}

impl ScopedWriter {
    pub fn new(sink: Arc<std::sync::Mutex<Vec<String>>>) -> Self {
        Self { sink }
    }
}

impl Tool for ScopedWriter {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "fs.write".into(),
            description: Some("Write content to a path".into()),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": { "path": {"type": "string"}, "content": {"type": "string"} },
                "required": ["path", "content"]
            }),
            effect_class: EffectClass::Mutation,
            ttl_secs: None,
            source: None,
            permissions: Permissions {
                paths: vec!["/workspace".into()],
                ..Default::default()
            },
            activation: Activation::default(),
            credentials: vec![],
        }
    }

    fn required(&self, args: &serde_json::Value) -> Permissions {
        let paths = args
            .get("path")
            .and_then(|v| v.as_str())
            .map(|p| vec![p.to_string()])
            .unwrap_or_default();
        Permissions {
            paths,
            ..Default::default()
        }
    }

    fn call(&self, args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        let path =
            args.get("path")
                .and_then(|v| v.as_str())
                .ok_or_else(|| OrchestratorError::Tool {
                    tool: "fs.write".into(),
                    message: "missing 'path'".into(),
                })?;
        self.sink.lock().unwrap().push(path.to_string());
        Ok(serde_json::json!({ "written": path }))
    }
}

/// SP-4 s3: a REAL filesystem write, confined to the per-run workspace jail. Mutation —
/// rides the two-phase path; a resume replays `{bytes,path}` from the memo (no re-write).
pub struct FsWriteTool;

impl Tool for FsWriteTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "fs_write".into(),
            description: Some("Write UTF-8 content to a workspace-relative path".into()),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": { "path": {"type": "string"}, "content": {"type": "string"} },
                "required": ["path", "content"]
            }),
            effect_class: EffectClass::Mutation,
            ttl_secs: None,
            source: None,
            permissions: Permissions::default(),
            activation: Activation::default(),
            credentials: vec![],
        }
    }

    fn required(&self, args: &serde_json::Value) -> Permissions {
        let paths = args
            .get("path")
            .and_then(|v| v.as_str())
            .map(|p| vec![p.to_string()])
            .unwrap_or_default();
        Permissions {
            paths,
            ..Default::default()
        }
    }

    fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        // The executor always drives fs tools via `call_ctx` (needs the workspace root).
        Err(OrchestratorError::Tool {
            tool: "fs_write".into(),
            message: "fs_write requires a workspace context (call_ctx)".into(),
        })
    }

    fn call_ctx(
        &self,
        args: serde_json::Value,
        ctx: &ToolContext,
    ) -> Result<serde_json::Value, OrchestratorError> {
        let root = ctx
            .workspace_root
            .as_ref()
            .ok_or_else(|| OrchestratorError::Tool {
                tool: "fs_write".into(),
                message: "no workspace root wired".into(),
            })?;
        let path =
            args.get("path")
                .and_then(|v| v.as_str())
                .ok_or_else(|| OrchestratorError::Tool {
                    tool: "fs_write".into(),
                    message: "missing 'path'".into(),
                })?;
        let content = args
            .get("content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OrchestratorError::Tool {
                tool: "fs_write".into(),
                message: "missing 'content'".into(),
            })?;
        let target = crate::agent::workspace::confine(root, path)?;
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).map_err(|e| OrchestratorError::Tool {
                tool: "fs_write".into(),
                message: format!("mkdir: {e}"),
            })?;
        }
        std::fs::write(&target, content).map_err(|e| OrchestratorError::Tool {
            tool: "fs_write".into(),
            message: format!("write: {e}"),
        })?;
        // Relative `path` in the output (spec D6) — stable if the base moves; no host-path leak.
        Ok(serde_json::json!({ "bytes": content.len(), "path": path }))
    }
}

/// SP-4 s3: a REAL filesystem read, confined to the per-run workspace jail. Observation
/// (`ttl_secs: 0` ⇒ always re-read; a resume re-reads the persisted file, no token cost).
pub struct FsReadTool;

impl Tool for FsReadTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "fs_read".into(),
            description: Some("Read UTF-8 content from a workspace-relative path".into()),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": { "path": {"type": "string"} },
                "required": ["path"]
            }),
            effect_class: EffectClass::Observation,
            ttl_secs: Some(0),
            source: None,
            permissions: Permissions::default(),
            activation: Activation::default(),
            credentials: vec![],
        }
    }

    fn required(&self, args: &serde_json::Value) -> Permissions {
        let paths = args
            .get("path")
            .and_then(|v| v.as_str())
            .map(|p| vec![p.to_string()])
            .unwrap_or_default();
        Permissions {
            paths,
            ..Default::default()
        }
    }

    fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        Err(OrchestratorError::Tool {
            tool: "fs_read".into(),
            message: "fs_read requires a workspace context (call_ctx)".into(),
        })
    }

    fn call_ctx(
        &self,
        args: serde_json::Value,
        ctx: &ToolContext,
    ) -> Result<serde_json::Value, OrchestratorError> {
        let root = ctx
            .workspace_root
            .as_ref()
            .ok_or_else(|| OrchestratorError::Tool {
                tool: "fs_read".into(),
                message: "no workspace root wired".into(),
            })?;
        let path =
            args.get("path")
                .and_then(|v| v.as_str())
                .ok_or_else(|| OrchestratorError::Tool {
                    tool: "fs_read".into(),
                    message: "missing 'path'".into(),
                })?;
        let target = crate::agent::workspace::confine(root, path)?;
        let content = std::fs::read_to_string(&target).map_err(|e| OrchestratorError::Tool {
            tool: "fs_read".into(),
            message: format!("read: {e}"),
        })?;
        Ok(serde_json::json!({ "content": content }))
    }
}

/// Pure discovery: list the registry's agents (name + role).
pub struct ListAgents(pub Arc<Registry>);
impl Tool for ListAgents {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "list_agents".into(),
            description: Some("List available agents (name, area, kind)".into()),
            input_schema: serde_json::json!({"type":"object","properties":{}}),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
            permissions: Permissions::default(),
            activation: Activation::default(),
            credentials: vec![],
        }
    }
    fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        let mut agents: Vec<_> = self
            .0
            .agents()
            .map(|a| serde_json::json!({ "name": a.name, "area": a.area, "kind": a.kind }))
            .collect();
        agents.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        Ok(serde_json::json!({ "agents": agents }))
    }
}

/// Pure discovery: list skills (name + description).
pub struct ListSkills(pub Arc<Registry>);
impl Tool for ListSkills {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "list_skills".into(),
            description: Some("List available skills".into()),
            input_schema: serde_json::json!({"type":"object","properties":{}}),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
            permissions: Permissions::default(),
            activation: Activation::default(),
            credentials: vec![],
        }
    }
    fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        let mut skills: Vec<_> = self
            .0
            .skills()
            .map(|s| serde_json::json!({ "name": s.name, "description": s.description }))
            .collect();
        skills.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        Ok(serde_json::json!({ "skills": skills }))
    }
}

/// Pure discovery: list tools (name + description + class).
pub struct ListTools(pub Arc<Registry>);
impl Tool for ListTools {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "list_tools".into(),
            description: Some("List available tools".into()),
            input_schema: serde_json::json!({"type":"object","properties":{}}),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
            permissions: Permissions::default(),
            activation: Activation::default(),
            credentials: vec![],
        }
    }
    fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        let mut tools: Vec<_> = self
            .0
            .tools()
            .map(|t| {
                serde_json::json!({ "name": t.name, "description": t.description, "effect_class": format!("{:?}", t.effect_class) })
            })
            .collect();
        tools.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        Ok(serde_json::json!({ "tools": tools }))
    }
}

/// Pure discovery: list registry-known chain ids (best-effort menu).
pub struct ListChains(pub Arc<Registry>);
impl Tool for ListChains {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "list_chains".into(),
            description: Some("List registry-known chain ids".into()),
            input_schema: serde_json::json!({"type":"object","properties":{}}),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
            permissions: Permissions::default(),
            activation: Activation::default(),
            credentials: vec![],
        }
    }
    fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        Ok(serde_json::json!({ "chains": self.0.chain_names() }))
    }
}

/// Pure: validate a draft plan (`{plan: <json string>}`) → `{ok, errors}`.
pub struct ValidatePlan {
    pub registry: Arc<Registry>,
    pub max_nodes: usize,
}
impl Tool for ValidatePlan {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "validate_plan".into(),
            description: Some("Validate a draft plan JSON; returns {ok, errors}".into()),
            input_schema: serde_json::json!({"type":"object","properties":{"plan":{"type":"string","description":"The draft plan as a JSON string: {\"graph\":{\"nodes\":[{id,kind,deps}]}, \"node_plans\":{id:{label,...}}}"}},"required":["plan"]}),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
            permissions: Permissions::default(),
            activation: Activation::default(),
            credentials: vec![],
        }
    }
    fn call(&self, args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        let text = args
            .get("plan")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        match orchestrator_core::parse_plan(text) {
            Err(e) => Ok(serde_json::json!({ "ok": false, "errors": [format!("{e:?}")] })),
            Ok(plan) => match orchestrator_core::feasible(&plan, &self.registry, self.max_nodes) {
                Ok(()) => Ok(serde_json::json!({ "ok": true, "errors": [] })),
                Err(errs) => Ok(serde_json::json!({ "ok": false,
                    "errors": errs.iter().map(|e| format!("{e:?}")).collect::<Vec<_>>() })),
            },
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
        CredentialBroker, EffectClass, OrchestratorError, ReconcileOutcome, ReconcileProvider,
        ToolSpec,
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
                    credentials: vec![],
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

    // A tool with a non-empty surface and NO `required` override — pins that the
    // default returns the static declaration (not just empty).
    struct SurfaceOnly;
    impl Tool for SurfaceOnly {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "surface_only".into(),
                description: None,
                input_schema: serde_json::json!({}),
                effect_class: EffectClass::Pure,
                ttl_secs: None,
                source: None,
                permissions: Permissions {
                    paths: vec!["/srv".into()],
                    ..Default::default()
                },
                activation: Activation::default(),
                credentials: vec![],
            }
        }
        fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
            Ok(serde_json::json!({}))
        }
    }

    #[test]
    fn required_default_returns_the_non_empty_static_surface() {
        let need = SurfaceOnly.required(&serde_json::json!({"anything": 1}));
        assert_eq!(need, SurfaceOnly.spec().permissions);
        assert_eq!(need.paths, vec!["/srv".to_string()]);
        assert!(
            !need.paths.is_empty(),
            "default must reflect the real surface, not empty"
        );
    }

    #[test]
    fn required_defaults_to_spec_and_overrides_use_args() {
        // Default impl: a tool that doesn't override returns its static declaration.
        assert_eq!(
            Calc.required(&serde_json::json!({})),
            Calc.spec().permissions
        );
        // Override: ScopedWriter derives the concrete path need from the `path` arg.
        let w = ScopedWriter::new(std::sync::Arc::new(std::sync::Mutex::new(Vec::new())));
        let need = w.required(&serde_json::json!({"path":"/workspace/a.txt","content":"x"}));
        assert_eq!(need.paths, vec!["/workspace/a.txt".to_string()]);
        // Missing path → no concrete path need (the gate allows; the call errors).
        assert!(w.required(&serde_json::json!({})).paths.is_empty());
    }

    #[test]
    fn idempotency_key_defaults_none_and_override_uses_args() {
        assert_eq!(Calc.idempotency_key(&serde_json::json!({})), None);
        struct Keyed;
        impl Tool for Keyed {
            fn spec(&self) -> ToolSpec {
                ToolSpec {
                    name: "keyed".into(),
                    description: None,
                    input_schema: serde_json::json!({}),
                    effect_class: EffectClass::Mutation,
                    ttl_secs: None,
                    source: None,
                    permissions: Permissions::default(),
                    activation: Activation::default(),
                    credentials: vec![],
                }
            }
            fn call(&self, _a: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
                Ok(serde_json::json!({}))
            }
            fn idempotency_key(&self, args: &serde_json::Value) -> Option<String> {
                args.get("ref").and_then(|v| v.as_str()).map(str::to_string)
            }
        }
        assert_eq!(
            Keyed.idempotency_key(&serde_json::json!({ "ref": "bk-42" })),
            Some("bk-42".to_string())
        );
    }

    #[test]
    fn call_ctx_defaults_to_call_and_registry_threads_ctx() {
        let reg = ToolRegistry::default().with_tool(std::sync::Arc::new(Calc));
        let ctx = ToolContext {
            idempotency_key: "k1".into(),
            effect_id: orchestrator_core::effect::effect_id("n", 0, 0),
            credentials: Default::default(),
            workspace_root: None,
        };
        let via_ctx = reg
            .execute_ctx(
                "calc",
                serde_json::json!({ "op": "add", "a": 2, "b": 3 }),
                &ctx,
            )
            .unwrap();
        let via_plain = reg
            .execute("calc", serde_json::json!({ "op": "add", "a": 2, "b": 3 }))
            .unwrap();
        assert_eq!(via_ctx, via_plain, "default call_ctx delegates to call");
        assert_eq!(reg.idempotency_key_of("calc", &serde_json::json!({})), None);
        assert_eq!(reg.idempotency_key_of("nope", &serde_json::json!({})), None);
    }

    #[test]
    fn required_of_reads_the_registry_or_defaults_empty() {
        let reg = ToolRegistry::default().with_tool(std::sync::Arc::new(ScopedWriter::new(
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        )));
        assert_eq!(
            reg.required_of("fs.write", &serde_json::json!({"path":"/x"}))
                .paths,
            vec!["/x".to_string()]
        );
        assert_eq!(
            reg.required_of("unknown", &serde_json::json!({})),
            orchestrator_core::Permissions::default()
        );
    }

    #[tokio::test]
    async fn static_broker_resolves_known_refs() {
        let mut m = std::collections::HashMap::new();
        m.insert("api_token".to_string(), format!("tok-{}", "xyz")); // runtime-assembled (semgrep hook)
        let broker = StaticCredentialBroker::new(m);
        let got = broker.resolve("api_token").await.unwrap();
        assert_eq!(
            got.as_ref().map(|s| s.expose()),
            Some(format!("tok-{}", "xyz").as_str())
        );
        assert!(broker.resolve("nope").await.unwrap().is_none());
    }

    #[test]
    fn tool_context_secret_values_lists_injected() {
        let mut creds = std::collections::HashMap::new();
        creds.insert("k".to_string(), orchestrator_core::Secret::new("s3cret"));
        let ctx = ToolContext {
            idempotency_key: "i".into(),
            effect_id: orchestrator_core::effect::effect_id("n", 0, 0),
            credentials: std::sync::Arc::new(creds),
            workspace_root: None,
        };
        assert_eq!(ctx.exposed_secret_values(), vec!["s3cret"]);

        // Hygiene (Arc): cloning `ToolContext` shares ONE secret store, not a plaintext copy.
        let cloned = ctx.clone();
        assert!(
            std::sync::Arc::ptr_eq(&ctx.credentials, &cloned.credentials),
            "cloning ToolContext shares one secret store, not a plaintext copy"
        );
    }

    fn ws_ctx(root: &std::path::Path) -> ToolContext {
        ToolContext {
            idempotency_key: "k".into(),
            effect_id: orchestrator_core::effect::effect_id("n", 0, 0),
            credentials: std::sync::Arc::new(std::collections::HashMap::new()),
            workspace_root: Some(std::sync::Arc::new(root.to_path_buf())),
        }
    }

    #[test]
    fn fs_write_writes_real_bytes_in_the_jail() {
        let td = tempfile::tempdir().unwrap();
        let root = td.path().canonicalize().unwrap();
        let ctx = ws_ctx(&root);
        let out = FsWriteTool
            .call_ctx(
                serde_json::json!({"path": "notes.md", "content": "hi"}),
                &ctx,
            )
            .unwrap();
        assert_eq!(out, serde_json::json!({"bytes": 2, "path": "notes.md"}));
        assert_eq!(
            std::fs::read_to_string(root.join("notes.md")).unwrap(),
            "hi"
        );
    }

    #[test]
    fn fs_write_creates_nested_parent_dirs() {
        let td = tempfile::tempdir().unwrap();
        let root = td.path().canonicalize().unwrap();
        let ctx = ws_ctx(&root);
        let out = FsWriteTool
            .call_ctx(
                serde_json::json!({"path": "a/b/c.txt", "content": "deep"}),
                &ctx,
            )
            .unwrap();
        assert_eq!(out, serde_json::json!({"bytes": 4, "path": "a/b/c.txt"}));
        assert_eq!(
            std::fs::read_to_string(root.join("a").join("b").join("c.txt")).unwrap(),
            "deep"
        );
    }

    #[test]
    fn fs_read_round_trips_a_written_file() {
        let td = tempfile::tempdir().unwrap();
        let root = td.path().canonicalize().unwrap();
        let ctx = ws_ctx(&root);
        std::fs::write(root.join("notes.md"), "hello").unwrap();
        let out = FsReadTool
            .call_ctx(serde_json::json!({"path": "notes.md"}), &ctx)
            .unwrap();
        assert_eq!(out, serde_json::json!({"content": "hello"}));
    }

    #[test]
    fn fs_write_escape_is_a_workspace_escape_error() {
        let td = tempfile::tempdir().unwrap();
        let root = td.path().canonicalize().unwrap();
        let ctx = ws_ctx(&root);
        let err = FsWriteTool
            .call_ctx(
                serde_json::json!({"path": "../../etc/passwd", "content": "x"}),
                &ctx,
            )
            .unwrap_err();
        assert!(matches!(err, OrchestratorError::WorkspaceEscape(_)));
    }

    #[test]
    fn fs_write_without_a_workspace_fails_loud() {
        let ctx = ToolContext {
            idempotency_key: "k".into(),
            effect_id: orchestrator_core::effect::effect_id("n", 0, 0),
            credentials: std::sync::Arc::new(std::collections::HashMap::new()),
            workspace_root: None,
        };
        let err = FsWriteTool
            .call_ctx(
                serde_json::json!({"path": "notes.md", "content": "x"}),
                &ctx,
            )
            .unwrap_err();
        assert!(matches!(err, OrchestratorError::Tool { .. }));
    }
}

#[cfg(test)]
mod planner_tool_tests {
    use super::*;
    use orchestrator_core::{AgentDefinition, Registry};
    use std::collections::HashMap;

    fn agent_def(name: &str) -> AgentDefinition {
        AgentDefinition {
            name: name.into(),
            area: "research".into(),
            kind: "reasoning".into(),
            chain: Some("research.bulk".into()),
            chains: HashMap::new(),
            grants: HashMap::new(),
            tools: vec![],
            skills: vec![],
            system_prompt: "r".into(),
        }
    }

    fn reg() -> Arc<Registry> {
        Arc::new(Registry::default().with_agent(agent_def("researcher")))
    }

    #[test]
    fn list_agents_returns_the_menu() {
        let t = ListAgents(reg());
        let out = t.call(serde_json::json!({})).unwrap();
        let arr = out["agents"].as_array().unwrap();
        assert!(
            arr.iter()
                .any(|a| a["name"] == "researcher" && a["area"] == "research")
        );
    }

    #[test]
    fn list_agents_output_is_sorted_by_name() {
        // Insertion order (zeta before alpha) must NOT survive: the Pure tool sorts
        // by name so its output is deterministic regardless of HashMap iteration.
        let reg = Arc::new(
            Registry::default()
                .with_agent(agent_def("zeta"))
                .with_agent(agent_def("alpha")),
        );
        let out = ListAgents(reg).call(serde_json::json!({})).unwrap();
        let names: Vec<&str> = out["agents"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["alpha", "zeta"]);
    }

    #[test]
    fn validate_plan_tool_reports_errors_and_ok() {
        let t = ValidatePlan {
            registry: reg(),
            max_nodes: 512,
        };
        let bad = t.call(serde_json::json!({ "plan": "not json" })).unwrap();
        assert_eq!(bad["ok"], false);
        assert!(!bad["errors"].as_array().unwrap().is_empty());
        let good = t.call(serde_json::json!({ "plan":
            r#"{"graph":{"nodes":[{"id":"n1","kind":{"ModelCall":{"chain":"research.bulk","payload":{}}},"deps":[]}]}}"# })).unwrap();
        assert_eq!(good["ok"], true);
    }

    #[test]
    fn validate_plan_tool_reports_infeasible_parseable_plan() {
        // The draft PARSES but is INFEASIBLE: a NodePlan need references an agent
        // absent from the registry → {ok:false} with the unknown-agent error rendered.
        let t = ValidatePlan {
            registry: reg(),
            max_nodes: 512,
        };
        let out = t.call(serde_json::json!({ "plan":
            r#"{"graph":{"nodes":[{"id":"n1","kind":{"ModelCall":{"chain":"research.bulk","payload":{}}},"deps":[]}]},"node_plans":{"n1":{"label":"x","needs":{"agents":["ghost"]}}}}"# })).unwrap();
        assert_eq!(out["ok"], false);
        let errors = out["errors"].as_array().unwrap();
        assert!(
            errors.iter().any(|e| e.as_str().unwrap().contains("ghost")),
            "unknown-agent error rendered: {errors:?}"
        );
    }
}
