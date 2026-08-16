# SP-4 Workspace Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship two real filesystem tools (`fs_write` Mutation, `fs_read` Observation) confined to a durable per-run workspace jail, making the SP-4 enforcement arc (s1 gate, s2 redaction, credential broker) concrete against a tool that actually does I/O.

**Architecture:** A pure `confine()` path-jail helper (orchestrator crate; uses `std::fs::canonicalize`) enforces that a tool's declared paths resolve within a per-run `base/<run_id>/` directory injected into `ToolContext.workspace_root`. The executor resolves+creates the canonical per-run root lazily (`with_workspace_root(base)`, default None ⇒ byte-identical), pre-checks the declared path surface (a jail escape → a terse Pure denial, same shape as the s1 denial), and the real tools resolve their target through the same helper. The jail's unique value over s1 is per-run **isolation** and **symlink/canonicalization** defense (s1 already rejects `..`/un-granted paths lexically and runs *before* the jail check).

**Tech Stack:** Rust, `std::fs`, `std::path::{Component, Path, PathBuf}`, `serde_json`, `tempfile` (dev), the existing durable-executor two-phase Mutation + Observation-TTL machinery.

**Spec:** `docs/superpowers/specs/2026-08-16-sp4-workspace-isolation-design.md`

**Baseline:** `develop` at `1d7015f`; full workspace **1083 tests** green. `cargo fmt --all` before every commit (pre-commit hook = fmt-check + workspace `clippy -D warnings`, runs NO tests → always `cargo test --workspace` yourself, real unpiped exit code). Every secret fixture assembled at runtime (semgrep CWE-798). Do NOT push (the coordinator pushes after the whole-slice review).

---

## File Structure

- **Create** `crates/orchestrator/src/agent/workspace.rs` — the `confine()` jail helper + its unit tests. One responsibility: path confinement.
- **Modify** `crates/orchestrator-core/src/error.rs` — add `OrchestratorError::WorkspaceEscape(String)`.
- **Modify** `crates/orchestrator/src/agent/mod.rs` — declare `pub mod workspace;`.
- **Modify** `crates/orchestrator/src/agent/tools.rs` — add `ToolContext.workspace_root` field; add `FsWriteTool` + `FsReadTool`; update the 3 `ToolContext {…}` test literals.
- **Modify** `crates/orchestrator/src/executor/mod.rs` — `workspace_root_base: Option<PathBuf>` field + `new` default + `with_workspace_root` builder.
- **Modify** `crates/orchestrator/src/executor/agent.rs` — `workspace_root_for` helper; inject `workspace_root` into the `ToolContext` built in `record_tool_effect`; the jail pre-check in `execute_tool_effect`.
- **Modify** `crates/orchestrator/Cargo.toml` — add `tempfile = "3"` to `[dev-dependencies]`.
- **Test** `crates/orchestrator/src/executor/tests.rs` — e2e (round-trip, escape denial, isolation, resume, redaction, additivity).

---

## Task 1: The `confine()` jail helper + `WorkspaceEscape` error

**Files:**
- Modify: `crates/orchestrator-core/src/error.rs`
- Create: `crates/orchestrator/src/agent/workspace.rs`
- Modify: `crates/orchestrator/src/agent/mod.rs`

- [ ] **Step 1: Add the error variant**

In `crates/orchestrator-core/src/error.rs`, inside `pub enum OrchestratorError { … }` (it derives `thiserror::Error`), add (place it near `Tool { … }`):

```rust
    /// A tool requested a path that escapes its per-run workspace jail (SP-4 s3).
    /// The message names the requested (relative) path but NOT the absolute host root,
    /// so the journal/transcript never leaks the host filesystem layout.
    #[error("workspace escape: {0}")]
    WorkspaceEscape(String),
```

- [ ] **Step 2: Declare the module**

In `crates/orchestrator/src/agent/mod.rs`, add after `pub mod tools;`:

```rust
pub mod workspace;
```

- [ ] **Step 3: Write the failing tests**

Create `crates/orchestrator/src/agent/workspace.rs` with ONLY the tests first (the `confine` fn comes in Step 5). Add `tempfile = "3"` to `crates/orchestrator/Cargo.toml` `[dev-dependencies]` now (Task 2 also needs it):

```rust
//! SP-4 s3: path confinement for the per-run workspace jail. A tool arg names a
//! workspace-RELATIVE path; `confine` resolves it against the canonical per-run root and
//! rejects anything that escapes (absolute, `..`, or a symlink resolving outside).

use std::path::{Component, Path, PathBuf};

use orchestrator_core::OrchestratorError;

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn canon_tmp() -> (tempfile::TempDir, PathBuf) {
        let td = tempfile::tempdir().unwrap();
        let root = td.path().canonicalize().unwrap(); // resolve /var -> /private/var on macOS
        (td, root)
    }

    #[test]
    fn confines_a_relative_path_within_root() {
        let (_td, root) = canon_tmp();
        let got = confine(&root, "a/b.txt").unwrap();
        assert!(got.starts_with(&root), "{got:?} not under {root:?}");
        assert_eq!(got, root.join("a").join("b.txt"));
    }

    #[test]
    fn confines_a_not_yet_existing_nested_path() {
        let (_td, root) = canon_tmp();
        // deepest existing ancestor is `root` itself; still Ok.
        let got = confine(&root, "deep/nested/new.txt").unwrap();
        assert_eq!(got, root.join("deep").join("nested").join("new.txt"));
    }

    #[test]
    fn rejects_parent_dir_escape() {
        let (_td, root) = canon_tmp();
        assert!(matches!(
            confine(&root, "../../etc/passwd"),
            Err(OrchestratorError::WorkspaceEscape(_))
        ));
    }

    #[test]
    fn rejects_absolute_path() {
        let (_td, root) = canon_tmp();
        assert!(matches!(
            confine(&root, "/etc/passwd"),
            Err(OrchestratorError::WorkspaceEscape(_))
        ));
    }

    #[test]
    fn rejects_symlink_that_resolves_outside_root() {
        let (_td, root) = canon_tmp();
        let outside = tempfile::tempdir().unwrap();
        // root/link -> <outside dir>
        std::os::unix::fs::symlink(outside.path(), root.join("link")).unwrap();
        fs::create_dir_all(outside.path().join("sub")).unwrap();
        assert!(
            matches!(
                confine(&root, "link/sub/x.txt"),
                Err(OrchestratorError::WorkspaceEscape(_))
            ),
            "a symlink resolving outside root must be rejected"
        );
    }
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cargo test -p sensei-orchestrator --lib agent::workspace 2>&1 | tail -20`
Expected: FAIL to COMPILE — `cannot find function confine in this scope`.

- [ ] **Step 5: Implement `confine`**

Add to `crates/orchestrator/src/agent/workspace.rs` above the `#[cfg(test)] mod tests`:

```rust
/// Confine `requested` (a workspace-RELATIVE tool-arg path) to the CANONICAL per-run
/// workspace `root`. Rejects absolute paths, any `..`/root/prefix component, and a symlink
/// whose deepest existing ancestor resolves outside `root`. The returned path may not exist
/// yet (a write target). `root` MUST already be canonical (the executor canonicalizes the
/// per-run dir once). Deterministic given the filesystem state; performs no writes.
///
/// This confines the DECLARED path surface. An in-process tool with ambient authority that
/// bypasses this helper cannot be prevented here — bypass-proof confinement is the (deferred)
/// subprocess sandbox (spec §6).
pub(crate) fn confine(root: &Path, requested: &str) -> Result<PathBuf, OrchestratorError> {
    let req = Path::new(requested);
    if req.is_absolute() {
        return Err(OrchestratorError::WorkspaceEscape(requested.to_string()));
    }
    // Fold components lexically; ANY `..`/root/prefix is a hard reject (no in-jail `..`
    // traversal — a safe, strict superset of "no net escape").
    let mut out = root.to_path_buf();
    for comp in req.components() {
        match comp {
            Component::Normal(seg) => out.push(seg),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(OrchestratorError::WorkspaceEscape(requested.to_string()));
            }
        }
    }
    // Symlink-out defense: canonicalize the deepest EXISTING ancestor and require it stays
    // within `root`. (`Path::exists` follows symlinks, so a symlink to an existing outside
    // dir is caught here.)
    let mut probe: &Path = out.as_path();
    let existing = loop {
        if probe.exists() {
            break probe
                .canonicalize()
                .map_err(|e| OrchestratorError::WorkspaceEscape(format!("{requested}: {e}")))?;
        }
        match probe.parent() {
            Some(p) => probe = p,
            None => break root.to_path_buf(),
        }
    };
    if !existing.starts_with(root) {
        return Err(OrchestratorError::WorkspaceEscape(requested.to_string()));
    }
    Ok(out)
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p sensei-orchestrator --lib agent::workspace 2>&1 | tail -20`
Expected: PASS (5 tests). Then `cargo clippy -p sensei-orchestrator --all-targets -- -D warnings 2>&1 | tail -5` → clean.

- [ ] **Step 7: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator-core/src/error.rs crates/orchestrator/src/agent/workspace.rs crates/orchestrator/src/agent/mod.rs crates/orchestrator/Cargo.toml
git commit -m "feat(orchestrator): SP-4 workspace (1/4) — confine() path jail + WorkspaceEscape error"
```

---

## Task 2: `ToolContext.workspace_root` + the real `fs_write` / `fs_read` tools

**Files:**
- Modify: `crates/orchestrator/src/agent/tools.rs`

- [ ] **Step 1: Add the `workspace_root` field to `ToolContext`**

In `crates/orchestrator/src/agent/tools.rs`, add to `pub struct ToolContext { … }` (after the `credentials` field, ~line 28):

```rust
    /// The CANONICAL per-run workspace root the executor resolved (SP-4 s3), or `None`
    /// when no workspace is wired. A confined fs tool resolves its target via
    /// [`workspace::confine`](crate::agent::workspace::confine) against this root.
    pub workspace_root: Option<std::sync::Arc<std::path::PathBuf>>,
```

- [ ] **Step 2: Update the existing `ToolContext {…}` test literals**

Still in `tools.rs`, find the `ToolContext {` literal(s) in the `#[cfg(test)]` module (e.g. in `call_ctx_defaults_to_call_and_registry_threads_ctx`) and add `workspace_root: None,` to each. Run `grep -n "ToolContext {" crates/orchestrator/src/agent/tools.rs` — every literal here needs the field (the executor's literal in `agent.rs` is handled in Task 3).

- [ ] **Step 3: Write the failing tool tests**

Add to the `#[cfg(test)] mod tests` in `tools.rs` (assemble a `ToolContext` with a real TempDir root):

```rust
    fn ws_ctx(root: &std::path::Path) -> ToolContext {
        ToolContext {
            idempotency_key: "k".into(),
            effect_id: orchestrator_core::effect_id("n", 0, 0),
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
            .call_ctx(serde_json::json!({"path": "notes.md", "content": "hi"}), &ctx)
            .unwrap();
        assert_eq!(out, serde_json::json!({"bytes": 2, "path": "notes.md"}));
        assert_eq!(std::fs::read_to_string(root.join("notes.md")).unwrap(), "hi");
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
            .call_ctx(serde_json::json!({"path": "../../etc/passwd", "content": "x"}), &ctx)
            .unwrap_err();
        assert!(matches!(err, OrchestratorError::WorkspaceEscape(_)));
    }

    #[test]
    fn fs_write_without_a_workspace_fails_loud() {
        let ctx = ToolContext {
            idempotency_key: "k".into(),
            effect_id: orchestrator_core::effect_id("n", 0, 0),
            credentials: std::sync::Arc::new(std::collections::HashMap::new()),
            workspace_root: None,
        };
        let err = FsWriteTool
            .call_ctx(serde_json::json!({"path": "notes.md", "content": "x"}), &ctx)
            .unwrap_err();
        assert!(matches!(err, OrchestratorError::Tool { .. }));
    }
```

(Confirm the helper name for building an `EffectId` in tests — `orchestrator_core::effect_id(parent, iter, idx)`; grep an existing tools.rs test for how it constructs `effect_id` and match it.)

- [ ] **Step 4: Run to verify they fail**

Run: `cargo test -p sensei-orchestrator --lib agent::tools 2>&1 | tail -20`
Expected: FAIL to COMPILE — `cannot find … FsWriteTool` / `FsReadTool`.

- [ ] **Step 5: Implement the tools**

Add to `tools.rs` (module scope, near `ScopedWriter` — note the distinct names `fs_write`/`fs_read`, NOT the existing demo `ScopedWriter`/`fs.write`):

```rust
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
        Permissions { paths, ..Default::default() }
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
        let root = ctx.workspace_root.as_ref().ok_or_else(|| OrchestratorError::Tool {
            tool: "fs_write".into(),
            message: "no workspace root wired".into(),
        })?;
        let path = args.get("path").and_then(|v| v.as_str()).ok_or_else(|| {
            OrchestratorError::Tool { tool: "fs_write".into(), message: "missing 'path'".into() }
        })?;
        let content = args.get("content").and_then(|v| v.as_str()).ok_or_else(|| {
            OrchestratorError::Tool { tool: "fs_write".into(), message: "missing 'content'".into() }
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
        Permissions { paths, ..Default::default() }
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
        let root = ctx.workspace_root.as_ref().ok_or_else(|| OrchestratorError::Tool {
            tool: "fs_read".into(),
            message: "no workspace root wired".into(),
        })?;
        let path = args.get("path").and_then(|v| v.as_str()).ok_or_else(|| {
            OrchestratorError::Tool { tool: "fs_read".into(), message: "missing 'path'".into() }
        })?;
        let target = crate::agent::workspace::confine(root, path)?;
        let content = std::fs::read_to_string(&target).map_err(|e| OrchestratorError::Tool {
            tool: "fs_read".into(),
            message: format!("read: {e}"),
        })?;
        Ok(serde_json::json!({ "content": content }))
    }
}
```

- [ ] **Step 6: Run to verify they pass**

Run: `cargo test -p sensei-orchestrator --lib agent::tools 2>&1 | tail -20`
Expected: PASS (existing + 4 new). Then clippy `-D warnings` clean.

- [ ] **Step 7: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/agent/tools.rs
git commit -m "feat(orchestrator): SP-4 workspace (2/4) — ToolContext.workspace_root + real fs_write/fs_read tools"
```

---

## Task 3: Executor wiring — `with_workspace_root`, per-run root resolution, jail pre-check

**Files:**
- Modify: `crates/orchestrator/src/executor/mod.rs`
- Modify: `crates/orchestrator/src/executor/agent.rs`
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Add the field + builder**

In `crates/orchestrator/src/executor/mod.rs`: add a field to `pub struct Executor { … }` (near `cas_threshold`):

```rust
    /// SP-4 s3: base dir for the per-run workspace jail (`base/<run_id>/`). `None` ⇒ no fs
    /// tools / byte-identical. Set via [`with_workspace_root`](Self::with_workspace_root).
    workspace_root_base: Option<std::path::PathBuf>,
```

In `Executor::new(...)`, add to the struct literal (near `cas_threshold: 4096,`):

```rust
            workspace_root_base: None,
```

Add the builder (near `with_cas_threshold`):

```rust
    /// SP-4 s3: root a durable per-run workspace jail at `base/<run_id>/`. Default none ⇒
    /// byte-identical, no fs tools. Confined `fs_write`/`fs_read` tools resolve their targets
    /// within the canonical per-run dir; the executor pre-checks each declared path.
    pub fn with_workspace_root(mut self, base: impl Into<std::path::PathBuf>) -> Self {
        self.workspace_root_base = Some(base.into());
        self
    }
```

- [ ] **Step 2: Add the `workspace_root_for` helper on the Executor**

In `crates/orchestrator/src/executor/agent.rs`, add this method inside the `impl Executor { … }` block (near `record_tool_effect`). Confirm the `use` for `PathBuf`/`Arc` (or fully-qualify as below):

```rust
    /// Resolve (and lazily create) the CANONICAL per-run workspace root, or `None` if no
    /// base is wired. `create_dir_all` is idempotent (safe on resume); `canonicalize`
    /// resolves symlinks in the base (e.g. macOS `/var`→`/private/var`) so the jail compares
    /// canonical-to-canonical. Called on the LIVE path only (a memo hit replays without it).
    pub(super) fn workspace_root_for(
        &self,
        run: RunId,
    ) -> Result<Option<std::sync::Arc<std::path::PathBuf>>, OrchestratorError> {
        let Some(base) = &self.workspace_root_base else {
            return Ok(None);
        };
        let dir = base.join(run.0.to_string());
        std::fs::create_dir_all(&dir).map_err(|e| OrchestratorError::Tool {
            tool: "workspace".into(),
            message: format!("create workspace {}: {e}", dir.display()),
        })?;
        let canon = dir.canonicalize().map_err(|e| OrchestratorError::Tool {
            tool: "workspace".into(),
            message: format!("canonicalize workspace {}: {e}", dir.display()),
        })?;
        Ok(Some(std::sync::Arc::new(canon)))
    }
```

(If `RunId` / `OrchestratorError` aren't already in scope in `agent.rs`, they are — the file already uses `ar.run: RunId` and returns `OrchestratorError`.)

- [ ] **Step 3: Inject `workspace_root` into the `ToolContext`**

In `agent.rs`, in `record_tool_effect`, the `ToolContext { … }` literal (currently `idempotency_key`, `effect_id`, `credentials`) — add the resolved root:

```rust
        let ctx = crate::agent::tools::ToolContext {
            idempotency_key: idempotency_key.to_string(),
            effect_id: teid.clone(),
            credentials: std::sync::Arc::new(resolved),
            workspace_root: self.workspace_root_for(ar.run)?,
        };
```

- [ ] **Step 4: Add the jail pre-check in `execute_tool_effect`**

In `agent.rs`, in `execute_tool_effect`, AFTER the s1 gate's denial `return` (the `if !(listed && grant.covers(&need)) { … }` block) and BEFORE the `match class { … }` live dispatch, insert:

```rust
        // SP-4 s3 workspace confinement: when a per-run jail is wired, every concrete path
        // THIS (s1-authorized) call declares must resolve WITHIN it. A declared escape is a
        // terse denial — recorded Pure (like the s1 denial), no side effect, replayed on
        // resume. The jail BINDS the abstract grant to a live per-run dir + canonicalizes
        // real paths (per-run isolation + symlink defense — what s1's lexical `covers`
        // cannot do). In-process ambient authority means this confines the DECLARED surface;
        // a tool bypassing the shared helper is the (deferred) subprocess sandbox's job.
        if !need.paths.is_empty() {
            if let Some(root) = self.workspace_root_for(ar.run)? {
                for p in &need.paths {
                    if crate::agent::workspace::confine(&root, p).is_err() {
                        tracing::debug!(tool = %call.name, path = %p, "workspace jail escape denied");
                        let detail = format!(
                            "the requested path for tool '{}' is outside its workspace",
                            call.name
                        );
                        return self.record_denied_effect(ar, teid, call, &tih, detail).await;
                    }
                }
            }
        }
```

- [ ] **Step 5: Verify it builds + existing suite still green**

Run: `cargo test -p sensei-orchestrator --lib 2>&1 | tail -8`
Expected: PASS, count unchanged from baseline for the crate (no e2e added yet; the injected `workspace_root: None` default keeps every existing test byte-identical). Then clippy `-D warnings` clean.

- [ ] **Step 6: Write the e2e tests (round-trip, escape denial, isolation)**

Add to `crates/orchestrator/src/executor/tests.rs`. Reuse the existing harness helpers — study them first: `writer_registry(tools, grants)` (~line 161, builds a registry whose agent LISTS `tools` + holds `grants`), `scripted_gateway(vec![...])`, `agent_node(id, agent, input)`, `tool_call_response(id, tool, args_json)`, `final_response(text)`, `recorded_output(events, eid)`, `effect_recorded_count(events, eid)`, `effect_id(node, iter, idx)`, and the `Graph { nodes, edges }` shape from a sibling agent-tool test (e.g. near line 807/980). Grant the fs tools a relative prefix (`"work"`) so s1 authorizes in-`work` paths.

```rust
/// SP-4 s3 e2e: a full agent run writes then reads a file inside its per-run workspace jail.
#[tokio::test]
async fn fs_tools_round_trip_within_the_workspace_jail() {
    let base = tempfile::tempdir().unwrap();
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let (gateway, _calls) = scripted_gateway(vec![
        tool_call_response("t1", "fs_write", r#"{"path":"work/notes.md","content":"hello"}"#),
        tool_call_response("t2", "fs_read", r#"{"path":"work/notes.md"}"#),
        final_response("done"),
    ])
    .await;
    let grants = std::collections::HashMap::from([
        ("fs_write".to_string(), Permissions { paths: vec!["work".into()], ..Default::default() }),
        ("fs_read".to_string(), Permissions { paths: vec!["work".into()], ..Default::default() }),
    ]);
    let tools = Arc::new(
        ToolRegistry::default()
            .with_tool(Arc::new(crate::agent::tools::FsWriteTool))
            .with_tool(Arc::new(crate::agent::tools::FsReadTool)),
    );
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(writer_registry(
            vec!["fs_write".into(), "fs_read".into()],
            grants,
        ))
        .with_tools(tools)
        .with_workspace_root(base.path());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "write then read")],
        edges: vec![],
    };
    let outcome = exec.run(run, &graph).await.expect("run");

    assert!(outcome.failed.is_none() && outcome.paused.is_none(), "failed={:?} paused={:?}", outcome.failed, outcome.paused);
    // The file really exists on disk in the per-run jail.
    let path_on_disk = base.path().join(run.0.to_string()).join("work").join("notes.md");
    assert_eq!(std::fs::read_to_string(&path_on_disk).unwrap(), "hello");
    // fs_write (turn 0, tool idx 1) journaled {bytes,path}; fs_read (turn 1, tool idx 1) content.
    let events = journal.load(run).await.unwrap();
    assert_eq!(
        recorded_output(&events, &effect_id("n1", 0, 1)).unwrap(),
        serde_json::json!({"bytes": 5, "path": "work/notes.md"})
    );
    assert_eq!(
        recorded_output(&events, &effect_id("n1", 1, 1)).unwrap(),
        serde_json::json!({"content": "hello"})
    );
}

/// SP-4 s3 e2e: the jail denies a symlink-out path that s1 GRANTS (grant covers `work/…`,
/// no `..`) — proving the jail's unique contribution over s1's lexical `covers`.
#[tokio::test]
async fn jail_denies_symlink_escape_that_s1_would_allow() {
    let base = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(outside.path().join("sub")).unwrap();
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    // Pre-create the per-run jail + a symlink `work/evil -> <outside>` inside it.
    let run_dir = base.path().join(run.0.to_string());
    std::fs::create_dir_all(run_dir.join("work")).unwrap();
    std::os::unix::fs::symlink(outside.path(), run_dir.join("work").join("evil")).unwrap();

    let (gateway, _calls) = scripted_gateway(vec![
        tool_call_response("t1", "fs_write", r#"{"path":"work/evil/sub/x","content":"pwned"}"#),
        final_response("done"),
    ])
    .await;
    let grants = std::collections::HashMap::from([(
        "fs_write".to_string(),
        Permissions { paths: vec!["work".into()], ..Default::default() }, // s1 COVERS work/evil/sub/x
    )]);
    let tools = Arc::new(ToolRegistry::default().with_tool(Arc::new(crate::agent::tools::FsWriteTool)));
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(writer_registry(vec!["fs_write".into()], grants))
        .with_tools(tools)
        .with_workspace_root(base.path());
    let graph = Graph { nodes: vec![agent_node("n1", "a", "write")], edges: vec![] };
    let outcome = exec.run(run, &graph).await.expect("run");

    assert!(outcome.failed.is_none() && outcome.paused.is_none());
    // No file escaped into the outside dir.
    assert!(!outside.path().join("sub").join("x").exists(), "the write ESCAPED the jail");
    // The effect was recorded as a terse DENIAL (Pure), not a Mutation write.
    let events = journal.load(run).await.unwrap();
    let out = recorded_output(&events, &effect_id("n1", 0, 1)).unwrap();
    assert_eq!(out["error"], serde_json::json!("permission_denied"));
    // No EffectIntent for the (denied) write — it never entered the two-phase path.
    assert!(
        !events.iter().any(|(_, e)| matches!(e, JournalEvent::EffectIntent { effect_id, .. } if effect_id == &effect_id("n1", 0, 1))),
        "a denied write must not journal an EffectIntent"
    );
}

/// SP-4 s3 e2e: two runs writing the SAME relative path land in isolated per-run dirs.
#[tokio::test]
async fn parallel_runs_get_isolated_workspaces() {
    let base = tempfile::tempdir().unwrap();
    let grants = std::collections::HashMap::from([(
        "fs_write".to_string(),
        Permissions { paths: vec!["work".into()], ..Default::default() },
    )]);
    let run_once = |content: &'static str| {
        let base = base.path().to_path_buf();
        let grants = grants.clone();
        async move {
            let journal = InMemoryJournal::new();
            let run = RunId(uuid::Uuid::new_v4());
            let (gateway, _c) = scripted_gateway(vec![
                tool_call_response("t1", "fs_write", &format!(r#"{{"path":"work/f","content":"{content}"}}"#)),
                final_response("done"),
            ])
            .await;
            let tools = Arc::new(ToolRegistry::default().with_tool(Arc::new(crate::agent::tools::FsWriteTool)));
            let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
                .with_registry(writer_registry(vec!["fs_write".into()], grants))
                .with_tools(tools)
                .with_workspace_root(base.clone());
            let graph = Graph { nodes: vec![agent_node("n1", "a", "write")], edges: vec![] };
            exec.run(run, &graph).await.unwrap();
            base.join(run.0.to_string()).join("work").join("f")
        }
    };
    let p1 = run_once("one").await;
    let p2 = run_once("two").await;
    assert_ne!(p1, p2, "runs must not share a path");
    assert_eq!(std::fs::read_to_string(&p1).unwrap(), "one");
    assert_eq!(std::fs::read_to_string(&p2).unwrap(), "two");
}
```

(Adjust the `Graph`/`RunOutcome` field access + `tool_call_response` arg-passing to match the exact sibling-test signatures; the shapes above mirror the broker/permission e2e tests.)

- [ ] **Step 7: Run the e2e tests**

Run: `cargo test -p sensei-orchestrator --lib fs_tools_round_trip_within_the_workspace_jail jail_denies_symlink_escape_that_s1_would_allow parallel_runs_get_isolated_workspaces 2>&1 | tail -20`
Expected: PASS (3). Then clippy `-D warnings` clean.

- [ ] **Step 8: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/mod.rs crates/orchestrator/src/executor/agent.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-4 workspace (3/4) — with_workspace_root + per-run jail pre-check + fs e2e"
```

---

## Task 4: Resume exactly-once, redaction compose, additivity gate

**Files:**
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the resume test (fs_write replays from memo, no re-write)**

Add to `tests.rs`. Mirror the resume-seed idiom of `broker_not_reinvoked_for_a_memoized_tool_on_resume` / `seed_in_doubt_store` (run to completion of the fs_write effect, truncate the journal before `RunCompleted`, resume). The decisive proof: after the seed completes the write, OVERWRITE the file on disk with a sentinel; if the resume re-ran the write it would clobber the sentinel back to the original — assert the sentinel SURVIVES.

```rust
/// SP-4 s3: a completed fs_write replays {bytes,path} from the memo on resume — the tool is
/// NOT re-run, so the file on disk is NOT re-written (exactly-once for a real side effect).
#[tokio::test]
async fn fs_write_replays_from_memo_without_rewriting_on_resume() {
    let base = tempfile::tempdir().unwrap();
    let run = RunId(uuid::Uuid::new_v4());
    let grants = std::collections::HashMap::from([(
        "fs_write".to_string(),
        Permissions { paths: vec!["work".into()], ..Default::default() },
    )]);
    let tool_eid = effect_id("n1", 0, 1);

    // --- seed run: write "orig", to completion ---
    let seed = InMemoryJournal::new();
    let (gw1, _c1) = scripted_gateway(vec![
        tool_call_response("t1", "fs_write", r#"{"path":"work/f","content":"orig"}"#),
        final_response("done"),
    ])
    .await;
    let tools1 = Arc::new(ToolRegistry::default().with_tool(Arc::new(crate::agent::tools::FsWriteTool)));
    let graph = Graph { nodes: vec![agent_node("n1", "a", "write")], edges: vec![] };
    Executor::new(Arc::new(gw1), Arc::new(seed.clone()), "v1")
        .with_registry(writer_registry(vec!["fs_write".into()], grants.clone()))
        .with_tools(tools1)
        .with_workspace_root(base.path())
        .run(run, &graph)
        .await
        .unwrap();
    let file = base.path().join(run.0.to_string()).join("work").join("f");
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "orig");

    // Truncate the journal to just before RunCompleted so the resume must drive a tail,
    // then externally clobber the file with a sentinel.
    let events = seed.load(run).await.unwrap();
    let cut = events.iter().position(|(_, e)| matches!(e, JournalEvent::EffectRecorded { effect_id, .. } if effect_id == &tool_eid)).unwrap();
    assert!(!events[..=cut].iter().any(|(_, e)| matches!(e, JournalEvent::RunCompleted)));
    let seeded = InMemoryJournal::new();
    for (s, e) in &events[..=cut] {
        seeded.append_raw(run, *s, e.clone()).await.unwrap(); // use the same raw-seed helper the sibling resume tests use
    }
    std::fs::write(&file, "SENTINEL").unwrap();

    // --- resume: fs_write memo-replays; the file must NOT be rewritten ---
    let (gw2, _c2) = scripted_gateway(vec![final_response("done")]).await;
    let tools2 = Arc::new(ToolRegistry::default().with_tool(Arc::new(crate::agent::tools::FsWriteTool)));
    let outcome = Executor::new(Arc::new(gw2), Arc::new(seeded.clone()), "v1")
        .with_registry(writer_registry(vec!["fs_write".into()], grants))
        .with_tools(tools2)
        .with_workspace_root(base.path())
        .start(run, &graph)
        .await
        .expect("resume");

    assert!(outcome.failed.is_none() && outcome.paused.is_none(), "failed={:?} paused={:?}", outcome.failed, outcome.paused);
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "SENTINEL", "resume RE-WROTE the file — not exactly-once");
    assert_eq!(effect_recorded_count(&seeded.load(run).await.unwrap(), &tool_eid), 1);
}
```

(Match the journal-seed mechanism to the sibling resume tests — use whatever raw-append/seed helper `broker_not_reinvoked_for_a_memoized_tool_on_resume` used to copy the prefix into a fresh `InMemoryJournal`; the pseudo-`append_raw` above is a placeholder for that exact helper.)

- [ ] **Step 2: Run it → PASS**

Run: `cargo test -p sensei-orchestrator --lib fs_write_replays_from_memo_without_rewriting_on_resume 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 3: Write the redaction-compose test**

A secret written then read back is `[REDACTED]` in the journaled `fs_read` output — wire `.with_redactor(Arc::new(orchestrator_core::PatternRedactor::default()))` and use a pattern-matching secret assembled at runtime.

```rust
/// SP-4 s3 × s2: a secret written to a file then read back is redacted in the fs_read output.
#[tokio::test]
async fn fs_read_output_is_redacted() {
    let base = tempfile::tempdir().unwrap();
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let secret = format!("sk-{}", "abcdefghij0123456789"); // s2 PatternRedactor matches sk-<20+>
    let grants = std::collections::HashMap::from([
        ("fs_write".to_string(), Permissions { paths: vec!["work".into()], ..Default::default() }),
        ("fs_read".to_string(), Permissions { paths: vec!["work".into()], ..Default::default() }),
    ]);
    let (gateway, _c) = scripted_gateway(vec![
        tool_call_response("t1", "fs_write", &format!(r#"{{"path":"work/s","content":"{secret}"}}"#)),
        tool_call_response("t2", "fs_read", r#"{"path":"work/s"}"#),
        final_response("done"),
    ])
    .await;
    let tools = Arc::new(
        ToolRegistry::default()
            .with_tool(Arc::new(crate::agent::tools::FsWriteTool))
            .with_tool(Arc::new(crate::agent::tools::FsReadTool)),
    );
    let outcome = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(writer_registry(vec!["fs_write".into(), "fs_read".into()], grants))
        .with_tools(tools)
        .with_workspace_root(base.path())
        .with_redactor(Arc::new(orchestrator_core::PatternRedactor::default()))
        .run(run, &graph_single("n1"))
        .await
        .unwrap();
    assert!(outcome.failed.is_none());
    let events = journal.load(run).await.unwrap();
    let read_out = recorded_output(&events, &effect_id("n1", 1, 1)).unwrap();
    assert!(!serde_json::to_string(&read_out).unwrap().contains(&secret), "secret leaked in fs_read output");
    // whole-journal scan too
    assert!(!serde_json::to_string(&events).unwrap().contains(&secret), "secret leaked in the journal");
}
```

(Use the file's existing single-node graph helper if there is one, else inline `Graph { nodes: vec![agent_node("n1","a","x")], edges: vec![] }`. Confirm `orchestrator_core::PatternRedactor` is the correct path from a sibling s2 test.)

- [ ] **Step 4: Run it → PASS**, then clippy clean.

- [ ] **Step 5: Additivity + full-suite gate**

Confirm no `workspace_root` wired ⇒ byte-identical (all Task-3/4 tests are additive; the injected default is `None`). Run the whole workspace, read the REAL unpiped exit code + aggregate DIRECTLY (do NOT pipe-to-tail to DECIDE pass):

```bash
cd /Users/Jerry/Developer/gateway
cargo test --workspace > /tmp/ws_fulltest.log 2>&1; echo "EXIT=$?"
grep -c "test result: ok" /tmp/ws_fulltest.log
grep -oE "[0-9]+ passed" /tmp/ws_fulltest.log | awk '{s+=$1} END{print s}'
grep -oE "[1-9][0-9]* failed" /tmp/ws_fulltest.log | head
cargo fmt --all --check; echo "FMT=$?"
cargo clippy --workspace --all-targets -- -D warnings > /tmp/ws_clippy.log 2>&1; echo "CLIPPY=$?"
```

Expected: `EXIT=0`, 0 failed, total ≈ **1083 + ~14** new tests (5 confine + 4 tool + 3 e2e + 2 resume/redaction), `FMT=0`, `CLIPPY=0`. Existing s1/s2/s5/broker suites byte-identical green.

- [ ] **Step 6: Commit** (do NOT push — the coordinator pushes after the whole-slice review)

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/tests.rs
git commit -m "test(orchestrator): SP-4 workspace (4/4) — resume exactly-once + redaction compose + additivity gate"
```

---

## Self-Review notes (author)

- **Spec coverage:** AC1 → Task 1 (confine unit tests). AC2/AC3 → Task 2 (tool unit) + Task 3 (e2e round-trip). AC4 → Task 1 (confine reject) + Task 3 (jail symlink denial, the s1-would-allow case that isolates the jail). AC5 → Task 3 (parallel isolation). AC6 → Task 4 (resume no-rewrite). AC7 → Task 4 (redaction). AC8 → Task 4 (full-suite additive gate).
- **Division of labor (important for reviewers):** `..`, absolute, and un-granted paths are denied by **s1** (lexical `covers`, runs first); the **jail** uniquely adds per-run isolation (AC5) + symlink/canonicalization defense (AC4 symlink) + binding the grant to a live dir. The jail-specific e2e therefore uses a symlink that s1 *grants*, so a passing test proves the jail (not s1) did the denying (mutation check: removing the Task-3 Step-4 pre-check makes `jail_denies_symlink_escape_that_s1_would_allow` fail — the write escapes).
- **Harness placeholders to resolve during implementation:** the exact journal raw-seed helper (Task 4 Step 1 `append_raw`), the single-node graph helper name, `tool_call_response` arg-passing, and `PatternRedactor` import path — all exist in sibling tests (`broker_not_reinvoked_for_a_memoized_tool_on_resume`, the s2 redaction tests); mirror them rather than inventing.
- **Additivity:** every production change is gated on `workspace_root_base.is_some()` (`workspace_root_for` returns `None`, the jail pre-check is skipped, `ToolContext.workspace_root` is `None`) ⇒ unwired = byte-identical.
```
