# SP-4 slice 1 — Tool Permission Enforcement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enforce tool permissions at runtime — before any tool call executes, require `tool ∈ agent.tools` AND `grant.covers(tool.required(args))`; a denied call is fed back to the agent as a journaled tool-result error.

**Architecture:** A single authorization gate at the top of `execute_tool_effect` (the chokepoint every Pure/Observation/Mutation call flows through), reading the acting agent's declared tools + grants and the tool's per-call concrete needs (`Tool::required(args)`, defaulting to the static `spec.permissions`). `covers()` is hardened to component-aware paths + host wildcards. The load-time full-surface grant check is dropped — the grant becomes a runtime ceiling. A denial is recorded as a Pure `EffectRecorded` (no tool run; no `EffectIntent` for a Mutation) so a resume replays it deterministically.

**Tech Stack:** Rust workspace crates `sensei-orchestrator-core` (pure types: `Permissions`/`covers`/`Registry`) and `sensei-orchestrator` (the `Executor` + `Tool`/`ToolRegistry` runtime). Design: `docs/superpowers/specs/2026-08-14-sp4-permission-enforcement-design.md`.

---

## File Structure

- `crates/orchestrator-core/src/registry.rs` **(modify)** — harden `Permissions::covers` (component paths) + `NetworkPolicy::covers` (host wildcards); add pure helpers `path_covers`/`host_covers`; relax `Registry::validate` to the ceiling model.
- `crates/orchestrator/src/agent/tools.rs` **(modify)** — add `Tool::required(&self, args) -> Permissions` (default = static spec); `ToolRegistry::required_of`; a demo `ScopedWriter` Mutation tool whose `required` derives from the `path` arg.
- `crates/orchestrator/src/executor/agent.rs` **(modify)** — carry the acting agent's `tools`+`grants` on `AgentRun`; the authorization gate + `record_denied_effect` helper in `execute_tool_effect`.
- `crates/orchestrator/src/executor/tests.rs` **(modify)** — the gate / denial / resume / e2e tests.

House rules: `cargo fmt --all` before every commit (pre-commit hook = fmt-check + workspace `clippy -D warnings`, runs NO tests). Verify REAL exit codes — read cargo's `test result:` line, never pipe to `tail`/`grep` to decide pass/fail. Do NOT push (the coordinator pushes after the whole-slice review).

---

## Task 1: Harden `covers()` — component-aware paths + host wildcards

**Files:**
- Modify: `crates/orchestrator-core/src/registry.rs` (`Permissions::covers` ~112-118, `NetworkPolicy::covers` ~124-131; add helpers near them; tests in `mod tests`)

- [ ] **Step 1: Write the failing tests**

In `registry.rs` `mod tests`, add:

```rust
    #[test]
    fn covers_paths_are_component_aware() {
        let grant = Permissions {
            paths: vec!["/workspace".into()],
            ..Default::default()
        };
        let inside = Permissions {
            paths: vec!["/workspace/sub/a.txt".into()],
            ..Default::default()
        };
        let sibling = Permissions {
            paths: vec!["/workspace-secret".into()],
            ..Default::default()
        };
        let outside = Permissions {
            paths: vec!["/etc/passwd".into()],
            ..Default::default()
        };
        assert!(grant.covers(&inside));
        assert!(grant.covers(&grant), "a path covers itself");
        assert!(!grant.covers(&sibling), "/workspace must NOT cover /workspace-secret");
        assert!(!grant.covers(&outside));
        let empty_grant = Permissions {
            paths: vec!["".into()],
            ..Default::default()
        };
        assert!(!empty_grant.covers(&inside), "an empty grant path covers nothing");
        let traversal = Permissions {
            paths: vec!["/workspace/../etc".into()],
            ..Default::default()
        };
        assert!(!grant.covers(&traversal), "a `..` traversal need is rejected");
    }

    #[test]
    fn covers_hosts_support_wildcards() {
        let wild = Permissions {
            network: NetworkPolicy::Hosts(vec!["*.example.com".into()]),
            ..Default::default()
        };
        let sub = Permissions {
            network: NetworkPolicy::Hosts(vec!["api.example.com".into()]),
            ..Default::default()
        };
        let evil = Permissions {
            network: NetworkPolicy::Hosts(vec!["example.evil.com".into()]),
            ..Default::default()
        };
        let bare = Permissions {
            network: NetworkPolicy::Hosts(vec!["example.com".into()]),
            ..Default::default()
        };
        assert!(wild.covers(&sub));
        assert!(!wild.covers(&evil), "*.example.com must not cover example.evil.com");
        assert!(!wild.covers(&bare), "*.example.com does not match the bare domain");
        let exact = Permissions {
            network: NetworkPolicy::Hosts(vec!["example.com".into()]),
            ..Default::default()
        };
        assert!(exact.covers(&bare));
        assert!(!exact.covers(&sub), "an exact host grant does not cover a subdomain");
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sensei-orchestrator-core --lib covers_paths_are_component_aware covers_hosts_support_wildcards`
Expected: FAIL — `covers_paths_are_component_aware` fails on the `/workspace-secret` assertion (raw `starts_with` matches it); `covers_hosts_support_wildcards` fails on the wildcard (exact `contains` doesn't match).

- [ ] **Step 3: Add the pure helpers**

In `registry.rs`, add these free functions near the `impl Permissions` block (after `cap_covers`, ~line 150):

```rust
/// Does the grant path `g` cover the need path `n`? Component-aware: `g`'s path
/// segments must be a prefix of `n`'s. `/workspace` covers `/workspace/sub` but not
/// `/workspace-secret`. An empty grant covers nothing; a need containing `..` is
/// rejected (no traversal). Lexical only — symlink/realpath confinement is the
/// sandbox's job (SP-4 slice 4).
fn path_covers(g: &str, n: &str) -> bool {
    if g.is_empty() {
        return false;
    }
    if n.split('/').any(|s| s == "..") {
        return false;
    }
    let gs: Vec<&str> = g.split('/').filter(|s| !s.is_empty()).collect();
    let ns: Vec<&str> = n.split('/').filter(|s| !s.is_empty()).collect();
    gs.len() <= ns.len() && gs.iter().zip(&ns).all(|(a, b)| a == b)
}

/// Does the grant host `g` cover the need host `n`? Exact match, or a `*.suffix`
/// wildcard grant matching any subdomain of `suffix` (not the bare domain).
/// Case-insensitive.
fn host_covers(g: &str, n: &str) -> bool {
    let (g, n) = (g.to_lowercase(), n.to_lowercase());
    match g.strip_prefix("*.") {
        Some(suffix) => n
            .strip_suffix(&suffix)
            .map(|prefix| prefix.ends_with('.'))
            .unwrap_or(false),
        None => g == n,
    }
}
```

- [ ] **Step 4: Rewire `covers()` to use the helpers**

In `Permissions::covers` (~112-118), change the paths line from
`.all(|p| self.paths.iter().any(|g| p.starts_with(g)))`
to:

```rust
        need.paths
            .iter()
            .all(|p| self.paths.iter().any(|g| path_covers(g, p)))
            && need.commands.iter().all(|c| self.commands.contains(c))
            && self.network.covers(&need.network)
            && self.caps.covers(&need.caps)
```

In `NetworkPolicy::covers` (~124-131), change the `Hosts` arm from
`(NetworkPolicy::Hosts(g), NetworkPolicy::Hosts(n)) => n.iter().all(|h| g.contains(h)),`
to:

```rust
            (NetworkPolicy::Hosts(g), NetworkPolicy::Hosts(n)) => {
                n.iter().all(|h| g.iter().any(|gh| host_covers(gh, h)))
            }
```

- [ ] **Step 5: Run the tests to verify they pass + no regressions**

Run: `cargo test -p sensei-orchestrator-core --lib`
Expected: PASS — the two new tests pass; the existing `covers` test (`permissions_cover_*`, ~1076-1100) still passes (its host cases use exact-match grants, which `host_covers` preserves). Read the `test result: ok. N passed; 0 failed` line (real exit 0). NOTE: the existing test `validate_requires_a_grant_covering_a_tools_declared_needs` may still be green here — it is updated in Task 2.

- [ ] **Step 6: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator-core/src/registry.rs
git commit -m "feat(orchestrator): SP-4 s1 (1/5) — harden covers() (component-aware paths + host wildcards + empty-path reject)"
```

---

## Task 2: Relax `validate()` to the ceiling model

**Files:**
- Modify: `crates/orchestrator-core/src/registry.rs` (`Registry::validate` ~358-393; tests ~846-878)

- [ ] **Step 1: Update the existing tests to the new model**

The load-time full-surface grant check is being removed (the grant is now a *runtime* ceiling, D3). Update `mod tests`:

Replace `validate_requires_a_grant_covering_a_tools_declared_needs` (which asserts `PermissionNotGranted`) with the following, using the SAME test helpers the sibling tests use — `tool_needing(name, perms)` and `role_agent(area, kind, chain)` (which names the agent `"role"` and, with `Some("c")`, gives it an explicit routable chain so `validate` doesn't trip `UnknownChainRef`):

```rust
    #[test]
    fn validate_accepts_a_grant_narrower_than_the_tool_surface() {
        // SP-4: a grant narrower than a tool's declared surface is now LEGAL
        // (enforced per-call at runtime), so `validate` no longer errors at load.
        // The tool declares a path need; the agent lists it but grants nothing.
        let tool = tool_needing(
            "fs.write",
            Permissions {
                paths: vec!["/workspace".into()],
                ..Default::default()
            },
        );
        let mut agent = role_agent("coding", "reasoning", Some("c"));
        agent.tools = vec!["fs.write".into()]; // lists the tool, grants nothing
        let reg = Registry::default().with_agent(agent).with_tool(tool);
        assert!(
            reg.validate().is_ok(),
            "a narrower-than-surface (here: absent) grant is legal now"
        );
    }
```

Keep `validate_needs_no_grant_for_a_permissionless_tool` unchanged.

- [ ] **Step 2: Run the updated test to verify it fails**

Run: `cargo test -p sensei-orchestrator-core --lib validate_accepts_a_grant_narrower_than_the_tool_surface`
Expected: FAIL — current `validate` returns `Err(PermissionNotGranted)` for a tool with declared needs and no covering grant.

- [ ] **Step 3: Remove the full-surface grant check from `validate`**

In `Registry::validate` (~368-385), the tool loop currently resolves the spec (keep that — `UnknownToolRef`) then checks `grant.covers(spec.permissions)`. Delete the grant-covers block so the loop keeps only the structural resolution:

```rust
            for tool in &agent.tools {
                if !self.tools.contains_key(tool) {
                    return Err(OrchestratorError::UnknownToolRef {
                        agent: agent.name.clone(),
                        tool: tool.clone(),
                    });
                }
            }
```

(Remove the `let Some(spec) = ...`/`no_grant`/`grant.covers`/`PermissionNotGranted` lines. Leave the `UnknownSkillRef` and `UnknownChainRef` checks untouched. Leave the `PermissionNotGranted` error variant in `error.rs` — it is a public variant, harmless if now unused, and may be reused by a future strict opt-in.) Update the doc-comment on `validate` to say it checks *structural* resolvability only (grants are now enforced per-call at runtime, SP-4 s1).

- [ ] **Step 4: Run the tests to verify pass**

Run: `cargo test -p sensei-orchestrator-core --lib`
Expected: PASS — `validate_accepts_a_grant_narrower_than_the_tool_surface` passes; `validate_needs_no_grant_for_a_permissionless_tool` still passes; no other core test regresses. Real exit 0.

- [ ] **Step 5: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator-core/src/registry.rs
git commit -m "feat(orchestrator): SP-4 s1 (2/5) — relax validate() to the ceiling model (grant enforced per-call at runtime)"
```

---

## Task 3: `Tool::required` + `ToolRegistry::required_of` + the `ScopedWriter` demo tool

**Files:**
- Modify: `crates/orchestrator/src/agent/tools.rs` (`Tool` trait ~16-19; `ToolRegistry` ~28-52; add `ScopedWriter`; tests in this file's `mod tests` or `executor/tests.rs`)

- [ ] **Step 1: Write the failing tests**

In `crates/orchestrator/src/agent/tools.rs` `#[cfg(test)] mod tests` (add one if absent), add:

```rust
    #[test]
    fn required_defaults_to_spec_and_overrides_use_args() {
        // Default impl: a tool that doesn't override returns its static declaration.
        assert_eq!(Calc.required(&serde_json::json!({})), Calc.spec().permissions);
        // Override: ScopedWriter derives the concrete path need from the `path` arg.
        let w = ScopedWriter::new(std::sync::Arc::new(std::sync::Mutex::new(Vec::new())));
        let need = w.required(&serde_json::json!({"path":"/workspace/a.txt","content":"x"}));
        assert_eq!(need.paths, vec!["/workspace/a.txt".to_string()]);
        // Missing path → no concrete path need (the gate allows; the call errors).
        assert!(w.required(&serde_json::json!({})).paths.is_empty());
    }

    #[test]
    fn required_of_reads_the_registry_or_defaults_empty() {
        let reg = ToolRegistry::default()
            .with_tool(std::sync::Arc::new(ScopedWriter::new(std::sync::Arc::new(
                std::sync::Mutex::new(Vec::new()),
            ))));
        assert_eq!(
            reg.required_of("fs.write", &serde_json::json!({"path":"/x"})).paths,
            vec!["/x".to_string()]
        );
        assert_eq!(
            reg.required_of("unknown", &serde_json::json!({})),
            orchestrator_core::Permissions::default()
        );
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sensei-orchestrator --lib required_defaults_to_spec_and_overrides_use_args required_of_reads_the_registry_or_defaults_empty`
Expected: FAIL — `required`, `required_of`, and `ScopedWriter` do not exist yet.

- [ ] **Step 3: Add `Tool::required` default + `ToolRegistry::required_of`**

In the `Tool` trait (~16-19), add a default method:

```rust
pub trait Tool: Send + Sync {
    fn spec(&self) -> ToolSpec;
    fn call(&self, args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError>;

    /// The CONCRETE permissions THIS specific call needs (SP-4 authorization gate).
    /// Default = the tool's static declared surface (`spec().permissions`), so tools
    /// with no permission-relevant arguments need no change. Tools whose arguments
    /// carry a path/host/command override this. Must be PURE (replay-stable).
    fn required(&self, _args: &serde_json::Value) -> Permissions {
        self.spec().permissions
    }
}
```

In `impl ToolRegistry` (~28-52), add:

```rust
    /// The concrete permissions the named tool needs for `args` (for the gate).
    /// Unknown tool → empty `Permissions` (which the separate `tool ∈ agent.tools`
    /// check denies).
    pub fn required_of(&self, name: &str, args: &serde_json::Value) -> Permissions {
        self.tools
            .get(name)
            .map(|t| t.required(args))
            .unwrap_or_default()
    }
```

- [ ] **Step 4: Add the `ScopedWriter` demo tool**

In `tools.rs`, near `RecordNote` (~168-210), add (model the struct/`new`/`call` on `RecordNote`):

```rust
/// Demo Mutation tool with a permission-relevant argument: writes to a path.
/// Its static surface is `/workspace`, but the gate authorizes each call against
/// the agent's grant using the CONCRETE path in `required(args)`. `call` just
/// records the path in a sink (the "filesystem" being mutated) — no real I/O.
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
        let path = args
            .get("path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OrchestratorError::Tool {
                tool: "fs.write".into(),
                message: "missing 'path'".into(),
            })?;
        self.sink.lock().unwrap().push(path.to_string());
        Ok(serde_json::json!({ "written": path }))
    }
}
```

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p sensei-orchestrator --lib required_defaults_to_spec_and_overrides_use_args required_of_reads_the_registry_or_defaults_empty`
Expected: PASS (real exit 0). Also `cargo test -p sensei-orchestrator --lib` broadly still green (the new default method changes no existing tool).

- [ ] **Step 6: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/agent/tools.rs
git commit -m "feat(orchestrator): SP-4 s1 (3/5) — Tool::required(args) + ToolRegistry::required_of + ScopedWriter demo tool"
```

---

## Task 4: The authorization gate in `execute_tool_effect`

**Files:**
- Modify: `crates/orchestrator/src/executor/agent.rs` (`AgentRun` ~32-40; `drive_agent` construction ~55-90; `execute_tool_effect` ~289-337; add `record_denied_effect`)
- Modify: `crates/orchestrator/src/executor/tests.rs` (gate tests)

- [ ] **Step 1: Carry the acting agent's tools + grants on `AgentRun`**

In `AgentRun<'a>` (~32-40) add two owned fields (owned clones avoid lifetime coupling; built once per agent-node-run):

```rust
struct AgentRun<'a> {
    run: RunId,
    node_id: &'a NodeId,
    chain: String,
    system: String,
    tools: Vec<ToolDefinition>,
    min_win: Option<u32>,
    fold: &'a Fold,
    agent_tools: Vec<String>,
    agent_grants: std::collections::HashMap<String, orchestrator_core::Permissions>,
}
```

In `drive_agent`, the `AgentDefinition` is already resolved as `agent` (~65). At the `AgentRun { .. }` construction (~73), add:

```rust
            agent_tools: agent.tools.clone(),
            agent_grants: agent.grants.clone(),
```

- [ ] **Step 2: Add the gate + `record_denied_effect`, then write the failing tests**

In `execute_tool_effect` (~289-337), insert the gate on the LIVE path — AFTER the memo-hit block (the `if let Some((recorded_ih, output)) = ar.fold.memo.get(teid) { … }` at ~306-317) and BEFORE the `match class` (~322):

```rust
        // SP-4 s1 authorization gate: the acting agent must LIST this tool AND hold
        // a grant covering the concrete permissions THIS call needs. Denials are fed
        // back to the agent (recorded as a Pure effect ⇒ replayed on resume). Runs on
        // the LIVE path only — a memo hit above already replayed a recorded allow/deny.
        let need = self.tools.required_of(&call.name, &args);
        let no_grant = orchestrator_core::Permissions::default();
        let grant = ar.agent_grants.get(&call.name).unwrap_or(&no_grant);
        let listed = ar.agent_tools.iter().any(|t| t == &call.name);
        if !(listed && grant.covers(&need)) {
            return self
                .record_denied_effect(ar, teid, call, &tih, &need, grant, listed)
                .await;
        }
```

Add the helper method (near `record_tool_effect`, ~448):

```rust
    /// Record a permission denial as the call's effect output — a Pure, memoize-
    /// forever `EffectRecorded` with NO tool execution (and, for a Mutation, NO
    /// `EffectIntent`) — and feed it back to the agent. The decision is a pure fn of
    /// (config grant, call args) ⇒ a resume replays it from the memo, tool never run.
    async fn record_denied_effect(
        &self,
        ar: &AgentRun<'_>,
        teid: &EffectId,
        call: &ToolCall,
        tih: &str,
        need: &orchestrator_core::Permissions,
        grant: &orchestrator_core::Permissions,
        listed: bool,
    ) -> Result<ToolOutcome<serde_json::Value>, OrchestratorError> {
        let detail = if !listed {
            format!("tool '{}' is not in the agent's declared tools", call.name)
        } else {
            format!(
                "call needs {:?} which the grant {:?} does not cover",
                need, grant
            )
        };
        let denial = serde_json::json!({
            "error": "permission_denied",
            "tool": call.name,
            "detail": detail,
        });
        let recorded = self.split_output(&denial).await?;
        self.append(
            ar.run,
            JournalEvent::EffectRecorded {
                node: ar.node_id.clone(),
                effect_id: teid.clone(),
                class: EffectClass::Pure,
                input_hash: tih.to_string(),
                seq: 0,
                output: recorded,
                observation: None,
            },
        )
        .await?;
        Ok(ToolOutcome::Ok(denial))
    }
```

Then in `crates/orchestrator/src/executor/tests.rs`, add the three gate tests. **Study the existing slice-4 tool tests first** (grep `RecordNote`, `scripted_gateway`, `tool_call_response`, `final_response`, and how an agent that CALLS a tool is registered + driven — mirror that harness exactly). The three tests, with an agent `writer` that lists `fs.write` and a `ScopedWriter` in the `ToolRegistry`:

- `granted_tool_call_executes` (AC4): agent grants `fs.write` `{paths:["/workspace"]}`; scripted gateway emits a tool_call `fs.write {path:"/workspace/a.txt", content:"x"}` then a final answer. Assert the run completes, the sink contains `/workspace/a.txt` (the tool ran), and the journal has the Mutation's `EffectIntent`+`EffectRecorded`.
- `ungranted_tool_is_denied_and_fed_back` (AC5): agent lists `fs.write` but its `grants` is EMPTY; scripted gateway emits `fs.write {path:"/workspace/a.txt"}` then a final answer. Assert: the sink is EMPTY (tool never ran), the tool's transcript result is the `{"error":"permission_denied",...}` value, the journal has an `EffectRecorded` for the call but **NO `EffectIntent`** (denied Mutation skips two-phase), and the run completes. Also a variant where the tool is NOT in `agent.tools` at all → same denial.
- `out_of_grant_argument_denied_then_in_grant_succeeds` (AC6): agent grants `fs.write` `{paths:["/workspace"]}`; scripted gateway emits (turn0) `fs.write {path:"/etc/passwd"}` → denied+fed back, then (turn1) `fs.write {path:"/workspace/ok.txt"}` → allowed, then a final answer. Assert the sink contains ONLY `/workspace/ok.txt` (the first was denied, the second ran), and both the denial `EffectRecorded` and the success `EffectIntent`+`EffectRecorded` are journaled at distinct tool effect ids.

Build the scripted-gateway sequence empirically (mirror the count discipline from the slice-4 tool tests: one gateway response per model turn; each tool_call turn is one response, the tool executes locally). If the sequence is off the test errors "gateway exhausted" / wrong-order — debug to a REAL pass; do not weaken assertions.

- [ ] **Step 3: Run to verify failure (before the gate) / then pass (after)**

Run: `cargo test -p sensei-orchestrator granted_tool_call_executes ungranted_tool_is_denied_and_fed_back out_of_grant_argument_denied_then_in_grant_succeeds`
Expected: with the gate in place, all three PASS (real exit 0). (If you write the tests before wiring the gate to see them fail first per TDD, the two denial tests fail — the tool runs / sink is populated — proving the gate is load-bearing.)

- [ ] **Step 4: Regression check**

Run: `cargo test -p sensei-orchestrator` — the full orchestrator suite still green (the gate is inert for empty-permission tools: existing agents/tools declare empty `permissions`, so `required` is empty and `grant.covers(empty)` is true, unless the tool isn't listed — verify the existing agent-tool e2e tests list their tools; they do). Read the `test result:` line, real exit 0. If any existing test now denies a call, it means that test's agent doesn't list the tool it calls — fix by adding the tool to the agent's `tools`, matching the additive intent (report any such fix).

- [ ] **Step 5: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/agent.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-4 s1 (4/5) — authorization gate in execute_tool_effect (tool∈tools ∧ grant.covers(required(args))); denial fed back"
```

---

## Task 5: Determinism-on-resume + e2e + full-suite gate

**Files:**
- Modify: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Determinism-on-resume test (AC7)**

Add `a_denied_call_replays_from_the_memo_on_resume`: run an agent that hits a denial (agent lists `fs.write`, empty grant, gateway emits `fs.write {path:"/x"}` then would continue), but seed a PARTIAL journal so the run journals the denial `EffectRecorded` then fails/exhausts before completing. Resume over the same journal with a FRESH gateway/tool-registry whose `ScopedWriter` sink would record a write if the tool ran. Assert: the resume replays the denial from the memo (the sink stays EMPTY — the tool is NOT re-invoked), the run reaches the same state, and the denial `EffectRecorded` for that tool effect id appears **exactly once** across both journals (recorded live once, replayed not re-recorded). Mirror the resume-truncation idiom from the slice-4B / gate-agent resume tests (`effect_recorded_count(...) == 1`). Mutation-verify: note that removing the memo-hit short-circuit would re-run the gate on resume — which still denies (deterministic), so the load-bearing assertion is "the tool is never invoked AND the effect is recorded exactly once."

- [ ] **Step 2: End-to-end (AC9)**

Add `agent_hits_a_denial_adapts_and_succeeds`: the AC6 shape driven end-to-end through the test gateway harness (the `demo_reference_tool_gateway`-style adapter or a scripted gateway) — a `writer` agent with a narrow `/workspace` grant, first attempting an out-of-scope path (denied, fed back into its transcript), then a valid `/workspace/...` path (succeeds), then completing. Assert the final run completes, the sink holds only the in-scope write, and the denial appears in the agent transcript / journal. (Reuse the Task-4 harness; this is the "narrow grant → adapt → succeed" journey.)

- [ ] **Step 3: Full-workspace + additive gate (AC8 + AC11)**

Run: `cargo test --workspace` — read the REAL aggregate + exit code directly (NOT piped). Confirm 0 failed; report the total. The relaxed `validate` (Task 2) means no test still asserts `PermissionNotGranted` for a narrower-than-surface grant; empty-permission tools stay byte-identical.

- [ ] **Step 4: Lint gate**

Run: `cargo fmt --all --check` (exit 0) + `cargo clippy --workspace --all-targets -- -D warnings` (exit 0, read the real unpiped exit code).

- [ ] **Step 5: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/tests.rs
git commit -m "test(orchestrator): SP-4 s1 (5/5) — denial resume-determinism + narrow-grant adapt-and-succeed e2e; full-suite green"
```

---

## Acceptance Criteria → Task map (self-review)

| Spec AC | Task | Test |
|---|---|---|
| 1 covers() component paths | 1 | `covers_paths_are_component_aware` |
| 2 covers() host wildcards | 1 | `covers_hosts_support_wildcards` |
| 3 required(args) default + override | 3 | `required_defaults_to_spec_and_overrides_use_args`, `required_of_reads_the_registry_or_defaults_empty` |
| 4 granted call executes | 4 | `granted_tool_call_executes` |
| 5 ungranted tool denied (no Intent) | 4 | `ungranted_tool_is_denied_and_fed_back` |
| 6 in-scope out-of-grant arg denied → adapt | 4 | `out_of_grant_argument_denied_then_in_grant_succeeds` |
| 7 denial deterministic on resume | 5 | `a_denied_call_replays_from_the_memo_on_resume` |
| 8 additive + relaxed validate | 2, 5 | `validate_accepts_a_grant_narrower_than_the_tool_surface` + `cargo test --workspace` |
| 9 end-to-end | 5 | `agent_hits_a_denial_adapts_and_succeeds` |

**Deferred (spec §6, NOT in this plan):** runtime confinement + resource-cap killing (sandbox slice 4); robust shell-argv parsing; path canonicalization; secret redaction (slice 2); unifying the two `ToolSpec` copies; an `on_tool_denied` hook (dropped from this slice to avoid the `OrchestratorHooks` trait change — the journaled `EffectRecorded` is the audit record).

**Self-review notes:** (1) every spec §7 AC maps to a task above. (2) No placeholders — all code shown; the executor tests (Task 4/5) intentionally give structure + assertions + a pointer to the exact slice-4 harness to mirror (the scripted-gateway sequencing is idiom-heavy, same approach used for the SP-3 s5 e2e). (3) Type consistency: `Permissions`/`NetworkPolicy`/`ToolSpec`/`Tool::required`/`ToolRegistry::required_of`/`AgentRun.agent_tools`/`agent_grants`/`record_denied_effect`/`EffectClass::Pure` `EffectRecorded` shape all match the real signatures read from the code.
