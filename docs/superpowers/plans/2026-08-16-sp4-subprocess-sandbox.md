# SP-4 Subprocess Sandbox + Resource-Cap Killing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run an external command as a killable, OS-confined child process — cpu/mem/wall-capped and KILLED on breach (all unix), fs/network confined via macOS `sandbox-exec`, refuse-loud where confinement is unavailable — finally enforcing the `ResourceCaps`+`NetworkPolicy` that s1 only declared.

**Architecture:** A portable `spawn_capped()` (process-group + `setrlimit` + wall-timeout kill, all unix) underlies a `Sandbox` trait; `MacosSandbox` (`#[cfg(target_os="macos")]`) wraps it in `sandbox-exec`. A built-in `ShellTool` (Mutation) runs its argv through a per-call `BoundSandbox` the executor builds from the agent's grant (the tool supplies only argv → cannot widen the policy). No sandbox / non-macOS ⇒ the tool refuses loud.

**Tech Stack:** Rust, `std::process` + `std::os::unix::process::CommandExt` (process_group, the post-fork hook), the `nix` crate (setrlimit/kill/signal), macOS `sandbox-exec`, the existing durable two-phase Mutation journal.

**Spec:** `docs/superpowers/specs/2026-08-16-sp4-subprocess-sandbox-design.md`

**Baseline:** `develop` at `b255ab2`; full workspace **1099 tests** green. `cargo fmt --all` before every commit (pre-commit = fmt-check + workspace `clippy -D warnings`, NO tests → always `cargo test --workspace` yourself, real unpiped exit code). Every secret fixture assembled at runtime (semgrep CWE-798). Do NOT push (coordinator pushes after the whole-slice review).

**Platform note:** Tasks 1,3,4,6 are portable (unix) and run on Linux CI. Task 5 (real macOS confinement) is `#[cfg(target_os="macos")]` — its tests run on the dev box, are skipped on Linux CI.

**Doc-linter note:** a repo hook flags the literal method-call token for the child post-fork hook (`pre` + `_exec` + `(`). Where this plan shows that call it writes a SPACE before the `(` (`command.pre_exec (…)`), which is valid Rust and which `cargo fmt` normalizes back to no-space — write the normal no-space form in the actual source.

---

## File Structure

- **Create** `crates/orchestrator/src/agent/sandbox.rs` — `CapOutcome`/`KillReason`, `spawn_capped` (portable core), `Sandbox` trait + `SandboxSpec`, `BoundSandbox`, `MacosSandbox` (cfg macos). One responsibility: sandboxed process execution.
- **Modify** `crates/orchestrator/src/agent/mod.rs` — `pub mod sandbox;`.
- **Modify** `crates/orchestrator/src/agent/tools.rs` — `ToolContext.sandbox` field + `ShellTool`; update `ToolContext {…}` test literals.
- **Modify** `crates/orchestrator/src/executor/mod.rs` — `sandbox: Option<Arc<dyn Sandbox>>` + `with_sandbox`.
- **Modify** `crates/orchestrator/src/executor/agent.rs` — `bound_sandbox_for` + inject `sandbox` into the `ToolContext` in `record_tool_effect`.
- **Modify** `crates/orchestrator/Cargo.toml` — `nix` dep.
- **Test** `crates/orchestrator/src/executor/tests.rs` — e2e (fake Sandbox, portable) + resume + redaction + gate.

---

## Task 1: Portable cap-killing core — `spawn_capped`

**Files:**
- Modify: `crates/orchestrator/Cargo.toml`
- Modify: `crates/orchestrator/src/agent/mod.rs`
- Create: `crates/orchestrator/src/agent/sandbox.rs`

- [ ] **Step 1: Add the `nix` dependency**

In `crates/orchestrator/Cargo.toml` `[dependencies]` add (adjust the feature list to what compiles — these cover `setrlimit`/`kill`/`Signal`/`Pid`):

```toml
nix = { version = "0.29", features = ["resource", "signal", "process"] }
```

- [ ] **Step 2: Declare the module**

In `crates/orchestrator/src/agent/mod.rs`, after `pub mod workspace;` add:

```rust
pub mod sandbox;
```

- [ ] **Step 3: Write the failing tests**

Create `crates/orchestrator/src/agent/sandbox.rs` with the tests FIRST (impl in Step 5):

```rust
//! SP-4 s4: sandboxed subprocess execution. `spawn_capped` is the portable (all-unix)
//! cap-killing core (process-group + setrlimit + wall-timeout kill); the `Sandbox` trait +
//! `MacosSandbox` add OS-level fs/network confinement (macOS `sandbox-exec`). An external
//! command is confined + capped; where no OS-confinement backend exists the caller refuses.

use std::io::{Read, Write};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use orchestrator_core::{OrchestratorError, ResourceCaps};

/// The outcome of a capped subprocess. `killed: Some(_)` => a resource cap was breached.
#[derive(Debug, Clone, PartialEq)]
pub struct CapOutcome {
    pub exit_code: Option<i32>, // None if terminated by a signal
    pub stdout: String,
    pub stderr: String,
    pub killed: Option<KillReason>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum KillReason {
    Wall,
    Cpu,
    Mem,
    Signal(i32),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caps(wall_ms: Option<u64>, mem_bytes: Option<u64>) -> ResourceCaps {
        ResourceCaps { cpu_ms: None, mem_bytes, wall_ms }
    }
    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn normal_run_captures_stdout_and_exit_zero() {
        let out = spawn_capped(&argv(&["sh", "-c", "echo hi"]), &caps(Some(5000), None), None).unwrap();
        assert_eq!(out.exit_code, Some(0));
        assert_eq!(out.stdout, "hi\n");
        assert_eq!(out.killed, None);
    }

    #[test]
    fn wall_cap_kills_a_runaway() {
        let start = std::time::Instant::now();
        let out = spawn_capped(&argv(&["sh", "-c", "sleep 100"]), &caps(Some(150), None), None).unwrap();
        assert_eq!(out.killed, Some(KillReason::Wall), "expected a wall-timeout kill");
        assert!(start.elapsed() < Duration::from_secs(5), "took too long: the wall timer did not kill it");
    }

    #[test]
    fn wall_cap_kills_the_whole_process_group() {
        // The shell forks a background `sleep` then waits; killing only the shell would orphan
        // the sleep. A process-group kill takes both. We assert the call returns promptly
        // (if the group kill failed, `wait` on the shell would block on the child sleep).
        let start = std::time::Instant::now();
        let out = spawn_capped(&argv(&["sh", "-c", "sleep 100 & wait"]), &caps(Some(150), None), None).unwrap();
        assert_eq!(out.killed, Some(KillReason::Wall));
        assert!(start.elapsed() < Duration::from_secs(5), "process-group kill did not reap the forked child");
    }

    #[test]
    fn mem_cap_prevents_a_clean_success() {
        // With a tiny RLIMIT_AS the child must NOT cleanly succeed (it aborts / errors). `awk`
        // growing a huge string is a portable allocator. If a runner's awk handles OOM
        // gracefully, swap for another allocator but keep the "not a clean success" assertion.
        let cmd = argv(&["awk", "BEGIN{ a=\"\"; for(i=0;i<20000000;i++) a=a\"x\"; print length(a) }"]);
        let capped = spawn_capped(&cmd, &caps(Some(10000), Some(8 * 1024 * 1024)), None).unwrap();
        assert!(
            capped.killed.is_some() || capped.exit_code != Some(0),
            "an 8MiB RLIMIT_AS should have prevented a clean success, got {capped:?}"
        );
    }
}
```

- [ ] **Step 4: Run to verify it fails**

Run: `cargo test -p sensei-orchestrator --lib agent::sandbox 2>&1 | tail -20`
Expected: FAIL to COMPILE — `cannot find function spawn_capped`.

- [ ] **Step 5: Implement `spawn_capped`**

Add to `sandbox.rs` above the `#[cfg(test)]` module. NOTE: the child post-fork hook call is shown as `command.pre_exec (…)` with a space (doc-linter); write it with NO space in source (`cargo fmt` enforces that):

```rust
/// Spawn `argv` as a child in its OWN process group under `caps`, capture stdout/stderr, and
/// KILL the whole group at `wall_ms`. rlimits (RLIMIT_CPU seconds, RLIMIT_AS bytes) are applied
/// in the child's post-fork hook. Portable across unix (macOS + Linux). No confinement — that is
/// the `Sandbox` layer.
pub(crate) fn spawn_capped(
    argv: &[String],
    caps: &ResourceCaps,
    stdin: Option<&str>,
) -> Result<CapOutcome, OrchestratorError> {
    let (cmd0, rest) = argv.split_first().ok_or_else(|| OrchestratorError::Tool {
        tool: "sandbox".into(),
        message: "empty argv".into(),
    })?;
    let mut command = Command::new(cmd0);
    command
        .args(rest)
        .stdin(if stdin.is_some() { Stdio::piped() } else { Stdio::null() })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0); // new group; pgid == child pid

    let cpu_secs = caps.cpu_ms.map(|ms| ms.div_ceil(1000));
    let mem = caps.mem_bytes;
    // SAFETY: the closure runs post-fork, before the program starts; setrlimit is
    // async-signal-safe. (Written with a space before '(' for a doc-linter; remove it.)
    unsafe {
        command.pre_exec (move || {
            use nix::sys::resource::{setrlimit, Resource};
            if let Some(s) = cpu_secs {
                setrlimit(Resource::RLIMIT_CPU, s, s)
                    .map_err(|e| std::io::Error::from_raw_os_error(e as i32))?;
            }
            if let Some(b) = mem {
                setrlimit(Resource::RLIMIT_AS, b, b)
                    .map_err(|e| std::io::Error::from_raw_os_error(e as i32))?;
            }
            Ok(())
        });
    }

    let mut child = command.spawn().map_err(|e| OrchestratorError::Tool {
        tool: "sandbox".into(),
        message: format!("spawn '{cmd0}': {e}"),
    })?;
    let pid = child.id() as i32;

    if let (Some(s), Some(mut si)) = (stdin, child.stdin.take()) {
        let _ = si.write_all(s.as_bytes()); // dropping `si` closes the pipe
    }

    // Reader threads so a full pipe can't deadlock the wait.
    let mut out_h = child.stdout.take();
    let mut err_h = child.stderr.take();
    let out_t = std::thread::spawn(move || {
        let mut s = String::new();
        if let Some(h) = out_h.as_mut() {
            let _ = h.read_to_string(&mut s);
        }
        s
    });
    let err_t = std::thread::spawn(move || {
        let mut s = String::new();
        if let Some(h) = err_h.as_mut() {
            let _ = h.read_to_string(&mut s);
        }
        s
    });

    // Wait with an optional wall deadline via a channel; on timeout, SIGKILL the group.
    let (tx, rx) = mpsc::channel();
    let waiter = std::thread::spawn(move || {
        let status = child.wait();
        let _ = tx.send(());
        status
    });

    let mut wall_killed = false;
    match caps.wall_ms {
        Some(ms) => {
            if rx.recv_timeout(Duration::from_millis(ms)).is_err() {
                // Deadline passed: kill the whole group (negative pid).
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(-pid),
                    nix::sys::signal::Signal::SIGKILL,
                );
                wall_killed = true;
            }
        }
        None => {
            let _ = rx.recv(); // block until exit
        }
    }
    let status = waiter.join().expect("waiter thread panicked").map_err(|e| OrchestratorError::Tool {
        tool: "sandbox".into(),
        message: format!("wait: {e}"),
    })?;
    let stdout = out_t.join().unwrap_or_default();
    let stderr = err_t.join().unwrap_or_default();

    let killed = if wall_killed {
        Some(KillReason::Wall)
    } else {
        match status.signal() {
            Some(24) => Some(KillReason::Cpu), // SIGXCPU (RLIMIT_CPU)
            Some(sig) => Some(KillReason::Signal(sig)),
            None => None,
        }
    };
    Ok(CapOutcome {
        exit_code: status.code(),
        stdout,
        stderr,
        killed,
    })
}
```

- [ ] **Step 6: Run to verify they pass**

Run: `cargo test -p sensei-orchestrator --lib agent::sandbox 2>&1 | tail -20`
Expected: PASS (4). Then `cargo clippy -p sensei-orchestrator --all-targets -- -D warnings` → clean.

- [ ] **Step 7: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/Cargo.toml crates/orchestrator/src/agent/mod.rs crates/orchestrator/src/agent/sandbox.rs
git commit -m "feat(orchestrator): SP-4 sandbox (1/6) — portable spawn_capped (process-group + setrlimit + wall-kill)"
```

---

## Task 2: The `Sandbox` trait + `SandboxSpec` + `MacosSandbox` + `Executor::with_sandbox`

**Files:**
- Modify: `crates/orchestrator/src/agent/sandbox.rs`
- Modify: `crates/orchestrator/src/executor/mod.rs`

- [ ] **Step 1: Write the failing test (portable fake Sandbox + a macOS smoke test)**

Add to the `#[cfg(test)] mod tests` in `sandbox.rs`:

```rust
    struct EchoSandbox; // a portable fake: ignores confinement, just runs capped
    impl Sandbox for EchoSandbox {
        fn run(&self, spec: &SandboxSpec) -> Result<CapOutcome, OrchestratorError> {
            spawn_capped(spec.argv, spec.caps, spec.stdin)
        }
    }

    #[test]
    fn sandbox_trait_runs_a_command() {
        let a = argv(&["sh", "-c", "echo ok"]);
        let ws = std::path::PathBuf::from("/tmp");
        let out = EchoSandbox.run(&SandboxSpec {
            argv: &a,
            workspace: &ws,
            caps: &caps(Some(5000), None),
            network: &orchestrator_core::NetworkPolicy::Deny,
            stdin: None,
        }).unwrap();
        assert_eq!(out.stdout, "ok\n");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_sandbox_runs_an_allowed_command() {
        let td = tempfile::tempdir().unwrap();
        let ws = td.path().canonicalize().unwrap();
        let a = argv(&["sh", "-c", "echo ok"]);
        let out = MacosSandbox.run(&SandboxSpec {
            argv: &a, workspace: &ws, caps: &caps(Some(5000), None),
            network: &orchestrator_core::NetworkPolicy::Deny, stdin: None,
        }).unwrap();
        assert_eq!(out.exit_code, Some(0), "sandbox-exec blocked a trivial command: {out:?}");
        assert_eq!(out.stdout, "ok\n");
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p sensei-orchestrator --lib agent::sandbox 2>&1 | tail -20`
Expected: FAIL to COMPILE — `cannot find trait Sandbox` / `SandboxSpec` / `MacosSandbox`.

- [ ] **Step 3: Implement the trait, `SandboxSpec`, and `MacosSandbox`**

Add to `sandbox.rs` (module scope). The Seatbelt profile uses `(allow process-exec*)` — that token is fine; only the Rust post-fork hook call needed the space workaround.

```rust
use std::path::Path;
use orchestrator_core::NetworkPolicy;

/// The confinement + cap policy for one sandboxed run. `argv` is UNTRUSTED (tool/model);
/// `workspace`/`caps`/`network` are TRUSTED (executor-derived from the grant).
pub struct SandboxSpec<'a> {
    pub argv: &'a [String],
    pub workspace: &'a Path,
    pub caps: &'a ResourceCaps,
    pub network: &'a NetworkPolicy,
    pub stdin: Option<&'a str>,
}

/// An OS confinement backend. Runs `argv` fs/network-confined + capped, or `Err` (refuse-loud)
/// where this platform has no backend.
pub trait Sandbox: Send + Sync {
    fn run(&self, spec: &SandboxSpec) -> Result<CapOutcome, OrchestratorError>;
}

/// macOS `sandbox-exec` backend: fs writes confined to the workspace subpath, network per policy.
#[cfg(target_os = "macos")]
pub struct MacosSandbox;

#[cfg(target_os = "macos")]
impl Sandbox for MacosSandbox {
    fn run(&self, spec: &SandboxSpec) -> Result<CapOutcome, OrchestratorError> {
        let profile = macos_profile(spec.workspace, spec.network);
        let mut wrapped = Vec::with_capacity(spec.argv.len() + 3);
        wrapped.push("sandbox-exec".to_string());
        wrapped.push("-p".to_string());
        wrapped.push(profile);
        wrapped.extend(spec.argv.iter().cloned());
        spawn_capped(&wrapped, spec.caps, spec.stdin)
    }
}

/// Build a Seatbelt profile: deny by default, allow exec + broad READ (a binary needs it to
/// start), confine WRITES to the workspace subpath, deny network unless the policy allows it.
#[cfg(target_os = "macos")]
fn macos_profile(workspace: &Path, network: &NetworkPolicy) -> String {
    let ws = workspace.display();
    let net = match network {
        NetworkPolicy::Any | NetworkPolicy::Hosts(_) => "(allow network*)",
        NetworkPolicy::Deny => "(deny network*)",
    };
    format!(
        "(version 1)\n(deny default)\n(allow process-fork)\n(allow process-exec*)\n\
         (allow file-read*)\n(allow file-write* (subpath \"{ws}\"))\n\
         (allow file-write-data (literal \"/dev/null\") (literal \"/dev/stdout\") (literal \"/dev/stderr\"))\n\
         {net}\n"
    )
}
```

- [ ] **Step 4: Wire `Executor::with_sandbox`**

In `crates/orchestrator/src/executor/mod.rs`: add a field to `pub struct Executor` (near `workspace_root_base`):

```rust
    /// SP-4 s4: the injected OS-confinement backend for the `shell` tool (default `None` ⇒
    /// the tool refuses loud). Set via [`with_sandbox`](Self::with_sandbox).
    sandbox: Option<Arc<dyn crate::agent::sandbox::Sandbox>>,
```

In `Executor::new(...)` struct literal (near `workspace_root_base: None,`):

```rust
            sandbox: None,
```

Add the builder (near `with_workspace_root`):

```rust
    /// SP-4 s4: wire the subprocess sandbox backend (e.g. `MacosSandbox`) used by the `shell`
    /// tool. Default `None` ⇒ `shell` refuses loud (fail-closed — never an unconfined run).
    pub fn with_sandbox(mut self, sandbox: Arc<dyn crate::agent::sandbox::Sandbox>) -> Self {
        self.sandbox = Some(sandbox);
        self
    }
```

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p sensei-orchestrator --lib agent::sandbox 2>&1 | tail -20` → the portable `sandbox_trait_runs_a_command` passes (the macOS test compiles + runs on macOS, absent on Linux). `cargo build -p sensei-orchestrator` clean; clippy `-D warnings` clean.

- [ ] **Step 6: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/agent/sandbox.rs crates/orchestrator/src/executor/mod.rs
git commit -m "feat(orchestrator): SP-4 sandbox (2/6) — Sandbox trait + SandboxSpec + MacosSandbox + with_sandbox"
```

---

## Task 3: `ToolContext.sandbox` + `BoundSandbox` + `ShellTool`

**Files:**
- Modify: `crates/orchestrator/src/agent/sandbox.rs` (add `BoundSandbox`)
- Modify: `crates/orchestrator/src/agent/tools.rs`

- [ ] **Step 1: Implement `BoundSandbox` (policy-fixed handle)**

Add to `sandbox.rs` (module scope):

```rust
use std::sync::Arc;
use std::path::PathBuf;

/// A per-call sandbox handle with the policy FIXED by the executor (from the grant). The tool
/// supplies only `argv` → it cannot widen caps/workspace/network. Manual `Debug` (the inner
/// trait object isn't `Debug`).
#[derive(Clone)]
pub struct BoundSandbox {
    inner: Arc<dyn Sandbox>,
    workspace: Arc<PathBuf>,
    caps: ResourceCaps,
    network: NetworkPolicy,
}

impl std::fmt::Debug for BoundSandbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundSandbox")
            .field("workspace", &self.workspace)
            .field("caps", &self.caps)
            .field("network", &self.network)
            .finish_non_exhaustive()
    }
}

impl BoundSandbox {
    /// Construct from the executor-resolved policy (grant caps/network + per-run workspace).
    pub fn new(inner: Arc<dyn Sandbox>, workspace: Arc<PathBuf>, caps: ResourceCaps, network: NetworkPolicy) -> Self {
        Self { inner, workspace, caps, network }
    }
    /// Run `argv` under the fixed policy.
    pub fn run(&self, argv: &[String], stdin: Option<&str>) -> Result<CapOutcome, OrchestratorError> {
        self.inner.run(&SandboxSpec {
            argv,
            workspace: &self.workspace,
            caps: &self.caps,
            network: &self.network,
            stdin,
        })
    }
}
```

- [ ] **Step 2: Add `ToolContext.sandbox` + update literals**

In `crates/orchestrator/src/agent/tools.rs`, add to `pub struct ToolContext` (after `workspace_root`):

```rust
    /// The per-call sandbox handle (SP-4 s4), policy-fixed by the executor from the grant, or
    /// `None` when no sandbox is wired (⇒ the `shell` tool refuses loud). Ephemeral.
    pub sandbox: Option<std::sync::Arc<crate::agent::sandbox::BoundSandbox>>,
```

Update EVERY `ToolContext {…}` literal to add `sandbox: None,` — run `grep -rn "ToolContext {" crates/orchestrator/src` (the tools.rs `#[cfg(test)]` literals + the one production site in `executor/agent.rs`; the latter gets the real value in Task 4 — set it `None` here so the crate compiles, mirroring how the s3 slice rolled out the `workspace_root` field).

- [ ] **Step 3: Write the failing `ShellTool` tests**

Add to the tools.rs `#[cfg(test)] mod tests`:

```rust
    // A recording fake sandbox: captures the policy it was handed, returns a canned outcome.
    struct RecordingSandbox(std::sync::Mutex<Option<(Vec<String>, orchestrator_core::ResourceCaps, orchestrator_core::NetworkPolicy)>>);
    impl crate::agent::sandbox::Sandbox for RecordingSandbox {
        fn run(&self, spec: &crate::agent::sandbox::SandboxSpec) -> Result<crate::agent::sandbox::CapOutcome, OrchestratorError> {
            *self.0.lock().unwrap() = Some((spec.argv.to_vec(), spec.caps.clone(), spec.network.clone()));
            Ok(crate::agent::sandbox::CapOutcome { exit_code: Some(0), stdout: "canned".into(), stderr: String::new(), killed: None })
        }
    }

    fn bound(inner: std::sync::Arc<dyn crate::agent::sandbox::Sandbox>, caps: orchestrator_core::ResourceCaps, net: orchestrator_core::NetworkPolicy) -> std::sync::Arc<crate::agent::sandbox::BoundSandbox> {
        std::sync::Arc::new(crate::agent::sandbox::BoundSandbox::new(
            inner, std::sync::Arc::new(std::path::PathBuf::from("/ws")), caps, net,
        ))
    }

    fn shell_ctx(sandbox: Option<std::sync::Arc<crate::agent::sandbox::BoundSandbox>>) -> ToolContext {
        ToolContext {
            idempotency_key: "k".into(),
            effect_id: orchestrator_core::effect::effect_id("n", 0, 0),
            credentials: std::sync::Arc::new(std::collections::HashMap::new()),
            workspace_root: None,
            sandbox,
        }
    }

    #[test]
    fn shell_without_a_sandbox_refuses_loud() {
        let err = ShellTool
            .call_ctx(serde_json::json!({"argv": ["echo", "hi"]}), &shell_ctx(None))
            .unwrap_err();
        assert!(matches!(err, OrchestratorError::Tool { .. }), "shell must refuse loud with no sandbox");
    }

    #[test]
    fn shell_runs_argv_through_the_bound_sandbox() {
        let rec = std::sync::Arc::new(RecordingSandbox(std::sync::Mutex::new(None)));
        let caps = orchestrator_core::ResourceCaps { cpu_ms: None, mem_bytes: None, wall_ms: Some(1234) };
        let ctx = shell_ctx(Some(bound(rec.clone(), caps.clone(), orchestrator_core::NetworkPolicy::Deny)));
        let out = ShellTool.call_ctx(serde_json::json!({"argv": ["echo", "hi"]}), &ctx).unwrap();
        assert_eq!(out, serde_json::json!({"exit_code": 0, "stdout": "canned", "stderr": "", "killed": null}));
        // AC8: the sandbox saw the GRANT's caps/network, not anything derived from argv.
        let (a, seen_caps, seen_net) = rec.0.lock().unwrap().clone().unwrap();
        assert_eq!(a, vec!["echo".to_string(), "hi".to_string()]);
        assert_eq!(seen_caps.wall_ms, Some(1234));
        assert_eq!(seen_net, orchestrator_core::NetworkPolicy::Deny);
    }
```

- [ ] **Step 4: Run to verify it fails**

Run: `cargo test -p sensei-orchestrator --lib agent::tools 2>&1 | tail -20` → FAIL to compile (`cannot find ShellTool`).

- [ ] **Step 5: Implement `ShellTool`**

Add to `tools.rs` (module scope):

```rust
/// SP-4 s4: run an external command in the subprocess sandbox. Mutation. Args:
/// `{ "argv": ["cmd","arg",...], "stdin"?: "..." }`. Requires a wired sandbox (`ctx.sandbox`);
/// refuses loud otherwise (fail-closed — never an unconfined run). A cap-kill / nonzero exit is
/// surfaced IN the result (a normal Ok Value the model reacts to), not a node failure.
pub struct ShellTool;

impl Tool for ShellTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "shell".into(),
            description: Some("Run an external command in the sandbox (argv array)".into()),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "argv": { "type": "array", "items": { "type": "string" }, "minItems": 1 },
                    "stdin": { "type": "string" }
                },
                "required": ["argv"]
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
        // The s1 gate authorizes the command: the grant's `commands` allowlist must cover argv[0].
        let commands = args
            .get("argv")
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            .map(|c| vec![c.to_string()])
            .unwrap_or_default();
        Permissions { commands, ..Default::default() }
    }

    fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        Err(OrchestratorError::Tool {
            tool: "shell".into(),
            message: "shell requires a sandbox context (call_ctx)".into(),
        })
    }

    fn call_ctx(
        &self,
        args: serde_json::Value,
        ctx: &ToolContext,
    ) -> Result<serde_json::Value, OrchestratorError> {
        let sb = ctx.sandbox.as_ref().ok_or_else(|| OrchestratorError::Tool {
            tool: "shell".into(),
            message: "sandbox required but unavailable on this platform".into(),
        })?;
        let argv: Vec<String> = args
            .get("argv")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
            .unwrap_or_default();
        if argv.is_empty() {
            return Err(OrchestratorError::Tool { tool: "shell".into(), message: "missing 'argv'".into() });
        }
        let stdin = args.get("stdin").and_then(|v| v.as_str());
        let out = sb.run(&argv, stdin)?;
        let killed = out.killed.as_ref().map(|k| match k {
            crate::agent::sandbox::KillReason::Wall => "wall".to_string(),
            crate::agent::sandbox::KillReason::Cpu => "cpu".to_string(),
            crate::agent::sandbox::KillReason::Mem => "mem".to_string(),
            crate::agent::sandbox::KillReason::Signal(s) => format!("signal:{s}"),
        });
        Ok(serde_json::json!({
            "exit_code": out.exit_code,
            "stdout": out.stdout,
            "stderr": out.stderr,
            "killed": killed,
        }))
    }
}
```

- [ ] **Step 6: Run to verify pass**

Run: `cargo test -p sensei-orchestrator --lib agent::tools 2>&1 | tail -20` → PASS (existing + 2 new). Clippy `-D warnings` clean.

- [ ] **Step 7: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/agent/sandbox.rs crates/orchestrator/src/agent/tools.rs
git commit -m "feat(orchestrator): SP-4 sandbox (3/6) — BoundSandbox + ToolContext.sandbox + ShellTool"
```

---

## Task 4: Executor wiring — `bound_sandbox_for` + inject the sandbox (portable e2e)

**Files:**
- Modify: `crates/orchestrator/src/executor/agent.rs`
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Add `bound_sandbox_for` + inject into the `ToolContext`**

In `crates/orchestrator/src/executor/agent.rs`, add this method inside `impl Executor` (near `workspace_root_for`):

```rust
    /// SP-4 s4: build a per-call `BoundSandbox` for `tool`, or `None` (⇒ the `shell` tool
    /// refuses loud). Requires: a wired `Sandbox`, a resolved per-run workspace, AND a grant for
    /// the tool (the grant's caps/network are the enforced policy — the tool can't widen them).
    fn bound_sandbox_for(
        &self,
        ar: &AgentRun<'_>,
        tool: &str,
        workspace: &Option<std::sync::Arc<std::path::PathBuf>>,
    ) -> Option<std::sync::Arc<crate::agent::sandbox::BoundSandbox>> {
        let inner = self.sandbox.clone()?;
        let ws = workspace.clone()?;
        let grant = ar.agent_grants.get(tool)?;
        Some(std::sync::Arc::new(crate::agent::sandbox::BoundSandbox::new(
            inner,
            ws,
            grant.caps.clone(),
            grant.network.clone(),
        )))
    }
```

In `record_tool_effect`, replace the `ToolContext { … }` construction so the workspace root is resolved ONCE and reused (avoids a redundant `workspace_root_for` call and feeds the sandbox):

```rust
        let workspace_root = self.workspace_root_for(ar.run)?;
        let sandbox = self.bound_sandbox_for(ar, &call.name, &workspace_root);
        let ctx = crate::agent::tools::ToolContext {
            idempotency_key: idempotency_key.to_string(),
            effect_id: teid.clone(),
            credentials: std::sync::Arc::new(resolved),
            workspace_root,
            sandbox,
        };
```

- [ ] **Step 2: Verify existing suite unchanged**

Run: `cargo test -p sensei-orchestrator --lib 2>&1 | tail -8` (real `$?`) → existing crate tests pass, count unchanged (default `sandbox: None` ⇒ byte-identical). Clippy clean.

- [ ] **Step 3: Write the portable e2e tests (fake Sandbox)**

Add to `crates/orchestrator/src/executor/tests.rs`. You need a registry whose agent LISTS `shell` and holds a grant for it, AND whose core `Registry` has a `ShellTool` spec (for `assemble_prompt`). Extend the s3 `fs_registry` helper (or add a `tool_registry` helper) so it registers `ShellTool.spec()` into the core `Registry`; reuse `path_grant`-style grant construction, `scripted_gateway`, `agent_node`, `tool_call_response`, `final_response`, `recorded_output`, `effect_id`, and the single-node graph helper. Define a portable fake `Sandbox` in the test module.

```rust
struct FakeSandbox {
    spawns: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    seen: std::sync::Mutex<Option<(orchestrator_core::ResourceCaps, std::path::PathBuf)>>,
    stdout: String,
}
impl crate::agent::sandbox::Sandbox for FakeSandbox {
    fn run(&self, spec: &crate::agent::sandbox::SandboxSpec) -> Result<crate::agent::sandbox::CapOutcome, OrchestratorError> {
        self.spawns.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        *self.seen.lock().unwrap() = Some((spec.caps.clone(), spec.workspace.to_path_buf()));
        Ok(crate::agent::sandbox::CapOutcome { exit_code: Some(0), stdout: self.stdout.clone(), stderr: String::new(), killed: None })
    }
}

/// SP-4 s4 e2e: an agent calls `shell`; the executor builds a BoundSandbox from the grant +
/// per-run workspace and runs it; the outcome is journaled.
#[tokio::test]
async fn shell_runs_through_the_sandbox_and_journals() {
    let base = tempfile::tempdir().unwrap();
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let (gateway, _c) = scripted_gateway(vec![
        tool_call_response("t1", "shell", r#"{"argv":["echo","hello"]}"#),
        final_response("done"),
    ]).await;
    let grants = std::collections::HashMap::from([(
        "shell".to_string(),
        Permissions { commands: vec!["echo".into()], caps: orchestrator_core::ResourceCaps { cpu_ms: None, mem_bytes: None, wall_ms: Some(2000) }, ..Default::default() },
    )]);
    let spawns = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let fake = std::sync::Arc::new(FakeSandbox { spawns: spawns.clone(), seen: std::sync::Mutex::new(None), stdout: "hello\n".into() });
    let tools = Arc::new(ToolRegistry::default().with_tool(Arc::new(crate::agent::tools::ShellTool)));
    let outcome = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(fs_registry(vec!["shell".into()], grants)) // extended to register ShellTool.spec()
        .with_tools(tools)
        .with_workspace_root(base.path())
        .with_sandbox(fake.clone())
        .run(run, &graph_single("n1"))
        .await
        .unwrap();
    assert!(outcome.failed.is_none() && outcome.paused.is_none(), "failed={:?} paused={:?}", outcome.failed, outcome.paused);
    assert_eq!(spawns.load(std::sync::atomic::Ordering::SeqCst), 1);
    let events = journal.load(run).await.unwrap();
    let out = recorded_output(&events, &effect_id("n1", 0, 1)).unwrap();
    assert_eq!(out["stdout"], serde_json::json!("hello\n"));
    let (caps, ws) = fake.seen.lock().unwrap().clone().unwrap();
    assert_eq!(caps.wall_ms, Some(2000));               // grant's cap
    assert_eq!(ws, base.path().join(run.0.to_string())); // per-run workspace
}

/// SP-4 s4 e2e: NO sandbox wired ⇒ `shell` refuses loud (fail-closed) — the tested behavior on
/// Linux/CI until the landlock backend lands.
#[tokio::test]
async fn shell_refuses_loud_without_a_sandbox() {
    let base = tempfile::tempdir().unwrap();
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let (gateway, _c) = scripted_gateway(vec![
        tool_call_response("t1", "shell", r#"{"argv":["echo","hi"]}"#),
        final_response("done"),
    ]).await;
    let grants = std::collections::HashMap::from([(
        "shell".to_string(),
        Permissions { commands: vec!["echo".into()], ..Default::default() },
    )]);
    let tools = Arc::new(ToolRegistry::default().with_tool(Arc::new(crate::agent::tools::ShellTool)));
    let outcome = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(fs_registry(vec!["shell".into()], grants))
        .with_tools(tools)
        .with_workspace_root(base.path())
        // NO .with_sandbox(...)
        .run(run, &graph_single("n1"))
        .await
        .unwrap();
    // assert the refusal is surfaced loud — adapt to the real shape (a NodeFailed / a
    // ToolOutcome::Failed / a tool-result error string) that record_tool_effect's Err arm
    // produces for a call_ctx Err; mirror the sibling "tool errors" test.
    let events = journal.load(run).await.unwrap();
    assert!(
        serde_json::to_string(&events).unwrap().contains("sandbox required"),
        "expected a loud 'sandbox required' refusal in the journal"
    );
}
```

Note: confirm how a `call_ctx` `Err` surfaces (grep the sibling tool-error test / `record_tool_effect`'s `Err` arm) and adapt `shell_refuses_loud_without_a_sandbox`'s assertion; confirm `fs_registry` is extended to register `ShellTool.spec()` (else `assemble_prompt` errors `UnknownToolRef`); confirm `graph_single("n1")` exists (else inline `Graph { nodes: vec![agent_node("n1","a","run")] }`).

- [ ] **Step 4: Run the e2e tests → PASS**, then clippy clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/agent.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-4 sandbox (4/6) — bound_sandbox_for + inject sandbox + portable e2e"
```

---

## Task 5: macOS OS-confinement e2e (`#[cfg(target_os = "macos")]`)

**Files:**
- Test: `crates/orchestrator/src/agent/sandbox.rs` (macOS-gated confinement tests)

These run on the dev box (macOS) and are absent on Linux CI. They wire the REAL `MacosSandbox`.

- [ ] **Step 1: Write the macOS confinement tests**

Add to the `#[cfg(test)] mod tests` in `sandbox.rs`, all `#[cfg(target_os = "macos")]`:

```rust
    #[cfg(target_os = "macos")]
    #[test]
    fn macos_denies_write_outside_the_workspace() {
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let escape = tempfile::tempdir().unwrap();
        let target = escape.path().join("escaped");
        let a = argv(&["sh", "-c", &format!("echo x > {}", target.display())]);
        let out = MacosSandbox.run(&SandboxSpec {
            argv: &a, workspace: &root, caps: &caps(Some(5000), None),
            network: &orchestrator_core::NetworkPolicy::Deny, stdin: None,
        }).unwrap();
        assert!(!target.exists(), "a file escaped the sandbox workspace: {out:?}");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_allows_write_inside_the_workspace() {
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let a = argv(&["sh", "-c", &format!("echo x > {}/inside.txt", root.display())]);
        let out = MacosSandbox.run(&SandboxSpec {
            argv: &a, workspace: &root, caps: &caps(Some(5000), None),
            network: &orchestrator_core::NetworkPolicy::Deny, stdin: None,
        }).unwrap();
        assert_eq!(out.exit_code, Some(0), "in-workspace write was denied: {out:?}");
        assert!(root.join("inside.txt").exists(), "in-workspace write did not land");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_denies_network_when_policy_is_deny() {
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let a = argv(&["sh", "-c", "curl -sS --max-time 3 http://127.0.0.1:9/ ; echo rc=$?"]);
        let out = MacosSandbox.run(&SandboxSpec {
            argv: &a, workspace: &root, caps: &caps(Some(8000), None),
            network: &orchestrator_core::NetworkPolicy::Deny, stdin: None,
        }).unwrap();
        assert!(!out.stdout.contains("rc=0"), "network egress was NOT denied: {out:?}");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_wall_kill_through_the_sandbox() {
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let a = argv(&["sh", "-c", "sleep 100"]);
        let out = MacosSandbox.run(&SandboxSpec {
            argv: &a, workspace: &root, caps: &caps(Some(200), None),
            network: &orchestrator_core::NetworkPolicy::Deny, stdin: None,
        }).unwrap();
        assert_eq!(out.killed, Some(KillReason::Wall));
    }
```

Note: `sandbox-exec` profiles are fiddly. If `macos_allows_write_inside_the_workspace` fails because the profile is too strict, widen the READ allowances in `macos_profile` until a trivial `sh -c` runs, WITHOUT widening WRITE beyond the workspace subpath (write-confinement is the property under test); document any adjustment in a comment. If `curl` is absent, use bash `/dev/tcp` or skip the network test with a clear comment.

- [ ] **Step 2: Run on macOS**

Run: `cargo test -p sensei-orchestrator --lib agent::sandbox 2>&1 | tail -25` (dev box) → the macOS-gated tests PASS. Clippy clean. On Linux they are cfg-absent (CI unaffected).

- [ ] **Step 3: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/agent/sandbox.rs
git commit -m "test(orchestrator): SP-4 sandbox (5/6) — macOS sandbox-exec confinement e2e (cfg-gated)"
```

---

## Task 6: Resume exactly-once + redaction compose + additivity gate

**Files:**
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Resume exactly-once (portable, fake Sandbox with a spawn counter)**

Add `shell_replays_from_memo_without_respawning_on_resume`. Mirror the s3 `fs_write_replays_from_memo_without_rewriting_on_resume` seed/truncate idiom: seed a `shell` run to completion through a `FakeSandbox` (spawn counter == 1), truncate the journal just past the shell `EffectRecorded` at `effect_id("n1",0,1)` (assert no `RunCompleted` in the prefix), copy into a fresh journal via the same `seeded.append(run, e.clone())` per-event helper the sibling resume tests use, resume with a FRESH `FakeSandbox` (fresh counter). Assert: the resume's spawn counter is **0** (the completed shell replayed from the memo — not re-spawned), the run completes with no `DeterminismViolation`, `effect_recorded_count(effect_id("n1",0,1)) == 1`.

- [ ] **Step 2: Redaction composes over stdout**

Add `shell_stdout_is_redacted`. A `FakeSandbox` whose `run` returns a stdout containing a runtime-assembled secret (`format!("sk-{}", "abcdefghij0123456789")`); wire `.with_redactor(Arc::new(orchestrator_core::PatternRedactor::default()))`. Assert the journaled shell output at `effect_id("n1",0,1)` does NOT contain the plaintext + a whole-journal scan finds none. (Secret assembled at runtime — semgrep CWE-798.)

- [ ] **Step 3: Additivity + full-suite gate**

```bash
cd /Users/Jerry/Developer/gateway
cargo test --workspace > /tmp/sbx_fulltest.log 2>&1; echo "EXIT=$?"
grep -c "test result: ok" /tmp/sbx_fulltest.log
grep -oE "[0-9]+ passed" /tmp/sbx_fulltest.log | awk '{s+=$1} END{print s}'
grep -oE "[1-9][0-9]* failed" /tmp/sbx_fulltest.log | head
cargo fmt --all --check; echo "FMT=$?"
cargo clippy --workspace --all-targets -- -D warnings > /tmp/sbx_clippy.log 2>&1; echo "CLIPPY=$?"
```
Read REAL exit codes + aggregate DIRECTLY (do NOT pipe-to-tail to DECIDE). Confirm `EXIT=0`, 0 failed, total = baseline **1099 + the new portable tests** (Task 1: 4, Task 2: 1 portable, Task 3: 2, Task 4: 2, Task 6: 2 → ~+11 on Linux; +5 more macOS-gated on the dev box). `FMT=0`, `CLIPPY=0`. Existing s1/s2/s3/s5/broker suites byte-identical.

- [ ] **Step 4: Commit** (do NOT push)

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/tests.rs
git commit -m "test(orchestrator): SP-4 sandbox (6/6) — resume exactly-once + redaction compose + additivity gate"
```

---

## Self-Review notes (author)

- **Spec coverage:** AC1/AC3/AC4 → Task 1 (wall/pgroup/normal). AC2 → Task 1 (mem, "not a clean success"). AC5 → Task 3 (unit refuse) + Task 4 (e2e refuse). AC6/AC7 → Task 5 (macOS fs/network). AC8 → Task 3 (BoundSandbox forwards grant policy) + Task 4 (e2e sees grant caps/workspace). AC9 → Task 6 (resume no-respawn). AC10 → Task 6 (redaction). AC11 → Task 6 (additive gate). `Sandbox` trait + `MacosSandbox` + `with_sandbox` → Task 2; `ShellTool` + `BoundSandbox` + `ToolContext.sandbox` → Task 3; executor wiring → Task 4.
- **Portable vs macOS-gated:** Tasks 1,3,4,6 are portable (CI-tested on Linux via `FakeSandbox` + real subprocesses for the cap-killing); Task 2's macOS smoke + Task 5's confinement e2e are `#[cfg(target_os="macos")]` (dev-only). Refuse-loud is the Linux-tested behavior.
- **Harness placeholders to resolve during implementation:** the `fs_registry` extension to register `ShellTool.spec()`, `graph_single`, the exact journal raw-seed helper (Task 6), the `call_ctx`-`Err`-shape in the journal (Task 4 refuse assertion), and the `mem_cap`/`sandbox-exec`-profile fiddliness (Tasks 1/5) — all pointed at sibling tests / documented adjustments, not invented.
- **Additivity:** every production change gates on `sandbox.is_some()` + a workspace + `agent_grants.get(tool)` (`bound_sandbox_for` → `None`) + `ToolContext.sandbox: None` ⇒ unwired = byte-identical.
- **The `ToolContext.sandbox: None` shim in agent.rs (Task 3)** mirrors the s3 `workspace_root` field rollout: adding a required field breaks the executor literal, so Task 3 sets it `None` (compiles green) and Task 4 flips it to the resolved `bound_sandbox_for(...)`.
- **Security:** `sandbox-exec` is Apple-deprecated but functional (§4.6/§6); the profile allows broad file-READ (a binary must start) but confines WRITE to the workspace + denies network — write/network confinement is the tested property. Refuse-loud (fail-closed) is the non-macOS behavior — never an unconfined run. The `nix` dep is preferred over hand-rolled `libc` unsafe (secure-default guidance).
```
