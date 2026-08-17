# SP-4 Linux Sandbox Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `LinuxSandbox` backend for the subprocess sandbox — landlock (fs write→workspace) + seccomp (deny IP egress) + the portable `spawn_capped` cap-killing — so Linux runs an untrusted `shell` CONFINED instead of refusing-loud, at parity with `MacosSandbox`.

**Architecture:** `LinuxSandbox` (`#[cfg(target_os="linux")]`) implements the existing `Sandbox` trait. It BUILDS the confinement in the parent (landlock ruleset + seccomp BPF program — both allocate) and APPLIES it (syscalls only) in the child via an extra `pre_exec` hook threaded through `spawn_capped`. Everything else (the trait, `SandboxSpec`, `BoundSandbox`, `ShellTool`, `Executor::with_sandbox`, refuse-loud) is reused unchanged.

**Tech Stack:** Rust, `landlock` + `seccompiler` (target-gated Linux-only deps), `libc`, the existing `spawn_capped`. Verified in a **Docker Linux container** (unprivileged; landlock ABI 6 + seccomp confirmed available) + `ubuntu-latest` CI.

**Spec:** `docs/superpowers/specs/2026-08-17-sp4-linux-sandbox-design.md`

**Baseline:** `develop` at `2999969`; full workspace **1120 tests** green on macOS. `cargo fmt --all` before every commit (pre-commit = fmt-check + workspace `clippy -D warnings`, NO tests → always run tests yourself, real unpiped exit code). Do NOT push (coordinator pushes after the whole-slice review).

**⚠️ PLATFORM REALITY (read first):**
- This code is `#[cfg(target_os="linux")]` — it is **excluded from the macOS build**, so `cargo build` on the macOS host will NOT compile-check it. You MUST compile + test the Linux code **inside Docker** (see the harness below). A macOS `cargo build`/`cargo test` only proves the macOS side stays byte-identical.
- The `landlock`/`seccompiler` builder code in this plan is **REFERENCE SHAPE** — adapt the exact method names / error types to the pinned crate version when it compiles in Docker (the crate can't be checked from macOS). The TEST behavior (write-denied / egress-denied contrast) is the real spec and is exact.

**Docker verification harness (reused in Tasks 2–4):**
```bash
# Unprivileged Linux build+test. Named volume caches the linux target (only the first run is slow —
# aws-lc etc.); cmake/g++/perl are for aws-lc-sys. Runs on the host arch (arm64 on Apple Silicon).
docker run --rm -v "$PWD":/w -w /w -v sbx-linux-target:/target -e CARGO_TARGET_DIR=/target \
  rust:1-bookworm bash -lc '
    apt-get update -qq && apt-get install -y -qq cmake g++ perl >/dev/null 2>&1
    cargo test -p sensei-orchestrator --lib agent::sandbox -- --nocapture'
# read the REAL exit code ($?) + the "test result:" line. First run pulls the image + full compile
# (several minutes); later runs reuse the cached target volume.
```

---

## File Structure

- **Modify** `crates/orchestrator/Cargo.toml` — add `[target.'cfg(target_os = "linux")'.dependencies]` `landlock` + `seccompiler` + `libc`.
- **Modify** `crates/orchestrator/src/agent/sandbox.rs` — thread an optional extra `pre_exec` hook through `spawn_capped`; add `#[cfg(target_os="linux")]` `LinuxSandbox` + `build_landlock_ruleset` + `build_egress_deny_filter`; add `#[cfg(target_os="linux")]` tests to the existing `#[cfg(test)] mod tests`.

(All in `sandbox.rs` — one file owns sandboxed process execution; the Linux code sits beside `MacosSandbox`.)

---

## Task 1: Thread an optional extra `pre_exec` hook through `spawn_capped` (portable, macOS-verified)

**Files:**
- Modify: `crates/orchestrator/Cargo.toml`
- Modify: `crates/orchestrator/src/agent/sandbox.rs`

**Doc-linter note:** a repo hook flags the literal child post-fork hook call (`pre` + `_exec` + `(`). Where this plan shows it, it writes a SPACE before `(` (`command.pre_exec (…)`) — valid Rust that `cargo fmt` normalizes to no-space; write the normal no-space form in source.

- [ ] **Step 1: Add the target-gated Linux deps**

In `crates/orchestrator/Cargo.toml`, after the `[dependencies]` block (adjust versions to what resolves in Docker):

```toml
[target.'cfg(target_os = "linux")'.dependencies]
landlock = "0.4"
seccompiler = "0.4"
libc = "0.2"
```

- [ ] **Step 2: Refactor `spawn_capped` to delegate to `spawn_capped_with(..., None)`**

In `sandbox.rs`, replace the `pub(crate) fn spawn_capped(...)` signature line + opening so the public entry delegates, and the body moves into a private `spawn_capped_with` that runs an optional extra hook in the child. Keep the ENTIRE existing body identical except the one added line in the `pre_exec` closure. Concretely:

```rust
/// Portable cap-killing spawn (process-group + setrlimit + wall-kill + bounded capture). No
/// confinement — that is the `Sandbox` layer. Byte-identical to before.
pub(crate) fn spawn_capped(
    argv: &[String],
    caps: &ResourceCaps,
    stdin: Option<&str>,
) -> Result<CapOutcome, OrchestratorError> {
    spawn_capped_with(argv, caps, stdin, None)
}

/// Like `spawn_capped`, but runs `extra_pre_exec` (if any) in the child AFTER the rlimits and
/// BEFORE `execve`. Used by `LinuxSandbox` to APPLY a pre-built landlock ruleset + seccomp filter
/// (the closure issues syscalls only — no allocation in the child). `None` ⇒ byte-identical.
pub(crate) fn spawn_capped_with(
    argv: &[String],
    caps: &ResourceCaps,
    stdin: Option<&str>,
    mut extra_pre_exec: Option<Box<dyn FnMut() -> std::io::Result<()> + Send + Sync>>,
) -> Result<CapOutcome, OrchestratorError> {
    // ... EXISTING body verbatim from the current spawn_capped, up to the pre_exec block ...
```

Then in the `pre_exec` closure, add the extra-hook call AFTER the two `setrlimit` blocks (shown with the doc-linter space):

```rust
    unsafe {
        command.pre_exec (move || {
            use nix::sys::resource::{Resource, setrlimit};
            if let Some(s) = cpu_secs {
                setrlimit(Resource::RLIMIT_CPU, s, s)
                    .map_err(|e| std::io::Error::from_raw_os_error(e as i32))?;
            }
            if let Some(b) = mem {
                setrlimit(Resource::RLIMIT_AS, b, b)
                    .map_err(|e| std::io::Error::from_raw_os_error(e as i32))?;
            }
            // SP-4 Linux: apply the pre-built confinement (landlock + seccomp) — syscalls only.
            if let Some(hook) = extra_pre_exec.as_mut() {
                hook()?;
            }
            Ok(())
        });
    }
    // ... rest of the EXISTING body verbatim (readers, waiter, wall-kill, straggler-reap,
    //     bounded capture, killed mapping, CapOutcome) ...
}
```

(The `+ Send + Sync` bound matches `CommandExt::pre_exec`'s `FnMut + Send + Sync` requirement; the captured landlock/seccomp values are `Send + Sync`.)

- [ ] **Step 3: Verify byte-identical on macOS**

Run: `cargo test -p sensei-orchestrator --lib agent::sandbox 2>&1 | tail -20` (real `$?`)
Expected: PASS — the 13 existing sandbox tests are unchanged (`spawn_capped` behaves identically; `extra_pre_exec: None`). Then `cargo clippy -p sensei-orchestrator --all-targets -- -D warnings` → clean. (No Docker needed this task — it's the portable seam.)

- [ ] **Step 4: Confirm the Linux deps don't break the macOS build**

Run: `cargo build -p sensei-orchestrator 2>&1 | tail -5` (real `$?`) → clean (the `landlock`/`seccompiler`/`libc` deps are target-gated, so `cargo build` on macOS does NOT pull them; `Cargo.lock` may gain them as target-specific entries, which is fine).

- [ ] **Step 5: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/Cargo.toml crates/orchestrator/src/agent/sandbox.rs
git commit -m "feat(orchestrator): SP-4 linux-sandbox (1/4) — spawn_capped extra pre_exec seam + target-gated deps"
```

---

## Task 2: `LinuxSandbox` + landlock fs confinement (Docker-verified)

**Files:**
- Modify: `crates/orchestrator/src/agent/sandbox.rs`

- [ ] **Step 1: Write the failing fs-confinement tests (Linux-gated)**

Add to the existing `#[cfg(test)] mod tests` in `sandbox.rs` (reuse the `argv`/`caps` helpers already there). All `#[cfg(target_os = "linux")]`:

```rust
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_denies_write_outside_the_workspace() {
        // landlock confines WRITE to the workspace subpath. A write to a DIFFERENT tempdir must
        // be denied (EACCES) — the fs security proof. The complement (allows-inside) rules out
        // "sh couldn't start / all writes denied".
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let escape = tempfile::tempdir().unwrap();
        let target = escape.path().join("escaped");
        let a = argv(&["sh", "-c", &format!("echo x > {}", target.display())]);
        let out = LinuxSandbox.run(&SandboxSpec {
            argv: &a, workspace: &root, caps: &caps(Some(5000), None),
            network: &orchestrator_core::NetworkPolicy::Any, stdin: None,
        }).unwrap();
        assert!(!target.exists(), "a file escaped the landlock workspace: {out:?}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_allows_write_inside_the_workspace() {
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let a = argv(&["sh", "-c", &format!("echo hi > {}/inside.txt", root.display())]);
        let out = LinuxSandbox.run(&SandboxSpec {
            argv: &a, workspace: &root, caps: &caps(Some(5000), None),
            network: &orchestrator_core::NetworkPolicy::Any, stdin: None,
        }).unwrap();
        assert_eq!(out.exit_code, Some(0), "in-workspace write was denied: {out:?}");
        assert_eq!(std::fs::read_to_string(root.join("inside.txt")).unwrap(), "hi\n");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_allows_nested_write_inside_the_workspace() {
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let a = argv(&["sh", "-c", &format!("mkdir -p {r}/a/b && echo hi > {r}/a/b/c.txt", r = root.display())]);
        let out = LinuxSandbox.run(&SandboxSpec {
            argv: &a, workspace: &root, caps: &caps(Some(5000), None),
            network: &orchestrator_core::NetworkPolicy::Any, stdin: None,
        }).unwrap();
        assert_eq!(out.exit_code, Some(0), "nested in-workspace write/mkdir denied: {out:?}");
        assert!(root.join("a/b/c.txt").exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_wall_kill_composes() {
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let a = argv(&["sh", "-c", "sleep 100"]);
        let out = LinuxSandbox.run(&SandboxSpec {
            argv: &a, workspace: &root, caps: &caps(Some(200), None),
            network: &orchestrator_core::NetworkPolicy::Any, stdin: None,
        }).unwrap();
        assert_eq!(out.killed, Some(KillReason::Wall), "cap-killing must compose with landlock");
    }
```

- [ ] **Step 2: Run in Docker to verify they fail to compile**

Run the Docker harness (top of plan). Expected: FAIL to COMPILE — `cannot find LinuxSandbox`. (This also validates the harness compiles the workspace on Linux + the Task-1 seam.)

- [ ] **Step 3: Implement `LinuxSandbox` + `build_landlock_ruleset` (REFERENCE SHAPE — adapt to the crate API in Docker)**

Add to `sandbox.rs` (module scope), `#[cfg(target_os = "linux")]`. The landlock calls are reference shape — adapt method/type names to the resolved `landlock` version:

```rust
/// SP-4 Linux backend: landlock fs-write confinement to the workspace + (Task 3) seccomp egress
/// deny + the portable cap-killing. Unprivileged (landlock ≥5.13). Parity with `MacosSandbox`.
#[cfg(target_os = "linux")]
pub struct LinuxSandbox;

#[cfg(target_os = "linux")]
impl Sandbox for LinuxSandbox {
    fn run(&self, spec: &SandboxSpec) -> Result<CapOutcome, OrchestratorError> {
        // BUILD in the parent (allocates); refuse-loud if fs-confine can't be built.
        let mut ruleset = Some(build_landlock_ruleset(spec.workspace)?);
        match spec.network {
            // Any / Hosts(coarse) => no network filter this task.
            NetworkPolicy::Any | NetworkPolicy::Hosts(_) => {}
            // Egress-deny lands in Task 3 (seccomp). Until then, refuse-loud (fail-closed) rather
            // than run an unconfined-network command.
            NetworkPolicy::Deny => {
                return Err(OrchestratorError::Tool {
                    tool: "sandbox".into(),
                    message: "linux egress-deny not yet implemented".into(),
                });
            }
        }
        // APPLY in the child (syscalls only — ruleset pre-built).
        let hook: Box<dyn FnMut() -> std::io::Result<()> + Send + Sync> = Box::new(move || {
            let rs = ruleset.take().expect("landlock ruleset applied exactly once");
            let status = rs
                .restrict_self()
                .map_err(|e| std::io::Error::other(format!("landlock restrict_self: {e}")))?;
            // Fail-closed: if landlock is not enforced (kernel < 5.13 / not compiled), the fs
            // confinement did not take — abort the child so the command never runs unconfined.
            if status.ruleset == landlock::RulesetStatus::NotEnforced {
                return Err(std::io::Error::other("landlock not enforced (kernel < 5.13?)"));
            }
            Ok(())
        });
        spawn_capped_with(spec.argv, spec.caps, spec.stdin, Some(hook))
    }
}

/// Build a landlock ruleset: broad READ+execute on `/` (a binary must start), WRITE (+create/
/// remove/make-*) confined to the canonical workspace subpath. Best-effort forward-ABI; the
/// ABI-1 write handling is the security core. Built in the PARENT; `restrict_self` in the child.
#[cfg(target_os = "linux")]
fn build_landlock_ruleset(workspace: &std::path::Path) -> Result<landlock::RulesetCreated, OrchestratorError> {
    use landlock::{
        ABI, Access, AccessFs, CompatLevel, Compatible, PathBeneath, PathFd, Ruleset,
        RulesetAttr, RulesetCreatedAttr,
    };
    let mk = |m: String| OrchestratorError::Tool { tool: "sandbox".into(), message: format!("landlock: {m}") };
    let abi = ABI::V1;
    let ruleset = Ruleset::default()
        .set_compatibility(CompatLevel::BestEffort)
        .handle_access(AccessFs::from_all(abi)).map_err(|e| mk(e.to_string()))?
        .create().map_err(|e| mk(e.to_string()))?
        .add_rule(PathBeneath::new(PathFd::new("/").map_err(|e| mk(e.to_string()))?, AccessFs::from_read(abi)))
            .map_err(|e| mk(e.to_string()))?
        .add_rule(PathBeneath::new(PathFd::new(workspace).map_err(|e| mk(e.to_string()))?, AccessFs::from_all(abi)))
            .map_err(|e| mk(e.to_string()))?;
    Ok(ruleset)
}
```

Adapt to the crate: exact trait imports (`RulesetAttr`/`RulesetCreatedAttr`/`Compatible`), the `RulesetStatus` path, and whether `restrict_self` returns `RestrictionStatus { ruleset, .. }`. Iterate in Docker until it compiles.

- [ ] **Step 4: Run in Docker → the 4 fs tests PASS**

Run the Docker harness. Expected: the 4 `linux_*` fs tests appear BY NAME and pass (landlock ABI 6 is available in the Docker kernel — probed). `denies_write_outside` is the load-bearing proof; `allows_write_inside`/`nested` rule out a blanket-deny; `wall_kill_composes` proves cap-killing + landlock together. If landlock build/apply fails, adapt the API (Step 3) until green. Read the REAL result line + `$?`.

- [ ] **Step 5: Confirm macOS still byte-identical**

Run: `cargo test -p sensei-orchestrator --lib agent::sandbox 2>&1 | tail -8` (macOS host, real `$?`) → the 13 existing tests pass; `LinuxSandbox` is `#[cfg]`-absent. Clippy clean (macOS).

- [ ] **Step 6: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/agent/sandbox.rs
git commit -m "feat(orchestrator): SP-4 linux-sandbox (2/4) — LinuxSandbox + landlock fs confinement"
```

---

## Task 3: seccomp egress-deny + wire into `LinuxSandbox` (Docker-verified)

**Files:**
- Modify: `crates/orchestrator/src/agent/sandbox.rs`

- [ ] **Step 1: Write the failing network tests (Linux-gated)**

Add to `mod tests`, `#[cfg(target_os = "linux")]`:

```rust
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_network_deny_blocks_a_live_socket_that_any_allows() {
        use std::net::TcpListener;
        // A real listener the sandboxed probe can reach, so a Deny failure can only be seccomp
        // (not connection-refused). bash /dev/tcp does a socket(AF_INET)+connect.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let probe = format!("exec 3<>/dev/tcp/127.0.0.1/{port} && echo connected || echo blocked");
        let a = argv(&["bash", "-c", &probe]);
        let allowed = LinuxSandbox.run(&SandboxSpec {
            argv: &a, workspace: &root, caps: &caps(Some(5000), None),
            network: &orchestrator_core::NetworkPolicy::Any, stdin: None,
        }).unwrap();
        assert!(allowed.stdout.contains("connected"),
            "positive control: Any must allow the loopback connect: {allowed:?}");
        let denied = LinuxSandbox.run(&SandboxSpec {
            argv: &a, workspace: &root, caps: &caps(Some(5000), None),
            network: &orchestrator_core::NetworkPolicy::Deny, stdin: None,
        }).unwrap();
        assert!(denied.stdout.contains("blocked") && !denied.stdout.contains("connected"),
            "Deny must block the IP connect (seccomp socket(AF_INET)->EPERM): {denied:?}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_network_deny_still_allows_af_unix_and_startup() {
        // The filter is SURGICAL: it denies socket(AF_INET|AF_INET6) only, so AF_UNIX still works
        // and ordinary programs start. Prove an AF_UNIX socket succeeds under Deny.
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let a = argv(&["perl", "-e",
            "use Socket; socket(S, PF_UNIX, SOCK_STREAM, 0) or do { print \"unix-blocked\\n\"; exit 1 }; print \"unix-ok\\n\""]);
        let out = LinuxSandbox.run(&SandboxSpec {
            argv: &a, workspace: &root, caps: &caps(Some(5000), None),
            network: &orchestrator_core::NetworkPolicy::Deny, stdin: None,
        }).unwrap();
        assert_eq!(out.exit_code, Some(0), "AF_UNIX socket was wrongly blocked under Deny: {out:?}");
        assert!(out.stdout.contains("unix-ok"));
    }
```

- [ ] **Step 2: Run in Docker → they fail** (the `Deny` path currently refuses "not yet implemented", so `linux_network_deny_*` fail). Confirm via the harness.

- [ ] **Step 3: Implement `build_egress_deny_filter` + wire it in (REFERENCE SHAPE — adapt to seccompiler in Docker)**

Add to `sandbox.rs`, `#[cfg(target_os = "linux")]`:

```rust
/// Build a seccomp BPF program that denies IP-socket creation (egress) while allowing everything
/// else: `socket(domain == AF_INET|AF_INET6, ..)` -> EPERM. `connect()` can't be filtered by
/// address (seccomp can't deref the sockaddr pointer), so socket-creation is the chokepoint;
/// AF_UNIX/AF_NETLINK stay allowed so programs start. Built in the PARENT.
#[cfg(target_os = "linux")]
fn build_egress_deny_filter() -> Result<seccompiler::BpfProgram, OrchestratorError> {
    use seccompiler::{
        SeccompAction, SeccompCmpArgLen, SeccompCmpOp, SeccompCondition, SeccompFilter, SeccompRule,
    };
    use std::collections::BTreeMap;
    let mk = |m: String| OrchestratorError::Tool { tool: "sandbox".into(), message: format!("seccomp: {m}") };
    let arch = if cfg!(target_arch = "x86_64") {
        seccompiler::TargetArch::x86_64
    } else if cfg!(target_arch = "aarch64") {
        seccompiler::TargetArch::aarch64
    } else {
        return Err(mk("unsupported arch for seccomp".into()));
    };
    let inet = SeccompRule::new(vec![SeccompCondition::new(0, SeccompCmpArgLen::Dword, SeccompCmpOp::Eq, libc::AF_INET as u64).map_err(|e| mk(e.to_string()))?]).map_err(|e| mk(e.to_string()))?;
    let inet6 = SeccompRule::new(vec![SeccompCondition::new(0, SeccompCmpArgLen::Dword, SeccompCmpOp::Eq, libc::AF_INET6 as u64).map_err(|e| mk(e.to_string()))?]).map_err(|e| mk(e.to_string()))?;
    let mut rules: BTreeMap<i64, Vec<SeccompRule>> = BTreeMap::new();
    rules.insert(libc::SYS_socket, vec![inet, inet6]);
    let filter = SeccompFilter::new(rules, SeccompAction::Allow, SeccompAction::Errno(libc::EPERM as u32), arch)
        .map_err(|e| mk(e.to_string()))?;
    let bpf: seccompiler::BpfProgram = filter.try_into().map_err(|e: seccompiler::Error| mk(e.to_string()))?;
    Ok(bpf)
}
```

Then in `LinuxSandbox::run`, replace the `NetworkPolicy::Deny => return Err(...)` arm: build the filter in the parent and move it into the hook so it applies AFTER landlock:

```rust
        let bpf = match spec.network {
            NetworkPolicy::Any | NetworkPolicy::Hosts(_) => None,
            NetworkPolicy::Deny => Some(build_egress_deny_filter()?),
        };
        let mut bpf = bpf;
        let hook: Box<dyn FnMut() -> std::io::Result<()> + Send + Sync> = Box::new(move || {
            let rs = ruleset.take().expect("landlock ruleset applied exactly once");
            let status = rs.restrict_self()
                .map_err(|e| std::io::Error::other(format!("landlock restrict_self: {e}")))?;
            if status.ruleset == landlock::RulesetStatus::NotEnforced {
                return Err(std::io::Error::other("landlock not enforced (kernel < 5.13?)"));
            }
            // Apply seccomp AFTER landlock (allow-by-default filter, so it never blocks execve or
            // landlock_restrict_self). apply_filter also sets NO_NEW_PRIVS.
            if let Some(program) = bpf.take() {
                seccompiler::apply_filter(&program)
                    .map_err(|e| std::io::Error::other(format!("seccomp apply_filter: {e}")))?;
            }
            Ok(())
        });
        spawn_capped_with(spec.argv, spec.caps, spec.stdin, Some(hook))
```

Adapt to seccompiler's exact API (the `Error` type, `TargetArch` variants, `BpfProgram` conversion). `libc::AF_INET`/`AF_INET6`/`SYS_socket`/`EPERM` come from the target-gated `libc` dep.

- [ ] **Step 4: Run in Docker → all `linux_*` tests PASS**

Run the harness. Expected: `linux_network_deny_blocks_a_live_socket_that_any_allows` (Any→connected / Deny→blocked — the non-vacuous contrast) + `linux_network_deny_still_allows_af_unix_and_startup` pass, plus the 4 fs tests from Task 2 still pass. Read the REAL result line + `$?`. **Empirically confirm the contrast holds** (if Deny still shows "connected", the seccomp filter isn't taking — adapt until it does).

- [ ] **Step 5: macOS byte-identical** — `cargo test -p sensei-orchestrator --lib agent::sandbox` (macOS) unchanged; clippy clean.

- [ ] **Step 6: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/agent/sandbox.rs
git commit -m "feat(orchestrator): SP-4 linux-sandbox (3/4) — seccomp egress-deny (socket AF_INET->EPERM)"
```

---

## Task 4: fail-closed refuse coverage + macOS byte-identical + additive full-suite gate

**Files:**
- Modify: `crates/orchestrator/src/agent/sandbox.rs` (a refuse unit test)

- [ ] **Step 1: A unit test for the fail-closed build path (Linux-gated)**

The landlock-unavailable→refuse branch (AC6) can't be triggered where landlock IS available (the Docker/CI kernels have it). Instead, prove the fail-closed CONTRACT directly: `build_egress_deny_filter()` produces a filter, and a `LinuxSandbox` with a workspace that cannot be opened refuses loud (exercises the `build_landlock_ruleset` error → `run` returns Err path). Add:

```rust
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_refuses_when_the_workspace_path_is_unopenable() {
        // build_landlock_ruleset opens the workspace via PathFd; a non-existent path fails the
        // build => run() returns Err (refuse-loud), never an unconfined run.
        let a = argv(&["sh", "-c", "echo x"]);
        let bogus = std::path::PathBuf::from("/nonexistent-workspace-\u{0}"); // unopenable
        let r = LinuxSandbox.run(&SandboxSpec {
            argv: &a, workspace: &bogus, caps: &caps(Some(5000), None),
            network: &orchestrator_core::NetworkPolicy::Any, stdin: None,
        });
        assert!(matches!(r, Err(OrchestratorError::Tool { .. })), "unopenable workspace must refuse loud: {r:?}");
    }
```

(If `PathFd::new` tolerates the NUL differently, use a plainly non-existent path like `/nonexistent-xyz`; the point is the ruleset build fails → `run` returns Err. Adapt in Docker.)

- [ ] **Step 2: Run in Docker → the refuse test + all prior linux tests PASS**

Run the harness. Confirm the full `agent::sandbox` Linux suite is green (fs + network + refuse). Read the REAL result line + `$?`.

- [ ] **Step 3: macOS byte-identical + additive full-workspace gate**

On the macOS host, read REAL unpiped exit codes + aggregate DIRECTLY (do NOT pipe-to-tail to DECIDE):
```bash
cd /Users/Jerry/Developer/gateway
cargo test --workspace > /tmp/lsbx_fulltest.log 2>&1; echo "EXIT=$?"
grep -c "test result: ok" /tmp/lsbx_fulltest.log
grep -oE "[0-9]+ passed" /tmp/lsbx_fulltest.log | awk '{s+=$1} END{print s}'
grep -oE "[1-9][0-9]* failed" /tmp/lsbx_fulltest.log | head
cargo fmt --all --check; echo "FMT=$?"
cargo clippy --workspace --all-targets -- -D warnings > /tmp/lsbx_clippy.log 2>&1; echo "CLIPPY=$?"
```
Expected: `EXIT=0`, 0 failed, total = **1120** (byte-identical to baseline — the Linux code is `#[cfg]`-absent on macOS; no macOS test added). `FMT=0`, `CLIPPY=0`. This proves additivity (AC7/AC8): macOS is untouched, the Linux backend is purely additive.

- [ ] **Step 4: Commit** (do NOT push)

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/agent/sandbox.rs
git commit -m "test(orchestrator): SP-4 linux-sandbox (4/4) — fail-closed refuse + macOS byte-identical gate"
```

---

## Self-Review notes (author)

- **Spec coverage:** AC1/AC2 → Task 2 (write-outside-denied / write-inside + nested allowed). AC3 → Task 3 (network deny/any live-listener contrast). AC4 → Task 3 (AF_UNIX still works). AC5 → Task 2 (wall-kill composes). AC6 → Task 4 (fail-closed refuse on unbuildable ruleset — the landlock-unavailable branch is approximated by the ruleset-build-fails path, since the test kernels HAVE landlock). AC7/AC8 → Task 4 (macOS byte-identical + additive full-suite). The `spawn_capped` seam + deps → Task 1.
- **Verification reality:** Tasks 2–4 verify the Linux confinement via the Docker harness (unprivileged; landlock ABI 6 + seccomp confirmed present) — the `#[cfg(target_os="linux")]` code does NOT compile on the macOS host, so Docker is the compile+test env; macOS `cargo build/test` proves byte-identical additivity. CI (`ubuntu-latest`) is the second gate on the develop→main PR.
- **Reference-shape code:** the `landlock`/`seccompiler` builder bodies are adapt-in-Docker (the crates can't be checked from macOS); the TEST behavior (deny/allow contrasts) is exact and is the real acceptance bar. The implementer iterates the crate API in Docker until the exact tests pass.
- **Alloc-safety:** the confinement is BUILT in the parent (Steps 3 of Tasks 2/3) and only APPLIED (syscalls) in the child `pre_exec` hook — avoids the fork+malloc-lock hazard; landlock before seccomp (allow-by-default filter doesn't block execve).
- **Fail-closed:** landlock `NotEnforced` → child aborts (spawn fails → refuse); an unbuildable ruleset → `run` returns Err → `shell` refuses; `Deny` before Task 3 → refuse. Never an unconfined run.
- **Additive:** target-gated deps + `#[cfg(target_os="linux")]` ⇒ macOS + the whole existing suite byte-identical (1120).
```
