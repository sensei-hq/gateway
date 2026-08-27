---
title: SP-4 slice — Subprocess sandbox + resource-cap killing (macOS-first)
doctype: design
module: orchestrator
spec: SP-4
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§13 enforcement & isolation, §132 tool permissions, §225 killing process trees for shell, §288 sandbox + resource-cap killing); ./2026-08-14-sp4-permission-enforcement-design.md (s1 — declares ResourceCaps+NetworkPolicy this slice ENFORCES); ./2026-08-16-sp4-workspace-isolation-design.md (s3 — the per-run workspace this sandbox roots its fs jail at; the in-process cooperative jail this OS-confines for real); ./2026-08-14-sp4-secret-redaction-design.md (s2 — stdout redaction); ./2026-08-15-sp4-credential-broker-design.md (the ToolContext seam this extends)
date: 2026-08-16
---

# SP-4 slice — Subprocess sandbox + resource-cap killing (macOS-first)

## 1. Goal

Run an **external command** as a **killable, confined child process** so a runaway or untrusted
command can be **cpu/mem/wall-capped and KILLED**, and its **filesystem/network confined by the
OS** — the one capability the s3 in-process jail explicitly could not deliver (an in-process
`Tool::call_ctx` has ambient authority and cannot be killed mid-call). This finally **enforces
the `ResourceCaps` + `NetworkPolicy` that s1 only declared**, closing the SP-4 enforcement arc:
s1 authorizes, s2 redacts, s5 exactly-once, the broker provisions secrets, s3 confines
in-process fs paths, and **s4 confines + caps + kills a real subprocess**.

**Scope (user-chosen):**
- **Portable cap-killing core** (all unix): spawn a child in its own process group, apply
  `setrlimit(RLIMIT_CPU, RLIMIT_AS)`, and `kill(-pgid, SIGKILL)` the whole tree at `wall_ms`.
- **macOS `sandbox-exec` OS confinement**: fs restricted to the per-run workspace subpath,
  network denied (or allowlisted per `NetworkPolicy`).
- **Refuse-loud (fail-closed)** where OS confinement is unavailable (Linux/BSD/CI): a
  sandboxed command **refuses** with a loud node failure — NEVER an unconfined run.
- **Deferred:** the Linux `landlock`(fs) + network-namespace(egress) + `seccomp` backend
  (a dedicated follow-on slice); see §6.

The sandbox confines **external commands**, not the in-process Rust tools (a Rust `call_ctx`
closure cannot be forked into a subprocess). The s3 fs tools (`fs_write`/`fs_read`) stay
in-process under their cooperative jail; the new `shell` tool runs its argv via the OS sandbox
rooted at the SAME per-run workspace.

## 2. Background & impact review

- **No subprocess machinery exists** in the orchestrator (`grep` for `Command`/`libc`/`nix` is
  empty). This slice introduces the first child-process execution + a new `nix` dependency
  (`setrlimit`, `kill`, `signal`) in the orchestrator crate.
- **`ResourceCaps{cpu_ms, mem_bytes, wall_ms}` + `NetworkPolicy` already exist** on
  `Permissions` (`orchestrator-core/src/registry.rs`) and are covered by s1's `covers()` (a
  grant is a ceiling), but are **enforced nowhere**. This slice enforces them.
- **`ToolContext` is the injection seam** (`{idempotency_key, effect_id, credentials,
  workspace_root}`). This slice adds `sandbox: Option<Arc<BoundSandbox>>`.
- **The master spec §225** wants "killing process trees for `shell`"; §288 wants "sandbox +
  resource-cap killing" — this slice is exactly that, macOS-first.
- **Platform reality:** the dev host is macOS/darwin; `main` CI runs on Linux with a coverage
  gate. `sandbox-exec` is macOS-only (Apple-deprecated but functional, no CLI replacement); the
  portable cap-killing is unix-wide. So the slice splits: a portable core (CI-tested on Linux)
  + a macOS-gated confinement layer (dev-tested) + a refuse path (tested on Linux).
- **Impact:** additive — an injected `Option<Arc<dyn Sandbox>>` on the `Executor` (default none
  ⇒ byte-identical), a `sandbox` field on `ToolContext`, one built-in `ShellTool` (opt-in
  registration), and a per-call `BoundSandbox` built in the effect path. No sandbox wired, or no
  `shell` tool registered ⇒ unchanged.

## 4. Design

### 4.1 The portable cap-killing core

`crates/orchestrator/src/agent/sandbox.rs` (orchestrator crate — process I/O; core stays
I/O-free). A pure-mechanism function, no confinement claim:

```rust
pub struct CapOutcome {
    pub exit_code: Option<i32>,   // None if killed by signal
    pub stdout: String,
    pub stderr: String,
    pub killed: Option<KillReason>, // Some(_) => a cap was breached
}
pub enum KillReason { Wall, Cpu, Mem, Signal(i32) }

/// Spawn `argv` as a child in its OWN process group under `caps`, capture stdout/stderr, and
/// KILL the whole group at `wall_ms`. The child's post-fork/pre-exec hook applies
/// setrlimit(RLIMIT_CPU=ceil(cpu_ms/1000), RLIMIT_AS=mem_bytes). RLIMIT_CPU => kernel
/// SIGXCPU/SIGKILL on cpu overrun; RLIMIT_AS => alloc fails. A `None` cap = unlimited for that
/// dimension. All-unix (macOS + Linux).
pub(crate) fn spawn_capped(argv: &[String], caps: &ResourceCaps, stdin: Option<&str>)
    -> Result<CapOutcome, OrchestratorError>;
```

- **Process group:** `std::os::unix::process::CommandExt::process_group(0)` (new group = child
  pid), so the wall-timer can `kill(-pgid, SIGKILL)` the whole tree (defeats a fork-bomb child).
- **rlimits:** via the `CommandExt::pre_exec` hook (runs in the child after fork, before the
  exec) — inside it call `nix::sys::resource::setrlimit` for `RLIMIT_CPU` and `RLIMIT_AS`.
  `RLIMIT_CPU` is in **seconds** (round up from `cpu_ms`).
- **Wall timeout:** a watchdog thread (or a `wait`-with-timeout) `kill`s the group at `wall_ms`,
  then reaps. Killed-vs-exited is read from the `WaitStatus` (signaled → `killed`).
- **Empty argv / spawn failure** → loud `OrchestratorError::Tool`.

### 4.2 The `Sandbox` seam + `MacosSandbox`

```rust
pub struct SandboxSpec<'a> {
    pub argv: &'a [String],          // UNTRUSTED (from the tool/model)
    pub workspace: &'a Path,         // fs confinement root (the s3 per-run jail)
    pub caps: &'a ResourceCaps,      // TRUSTED (from the grant)
    pub network: &'a NetworkPolicy,  // TRUSTED (from the grant)
    pub stdin: Option<&'a str>,
}
pub trait Sandbox: Send + Sync {
    /// Run `argv` OS-confined + capped. Returns the outcome, or `Err` (refuse-loud) where this
    /// platform has no confinement backend.
    fn run(&self, spec: &SandboxSpec) -> Result<CapOutcome, OrchestratorError>;
}
```

`MacosSandbox` (`#[cfg(target_os = "macos")]`): builds a `sandbox-exec -p '<profile>'` wrapper
argv around the real command and delegates to `spawn_capped`. The profile is derived from the
policy:
```
(version 1)
(deny default)
(allow process-fork) (allow process-exec)
(allow file-read*)                                  ; dyld/system libs need broad READ to start
(allow file-write* (subpath "<canonical workspace>"))
(deny network*)                                     ; unless NetworkPolicy::Any/Hosts
```
- **fs:** writes are confined to the workspace subpath (bypass-proof, OS-enforced — supersedes
  the s3 cooperative jail for external commands). Broad file-READ is allowed (a binary can't
  even start otherwise); write-confinement is the security property.
- **network:** `NetworkPolicy::Deny` → `(deny network*)`; `Any` → `(allow network*)`; `Hosts` →
  best-effort `(allow network*)` with a documented coarseness caveat (`sandbox-exec` host-level
  filtering is unreliable; precise host allowlists are deferred to the Linux/proxy layer).
- On **non-macOS**, `MacosSandbox` is not compiled; nothing constructs a `Sandbox` ⇒ refuse.

Injected: `Executor::with_sandbox(Arc<dyn Sandbox>)` (default `None`).

### 4.3 The `ShellTool` + the airtight `BoundSandbox` seam

**`ShellTool`** (built-in, `agent/tools.rs`; name `shell`, `effect_class: Mutation`):
- args `{ "argv": ["cmd","arg",...], "stdin"?: "..." }`.
- `required(args)` → `Permissions { commands: vec![argv[0]], ..Default::default() }` (the s1
  gate authorizes the command; the grant's `commands` allowlist must cover it).
- `call_ctx(args, ctx)`: `let sb = ctx.sandbox.as_ref().ok_or_else(|| OrchestratorError::Tool {
  tool:"shell", message:"sandbox required but unavailable on this platform" })?;` then
  `sb.run(argv, stdin)` → `Ok({ exit_code, stdout, stderr, killed })`. A `killed: Some(_)` or a
  nonzero exit is surfaced faithfully **in the tool result** (a normal `Ok` Value the model
  reacts to — "your command was killed: wall_ms exceeded" / "exit 1"), uniform with any tool
  output. Only a genuine harness error (spawn failure, no sandbox) is an `Err`/refusal. This
  keeps the executor from having to parse the output to decide node success.

**`BoundSandbox` (the crux — executor owns the policy):** the tool supplies only the argv
(untrusted); the executor builds a per-call handle with the policy **fixed**:
```rust
pub struct BoundSandbox {
    inner: Arc<dyn Sandbox>,
    workspace: Arc<PathBuf>,   // the s3 per-run root
    caps: ResourceCaps,        // from the grant (ceiling)
    network: NetworkPolicy,    // from the grant
}
impl BoundSandbox {
    pub fn run(&self, argv: &[String], stdin: Option<&str>) -> Result<CapOutcome, OrchestratorError> {
        self.inner.run(&SandboxSpec { argv, workspace: &self.workspace, caps: &self.caps,
                                      network: &self.network, stdin })
    }
}
```
`ToolContext.sandbox: Option<Arc<BoundSandbox>>`. The tool **cannot widen** caps/workspace/network
— they are not reachable from `args`; it only supplies `argv`. This **closes the s1 arc**: s1
*declared* `caps`+`network` on the grant, s4 *enforces* them (rlimits+kill+`sandbox-exec`).

The executor builds the `BoundSandbox` in the effect path (only when a `Sandbox` is wired AND a
workspace root is resolved AND the acting agent has a grant for the tool): `caps` = the grant's
`caps`, `network` = the grant's `network`, `workspace` = `workspace_root_for(run)`. Absent any of
these ⇒ `ToolContext.sandbox = None` ⇒ the `shell` tool refuses loud.

### 4.4 Determinism, journal & the kill

- **`ShellTool` is a Mutation:** two-phase `EffectIntent` → spawn → `EffectRecorded{exit,stdout,
  stderr,killed}`. On resume a **completed** command **replays its journaled outcome from the
  memo — NEVER re-spawned** (exactly-once). The per-run workspace + argv are the inputs; the
  resolved policy (caps/network) is NOT hashed (executor infra, like `workspace_root`).
- **A kill is a deterministic recorded outcome:** the wall/cpu/mem breach is timing-dependent,
  but **journaled-once** — the first run records the `{killed}`/`{exit}` outcome and a resume
  replays THAT, not the timing. Determinism holds exactly as for any effect. The kill surfaces
  as a normal tool result (`killed: wall_ms exceeded`) the agent reacts to, journaled like any
  Mutation output — NOT special-cased into a node failure.
- **In-doubt resume** (crash between `EffectIntent` and `EffectRecorded`): no `shell` reconciler
  ⇒ `Indeterminate` → `RunPaused` (R3), same as `fs_write` (§6 carry-forward: a reconciler could
  auto-resume idempotent commands).
- **s2 redaction** scrubs stdout/stderr before journaling + feed-back; large stdout rides the
  CAS split (`split_output`).

### 4.5 Refuse-loud (fail-closed) & additive

- **Refuse:** no `Sandbox` wired, OR `sandbox-exec` unavailable (non-macOS), OR no grant/workspace
  ⇒ `ToolContext.sandbox = None` ⇒ `ShellTool::call_ctx` returns a loud `OrchestratorError::Tool`
  → `NodeFailed`. An untrusted command is **NEVER** run unconfined. On Linux/CI the refuse path
  is the tested behavior until the landlock backend lands (§6).
- **Additive:** no `Sandbox` wired ⇒ `ToolContext.sandbox: None`, no `ShellTool` registered ⇒
  the whole path is **byte-identical**. `spawn_capped` + the trait are dead-code-free only once
  the tool + wiring reference them (same T1-style `#[allow(dead_code)]`-until-wired discipline as
  s3 if a task boundary needs it).

### 4.6 Trust boundary

The OS sandbox is a **real** confinement boundary for external commands (bypass-proof fs-write +
network on macOS, cpu/mem/wall kill on all unix) — stronger than the s3 cooperative jail. Limits:
broad file-READ is allowed (needed to run a binary — read-confinement + precise network host
allowlists are deferred); `sandbox-exec` is Apple-deprecated (functional, no replacement);
Linux/BSD have no backend yet (refuse). A cap-kill is best-effort against a determined child that
blocks in SIGKILL-immune states (uninterruptible I/O) — `SIGKILL` to the group is the strongest
portable tool.

## 5. Decisions

- **D1 — sandbox EXTERNAL commands, not in-process Rust tools** [approved]: a Rust `call_ctx`
  closure can't be forked; the confineable unit is an argv. The in-process fs tools keep the s3
  cooperative jail.
- **D2 — portable cap-killing core + macOS `sandbox-exec` confinement; Linux deferred** [approved]:
  cap-killing (setrlimit/pgroup/kill) is unix-portable + CI-testable; OS confinement is per-OS —
  macOS now (dev), Linux landlock a follow-on.
- **D3 — refuse-loud (fail-closed) where confinement is unavailable** [approved]: never run an
  untrusted command unconfined; safe default for a security boundary.
- **D4 — executor owns the policy via `BoundSandbox`; the tool supplies only argv** [approved]:
  the tool cannot widen caps/fs/network; closes the s1 declared→enforced arc.
- **D5 — `ShellTool` is a Mutation; kill = a journaled-once loud outcome** [approved]:
  two-phase, resume-replays-from-memo, in-doubt→pause (no reconciler yet).
- **D6 — `nix` for setrlimit/kill/signal** [approved]: a mature, well-tested libc wrapper
  (preferred over hand-rolled `libc` unsafe, per secure-default guidance).

## 6. Deferred (stated)

- **Linux backend:** `landlock`(fs write-confinement) + a network namespace (`unshare(CLONE_NEWNET)`
  = egress deny) + `seccomp` (syscall filter) + `setrlimit` — a dedicated follow-on slice so
  Linux/CI runs confined instead of refusing. BSD `pledge`/`unveil` likewise.
- **A `shell` reconciler** (idempotent-command auto-resume from in-doubt) — like the deferred
  `fs_write` reconciler.
- **Precise network host allowlists** (`sandbox-exec` host filtering is coarse; a real egress
  proxy or Linux netns+nftables is the future).
- **Read-confinement** (broad file-read is allowed so a binary can start); **cgroups** (finer
  cpu/mem than rlimits); **stdin/pty streaming**, long-running/detached commands; a
  `sandbox-exec`-deprecation migration path.

## 7. Acceptance criteria (TDD)

1. **Cap-kill: wall (portable).** `spawn_capped(["sh","-c","sleep 100"], wall_ms=100)` returns
   `killed: Some(Wall)` within ~a second, NOT after 100s. (Runs on Linux CI + macOS.)
2. **Cap-kill: mem (portable).** A child allocating >`mem_bytes` (`RLIMIT_AS`) fails/dies →
   `killed: Some(Mem)` or a nonzero exit (allocation aborted); a child within the cap succeeds.
3. **Cap-kill: process group (portable).** A command that forks a child (`sh -c 'sleep 100 & wait'`)
   capped at `wall_ms` kills BOTH (the group) — no orphaned `sleep` survives.
4. **Normal run (portable).** `spawn_capped(["sh","-c","echo hi"])` → `exit_code: Some(0)`,
   `stdout: "hi\n"`, `killed: None`.
5. **Refuse-loud (fail-closed).** A `shell` tool with NO sandbox wired (or on a non-macOS host)
   → a loud `NodeFailed` "sandbox required…", NO process run. (Tested on Linux CI.)
6. **macOS fs confinement** (`#[cfg(target_os="macos")]`). A `sandbox-exec` command writing
   OUTSIDE the workspace (`sh -c 'echo x > /tmp/escape'`) is **denied by the OS** (nonzero exit /
   no file), while an in-workspace write **succeeds** — proving OS confinement beyond the
   cooperative jail.
7. **macOS network confinement** (`#[cfg(target_os="macos")]`). A command attempting network
   egress under `NetworkPolicy::Deny` fails; under `Any` it is allowed (mechanism proven).
8. **`BoundSandbox` cannot be widened.** The policy the sandbox enforces (caps/workspace/network)
   comes from the `BoundSandbox` the executor built from the grant, not the tool args — a
   `ShellTool` given any argv runs under the grant's caps (unit: a `BoundSandbox` over a
   recording fake `Sandbox` forwards the grant's caps/workspace/network into the `SandboxSpec`,
   regardless of `argv`).
9. **Resume exactly-once.** A completed `shell` effect, on resume, replays `{exit,stdout,killed}`
   from the memo with the process **not re-spawned** (a spawn-counter / marker proves it) + no
   `DeterminismViolation`.
10. **Redaction composes.** A secret echoed to stdout by the command is `[REDACTED]` in the
    journaled output + fed-back value.
11. **Additive.** No sandbox wired ⇒ the full existing suite (s1/s2/s3/s5/broker) passes
    byte-identical; the `shell` tool is opt-in.
