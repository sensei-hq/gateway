//! SP-4 s4: sandboxed subprocess execution. `spawn_capped` is the portable (all-unix)
//! cap-killing core (process-group + setrlimit + wall-timeout kill); the `Sandbox` trait +
//! `MacosSandbox` add OS-level fs/network confinement (macOS `sandbox-exec`). An external
//! command is confined + capped; where no OS-confinement backend exists the caller refuses.

use std::io::{Read, Write};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use orchestrator_core::{NetworkPolicy, OrchestratorError, ResourceCaps};

/// The outcome of a capped subprocess. `killed: Some(_)` => a resource cap was breached.
/// A deadline race can set `killed: Some(Wall)` alongside a real `exit_code` (the child exited
/// just as the timer fired) — consumers should check `killed` FIRST.
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

/// Spawn `argv` as a child in its OWN process group under `caps`, capture stdout/stderr, and
/// KILL the whole group at `wall_ms`. rlimits (RLIMIT_CPU seconds, RLIMIT_AS bytes) are applied
/// in the child's post-fork hook. Portable across unix (macOS + Linux). No confinement — that is
/// the `Sandbox` layer.
///
/// Note: on Darwin `setrlimit(RLIMIT_AS)` returns EINVAL, so a `mem_bytes: Some(_)` cap makes the
/// child fail-closed (spawn `Err` — it refuses rather than running uncapped); Linux enforces it.
/// The TOTAL grace `spawn_capped` gives its two reader threads to drain after the process
/// group has been killed — shared by stdout and stderr, not granted to each.
///
/// Module-scope so the tests that assert a wall bound derive it from this rather than
/// hardcoding a number. They used to hardcode: the capture path's real worst case was
/// `2 × CAPTURE_GRACE` while one test asserted a 5s bound, leaving under a second of margin
/// and failing about one full-suite run in six. A bound and the constant it must clear
/// cannot be allowed to drift apart when they live in two files.
pub(crate) const CAPTURE_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

// The thin wrapper is reached by tests + `MacosSandbox`; on Linux the only in-crate caller is
// `LinuxSandbox`, which uses `spawn_capped_with` directly, so allow dead_code for it there.
#[allow(dead_code)]
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
#[allow(dead_code)]
pub(crate) fn spawn_capped_with(
    argv: &[String],
    caps: &ResourceCaps,
    stdin: Option<&str>,
    mut extra_pre_exec: Option<Box<dyn FnMut() -> std::io::Result<()> + Send + Sync>>,
) -> Result<CapOutcome, OrchestratorError> {
    let (cmd0, rest) = argv.split_first().ok_or_else(|| OrchestratorError::Tool {
        tool: "sandbox".into(),
        message: "empty argv".into(),
    })?;
    let mut command = Command::new(cmd0);
    command
        .args(rest)
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0); // new group; pgid == child pid

    // `.max(1)`: a `Some(0)` cap must not become RLIMIT_CPU=0 (an instant SIGXCPU on start).
    let cpu_secs = caps.cpu_ms.map(|ms| ms.div_ceil(1000).max(1));
    let mem = caps.mem_bytes;
    // SAFETY: the closure runs post-fork, before the program starts; setrlimit is
    // async-signal-safe.
    unsafe {
        command.pre_exec(move || {
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

    let mut child = command.spawn().map_err(|e| OrchestratorError::Tool {
        tool: "sandbox".into(),
        message: format!("spawn '{cmd0}': {e}"),
    })?;
    let pid = child.id() as i32;

    // Spawn the readers FIRST so both pipes drain while the child runs, then write stdin on its
    // OWN thread (never inline). A child that blocks writing >64KiB of stdout before draining its
    // stdin — or a large stdin payload — would otherwise deadlock the parent before the wall timer
    // is even armed; draining concurrently and writing off-thread keeps the wall cap the backstop.
    let mut out_h = child.stdout.take();
    let mut err_h = child.stderr.take();
    let stdin_h = child.stdin.take();
    // Readers SEND their captured output over a channel (fire-and-forget — NOT joined). A
    // descendant that escapes the process group via setsid() can hold the pipe write-end open
    // past the group kill, so a blocking join would hang the caller forever. On a normal run the
    // group kill closes the pipes and the readers finish immediately; the receive below then
    // returns instantly (no truncation).
    let (out_tx, out_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut s = String::new();
        if let Some(h) = out_h.as_mut() {
            let _ = h.read_to_string(&mut s);
        }
        let _ = out_tx.send(s); // ignore if the receiver already gave up
    });
    let (err_tx, err_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut s = String::new();
        if let Some(h) = err_h.as_mut() {
            let _ = h.read_to_string(&mut s);
        }
        let _ = err_tx.send(s);
    });
    // The stdin writer is fire-and-forget too: a child that never drains a large payload must not
    // block us. Dropping the handle sends EOF.
    if let Some(s) = stdin {
        let s = s.to_string();
        std::thread::spawn(move || {
            if let Some(mut si) = stdin_h {
                let _ = si.write_all(s.as_bytes());
            }
        });
    } else {
        drop(stdin_h);
    }

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
    let status = waiter
        .join()
        .expect("waiter thread panicked")
        .map_err(|e| OrchestratorError::Tool {
            tool: "sandbox".into(),
            message: format!("wait: {e}"),
        })?;
    // Reap any stragglers: a backgrounded descendant may still hold the pipe write-end and
    // outlive the direct child. The direct child is already reaped, so this only terminates
    // lingering group members (the containment goal) and bounds output capture.
    let _ = nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(-pid),
        nix::sys::signal::Signal::SIGKILL,
    );
    // A descendant that escaped the process group via setsid() can hold the pipe write-end open
    // past the group kill; don't block the caller forever on EOF. Give the readers a short grace
    // to drain, then return with whatever was captured. The escapee stays OS-alive but
    // Seatbelt-confined; the wall cap's caller-return guarantee is preserved.
    //
    // ONE deadline shared by both streams, not one grace each. `CAPTURE_GRACE` is documented
    // above as "a short grace to drain" — that is a property of the CALL, and two sequential
    // `recv_timeout(CAPTURE_GRACE)` calls silently made the real bound `2 × CAPTURE_GRACE`,
    // because a stdout that never EOFs consumes its whole grace before stderr's even starts.
    //
    // That doubling is what made `a_backgrounded_straggler_is_reaped_on_clean_exit` flaky: its
    // 5s assertion sat barely above the fixed path's own 4s worst case, so under full-suite
    // load — where process spawn and scheduling eat the difference — it failed roughly one run
    // in six while passing every time in isolation. The bound was not too tight; the path was
    // twice as slow as its own comment claimed. Fixing the test's number instead would have
    // preserved a 4s stall on every escaped-descendant capture in production.
    //
    // `saturating_sub` on the remainder: if stdout used the entire grace, stderr gets a
    // zero-length wait, which `recv_timeout` treats as a single non-blocking poll — still
    // correct (a reader that has already sent is picked up), and it cannot extend the total.
    let deadline = std::time::Instant::now() + CAPTURE_GRACE;
    let stdout = out_rx.recv_timeout(CAPTURE_GRACE).unwrap_or_default();
    let stderr = err_rx
        .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
        .unwrap_or_default();

    let killed = if wall_killed {
        Some(KillReason::Wall)
    } else {
        match status.signal() {
            Some(s) if s == nix::sys::signal::Signal::SIGXCPU as i32 => Some(KillReason::Cpu),
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
    pub fn new(
        inner: Arc<dyn Sandbox>,
        workspace: Arc<PathBuf>,
        caps: ResourceCaps,
        network: NetworkPolicy,
    ) -> Self {
        Self {
            inner,
            workspace,
            caps,
            network,
        }
    }
    /// Run `argv` under the fixed policy.
    pub fn run(
        &self,
        argv: &[String],
        stdin: Option<&str>,
    ) -> Result<CapOutcome, OrchestratorError> {
        self.inner.run(&SandboxSpec {
            argv,
            workspace: &self.workspace,
            caps: &self.caps,
            network: &self.network,
            stdin,
        })
    }
}

/// macOS `sandbox-exec` backend: fs writes confined to the workspace subpath, network per policy.
#[cfg(target_os = "macos")]
pub struct MacosSandbox;

#[cfg(target_os = "macos")]
impl Sandbox for MacosSandbox {
    fn run(&self, spec: &SandboxSpec) -> Result<CapOutcome, OrchestratorError> {
        // The workspace path is interpolated into the Seatbelt profile; a path containing profile
        // metacharacters would malform (never widen) it. Refuse loud so fail-closed is explicit.
        let ws = spec
            .workspace
            .to_str()
            .ok_or_else(|| OrchestratorError::Tool {
                tool: "sandbox".into(),
                message: "workspace path is not valid UTF-8 for a sandbox profile".into(),
            })?;
        if ws.contains(['"', '\\', '\n']) {
            return Err(OrchestratorError::Tool {
                tool: "sandbox".into(),
                message: "workspace path contains characters unsafe for a sandbox profile".into(),
            });
        }
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
        // NOTE: `Hosts(_)` coarsens to allow-ALL on macOS — Seatbelt host-level filtering is
        // unreliable, so precise host allowlists are deferred to the Linux/proxy layer (spec §6).
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

/// SP-4 Linux backend: landlock fs-write confinement to the workspace + seccomp egress-deny +
/// the portable cap-killing. Network `Deny` denies `socket(AF_INET|AF_INET6)` (EPERM) via seccomp;
/// `Any`/`Hosts` run unfiltered. Unprivileged (landlock ≥ 5.13). Parity target: `MacosSandbox`.
#[cfg(target_os = "linux")]
pub struct LinuxSandbox;

#[cfg(target_os = "linux")]
impl Sandbox for LinuxSandbox {
    fn run(&self, spec: &SandboxSpec) -> Result<CapOutcome, OrchestratorError> {
        // BUILD the confinement in the PARENT (the landlock ruleset + seccomp BPF program allocate).
        // Refuse-loud if the fs-confinement can't be built (e.g. an unopenable workspace) — never an
        // unconfined run.
        let mut ruleset = Some(build_landlock_ruleset(spec.workspace)?);
        // Egress-deny is a seccomp filter that denies `socket(AF_INET|AF_INET6)` (EPERM). Built in
        // the PARENT (it allocates); refuse-loud if the build fails. `Any`/`Hosts(coarse)` => no
        // network filter (Hosts is coarsened to allow-all here, per the macOS parity note).
        let mut bpf = match spec.network {
            NetworkPolicy::Any | NetworkPolicy::Hosts(_) => None,
            NetworkPolicy::Deny => Some(build_egress_deny_filter()?),
        };
        // APPLY in the CHILD via the extra pre_exec hook. The hook runs post-fork, before execve,
        // so it must be async-signal-safe: it issues syscalls ONLY (`restrict_self` +
        // `apply_filter`; the ruleset AND the BPF program were allocated in the parent) on the
        // success path, sidestepping the fork+malloc-lock hazard. Every abort path is ALSO
        // alloc-free + panic-free — errno-only `io::Error::from_raw_os_error(EPERM)` (no `format!`,
        // no `expect`) — so a multithreaded parent's held malloc/unwind locks can never deadlock the
        // child. The parent surfaces a failed hook as "spawn ... : Operation not permitted" (a
        // fail-closed refuse).
        let hook: Box<dyn FnMut() -> std::io::Result<()> + Send + Sync> = Box::new(move || {
            // `let-else`, not `.expect()`: the FnMut runs once so `None` is unreachable, but never
            // panic post-fork.
            let Some(rs) = ruleset.take() else {
                return Err(std::io::Error::from_raw_os_error(libc::EPERM));
            };
            let status = rs
                .restrict_self()
                .map_err(|_| std::io::Error::from_raw_os_error(libc::EPERM))?;
            // Fail-closed: if landlock did not enforce (kernel < 5.13 / not compiled in), the fs
            // confinement did NOT take — abort the child so the command never runs unconfined.
            if status.ruleset == landlock::RulesetStatus::NotEnforced {
                return Err(std::io::Error::from_raw_os_error(libc::EPERM));
            }
            // Apply seccomp egress-deny AFTER landlock: the filter is allow-by-default (only
            // `socket(AF_INET|AF_INET6)` is denied), so it never blocks execve or the landlock
            // syscall. `apply_filter` also sets NO_NEW_PRIVS. Syscalls only (BPF pre-built).
            if let Some(program) = bpf.take() {
                seccompiler::apply_filter(&program)
                    .map_err(|_| std::io::Error::from_raw_os_error(libc::EPERM))?;
            }
            Ok(())
        });
        spawn_capped_with(spec.argv, spec.caps, spec.stdin, Some(hook))
    }
}

/// Build a landlock ruleset: broad READ+execute on `/` (a binary must start + read shared libs),
/// WRITE (+create/remove/make-* + TRUNCATE/REFER/IOCTL_DEV) confined to the canonical `workspace`
/// subpath, plus a WriteFile+Truncate carve-out for safe pseudo-devices (`/dev/null` &c) so common
/// redirects work. Handles the ABI V5 access set (`CompatLevel::BestEffort` degrades gracefully on
/// older kernels) — CRUCIAL: landlock only mediates the rights it HANDLES, so handling only ABI-1
/// would leave `truncate(2)`/`ftruncate(2)` (the ABI-3 TRUNCATE right) UNMEDIATED, letting a
/// confined command zero any writable file OUTSIDE the workspace. Built in the PARENT (allocates);
/// `restrict_self` is applied in the child. An unopenable `workspace` makes the build fail
/// (refuse-loud).
#[cfg(target_os = "linux")]
fn build_landlock_ruleset(
    workspace: &std::path::Path,
) -> Result<landlock::RulesetCreated, OrchestratorError> {
    use landlock::{
        ABI, Access, AccessFs, CompatLevel, Compatible, PathBeneath, PathFd, Ruleset, RulesetAttr,
        RulesetCreatedAttr, path_beneath_rules,
    };
    let mk = |m: String| OrchestratorError::Tool {
        tool: "sandbox".into(),
        message: format!("landlock: {m}"),
    };
    // ABI V5 (not V1) so TRUNCATE (ABI v3) is a HANDLED right — see this fn's doc; handling only
    // V1 would leave `truncate(2)`/`ftruncate(2)` unmediated (an outside-the-jail write-escape).
    let abi = ABI::V5;
    // Open the workspace fd EXPLICITLY and propagate a failed open — do NOT route the workspace
    // through `path_beneath_rules`, which swallows a failed `PathFd::new` (`Err(_) => None`) as a
    // silently-skipped rule. An unopenable workspace must fail the build => `run` refuses-loud (this
    // fn's documented contract), never a silently-narrowed ruleset with the workspace-write rule
    // dropped. (A real canonical workspace opens fine, so this is byte-identical for live runs.)
    let ws_fd = PathFd::new(workspace).map_err(|e| mk(format!("open workspace: {e}")))?;
    // Parity with `MacosSandbox`'s /dev carve-out: writes to these pseudo-devices are safe
    // (`/dev/null` discards, the rest are read-mostly), so a WriteFile grant on the exact existing
    // nodes lets common redirects (`2>/dev/null`, `>/dev/null 2>&1`) work. Filtered to nodes that
    // exist (e.g. `/dev/tty` may be absent) so `PathFd::new` never fails the whole build. Scoped to
    // those device files only — WriteFile ONLY, no create/dir rights — so the workspace jail is NOT
    // widened (a write anywhere else stays denied).
    let dev_nodes = [
        "/dev/null",
        "/dev/zero",
        "/dev/full",
        "/dev/random",
        "/dev/urandom",
        "/dev/tty",
    ];
    let dev_writable: Vec<&str> = dev_nodes
        .into_iter()
        .filter(|p| std::path::Path::new(p).exists())
        .collect();
    let ruleset = Ruleset::default()
        .set_compatibility(CompatLevel::BestEffort)
        .handle_access(AccessFs::from_all(abi))
        .map_err(|e| mk(e.to_string()))?
        .create()
        .map_err(|e| mk(e.to_string()))?
        // Broad READ+execute on `/` so the binary + its shared libs load.
        .add_rules(path_beneath_rules(["/"], AccessFs::from_read(abi)))
        .map_err(|e| mk(e.to_string()))?
        // WRITE (+create/remove/make-*) confined to the workspace subpath (fd opened above so an
        // unopenable workspace already refused-loud). `from_all` is the same access
        // `path_beneath_rules` grants a directory, so this is byte-identical for a live workspace.
        .add_rule(PathBeneath::new(ws_fd, AccessFs::from_all(abi)))
        .map_err(|e| mk(e.to_string()))?
        // WriteFile+Truncate on the safe pseudo-devices (does NOT widen the workspace jail). Truncate
        // is REQUIRED now that TRUNCATE is handled: `>/dev/null` opens `O_WRONLY|O_TRUNC`, so without
        // the Truncate right on the node the redirect would regress.
        .add_rules(path_beneath_rules(
            dev_writable,
            AccessFs::WriteFile | AccessFs::Truncate,
        ))
        .map_err(|e| mk(e.to_string()))?;
    Ok(ruleset)
}

/// Build a seccomp BPF program that denies IP-socket creation (egress) while allowing everything
/// else: `socket(domain == AF_INET|AF_INET6|AF_PACKET, ..)` -> EPERM, plus `io_uring_setup` -> EPERM
/// (io_uring can create+use an AF_INET socket without the `socket` syscall, bypassing the domain
/// filter). `connect()` can't be filtered by address (seccomp can't deref the sockaddr pointer), so
/// socket-creation is the chokepoint; AF_UNIX/AF_NETLINK stay allowed so programs start. Built in
/// the PARENT (allocates); the child's `pre_exec` hook only `apply_filter`s it (syscalls). Filter is
/// allow-by-default (mismatch=Allow), so applying it never blocks execve or the landlock syscall.
#[cfg(target_os = "linux")]
fn build_egress_deny_filter() -> Result<seccompiler::BpfProgram, OrchestratorError> {
    use seccompiler::{
        SeccompAction, SeccompCmpArgLen, SeccompCmpOp, SeccompCondition, SeccompFilter, SeccompRule,
    };
    use std::collections::BTreeMap;
    let mk = |m: String| OrchestratorError::Tool {
        tool: "sandbox".into(),
        message: format!("seccomp: {m}"),
    };
    let arch = if cfg!(target_arch = "x86_64") {
        seccompiler::TargetArch::x86_64
    } else if cfg!(target_arch = "aarch64") {
        seccompiler::TargetArch::aarch64
    } else {
        return Err(mk("unsupported arch for seccomp".into()));
    };
    // arg0 (domain) == AF_INET  -> the rule matches -> match_action (EPERM).
    let inet = SeccompRule::new(vec![
        SeccompCondition::new(
            0,
            SeccompCmpArgLen::Dword,
            SeccompCmpOp::Eq,
            libc::AF_INET as u64,
        )
        .map_err(|e| mk(e.to_string()))?,
    ])
    .map_err(|e| mk(e.to_string()))?;
    // arg0 (domain) == AF_INET6 -> the rule matches -> match_action (EPERM).
    let inet6 = SeccompRule::new(vec![
        SeccompCondition::new(
            0,
            SeccompCmpArgLen::Dword,
            SeccompCmpOp::Eq,
            libc::AF_INET6 as u64,
        )
        .map_err(|e| mk(e.to_string()))?,
    ])
    .map_err(|e| mk(e.to_string()))?;
    // Also deny AF_PACKET (raw L2 frames) — closes raw egress even if the child were to hold
    // CAP_NET_RAW (this layer doesn't guarantee it doesn't). AF_UNIX/AF_NETLINK stay allowed.
    let packet = SeccompRule::new(vec![
        SeccompCondition::new(
            0,
            SeccompCmpArgLen::Dword,
            SeccompCmpOp::Eq,
            libc::AF_PACKET as u64,
        )
        .map_err(|e| mk(e.to_string()))?,
    ])
    .map_err(|e| mk(e.to_string()))?;
    let mut rules: BTreeMap<i64, Vec<SeccompRule>> = BTreeMap::new();
    rules.insert(libc::SYS_socket, vec![inet, inet6, packet]);
    // io_uring can create+use an AF_INET socket WITHOUT the socket() syscall (IORING_OP_SOCKET/
    // CONNECT/SEND), bypassing the socket-domain filter entirely on kernels ≥ 5.19. Deny
    // io_uring_setup outright (an EMPTY rule vec matches the syscall unconditionally → the
    // match_action EPERM) so no io_uring instance can be created in the first place.
    rules.insert(libc::SYS_io_uring_setup, vec![]);
    // mismatch_action = Allow (everything not matched runs); match_action = Errno(EPERM).
    let filter = SeccompFilter::new(
        rules,
        SeccompAction::Allow,
        SeccompAction::Errno(libc::EPERM as u32),
        arch,
    )
    .map_err(|e| mk(e.to_string()))?;
    let bpf: seccompiler::BpfProgram = filter
        .try_into()
        .map_err(|e: seccompiler::BackendError| mk(e.to_string()))?;
    Ok(bpf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caps(wall_ms: Option<u64>, mem_bytes: Option<u64>) -> ResourceCaps {
        ResourceCaps {
            cpu_ms: None,
            mem_bytes,
            wall_ms,
        }
    }
    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn normal_run_captures_stdout_and_exit_zero() {
        let out = spawn_capped(
            &argv(&["sh", "-c", "echo hi"]),
            &caps(Some(5000), None),
            None,
        )
        .unwrap();
        assert_eq!(out.exit_code, Some(0));
        assert_eq!(out.stdout, "hi\n");
        assert_eq!(out.killed, None);
    }

    #[test]
    fn wall_cap_kills_a_runaway() {
        let start = std::time::Instant::now();
        let out = spawn_capped(
            &argv(&["sh", "-c", "sleep 100"]),
            &caps(Some(150), None),
            None,
        )
        .unwrap();
        assert_eq!(
            out.killed,
            Some(KillReason::Wall),
            "expected a wall-timeout kill"
        );
        // Derived, for the same reason as the two below: the killed `sleep` holds both pipes,
        // so this path pays the capture grace in full. At `2 × CAPTURE_GRACE` it was
        // 150ms + 4s against a hardcoded 5s — the same sub-second margin that made
        // `a_backgrounded_straggler_is_reaped_on_clean_exit` fail one full-suite run in six.
        // It had not failed yet; it was the same defect waiting for a slower machine.
        let bound = Duration::from_millis(150) + CAPTURE_GRACE + Duration::from_secs(3);
        assert!(
            start.elapsed() < bound,
            "took too long: the wall timer did not kill it (elapsed {:?}, bound {bound:?})",
            start.elapsed()
        );
    }

    #[test]
    fn wall_cap_kills_the_whole_process_group() {
        // The shell forks a background `sleep` then waits; killing only the shell would orphan
        // the sleep. A process-group kill takes both. We assert the call returns promptly
        // (if the group kill failed, `wait` on the shell would block on the child sleep).
        let start = std::time::Instant::now();
        let out = spawn_capped(
            &argv(&["sh", "-c", "sleep 100 & wait"]),
            &caps(Some(150), None),
            None,
        )
        .unwrap();
        assert_eq!(out.killed, Some(KillReason::Wall));
        // Derived — same exposure as its sibling above.
        let bound = Duration::from_millis(150) + CAPTURE_GRACE + Duration::from_secs(3);
        assert!(
            start.elapsed() < bound,
            "process-group kill did not reap the forked child (elapsed {:?}, bound {bound:?})",
            start.elapsed()
        );
    }

    #[test]
    fn a_backgrounded_straggler_is_reaped_on_clean_exit() {
        // The direct child exits 0 immediately, but a backgrounded `sleep` inherits the stdout
        // write-end. Without the post-wait group reap, reading stdout would block until the
        // straggler's 30s sleep ends. With it, the call returns promptly.
        let start = std::time::Instant::now();
        let out = spawn_capped(
            &argv(&["sh", "-c", "sleep 30 & exit 0"]),
            &caps(Some(5000), None),
            None,
        )
        .unwrap();
        assert_eq!(out.exit_code, Some(0));
        // Derived from CAPTURE_GRACE, not hardcoded. This assertion used to read `< 5s`
        // against a capture path whose own worst case was `2 × CAPTURE_GRACE` = 4s, so it
        // carried under a second of margin and failed roughly one full-suite run in six
        // (never in isolation — the load is what ate the margin). The capture now shares ONE
        // deadline across both streams, so the fixed path is bounded by CAPTURE_GRACE, and
        // this bound is that plus a generous allowance for spawn and scheduling under load.
        let bound = CAPTURE_GRACE + Duration::from_secs(3);
        assert!(
            start.elapsed() < bound,
            "straggler not reaped — spawn_capped hung on the pipe (elapsed {:?}, bound {bound:?})",
            start.elapsed()
        );
    }

    #[test]
    fn a_session_escaped_descendant_does_not_hang_capture() {
        // A grandchild that setsid()s escapes the process group AND holds the stdout/stderr pipes,
        // so the group kill can't reap it. Bounded capture must still return promptly (not block on
        // EOF). The grandchild syncs on a pipe so it has provably setsid()'d BEFORE the direct
        // child (its parent) exits — otherwise the straggler-reap would race and kill it first,
        // making this test only intermittently exercise the escape. The stray grandchild sleeps 8s
        // (self-cleans): longer than the fixed path's CAPTURE_GRACE, so a regression to a
        // blocking capture would wait the full 8s and blow the bound; short enough not to bloat
        // the parallel test run or destabilize sibling wall-timer tests.
        //
        // The fixed path used to be ~4s here (`2 × CAPTURE_GRACE`, one grace per stream) and
        // this comment said so. Both streams now share one deadline, so it is CAPTURE_GRACE —
        // which is why the 8s straggler still separates a working capture from a blocking one
        // with room to spare.
        let start = std::time::Instant::now();
        let out = spawn_capped(
            &argv(&[
                "perl",
                "-e",
                "use POSIX qw(setsid); pipe(R,W); my $pid=fork(); \
                 if($pid==0){ close(R); setsid(); close(W); sleep 8; } \
                 else { close(W); my $x=<R>; exit 0; }",
            ]),
            &caps(Some(3000), None),
            None,
        )
        .unwrap();
        assert_eq!(out.exit_code, Some(0));
        // Also derived. The escapee holds BOTH pipes, so this is the path that actually
        // consumes the whole grace — and the one the shared deadline halves.
        let bound = CAPTURE_GRACE + Duration::from_secs(4);
        assert!(
            start.elapsed() < bound,
            "capture blocked on a session-escaped descendant holding the pipe (elapsed {:?}, \
             bound {bound:?})",
            start.elapsed()
        );
    }

    #[test]
    fn cpu_cap_kills_a_busy_loop() {
        // RLIMIT_CPU is POSIX + implemented on Darwin (unlike RLIMIT_AS). A busy loop capped at 1s
        // CPU is killed by the cpu cap. The delivered signal is arch/kernel-specific: SIGXCPU(24)
        // => Cpu on macOS + x86_64, but SIGKILL(9) => Signal(9) on arm64 kernels where soft==hard
        // (the hard-limit SIGKILL races ahead of the soft-limit SIGXCPU). Both are a real cpu-cap
        // breach — accept either. Wall set high (15s) so the WALL timer doesn't fire first: if
        // RLIMIT_CPU were NOT applied, the loop would run to the wall cap => killed: Some(Wall),
        // which is NEITHER arm => the assertion catches that regression (and the test returns in
        // ~1s from the cpu kill, not 15s).
        let out = spawn_capped(
            &argv(&["sh", "-c", "while :; do :; done"]),
            &ResourceCaps {
                cpu_ms: Some(1000),
                mem_bytes: None,
                wall_ms: Some(15000),
            },
            None,
        )
        .unwrap();
        assert!(
            matches!(
                out.killed,
                Some(KillReason::Cpu) | Some(KillReason::Signal(9))
            ),
            "expected a CPU-cap kill — SIGXCPU=>Cpu (macOS/x86_64) or SIGKILL(9) where soft==hard (arm64 kernels), got {out:?}"
        );
    }

    #[test]
    fn mem_cap_prevents_a_clean_success() {
        // With a tiny RLIMIT_AS the child must NOT cleanly succeed. `awk` growing a huge string
        // is a portable allocator. The invariant we assert is "requesting an 8MiB mem cap yields
        // NO clean success", satisfied two ways: on Linux the applied RLIMIT_AS aborts/errors the
        // allocator (nonzero exit or a kill signal); on macOS `setrlimit(RLIMIT_AS)` is not
        // implemented and returns EINVAL, so `spawn_capped` refuses at spawn (fail-closed `Err`)
        // — the cap is never silently dropped. Either way, no clean success.
        let cmd = argv(&[
            "awk",
            "BEGIN{ a=\"\"; for(i=0;i<20000000;i++) a=a\"x\"; print length(a) }",
        ]);
        let result = spawn_capped(&cmd, &caps(Some(10000), Some(8 * 1024 * 1024)), None);
        let clean_success =
            matches!(&result, Ok(o) if o.killed.is_none() && o.exit_code == Some(0));
        assert!(
            !clean_success,
            "an 8MiB mem cap should have prevented a clean success, got {result:?}"
        );
    }

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
        let out = EchoSandbox
            .run(&SandboxSpec {
                argv: &a,
                workspace: &ws,
                caps: &caps(Some(5000), None),
                network: &orchestrator_core::NetworkPolicy::Deny,
                stdin: None,
            })
            .unwrap();
        assert_eq!(out.stdout, "ok\n");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_sandbox_runs_an_allowed_command() {
        let td = tempfile::tempdir().unwrap();
        let ws = td.path().canonicalize().unwrap();
        let a = argv(&["sh", "-c", "echo ok"]);
        let out = MacosSandbox
            .run(&SandboxSpec {
                argv: &a,
                workspace: &ws,
                caps: &caps(Some(5000), None),
                network: &orchestrator_core::NetworkPolicy::Deny,
                stdin: None,
            })
            .unwrap();
        assert_eq!(
            out.exit_code,
            Some(0),
            "sandbox-exec blocked a trivial command: {out:?}"
        );
        assert_eq!(out.stdout, "ok\n");
    }

    // ── Task 5: real `sandbox-exec` (Seatbelt) confinement e2e (macOS dev box only) ──

    /// THE load-bearing confinement proof: a write to a path OUTSIDE the workspace must be denied
    /// by the OS. The escape target lives in a DIFFERENT tempdir than the workspace, so the only
    /// thing that could let the file land is a too-permissive `file-write*` allowance.
    #[cfg(target_os = "macos")]
    #[test]
    fn macos_denies_write_outside_the_workspace() {
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let escape = tempfile::tempdir().unwrap();
        let target = escape.path().canonicalize().unwrap().join("escaped");
        let a = argv(&["sh", "-c", &format!("echo x > {}", target.display())]);
        let out = MacosSandbox
            .run(&SandboxSpec {
                argv: &a,
                workspace: &root,
                caps: &caps(Some(5000), None),
                network: &orchestrator_core::NetworkPolicy::Deny,
                stdin: None,
            })
            .unwrap();
        assert!(
            !target.exists(),
            "a file escaped the sandbox workspace (write confinement is broken): {out:?}"
        );
    }

    /// The complement: a write INSIDE the workspace subpath must succeed (exit 0 + the file lands).
    #[cfg(target_os = "macos")]
    #[test]
    fn macos_allows_write_inside_the_workspace() {
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let a = argv(&[
            "sh",
            "-c",
            &format!("echo x > {}/inside.txt", root.display()),
        ]);
        let out = MacosSandbox
            .run(&SandboxSpec {
                argv: &a,
                workspace: &root,
                caps: &caps(Some(5000), None),
                network: &orchestrator_core::NetworkPolicy::Deny,
                stdin: None,
            })
            .unwrap();
        assert_eq!(
            out.exit_code,
            Some(0),
            "in-workspace write was denied: {out:?}"
        );
        assert!(
            root.join("inside.txt").exists(),
            "in-workspace write did not land: {out:?}"
        );
    }

    /// Network egress: a real CONTRAST against a LIVE loopback listener. A closed-port probe would
    /// be vacuous (connection-refused whether or not Seatbelt denies), so we bind an actual
    /// `TcpListener` in the (unsandboxed) test and run the SAME connect under `Any` (positive
    /// control → must connect) and `Deny` (must be blocked). Because the port IS listening, a Deny
    /// failure can only be Seatbelt, never conn-refused.
    #[cfg(target_os = "macos")]
    #[test]
    fn macos_network_deny_blocks_a_live_socket_that_any_allows() {
        use std::net::TcpListener;
        // A real listener the sandboxed probe can actually reach (no accept() needed — the kernel
        // completes the handshake from the listen backlog, so connect() succeeds).
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let probe = format!("exec 3<>/dev/tcp/127.0.0.1/{port} && echo connected || echo blocked");
        let a = argv(&["bash", "-c", &probe]);

        // Positive control: Any allows the connect to the LIVE listener.
        let allowed = MacosSandbox
            .run(&SandboxSpec {
                argv: &a,
                workspace: &root,
                caps: &caps(Some(5000), None),
                network: &orchestrator_core::NetworkPolicy::Any,
                stdin: None,
            })
            .unwrap();
        assert!(
            allowed.stdout.contains("connected"),
            "positive control failed — Any did not allow the loopback connect: {allowed:?}"
        );

        // Deny: the SAME connect to the SAME live listener is blocked by Seatbelt (not conn-refused).
        let denied = MacosSandbox
            .run(&SandboxSpec {
                argv: &a,
                workspace: &root,
                caps: &caps(Some(5000), None),
                network: &orchestrator_core::NetworkPolicy::Deny,
                stdin: None,
            })
            .unwrap();
        assert!(
            denied.stdout.contains("blocked") && !denied.stdout.contains("connected"),
            "network egress was NOT denied under Deny (a live listener → only Seatbelt can block): {denied:?}"
        );
    }

    /// Cap-killing composes WITH OS confinement: a `sleep 100` inside the real sandbox must still be
    /// wall-killed (the sandbox-exec wrapper runs in the same capped process group).
    #[cfg(target_os = "macos")]
    #[test]
    fn macos_wall_kill_through_the_sandbox() {
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let a = argv(&["sh", "-c", "sleep 100"]);
        let out = MacosSandbox
            .run(&SandboxSpec {
                argv: &a,
                workspace: &root,
                caps: &caps(Some(200), None),
                network: &orchestrator_core::NetworkPolicy::Deny,
                stdin: None,
            })
            .unwrap();
        assert_eq!(
            out.killed,
            Some(KillReason::Wall),
            "wall cap did not fire through the sandbox: {out:?}"
        );
    }

    // ── SP-4 linux-sandbox (2/4): landlock fs-write confinement e2e (Docker/Linux only) ──

    /// THE load-bearing landlock proof: a write to a path OUTSIDE the workspace must be denied
    /// (landlock confines WRITE to the workspace subpath). The escape target lives in a DIFFERENT
    /// tempdir, so the only thing that could let the file land is a too-permissive write rule.
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_denies_write_outside_the_workspace() {
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let escape = tempfile::tempdir().unwrap();
        let target = escape.path().join("escaped");
        let a = argv(&["sh", "-c", &format!("echo x > {}", target.display())]);
        let out = LinuxSandbox
            .run(&SandboxSpec {
                argv: &a,
                workspace: &root,
                caps: &caps(Some(5000), None),
                network: &orchestrator_core::NetworkPolicy::Any,
                stdin: None,
            })
            .unwrap();
        assert!(
            !target.exists(),
            "a file escaped the landlock workspace: {out:?}"
        );
    }

    /// The complement: a write INSIDE the workspace subpath must succeed (exit 0 + the file lands).
    /// Rules out "sh couldn't start / all writes denied".
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_allows_write_inside_the_workspace() {
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let a = argv(&[
            "sh",
            "-c",
            &format!("echo hi > {}/inside.txt", root.display()),
        ]);
        let out = LinuxSandbox
            .run(&SandboxSpec {
                argv: &a,
                workspace: &root,
                caps: &caps(Some(5000), None),
                network: &orchestrator_core::NetworkPolicy::Any,
                stdin: None,
            })
            .unwrap();
        assert_eq!(
            out.exit_code,
            Some(0),
            "in-workspace write was denied: {out:?}"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("inside.txt")).unwrap(),
            "hi\n"
        );
    }

    /// Nested writes/mkdir inside the workspace must also succeed (create/make-dir handling).
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_allows_nested_write_inside_the_workspace() {
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let a = argv(&[
            "sh",
            "-c",
            &format!(
                "mkdir -p {r}/a/b && echo hi > {r}/a/b/c.txt",
                r = root.display()
            ),
        ]);
        let out = LinuxSandbox
            .run(&SandboxSpec {
                argv: &a,
                workspace: &root,
                caps: &caps(Some(5000), None),
                network: &orchestrator_core::NetworkPolicy::Any,
                stdin: None,
            })
            .unwrap();
        assert_eq!(
            out.exit_code,
            Some(0),
            "nested in-workspace write/mkdir denied: {out:?}"
        );
        assert!(root.join("a/b/c.txt").exists());
    }

    /// Cap-killing composes WITH landlock: a `sleep 100` inside the confined child must still be
    /// wall-killed (the confinement hook runs in the same capped process group).
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_wall_kill_composes() {
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let a = argv(&["sh", "-c", "sleep 100"]);
        let out = LinuxSandbox
            .run(&SandboxSpec {
                argv: &a,
                workspace: &root,
                caps: &caps(Some(200), None),
                network: &orchestrator_core::NetworkPolicy::Any,
                stdin: None,
            })
            .unwrap();
        assert_eq!(
            out.killed,
            Some(KillReason::Wall),
            "cap-killing must compose with landlock"
        );
    }

    /// The /dev carve-out (parity with `MacosSandbox`): common redirects to `/dev/null` must work
    /// under landlock. Paired with `linux_denies_write_outside_the_workspace` (unchanged), this
    /// proves the carve-out grants ONLY the specific device nodes — it does NOT widen the jail.
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_allows_writing_to_dev_null() {
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let a = argv(&[
            "sh",
            "-c",
            "echo hi 2>/dev/null; echo x > /dev/null; echo done",
        ]);
        let out = LinuxSandbox
            .run(&SandboxSpec {
                argv: &a,
                workspace: &root,
                caps: &caps(Some(5000), None),
                network: &orchestrator_core::NetworkPolicy::Any,
                stdin: None,
            })
            .unwrap();
        assert_eq!(
            out.exit_code,
            Some(0),
            "redirect to /dev/null was denied: {out:?}"
        );
        assert!(out.stdout.contains("done"));
    }

    /// Whole-slice-review regression: TRUNCATE (ABI v3) must be a HANDLED+ungranted right OUTSIDE
    /// the workspace — else `truncate(2)` is unmediated and a confined command can zero any writable
    /// file (a data-destruction escape). Non-vacuous: under an `ABI::V1` pin this fails (the outside
    /// file becomes size 0); under the `ABI::V5` fix landlock denies the truncate → the file keeps
    /// its 10 bytes and perl prints "trunc-denied".
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_denies_truncate_outside_the_workspace() {
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let victim_dir = tempfile::tempdir().unwrap();
        let victim = victim_dir.path().join("data");
        std::fs::write(&victim, b"0123456789").unwrap(); // 10 bytes, OUTSIDE the workspace
        let a = argv(&[
            "perl",
            "-e",
            &format!(
                "truncate('{}', 0) or print \"trunc-denied\\n\"",
                victim.display()
            ),
        ]);
        let out = LinuxSandbox
            .run(&SandboxSpec {
                argv: &a,
                workspace: &root,
                caps: &caps(Some(5000), None),
                network: &orchestrator_core::NetworkPolicy::Any,
                stdin: None,
            })
            .unwrap();
        let size = std::fs::metadata(&victim).unwrap().len();
        assert_eq!(
            size, 10,
            "truncate() escaped the workspace and zeroed an outside file: {out:?}"
        );
    }

    // ── SP-4 linux-sandbox (3/4): seccomp egress-deny e2e (Docker/Linux only) ──

    /// THE load-bearing seccomp proof: a real CONTRAST against a LIVE loopback listener. Bind an
    /// actual `TcpListener` (unsandboxed) and run the SAME connect under `Any` (positive control →
    /// must connect) and `Deny` (must be blocked). Because the port IS listening, a Deny failure can
    /// only be the seccomp `socket(AF_INET)->EPERM`, never connection-refused.
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
        let allowed = LinuxSandbox
            .run(&SandboxSpec {
                argv: &a,
                workspace: &root,
                caps: &caps(Some(5000), None),
                network: &orchestrator_core::NetworkPolicy::Any,
                stdin: None,
            })
            .unwrap();
        assert!(
            allowed.stdout.contains("connected"),
            "positive control: Any must allow the loopback connect: {allowed:?}"
        );
        let denied = LinuxSandbox
            .run(&SandboxSpec {
                argv: &a,
                workspace: &root,
                caps: &caps(Some(5000), None),
                network: &orchestrator_core::NetworkPolicy::Deny,
                stdin: None,
            })
            .unwrap();
        assert!(
            denied.stdout.contains("blocked") && !denied.stdout.contains("connected"),
            "Deny must block the IP connect (seccomp socket(AF_INET)->EPERM): {denied:?}"
        );
    }

    /// The filter is SURGICAL: it denies `socket(AF_INET|AF_INET6)` only, so AF_UNIX still works and
    /// ordinary programs start. Prove an AF_UNIX socket succeeds under Deny.
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_network_deny_still_allows_af_unix_and_startup() {
        let ws = tempfile::tempdir().unwrap();
        let root = ws.path().canonicalize().unwrap();
        let a = argv(&[
            "perl",
            "-e",
            "use Socket; socket(S, PF_UNIX, SOCK_STREAM, 0) or do { print \"unix-blocked\\n\"; exit 1 }; print \"unix-ok\\n\"",
        ]);
        let out = LinuxSandbox
            .run(&SandboxSpec {
                argv: &a,
                workspace: &root,
                caps: &caps(Some(5000), None),
                network: &orchestrator_core::NetworkPolicy::Deny,
                stdin: None,
            })
            .unwrap();
        assert_eq!(
            out.exit_code,
            Some(0),
            "AF_UNIX socket was wrongly blocked under Deny: {out:?}"
        );
        assert!(out.stdout.contains("unix-ok"));
    }

    // ── SP-4 linux-sandbox (4/4): fail-closed refuse coverage (Docker/Linux only) ──

    /// AC6 fail-closed: `build_landlock_ruleset` opens the workspace via `PathFd`; a non-existent
    /// (unopenable) workspace fails the ruleset build => `run` returns `Err` (refuse-loud), never
    /// an unconfined run. The landlock-unavailable branch can't be triggered where landlock IS
    /// present (the Docker/CI kernels have it), so the ruleset-build-fails path proves the same
    /// contract: no confinement built ⇒ the command never runs.
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_refuses_when_the_workspace_path_is_unopenable() {
        let a = argv(&["sh", "-c", "echo x"]);
        let bogus = std::path::PathBuf::from("/nonexistent-workspace-xyz"); // unopenable
        let r = LinuxSandbox.run(&SandboxSpec {
            argv: &a,
            workspace: &bogus,
            caps: &caps(Some(5000), None),
            network: &orchestrator_core::NetworkPolicy::Any,
            stdin: None,
        });
        assert!(
            matches!(r, Err(OrchestratorError::Tool { .. })),
            "unopenable workspace must refuse loud: {r:?}"
        );
    }
}
