---
title: SP-4 slice — Linux sandbox backend (landlock fs + seccomp egress)
doctype: design
module: orchestrator
spec: SP-4
status: approved
companion: ./2026-08-16-sp4-subprocess-sandbox-design.md (§4.2 the Sandbox seam, §6 the deferred Linux backend this delivers; MacosSandbox is the parity target — fs-write→workspace, network deny/allow per NetworkPolicy); ./2026-08-16-sp4-workspace-isolation-design.md (the per-run workspace this confines writes to)
date: 2026-08-17
---

# SP-4 slice — Linux sandbox backend (landlock fs + seccomp egress)

## 1. Goal

Give the subprocess sandbox a **Linux OS-confinement backend** so Linux/prod runs an untrusted
`shell` command **CONFINED** instead of refusing-loud. A `LinuxSandbox` implements the existing
`Sandbox` trait using **landlock** (filesystem: write confined to the per-run workspace, broad
read) + **seccomp** (network: deny IP egress when `NetworkPolicy::Deny`) + the already-portable
**`spawn_capped`** (cpu/mem/wall cap-killing). Both mechanisms are **unprivileged** (landlock is
unprivileged since Linux 5.13; a seccomp filter needs only `NO_NEW_PRIVS`), matching how a
server process actually runs. This reaches **parity with `MacosSandbox`** and closes the s4 §6
"Linux backend" deferral.

**Scope (user-chosen): landlock (fs) + seccomp (network) — full macOS parity.** A network
namespace was rejected: `unshare(CLONE_NEWNET)` is **blocked unprivileged** (probed: EPERM in a
default container, only `--privileged` works), so it would impose a deployment privilege burden
and can't be verified unprivileged; seccomp is the unprivileged egress-deny mechanism.

**Reuses the whole s4 seam unchanged:** `Sandbox` trait, `SandboxSpec{argv, workspace, caps,
network}`, `spawn_capped`, `BoundSandbox` (executor owns the policy from the grant), `ShellTool`,
`Executor::with_sandbox`, refuse-loud fallback. This slice adds only `LinuxSandbox` + its
confinement + target-gated deps + Linux-gated tests.

## 2. Background & impact review

- **The `Sandbox` seam already exists** (`crates/orchestrator/src/agent/sandbox.rs`):
  `MacosSandbox` is `#[cfg(target_os = "macos")]`; `LinuxSandbox` is the `#[cfg(target_os =
  "linux")]` sibling. `BoundSandbox`/`ShellTool`/`bound_sandbox_for`/`with_sandbox` are portable
  and untouched.
- **Empirically verified (probed in the local Docker Linux kernel 6.12, unprivileged):**
  `CONFIG_SECURITY_LANDLOCK=y` + landlock in the LSM list; `landlock_create_ruleset` syscall
  returns ABI version 6 (works unprivileged); `CONFIG_SECCOMP_FILTER=y`. `unshare --net` returns
  EPERM unprivileged (netns needs `--privileged`). So landlock + seccomp are testable
  unprivileged in Docker AND on CI (`ubuntu-latest`); netns is not — hence the seccomp choice.
- **Platform reality:** the dev box is macOS/darwin — this code cannot build or run natively
  there. The workspace MUST stay compile-clean on macOS (target-gated deps + `#[cfg]`); the real
  confinement is verified in a **Docker Linux container** (unprivileged) + `ubuntu-latest` CI.
- **New deps (target-gated so macOS never builds them):** `landlock` (fs ruleset + ABI
  negotiation) and `seccompiler` (pure-Rust BPF filter compiler; AWS Firecracker; preferred over
  a `libseccomp` C dep), both under `[target.'cfg(target_os = "linux")'.dependencies]`.
- **Impact:** additive — one new `#[cfg(target_os="linux")]` `Sandbox` impl. No change to the
  trait, the tool, the executor wiring, determinism, or the macOS backend. A Linux caller wires
  `LinuxSandbox` via `with_sandbox`; unwired ⇒ refuse-loud (unchanged).

## 4. Design

### 4.1 `LinuxSandbox` + the confinement it builds

`#[cfg(target_os = "linux")]` in `agent/sandbox.rs`:
```rust
pub struct LinuxSandbox;

impl Sandbox for LinuxSandbox {
    fn run(&self, spec: &SandboxSpec) -> Result<CapOutcome, OrchestratorError> {
        // 1. BUILD the confinement IN THE PARENT (allocates): landlock ruleset (fs) + the
        //    seccomp BpfProgram (network). A build failure (e.g. landlock unavailable, or a
        //    Deny policy with no seccomp) => refuse-loud (Err), never an unconfined run.
        let ruleset = build_landlock_ruleset(spec.workspace)?;          // fs: write⊆workspace, read *
        let bpf: Option<BpfProgram> = match spec.network {
            NetworkPolicy::Deny => Some(build_egress_deny_filter()?),   // deny socket(AF_INET*)
            NetworkPolicy::Any | NetworkPolicy::Hosts(_) => None,       // Hosts = coarse allow (documented)
        };
        // 2. spawn_capped with a pre_exec that only APPLIES (syscalls, no alloc) in the child.
        spawn_capped_confined(spec.argv, spec.caps, spec.stdin, ruleset, bpf)
    }
}
```
`spawn_capped_confined` is `spawn_capped` extended with a caller-supplied `pre_exec` extension
(landlock `restrict_self` + seccomp `apply_filter`); factor the shared body so the portable
`spawn_capped` and the confined variant share the process-group / rlimits / wall-kill / bounded
capture. (Alternatively, `spawn_capped` gains an optional `confine: Option<Confinement>` param —
the implementer picks the cleaner factoring; the portable path stays byte-identical.)

### 4.2 fs — landlock (write ⊆ workspace, read *)

The `landlock` crate. **Default-deny once restricted:** create a ruleset handling the FS access
rights, grant **read+execute on `/`** (broad read — a binary must start; parity with the macOS
`(allow file-read*)`) and **write (+ create/remove/make-*) on the canonical workspace path**,
then `restrict_self`. Any write outside the workspace → EACCES. Use the crate's **best-effort
ABI compatibility** for forward rights (REFER/TRUNCATE/IOCTL_DEV added in later ABIs), but
**require at least the ABI-1 write handling** — if landlock is entirely unavailable
(kernel < 5.13 / not compiled), the ruleset build **fails → refuse-loud** (the requested fs
confinement can't be enforced). The workspace path is executor-derived (canonical `base/<run_id>`),
not model-controlled.

### 4.3 network — seccomp (deny IP egress)

`seccompiler`. For `NetworkPolicy::Deny`, compile a BPF filter: **default action Allow**, with
one rule — `socket` (the syscall) with **arg0 (domain) == AF_INET (2) OR AF_INET6 (10)** →
**Errno(EPERM)**. This blocks creating IP sockets (no egress) while leaving `AF_UNIX`/`AF_NETLINK`
(local IPC, glibc NSS/`getaddrinfo` plumbing) working so ordinary programs still start.
`connect()` is NOT filtered (seccomp-BPF cannot deref the sockaddr pointer to read the family) —
denying `socket(AF_INET*)` is the correct, deref-free chokepoint. `Any` ⇒ no filter. `Hosts(_)`
⇒ no filter (coarse allow-all, documented — precise host allowlists are deferred, same caveat as
macOS). The filter is **allow-by-default**, so `execve` and everything else the child needs pass.

### 4.4 The apply model — build in parent, apply in child (alloc-safety)

`spawn_capped`'s parent is multi-threaded (tokio). Calling allocating library code
(`landlock`/`seccompiler` ruleset construction) in the post-fork child (`pre_exec`) risks a
malloc-lock deadlock (a classic fork-in-a-threaded-process hazard; the s4 `pre_exec` only called
async-signal-safe `setrlimit`). So:
- **Parent (before spawn):** build the landlock `RulesetCreated` (owns a ruleset FD) and compile
  the seccomp `BpfProgram` (a `Vec<sock_filter>`). Both allocate here.
- **Child (`pre_exec`, syscalls only):** `prctl(PR_SET_NO_NEW_PRIVS, 1)` → landlock
  `restrict_self(fd)` → `seccompiler::apply_filter(&bpf)`. **Order matters:** apply landlock
  BEFORE the seccomp filter (so the filter can't block `landlock_restrict_self`); the seccomp
  filter is allow-by-default so it doesn't block the subsequent `execve`. `RulesetCreated`
  (OwnedFd) and `BpfProgram` are `Send`, so they move into the `pre_exec` closure. No allocation
  in the child.

### 4.5 Fail-closed (refuse-loud) & additive

- **Refuse** (never an unconfined run): landlock unavailable (< 5.13 / not compiled) → the
  ruleset build fails → `run` returns `Err` → `ShellTool` refuses loud. `Deny` requested but the
  seccomp filter can't be built → refuse. No sandbox wired (Linux caller didn't
  `with_sandbox(LinuxSandbox)`) → `ctx.sandbox: None` → refuse (unchanged s4 behavior).
- **Additive:** `LinuxSandbox` + the `landlock`/`seccompiler` deps are `#[cfg(target_os="linux")]`
  / target-gated ⇒ the macOS build + the whole existing suite are **byte-identical**; the trait,
  tool, executor wiring, determinism, and `MacosSandbox` are untouched.

### 4.6 Determinism & trust boundary

Inherited from s4 unchanged: `ShellTool` is a Mutation; the policy (caps/network/workspace) is
NOT hashed/journaled (only the `{exit,stdout,stderr,killed}` output); a completed shell replays
from the memo (not re-spawned); stdout/stderr flow through s2 redaction; in-doubt resume → pause.
Trust boundary: landlock + seccomp are **real** OS confinement (bypass-proof for fs-write + IP
egress). Limits (documented, deferred §6): broad file-READ (a binary must start); `Hosts` is
coarse allow-all (no precise host allowlist); network deny is IP-only (AF_UNIX/AF_NETLINK
allowed); no read-confinement / cgroups / syscall-hardening beyond network egress.

## 5. Decisions

- **D1 — landlock (fs) + seccomp (network), unprivileged; netns REJECTED** [approved]:
  `unshare(CLONE_NEWNET)` is blocked unprivileged (probed EPERM; needs `--privileged`), imposing
  a deployment/testing privilege burden; seccomp denies IP egress unprivileged (`NO_NEW_PRIVS`
  only) and is Docker/CI-verifiable.
- **D2 — build the confinement in the parent, apply (syscalls) in the child** [approved]:
  avoids the fork+alloc deadlock in `pre_exec`; the portable `spawn_capped` cap-killing path stays
  byte-identical.
- **D3 — deny egress at `socket(AF_INET|AF_INET6)`, not `connect()`** [approved]: seccomp-BPF
  can't deref the `connect` sockaddr; denying IP socket-creation is the deref-free chokepoint and
  leaves `AF_UNIX`/`AF_NETLINK` working so programs start.
- **D4 — fail-closed: landlock unavailable ⇒ refuse-loud** [approved]: never run untrusted code
  with the requested fs-confine unenforced; consistent with the s4 refuse-loud posture.
- **D5 — verify via Docker (unprivileged) + `ubuntu-latest` CI** [approved]: the macOS dev box
  can't run Linux; landlock ABI 6 + seccomp confirmed working unprivileged in the local Docker
  kernel; a separate `CARGO_TARGET_DIR` keeps the macOS `target/` clean.
- **D6 — target-gated deps (`landlock`, `seccompiler`)** [approved]: `[target.'cfg(target_os =
  "linux")'.dependencies]` so macOS never builds them; `seccompiler` (pure-Rust) over a
  `libseccomp` C dependency.

## 6. Deferred (stated)

- **Precise network host allowlists** (`Hosts` stays coarse allow-all both platforms — needs an
  egress proxy or per-connection policy).
- **Read-confinement** (broad file-read is allowed so a binary can start); **syscall-hardening**
  beyond IP egress (a general seccomp deny-list); **cgroups** (finer cpu/mem than rlimits).
- **BSD `pledge`/`unveil`**; a **`shell` reconciler** (in-doubt auto-resume — shared with the
  macOS backend's deferred list).
- **A dedicated privileged/netns mode** for hosts that want full network isolation.

## 7. Acceptance criteria (TDD; `#[cfg(target_os="linux")]`, run in Docker + CI)

1. **fs confinement — write outside the workspace is DENIED (the security proof).** A command
   writing to a path outside the workspace root → EACCES / no file, while an in-workspace write
   SUCCEEDS (the deny/allow contrast, like the macOS pair). Landlock-enforced.
2. **fs — a nested in-workspace write works** (landlock grants create/make under the subpath).
3. **network deny — IP egress blocked, contrast against `Any`.** Bind a live loopback
   `TcpListener`; under `NetworkPolicy::Deny` a `socket(AF_INET)`/connect FAILS (EPERM), under
   `Any` the same connect SUCCEEDS (a positive/negative control — non-vacuous, like the macOS
   live-listener test).
4. **network deny leaves local IPC working** — an `AF_UNIX` socket (or a program that needs
   `AF_NETLINK` to start, e.g. `getent`/DNS-less `sh`) still runs under `Deny` (proves the filter
   is surgical, not a blanket `socket()` block that breaks startup).
5. **cap-killing composes** — `sleep 100` capped at `wall_ms` through `LinuxSandbox` →
   `killed: Wall` (confinement + cap-killing together).
6. **refuse-loud when fs-confine can't be enforced** — the landlock-unavailable path returns Err
   (assert the build-fails→refuse branch; simulate by forcing the unavailable path if the runner
   supports landlock).
7. **macOS stays byte-identical** — `cargo build` / `cargo test` on the macOS host is unchanged
   (landlock/seccomp not compiled; `LinuxSandbox` absent); the full existing suite passes.
8. **Additive** — no sandbox wired ⇒ byte-identical; the portable `spawn_capped` cap-killing
   tests still pass on both platforms.
