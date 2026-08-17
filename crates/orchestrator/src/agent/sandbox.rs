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
// Transient: the first non-test caller (the `Sandbox` impls) lands in Task 2; drop this then.
#[allow(dead_code)]
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
    let stdin_t = stdin.map(|s| {
        let s = s.to_string();
        std::thread::spawn(move || {
            if let Some(mut si) = stdin_h {
                let _ = si.write_all(s.as_bytes()); // drop `si` => EOF
            }
        })
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
    let stdout = out_t.join().unwrap_or_default();
    let stderr = err_t.join().unwrap_or_default();
    if let Some(t) = stdin_t {
        let _ = t.join();
    }

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
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "took too long: the wall timer did not kill it"
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
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "process-group kill did not reap the forked child"
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
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "straggler not reaped — spawn_capped hung on the pipe"
        );
    }

    #[test]
    fn cpu_cap_kills_a_busy_loop() {
        // RLIMIT_CPU is POSIX + implemented on Darwin (unlike RLIMIT_AS). A busy loop capped at 1s
        // CPU → SIGXCPU → Cpu. Wall set high so the WALL timer doesn't fire first.
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
        assert_eq!(
            out.killed,
            Some(KillReason::Cpu),
            "expected a CPU-cap (SIGXCPU) kill, got {out:?}"
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
}
