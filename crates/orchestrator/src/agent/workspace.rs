//! SP-4 s3: path confinement for the per-run workspace jail. A tool arg names a
//! workspace-RELATIVE path; `confine` resolves it against the canonical per-run root and
//! rejects anything that escapes (absolute, `..`, or a symlink resolving outside).

use std::path::{Component, Path, PathBuf};

use orchestrator_core::OrchestratorError;

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
    // within `root`. Probe with `symlink_metadata` (lstat), which detects the path ENTRY
    // itself — it is `Ok` for a real dir/file AND for a *dangling* symlink (target absent),
    // but `Err` for a genuinely-absent component. This is fail-closed: a dangling symlink is
    // treated as existing → `canonicalize` fails (no target) → `WorkspaceEscape`. Using
    // `Path::exists` (stat) here would be a jail escape — it follows the link and returns
    // `false` for a dangling one, so the loop would skip past the symlink to a canonical-inside
    // ancestor and hand back a path that a subsequent `O_CREAT` write follows OUTSIDE root.
    let mut probe: &Path = out.as_path();
    let existing = loop {
        if probe.symlink_metadata().is_ok() {
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

    #[test]
    fn rejects_dangling_symlink_final_component() {
        let (_td, root) = canon_tmp();
        let outside = tempfile::tempdir().unwrap();
        // link target does NOT exist -> Path::exists() is false, but the link itself does.
        std::os::unix::fs::symlink(outside.path().join("nope"), root.join("danglink")).unwrap();
        assert!(matches!(
            confine(&root, "danglink"),
            Err(OrchestratorError::WorkspaceEscape(_))
        ));
    }
}
