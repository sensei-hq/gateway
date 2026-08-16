---
title: SP-4 slice — Workspace isolation (in-process jail + real fs tools)
doctype: design
module: orchestrator
spec: SP-4
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§13 enforcement & isolation, §287 tool permission model, §290 workspace isolation, §225 killing process trees); ./2026-08-14-sp4-permission-enforcement-design.md (s1 — the authorize boundary this confines); ./2026-08-14-sp4-secret-redaction-design.md (s2 — redaction the fs output rides through); ./2026-08-15-sp4-credential-broker-design.md (the `ToolContext` seam this extends); ./2026-08-15-sp4-exactly-once-idempotency-design.md (the two-phase Mutation path fs_write rides)
date: 2026-08-16
---

# SP-4 slice — Workspace isolation (in-process jail + real fs tools)

## 1. Goal

Make the SP-4 enforcement arc **concrete against a tool that actually does I/O**. Today every
tool is an in-process demo sink (`grep -r 'std::fs|Command::|std::net|reqwest'` in the
orchestrator crates is empty), so s1 (permission gate), s2 (redaction), and the credential
broker are proven only against tools that touch nothing. This slice ships **two real
filesystem tools** — `fs_write` (Mutation) and `fs_read` (Observation) — confined to a
**per-run workspace directory** by an **in-process path jail**, and threads a
`workspace_root` into the tool `ToolContext`. This delivers the master spec's **workspace
isolation** (§290) and **executor-enforced path allowlists** (§287) as a working, tested
capability, and makes the whole SP-4 arc real end-to-end (a real write → the s1 grant gates
it, the jail confines it, s2 redacts its output, resume replays it exactly-once).

**Scope (user-chosen): the in-process workspace jail + the two fs tools only.** A
**subprocess sandbox** and **true resource-cap KILLING** (cpu/mem/wall) are **deferred** (§6)
— both need a killable/confineable execution unit (a child process), i.e. the
tool-execution-model decision this slice deliberately does not take. The in-process jail is a
spike: it binds the abstract permission surface to a live per-run directory and confines the
declared path surface, honestly leaving bypass-proof confinement to the (future) sandbox.

## 2. Background & impact review

- **The tool execution model is synchronous + in-process.** `Tool::call_ctx(&self, args,
  ctx: &ToolContext) -> Result<Value>` (`crates/orchestrator/src/agent/tools.rs`) is sync and
  runs in the executor's own process. A real subprocess sandbox would change this contract
  (async + IPC) — out of scope; this slice keeps the sync in-process model.
- **s1 already authorizes but explicitly does NOT confine.** The `execute_tool_effect` gate
  denies unless `tool ∈ agent.tools` ∧ `agent.grants[tool].covers(tool.required(args))`
  (`Permissions{paths, commands, network, caps}`; `covers()` is component-aware with `..`
  rejected). s1's own trust-boundary note: *"a tool that under-reports its `required` bypasses
  the gate — runtime confinement + cap-killing = the sandbox slice."* This slice is that
  confinement, for the **filesystem path** dimension.
- **`ToolContext` is the injection seam** (`{idempotency_key, effect_id, credentials}` from
  s5 + the broker). This slice adds `workspace_root`.
- **Effect classes already exist.** `fs_write` is a **Mutation** (two-phase `EffectIntent →
  EffectRecorded`, exactly-once, resume-replays-from-memo); `fs_read` is an **Observation**
  (TTL memo). No new effect machinery — the tools slot into the existing dispatch in
  `record_tool_effect`.
- **Impact:** additive. An injected `Option<PathBuf>` workspace root on the `Executor`
  (default `None` ⇒ byte-identical), a `workspace_root` field on `ToolContext`, a `confine()`
  jail helper, two new `Tool` impls, and a pre-run confinement check in the fs-effect path. No
  broker/redactor/registry contract changes. Unwired ⇒ the whole path is unchanged.

## 4. Design

### 4.1 The workspace root — durable, per-run

- **`Executor::with_workspace_root(base: impl Into<PathBuf>)`** sets
  `workspace_root_base: Option<PathBuf>` (default `None`). The `Executor` already derives
  `Clone`, so the pin carries.
- At `run`/`start` entry, when a base is wired, the executor resolves the **per-run root**
  `base/<run_id>/` and `std::fs::create_dir_all`s it (idempotent — safe to re-run on resume).
  The **canonical** per-run root (`canonicalize` after mkdir) is what gets injected and
  jailed against. **Durable, no auto-delete:** the directory survives a crash so a resume's
  memo-replay is consistent with the files still on disk; cleanup/GC is out of scope (§6).
- The concrete per-run root is threaded into every tool call via
  **`ToolContext.workspace_root: Option<Arc<PathBuf>>`** (Arc so `ToolContext`'s existing
  `Clone`/`Debug` derives are cheap; `None` when no base is wired).

### 4.2 The jail — `confine()`

A path-confinement helper lives in the **orchestrator crate**
(`crates/orchestrator/src/agent/workspace.rs`), NOT `orchestrator-core` (which is I/O-free
"beyond trait signatures" — `confine` calls `std::fs::canonicalize`):

```rust
/// Resolve `requested` (a relative path from a tool arg) against the canonical per-run
/// workspace `root`, confining the result to the jail. Rejects absolute paths, `..` escapes,
/// and symlinks that resolve outside `root`.
pub(crate) fn confine(root: &Path, requested: &str) -> Result<PathBuf, OrchestratorError> {
    // 1. reject absolute requested paths (must be workspace-relative)
    // 2. lexically normalize `.` / `..` on root.join(requested); a normalized path that
    //    would leave `root` (net `..`) → WorkspaceEscape
    // 3. canonicalize the DEEPEST EXISTING ANCESTOR (the file may not exist yet for a write)
    //    and assert it starts_with(root) → defeats an in-workspace symlink pointing out
    // 4. return the confined absolute path (root ‖ remaining components)
}
```

- New error variant **`OrchestratorError::WorkspaceEscape(String)`** (the message names the
  requested path but NOT the absolute root → no host-path disclosure, mirroring s1's terse
  denial).
- `confine` is deterministic given the filesystem state; it performs no writes.

### 4.3 The tools

`fs_write` (`agent/tools.rs`):
- **spec:** name `fs_write`, `effect_class: Mutation`, args schema `{path: string, content:
  string}`.
- **`required(args)`** → `Permissions { paths: vec![args.path], ..Default::default() }` — the
  concrete path this call touches (the s1 runtime ceiling).
- **`call_ctx(args, ctx)`:** `root = ctx.workspace_root` (absent → loud
  `OrchestratorError` "fs_write requires a workspace root"); `target = confine(root, path)?`;
  `create_dir_all(target.parent())` (within the jail); `std::fs::write(target, content)`;
  return `{ "bytes": content.len(), "path": <relative-to-root> }` (relative so the journaled
  output is stable if the base moves).

`fs_read`:
- **spec:** name `fs_read`, `effect_class: Observation`, `ttl_secs: Some(0)` (always re-read —
  simplest + deterministic given a stable on-disk file; a memo replay on resume re-reads the
  persisted file, no token cost), args `{path: string}`.
- **`required(args)`** → `paths: vec![args.path]`.
- **`call_ctx`:** `confine(root, path)?` then `std::fs::read_to_string` → `{ "content": … }`
  (a missing/unreadable file → loud `OrchestratorError`, never a silent empty read).

Both are registered by the caller into the executable `ToolRegistry` and declared on an agent
(with a workspace-scoped grant); neither is registered by default.

### 4.4 Enforcement — two checks, and the honest limit

Two distinct, independently-tested checks both hold for an fs tool call:

1. **s1 authorize (unchanged):** `agent.grants[tool].covers(tool.required(args))` — the
   agent's grant must permit the declared path surface. A workspace-scoped grant (e.g. a path
   prefix covering the tool's relative targets) passes; an ungranted path is denied by s1
   before this slice's logic runs.
2. **s3 confine (new):** in the fs-effect path, **before** the tool runs, the executor calls
   `confine(root, p)` for each `p` in `tool.required(args).paths`; a `WorkspaceEscape` is
   recorded as a **Pure `EffectRecorded`** denial (no fs touched, **no `EffectIntent`** for
   the Mutation) and fed back as a terse tool-result error — the SAME shape as the s1 denial,
   so a resume replays the denial from the memo and the tool is never invoked. The tools
   additionally resolve their real path through `confine` (defense in depth).

**Honest limit (documented, = the deferred sandbox's job):** an in-process tool has ambient
filesystem authority — the jail is enforced at the shared `confine` helper + the declared-path
pre-check, so a tool that *declares* an escape is stopped, but a tool that bypasses the helper
and calls `std::fs::write("/etc/x")` directly cannot be prevented in-process. True bypass-proof
confinement (and cpu/mem/wall cap-KILLING) requires a subprocess sandbox — **deferred** (§6).
This slice's confinement is real for cooperative + honest-but-buggy tools and for the entire
declared surface; it is not a security boundary against a malicious in-process tool.

### 4.5 Determinism, resume & composition

- **`fs_write` (Mutation):** rides the existing two-phase path — `EffectIntent{key}` →
  `std::fs::write` → `EffectRecorded{output:{bytes,path}}`. On resume a **completed** write
  **replays its journaled `{bytes,path}` from the memo — the tool is NOT re-run, so the file
  is NOT re-written** (exactly-once, already proven). The **durable** workspace means run-1's
  files are still on disk for any later reader. The per-run root is keyed by `RunId` ⇒ stable
  across resume ⇒ identical journaled output.
- **`fs_read` (Observation):** memoized with TTL; a fresh hit replays, a stale one re-reads
  and supersedes (existing Observation semantics).
- **s2 redaction + broker scrub:** the fs output (`{bytes,path}` / `{content}`) flows through
  the existing `record_tool_effect` redact-then-… chain unchanged — a secret in read-back
  content is redacted before journaling and feed-back for free.
- **`input_hash`:** over `args` (the relative path + content), as today. `workspace_root` is
  NOT hashed (it's per-run infra, like the injected credentials) — it adds zero determinism
  surface. `confine()` is not journaled; only its `{bytes,path}`/`{content}` result is.

### 4.6 Additive & trust boundary

- **Additive:** no `workspace_root_base` wired ⇒ `ToolContext.workspace_root` is `None`, no
  per-run dir is made, the fs tools (if somehow called) fail loud, and every existing tool /
  path is **byte-identical**. The fs tools are opt-in registrations.
- **Trust boundary:** confines the declared filesystem path surface to a per-run jail and
  isolates parallel runs; it does NOT sandbox a malicious in-process tool, confine network
  egress, or kill on resource-cap breach — those are the deferred subprocess sandbox.

## 5. Decisions

- **D1 — in-process jail, no subprocess** [approved]: keeps the sync `Tool::call_ctx` model;
  a spike to make the arc concrete. Cpu/mem/wall cap-KILLING deferred (needs a child process).
- **D2 — durable per-run workspace `base/<run_id>/`, no auto-delete** [approved]: survives a
  crash so resume's memo-replay is consistent with on-disk files; matches the durable-executor
  philosophy. Cleanup/GC deferred.
- **D3 — `confine()` in the orchestrator crate, not core** [approved]: it uses
  `std::fs::canonicalize` (symlink-out defense); `orchestrator-core` stays I/O-free.
- **D4 — two real tools: `fs_write` (Mutation) + `fs_read` (Observation)** [approved]:
  exercises both real effect classes against real I/O + a write→read round-trip; the second
  tool is cheap (shared jail helper).
- **D5 — confinement enforced at the shared helper + a declared-path pre-check; in-process
  ambient-authority bypass documented as the deferred sandbox's job** [approved]: honest about
  what an in-process jail can and cannot guarantee.
- **D6 — relative `path` in the journaled output** [approved]: stable if the workspace base
  moves; the absolute path (embedding the host root) stays internal (also avoids host-path
  disclosure in the journal).

## 6. Deferred (stated)

- **Subprocess sandbox + resource-cap KILLING** — a child process under macOS `sandbox-exec` /
  Linux landlock+seccomp with `setrlimit` + a kill-on-timeout supervisor (the master spec's
  §225 "killing process trees for shell" + true cpu/mem/wall enforcement). The bypass-proof
  confinement the in-process jail cannot provide. Blocked on the tool-execution-model decision.
- **Network egress confinement** — a `NetworkPolicy`-enforcing tool + a real network tool.
- **Workspace cleanup / GC / quota** — retention policy, disk-usage caps, orphan reaping.
- **COW / git-worktree isolation for parallel branches** + declared resource locks (§290 the
  concurrency-safety half — this slice isolates by per-run dir, not per-branch COW).
- **A bypass-proof in-process jail** (e.g. via a capability-restricted fs handle) — likely not
  worth it vs. the subprocess sandbox.

## 7. Acceptance criteria (TDD)

1. **`confine` confines.** `confine(root, "a/b.txt")` → an absolute path under the canonical
   root; `confine(root, "../../etc/passwd")`, an absolute `/etc/passwd`, and an in-workspace
   symlink resolving outside → `WorkspaceEscape` (never a path outside root).
2. **`fs_write` writes real bytes in the jail.** With a `TempDir` base + a workspace-granted
   agent, `fs_write{path:"notes.md", content:"hi"}` creates `base/<run_id>/notes.md` with
   `"hi"`; output `{bytes:2, path:"notes.md"}`.
3. **`fs_read` round-trips.** `fs_read{path:"notes.md"}` after the write returns
   `{content:"hi"}`.
4. **Escape denied (both tools).** `fs_write`/`fs_read` with `path:"../../etc/passwd"` (or an
   absolute-outside path) → a terse denial, **no file written / read**, recorded Pure (no
   `EffectIntent` for the write), fed back to the agent; a resume replays the denial.
5. **Parallel runs isolated.** Two runs writing the same relative `path` land in distinct
   `base/<run1>/` vs `base/<run2>/` files — no collision, no cross-read.
6. **Resume replays `fs_write` exactly-once.** A completed `fs_write` effect, on resume,
   replays `{bytes,path}` from the memo with **zero re-writes** (a write-counter / mtime proves
   the file is untouched) and **no `DeterminismViolation`**; the file persists on disk.
7. **Redaction composes.** A secret written then read back is `[REDACTED]` in the journaled
   `fs_read` output and the value fed back (s2 over real file content).
8. **Additive.** No `workspace_root` wired ⇒ the full existing suite (s1 gate + s2 redaction +
   s5 idempotency + broker) passes **byte-identical**; a tool with no fs need is unaffected.
