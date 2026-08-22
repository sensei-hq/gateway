---
title: SP-DATA-4 — torii management CLI (operator control plane + config write path)
doctype: design-spec
module: orchestrator
slice: SP-DATA-4
status: approved
date: 2026-08-22
---

# SP-DATA-4 — `torii`: the management CLI

## 1. Summary

The workspace's **first binary**: `torii`, an operator control plane over the durable
orchestrator. It observes and intervenes on runs (`status`/`list-paused`/`cancel`/`wake`),
**runs the worker that actually drives due wakes** (`worker serve` — today `Scheduler::tick`
has no production caller at all), submits new runs, and owns the **durable config write
path** (`config push`/`version`).

Shipping a live config writer forces the two SP-DATA-2 carry-forwards closed, which is the
other half of the slice: an **atomic config read** (`load_versioned` — one `REPEATABLE READ`
snapshot) and an **atomic config write** (`store_and_bump` — one transaction).

**CLI only.** No HTTP surface: exposing `cancel`/`force_wake`/config-write over a socket
needs an auth/authz design that doesn't exist yet, and nothing remote consumes it. A future
slice can add a second `[[bin]]` to this crate reusing `boot` verbatim.

## 2. Motivation

SP-DATA-1/2/3 built a durable, cross-process, self-healing executor — and left it with **no
way to operate it**. Concretely, today:

- **Nothing calls `tick()` outside tests.** `Scheduler::tick` (`crates/orchestrator/src/scheduler.rs:62`)
  is the mechanism that wakes a paused run, and s3 explicitly deferred a `run_forever`
  supervisor. So in production a `RunPaused{resume_after}` still never wakes. The durable
  scheduler is built and inert.
- **The HOTL path has no hands.** s3's `resume_after = None` class (in-doubt Mutation) can
  *only* be resolved by `force_wake` — a human decision with no human-facing surface.
- **There is no way in.** All ten crates are libraries; there is no `[[bin]]` and no `clap`
  in the workspace. The wiring from "a machine with a `DATABASE_URL`" to "a live `Executor`
  + `Scheduler`" exists nowhere, in any form.

The run commands themselves are thin — `Scheduler::{status, list_paused, cancel, force_wake}`
already exist and are tested. **The substance of this slice is the boot wiring and the config
write path**, not the command surface.

## 3. Goals / Non-goals

**Goals**
- A `torii` binary: `run submit|status|list-paused|cancel|wake`, `worker serve`, `config push|version`.
- The first production `tick()` caller — a resilient poll loop with graceful shutdown.
- Close both SP-DATA-2 carry-forwards: `load_versioned()` (atomic read) + `store_and_bump()` (atomic write).
- Make the un-bumped write **unreachable from production code**, not merely documented.
- Commands report the **effect they achieved**, never the fact that a call returned `Ok`.
- `orchestrator` (executor) logic **unchanged**; the `orchestrator-core` change is one
  defaulted trait method.

**Non-goals (deferred, §10)**
- Any HTTP/gRPC surface, and therefore any auth/authz model.
- `run submit --detach` (needs a real `pending` status — see §5.3).
- Wake backoff/jitter/`max_attempts`/dead-letter (still deferred from s3; see §7.2).
- Terminal-row pruning, metrics/tracing export, a config `pull`/rollback command.

## 4. Architecture & layering

```
crates/torii                      NEW — [package] sensei-torii, [[bin]] torii
  main.rs        clap parse -> dispatch -> exit code
  boot.rs        env + files -> Deps (two tiers, §4.1)
  cmd/run.rs     submit · status · list-paused · cancel · wake
  cmd/config.rs  push (validate -> diff -> confirm -> store_and_bump) · version
  cmd/worker.rs  serve (poll loop · backoff · graceful shutdown)
  diff.rs        pure RegistryConfig diff (the guard on a destructive write)
  render.rs      human tables (default) | --json
  errors.rs      OrchestratorError/JournalError -> message + exit code

crates/orchestrator-store         load_versioned · store_and_bump · test-support gate
crates/orchestrator-core          ONE defaulted trait method + reload/from_source use it
crates/orchestrator               Cargo.toml feature passthrough only — no logic change
```

The wiring lives in the **binary crate**, not the library. `Executor` takes every backend as
an injected `Arc<dyn …>` precisely so the library knows nothing about Postgres, env vars, or
config files; teaching `orchestrator` to read `DATABASE_URL` would invert four slices of that
discipline. A future HTTP binary reuses `boot` as a sibling `[[bin]]` in this crate.

### 4.1 Two boot tiers

Commands need very different inputs, and `boot` enforces the split rather than demanding
everything for everything:

| Tier | Needs | Commands |
|---|---|---|
| **Light** | `DATABASE_URL` → pool → `PostgresSchedulerStore` / `PostgresConfigSource` | `run status\|list-paused\|cancel\|wake`, `config push\|version` |
| **Heavy** | light + `TORII_FENCE_VERSION` + `--gateway-config <file>` → full `Executor` + `Scheduler` | `run submit`, `worker serve` |

So an operator can cancel a runaway run or inspect the wake queue **on a box with no model
credentials at all**. A missing heavy-tier input is a loud, specific startup error — never a
half-built executor.

Heavy-tier wiring: `PostgresJournal` + `PostgresContentStore` + `PostgresContextStore`
(s1), `RegistryHandle::from_source(PostgresConfigSource)` (s2), `PostgresSchedulerStore` (s3),
`SystemClock`, the built-in `ToolRegistry`, and **`PatternRedactor` unconditionally** — s2
defaults the redactor off in the library to keep it byte-identical, but a production binary
defaults secure, and there is deliberately no `--no-redact` flag. `--workspace-root <dir>`
(optional) enables the fs tools and the platform sandbox (`MacosSandbox`/`LinuxSandbox`);
absent, those tools keep their existing fail-closed refusal. No reconcilers are wired, so an
in-doubt Mutation pauses and becomes a `wake` decision — consistent with s3.

### 4.2 `GatewayConfig` from a file

`kernel::types::config::GatewayConfig` derives `Serialize + Deserialize`
(`crates/kernel/src/types/config.rs:347`), so `--gateway-config <file.json>` deserializes and
feeds `Gateway::new` (`crates/gateway/src/facade.rs:52`). No new gateway loader is needed.

## 5. Command semantics

`Graph { nodes: Vec<Node> }` is `Deserialize` (`crates/orchestrator-core/src/graph.rs:9`), so
`--graph plan.json` works. `RunId(pub uuid::Uuid)` (`crates/orchestrator-core/src/ids.rs:5`)
has no `FromStr`/`Display`, so id parsing and formatting stay **CLI-local** — no core change.

The read commands — `run status`, `run list-paused`, `config version` — take `--json`; the
write commands (`submit`, `cancel`, `wake`, `config push`) print one human line. Exit codes:
**0** did it, **1** error, **2** not-found or precondition-not-met.

### 5.1 `cancel` and `wake` must report the effect, not the call

`SchedulerStore::cancel` is "any non-terminal → cancelled, idempotent" and `force_wake` is
conditional on `paused` (`crates/orchestrator-core/src/scheduler.rs:112-116`). So **cancelling
a terminal run and waking a non-paused run are silent no-ops at the store level.** Printing
"cancelled" on the strength of `Ok(())` reports a proxy for the intended effect — the exact
thing the mandatory verify-the-outcome rule forbids.

Both commands therefore **re-read `status` afterward** and report the real transition:

```
cancelled: 5f0a…c1
not cancelled: 5f0a…c1 is already completed          (exit 2)
```

`wake` additionally must not claim the run resumed. `force_wake` only sets `next_wake = now`;
a worker tick does the driving:

```
queued for wake: 9b21…7e (a worker tick will drive it)
```

**The re-read must key on `status`, not on `next_wake` presence.** `next_wake` is not a value
uniquely produced by a successful `force_wake`: `claim_due` flips `paused → waking` and leaves
`next_wake` **untouched**. So `after.next_wake.is_some()` cannot distinguish "my `force_wake` set
this" from "this was already set and my call was a no-op" — and the no-op case is ordinary, because an
operator runs `wake` precisely on the overdue pauses a worker tick is also about to claim. With an
`is_some()` check, a run claimed between the pre-check and the `force_wake` reports *"queued for wake
— a worker tick will drive it"* at **exit 0** while the run is already being driven and the command
changed nothing. The correct condition keys on **status** as the primary signal — `after.status == Paused` — because a
successful `force_wake` leaves the run `paused`, whereas a lost race leaves it `waking` (worker
claimed) or `cancelled` (another operator). The failure branch names the actual status, so a
cancellation is not misdiagnosed as a missing deadline.

The timestamp is secondary corroboration, and it needs a **symmetric drift tolerance**, not an
equality or an inequality. Measured against the real backend: Postgres `timestamptz` is
microsecond-resolution while `chrono` carries nanoseconds, and Postgres **rounds to nearest** rather
than truncating — sending `…10.123456789Z` reads back `…10.123457000Z`, i.e. **211 ns later** than the
value sent. So `t == now` fails always and `t <= now` fails whenever the nanosecond fraction rounds
up, which is about half of real `Utc::now()` calls — a false negative on the happy path, worse than
the bug being fixed. Use `|t - now| <= 1ms`: three orders of magnitude above the observed rounding
error, still far tighter than any real stale-deadline value. The timestamp check is load-bearing
despite `status == Paused`: a run that went `paused → waking → paused` via a claim and a re-pause
would satisfy the status check while carrying the *new pause's* deadline rather than this call's.

### 5.2 `config push` validates before it writes

Order is **validate → diff → confirm → write**, and the validate step is load-bearing:

1. `FilesystemConfigSource::load(dir)` **and `Registry::from_config`**. Skipping this lets you
   push config that assembles nowhere — every later `load()` fails `RegistryLoad`, every worker
   in the fleet stops resuming anything, and the config that would fix it is the config you
   just destroyed.
2. `load_versioned()` for the current durable state; compute the pure `diff` (§6.1).
3. If the diff **removes** anything, require confirmation (`--yes` bypasses for CI). `store` is
   replace-all, so pushing an incomplete directory silently deletes every entity not in it.
4. `store_and_bump()` — one transaction. There is no `--no-bump`: that flag *is* the footgun
   the carry-forward names.

```
$ torii config push ./config
config diff (durable v7 -> ./config):
  + agent  planner-v2
  ~ agent  researcher      (tools, grants)
  - agent  legacy-scraper
  - skill  verbose-mode
  = 4 unchanged

This REMOVES 2 entities. Continue? [y/N]
```

Non-interactive stdin (EOF/not a tty) without `--yes` **refuses and changes nothing**. That guarantee
is what stops `torii config push ./cfg < /dev/null` in a cron job from wiping config, so the default
confirmer lives in `cmd/config.rs` beside the doc that specifies it — generic over reader/writer and
unit-tested for EOF ⇒ false — rather than being described in one module and implemented in another.

**The diff must be re-validated immediately before the write.** Steps 2–4 are a check-then-act
sequence: the diff is computed from a snapshot, the prompt can sit in front of a human for minutes,
and then a replace-all executes. A concurrent writer in that window means the approved diff no longer
describes what the write will do. Demonstrated: durable `{a, b}` at v1, the operator is shown
"REMOVES 2 — a, b" and approves, a second writer commits `{a, b, c-brand-new}` → v2 during the read,
and the push lands `{survivor}` at v3 — **destroying `c-brand-new`, which appears nowhere in the text
the operator approved**, unrecoverably. The `Apply` branch has the same hole with a millisecond window
and *no prompt at all*. So `push` re-reads the generation immediately before writing and refuses if it
moved, on both branches. **`--yes` does not bypass this guard** — it consents to *removals*, not to a
config different from the one that was diffed. The operational consequence is deliberate: a `--yes` CI
job under a concurrent writer now exits 2 with a retry instruction rather than clobbering that writer.

Everything on this path fails **closed**. That includes the display: if the prompt cannot be written
(stderr closed, a broken pipe into a dead pager), consent is refused rather than accepted, because
there is nothing to consent to if the operator never saw the diff. Both the entity names *and* the
source path are sanitized — the path is free text and an `\u{1b}[8m` in it is SGR conceal, which would
render the removal list and the `REMOVES N` warning invisible.

**Entity names are sanitized in the prompt.** The prompt text *is* the destruction consent, so it is
the most safety-critical renderer in the CLI — more so than the run table. A name containing a newline
or an ANSI escape survives `FilesystemConfigSource`, passes `Registry::from_config`, and round-trips
through Postgres jsonb intact, and getting one in requires no confirmation because a pure addition
never prompts. Such a name can then forge removal lines (including a verbatim copy of torii's own
"no changes" message) or use `\u{1b}[4A\u{1b}[2K` to erase the real removal lines above it. The
`requires_confirmation()` gate and the trailing `REMOVES N` count are computed from the diff and stay
truthful; what a raw render compromises is the operator's knowledge of *which* entities die. Every
name goes through the shared `one_line` control-character collapse.

### 5.3 `submit` drives inline

`Scheduler::submit` enqueues *and* drives (`crates/orchestrator/src/scheduler.rs:53`), so the
command blocks until the run pauses or finishes. The run id prints **before** driving, so an
operator who loses the terminal can still find the run.

No `--detach` this slice: `enqueue` stamps the row `waking`/`claimed_at = now`, so a detached
run would only be picked up once the 60s lease expired and the **stale-reclaim** path grabbed
it — abusing a crash-recovery mechanism as a scheduling primitive. Doing it properly wants a
real `pending` status. Deferred and documented rather than faked.

## 6. The two SP-DATA-2 carry-forwards

### 6.1 Atomic read — `load_versioned()`

The hole (documented at `crates/orchestrator-store/src/postgres.rs:431-441`): `load()` reads
four config tables across four independent pool snapshots and `version()` is a fifth read, so
a concurrent writer can hand a reload a torn **(stale config, fresh generation)** pair. The run
then stamps a fresh-gen fence over stale config, and a later resume reads the now-consistent
(fresh, fresh) state, **matches the fence, and silently continues under different config**.
Re-reading `version()` at resume does not save it.

An inherent method on `PostgresConfigSource` alone is insufficient: `RegistryHandle::reload`
(`crates/orchestrator-core/src/registry.rs:489`) is what a worker calls to pick up config, and
it does the two reads separately. The CLI would be safe and the worker wouldn't — backwards.
So add a **defaulted trait method**, mirroring exactly how s2 added `version()`:

```rust
// orchestrator-core, ConfigSource
async fn load_versioned(&self) -> Result<(RegistryConfig, Option<u64>), OrchestratorError> {
    Ok((self.load().await?, self.version().await?))   // default == today's behavior
}
```

`PostgresConfigSource` overrides it with **one `REPEATABLE READ` transaction** spanning the four
config tables *and* `config_versions`. `reload`/`from_source` switch to calling it.

For filesystem/in-memory sources the default's tear is **harmless** — `version()` is `None`, so
there is no generation for the content to be inconsistent with. Hence the default is safe and
those sources stay byte-identical.

### 6.2 Atomic write — `store_and_bump()`

`store_and_bump(&cfg) -> u64` performs the version increment, the deletes, and the inserts in
**one transaction**. This closes more than the documented "caller forgets to bump" footgun:
even a disciplined caller doing `store()` then `bump()` has a **crash window** between them,
and dying in it leaves new content under an old generation *durably* — precisely the
silent-wrong-config state the fence exists to prevent. One transaction removes the window
instead of asking callers to be careful.

**The increment goes FIRST, and that ordering is load-bearing.** With the bump last, two concurrent
`store_and_bump` calls at the default READ COMMITTED silently **merge**: writer B's `DELETE` takes its
statement snapshot before A commits, so A's freshly-inserted rows are invisible to it and survive B's
"replace-all". Both writers return `Ok` and the durable config is *neither* writer's. The damaging
case is a revocation — config `{x}` durable, A pushes `{x, z}`, B concurrently pushes `{}` to revoke
everything; B deletes nothing it can see, inserts nothing, and the result is `{x, z}` at gen+2, so the
revocation silently did not happen with a success exit code. Bumping first serializes every writer on
the single `config_versions` row, so B blocks until A commits and B's `DELETE` then runs on a
post-commit snapshot: true last-writer-wins. It also removes a pkey-index deadlock between two
writers inserting shared names in different order. Note the **fence survived even the merge** (every
content-mutating transaction bumps within that transaction, so content-changed ⇒ generation-changed
holds) — this was a wrong-durable-content bug, not a wrong-config-resume one.

**The two fixes compose provably.** A `REPEATABLE READ` reader takes its snapshot at first
read; a single-transaction writer either committed before it (reader sees new content *and*
new generation) or after (old and old). No interleaving yields a torn pair.

### 6.3 Retiring the footgun

All eight callers of `store`/`bump_config_version` are in test modules. The s5 precedent for
this situation was gating the bypass `#[cfg(test)]` (as `ToolRegistry::execute` was), but that
**will not work here**: three callers live in `crates/orchestrator/src/executor/tests.rs`, a
*different crate*, and `#[cfg(test)]` is not enabled for a dependency.

So gate them behind a **`test-support` feature** on `orchestrator-store`, enabled by
`orchestrator`'s existing `postgres-tests` feature. Production code can then only reach
`store_and_bump`; tests that need the un-bumped write to *prove* the fix still have it.

## 7. Worker resilience, errors, secret hygiene

### 7.1 The poll loop

`tick()` already draws the right line: a *drive's* failure is recorded terminal in the store,
only a *store* failure returns `Err` (`crates/orchestrator/src/scheduler.rs:60-73`). The loop
honors that distinction: `Ok(n)` logs the count and sleeps `--interval` (default **5s**); `Err` is a store fault
— logged at ERROR with the full chain, retried with bounded exponential backoff, and after **5
consecutive failures the worker exits non-zero** so a supervisor restarts it and an alert fires.
Surviving a Postgres failover is worth a retry; treating a dead database as normal is not.

`--once` runs a single tick and exits (cron-friendly, and the e2e's driver).

### 7.2 A panicking run, and the crash-loop limitation

A panicking node propagates through `tick()` and takes the worker down. Drives are **not**
wrapped in `catch_unwind` — unsound across an await in general, and the lease already makes the
crash safe: the row stays `waking` and is reclaimed ~60s later.

What the lease does **not** prevent is a **crash loop** on a poison-pill run, because s3
deferred max-attempts and backoff. Attempt-counting is a schema change and real scope, so it
stays deferred — mitigated by logging the run id **before** each drive, so the poison run is
identifiable from the last line before the crash and `torii run cancel <id>` breaks the loop.
Named here rather than discovered at 3am.

### 7.3 Shutdown

SIGINT/SIGTERM finishes the in-flight tick, then exits; a second signal exits immediately. Both
are safe — an abandoned `waking` row is what the lease reclaim was built for. Graceful shutdown
is about not wasting a partial drive, not about correctness.

### 7.4 Errors are mapped, never flattened

Every loud error the stack already produces gets an actionable message plus its full chain on
stderr — the taxonomy s1 built exists so an operator can tell these apart:

| Error | Operator message |
|---|---|
| `VersionFenceMismatch` | the run's config generation drifted; points at `config version` |
| `JournalError::IncompatibleFormat` | binary and journal formats disagree; refuse to continue |
| `OrchestratorError::Store` / `JournalError::Backend` | transport fault (retryable) |
| `RegistryLoad` | bad config; names the offending file |

Nothing collapses to "something went wrong".

### 7.5 Secret hygiene

- **`DATABASE_URL` is env-only, no flag** — a `--database-url` puts the password in `ps` output
  and shell history for every user on the box.
- **The URL is never echoed** — not in a "connected to" line, not in a connect error. Failures
  report host and database name only, via a pure `redact_url()`.
- **`--gateway-config` contents are never echoed**, only its path (it holds provider API keys).
- **Test fixtures assemble credential-shaped strings at runtime** — the repo's Semgrep CWE-798
  hook blocks literal ones.

### 7.6 The fence base is explicit and required

`TORII_FENCE_VERSION` has **no default**; the heavy tier refuses to start without it. It is the
`Executor::new(gateway, journal, version)` fence base recorded in every `RunStarted` and checked
on resume as `{version}#cfg{gen}`, so a fleet must agree on it. Deriving it from
`CARGO_PKG_VERSION` would make every patch release strand every paused run as terminal-`Failed`
on its next wake — a self-inflicted mass stranding on a routine deploy. Naming it `FENCE`
rather than `VERSION` is deliberate: an operator who reads it as "the app version" will bump it.

## 8. What "pause reason" exposes (flagged, not fixed)

`ScheduledRun.reason` is free text lifted from `PauseInfo.reason` and provider messages, and
`list-paused` prints it. The s2 `Redactor` covers effect outputs and model output — **not pause
reasons**. A provider embedding something sensitive in a quota message would surface it in
operator output. s3 already stores these unredacted in Postgres, so torii is not introducing the
exposure — it is the first thing that *displays* it. Carry-forward for the redactor's coverage
(§10), not scope here.

## 9. Acceptance criteria

- **AC1 — the binary exists and boots in two tiers:** `torii --help` lists all three command
  groups; a light-tier command runs with **only** `DATABASE_URL` set (no gateway config, no
  fence version); a heavy-tier command without `TORII_FENCE_VERSION` fails with a specific,
  actionable error naming the variable (exit 1), never a half-built executor.
- **AC2 — honest effect reporting:** `cancel` on a `completed` run prints *not cancelled* and
  exits 2 (the store call itself succeeds — this asserts the re-read, not the call); `wake` on a
  non-`paused` run prints *not queued* and exits 2; `wake` on a paused run says *queued*, not
  *resumed*. `status` on an unknown run exits 2.
- **AC3 — `config push` validate-before-write:** a directory that loads but fails
  `Registry::from_config` (e.g. a duplicate chain binding) is rejected with **zero rows written
  and the version unchanged**.
- **AC4 — `config push` guard:** a push whose diff removes an entity, with `--yes` absent and
  EOF on stdin, **changes nothing** (content and version both unchanged); the same push with
  `--yes` applies and bumps. A pure-addition diff needs no confirmation.
- **AC5 — atomic read (adversarial, Docker):** open the `load_versioned` transaction, perform its
  first read, run a **complete `store_and_bump` from a second connection**, then finish reading
  in the first transaction → the returned pair is the **old consistent** pair. Deterministic
  proof that `(stale config, fresh gen)` is unreachable, not a race-hopeful one.
  **The assertion must be on the pair `load_versioned` itself returns**, with the writer interleaved
  *inside* that call (block it mid-read with a `LOCK TABLE … ACCESS EXCLUSIVE` on a table it reads
  after the first one, commit the writer, then await it). A test that hand-rolls its own transaction
  and asserts on values read inside *that* transaction proves only that Postgres implements
  `REPEATABLE READ` — it passes with the `SET` deleted from the implementation, so it does not guard
  the line. Verify the test FAILS with the `SET` removed.
- **AC6 — atomic write (Docker):** `store_and_bump` moves content and version together; a
  rolled-back transaction moves neither. `load_versioned` afterward reproduces `cfg` exactly,
  including nested grants/permissions/credentials/input_schema (s2's AC2 property).
- **AC7 — footgun unreachable:** `cargo build -p sensei-torii` succeeds with `test-support` **off**,
  which is itself the proof — if any production path called `store` or `bump_config_version` it
  would not compile. No `trybuild` dependency is added; the guarantee comes from the feature gate
  plus the fact that the binary builds without it. The full test suite still builds with the
  feature on.
- **AC8 — the operator loop e2e (Docker, cross-process):** process A `torii run submit`s a graph
  that pauses; `torii run list-paused` shows it; `torii run wake` queues it; a **fresh process**
  running `torii worker serve --once` drives it to completion; `torii run status` reports
  `completed` — with a call counter asserting **zero token re-spend** of the completed prefix.
- **AC9 — worker resilience:** a store fault does not kill the loop on the first failure (backoff,
  logged loudly) and **does** exit non-zero after 5 consecutive failures; `--once` runs exactly
  one tick; SIGINT after the in-flight tick exits 0.
- **AC10 — secret hygiene:** `redact_url` output never contains the password for a runtime-assembled
  URL; no command's stdout/stderr contains `DATABASE_URL`'s password on the connect-failure path.
- **AC11 — pure guards are tested and are real guards:** `diff` detects removals, per-field changes,
  and reports an **empty incoming config as all-removed** (not "no changes");
  `requires_confirmation()` is true for any removal and false for pure additions. **Mutation-verified:**
  breaking `diff`'s removal detection fails a test, and reverting `load_versioned` to the torn
  two-read form fails AC5.
- **AC12 — additivity:** `orchestrator`'s executor logic is unchanged; the `orchestrator-core` delta
  is one defaulted trait method plus `reload`/`from_source` calling it; unversioned sources behave
  identically. The **1149 pre-slice tests all still pass** (measured `cargo test --workspace`,
  0 failed, real exit 0 — the "1131" in the s3 checkpoint was a different measure and is not the
  baseline to verify against) and every test added by this slice runs **without a database** — so
  `cargo test --workspace` stays DB-free and no existing count regresses.
- **AC13 — Docker verification:** the store suite (`--features postgres`), the torii DB suite, and
  the e2e run green against `postgres:16` with **real, unpiped exit codes**.

## 10. Deferred / carry-forward

- **HTTP/gRPC management API** + the auth/authz model it requires (a second `[[bin]]` reusing `boot`).
- **`run submit --detach`** — needs a real `pending` status rather than leaning on lease-reclaim (§5.3).
- **Wake backoff/jitter/`max_attempts`/dead-letter** (still open from s3) — until then a poison-pill
  run can crash-loop a worker (§7.2).
- **Redactor coverage for pause reasons** (§8), and a `reason` length cap.
- **`config pull` / rollback** — today recovery from a bad push means re-pushing a good directory;
  the old rows are gone.
- **An airtight compare-and-swap on the config write.** `push` re-reads the generation immediately
  before writing and refuses if it moved (§5.2), which closes the minutes-long human-latency window —
  the entire practical risk. A residual ~1 ms window remains between that re-read returning and
  `store_and_bump`'s bump committing. Closing it properly is cheap and known: because `store_and_bump`
  already bumps *first*, a `store_and_bump_if(expected_version)` adding `where version = $1` to that
  `UPDATE` is a true CAS at zero extra round-trips. Deferred only because it is an `orchestrator-store`
  API change, and the residual window is negligible against two humans.
- **Pool sizing** — `connect()` hardcodes `max_connections(8)` (s1 defer-minor); a multi-worker
  deployment will want it configurable.
- **Metrics/tracing export** from `worker serve` (counts, wake latency, backoff state).
- **Terminal-row pruning/retention** for `scheduled_runs`.
- **`sqlx` now compiles in the default workspace build** (torii depends on it unconditionally) —
  accepted tradeoff; the invariant preserved is that *tests* need no database.

## 11. Files touched

- `crates/torii/` (**new crate**): `Cargo.toml` (`[package] sensei-torii`, `[[bin]] torii`, `clap`,
  `orchestrator-store` with `postgres`), `src/main.rs`, `src/boot.rs`, `src/cmd/{run,config,worker}.rs`,
  `src/diff.rs`, `src/render.rs`, `src/errors.rs`, `tests/` (DB-free + `postgres-tests`-gated).
- `Cargo.toml` (workspace): add `crates/torii` to `members`.
- `crates/orchestrator-core/src/registry.rs`: defaulted `ConfigSource::load_versioned`;
  `RegistryHandle::{reload, from_source}` call it.
- `crates/orchestrator-store/src/postgres.rs`: `PostgresConfigSource::{load_versioned, store_and_bump}`;
  `store`/`bump_config_version` gated behind `test-support`.
- `crates/orchestrator-store/Cargo.toml`: new `test-support` feature.
- `crates/orchestrator/Cargo.toml`: `postgres-tests` also enables `orchestrator-store/test-support`.
- `docs/superpowers/orchestrator-overview.md`: decision log + feature status + spec index.
