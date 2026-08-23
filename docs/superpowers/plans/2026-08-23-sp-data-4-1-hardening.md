# SP-DATA-4.1 — hardening pass Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the seven small deferrals carried out of SP-DATA-3 and SP-DATA-4, three of which are correctness or security and one of which closes a claim the spec currently makes falsely.

**Why this is a plan and not a spec→plan cycle:** every item here is a *recorded decision awaiting implementation*, not open design. The mechanisms are already written down in
`docs/superpowers/specs/2026-08-18-sp-data-3-durable-scheduler-design.md` §11 and
`docs/superpowers/specs/2026-08-22-sp-data-4-torii-management-cli-design.md` §10. Where a genuine fork existed, the call is made explicitly in the task below and the reasoning is recorded.

**Tech Stack:** Rust 2024, `sqlx` 0.8 runtime queries, `tokio`, existing `orchestrator`/`orchestrator-core`/`orchestrator-store`/`torii` crates, Docker `postgres:16`.

**Baseline that must not regress:** `cargo test --workspace` = **1258 passed / 0 failed**, green both with and without `DATABASE_URL` at default parallelism; `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings` both exit 0.

**Database:** container `torii-pg`, port 5433, trust auth, schema applied.
`DATABASE_URL=postgres://postgres@localhost:5433/postgres` — **env does not persist between shell calls; prefix every command.** A DB test with the variable unset returns early and passes vacuously; the suites now print `SKIP <test>: DATABASE_URL not set` to fd 2 so that is visible.

---

## The seven items, and why each is here

| # | Item | Class | Source |
|---|---|---|---|
| 1 | `store_and_bump_if` — an airtight CAS on the config write | correctness | s4 §10 |
| 2 | Pause-reason redaction + length cap in operator output | security | s4 §8, §10 |
| 3 | Cross-process DB-test isolation via a Postgres advisory lock | correctness (test integrity) | s4 §10 |
| 4 | Second-signal fast path for `worker serve` | closes a false spec claim | s4 §7.3 |
| 5 | Configurable pool size on `connect()` | ops | s1 defer, s4 §10 |
| 6 | s3's AC8 fence-composition e2e | untested arm | s3 §11 |
| 7 | `scheduled_runs` retention pruning | ops | s3 §11, s4 §10 |

---

## Task 1: `store_and_bump_if` — close the residual config-write window

**Files:**
- Modify: `crates/orchestrator-store/src/postgres.rs` (`PostgresConfigSource`)
- Modify: `crates/torii/src/cmd/config.rs` (use it in `write_and_report`)

**Why.** `push` currently re-reads the generation immediately before writing and refuses if it moved. That closes the *human-latency* window — the entire practical risk — but leaves ~1 ms between the re-read returning and the bump committing. The fix is free because `store_and_bump` already performs its version bump **first**, precisely so writers serialize on the `config_versions` row: adding `where version = $1` to that `UPDATE` makes it a true compare-and-swap at zero extra round-trips.

- [ ] **Step 1: Write the failing test**

In `postgres.rs`'s test module, guarded by `db_url()` and taking the existing `CONFIG_TABLES` guard:

```rust
    /// A CAS write must apply ONLY at the expected generation. This is the residual
    /// window the SP-DATA-4 re-read could not close: between `version()` returning and
    /// the bump committing.
    #[tokio::test]
    async fn store_and_bump_if_refuses_at_an_unexpected_generation() {
        let Some(url) = db_url() else { return };
        let _guard = config_guard().await;
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());

        let v = src.store_and_bump(&cfg_with_skill("base")).await.unwrap();

        // Wrong expectation -> refuse, and change NOTHING.
        let refused = src
            .store_and_bump_if(&cfg_with_skill("should-not-land"), v - 1)
            .await
            .unwrap();
        assert!(refused.is_none(), "a stale expectation must not apply");
        let (cfg, now) = src.load_versioned().await.unwrap();
        assert_eq!(now, Some(v), "generation must not advance on a refusal");
        assert!(
            cfg.skills.iter().any(|s| s.name == "base"),
            "content must not change on a refusal"
        );

        // Correct expectation -> applies and advances by exactly one.
        let applied = src
            .store_and_bump_if(&cfg_with_skill("landed"), v)
            .await
            .unwrap()
            .expect("the matching expectation must apply");
        assert_eq!(applied, v + 1);
        let (cfg, now) = src.load_versioned().await.unwrap();
        assert_eq!(now, Some(v + 1));
        assert!(cfg.skills.iter().any(|s| s.name == "landed"));
    }
```

- [ ] **Step 2: Run it and confirm it FAILS to compile**

Run: `DATABASE_URL=postgres://postgres@localhost:5433/postgres cargo test -p sensei-orchestrator-store --features postgres,test-support store_and_bump_if > /tmp/t1.log 2>&1; echo "exit=$?"`
Expected: non-zero — `no method named store_and_bump_if`.

- [ ] **Step 3: Implement**

Add to `impl PostgresConfigSource`, mirroring `store_and_bump` but with a conditional bump. `Ok(None)` means "the generation moved, nothing written"; `Ok(Some(v))` is the new generation:

```rust
    /// Replace the whole registry AND advance the generation, but ONLY if the current
    /// generation is still `expected`. Returns `Ok(None)` if it moved — a true CAS.
    ///
    /// The bump runs FIRST and carries the `where version = $1` predicate, so a losing
    /// writer neither advances the generation nor reaches `write_all`: the whole
    /// transaction is decided by that one row before any content is touched.
    pub async fn store_and_bump_if(
        &self,
        cfg: &RegistryConfig,
        expected: u64,
    ) -> Result<Option<u64>, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(store_err)?;
        let bumped: Option<(i64,)> = sqlx::query_as(
            "update orchestrator.config_versions
                set version = version + 1, updated_at = now()
              where id = true and version = $1
              returning version",
        )
        .bind(expected as i64)
        .fetch_optional(&mut *tx)
        .await
        .map_err(store_err)?;
        let Some((v,)) = bumped else {
            // No row matched: either the generation moved, or it does not exist yet
            // (a first-ever push, where `expected` can only legitimately be 0 and the
            // absent row means version 0 — handled by the caller falling back to
            // `store_and_bump`). Roll back and report the miss.
            return Ok(None);
        };
        write_all(&mut tx, cfg).await?;
        tx.commit().await.map_err(store_err)?;
        Ok(Some(v as u64))
    }
```

**Note the first-push case:** `config_versions` starts with NO row (absent ⇒ generation 0), so a `where version = 0` update matches nothing and the CAS reports a miss even though nothing raced. Handle it in the caller: if `expected == 0` and `version()` still reports `Some(0)`, fall back to `store_and_bump`. Add a test for a genuine first push through the caller.

- [ ] **Step 4: Use it in `push`**

In `crates/torii/src/cmd/config.rs`, `write_and_report` currently re-reads `version()` and compares. Replace the re-read + `store_and_bump` with `store_and_bump_if(cfg, current_v)`, mapping `Ok(None)` to the SAME refusal outcome it produces today (text containing "moved v" and "nothing written" — do not change the operator-facing wording, the existing tests assert on it). Keep the first-push fallback described above.

- [ ] **Step 5: Verify**

Run the store suite and the torii suite with `DATABASE_URL` set; both must be green and the existing stale-diff tests must still pass unchanged. Report counts and real exit codes.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-store/src/postgres.rs crates/torii/src/cmd/config.rs
git commit -m "feat(store): SP-DATA-4.1 (1/7) — store_and_bump_if closes the residual config-write window"
```

---

## Task 2: redact and cap pause reasons in operator output

**Files:**
- Modify: `crates/torii/src/render.rs`
- Modify: `crates/torii/Cargo.toml` if `orchestrator-core`'s redactor is not already reachable (it is — `orchestrator_core::{PatternRedactor, Redactor}`)

**Why, and the fork I resolved.** `ScheduledRun.reason` is free text lifted from `PauseInfo.reason` and provider messages, and `list-paused`/`status` print it. The SP-4 s2 `Redactor` covers effect outputs and model output — **not** pause reasons. s3 already stores them unredacted, so torii is the first thing to *display* them.

**Decision: redact at DISPLAY time, in `render.rs`, not at write time in the scheduler.** Write-time would mean injecting a `Redactor` into `Scheduler` and changing what lands in durable storage — a larger question about the redactor's coverage, and one that touches the determinism reasoning s2 was careful about. Display-time closes the exposure torii actually introduced, costs nothing, and leaves the durable row truthful. **Record explicitly that the durable `scheduled_runs.reason` still holds the raw text**, so anyone querying Postgres directly is still exposed — that residue stays a carry-forward.

Also cap the rendered length: an unbounded provider message can push the table into uselessness.

- [ ] **Step 1: Write the failing tests**

```rust
    #[test]
    fn a_pause_reason_is_redacted_before_display() {
        let secret = format!("sk-{}", "A".repeat(24));
        let mut r = row(None, Some(&format!("quota exceeded for {secret}")));
        let out = table(&[r.clone()]);
        assert!(!out.contains(&secret), "a secret-shaped reason leaked: {out}");
        assert!(out.contains("[REDACTED]"), "{out}");

        r.reason = Some(format!("quota exceeded for {secret}"));
        let j = json(&[r]).expect("serializes");
        assert!(!j.contains(&secret), "the JSON path leaked: {j}");
    }

    #[test]
    fn an_overlong_pause_reason_is_capped() {
        let long = "x".repeat(5_000);
        let out = table(&[row(None, Some(&long))]);
        let line = out.lines().nth(1).expect("a data row");
        assert!(line.len() < 400, "an unbounded reason wrecks the table: {} chars", line.len());
        assert!(out.contains('…'), "truncation must be visible: {out}");
    }
```

Note the JSON assertion: redaction must apply on **both** paths here, unlike `one_line` (which is display-only because a script wants the exact stored text). A secret is different from a newline — a script consuming `--json` should not receive it either.

- [ ] **Step 2: Run and confirm they fail**

Run: `cargo test -p sensei-torii render > /tmp/t2.log 2>&1; echo "exit=$?"`
Expected: non-zero, both new tests failing (the secret appears verbatim; no truncation).

- [ ] **Step 3: Implement**

Add a private helper applied to `reason` in BOTH `table()` and `json()`. For JSON, map the rows through a redacted copy before serializing rather than post-processing the string:

```rust
const REASON_MAX: usize = 300;

/// Pause reasons are free text from a provider or a pause site. s3 stores them
/// unredacted and torii is the first thing to DISPLAY them, so scrub here — on the
/// JSON path too, because a secret is not like a newline: a script should not
/// receive it either. The durable `scheduled_runs.reason` still holds the raw text.
fn safe_reason(s: &str) -> String {
    use orchestrator_core::Redactor;
    let scrubbed = orchestrator_core::PatternRedactor::default().redact(s);
    let one = one_line(&scrubbed);
    if one.chars().count() <= REASON_MAX {
        one
    } else {
        let mut t: String = one.chars().take(REASON_MAX).collect();
        t.push('…');
        t
    }
}
```

Verify `Redactor`'s real method name and signature before using it — check `crates/orchestrator-core/src/redact.rs`. Construct the `PatternRedactor` once (a `std::sync::LazyLock`) rather than per row; its regex set is not free.

- [ ] **Step 4: Verify, then commit**

Run `cargo test -p sensei-torii; echo "exit=$?"`. Then:

```bash
cargo fmt --all
git add crates/torii/src/render.rs
git commit -m "fix(torii): SP-DATA-4.1 (2/7) — redact and cap pause reasons before they reach an operator"
```

---

## Task 3: cross-process DB-test isolation via a Postgres advisory lock

**Files:**
- Modify: `crates/orchestrator-store/src/postgres.rs` (the `CONFIG_TABLES`/`SCHEDULED_RUNS` guards)
- Modify: `crates/torii/src/lib.rs` (`test_guard`)

**Why.** The in-process mutexes serialize each crate's own tests, but torii's DB tests hit the same global `config_*` tables from a **different process**. Forcing two concurrent `cargo` invocations reproduces a failure roughly one run in four. Not reachable through `cargo test --workspace` (cargo runs test binaries sequentially), but **`cargo nextest` runs tests in separate processes in parallel and would defeat the in-process guards entirely** — so this is a trap laid for whoever adopts it.

- [ ] **Step 1: Implement the shared lock**

Postgres session-level advisory locks are the right instrument: they are held by a *connection*, released on disconnect, and visible across processes. Both crates' guards must use the **same key**.

In each guard, after taking the in-process mutex, acquire `pg_advisory_lock(<key>)` on a dedicated connection held for the test's duration, and release it (or simply drop the connection) at the end. Use one well-known key per resource class, defined identically in both crates with a comment naming the other site:

```rust
/// Shared with `crates/torii/src/lib.rs`'s `test_guard`. Session-level advisory locks
/// are held by a CONNECTION and are visible across processes, which is what the
/// in-process mutex cannot do: torii's DB tests hit these same global tables from a
/// separate process. Both sites MUST use these exact keys.
const ADVISORY_CONFIG_TABLES: i64 = 0x5350_4441_5441_3401; // "SPDATA4" + 01
const ADVISORY_SCHEDULED_RUNS: i64 = 0x5350_4441_5441_3402;
```

The guard should return an RAII value holding both the mutex guard and the connection, so a panicking test still releases (the connection drops).

- [ ] **Step 2: Prove it works across processes**

This is the whole point, so demonstrate it rather than asserting it. Run the store suite and the torii suite as **two concurrent `cargo` invocations**, at least 8 times:

```bash
for i in $(seq 1 8); do
  DATABASE_URL=postgres://postgres@localhost:5433/postgres cargo test -p sensei-orchestrator-store --features postgres,test-support > /tmp/a$i.log 2>&1 &
  A=$!
  DATABASE_URL=postgres://postgres@localhost:5433/postgres cargo test -p sensei-torii > /tmp/b$i.log 2>&1 &
  B=$!
  wait $A; RA=$?; wait $B; RB=$?
  echo "round $i: store=$RA torii=$RB"
done
```

Expected: all 16 exit codes 0. **Before the fix this reproduces a failure in roughly one round in four** — run the same loop against the pre-fix code first and report how many rounds failed, so we know the test of the fix is real.

- [ ] **Step 3: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-store/src/postgres.rs crates/torii/src/lib.rs
git commit -m "test: SP-DATA-4.1 (3/7) — a shared PG advisory lock serializes DB tests across processes"
```

---

## Task 4: the second-signal fast path

**Files:**
- Modify: `crates/torii/src/main.rs` (`shutdown_signal`)
- Modify: `crates/torii/src/cmd/worker.rs` (`serve`)

**Why.** The spec's §7.3 claims "a second signal exits immediately" and leans on it as the bound on an unbounded tick. There is no such path: `shutdown_signal()` yields a one-shot future, so signals arriving during a tick are consumed and discarded. Demonstrated by blocking `claim_due` with `LOCK TABLE … ACCESS EXCLUSIVE`: two SIGTERMs and a SIGINT all left the worker alive until SIGKILL. Safe (the lease covers a hard kill) but an operator following the doc concludes the process is wedged.

- [ ] **Step 1: Write the failing test**

`serve` takes `shutdown: impl Future`. Extend it to take a *second* future, or better, change the parameter to something that can fire twice — a `tokio::sync::watch` receiver or an `Arc<Notify>` is cleaner than two futures. Whatever shape you pick, the test must be able to fire the signal twice with a tick in flight:

```rust
    /// A tick's duration is unbounded (it drives runs inline), so the FIRST signal
    /// cannot interrupt it. The second must, or an operator watching a slow tick has
    /// no way to stop the process short of SIGKILL.
    #[tokio::test(start_paused = true)]
    async fn a_second_signal_abandons_an_in_flight_tick() { /* … */ }
```

Use a ticker that blocks on a channel so the tick is genuinely in flight, fire the signal twice, and assert `serve` returns without waiting for the tick to finish. Assert the outcome text distinguishes an abandoned tick from a clean shutdown.

- [ ] **Step 2: Confirm it fails, implement, confirm it passes**

The implementation races the second signal against `ticker.tick()` itself, not just against the sleep. Keep the first-signal semantics exactly as they are (finish the in-flight tick). Report both outputs.

- [ ] **Step 3: Wire it in `main.rs` and verify against the real binary**

Send SIGTERM twice to a `worker serve` blocked mid-tick (block it by holding `LOCK TABLE orchestrator.scheduled_runs IN ACCESS EXCLUSIVE MODE` from psql) and confirm the second one exits. Report the observed exit code and output.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/torii/src/main.rs crates/torii/src/cmd/worker.rs
git commit -m "feat(torii): SP-DATA-4.1 (4/7) — a second signal abandons an in-flight tick"
```

---

## Task 5: configurable pool size

**Files:**
- Modify: `crates/orchestrator-store/src/postgres.rs` (`connect`)
- Modify: `crates/torii/src/boot.rs`

**Why.** `connect()` hardcodes `max_connections(8)`, an SP-DATA-1 deferral. SP-DATA-4 then made every adapter share ONE pool, so those 8 are now the worker's entire budget — the right tradeoff, but it means the knob matters more, and there is no knob.

- [ ] **Step 1: Add `connect_with_max`**

Keep `connect(url)` as-is (it has many callers) and add `connect_with_max(url, max: u32)`; make `connect` delegate with the existing default of 8 so behaviour is unchanged. Document that 8 was chosen for a single-adapter-per-pool world and that a shared-pool worker may want more.

- [ ] **Step 2: Expose it in torii**

Read an optional `TORII_POOL_SIZE` env var in `boot::env_config_from` (the injected-getter shape — do not read `std::env` directly), defaulting to 8. Reject a zero or unparseable value loudly, with the same discipline `parse_interval` uses. Add pure tests: absent ⇒ 8; `"16"` ⇒ 16; `"0"` ⇒ error; `"abc"` ⇒ error.

- [ ] **Step 3: Verify, then commit**

```bash
cargo fmt --all
git add crates/orchestrator-store/src/postgres.rs crates/torii/src/boot.rs
git commit -m "feat(store): SP-DATA-4.1 (5/7) — configurable pool size, exposed as TORII_POOL_SIZE"
```

---

## Task 6: s3's AC8 fence-composition e2e

**Files:**
- Modify: `crates/torii/tests/e2e_pg.rs`

**Why.** SP-DATA-3 shipped the arm where a wake whose config generation drifted records the run terminal-`Failed` with a "stale: config changed" reason — and explicitly deferred testing it. SP-DATA-4 then made that arm *reachable in production* through `torii config push`, and the whole-slice review demonstrated it manually. It should be a test.

- [ ] **Step 1: Write it**

Mirror the existing cross-process e2e's structure. Process A submits a run that pauses at generation *g*. Then bump the generation (via `store_and_bump`, or through the real `push` path). Then a fresh process-B worker ticks. Assert:
- the run's status becomes `Failed`, not `Completed`;
- its `reason` contains "stale" and names both generations;
- **zero** gateway calls were made for that run (it must fail at the fence, before spending anything) — use the attributable-marker technique the existing e2e uses, not a bare `calls.len()`;
- `torii run wake` on it afterwards reports `not queued` naming `failed`, i.e. the operator gets an honest answer rather than a silent no-op.

Take the `SCHEDULED_RUNS` guard, as the sibling tests do.

- [ ] **Step 2: Prove it discriminates**

Temporarily make `Scheduler::record` treat a `VersionFenceMismatch` as a pause rather than terminal-`Failed`, confirm the test fails, restore. Report both outputs.

- [ ] **Step 3: Commit**

```bash
cargo fmt --all
git add crates/torii/tests/e2e_pg.rs
git commit -m "test(torii): SP-DATA-4.1 (6/7) — cover s3's deferred fence-composition arm"
```

---

## Task 7: `scheduled_runs` retention pruning

**Files:**
- Modify: `crates/orchestrator-core/src/scheduler.rs` (`SchedulerStore` trait)
- Modify: `crates/orchestrator-store/src/scheduler_store.rs`, `crates/orchestrator-store/src/postgres.rs`
- Modify: `crates/torii/src/cmd/run.rs`, `crates/torii/src/main.rs`

**Why, and the fork I resolved.** Terminal rows accumulate forever — the review's environment reached 234 rows from tests alone, and `claim_due`'s `limit 64` with `order by next_wake` means a large enough backlog could in principle exclude a due run from a batch.

**Decision: an explicit operator command, not automatic pruning inside `tick()`.** Deleting durable rows as a silent side effect of a poll loop is precisely the kind of surprise that should not exist, and it would make `tick()`'s contract much harder to reason about. `torii run prune --older-than <dur>` shows what it will delete and requires confirmation unless `--yes`, matching `config push`'s discipline.

- [ ] **Step 1: Add `prune_terminal` to the trait**

```rust
    /// Delete TERMINAL rows (`completed`/`failed`/`cancelled`) whose `updated_at` is
    /// older than `before`, returning the count deleted. Never touches a non-terminal
    /// row: a `paused` run has no age at which it becomes safe to forget, and a
    /// `waking` row may be a live lease.
    async fn prune_terminal(&self, before: DateTime<Utc>) -> Result<u64, OrchestratorError>;
```

Implement in both `InMemorySchedulerStore` and `PostgresSchedulerStore`. Add a **counting** companion the CLI can call before confirming — either a `count_terminal_before` method or have `prune_terminal` take a `dry_run: bool`; prefer the separate count method, it is harder to misuse.

- [ ] **Step 2: Tests (in-memory, DB-free) then Postgres**

In-memory first: a terminal row older than the cutoff is deleted; a terminal row newer is kept; a `paused` row is NEVER deleted regardless of age; a `waking` row is never deleted. That last pair is the safety property — assert it explicitly. Then the Postgres parity test under the `SCHEDULED_RUNS` guard.

- [ ] **Step 3: The CLI command**

`torii run prune --older-than 30d [--yes]`, light tier. Reuse `cmd::worker::parse_interval`'s style for the duration but note it must accept days — extend it or add a sibling parser, and say which. Show the count, require confirmation via `cmd::config::interactive_confirm` when the count is non-zero, and report the number actually deleted (re-read or use the returned count — report the EFFECT, per this project's rule).

- [ ] **Step 4: Verify, then commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat(torii): SP-DATA-4.1 (7/7) — torii run prune, with paused and waking rows never eligible"
```

---

## Final verification

```bash
cargo fmt --all --check;                                              echo "fmt=$?"
cargo clippy --workspace --all-targets -- -D warnings;                echo "clippy=$?"
cargo test --workspace;                                               echo "ws-nodb=$?"
DATABASE_URL=postgres://postgres@localhost:5433/postgres cargo test --workspace; echo "ws-db=$?"
DATABASE_URL=... cargo test -p sensei-orchestrator-store --features postgres,test-support; echo "store=$?"
DATABASE_URL=... cargo test -p sensei-orchestrator --features postgres-tests -- --test-threads=1; echo "orch=$?"
DATABASE_URL=... cargo test -p sensei-torii;                          echo "torii=$?"
```

Plus the Task 3 concurrent-invocation loop, which is the only check that exercises the cross-process guard.

Then update `docs/superpowers/specs/2026-08-22-sp-data-4-torii-management-cli-design.md` §10 to strike the seven closed items, and `docs/superpowers/orchestrator-overview.md`'s slice-4 bullet to record the hardening pass. **§7.3's second-signal paragraph must change from "deliberately not implemented" back to a statement of what now exists** — it was corrected to say the path was absent, and Task 4 makes it present again.

## Self-Review

**Coverage:** all seven deferrals in the table have a task. **Placeholders:** none — every step names files, gives code or an exact command, and states its expected result. **Type consistency:** `store_and_bump_if` returns `Option<u64>` and is consumed as such in Task 1 Step 4; `prune_terminal` returns `u64` and is consumed in Task 7 Step 3; the advisory-lock keys in Task 3 are defined once and referenced from both crates.

**Two design calls made explicitly rather than silently**, both recorded above with reasoning: pause-reason redaction is display-time (Task 2), and pruning is operator-invoked (Task 7).

**One risk worth naming:** Task 4 changes `serve`'s signature, and Task 6's e2e calls `serve`. Sequence Task 4 before Task 6, or Task 6 will need rework.
