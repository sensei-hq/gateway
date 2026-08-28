//! The ONE database-test isolation guard, shared by every crate whose tests touch the
//! global `orchestrator.*` tables.
//!
//! # Why this module exists at all
//!
//! Most DB tests isolate themselves with a fresh `RunId`, so they are parallel-safe. Two
//! groups cannot:
//!
//! * **`config_*`** — `orchestrator.config_versions` is `id boolean primary key check(id)`,
//!   i.e. deliberately ONE row, and `store`/`store_and_bump` are replace-ALL. Any test that
//!   asserts a specific generation conflicts with any concurrent config writer.
//! * **`scheduled_runs`** — `claim_due` is an instance-wide sweep (`… limit N`), so a
//!   concurrent claim steals another test's due row and the victim's assertion fails.
//!
//! # Why it lives HERE, in `orchestrator-store`, rather than being copy-pasted
//!
//! It used to be copy-pasted, and the copies did not cover the same ground. Three crates
//! run tests against these tables:
//!
//! | crate | before | after |
//! |---|---|---|
//! | `sensei-orchestrator-store` (this one) | private copy in `postgres::tests` | uses this |
//! | `sensei-torii` (`src/lib.rs`, `tests/e2e_pg.rs`) | a second private copy, config only | uses this |
//! | `sensei-orchestrator` (`executor::tests::postgres_e2e`) | **NOTHING** | uses this |
//!
//! The third row was the bug. `sensei-orchestrator` has no `sqlx` dependency at all — it
//! reaches Postgres only through `orchestrator_store::postgres` — so the guard could not
//! simply be copied a third time; it has to be LENT from this crate. Until it was, that
//! crate's two `config_versions` tests raced *each other inside one test binary* (they are
//! `#[tokio::test]`s in the same `mod`, so libtest runs them on parallel threads), which
//! reproduced 5 runs out of 5 as
//!
//! ```text
//! assertion `left == right` failed: handle boots at the durable version
//!   left: 2
//!  right: 1
//! ```
//!
//! and raced every OTHER process's guarded tests too: with three concurrent
//! `orchestrator-store` suites alongside it, the orchestrator config test failed 10/10.
//! An advisory lock only serializes the parties that TAKE it, so one unguarded party makes
//! the lock worthless for everybody.
//!
//! Keeping the implementation in one place is also what keeps the two advisory KEYS from
//! drifting: they are `const`s here, and there is no second definition to fall out of sync.
//!
//! # The two layers
//!
//! 1. **In-process**: a `tokio::sync::Mutex` per lock class, taken FIRST so same-process
//!    tests queue on a cheap local mutex instead of on Postgres, and so this never holds a
//!    database connection while merely waiting for a local turn.
//!
//!    `tokio::sync::Mutex`, not `std::sync::Mutex`: the guard is held across the test's
//!    awaits, which trips `clippy::await_holding_lock` and would park a worker thread of a
//!    multi-threaded test. A bonus for this use: tokio's mutex has NO poisoning, so a
//!    panicking test simply releases the guard instead of cascading one real failure into a
//!    dozen confusing ones across its whole group.
//!
//! 2. **Cross-process**: a Postgres SESSION-level advisory lock on a dedicated connection.
//!    An in-process mutex cannot serialize across processes; `cargo test --workspace` runs
//!    test binaries one at a time so it hides that, but `cargo nextest` runs tests in
//!    separate processes IN PARALLEL and would defeat the mutexes entirely — a trap laid
//!    for whoever adopts it.
//!
//! # Panic safety
//!
//! A test that panics while holding the guard must not wedge every later test.
//!
//! * The in-process half is a `tokio::sync::Mutex`, which has no poisoning: unwinding drops
//!   the `MutexGuard` and the mutex is simply free again.
//! * The cross-process half is a `sqlx::PgConnection` owned BY VALUE. Unwinding drops it,
//!   the socket closes, and Postgres releases a session-level advisory lock on disconnect.
//!   No explicit `pg_advisory_unlock` is needed — and would not run on unwind anyway.
//! * The re-entrancy counter is decremented in `Drop`, which runs during unwind.
//!
//! [`a_panicking_holder_does_not_wedge_the_next_taker`] pins this.
//!
//! # Re-entrancy
//!
//! Taking the same lock class twice in one test used to HANG FOREVER on the non-reentrant
//! `tokio::sync::Mutex` — before ever reaching Postgres, so the connection's
//! `statement_timeout` did not bound it (measured: killed at a 60s cap, `exit=124`). The
//! old code's only defence was a comment asserting that no test does this, which is a
//! convention, not a guarantee.
//!
//! [`DbGuard`] now counts depth per lock class in a thread-local: depth 0 acquires the
//! mutex and the advisory lock, depth > 0 returns an INERT guard, and `Drop` decrements.
//!
//! A thread-local is sound here because a guard can never leave the thread that made it:
//! [`DbGuard`] is `!Send` (see its `_not_send` field), which the compiler enforces. And a
//! `#[tokio::test]` body is driven by `Runtime::block_on` ON the libtest thread that owns
//! the test — true for the `current_thread` default AND for the `multi_thread` flavor,
//! whose worker pool only runs `spawn`ed futures, never the root one — so "this thread"
//! and "this test" are the same scope.
//!
//! (Postgres was never the deadlock: `pg_advisory_lock` is itself session-reentrant and
//! counted. The local mutex was.)

use std::cell::Cell;
use std::marker::PhantomData;
use std::thread::LocalKey;

use sqlx::Connection;

/// The environment variable every DB test reads. Named here so the guard's dedicated
/// advisory-lock connection provably points at the SAME database as the test it is
/// isolating; `sensei-torii` pins that agreement with an assertion against its own
/// `boot::ENV_DATABASE_URL`.
pub const ENV_DATABASE_URL: &str = "DATABASE_URL";

/// Lock class 1 — the `config_*` tables and the single-row `config_versions`.
///
/// There is no per-test key to scope by: the row is deliberately a singleton, so every
/// writer is every other writer's conflict.
const ADVISORY_CONFIG_TABLES: i64 = 0x5350_4441_5441_3401; // "SPDATA4" + 01

/// Lock class 2 — `scheduled_runs` claim sweeps. A fresh `RunId` is NOT enough: `claim_due`
/// is an instance-wide sweep, so a concurrent test's claim steals another test's due row.
/// Only tests that CLAIM need this; enqueue/status-only tests scope fine by run id.
const ADVISORY_SCHEDULED_RUNS: i64 = 0x5350_4441_5441_3402; // "SPDATA4" + 02

static CONFIG_TABLES: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static SCHEDULED_RUNS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

thread_local! {
    /// How many live [`DbGuard`]s of each class this thread — i.e. this test — holds.
    static CONFIG_DEPTH: Cell<u32> = const { Cell::new(0) };
    static SCHEDULED_DEPTH: Cell<u32> = const { Cell::new(0) };
}

/// The raw `DATABASE_URL` lookup with NO side effects.
///
/// Deliberately separate from each crate's announcing `db_url()`: those print a visible
/// `SKIP <test>` notice so a green run that touched no database is distinguishable from one
/// that did, and the guard must not print a SECOND such line for the same test.
pub fn database_url_raw() -> Option<String> {
    std::env::var(ENV_DATABASE_URL)
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// RAII over both isolation layers plus this thread's re-entrancy depth.
///
/// Dropping releases everything it owns: the mutex guard, and the connection whose `Drop`
/// closes the socket — which Postgres treats as a disconnect and which releases the
/// session-level advisory lock immediately. Both survive an unwind; see the module docs.
pub struct DbGuard {
    /// `None` on a re-entrant (inert) guard: the OUTER guard on this thread owns the lock.
    _mutex: Option<tokio::sync::MutexGuard<'static, ()>>,
    /// `None` when no database is configured (the test skips) or on an inert guard.
    _conn: Option<sqlx::PgConnection>,
    /// The class whose thread-local depth this guard decrements on drop.
    depth: &'static LocalKey<Cell<u32>>,
    /// Makes `DbGuard` `!Send`, which is what makes the thread-local depth counter SOUND:
    /// the compiler now refuses any code that would acquire on one thread and drop on
    /// another (or hold one across a `tokio::spawn`), so increment and decrement always
    /// land on the same counter. Not merely documentation — a build error.
    _not_send: PhantomData<*const ()>,
}

impl Drop for DbGuard {
    fn drop(&mut self) {
        // `try_with`, not `with`: during thread teardown the thread-local may already be
        // destroyed, and `with` would panic — a panic inside `Drop` while possibly already
        // unwinding aborts the process.
        let _ = self.depth.try_with(|c| c.set(c.get().saturating_sub(1)));
    }
}

/// Serialize on lock class 1 (`config_*` / `config_versions`) — process-wide AND
/// cross-process. Safe to take more than once within a single test; see the module docs.
pub async fn config_guard() -> DbGuard {
    acquire(&CONFIG_TABLES, &CONFIG_DEPTH, ADVISORY_CONFIG_TABLES).await
}

/// Serialize on lock class 2 (`scheduled_runs` claim sweeps) — process-wide AND
/// cross-process. Safe to take more than once within a single test; see the module docs.
pub async fn scheduler_guard() -> DbGuard {
    acquire(&SCHEDULED_RUNS, &SCHEDULED_DEPTH, ADVISORY_SCHEDULED_RUNS).await
}

/// Lock ORDER, for the day someone needs both: acquire **config-tables before
/// scheduled-runs**, always, in every crate. Nothing takes both today (a test taking both
/// `config_guard` and `scheduler_guard` would show up in the same grep that found the
/// unguarded crate), so there is no path that could take these two advisory locks in
/// opposite orders and deadlock.
async fn acquire(
    mutex: &'static tokio::sync::Mutex<()>,
    depth: &'static LocalKey<Cell<u32>>,
    key: i64,
) -> DbGuard {
    // Incremented BEFORE the await, so the counter reads "this thread is inside the
    // guard's critical path", covering the window where it is still waiting for its turn.
    // Incrementing only after acquisition would leave a re-entrant take during that window
    // trying to lock the mutex a second time — the exact hang this counter exists to stop.
    let already_held = depth.with(|c| {
        let d = c.get();
        c.set(d + 1);
        d
    });
    if already_held > 0 {
        return DbGuard {
            _mutex: None,
            _conn: None,
            depth,
            _not_send: PhantomData,
        };
    }
    let mutex = mutex.lock().await;
    let conn = acquire_advisory(key).await;
    DbGuard {
        _mutex: Some(mutex),
        _conn: conn,
        depth,
        _not_send: PhantomData,
    }
}

/// Open a DEDICATED connection and hold `pg_advisory_lock(key)` on it.
///
/// Deliberately NOT a connection borrowed from a pool: returning a pooled connection while
/// the session-level lock is still held would leak the lock to whichever caller borrows
/// that connection next. `None` when no database is configured — every caller's test
/// checks `db_url()` itself and skips, so nothing here needs to run.
///
/// Blocking `pg_advisory_lock` (not `pg_try_advisory_lock`) is correct: a test should WAIT
/// for its turn, not fail because another process got there first. An indefinite block only
/// matters if a lock is somehow orphaned, and a session-level lock requires a LIVE
/// connection to hold it — a crashed process's socket is reclaimed by the OS, which
/// Postgres notices and releases the lock. As a defensive bound anyway (e.g. a wedged
/// connection the OS has not yet reaped), the wait is capped by a `statement_timeout` on
/// this dedicated connection rather than left truly infinite, so a genuinely stuck case
/// fails loudly instead of hanging CI forever.
async fn acquire_advisory(key: i64) -> Option<sqlx::PgConnection> {
    let url = database_url_raw()?;
    let mut conn = sqlx::PgConnection::connect(&url)
        .await
        .expect("advisory-lock connection: DATABASE_URL is set, so this must succeed");
    sqlx::query("set statement_timeout = '30s'")
        .execute(&mut conn)
        .await
        .expect("set statement_timeout");
    sqlx::query("select pg_advisory_lock($1)")
        .bind(key)
        .execute(&mut conn)
        .await
        .expect("pg_advisory_lock must succeed once connected");
    Some(conn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Taking the SAME class twice in one test must return promptly, not hang.
    ///
    /// Red before the depth counter: this exact shape was killed at a 60s cap (`exit=124`)
    /// against the plain mutex-then-advisory design. `timeout` here so a regression fails
    /// LOUDLY in seconds instead of wedging CI until the job's own limit.
    #[tokio::test]
    async fn a_lock_class_is_reentrant_within_one_test() {
        if database_url_raw().is_none() {
            return;
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            let outer = config_guard().await;
            let inner = config_guard().await;
            let innermost = config_guard().await;
            drop(innermost);
            drop(inner);
            drop(outer);
        })
        .await
        .expect("config_guard must be re-entrant within one test, not deadlock");

        // Depth is back to zero, so the NEXT acquisition really takes the locks again
        // rather than handing out a permanently-inert guard.
        assert_eq!(
            CONFIG_DEPTH.with(|c| c.get()),
            0,
            "every guard was released"
        );
        tokio::time::timeout(Duration::from_secs(10), config_guard())
            .await
            .expect("a fresh acquisition after the nest still works");
    }

    /// The two classes are independent, and holding both is fine in the documented order.
    #[tokio::test]
    async fn the_two_lock_classes_are_independent() {
        if database_url_raw().is_none() {
            return;
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            // Documented global order: config-tables BEFORE scheduled-runs.
            let _cfg = config_guard().await;
            let _sched = scheduler_guard().await;
        })
        .await
        .expect("the two classes must not contend with each other");
    }

    /// A test that panics while holding the guard must release BOTH layers, or one real
    /// failure cascades into a wedged suite.
    ///
    /// The unwind is driven synchronously through `catch_unwind` so the whole
    /// acquire → panic → drop → re-acquire cycle is provable inside ONE passing test.
    /// (`AssertUnwindSafe` because a `DbGuard` is deliberately `!Send`/`!UnwindSafe`; the
    /// assertion is honest — the guard is dropped by the unwind, never observed after it.)
    #[tokio::test]
    async fn a_panicking_holder_does_not_wedge_the_next_taker() {
        if database_url_raw().is_none() {
            return;
        }
        let guard = config_guard().await;
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _held = guard;
            panic!("deliberate: a test panicking while it holds the guard");
        }));
        assert!(unwound.is_err(), "the panic really did unwind");
        assert_eq!(
            CONFIG_DEPTH.with(|c| c.get()),
            0,
            "Drop ran during the unwind, so the depth counter did not leak"
        );

        // The real proof: acquiring again completes. That exercises BOTH layers — a
        // poisoned/unreleased mutex or a still-held advisory lock would time out here.
        tokio::time::timeout(Duration::from_secs(10), config_guard())
            .await
            .expect("a panicking holder must not wedge the next taker");
    }
}
