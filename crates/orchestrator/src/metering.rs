//! Which run a model call belongs to — for the store that records it.
//!
//! The gateway records every call through a `GatewayStore`, and the store is built once
//! and shared: one `Gateway` serves every run the process drives. So the run identity
//! cannot live on the store, or on the gateway — a store field would be whichever run was
//! built last, and two runs in flight would overwrite each other's rows.
//!
//! It is per TASK. [`Executor::drive`](crate::executor::Executor) enters [`with_run`]
//! around the drive loop, so every model call a run makes — decision or chat, nested agent
//! or not — is awaited inside that scope, and a store can ask [`current_run`] while it
//! writes the row. Concurrency-safe by construction: `tokio::task_local::scope` installs
//! the value for the duration of each poll of that future, so two runs joined in one task
//! see their own ids, not each other's.
//!
//! `None` is the honest answer outside a drive — the HTTP chat route, a judge call, a
//! test — and those calls are exactly the ones with no run to attribute.

use std::future::Future;

use orchestrator_core::RunId;

tokio::task_local! {
    /// The run whose drive is executing on this task. Set only by [`with_run`].
    static CURRENT_RUN: RunId;
}

/// Run `fut` with `run` as the current run, so a store it reaches can attribute its rows.
///
/// `Executor::drive` is the production caller. It is public so a test (or another run
/// driver) can exercise a store without booting an executor, and so the seam is visible
/// rather than magic.
pub async fn with_run<F: Future>(run: RunId, fut: F) -> F::Output {
    CURRENT_RUN.scope(run, fut).await
}

/// The run whose drive is executing on THIS task, if any.
///
/// `None` outside a [`with_run`] scope: a chat request, a judge call, an unowned gateway
/// call. A store must treat `None` as "no run to attribute", never as "run unknown".
pub fn current_run() -> Option<RunId> {
    CURRENT_RUN.try_with(|run| *run).ok()
}

#[cfg(test)]
mod tests {
    use super::{current_run, with_run};
    use orchestrator_core::RunId;
    use uuid::Uuid;

    #[test]
    fn outside_a_scope_there_is_no_run() {
        // The honest answer for a chat request, a judge call, or any test that is not
        // driving: there is no run, so a store must attribute nothing.
        assert_eq!(current_run(), None);
    }

    #[tokio::test]
    async fn the_scope_is_visible_inside_and_restored_after() {
        let run = RunId(Uuid::new_v4());
        let seen = with_run(run, async { current_run() }).await;
        assert_eq!(seen, Some(run), "the drive's own future must see its run");
        assert_eq!(current_run(), None, "and the value does not leak out of it");
    }

    #[tokio::test]
    async fn a_nested_scope_restores_the_outer_run() {
        // A nested `drive_nested` re-enters `drive` with the SAME id, so this pins the
        // restore semantics rather than a different-run case that cannot occur today.
        let outer = RunId(Uuid::new_v4());
        let inner = with_run(outer, async {
            let inside = with_run(outer, async { current_run() }).await;
            (inside, current_run())
        })
        .await;
        assert_eq!(inner.0, Some(outer));
        assert_eq!(
            inner.1,
            Some(outer),
            "the outer scope survives the inner one"
        );
    }

    #[tokio::test]
    async fn two_runs_sharing_a_task_each_see_their_own_id() {
        // `tokio::task_local::scope` installs the value per POLL, not per task, so two
        // drives joined into one task cannot read each other's rows.
        let a = RunId(Uuid::new_v4());
        let b = RunId(Uuid::new_v4());
        let (seen_a, seen_b) = tokio::join!(
            with_run(a, async { current_run() }),
            with_run(b, async { current_run() })
        );
        assert_eq!(seen_a, Some(a));
        assert_eq!(seen_b, Some(b));
    }
}
