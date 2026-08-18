//! SP-DATA-1: Postgres adapters for the run-state seams (`ExecutionJournal`/`ContentStore`/
//! `ContextStore`). Schema-agnostic — runs against the dbd-managed `orchestrator.*` schema.
//! Uses sqlx RUNTIME queries (not the compile-time `query!` macros) so the crate builds with
//! no database. Feature-gated: default builds don't pull sqlx.

use orchestrator_core::{
    ExecutionJournal, FORMAT_VERSION, JournalError, JournalEvent, RunId, Seq, Snapshot,
};
use sqlx::postgres::{PgPool, PgPoolOptions};

/// Connect a pool to `database_url` (the dbd-applied `orchestrator.*` schema must exist).
pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(8)
        .connect(database_url)
        .await
}

/// Map a Postgres/sqlx transport error onto the strict, surfaced journal error. Journal
/// writes never swallow a backend failure — it becomes a loud `Backend`.
fn pg_err(e: sqlx::Error) -> JournalError {
    JournalError::Backend(e.to_string())
}

/// Map a serde (de)serialization error onto the same strict backend error — a malformed
/// journal payload is a backend fault, never silently dropped.
fn ser_err(e: serde_json::Error) -> JournalError {
    JournalError::Backend(e.to_string())
}

/// A durable [`ExecutionJournal`] backed by the dbd-managed `orchestrator.*` schema.
///
/// Parity with [`InMemoryJournal`](crate::InMemoryJournal): `append` stamps a monotonic
/// `Seq` (the `bigserial` `journal_events.seq`), `load`/`load_since` return events in
/// ascending `Seq`, `snapshot`/`latest_snapshot` are latest-wins, and `compact` removes
/// the named seqs and appends the manifest in one transaction. Additionally, every load
/// checks the run's persisted [`FORMAT_VERSION`] and fences with
/// [`JournalError::IncompatibleFormat`] on a mismatch — a journal written by an
/// incompatible effect-id/serialization scheme halts resume loudly rather than mis-folding.
#[derive(Clone)]
pub struct PostgresJournal {
    pool: PgPool,
}

impl PostgresJournal {
    /// Wrap a connection pool (see [`connect`]).
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Fence: if this run's persisted `format_version` differs from this build's
    /// [`FORMAT_VERSION`], the durable journal was written by an incompatible scheme —
    /// halt loudly. A run with no `runs` row (no `RunStarted` yet) is not fenced.
    async fn check_format(&self, run: RunId) -> Result<(), JournalError> {
        let row: Option<(i32,)> =
            sqlx::query_as("select format_version from orchestrator.runs where run_id = $1")
                .bind(run.0)
                .fetch_optional(&self.pool)
                .await
                .map_err(pg_err)?;
        if let Some((stored,)) = row
            && stored != FORMAT_VERSION
        {
            return Err(JournalError::IncompatibleFormat {
                run,
                stored,
                expected: FORMAT_VERSION,
            });
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ExecutionJournal for PostgresJournal {
    async fn append(&self, run: RunId, event: JournalEvent) -> Result<Seq, JournalError> {
        let ev = serde_json::to_value(&event).map_err(ser_err)?;
        // Stamp the format version once per run (on the first RunStarted); idempotent.
        if matches!(event, JournalEvent::RunStarted { .. }) {
            sqlx::query(
                "insert into orchestrator.runs (run_id, format_version) values ($1, $2) \
                 on conflict (run_id) do nothing",
            )
            .bind(run.0)
            .bind(FORMAT_VERSION)
            .execute(&self.pool)
            .await
            .map_err(pg_err)?;
        }
        let (seq,): (i64,) = sqlx::query_as(
            "insert into orchestrator.journal_events (run_id, event) values ($1, $2) returning seq",
        )
        .bind(run.0)
        .bind(ev)
        .fetch_one(&self.pool)
        .await
        .map_err(pg_err)?;
        Ok(seq as Seq)
    }

    async fn load(&self, run: RunId) -> Result<Vec<(Seq, JournalEvent)>, JournalError> {
        self.check_format(run).await?;
        let rows: Vec<(i64, serde_json::Value)> = sqlx::query_as(
            "select seq, event from orchestrator.journal_events where run_id = $1 order by seq",
        )
        .bind(run.0)
        .fetch_all(&self.pool)
        .await
        .map_err(pg_err)?;
        rows.into_iter()
            .map(|(s, v)| Ok((s as Seq, serde_json::from_value(v).map_err(ser_err)?)))
            .collect()
    }

    async fn load_since(
        &self,
        run: RunId,
        since: Seq,
    ) -> Result<Vec<(Seq, JournalEvent)>, JournalError> {
        self.check_format(run).await?;
        let rows: Vec<(i64, serde_json::Value)> = sqlx::query_as(
            "select seq, event from orchestrator.journal_events \
             where run_id = $1 and seq > $2 order by seq",
        )
        .bind(run.0)
        .bind(since as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(pg_err)?;
        rows.into_iter()
            .map(|(s, v)| Ok((s as Seq, serde_json::from_value(v).map_err(ser_err)?)))
            .collect()
    }

    async fn snapshot(&self, run: RunId, snap: Snapshot) -> Result<(), JournalError> {
        let v = serde_json::to_value(&snap).map_err(ser_err)?;
        sqlx::query(
            "insert into orchestrator.run_snapshots (run_id, seq, snapshot) values ($1, $2, $3) \
             on conflict (run_id) do update set \
             seq = excluded.seq, snapshot = excluded.snapshot, updated_at = now()",
        )
        .bind(run.0)
        .bind(snap.seq as i64)
        .bind(v)
        .execute(&self.pool)
        .await
        .map_err(pg_err)?;
        Ok(())
    }

    async fn latest_snapshot(&self, run: RunId) -> Result<Option<Snapshot>, JournalError> {
        let row: Option<(serde_json::Value,)> =
            sqlx::query_as("select snapshot from orchestrator.run_snapshots where run_id = $1")
                .bind(run.0)
                .fetch_optional(&self.pool)
                .await
                .map_err(pg_err)?;
        row.map(|(v,)| serde_json::from_value(v).map_err(ser_err))
            .transpose()
    }

    async fn compact(
        &self,
        run: RunId,
        remove_seqs: &[Seq],
        add: JournalEvent,
    ) -> Result<(), JournalError> {
        let ev = serde_json::to_value(&add).map_err(ser_err)?;
        let removes: Vec<i64> = remove_seqs.iter().map(|s| *s as i64).collect();
        // One transaction: drop the compacted events, then append the manifest (a fresh,
        // higher seq). The remaining events keep their original ascending seq order.
        let mut tx = self.pool.begin().await.map_err(pg_err)?;
        sqlx::query("delete from orchestrator.journal_events where run_id = $1 and seq = any($2)")
            .bind(run.0)
            .bind(&removes)
            .execute(&mut *tx)
            .await
            .map_err(pg_err)?;
        sqlx::query("insert into orchestrator.journal_events (run_id, event) values ($1, $2)")
            .bind(run.0)
            .bind(ev)
            .execute(&mut *tx)
            .await
            .map_err(pg_err)?;
        tx.commit().await.map_err(pg_err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{
        ChildStatus, CompactChild, Digest, ExecutionJournal, JournalError, JournalEvent, NodeId,
        RunId, Snapshot,
    };

    /// Tests require a live PG at $DATABASE_URL with the dbd schema applied (the Docker harness).
    /// Absent DATABASE_URL, they skip (so a bare `cargo test --features postgres` without a DB
    /// doesn't fail spuriously).
    fn db_url() -> Option<String> {
        std::env::var("DATABASE_URL").ok()
    }

    /// A fresh, unique run id — every test gets its own so the shared `orchestrator.*`
    /// tables never collide across tests (belt-and-suspenders with `--test-threads=1`).
    fn run() -> RunId {
        RunId(uuid::Uuid::new_v4())
    }

    fn started() -> JournalEvent {
        JournalEvent::RunStarted {
            version: "v1".into(),
        }
    }

    fn node_started(id: &str) -> JournalEvent {
        JournalEvent::NodeStarted {
            node: NodeId(id.into()),
        }
    }

    #[tokio::test]
    async fn connects_and_the_schema_exists() {
        let Some(url) = db_url() else { return };
        let pool = connect(&url).await.expect("connect");
        let (n,): (i64,) = sqlx::query_as(
            "select count(*) from information_schema.tables where table_schema='orchestrator'",
        )
        .fetch_one(&pool)
        .await
        .expect("query");
        assert!(n >= 5, "expected the 5 orchestrator.* tables, saw {n}");
    }

    #[tokio::test]
    async fn append_then_load_returns_events_in_ascending_seq() {
        let Some(url) = db_url() else { return };
        let j = PostgresJournal::new(connect(&url).await.unwrap());
        let r = run();

        let s1 = j.append(r, started()).await.unwrap();
        let s2 = j.append(r, node_started("n1")).await.unwrap();
        assert!(s2 > s1, "seq monotonic");

        let evs = j.load(r).await.unwrap();
        assert_eq!(evs.len(), 2, "both events present");
        assert_eq!(evs[0].0, s1);
        assert_eq!(evs[1].0, s2);
        assert!(evs[0].0 < evs[1].0, "load returns ascending seq order");
        assert!(matches!(evs[0].1, JournalEvent::RunStarted { .. }));
        assert!(matches!(evs[1].1, JournalEvent::NodeStarted { .. }));
    }

    #[tokio::test]
    async fn load_since_returns_only_the_tail() {
        let Some(url) = db_url() else { return };
        let j = PostgresJournal::new(connect(&url).await.unwrap());
        let r = run();

        let s1 = j.append(r, started()).await.unwrap();
        let s2 = j.append(r, node_started("n1")).await.unwrap();

        let tail = j.load_since(r, s1).await.unwrap();
        assert_eq!(tail.len(), 1, "only events with seq > s1");
        assert_eq!(tail[0].0, s2, "the tail is the second event");
    }

    #[tokio::test]
    async fn incompatible_format_version_fences_on_load() {
        let Some(url) = db_url() else { return };
        let pool = connect(&url).await.unwrap();
        let j = PostgresJournal::new(pool.clone());
        let r = run();

        j.append(r, started()).await.unwrap();
        // Simulate a journal written by an OLDER scheme: corrupt the runs.format_version.
        sqlx::query("update orchestrator.runs set format_version = -999 where run_id = $1")
            .bind(r.0)
            .execute(&pool)
            .await
            .unwrap();

        let err = j.load(r).await.unwrap_err();
        assert!(
            matches!(
                err,
                JournalError::IncompatibleFormat {
                    stored: -999,
                    expected: 1,
                    ..
                }
            ),
            "must fence loudly, got {err:?}"
        );
        // load_since fences on the same check.
        assert!(matches!(
            j.load_since(r, 0).await.unwrap_err(),
            JournalError::IncompatibleFormat { .. }
        ));
    }

    #[tokio::test]
    async fn snapshot_round_trips_latest_wins() {
        let Some(url) = db_url() else { return };
        let j = PostgresJournal::new(connect(&url).await.unwrap());
        let r = run();

        // No snapshot yet → None (never a silent empty struct).
        assert!(j.latest_snapshot(r).await.unwrap().is_none());

        let s1 = j.append(r, started()).await.unwrap();
        let s2 = j.append(r, node_started("n1")).await.unwrap();

        let snap = Snapshot {
            seq: s1,
            completed: vec![NodeId("n1".into())],
            skipped: vec![],
            outputs: vec![],
        };
        j.snapshot(r, snap).await.unwrap();
        let got = j
            .latest_snapshot(r)
            .await
            .unwrap()
            .expect("snapshot present");
        assert_eq!(got.seq, s1);
        assert_eq!(got.completed, vec![NodeId("n1".into())]);

        // Latest wins: a second snapshot overwrites the first.
        let snap2 = Snapshot {
            seq: s2,
            completed: vec![NodeId("n1".into()), NodeId("n2".into())],
            skipped: vec![],
            outputs: vec![],
        };
        j.snapshot(r, snap2).await.unwrap();
        assert_eq!(j.latest_snapshot(r).await.unwrap().unwrap().seq, s2);
    }

    #[tokio::test]
    async fn compact_removes_the_named_events_and_appends_the_manifest() {
        let Some(url) = db_url() else { return };
        let j = PostgresJournal::new(connect(&url).await.unwrap());
        let r = run();

        let s0 = j.append(r, started()).await.unwrap();
        let s1 = j.append(r, node_started("n1")).await.unwrap();
        let s2 = j.append(r, node_started("n2")).await.unwrap();

        let manifest = JournalEvent::MapCompacted {
            node: NodeId("m".into()),
            children: vec![CompactChild {
                index: 0,
                status: ChildStatus::Ok,
                digest: Some(Digest("abc".into())),
                input_hash: Some("h".into()),
            }],
        };
        j.compact(r, &[s1, s2], manifest).await.unwrap();

        let events = j.load(r).await.unwrap();
        let seqs: Vec<_> = events.iter().map(|(s, _)| *s).collect();
        assert!(seqs.contains(&s0), "the untouched event stays: {seqs:?}");
        assert!(
            !seqs.contains(&s1) && !seqs.contains(&s2),
            "the compacted events are removed: {seqs:?}"
        );
        assert!(
            events.iter().any(
                |(_, e)| matches!(e, JournalEvent::MapCompacted { node, .. } if node.0 == "m")
            ),
            "the manifest is appended"
        );
    }
}
