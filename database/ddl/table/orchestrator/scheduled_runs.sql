create table if not exists orchestrator.scheduled_runs (
    run_id     uuid        primary key,
    graph      jsonb       not null,   -- serde(Graph): the ORIGINAL submitted graph
    status     text        not null,   -- 'waking' | 'paused' | 'completed' | 'failed' | 'cancelled'
    next_wake  timestamptz,            -- auto-wake deadline; NULL = no timer (needs force_wake)
    claimed_at timestamptz,            -- lease stamp for 'waking' rows (crash-reclaim)
    reason     text,                   -- last pause/fail reason (observe surface)
    updated_at timestamptz not null default now()
);
create index if not exists scheduled_runs_due_idx
    on orchestrator.scheduled_runs (status, next_wake);
