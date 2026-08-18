create table if not exists orchestrator.journal_events (
    seq        bigserial primary key,
    run_id     uuid        not null,
    event      jsonb       not null,
    created_at timestamptz not null default now()
);
create index if not exists journal_events_run_seq_idx
    on orchestrator.journal_events (run_id, seq);
