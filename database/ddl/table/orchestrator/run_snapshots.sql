create table if not exists orchestrator.run_snapshots (
    run_id     uuid        primary key,
    seq        bigint      not null,
    snapshot   jsonb       not null,
    updated_at timestamptz not null default now()
);
