create table if not exists orchestrator.runs (
    run_id         uuid        primary key,
    format_version integer     not null,
    created_at     timestamptz not null default now()
);
