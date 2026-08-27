create table if not exists orchestrator.config_tools (
    name       text        primary key,
    spec       jsonb       not null,
    updated_at timestamptz not null default now()
);
