create table if not exists orchestrator.config_agents (
    name       text        primary key,
    def        jsonb       not null,
    updated_at timestamptz not null default now()
);
