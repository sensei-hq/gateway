create table if not exists orchestrator.config_versions (
    id         boolean     primary key default true check (id),
    version    bigint      not null default 1,
    updated_at timestamptz not null default now()
);
