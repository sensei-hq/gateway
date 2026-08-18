create table if not exists orchestrator.config_chain_bindings (
    area       text        not null,
    kind       text        not null,
    chain      text        not null,
    updated_at timestamptz not null default now(),
    primary key (area, kind)
);
