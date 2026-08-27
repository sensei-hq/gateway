create table if not exists orchestrator.cas_blobs (
    digest     text        primary key,
    bytes      bytea       not null,
    created_at timestamptz not null default now()
);
