-- SP-DATA-1 harness applicator (NOT a dbd migration).
--
-- Single idempotent apply of the durable-run-state schema for the Docker-Postgres test
-- harness (Tasks 1-5) and CI. Mirrors the dbd-authored per-type table DDL verbatim, in
-- dependency order (schema, then the five tables). The contents are INLINED (not `\i`'d):
-- the harness pipes this file into a containerized psql via `docker exec -i ... < file`, so
-- `\i database/ddl/...` paths would resolve against the CONTAINER filesystem and fail. Every
-- statement is `... if not exists`, so re-applying is a clean no-op.
--
-- Authoring workflow remains dbd (edit ddl/, `dbd reconcile`); this file is only the
-- schema-agnostic applicator the sqlx adapters + CI test against. dbd owns schema CREATION
-- via `design.yaml` `schemas: [orchestrator]` (there is no `ddl/schema/` type folder in dbd);
-- the `create schema` below is inlined solely so this psql applicator is self-contained.
-- Lives BESIDE `ddl/` (not inside it) so dbd's folder scanner never mistakes it for a type
-- folder — keeps `dbd inspect` zero-noise. Apply with `psql ... < database/_apply_all.sql`.

-- schema (dbd: design.yaml `schemas: [orchestrator]`)
create schema if not exists orchestrator;

-- ddl/table/orchestrator/journal_events.sql
create table if not exists orchestrator.journal_events (
    seq        bigserial primary key,
    run_id     uuid        not null,
    event      jsonb       not null,
    created_at timestamptz not null default now()
);
create index if not exists journal_events_run_seq_idx
    on orchestrator.journal_events (run_id, seq);

-- ddl/table/orchestrator/cas_blobs.sql
create table if not exists orchestrator.cas_blobs (
    digest     text        primary key,
    bytes      bytea       not null,
    created_at timestamptz not null default now()
);

-- ddl/table/orchestrator/context_refs.sql
create table if not exists orchestrator.context_refs (
    scope_kind text        not null,   -- 'run' | 'node'
    scope_id   text        not null,   -- run id or node path
    ctx_key    text        not null,
    ctx_ref    jsonb       not null,   -- serialized ContextRef (references a cas digest)
    created_at timestamptz not null default now(),
    primary key (scope_kind, scope_id, ctx_key)
);

-- ddl/table/orchestrator/run_snapshots.sql
create table if not exists orchestrator.run_snapshots (
    run_id     uuid        primary key,
    seq        bigint      not null,
    snapshot   jsonb       not null,
    updated_at timestamptz not null default now()
);

-- ddl/table/orchestrator/runs.sql
create table if not exists orchestrator.runs (
    run_id         uuid        primary key,
    format_version integer     not null,
    created_at     timestamptz not null default now()
);

-- ddl/table/orchestrator/config_agents.sql
create table if not exists orchestrator.config_agents (
    name       text        primary key,
    def        jsonb       not null,
    updated_at timestamptz not null default now()
);

-- ddl/table/orchestrator/config_skills.sql
create table if not exists orchestrator.config_skills (
    name       text        primary key,
    def        jsonb       not null,
    updated_at timestamptz not null default now()
);

-- ddl/table/orchestrator/config_tools.sql
create table if not exists orchestrator.config_tools (
    name       text        primary key,
    spec       jsonb       not null,
    updated_at timestamptz not null default now()
);

-- ddl/table/orchestrator/config_chain_bindings.sql
create table if not exists orchestrator.config_chain_bindings (
    area       text        not null,
    kind       text        not null,
    chain      text        not null,
    updated_at timestamptz not null default now(),
    primary key (area, kind)
);

-- ddl/table/orchestrator/config_versions.sql
create table if not exists orchestrator.config_versions (
    id         boolean     primary key default true check (id),
    version    bigint      not null default 1,
    updated_at timestamptz not null default now()
);

-- ddl/table/orchestrator/scheduled_runs.sql
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
