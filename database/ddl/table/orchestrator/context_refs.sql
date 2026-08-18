create table if not exists orchestrator.context_refs (
    scope_kind text        not null,   -- 'run' | 'node'
    scope_id   text        not null,   -- run id or node path
    ctx_key    text        not null,
    ctx_ref    jsonb       not null,   -- serialized ContextRef (references a cas digest)
    created_at timestamptz not null default now(),
    primary key (scope_kind, scope_id, ctx_key)
);
