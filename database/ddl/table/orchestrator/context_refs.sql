-- The scoped blackboard. Keyed by (run_id, scope_kind, scope_id, ctx_key).
--
-- `run_id` is load-bearing (SP-OPS-1.1, analysis §2.3), not bookkeeping. Before it, the
-- key was (scope_kind, scope_id, ctx_key) and `Scope::Run` mapped to an EMPTY scope_id,
-- so a run-scoped entry was global to the deployment forever. Since node ids are
-- author-chosen and stable, re-running the same graph collided on its first completed
-- node and aborted the drive after the model call had already been paid for -- and the
-- failed insert journals no ContextWrite, so the fold guard never engaged and every
-- resume re-collided. `scope_id`'s old "run id or node path" comment described an intent
-- the code never implemented; the run dimension is now its own column.
--
-- Scope::Run  -> (run_id, 'run',  '')        -- shared by the whole run
-- Scope::Node -> (run_id, 'node', node_path) -- private to one node, resolves up to 'run'
create table if not exists orchestrator.context_refs (
    run_id     uuid        not null,
    scope_kind text        not null,   -- 'run' | 'node'
    scope_id   text        not null,   -- '' for run scope; the node path for node scope
    ctx_key    text        not null,
    ctx_ref    jsonb       not null,   -- serialized ContextRef (references a cas digest)
    created_at timestamptz not null default now(),
    primary key (run_id, scope_kind, scope_id, ctx_key)
);
